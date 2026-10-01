using System.Collections.Concurrent;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Net.Http.Headers;
using Oscope.Forwarder.Core;

namespace Oscope.Forwarder.Http;

/// <summary>The local endpoint's settings (research/entra-ingress.md §4.3).</summary>
public sealed record LocalOptions
{
    /// <summary>The loopback port: the tools' LANGFUSE_BASE_URL is http://127.0.0.1:{Port}.</summary>
    public int Port { get; init; } = 14318;

    /// <summary>The per-user random pair the tools use as their Langfuse keys (LS-E2): a local secret only.</summary>
    public required string PublicKey { get; init; }

    public required string SecretKey { get; init; }

    /// <summary>Requests being read at once; with the queue's bound this bounds memory (MaxQueueBytes + this × MaxEntryBytes).</summary>
    public int MaxConcurrentIntake { get; init; } = 2;

    public LocalOptions Validate()
    {
        if (Port is <= 0 or > 65535) throw new ArgumentException("local: Port out of range");
        if (string.IsNullOrEmpty(PublicKey) || string.IsNullOrEmpty(SecretKey) || SecretKey.Length < 16)
            throw new ArgumentException("local: PublicKey and a SecretKey of at least 16 characters are required (generated per user at install)");
        if (MaxConcurrentIntake <= 0) throw new ArgumentException("local: MaxConcurrentIntake must be > 0");
        return this;
    }
}

/// <summary>
/// What the tools talk to: OTLP/HTTP on loopback, Langfuse's paths and the standard
/// ones. Refuses anything that is not this user's tool (LS-E2, SEC-E3): a wrong local key,
/// an Origin header (a web page), a non-loopback Host (DNS rebinding), a content type a
/// browser can send without a preflight. An accepted request is answered at once from
/// memory (the tool never waits for Entra or the network: H-E9, TM-E2); a full queue is
/// answered 503 with Retry-After (backpressure, never buffering beyond the bound: R-E7).
/// </summary>
public sealed class LocalEndpoint
{
    public static readonly IReadOnlyDictionary<string, string> Paths = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
    {
        ["/v1/traces"] = "/v1/traces",
        ["/v1/logs"] = "/v1/logs",
        ["/api/public/otel/v1/traces"] = "/v1/traces",
        ["/api/public/otel/v1/logs"] = "/v1/logs",
    };

    private readonly Pump _pump;
    private readonly LocalOptions _o;
    private readonly byte[] _credential;
    private readonly SemaphoreSlim _intake;
    private readonly ConcurrentDictionary<string, long> _refused = new(StringComparer.Ordinal);

    public LocalEndpoint(Pump pump, LocalOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _pump = pump;
        _o = options.Validate();
        _credential = Encoding.UTF8.GetBytes(options.PublicKey + ":" + options.SecretKey);
        _intake = new SemaphoreSlim(options.MaxConcurrentIntake);
    }

    /// <summary>Refusals at the local endpoint, by reason (none of them was acknowledged).</summary>
    public IReadOnlyDictionary<string, long> Refused => new Dictionary<string, long>(_refused);

    public void Map(IEndpointRouteBuilder app)
    {
        ArgumentNullException.ThrowIfNull(app);
        foreach (var p in Paths.Keys) app.MapPost(p, (RequestDelegate)HandleAsync);
        app.MapGet("/status", (RequestDelegate)StatusAsync);
    }

    private Task Refuse(HttpContext ctx, int status, string reason, TimeSpan? retryAfter = null)
    {
        _refused.AddOrUpdate(reason, 1, (_, n) => n + 1);
        ctx.Response.StatusCode = status;
        if (retryAfter is { } ra)
            ctx.Response.Headers.RetryAfter = ((int)Math.Ceiling(ra.TotalSeconds)).ToString(CultureInfo.InvariantCulture);
        return Task.CompletedTask;
    }

    /// <summary>Loopback Host, no Origin, the local key: whatever else the request is.</summary>
    private string? Gate(HttpContext ctx)
    {
        var r = ctx.Request;
        if (r.Headers.ContainsKey(HeaderNames.Origin)) return "origin";
        var host = r.Host.Host;
        if (host is not ("127.0.0.1" or "localhost" or "[::1]" or "::1")) return "host";
        var auth = r.Headers.Authorization.ToString();
        if (!auth.StartsWith("Basic ", StringComparison.Ordinal)) return "auth";
        byte[] given;
        try { given = Convert.FromBase64String(auth["Basic ".Length..].Trim()); }
        catch (FormatException) { return "auth"; }
        return CryptographicOperations.FixedTimeEquals(given, _credential) ? null : "auth";
    }

    internal async Task HandleAsync(HttpContext ctx)
    {
        var why = Gate(ctx);
        if (why is not null)
        {
            if (why == "auth") ctx.Response.Headers.WWWAuthenticate = "Basic realm=\"oscope-forwarder\"";
            await Refuse(ctx, why == "auth" ? 401 : 403, why).ConfigureAwait(false);
            return;
        }
        var r = ctx.Request;
        if (!MediaTypeHeaderValue.TryParse(r.ContentType, out var mt) ||
            !(mt.MediaType.Equals("application/x-protobuf", StringComparison.OrdinalIgnoreCase) ||
              mt.MediaType.Equals("application/json", StringComparison.OrdinalIgnoreCase)))
        {
            await Refuse(ctx, 415, "content_type").ConfigureAwait(false);
            return;
        }
        var enc = r.Headers.ContentEncoding.ToString();
        if (enc is not ("" or "identity" or "gzip"))
        {
            await Refuse(ctx, 415, "content_encoding").ConfigureAwait(false);
            return;
        }
        var max = _pump.Options.MaxEntryBytes;
        if (r.ContentLength > max)
        {
            await Refuse(ctx, 413, "too_large").ConfigureAwait(false);
            return;
        }
        if (!await _intake.WaitAsync(TimeSpan.FromMilliseconds(250), ctx.RequestAborted).ConfigureAwait(false))
        {
            await Refuse(ctx, 503, "busy", TimeSpan.FromSeconds(1)).ConfigureAwait(false);
            return;
        }
        byte[] body;
        try
        {
            body = await ReadBoundedAsync(r.Body, max, ctx.RequestAborted).ConfigureAwait(false);
        }
        catch (InvalidDataException)
        {
            await Refuse(ctx, 413, "too_large").ConfigureAwait(false);
            return;
        }
        finally
        {
            _intake.Release();
        }
        if (!Paths.TryGetValue(r.Path.Value ?? "", out var upstreamPath))
        {
            await Refuse(ctx, 404, "path").ConfigureAwait(false);
            return;
        }
        var a = _pump.Offer(new ForwardRequest(upstreamPath, mt.MediaType.ToString().ToLowerInvariant(),
            enc is "" or "identity" ? null : enc, body));
        if (!a.Accepted)
        {
            var ra = _pump.Options.RefusalRetryAfter;
            await (a.Reason switch
            {
                RefusalReason.TooLarge => Refuse(ctx, 413, "too_large"),
                RefusalReason.NoAccount => Refuse(ctx, 503, "no_account", ra),
                RefusalReason.ShuttingDown => Refuse(ctx, 503, "shutting_down", ra),
                _ => Refuse(ctx, 503, "full", ra),
            }).ConfigureAwait(false);
            return;
        }
        // OTLP/HTTP success: an empty Export*ServiceResponse in the request's encoding.
        ctx.Response.StatusCode = 200;
        if (mt.MediaType.Equals("application/json", StringComparison.OrdinalIgnoreCase))
        {
            ctx.Response.ContentType = "application/json";
            await ctx.Response.WriteAsync("{}", ctx.RequestAborted).ConfigureAwait(false);
        }
        else
        {
            ctx.Response.ContentType = "application/x-protobuf";
        }
    }

    private async Task StatusAsync(HttpContext ctx)
    {
        var why = Gate(ctx);
        if (why is not null)
        {
            await Refuse(ctx, why == "auth" ? 401 : 403, why).ConfigureAwait(false);
            return;
        }
        var s = _pump.Snapshot();
        ctx.Response.ContentType = "application/json";
        await JsonSerializer.SerializeAsync(ctx.Response.Body, new
        {
            account = _pump.CurrentAccount?.ObjectId,
            accepted = s.Accepted,
            committed = s.Committed,
            committed_after_unknown = s.CommittedAfterUnknown,
            attempts = s.Attempts,
            unknown_outcomes = s.UnknownOutcomes,
            held_entries = s.HeldEntries,
            held_bytes = s.HeldBytes,
            in_flight = s.InFlight,
            oldest_age_s = s.OldestAge.TotalSeconds,
            clock_regressions = s.ClockRegressions,
            balances = s.Balances,
            refused = s.Refused.ToDictionary(kv => kv.Key.ToString().ToLowerInvariant(), kv => kv.Value),
            dropped = s.Dropped,
            dropped_maybe_landed = s.DroppedMaybeLanded,
            local_refused = Refused,
            token_failures = _pump.TokenFailures,
        }, cancellationToken: ctx.RequestAborted).ConfigureAwait(false);
    }

    /// <summary>Reads at most <paramref name="max"/> bytes; more is <see cref="InvalidDataException"/>.</summary>
    internal static async Task<byte[]> ReadBoundedAsync(Stream s, long max, CancellationToken ct)
    {
        using var ms = new MemoryStream();
        var buf = new byte[64 * 1024];
        while (true)
        {
            var n = await s.ReadAsync(buf, ct).ConfigureAwait(false);
            if (n == 0) return ms.ToArray();
            if (ms.Length + n > max) throw new InvalidDataException("body over the per-entry bound");
            ms.Write(buf, 0, n);
        }
    }
}
