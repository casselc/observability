using System.Collections.Concurrent;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Routing;
using Microsoft.Net.Http.Headers;
using Oscope.Forwarder.Auth;
using Yarp.ReverseProxy.Forwarder;

namespace Oscope.Forwarder.Http;

/// <summary>The local endpoint's settings (research/entra-ingress.md §4.3).</summary>
public sealed record LocalOptions
{
    /// <summary>The loopback port: the tools' LANGFUSE_BASE_URL is http://127.0.0.1:{Port}. 0: any free port (tests).</summary>
    public int Port { get; init; } = 14318;

    /// <summary>The per-user random pair the tools use as their Langfuse keys (LS-E2): a local secret only.</summary>
    public required string PublicKey { get; init; }

    public required string SecretKey { get; init; }

    public LocalOptions Validate()
    {
        if (Port is < 0 or > 65535) throw new ArgumentException("local: Port out of range");
        if (string.IsNullOrEmpty(PublicKey) || string.IsNullOrEmpty(SecretKey) || SecretKey.Length < 16)
            throw new ArgumentException("local: PublicKey and a SecretKey of at least 16 characters are required (generated per user at install)");
        return this;
    }
}

/// <summary>
/// What the tools talk to: OTLP/HTTP on loopback, Langfuse's paths and the standard ones,
/// proxied live to the ingress (D40, pass-through amendment 2026-10-02). The forwarder's
/// own decisions are only these:
/// <list type="bullet">
/// <item>refuse anything that is not this user's tool (LS-E2, SEC-E3): an Origin header (a
/// web page), a non-loopback Host (DNS rebinding), a wrong local key, a content type or
/// encoding a browser could send without a preflight, another path;</item>
/// <item>refuse a body over the size bound (413) and a request over the concurrency bound
/// (503 + Retry-After: bounded memory under a slow ingress);</item>
/// <item>answer 503 + Retry-After when no token can be had (nobody signed in, the broker
/// unavailable): never a prompt on the tool's path (H-E9, TM-E2);</item>
/// <item>after a 401 from the ingress, have the next request's token refreshed
/// (<see cref="TokenGate"/>), without replaying this one.</item>
/// </list>
/// Everything else is the ingress's answer, passed through: a 200 to the tool means the
/// ingress committed (R-E5). Retries, back-off, timeouts and batching are the tool's
/// exporter's. The forwarder's own answers carry <see cref="LocalHeader"/>, so a tool's log
/// (and a test) can tell them from the ingress's.
/// </summary>
public sealed class LocalEndpoint
{
    /// <summary>Set on every answer the forwarder gives itself, with the reason.</summary>
    public const string LocalHeader = "X-Oscope-Forwarder";

    public static readonly IReadOnlyDictionary<string, string> Paths = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
    {
        ["/v1/traces"] = "/v1/traces",
        ["/v1/logs"] = "/v1/logs",
        ["/api/public/otel/v1/traces"] = "/v1/traces",
        ["/api/public/otel/v1/logs"] = "/v1/logs",
    };

    private readonly ProxyOptions _p;
    private readonly TokenGate _tokens;
    private readonly IHttpForwarder _forwarder;
    private readonly HttpMessageInvoker _client;
    private readonly string _prefix;
    private readonly string? _namespace;
    private readonly ForwarderRequestConfig _config;
    private readonly byte[] _credential;
    private readonly SemaphoreSlim _slots;
    private readonly ConcurrentDictionary<string, long> _refused = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, long> _proxied = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, long> _errors = new(StringComparer.Ordinal);
    private int _inFlight;

    /// <param name="namespaceChoice">from the forwarder's configuration only (MDM), never from the tool; it chooses among the person's grants and never grants</param>
    public LocalEndpoint(LocalOptions local, ProxyOptions proxy, TokenGate tokens, IHttpForwarder forwarder, HttpMessageInvoker client,
        Uri ingress, string? namespaceChoice)
    {
        ArgumentNullException.ThrowIfNull(local);
        ArgumentNullException.ThrowIfNull(proxy);
        ArgumentNullException.ThrowIfNull(ingress);
        local.Validate();
        _p = proxy.Validate();
        _tokens = tokens;
        _forwarder = forwarder;
        _client = client;
        _prefix = ingress.GetLeftPart(UriPartial.Path).TrimEnd('/');
        _namespace = namespaceChoice;
        _config = new ForwarderRequestConfig { ActivityTimeout = proxy.ActivityTimeout };
        _credential = Encoding.UTF8.GetBytes(local.PublicKey + ":" + local.SecretKey);
        _slots = new SemaphoreSlim(proxy.MaxConcurrentRequests);
    }

    /// <summary>The forwarder's own refusals, by reason (none of them reached the ingress).</summary>
    public IReadOnlyDictionary<string, long> Refused => new Dictionary<string, long>(_refused);

    /// <summary>The ingress's answers passed to the tools, by status class (2xx, 4xx, ...).</summary>
    public IReadOnlyDictionary<string, long> Proxied => new Dictionary<string, long>(_proxied);

    /// <summary>Requests YARP could not complete, by its error (the tool got 502/504 or a reset).</summary>
    public IReadOnlyDictionary<string, long> ProxyErrors => new Dictionary<string, long>(_errors);

    public int InFlight => Volatile.Read(ref _inFlight);

    public TokenGate Tokens => _tokens;

    public void Map(IEndpointRouteBuilder app)
    {
        ArgumentNullException.ThrowIfNull(app);
        foreach (var p in Paths.Keys) app.MapPost(p, (RequestDelegate)HandleAsync);
        app.MapGet("/status", (RequestDelegate)StatusAsync);
    }

    private static void Add(ConcurrentDictionary<string, long> d, string key) => d.AddOrUpdate(key, 1, (_, n) => n + 1);

    private async Task Refuse(HttpContext ctx, int status, string reason, TimeSpan? retryAfter = null, string? detail = null)
    {
        Add(_refused, reason);
        var resp = ctx.Response;
        resp.StatusCode = status;
        resp.Headers[LocalHeader] = reason;
        if (retryAfter is { } ra)
            resp.Headers.RetryAfter = ((int)Math.Ceiling(ra.TotalSeconds)).ToString(CultureInfo.InvariantCulture);
        resp.ContentType = "text/plain; charset=utf-8";
        await resp.WriteAsync($"oscope-forwarder: {reason}{(detail is null ? "" : ": " + detail)}\n", ctx.RequestAborted).ConfigureAwait(false);
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

    private Task RefuseGate(HttpContext ctx, string why)
    {
        if (why == "auth") ctx.Response.Headers.WWWAuthenticate = "Basic realm=\"oscope-forwarder\"";
        return Refuse(ctx, why == "auth" ? 401 : 403, why);
    }

    internal async Task HandleAsync(HttpContext ctx)
    {
        if (Gate(ctx) is { } why)
        {
            await RefuseGate(ctx, why).ConfigureAwait(false);
            return;
        }
        var r = ctx.Request;
        if (!Paths.TryGetValue(r.Path.Value ?? "", out var upstreamPath))
        {
            await Refuse(ctx, 404, "path").ConfigureAwait(false);
            return;
        }
        if (!MediaTypeHeaderValue.TryParse(r.ContentType, out var mt) ||
            !(mt.MediaType.Equals("application/x-protobuf", StringComparison.OrdinalIgnoreCase) ||
              mt.MediaType.Equals("application/json", StringComparison.OrdinalIgnoreCase)))
        {
            await Refuse(ctx, 415, "content_type").ConfigureAwait(false);
            return;
        }
        if (r.Headers.ContentEncoding.ToString() is not ("" or "identity" or "gzip"))
        {
            await Refuse(ctx, 415, "content_encoding").ConfigureAwait(false);
            return;
        }
        if (r.ContentLength > _p.MaxRequestBytes)
        {
            await Refuse(ctx, 413, "too_large").ConfigureAwait(false);
            return;
        }
        // A chunked body is cut at the same bound while it streams (Kestrel's limit).
        if (ctx.Features.Get<IHttpMaxRequestBodySizeFeature>() is { IsReadOnly: false } size) size.MaxRequestBodySize = _p.MaxRequestBytes;
        if (!_slots.Wait(0))
        {
            await Refuse(ctx, 503, "busy", _p.RefusalRetryAfter).ConfigureAwait(false);
            return;
        }
        Interlocked.Increment(ref _inFlight);
        try
        {
            await ProxyAsync(ctx, upstreamPath).ConfigureAwait(false);
        }
        finally
        {
            Interlocked.Decrement(ref _inFlight);
            _slots.Release();
        }
    }

    private async Task ProxyAsync(HttpContext ctx, string upstreamPath)
    {
        var tok = await _tokens.GetAsync(ctx.RequestAborted).ConfigureAwait(false);
        if (tok.Status != TokenStatus.Ok || tok.AccessToken is null)
        {
            // 503, not 401: a 401 from this endpoint already means "wrong local key", and
            // OTLP exporters retry a 503 (honouring Retry-After) but drop a 401's batch.
            var what = tok.Status == TokenStatus.InteractionRequired
                ? "nobody is signed in to the forwarder (or the sign-in needs you): sign in from the forwarder's status item"
                : "the sign-in broker is unavailable; try again later";
            await Refuse(ctx, 503, "no_token", _p.RefusalRetryAfter, what).ConfigureAwait(false);
            return;
        }
        var err = await _forwarder.SendAsync(ctx, _prefix, _client, _config,
            new IngressProxy.Transformer(upstreamPath, tok.AccessToken, _namespace), ctx.RequestAborted).ConfigureAwait(false);
        var resp = ctx.Response;
        if (err == ForwarderError.None || resp.HasStarted)
        {
            // The ingress's answer reached the tool (or began to): count it by class.
            Add(_proxied, $"{resp.StatusCode / 100}xx");
            if (resp.StatusCode == StatusCodes.Status401Unauthorized) _tokens.Rejected(tok.AccessToken);
        }
        if (err == ForwarderError.None) return;
        Add(_errors, err.ToString().ToLowerInvariant());
        if (resp.HasStarted) return; // the ingress's answer was cut: the tool sees the connection end
        if (TooLarge(ctx.Features.Get<IForwarderErrorFeature>()?.Exception))
        {
            // A chunked body went over the bound while it streamed: the ingress got a
            // truncated request (and aborted), the tool gets the reason.
            await Refuse(ctx, 413, "too_large").ConfigureAwait(false);
            return;
        }
        // YARP has set 502 (no answer: refused, reset, lost) or 504 (no progress within the
        // activity timeout). Both are retryable for the tool, and both mean the outcome is
        // unknown: the ingress may have committed (CAST rows 50, 74, 83); the tool's retry of
        // the same bytes is then a copy the consumer skips (D11).
        resp.Headers[LocalHeader] = err == ForwarderError.RequestTimedOut ? "ingress_timeout" : "ingress_error";
    }

    private static bool TooLarge(Exception? e)
    {
        for (var x = e; x is not null; x = x.InnerException)
            if (x is BadHttpRequestException { StatusCode: StatusCodes.Status413PayloadTooLarge }) return true;
        return false;
    }

    private async Task StatusAsync(HttpContext ctx)
    {
        if (Gate(ctx) is { } why)
        {
            await RefuseGate(ctx, why).ConfigureAwait(false);
            return;
        }
        ctx.Response.ContentType = "application/json";
        await JsonSerializer.SerializeAsync(ctx.Response.Body, new
        {
            account = _tokens.CurrentAccount?.ObjectId,
            account_changes = _tokens.AccountChanges,
            in_flight = InFlight,
            max_concurrent_requests = _p.MaxConcurrentRequests,
            proxied = Proxied,
            proxy_errors = ProxyErrors,
            local_refused = Refused,
            token_failures = _tokens.Failures,
            forced_token_refreshes = _tokens.ForcedRefreshes,
        }, cancellationToken: ctx.RequestAborted).ConfigureAwait(false);
    }
}
