using System.Diagnostics;
using System.Net;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CsCheck;
using Microsoft.AspNetCore.Http;
using Oscope.Forwarder.Auth;
using Oscope.Forwarder.Http;
using Xunit;

namespace Oscope.Forwarder.Tests;

/// <summary>
/// The pass-through's contract (D40, pass-through amendment 2026-10-02), in process on
/// real Kestrel and real sockets, against a fake ingress: the body arrives unchanged and the
/// ingress's answer comes back unchanged; only the bearer token, the configured namespace and
/// the framing headers leave; a 401 refreshes the NEXT request's token and is never
/// replayed; no token is a local 503; the request streams, and a slow ingress holds the
/// tool back by TCP, not by buffering; the local endpoint refuses anything but this user's
/// tool.
/// </summary>
public class ProxyTests
{
    private static readonly HashSet<string> AllowedAtIngress = new(StringComparer.OrdinalIgnoreCase)
    {
        "host", "content-type", "content-length", "transfer-encoding", "content-encoding", "authorization", "x-oscope-namespace",
    };

    private static string Sha(byte[] b) => Convert.ToHexStringLower(SHA256.HashData(b));

    private static bool Has(HttpResponseMessage r, string header) => r.Headers.TryGetValues(header, out _);

    // ---------------------------------------------------------------- the passthrough property

    private const string Leak = "zqleak";

    /// <summary>Client headers a tool (or something pretending to be one) might send; none may reach the ingress.</summary>
    private static readonly string[] ClientHeaders =
    [
        "x-user-id", "x-langfuse-sdk-name", "x-langfuse-public-key", "X-Oscope-Namespace", "traceparent", "tracestate", "baggage",
        "Cookie", "Forwarded", "X-Forwarded-For", "X-Forwarded-Host", "User-Agent", "Accept-Encoding", "Content-Language",
        "X-Ms-Client-Principal-Id", "Via", "Prefer", "X-Oscope-Forwarder",
    ];

    private static readonly int[] Statuses = [200, 400, 401, 403, 404, 413, 415, 429, 500, 502, 503, 504];

    public sealed record Case(int BodyLen, int BodySeed, bool Json, bool Gzip, bool Chunked, bool LangfusePath,
        int StatusIx, int RetryAfter, int AnswerLen, int AnswerSeed, int[] Headers, int ValueSeed)
    {
        public int Status => Statuses[StatusIx];
        public override string ToString() =>
            $"body={BodyLen}/{BodySeed} json={Json} gzip={Gzip} chunked={Chunked} langfuse={LangfusePath} status={Status} " +
            $"retry_after={RetryAfter} answer={AnswerLen}/{AnswerSeed} headers=[{string.Join(",", Headers.Select(i => ClientHeaders[i]))}]";
    }

    private static readonly Gen<Case> GenCase =
        Gen.Select(
            Gen.Select(Gen.Int[0, 300_000], Gen.Int, Gen.Bool, Gen.Bool, (n, s, j, g) => (n, s, j, g)),
            Gen.Select(Gen.Bool, Gen.Bool, Gen.Int[0, Statuses.Length - 1], Gen.Int[0, 3], (c, l, st, ra) => (c, l, st, ra)),
            Gen.Select(Gen.Int[0, 70_000], Gen.Int, Gen.Int[0, ClientHeaders.Length - 1].Array[0, 8], Gen.Int, (an, asd, h, v) => (an, asd, h, v)),
            (a, b, c) => new Case(a.n, a.s, a.j, a.g, b.c, b.l, b.st, b.ra, c.an, c.asd, c.h.Distinct().ToArray(), c.v));

    /// <summary>A body that is sent without a length (chunked), a few KiB per write.</summary>
    private sealed class ChunkedContent(byte[] body) : HttpContent
    {
        protected override async Task SerializeToStreamAsync(Stream stream, TransportContext? context)
        {
            for (var i = 0; i < body.Length; i += 7000) await stream.WriteAsync(body.AsMemory(i, Math.Min(7000, body.Length - i)));
        }

        protected override bool TryComputeLength(out long length)
        {
            length = 0;
            return false;
        }
    }

    private static async Task RunCase(Rig rig, Case c, (int Status, string? Token) prev)
    {
        var body = Rig.Body(c.BodyLen, c.BodySeed);
        var answer = Rig.Body(c.AnswerLen, c.AnswerSeed);
        var ct = c.Json ? "application/json" : "application/x-protobuf";
        rig.Ingress.Answer = async (ctx, _) =>
        {
            ctx.Response.StatusCode = c.Status;
            ctx.Response.ContentType = "application/x-test-answer";
            if (c.RetryAfter > 0) ctx.Response.Headers.RetryAfter = c.RetryAfter.ToString(System.Globalization.CultureInfo.InvariantCulture);
            ctx.Response.Headers["X-Ingress-Says"] = "seq-" + c.AnswerSeed;
            await ctx.Response.Body.WriteAsync(answer);
        };
        HttpContent content = c.Chunked ? new ChunkedContent(body) : new ByteArrayContent(body);
        content.Headers.ContentType = new MediaTypeHeaderValue(ct);
        if (c.Gzip) content.Headers.ContentEncoding.Add("gzip"); // passed through as sent: the forwarder never decodes
        var path = c.LangfusePath ? "/api/public/otel/v1/traces" : "/v1/logs";
        var req = new HttpRequestMessage(HttpMethod.Post, path) { Content = content };
        var rnd = new Random(c.ValueSeed);
        var traceId = Convert.ToHexStringLower(RandomNumberGenerator.GetBytes(16));
        foreach (var i in c.Headers)
        {
            var name = ClientHeaders[i];
            var value = name switch
            {
                "traceparent" => $"00-{traceId}-{Convert.ToHexStringLower(RandomNumberGenerator.GetBytes(8))}-01",
                _ => $"{Leak}{rnd.Next():x}",
            };
            if (name == "Content-Language") content.Headers.TryAddWithoutValidation(name, value);
            else req.Headers.TryAddWithoutValidation(name, value);
        }
        var before = rig.Ingress.Requests.Count;
        var forcedBefore = rig.Tokens.Forced;
        using var resp = await rig.Tool.SendAsync(req);
        var got = await resp.Content.ReadAsByteArrayAsync();

        // The ingress's answer, unchanged: status, body, Retry-After, its own headers.
        Assert.Equal(c.Status, (int)resp.StatusCode);
        Assert.Equal(Sha(answer), Sha(got));
        Assert.Equal("application/x-test-answer", resp.Content.Headers.ContentType?.MediaType);
        Assert.Equal(c.RetryAfter > 0 ? c.RetryAfter.ToString(System.Globalization.CultureInfo.InvariantCulture) : null,
            resp.Headers.TryGetValues("Retry-After", out var ra) ? string.Join(",", ra) : null);
        Assert.Equal("seq-" + c.AnswerSeed, resp.Headers.GetValues("X-Ingress-Says").Single());
        Assert.False(Has(resp, LocalEndpoint.LocalHeader), "an ingress answer was marked as the forwarder's own");

        // One request reached the ingress, the body unchanged, and nothing else of the tool's.
        Assert.Equal(before + 1, rig.Ingress.Requests.Count);
        var seen = rig.Ingress.Requests.ToArray()[^1];
        Assert.Equal(c.LangfusePath ? "/v1/traces" : "/v1/logs", seen.Path);
        Assert.Equal(Sha(body), Sha(seen.Body));
        Assert.Equal(ct, seen.Headers["content-type"]);
        Assert.Equal(c.Gzip ? "gzip" : null, seen.Headers.GetValueOrDefault("content-encoding"));
        Assert.Equal("dev-payments", seen.Headers["x-oscope-namespace"]);
        Assert.Equal("Bearer " + rig.Tokens.LastIssued, seen.Headers["authorization"]);
        foreach (var (k, v) in seen.Headers)
        {
            Assert.Contains(k, AllowedAtIngress); // no other header of the tool's reached the ingress
            Assert.DoesNotContain(Leak, v, StringComparison.Ordinal);
            Assert.DoesNotContain(traceId, v, StringComparison.Ordinal);
        }

        // After a 401, this request (the next) went out with a fresh token, forced once.
        if (prev.Status == 401)
        {
            Assert.Equal(forcedBefore + 1, rig.Tokens.Forced);
            Assert.NotEqual("Bearer " + prev.Token, seen.Headers["authorization"]);
        }
        else Assert.Equal(forcedBefore, rig.Tokens.Forced);
    }

    [Fact]
    public async Task Any_body_status_and_header_set_passes_through_unchanged_with_only_the_bearer_token_added()
    {
        OscopeTrace.Covers("PH", "R-E1 R-E5 R-E6 SEC-E2 SEC-E7 LS-E3 LS-E7 UCA-E4 H-E4");
        await using var rig = await Rig.StartAsync();
        (int Status, string? Token) prev = (0, null);
        // One rig, cases in order (threads: 1): the property about 401 spans two requests.
        await GenCase.SampleAsync(async c =>
        {
            try { await RunCase(rig, c, prev); }
            finally { prev = (c.Status, rig.Tokens.LastIssued); }
        }, iter: 150, threads: 1, print: c => c.ToString());
        Assert.Empty(rig.Local.Refused);
        Assert.Empty(rig.Local.ProxyErrors);
    }

    // ---------------------------------------------------------------- headers

    [Fact]
    public async Task Only_the_bearer_token_the_namespace_and_framing_reach_the_ingress()
    {
        OscopeTrace.Covers("FI", "R-E1 SEC-E2 SEC-E7 LS-E7 H-E1");
        await using var rig = await Rig.StartAsync();
        var body = Rig.Body(3000);
        var req = new HttpRequestMessage(HttpMethod.Post, "/api/public/otel/v1/traces") { Content = Rig.Proto(body) };
        req.Headers.TryAddWithoutValidation("x-langfuse-sdk-name", "opencode");
        req.Headers.TryAddWithoutValidation("X-Oscope-Namespace", "dev-someone-else"); // the tool cannot choose
        req.Headers.TryAddWithoutValidation("x-user-id", "mallory");
        req.Headers.TryAddWithoutValidation("traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
        req.Headers.TryAddWithoutValidation("X-Forwarded-For", "10.0.0.66");
        var resp = await rig.Tool.SendAsync(req);
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        var seen = Assert.Single(rig.Ingress.Requests);
        Assert.Equal("/v1/traces", seen.Path);
        Assert.Equal(body, seen.Body);
        Assert.Equal("Bearer " + rig.Tokens.LastIssued, seen.Headers["authorization"]); // not the tool's local key
        Assert.Equal("dev-payments", seen.Headers["x-oscope-namespace"]);
        Assert.Equal(3000, long.Parse(seen.Headers["content-length"], System.Globalization.CultureInfo.InvariantCulture));
        Assert.All(seen.Headers.Keys, k => Assert.Contains(k, AllowedAtIngress));
        Assert.DoesNotContain(seen.Headers.Values, v => v.Contains("0af7651916cd43dd8448eb211c80319c", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Without_a_configured_namespace_no_namespace_header_is_sent()
    {
        OscopeTrace.Covers("FI", "SEC-E7 R-E1");
        await using var rig = await Rig.StartAsync(ns: null);
        var req = new HttpRequestMessage(HttpMethod.Post, "/v1/traces") { Content = Rig.Proto(Rig.Body(10)) };
        req.Headers.TryAddWithoutValidation("X-Oscope-Namespace", "dev-search");
        Assert.Equal(HttpStatusCode.OK, (await rig.Tool.SendAsync(req)).StatusCode);
        Assert.DoesNotContain("x-oscope-namespace", Assert.Single(rig.Ingress.Requests).Headers.Keys);
    }

    // ---------------------------------------------------------------- tokens

    [Fact]
    public async Task After_a_401_the_next_request_gets_a_fresh_token_and_nothing_is_replayed()
    {
        OscopeTrace.Covers("FI", "R-E6 UCA-E8 H-E9 TM-E2 H-E6");
        await using var rig = await Rig.StartAsync();
        var refused = new HashSet<string>();
        rig.Ingress.Answer = async (ctx, seen) =>
        {
            if (refused.Count == 0) refused.Add(seen.Headers["authorization"]); // the first token "expired in flight"
            if (refused.Contains(seen.Headers["authorization"]))
            {
                ctx.Response.StatusCode = 401;
                ctx.Response.Headers.WWWAuthenticate = "Bearer error=\"invalid_token\"";
                await ctx.Response.WriteAsync("invalid token");
                return;
            }
            ctx.Response.StatusCode = 200;
        };
        var r1 = await rig.Send(Rig.Body(100, 1));
        Assert.Equal(HttpStatusCode.Unauthorized, r1.StatusCode); // the ingress's 401, passed through
        Assert.Equal("Bearer error=\"invalid_token\"", r1.Headers.WwwAuthenticate.ToString());
        Assert.Equal("invalid token", await r1.Content.ReadAsStringAsync());
        Assert.False(Has(r1, LocalEndpoint.LocalHeader));
        await Task.Delay(300);
        Assert.Single(rig.Ingress.Requests); // not replayed
        Assert.Equal(0, rig.Tokens.Forced);

        var r2 = await rig.Send(Rig.Body(100, 2));
        Assert.Equal(HttpStatusCode.OK, r2.StatusCode);
        Assert.Equal(1, rig.Tokens.Forced);
        var seen = rig.Ingress.Requests.ToArray();
        Assert.Equal(2, seen.Length);
        Assert.NotEqual(seen[0].Headers["authorization"], seen[1].Headers["authorization"]);
        Assert.Equal(Rig.Body(100, 2), seen[1].Body); // the second request's own bytes, not the first's

        Assert.Equal(HttpStatusCode.OK, (await rig.Send(Rig.Body(100, 3))).StatusCode);
        Assert.Equal(1, rig.Tokens.Forced); // one refresh per refusal
        Assert.Equal(1, rig.Local.Tokens.ForcedRefreshes);
    }

    [Fact]
    public async Task A_refused_refreshed_token_cannot_drive_a_refresh_loop()
    {
        OscopeTrace.Covers("ML", "CAST-39 R-E6 H-E5");
        await using var rig = await Rig.StartAsync(Rig.Fast with { ForcedRefreshMinInterval = TimeSpan.FromHours(1) });
        rig.Ingress.Answer = FakeIngress.Status(401); // every token refused (a revoked grant, say)
        for (var i = 0; i < 10; i++) Assert.Equal(HttpStatusCode.Unauthorized, (await rig.Send(Rig.Body(10, i))).StatusCode);
        Assert.Equal(1, rig.Tokens.Forced); // one forced refresh, then none within the interval
        Assert.Equal(10, rig.Ingress.Requests.Count); // and no request was replayed
    }

    public static TheoryData<string> NoTokenCases => new() { "signed_out", "unavailable", "hung" };

    [Theory]
    [MemberData(nameof(NoTokenCases))]
    public async Task With_no_token_the_tool_is_answered_503_with_Retry_After_and_nothing_is_sent(string why)
    {
        OscopeTrace.Covers("FI", "H-E9 UCA-E8 TM-E1 TM-E2 CAST-39");
        await using var rig = await Rig.StartAsync();
        switch (why)
        {
            case "signed_out": rig.Tokens.Account = null; break;
            case "unavailable": rig.Tokens.Status = TokenStatus.Unavailable; break;
            case "hung": rig.Tokens.Hang = new TaskCompletionSource(); break;
        }
        var sw = Stopwatch.StartNew();
        var resp = await rig.Send(Rig.Body(10));
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(5), $"the tool waited {sw.Elapsed} (the token timeout is 1 s)");
        Assert.Equal(HttpStatusCode.ServiceUnavailable, resp.StatusCode);
        Assert.Equal<TimeSpan?>(TimeSpan.FromSeconds(1), resp.Headers.RetryAfter?.Delta);
        Assert.Equal("no_token", resp.Headers.GetValues(LocalEndpoint.LocalHeader).Single());
        var text = await resp.Content.ReadAsStringAsync();
        Assert.Contains(why == "signed_out" ? "sign in" : "unavailable", text, StringComparison.Ordinal);
        Assert.Empty(rig.Ingress.Requests);
        Assert.Equal(1, rig.Local.Refused["no_token"]);
        Assert.Equal(1, rig.Local.Tokens.Failures[why switch { "signed_out" => "interactionrequired", "unavailable" => "unavailable", _ => "timeout" }]);
        rig.Tokens.Hang?.TrySetResult();
    }

    [Fact]
    public async Task Each_request_goes_out_under_whoever_is_signed_in_when_it_is_sent()
    {
        OscopeTrace.Covers("FI", "R-E6 LS-E5 H-E1 TM-E1");
        await using var rig = await Rig.StartAsync();
        Assert.Equal(HttpStatusCode.OK, (await rig.Send(Rig.Body(10, 1))).StatusCode);
        rig.Tokens.Account = FakeTokens.Bob; // Alice signs out, Bob signs in
        Assert.Equal(HttpStatusCode.OK, (await rig.Send(Rig.Body(10, 2))).StatusCode);
        var seen = rig.Ingress.Requests.ToArray();
        Assert.Contains(FakeTokens.Alice.ObjectId, seen[0].Headers["authorization"], StringComparison.Ordinal);
        Assert.Contains(FakeTokens.Bob.ObjectId, seen[1].Headers["authorization"], StringComparison.Ordinal);
        Assert.Equal(1, rig.Local.Tokens.AccountChanges); // shown on /status
        var status = await Status(rig);
        Assert.Equal(FakeTokens.Bob.ObjectId, status.GetProperty("account").GetString());
        Assert.Equal(1, status.GetProperty("account_changes").GetInt64());
        Assert.Equal(2, status.GetProperty("proxied").GetProperty("2xx").GetInt64());
    }

    private static async Task<JsonElement> Status(Rig rig) =>
        JsonDocument.Parse(await rig.Tool.GetStringAsync("/status")).RootElement;

    // ---------------------------------------------------------------- streaming and bounds

    /// <summary>Writes the first part, then waits for <paramref name="gate"/> before the rest.</summary>
    private sealed class GatedContent(byte[] body, int first, Task gate) : HttpContent
    {
        protected override async Task SerializeToStreamAsync(Stream stream, TransportContext? context)
        {
            await stream.WriteAsync(body.AsMemory(0, first));
            await stream.FlushAsync();
            try { await gate.WaitAsync(TimeSpan.FromSeconds(15)); }
            catch (TimeoutException) { throw new IOException("the ingress never saw the first part while the tool held the rest: the forwarder buffered"); }
            await stream.WriteAsync(body.AsMemory(first));
        }

        protected override bool TryComputeLength(out long length)
        {
            length = body.Length;
            return true;
        }
    }

    [Fact]
    public async Task The_request_is_streamed_not_buffered()
    {
        OscopeTrace.Covers("FI", "R-E7 CAST-38");
        await using var rig = await Rig.StartAsync();
        var body = Rig.Body(256 * 1024, 7);
        var firstPart = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        rig.Ingress.Handler = async ctx =>
        {
            var buf = new byte[16 * 1024];
            using var all = new MemoryStream();
            int n;
            while ((n = await ctx.Request.Body.ReadAsync(buf)) > 0)
            {
                all.Write(buf, 0, n);
                if (all.Length >= 32 * 1024) firstPart.TrySetResult(); // the tool still holds the rest
            }
            ctx.Response.StatusCode = 200;
            await ctx.Response.WriteAsync(Sha(all.ToArray()));
        };
        var content = new GatedContent(body, 64 * 1024, firstPart.Task);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/x-protobuf");
        var resp = await rig.Tool.PostAsync("/v1/traces", content);
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        Assert.Equal(Sha(body), await resp.Content.ReadAsStringAsync());
    }

    /// <summary>Counts what the tool's HTTP client has taken from the body (written, or about to be).</summary>
    private sealed class CountingStream(byte[] data) : MemoryStream(data, writable: false)
    {
        private long _read;
        public long Taken => Interlocked.Read(ref _read);

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            var n = await base.ReadAsync(buffer, cancellationToken);
            Interlocked.Add(ref _read, n);
            return n;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var n = base.Read(buffer, offset, count);
            Interlocked.Add(ref _read, n);
            return n;
        }
    }

    [Fact]
    public async Task A_slow_ingress_holds_the_tool_back_by_TCP_and_the_tool_sees_its_latency()
    {
        OscopeTrace.Covers("FI", "R-E7 CAST-38 H-E9");
        // Small socket buffers everywhere on the path, so what the path itself can hold is known
        // (well under 2 MiB); a forwarder that buffered would take all 16 MiB from the tool.
        const int sock = 64 * 1024;
        await using var rig = await Rig.StartAsync(Rig.Fast with { MaxRequestBytes = 16 << 20 }, socketBuffer: sock);
        var body = Rig.Body(16 << 20, 9);
        long received = 0;
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        rig.Ingress.Handler = async ctx =>
        {
            var buf = new byte[16 * 1024];
            using var sha = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            int n;
            var stalled = false;
            while ((n = await ctx.Request.Body.ReadAsync(buf)) > 0)
            {
                sha.AppendData(buf, 0, n);
                Interlocked.Add(ref received, n);
                if (!stalled && Interlocked.Read(ref received) >= 256 * 1024)
                {
                    stalled = true;
                    await release.Task; // the ingress stops reading: slow, or stuck
                }
            }
            ctx.Response.StatusCode = 200;
            await ctx.Response.WriteAsync(Convert.ToHexStringLower(sha.GetHashAndReset()));
        };
        var source = new CountingStream(body);
        var content = new StreamContent(source, 16 * 1024);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/x-protobuf");
        var sw = Stopwatch.StartNew();
        var send = rig.Tool.PostAsync("/v1/traces", content);
        // Let the path fill: until the tool's writes stop advancing.
        long last = -1;
        for (var i = 0; i < 100 && source.Taken != last; i++)
        {
            last = source.Taken;
            await Task.Delay(200);
        }
        var held = source.Taken - Interlocked.Read(ref received);
        Assert.False(send.IsCompleted, "the tool was answered while the ingress had not read the body");
        Assert.True(held < 2 << 20, $"between the tool and the stalled ingress {held} bytes were held: the forwarder buffered");
        Assert.True(source.Taken < body.Length / 2, $"the tool wrote {source.Taken} of {body.Length} bytes to a stalled ingress");
        await Task.Delay(500);
        release.SetResult();
        var resp = await send;
        Assert.True(sw.Elapsed >= TimeSpan.FromMilliseconds(500), "the tool did not see the ingress's latency");
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        Assert.Equal(Sha(body), await resp.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task Over_the_concurrency_bound_the_tool_is_answered_503_at_once()
    {
        OscopeTrace.Covers("FI", "R-E7 CAST-38 H-E9 CAST-21");
        await using var rig = await Rig.StartAsync(Rig.Fast with { MaxConcurrentRequests = 2 });
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        rig.Ingress.Handler = async ctx =>
        {
            await ctx.Request.Body.CopyToAsync(Stream.Null);
            await release.Task;
            ctx.Response.StatusCode = 200;
        };
        var a = rig.Send(Rig.Body(10, 1));
        var b = rig.Send(Rig.Body(10, 2));
        var sw = Stopwatch.StartNew();
        while (rig.Local.InFlight < 2)
        {
            Assert.True(sw.Elapsed < TimeSpan.FromSeconds(10), "two requests never got in flight");
            await Task.Delay(20);
        }
        sw.Restart();
        var c = await rig.Send(Rig.Body(10, 3));
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(2), $"the third request waited {sw.Elapsed}");
        Assert.Equal(HttpStatusCode.ServiceUnavailable, c.StatusCode);
        Assert.Equal("busy", c.Headers.GetValues(LocalEndpoint.LocalHeader).Single());
        Assert.NotNull(c.Headers.RetryAfter);
        release.SetResult();
        Assert.Equal(HttpStatusCode.OK, (await a).StatusCode);
        Assert.Equal(HttpStatusCode.OK, (await b).StatusCode);
        Assert.Equal(0, rig.Local.InFlight);
    }

    // ---------------------------------------------------------------- failures below HTTP

    [Fact]
    public async Task A_lost_answer_is_502_to_the_tool_and_never_replayed_the_tools_retry_is_its_own()
    {
        OscopeTrace.Covers("FI", "CAST-50 H-E6 LS-E3 UCA-E4");
        await using var rig = await Rig.StartAsync();
        rig.Ingress.Script.Enqueue(FakeIngress.LoseAnswer); // it arrived (and may have committed); the answer is lost
        var body = Rig.Body(500, 2);
        var r1 = await rig.Send(body);
        Assert.Equal(HttpStatusCode.BadGateway, r1.StatusCode); // retryable for OTLP: the outcome is unknown, not failed
        Assert.Equal("ingress_error", r1.Headers.GetValues(LocalEndpoint.LocalHeader).Single());
        await Task.Delay(300);
        Assert.Single(rig.Ingress.Requests);
        // The tool's exporter retries the same bytes (a D11 copy if the first landed).
        Assert.Equal(HttpStatusCode.OK, (await rig.Send(body)).StatusCode);
        var seen = rig.Ingress.Requests.ToArray();
        Assert.Equal(2, seen.Length);
        Assert.Equal(seen[0].Body, seen[1].Body);
        Assert.NotEmpty(rig.Local.ProxyErrors);
    }

    [Fact]
    public async Task An_ingress_that_never_answers_is_504_after_the_activity_timeout()
    {
        OscopeTrace.Covers("FI", "CAST-39 CAST-50 H-E9");
        await using var rig = await Rig.StartAsync(Rig.Fast with { ActivityTimeout = TimeSpan.FromSeconds(2), TokenTimeout = TimeSpan.FromSeconds(1) });
        var never = new TaskCompletionSource();
        rig.Ingress.Handler = async ctx =>
        {
            await ctx.Request.Body.CopyToAsync(Stream.Null);
            await never.Task.WaitAsync(ctx.RequestAborted);
        };
        var sw = Stopwatch.StartNew();
        var r = await rig.Send(Rig.Body(10));
        Assert.Equal(HttpStatusCode.GatewayTimeout, r.StatusCode);
        Assert.Equal("ingress_timeout", r.Headers.GetValues(LocalEndpoint.LocalHeader).Single());
        Assert.InRange(sw.Elapsed.TotalSeconds, 1.5, 20);
    }

    [Fact]
    public async Task A_connection_that_never_opened_is_502()
    {
        OscopeTrace.Covers("FI", "CAST-50 CAST-2");
        // A port nobody listens on (Windows answers a closed port only after ~2 s of SYN retries).
        await using var rig = await Rig.StartAsync(ingressOverride: new Uri("http://127.0.0.1:1/"));
        var r = await rig.Send(Rig.Body(10));
        Assert.Equal(HttpStatusCode.BadGateway, r.StatusCode);
        Assert.Equal("ingress_error", r.Headers.GetValues(LocalEndpoint.LocalHeader).Single());
    }

    // ---------------------------------------------------------------- the local endpoint

    [Theory]
    [InlineData("origin", HttpStatusCode.Forbidden)]
    [InlineData("host", HttpStatusCode.Forbidden)]
    [InlineData("key", HttpStatusCode.Unauthorized)]
    [InlineData("nokey", HttpStatusCode.Unauthorized)]
    [InlineData("text", HttpStatusCode.UnsupportedMediaType)]
    [InlineData("big", HttpStatusCode.RequestEntityTooLarge)]
    [InlineData("bigchunked", HttpStatusCode.RequestEntityTooLarge)]
    [InlineData("deflate", HttpStatusCode.UnsupportedMediaType)]
    [InlineData("path", HttpStatusCode.NotFound)]
    public async Task The_local_endpoint_refuses_anything_but_this_users_tool(string what, HttpStatusCode want)
    {
        OscopeTrace.Covers("FI", "SEC-E3 LS-E2 SEC-E9");
        await using var rig = await Rig.StartAsync(Rig.Fast with { MaxRequestBytes = 64 * 1024 });
        var body = Rig.Body(what.StartsWith("big", StringComparison.Ordinal) ? 200_000 : 10);
        HttpContent content = what == "bigchunked" ? new ChunkedContent(body) : new ByteArrayContent(body);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/x-protobuf");
        var req = new HttpRequestMessage(HttpMethod.Post, what == "path" ? "/v1/metrics" : "/v1/traces") { Content = content };
        switch (what)
        {
            case "origin": req.Headers.TryAddWithoutValidation("Origin", "https://evil.example"); break;
            case "host": req.Headers.Host = "evil.example"; break;
            case "key": req.Headers.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes("pk:wrong"))); break;
            case "nokey": req.Headers.Authorization = new AuthenticationHeaderValue("Bearer", "anything"); break;
            case "text": content.Headers.ContentType = new MediaTypeHeaderValue("text/plain"); break;
            case "deflate": content.Headers.ContentEncoding.Add("deflate"); break;
        }
        var resp = await rig.Tool.SendAsync(req);
        Assert.Equal(want, resp.StatusCode);
        if (what != "path") Assert.True(Has(resp, LocalEndpoint.LocalHeader), "a local refusal is marked as the forwarder's");
        // Nothing of a refused request was committed: at most a cut-off body reached the
        // ingress (the chunked case streams until the bound), and it was never answered 200.
        if (what != "bigchunked") Assert.Empty(rig.Ingress.Requests);
        Assert.DoesNotContain("2xx", rig.Local.Proxied.Keys);
    }

    [Fact]
    public async Task Status_needs_the_local_key()
    {
        OscopeTrace.Covers("FI", "SEC-E3 LS-E2 TM-E1");
        await using var rig = await Rig.StartAsync();
        using var anon = new HttpClient { BaseAddress = rig.Fwd.Address };
        Assert.Equal(HttpStatusCode.Unauthorized, (await anon.GetAsync("/status")).StatusCode);
        var s = await Status(rig);
        Assert.Equal(FakeTokens.Alice.ObjectId, s.GetProperty("account").GetString());
        Assert.Equal(Rig.Fast.MaxConcurrentRequests, s.GetProperty("max_concurrent_requests").GetInt32());
    }

    // ---------------------------------------------------------------- configuration

    [Fact]
    public void Options_that_cannot_work_together_are_refused()
    {
        OscopeTrace.Covers("P", "CAST-5 CAST-25");
        var ok = new ProxyOptions();
        Assert.Same(ok, ok.Validate());
        Assert.Throws<ArgumentException>(() => (ok with { MaxRequestBytes = ProxyOptions.IngressMaxBodyBytes + 1 }).Validate());
        Assert.Throws<ArgumentException>(() => (ok with { MaxRequestBytes = 0 }).Validate());
        Assert.Throws<ArgumentException>(() => (ok with { MaxConcurrentRequests = 0 }).Validate());
        Assert.Throws<ArgumentException>(() => (ok with { TokenTimeout = TimeSpan.FromSeconds(200) }).Validate()); // over ActivityTimeout
        Assert.Throws<ArgumentException>(() => (ok with { RefusalRetryAfter = TimeSpan.FromMilliseconds(500) }).Validate()); // would be Retry-After: 0
        Assert.Throws<ArgumentException>(() => (ok with { ForcedRefreshMinInterval = TimeSpan.FromSeconds(-1) }).Validate());
        var local = new LocalOptions { PublicKey = "pk", SecretKey = "0123456789abcdef" };
        Assert.Throws<ArgumentException>(() => (local with { SecretKey = "short" }).Validate());
        Assert.Throws<ArgumentException>(() => new ForwarderSettings { Ingress = new Uri("http://ingress.example.com/"), Local = local }.Validate());
        new ForwarderSettings { Ingress = new Uri("https://ingress.example.com/"), Local = local }.Validate();
    }
}
