using System.Diagnostics;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using Oscope.Forwarder.Auth;
using Oscope.Forwarder.Core;
using Xunit;

namespace Oscope.Forwarder.Tests;

/// <summary>
/// The whole forwarder in process — local endpoint, pump, YARP pass-through — against a
/// fake ingress that injects the faults of §10a: lost answers, a slow or stalled ingress,
/// 401/403/429/5xx, a token that expires mid-flight, an account switch (VERIFICATION.md
/// §1 "FI" at the answer boundary, in process).
/// </summary>
public class ForwarderFaultTests
{
    private static byte[] Body(int n, int seed = 1)
    {
        var b = new byte[n];
        new Random(seed).NextBytes(b);
        return b;
    }

    [Fact]
    public async Task The_tools_bytes_pass_through_unchanged_with_only_a_bearer_token()
    {
        OscopeTrace.Covers("FI", "R-E1 R-E6 SEC-E2 SEC-E7 LS-E3 LS-E7");
        await using var rig = await Rig.StartAsync();
        var body = Body(3000);
        var req = new HttpRequestMessage(HttpMethod.Post, "/api/public/otel/v1/traces") { Content = Rig.Proto(body) };
        req.Content.Headers.ContentEncoding.Add("gzip"); // passed through as sent: the forwarder never decodes
        req.Headers.TryAddWithoutValidation("x-langfuse-sdk-name", "opencode");
        req.Headers.TryAddWithoutValidation("X-Oscope-Namespace", "dev-someone-else"); // the tool cannot choose
        req.Headers.TryAddWithoutValidation("x-user-id", "mallory");
        var resp = await rig.Tool.SendAsync(req);
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        await rig.Until(s => s.Committed == 1);
        var seen = Assert.Single(rig.Ingress.Requests);
        Assert.Equal("/v1/traces", seen.Path);
        Assert.Equal(body, seen.Body);
        Assert.Equal("gzip", seen.Headers["content-encoding"]);
        Assert.Equal("application/x-protobuf", seen.Headers["content-type"]);
        Assert.Equal("Bearer " + rig.Tokens.Token, seen.Headers["authorization"]);
        Assert.Equal("dev-payments", seen.Headers["x-oscope-namespace"]);
        Assert.DoesNotContain(seen.Headers.Keys, k => k.StartsWith("x-langfuse", StringComparison.Ordinal) || k == "x-user-id");
    }

    [Fact]
    public async Task A_lost_answer_is_retried_with_the_same_bytes_and_counted_as_unknown_not_failed()
    {
        OscopeTrace.Covers("FI", "CAST-50 H-E6 LS-E3 R-E6 UCA-E6");
        await using var rig = await Rig.StartAsync();
        rig.Ingress.Script.Enqueue(FakeIngress.LoseAnswer);
        var body = Body(500, 2);
        Assert.Equal(HttpStatusCode.OK, (await rig.Send(body)).StatusCode);
        var s = await rig.Until(s => s.Committed == 1);
        Assert.Equal(1, s.CommittedAfterUnknown);
        Assert.Equal(1, s.UnknownOutcomes);
        Assert.Equal(0, s.DroppedTotal);
        var seen = rig.Ingress.Requests.ToArray();
        Assert.Equal(2, seen.Length);
        Assert.Equal(seen[0].Body, seen[1].Body); // a copy: the ingress's stamp makes the same content key (TestRetryIsACopy)
        Assert.Equal(body, seen[1].Body);
    }

    [Fact]
    public async Task A_token_that_expires_mid_flight_is_refreshed_and_the_request_resent()
    {
        OscopeTrace.Covers("FI", "R-E6 H-E9 UCA-E8 TM-E2");
        await using var rig = await Rig.StartAsync();
        var stale = rig.Tokens.Token;
        rig.Ingress.Default = (ctx, seen) =>
        {
            ctx.Response.StatusCode = seen.Headers["authorization"] == "Bearer " + stale ? 401 : 200;
            return Task.CompletedTask;
        };
        Assert.Equal(HttpStatusCode.OK, (await rig.Send(Body(100))).StatusCode);
        var s = await rig.Until(s => s.Committed == 1);
        Assert.Equal(1, rig.Tokens.Forced);
        Assert.Equal(2, s.Attempts);
        Assert.Equal(0, s.UnknownOutcomes); // a 401 is refused before the body is read: definite, not unknown
    }

    [Fact]
    public async Task A_stalled_ingress_never_blocks_the_tool_and_memory_stays_bounded()
    {
        OscopeTrace.Covers("FI", "R-E7 H-E9 H-E4 CAST-38 TM-E2");
        await using var rig = await Rig.StartAsync();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        rig.Ingress.Gate = gate;
        var bound = Rig.FastQueue.MaxQueueBytes;
        int ok = 0, refused = 0;
        var slowest = TimeSpan.Zero;
        for (var i = 0; i < 40; i++)
        {
            var sw = Stopwatch.StartNew();
            var resp = await rig.Send(Body(4000, i));
            sw.Stop();
            if (sw.Elapsed > slowest) slowest = sw.Elapsed;
            if (resp.StatusCode == HttpStatusCode.OK) ok++;
            else
            {
                Assert.Equal(HttpStatusCode.ServiceUnavailable, resp.StatusCode); // backpressure, never an unbounded buffer
                Assert.NotNull(resp.Headers.RetryAfter);
                refused++;
            }
            Assert.True(rig.Counters.HeldBytes <= bound, $"held {rig.Counters.HeldBytes} > {bound}");
        }
        Assert.True(slowest < TimeSpan.FromSeconds(2), $"the tool waited {slowest}");
        Assert.Equal(16, ok); // 16 × 4000 = 64000 of 65536 bytes
        Assert.Equal(24, refused);
        gate.SetResult();
        var s = await rig.Until(s => s.HeldEntries == 0);
        Assert.Equal(16, s.Committed);
        Assert.True(s.Balances);
        Assert.Equal(24, s.Refused[RefusalReason.Full]);
    }

    [Fact]
    public async Task Rate_limits_and_server_errors_are_retried_and_definite_refusals_are_counted_drops()
    {
        OscopeTrace.Covers("FI", "H-E4 R-E5 CAST-39 CAST-15");
        await using var rig = await Rig.StartAsync();
        rig.Ingress.Script.Enqueue(FakeIngress.Status(429, 1));
        rig.Ingress.Script.Enqueue(FakeIngress.Status(503));
        rig.Ingress.Script.Enqueue(FakeIngress.Status(200));
        Assert.Equal(HttpStatusCode.OK, (await rig.Send(Body(10, 1))).StatusCode);
        var s = await rig.Until(s => s.Committed == 1);
        Assert.Equal(3, s.Attempts);
        Assert.Equal(1, s.CommittedAfterUnknown); // the 503 may have committed
        rig.Ingress.Script.Enqueue(FakeIngress.Status(403));
        Assert.Equal(HttpStatusCode.OK, (await rig.Send(Body(10, 2))).StatusCode);
        await rig.Until(s => s.Dropped.GetValueOrDefault(DropReason.Forbidden) == 1);
        rig.Ingress.Script.Enqueue(FakeIngress.Status(400));
        Assert.Equal(HttpStatusCode.OK, (await rig.Send(Body(10, 3))).StatusCode);
        s = await rig.Until(s => s.Dropped.GetValueOrDefault(DropReason.Rejected) == 1);
        Assert.True(s.Balances);
        Assert.Equal(5, rig.Ingress.Requests.Count);
    }

    [Fact]
    public async Task Requests_accepted_under_one_person_are_never_sent_under_another()
    {
        OscopeTrace.Covers("FI", "H-E1 R-E6 LS-E5 UCA-E6");
        // Slow retries, so Alice's entries are still held when the account changes.
        await using var rig = await Rig.StartAsync(Rig.FastQueue with
        {
            MaxAttempts = 100, MaxAge = TimeSpan.FromSeconds(30), BackoffBase = TimeSpan.FromMilliseconds(200), BackoffCap = TimeSpan.FromSeconds(1),
        });
        rig.Tokens.Status = TokenStatus.Unavailable; // the broker is unreachable for a moment: Alice's entries wait
        Assert.Equal(HttpStatusCode.OK, (await rig.Send(Body(10, 1))).StatusCode);
        Assert.Equal(HttpStatusCode.OK, (await rig.Send(Body(10, 2))).StatusCode);
        rig.Tokens.Account = CoreModelTests.Bob; // the person signs out; Bob signs in
        rig.Tokens.Status = TokenStatus.Ok;
        Assert.Equal(HttpStatusCode.OK, (await rig.Send(Body(10, 3))).StatusCode);
        var s = await rig.Until(s => s.HeldEntries == 0);
        Assert.Equal(2, s.Dropped[DropReason.AccountChanged]);
        Assert.Equal(1, s.Committed);
        var seen = Assert.Single(rig.Ingress.Requests);
        Assert.Contains(CoreModelTests.Bob.ObjectId, seen.Headers["authorization"], StringComparison.Ordinal);
        Assert.Equal(Body(10, 3), seen.Body);
    }

    [Fact]
    public async Task With_no_account_signed_in_the_tool_is_told_to_retry_and_nothing_is_held()
    {
        OscopeTrace.Covers("FI", "H-E4 H-E9 UCA-E8 TM-E1");
        await using var rig = await Rig.StartAsync();
        rig.Tokens.Account = null;
        var resp = await rig.Send(Body(10));
        Assert.Equal(HttpStatusCode.ServiceUnavailable, resp.StatusCode);
        Assert.NotNull(resp.Headers.RetryAfter);
        Assert.Equal(0, rig.Counters.Accepted);
        Assert.Equal(1, rig.Counters.Refused[RefusalReason.NoAccount]);
    }

    [Fact]
    public async Task Stopping_counts_every_held_entry()
    {
        OscopeTrace.Covers("FI", "H-E4 R-E7 UCA-E7");
        var rig = await Rig.StartAsync();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        rig.Ingress.Gate = gate;
        for (var i = 0; i < 5; i++) Assert.Equal(HttpStatusCode.OK, (await rig.Send(Body(100, i))).StatusCode);
        await rig.Until(s => s.InFlight == 2);
        await rig.Fwd.Pump.StopAsync(); // two in flight past the drain timeout, three queued
        var s = rig.Counters;
        Assert.Equal(5, s.Accepted);
        Assert.Equal(0, s.HeldEntries);
        Assert.Equal(3, s.Dropped[DropReason.Shutdown]);
        Assert.Equal(2, s.DroppedMaybeLanded[DropReason.Shutdown]);
        Assert.True(s.Balances);
        await rig.DisposeAsync();
    }

    [Theory]
    [InlineData("origin", HttpStatusCode.Forbidden)]
    [InlineData("host", HttpStatusCode.Forbidden)]
    [InlineData("key", HttpStatusCode.Unauthorized)]
    [InlineData("text", HttpStatusCode.UnsupportedMediaType)]
    [InlineData("big", HttpStatusCode.RequestEntityTooLarge)]
    [InlineData("deflate", HttpStatusCode.UnsupportedMediaType)]
    public async Task The_local_endpoint_refuses_anything_but_this_users_tool(string what, HttpStatusCode want)
    {
        OscopeTrace.Covers("FI", "SEC-E3 LS-E2 SEC-E9");
        await using var rig = await Rig.StartAsync();
        var req = new HttpRequestMessage(HttpMethod.Post, "/v1/traces") { Content = Rig.Proto(Body(what == "big" ? 20_000 : 10)) };
        switch (what)
        {
            case "origin": req.Headers.TryAddWithoutValidation("Origin", "https://evil.example"); break;
            case "host": req.Headers.Host = "evil.example"; break;
            case "key": req.Headers.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes("pk:wrong"))); break;
            case "text": req.Content.Headers.ContentType = new MediaTypeHeaderValue("text/plain"); break;
            case "deflate": req.Content.Headers.ContentEncoding.Add("deflate"); break;
        }
        var resp = await rig.Tool.SendAsync(req);
        Assert.Equal(want, resp.StatusCode);
        Assert.Equal(0, rig.Counters.Accepted);
        Assert.Empty(rig.Ingress.Requests);
    }

    [Fact]
    public async Task A_connection_that_never_opened_is_not_sent_and_anything_later_is_unknown()
    {
        OscopeTrace.Covers("FI", "CAST-50 CAST-2");
        using var client = new HttpMessageInvoker(new SocketsHttpHandler { ConnectTimeout = TimeSpan.FromSeconds(20) } // Windows answers a closed port only after ~2 s of SYN retries);
        var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();
        Microsoft.Extensions.DependencyInjection.LoggingServiceCollectionExtensions.AddLogging(services);
        Microsoft.Extensions.DependencyInjection.ReverseProxyServiceCollectionExtensions.AddHttpForwarder(services);
        using var sp = Microsoft.Extensions.DependencyInjection.ServiceCollectionContainerBuilderExtensions.BuildServiceProvider(services);
        var fwd = Microsoft.Extensions.DependencyInjection.ServiceProviderServiceExtensions.GetRequiredService<Yarp.ReverseProxy.Forwarder.IHttpForwarder>(sp);
        // A port nobody listens on: the connection is refused, so nothing can have arrived.
        var sender = new Http.YarpIngressSender(fwd, client, new Uri("http://127.0.0.1:1/"), TimeSpan.FromSeconds(5), null);
        var attempt = new Attempt(1, new ForwardRequest("/v1/traces", "application/x-protobuf", null, Body(10)), CoreModelTests.Alice, 1, false);
        var o = await sender.SendAsync(attempt, "tok", CancellationToken.None);
        Assert.Equal(OutcomeKind.NotSent, o.Kind);
        Assert.Equal(OutcomeKind.Unknown, Http.YarpIngressSender.Classify(Yarp.ReverseProxy.Forwarder.ForwarderError.Request, new IOException("reset")).Kind);
        Assert.Equal(OutcomeKind.Unknown, Http.YarpIngressSender.Classify(Yarp.ReverseProxy.Forwarder.ForwarderError.RequestTimedOut, null).Kind);
    }

    [Fact]
    public void Retry_after_is_read_as_seconds_or_a_date()
    {
        OscopeTrace.Covers("P", "CAST-39");
        Assert.Equal(TimeSpan.FromSeconds(5), Http.YarpIngressSender.RetryAfter("5"));
        Assert.Null(Http.YarpIngressSender.RetryAfter("soon"));
        Assert.Null(Http.YarpIngressSender.RetryAfter(""));
        var d = Http.YarpIngressSender.RetryAfter(DateTimeOffset.UtcNow.AddSeconds(30).ToString("r", System.Globalization.CultureInfo.InvariantCulture));
        Assert.NotNull(d);
        Assert.InRange(d!.Value.TotalSeconds, 25, 31);
    }
}
