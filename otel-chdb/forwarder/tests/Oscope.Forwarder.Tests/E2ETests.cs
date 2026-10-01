using System.Diagnostics;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using Xunit;

namespace Oscope.Forwarder.Tests;

/// <summary>
/// The harness pair started by ci/forwarder.sh: the Go ingress (real handler, fake Entra,
/// in-memory store; ingress/cmd/ingress-e2e) and the forwarder process
/// (tests/Oscope.Forwarder.E2EHost). Run only with --filter Category=E2E, where the
/// environment names both; a missing variable fails the test (an unrun test is not a pass).
/// </summary>
public sealed class E2E : IDisposable
{
    public const string Alice = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
    public const string Bob = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";

    public HttpClient Ingress { get; }
    public HttpClient Forwarder { get; }

    public E2E()
    {
        static string Need(string n) => Environment.GetEnvironmentVariable(n) is { Length: > 0 } v ? v
            : throw new InvalidOperationException($"{n} is not set: run through ci/forwarder.sh e2e");
        Ingress = new HttpClient { BaseAddress = new Uri(Need("OSCOPE_E2E_INGRESS")), Timeout = TimeSpan.FromSeconds(30) };
        Forwarder = new HttpClient { BaseAddress = new Uri(Need("OSCOPE_E2E_FORWARDER")), Timeout = TimeSpan.FromSeconds(30) };
        var pk = Environment.GetEnvironmentVariable("OSCOPE_E2E_PK") ?? "pk-lf-local-e2e";
        var sk = Environment.GetEnvironmentVariable("OSCOPE_E2E_SK") ?? "sk-lf-local-e2e-0123456789";
        Forwarder.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes($"{pk}:{sk}")));
    }

    public void Dispose()
    {
        Ingress.Dispose();
        Forwarder.Dispose();
    }

    public async Task Reset()
    {
        (await Ingress.PostAsync("/_e2e/fault?kind=clear", null)).EnsureSuccessStatusCode();
        (await Ingress.PostAsync($"/_e2e/broker?oid={Alice}&role=Team.Payments&lifetime_s=4500&lie_s=0&mode=ok", null)).EnsureSuccessStatusCode();
        // The forwarder asks the broker which account is signed in every AccountRefresh.
        var sw = Stopwatch.StartNew();
        while (true)
        {
            var s = await Status();
            if (s.GetProperty("account").ValueKind == JsonValueKind.String && s.GetProperty("account").GetString() == Alice
                && s.GetProperty("held_entries").GetInt64() == 0) return;
            if (sw.Elapsed > TimeSpan.FromSeconds(20)) throw new TimeoutException($"the forwarder never settled on {Alice}: {s}");
            await Task.Delay(50);
        }
    }

    public async Task<byte[]> Sample(int seed, int spans = 0) =>
        await Ingress.GetByteArrayAsync($"/_e2e/sample?format=proto&n={spans}&seed={seed}");

    public async Task<HttpResponseMessage> Send(byte[] body, string path = "/api/public/otel/v1/traces")
    {
        var c = new ByteArrayContent(body);
        c.Headers.ContentType = new MediaTypeHeaderValue("application/x-protobuf");
        return await Forwarder.PostAsync(path, c);
    }

    public async Task<JsonElement> Status() => await Forwarder.GetFromJsonAsync<JsonElement>("/status");

    public async Task<long> IngressCount(string outcome)
    {
        var c = await Ingress.GetFromJsonAsync<Dictionary<string, long>>("/_e2e/counts");
        return c!.GetValueOrDefault(outcome);
    }

    public sealed record Commit(string Key, string Signal, string ContentKey, List<Dictionary<string, string>> Resources);

    public async Task<List<Commit>> Commits()
    {
        var doc = await Ingress.GetFromJsonAsync<JsonElement>("/_e2e/commits");
        return doc.EnumerateArray().Select(c => new Commit(c.GetProperty("key").GetString()!, c.GetProperty("signal").GetString()!,
            c.GetProperty("content_key").GetString()!,
            c.GetProperty("resources").ValueKind == JsonValueKind.Array
                ? c.GetProperty("resources").EnumerateArray().Select(r => r.EnumerateObject().ToDictionary(p => p.Name, p => p.Value.GetString() ?? "")).ToList()
                : [])).ToList();
    }

    /// <summary>Waits until the forwarder holds nothing; returns its status then.</summary>
    public async Task<JsonElement> Drained(int seconds = 60)
    {
        var sw = Stopwatch.StartNew();
        while (true)
        {
            var s = await Status();
            if (s.GetProperty("held_entries").GetInt64() == 0) return s;
            if (sw.Elapsed > TimeSpan.FromSeconds(seconds)) throw new TimeoutException($"forwarder still holds entries: {s}");
            await Task.Delay(50);
        }
    }

    public static long Get(JsonElement s, string name) => s.GetProperty(name).GetInt64();

    public static long Sub(JsonElement s, string obj, string key) =>
        s.GetProperty(obj).TryGetProperty(key, out var v) ? v.GetInt64() : 0;
}

[Trait("Category", "E2E")]
public class E2ETests
{
    [Fact]
    public async Task Committed_under_the_person_of_the_token_never_the_tools_claims()
    {
        OscopeTrace.Covers("IT", "R-E1 H-E1 SEC-E2 LS-E1 UCA-E2 R-E6");
        using var e = new E2E();
        await e.Reset();
        var before = (await e.Commits()).Select(c => c.Key).ToHashSet();
        var body = await e.Sample(seed: 101, spans: 3);
        Assert.Equal(HttpStatusCode.OK, (await e.Send(body)).StatusCode);
        var s = await e.Drained();
        Assert.True(s.GetProperty("balances").GetBoolean());
        var fresh = (await e.Commits()).Where(c => !before.Contains(c.Key) && c.Signal == "traces").ToList();
        var res = Assert.Single(fresh).Resources.First();
        Assert.Equal(E2E.Alice, res["user.id"]);
        Assert.Equal("dev-payments", res["k8s.namespace.name"]);
        Assert.Equal("devtools", res["k8s.cluster.name"]);
        Assert.Equal("mallory@example.com", res["oscope.ingress.claimed.user.id"]);
        Assert.Equal("prod-billing", res["oscope.ingress.claimed.k8s.namespace.name"]);
    }

    [Fact]
    public async Task A_lost_answer_after_the_commit_is_resent_as_a_copy()
    {
        OscopeTrace.Covers("IT", "CAST-50 H-E6 LS-E3 R-E4 R-E6 UCA-E3");
        using var e = new E2E();
        await e.Reset();
        var s0 = await e.Status();
        var before = (await e.Commits()).Select(c => c.Key).ToHashSet();
        var ok0 = await e.IngressCount("ok");
        (await e.Ingress.PostAsync("/_e2e/fault?kind=lose_answer&n=1", null)).EnsureSuccessStatusCode();
        Assert.Equal(HttpStatusCode.OK, (await e.Send(await e.Sample(seed: 202))).StatusCode);
        var s1 = await e.Drained();
        Assert.Equal(2, await e.IngressCount("ok") - ok0); // committed twice at the ingress
        Assert.Equal(1, E2E.Get(s1, "committed_after_unknown") - E2E.Get(s0, "committed_after_unknown"));
        Assert.Equal(1, E2E.Get(s1, "committed") - E2E.Get(s0, "committed"));
        var fresh = (await e.Commits()).Where(c => !before.Contains(c.Key) && c.Signal == "traces").ToList();
        Assert.NotEmpty(fresh);
        Assert.Single(fresh.Select(c => c.ContentKey).Distinct()); // ...as one content key: a copy the consumer skips (D11)
    }

    [Fact]
    public async Task An_unresolved_commit_is_retried_until_it_lands()
    {
        OscopeTrace.Covers("IT", "R-E5 H-E4 CAST-15 CAST-39");
        using var e = new E2E();
        await e.Reset();
        var s0 = await e.Status();
        var un0 = await e.IngressCount("unavailable");
        (await e.Ingress.PostAsync("/_e2e/fault?kind=store_drop&n=64", null)).EnsureSuccessStatusCode();
        Assert.Equal(HttpStatusCode.OK, (await e.Send(await e.Sample(seed: 303))).StatusCode);
        var sw = Stopwatch.StartNew();
        while (await e.IngressCount("unavailable") == un0)
        {
            Assert.True(sw.Elapsed < TimeSpan.FromSeconds(30), "the ingress never answered 503");
            await Task.Delay(50);
        }
        (await e.Ingress.PostAsync("/_e2e/fault?kind=clear", null)).EnsureSuccessStatusCode();
        var s1 = await e.Drained();
        Assert.Equal(1, E2E.Get(s1, "committed") - E2E.Get(s0, "committed"));
        Assert.True(E2E.Get(s1, "unknown_outcomes") > E2E.Get(s0, "unknown_outcomes"));
    }

    [Fact]
    public async Task A_token_that_expires_in_flight_is_refreshed_without_the_tool_noticing()
    {
        OscopeTrace.Covers("IT", "R-E6 H-E9 UCA-E8 TM-E2");
        using var e = new E2E();
        await e.Reset();
        // The broker hands out a token that lives 1 s but claims to live an hour.
        (await e.Ingress.PostAsync("/_e2e/broker?lifetime_s=1&lie_s=3600", null)).EnsureSuccessStatusCode();
        Assert.Equal(HttpStatusCode.OK, (await e.Send(await e.Sample(seed: 404))).StatusCode);
        await e.Drained();
        await Task.Delay(TimeSpan.FromSeconds(2.5)); // past its expiry and the ingress's 1 s leeway
        var s0 = await e.Status();
        var unauth0 = await e.IngressCount("unauthenticated");
        var sw = Stopwatch.StartNew();
        Assert.Equal(HttpStatusCode.OK, (await e.Send(await e.Sample(seed: 405))).StatusCode);
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(2), $"the tool waited {sw.Elapsed}");
        var s1 = await e.Drained();
        Assert.Equal(1, E2E.Get(s1, "committed") - E2E.Get(s0, "committed"));
        Assert.True(await e.IngressCount("unauthenticated") > unauth0, "the stale token was never presented");
        Assert.Equal(0, E2E.Get(s1, "unknown_outcomes") - E2E.Get(s0, "unknown_outcomes"));
    }

    [Fact]
    public async Task Held_requests_are_dropped_not_sent_when_another_person_signs_in()
    {
        OscopeTrace.Covers("IT", "H-E1 R-E6 LS-E5 UCA-E6");
        using var e = new E2E();
        await e.Reset();
        var s0 = await e.Status();
        var before = (await e.Commits()).Select(c => c.Key).ToHashSet();
        (await e.Ingress.PostAsync("/_e2e/fault?kind=status&code=503&n=1000&retry_after=1", null)).EnsureSuccessStatusCode();
        Assert.Equal(HttpStatusCode.OK, (await e.Send(await e.Sample(seed: 505))).StatusCode);
        await Task.Delay(500);
        (await e.Ingress.PostAsync($"/_e2e/broker?oid={E2E.Bob}", null)).EnsureSuccessStatusCode();
        (await e.Ingress.PostAsync("/_e2e/fault?kind=clear", null)).EnsureSuccessStatusCode();
        var s1 = await e.Drained();
        Assert.Equal(1, E2E.Sub(s1, "dropped_maybe_landed", "account_changed") + E2E.Sub(s1, "dropped", "account_changed")
                        - E2E.Sub(s0, "dropped_maybe_landed", "account_changed") - E2E.Sub(s0, "dropped", "account_changed"));
        var fresh = (await e.Commits()).Where(c => !before.Contains(c.Key) && c.Signal == "traces").ToList();
        Assert.DoesNotContain(fresh, c => c.Resources.Any(r => r.GetValueOrDefault("user.id") == E2E.Bob));
        Assert.Empty(fresh);
    }

    [Fact]
    public async Task A_slow_ingress_never_blocks_the_tool_and_the_forwarder_holds_no_more_than_its_bound()
    {
        OscopeTrace.Covers("IT", "R-E7 H-E9 H-E4 CAST-38 TM-E2");
        using var e = new E2E();
        await e.Reset();
        var s0 = await e.Status();
        var bound = long.Parse(Environment.GetEnvironmentVariable("OSCOPE_E2E_MAX_QUEUE_BYTES") ?? "1048576", System.Globalization.CultureInfo.InvariantCulture);
        (await e.Ingress.PostAsync("/_e2e/fault?kind=delay&n=60&delay_ms=700", null)).EnsureSuccessStatusCode();
        var body = await e.Sample(seed: 606, spans: 600);
        int ok = 0, busy = 0;
        var slowest = TimeSpan.Zero;
        long maxHeld = 0;
        for (var i = 0; i < 40; i++)
        {
            var b = await e.Sample(seed: 6000 + i, spans: 600);
            var sw = Stopwatch.StartNew();
            var r = await e.Send(b);
            if (sw.Elapsed > slowest) slowest = sw.Elapsed;
            if (r.StatusCode == HttpStatusCode.OK) ok++;
            else
            {
                Assert.Equal(HttpStatusCode.ServiceUnavailable, r.StatusCode);
                busy++;
            }
            maxHeld = Math.Max(maxHeld, E2E.Get(await e.Status(), "held_bytes"));
        }
        Assert.True(body.Length * 4 < bound, "the test's bodies are sized to fill the queue");
        Assert.True(slowest < TimeSpan.FromSeconds(2), $"the tool waited {slowest}");
        Assert.True(maxHeld <= bound, $"held {maxHeld} > {bound}");
        Assert.True(busy > 0, $"the queue never filled ({ok} accepted)");
        var s1 = await e.Drained(120);
        Assert.Equal(ok, E2E.Get(s1, "accepted") - E2E.Get(s0, "accepted"));
        Assert.Equal(ok, E2E.Get(s1, "committed") - E2E.Get(s0, "committed"));
        Assert.True(s1.GetProperty("balances").GetBoolean());
    }

    [Fact]
    public async Task The_forwarders_counters_reach_the_persons_own_namespace()
    {
        OscopeTrace.Covers("IT", "H-E4 TM-E1 R-E7");
        using var e = new E2E();
        await e.Reset();
        // A second forwarder process, reporting every second (the first reports never, so its
        // counters stay exact for the other tests).
        using var reporting = new HttpClient { BaseAddress = new Uri(Environment.GetEnvironmentVariable("OSCOPE_E2E_FORWARDER_REPORTING")
            ?? throw new InvalidOperationException("OSCOPE_E2E_FORWARDER_REPORTING is not set")) };
        reporting.DefaultRequestHeaders.Authorization = e.Forwarder.DefaultRequestHeaders.Authorization;
        Assert.Equal(HttpStatusCode.OK, (await reporting.GetAsync("/status")).StatusCode);
        var sw = Stopwatch.StartNew();
        while (true)
        {
            var reports = (await e.Commits()).Where(c => c.Signal == "logs"
                && c.Resources.Any(r => r.GetValueOrDefault("service.name") == "oscope-forwarder")).ToList();
            if (reports.Count > 0)
            {
                var r = reports[0].Resources.First(r => r.GetValueOrDefault("service.name") == "oscope-forwarder");
                Assert.Equal("dev-payments", r["k8s.namespace.name"]);
                Assert.False(string.IsNullOrEmpty(r["user.id"]));
                return;
            }
            Assert.True(sw.Elapsed < TimeSpan.FromSeconds(30), "no counters report was committed");
            await Task.Delay(250);
        }
    }
}
