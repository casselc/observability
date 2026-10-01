using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Text;
using System.Text.Json;
using Xunit;

namespace Oscope.Forwarder.Tests;

/// <summary>
/// The nightly stress run (ci/forwarder.sh stress; nightly job forwarder-stress), for a
/// bounded time (OSCOPE_STRESS_SECONDS). Several tools send at once through one forwarder
/// process to the Go ingress while a chaos loop rotates the faults of the STPA analysis;
/// each fault is correlated with what it threatens (research/entra-ingress.md §10b):
///
///   lost answers after commit         CAST 50/74/83, H-E6, LS-E3   (unknown, never failed; copies)
///   unresolved commits (503)          R-E5, CAST 15, CAST 39       (bounded retries)
///   429 from the ingress              H-E5, CAST 35                (back off, no hot loop)
///   a slow ingress                    R-E7, H-E9, CAST 38          (bounded memory, the tool not blocked)
///   tokens expiring mid-flight        R-E6, H-E9, UCA-E8           (refresh, never a prompt)
///   the broker unavailable            H-E9, H-E4                   (held, then counted)
///   another person signing in         H-E1, LS-E5                  (never sent under them)
///
/// Checked all along: the ledger balances (H-E4: accepted = committed + dropped + held),
/// held bytes stay under the bound, the forwarder's resident memory stays under a bound
/// that does not grow with the bytes sent, and no tool request waits more than 2 s. At
/// the end, with the faults cleared: everything drains, and the ingress committed at
/// least as many requests as the forwarder counted committed (a copy may add more).
/// The summary goes to OSCOPE_STRESS_OUT (JSON) for the job's step summary.
/// </summary>
[Trait("Category", "Stress")]
public class StressTests
{
    private sealed record Fault(string Name, string Ids, Func<E2E, Task> Start, Func<E2E, Task> Stop);

    private static Task Post(E2E e, string url) => e.Ingress.PostAsync(url, null).ContinueWith(t => t.Result.EnsureSuccessStatusCode());

    private static readonly Fault[] Faults =
    [
        new("lost answers", "CAST-50 H-E6 LS-E3", e => Post(e, "/_e2e/fault?kind=lose_answer&n=20"), e => Post(e, "/_e2e/fault?kind=clear")),
        new("unresolved commits", "R-E5 CAST-15 CAST-39", e => Post(e, "/_e2e/fault?kind=store_drop&n=40"), e => Post(e, "/_e2e/fault?kind=clear")),
        new("rate limited", "H-E5 CAST-35", e => Post(e, "/_e2e/fault?kind=status&code=429&n=30&retry_after=1"), e => Post(e, "/_e2e/fault?kind=clear")),
        new("503 before handling", "R-E5 CAST-39", e => Post(e, "/_e2e/fault?kind=status&code=503&n=30&retry_after=2"), e => Post(e, "/_e2e/fault?kind=clear")),
        new("slow ingress", "R-E7 H-E9 CAST-38", e => Post(e, "/_e2e/fault?kind=delay&n=200&delay_ms=400"), e => Post(e, "/_e2e/fault?kind=clear")),
        new("token expiring in flight", "R-E6 H-E9 UCA-E8", e => Post(e, "/_e2e/broker?lifetime_s=2&lie_s=3600"), e => Post(e, "/_e2e/broker?lifetime_s=4500&lie_s=0")),
        new("broker unavailable", "H-E9 H-E4", e => Post(e, "/_e2e/broker?mode=unavailable"), e => Post(e, "/_e2e/broker?mode=ok")),
        new("another person signs in", "H-E1 LS-E5", e => Post(e, $"/_e2e/broker?oid={E2E.Bob}"), e => Post(e, $"/_e2e/broker?oid={E2E.Alice}")),
    ];

    [Fact]
    public async Task Many_tools_through_rotating_faults_keep_the_ledger_the_bounds_and_the_tools_latency()
    {
        OscopeTrace.Covers("FI", "H-E4 H-E6 H-E9 H-E1 R-E5 R-E6 R-E7 LS-E3 LS-E5 CAST-38 CAST-39 CAST-50 CAST-21");
        using var e = new E2E();
        await e.Reset();
        var seconds = int.Parse(Environment.GetEnvironmentVariable("OSCOPE_STRESS_SECONDS") ?? "120", CultureInfo.InvariantCulture);
        var bound = long.Parse(Environment.GetEnvironmentVariable("OSCOPE_E2E_MAX_QUEUE_BYTES") ?? "1048576", CultureInfo.InvariantCulture);
        var pid = int.Parse(Environment.GetEnvironmentVariable("OSCOPE_E2E_FORWARDER_PID") ?? "0", CultureInfo.InvariantCulture);
        var proc = pid > 0 ? Process.GetProcessById(pid) : null;
        var s0 = await e.Status();
        var ok0 = await e.IngressCount("ok");

        // Bodies prepared once (a few sizes), so the tools' own work does not dominate.
        var bodies = new List<byte[]>();
        for (var i = 0; i < 12; i++) bodies.Add(await e.Sample(seed: 900_000 + i, spans: i * 40));
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(seconds));
        var latencies = new ConcurrentBag<double>();
        var answers = new ConcurrentDictionary<int, long>();
        long sentBytes = 0;
        var tools = Enumerable.Range(0, 6).Select(t => Task.Run(async () =>
        {
            var rnd = new Random(t);
            using var c = new HttpClient { BaseAddress = e.Forwarder.BaseAddress, Timeout = TimeSpan.FromSeconds(30) };
            c.DefaultRequestHeaders.Authorization = e.Forwarder.DefaultRequestHeaders.Authorization;
            while (!stop.IsCancellationRequested)
            {
                var b = bodies[rnd.Next(bodies.Count)];
                var content = new ByteArrayContent(b);
                content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/x-protobuf");
                var sw = Stopwatch.StartNew();
                using var r = await c.PostAsync("/v1/traces", content);
                latencies.Add(sw.Elapsed.TotalMilliseconds);
                answers.AddOrUpdate((int)r.StatusCode, 1, (_, n) => n + 1);
                if (r.StatusCode == HttpStatusCode.OK) Interlocked.Add(ref sentBytes, b.Length);
                else await Task.Delay(TimeSpan.FromMilliseconds(rnd.Next(50, 300))); // a client honouring the 503's hint, roughly
                await Task.Delay(rnd.Next(0, 20));
            }
        })).ToList();

        var faultsRun = new List<string>();
        var maxHeld = 0L;
        long rss0 = 0, rssMax = 0;
        var violations = new List<string>();
        var chaos = Task.Run(async () =>
        {
            var rnd = new Random(42);
            while (!stop.IsCancellationRequested)
            {
                var f = Faults[rnd.Next(Faults.Length)];
                faultsRun.Add(f.Name);
                await f.Start(e);
                try { await Task.Delay(TimeSpan.FromSeconds(rnd.Next(2, 6)), stop.Token); }
                catch (OperationCanceledException) { }
                await f.Stop(e);
                try { await Task.Delay(TimeSpan.FromSeconds(1), stop.Token); }
                catch (OperationCanceledException) { }
            }
        });
        var sampler = Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                var s = await e.Status();
                maxHeld = Math.Max(maxHeld, E2E.Get(s, "held_bytes"));
                if (!s.GetProperty("balances").GetBoolean()) violations.Add($"ledger: {s}");
                if (E2E.Get(s, "held_bytes") > bound) violations.Add($"held {E2E.Get(s, "held_bytes")} > {bound}");
                if (proc is not null)
                {
                    proc.Refresh();
                    if (rss0 == 0) rss0 = proc.WorkingSet64;
                    rssMax = Math.Max(rssMax, proc.WorkingSet64);
                }
                try { await Task.Delay(250, stop.Token); }
                catch (OperationCanceledException) { }
            }
        });
        await Task.WhenAll(tools);
        await chaos;
        await sampler;
        // Faults cleared, Alice signed in: everything drains.
        await Post(e, "/_e2e/fault?kind=clear");
        await Post(e, $"/_e2e/broker?oid={E2E.Alice}&lifetime_s=4500&lie_s=0&mode=ok");
        var s1 = await e.Drained(180);
        var ok1 = await e.IngressCount("ok");

        var accepted = E2E.Get(s1, "accepted") - E2E.Get(s0, "accepted");
        var committed = E2E.Get(s1, "committed") - E2E.Get(s0, "committed");
        var lat = latencies.OrderBy(x => x).ToArray();
        double P(double q) => lat.Length == 0 ? 0 : lat[Math.Min(lat.Length - 1, (int)(q * lat.Length))];
        // The memory bound: the queue and the bodies being read, plus a margin for the runtime's
        // own growth; it does not depend on how many bytes went through.
        var rssBound = rss0 + 4 * (bound + 4 * 262144) + (96L << 20);
        var summary = new Dictionary<string, object>
        {
            ["seconds"] = seconds, ["requests"] = lat.Length, ["answers"] = answers.ToDictionary(kv => kv.Key.ToString(CultureInfo.InvariantCulture), kv => kv.Value),
            ["accepted"] = accepted, ["committed"] = committed, ["ingress_ok"] = ok1 - ok0, ["sent_bytes"] = sentBytes,
            ["dropped"] = JsonSerializer.Deserialize<object>(s1.GetProperty("dropped").GetRawText())!,
            ["dropped_maybe_landed"] = JsonSerializer.Deserialize<object>(s1.GetProperty("dropped_maybe_landed").GetRawText())!,
            ["committed_after_unknown"] = E2E.Get(s1, "committed_after_unknown") - E2E.Get(s0, "committed_after_unknown"),
            ["max_held_bytes"] = maxHeld, ["queue_bound"] = bound, ["rss_start"] = rss0, ["rss_max"] = rssMax, ["rss_bound"] = rssBound,
            ["latency_ms_p50"] = P(0.5), ["latency_ms_p99"] = P(0.99), ["latency_ms_max"] = lat.Length == 0 ? 0 : lat[^1],
            ["faults"] = faultsRun.GroupBy(x => x).ToDictionary(g => g.Key, g => g.Count()),
            ["fault_ids"] = Faults.ToDictionary(f => f.Name, f => f.Ids),
            ["violations"] = violations,
        };
        var json = JsonSerializer.Serialize(summary, new JsonSerializerOptions { WriteIndented = true });
        if (Environment.GetEnvironmentVariable("OSCOPE_STRESS_OUT") is { Length: > 0 } outPath) await File.WriteAllTextAsync(outPath, json);
        Console.WriteLine(json);

        Assert.Empty(violations);
        Assert.True(s1.GetProperty("balances").GetBoolean(), $"ledger at the end: {s1}");
        Assert.True(committed > 0, "nothing committed");
        Assert.True(ok1 - ok0 >= committed, $"the ingress committed {ok1 - ok0}, fewer than the forwarder's {committed}");
        Assert.True(lat.Length > 0 && lat[^1] < 2000, $"a tool waited {lat[^1]} ms (H-E9)");
        Assert.True(maxHeld <= bound);
        if (proc is not null) Assert.True(rssMax <= rssBound, $"resident memory {rssMax} > {rssBound} after {sentBytes} bytes");
        Assert.True(sentBytes > 4 * bound, $"the run moved only {sentBytes} bytes: too little to say memory is bounded");
    }
}
