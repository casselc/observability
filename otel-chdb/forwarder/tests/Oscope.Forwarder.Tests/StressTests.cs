using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using Xunit;

namespace Oscope.Forwarder.Tests;

/// <summary>
/// The nightly stress run (ci/forwarder.sh stress; nightly job forwarder-stress), for a
/// bounded time (OSCOPE_STRESS_SECONDS). Many tools send at once, several requests each,
/// through one forwarder process (the pass-through, D40 as amended 2026-10-02) to the Go
/// ingress, while a chaos loop rotates the faults of the STPA analysis
/// (research/entra-ingress.md §10b):
///
///   lost answers after commit       CAST 50, H-E6, LS-E3   502 to the tool, never replayed
///   unresolved commits (503)        R-E5, CAST 15          the ingress's 503 + Retry-After, unchanged
///   429 / 503 before handling       H-E5, CAST 39          passed through with Retry-After
///   a slow ingress                  R-E7, CAST 38, H-E9    memory bounded; the tool sees the latency
///   tokens expiring in flight       R-E6, UCA-E8           401 once, the next request refreshed
///   the broker unavailable          H-E9, TM-E2            503 no_token at once, never a prompt
///   another person signs in         H-E1, LS-E5            the next requests go under them
///   more requests than the bound    CAST 21, CAST 38       503 busy at once
///
/// Every answer the ingress gave is matched, by its sequence number, with what a tool got:
/// the same status, Retry-After and body, the tool's own request body hash at the ingress,
/// no header of the tool's at the ingress, and each answer once (nothing replayed). Every
/// answer without a sequence number is marked as the forwarder's own. The forwarder's
/// resident memory stays under a bound that does not grow with the bytes moved.
/// The summary goes to OSCOPE_STRESS_OUT (JSON) for the job's step summary.
/// </summary>
[Trait("Category", "Stress")]
public class StressTests
{
    private sealed record Fault(string Name, string Ids, Func<E2E, Task> Start, Func<E2E, Task> Stop);

    private static readonly Fault[] Faults =
    [
        new("lost answers", "CAST-50 H-E6 LS-E3", e => e.Post("/_e2e/fault?kind=lose_answer&n=20"), e => e.Post("/_e2e/fault?kind=clear")),
        new("unresolved commits", "R-E5 CAST-15 CAST-39", e => e.Post("/_e2e/fault?kind=store_drop&n=40"), e => e.Post("/_e2e/fault?kind=clear")),
        new("rate limited", "H-E5 CAST-39", e => e.Post("/_e2e/fault?kind=status&code=429&n=30&retry_after=1"), e => e.Post("/_e2e/fault?kind=clear")),
        new("503 before handling", "R-E5 CAST-39", e => e.Post("/_e2e/fault?kind=status&code=503&n=30&retry_after=2"), e => e.Post("/_e2e/fault?kind=clear")),
        new("slow ingress", "R-E7 H-E9 CAST-38", e => e.Post("/_e2e/fault?kind=delay&n=200&delay_ms=400"), e => e.Post("/_e2e/fault?kind=clear")),
        new("token expiring in flight", "R-E6 UCA-E8", e => e.Post("/_e2e/broker?lifetime_s=2&lie_s=3600"), e => e.Post("/_e2e/broker?lifetime_s=4500&lie_s=0")),
        new("broker unavailable", "H-E9 TM-E2", e => e.Post("/_e2e/broker?mode=unavailable"), e => e.Post("/_e2e/broker?mode=ok")),
        new("another person signs in", "H-E1 LS-E5", e => e.Post($"/_e2e/broker?oid={E2E.Bob}"), e => e.Post($"/_e2e/broker?oid={E2E.Alice}")),
    ];

    private sealed record Sent(string ReqSha, E2E.Got Got, double Ms);

    [Fact]
    public async Task Many_tools_through_rotating_faults_get_the_ingress_answers_unchanged_in_bounded_memory()
    {
        OscopeTrace.Covers("FI", "R-E5 R-E6 R-E7 H-E1 H-E5 H-E6 H-E9 LS-E3 LS-E5 UCA-E4 UCA-E8 CAST-21 CAST-38 CAST-39 CAST-50");
        using var e = new E2E();
        await e.Reset();
        var seconds = int.Parse(Environment.GetEnvironmentVariable("OSCOPE_STRESS_SECONDS") ?? "120", CultureInfo.InvariantCulture);
        var pid = int.Parse(Environment.GetEnvironmentVariable("OSCOPE_E2E_FORWARDER_PID") ?? "0", CultureInfo.InvariantCulture);
        var proc = pid > 0 ? Process.GetProcessById(pid) : null;
        var maxConcurrent = (await e.Status()).GetProperty("max_concurrent_requests").GetInt32();
        var seq0 = await e.LastSeq();
        var ok0 = await e.IngressCount("ok");

        // Bodies prepared once: OTLP samples of a few sizes, and some large undecodable ones
        // (the ingress reads them whole, then answers 400) so the run moves many bytes.
        var bodies = new List<byte[]>();
        for (var i = 0; i < 12; i++) bodies.Add(await e.Sample(seed: 900_000 + i, spans: i * 40));
        foreach (var mib in new[] { 1, 2, 4 }) bodies.Add(Rig.Body(mib << 20, mib));
        var shas = bodies.Select(E2E.Sha).ToArray();

        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(seconds));
        var sent = new ConcurrentBag<Sent>();
        var violations = new ConcurrentQueue<string>();
        long resets = 0;
        const int tools = 8, perTool = 3; // 24 senders against the forwarder's bound
        var senders = Enumerable.Range(0, tools * perTool).Select(t => Task.Run(async () =>
        {
            var rnd = new Random(t);
            using var c = new HttpClient { BaseAddress = e.Forwarder.BaseAddress, Timeout = TimeSpan.FromSeconds(60) };
            c.DefaultRequestHeaders.Authorization = E2E.LocalKey();
            c.DefaultRequestHeaders.TryAddWithoutValidation("x-langfuse-sdk-name", "stress"); // must never reach the ingress
            while (!stop.IsCancellationRequested)
            {
                var k = rnd.Next(bodies.Count);
                var content = new ByteArrayContent(bodies[k]);
                content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/x-protobuf");
                var sw = Stopwatch.StartNew();
                try
                {
                    using var r = await c.PostAsync(rnd.Next(2) == 0 ? "/v1/traces" : "/api/public/otel/v1/traces", content);
                    var got = await E2E.Read(r);
                    sent.Add(new Sent(shas[k], got, sw.Elapsed.TotalMilliseconds));
                    if (got.Seq is null && got.Local is null) violations.Enqueue($"answer {got.Status} neither the ingress's nor marked as the forwarder's");
                    if (got.BodySha is { } bs && bs != shas[k]) violations.Enqueue($"seq {got.Seq}: the ingress read other bytes than the tool sent");
                    if (got.Status >= 400) await Task.Delay(TimeSpan.FromMilliseconds(rnd.Next(50, 300))); // a client backing off, roughly
                }
                catch (HttpRequestException)
                {
                    Interlocked.Increment(ref resets); // an ingress answer cut off mid-body: the connection ends
                }
                await Task.Delay(rnd.Next(0, 20));
            }
        })).ToList();

        var faultsRun = new ConcurrentQueue<string>();
        long rss0 = 0, rssMax = 0;
        var maxInFlight = 0;
        var chaos = Task.Run(async () =>
        {
            var rnd = new Random(42);
            while (!stop.IsCancellationRequested)
            {
                var f = Faults[rnd.Next(Faults.Length)];
                faultsRun.Enqueue(f.Name);
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
                var inFlight = s.GetProperty("in_flight").GetInt32();
                maxInFlight = Math.Max(maxInFlight, inFlight);
                if (inFlight > maxConcurrent) violations.Enqueue($"in flight {inFlight} > {maxConcurrent}");
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
        await Task.WhenAll(senders);
        await chaos;
        await sampler;
        await e.Reset();

        // Match every answer the ingress gave with what a tool got.
        var answers = await e.Answers(seq0);
        var bySeq = sent.Where(s => s.Got.Seq is not null).GroupBy(s => s.Got.Seq!).ToDictionary(g => g.Key, g => g.ToList());
        long moved = 0, lost = 0, matched = 0;
        foreach (var a in answers)
        {
            moved += a.ReqBytes;
            foreach (var h in a.Headers ?? [])
                if (!E2E.AllowedAtIngress.Contains(h)) violations.Enqueue($"seq {a.Seq}: header {h} reached the ingress");
            if (a.Lost)
            {
                lost++;
                if (bySeq.ContainsKey(a.Seq.ToString(CultureInfo.InvariantCulture))) violations.Enqueue($"seq {a.Seq}: a lost answer reached a tool");
                continue;
            }
            if (!bySeq.TryGetValue(a.Seq.ToString(CultureInfo.InvariantCulture), out var tool))
            {
                // A sender cut off mid-answer (a reset) never read the sequence number.
                continue;
            }
            if (tool.Count != 1) violations.Enqueue($"seq {a.Seq}: {tool.Count} tool answers for one ingress answer");
            var t = tool[0];
            matched++;
            if (t.Got.Status != a.Status || (t.Got.RetryAfter ?? "") != a.RetryAfter || t.Got.RespSha != a.RespSha256)
                violations.Enqueue($"seq {a.Seq}: the ingress answered {a.Status} ra={a.RetryAfter} {a.RespSha256[..8]}, the tool got {t.Got.Status} ra={t.Got.RetryAfter} {t.Got.RespSha[..8]}");
            if (a.ReqSha256.Length > 0 && a.ReqSha256 != t.ReqSha) violations.Enqueue($"seq {a.Seq}: the ingress read other bytes than the tool sent");
        }
        var local = sent.Where(s => s.Got.Local is not null).GroupBy(s => s.Got.Local!).ToDictionary(g => g.Key, g => (long)g.Count());
        // Nothing replayed, nothing invented: every request that reached the ingress is one
        // ingress answer one tool got, one lost answer (a 502 to the tool), or one reset (an
        // answer cut off on its way back); every answer a tool got from the ingress is in its log.
        var unread = answers.Count(a => !a.Lost && !bySeq.ContainsKey(a.Seq.ToString(CultureInfo.InvariantCulture)));
        if (unread != resets) violations.Enqueue($"{unread} ingress answers reached no tool, but {resets} tool requests were reset");
        var given = answers.Select(a => a.Seq.ToString(CultureInfo.InvariantCulture)).ToHashSet();
        foreach (var s in bySeq.Keys.Where(k => !given.Contains(k))) violations.Enqueue($"a tool got seq {s}, which the ingress never gave");
        if (local.GetValueOrDefault("ingress_error") != lost)
            violations.Enqueue($"{lost} answers were lost at the ingress, but the tools got {local.GetValueOrDefault("ingress_error")} local 502s");
        var ok1 = await e.IngressCount("ok");
        var tool200 = sent.Count(s => s.Got.Status == 200);
        // A 200 means committed: every 200 a tool got is an ingress commit (a lost answer commits too).
        if (tool200 > ok1 - ok0 || ok1 - ok0 > tool200 + lost)
            violations.Enqueue($"the tools got {tool200} 200s; the ingress committed {ok1 - ok0} with {lost} answers lost");
        if (sent.Any(s => s.Got.Local is null && s.Got.Seq is null)) violations.Enqueue("an unmarked answer");

        var lat = sent.Select(s => s.Ms).Order().ToArray();
        double P(double q) => lat.Length == 0 ? 0 : lat[Math.Min(lat.Length - 1, (int)(q * lat.Length))];
        // The memory bound: what the forwarder holds per request in flight is a few buffers of
        // 64 KiB, so its resident memory is the runtime's own plus a margin; it must not depend
        // on how many bytes went through, which is many times the margin.
        const long margin = 128L << 20;
        var rssBound = rss0 + margin;
        var summary = new Dictionary<string, object>
        {
            ["seconds"] = seconds, ["senders"] = tools * perTool, ["max_concurrent_requests"] = maxConcurrent, ["max_in_flight"] = maxInFlight,
            ["requests"] = sent.Count, ["resets"] = resets,
            ["answers"] = sent.GroupBy(s => s.Got.Status).ToDictionary(g => g.Key.ToString(CultureInfo.InvariantCulture), g => g.Count()),
            ["local_answers"] = local, ["ingress_answers"] = answers.Count, ["matched"] = matched, ["lost"] = lost,
            ["ingress_ok"] = ok1 - ok0, ["tool_200"] = tool200, ["moved_bytes"] = moved,
            ["rss_start"] = rss0, ["rss_max"] = rssMax, ["rss_bound"] = rssBound,
            ["latency_ms_p50"] = P(0.5), ["latency_ms_p99"] = P(0.99), ["latency_ms_max"] = lat.Length == 0 ? 0 : lat[^1],
            ["faults"] = faultsRun.GroupBy(x => x).ToDictionary(g => g.Key, g => g.Count()),
            ["fault_ids"] = Faults.ToDictionary(f => f.Name, f => f.Ids),
            ["violations"] = violations.Take(50).ToList(), ["violation_count"] = violations.Count,
        };
        var json = JsonSerializer.Serialize(summary, new JsonSerializerOptions { WriteIndented = true });
        if (Environment.GetEnvironmentVariable("OSCOPE_STRESS_OUT") is { Length: > 0 } outPath) await File.WriteAllTextAsync(outPath, json);
        Console.WriteLine(json);

        Assert.Empty(violations);
        Assert.True(tool200 > 0, "nothing committed");
        Assert.True(local.GetValueOrDefault("busy") > 0, "the concurrency bound was never reached: the run did not test it");
        Assert.True(maxInFlight <= maxConcurrent);
        if (proc is not null) Assert.True(rssMax <= rssBound, $"resident memory {rssMax} > {rssBound} after {moved} bytes");
        var need = Math.Min(4 * margin, seconds * (4L << 20));
        Assert.True(moved >= need, $"the run moved only {moved} bytes (want {need}): too little to say memory is bounded");
    }
}
