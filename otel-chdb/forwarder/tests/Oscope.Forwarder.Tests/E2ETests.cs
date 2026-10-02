using System.Diagnostics;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Oscope.Forwarder.Http;
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

    /// <summary>Request headers the forwarder may pass to the ingress (the harness's answers log lists names).</summary>
    public static readonly HashSet<string> AllowedAtIngress = new(StringComparer.OrdinalIgnoreCase)
    {
        "host", "content-type", "content-length", "transfer-encoding", "content-encoding", "authorization", "x-oscope-namespace",
    };

    public HttpClient Ingress { get; }
    public HttpClient Forwarder { get; }

    public E2E()
    {
        static string Need(string n) => Environment.GetEnvironmentVariable(n) is { Length: > 0 } v ? v
            : throw new InvalidOperationException($"{n} is not set: run through ci/forwarder.sh e2e");
        Ingress = new HttpClient { BaseAddress = new Uri(Need("OSCOPE_E2E_INGRESS")), Timeout = TimeSpan.FromSeconds(30) };
        Forwarder = new HttpClient { BaseAddress = new Uri(Need("OSCOPE_E2E_FORWARDER")), Timeout = TimeSpan.FromSeconds(30) };
        Forwarder.DefaultRequestHeaders.Authorization = LocalKey();
    }

    public static AuthenticationHeaderValue LocalKey()
    {
        var pk = Environment.GetEnvironmentVariable("OSCOPE_E2E_PK") ?? "pk-lf-local-e2e";
        var sk = Environment.GetEnvironmentVariable("OSCOPE_E2E_SK") ?? "sk-lf-local-e2e-0123456789";
        return new AuthenticationHeaderValue("Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes($"{pk}:{sk}")));
    }

    public void Dispose()
    {
        Ingress.Dispose();
        Forwarder.Dispose();
    }

    public Task Post(string url) => Ingress.PostAsync(url, null).ContinueWith(t => t.Result.EnsureSuccessStatusCode());

    public async Task Reset()
    {
        await Post("/_e2e/fault?kind=clear");
        await Post($"/_e2e/broker?oid={Alice}&role=Team.Payments&lifetime_s=4500&lie_s=0&mode=ok");
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

    /// <summary>One entry of the harness's answers log (ingress/cmd/ingress-e2e).</summary>
    public sealed record Answer(long Seq, int Status, string RetryAfter, string RespSha256, string ReqSha256, long ReqBytes, string[]? Headers, bool Lost);

    public async Task<List<Answer>> Answers(long since = 0)
    {
        var web = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower };
        return (await Ingress.GetFromJsonAsync<List<Answer>>($"/_e2e/answers?since={since}", web))!;
    }

    public async Task<long> LastSeq() => (await Answers()).Select(a => a.Seq).DefaultIfEmpty(0).Max();

    public static string Sha(byte[] b) => Convert.ToHexStringLower(SHA256.HashData(b));

    public static long Sub(JsonElement s, string obj, string key) =>
        s.GetProperty(obj).TryGetProperty(key, out var v) ? v.GetInt64() : 0;

    /// <summary>What the tool got, read whole: status, headers, body hash.</summary>
    public sealed record Got(int Status, string? Seq, string? BodySha, string? RetryAfter, string? Local, string RespSha, string Text);

    public static async Task<Got> Read(HttpResponseMessage r)
    {
        var body = await r.Content.ReadAsByteArrayAsync();
        static string? H(HttpResponseMessage r, string n) => r.Headers.TryGetValues(n, out var v) ? string.Join(",", v) : null;
        return new Got((int)r.StatusCode, H(r, "X-E2E-Seq"), H(r, "X-E2E-Body-SHA256"), H(r, "Retry-After"), H(r, LocalEndpoint.LocalHeader),
            Sha(body), Encoding.UTF8.GetString(body));
    }
}

[Trait("Category", "E2E")]
public class E2ETests
{
    [Fact]
    public async Task A_200_means_committed_under_the_person_of_the_token_never_the_tools_claims()
    {
        OscopeTrace.Covers("IT", "R-E1 R-E5 H-E1 H-E4 SEC-E2 LS-E1 UCA-E2 UCA-E4 R-E6");
        using var e = new E2E();
        await e.Reset();
        var before = (await e.Commits()).Select(c => c.Key).ToHashSet();
        var body = await e.Sample(seed: 101, spans: 3);
        var got = await E2E.Read(await e.Send(body));
        Assert.Equal(200, got.Status);
        Assert.Null(got.Local);
        Assert.Equal(E2E.Sha(body), got.BodySha); // the ingress read exactly the tool's bytes
        // No waiting: the 200 came from the ingress after the commit, so the commit is there now.
        var fresh = (await e.Commits()).Where(c => !before.Contains(c.Key) && c.Signal == "traces").ToList();
        var res = Assert.Single(fresh).Resources.First();
        Assert.Equal(E2E.Alice, res["user.id"]);
        Assert.Equal("dev-payments", res["k8s.namespace.name"]);
        Assert.Equal("devtools", res["k8s.cluster.name"]);
        Assert.Equal("mallory@example.com", res["oscope.ingress.claimed.user.id"]);
        Assert.Equal("prod-billing", res["oscope.ingress.claimed.k8s.namespace.name"]);
        var a = (await e.Answers()).Single(x => x.Seq.ToString(System.Globalization.CultureInfo.InvariantCulture) == got.Seq);
        Assert.All(a.Headers ?? [], h => Assert.Contains(h, E2E.AllowedAtIngress));
    }

    [Fact]
    public async Task The_ingress_answer_reaches_the_tool_unchanged_status_Retry_After_and_body()
    {
        OscopeTrace.Covers("IT", "R-E5 UCA-E4 H-E5 CAST-39");
        using var e = new E2E();
        await e.Reset();
        await e.Post("/_e2e/fault?kind=status&code=429&n=1&retry_after=7");
        var body = await e.Sample(seed: 151);
        var got = await E2E.Read(await e.Send(body));
        Assert.Equal(429, got.Status);
        Assert.Equal("7", got.RetryAfter);
        Assert.Equal("injected\n", got.Text);
        Assert.Null(got.Local);
        var a = (await e.Answers()).Single(x => x.Seq.ToString(System.Globalization.CultureInfo.InvariantCulture) == got.Seq);
        Assert.Equal((429, "7", got.RespSha, E2E.Sha(body)), (a.Status, a.RetryAfter, a.RespSha256, a.ReqSha256));
    }

    [Fact]
    public async Task A_lost_answer_after_the_commit_is_502_to_the_tool_and_the_tools_retry_is_a_copy()
    {
        OscopeTrace.Covers("IT", "CAST-50 H-E6 LS-E3 R-E4 UCA-E3");
        using var e = new E2E();
        await e.Reset();
        var before = (await e.Commits()).Select(c => c.Key).ToHashSet();
        var ok0 = await e.IngressCount("ok");
        await e.Post("/_e2e/fault?kind=lose_answer&n=1");
        var body = await e.Sample(seed: 202);
        var first = await E2E.Read(await e.Send(body));
        Assert.Equal(502, first.Status); // unknown, and retryable: the exporter's to retry
        Assert.Equal("ingress_error", first.Local);
        Assert.Equal(1, await e.IngressCount("ok") - ok0); // it did commit, and the forwarder sent nothing more
        var second = await E2E.Read(await e.Send(body)); // the tool's retry: the same bytes
        Assert.Equal(200, second.Status);
        Assert.Equal(2, await e.IngressCount("ok") - ok0);
        var fresh = (await e.Commits()).Where(c => !before.Contains(c.Key) && c.Signal == "traces").ToList();
        Assert.NotEmpty(fresh); // (the harness's store may keep one object per key; the ingress counted two commits)
        Assert.Single(fresh.Select(c => c.ContentKey).Distinct()); // one content key: a copy the consumer skips (D11)
    }

    [Fact]
    public async Task An_unresolved_commit_is_the_ingress_503_with_its_Retry_After()
    {
        OscopeTrace.Covers("IT", "R-E5 H-E4 CAST-15 CAST-39");
        using var e = new E2E();
        await e.Reset();
        await e.Post("/_e2e/fault?kind=store_drop&n=64");
        var body = await e.Sample(seed: 303);
        var got = await E2E.Read(await e.Send(body));
        Assert.Equal(503, got.Status);
        Assert.Null(got.Local); // the ingress's own 503, not the forwarder's
        Assert.NotNull(got.RetryAfter);
        Assert.NotNull(got.Seq);
        await e.Post("/_e2e/fault?kind=clear");
        Assert.Equal(200, (await E2E.Read(await e.Send(body))).Status);
    }

    [Fact]
    public async Task A_token_that_expires_in_flight_is_a_401_once_and_the_next_request_gets_a_fresh_one()
    {
        OscopeTrace.Covers("IT", "R-E6 H-E9 UCA-E8 TM-E2");
        using var e = new E2E();
        await e.Reset();
        // The broker hands out a token that lives 1 s but claims to live an hour.
        await e.Post("/_e2e/broker?lifetime_s=1&lie_s=3600");
        Assert.Equal(200, (await E2E.Read(await e.Send(await e.Sample(seed: 404)))).Status);
        await Task.Delay(TimeSpan.FromSeconds(2.5)); // past its expiry and the ingress's 1 s leeway
        var s0 = await e.Status();
        var unauth0 = await e.IngressCount("unauthenticated");
        var r1 = await E2E.Read(await e.Send(await e.Sample(seed: 405)));
        Assert.Equal(401, r1.Status); // the ingress's 401, passed through; not replayed
        Assert.Null(r1.Local);
        Assert.Equal(1, await e.IngressCount("unauthenticated") - unauth0);
        var r2 = await E2E.Read(await e.Send(await e.Sample(seed: 406)));
        Assert.Equal(200, r2.Status);
        Assert.Equal(1, await e.IngressCount("unauthenticated") - unauth0);
        var s1 = await e.Status();
        Assert.Equal(1, s1.GetProperty("forced_token_refreshes").GetInt64() - s0.GetProperty("forced_token_refreshes").GetInt64());
    }

    [Fact]
    public async Task After_another_person_signs_in_the_next_request_goes_under_them()
    {
        OscopeTrace.Covers("IT", "H-E1 R-E6 LS-E5 TM-E1");
        using var e = new E2E();
        await e.Reset();
        var before = (await e.Commits()).Select(c => c.Key).ToHashSet();
        Assert.Equal(200, (await E2E.Read(await e.Send(await e.Sample(seed: 501)))).Status);
        var s0 = await e.Status();
        await e.Post($"/_e2e/broker?oid={E2E.Bob}");
        Assert.Equal(200, (await E2E.Read(await e.Send(await e.Sample(seed: 502)))).Status);
        var s1 = await e.Status();
        Assert.Equal(E2E.Bob, s1.GetProperty("account").GetString());
        Assert.Equal(1, s1.GetProperty("account_changes").GetInt64() - s0.GetProperty("account_changes").GetInt64());
        var users = (await e.Commits()).Where(c => !before.Contains(c.Key) && c.Signal == "traces")
            .Select(c => c.Resources.First()["user.id"]).ToList();
        Assert.Equal(new[] { E2E.Alice, E2E.Bob }, users.Order(StringComparer.Ordinal).ToArray());
    }

    [Fact]
    public async Task With_nobody_signed_in_the_tool_gets_503_and_the_ingress_nothing()
    {
        OscopeTrace.Covers("IT", "H-E9 UCA-E8 TM-E2 TM-E1");
        using var e = new E2E();
        await e.Reset();
        await e.Post("/_e2e/broker?mode=interaction_required");
        var seq0 = await e.LastSeq();
        var sw = Stopwatch.StartNew();
        var got = await E2E.Read(await e.Send(await e.Sample(seed: 601)));
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(5), $"the tool waited {sw.Elapsed}");
        Assert.Equal(503, got.Status);
        Assert.Equal("no_token", got.Local);
        Assert.NotNull(got.RetryAfter);
        Assert.Contains("sign in", got.Text, StringComparison.Ordinal);
        Assert.Equal(seq0, await e.LastSeq()); // nothing reached the ingress
        await e.Post("/_e2e/broker?mode=ok");
    }

    [Fact]
    public async Task A_slow_ingress_is_seen_by_the_tool_as_latency_and_the_body_still_arrives_whole()
    {
        OscopeTrace.Covers("IT", "R-E7 H-E9 CAST-38");
        using var e = new E2E();
        await e.Reset();
        await e.Post("/_e2e/fault?kind=delay&n=1&delay_ms=700");
        var body = await e.Sample(seed: 606, spans: 600);
        var sw = Stopwatch.StartNew();
        var got = await E2E.Read(await e.Send(body));
        Assert.True(sw.Elapsed >= TimeSpan.FromMilliseconds(650), $"the tool was answered in {sw.Elapsed}, before the ingress");
        Assert.Equal(200, got.Status);
        Assert.Equal(E2E.Sha(body), got.BodySha);
    }
}
