using System.Collections.Concurrent;
using System.Net.Http.Headers;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Oscope.Forwarder.Auth;
using Oscope.Forwarder.Core;
using Oscope.Forwarder.Http;

namespace Oscope.Forwarder.Tests;

/// <summary>A request as the fake ingress saw it.</summary>
public sealed record Seen(string Path, Dictionary<string, string> Headers, byte[] Body);

/// <summary>
/// An in-process stand-in for the ingress (TestServer), answering each request with the
/// next scripted behaviour (200 when the script is empty) and recording what arrived.
/// </summary>
public sealed class FakeIngress : IAsyncDisposable
{
    private readonly WebApplication _app;
    public ConcurrentQueue<Func<HttpContext, Seen, Task>> Script { get; } = new();
    public ConcurrentQueue<Seen> Requests { get; } = new();
    /// <summary>When set, every request waits for it (a slow or stalled ingress).</summary>
    public TaskCompletionSource? Gate { get; set; }
    /// <summary>When set, decides the answer for every request the script does not.</summary>
    public Func<HttpContext, Seen, Task>? Default { get; set; }

    public HttpMessageInvoker Invoker { get; }

    private FakeIngress(WebApplication app)
    {
        _app = app;
        Invoker = new HttpMessageInvoker(app.GetTestServer().CreateHandler());
    }

    public static async Task<FakeIngress> StartAsync()
    {
        var b = WebApplication.CreateSlimBuilder();
        b.WebHost.UseTestServer();
        var app = b.Build();
        FakeIngress? self = null;
        app.Run(async ctx =>
        {
            using var ms = new MemoryStream();
            await ctx.Request.Body.CopyToAsync(ms);
            var seen = new Seen(ctx.Request.Path.Value ?? "",
                ctx.Request.Headers.ToDictionary(h => h.Key.ToLowerInvariant(), h => h.Value.ToString()), ms.ToArray());
            self!.Requests.Enqueue(seen);
            if (self.Gate is { } g) await g.Task;
            if (self.Script.TryDequeue(out var f)) await f(ctx, seen);
            else if (self.Default is { } d) await d(ctx, seen);
            else ctx.Response.StatusCode = 200;
        });
        self = new FakeIngress(app);
        await app.StartAsync();
        return self;
    }

    public static Func<HttpContext, Seen, Task> Status(int code, int retryAfterS = 0) => (ctx, _) =>
    {
        ctx.Response.StatusCode = code;
        if (retryAfterS > 0) ctx.Response.Headers.RetryAfter = retryAfterS.ToString(System.Globalization.CultureInfo.InvariantCulture);
        return Task.CompletedTask;
    };

    /// <summary>The request arrived (and, at a real ingress, may have committed); the answer is lost.</summary>
    public static Func<HttpContext, Seen, Task> LoseAnswer => (ctx, _) =>
    {
        ctx.Abort();
        return Task.CompletedTask;
    };

    public async ValueTask DisposeAsync()
    {
        Gate?.TrySetResult();
        Invoker.Dispose();
        await _app.DisposeAsync();
    }
}

/// <summary>A broker stand-in: tokens for whichever account is "signed in".</summary>
public sealed class FakeTokens : ITokenAcquirer
{
    private readonly Lock _gate = new();
    private int _version = 1;

    public AccountKey? Account { get; set; } = CoreModelTests.Alice;

    public TokenStatus Status { get; set; } = TokenStatus.Ok;

    public int Calls { get; private set; }

    public int Forced { get; private set; }

    public AccountKey? CurrentAccount => Account;

    public string Token
    {
        get
        {
            lock (_gate) return $"tok-{Account?.ObjectId}-{_version}";
        }
    }

    public ValueTask<TokenResult> AcquireSilentAsync(AccountKey? expected, bool forceRefresh, CancellationToken ct)
    {
        lock (_gate)
        {
            Calls++;
            if (forceRefresh)
            {
                Forced++;
                _version++;
            }
            var acct = Account;
            if (Status != TokenStatus.Ok || acct is null)
                return ValueTask.FromResult(TokenResult.Failed(acct is null ? TokenStatus.InteractionRequired : Status, "fake"));
            return ValueTask.FromResult(new TokenResult(TokenStatus.Ok, $"tok-{acct.Value.ObjectId}-{_version}", acct,
                DateTimeOffset.UtcNow.AddHours(1)));
        }
    }
}

/// <summary>A forwarder in process: the local endpoint on a TestServer, the fake ingress behind YARP.</summary>
public sealed class Rig : IAsyncDisposable
{
    public const string PublicKey = "pk-lf-local-test";
    public const string SecretKey = "sk-lf-local-0123456789abcdef";

    public required ForwarderInstance Fwd { get; init; }
    public required FakeIngress Ingress { get; init; }
    public required FakeTokens Tokens { get; init; }
    public required HttpClient Tool { get; init; }

    public static readonly ForwarderOptions FastQueue = new ForwarderOptions
    {
        MaxQueueBytes = 64 * 1024, MaxEntries = 64, MaxEntryBytes = 16 * 1024, MaxAttempts = 8,
        MaxAge = TimeSpan.FromSeconds(8), BackoffBase = TimeSpan.FromMilliseconds(20), BackoffCap = TimeSpan.FromMilliseconds(200),
        MaxRetryAfter = TimeSpan.FromSeconds(1), MaxInFlight = 2, RefusalRetryAfter = TimeSpan.FromSeconds(1),
    };

    public static readonly PumpOptions FastPump = new()
    {
        AttemptTimeout = TimeSpan.FromSeconds(3), TokenTimeout = TimeSpan.FromSeconds(1), DrainTimeout = TimeSpan.FromSeconds(1),
        ReportInterval = TimeSpan.Zero, MaxIdle = TimeSpan.FromMilliseconds(50),
    };

    public static async Task<Rig> StartAsync(ForwarderOptions? queue = null, string? ns = "dev-payments")
    {
        var ingress = await FakeIngress.StartAsync();
        var tokens = new FakeTokens();
        var settings = new ForwarderSettings
        {
            Ingress = new Uri("http://127.0.0.1:9/"), // loopback http, as tests may; YARP sends through the TestServer handler
            Namespace = ns,
            Local = new LocalOptions { PublicKey = PublicKey, SecretKey = SecretKey, MaxConcurrentIntake = 4 },
            Queue = queue ?? FastQueue,
            Pump = FastPump,
        };
        var fwd = ForwarderHost.Build(settings, tokens, configureWebHost: w => w.UseTestServer(), ingressClient: ingress.Invoker);
        await fwd.App.StartAsync();
        var tool = fwd.App.GetTestServer().CreateClient();
        tool.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic",
            Convert.ToBase64String(Encoding.UTF8.GetBytes($"{PublicKey}:{SecretKey}")));
        return new Rig { Fwd = fwd, Ingress = ingress, Tokens = tokens, Tool = tool };
    }

    public static HttpContent Proto(byte[] body)
    {
        var c = new ByteArrayContent(body);
        c.Headers.ContentType = new MediaTypeHeaderValue("application/x-protobuf");
        return c;
    }

    public Task<HttpResponseMessage> Send(byte[] body, string path = "/api/public/otel/v1/traces") => Tool.PostAsync(path, Proto(body));

    public CountersSnapshot Counters => Fwd.Pump.Snapshot();

    /// <summary>Waits (real time, bounded) until the forwarder's counters satisfy <paramref name="done"/>.</summary>
    public async Task<CountersSnapshot> Until(Func<CountersSnapshot, bool> done, int seconds = 15)
    {
        var deadline = DateTime.UtcNow.AddSeconds(seconds);
        while (true)
        {
            var s = Counters;
            if (done(s)) return s;
            if (DateTime.UtcNow > deadline) throw new TimeoutException($"forwarder counters never reached the state: {s}");
            await Task.Delay(20);
        }
    }

    public async ValueTask DisposeAsync()
    {
        Tool.Dispose();
        Ingress.Gate?.TrySetResult();
        await Fwd.App.StopAsync();
        await Fwd.App.DisposeAsync();
        await Ingress.DisposeAsync();
    }
}
