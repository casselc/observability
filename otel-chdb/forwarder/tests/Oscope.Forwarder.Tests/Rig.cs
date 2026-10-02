using System.Collections.Concurrent;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.Kestrel.Transport.Sockets;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Oscope.Forwarder.Auth;
using Oscope.Forwarder.Http;

namespace Oscope.Forwarder.Tests;

/// <summary>A request as the fake ingress saw it.</summary>
public sealed record Seen(string Path, Dictionary<string, string> Headers, byte[] Body);

/// <summary>
/// An in-process stand-in for the ingress on real Kestrel on loopback (real sockets, so
/// streaming and TCP backpressure are the real ones). By default it reads the whole body,
/// records it, and answers with <see cref="Answer"/> (200 when unset); <see cref="Handler"/>
/// replaces all of that for the streaming tests.
/// </summary>
public sealed class FakeIngress : IAsyncDisposable
{
    private readonly WebApplication _app;
    public ConcurrentQueue<Seen> Requests { get; } = new();
    /// <summary>Answers the next requests in order, before <see cref="Answer"/>.</summary>
    public ConcurrentQueue<Func<HttpContext, Seen, Task>> Script { get; } = new();
    /// <summary>Decides the answer for every request the script does not.</summary>
    public Func<HttpContext, Seen, Task>? Answer { get; set; }
    /// <summary>When set, the whole request is this (it reads the body itself, or not).</summary>
    public Func<HttpContext, Task>? Handler { get; set; }
    public Uri Address { get; }

    private FakeIngress(WebApplication app, Uri address)
    {
        _app = app;
        Address = address;
    }

    /// <param name="socketBuffer">when set, the listening socket's receive buffer (accepted sockets inherit it)</param>
    public static async Task<FakeIngress> StartAsync(int? socketBuffer = null)
    {
        var b = WebApplication.CreateSlimBuilder();
        b.Logging.ClearProviders();
        if (socketBuffer is { } sb) b.WebHost.UseSockets(o => o.CreateBoundListenSocket = ep => SmallListen(ep, sb));
        b.WebHost.ConfigureKestrel(k =>
        {
            k.Listen(IPAddress.Loopback, 0);
            k.Limits.MaxRequestBodySize = null;
            if (socketBuffer is { } sbk) k.Limits.MaxRequestBufferSize = sbk; // what the ingress side reads ahead, too
        });
        var app = b.Build();
        FakeIngress? self = null;
        app.Run(async ctx =>
        {
            if (self!.Handler is { } h)
            {
                await h(ctx);
                return;
            }
            using var ms = new MemoryStream();
            await ctx.Request.Body.CopyToAsync(ms);
            var seen = new Seen(ctx.Request.Path.Value ?? "",
                ctx.Request.Headers.ToDictionary(x => x.Key.ToLowerInvariant(), x => x.Value.ToString()), ms.ToArray());
            self.Requests.Enqueue(seen);
            if (self.Script.TryDequeue(out var f)) await f(ctx, seen);
            else if (self.Answer is { } a) await a(ctx, seen);
            else ctx.Response.StatusCode = 200;
        });
        await app.StartAsync();
        var addr = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
        self = new FakeIngress(app, new Uri(addr));
        return self;
    }

    internal static Socket SmallListen(EndPoint ep, int size)
    {
        var s = SocketTransportOptions.CreateDefaultBoundListenSocket(ep);
        s.ReceiveBufferSize = size;
        s.SendBufferSize = size;
        return s;
    }

    /// <summary>A client connection with small socket buffers (explicit sizes turn off autotuning).</summary>
    internal static async ValueTask<Stream> SmallConnect(SocketsHttpConnectionContext ctx, int size, CancellationToken ct)
    {
        var s = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true, SendBufferSize = size, ReceiveBufferSize = size };
        try
        {
            await s.ConnectAsync(ctx.DnsEndPoint, ct);
            return new NetworkStream(s, ownsSocket: true);
        }
        catch
        {
            s.Dispose();
            throw;
        }
    }

    public static Func<HttpContext, Seen, Task> Status(int code, int retryAfterS = 0, string? body = null) => async (ctx, _) =>
    {
        ctx.Response.StatusCode = code;
        if (retryAfterS > 0) ctx.Response.Headers.RetryAfter = retryAfterS.ToString(System.Globalization.CultureInfo.InvariantCulture);
        if (body is not null) await ctx.Response.WriteAsync(body);
    };

    /// <summary>The request arrived (and, at a real ingress, may have committed); the answer is lost.</summary>
    public static Func<HttpContext, Seen, Task> LoseAnswer => (ctx, _) =>
    {
        ctx.Abort();
        return Task.CompletedTask;
    };

    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync();
        await _app.DisposeAsync();
    }
}

/// <summary>A broker stand-in: tokens for whichever account is "signed in".</summary>
public sealed class FakeTokens : ITokenAcquirer
{
    public static readonly AccountKey Alice = new("11111111-1111-4111-8111-111111111111", "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa");
    public static readonly AccountKey Bob = new("11111111-1111-4111-8111-111111111111", "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb");

    private readonly Lock _gate = new();
    private int _version = 1;

    public AccountKey? Account { get; set; } = Alice;

    public TokenStatus Status { get; set; } = TokenStatus.Ok;

    /// <summary>When set, every acquisition waits for it (a hung broker).</summary>
    public TaskCompletionSource? Hang { get; set; }

    public int Calls { get; private set; }

    public int Forced { get; private set; }

    /// <summary>The token the last successful acquisition gave out.</summary>
    public string? LastIssued { get; private set; }

    public AccountKey? CurrentAccount => Account;

    public async ValueTask<TokenResult> AcquireSilentAsync(bool forceRefresh, CancellationToken ct)
    {
        if (Hang is { } h) await h.Task.WaitAsync(ct);
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
                return TokenResult.Failed(acct is null ? TokenStatus.InteractionRequired : Status, "fake");
            LastIssued = $"tok-{acct.Value.ObjectId}-{_version}";
            return new TokenResult(TokenStatus.Ok, LastIssued, acct, DateTimeOffset.UtcNow.AddHours(1));
        }
    }
}

/// <summary>A forwarder in process on real Kestrel (loopback, any port), the fake ingress behind it.</summary>
public sealed class Rig : IAsyncDisposable
{
    public const string PublicKey = "pk-lf-local-test";
    public const string SecretKey = "sk-lf-local-0123456789abcdef";

    public required ForwarderInstance Fwd { get; init; }
    public required FakeIngress Ingress { get; init; }
    public required FakeTokens Tokens { get; init; }
    public required HttpClient Tool { get; init; }

    public static readonly ProxyOptions Fast = new()
    {
        MaxRequestBytes = 1 << 20, MaxConcurrentRequests = 4, TokenTimeout = TimeSpan.FromSeconds(1),
        ActivityTimeout = TimeSpan.FromSeconds(10), RefusalRetryAfter = TimeSpan.FromSeconds(1), ForcedRefreshMinInterval = TimeSpan.Zero,
    };

    /// <param name="socketBuffer">when set, every socket on the path (tool, forwarder both sides, ingress) gets
    /// buffers of this size, so what the path can hold is known and the rest is what the forwarder holds</param>
    public static async Task<Rig> StartAsync(ProxyOptions? proxy = null, string? ns = "dev-payments", Uri? ingressOverride = null,
        int? socketBuffer = null)
    {
        var ingress = await FakeIngress.StartAsync(socketBuffer);
        var tokens = new FakeTokens();
        var settings = new ForwarderSettings
        {
            Ingress = ingressOverride ?? ingress.Address, // loopback http, as tests may
            Namespace = ns,
            Local = new LocalOptions { Port = 0, PublicKey = PublicKey, SecretKey = SecretKey },
            Proxy = proxy ?? Fast,
        };
        var fwd = socketBuffer is { } sb
            ? ForwarderHost.Build(settings, tokens,
                configureWebHost: w => w.UseSockets(o => o.CreateBoundListenSocket = ep => FakeIngress.SmallListen(ep, sb)),
                ingressClient: IngressProxy.CreateClient(h => h.ConnectCallback = (ctx, ct) => FakeIngress.SmallConnect(ctx, sb, ct)))
            : ForwarderHost.Build(settings, tokens);
        await fwd.App.StartAsync();
        var handler = new SocketsHttpHandler();
        if (socketBuffer is { } tb) handler.ConnectCallback = (ctx, ct) => FakeIngress.SmallConnect(ctx, tb, ct);
        var tool = new HttpClient(handler) { BaseAddress = fwd.Address, Timeout = TimeSpan.FromSeconds(60) };
        tool.DefaultRequestHeaders.Authorization = LocalKey();
        return new Rig { Fwd = fwd, Ingress = ingress, Tokens = tokens, Tool = tool };
    }

    public static AuthenticationHeaderValue LocalKey() =>
        new("Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes($"{PublicKey}:{SecretKey}")));

    public static HttpContent Proto(byte[] body)
    {
        var c = new ByteArrayContent(body);
        c.Headers.ContentType = new MediaTypeHeaderValue("application/x-protobuf");
        return c;
    }

    public Task<HttpResponseMessage> Send(byte[] body, string path = "/api/public/otel/v1/traces") => Tool.PostAsync(path, Proto(body));

    public LocalEndpoint Local => Fwd.Local;

    public static byte[] Body(int n, int seed = 1)
    {
        var b = new byte[n];
        new Random(seed).NextBytes(b);
        return b;
    }

    public async ValueTask DisposeAsync()
    {
        Tool.Dispose();
        await Fwd.App.StopAsync();
        await Fwd.App.DisposeAsync();
        await Ingress.DisposeAsync();
    }
}
