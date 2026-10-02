using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Oscope.Forwarder.Auth;
using Oscope.Forwarder.Http;
using Yarp.ReverseProxy.Forwarder;

namespace Oscope.Forwarder;

/// <summary>Everything a forwarder process is configured with (one record, validated together).</summary>
public sealed record ForwarderSettings
{
    /// <summary>The ingress's base URL. https, or a loopback http URL for tests (a token never crosses a network in clear).</summary>
    public required Uri Ingress { get; init; }

    /// <summary>X-Oscope-Namespace: chooses among the person's grants, from MDM config only; never from the tool.</summary>
    public string? Namespace { get; init; }

    public required LocalOptions Local { get; init; }

    public ProxyOptions Proxy { get; init; } = new();

    public ForwarderSettings Validate()
    {
        ArgumentNullException.ThrowIfNull(Ingress);
        if (Ingress.Scheme != Uri.UriSchemeHttps && !(Ingress.Scheme == Uri.UriSchemeHttp && Ingress.IsLoopback))
            throw new ArgumentException("forwarder: the ingress URL must be https (http only on loopback, for tests)");
        Local.Validate();
        Proxy.Validate();
        return this;
    }
}

/// <summary>A built forwarder: the web app and its endpoint (counters, tokens) for status and tests.</summary>
public sealed record ForwarderInstance(WebApplication App, LocalEndpoint Local)
{
    /// <summary>The address Kestrel bound (after start), e.g. http://127.0.0.1:14318.</summary>
    public Uri Address => new(App.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First());
}

/// <summary>Builds the forwarder's web host: Kestrel on loopback, the local endpoint, YARP.</summary>
public static class ForwarderHost
{
    /// <summary>
    /// Kestrel's request pipe per connection. Small, so a request held up by a slow ingress
    /// holds little: Kestrel stops reading the tool's socket once this much is unread, and
    /// TCP pushes back to the tool (the pass-through's memory bound, with the concurrency bound).
    /// </summary>
    public const long RequestBufferBytes = 64 * 1024;

    /// <param name="configureWebHost">tests may change the web host here</param>
    /// <param name="ingressClient">tests may pass their own; the default is <see cref="IngressProxy.CreateClient"/></param>
    public static ForwarderInstance Build(ForwarderSettings settings, ITokenAcquirer tokens, string[]? args = null,
        Action<IWebHostBuilder>? configureWebHost = null, HttpMessageInvoker? ingressClient = null, TimeProvider? time = null)
    {
        ArgumentNullException.ThrowIfNull(settings);
        settings.Validate();
        var b = WebApplication.CreateSlimBuilder(args ?? []);
        b.Logging.ClearProviders();
        b.Logging.AddSimpleConsole(o => o.SingleLine = true);
        b.Logging.SetMinimumLevel(LogLevel.Warning);
        b.WebHost.ConfigureKestrel(k =>
        {
            k.Listen(IPAddress.Loopback, settings.Local.Port);
            k.Limits.MaxRequestBodySize = settings.Proxy.MaxRequestBytes;
            k.Limits.MaxRequestBufferSize = RequestBufferBytes;
            k.AddServerHeader = false;
        });
        configureWebHost?.Invoke(b.WebHost);
        b.Services.AddHttpForwarder();
        var app = b.Build();
        var gate = new TokenGate(tokens, time ?? TimeProvider.System, settings.Proxy.TokenTimeout, settings.Proxy.ForcedRefreshMinInterval);
        var local = new LocalEndpoint(settings.Local, settings.Proxy, gate, app.Services.GetRequiredService<IHttpForwarder>(),
            ingressClient ?? IngressProxy.CreateClient(), settings.Ingress, settings.Namespace);
        local.Map(app);
        return new ForwarderInstance(app, local);
    }
}
