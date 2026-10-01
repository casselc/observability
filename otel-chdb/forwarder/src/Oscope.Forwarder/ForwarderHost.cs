using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Oscope.Forwarder.Auth;
using Oscope.Forwarder.Core;
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

    public ForwarderOptions Queue { get; init; } = new();

    public PumpOptions Pump { get; init; } = new();

    public ForwarderSettings Validate()
    {
        ArgumentNullException.ThrowIfNull(Ingress);
        var loopback = Ingress.IsLoopback;
        if (Ingress.Scheme != Uri.UriSchemeHttps && !(Ingress.Scheme == Uri.UriSchemeHttp && loopback))
            throw new ArgumentException("forwarder: the ingress URL must be https (http only on loopback, for tests)");
        Local.Validate();
        Queue.Validate();
        if (Pump.AttemptTimeout <= TimeSpan.Zero || Pump.TokenTimeout <= TimeSpan.Zero || Pump.DrainTimeout < TimeSpan.Zero)
            throw new ArgumentException("forwarder: timeouts must be positive");
        if (Pump.AttemptTimeout >= Queue.MaxAge)
            throw new ArgumentException("forwarder: AttemptTimeout must be below MaxAge (else one attempt outlives its entry)");
        return this;
    }
}

/// <summary>A built forwarder: the web app, and its parts for status and tests.</summary>
public sealed record ForwarderInstance(WebApplication App, Pump Pump, LocalEndpoint Local);

/// <summary>Builds the forwarder's web host: Kestrel on loopback, the local endpoint, the pump.</summary>
public static class ForwarderHost
{
    /// <param name="configureWebHost">tests swap Kestrel for a TestServer here</param>
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
            k.Limits.MaxRequestBodySize = settings.Queue.MaxEntryBytes;
            k.AddServerHeader = false;
        });
        configureWebHost?.Invoke(b.WebHost);
        b.Services.AddHttpForwarder();
        var app = b.Build();
        var client = ingressClient ?? new HttpMessageInvoker(new SocketsHttpHandler
        {
            // YARP's guidance for a forwarding client: no redirects, no cookies, no decompression
            // (the bytes are passed through, not read); the system proxy is honoured.
            AllowAutoRedirect = false,
            UseCookies = false,
            AutomaticDecompression = DecompressionMethods.None,
            ConnectTimeout = TimeSpan.FromSeconds(15),
            PooledConnectionLifetime = TimeSpan.FromMinutes(5),
        });
        var sender = new YarpIngressSender(app.Services.GetRequiredService<IHttpForwarder>(), client, settings.Ingress,
            settings.Pump.AttemptTimeout, settings.Namespace);
        var pump = new Pump(new ForwarderCore(settings.Queue), tokens, sender, time ?? TimeProvider.System, settings.Pump);
        var local = new LocalEndpoint(pump, settings.Local);
        local.Map(app);
        var life = app.Services.GetRequiredService<IHostApplicationLifetime>();
        life.ApplicationStarted.Register(pump.Start);
        life.ApplicationStopping.Register(() => pump.StopAsync().GetAwaiter().GetResult());
        return new ForwarderInstance(app, pump, local);
    }
}
