using System.Net.Http.Headers;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Oscope.Forwarder.Core;
using Yarp.ReverseProxy.Forwarder;

namespace Oscope.Forwarder.Http;

/// <summary>Sends one attempt to the ingress; the pump's only network seam.</summary>
public interface IIngressSender
{
    Task<Outcome> SendAsync(Attempt attempt, string accessToken, CancellationToken ct);
}

/// <summary>
/// The pass-through (D37: YARP). Each attempt is forwarded by YARP's
/// <see cref="IHttpForwarder"/> from a request context built over the entry's own byte
/// array: the body is the tool's bytes, unchanged on every attempt (LS-E3); the only
/// headers that leave are the content type and encoding, the bearer token, and the
/// configured namespace choice. Nothing the tool sent is trusted or passed on: not its
/// Authorization (a local key), not its x-langfuse-* headers, not a user id (R-E1: the
/// ingress stamps the person from the token).
/// </summary>
public sealed class YarpIngressSender : IIngressSender
{
    public const string NamespaceHeader = "X-Oscope-Namespace";

    private readonly IHttpForwarder _forwarder;
    private readonly HttpMessageInvoker _client;
    private readonly string _prefix;
    private readonly ForwarderRequestConfig _config;
    private readonly string? _namespace;

    /// <param name="namespaceChoice">from the forwarder's configuration only (MDM), never from the tool; it chooses among the person's grants and never grants</param>
    public YarpIngressSender(IHttpForwarder forwarder, HttpMessageInvoker client, Uri ingress, TimeSpan attemptTimeout, string? namespaceChoice)
    {
        ArgumentNullException.ThrowIfNull(ingress);
        _forwarder = forwarder;
        _client = client;
        _prefix = ingress.GetLeftPart(UriPartial.Path).TrimEnd('/');
        _config = new ForwarderRequestConfig { ActivityTimeout = attemptTimeout };
        _namespace = namespaceChoice;
    }

    public async Task<Outcome> SendAsync(Attempt attempt, string accessToken, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(attempt);
        var ctx = new DefaultHttpContext();
        ctx.Features.Set<IHttpRequestBodyDetectionFeature>(new HasBody());
        var req = ctx.Request;
        req.Protocol = "HTTP/1.1";
        req.Method = HttpMethods.Post;
        req.Scheme = "http";
        req.Host = new HostString("127.0.0.1");
        req.Path = attempt.Request.Path;
        req.ContentType = attempt.Request.ContentType;
        if (attempt.Request.ContentEncoding is { } enc) req.Headers.ContentEncoding = enc;
        req.ContentLength = attempt.Request.Body.LongLength;
        req.Body = new MemoryStream(attempt.Request.Body, writable: false);
        using var answer = new MemoryStream();
        ctx.Response.Body = answer;
        ctx.RequestAborted = ct;
        var err = await _forwarder.SendAsync(ctx, _prefix, _client, _config, new BearerTransformer(accessToken, _namespace), ct)
            .ConfigureAwait(false);
        if (err == ForwarderError.None)
            return Outcome.FromStatus(ctx.Response.StatusCode, RetryAfter(ctx.Response.Headers.RetryAfter.ToString()));
        return Classify(err, ctx.Features.Get<IForwarderErrorFeature>()?.Exception);
    }

    /// <summary>
    /// A send that failed below HTTP. Only a failure to connect is definitely "not sent";
    /// anything after the bytes may have left is unknown (CAST rows 50, 74, 83), never "failed".
    /// </summary>
    internal static Outcome Classify(ForwarderError err, Exception? e)
    {
        if (err == ForwarderError.RequestCreation) return new Outcome(OutcomeKind.NotSent);
        for (var x = e; x is not null; x = x.InnerException)
        {
            if (x is HttpRequestException { HttpRequestError: HttpRequestError.ConnectionError or HttpRequestError.NameResolutionError or HttpRequestError.SecureConnectionError or HttpRequestError.ProxyTunnelError })
                return new Outcome(OutcomeKind.NotSent);
        }
        return Outcome.Unknown();
    }

    internal static TimeSpan? RetryAfter(string? value)
    {
        if (string.IsNullOrWhiteSpace(value) || !RetryConditionHeaderValue.TryParse(value, out var v)) return null;
        if (v.Delta is { } d) return d;
        if (v.Date is { } at)
        {
            var left = at - DateTimeOffset.UtcNow;
            return left > TimeSpan.Zero ? left : TimeSpan.Zero;
        }
        return null;
    }

    private sealed class HasBody : IHttpRequestBodyDetectionFeature
    {
        public bool CanHaveBody => true;
    }

    private sealed class BearerTransformer(string token, string? ns) : HttpTransformer
    {
        public override async ValueTask TransformRequestAsync(HttpContext httpContext, HttpRequestMessage proxyRequest,
            string destinationPrefix, CancellationToken cancellationToken)
        {
            await base.TransformRequestAsync(httpContext, proxyRequest, destinationPrefix, cancellationToken).ConfigureAwait(false);
            // An allow-list of nothing: every request header is dropped, then ours are set.
            // (Content-Type and Content-Encoding travel on the content, untouched.)
            foreach (var name in proxyRequest.Headers.Select(h => h.Key).ToList()) proxyRequest.Headers.Remove(name);
            proxyRequest.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
            if (ns is not null) proxyRequest.Headers.TryAddWithoutValidation(NamespaceHeader, ns);
        }
    }
}
