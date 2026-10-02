using System.Diagnostics;
using System.Net;
using System.Net.Http.Headers;
using Microsoft.AspNetCore.Http;
using Yarp.ReverseProxy.Forwarder;

namespace Oscope.Forwarder.Http;

/// <summary>
/// The pass-through's outbound half (D40, pass-through amendment): YARP's
/// <see cref="IHttpForwarder"/> proxies the tool's live request on its own
/// <see cref="HttpContext"/>, streaming the body up and the ingress's answer (status,
/// headers, body) back unchanged. The only request headers that leave are the framing
/// ones (content type, encoding and length; chunked framing is the client's) plus the
/// bearer token and the configured namespace choice: nothing the tool sent is trusted or
/// passed on, not its Authorization (the local key), not its x-langfuse-* headers, not a
/// user id, not its trace context (R-E1, SEC-E2, SEC-E7: the ingress stamps the person).
/// </summary>
public static class IngressProxy
{
    public const string NamespaceHeader = "X-Oscope-Namespace";

    /// <summary>The content headers that are passed through: what the ingress needs to read the body as sent.</summary>
    public static readonly IReadOnlySet<string> ContentHeaders =
        new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "Content-Type", "Content-Encoding", "Content-Length" };

    /// <summary>
    /// The forwarding client, as YARP recommends for a proxy: no redirects, no cookies, no
    /// decompression (the bytes pass through, unread); the system proxy is honoured. And no
    /// trace context of its own: the default propagator would inject a <c>traceparent</c>
    /// derived from the tool's (ASP.NET Core parents the request's activity on it), which
    /// would carry a client header to the ingress in another form.
    /// </summary>
    /// <param name="configure">tests only: e.g. small socket buffers, to measure what the forwarder itself holds</param>
    public static HttpMessageInvoker CreateClient(Action<SocketsHttpHandler>? configure = null)
    {
        var h = new SocketsHttpHandler
        {
            AllowAutoRedirect = false,
            UseCookies = false,
            AutomaticDecompression = DecompressionMethods.None,
            ConnectTimeout = TimeSpan.FromSeconds(15),
            PooledConnectionLifetime = TimeSpan.FromMinutes(5),
            ActivityHeadersPropagator = DistributedContextPropagator.CreateNoOutputPropagator(),
        };
        configure?.Invoke(h);
        return new HttpMessageInvoker(h);
    }

    /// <summary>One request's transform: the upstream path, this request's token, the configured namespace.</summary>
    internal sealed class Transformer(string upstreamPath, string token, string? ns) : HttpTransformer
    {
        public override async ValueTask TransformRequestAsync(HttpContext httpContext, HttpRequestMessage proxyRequest,
            string destinationPrefix, CancellationToken cancellationToken)
        {
            await base.TransformRequestAsync(httpContext, proxyRequest, destinationPrefix, cancellationToken).ConfigureAwait(false);
            // The tool's path and query are not passed on: the path is mapped, there is no query.
            proxyRequest.RequestUri = new Uri(destinationPrefix + upstreamPath, UriKind.Absolute);
            // An allow-list of nothing for request headers, then ours; content headers by allow-list.
            foreach (var name in proxyRequest.Headers.Select(h => h.Key).ToList()) proxyRequest.Headers.Remove(name);
            if (proxyRequest.Content is { } content)
            {
                foreach (var name in content.Headers.Select(h => h.Key).ToList())
                    if (!ContentHeaders.Contains(name)) content.Headers.Remove(name);
            }
            proxyRequest.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
            if (ns is not null) proxyRequest.Headers.TryAddWithoutValidation(NamespaceHeader, ns);
        }
    }
}
