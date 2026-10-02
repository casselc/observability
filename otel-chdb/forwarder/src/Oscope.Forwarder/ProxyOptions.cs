namespace Oscope.Forwarder;

/// <summary>
/// The pass-through's bounds (D40, pass-through amendment 2026-10-02). There is no queue
/// and no retry: these bound one request's size, how many are proxied at once, and how
/// long the forwarder's own steps may take. Validated together (CAST rows 5 and 25).
/// </summary>
public sealed record ProxyOptions
{
    /// <summary>The ingress's cap on a request body as sent (ingress DefaultLimits.MaxBodyBytes).</summary>
    public const long IngressMaxBodyBytes = 16L << 20;

    /// <summary>One request's body as sent by the tool; more is 413 (the ingress would refuse it too).</summary>
    public long MaxRequestBytes { get; init; } = IngressMaxBodyBytes;

    /// <summary>
    /// Requests proxied at once; the next is answered 503 + <c>Retry-After</c> at once.
    /// Each request in flight holds a bounded set of buffers (Kestrel's request pipe, YARP's
    /// copy buffers), so this bounds the forwarder's memory under a slow ingress.
    /// </summary>
    public int MaxConcurrentRequests { get; init; } = 16;

    /// <summary>How long one silent token acquisition may take before the tool is answered 503.</summary>
    public TimeSpan TokenTimeout { get; init; } = TimeSpan.FromSeconds(10);

    /// <summary>YARP's activity timeout: no progress from the ingress for this long is 504 to the tool.</summary>
    public TimeSpan ActivityTimeout { get; init; } = TimeSpan.FromSeconds(100);

    /// <summary>The <c>Retry-After</c> on the forwarder's own 503s (busy, no token).</summary>
    public TimeSpan RefusalRetryAfter { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>The least time between two forced token refreshes (CAST 39: no refresh loop).</summary>
    public TimeSpan ForcedRefreshMinInterval { get; init; } = TimeSpan.FromSeconds(10);

    /// <summary>Throws if the bounds cannot work together.</summary>
    public ProxyOptions Validate()
    {
        var errors = new List<string>();
        if (MaxRequestBytes <= 0 || MaxRequestBytes > IngressMaxBodyBytes)
            errors.Add($"MaxRequestBytes must be in 1..{IngressMaxBodyBytes} (the ingress refuses a larger body)");
        if (MaxConcurrentRequests is <= 0 or > 1024) errors.Add("MaxConcurrentRequests must be in 1..1024");
        if (TokenTimeout <= TimeSpan.Zero) errors.Add("TokenTimeout must be > 0");
        if (ActivityTimeout <= TimeSpan.Zero) errors.Add("ActivityTimeout must be > 0");
        if (TokenTimeout >= ActivityTimeout) errors.Add("TokenTimeout must be below ActivityTimeout (the token is only the first step of a request)");
        // Retry-After is whole seconds: anything under one second would be sent as 0, which
        // tells every tool to retry at once (a hot loop against a forwarder that just refused).
        if (RefusalRetryAfter < TimeSpan.FromSeconds(1)) errors.Add("RefusalRetryAfter must be at least 1 s (Retry-After is whole seconds)");
        if (ForcedRefreshMinInterval < TimeSpan.Zero) errors.Add("ForcedRefreshMinInterval must be >= 0");
        if (errors.Count > 0) throw new ArgumentException("forwarder proxy options: " + string.Join("; ", errors));
        return this;
    }
}
