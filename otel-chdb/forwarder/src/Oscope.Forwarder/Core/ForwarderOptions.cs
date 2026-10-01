namespace Oscope.Forwarder.Core;

/// <summary>
/// The bounds of the in-memory queue and its retries (D37: a small bounded in-memory
/// queue that rides over token refreshes and brief network loss; no disk). Validated
/// together (CAST rows 5 and 25: combined parameters are checked together).
/// </summary>
public sealed record ForwarderOptions
{
    /// <summary>Bytes of request bodies held, in the queue and in flight together.</summary>
    public long MaxQueueBytes { get; init; } = 32L << 20;

    /// <summary>Entries held, in the queue and in flight together.</summary>
    public int MaxEntries { get; init; } = 1024;

    /// <summary>One request's body as sent by the tool (the ingress's compressed cap is 16 MiB).</summary>
    public long MaxEntryBytes { get; init; } = 16L << 20;

    /// <summary>Attempts per entry, the first included (CAST 39: every retry loop bounded).</summary>
    public int MaxAttempts { get; init; } = 12;

    /// <summary>How long an entry may be held, by the age clock; then it is dropped and counted.</summary>
    public TimeSpan MaxAge { get; init; } = TimeSpan.FromMinutes(10);

    /// <summary>First retry delay; doubled per attempt, with jitter, up to <see cref="BackoffCap"/>.</summary>
    public TimeSpan BackoffBase { get; init; } = TimeSpan.FromMilliseconds(500);

    public TimeSpan BackoffCap { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>The longest Retry-After honoured; a longer hint is cut to this.</summary>
    public TimeSpan MaxRetryAfter { get; init; } = TimeSpan.FromSeconds(60);

    /// <summary>Attempts running at once.</summary>
    public int MaxInFlight { get; init; } = 2;

    /// <summary>The Retry-After the tool is given when the queue is full or no account is signed in.</summary>
    public TimeSpan RefusalRetryAfter { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>Throws if the bounds cannot work together.</summary>
    public ForwarderOptions Validate()
    {
        var errors = new List<string>();
        if (MaxQueueBytes <= 0) errors.Add("MaxQueueBytes must be > 0");
        if (MaxEntries <= 0) errors.Add("MaxEntries must be > 0");
        if (MaxEntryBytes <= 0 || MaxEntryBytes > MaxQueueBytes)
            errors.Add("MaxEntryBytes must be > 0 and <= MaxQueueBytes (else one request can never be accepted)");
        if (MaxAttempts < 2) errors.Add("MaxAttempts must be >= 2 (a 401 needs one refreshed retry)");
        if (BackoffBase <= TimeSpan.Zero || BackoffCap < BackoffBase) errors.Add("want 0 < BackoffBase <= BackoffCap");
        if (MaxAge <= BackoffCap) errors.Add("MaxAge must exceed BackoffCap (else an entry expires before its first capped retry)");
        if (MaxRetryAfter <= TimeSpan.Zero) errors.Add("MaxRetryAfter must be > 0");
        if (MaxInFlight <= 0 || MaxInFlight > MaxEntries) errors.Add("MaxInFlight must be in 1..MaxEntries");
        if (RefusalRetryAfter <= TimeSpan.Zero) errors.Add("RefusalRetryAfter must be > 0");
        if (errors.Count > 0) throw new ArgumentException("forwarder options: " + string.Join("; ", errors));
        return this;
    }
}
