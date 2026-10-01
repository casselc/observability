namespace Oscope.Forwarder.Core;

/// <summary>
/// The person a request was produced under: the Entra tenant id and object id of the
/// account signed in when the tool's request was accepted (R-E6, LS-E5). An entry is
/// only ever sent with a token for this same account.
/// </summary>
public readonly record struct AccountKey(string TenantId, string ObjectId)
{
    public override string ToString() => $"{TenantId}/{ObjectId}";
}

/// <summary>
/// A tool's request exactly as received: the bytes are never decoded, re-encoded or
/// re-batched, so every attempt sends the same bytes and a retry of a request that did
/// land is a content-key copy at the ingress (LS-E3, R-E6, D11).
/// </summary>
public sealed record ForwardRequest(string Path, string ContentType, string? ContentEncoding, byte[] Body);

/// <summary>Why a request was refused at the local endpoint (never acknowledged).</summary>
public enum RefusalReason
{
    /// <summary>The queue's byte or entry bound would be exceeded: 503 + Retry-After to the tool.</summary>
    Full,
    /// <summary>The body is over the per-entry bound: 413.</summary>
    TooLarge,
    /// <summary>No account is signed in through the broker yet: 503 + Retry-After.</summary>
    NoAccount,
    /// <summary>The forwarder is stopping.</summary>
    ShuttingDown,
}

/// <summary>The answer to <see cref="ForwarderCore.Offer"/>.</summary>
public readonly record struct Admission(bool Accepted, long Id, RefusalReason Reason)
{
    public static Admission Ok(long id) => new(true, id, default);
    public static Admission Refused(RefusalReason r) => new(false, 0, r);
}

/// <summary>One send of one entry, handed to the I/O shell.</summary>
public sealed record Attempt(long Id, ForwardRequest Request, AccountKey Account, int Number, bool ForceRefresh);

/// <summary>What became of an attempt, as the shell observed it.</summary>
public enum OutcomeKind
{
    /// <summary>2xx from the ingress: every object committed (R-E5).</summary>
    Committed,
    /// <summary>A definite refusal that a retry cannot change (400, 413, 415, other 4xx): nothing landed.</summary>
    Rejected,
    /// <summary>403: no grant for this person; nothing landed.</summary>
    Forbidden,
    /// <summary>401: the token was refused before the body was read; nothing landed.</summary>
    Unauthorized,
    /// <summary>429: refused before publishing; nothing landed; retry after the hint.</summary>
    RateLimited,
    /// <summary>
    /// The outcome is not known: a lost answer, a timeout after the bytes left, a reset,
    /// 5xx (the ingress's 503 means "may still commit"). Not a failure: it may have
    /// landed, and the retry of the same bytes is then a copy (CAST rows 50, 74, 83).
    /// </summary>
    Unknown,
    /// <summary>Definitely not sent: no connection, or no token could be obtained.</summary>
    NotSent,
    /// <summary>The broker now answers for another account: the entry is never sent under it (LS-E5).</summary>
    AccountChanged,
}

/// <summary>An outcome with the ingress's Retry-After hint, if any.</summary>
public readonly record struct Outcome(OutcomeKind Kind, TimeSpan? RetryAfter = null, int Status = 0)
{
    public static Outcome Committed => new(OutcomeKind.Committed);
    public static Outcome Unknown(TimeSpan? retryAfter = null, int status = 0) => new(OutcomeKind.Unknown, retryAfter, status);

    /// <summary>
    /// Classifies an HTTP status from the ingress (research/entra-ingress.md §1.9 row 2:
    /// 200 = committed, 4xx = definite, 503 = unknown).
    /// </summary>
    public static Outcome FromStatus(int status, TimeSpan? retryAfter) => status switch
    {
        >= 200 and < 300 => new(OutcomeKind.Committed, null, status),
        401 => new(OutcomeKind.Unauthorized, null, status),
        403 => new(OutcomeKind.Forbidden, null, status),
        408 => new(OutcomeKind.Unknown, retryAfter, status),
        429 => new(OutcomeKind.RateLimited, retryAfter, status),
        >= 400 and < 500 => new(OutcomeKind.Rejected, null, status),
        // 5xx, and anything else a proxy in between might answer: it may have landed.
        _ => new(OutcomeKind.Unknown, retryAfter, status),
    };
}

/// <summary>Why an acknowledged entry left the queue without a commit (H-E4: counted, never silent).</summary>
public static class DropReason
{
    public const string Rejected = "rejected";
    public const string Forbidden = "forbidden";
    public const string Expired = "expired";
    public const string AttemptsExhausted = "attempts_exhausted";
    public const string AccountChanged = "account_changed";
    public const string Shutdown = "shutdown";
}
