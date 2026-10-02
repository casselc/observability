namespace Oscope.Forwarder.Auth;

/// <summary>
/// The person a token was issued to: the Entra tenant id and object id. Shown on
/// <c>/status</c> (TM-E1); the forwarder never puts it into a request (the ingress stamps
/// the person from the token: R-E1).
/// </summary>
public readonly record struct AccountKey(string TenantId, string ObjectId)
{
    public override string ToString() => $"{TenantId}/{ObjectId}";
}

public enum TokenStatus
{
    Ok,
    /// <summary>The broker needs the person (sign-in, consent, MFA, a CA remedy). Never prompted on the tool's path (R-E6, TM-E2).</summary>
    InteractionRequired,
    /// <summary>The broker or the network failed; a later request may succeed.</summary>
    Unavailable,
}

/// <summary>An access token for the ingress API, and whose it is.</summary>
public sealed record TokenResult(TokenStatus Status, string? AccessToken, AccountKey? Account, DateTimeOffset ExpiresOn, string? Detail = null)
{
    public static TokenResult Failed(TokenStatus s, string detail) => new(s, null, null, default, detail);
}

/// <summary>
/// Where the forwarder gets tokens: MSAL.NET with the platform broker on a device
/// (<c>MsalBrokerTokenAcquirer</c> in the app), a fake in tests. Each request is sent with
/// a token for the account the broker answers for when that request is sent (D40,
/// pass-through amendment). The forwarder never stores a token anywhere but this object's
/// memory, and never logs one (SEC-E1).
/// </summary>
public interface ITokenAcquirer
{
    /// <summary>The account the broker last answered for, if known (for <c>/status</c>).</summary>
    AccountKey? CurrentAccount { get; }

    /// <summary>
    /// Silently obtains a token for the broker's signed-in account. Never shows UI.
    /// <paramref name="forceRefresh"/> skips the cached access token (after the ingress
    /// refused it with a 401).
    /// </summary>
    ValueTask<TokenResult> AcquireSilentAsync(bool forceRefresh, CancellationToken ct);
}
