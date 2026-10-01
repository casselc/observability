using Oscope.Forwarder.Core;

namespace Oscope.Forwarder.Auth;

public enum TokenStatus
{
    Ok,
    /// <summary>The broker needs the person (sign-in, consent, MFA, a CA remedy). Never prompted on the tool's path (R-E6, TM-E2).</summary>
    InteractionRequired,
    /// <summary>The broker or the network failed; try again later.</summary>
    Unavailable,
    /// <summary>The expected account is no longer signed in (sign-out, account switch): its entries are never sent (LS-E5).</summary>
    AccountGone,
}

/// <summary>An access token for the ingress API, and whose it is.</summary>
public sealed record TokenResult(TokenStatus Status, string? AccessToken, AccountKey? Account, DateTimeOffset ExpiresOn, string? Detail = null)
{
    public static TokenResult Failed(TokenStatus s, string detail) => new(s, null, null, default, detail);
}

/// <summary>
/// Where the forwarder gets tokens: MSAL.NET with the platform broker on a device
/// (<see cref="MsalBrokerTokenAcquirer"/>), a fake in tests. The forwarder never stores
/// a token anywhere but this object's memory, and never logs one (SEC-E1).
/// </summary>
public interface ITokenAcquirer
{
    /// <summary>The account the broker answers for now, if known; requests are accepted under it.</summary>
    AccountKey? CurrentAccount { get; }

    /// <summary>
    /// Silently obtains a token for <paramref name="expected"/>'s account (the account an
    /// entry was accepted under), or for the broker's default account when null. Never
    /// shows UI. The caller compares the result's account with the one it expected.
    /// </summary>
    ValueTask<TokenResult> AcquireSilentAsync(AccountKey? expected, bool forceRefresh, CancellationToken ct);
}
