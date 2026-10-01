using Microsoft.Identity.Client;
using Microsoft.Identity.Client.Broker;
using Oscope.Forwarder.Auth;
using Oscope.Forwarder.Core;

namespace Oscope.Forwarder.App;

/// <summary>Where the forwarder's tokens come from on a device (research/entra-ingress.md §4.2).</summary>
public sealed record EntraOptions
{
    /// <summary>The forwarder's public client registration.</summary>
    public required string ClientId { get; init; }

    /// <summary>The home tenant's GUID (single-tenant, O-E1; never "common" or "organizations").</summary>
    public required string TenantId { get; init; }

    /// <summary>The ingress API's delegated scope, e.g. "api://oscope-ingress/Telemetry.Write".</summary>
    public required string Scope { get; init; }

    public string Instance { get; init; } = "https://login.microsoftonline.com";

    public EntraOptions Validate()
    {
        if (!Guid.TryParse(ClientId, out _)) throw new ArgumentException("entra: ClientId must be a GUID");
        if (!Guid.TryParse(TenantId, out _))
            throw new ArgumentException("entra: TenantId must be the home tenant's GUID (not 'common', 'organizations' or a domain)");
        if (string.IsNullOrWhiteSpace(Scope) || Scope.Contains(' ', StringComparison.Ordinal))
            throw new ArgumentException("entra: Scope must be exactly one scope, the ingress API's");
        return this;
    }
}

/// <summary>
/// MSAL.NET with the platform broker: WAM on Windows, the Enterprise SSO extension (and
/// Platform SSO) on macOS (research/entra-ingress.md §4.1). The broker holds the refresh
/// token, bound to the device; this class holds only MSAL's in-memory cache of access
/// tokens and never writes one to disk or to a log (SEC-E1, R-E7).
///
/// NOT VERIFIED in CI: the broker needs a real tenant, an enrolled device and a person.
/// The manual steps are deploy/validation/entra-ingress.md (ENT-F1..F6); tests use
/// <see cref="ITokenAcquirer"/> fakes.
/// </summary>
public sealed class MsalBrokerTokenAcquirer : ITokenAcquirer
{
    private readonly IPublicClientApplication _app;
    private readonly string[] _scopes;
    private readonly Lock _gate = new();
    private AccountKey? _current;

    public MsalBrokerTokenAcquirer(EntraOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        _scopes = [options.Scope];
        _app = PublicClientApplicationBuilder.Create(options.ClientId)
            .WithAuthority($"{options.Instance.TrimEnd('/')}/{options.TenantId}")
            .WithDefaultRedirectUri()
            .WithBroker(new BrokerOptions(BrokerOptions.OperatingSystems.Windows | BrokerOptions.OperatingSystems.OSX))
            .Build();
    }

    public AccountKey? CurrentAccount
    {
        get
        {
            lock (_gate) return _current;
        }
    }

    public async ValueTask<TokenResult> AcquireSilentAsync(AccountKey? expected, bool forceRefresh, CancellationToken ct)
    {
        try
        {
            IAccount? account;
            if (expected is { } want)
            {
                var accounts = await _app.GetAccountsAsync().ConfigureAwait(false);
                account = accounts.FirstOrDefault(a => a.HomeAccountId?.ObjectId == want.ObjectId && a.HomeAccountId?.TenantId == want.TenantId);
                if (account is null && OperatingSystem.IsWindows())
                    account = PublicClientApplication.OperatingSystemAccount; // WAM: the account signed in to Windows
                if (account is null) return Forget(TokenResult.Failed(TokenStatus.AccountGone, "the account is no longer signed in"));
            }
            else
            {
                account = OperatingSystem.IsWindows()
                    ? PublicClientApplication.OperatingSystemAccount
                    : (await _app.GetAccountsAsync().ConfigureAwait(false)).FirstOrDefault();
                if (account is null) return TokenResult.Failed(TokenStatus.InteractionRequired, "no account signed in");
            }
            var r = await _app.AcquireTokenSilent(_scopes, account).WithForceRefresh(forceRefresh).ExecuteAsync(ct).ConfigureAwait(false);
            var got = new AccountKey(r.TenantId, r.UniqueId);
            lock (_gate) _current = got;
            return new TokenResult(TokenStatus.Ok, r.AccessToken, got, r.ExpiresOn);
        }
        catch (MsalUiRequiredException e)
        {
            return TokenResult.Failed(TokenStatus.InteractionRequired, e.ErrorCode);
        }
        catch (MsalException e)
        {
            return TokenResult.Failed(TokenStatus.Unavailable, e.ErrorCode);
        }
        catch (HttpRequestException e)
        {
            return TokenResult.Failed(TokenStatus.Unavailable, e.GetType().Name);
        }
    }

    /// <summary>
    /// The interactive sign-in, for the status item only (TM-E2: never on the tool's call
    /// path). On macOS MSAL requires the main thread and a signed app bundle.
    /// </summary>
    public async Task<AccountKey> SignInInteractiveAsync(IntPtr parentWindow, CancellationToken ct)
    {
        var r = await _app.AcquireTokenInteractive(_scopes).WithParentActivityOrWindow(parentWindow)
            .ExecuteAsync(ct).ConfigureAwait(false);
        var got = new AccountKey(r.TenantId, r.UniqueId);
        lock (_gate) _current = got;
        return got;
    }

    private TokenResult Forget(TokenResult r)
    {
        lock (_gate) _current = null;
        return r;
    }
}
