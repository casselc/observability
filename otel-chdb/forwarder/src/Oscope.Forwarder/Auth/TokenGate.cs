using System.Collections.Concurrent;

namespace Oscope.Forwarder.Auth;

/// <summary>
/// The token for each proxied request, and the only token state the forwarder keeps
/// (D40, pass-through amendment). It asks the broker per request (the broker caches);
/// after the ingress refused a token with a 401 (<see cref="Rejected"/>), the next request
/// that would be given that same token gets a forced refresh instead, once (a single
/// flight, so concurrent requests do not each force one), and at most once per
/// <c>forcedRefreshMinInterval</c> (CAST 39: a refused refreshed token cannot drive a
/// refresh loop at the tools' request rate). The refused request itself is never
/// replayed: its 401 goes back to the tool.
/// </summary>
public sealed class TokenGate
{
    private readonly ITokenAcquirer _tokens;
    private readonly TimeProvider _time;
    private readonly TimeSpan _timeout;
    private readonly TimeSpan _minForceInterval;
    private readonly SemaphoreSlim _refresh = new(1, 1);
    private readonly ConcurrentDictionary<string, long> _failures = new(StringComparer.Ordinal);
    private readonly Lock _gate = new();
    private string? _rejected;
    private long _lastForced;
    private bool _forcedOnce;
    private AccountKey? _lastAccount;
    private long _accountChanges, _forcedRefreshes;

    public TokenGate(ITokenAcquirer tokens, TimeProvider time, TimeSpan timeout, TimeSpan forcedRefreshMinInterval)
    {
        _tokens = tokens;
        _time = time;
        _timeout = timeout;
        _minForceInterval = forcedRefreshMinInterval;
    }

    /// <summary>The account the last token was issued to.</summary>
    public AccountKey? CurrentAccount => _tokens.CurrentAccount;

    /// <summary>Token acquisitions that gave no token, by reason (<c>/status</c>).</summary>
    public IReadOnlyDictionary<string, long> Failures => new Dictionary<string, long>(_failures);

    /// <summary>How often a request went out under another account than the one before it (TM-E1, LS-E5).</summary>
    public long AccountChanges => Interlocked.Read(ref _accountChanges);

    public long ForcedRefreshes => Interlocked.Read(ref _forcedRefreshes);

    /// <summary>The ingress answered 401 to a request sent with <paramref name="token"/>.</summary>
    public void Rejected(string token) => Volatile.Write(ref _rejected, token);

    /// <summary>
    /// A token for one request, bounded by the token timeout. A token that cannot be had
    /// comes back as a failed result (counted), never as an exception, unless
    /// <paramref name="ct"/> (the tool's own request) was cancelled.
    /// </summary>
    public async ValueTask<TokenResult> GetAsync(CancellationToken ct)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(_timeout);
        TokenResult r;
        try
        {
            r = await _tokens.AcquireSilentAsync(false, cts.Token).ConfigureAwait(false);
            if (r.Status == TokenStatus.Ok && r.AccessToken is { } t && t == Volatile.Read(ref _rejected))
                r = await RefreshAsync(r, cts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            r = TokenResult.Failed(TokenStatus.Unavailable, "timeout");
            Count("timeout");
            return r;
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            r = TokenResult.Failed(TokenStatus.Unavailable, e.GetType().Name);
            Count("exception");
            return r;
        }
        if (r.Status != TokenStatus.Ok || r.AccessToken is null)
        {
            Count(r.Status == TokenStatus.Ok ? "empty" : r.Status.ToString().ToLowerInvariant());
            return r with { Status = r.Status == TokenStatus.Ok ? TokenStatus.Unavailable : r.Status };
        }
        lock (_gate)
        {
            if (_lastAccount is { } was && r.Account is { } now && was != now) _accountChanges++;
            if (r.Account is not null) _lastAccount = r.Account;
        }
        return r;
    }

    private async ValueTask<TokenResult> RefreshAsync(TokenResult stale, CancellationToken ct)
    {
        await _refresh.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var rejected = Volatile.Read(ref _rejected);
            if (rejected != stale.AccessToken)
                return await _tokens.AcquireSilentAsync(false, ct).ConfigureAwait(false); // refreshed while we waited
            lock (_gate)
            {
                if (_forcedOnce && _time.GetElapsedTime(_lastForced) < _minForceInterval) return stale;
                _forcedOnce = true;
                _lastForced = _time.GetTimestamp();
            }
            Interlocked.Increment(ref _forcedRefreshes);
            var fresh = await _tokens.AcquireSilentAsync(true, ct).ConfigureAwait(false);
            if (fresh.Status == TokenStatus.Ok) Interlocked.CompareExchange(ref _rejected, null, rejected);
            return fresh;
        }
        finally
        {
            _refresh.Release();
        }
    }

    private void Count(string reason) => _failures.AddOrUpdate(reason, 1, (_, n) => n + 1);
}
