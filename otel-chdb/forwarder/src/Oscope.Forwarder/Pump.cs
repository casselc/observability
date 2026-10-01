using System.Collections.Concurrent;
using Oscope.Forwarder.Auth;
using Oscope.Forwarder.Core;
using Oscope.Forwarder.Http;

namespace Oscope.Forwarder;

/// <summary>The shell's timing (all bounded: CAST 39).</summary>
public sealed record PumpOptions
{
    /// <summary>How long one silent token acquisition may take before it counts as "not sent".</summary>
    public TimeSpan TokenTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>How long one send may take before its outcome counts as unknown.</summary>
    public TimeSpan AttemptTimeout { get; init; } = TimeSpan.FromSeconds(60);

    /// <summary>On stop: how long running attempts may finish before they are abandoned (counted).</summary>
    public TimeSpan DrainTimeout { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>How often the counters are sent to the ingress as a log record (§4.4); zero: never.</summary>
    public TimeSpan ReportInterval { get; init; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// How often the broker is asked, in the background, which account is signed in, so a
    /// tool's request is accepted under the person signed in now (LS-E5) and a sign-out is
    /// noticed without a tool's request (never on the tool's path).
    /// </summary>
    public TimeSpan AccountRefresh { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>The longest the loop sleeps without looking at the queue.</summary>
    public TimeSpan MaxIdle { get; init; } = TimeSpan.FromSeconds(5);
}

/// <summary>
/// The I/O shell around <see cref="ForwarderCore"/>: it takes attempts, gets a token for
/// the entry's own account, sends through <see cref="IIngressSender"/>, and reports the
/// outcome. Every call into the core holds one lock; nothing here decides policy. The
/// tool's path (<see cref="Offer"/>) never waits for Entra or the network (H-E9, TM-E2).
/// </summary>
public sealed class Pump : IAsyncDisposable
{
    private readonly ForwarderCore _core;
    private readonly ITokenAcquirer _tokens;
    private readonly IIngressSender _sender;
    private readonly AgeClock _clock;
    private readonly PumpOptions _o;
    private readonly Lock _gate = new();
    private readonly SemaphoreSlim _signal = new(0);
    private readonly CancellationTokenSource _stop = new();
    private readonly CancellationTokenSource _abort = new();
    private readonly ConcurrentDictionary<long, Task> _running = new();
    private readonly ConcurrentDictionary<string, long> _tokenFailures = new(StringComparer.Ordinal);
    private Task? _loop;
    private bool _abandoned;
    private int _warming;
    private TimeSpan _nextReport;
    private TimeSpan _nextAccountCheck;

    public Pump(ForwarderCore core, ITokenAcquirer tokens, IIngressSender sender, TimeProvider time, PumpOptions? options = null)
    {
        _core = core;
        _tokens = tokens;
        _sender = sender;
        _clock = new AgeClock(time);
        _o = options ?? new PumpOptions();
        _nextReport = _o.ReportInterval;
    }

    public ForwarderOptions Options => _core.Options;

    /// <summary>The account requests are accepted under now (the broker's, as last seen).</summary>
    public AccountKey? CurrentAccount => _tokens.CurrentAccount;

    /// <summary>The tool's request: accepted into memory or refused at once.</summary>
    public Admission Offer(ForwardRequest request)
    {
        var account = _tokens.CurrentAccount;
        Admission a;
        lock (_gate) a = _core.Offer(_clock.Now(), account, request);
        if (a.Accepted) Wake();
        else if (a.Reason == RefusalReason.NoAccount) WarmUp();
        return a;
    }

    public CountersSnapshot Snapshot()
    {
        lock (_gate)
        {
            _core.Tick(_clock.Now());
            return _core.Snapshot();
        }
    }

    public IReadOnlyDictionary<string, long> TokenFailures => new Dictionary<string, long>(_tokenFailures);

    public void Start()
    {
        _loop ??= Task.Run(() => LoopAsync(_stop.Token));
        WarmUp();
    }

    /// <summary>Stops accepting, lets running attempts finish within the drain timeout, and counts the rest.</summary>
    public async Task StopAsync()
    {
        lock (_gate) _core.BeginShutdown(_clock.Now());
        await _stop.CancelAsync().ConfigureAwait(false);
        if (_loop is not null)
        {
            try { await _loop.ConfigureAwait(false); }
            catch (OperationCanceledException) { }
        }
        var running = Task.WhenAll(_running.Values.ToArray());
        await Task.WhenAny(running, Task.Delay(_o.DrainTimeout)).ConfigureAwait(false);
        await _abort.CancelAsync().ConfigureAwait(false);
        await Task.WhenAny(running, Task.Delay(TimeSpan.FromSeconds(1))).ConfigureAwait(false);
        lock (_gate)
        {
            _abandoned = true;
            _core.Abandon(_clock.Now());
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (!_abandoned) await StopAsync().ConfigureAwait(false);
        _stop.Dispose();
        _abort.Dispose();
        _signal.Dispose();
    }

    /// <summary>Wakes the loop (a new entry, a finished attempt).</summary>
    private void Wake()
    {
        try { _signal.Release(); }
        catch (ObjectDisposedException) { }
    }

    /// <summary>No account known yet: ask the broker once, in the background (never on the tool's path).</summary>
    private void WarmUp()
    {
        if (Interlocked.Exchange(ref _warming, 1) == 1) return;
        _ = Task.Run(async () =>
        {
            try
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(_abort.Token);
                cts.CancelAfter(_o.TokenTimeout);
                var r = await _tokens.AcquireSilentAsync(null, false, cts.Token).ConfigureAwait(false);
                if (r.Status != TokenStatus.Ok) _tokenFailures.AddOrUpdate(r.Status.ToString(), 1, (_, n) => n + 1);
            }
            catch (Exception e) when (e is OperationCanceledException or HttpRequestException)
            {
                _tokenFailures.AddOrUpdate("Exception", 1, (_, n) => n + 1);
            }
            finally
            {
                Volatile.Write(ref _warming, 0);
            }
        });
    }

    private async Task LoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            Attempt? a;
            TimeSpan? wake;
            TimeSpan now;
            bool full;
            lock (_gate)
            {
                now = _clock.Now();
                a = _core.Next(now);
                wake = _core.NextWake();
                full = _core.InFlight >= _core.Options.MaxInFlight;
            }
            if (a is not null)
            {
                var t = RunAttemptAsync(a);
                _running[a.Id] = t;
                _ = t.ContinueWith(done => _running.TryRemove(a.Id, out _), TaskScheduler.Default);
                continue;
            }
            if (_o.AccountRefresh > TimeSpan.Zero && now >= _nextAccountCheck)
            {
                _nextAccountCheck = now + _o.AccountRefresh;
                WarmUp();
            }
            if (_o.ReportInterval > TimeSpan.Zero && now >= _nextReport)
            {
                _nextReport = now + _o.ReportInterval;
                Report();
                continue;
            }
            var delay = _o.MaxIdle;
            if (!full && wake is { } w && w - now < delay) delay = w - now;
            if (delay < TimeSpan.FromMilliseconds(1)) delay = TimeSpan.FromMilliseconds(1);
            try { await _signal.WaitAsync(delay, ct).ConfigureAwait(false); }
            catch (OperationCanceledException) { return; }
        }
    }

    private async Task RunAttemptAsync(Attempt a)
    {
        var outcome = await AttemptAsync(a).ConfigureAwait(false);
        lock (_gate)
        {
            if (_abandoned) return; // counted by Abandon
            _core.Complete(_clock.Now(), a.Id, outcome);
        }
        Wake();
    }

    private async Task<Outcome> AttemptAsync(Attempt a)
    {
        TokenResult tok;
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(_abort.Token);
            cts.CancelAfter(_o.TokenTimeout);
            tok = await _tokens.AcquireSilentAsync(a.Account, a.ForceRefresh, cts.Token).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            _tokenFailures.AddOrUpdate(e is OperationCanceledException ? "Timeout" : "Exception", 1, (_, n) => n + 1);
            return new Outcome(OutcomeKind.NotSent);
        }
        if (tok.Status == TokenStatus.AccountGone) return new Outcome(OutcomeKind.AccountChanged);
        if (tok.Status != TokenStatus.Ok || tok.AccessToken is null)
        {
            _tokenFailures.AddOrUpdate(tok.Status.ToString(), 1, (_, n) => n + 1);
            return new Outcome(OutcomeKind.NotSent);
        }
        if (tok.Account != a.Account) return new Outcome(OutcomeKind.AccountChanged); // LS-E5: never under another person
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(_abort.Token);
            cts.CancelAfter(_o.AttemptTimeout);
            return await _sender.SendAsync(a, tok.AccessToken, cts.Token).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Whatever failed after the send began, the bytes may have arrived: unknown.
            return Outcome.Unknown();
        }
    }

    private void Report()
    {
        var snap = Snapshot();
        var body = CountersReport.Json(snap, TokenFailures, DateTimeOffset.UtcNow);
        Offer(new ForwardRequest("/v1/logs", "application/json", null, body));
    }
}
