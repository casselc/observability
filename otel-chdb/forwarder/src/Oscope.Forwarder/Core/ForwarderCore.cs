namespace Oscope.Forwarder.Core;

/// <summary>A point-in-time copy of the counters (H-E4, TM-E1: every drop counted and shown).</summary>
public sealed record CountersSnapshot(
    long Accepted,
    long Committed,
    long CommittedAfterUnknown,
    IReadOnlyDictionary<RefusalReason, long> Refused,
    IReadOnlyDictionary<string, long> Dropped,
    IReadOnlyDictionary<string, long> DroppedMaybeLanded,
    long Attempts,
    long UnknownOutcomes,
    long HeldEntries,
    long HeldBytes,
    int InFlight,
    TimeSpan OldestAge,
    long ClockRegressions)
{
    /// <summary>Acknowledged entries that left without a commit, whatever the reason.</summary>
    public long DroppedTotal => Dropped.Values.Sum() + DroppedMaybeLanded.Values.Sum();

    /// <summary>
    /// H-E4's ledger: every acknowledged entry is committed, counted as dropped, or still
    /// held. Nothing else can happen to it.
    /// </summary>
    public bool Balances => Accepted == Committed + DroppedTotal + HeldEntries;
}

/// <summary>
/// The forwarder's queue and retry policy, with no I/O, no threads and no clock of its
/// own (research/entra-ingress.md §10a: verification first). The shell calls it under one
/// lock: <see cref="Offer"/> when a tool's request arrives, <see cref="Next"/> to take an
/// attempt, <see cref="Complete"/> with what the attempt's send observed, and
/// <see cref="Tick"/> as time passes. Time is the shell's age clock: elapsed time, never
/// a wall-clock reading, so a clock set backwards cannot reorder or un-age anything
/// (CAST rows 26, 34); it may only stand still or advance.
///
/// Guarantees, each checked by the stateful tests against a reference model:
/// <list type="bullet">
/// <item>Bounded memory: held bytes (queued and in flight) never exceed MaxQueueBytes; a
/// request that would exceed it is refused (backpressure), never buffered beyond it.</item>
/// <item>Retries are copies: an attempt carries the entry's own byte array, unchanged.</item>
/// <item>Every acknowledged entry ends exactly once: committed, or dropped with a counted
/// reason (H-E4); a drop after an unknown outcome is counted apart, since it may have
/// landed (CAST rows 50, 74, 83).</item>
/// <item>An entry is sent only under the account it was accepted under (R-E6, LS-E5).</item>
/// <item>Every retry loop is bounded by attempts and by age (CAST 39).</item>
/// </list>
/// </summary>
public sealed class ForwarderCore
{
    private sealed class Entry(long id, ForwardRequest request, AccountKey account, TimeSpan enqueuedAt)
    {
        public long Id { get; } = id;
        public ForwardRequest Request { get; } = request;
        public AccountKey Account { get; } = account;
        public TimeSpan EnqueuedAt { get; } = enqueuedAt;
        public TimeSpan NextAt { get; set; } = enqueuedAt;
        public int Attempts { get; set; }
        public bool InFlight { get; set; }
        public bool MaybeLanded { get; set; }
        public bool ForceRefresh { get; set; }
        public bool RefreshedAfter401 { get; set; }
    }

    private readonly ForwarderOptions _o;
    private readonly Func<double> _jitter;
    private readonly SortedDictionary<long, Entry> _entries = new();
    private readonly Dictionary<RefusalReason, long> _refused = new();
    private readonly Dictionary<string, long> _dropped = new(StringComparer.Ordinal);
    private readonly Dictionary<string, long> _droppedMaybe = new(StringComparer.Ordinal);
    private long _nextId = 1, _heldBytes, _accepted, _committed, _committedAfterUnknown, _attempts, _unknown, _clockRegressions;
    private int _inFlight;
    private TimeSpan _now;
    private bool _stopping;

    /// <param name="options">validated bounds</param>
    /// <param name="jitter">a source in [0, 1) for backoff jitter (seeded in tests)</param>
    public ForwarderCore(ForwarderOptions options, Func<double>? jitter = null)
    {
        _o = options.Validate();
        _jitter = jitter ?? Random.Shared.NextDouble;
    }

    public ForwarderOptions Options => _o;

    public long HeldBytes => _heldBytes;

    public int HeldEntries => _entries.Count;

    public int InFlight => _inFlight;

    public bool Stopping => _stopping;

    private TimeSpan Advance(TimeSpan now)
    {
        if (now < _now)
        {
            _clockRegressions++; // the shell's age clock must not go back; hold it where it was
            return _now;
        }
        _now = now;
        return now;
    }

    /// <summary>A tool's request. Accepted means held in memory, best effort (D37).</summary>
    public Admission Offer(TimeSpan now, AccountKey? account, ForwardRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        now = Advance(now);
        Expire(now);
        RefusalReason? r = null;
        if (_stopping) r = RefusalReason.ShuttingDown;
        else if (request.Body.LongLength > _o.MaxEntryBytes) r = RefusalReason.TooLarge;
        else if (account is null) r = RefusalReason.NoAccount;
        else if (_entries.Count >= _o.MaxEntries || _heldBytes + request.Body.LongLength > _o.MaxQueueBytes) r = RefusalReason.Full;
        if (r is { } reason)
        {
            _refused[reason] = _refused.GetValueOrDefault(reason) + 1;
            return Admission.Refused(reason);
        }
        var e = new Entry(_nextId++, request, account!.Value, now);
        _entries.Add(e.Id, e);
        _heldBytes += request.Body.LongLength;
        _accepted++;
        return Admission.Ok(e.Id);
    }

    /// <summary>The oldest ready entry, marked in flight; null if none is ready or MaxInFlight are running.</summary>
    public Attempt? Next(TimeSpan now)
    {
        now = Advance(now);
        Expire(now);
        if (_inFlight >= _o.MaxInFlight) return null;
        foreach (var e in _entries.Values)
        {
            if (e.InFlight || e.NextAt > now) continue;
            e.InFlight = true;
            e.Attempts++;
            _inFlight++;
            _attempts++;
            return new Attempt(e.Id, e.Request, e.Account, e.Attempts, e.ForceRefresh);
        }
        return null;
    }

    /// <summary>When the shell should call <see cref="Next"/> or <see cref="Tick"/> again; null if nothing waits.</summary>
    public TimeSpan? NextWake()
    {
        TimeSpan? w = null;
        foreach (var e in _entries.Values)
        {
            if (e.InFlight) continue;
            var t = e.NextAt < e.EnqueuedAt + _o.MaxAge ? e.NextAt : e.EnqueuedAt + _o.MaxAge;
            if (w is null || t < w) w = t;
        }
        return w;
    }

    /// <summary>Records an attempt's outcome. Each attempt is completed exactly once.</summary>
    public void Complete(TimeSpan now, long id, Outcome outcome)
    {
        now = Advance(now);
        if (!_entries.TryGetValue(id, out var e) || !e.InFlight)
            throw new InvalidOperationException($"attempt {id} is not in flight (completed twice?)");
        e.InFlight = false;
        _inFlight--;
        switch (outcome.Kind)
        {
            case OutcomeKind.Committed:
                Remove(e);
                _committed++;
                if (e.MaybeLanded) _committedAfterUnknown++;
                return;
            case OutcomeKind.Rejected:
                Drop(e, DropReason.Rejected);
                return;
            case OutcomeKind.Forbidden:
                Drop(e, DropReason.Forbidden);
                return;
            case OutcomeKind.AccountChanged:
                Drop(e, DropReason.AccountChanged);
                return;
            case OutcomeKind.Unauthorized:
                // The token was refused before the body was read. Refresh once at
                // once; if a refreshed token is refused too, back off and keep refreshing.
                e.ForceRefresh = true;
                if (!e.RefreshedAfter401 && !_stopping)
                {
                    e.RefreshedAfter401 = true;
                    e.NextAt = now;
                    CheckBounds(e, now);
                    return;
                }
                break;
            case OutcomeKind.Unknown:
                e.MaybeLanded = true;
                _unknown++;
                break;
            case OutcomeKind.RateLimited:
            case OutcomeKind.NotSent:
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(outcome), outcome.Kind, null);
        }
        if (_stopping)
        {
            Drop(e, DropReason.Shutdown);
            return;
        }
        e.NextAt = now + Delay(e.Attempts, outcome.RetryAfter);
        CheckBounds(e, now);
    }

    /// <summary>Drops entries that have aged out (not those in flight: they age out when they complete).</summary>
    public void Tick(TimeSpan now) => Expire(Advance(now));

    /// <summary>Stops accepting, and drops every entry not in flight; in-flight ones are dropped as they complete.</summary>
    public void BeginShutdown(TimeSpan now)
    {
        Advance(now);
        _stopping = true;
        foreach (var e in _entries.Values.Where(e => !e.InFlight).ToList()) Drop(e, DropReason.Shutdown);
    }

    /// <summary>
    /// The shell's drain deadline passed with attempts still running: they are dropped as
    /// shutdown, counted as maybe landed (their outcome will never be known).
    /// </summary>
    public void Abandon(TimeSpan now)
    {
        Advance(now);
        _stopping = true;
        foreach (var e in _entries.Values.ToList())
        {
            if (e.InFlight)
            {
                e.InFlight = false;
                _inFlight--;
                e.MaybeLanded = true;
            }
            Drop(e, DropReason.Shutdown);
        }
    }

    public CountersSnapshot Snapshot()
    {
        var oldest = _entries.Count == 0 ? TimeSpan.Zero : _now - _entries.Values.First().EnqueuedAt;
        return new CountersSnapshot(_accepted, _committed, _committedAfterUnknown,
            new Dictionary<RefusalReason, long>(_refused), new Dictionary<string, long>(_dropped),
            new Dictionary<string, long>(_droppedMaybe), _attempts, _unknown, _entries.Count, _heldBytes, _inFlight,
            oldest, _clockRegressions);
    }

    internal TimeSpan Delay(int attempts, TimeSpan? retryAfter)
    {
        if (retryAfter is { } ra && ra > TimeSpan.Zero)
            return ra < _o.MaxRetryAfter ? ra : _o.MaxRetryAfter;
        // base * 2^(attempts-1), capped, then "equal jitter": half fixed, half random,
        // so retries from many entries spread out and never exceed the cap.
        var exp = Math.Min(Math.Max(attempts - 1, 0), 30);
        var d = Math.Min(_o.BackoffBase.TotalMilliseconds * Math.Pow(2, exp), _o.BackoffCap.TotalMilliseconds);
        var j = Math.Clamp(_jitter(), 0.0, 1.0);
        return TimeSpan.FromMilliseconds(d / 2 + j * d / 2);
    }

    private void CheckBounds(Entry e, TimeSpan now)
    {
        if (e.Attempts >= _o.MaxAttempts) Drop(e, DropReason.AttemptsExhausted);
        else if (e.NextAt >= e.EnqueuedAt + _o.MaxAge || now >= e.EnqueuedAt + _o.MaxAge) Drop(e, DropReason.Expired);
    }

    private void Expire(TimeSpan now)
    {
        List<Entry>? old = null;
        foreach (var e in _entries.Values)
            if (!e.InFlight && now >= e.EnqueuedAt + _o.MaxAge) (old ??= new()).Add(e);
        if (old is null) return;
        foreach (var e in old) Drop(e, DropReason.Expired);
    }

    private void Drop(Entry e, string reason)
    {
        Remove(e);
        var d = e.MaybeLanded ? _droppedMaybe : _dropped;
        d[reason] = d.GetValueOrDefault(reason) + 1;
    }

    private void Remove(Entry e)
    {
        _entries.Remove(e.Id);
        _heldBytes -= e.Request.Body.LongLength;
    }
}
