using Oscope.Forwarder.Core;

namespace Oscope.Forwarder.Tests;

/// <summary>
/// The reference model of the queue for the stateful tests: the rules of
/// research/entra-ingress.md §4.4 as revised by D37 (in memory only), written plainly
/// (lists and linear scans) and independently of <see cref="ForwarderCore"/>, which must
/// agree with it after every operation.
/// </summary>
internal sealed class QueueModel(ForwarderOptions o, double jitter)
{
    internal sealed class E
    {
        public long Id;
        public long Size;
        public AccountKey Account;
        public TimeSpan Enq, NextAt;
        public int Attempts;
        public bool InFlight, Maybe, Force, Refreshed;
    }

    public readonly List<E> Held = [];
    public readonly Dictionary<RefusalReason, long> Refused = new();
    public readonly Dictionary<string, long> Dropped = new();
    public readonly Dictionary<string, long> DroppedMaybe = new();
    public long Accepted, Committed, CommittedAfterUnknown, Attempts, Unknown;
    public bool Stopping;
    private long _id;

    public long HeldBytes => Held.Sum(e => e.Size);

    public int InFlight => Held.Count(e => e.InFlight);

    private static void Inc<K>(Dictionary<K, long> d, K k) where K : notnull => d[k] = d.GetValueOrDefault(k) + 1;

    private void Drop(E e, string why)
    {
        Held.Remove(e);
        Inc(e.Maybe ? DroppedMaybe : Dropped, why);
    }

    private void Expire(TimeSpan now)
    {
        foreach (var e in Held.Where(e => !e.InFlight && now >= e.Enq + o.MaxAge).ToList()) Drop(e, DropReason.Expired);
    }

    public Admission Offer(TimeSpan now, AccountKey? account, long size)
    {
        Expire(now);
        RefusalReason? r =
            Stopping ? RefusalReason.ShuttingDown :
            size > o.MaxEntryBytes ? RefusalReason.TooLarge :
            account is null ? RefusalReason.NoAccount :
            Held.Count >= o.MaxEntries || HeldBytes + size > o.MaxQueueBytes ? RefusalReason.Full : null;
        if (r is not null)
        {
            Inc(Refused, r.Value);
            return Admission.Refused(r.Value);
        }
        var e = new E { Id = ++_id, Size = size, Account = account!.Value, Enq = now, NextAt = now };
        Held.Add(e);
        Accepted++;
        return Admission.Ok(e.Id);
    }

    public E? Next(TimeSpan now)
    {
        Expire(now);
        if (InFlight >= o.MaxInFlight) return null;
        var e = Held.Where(x => !x.InFlight && x.NextAt <= now).OrderBy(x => x.Id).FirstOrDefault();
        if (e is null) return null;
        e.InFlight = true;
        e.Attempts++;
        Attempts++;
        return e;
    }

    private TimeSpan Delay(int attempts, TimeSpan? ra)
    {
        if (ra is { } x && x > TimeSpan.Zero) return x < o.MaxRetryAfter ? x : o.MaxRetryAfter;
        var d = Math.Min(o.BackoffBase.TotalMilliseconds * Math.Pow(2, Math.Min(Math.Max(attempts - 1, 0), 30)), o.BackoffCap.TotalMilliseconds);
        return TimeSpan.FromMilliseconds(d / 2 + jitter * d / 2);
    }

    private void Bounds(E e, TimeSpan now)
    {
        if (e.Attempts >= o.MaxAttempts) Drop(e, DropReason.AttemptsExhausted);
        else if (e.NextAt >= e.Enq + o.MaxAge || now >= e.Enq + o.MaxAge) Drop(e, DropReason.Expired);
    }

    public void Complete(TimeSpan now, long id, Outcome outcome)
    {
        var e = Held.Single(x => x.Id == id && x.InFlight);
        e.InFlight = false;
        switch (outcome.Kind)
        {
            case OutcomeKind.Committed:
                Held.Remove(e);
                Committed++;
                if (e.Maybe) CommittedAfterUnknown++;
                return;
            case OutcomeKind.Rejected: Drop(e, DropReason.Rejected); return;
            case OutcomeKind.Forbidden: Drop(e, DropReason.Forbidden); return;
            case OutcomeKind.AccountChanged: Drop(e, DropReason.AccountChanged); return;
            case OutcomeKind.Unauthorized:
                e.Force = true;
                if (!e.Refreshed && !Stopping)
                {
                    e.Refreshed = true;
                    e.NextAt = now;
                    Bounds(e, now);
                    return;
                }
                break;
            case OutcomeKind.Unknown:
                e.Maybe = true;
                Unknown++;
                break;
        }
        if (Stopping)
        {
            Drop(e, DropReason.Shutdown);
            return;
        }
        e.NextAt = now + Delay(e.Attempts, outcome.RetryAfter);
        Bounds(e, now);
    }

    public void Tick(TimeSpan now) => Expire(now);

    public void BeginShutdown()
    {
        Stopping = true;
        foreach (var e in Held.Where(e => !e.InFlight).ToList()) Drop(e, DropReason.Shutdown);
    }

    public void Abandon()
    {
        Stopping = true;
        foreach (var e in Held.ToList())
        {
            if (e.InFlight) e.Maybe = true;
            e.InFlight = false;
            Drop(e, DropReason.Shutdown);
        }
    }
}
