namespace Oscope.Forwarder.Core;

/// <summary>
/// The core's notion of time: how long the forwarder has run, as the larger of the
/// monotonic elapsed time and the wall-clock elapsed time, and never less than it last
/// said. Monotonic time alone stops while a Mac sleeps (mach_absolute_time), so a held
/// entry would not age across a night's sleep; wall time alone goes back when the clock
/// is set back. The maximum of the two only advances: a clock set forward ages entries
/// early (a counted drop, safe); a clock set back changes nothing (CAST rows 26, 34).
/// Nothing from this clock is ever written into a request's bytes.
/// </summary>
public sealed class AgeClock
{
    private readonly TimeProvider _tp;
    private readonly long _startTs;
    private readonly DateTimeOffset _startWall;
    private readonly Lock _gate = new();
    private TimeSpan _last;

    public AgeClock(TimeProvider tp)
    {
        _tp = tp;
        _startTs = tp.GetTimestamp();
        _startWall = tp.GetUtcNow();
    }

    public TimeSpan Now()
    {
        var mono = _tp.GetElapsedTime(_startTs);
        var wall = _tp.GetUtcNow() - _startWall;
        var t = wall > mono ? wall : mono;
        lock (_gate)
        {
            if (t < _last) t = _last;
            _last = t;
            return t;
        }
    }
}
