using Oscope.Forwarder.Core;
using Xunit;

namespace Oscope.Forwarder.Tests;

/// <summary>The core's rules, one story each, named for the hazard or CAST lesson it carries.</summary>
public class CoreTests
{
    private static readonly AccountKey Alice = CoreModelTests.Alice;
    private static readonly AccountKey Bob = CoreModelTests.Bob;

    private static ForwardRequest Req(int n, byte fill = 1) =>
        new("/v1/traces", "application/x-protobuf", null, Enumerable.Repeat(fill, n).ToArray());

    private static ForwarderCore Core(ForwarderOptions? o = null) => new(o ?? CoreModelTests.Small, () => 0.5);

    private static TimeSpan Ms(double ms) => TimeSpan.FromMilliseconds(ms);

    [Fact]
    public void A_lost_answer_is_unknown_not_failed_and_the_retry_sends_the_same_bytes()
    {
        OscopeTrace.Covers("SM", "CAST-50 H-E6 LS-E3 R-E6");
        var c = Core();
        var body = Req(100);
        Assert.True(c.Offer(Ms(0), Alice, body).Accepted);
        var a1 = c.Next(Ms(0))!;
        c.Complete(Ms(10), a1.Id, Outcome.Unknown()); // the ingress may have committed it
        Assert.Equal(0, c.Snapshot().DroppedTotal);
        Assert.Equal(1, c.Snapshot().HeldEntries);
        var a2 = c.Next(Ms(10_000))!;
        Assert.Same(a1.Request.Body, a2.Request.Body);
        Assert.Equal(2, a2.Number);
        c.Complete(Ms(10_010), a2.Id, Outcome.Committed);
        var s = c.Snapshot();
        Assert.Equal(1, s.Committed);
        Assert.Equal(1, s.CommittedAfterUnknown); // the ingress may hold two copies; the consumer skips the second (D11)
        Assert.True(s.Balances);
    }

    [Fact]
    public void A_drop_after_an_unknown_outcome_is_counted_as_maybe_landed()
    {
        OscopeTrace.Covers("SM", "H-E4 CAST-50");
        var c = Core();
        c.Offer(Ms(0), Alice, Req(10));
        var now = Ms(0);
        for (var i = 0; i < CoreModelTests.Small.MaxAttempts; i++)
        {
            var a = c.Next(now);
            if (a is null) break;
            c.Complete(now, a.Id, i == 0 ? Outcome.Unknown() : new Outcome(OutcomeKind.NotSent));
            now += Ms(3000);
        }
        var s = c.Snapshot();
        Assert.Equal(0, s.HeldEntries);
        Assert.Empty(s.Dropped);
        Assert.Equal(1, s.DroppedMaybeLanded.Values.Sum());
        Assert.True(s.Balances);
    }

    [Fact]
    public void A_401_refreshes_the_token_once_at_once_then_backs_off()
    {
        OscopeTrace.Covers("SM", "R-E6 H-E9 UCA-E8");
        var c = Core();
        c.Offer(Ms(0), Alice, Req(10));
        var a1 = c.Next(Ms(0))!;
        Assert.False(a1.ForceRefresh);
        c.Complete(Ms(5), a1.Id, new Outcome(OutcomeKind.Unauthorized));
        var a2 = c.Next(Ms(5))!; // at once, with a forced refresh (a token expired mid-flight)
        Assert.True(a2.ForceRefresh);
        c.Complete(Ms(6), a2.Id, new Outcome(OutcomeKind.Unauthorized));
        Assert.Null(c.Next(Ms(6))); // a refreshed token refused too: not a hot loop
        var a3 = c.Next(Ms(2_000))!;
        Assert.True(a3.ForceRefresh);
        c.Complete(Ms(2_001), a3.Id, Outcome.Committed);
        Assert.True(c.Snapshot().Balances);
    }

    [Fact]
    public void An_entry_is_never_sent_under_another_account()
    {
        OscopeTrace.Covers("SM", "H-E1 R-E6 LS-E5 UCA-E6");
        var c = Core();
        c.Offer(Ms(0), Alice, Req(10));
        var a = c.Next(Ms(0))!;
        Assert.Equal(Alice, a.Account);
        // The shell got a token for Bob (the broker switched accounts): the entry is dropped, counted.
        c.Complete(Ms(1), a.Id, new Outcome(OutcomeKind.AccountChanged));
        var s = c.Snapshot();
        Assert.Equal(1, s.Dropped[DropReason.AccountChanged]);
        Assert.Null(c.Next(Ms(100_000)));
        // Bob's own requests go under Bob.
        c.Offer(Ms(2), Bob, Req(10));
        Assert.Equal(Bob, c.Next(Ms(2))!.Account);
    }

    [Fact]
    public void A_full_queue_refuses_rather_than_buffering_beyond_its_bound()
    {
        OscopeTrace.Covers("SM", "R-E7 H-E4 H-E9 CAST-38");
        var c = Core();
        Assert.True(c.Offer(Ms(0), Alice, Req(400)).Accepted);
        Assert.True(c.Offer(Ms(0), Alice, Req(400)).Accepted);
        var refused = c.Offer(Ms(0), Alice, Req(400)); // 1200 > 1000
        Assert.False(refused.Accepted);
        Assert.Equal(RefusalReason.Full, refused.Reason);
        Assert.Equal(RefusalReason.TooLarge, c.Offer(Ms(0), Alice, Req(401)).Reason);
        Assert.Equal(RefusalReason.NoAccount, c.Offer(Ms(0), null, Req(1)).Reason);
        Assert.True(c.Offer(Ms(0), Alice, Req(200)).Accepted); // what still fits
        var s = c.Snapshot();
        Assert.Equal(1000, s.HeldBytes);
        Assert.Equal(3, s.Accepted);
        Assert.Equal(3, s.Refused.Values.Sum());
    }

    [Fact]
    public void Every_retry_loop_is_bounded_by_attempts_and_by_age()
    {
        OscopeTrace.Covers("ML", "CAST-39 H-E4");
        var c = Core();
        c.Offer(Ms(0), Alice, Req(10));
        var now = TimeSpan.Zero;
        var attempts = 0;
        for (var i = 0; i < 1000 && c.HeldEntries > 0; i++)
        {
            var a = c.Next(now);
            if (a is not null)
            {
                attempts++;
                c.Complete(now, a.Id, new Outcome(OutcomeKind.RateLimited, TimeSpan.FromHours(1)));
            }
            now += Ms(500);
            c.Tick(now);
        }
        Assert.Equal(0, c.HeldEntries);
        Assert.True(attempts <= CoreModelTests.Small.MaxAttempts);
        Assert.True(now <= CoreModelTests.Small.MaxAge + Ms(1000), $"held for {now}");
        Assert.Equal(1, c.Snapshot().Dropped.Values.Sum());
    }

    [Fact]
    public void Retry_after_is_honoured_and_capped_and_backoff_stays_under_the_cap()
    {
        OscopeTrace.Covers("P", "CAST-39 CAST-5");
        var c = Core();
        Assert.Equal(TimeSpan.FromSeconds(2), c.Delay(1, TimeSpan.FromSeconds(2)));
        Assert.Equal(CoreModelTests.Small.MaxRetryAfter, c.Delay(1, TimeSpan.FromDays(1)));
        for (var n = 1; n < 100; n++)
        {
            var d = c.Delay(n, null);
            Assert.True(d > TimeSpan.Zero && d <= CoreModelTests.Small.BackoffCap, $"attempt {n}: {d}");
        }
    }

    [Fact]
    public void A_clock_that_goes_back_neither_reorders_nor_un_ages_entries()
    {
        OscopeTrace.Covers("P2C", "CAST-26 CAST-34");
        var c = Core();
        c.Offer(Ms(5000), Alice, Req(10));
        var a = c.Next(Ms(1000))!; // the shell's clock went back: held where it was
        c.Complete(Ms(0), a.Id, Outcome.Unknown());
        Assert.Equal(2, c.Snapshot().ClockRegressions);
        c.Tick(Ms(5000) + CoreModelTests.Small.MaxAge);
        Assert.Equal(0, c.HeldEntries); // aged by the clock as it last stood, not rewound
    }

    [Fact]
    public void An_age_clock_only_advances_and_counts_wall_time_slept()
    {
        OscopeTrace.Covers("P2C", "CAST-26 CAST-34 CAST-44");
        var tp = new ManualTime();
        var clock = new AgeClock(tp);
        tp.Mono += TimeSpan.FromSeconds(10);
        tp.Wall += TimeSpan.FromSeconds(10);
        Assert.Equal(TimeSpan.FromSeconds(10), clock.Now());
        tp.Wall -= TimeSpan.FromHours(1); // the clock is set back
        Assert.Equal(TimeSpan.FromSeconds(10), clock.Now());
        tp.Wall += TimeSpan.FromHours(1) + TimeSpan.FromHours(8); // a night asleep: the monotonic clock stood still (macOS)
        Assert.Equal(TimeSpan.FromHours(8) + TimeSpan.FromSeconds(10), clock.Now());
    }

    [Fact]
    public void Options_that_cannot_work_together_are_refused()
    {
        OscopeTrace.Covers("P", "CAST-25 CAST-5 H-E6");
        Assert.Throws<ArgumentException>(() => new ForwarderOptions { MaxEntryBytes = 2, MaxQueueBytes = 1 }.Validate());
        Assert.Throws<ArgumentException>(() => new ForwarderOptions { MaxAge = TimeSpan.FromSeconds(1) }.Validate());
        Assert.Throws<ArgumentException>(() => new ForwarderOptions { MaxAttempts = 1 }.Validate());
        Assert.Throws<ArgumentException>(() => new ForwarderOptions { MaxInFlight = 0 }.Validate());
        Assert.Throws<ArgumentException>(() => new ForwarderOptions { MaxAge = TimeSpan.FromDays(3) }.Validate()); // past D11's copy horizon
        new ForwarderOptions().Validate();
    }

    [Theory]
    [InlineData(200, OutcomeKind.Committed)]
    [InlineData(400, OutcomeKind.Rejected)]
    [InlineData(401, OutcomeKind.Unauthorized)]
    [InlineData(403, OutcomeKind.Forbidden)]
    [InlineData(413, OutcomeKind.Rejected)]
    [InlineData(429, OutcomeKind.RateLimited)]
    [InlineData(500, OutcomeKind.Unknown)]
    [InlineData(502, OutcomeKind.Unknown)]
    [InlineData(503, OutcomeKind.Unknown)]
    [InlineData(504, OutcomeKind.Unknown)]
    public void Statuses_are_classified_as_the_ingress_means_them(int status, OutcomeKind kind)
    {
        OscopeTrace.Covers("P", "R-E5 CAST-2 CAST-15");
        Assert.Equal(kind, Outcome.FromStatus(status, null).Kind);
    }

    /// <summary>A TimeProvider whose monotonic and wall clocks the test moves apart.</summary>
    private sealed class ManualTime : TimeProvider
    {
        public TimeSpan Mono;
        public DateTimeOffset Wall = new(2026, 10, 1, 0, 0, 0, TimeSpan.Zero);

        public override DateTimeOffset GetUtcNow() => Wall;

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override long GetTimestamp() => Mono.Ticks;
    }
}
