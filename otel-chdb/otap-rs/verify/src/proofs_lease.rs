//! The lease window (coord.rs, DECISIONS.md D8/D9, model/s3InlineConsumer.qnt
//! `safeUntil`, `mayStart`, `fenceOf`, `takeable`, `landBy`, `settledBy`).
//!
//! Time is modelled as a real-time axis nobody can read (`ts`, `tf`, `r`, …,
//! in ms) and each party's clock as real time plus an arbitrary offset: the
//! holder's monotonic clock read `sent_ms` at `ts`, the observer's read
//! `seen_ms` at `tf`, the holder's wall clock read `sent_wall` at `ts`, and
//! ClickHouse's wall clock is the holder's plus a skew within ±margin (the
//! D9 clock assumption). Clocks run at the same rate (the design's other
//! assumption, rates within margin / ttl, is not modelled).

use crate::coord::*;

/// Bound on clock readings, instants and durations in the window
/// harnesses: 2^40 ms ≈ 34 years (so the harnesses' own sums never
/// overflow). The arithmetic harnesses below are unbounded.
const B: u64 = 1 << 40;

pub(crate) fn held(sent_ms: u64, sent_wall_ms: u64, ttl_ms: u64) -> Held {
    Held {
        doc: LeaseDoc { lane: String::new(), owner: String::from("w"), epoch: 1, beat: 0, ttl_ms, wall_ms: sent_wall_ms },
        etag: String::new(),
        sent_ms,
        sent_wall_ms,
    }
}

pub(crate) fn any_mutation() -> Mutation {
    match kani::any::<u8>() % 7 {
        0 => Mutation::None,
        1 => Mutation::NoTimeBound,
        2 => Mutation::NoVerify,
        3 => Mutation::EarlyCompact,
        4 => Mutation::ReleaseInFlight,
        5 => Mutation::WallRange,
        _ => Mutation::ErrorSettles,
    }
}

fn any_timing(bound: u64, mutation: Mutation) -> Timing {
    let t = Timing { ttl_ms: kani::any(), margin_ms: kani::any(), budget_ms: kani::any(), slack_ms: kani::any(), mutation };
    kani::assume(t.ttl_ms <= bound && t.margin_ms <= bound && t.budget_ms <= bound && t.slack_ms <= bound);
    t
}

/// The premises the window proofs need, weaker than `Timing::check`
/// (`check_implies_the_safety_premises`): the commit slack within the
/// margin (D9), and a statement plus one margin within the TTL (only for
/// windows so early that the wall-clock fence would clip at 0, i.e. never
/// in practice). The ttl/3 renewal clause of `check` is for liveness
/// (`check_leaves_room_after_renewal`), not for safety.
fn safety_premises(t: &Timing) -> bool {
    t.slack_ms <= t.margin_ms && t.budget_ms + t.margin_ms <= t.ttl_ms
}

/// `Timing::check` builds its error with `format!`; the harnesses only look
/// at `is_ok()`, and formatting symbolic integers is expensive for CBMC.
fn no_format(_: std::fmt::Arguments<'_>) -> String {
    String::new()
}

/// One lease version and an observer that may take it over. Real time is
/// measured from `ts`, the instant the holder sent the write that installed
/// the version (only differences of real instants matter).
struct World {
    t: Timing,
    h: Held,
    /// Real ms from the send to the takeover (if `took`).
    take_at: u64,
    took: bool,
}

fn world(t: Timing) -> World {
    let sent_ms: u64 = kani::any(); // the holder's monotonic clock at the send
    let sent_wall: u64 = kani::any(); // the holder's wall clock at the send
    let seen_ms: u64 = kani::any(); // the observer's monotonic clock when it first saw the version
    let lag: u64 = kani::any(); // real ms from the send to that first sight (≥ 0: applied, then listed)
    let waited: u64 = kani::any(); // real ms the observer then waited before deciding
    kani::assume(sent_ms <= B && sent_wall <= B && seen_ms <= B && lag <= B && waited <= B);
    // `Observer::may_take` on an unreleased lease is `expired(unchanged_for)`,
    // unchanged_for = now − first seen, both on the observer's clock.
    let now_o = seen_ms + waited;
    let took = expired(now_o.saturating_sub(seen_ms), t.ttl_ms, t.margin_ms);
    World { h: held(sent_ms, sent_wall, t.ttl_ms), t, take_at: lag + waited, took }
}

/// While the holder has not lapsed (by its own clock) it is more than
/// 2 × margin before any takeover.
#[kani::proof]
fn holder_window_ends_before_takeover() {
    let t = any_timing(B, Mutation::None);
    kani::assume(safety_premises(&t));
    let w = world(t);
    let dr: u64 = kani::any(); // real ms after the send at which the holder reads its clock
    kani::assume(dr <= 2 * B);
    if w.took && !w.h.lapsed(w.h.sent_ms + dr, &w.t) {
        assert!(dr + 2 * w.t.margin_ms < w.take_at);
    }
    kani::cover!(w.took && dr + 2 * w.t.margin_ms + 1 == w.take_at, "tight");
}

/// A statement the holder may start ends (by its budget) 2 × margin before
/// any takeover.
#[kani::proof]
fn started_statement_ends_before_takeover() {
    let t = any_timing(B, Mutation::None);
    kani::assume(safety_premises(&t));
    let w = world(t);
    let dr: u64 = kani::any();
    kani::assume(dr <= 2 * B);
    if w.took && w.h.may_start(w.h.sent_ms + dr, &w.t) {
        assert!(dr + w.t.budget_ms + 2 * w.t.margin_ms <= w.take_at);
    }
    kani::cover!(w.took && w.h.may_start(w.h.sent_ms + dr, &w.t), "a takeover after a statement the holder may start");
}

/// The fence alone (whatever the holder's process did between deciding and
/// sending: a GC pause, SIGSTOP) keeps a statement from landing after a
/// takeover: a statement that reaches the server at any instant, passes the
/// server-side fence on a server clock within ±margin of the holder's wall
/// clock, runs at most `budget` and commits at most `slack` later has landed
/// by the time anyone takes the lane over.
fn fenced_lands_before_takeover(t: Timing) {
    let w = world(t);
    let skew: i64 = kani::any();
    let m = w.t.margin_ms as i64;
    kani::assume(-m <= skew && skew <= m);
    let dr: u64 = kani::any(); // real ms after the send at which the statement reaches the server
    kani::assume(dr <= 2 * B);
    let server_wall = w.h.sent_wall_ms as i64 + dr as i64 + skew;
    let fence = w.h.fence_wall_ms(&w.t);
    kani::assume(fence == u64::MAX || server_wall <= fence as i64); // `WHERE now64(3) <= fence`
    let land = dr + w.t.budget_ms + w.t.slack_ms; // the latest it can land
    if w.took {
        assert!(land <= w.take_at, "a statement landed after the takeover");
    }
    kani::cover!(w.took && land == w.take_at, "the bound is tight");
}

#[kani::proof]
fn fenced_statement_lands_before_takeover() {
    let t = any_timing(B, Mutation::None);
    kani::assume(safety_premises(&t));
    fenced_lands_before_takeover(t);
}

/// A statement whose answer was lost: the worker leaves its lanes alone
/// (`may_act`) until `settled_by`, and by then the statement has landed
/// (a fenced statement lands by send + ttl + slack, real time).
#[kani::proof]
fn lost_statement_settles_before_the_worker_acts() {
    let t = any_timing(B, Mutation::None);
    kani::assume(safety_premises(&t));
    let h = held(kani::any(), kani::any(), t.ttl_ms);
    kani::assume(h.sent_ms <= B);
    let dr: u64 = kani::any();
    kani::assume(dr <= 4 * B);
    if may_act(Some(h.settled_by(&t)), h.sent_ms + dr, &t) {
        assert!(dr > t.ttl_ms + t.slack_ms, "the worker acted while its statement could still land");
    }
}

/// Mutant: no time bound (no fence). Expected to FAIL.
#[cfg(feature = "mutants")]
#[kani::proof]
fn mutant_no_time_bound_lands_after_takeover() {
    let t = any_timing(B, Mutation::NoTimeBound);
    kani::assume(safety_premises(&t));
    fenced_lands_before_takeover(t);
}

/// Mutant: a commit slack above the margin (the model's keeperOverrun,
/// SLACK > MARGIN), everything else as `check` wants. Expected to FAIL.
#[cfg(feature = "mutants")]
#[kani::proof]
fn mutant_slack_above_margin_lands_after_takeover() {
    let t = any_timing(B, Mutation::None);
    kani::assume(t.budget_ms + 2 * t.margin_ms + t.ttl_ms / 3 <= t.ttl_ms);
    fenced_lands_before_takeover(t);
}

/// Mutant: a worker that acts again once its own statement's fence is past
/// (the model's releaseInFlight: it ignores the commit slack). Expected to FAIL.
#[cfg(feature = "mutants")]
#[kani::proof]
fn mutant_release_in_flight_acts_before_landing() {
    let t = any_timing(B, Mutation::ReleaseInFlight);
    kani::assume(safety_premises(&t));
    let h = held(kani::any(), kani::any(), t.ttl_ms);
    kani::assume(h.sent_ms <= B);
    let dr: u64 = kani::any();
    kani::assume(dr <= 4 * B);
    if may_act(Some(h.settled_by(&t)), h.sent_ms + dr, &t) {
        assert!(dr > t.ttl_ms + t.slack_ms);
    }
}

/// `check` implies the premises the window proofs use. All u64 inputs.
#[kani::proof]
#[kani::stub(std::fmt::format, no_format)]
fn check_implies_the_safety_premises() {
    let t = any_timing(u64::MAX, Mutation::None);
    if t.check().is_ok() {
        assert!(t.slack_ms <= t.margin_ms);
        assert!(t.budget_ms as u128 + t.margin_ms as u128 <= t.ttl_ms as u128);
    }
}

/// `Timing::check` accepts exactly: budget + 2·margin + ttl/3 ≤ ttl (in
/// exact arithmetic) and slack ≤ margin; `check_production` additionally
/// margin ≥ MIN_MARGIN_MS unless allowed short. All u64 inputs.
#[kani::proof]
#[kani::stub(std::fmt::format, no_format)]
fn timing_check_is_exact() {
    let t = any_timing(u64::MAX, Mutation::None);
    let need = t.budget_ms as u128 + 2 * t.margin_ms as u128 + (t.ttl_ms / 3) as u128;
    let ok = need <= t.ttl_ms as u128 && t.slack_ms <= t.margin_ms;
    assert_eq!(t.check().is_ok(), ok);
    let allow_short: bool = kani::any();
    assert_eq!(t.check_production(allow_short).is_ok(), ok && (allow_short || t.margin_ms >= MIN_MARGIN_MS));
}

/// What the check's first clause buys: when a renewal falls due (ttl/3 after
/// the send), a statement still fits in the window.
#[kani::proof]
#[kani::stub(std::fmt::format, no_format)]
fn check_leaves_room_after_renewal() {
    let t = any_timing(u64::MAX, Mutation::None);
    kani::assume(t.check().is_ok());
    let sent: u64 = kani::any();
    kani::assume(sent <= u64::MAX - t.ttl_ms);
    let h = held(sent, 0, t.ttl_ms);
    let now = sent + t.ttl_ms / 3;
    assert!(h.renew_due(now));
    assert!(h.may_start(now, &t));
    if t.budget_ms > 0 {
        assert!(!h.lapsed(now, &t));
    }
}

/// No panic or overflow anywhere in the window arithmetic, for every u64
/// input and every mutation, and saturation only ever errs on the safe side:
/// each predicate implies its exact (u128) counterpart.
#[kani::proof]
#[kani::stub(std::fmt::format, no_format)]
fn window_math_total_and_conservative() {
    let t = any_timing(u64::MAX, any_mutation());
    let h = held(kani::any(), kani::any(), kani::any());
    let now: u64 = kani::any();
    let (sent, wall, ttl) = (h.sent_ms as u128, h.sent_wall_ms as u128, h.doc.ttl_ms as u128);
    let (margin, budget, slack) = (t.margin_ms as u128, t.budget_ms as u128, t.slack_ms as u128);
    let bounded = t.mutation != Mutation::NoTimeBound;

    let su = h.safe_until(&t) as u128;
    assert!(su + margin <= sent + ttl || su == 0);
    // (A window that ends before it starts, sent + ttl < margin, clips to 0:
    // a statement of budget 0 at instant 0 may start. `check` rules it out.)
    if h.may_start(now, &t) && bounded && sent + ttl >= margin {
        assert!(now as u128 + budget + margin <= sent + ttl);
    }
    if !h.lapsed(now, &t) && bounded {
        assert!((now as u128) + margin < sent + ttl);
    }
    let later: u64 = kani::any();
    if h.lapsed(now, &t) && later >= now {
        assert!(h.lapsed(later, &t), "lapsed is monotone");
    }
    let f = h.fence_wall_ms(&t);
    if bounded && f != 0 {
        assert!(f as u128 + margin + budget <= wall + ttl);
    }
    let _ = h.renew_due(now);
    let s = h.settled_by(&t) as u128;
    assert!(s >= (sent + ttl + slack).min(u64::MAX as u128), "never settled early");
    let u: Option<u64> = kani::any();
    let _ = may_act(u, now, &t);
    let d: u64 = kani::any();
    if expired(d, h.doc.ttl_ms, t.margin_ms) {
        assert!(d as u128 >= ttl + margin, "never expired early");
    }
    let _ = t.check();
    let _ = t.check_production(kani::any());
}

/// Fair share: at least one, and the least number that covers every lane
/// (ceil). Up to 1024 lanes and 1024 workers (a symbolic × symbolic product
/// is what CBMC finds hard; the arithmetic in `fair_share` is `div_ceil`).
#[kani::proof]
fn fair_share_covers_every_lane() {
    let (lanes, workers): (usize, usize) = (kani::any(), kani::any());
    kani::assume(lanes <= 1024 && workers <= 1024);
    let f = fair_share(lanes, workers);
    let w = workers.max(1) as u32;
    assert!(f >= 1 && f <= 1024);
    assert!(f as u32 * w >= lanes as u32);
    if lanes >= 1 {
        assert!((f as u32 - 1) * w < lanes as u32, "the least that covers");
    }
}
