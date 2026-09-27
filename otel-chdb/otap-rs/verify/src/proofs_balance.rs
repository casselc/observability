//! Load-based balancing (coord.rs, DECISIONS.md D8): the EWMA of a lane's
//! rows/s, the water-level target and the release / take decisions. These
//! only affect liveness and balance, never safety (a take still needs
//! `Observer::may_take`, a release `may_act`), so what matters is that no
//! input makes them NaN, infinite or panic, and that they keep their shape.

use crate::coord::*;

/// Bounds: rows per add ≤ 1e9, clocks ≤ 2^40 ms, windows 1 ms ..= 1 day.
const B: u64 = 1 << 40;

fn any_rows() -> f64 {
    let x: f64 = kani::any();
    kani::assume(x.is_finite() && (0.0..=1e9).contains(&x));
    x
}

/// Kani's `exp` is unconstrained (any f64, NaN and ∞ included: a probe
/// harness asserting exp(x) ∈ [0, 1] for x ∈ [−100, 0] fails), so the EWMA
/// harness stubs it with ANY factor in [0, 1] on (−∞, 0] (libm's exp is one
/// such): finiteness is proved for every decay factor, whatever exp returns
/// there. The stub ignores its argument, which also lets CBMC slice the
/// float division before it away.
///
/// Not here: that the rate only decays between adds. It needs a monotone
/// exp, hence the division; with a monotone stand-in (1 / (1 − x) and two
/// adds, or max(0, 1 + x) and one add) CBMC did not finish in 15 min.
fn exp_any(x: f64) -> f64 {
    let y: f64 = kani::any();
    if x <= 0.0 {
        kani::assume((0.0..=1.0).contains(&y));
    }
    y
}

/// Two adds and two reads, any decay factors: finite and non-negative; a
/// seeded EWMA is finite.
#[kani::proof]
#[kani::stub(f64::exp, exp_any)]
fn ewma_finite() {
    let window: u64 = kani::any();
    kani::assume(1 <= window && window <= 86_400_000);
    let (t0, t1, r1): (u64, u64, u64) = (kani::any(), kani::any(), kani::any());
    kani::assume(t0 <= B && t1 <= B && r1 <= B);
    let mut e = Ewma::default();
    e.add(any_rows(), t0, window);
    e.add(any_rows(), t1, window);
    let a = e.rate(r1, window);
    assert!(a.is_finite() && a >= 0.0);
    let s = Ewma::seeded(any_rows(), t0, window);
    let b = s.rate(r1, window);
    assert!(b.is_finite() && b >= 0.0);
}

// Not here: `load_target` (a sort of f64s) and `pick_release` (a Vec of
// (String, f64, bool)) exceed CBMC's budget (10 min, 6 GB) already at two
// lanes; they are covered by coord.rs's unit tests and the fleet tests.

/// A lane nobody holds is taken by the least loaded worker below target,
/// and by anyone once orphaned.
#[kani::proof]
fn take_by_load_liveness() {
    let (load, w, target) = (any_rows(), any_rows(), any_rows());
    let h: f64 = kani::any();
    kani::assume((0.0..=1.0).contains(&h));
    assert!(take_by_load(load, w, target, h, kani::any(), true));
    if load < target {
        assert!(take_by_load(load, w, target, h, true, false));
    }
}

