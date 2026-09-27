//! The check's partition range and the scan (plan.rs, DECISIONS.md D11,
//! model/s3InlineConsumer.qnt `rangeLo`/`rangeHi`, mutant `wallRange`).
//!
//! `unranged` is always the empty set here: a non-empty one only turns a
//! range into `None` (read every partition), which is safe by construction,
//! and hashing a `String` is out of reach for CBMC (std's SipHash loop does
//! not unwind); `own_range`'s membership test on an empty set never hashes.

use crate::coord::Mutation;
use crate::plan::*;
use std::collections::HashSet;
use std::hash::RandomState;

/// An empty `unranged` set. `HashSet::new()` seeds its hasher from the OS
/// (`getrandom`), a syscall Kani does not model; an empty set never hashes,
/// so the keys are irrelevant and a fixed pair (RandomState is two u64 keys)
/// stands in. Harness-only.
fn no_unranged() -> HashSet<String> {
    // SAFETY: RandomState is two u64 SipHash keys (size checked by transmute);
    // any value is a valid key pair, and an empty set never uses them.
    let rs: RandomState = unsafe { std::mem::transmute::<[u64; 2], RandomState>([0, 0]) };
    HashSet::with_hasher(rs)
}

fn obj(seq: u64, received_ns: u64) -> Obj {
    Obj {
        lane: String::new(),
        epoch: String::new(),
        seq,
        key: String::new(),
        size: 0,
        content: String::new(),
        rows: 0,
        received_ns,
        seen_ms: 0,
    }
}

fn any_range() -> CheckRange {
    let r = CheckRange { lo_ns: kani::any(), hi_ns: kani::any() };
    kani::assume(r.lo_ns <= r.hi_ns);
    r
}

/// The objects (up to `M`) of a statement: any received time, any count.
/// (Loops over them need `#[kani::unwind(M + 1)]`.)
fn any_objs<const M: usize>() -> ([Obj; M], usize) {
    let n: usize = kani::any();
    kani::assume(n <= M);
    (std::array::from_fn(|i| obj(i as u64, kani::any())), n)
}

/// `own_range` contains every object's received instant, and is exactly
/// [min, max]; no objects or one without a received time: None (read
/// everything). Any u64 received times; up to two objects (at three this
/// harness needs just under 6 GB, over it on some runs; containment at three
/// follows from `check_range_holds_every_copy_within_horizon` with a zero
/// horizon, where the copy is the object itself).
fn own_range_holds_every_received_day(mutation: Mutation) {
    let (objs, n) = any_objs::<2>();
    let refs: Vec<&Obj> = objs[..n].iter().collect();
    let wall: u64 = kani::any();
    let r = own_range(&refs, &no_unranged(), mutation, wall);
    if n == 0 || objs[..n].iter().any(|o| o.received_ns == 0) {
        if mutation != Mutation::WallRange {
            assert!(r.is_none());
        }
        return;
    }
    let r = r.expect("objects with received times have a range");
    for o in &objs[..n] {
        // In [lo, hi], hence in a partition the check reads (`interval_implies_covers`).
        assert!(r.lo_ns <= o.received_ns && o.received_ns <= r.hi_ns, "an object's partition is outside the check's range");
    }
    if mutation == Mutation::None {
        assert!(objs[..n].iter().any(|o| o.received_ns == r.lo_ns) && objs[..n].iter().any(|o| o.received_ns == r.hi_ns));
    }
}

#[kani::proof]
#[kani::unwind(3)]
fn own_range_holds_every_received_day_proof() {
    own_range_holds_every_received_day(Mutation::None);
}

/// Mutant `WallRange`: the range from the worker's clock ("today") misses an
/// object received on an earlier day (an old batch, a lagging lane).
/// Expected to FAIL.
#[cfg(feature = "mutants")]
#[kani::proof]
#[kani::unwind(3)]
fn mutant_wall_range_misses_a_received_day() {
    own_range_holds_every_received_day(Mutation::WallRange);
}

/// `check_range` (own range widened by the horizon) covers every instant a
/// copy of any of the objects can have within the horizon, on either side;
/// no horizon: None.
#[kani::proof]
#[kani::unwind(4)]
fn check_range_holds_every_copy_within_horizon() {
    let (objs, n) = any_objs::<3>();
    let refs: Vec<&Obj> = objs[..n].iter().collect();
    let h: Option<u64> = kani::any();
    let r = check_range(&refs, &no_unranged(), h, Mutation::None, kani::any());
    let Some(h) = h else {
        assert!(r.is_none());
        return;
    };
    if n == 0 || objs[..n].iter().any(|o| o.received_ns == 0) {
        assert!(r.is_none());
        return;
    }
    let r = r.expect("a range");
    let i: usize = kani::any();
    kani::assume(i < n);
    let copy: u64 = kani::any();
    kani::assume(copy.abs_diff(objs[i].received_ns) <= h);
    assert!(r.lo_ns <= copy && copy <= r.hi_ns, "a copy within the horizon is in a partition the check does not read");
}

/// `union` is the interval hull: commutative, associative, idempotent, and
/// contains both operands. Any u64. (Partitions follow from intervals:
/// `interval_implies_covers`.)
#[kani::proof]
fn union_is_the_interval_hull() {
    let (a, b, c) = (any_range(), any_range(), any_range());
    assert_eq!(a.union(b), b.union(a));
    assert_eq!(a.union(b).union(c), a.union(b.union(c)));
    assert_eq!(a.union(a), a);
    let u = a.union(b);
    assert!(u.lo_ns <= u.hi_ns);
    let x: u64 = kani::any();
    if (a.lo_ns <= x && x <= a.hi_ns) || (b.lo_ns <= x && x <= b.hi_ns) {
        assert!(u.lo_ns <= x && x <= u.hi_ns);
    }
}

/// `widen` saturates at the u64 edges, is monotone in its argument and in
/// the range, never narrows, and composes: widen(a).widen(b) = widen(a + b).
#[kani::proof]
fn widen_is_monotone_and_saturating() {
    let r = any_range();
    let (a, b): (u64, u64) = (kani::any(), kani::any());
    let wa = r.widen(a);
    assert!(wa.lo_ns <= r.lo_ns && r.hi_ns <= wa.hi_ns, "never narrows");
    if a <= b {
        let wb = r.widen(b);
        assert!(wb.lo_ns <= wa.lo_ns && wa.hi_ns <= wb.hi_ns, "monotone");
    }
    assert_eq!(wa.widen(b), r.widen(a.saturating_add(b)));
    assert_eq!(wa.lo_ns, (r.lo_ns as u128).saturating_sub(a as u128) as u64);
    assert_eq!(wa.hi_ns as u128, (r.hi_ns as u128 + a as u128).min(u64::MAX as u128));
    let s = any_range();
    if s.lo_ns >= r.lo_ns && s.hi_ns <= r.hi_ns {
        let ws = s.widen(a);
        assert!(wa.lo_ns <= ws.lo_ns && ws.hi_ns <= wa.hi_ns, "monotone in the range");
    }
}

/// Every instant in [lo, hi] is in a partition the range reads. This is
/// what lets the other harnesses reason about intervals only (the divisions
/// by DAY_NS are what CBMC finds expensive). Any u64.
///
/// It also gives monotonicity in the range, so `widen` and `union` never
/// drop a partition: `covers` is day(lo) ≤ day(x) ≤ day(hi), and for
/// r ⊇ s this harness on [r.lo, s.lo] ∋ s.lo and [s.hi, r.hi] ∋ s.hi gives
/// day(r.lo) ≤ day(s.lo) and day(s.hi) ≤ day(r.hi). (A harness stating that
/// directly, four divisions, did not finish in 15 min.)
#[kani::proof]
#[kani::solver(kissat)]
fn interval_implies_covers() {
    let r = any_range();
    let x: u64 = kani::any();
    kani::assume(r.lo_ns <= x && x <= r.hi_ns);
    assert!(r.covers(x));
}

/// The checkpoint only moves past consecutive, done slots: every slot in
/// [next, result) is in the run in order and done, and the one at result
/// (if the run goes on) is not both. Up to 4 slots, seq < 8.
#[kani::proof]
#[kani::unwind(6)]
fn advance_to_stops_at_the_first_undone_or_gap() {
    let n: usize = kani::any();
    kani::assume(n <= 4);
    let seqs: [u64; 4] = kani::any();
    kani::assume(seqs.iter().all(|s| *s < 8));
    let mask: u8 = kani::any();
    let done = |s: u64| s < 8 && (mask >> s) & 1 == 1;
    let next: u64 = kani::any();
    kani::assume(next < 8);
    let r = advance_to(next, &seqs[..n], done);
    assert!(next <= r && r - next <= n as u64);
    for k in next..r {
        assert!(seqs[(k - next) as usize] == k && done(k));
    }
    let i = (r - next) as usize;
    if i < n {
        assert!(!(seqs[i] == r && done(r)));
    }
}

/// The verdict is a total classification of central's count.
#[kani::proof]
fn verdict_is_exact() {
    let (rows, have): (u64, u64) = (kani::any(), kani::any());
    match verdict(rows, have) {
        Verdict::Absent => assert!(have == 0),
        Verdict::Present => assert!(have == rows && have > 0),
        Verdict::Partial(h) => assert!(h == have && 0 < h && h < rows),
        Verdict::Over(h) => assert!(h == have && h > rows && h > 0),
    }
}

// Not here: that `covers`'s UTC day, `x / DAY_NS`, is ClickHouse's
// partition key `toDate(fromUnixTimestamp64Nano(x))`, which truncates to
// seconds first (⌊⌊x / a⌋ / b⌋ = ⌊x / ab⌋ for positive integers): two
// symbolic 64-bit divisions did not finish in 20 min, and a harness adding
// fixed-offset time zones in i128 not in 15.
// Nor `scan_epoch` (std's `BTreeMap`), which exceeds CBMC's 6 GB budget
// already at one listed slot (two: over 10 min); `advance_to` above is the
// checkpoint's decision, and plan.rs's unit tests cover the scan.
// Nor `group` / `fills` (a Vec<Vec<Obj>> of four-String objects), which
// exceed it already at two objects; plan.rs's unit tests cover them.
