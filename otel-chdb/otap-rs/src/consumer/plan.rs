//! What to ingest, sans-IO: the scan of an epoch from its checkpoint, what a
//! slot's metadata says, the check and verify verdicts against central, and
//! grouping objects into statements.

use super::coord::Mutation;
use otap_s3pq::proto;
use std::collections::{BTreeMap, HashMap, HashSet};

/// A slot the LIST returned: (seq, key, size).
pub type Listed = (u64, String, u64);

/// One epoch, scanned from its checkpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EpochScan {
    pub epoch: String,
    /// The checkpoint's slot.
    pub next: u64,
    /// The consecutive slots from `next` that LIST returned.
    pub run: Vec<Listed>,
    /// LIST returned a slot after a missing one: `next + run.len()` is free
    /// (or not listed yet) while a later slot exists. A live writer never
    /// leaves one (it resolves a slot before moving on), so this is either
    /// LIST lag or an anomaly (a deleted slot); the scan stops there and
    /// never tombstones it.
    pub gap_then: Option<u64>,
}

impl EpochScan {
    /// The slot after the run: where a tombstone would go.
    pub fn head(&self) -> u64 {
        self.next + self.run.len() as u64
    }
}

/// `listed`: the keys LIST returned for the epoch after the checkpoint, any order.
pub fn scan_epoch(epoch: &str, next: u64, listed: &[Listed]) -> EpochScan {
    let mut by: BTreeMap<u64, &Listed> = BTreeMap::new();
    for l in listed.iter().filter(|l| l.0 >= next) {
        let _ = by.insert(l.0, l);
    }
    let mut run = Vec::new();
    let mut s = next;
    while let Some(l) = by.get(&s) {
        run.push((*l).clone());
        s += 1;
    }
    let gap_then = by.range(s..).next().map(|(k, _)| *k);
    EpochScan { epoch: epoch.to_string(), next, run, gap_then }
}

/// Whether to race a tombstone into the epoch's free head: the epoch is
/// superseded (not the lane's newest), nothing after the head is listed, and
/// nothing has arrived for `quiet_ms`. The quiet time only affects liveness:
/// a premature tombstone makes a live writer move to a new epoch.
pub fn may_tomb(scan: &EpochScan, newest: bool, quiet_for_ms: u64, quiet_ms: u64) -> bool {
    !newest && scan.gap_then.is_none() && quiet_for_ms >= quiet_ms
}

/// What a slot holds, from its HEAD.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Found {
    /// `low_ns`: the object's `oscope-low` (None: absent, as 0). `late`: the
    /// object is the late part of a split request (`oscope-part: late`,
    /// DECISIONS.md D31); central writes it as `late_part` (D34).
    Data { content: String, rows: u64, received_ns: u64, low_ns: Option<u64>, announce: u64, late: bool },
    /// A heartbeat (`../../FORMAT.md` §2): nothing to ingest, only its low.
    Beat { low_ns: u64 },
    /// Its publisher's orderly close (`../../FORMAT.md` §3.1): nothing to
    /// ingest; its low is R once the lane is retired by it.
    Close { low_ns: u64 },
    Tomb,
}

impl Found {
    /// The slot's custody floor (0 when the object carries none: sound, it
    /// only holds the lane's watermark where it is).
    pub fn low_ns(&self) -> u64 {
        match self {
            Found::Data { low_ns, .. } => low_ns.unwrap_or(0),
            Found::Beat { low_ns } | Found::Close { low_ns } => *low_ns,
            Found::Tomb => 0,
        }
    }
}

/// Whether an object is the late part of a split request (`oscope-part:
/// late`). Anything else (bulk, absent: an unsplit request, or an edge from
/// before D31) is bulk, `late_part` 0: the column only steers partitioning,
/// so a wrong answer costs pruning, never a row.
pub fn is_late_part(meta: &HashMap<String, String>) -> bool {
    meta.get(proto::META_PART).map(String::as_str) == Some(proto::PART_LATE)
}

pub fn found(meta: &HashMap<String, String>) -> Found {
    let low_ns = meta.get(proto::META_LOW).and_then(|r| r.parse().ok());
    match proto::Slot::from_meta(meta) {
        proto::Slot::Tomb => Found::Tomb,
        _ if meta.get(proto::META_KIND).map(String::as_str) == Some(proto::KIND_BEAT) => Found::Beat { low_ns: low_ns.unwrap_or(0) },
        _ if meta.get(proto::META_KIND).map(String::as_str) == Some(proto::KIND_CLOSE) => Found::Close { low_ns: low_ns.unwrap_or(0) },
        _ => Found::Data {
            content: meta.get(proto::META_CONTENT).cloned().unwrap_or_default(),
            rows: meta.get(proto::META_ROWS).and_then(|r| r.parse().ok()).unwrap_or(0),
            received_ns: meta.get(proto::META_RECEIVED).and_then(|r| r.parse().ok()).unwrap_or(0),
            low_ns,
            announce: meta.get(proto::META_ANNOUNCE).and_then(|r| r.parse().ok()).unwrap_or(0),
            late: is_late_part(meta),
        },
    }
}

/// A lane's watermark at a full listing (`../../FORMAT.md` §3,
/// `../../model/completeness.qnt`): `min(M, U)`, where `m` is the highest
/// `oscope-low` the checkpoint had passed before the LIST, and U the lowest
/// `received_at` over the data slots that LIST shows above the checkpoint.
/// `pending` holds, per such slot, what its HEAD found (None: not HEADed:
/// past a gap or the HEAD budget, so no watermark this time). A heartbeat
/// or a close holds no request; a tombstone ends its epoch.
pub fn lane_wm(m: u64, pending: &[Vec<Option<Found>>]) -> Option<u64> {
    let mut u = u64::MAX;
    for epoch in pending {
        for f in epoch {
            match f {
                None => return None,
                Some(Found::Tomb) => break,
                Some(Found::Beat { .. } | Found::Close { .. }) => {}
                Some(Found::Data { received_ns, .. }) => u = u.min(*received_ns),
            }
        }
    }
    Some(m.min(u))
}

/// The checkpoint's next position: past the consecutive done slots of
/// `seqs` (the epoch's run from `next`, in order), stopping at the first
/// that is not done.
pub fn advance_to(next: u64, seqs: &[u64], done: impl Fn(u64) -> bool) -> u64 {
    let mut n = next;
    for &s in seqs {
        if s == n && done(s) {
            n = s + 1;
        } else {
            break;
        }
    }
    n
}

/// A committed object to ingest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Obj {
    pub lane: String,
    pub epoch: String,
    pub seq: u64,
    pub key: String,
    pub size: u64,
    pub content: String,
    pub rows: u64,
    pub received_ns: u64,
    /// When this worker first HEADed the slot (monotonic ms): the linger's clock.
    pub seen_ms: u64,
    /// Resources the object announces (`oscope-announce`; traces and logs):
    /// inserted into `otel_resources` before its rows.
    pub announce: u64,
    /// The late part of a split request (`oscope-part: late`): its rows get
    /// `late_part = 1`, a partition of their own where the table has the
    /// column (DECISIONS.md D34).
    pub late: bool,
}

// ---- the check's partition range ------------------------------------------------------

pub const DAY_NS: u64 = 86_400 * 1_000_000_000;

/// The partitions a count check reads: those whose `toDate(received_at)`
/// lies in [toDate(lo), toDate(hi)] (`_partition_value`, which keeps the
/// by_content projection in use [M]; a predicate on `received_at` itself
/// makes ClickHouse read the table instead).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckRange {
    pub lo_ns: u64,
    pub hi_ns: u64,
}

impl CheckRange {
    pub fn union(self, o: CheckRange) -> CheckRange {
        CheckRange { lo_ns: self.lo_ns.min(o.lo_ns), hi_ns: self.hi_ns.max(o.hi_ns) }
    }
    pub fn widen(self, by_ns: u64) -> CheckRange {
        CheckRange { lo_ns: self.lo_ns.saturating_sub(by_ns), hi_ns: self.hi_ns.saturating_add(by_ns) }
    }
    /// Whether a row received at `ns` is in a partition the range reads
    /// (UTC days here; ClickHouse uses its own time zone for both sides).
    pub fn covers(&self, ns: u64) -> bool {
        (self.lo_ns / DAY_NS..=self.hi_ns / DAY_NS).contains(&(ns / DAY_NS))
    }
}

/// Where the rows of these objects are, from the objects themselves: each
/// object's `received_at` (constant per object: the edge writes the same
/// value into every row and into `x-amz-meta-oscope-received`), and the
/// insert asserts that every row it writes matches it (`throwIf`, see
/// `sql.rs`), so the rows an insert of an object has written are provably in
/// this range. None (read every partition) if any object has no such value,
/// or its assertion once failed (`unranged`).
///
/// The `WallRange` mutant takes "today", by the worker's wall clock, instead.
pub fn own_range(objs: &[&Obj], unranged: &HashSet<String>, mutation: Mutation, wall_ns: u64) -> Option<CheckRange> {
    if mutation == Mutation::WallRange {
        return Some(CheckRange { lo_ns: wall_ns, hi_ns: u64::MAX });
    }
    let mut r: Option<CheckRange> = None;
    for o in objs {
        if o.received_ns == 0 || unranged.contains(&o.content) {
            return None;
        }
        let x = CheckRange { lo_ns: o.received_ns, hi_ns: o.received_ns };
        r = Some(r.map_or(x, |a| a.union(x)));
    }
    r
}

/// The range for a check that must find *any* object with the same content
/// key, not just earlier attempts of these: a copy of a request (resent
/// after a lost ack, into a new epoch) has the same key but a new
/// `received_at`. The objects' own range widened by the copy horizon: a
/// copy received within `horizon` of the original is found. None: no
/// horizon (read everything), or no own range.
pub fn check_range(objs: &[&Obj], unranged: &HashSet<String>, horizon_ns: Option<u64>, mutation: Mutation, wall_ns: u64) -> Option<CheckRange> {
    let h = horizon_ns?;
    let r = own_range(objs, unranged, mutation, wall_ns)?;
    Some(if mutation == Mutation::WallRange { r } else { r.widen(h) })
}

/// Whether a table's pending objects already fill a statement (no linger).
pub fn fills(objs: &[Obj], l: &Limits) -> bool {
    let (bytes, rows) = objs.iter().fold((0u64, 0u64), |(b, r), o| (b + o.size, r + o.rows));
    objs.len() >= l.max_objects || bytes >= l.max_bytes || rows >= l.max_rows || objs.iter().any(|o| o.rows > l.solo_rows || o.size > l.solo_bytes)
}

/// Central's count for a content key against the committed row count.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// Not there: insert it.
    Absent,
    /// All rows are there (ingested, a copy from another epoch, or a retry).
    Present,
    /// Some rows are there: insert the missing row ordinals only.
    Partial(u64),
    /// More rows than committed: a duplicate got in. Reported; not fixable here.
    Over(u64),
}

pub fn verdict(rows: u64, have: u64) -> Verdict {
    match have {
        0 => Verdict::Absent,
        h if h == rows => Verdict::Present,
        h if h < rows => Verdict::Partial(h),
        h => Verdict::Over(h),
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Objects per statement.
    pub max_objects: usize,
    /// Compressed bytes per statement.
    pub max_bytes: u64,
    /// Rows per statement: a squashed statement is one block, which must
    /// stay well below the ~100 MB decoded where ../parquetgo saw 26.10
    /// split a block.
    pub max_rows: u64,
    /// Rows per object that still go into a shared statement. A bigger
    /// object goes alone: ../parquetgo saw ClickHouse 26.10 split a 150k-point
    /// (158 MB decoded) object into two blocks under the single-block settings.
    pub solo_rows: u64,
    /// Compressed bytes per object that still go into a shared statement.
    pub solo_bytes: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Limits { max_objects: 32, max_bytes: 16 << 20, max_rows: 200_000, solo_rows: 100_000, solo_bytes: 8 << 20 }
    }
}

/// Groups objects into statements under the limits. Shared statements keep
/// the objects' order; an object over a solo limit gets a statement of its
/// own at once, ahead of the shared one still being filled (so statements
/// are not in slot order: harmless, as the checkpoint advances over done
/// slots in slot order whatever order their statements ran in;
/// tests/hegel_props.rs `prop_group_partitions_within_limits`).
///
/// Late parts and bulk objects never share a statement (DECISIONS.md D34):
/// under `PARTITION BY (toDate(received_at), late_part)` a mixed statement
/// would write two parts per day, so a squashed statement would no longer
/// land whole or not at all, and the late rows' small parts would multiply.
/// Each kind fills its own statements, in the objects' order.
pub fn group(objs: Vec<Obj>, l: &Limits) -> Vec<Vec<Obj>> {
    group_parts(objs, l, true)
}

/// `group`, with the late parts kept apart or (the `MixLateParts` mutant) not.
pub fn group_parts(objs: Vec<Obj>, l: &Limits, apart: bool) -> Vec<Vec<Obj>> {
    let mut out: Vec<Vec<Obj>> = Vec::new();
    // [bulk, late]: (objects, bytes, rows)
    let mut cur: [(Vec<Obj>, u64, u64); 2] = Default::default();
    for o in objs {
        if o.rows > l.solo_rows || o.size > l.solo_bytes {
            out.push(vec![o]);
            continue;
        }
        let c = &mut cur[(apart && o.late) as usize];
        if !c.0.is_empty() && (c.0.len() >= l.max_objects || c.1 + o.size > l.max_bytes || c.2 + o.rows > l.max_rows) {
            out.push(std::mem::take(&mut c.0));
            (c.1, c.2) = (0, 0);
        }
        c.1 += o.size;
        c.2 += o.rows;
        c.0.push(o);
    }
    for (c, _, _) in cur {
        if !c.is_empty() {
            out.push(c);
        }
    }
    out
}

/// A statement's dedup token: a hash of its ordered key list, so an exact
/// retry of the same statement is dropped by ClickHouse (the backstop; the
/// count check is what guarantees exactly once).
pub fn token(kind: &str, keys: &[&str]) -> String {
    let mut h = blake3::Hasher::new();
    let _ = h.update(kind.as_bytes());
    for k in keys {
        let _ = h.update(b"\0");
        let _ = h.update(k.as_bytes());
    }
    hex::encode(&h.finalize().as_bytes()[..16])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn l(s: u64) -> Listed {
        (s, format!("k{s}"), 10)
    }

    #[test]
    fn scan_runs_and_gaps() {
        let s = scan_epoch("E", 2, &[l(3), l(2), l(1), l(4)]);
        assert_eq!(s.run.iter().map(|x| x.0).collect::<Vec<_>>(), vec![2, 3, 4]);
        assert_eq!((s.head(), s.gap_then), (5, None));
        let g = scan_epoch("E", 2, &[l(2), l(4)]);
        assert_eq!((g.run.len(), g.head(), g.gap_then), (1, 3, Some(4)));
        let empty = scan_epoch("E", 0, &[]);
        assert!(empty.run.is_empty() && empty.gap_then.is_none());
        // a dead head is tombstoned only when superseded, quiet, and nothing after it
        assert!(may_tomb(&empty, false, 5000, 5000));
        assert!(!may_tomb(&empty, true, 1_000_000, 5000));
        assert!(!may_tomb(&empty, false, 4999, 5000));
        assert!(!may_tomb(&scan_epoch("E", 0, &[l(1)]), false, 1_000_000, 0), "never over a gap");
    }

    #[test]
    fn advancing() {
        assert_eq!(advance_to(3, &[3, 4, 5], |s| s != 5), 5);
        assert_eq!(advance_to(3, &[3, 4, 5], |s| s != 3), 3, "never past a slot that isn't done");
        assert_eq!(advance_to(3, &[4, 5], |_| true), 3, "never past a gap");
        assert_eq!(advance_to(0, &[], |_| true), 0);
    }

    #[test]
    fn verdicts() {
        assert_eq!(verdict(10, 0), Verdict::Absent);
        assert_eq!(verdict(10, 10), Verdict::Present);
        assert_eq!(verdict(10, 4), Verdict::Partial(4));
        assert_eq!(verdict(10, 20), Verdict::Over(20));
    }

    fn o(seq: u64, rows: u64, size: u64) -> Obj {
        Obj {
            lane: "l".into(),
            epoch: "E".into(),
            seq,
            key: format!("k{seq}"),
            size,
            content: format!("c{seq}"),
            rows,
            received_ns: 0,
            seen_ms: 0,
            announce: 0,
            late: false,
        }
    }

    #[test]
    fn ranges_come_from_the_objects() {
        let none = HashSet::new();
        let day = |d: u64, h: u64| d * DAY_NS + h * 3_600_000_000_000;
        let mut a = o(0, 1, 1);
        let mut b = o(1, 1, 1);
        // a batch spanning a day boundary: 23:00 on day 100, 01:00 on day 101
        a.received_ns = day(100, 23);
        b.received_ns = day(101, 1);
        let r = own_range(&[&a, &b], &none, Mutation::None, day(300, 0)).unwrap();
        assert!(r.covers(day(100, 0)) && r.covers(day(101, 23)) && !r.covers(day(99, 23)) && !r.covers(day(102, 0)));
        // old timestamps: the range follows the data, not the (much later) wall clock
        assert_eq!(r, CheckRange { lo_ns: day(100, 23), hi_ns: day(101, 1) });
        let c = check_range(&[&a, &b], &none, Some(DAY_NS), Mutation::None, day(300, 0)).unwrap();
        assert!(c.covers(day(99, 0)) && c.covers(day(102, 0)) && !c.covers(day(98, 23)));
        assert_eq!(check_range(&[&a, &b], &none, None, Mutation::None, 0), None, "no horizon: read everything");
        // no metadata, or the object's assertion failed: read everything
        let mut z = o(2, 1, 1);
        assert_eq!(own_range(&[&a, &z], &none, Mutation::None, 0), None);
        z.received_ns = day(5, 0);
        let un: HashSet<String> = [z.content.clone()].into();
        assert_eq!(own_range(&[&z], &un, Mutation::None, 0), None);
        // the mutant: today's partition only
        let w = own_range(&[&a], &none, Mutation::WallRange, day(300, 0)).unwrap();
        assert!(!w.covers(a.received_ns));
    }

    #[test]
    fn a_statement_is_full_at_any_limit() {
        let lim = Limits { max_objects: 3, max_bytes: 100, max_rows: 50, solo_rows: 40, solo_bytes: 60 };
        assert!(!fills(&[o(0, 1, 10), o(1, 1, 10)], &lim));
        assert!(fills(&[o(0, 1, 10), o(1, 1, 10), o(2, 1, 10)], &lim));
        assert!(fills(&[o(0, 30, 10), o(1, 30, 10)], &lim), "rows");
        assert!(fills(&[o(0, 1, 70)], &lim), "a solo object");
    }

    #[test]
    fn grouping() {
        let lim = Limits { max_objects: 3, max_bytes: 100, max_rows: 10_000, solo_rows: 1000, solo_bytes: 60 };
        let g = group(vec![o(0, 1, 10), o(1, 1, 10), o(2, 2000, 10), o(3, 1, 10), o(4, 1, 10), o(5, 1, 50), o(6, 1, 50), o(7, 1, 70)], &lim);
        let seqs: Vec<Vec<u64>> = g.iter().map(|s| s.iter().map(|x| x.seq).collect()).collect();
        // (a solo object is emitted at once; progress is tracked per slot, not by statement order)
        assert_eq!(seqs, vec![vec![2], vec![0, 1, 3], vec![4, 5], vec![7], vec![6]]);
        // late parts fill statements of their own, in order, under the same limits
        let lt = |s, r, z| Obj { late: true, ..o(s, r, z) };
        let g = group(vec![o(0, 1, 10), lt(1, 1, 10), o(2, 1, 10), lt(3, 1, 10), lt(4, 1, 10), lt(5, 1, 10), o(6, 1, 10), lt(7, 2000, 10)], &lim);
        let seqs: Vec<Vec<u64>> = g.iter().map(|s| s.iter().map(|x| x.seq).collect()).collect();
        assert_eq!(seqs, vec![vec![1, 3, 4], vec![7], vec![0, 2, 6], vec![5]]);
        assert!(g.iter().all(|s| s.iter().all(|x| x.late == s[0].late)), "never mixed");
    }

    #[test]
    fn tokens() {
        assert_eq!(token("t", &["a", "b"]), token("t", &["a", "b"]));
        assert_ne!(token("t", &["a", "b"]), token("t", &["b", "a"]));
        assert_ne!(token("t", &["ab"]), token("t", &["a", "b"]));
        let mut m = HashMap::new();
        m.insert(proto::META_KIND.to_string(), proto::KIND_TOMB.to_string());
        assert_eq!(found(&m), Found::Tomb);
        m.insert(proto::META_KIND.to_string(), proto::KIND_DATA.to_string());
        m.insert(proto::META_CONTENT.to_string(), "h".to_string());
        m.insert(proto::META_ROWS.to_string(), "12".to_string());
        assert_eq!(found(&m), Found::Data { content: "h".into(), rows: 12, received_ns: 0, low_ns: None, announce: 0, late: false });
        let _ = m.insert(proto::META_PART.into(), proto::PART_BULK.into());
        assert!(!is_late_part(&m));
        let _ = m.insert(proto::META_PART.into(), proto::PART_LATE.into());
        assert!(is_late_part(&m) && matches!(found(&m), Found::Data { late: true, .. }));
        let _ = m.insert(proto::META_PART.into(), "LATE".into());
        assert!(!is_late_part(&m), "only the exact value: anything else is bulk (pruning only)");
        let _ = m.remove(proto::META_PART);
        let _ = m.insert(proto::META_LOW.into(), "7".into());
        assert_eq!(found(&m).low_ns(), 7);
        let _ = m.insert(proto::META_KIND.into(), proto::KIND_BEAT.into());
        assert_eq!(found(&m), Found::Beat { low_ns: 7 });
        let d = |r| Some(Found::Data { content: String::new(), rows: 1, received_ns: r, low_ns: None, announce: 0, late: false });
        // nothing pending: M; a pending request below M: its received_at; unknown: none
        assert_eq!(lane_wm(50, &[]), Some(50));
        assert_eq!(lane_wm(50, &[vec![Some(Found::Beat { low_ns: 60 }), d(40)], vec![d(45)]]), Some(40));
        assert_eq!(lane_wm(50, &[vec![d(70), Some(Found::Tomb), None]]), Some(50));
        assert_eq!(lane_wm(50, &[vec![d(70), None]]), None);
    }
}
