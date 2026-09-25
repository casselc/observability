//! What to ingest, sans-IO: the scan of an epoch from its checkpoint, what a
//! slot's metadata says, the check and verify verdicts against central, and
//! grouping objects into statements.

use otap_s3pq::proto;
use std::collections::{BTreeMap, HashMap};

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
    Data { content: String, rows: u64, received_ns: u64 },
    Tomb,
}

pub fn found(meta: &HashMap<String, String>) -> Found {
    match proto::Slot::from_meta(meta) {
        proto::Slot::Tomb => Found::Tomb,
        _ => Found::Data {
            content: meta.get(proto::META_CONTENT).cloned().unwrap_or_default(),
            rows: meta.get(proto::META_ROWS).and_then(|r| r.parse().ok()).unwrap_or(0),
            received_ns: meta.get(proto::META_RECEIVED).and_then(|r| r.parse().ok()).unwrap_or(0),
        },
    }
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

/// Groups objects (in order) into statements under the limits.
pub fn group(objs: Vec<Obj>, l: &Limits) -> Vec<Vec<Obj>> {
    let mut out: Vec<Vec<Obj>> = Vec::new();
    let mut cur: Vec<Obj> = Vec::new();
    let (mut bytes, mut rows) = (0, 0);
    for o in objs {
        if o.rows > l.solo_rows || o.size > l.solo_bytes {
            out.push(vec![o]);
            continue;
        }
        if !cur.is_empty() && (cur.len() >= l.max_objects || bytes + o.size > l.max_bytes || rows + o.rows > l.max_rows) {
            out.push(std::mem::take(&mut cur));
            (bytes, rows) = (0, 0);
        }
        bytes += o.size;
        rows += o.rows;
        cur.push(o);
    }
    if !cur.is_empty() {
        out.push(cur);
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
        }
    }

    #[test]
    fn grouping() {
        let lim = Limits { max_objects: 3, max_bytes: 100, max_rows: 10_000, solo_rows: 1000, solo_bytes: 60 };
        let g = group(vec![o(0, 1, 10), o(1, 1, 10), o(2, 2000, 10), o(3, 1, 10), o(4, 1, 10), o(5, 1, 50), o(6, 1, 50), o(7, 1, 70)], &lim);
        let seqs: Vec<Vec<u64>> = g.iter().map(|s| s.iter().map(|x| x.seq).collect()).collect();
        // (a solo object is emitted at once; progress is tracked per slot, not by statement order)
        assert_eq!(seqs, vec![vec![2], vec![0, 1, 3], vec![4, 5], vec![7], vec![6]]);
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
        assert_eq!(found(&m), Found::Data { content: "h".into(), rows: 12, received_ns: 0 });
    }
}
