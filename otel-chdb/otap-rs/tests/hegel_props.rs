//! Property tests with Hegel (HEGEL.md): the consumer's pure decisions
//! (check ranges, the epoch scan, grouping, the checkpoint's advance) at
//! sizes beyond Kani's bounds (VERIFY.md), the SQL the consumer splices
//! strings into (CAST #7), value rendering, and the exporter's rows: content
//! keys that do not depend on encoder state or process (CAST #10), and the
//! Parquet round trip with hostile data, statistics truncation included
//! (CAST #9).
//!
//!   cargo test --release --test hegel_props -- prop_
//!   HEGEL_TEST_CASES=5000 cargo test --release --test hegel_props -- prop_
//!   HEGEL_CH=1 cargo test --release --test hegel_props -- prop_sql_on_clickhouse   # + a local ClickHouse
//!
//! Every property draws from Hegel's generators, so a failure is shrunk to a
//! minimal input and printed with a `#[hegel::reproduce_failure]` line.

#[path = "../src/consumer/mod.rs"]
#[allow(dead_code, unused_imports, clippy::all)]
mod consumer;

use consumer::bucket::MemBucket;
use consumer::coord::Mutation;
use consumer::plan::{self, CheckRange, DAY_NS, Limits, Listed, Obj};
use consumer::sql::{ClickHouseCentral, Fence, LaneKind};
use hegel::TestCase;
use hegel::generators as gs;
use std::collections::{BTreeSet, HashSet};
use std::rc::Rc;

// ---- the check range, beyond Kani's three objects ----------------------------------------

fn obj(seq: u64, rows: u64, size: u64, received_ns: u64) -> Obj {
    Obj {
        lane: "p/logs".into(),
        epoch: "E".into(),
        seq,
        key: format!("r/p/logs/E/{seq:020}.parquet"),
        size,
        content: format!("c{seq}"),
        rows,
        received_ns,
        seen_ms: 0,
        announce: 0,
        payloads: Default::default(),
        late: false,
    }
}

/// Received times that cluster (same day, day edges, u64 edges) as well as spread.
fn draw_received(tc: &TestCase) -> u64 {
    match tc.draw(gs::integers::<u8>().max_value(4)) {
        0 => 0,
        1 => tc.draw(gs::integers::<u64>()),
        2 => u64::MAX - tc.draw(gs::integers::<u64>().max_value(3 * DAY_NS)),
        3 => tc.draw(gs::integers::<u64>().min_value(20_000).max_value(20_010)) * DAY_NS + tc.draw(gs::integers::<u64>().max_value(DAY_NS - 1)),
        _ => tc.draw(gs::integers::<u64>().max_value(3 * DAY_NS)),
    }
}

/// `own_range` is None exactly when some object has no received time or is
/// unranged (or there is none), and otherwise is exactly [min, max] of the
/// received times: every object's partition is read. Up to 64 objects
/// (Kani: 2, and 3 with a zero horizon).
#[hegel::test]
fn prop_own_range_is_the_hull(tc: TestCase) {
    let n = tc.draw(gs::integers::<usize>().max_value(64));
    let objs: Vec<Obj> = (0..n).map(|i| obj(i as u64, 1, 1, draw_received(&tc))).collect();
    let unranged: HashSet<String> = if tc.draw(gs::booleans()) && n > 0 {
        let i = tc.draw(gs::integers::<usize>().max_value(n - 1));
        HashSet::from([objs[i].content.clone()])
    } else {
        HashSet::new()
    };
    let refs: Vec<&Obj> = objs.iter().collect();
    let r = plan::own_range(&refs, &unranged, Mutation::None, 0);
    let unknowable = objs.is_empty() || objs.iter().any(|o| o.received_ns == 0 || unranged.contains(&o.content));
    if unknowable {
        assert_eq!(r, None, "must read every partition");
        return;
    }
    let r = r.expect("a range");
    let lo = objs.iter().map(|o| o.received_ns).min().unwrap();
    let hi = objs.iter().map(|o| o.received_ns).max().unwrap();
    assert_eq!(r, CheckRange { lo_ns: lo, hi_ns: hi });
    for o in &objs {
        assert!(r.covers(o.received_ns), "object {} at {} outside {r:?}", o.seq, o.received_ns);
    }
}

/// `check_range` holds every instant a copy of any object can have: its
/// received time ± up to the horizon (saturating at the u64 edges).
#[hegel::test]
fn prop_check_range_holds_every_copy(tc: TestCase) {
    let n = tc.draw(gs::integers::<usize>().min_value(1).max_value(48));
    let objs: Vec<Obj> = (0..n).map(|i| obj(i as u64, 1, 1, draw_received(&tc).max(1))).collect();
    let refs: Vec<&Obj> = objs.iter().collect();
    let horizon = match tc.draw(gs::integers::<u8>().max_value(3)) {
        0 => None,
        1 => Some(0),
        2 => Some(DAY_NS),
        _ => Some(tc.draw(gs::integers::<u64>())),
    };
    let r = plan::check_range(&refs, &HashSet::new(), horizon, Mutation::None, 0);
    let Some(h) = horizon else {
        assert_eq!(r, None, "no horizon: read everything");
        return;
    };
    let r = r.expect("a range");
    for _ in 0..8 {
        let o = &objs[tc.draw(gs::integers::<usize>().max_value(n - 1))];
        let d = tc.draw(gs::integers::<u64>().max_value(h));
        let copy = if tc.draw(gs::booleans()) { o.received_ns.saturating_add(d) } else { o.received_ns.saturating_sub(d) };
        assert!(r.covers(copy), "a copy of {} received at {copy} (Δ {d}) is outside {r:?}", o.received_ns);
    }
}

// ---- the epoch scan and the checkpoint's advance ------------------------------------------

/// `scan_epoch` for any listing (Kani: out of memory at one slot): the run
/// is exactly the consecutive listed slots from `next`, the gap is the first
/// listed slot after the run, and the head is free.
#[hegel::test]
fn prop_scan_epoch_is_the_consecutive_run(tc: TestCase) {
    let next = tc.draw(gs::integers::<u64>().max_value(40));
    let seqs: BTreeSet<u64> = tc.draw(gs::btree_sets(gs::integers::<u64>().max_value(80)).max_size(60));
    let mut listed: Vec<Listed> = seqs.iter().map(|&s| (s, format!("k{s}"), s * 7)).collect();
    // LIST order doesn't matter, and a repeat of a slot (two pages) neither.
    let perm = tc.draw(gs::permutations(listed.clone()));
    listed = perm;
    if tc.draw(gs::booleans()) && !listed.is_empty() {
        let dup = listed[0].clone();
        listed.push(dup);
    }
    let sc = plan::scan_epoch("E", next, &listed);
    let mut want = Vec::new();
    let mut s = next;
    while seqs.contains(&s) {
        want.push(s);
        s += 1;
    }
    assert_eq!(sc.run.iter().map(|l| l.0).collect::<Vec<_>>(), want);
    assert!(sc.run.iter().all(|l| l.1 == format!("k{}", l.0) && l.2 == l.0 * 7), "each slot keeps its key and size");
    assert_eq!(sc.head(), s);
    assert!(!seqs.contains(&sc.head()), "the head is free");
    assert_eq!(sc.gap_then, seqs.range(s..).next().copied());
    // A tombstone only over a free head with nothing after it.
    if plan::may_tomb(&sc, false, u64::MAX, 0) {
        assert!(seqs.range(sc.head()..).next().is_none());
    }
}

/// `advance_to` moves over consecutive done slots only (Kani: 4 slots).
#[hegel::test]
fn prop_advance_to_stops_at_the_first_undone(tc: TestCase) {
    let next = tc.draw(gs::integers::<u64>().max_value(20));
    let run_len = tc.draw(gs::integers::<u64>().max_value(200));
    let mut seqs: Vec<u64> = (next..next + run_len).collect();
    if tc.draw(gs::booleans()) && !seqs.is_empty() {
        let i = tc.draw(gs::integers::<usize>().max_value(seqs.len() - 1));
        let _ = seqs.remove(i); // a gap
    }
    let done: BTreeSet<u64> = tc.draw(gs::btree_sets(gs::integers::<u64>().max_value(next + run_len)).max_size(250));
    let all_done = tc.draw(gs::booleans());
    let is_done = |s: u64| all_done || done.contains(&s);
    let n = plan::advance_to(next, &seqs, is_done);
    assert!(n >= next);
    for s in next..n {
        assert!(seqs.contains(&s) && is_done(s), "moved past slot {s}, which is missing or not done");
    }
    // It stopped at the first slot it may not pass.
    let stop = (next..).find(|s| !(seqs.contains(s) && is_done(*s))).unwrap();
    assert_eq!(n, stop);
}

// ---- dead-lane retirement (FORMAT.md §3.1, DECISIONS.md D35) ------------------------------

/// `CkptDoc::close_proof` against its definition: a proof exactly when the
/// newest epoch ends with a passed close and nothing listed after it, and
/// every other epoch is tombstoned or ends likewise; R is the highest of
/// those closes' lows. Never a proof without a close (only the
/// `RetireStale` mutant makes one).
#[hegel::test]
fn prop_close_proof_needs_every_epoch_sealed(tc: TestCase) {
    let _trace = otap_s3pq::oscope_trace::covers("P", &["CAST-47", "H-4"]);
    use consumer::coord::{CkptDoc, CloseProof, epoch_key};
    let n = tc.draw(gs::integers::<usize>().min_value(1).max_value(5));
    let mut c = CkptDoc::new("l");
    let mut listed = std::collections::BTreeMap::new();
    for i in 0..n {
        let e = format!("E{i:02}");
        let next = tc.draw(gs::integers::<u64>().max_value(6));
        c.advance(&e, next);
        match tc.draw(gs::integers::<u8>().max_value(2)) {
            0 => {}
            1 => c.close(&e, next),
            _ => c.epochs.get_mut(&e).unwrap().close_low = tc.draw(gs::integers::<u64>().min_value(1).max_value(1_000)),
        }
        if tc.draw(gs::booleans()) {
            let _ = listed.insert(e.clone(), tc.draw(gs::integers::<u64>().max_value(8)));
        }
    }
    let pending = |e: &str| listed.get(e).is_some_and(|m| *m >= c.next(e));
    let newest = c.epochs.keys().max_by_key(|e| epoch_key(e)).unwrap().clone();
    let close = |e: &str| c.epochs[e].close_low;
    let others_sealed = c.epochs.keys().filter(|e| **e != newest).all(|e| c.closed(e) || (close(e) > 0 && !pending(e)));
    let want = (close(&newest) > 0 && !pending(&newest) && others_sealed).then(|| CloseProof {
        epoch: newest.clone(),
        r_ns: c.epochs.iter().filter(|(e, p)| **e == newest || !p.closed).map(|(_, p)| p.close_low).max().unwrap(),
    });
    assert_eq!(c.close_proof(&listed, Mutation::None, 5_000), want);
    if let Some(p) = c.close_proof(&listed, Mutation::None, 5_000) {
        assert!(p.r_ns > 0 && p.r_ns <= 1_000, "R is a close's low");
    }
}

/// A retired lane is +inf in every minimum: the fleet's, a cluster's and a
/// signal's values equal those computed without it, and it is never
/// holding or stale.
#[hegel::test]
fn prop_a_retired_lane_is_absent_from_every_minimum(tc: TestCase) {
    use consumer::watermark::{ClusterWmDoc, RETIRED, WmConfig, WmDoc, compute_cluster, compute_doc};
    use std::collections::BTreeMap;
    let cfg = WmConfig { skew_ms: 5_000, stale_ms: 60_000, ..WmConfig::new("r", "c") };
    let wall = 1_000_000u64;
    let n = tc.draw(gs::integers::<usize>().min_value(1).max_value(12));
    let mut all = BTreeMap::new();
    let mut live = BTreeMap::new();
    let mut retired = BTreeMap::new();
    for i in 0..n {
        let id = format!("c{}/p{i}/{}", tc.draw(gs::integers::<u8>().max_value(2)), ["logs", "traces"][usize::from(tc.draw(gs::booleans()))]);
        if tc.draw(gs::booleans()) {
            let _ = all.insert(id.clone(), RETIRED);
            let _ = retired.insert(id, tc.draw(gs::integers::<u64>().max_value(wall * 1_000_000)));
        } else {
            let w = tc.draw(gs::integers::<u64>().max_value(wall * 1_000_000));
            let _ = all.insert(id.clone(), w);
            let _ = live.insert(id, w);
        }
    }
    let prev = WmDoc::default();
    let (a, b) = (compute_doc(&prev, &all, wall, &cfg), compute_doc(&prev, &live, wall, &cfg));
    assert_eq!((a.computed_ns, a.complete_through_ns), (b.computed_ns, b.complete_through_ns));
    for (cl, v) in &b.clusters {
        assert_eq!(a.clusters[cl], *v);
    }
    for (s, v) in &a.signals {
        assert_eq!(*v, b.signals.get(s).copied().unwrap_or(a.unlisted_signals_ns), "signal {s}");
    }
    assert!(a.holding.iter().chain(a.stale.iter()).all(|l| !retired.contains_key(&l.lane)));
    assert_eq!(a.retired_lanes, retired.len());
    for cl in a.clusters.keys() {
        let mine = |m: &BTreeMap<String, u64>| m.iter().filter(|(k, _)| k.starts_with(&format!("{cl}/"))).map(|(k, v)| (k.clone(), *v)).collect::<BTreeMap<_, _>>();
        let d = compute_cluster(&ClusterWmDoc::default(), cl, &mine(&all), &retired, a.clusters[cl], wall, &cfg);
        assert_eq!(d.complete_through_ns, a.clusters[cl]);
        assert_eq!(d.retired.len(), mine(&retired).len());
        assert!(d.lane_wm.values().all(|v| *v != RETIRED && *v >= d.complete_through_ns));
    }
}

// ---- grouping objects into statements -----------------------------------------------------

/// `group` (Kani: out of memory at two objects) keeps every object once, in
/// order; a multi-object statement stays within every limit; an object over
/// a solo limit goes alone; a statement never mixes late parts and bulk
/// objects (DECISIONS.md D34); and a statement is only cut where the next
/// object of its kind would break a limit (or goes alone).
#[hegel::test]
fn prop_group_partitions_within_limits(tc: TestCase) {
    let l = Limits {
        max_objects: tc.draw(gs::integers::<usize>().min_value(1).max_value(40)),
        max_bytes: tc.draw(gs::integers::<u64>().min_value(1).max_value(1 << 20)),
        max_rows: tc.draw(gs::integers::<u64>().min_value(1).max_value(100_000)),
        solo_rows: tc.draw(gs::integers::<u64>().max_value(100_000)),
        solo_bytes: tc.draw(gs::integers::<u64>().max_value(1 << 20)),
    };
    let n = tc.draw(gs::integers::<usize>().max_value(200));
    let late_share = tc.draw(gs::sampled_from(vec![0u8, 1, 50, 100]));
    let objs: Vec<Obj> = (0..n)
        .map(|i| {
            let mut o = obj(i as u64, tc.draw(gs::integers::<u64>().max_value(120_000)), tc.draw(gs::integers::<u64>().max_value(1 << 20)), 0);
            o.late = tc.draw(gs::integers::<u8>().max_value(99)) < late_share;
            o
        })
        .collect();
    let groups = plan::group(objs.clone(), &l);
    for (gi, g) in groups.iter().enumerate() {
        assert!(g.iter().all(|o| o.late == g[0].late), "statement {gi} mixes late parts and bulk objects");
    }
    let solo = |o: &Obj| o.rows > l.solo_rows || o.size > l.solo_bytes;
    let mut flat: Vec<u64> = groups.iter().flatten().map(|o| o.seq).collect();
    // A solo object's statement is emitted at once, ahead of the shared
    // statement still being filled (Hegel's first finding here: `group`'s
    // doc said "in order"). Harmless: the checkpoint advances over done
    // slots in slot order, whatever order their statements ran in.
    // Likewise late parts and bulk objects fill separate statements (D34):
    // each kind keeps its order.
    for late in [false, true] {
        let shared: Vec<u64> = flat.iter().copied().filter(|s| !solo(&objs[*s as usize]) && objs[*s as usize].late == late).collect();
        assert!(shared.windows(2).all(|w| w[0] < w[1]), "shared objects (late {late}) keep their order: {shared:?}");
    }
    flat.sort_unstable();
    assert_eq!(flat, (0..n as u64).collect::<Vec<_>>(), "every object exactly once");
    for (gi, g) in groups.iter().enumerate() {
        assert!(!g.is_empty(), "no empty statement");
        if g.len() > 1 {
            assert!(g.iter().all(|o| !solo(o)), "a solo object shares statement {gi}");
            assert!(g.len() <= l.max_objects, "statement {gi}: {} objects > {}", g.len(), l.max_objects);
            // The first object is always admitted; the limits bind from the second on.
            let bytes: u64 = g.iter().map(|o| o.size).sum();
            let rows: u64 = g.iter().map(|o| o.rows).sum();
            assert!(bytes <= l.max_bytes.max(g[0].size) && rows <= l.max_rows.max(g[0].rows), "statement {gi}: {bytes} B / {rows} rows over the limits");
        }
        // Maximal: the next shared statement of the same kind starts with an
        // object that would have broken a limit.
        let next_same = groups[gi + 1..].iter().find(|n| n.first().is_some_and(|f| f.late == g[0].late && !solo(f)));
        if let Some(next) = next_same.and_then(|n| n.first()) {
            if !solo(next) && !g.iter().any(solo) {
                let bytes: u64 = g.iter().map(|o| o.size).sum::<u64>() + next.size;
                let rows: u64 = g.iter().map(|o| o.rows).sum::<u64>() + next.rows;
                assert!(g.len() >= l.max_objects || bytes > l.max_bytes || rows > l.max_rows, "statement {gi} was cut early");
            }
        }
    }
    assert_eq!(plan::fills(&objs, &l), objs.len() >= l.max_objects
        || objs.iter().map(|o| o.size).sum::<u64>() >= l.max_bytes
        || objs.iter().map(|o| o.rows).sum::<u64>() >= l.max_rows
        || objs.iter().any(solo));
}

// ---- the partition keys the check's range understands (D34) --------------------------------

/// `range_partition_key` accepts a key exactly when the check's
/// `_partition_value.1 BETWEEN toDate(lo) AND toDate(hi)` reads the right
/// partitions under it: a tuple (or a lone expression) whose FIRST element is
/// `toDate(received_at)` and whose other elements are object-constant
/// (`late_part`), however ClickHouse spaces it. A key accepted wrongly is the
/// silent-loss hazard of a wrong partition parse (the check reads other
/// days, or `late_part` values as days); one refused wrongly only costs a
/// full read. The model here is independent of the implementation: a tiny
/// parser of the tuple, not the list of strings the code compares with.
#[hegel::test]
fn prop_range_partition_key_is_exact(tc: TestCase) {
    let atoms = vec![
        "toDate(received_at)", "late_part", "toDate(Timestamp)", "toYYYYMM(received_at)", "received_at", "toStartOfHour(received_at)",
        "ServiceName", "toDate(received_at, 'UTC')", "toUInt8(late_part)", "toDate(receivedat)",
    ];
    let k = tc.draw(gs::integers::<usize>().min_value(1).max_value(3));
    let elems: Vec<&str> = (0..k).map(|_| tc.draw(gs::sampled_from(atoms.clone()))).collect();
    let tuple = k > 1;
    let sp = |tc: &TestCase| -> String { tc.draw(gs::sampled_from(vec!["", " ", "  ", "\t", "\n"])).to_string() };
    let mut key = String::new();
    if tuple {
        key.push('(');
    }
    for (i, e) in elems.iter().enumerate() {
        if i > 0 {
            key.push(',');
        }
        key.push_str(&sp(&tc));
        // whitespace inside the call's parentheses too
        key.push_str(&e.replace('(', &format!("({}", sp(&tc))));
        key.push_str(&sp(&tc));
    }
    if tuple {
        key.push(')');
    }
    tc.note(&format!("key {key:?}"));
    // Safe: the check reads the right partitions under the key.
    let safe = elems[0] == "toDate(received_at)" && elems[1..].iter().all(|e| *e == "late_part");
    // Shipped: the two keys the consumer's DDL uses, as ClickHouse prints them.
    let shipped = elems == ["toDate(received_at)"] || elems == ["toDate(received_at)", "late_part"];
    let accepted = consumer::sql::range_partition_key(&key);
    assert!(!accepted || safe, "{key:?} accepted, but the check would read the wrong partitions under it");
    assert!(!shipped || accepted, "{key:?} is a shipped key and was refused (every check would read every partition)");
}

/// A statement's dedup token names its ordered key list: equal lists, equal
/// tokens; different lists (keys without NUL, which S3 keys here never
/// hold), different tokens.
#[hegel::test]
fn prop_token_names_the_key_list(tc: TestCase) {
    let key = || gs::text().max_size(12).exclude_characters("\0");
    let a: Vec<String> = tc.draw(gs::vecs(key()).max_size(6));
    let b: Vec<String> = if tc.draw(gs::booleans()) { a.clone() } else { tc.draw(gs::vecs(key()).max_size(6)) };
    let kind = tc.draw(gs::sampled_from(vec!["logs", "traces"]));
    let ta = plan::token(kind, &a.iter().map(String::as_str).collect::<Vec<_>>());
    let tb = plan::token(kind, &b.iter().map(String::as_str).collect::<Vec<_>>());
    assert_eq!(ta == tb, a == b, "{a:?} vs {b:?}");
}

/// Slot keys round-trip through `parse_slot_key` for any epoch name without
/// a '/' and any sequence number, and sort by sequence (LIST order).
#[hegel::test]
fn prop_slot_keys_round_trip_and_sort(tc: TestCase) {
    let prefix = tc.draw(gs::text().min_size(1).max_size(20).exclude_characters("/"));
    let prefix = format!("r/{prefix}/logs");
    let epoch = tc.draw(gs::text().min_size(1).max_size(20).exclude_characters("/"));
    let (a, b) = (tc.draw(gs::integers::<u64>()), tc.draw(gs::integers::<u64>()));
    let (ka, kb) = (otap_s3pq::proto::slot_key(&prefix, &epoch, a), otap_s3pq::proto::slot_key(&prefix, &epoch, b));
    assert_eq!(otap_s3pq::proto::parse_slot_key(&prefix, &ka), Some((epoch.clone(), a)));
    assert_eq!(ka.cmp(&kb), a.cmp(&b), "{ka} vs {kb}");
}

// ---- SQL: every spliced string stays one literal (CAST #7) -------------------------------

/// ClickHouse's single-quoted string literals of `sql`, decoded (`\x` → x,
/// `''` → '), or an error if one does not close. Outside literals it skips
/// back-quoted identifiers.
fn literals(sql: &str) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    let mut it = sql.chars().peekable();
    while let Some(c) = it.next() {
        match c {
            '\'' => {
                let mut s = String::new();
                loop {
                    match it.next() {
                        None => return Err(format!("unclosed literal after {:?}", out.last())),
                        Some('\\') => match it.next() {
                            Some(x) => s.push(x),
                            None => return Err("dangling backslash".into()),
                        },
                        Some('\'') if it.peek() == Some(&'\'') => {
                            let _ = it.next();
                            s.push('\'');
                        }
                        Some('\'') => break,
                        Some(x) => s.push(x),
                    }
                }
                out.push(s);
            }
            '`' => {
                for x in it.by_ref() {
                    if x == '`' {
                        break;
                    }
                }
            }
            _ => {}
        }
    }
    Ok(out)
}

/// Strings as hostile as S3 keys and content keys can be: quotes,
/// backslashes, braces and commas, control characters, any code point.
fn hostile_text(tc: &TestCase, max: usize) -> String {
    if tc.draw(gs::booleans()) {
        tc.draw(gs::text().max_size(max))
    } else {
        tc.draw(gs::text().alphabet("'\\\"`{},*?%/ \n\t\0aé😀").max_size(max))
    }
}

/// Every insert, repair-shaped and count statement the consumer builds keeps
/// each spliced value (object path, content key, credentials, structure) as
/// exactly one literal, whatever it contains.
#[hegel::test]
fn prop_statements_keep_every_value_one_literal(tc: TestCase) {
    let _trace = otap_s3pq::oscope_trace::covers("PH", &["CAST-7", "H-1", "SEC-7"]);
    let n = tc.draw(gs::integers::<usize>().min_value(1).max_value(5));
    let objs: Vec<Obj> = (0..n)
        .map(|i| {
            let mut o = obj(i as u64, 3, 10, if tc.draw(gs::booleans()) { 0 } else { 1 + i as u64 });
            o.key = format!("r/{}/logs/E/{:020}.parquet", hostile_text(&tc, 12), i);
            o.content = hostile_text(&tc, 16);
            o
        })
        .collect();
    let key = hostile_text(&tc, 8);
    let secret = hostile_text(&tc, 8);
    let c = ClickHouseCentral::new("http://x", "db", Rc::new(MemBucket::default()), &key, &secret, 1000);
    let k = LaneKind::for_signal(tc.draw(gs::sampled_from(vec!["logs", "traces", "metrics_gauge"]))).unwrap();
    // A table with `late_part` (D34), objects late or not: a mixed statement
    // splices each path once more, into the late_part transform.
    // (metrics tables never have the column: their objects are not split)
    let late_col = tc.draw(gs::booleans()) && k.signal != "metrics_gauge";
    c.set_late_part(&k, late_col);
    let objs: Vec<Obj> = objs.into_iter().map(|mut o| {
        o.late = tc.draw(gs::booleans());
        o
    }).collect();
    let refs: Vec<&Obj> = objs.iter().collect();
    let f = Fence { wall_ms: tc.draw(gs::integers::<u64>()), budget_ms: 3000 };
    let sql = c.insert_sql(&k, &refs, f, tc.draw(gs::booleans()));
    tc.note(&sql);
    let lits = literals(&sql).unwrap_or_else(|e| panic!("{e}: {sql}"));
    let has = |v: &str| lits.iter().any(|l| l == v);
    assert!(has(&key) && has(&secret), "credentials not intact");
    if n == 1 {
        assert!(has(&format!("mem://{}", objs[0].key)), "the object URL is one literal");
    }
    for o in &objs {
        assert!(has(&o.content), "content key {:?} not one literal", o.content);
        if n > 1 {
            assert!(has(&format!("mem/{}", o.key)), "path {:?} not one literal", o.key);
        }
    }
    // Nothing outside the literals changed: with every literal blanked, the
    // statement is the same as for plain values.
    let plain: Vec<Obj> = objs.iter().enumerate().map(|(i, o)| {
        let mut p = o.clone();
        p.key = format!("r/x/logs/E/{i:020}.parquet");
        p.content = "c".into();
        p
    }).collect();
    let pc = ClickHouseCentral::new("http://x", "db", Rc::new(MemBucket::default()), "k", "s", 1000);
    pc.set_late_part(&k, late_col);
    let prefs: Vec<&Obj> = plain.iter().collect();
    let writes = late_col;
    assert_eq!(sql.contains(", late_part) SELECT "), writes, "late_part only where the table has it: {sql}");
    if writes {
        let mixed = objs.iter().any(|o| o.late != objs[0].late);
        assert_eq!(sql.contains("transform(_path, ["), n > 1, "{sql}");
        assert!(mixed || sql.contains(&format!(", toUInt8({}) FROM ", objs[0].late as u8)), "an agreeing statement writes a constant: {sql}");
    }
    let guard = sql.contains("throwIf");
    let psql = pc.insert_sql(&k, &prefs, f, guard);
    assert_eq!(blank(&sql), blank(&psql));
}

/// The statement with every literal replaced by `''`.
fn blank(sql: &str) -> String {
    let mut out = String::new();
    let mut it = sql.chars().peekable();
    while let Some(c) = it.next() {
        if c != '\'' {
            out.push(c);
            continue;
        }
        loop {
            match it.next() {
                None => break,
                Some('\\') => {
                    let _ = it.next();
                }
                Some('\'') if it.peek() == Some(&'\'') => {
                    let _ = it.next();
                }
                Some('\'') => break,
                Some(_) => {}
            }
        }
        out.push_str("''");
    }
    out
}

/// `sq` on a real server: `SELECT hex(sq(s))` is `hex(s)` for any string,
/// and the consumer's statements with hostile values parse (`EXPLAIN AST`).
/// Opt-in (`HEGEL_CH=1`; `OTAPRS_CH`, default http://127.0.0.1:18123).
#[hegel::test]
fn prop_sql_on_clickhouse(tc: TestCase) {
    let _trace = otap_s3pq::oscope_trace::covers("PH", &["CAST-7", "H-1", "SEC-7"]);
    if std::env::var_os("HEGEL_CH").is_none() {
        return otap_s3pq::oscope_trace::skipped();
    }
    static RT: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    let rt = RT.get_or_init(|| tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap());
    let url = std::env::var("OTAPRS_CH").unwrap_or_else(|_| "http://127.0.0.1:18123".into());
    let ch = otap_s3pq::central::ClickHouse::new(&url);
    let s = hostile_text(&tc, 64);
    let got = rt.block_on(ch.query(&format!("SELECT hex({}) FORMAT TSV", otap_s3pq::central::sq(&s)), &[])).expect("query");
    assert_eq!(got.trim_end_matches('\n'), hex::encode_upper(s.as_bytes()), "{s:?}");
    let mut o = obj(0, 3, 10, 5);
    o.key = format!("r/{}/logs/E/0.parquet", hostile_text(&tc, 12));
    o.content = s;
    let mut z = obj(1, 3, 10, 6);
    z.content = hostile_text(&tc, 12);
    let c = ClickHouseCentral::new(&url, "db", Rc::new(MemBucket::default()), "k", "s", 1000);
    let k = LaneKind::for_signal("logs").unwrap();
    for sql in [c.insert_sql(&k, &[&o, &z], Fence { wall_ms: 1, budget_ms: 1 }, true), c.insert_sql(&k, &[&o], Fence { wall_ms: 1, budget_ms: 1 }, false)] {
        let r = rt.block_on(ch.query(&format!("EXPLAIN AST {sql}"), &[]));
        assert!(r.is_ok(), "{sql}\n{r:?}");
    }
}

// ---- value rendering ------------------------------------------------------------------------

/// A finite double's rendering (Go's float64AsString) parses back to the
/// same double, and has Go's shape: fixed notation for 1e-6 ≤ |f| < 1e21 and
/// 0, else an exponent with a sign and no leading zero.
#[hegel::test]
fn prop_double_rendering_round_trips(tc: TestCase) {
    let f: f64 = tc.draw(gs::floats::<f64>());
    let mut b = Vec::new();
    otap_s3pq::render::double(&mut b, f);
    let s = String::from_utf8(b).unwrap();
    if f.is_nan() {
        assert_eq!(s, "NaN");
        return;
    }
    if f.is_infinite() {
        assert_eq!(s, if f > 0.0 { "Infinity" } else { "-Infinity" });
        return;
    }
    let back: f64 = s.parse().unwrap_or_else(|e| panic!("{s:?}: {e}"));
    assert_eq!(back.to_bits(), f.to_bits(), "{f:e} rendered {s:?}");
    let a = f.abs();
    if a == 0.0 || (1e-6..1e21).contains(&a) {
        assert!(!s.contains('e'), "{s}");
    } else {
        let (_, exp) = s.split_once('e').expect("exponent");
        assert!(exp.starts_with('+') || exp.starts_with('-'), "{s}");
        assert!(!exp[1..].starts_with('0'), "{s}");
    }
}

// ---- OTLP requests, hostile ---------------------------------------------------------------

use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList, any_value::Value};
use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{LogRecord, LogsData, ResourceLogs, ScopeLogs};
use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
use otel_arrow_dfe_pdata::proto::opentelemetry::trace::v1::{ResourceSpans, ScopeSpans, Span, Status, TracesData, span};
use prost::Message;

/// A string: short and ordinary, empty, hostile characters, or huge (up to
/// ~1 MiB, a drawn pattern repeated).
fn string(tc: &TestCase, huge: bool) -> String {
    match tc.draw(gs::integers::<u8>().max_value(if huge { 9 } else { 8 })) {
        0 => String::new(),
        1..=5 => tc.draw(gs::text().max_size(10)),
        6..=8 => hostile_text(tc, 24),
        _ => {
            let unit = tc.draw(gs::text().min_size(1).max_size(8));
            let times = tc.draw(gs::integers::<usize>().min_value(1).max_value((1 << 20) / unit.len().max(1)));
            unit.repeat(times)
        }
    }
}

fn any_value(tc: &TestCase, depth: u32, huge: bool) -> AnyValue {
    let max = if depth == 0 { 4 } else { 6 };
    let v = match tc.draw(gs::integers::<u8>().max_value(max + 1)) {
        0 => None,
        1 => Some(Value::StringValue(string(tc, huge))),
        2 => Some(Value::IntValue(tc.draw(gs::integers::<i64>()))),
        3 => Some(Value::DoubleValue(tc.draw(gs::floats::<f64>()))),
        4 => Some(Value::BoolValue(tc.draw(gs::booleans()))),
        5 => Some(Value::BytesValue(tc.draw(gs::binary().max_size(40)))),
        6 => {
            let n = tc.draw(gs::integers::<usize>().max_value(4));
            Some(Value::ArrayValue(ArrayValue { values: (0..n).map(|_| any_value(tc, depth - 1, false)).collect() }))
        }
        _ => {
            let n = tc.draw(gs::integers::<usize>().max_value(4));
            Some(Value::KvlistValue(KeyValueList { values: (0..n).map(|_| kv(tc, depth - 1, false)).collect() }))
        }
    };
    AnyValue { value: v }
}

fn kv(tc: &TestCase, depth: u32, huge: bool) -> KeyValue {
    // Keys repeat now and then (duplicate keys are legal in OTLP).
    let key = if tc.draw(gs::booleans()) { tc.draw(gs::sampled_from(vec!["k", "service.name", "a.b", "", "k8s.pod.name", "k8s.pod.label.a"])).to_string() } else { string(tc, false) };
    let value = if tc.draw(gs::integers::<u8>().max_value(9)) == 0 { None } else { Some(any_value(tc, depth, huge)) };
    KeyValue { key, value }
}

fn attrs(tc: &TestCase, max: usize, huge: bool) -> Vec<KeyValue> {
    let n = tc.draw(gs::integers::<usize>().max_value(max));
    (0..n).map(|_| kv(tc, 3, huge)).collect()
}

fn id(tc: &TestCase, len: usize) -> Vec<u8> {
    match tc.draw(gs::integers::<u8>().max_value(5)) {
        0 => Vec::new(),
        1 => vec![0; len],
        _ => tc.draw(gs::binary().min_size(len).max_size(len)),
    }
}

fn time(tc: &TestCase) -> u64 {
    match tc.draw(gs::integers::<u8>().max_value(3)) {
        0 => 0,
        1 => tc.draw(gs::integers::<u64>()),
        _ => 1_790_000_000_000_000_000 + tc.draw(gs::integers::<u64>().max_value(1 << 40)),
    }
}

fn resource(tc: &TestCase, huge: bool) -> Option<Resource> {
    tc.draw(gs::booleans()).then(|| Resource { attributes: attrs(tc, 4, huge), dropped_attributes_count: 0, entity_refs: Vec::new() })
}

fn scope(tc: &TestCase) -> Option<InstrumentationScope> {
    tc.draw(gs::booleans()).then(|| InstrumentationScope { name: string(tc, false), version: string(tc, false), attributes: attrs(tc, 2, false), dropped_attributes_count: 0 })
}

fn traces(tc: &TestCase, huge: bool) -> TracesData {
    let nr = tc.draw(gs::integers::<usize>().max_value(2));
    TracesData {
        resource_spans: (0..nr)
            .map(|_| ResourceSpans {
                resource: resource(tc, huge),
                schema_url: string(tc, false),
                scope_spans: (0..tc.draw(gs::integers::<usize>().max_value(2)))
                    .map(|_| ScopeSpans {
                        scope: scope(tc),
                        schema_url: String::new(),
                        spans: (0..tc.draw(gs::integers::<usize>().max_value(4)))
                            .map(|_| Span {
                                trace_id: id(tc, 16),
                                span_id: id(tc, 8),
                                trace_state: string(tc, false),
                                parent_span_id: id(tc, 8),
                                flags: 0,
                                name: string(tc, huge),
                                kind: tc.draw(gs::integers::<i32>().min_value(-1).max_value(6)),
                                start_time_unix_nano: time(tc),
                                end_time_unix_nano: time(tc),
                                attributes: attrs(tc, 5, huge),
                                dropped_attributes_count: 0,
                                events: (0..tc.draw(gs::integers::<usize>().max_value(2)))
                                    .map(|_| span::Event { time_unix_nano: time(tc), name: string(tc, false), attributes: attrs(tc, 2, false), dropped_attributes_count: 0 })
                                    .collect(),
                                dropped_events_count: 0,
                                links: (0..tc.draw(gs::integers::<usize>().max_value(2)))
                                    .map(|_| span::Link { trace_id: id(tc, 16), span_id: id(tc, 8), trace_state: string(tc, false), attributes: attrs(tc, 2, false), dropped_attributes_count: 0, flags: 0 })
                                    .collect(),
                                dropped_links_count: 0,
                                status: tc.draw(gs::booleans()).then(|| Status { message: string(tc, false), code: tc.draw(gs::integers::<i32>().min_value(-1).max_value(3)) }),
                            })
                            .collect(),
                    })
                    .collect(),
            })
            .collect(),
    }
}

fn logs(tc: &TestCase, huge: bool) -> LogsData {
    let nr = tc.draw(gs::integers::<usize>().max_value(2));
    LogsData {
        resource_logs: (0..nr)
            .map(|_| ResourceLogs {
                resource: resource(tc, huge),
                schema_url: string(tc, false),
                scope_logs: (0..tc.draw(gs::integers::<usize>().max_value(2)))
                    .map(|_| ScopeLogs {
                        scope: scope(tc),
                        schema_url: string(tc, false),
                        log_records: (0..tc.draw(gs::integers::<usize>().max_value(4)))
                            .map(|_| LogRecord {
                                time_unix_nano: time(tc),
                                observed_time_unix_nano: time(tc),
                                severity_number: tc.draw(gs::integers::<i32>().min_value(0).max_value(24)),
                                severity_text: string(tc, false),
                                body: tc.draw(gs::booleans()).then(|| any_value(tc, 3, huge)),
                                attributes: attrs(tc, 5, huge),
                                dropped_attributes_count: 0,
                                flags: tc.draw(gs::integers::<u32>().max_value(255)),
                                trace_id: id(tc, 16),
                                span_id: id(tc, 8),
                                event_name: string(tc, false),
                            })
                            .collect(),
                    })
                    .collect(),
            })
            .collect(),
    }
}

// ---- the exporter's objects -----------------------------------------------------------------

use arrow::array::{Array, ArrayRef, BinaryArray, MapArray, RecordBatch, TimestampNanosecondArray, UInt64Array};
use otap_s3pq::Signal;
use otap_s3pq::batch::{Encoder, Format, Input};
use otap_s3pq::encode::{ParquetOptions, RowGroupSplit, SortBy, SortOptions};
use otap_s3pq::flatten::Envelope;

enum Req {
    Traces(TracesData),
    Logs(LogsData),
}

impl Req {
    fn draw(tc: &TestCase, huge: bool) -> Req {
        if tc.draw(gs::booleans()) { Req::Traces(traces(tc, huge)) } else { Req::Logs(logs(tc, huge)) }
    }
    fn signal(&self) -> Signal {
        match self {
            Req::Traces(_) => Signal::Traces,
            Req::Logs(_) => Signal::Logs,
        }
    }
    fn note(&self, tc: &TestCase) {
        match self {
            Req::Traces(t) => tc.note(&format!("{t:?}")),
            Req::Logs(l) => tc.note(&format!("{l:?}")),
        }
    }
    fn bytes(&self) -> Vec<u8> {
        match self {
            Req::Traces(t) => t.encode_to_vec(),
            Req::Logs(l) => l.encode_to_vec(),
        }
    }
}

fn env(tc: &TestCase) -> Envelope {
    Envelope { producer: "p".into(), epoch: "E".into(), batch: tc.draw(gs::integers::<u64>().max_value(1000)), received_ns: 1_790_000_000_000_000_000 }
}

fn opts(tc: &TestCase) -> ParquetOptions {
    let mut o = ParquetOptions::default();
    if tc.draw(gs::booleans()) {
        o.sort = SortOptions {
            by: SortBy::ServiceTime,
            row_groups: tc.draw(gs::integers::<usize>().min_value(1).max_value(4)),
            split: if tc.draw(gs::booleans()) { RowGroupSplit::Hash } else { RowGroupSplit::Range },
        };
    }
    o.dictionary = tc.draw(gs::booleans());
    o
}

/// What one request becomes: content key, columns, encoded object.
fn encode_one(enc: &mut Encoder, sig: Signal, b: &[u8], env: &Envelope) -> (String, Vec<ArrayRef>, usize, Vec<u8>) {
    let f = enc.flatten(&Input::Otlp(sig, b)).expect("flatten");
    let e = enc.encode(&f, env).expect("encode");
    (f.content.clone(), f.cols.clone(), f.stats.rows, e.body.to_vec())
}

/// Content keys, rows and objects are functions of the request alone: an
/// encoder that has seen other requests (its reused buffers) gives exactly
/// what a fresh one gives, and a retry of the same bytes gives the same key
/// and the same object bytes (CAST #10: anything that re-cuts or re-encodes
/// a request must be deterministic, or a retry is a new content key).
#[hegel::test]
fn prop_objects_depend_on_the_request_only(tc: TestCase) {
    let _trace = otap_s3pq::oscope_trace::covers("D", &["CAST-10", "H-2"]);
    let n = tc.draw(gs::integers::<usize>().min_value(1).max_value(4));
    let reqs: Vec<Req> = (0..n).map(|_| Req::draw(&tc, false)).collect();
    let o = opts(&tc);
    let e = env(&tc);
    let mut shared = Encoder::new(o.clone(), Format::Parquet);
    for (i, r) in reqs.iter().enumerate() {
        let b = r.bytes();
        let a = encode_one(&mut shared, r.signal(), &b, &e);
        let fresh = encode_one(&mut Encoder::new(o.clone(), Format::Parquet), r.signal(), &b, &e);
        assert_eq!(a.0, fresh.0, "request {i}: content key depends on encoder state");
        assert_eq!(a.2, fresh.2, "request {i}: rows");
        for (c, (x, y)) in a.1.iter().zip(fresh.1.iter()).enumerate() {
            assert_eq!(x.to_data(), y.to_data(), "request {i}: column {c} depends on encoder state");
        }
        assert!(a.3 == fresh.3, "request {i}: object bytes depend on encoder state");
        assert_eq!(a.0, otap_s3pq::batch::content_hash_otlp(r.signal(), &b));
    }
}

/// The OTAP path keys a request by its flattened columns
/// (`content_hash_cols`): the same request, converted to OTAP records and
/// flattened on two threads (each with its own `HashMap` seeds, as two
/// processes would be), gives the same key and the same columns as the OTLP
/// path's rows.
#[hegel::test]
fn prop_otap_rows_match_otlp_and_do_not_depend_on_the_process(tc: TestCase) {
    use otel_arrow_dfe_pdata::{OtapArrowRecords, OtlpProtoBytes, TryIntoWithOptions};
    let r = Req::draw(&tc, false);
    r.note(&tc);
    let (sig, b) = (r.signal(), r.bytes());
    let direct = Encoder::new(ParquetOptions::default(), Format::Parquet).flatten(&Input::Otlp(sig, &b)).expect("flatten");
    let p = match sig {
        Signal::Traces => OtlpProtoBytes::ExportTracesRequest(b.clone().into()),
        _ => OtlpProtoBytes::ExportLogsRequest(b.clone().into()),
    };
    let recs: OtapArrowRecords = match p.try_into_with_default() {
        Ok(r) => r,
        Err(e) => {
            // Upstream's conversion refuses some requests; nothing to compare.
            tc.note(&format!("OTLP -> OTAP refused: {e}"));
            return;
        }
    };
    let res = |f: &otap_s3pq::batch::Flat| f.resources.iter().map(|(c, r)| (c.id, c.pairs.clone(), *r)).collect::<Vec<_>>();
    let via = Encoder::new(ParquetOptions::default(), Format::Parquet).flatten(&Input::Otap(sig, &recs)).expect("flatten otap");
    assert_eq!(res(&via), res(&direct), "the resources to announce differ between the OTLP and OTAP paths");
    let flat_on_thread = |recs: OtapArrowRecords| {
        std::thread::spawn(move || {
            let f = Encoder::new(ParquetOptions::default(), Format::Parquet).flatten(&Input::Otap(sig, &recs)).expect("flatten otap");
            (f.content, f.cols.iter().map(|c| c.to_data()).collect::<Vec<_>>(), f.stats.rows)
        })
        .join()
        .expect("thread")
    };
    let (k1, c1, n1) = flat_on_thread(recs.clone());
    let (k2, c2, n2) = flat_on_thread(recs);
    assert_eq!((k1.clone(), n1), (k2, n2), "the OTAP content key depends on the process");
    assert!(c1 == c2);
    assert_eq!(n1, direct.stats.rows, "OTAP and OTLP rows");
    // (Patch 0005 fixed the three upstream differences this found: the
    // duration of a span with a zero time, a log body of int 0 / double 0,
    // and half-precision floats inside arrays and maps; see the regression
    // tests below.)
    for (i, (a, c)) in direct.cols.iter().zip(c1.iter()).enumerate() {
        assert_eq!(&a.to_data(), c, "column {i} ({}) differs between the OTLP and OTAP paths", Encoder::new(ParquetOptions::default(), Format::Parquet).schemas(sig).arrow.field(i).name());
    }
}

/// A request's rows from the OTLP path (the direct walk) and from the OTAP
/// path (upstream's OTLP -> OTAP conversion, then its views).
fn both_paths(sig: Signal, b: &[u8]) -> (otap_s3pq::batch::Flat, otap_s3pq::batch::Flat) {
    use otel_arrow_dfe_pdata::{OtapArrowRecords, OtlpProtoBytes, TryIntoWithOptions};
    let mut enc = Encoder::new(ParquetOptions::default(), Format::Parquet);
    let direct = enc.flatten(&Input::Otlp(sig, b)).unwrap();
    let p = match sig {
        Signal::Traces => OtlpProtoBytes::ExportTracesRequest(b.to_vec().into()),
        _ => OtlpProtoBytes::ExportLogsRequest(b.to_vec().into()),
    };
    let recs: OtapArrowRecords = p.try_into_with_default().unwrap();
    let via = enc.flatten(&Input::Otap(sig, &recs)).unwrap();
    (direct, via)
}

fn one_log(r: LogRecord) -> Vec<u8> {
    LogsData {
        resource_logs: vec![ResourceLogs {
            resource: None,
            schema_url: String::new(),
            scope_logs: vec![ScopeLogs { scope: None, schema_url: String::new(), log_records: vec![r] }],
        }],
    }
    .encode_to_vec()
}

/// Regression (upstream otel-arrow `pdata/src/views/otap/common.rs`
/// `cbor_to_any_value`, fixed by patches/0005): OTAP stores array and map
/// values as CBOR, and the Rust OTLP -> OTAP conversion (serde_cbor) writes
/// a double in the shortest float that holds it exactly, half precision
/// (major type 7, additional info 25) included. The view read only single
/// and double precision, so 0, 1.5, 65504, NaN inside an array or map read
/// as null: `[1.5,7]` from the OTLP path was `[null,7]` from the OTAP path.
/// Found by `prop_otap_rows_match_otlp_…`, shrunk to one log attribute
/// `[0.0]`.
#[test]
fn regression_otap_nested_half_float() {
    let _trace = otap_s3pq::oscope_trace::covers("PH", &["CAST-19", "H-2"]);
    for (d, want) in [(0.0, "[0,7]"), (-0.0, "[-0,7]"), (1.5, "[1.5,7]"), (65504.0, "[65504,7]"), (5.960464477539063e-8, "[5.960464477539063e-8,7]"), (0.1, "[0.1,7]"), (f64::NAN, ""), (f64::NEG_INFINITY, "")] {
        let arr = Value::ArrayValue(ArrayValue { values: vec![AnyValue { value: Some(Value::DoubleValue(d)) }, AnyValue { value: Some(Value::IntValue(7)) }] });
        let b = one_log(LogRecord { attributes: vec![KeyValue { key: "a".into(), value: Some(AnyValue { value: Some(arr) }) }], ..Default::default() });
        let (direct, via) = both_paths(Signal::Logs, &b);
        let v = |f: &otap_s3pq::batch::Flat| {
            let m = f.cols[14].as_any().downcast_ref::<MapArray>().unwrap().value(0);
            String::from_utf8(m.column(1).as_any().downcast_ref::<BinaryArray>().unwrap().value(0).to_vec()).unwrap()
        };
        // (NaN and ±Inf inside an array make the whole value "", as Go's
        // json.Encoder fails on them: render.rs.)
        assert_eq!(v(&direct), want, "{d}: the OTLP path");
        assert_eq!(v(&via), want, "{d}: the OTAP path");
    }
}

/// The traces schema's Duration column, the logs schema's Body and LogAttributes.
const DURATION: usize = 12;
const BODY: usize = 7;
const LOG_ATTRS: usize = 14;

/// Regression (upstream otel-arrow `pdata/src/views/otap/logs.rs`
/// `get_body_from_struct` and `encode/record/attributes.rs`, fixed by
/// patches/0005): OTAP omits a column whose values all equal the default,
/// and the attribute view and the OTAP -> OTLP decoder read a missing int /
/// double column as 0, but the logs view read the body as Empty. A log whose
/// body was the int 0 (or the double ±0) in a batch with no other such body
/// landed with Body "" from the OTAP path and "0" from the OTLP path (and
/// the Go edge). Found by `prop_otap_rows_match_otlp_…`, shrunk to one
/// record with body `IntValue(0)`. The same omission turned an attribute's
/// -0.0 into 0.0.
#[test]
fn regression_otap_zero_values() {
    let _trace = otap_s3pq::oscope_trace::covers("PH", &["CAST-18", "H-2"]);
    for (v, want) in [(Value::IntValue(0), "0"), (Value::DoubleValue(0.0), "0"), (Value::DoubleValue(-0.0), "-0"), (Value::BoolValue(false), "false"), (Value::StringValue(String::new()), "")] {
        let b = one_log(LogRecord {
            body: Some(AnyValue { value: Some(v.clone()) }),
            attributes: vec![KeyValue { key: "a".into(), value: Some(AnyValue { value: Some(v.clone()) }) }],
            ..Default::default()
        });
        let (direct, via) = both_paths(Signal::Logs, &b);
        let body = |f: &otap_s3pq::batch::Flat| f.cols[BODY].as_any().downcast_ref::<BinaryArray>().unwrap().value(0).to_vec();
        assert_eq!(String::from_utf8(body(&direct)).unwrap(), want, "{v:?}: the OTLP path");
        assert_eq!(String::from_utf8(body(&via)).unwrap(), want, "{v:?}: the OTAP path");
        assert!(direct.cols[LOG_ATTRS].to_data() == via.cols[LOG_ATTRS].to_data(), "{v:?}: the attribute differs between the paths");
    }
}

/// Regression (upstream otel-arrow `pdata/src/encode/mod.rs`, fixed by
/// patches/0005): converting OTLP to OTAP stored `duration = end − start`
/// only when both times were present, and a proto3 time of 0 is absent on
/// the wire: a span with start 0 and end 1 came out of the OTAP path with
/// Duration 0, from the OTLP path (and the Go edge) with 1. Found by
/// `prop_otap_rows_match_otlp_…`, shrunk to one span, start 0, end 1.
#[test]
fn regression_otap_duration_with_a_zero_time() {
    let _trace = otap_s3pq::oscope_trace::covers("PH", &["CAST-17", "H-2"]);
    for (start, end) in [(0u64, 1u64), (5, 0), (0, 0), (u64::MAX, 3), (1_700_000_000_000_000_000, 1_700_000_000_000_001_000)] {
        let t = TracesData {
            resource_spans: vec![ResourceSpans {
                resource: None,
                schema_url: String::new(),
                scope_spans: vec![ScopeSpans { scope: None, schema_url: String::new(), spans: vec![Span { start_time_unix_nano: start, end_time_unix_nano: end, ..Default::default() }] }],
            }],
        };
        let (direct, via) = both_paths(Signal::Traces, &t.encode_to_vec());
        let d = |f: &otap_s3pq::batch::Flat| f.cols[DURATION].as_any().downcast_ref::<UInt64Array>().unwrap().value(0);
        assert_eq!(d(&via), d(&direct), "start {start} end {end}");
    }
}

/// Reads an object back, cast to the schema it was built with (the Parquet
/// schema publishes the byte columns as STRING, so a reader sees Utf8).
fn decode(sc: &otap_s3pq::schema::Schemas, body: &[u8]) -> RecordBatch {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let rd = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(body)).expect("reader").build().expect("build");
    let schema = arrow::array::RecordBatchReader::schema(&rd);
    let batches: Vec<RecordBatch> = rd.map(|b| b.expect("batch")).collect();
    let rb = arrow::compute::concat_batches(&schema, &batches).expect("concat");
    let cols: Vec<ArrayRef> = sc
        .arrow
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| arrow::compute::cast(rb.column(i), f.data_type()).unwrap_or_else(|e| panic!("{}: {e}", f.name())))
        .collect();
    RecordBatch::try_new(sc.arrow.clone(), cols).expect("batch")
}

fn bin(rb: &RecordBatch, name: &str) -> Vec<Vec<u8>> {
    let a = rb.column_by_name(name).unwrap_or_else(|| panic!("{name}"));
    let a = a.as_any().downcast_ref::<BinaryArray>().expect("binary");
    (0..a.len()).map(|i| a.value(i).to_vec()).collect()
}

fn map_rows(rb: &RecordBatch, name: &str) -> Vec<Vec<(Vec<u8>, Vec<u8>)>> {
    let m = rb.column_by_name(name).unwrap().as_any().downcast_ref::<MapArray>().expect("map");
    (0..m.len())
        .map(|i| {
            let e = m.value(i);
            let k = e.column(0).as_any().downcast_ref::<BinaryArray>().unwrap();
            let v = e.column(1).as_any().downcast_ref::<BinaryArray>().unwrap();
            (0..k.len()).map(|j| (k.value(j).to_vec(), v.value(j).to_vec())).collect()
        })
        .collect()
}

fn string_of(v: &Option<AnyValue>) -> Option<&str> {
    match v.as_ref().and_then(|v| v.value.as_ref()) {
        Some(Value::StringValue(s)) => Some(s),
        _ => None,
    }
}

/// Encode → decode is lossless (every column, sorted objects restored by
/// `row_ordinal`), and the rows are the request's: one per span / record,
/// names, bodies, attribute keys and string values byte for byte, times,
/// durations, and the object's metadata (rows, time range).
#[hegel::test]
fn prop_parquet_round_trip(tc: TestCase) {
    let r = Req::draw(&tc, tc.draw(gs::weighted_booleans(0.2)));
    r.note(&tc);
    let (sig, b) = (r.signal(), r.bytes());
    let o = opts(&tc);
    let e = env(&tc);
    let mut enc = Encoder::new(o.clone(), Format::Parquet);
    let f = enc.flatten(&Input::Otlp(sig, &b)).expect("flatten");
    let obj = enc.encode(&f, &e).expect("encode");
    let sc = enc.schemas(sig);
    let want = enc.rows(&f, &e, &|_| true, &|_| true).0;
    let mut got = decode(sc, &obj.body);
    assert_eq!(got.num_rows(), want.num_rows());
    if o.sort.enabled() {
        let ord = got.column_by_name("row_ordinal").unwrap().clone();
        let idx = arrow::compute::sort_to_indices(&ord, None, None).unwrap();
        got = arrow::compute::take_record_batch(&got, &idx).unwrap();
    }
    for (i, field) in sc.arrow.fields().iter().enumerate() {
        assert_eq!(got.column(i).to_data(), want.column(i).to_data(), "column {} after the round trip", field.name());
    }
    // The rows are the request's.
    let ts: Vec<i64> = got.column_by_name("Timestamp").unwrap().as_any().downcast_ref::<TimestampNanosecondArray>().unwrap().values().to_vec();
    let mut want_ts = Vec::new();
    match &r {
        Req::Traces(t) => {
            let spans: Vec<(&Option<Resource>, &Span)> =
                t.resource_spans.iter().flat_map(|rs| rs.scope_spans.iter().flat_map(move |ss| ss.spans.iter().map(move |s| (&rs.resource, s)))).collect();
            assert_eq!(got.num_rows(), spans.len(), "one row per span");
            let names = bin(&got, "SpanName");
            let dur = got.column_by_name("Duration").unwrap().as_any().downcast_ref::<UInt64Array>().unwrap().values().to_vec();
            let at = map_rows(&got, "SpanAttributes");
            let svc = bin(&got, "ServiceName");
            for (i, (res, s)) in spans.iter().enumerate() {
                assert_eq!(names[i], s.name.as_bytes(), "row {i}: SpanName");
                assert_eq!(dur[i], s.end_time_unix_nano.wrapping_sub(s.start_time_unix_nano), "row {i}: Duration");
                want_ts.push(s.start_time_unix_nano);
                check_attrs(i, &at[i], &s.attributes);
                check_service(i, &svc[i], res);
            }
        }
        Req::Logs(l) => {
            let recs: Vec<(&Option<Resource>, &LogRecord)> =
                l.resource_logs.iter().flat_map(|rl| rl.scope_logs.iter().flat_map(move |sl| sl.log_records.iter().map(move |x| (&rl.resource, x)))).collect();
            assert_eq!(got.num_rows(), recs.len(), "one row per log record");
            let body = bin(&got, "Body");
            let at = map_rows(&got, "LogAttributes");
            let svc = bin(&got, "ServiceName");
            for (i, (res, x)) in recs.iter().enumerate() {
                if let Some(s) = string_of(&x.body) {
                    assert_eq!(body[i], s.as_bytes(), "row {i}: Body");
                }
                want_ts.push(if x.time_unix_nano == 0 { x.observed_time_unix_nano } else { x.time_unix_nano });
                check_attrs(i, &at[i], &x.attributes);
                check_service(i, &svc[i], res);
            }
        }
    }
    assert_eq!(ts, want_ts.iter().map(|t| *t as i64).collect::<Vec<_>>(), "Timestamp");
    // The metadata: rows; the time range as the Go writer computes it (0
    // means "unset" for the minimum: PBT.md finding 5, kept for byte
    // compatibility with parquetgo).
    let meta = |k: &str| obj.meta.get(k).cloned().unwrap_or_default();
    assert_eq!(meta(otap_s3pq::proto::META_ROWS), want_ts.len().to_string());
    let max = want_ts.iter().copied().max().unwrap_or(0);
    let mut min = 0u64;
    for &t in &want_ts {
        if min == 0 || t < min {
            min = t;
        }
    }
    assert_eq!(meta(otap_s3pq::proto::META_MAX_TIME), max.to_string());
    assert_eq!(meta(otap_s3pq::proto::META_MIN_TIME), min.to_string());
}

fn check_attrs(row: usize, got: &[(Vec<u8>, Vec<u8>)], want: &[KeyValue]) {
    assert_eq!(got.len(), want.len(), "row {row}: attribute count (duplicates kept, in order)");
    for (j, (kv, (k, v))) in want.iter().zip(got).enumerate() {
        assert_eq!(k, kv.key.as_bytes(), "row {row}: attribute {j} key");
        if let Some(s) = string_of(&kv.value) {
            assert_eq!(v, s.as_bytes(), "row {row}: attribute {j} value");
        }
    }
}

/// ServiceName: the first `service.name`, when it is a string.
fn check_service(row: usize, got: &[u8], res: &Option<Resource>) {
    let first = res.as_ref().and_then(|r| r.attributes.iter().find(|kv| kv.key == "service.name"));
    match first {
        None => assert!(got.is_empty(), "row {row}: ServiceName without service.name"),
        Some(kv) => {
            if let Some(s) = string_of(&kv.value) {
                assert_eq!(got, s.as_bytes(), "row {row}: ServiceName");
            }
        }
    }
}

/// CAST #9: one huge value must not make an object many times larger.
/// Parquet repeats a column's min and max in the chunk statistics and in the
/// page index, uncompressed; parquet-rs truncates both (to 64 bytes by
/// default), which is what the Go writer had to be taught. Measured
/// differentially: the object with statistics on is at most 64 KiB larger
/// than the same object with statistics off, and no chunk statistic is over
/// 64 bytes, for values up to 1 MiB.
#[hegel::test]
fn prop_statistics_stay_small_with_huge_values(tc: TestCase) {
    let _trace = otap_s3pq::oscope_trace::covers("PH", &["CAST-9", "H-7"]);
    let r = Req::draw(&tc, true);
    let (sig, b) = (r.signal(), r.bytes());
    let e = env(&tc);
    let mut on = ParquetOptions::default();
    on.dictionary = tc.draw(gs::booleans());
    let mut off = on.clone();
    off.statistics = "none".into();
    let mut enc = Encoder::new(on, Format::Parquet);
    let f = enc.flatten(&Input::Otlp(sig, &b)).expect("flatten");
    let with = enc.encode(&f, &e).expect("encode").body;
    let without = Encoder::new(off, Format::Parquet).encode(&f, &e).expect("encode").body;
    let largest = b.len();
    tc.note(&format!("request {largest} B, object {} B with statistics, {} B without", with.len(), without.len()));
    assert!(with.len() <= without.len() + (64 << 10), "statistics cost {} B for a {largest} B request", with.len() - without.len());
    let md = parquet::file::metadata::ParquetMetaDataReader::new().parse_and_finish(&bytes::Bytes::copy_from_slice(&with)).expect("footer");
    for rg in md.row_groups() {
        for c in rg.columns() {
            if let Some(s) = c.statistics() {
                for v in [s.min_bytes_opt(), s.max_bytes_opt()].into_iter().flatten() {
                    assert!(v.len() <= 64, "{}: a {} B statistic", c.column_path(), v.len());
                }
            }
        }
    }
}



// ---- the watermark's history (D29 amendment 2026-10-01; model/wmHistory.qnt) --------------

use consumer::wmhistory::{self, HOUR_MS, HistHour, HistMutation, HistStep, History};
use std::collections::BTreeMap;

/// The model's harness over the real `advance`, `without_sealed` and `as_of`:
/// publishers with clocks within `skew` of real time, each run an observe
/// (the truth now), a GET (the document, and the clock for the stamp) and a
/// CAS (only if the document is unchanged); sealing writes each frozen hour
/// once (create-only) and drops it; readers ask "as of t" for t up to now.
/// Checks the model's `sound`, `monotone` and `answersHold`. `draw(n)` is
/// in 0..n. `stamp_early`: the harness stamps at observation (the model's
/// `stampEarly`, a mistake of the caller of `advance`).
fn history_sim(mut draw: impl FnMut(u64) -> u64, m: HistMutation, stamp_early: bool) -> Result<(), String> {
    const S: u64 = 2_000; // the skew bound (ms)
    const H: u64 = HOUR_MS;
    let pubs = 2 + draw(2) as usize;
    let off: Vec<i64> = (0..pubs).map(|_| draw(2 * S + 1) as i64 - S as i64).collect();
    let every = [0, 1_000, 60_000][draw(3) as usize];
    let mut rt: u64 = 10 * H; // real time (ms)
    let mut ing: u64 = 0; // the truth (ns)
    let mut ing_log: Vec<(u64, u64)> = vec![(rt, 0)]; // (real time, truth from then on)
    let mut doc = (0u64, 0u64, History::default()); // (version, ct, history)
    let mut objects: BTreeMap<u64, HistHour> = BTreeMap::new();
    // per publisher: phase (0 idle, 1 observed, 2 read), obs, t_obs, read doc, t_get
    let mut ps: Vec<(u8, u64, u64, (u64, u64, History), u64)> = vec![(0, 0, 0, Default::default(), 0); pubs];
    let mut answers: Vec<(u64, u64, bool)> = Vec::new();
    let clk = |p: usize, rt: u64| (rt as i64 + off[p]).max(0) as u64;
    let ing_at = |log: &[(u64, u64)], t: u64| log.iter().rev().find(|(t0, _)| *t0 <= t).map_or(0, |x| x.1);
    let all_steps = |doc: &History, objects: &BTreeMap<u64, HistHour>| -> Vec<HistStep> {
        let mut v: Vec<HistStep> = objects.values().flat_map(|h| h.steps.clone()).collect();
        v.extend(doc.sealing.iter().flat_map(|h| h.steps.clone()));
        v.extend(doc.open.steps.clone());
        v
    };
    for step in 0..120 {
        match draw(9) {
            0 => rt += 1 + draw(H / 4),
            1 => {
                ing += 1 + draw(1_000);
                ing_log.push((rt, ing));
            }
            2..=5 => {
                let p = draw(pubs as u64) as usize;
                let x = &mut ps[p];
                match x.0 {
                    0 => *x = (1, ing, clk(p, rt), x.3.clone(), 0),
                    1 => {
                        x.3 = doc.clone();
                        x.4 = clk(p, rt);
                        x.0 = 2;
                    }
                    _ => {
                        if x.3.0 != doc.0 {
                            x.0 = 1; // lost the race: GET again
                        } else {
                            let ct = x.3.1.max(x.1);
                            let t = if stamp_early { x.2 } else { x.4 };
                            let at = wmhistory::stamp(t, t, S, m);
                            let h = wmhistory::advance(&x.3.2, HistStep { at_ms: at, ct_ns: ct, ..Default::default() }, every, m);
                            doc = (doc.0 + 1, ct, h);
                            x.0 = 0;
                        }
                    }
                }
            }
            6 => {
                // seal every frozen hour (create-only), then drop them (a second CAS)
                let done: Vec<u64> = doc.2.sealing.iter().map(|h| h.hour_ms).collect();
                for h in &doc.2.sealing {
                    let _ = objects.entry(h.hour_ms).or_insert_with(|| h.clone());
                }
                if let Some(n) = wmhistory::without_sealed(&doc.2, &done) {
                    doc = (doc.0 + 1, doc.1, n);
                }
            }
            _ => {
                let t = 10 * H + draw(rt - 10 * H + 1);
                if let Some(a) = wmhistory::as_of(&doc.2, t, 1_000, |h| objects.get(&h).cloned()) {
                    answers.push((t, a.step.ct_ns, a.final_));
                }
            }
        }
        // the properties, after every step
        let steps = all_steps(&doc.2, &objects);
        for s in &steps {
            let truth = if s.at_ms >= rt { ing } else { ing_at(&ing_log, s.at_ms) };
            if s.ct_ns > truth {
                return Err(format!("step {step}: unsound: {s:?} claims {} by {}, true then: {truth}", s.ct_ns, s.at_ms));
            }
            if s.ct_ns > doc.1 {
                return Err(format!("step {step}: {s:?} above the published {}", doc.1));
            }
        }
        for a in &steps {
            if let Some(b) = steps.iter().find(|b| a.at_ms < b.at_ms && a.ct_ns > b.ct_ns) {
                return Err(format!("step {step}: not monotone: {a:?} then {b:?}"));
            }
        }
        for (t, v, fin) in &answers {
            let now = wmhistory::as_of(&doc.2, *t, 1_000, |h| objects.get(&h).cloned()).map(|a| a.step.ct_ns);
            if (*fin && now != Some(*v)) || now.is_none_or(|n| n < *v) {
                return Err(format!("step {step}: as of {t} was {v} (final {fin}), now {now:?}"));
            }
        }
    }
    Ok(())
}

/// The design: sound, monotone, final answers stable, provisional ones never
/// falling, under any interleaving of publishers with skewed clocks.
#[hegel::test]
fn prop_watermark_history_is_sound_and_stable(tc: TestCase) {
    let _trace = otap_s3pq::oscope_trace::covers("SM", &["H-2", "H-5", "R-S1"]);
    let r = history_sim(|n| tc.draw(gs::integers::<u64>().max_value(n.saturating_sub(1))), HistMutation::None, false);
    assert!(r.is_ok(), "{}", r.unwrap_err());
}

/// The model's mutants, against the same harness: each is caught.
#[test]
fn watermark_history_mutants_are_caught() {
    let _trace = otap_s3pq::oscope_trace::covers("MU", &["H-2", "H-5", "R-S1"]);
    for (name, m, early) in [("stampEarly", HistMutation::None, true), ("noSkew", HistMutation::NoSkew, false), ("noClamp", HistMutation::NoClamp, false)] {
        let caught = (1..=3_000u64).find(|seed| {
            let mut x = *seed * 0x9E37_79B9_7F4A_7C15;
            let draw = |n: u64| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                if n == 0 { 0 } else { x % n }
            };
            history_sim(draw, m, early).is_err()
        });
        assert!(caught.is_some(), "{name} not caught in 3,000 seeds");
    }
    // and the design is not "caught" by those seeds
    for seed in 1..=500u64 {
        let mut x = seed * 0x9E37_79B9_7F4A_7C15;
        let draw = |n: u64| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            if n == 0 { 0 } else { x % n }
        };
        let r = history_sim(draw, HistMutation::None, false);
        assert!(r.is_ok(), "seed {seed}: {}", r.unwrap_err());
    }
}
