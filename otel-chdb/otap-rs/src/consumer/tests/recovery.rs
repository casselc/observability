//! The insert path's recovery branches, the worker's per-lane memory, the
//! tombstone race and giving lanes back: one test per branch, each driving
//! it with an injected fault (a slow answer, a server-side fence, a partial
//! batch, a lost or stuck write, a 412 with nothing behind it). Written for
//! the mutation nightly over worker.rs (runs 54 and 57), where each of
//! these branches had a surviving mutant: the fleet DST's end-to-end
//! invariants held under them, so a test that names the branch's outcome
//! is what pins it.

use super::*;
use super::super::coord::LeaseDoc;
use super::super::gc::GcDoc;
use super::super::plan;
use super::super::worker::{TombResult, tombstone};

/// An object at slot 0 of epoch E0001 of `lane`, with extra metadata.
fn put_with(b: &MemBucket, lane: &str, content: &str, extra: &[(&str, &str)]) {
    let key = proto::slot_key(&format!("{ROOT}/{lane}"), "E0001", 0);
    let mut m = meta("E0001", 0, content, 4);
    for (k, v) in extra {
        let _ = m.insert(k.to_string(), v.to_string());
    }
    b.insert(&key, Bytes::from(vec![0u8; 100]), m);
}

/// Rows of `content` already in central (an earlier attempt's), on day 0.
fn seed_rows(c: &MemCentral, table: &str, content: &str, rows: u64) {
    *c.rows.borrow_mut().entry((table.into(), content.into())).or_default() += rows;
    *c.by_day.borrow_mut().entry((table.into(), content.into(), 0)).or_default() += rows;
}

fn lease_of(b: &MemBucket, lane: &str) -> LeaseDoc {
    serde_json::from_slice(&b.objs.borrow()[&format!("{CTL}/lease/{lane}.json")].body).unwrap()
}

/// A statement the server ran, answered after its fence + budget (ttl -
/// margin from the lease's send, here 8 s), is not known to have landed in
/// the window: the series insert, the announcement and the payload
/// statement each count it fenced and pass nothing. Answered inside it
/// (6 s), each is taken. (Run 54: the three `wall <= fence + budget` match
/// guards replaced by true, and their `+` by `*` or `-`, survived.)
#[tokio::test(flavor = "current_thread")]
async fn an_answer_after_the_fence_and_budget_is_not_taken_for_landed() {
    let _trace = otap_s3pq::oscope_trace::covers("DST", &["H-2"]);
    for (delay, in_time) in [(6_000, true), (8_001, false)] {
        let n = u64::from(in_time);
        // The series table: no check, the answer is all there is.
        let (b, c, clk) = setup();
        let mut e = Edge::new("c1/p1", "metrics_series");
        e.commit(&b, "s0", 3).await;
        let mut w = worker("w1", &b, &c, &clk);
        c.answer_delay_ms.set(delay);
        let _ = w.step().await;
        assert_eq!((w.stats.series_objects_inserted, w.stats.fenced_by_server), (n, 1 - n), "series, answered after {delay} ms: {:?}", w.stats);
        // An announcement: its lane's rows wait for it.
        let (b, c, clk) = setup();
        put_with(&b, "c1/p1/logs", "h0", &[(proto::META_ANNOUNCE, "1")]);
        let mut w = worker("w1", &b, &c, &clk);
        c.answer_delay_ms.set(delay);
        let _ = w.step().await;
        assert_eq!(w.stats.announce_objects, n, "announcement, answered after {delay} ms: {:?}", w.stats);
        if !in_time {
            assert_eq!((w.stats.fenced_by_server, w.stats.announce_deferred), (1, 1), "{:?}", w.stats);
            assert_eq!(c.count("otel_logs", "h0"), 0, "no row before its announcement surely landed");
        }
        // A payload part: likewise.
        let (b, c, clk) = setup();
        put_with(&b, "c1/p1/traces", "h0", &[(proto::META_PAYLOADS, "1")]);
        let mut w = worker("w1", &b, &c, &clk);
        c.answer_delay_ms.set(delay);
        let _ = w.step().await;
        assert_eq!(w.stats.payload_objects, n, "payloads, answered after {delay} ms: {:?}", w.stats);
        if !in_time {
            assert_eq!((w.stats.fenced_by_server, w.stats.payload_deferred), (1, 1), "{:?}", w.stats);
            assert_eq!(c.count("otel_traces", "h0"), 0, "no row before its payloads surely landed");
        }
    }
}

/// A statement the server fenced (its clock was past the fence) answers
/// like one that wrote nothing it had to. The verify finds the object
/// missing over its own range and recounts over the horizon; then the
/// worker's own clock decides: answered at the fence exactly, the
/// statement may have run in time, so the object goes again alone; answered
/// after it, the statement reached the server late, a no-op by design.
/// (Run 54: `wall > fence` as `>=` or `==`, and the recount's `wide !=
/// range` as `==`, survived.)
#[tokio::test(flavor = "current_thread")]
async fn a_statement_the_server_fenced_is_retried_only_if_it_may_have_run_in_time() {
    let _trace = otap_s3pq::oscope_trace::covers("DST", &["H-2"]);
    for (delay, retried) in [(5_000, 1), (5_001, 0)] {
        let (b, c, clk) = setup();
        let mut e = Edge::stamped("c1/p1", "traces", &clk);
        e.commit(&b, "h0", 5).await;
        let mut w = worker("w1", &b, &c, &clk);
        // The fence is 5 s after the take; the server's clock is past it.
        c.skew_ms.set(5_001);
        c.answer_delay_ms.set(delay);
        let _ = w.step().await;
        assert_eq!(c.count("otel_traces", "h0"), 0);
        assert_eq!((w.stats.retried_missing, w.stats.fenced_by_server), (retried, 1 - retried), "answered after {delay} ms: {:?}", w.stats);
        assert_eq!(w.stats.range_recounts, 1 + retried, "each verify recounts the short object over the horizon: {:?}", w.stats);
        assert_eq!(w.checkpoint("c1/p1/traces").unwrap().next("E0001"), 0);
    }
}

/// Without a horizon, every count reads every partition: the verify too,
/// although the statement asserted its rows' received time. (Run 54:
/// `own_range_of`'s `||` as `&&` survived.)
#[tokio::test(flavor = "current_thread")]
async fn with_no_horizon_every_count_reads_every_partition() {
    let _trace = otap_s3pq::oscope_trace::covers("DST", &["H-2"]);
    let (b, c, clk) = setup();
    let mut e = Edge::stamped("c1/p1", "traces", &clk);
    e.commit(&b, "h0", 5).await;
    let mut cf = cfg("w1");
    cf.horizon_ms = None;
    let mut w = Worker::new(cf, b.clone(), c.clone(), clk.clone());
    let _ = w.step().await;
    assert_eq!(c.count("otel_traces", "h0"), 5);
    assert_eq!((w.stats.range_checks, w.stats.full_checks), (0, 2), "the check and the verify, both over every partition: {:?}", w.stats);
}

/// A series object HEADed while it lingered, whose lane's renewals are
/// lost from then on: once the lease cannot cover a statement any more,
/// none is sent. Sent anyway, the server fences it (its clock is past the
/// fence) and answers as if it ran, inside fence + budget: the worker would
/// pass an object no row of which landed. (Run 54: `window`'s `&&` as `||`
/// survived.)
#[tokio::test(flavor = "current_thread")]
async fn a_series_statement_is_not_sent_once_the_lease_cannot_cover_it() {
    let _trace = otap_s3pq::oscope_trace::covers("DST", &["H-2"]);
    let (b, c, clk) = setup();
    let mut e = Edge::new("c1/p1", "metrics_series");
    e.commit(&b, "s0", 3).await;
    let mut cf = cfg("w1");
    cf.linger_ms = 2_000;
    let mut w = Worker::new(cf, b.clone(), c.clone(), clk.clone());
    let t0 = clk.0.get();
    let _ = w.step().await;
    assert_eq!((w.stats.series_objects_inserted, w.stats.linger_deferred), (0, 1), "{:?}", w.stats);
    *b.faults.borrow_mut() = MemFaults { matching: "/lease/".into(), drop_every: 1, ..Default::default() };
    // 5.5 s: the lease (not renewed since the take) is good to 8 s, so no
    // statement of 3 s can start.
    clk.0.set(t0 + 5_500);
    let _ = w.step().await;
    assert_eq!(w.held_lanes(), vec!["c1/p1/metrics_series".to_string()]);
    assert_eq!((w.stats.series_objects_inserted, c.fenced.get()), (0, 0), "nothing sent: {:?}", w.stats);
    assert!(w.stats.deferred_by_lease > 0, "{:?}", w.stats);
}

/// One statement for objects of two lanes, one of which can no longer
/// cover it (its renewals are lost; its statement of long ago settled):
/// the other lane's object goes in now, without it. (Run 54: the
/// partition's `&&` as `||` survived: the whole group waited.)
#[tokio::test(flavor = "current_thread")]
async fn a_lane_that_cannot_cover_a_statement_does_not_hold_back_the_others() {
    let _trace = otap_s3pq::oscope_trace::covers("DST", &["H-2"]);
    let (b, c, clk) = setup();
    let mut ea = Edge::new("c1/pa", "traces");
    let mut eb = Edge::new("c1/pb", "traces");
    ea.commit(&b, "a0", 3).await;
    let mut w = worker("w1", &b, &c, &clk);
    let t0 = clk.0.get();
    // a0 lands, its answer is lost: pa is left alone until t0 + 10 s.
    c.lost_answer_every.set(1);
    let _ = w.step().await;
    c.lost_answer_every.set(0);
    assert_eq!(w.stats.unsettled, 1);
    ea.commit(&b, "a1", 3).await;
    eb.commit(&b, "b0", 3).await;
    for dt in (500..=10_500).step_by(500) {
        // pa's renewal at 3 s lands (good to 11 s); every later one is lost.
        if dt == 3_500 {
            *b.faults.borrow_mut() = MemFaults { matching: "/lease/c1/pa/".into(), drop_every: 1, ..Default::default() };
        }
        if dt == 10_500 {
            eb.commit(&b, "b1", 3).await;
        }
        clk.0.set(t0 + dt);
        let _ = w.step().await;
    }
    // At 10.5 s a1 (pa) and b1 (pb) are both due, in one group.
    assert_eq!(w.held_lanes().len(), 2, "{:?}", w.stats);
    assert_eq!(c.count("otel_traces", "b1"), 3, "pb's object went in: {:?}", w.stats);
    assert_eq!(c.count("otel_traces", "a1"), 0, "pa's waits for a lease that covers it");
}

/// A batch an earlier attempt left partial: its missing rows are repaired,
/// and the slot passes only once all are there. A repair the server fenced
/// leaves it partial and the slot unpassed; a repair error that may still
/// land leaves the lane alone until it has settled, one that cannot does
/// not. (Run 54: `repair` returning true or false, and the `!` of its
/// settled check, survived: no test ever made a batch partial by rows.)
#[tokio::test(flavor = "current_thread")]
async fn a_partial_batch_passes_only_once_repaired() {
    let _trace = otap_s3pq::oscope_trace::covers("DST", &["H-2"]);
    for case in ["lands", "fenced", "settled error", "unsettled error"] {
        let (b, c, clk) = setup();
        let mut e = Edge::new("c1/p1", "traces");
        e.commit(&b, "h0", 5).await;
        seed_rows(&c, "otel_traces", "h0", 2);
        match case {
            "fenced" => c.skew_ms.set(T.ttl_ms),
            "settled error" => c.repair_err.set(Some(true)),
            "unsettled error" => c.repair_err.set(Some(false)),
            _ => {}
        }
        let mut w = worker("w1", &b, &c, &clk);
        let _ = w.step().await;
        let passed = w.checkpoint("c1/p1/traces").unwrap().next("E0001");
        assert_eq!(w.stats.repaired_partial, 1, "{case}: {:?}", w.stats);
        let landed = case == "lands";
        assert_eq!(c.count("otel_traces", "h0"), if landed { 5 } else { 2 }, "{case}");
        assert_eq!(passed, u64::from(landed), "{case}: passed only when complete");
        assert_eq!(w.stats.unsettled, u64::from(case == "unsettled error"), "{case}: {:?}", w.stats);
    }
}

/// One visibility sample per object inserted that carries its received
/// time: insert returned minus received, exactly; none for an object
/// without; at most a million kept. (Run 54: each of the sample's
/// conditions, its bound and its subtraction survived.)
#[tokio::test(flavor = "current_thread")]
async fn visibility_samples_are_exact_need_a_receive_time_and_are_bounded() {
    let _trace = otap_s3pq::oscope_trace::covers("DST", &["H-2"]);
    let (b, c, clk) = setup();
    let mut stamped = Edge::stamped("c1/p1", "traces", &clk);
    let mut plain = Edge::new("c1/p2", "traces");
    stamped.commit(&b, "h0", 2).await;
    plain.commit(&b, "u0", 2).await;
    let mut w = worker("w1", &b, &c, &clk);
    clk.0.set(clk.0.get() + 1_234);
    let _ = w.step().await;
    assert_eq!(w.stats.objects_inserted, 2);
    assert_eq!(w.stats.visible_ms, vec![1_234.0]);
    w.stats.visible_ms = vec![0.0; 999_999];
    stamped.commit(&b, "h1", 2).await;
    stamped.commit(&b, "h2", 2).await;
    let _ = w.step().await;
    assert_eq!(w.stats.objects_inserted, 4);
    assert_eq!(w.stats.visible_ms.len(), 1_000_000);
}

/// Statements of objects without received time, or of a content a range
/// guard once refused, assert nothing: the guard flag on such a statement
/// changes no SQL, and such objects have no own range, so the verify reads
/// every partition whatever the flag. This is why `guarded`'s `> 0` as
/// `>= 0`, and `guard && !range_err` as `||`, are equivalent (run 54; see
/// ci/mutants-baseline.txt).
#[test]
fn an_unasserted_statement_has_no_own_range() {
    let _trace = otap_s3pq::oscope_trace::covers("DST", &["H-2"]);
    let obj = |content: &str, received_ns: u64| plan::Obj {
        lane: "c1/p1/traces".into(),
        epoch: "E0001".into(),
        seq: 0,
        key: format!("k/{content}"),
        size: 1,
        content: content.into(),
        rows: 2,
        received_ns,
        seen_ms: 0,
        announce: 0,
        payloads: Default::default(),
        late: false,
    };
    let (plain, stamped) = (obj("h0", 0), obj("h1", 7 * DAY_NS));
    let none = std::collections::HashSet::new();
    assert!(plan::own_range(&[&plain], &none, Mutation::None, 0).is_none(), "no received time: no own range");
    assert!(plan::own_range(&[&stamped, &plain], &none, Mutation::None, 0).is_none());
    assert!(plan::own_range(&[&stamped], &none, Mutation::None, 0).is_some());
    let refused: std::collections::HashSet<String> = ["h1".to_string()].into();
    assert!(plan::own_range(&[&stamped], &refused, Mutation::None, 0).is_none(), "refused by a range guard: no own range");
}

// ---- the worker's per-lane memory ----------------------------------------------------------

/// After a compaction answered as written, the lane keeps only live
/// epochs: the one GC retired (above the floor: an older epoch is not
/// retired yet) leaves the epochs it knows and times, and the HEAD cache
/// holds only slots above the checkpoint of open epochs (a closed epoch's
/// tombstone HEAD goes; a lingering batch's stays). (Run 54: the
/// known-epoch pruning's `!` and `&&`, and the HEAD cache pruning's, after
/// an answered write, survived; `a_late_checkpoint_that_compacted_an_epoch_does_not_reopen_it`
/// covers a write taken back late.)
#[tokio::test(flavor = "current_thread")]
async fn an_answered_compaction_forgets_the_epochs_it_dropped() {
    let _trace = otap_s3pq::oscope_trace::covers("DST", &["CAST-75", "H-2"]);
    let (b, c, clk) = setup();
    let lane = "c1/p1/traces";
    let mut e = Edge::new("c1/p1", "traces");
    e.commit(&b, "h0", 3).await; // E0001/0
    e.new_epoch();
    e.commit(&b, "h1", 3).await; // E0002/0
    e.new_epoch();
    e.commit(&b, "h2", 3).await; // E0003/0
    // E0001 and E0002 were closed by another worker's tombstones: the
    // scan HEADs them (and caches them) before the checkpoint closes them.
    let mut tm = BTreeMap::new();
    let _ = tm.insert(proto::META_KIND.to_string(), proto::KIND_TOMB.to_string());
    for ep in ["E0001", "E0002"] {
        let _ = tm.insert(proto::META_EPOCH.to_string(), ep.to_string());
        b.insert(&proto::slot_key(&e.prefix(), ep, 1), Bytes::new(), tm.clone());
    }
    let mut ws = vec![worker("w1", &b, &c, &clk)];
    run(&mut ws, &clk, 4, 500).await;
    let held = ws[0].checkpoint(lane).unwrap().clone();
    assert!(held.closed("E0001") && held.closed("E0002") && !held.closed("E0003"), "{held:?}");
    // A batch HEADed but held back this round (it lingers): its HEAD stays.
    e.commit(&b, "h3", 3).await; // E0003/1
    ws[0].cfg.linger_ms = 1_000;
    // GC retired E0002 (closed; every key deleted); E0001, closed but not
    // retired, keeps the floor below it.
    let gone: Vec<String> = (0..2).map(|s| proto::slot_key(&e.prefix(), "E0002", s)).collect();
    let _ = b.delete(&gone).await;
    let gc = GcDoc { retired: [(lane.to_string(), ["E0002".to_string()].into())].into(), ..Default::default() };
    b.insert(&format!("{CTL}/gc.json"), Bytes::from(serde_json::to_vec(&gc).unwrap()), BTreeMap::new());
    run(&mut ws, &clk, 1, 100).await;
    let ck = ws[0].checkpoint(lane).unwrap().clone();
    assert_eq!(ws[0].stats.epochs_compacted, 1, "{:?}", ws[0].stats);
    assert!(!ck.epochs.contains_key("E0002") && ck.floor.is_empty(), "{ck:?}");
    assert_eq!(c.count("otel_traces", "h3"), 0, "h3 lingers");
    let (known, seen, heads) = ws[0].lane_memory(lane).unwrap();
    assert_eq!(known, vec!["E0001", "E0003"]);
    assert_eq!(seen, vec!["E0001", "E0003"]);
    assert_eq!(heads, vec![("E0003".to_string(), 1)], "only the lingering batch's HEAD");
    // The cached HEAD is used: the next round inserts h3 without a HEAD.
    ws[0].cfg.linger_ms = 0;
    let before = b.counts.head.get();
    run(&mut ws, &clk, 1, 100).await;
    assert_eq!(c.count("otel_traces", "h3"), 3);
    assert_eq!(b.counts.head.get(), before, "no slot HEADed again");
}

/// A checkpoint write taken back late (it closed E0001 at its tombstone
/// and passed E0002/1) prunes the HEAD cache as an answered write does:
/// the closed epoch's tombstone HEAD and the passed slot's go, a lingering
/// batch's stays and is not HEADed again. (Run 54: `refresh_own_ckpt`'s
/// HEAD-cache pruning: its `!`, both `&&` and the `>=` survived.)
#[tokio::test(flavor = "current_thread")]
async fn a_checkpoint_taken_back_late_prunes_the_head_cache() {
    let _trace = otap_s3pq::oscope_trace::covers("DST", &["CAST-50", "H-2"]);
    let (b, c, clk) = setup();
    let lane = "c1/p1/traces";
    let key = format!("{CTL}/ckpt/{lane}.json");
    let mut e = Edge::new("c1/p1", "traces");
    e.commit(&b, "h0", 3).await; // E0001/0
    e.new_epoch();
    e.commit(&b, "h1", 3).await; // E0002/0
    // E0001 is closed by another worker's tombstone.
    let mut tm = BTreeMap::new();
    let _ = tm.insert(proto::META_KIND.to_string(), proto::KIND_TOMB.to_string());
    let _ = tm.insert(proto::META_EPOCH.to_string(), "E0001".to_string());
    b.insert(&proto::slot_key(&e.prefix(), "E0001", 1), Bytes::new(), tm);
    let mut ws = vec![worker("w1", &b, &c, &clk)];
    // Every checkpoint write after the take's fence is lost: E0001's close
    // and E0002's progress stay unwritten, their HEADs cached.
    *b.faults.borrow_mut() = MemFaults { matching: "/ckpt/".into(), drop_every: 1, skip_first: 1, ..Default::default() };
    run(&mut ws, &clk, 2, 100).await;
    e.commit(&b, "h2", 3).await; // E0002/1
    run(&mut ws, &clk, 1, 100).await;
    assert_eq!((c.count("otel_traces", "h1"), c.count("otel_traces", "h2")), (3, 3));
    let held = ws[0].checkpoint(lane).unwrap().clone();
    assert!(!held.closed("E0001"), "the close was not written: {held:?}");
    // A batch HEADed but held back (it lingers).
    e.commit(&b, "h3", 3).await; // E0002/2
    ws[0].cfg.linger_ms = 1_000;
    run(&mut ws, &clk, 1, 100).await;
    let (_, _, heads) = ws[0].lane_memory(lane).unwrap();
    assert!(heads.contains(&("E0001".to_string(), 1)) && heads.contains(&("E0002".to_string(), 2)), "{heads:?}");
    // One of the lost writes lands late: E0001 closed at 1, E0002 passed to 2.
    let mut late = held.bumped(held.lease_epoch);
    late.close("E0001", 1);
    late.advance("E0002", 2);
    b.insert(&key, Bytes::from(serde_json::to_vec(&late).unwrap()), BTreeMap::new());
    let before = b.counts.head.get();
    run(&mut ws, &clk, 1, 100).await;
    assert_eq!(ws[0].stats.ckpt_late_taken, 1, "{:?}", ws[0].stats);
    let (_, _, heads) = ws[0].lane_memory(lane).unwrap();
    assert_eq!(heads, vec![("E0002".to_string(), 2)], "only the lingering batch's HEAD");
    assert_eq!(b.counts.head.get(), before, "and it was not HEADed again");
}

/// A lost checkpoint write that reads back unchanged keeps the worker in
/// doubt even through a step that writes nothing (its batches linger): when
/// the write lands later it is taken back, and the lane goes on past the
/// slots GC deleted under it. (Run 54: the unchanged-read guard replaced by
/// false survived; a step that re-sends the write masks it.)
#[tokio::test(flavor = "current_thread")]
async fn a_lost_checkpoint_write_stays_in_doubt_through_a_quiet_step() {
    let _trace = otap_s3pq::oscope_trace::covers("DST", &["CAST-50", "H-2"]);
    let (b, c, clk) = setup();
    let lane = "c1/p1/traces";
    let mut e = Edge::new("c1/p1", "traces");
    for i in 0..2 {
        e.commit(&b, &format!("h{i}"), 3).await;
    }
    let mut ws = vec![worker("w1", &b, &c, &clk)];
    run(&mut ws, &clk, 3, 100).await;
    *b.faults.borrow_mut() = MemFaults { matching: "/ckpt/".into(), drop_every: 1, hold: true, ..Default::default() };
    for i in 2..4 {
        e.commit(&b, &format!("h{i}"), 3).await;
    }
    run(&mut ws, &clk, 1, 100).await;
    assert_eq!(b.held.borrow().len(), 1, "the write is stuck on the way");
    // A step that writes nothing: its batches (h2, h3 again, above the
    // checkpoint it holds) linger. The read-back finds the checkpoint unchanged.
    ws[0].cfg.linger_ms = 1_000;
    run(&mut ws, &clk, 1, 100).await;
    assert_eq!(b.held.borrow().len(), 1, "nothing written this step");
    // The write lands; GC deletes the slots it passed.
    *b.faults.borrow_mut() = MemFaults::default();
    assert!(matches!(b.land_held(), Some(Put::Ok(_))));
    let gone: Vec<String> = (0..4).map(|s| proto::slot_key(&e.prefix(), "E0001", s)).collect();
    let _ = b.delete(&gone).await;
    e.commit(&b, "h4", 3).await;
    ws[0].cfg.linger_ms = 0;
    run(&mut ws, &clk, 4, 100).await;
    assert_eq!(ws[0].stats.ckpt_late_taken, 1, "{:?}", ws[0].stats);
    assert_eq!(c.count("otel_traces", "h4"), 3, "the lane went on past the slots GC deleted");
    assert_eq!(ws[0].checkpoint(lane).unwrap().next("E0001"), 5);
}

/// While a write of ours is in doubt, a checkpoint another holder wrote
/// (a higher lease epoch) is never taken for ours, and never written over.
/// (Run 54: `ours_landed_late` replaced by true survived.)
#[tokio::test(flavor = "current_thread")]
async fn a_checkpoint_another_holder_wrote_is_not_taken_for_ours() {
    let _trace = otap_s3pq::oscope_trace::covers("DST", &["CAST-50", "H-2"]);
    let (b, c, clk) = setup();
    let lane = "c1/p1/traces";
    let key = format!("{CTL}/ckpt/{lane}.json");
    let mut e = Edge::new("c1/p1", "traces");
    e.commit(&b, "h0", 3).await;
    let mut ws = vec![worker("w1", &b, &c, &clk)];
    run(&mut ws, &clk, 2, 100).await;
    let held = ws[0].checkpoint(lane).unwrap().clone();
    *b.faults.borrow_mut() = MemFaults { matching: "/ckpt/".into(), drop_every: 1, ..Default::default() };
    e.commit(&b, "h1", 3).await;
    run(&mut ws, &clk, 1, 100).await;
    *b.faults.borrow_mut() = MemFaults::default();
    // Another holder's checkpoint: a later lease epoch, further on.
    let mut theirs = held.bumped(held.lease_epoch + 1);
    theirs.advance("E0001", 2);
    b.insert(&key, Bytes::from(serde_json::to_vec(&theirs).unwrap()), BTreeMap::new());
    e.commit(&b, "h2", 3).await;
    run(&mut ws, &clk, 2, 100).await;
    assert_eq!(ws[0].stats.ckpt_late_taken, 0, "{:?}", ws[0].stats);
    let stored: super::super::coord::CkptDoc = serde_json::from_slice(&b.objs.borrow()[&key].body).unwrap();
    assert_eq!(stored, theirs, "never written over");
}

// ---- the tombstone race ----------------------------------------------------------------

/// `tombstone`: what decides the epoch's close is what the HEAD finds after
/// a create that did not plainly succeed. Ours when nothing was there, or
/// when ours applied unheard; another's when a 412 met its tombstone; lost
/// when a batch got the slot; sent again when ours did not land; and
/// unresolved when a 412 met nothing at all (a create racing another that
/// never completed). (Run 54: `Closed(!conflict)`'s `!` and the
/// `Ok(None) if conflict` guard survived: MemBucket never produced the race.)
#[tokio::test(flavor = "current_thread")]
async fn a_tombstone_race_is_decided_by_what_the_head_finds() {
    let _trace = otap_s3pq::oscope_trace::covers("DST", &["H-2"]);
    let pre = format!("{ROOT}/c1/p1/traces");
    let key = proto::slot_key(&pre, "E0001", 0);
    let fresh = |f: MemFaults| {
        let b = MemBucket::default();
        *b.faults.borrow_mut() = f;
        b
    };
    let on = |f: MemFaults| MemFaults { matching: key.clone(), ..f };
    // Nothing there: ours.
    let b = fresh(MemFaults::default());
    assert!(matches!(tombstone(&b, &pre, "E0001", 0).await, TombResult::Closed(true)));
    // Another's tombstone there: a 412, then the HEAD finds it.
    assert!(matches!(tombstone(&b, &pre, "E0001", 0).await, TombResult::Closed(false)));
    // Ours applied, its answer lost: the HEAD finds it.
    let b = fresh(on(MemFaults { ambiguous_every: 1, ..Default::default() }));
    assert!(matches!(tombstone(&b, &pre, "E0001", 0).await, TombResult::Closed(true)));
    assert_eq!(b.counts.put_create.get(), 1);
    // A batch got there first.
    let b = fresh(MemFaults::default());
    b.insert(&key, Bytes::new(), meta("E0001", 0, "h0", 1));
    assert!(matches!(tombstone(&b, &pre, "E0001", 0).await, TombResult::LostToData));
    // Ours dropped on the way (the first matching PUT; puts are counted
    // from 1, every second dropped): not there, so sent again, and ours.
    let b = fresh(on(MemFaults { drop_every: 2, ..Default::default() }));
    b.puts.set(1);
    assert!(matches!(tombstone(&b, &pre, "E0001", 0).await, TombResult::Closed(true)));
    assert_eq!(b.counts.put_create.get(), 2);
    // A 412 with nothing behind it: unresolved, not sent again.
    let b = fresh(on(MemFaults { create_conflict_unstored_every: 1, ..Default::default() }));
    assert!(matches!(tombstone(&b, &pre, "E0001", 0).await, TombResult::Unresolved(w) if w.contains("412")));
    assert_eq!(b.counts.put_create.get(), 1);
}

// ---- giving lanes back --------------------------------------------------------------------

/// A graceful stop gives back every lane none of whose statements can still
/// land, and leaves the lease of one that can to expire; a fair-share
/// rebalance likewise gives back the settled lane, never the other.
/// (Run 54: `release_all` replaced by () survived: only the unsettled case
/// was tested, where nothing is given back either way; and the release
/// counter's `+=` as `*=`.)
#[tokio::test(flavor = "current_thread")]
async fn only_lanes_whose_statements_have_settled_are_given_back() {
    let _trace = otap_s3pq::oscope_trace::covers("DST", &["H-2"]);
    for stop in [true, false] {
        let (b, c, clk) = setup();
        let mut ea = Edge::new("c1/pa", "traces");
        let mut eb = Edge::new("c1/pb", "traces");
        ea.commit(&b, "a0", 3).await;
        let mut w = worker("w1", &b, &c, &clk);
        let _ = w.step().await;
        eb.commit(&b, "b0", 3).await;
        c.lost_answer_every.set(1);
        clk.0.set(clk.0.get() + 100);
        let _ = w.step().await;
        c.lost_answer_every.set(0);
        assert_eq!((w.held_lanes().len(), w.stats.unsettled), (2, 1), "{:?}", w.stats);
        if stop {
            w.release_all().await;
            assert!(!b.keys().contains(&format!("{CTL}/workers/w1.json")), "the heartbeat is gone");
        } else {
            // A second worker: the fair share is one lane each.
            b.insert(&format!("{CTL}/workers/w2.json"), Bytes::from_static(b"{}"), BTreeMap::new());
            clk.0.set(clk.0.get() + 100);
            let _ = w.step().await;
            assert_eq!(w.held_lanes(), vec!["c1/pb/traces".to_string()]);
            assert_eq!(w.stats.lanes_released, 1, "{:?}", w.stats);
        }
        assert!(lease_of(&b, "c1/pa/traces").released(), "pa (settled) given back");
        assert_eq!(lease_of(&b, "c1/pb/traces").owner, "w1", "pb's statement may still land: left to expire");
    }
}

/// In load mode a lane is given back for balance only once held for
/// `min_hold_ms`. (Run 54: `taken_at + min_hold` as `-` survived.)
#[tokio::test(flavor = "current_thread")]
async fn a_lane_is_given_back_for_balance_only_after_its_minimum_hold() {
    let _trace = otap_s3pq::oscope_trace::covers("DST", &["H-2"]);
    let (b, c, clk) = setup();
    let mut ea = Edge::new("c1/pa", "traces");
    let mut eb = Edge::new("c1/pb", "traces");
    ea.commit(&b, "a0", 3).await;
    eb.commit(&b, "b0", 3).await;
    let mut w = Worker::new(scale_cfg("w1"), b.clone(), c.clone(), clk.clone());
    let t0 = clk.0.get();
    let _ = w.step().await;
    assert_eq!(w.held_lanes().len(), 2, "alone, it takes both: {:?}", w.stats);
    // A peer with no load: one lane is above the band now.
    b.insert(&format!("{CTL}/workers/peer.json"), Bytes::from_static(b"{\"load\":0.0}"), BTreeMap::new());
    let mut released = Vec::new();
    for dt in [500, 1_000, 1_500, 1_999, 2_000, 2_500] {
        clk.0.set(t0 + dt);
        let _ = w.step().await;
        released.push(w.stats.lanes_released);
    }
    assert_eq!(released, vec![0, 0, 0, 0, 1, 1], "held 2 s (min_hold_ms) first: {:?}", w.stats);
}

/// A release with no answer stays pending only while our take is stored:
/// once the store holds anything else, it can no longer land (it is
/// conditional on the take's ETag) and is not sent again. If it applied
/// unheard, the next step sends no release, only the take afresh; if the
/// lease object is gone, the lane is taken afresh too. (Nightly 58:
/// `refresh_own_takes`' `e == held.etag` guard replaced by true survived:
/// the lane gone from the store stayed blocked forever.)
#[tokio::test(flavor = "current_thread")]
async fn a_pending_release_ends_once_our_take_is_no_longer_stored() {
    let _trace = otap_s3pq::oscope_trace::covers("DST", &["H-2"]);
    let lane = "c1/p1/traces";
    let key = format!("{CTL}/lease/{lane}.json");
    for gone in [false, true] {
        let (b, c, clk) = setup();
        let mut e = Edge::new("c1/p1", "traces");
        e.commit(&b, "h0", 5).await;
        let mut w = worker("w1", &b, &c, &clk);
        // The heartbeat and the take pass; the three fence attempts and the
        // release are stuck on the way.
        *b.faults.borrow_mut() = MemFaults { matching: "/ctl/".into(), drop_every: 1, hold: true, skip_first: 2, ..Default::default() };
        let t0 = clk.0.get();
        let _ = w.step().await;
        assert!(w.held_lanes().is_empty(), "the fence failed: given back");
        let release = b.held.borrow_mut().drain(..).filter(|(k, ..)| *k == key).collect::<Vec<_>>();
        assert_eq!(release.len(), 1);
        *b.faults.borrow_mut() = MemFaults::default();
        if gone {
            let _ = b.delete(std::slice::from_ref(&key)).await;
        } else {
            b.held.borrow_mut().extend(release);
            assert!(matches!(b.land_held(), Some(Put::Ok(_))), "the release applies, unheard");
        }
        let cas = b.counts.put_cas.get();
        clk.0.set(t0 + 100);
        let _ = w.step().await;
        assert_eq!(w.held_lanes(), vec![lane.to_string()], "taken afresh (lease gone: {gone})");
        if !gone {
            assert_eq!(b.counts.put_cas.get() - cas, 2, "the take and the checkpoint fence; no release sent again");
            assert_eq!(lease_of(&b, lane).epoch, 3, "take (1), release (2), take (3)");
        }
        clk.0.set(t0 + 200);
        let _ = w.step().await;
        assert_eq!(c.count("otel_traces", "h0"), 5);
    }
}
