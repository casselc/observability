//! Worker tests on an in-memory bucket and central with a fake clock:
//! several workers, lease expiry and takeover, zombies, the server-side
//! fence, partial statements, lost answers, GC, and a randomized run.

use super::bucket::{Bucket, Cond, MemBucket, MemFaults, Put};
use super::coord::{Mutation, Timing};
use super::discovery::{Backoff, MemHints};
use super::gc::{GcConfig, gc_step};
use super::plan::DAY_NS;
use super::sql::MemCentral;
use super::worker::{BalanceMode, Config, FakeClock, Worker};
use bytes::Bytes;
use otap_s3pq::proto;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::rc::Rc;

const ROOT: &str = "r/edges";
const CTL: &str = "r/ctl";
const T: Timing = Timing { ttl_ms: 9000, margin_ms: 1000, budget_ms: 3000, slack_ms: 1000, mutation: Mutation::None };

type W = Worker<MemBucket, MemCentral, FakeClock>;

/// The original configuration: count-based fair share, a LIST every poll,
/// no linger (the tests written before the fleet-scale features).
fn cfg(name: &str) -> Config {
    let mut c = Config::new(ROOT, CTL, name);
    c.timing = T;
    c.discover_ms = 0;
    c.lanes_every_ms = 0;
    c.full_list_ms = 0;
    c.quiet_ms = 2000;
    c.limits.max_objects = 4;
    c.backoff = Backoff::off();
    c.poll_ms = 0;
    c.balance.mode = BalanceMode::Count;
    c
}

/// The fleet-scale configuration: load balancing, idle backoff, a linger,
/// the check's partition range with a one-day horizon.
fn scale_cfg(name: &str) -> Config {
    let mut c = cfg(name);
    c.backoff = Backoff { after_ms: 1_000, min_ms: 500, max_ms: 8_000, jitter: 0.2 };
    c.linger_ms = 300;
    c.balance.mode = BalanceMode::Load;
    c.balance.min_hold_ms = 2_000;
    c.balance.loads_every_ms = 1_000;
    c.balance.window_ms = 10_000;
    c.balance.base_weight = 1.0;
    c.horizon_ms = Some(86_400_000);
    c
}

fn worker(name: &str, b: &Rc<MemBucket>, c: &Rc<MemCentral>, clk: &FakeClock) -> W {
    Worker::new(cfg(name), b.clone(), c.clone(), clk.clone())
}

fn meta(epoch: &str, seq: u64, content: &str, rows: u64) -> BTreeMap<String, String> {
    meta_at(epoch, seq, content, rows, 0)
}

/// With `received_ns` > 0, the object's received time (the partition source).
fn meta_at(epoch: &str, seq: u64, content: &str, rows: u64, received_ns: u64) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    if received_ns > 0 {
        let _ = m.insert(proto::META_RECEIVED.to_string(), received_ns.to_string());
    }
    for (k, v) in [
        (proto::META_KIND, proto::KIND_DATA.to_string()),
        (proto::META_EPOCH, epoch.to_string()),
        (proto::META_SEQ, seq.to_string()),
        (proto::META_CONTENT, content.to_string()),
        (proto::META_ROWS, rows.to_string()),
    ] {
        let _ = m.insert(k.to_string(), v);
    }
    m
}

/// A writer lane as the exporter runs it (create-only, halt on a tombstone).
struct Edge {
    producer: String,
    signal: String,
    epoch: String,
    next: u64,
    n_epochs: u32,
    /// Stamps each object's received time (ns) from this clock (ms), if set.
    recv_clock: Option<Rc<std::cell::Cell<u64>>>,
}

impl Edge {
    fn new(producer: &str, signal: &str) -> Self {
        let mut e = Edge { producer: producer.into(), signal: signal.into(), epoch: String::new(), next: 0, n_epochs: 0, recv_clock: None };
        e.new_epoch();
        e
    }
    fn stamped(producer: &str, signal: &str, clk: &FakeClock) -> Self {
        let mut e = Edge::new(producer, signal);
        e.recv_clock = Some(clk.0.clone());
        e
    }
    fn prefix(&self) -> String {
        format!("{ROOT}/{}/{}", self.producer, self.signal)
    }
    fn new_epoch(&mut self) {
        self.n_epochs += 1;
        self.epoch = format!("E{:04}", self.n_epochs);
        self.next = 0;
    }
    /// Commits `content` (rows) in this epoch; on a tombstone, halts and goes
    /// on in a new epoch. Returns where it landed.
    async fn commit(&mut self, b: &MemBucket, content: &str, rows: u64) -> (String, u64) {
        loop {
            let key = proto::slot_key(&self.prefix(), &self.epoch, self.next);
            let recv = self.recv_clock.as_ref().map_or(0, |c| c.get() * 1_000_000);
            let m = meta_at(&self.epoch, self.next, content, rows, recv);
            match b.put(&key, Bytes::from(vec![0u8; 100]), Cond::Create, &m).await {
                Put::Ok(_) => {
                    self.next += 1;
                    return (self.epoch.clone(), self.next - 1);
                }
                _ => match b.head(&key).await.unwrap() {
                    Some(h) if proto::Slot::from_meta(&h) == proto::Slot::Tomb => self.new_epoch(),
                    Some(_) => self.next += 1,
                    None => {}
                },
            }
        }
    }
}

async fn run(ws: &mut [W], clk: &FakeClock, steps: usize, dt: u64) {
    for _ in 0..steps {
        for w in ws.iter_mut() {
            let _ = w.step().await;
        }
        clk.0.set(clk.0.get() + dt);
    }
}

fn setup() -> (Rc<MemBucket>, Rc<MemCentral>, FakeClock) {
    let clk = FakeClock::default();
    clk.0.set(1_000_000);
    // The server's clock is the workers' clock here.
    let c = MemCentral { clock: clk.0.clone(), ..Default::default() };
    let b = Rc::new(MemBucket { clock: clk.0.clone(), ..Default::default() });
    (b, Rc::new(c), clk)
}

#[tokio::test(flavor = "current_thread")]
async fn ingests_in_order_skips_copies_and_closes_dead_epochs() {
    let (b, c, clk) = setup();
    let mut e = Edge::new("c1/p1", "traces");
    for i in 0..5 {
        e.commit(&b, &format!("h{i}"), 10).await;
    }
    // A restart: the new epoch holds a copy of h4 (the sender's resend) and a new batch.
    e.new_epoch();
    e.commit(&b, "h4", 10).await;
    e.commit(&b, "h5", 10).await;
    let mut ws = vec![worker("w1", &b, &c, &clk)];
    run(&mut ws, &clk, 1, 100).await;
    for i in 0..6 {
        assert_eq!(c.count("otel_traces", &format!("h{i}")), 10, "h{i}");
    }
    assert_eq!(c.applied.borrow().len(), 6, "the copy was skipped: {:?}", c.applied.borrow());
    // E0001 is superseded; its head (slot 5) is free: after `quiet` it is tombstoned.
    run(&mut ws, &clk, 30, 100).await;
    let ck = ws[0].checkpoint("c1/p1/traces").unwrap().clone();
    assert!(ck.closed("E0001") && ck.next("E0001") == 5, "{ck:?}");
    assert!(!ck.closed("E0002") && ck.next("E0002") == 2);
    let s = &ws[0].stats;
    assert_eq!((s.tombstones_won, s.epochs_closed, s.dedup_skipped), (1, 1, 1), "{s:?}");
    assert!(s.statements <= 2, "5 + 1 objects in at most 2 statements of 4: {}", s.statements);
    // The dead writer (a zombie) tries its next slot: the tombstone halts it.
    let mut z = Edge { producer: "c1/p1".into(), signal: "traces".into(), epoch: "E0001".into(), next: 5, n_epochs: 5, recv_clock: None };
    let (ep, _) = z.commit(&b, "h9", 10).await;
    assert_eq!(ep, "E0006", "halted and moved on");
}

/// A lease or checkpoint CAS that applied but was answered 412 (an error
/// answer after the write, then object_store's retry met our own new ETag;
/// `tests/ambig_s3.rs` shows the 412 against SeaweedFS). The worker reads the
/// object back and keeps the lane: before the fix every such answer dropped
/// the lane as "lost to another worker" and left it unheld until our own
/// lease expired (TTL + margin).
#[tokio::test(flavor = "current_thread")]
async fn a_412_for_our_own_lease_or_checkpoint_write_keeps_the_lane() {
    let _trace = otap_s3pq::oscope_trace::covers("DST", &["CAST-15", "H-2", "L-1"]);
    let (b, c, clk) = setup();
    *b.faults.borrow_mut() = MemFaults { matching: "/ctl/".into(), own_conflict_every: 2, ..Default::default() };
    let mut e = Edge::new("c1/p1", "traces");
    let mut ws = vec![worker("w1", &b, &c, &clk)];
    for r in 0..6 {
        for i in 0..3 {
            e.commit(&b, &format!("h{r}-{i}"), 3).await;
        }
        run(&mut ws, &clk, 40, 100).await;
    }
    for r in 0..6 {
        for i in 0..3 {
            assert_eq!(c.count("otel_traces", &format!("h{r}-{i}")), 3, "h{r}-{i}");
        }
    }
    let s = &ws[0].stats;
    assert!(s.renewals >= 3 && s.ckpt_writes >= 3, "renewals {} ckpt writes {}", s.renewals, s.ckpt_writes);
    assert_eq!(s.lanes_lost_cas, 0, "a 412 for our own write taken as a lost lane: {s:?}");
    assert_eq!(s.lanes_taken, 1, "{s:?}");
}

/// A lease or checkpoint CAS whose request was lost (never applied, no
/// answer). The read-back finds the version we wrote on, still ours: the
/// worker keeps the lane and retries on that version, inside the window it
/// already had. Before the fix it dropped the lane as taken over
/// (`lanes_lost_cas`) and the lane idled until our own lease expired (the
/// MBT's `designSlow`, seed 0x29e8aebd).
#[tokio::test(flavor = "current_thread")]
async fn a_lost_lease_or_checkpoint_write_keeps_the_lane() {
    let _trace = otap_s3pq::oscope_trace::covers("DST", &["CAST-20", "H-2", "L-1"]);
    let (b, c, clk) = setup();
    *b.faults.borrow_mut() = MemFaults { matching: "/ctl/".into(), drop_every: 3, ..Default::default() };
    let mut e = Edge::new("c1/p1", "traces");
    let mut ws = vec![worker("w1", &b, &c, &clk)];
    for r in 0..6 {
        for i in 0..3 {
            e.commit(&b, &format!("h{r}-{i}"), 3).await;
        }
        run(&mut ws, &clk, 40, 100).await;
        // No stall: each round's batches are in within the round.
        assert_eq!(ws[0].stats.objects_inserted, 3 * (r + 1), "round {r}: {:?}", ws[0].stats);
    }
    for r in 0..6 {
        for i in 0..3 {
            assert_eq!(c.count("otel_traces", &format!("h{r}-{i}")), 3, "h{r}-{i}");
        }
    }
    let s = &ws[0].stats;
    assert!(s.renewals >= 3 && s.ckpt_writes >= 3, "renewals {} ckpt writes {}", s.renewals, s.ckpt_writes);
    assert_eq!(s.lanes_lost_cas, 0, "a lost request taken as a lost lane: {s:?}");
    assert_eq!(s.lanes_taken, 1, "{s:?}");
}

#[tokio::test(flavor = "current_thread")]
async fn gaps_are_never_skipped_nor_tombstoned() {
    let (b, c, clk) = setup();
    let mut e = Edge::new("c1/p1", "logs");
    e.commit(&b, "a", 1).await;
    // slot 1 missing, slot 2 present (a deleted slot, or LIST ahead of HEAD)
    let k2 = proto::slot_key(&e.prefix(), "E0001", 2);
    b.insert(&k2, Bytes::from("x"), meta("E0001", 2, "c", 1));
    let mut e2 = Edge::new("c1/p1", "logs");
    e2.new_epoch(); // a newer epoch exists, so E0001 is superseded
    e2.commit(&b, "d", 1).await;
    let mut ws = vec![worker("w1", &b, &c, &clk)];
    run(&mut ws, &clk, 50, 200).await;
    let ck = ws[0].checkpoint("c1/p1/logs").unwrap().clone();
    assert_eq!((ck.next("E0001"), ck.closed("E0001")), (1, false));
    assert_eq!(c.count("otel_logs", "c"), 0);
    assert_eq!(ws[0].stats.gaps_seen, 1);
    assert!(b.get(&proto::slot_key(&e.prefix(), "E0001", 1)).await.unwrap().is_none(), "no tombstone in the gap");
}

#[tokio::test(flavor = "current_thread")]
async fn takeover_waits_for_expiry_and_fences_the_old_holder() {
    let (b, c, clk) = setup();
    let mut e = Edge::new("c1/p1", "traces");
    e.commit(&b, "h0", 5).await;
    let mut w1 = worker("w1", &b, &c, &clk);
    let mut w2 = worker("w2", &b, &c, &clk);
    let _ = w1.step().await;
    let _ = w2.step().await;
    assert_eq!(w1.held_lanes(), vec!["c1/p1/traces"]);
    assert!(w2.held_lanes().is_empty());
    // w1 stops (a pause): w2 may take the lane only after ttl + margin of
    // seeing the same lease version.
    e.commit(&b, "h1", 5).await;
    for _ in 0..9 {
        clk.0.set(clk.0.get() + 1000);
        let _ = w2.step().await;
    }
    assert!(w2.held_lanes().is_empty(), "9 s < ttl + margin");
    for _ in 0..3 {
        clk.0.set(clk.0.get() + 1000);
        let _ = w2.step().await;
    }
    assert_eq!(w2.held_lanes(), vec!["c1/p1/traces"]);
    assert_eq!(c.count("otel_traces", "h1"), 5);
    // w1 resumes: its own clock says its window is over; it drops the lane
    // without inserting, and its checkpoint CAS would fail anyway.
    e.commit(&b, "h2", 5).await;
    let before = c.applied.borrow().len();
    let _ = w1.step().await;
    assert!(w1.held_lanes().is_empty());
    assert_eq!(w1.stats.lanes_lapsed, 1);
    assert_eq!(c.applied.borrow().len(), before, "the old holder inserted nothing");
    let _ = w2.step().await;
    assert_eq!(c.count("otel_traces", "h2"), 5);
}

/// The soak's finding: a worker paused past its leases drops them on
/// resume; the others are at their fair share (it is alive again), so the
/// lanes must be taken back by the worker itself once they expire.
#[tokio::test(flavor = "current_thread")]
async fn a_worker_retakes_lanes_it_let_lapse() {
    let (b, c, clk) = setup();
    let mut e1 = Edge::new("c1/p1", "traces");
    let mut e2 = Edge::new("c1/p2", "traces");
    e1.commit(&b, "a0", 3).await;
    e2.commit(&b, "b0", 3).await;
    let mut w1 = worker("w1", &b, &c, &clk);
    let mut w2 = worker("w2", &b, &c, &clk);
    for _ in 0..3 {
        let _ = w1.step().await;
        let _ = w2.step().await;
        clk.0.set(clk.0.get() + 500);
    }
    assert_eq!(w1.held_lanes().len() + w2.held_lanes().len(), 2);
    let mine = w1.held_lanes();
    assert_eq!(mine.len(), 1, "one lane each: {:?} / {:?}", mine, w2.held_lanes());
    // w1 pauses past its lease; w2 keeps running but, at its share, doesn't take more.
    for _ in 0..4 {
        clk.0.set(clk.0.get() + 2000);
        let _ = w2.step().await;
    }
    e1.commit(&b, "a1", 3).await;
    e2.commit(&b, "b1", 3).await;
    // w1 resumes: it drops the lapsed lease, then must take it back.
    for _ in 0..20 {
        let _ = w1.step().await;
        let _ = w2.step().await;
        clk.0.set(clk.0.get() + 500);
    }
    assert!(w1.stats.lanes_lapsed >= 1);
    for h in ["a1", "b1"] {
        assert_eq!(c.count("otel_traces", h), 3, "{h} ingested after the pause");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn the_server_fences_a_statement_sent_after_the_window() {
    let (b, c, clk) = setup();
    let mut e = Edge::new("c1/p1", "logs");
    e.commit(&b, "h0", 5).await;
    let mut w1 = worker("w1", &b, &c, &clk);
    let _ = w1.step().await; // takes the lane, ingests h0
    e.commit(&b, "h1", 5).await;
    // The worker's clock stands still (it was paused right after its own
    // check), while the server's has moved past the fence.
    c.skew_ms.set(T.ttl_ms);
    let _ = w1.step().await;
    assert_eq!(c.count("otel_logs", "h1"), 0);
    // the statement, and the verify's retry of the missing object: both no-ops
    assert_eq!(c.fenced.get(), 2);
    assert_eq!(w1.stats.retried_missing, 1);
    assert_eq!(w1.checkpoint("c1/p1/logs").unwrap().next("E0001"), 1, "not advanced past the fenced object");
    // When the worker's own clock catches up it sees the window is over.
    clk.0.set(clk.0.get() + T.ttl_ms);
    let _ = w1.step().await;
    assert!(w1.held_lanes().is_empty() && w1.stats.lanes_lapsed == 1);
}

#[tokio::test(flavor = "current_thread")]
async fn partial_statements_and_lost_answers_are_repaired_by_the_verify() {
    let (b, c, clk) = setup();
    c.partial_every.set(2); // every 2nd statement: only its first object lands
    c.lost_answer_every.set(3); // every 3rd: lands, answer lost
    let mut e = Edge::new("c1/p1", "traces");
    for i in 0..20 {
        e.commit(&b, &format!("h{i}"), 7).await;
    }
    let mut ws = vec![worker("w1", &b, &c, &clk)];
    // (A lost answer leaves its lane alone until the statement has settled:
    // ttl + slack = 10 s after the lease version it was sent under.)
    run(&mut ws, &clk, 200, 500).await;
    for i in 0..20 {
        assert_eq!(c.count("otel_traces", &format!("h{i}")), 7, "h{i}");
    }
    assert_eq!(c.applied.borrow().len(), 20, "each object applied exactly once");
    assert!(ws[0].stats.retried_missing > 0 && ws[0].stats.insert_errors > 0);
    assert!(ws[0].stats.unsettled > 0, "{:?}", ws[0].stats);
    assert_eq!(ws[0].checkpoint("c1/p1/traces").unwrap().next("E0001"), 20);
}

/// Regression (dst_consumer seed 504836, nightly 2026-09-29): a checkpoint
/// write that got no answer, and read back unchanged, landed later. The
/// worker went on from the version it held; GC, which reads the stored
/// checkpoint, deleted the slots between the two; the scan then waited at a
/// "gap" at the old head forever, and nothing after it was ingested. While
/// such a write may still land, the worker reads its checkpoint back and
/// takes it when it is its own (same lease epoch, a later version).
#[tokio::test(flavor = "current_thread")]
async fn a_checkpoint_write_landing_late_is_taken_back() {
    let _trace = otap_s3pq::oscope_trace::covers("DST", &["CAST-50", "H-2"]);
    let (b, c, clk) = setup();
    let lane = "c1/p1/traces";
    let key = format!("{CTL}/ckpt/{lane}.json");
    let mut e = Edge::new("c1/p1", "traces");
    for i in 0..2 {
        e.commit(&b, &format!("h{i}"), 3).await;
    }
    let mut ws = vec![worker("w1", &b, &c, &clk)];
    run(&mut ws, &clk, 3, 100).await;
    assert_eq!(ws[0].checkpoint(lane).unwrap().next("E0001"), 2);
    // Checkpoint writes get no answer and do not apply (yet).
    *b.faults.borrow_mut() = MemFaults { matching: "/ckpt/".into(), drop_every: 1, ..Default::default() };
    for i in 2..4 {
        e.commit(&b, &format!("h{i}"), 3).await;
    }
    run(&mut ws, &clk, 1, 100).await;
    assert_eq!((c.count("otel_traces", "h2"), c.count("otel_traces", "h3")), (3, 3));
    let held = ws[0].checkpoint(lane).unwrap().clone();
    assert_eq!(held.next("E0001"), 2, "the write did not apply");
    let (_, held_etag) = b.get(&key).await.unwrap().unwrap();
    // It lands late: the version the worker wrote (h2, h3 passed) is stored.
    *b.faults.borrow_mut() = MemFaults::default();
    let mut late = held.bumped(held.lease_epoch);
    late.advance("E0001", 4);
    let put = b.put(&key, Bytes::from(serde_json::to_vec(&late).unwrap()), Cond::IfMatch(&held_etag), &BTreeMap::new()).await;
    assert!(matches!(put, Put::Ok(_)), "{put:?}");
    // GC, reading the stored checkpoint, deletes the slots it passed.
    let gone: Vec<String> = (0..4).map(|s| proto::slot_key(&e.prefix(), "E0001", s)).collect();
    let _ = b.delete(&gone).await;
    e.commit(&b, "h4", 3).await;
    run(&mut ws, &clk, 4, 100).await;
    assert_eq!(c.count("otel_traces", "h4"), 3, "the lane went on past the slots GC deleted");
    assert_eq!(ws[0].checkpoint(lane).unwrap().next("E0001"), 5);
    assert_eq!(ws[0].stats.ckpt_late_taken, 1);
    for i in 0..4 {
        assert_eq!(c.count("otel_traces", &format!("h{i}")), 3, "h{i} once");
    }
}

/// A worker holding one lane whose lease renewal (due at 3 s) gets no
/// answer and reads back unchanged, then lands late, before the probe at
/// `probe_ms` (the old window ends at 8 s, the late renewal's at 11 s).
/// Returns the worker, central and the time of the probe.
async fn late_renewal(m: Mutation, probe_ms: u64) -> (W, Rc<MemCentral>, FakeClock, Edge, Rc<MemBucket>, u64) {
    let (b, c, clk) = setup();
    let lane = "c1/p1/traces";
    let mut e = Edge::new("c1/p1", "traces");
    e.commit(&b, "h0", 5).await;
    let mut cf = cfg("w1");
    cf.timing.mutation = m;
    let mut w = Worker::new(cf, b.clone(), c.clone(), clk.clone());
    let t0 = clk.0.get();
    let _ = w.step().await;
    assert_eq!((w.held_lanes(), c.count("otel_traces", "h0")), (vec![lane.to_string()], 5));
    // The renewal (and its retry in the same step) get no answer and do not
    // apply yet: the read-back shows the version of t0.
    *b.faults.borrow_mut() = MemFaults { matching: "/lease/".into(), drop_every: 1, hold: true, ..Default::default() };
    clk.0.set(t0 + 3_000);
    let _ = w.step().await;
    assert_eq!(w.held_lanes(), vec![lane.to_string()], "an unchanged read-back keeps the lane");
    assert!(!b.held.borrow().is_empty(), "the renewal is stuck on the way");
    *b.faults.borrow_mut() = MemFaults::default();
    // The first renewal lands, unheard (the others, on the same version, could only meet a 412).
    assert!(matches!(b.land_held(), Some(Put::Ok(_))));
    b.held.borrow_mut().clear();
    e.commit(&b, "h1", 5).await;
    clk.0.set(t0 + probe_ms);
    let _ = w.step().await;
    (w, c, clk, e, b, t0)
}

/// Regression (STPA.md CAST-74; ../../model/s3InlineConsumer.qnt
/// `lateLeaseLostBreaksTest`): a lease renewal that got no answer, and read
/// back unchanged, landed later. The holder went on counting its window from
/// the older version and lapsed on it (8 s), while the store held its own
/// newer version (sent at 3 s): nobody, itself included, could take the lane
/// before that version was ttl + margin old, and the lane idled ~10 s. Now
/// the holder reads the lease back before its lapse check and adopts its own
/// late renewal (`refresh_own_lease`, `coord::own_late_renewal`).
#[tokio::test(flavor = "current_thread")]
async fn a_lease_renewal_landing_late_is_adopted() {
    let _trace = otap_s3pq::oscope_trace::covers("DST", &["H-2"]);
    let lane = "c1/p1/traces";
    // The design: at 8.5 s (the old window is over) the lane is still held,
    // by the adopted version, and h1 goes in at once.
    let (w, c, ..) = late_renewal(Mutation::None, 8_500).await;
    assert_eq!(w.held_lanes(), vec![lane.to_string()]);
    assert_eq!((w.stats.lease_late_taken, w.stats.lanes_lapsed, w.stats.lanes_lost_cas), (1, 0, 0), "{:?}", w.stats);
    assert_eq!(c.count("otel_traces", "h1"), 5);
    // The code before: it lapses at 8.5 s on the old window, and the lease,
    // its own, first seen changed at 8.5 s, may be taken only 10 s later.
    let (mut w, c, clk, _e, _b, t0) = late_renewal(Mutation::LateRenewalLost, 8_500).await;
    assert!(w.held_lanes().is_empty());
    assert_eq!((w.stats.lease_late_taken, w.stats.lanes_lapsed), (0, 1));
    while clk.0.get() < t0 + 18_000 {
        clk.0.set(clk.0.get() + 500);
        let _ = w.step().await;
        assert_eq!(c.count("otel_traces", "h1"), 0, "the lane idles until the late version is ttl + margin old");
    }
    let mut ws = vec![w];
    run(&mut ws, &clk, 4, 500).await;
    assert_eq!(c.count("otel_traces", "h1"), 5, "taken back once it expired");
}

/// The same with a renewal in between (at 5 s): the code before retried on
/// the old version, met the late one's 412, read back a lease that was
/// neither its retry nor its old version, and dropped the lane, its own
/// (`lateLeaseDropBreaksTest`'s shape). The design adopts it at that
/// maintain's read-back.
#[tokio::test(flavor = "current_thread")]
async fn a_renewal_meeting_our_own_late_renewal_keeps_the_lane() {
    let _trace = otap_s3pq::oscope_trace::covers("DST", &["H-2"]);
    let lane = "c1/p1/traces";
    let (w, c, ..) = late_renewal(Mutation::None, 5_000).await;
    assert_eq!(w.held_lanes(), vec![lane.to_string()]);
    assert_eq!((w.stats.lease_late_taken, w.stats.lanes_lost_cas), (1, 0));
    assert_eq!(c.count("otel_traces", "h1"), 5);
    let (w, c, ..) = late_renewal(Mutation::LateRenewalLost, 5_000).await;
    assert!(w.held_lanes().is_empty());
    assert_eq!((w.stats.lease_late_taken, w.stats.lanes_lost_cas), (0, 1), "{:?}", w.stats);
    assert_eq!(c.count("otel_traces", "h1"), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn series_lane_needs_no_check() {
    let (b, c, clk) = setup();
    let mut e = Edge::new("c1/p1", "metrics_series");
    e.commit(&b, "s0", 3).await;
    e.commit(&b, "s1", 3).await;
    let mut ws = vec![worker("w1", &b, &c, &clk)];
    run(&mut ws, &clk, 2, 100).await;
    assert_eq!(ws[0].stats.series_objects_inserted, 2);
    assert_eq!(ws[0].stats.checks, 0);
    assert_eq!(ws[0].checkpoint("c1/p1/metrics_series").unwrap().next("E0001"), 2);
}

/// Many edge restarts on one lane, with GC retiring closed epochs: the
/// checkpoint keeps only the epochs not retired yet, its floor moves up, the
/// full listing starts after the floor, gc.json stays small, and every batch
/// is ingested exactly once.
#[tokio::test(flavor = "current_thread")]
async fn checkpoint_stays_bounded_across_many_epochs() {
    let (b, c, clk) = setup();
    let mut e = Edge::new("c1/p1", "traces");
    let mut ws = vec![worker("w1", &b, &c, &clk)];
    let gcc = GcConfig { root: ROOT.into(), ctl: CTL.into(), delay_ms: 1000, zombie_ms: 3000, dry_run: false };
    let mut n = 0;
    let (mut max_epochs, mut max_bytes, mut max_gc) = (0, 0, 0);
    for round in 0..120 {
        if round > 0 {
            e.new_epoch(); // an edge restart
        }
        for _ in 0..2 {
            n += 1;
            e.commit(&b, &format!("h{n}"), 3).await;
        }
        for _ in 0..6 {
            run(&mut ws, &clk, 1, 500).await;
            let _ = gc_step(&*b, &gcc, clk.0.get()).await.unwrap();
        }
        if round >= 20 {
            let ck = ws[0].checkpoint("c1/p1/traces").unwrap();
            max_epochs = max_epochs.max(ck.epochs.len());
            max_bytes = max_bytes.max(serde_json::to_vec(ck).unwrap().len());
            max_gc = max_gc.max(b.get(&format!("{CTL}/gc.json")).await.unwrap().unwrap().0.len());
        }
    }
    for i in 1..=n {
        assert_eq!(c.count("otel_traces", &format!("h{i}")), 3, "h{i}");
    }
    assert_eq!(c.applied.borrow().len(), n, "each batch applied once");
    let ck = ws[0].checkpoint("c1/p1/traces").unwrap().clone();
    let s = &ws[0].stats;
    // An epoch lives 3 s here (6 × 500 ms per restart) and retires after
    // quiet (2 s) + the GC delay and zombie bound (3 s): about 3 to 4 at once.
    assert!(max_epochs <= 5, "explicit epochs {max_epochs}: {ck:?}");
    assert!(max_bytes < 600 && max_gc < 6000, "checkpoint {max_bytes} B, gc.json {max_gc} B");
    assert!(ck.floor.as_str() > "E0100", "floor {}", ck.floor);
    assert!(s.epochs_compacted >= 110, "{s:?}");
    // The full listing starts after the floor: a whole-lane LIST of the
    // bucket right now returns only the epochs above it.
    let items = b.list(&format!("{ROOT}/c1/p1/traces"), super::coord::floor_start_after(&format!("{ROOT}/c1/p1/traces"), &ck.floor).as_deref()).await.unwrap();
    let epochs: BTreeSet<String> = items.iter().filter_map(|i| proto::parse_slot_key(&format!("{ROOT}/c1/p1/traces"), &i.key)).map(|(e, _)| e).collect();
    assert!(epochs.len() <= 5 && epochs.iter().all(|x| x.as_str() > ck.floor.as_str()), "{epochs:?}");
}

/// Compacting an epoch before it is closed and retired (the model's
/// `earlyCompact`) loses a batch its writer commits afterwards; the design
/// ingests it.
#[tokio::test(flavor = "current_thread")]
async fn early_compaction_would_skip_a_late_batch() {
    for mutant in [false, true] {
        let (b, c, clk) = setup();
        let mut e1 = Edge::new("c1/p1", "logs");
        e1.commit(&b, "a", 2).await;
        e1.commit(&b, "b", 2).await;
        // A second writer (another exporter lane of the edge) opens a newer
        // epoch: E0001 is superseded while its writer is still alive.
        let mut e2 = Edge::new("c1/p1", "logs");
        e2.new_epoch();
        e2.commit(&b, "c", 2).await;
        let mut cfg = cfg("w1");
        if mutant {
            cfg.timing.mutation = Mutation::EarlyCompact;
        }
        let mut w = Worker::new(cfg, b.clone(), c.clone(), clk.clone());
        let _ = w.step().await;
        // E0001's writer commits one more batch (before `quiet` passes).
        e1.commit(&b, "late", 2).await;
        for _ in 0..10 {
            clk.0.set(clk.0.get() + 100);
            let _ = w.step().await;
        }
        let ck = w.checkpoint("c1/p1/logs").unwrap();
        if mutant {
            assert_eq!(ck.floor, "E0001", "compacted while open");
            assert_eq!(c.count("otel_logs", "late"), 0, "the mutant never ingests the late batch");
        } else {
            assert!(ck.floor.is_empty() && ck.next("E0001") == 3, "{ck:?}");
            assert_eq!(c.count("otel_logs", "late"), 2);
        }
    }
}

/// A write into a retired epoch (a zombie writer outliving the zombie bound,
/// which the design assumes can't happen): at or below the floor it is
/// ignored (never read, so never ingested twice); above the floor (a retired
/// epoch past an epoch still open), the epoch is new again: slot 0 is
/// ingested, a later slot is a gap (reported, never skipped over).
#[tokio::test(flavor = "current_thread")]
async fn writes_into_retired_epochs_are_ignored_or_ingested_once() {
    let (b, c, clk) = setup();
    let prefix = format!("{ROOT}/c1/p1/traces");
    // E0001: closed after one batch. E0002: a gap (slot 1 missing), so it
    // stays open for ever. E0003: closed after one batch. E0004: the newest.
    let mut e = Edge::new("c1/p1", "traces");
    e.commit(&b, "a", 1).await;
    e.new_epoch();
    e.commit(&b, "b", 1).await;
    b.insert(&proto::slot_key(&prefix, "E0002", 2), Bytes::from("x"), meta("E0002", 2, "g", 1));
    e.new_epoch();
    e.commit(&b, "c", 1).await;
    e.new_epoch();
    e.commit(&b, "d", 1).await;
    let mut ws = vec![worker("w1", &b, &c, &clk)];
    let gcc = GcConfig { root: ROOT.into(), ctl: CTL.into(), delay_ms: 1000, zombie_ms: 3000, dry_run: false };
    for _ in 0..40 {
        run(&mut ws, &clk, 1, 500).await;
        let _ = gc_step(&*b, &gcc, clk.0.get()).await.unwrap();
    }
    let ck = ws[0].checkpoint("c1/p1/traces").unwrap().clone();
    assert_eq!(ck.floor, "E0001", "{ck:?}");
    assert!(!ck.epochs.contains_key("E0003"), "retired above the open E0002: dropped too");
    assert!(ck.epochs.contains_key("E0002") && !ck.closed("E0002"));
    // Zombies (bound violated) write into E0001 (below the floor) and E0003 (above it).
    b.insert(&proto::slot_key(&prefix, "E0001", 1), Bytes::from("x"), meta("E0001", 1, "z1", 1));
    b.insert(&proto::slot_key(&prefix, "E0003", 0), Bytes::from("x"), meta("E0003", 0, "z3", 1));
    b.insert(&proto::slot_key(&prefix, "E0003", 1), Bytes::from("x"), meta("E0003", 1, "c", 1));
    let gaps = ws[0].stats.gaps_seen;
    run(&mut ws, &clk, 5, 500).await;
    assert_eq!(c.count("otel_traces", "z1"), 0, "below the floor: never read");
    assert_eq!(c.count("otel_traces", "z3"), 1, "above it: a new epoch, from slot 0");
    assert_eq!(c.count("otel_traces", "c"), 1, "a copy of an ingested batch: skipped by the check");
    assert_eq!(c.applied.borrow().iter().filter(|(_, k)| k == "c").count(), 1);
    assert_eq!(ws[0].stats.gaps_seen, gaps, "no new gap");
}

/// A randomized run: 3 producers × 3 signals, edges restarting (with
/// copies of their last batch in the new epoch), 3 workers taking and
/// losing lanes, crashing (a new incarnation) and pausing past their
/// leases, ambiguous answers on lease and checkpoint writes, statements that
/// half-land or lose their answer, and GC running throughout. At the end,
/// every committed content key is in central exactly once.
#[tokio::test(flavor = "current_thread")]
async fn randomized_fleet() {
    for seed in 1..=12u64 {
        randomized(seed, 60_000, false).await;
    }
}

/// The same fleet with every fleet-scale feature on: load balancing, idle
/// backoff, a linger, the check's partition range (objects carry received
/// times; the run crosses midnight, and copies of a request made after an
/// edge restart carry a later one), and statements that get no answer and
/// land late.
#[tokio::test(flavor = "current_thread")]
async fn randomized_fleet_at_scale() {
    for seed in 1..=12u64 {
        randomized(seed, 60_000, true).await;
    }
}

/// The same, with a zombie bound short enough that GC retires closed epochs
/// and the workers compact their checkpoints throughout (the test's edges
/// abandon an epoch at a restart, so no writer outlives the bound).
#[tokio::test(flavor = "current_thread")]
async fn randomized_fleet_compacting() {
    let mut compacted = 0;
    for seed in 1..=12u64 {
        compacted += randomized(seed, 3_000, seed % 2 == 0).await;
    }
    assert!(compacted > 50, "compaction ran: {compacted} epochs");
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

async fn randomized(seed: u64, zombie_ms: u64, scale: bool) -> u64 {
    let (b, c, clk) = setup();
    if scale {
        // three minutes before midnight (UTC day 20,000)
        clk.0.set(20_000 * 86_400_000 - 180_000);
    }
    *b.faults.borrow_mut() = MemFaults { matching: "/ctl/".into(), ambiguous_every: 7, drop_every: 11, own_conflict_every: 13, hold: false };
    c.partial_every.set(5);
    c.lost_answer_every.set(7);
    c.late_every.set(11);
    c.late_by_ms.set(1500);
    c.slack_ms.set(T.slack_ms);
    let mut rng = Rng(seed * 0x9E37_79B9_7F4A_7C15 + 1);
    let mut edges: Vec<Edge> = Vec::new();
    for p in 0..3 {
        for s in ["traces", "logs", "metrics_gauge"] {
            edges.push(if scale { Edge::stamped(&format!("c{}/p{p}", p % 2), s, &clk) } else { Edge::new(&format!("c{}/p{p}", p % 2), s) });
        }
    }
    let mk = |name: &str| Worker::new(if scale { scale_cfg(name) } else { cfg(name) }, b.clone(), c.clone(), clk.clone());
    let mut committed: HashMap<(String, String), u64> = HashMap::new(); // (table, content) -> rows
    let mut last: HashMap<usize, String> = HashMap::new();
    let mut ws: Vec<W> = (0..3).map(|i| mk(&format!("w{i}-0"))).collect();
    let mut paused: Vec<u64> = vec![0; 3];
    let mut incarn = vec![0u32; 3];
    let gcc = GcConfig { root: ROOT.into(), ctl: CTL.into(), delay_ms: T.ttl_ms + T.margin_ms + 2000, zombie_ms, dry_run: false };
    let mut n = 0u64;
    for _ in 0..1500 {
        let now = clk.0.get();
        match rng.below(20) {
            0..=7 => {
                let i = rng.below(edges.len() as u64) as usize;
                n += 1;
                let content = format!("c{n}");
                let rows = 1 + rng.below(9);
                let table = otap_s3pq::Signal::from_name(&edges[i].signal).unwrap().table().to_string();
                let _ = edges[i].commit(&b, &content, rows).await;
                let _ = committed.insert((table, content.clone()), rows);
                let _ = last.insert(i, content);
            }
            8 => {
                // an edge restart: new epoch, the last (unacked) request resent into it
                let i = rng.below(edges.len() as u64) as usize;
                edges[i].new_epoch();
                if let Some(cn) = last.get(&i).cloned() {
                    let table = otap_s3pq::Signal::from_name(&edges[i].signal).unwrap().table().to_string();
                    let rows = committed[&(table, cn.clone())];
                    let _ = edges[i].commit(&b, &cn, rows).await;
                }
            }
            9 => {
                // a worker crash: a new incarnation (new id)
                let i = rng.below(3) as usize;
                incarn[i] += 1;
                ws[i] = mk(&format!("w{i}-{}", incarn[i]));
            }
            10 => {
                // a pause long enough to lose the lease
                let i = rng.below(3) as usize;
                paused[i] = now + T.ttl_ms + 2000 + rng.below(5000);
            }
            11 => {
                let r = gc_step(&*b, &gcc, now).await.unwrap();
                let _ = r;
            }
            _ => {
                let i = rng.below(3) as usize;
                if paused[i] <= now {
                    let _ = ws[i].step().await;
                }
            }
        }
        clk.0.set(now + 50 + rng.below(400));
    }
    // Quiesce: everyone alive, time moves until nothing is left.
    for _ in 0..400 {
        for w in ws.iter_mut() {
            let _ = w.step().await;
        }
        clk.0.set(clk.0.get() + 500);
    }
    c.flush_late();
    assert!(c.late.borrow().is_empty());
    let mut missing = Vec::new();
    for ((table, content), rows) in &committed {
        if c.count(table, content) != *rows {
            missing.push((table.clone(), content.clone(), *rows, c.count(table, content)));
        }
    }
    let applied: Vec<(String, String)> = c.applied.borrow().clone();
    let uniq: BTreeSet<&(String, String)> = applied.iter().collect();
    let extra: Vec<&(String, String)> = uniq.iter().filter(|k| !committed.contains_key(*k)).cloned().collect();
    let stats: Vec<String> = ws.iter().map(|w| w.stats_json().to_string()).collect();
    assert!(missing.is_empty(), "seed {seed}: not exactly once: {missing:?}\n{stats:#?}");
    assert_eq!(applied.len(), uniq.len(), "seed {seed}: an object was applied twice");
    assert!(extra.is_empty(), "seed {seed}: uncommitted content ingested: {extra:?}");
    let s: u64 = ws.iter().map(|w| w.stats.lanes_taken).sum();
    assert!(s > 0);
    assert!(c.landed_late.get() > 0, "seed {seed}: a late statement landed");
    if scale {
        let rc: u64 = ws.iter().map(|w| w.stats.range_checks).sum();
        assert!(rc > 0 && c.range_checks.get() > 0, "seed {seed}: checks used a range");
    }
    ws.iter().map(|w| w.stats.epochs_compacted).sum()
}

// ---- fleet scale -----------------------------------------------------------------------

/// Idle lanes back off: 1 busy lane and 9 idle ones; the idle lanes are
/// LISTed far less often, a hint brings an idle lane's new object in at the
/// next poll, and with no hint it still comes in within the longest backoff.
#[tokio::test(flavor = "current_thread")]
async fn idle_lanes_back_off_and_hints_wake_them() {
    let (b, c, clk) = setup();
    let mut busy = Edge::new("c1/busy", "traces");
    let mut idle: Vec<Edge> = (0..9).map(|i| Edge::new(&format!("c1/idle{i}"), "traces")).collect();
    for e in idle.iter_mut() {
        e.commit(&b, &format!("{}-0", e.producer), 1).await;
    }
    let mut cf = cfg("w1");
    cf.backoff = Backoff { after_ms: 2_000, min_ms: 1_000, max_ms: 16_000, jitter: 0.2 };
    cf.poll_ms = 200;
    cf.full_list_ms = 60_000;
    let hints = Rc::new(MemHints::default());
    let mut w = Worker::new(cf, b.clone(), c.clone(), clk.clone());
    w.hints = Some(hints.clone());
    // 5 minutes at a 200 ms poll; the busy lane gets an object every poll.
    let mut n = 0;
    for _ in 0..1500 {
        n += 1;
        busy.commit(&b, &format!("b{n}"), 1).await;
        let _ = w.step().await;
        clk.0.set(clk.0.get() + 200);
    }
    let per = |p: &str| w.stats.lists_by_lane.get(&format!("{p}/traces")).copied().unwrap_or(0);
    let busy_lists = per("c1/busy");
    let idle_lists: u64 = (0..9).map(|i| per(&format!("c1/idle{i}"))).sum::<u64>() / 9;
    assert!(busy_lists >= 1400, "the busy lane is listed every poll: {busy_lists}");
    // 300 s at a 16 s cap: about 35 LISTs per idle lane (2 s of grace at
    // every poll, the doubling, then the cap), not 1,500.
    assert!(idle_lists <= 45, "idle lanes back off: {idle_lists} LISTs each");
    for i in 1..=n {
        assert_eq!(c.count("otel_traces", &format!("b{i}")), 1);
    }
    // A hint: the idle lane's new object is in at the next poll.
    idle[3].commit(&b, "idle3-1", 1).await;
    hints.push("c1/idle3/traces");
    let _ = w.step().await;
    assert_eq!(c.count("otel_traces", "idle3-1"), 1, "woken by the hint");
    assert_eq!(w.stats.hint_wakeups, 1);
    // No hint (lost): it still comes in, within the cap (16 s + jitter).
    idle[5].commit(&b, "idle5-1", 1).await;
    let mut waited = 0;
    while c.count("otel_traces", "idle5-1") == 0 {
        clk.0.set(clk.0.get() + 200);
        waited += 200;
        let _ = w.step().await;
        assert!(waited <= 18_000, "an idle lane is listed at least every max backoff");
    }
    // Spurious hints (lanes with nothing new, unknown lanes) change nothing.
    hints.push("c1/idle1/traces");
    hints.push("c1/nope/traces");
    hints.push("garbage");
    let before = c.applied.borrow().len();
    let _ = w.step().await;
    assert_eq!(c.applied.borrow().len(), before);
    assert_eq!(w.stats.hint_new_lanes, 1, "an unknown lane named by a hint is learnt");
}

/// The linger fills statements at a low rate, and bounds the wait.
#[tokio::test(flavor = "current_thread")]
async fn linger_fills_statements_within_its_bound() {
    for linger in [0, 1_000] {
        let (b, c, clk) = setup();
        let mut edges: Vec<Edge> = (0..4).map(|i| Edge::stamped(&format!("c1/p{i}"), "logs", &clk)).collect();
        let mut cf = cfg("w1");
        cf.linger_ms = linger;
        cf.limits.max_objects = 8;
        let mut w = Worker::new(cf, b.clone(), c.clone(), clk.clone());
        let mut n = 0;
        let mut max_wait = 0;
        // one object every 100 ms, round robin over 4 lanes; a 100 ms poll
        let mut first: HashMap<String, u64> = HashMap::new();
        for step in 0..400 {
            if step < 300 {
                n += 1;
                let name = format!("x{n}");
                let _ = edges[n % 4].commit(&b, &name, 1).await;
                let _ = first.insert(name, clk.0.get());
            }
            let _ = w.step().await;
            for (name, t) in first.iter() {
                if c.count("otel_logs", name) == 1 {
                    max_wait = max_wait.max(clk.0.get() - t);
                }
            }
            first.retain(|name, _| c.count("otel_logs", name) == 0);
            clk.0.set(clk.0.get() + 100);
        }
        assert!(first.is_empty());
        let s = &w.stats;
        let per = s.statement_objects as f64 / s.statements as f64;
        if linger == 0 {
            assert!(per < 1.5, "no linger: about one object per statement ({per})");
        } else {
            assert!(per >= 7.0, "the linger fills statements of 8 ({per})");
            assert!(max_wait <= linger + 100, "bounded: {max_wait} ms");
            assert!(s.linger_deferred > 0);
        }
        assert_eq!(c.applied.borrow().len(), 300);
    }
}

/// Load-based balancing: one heavy lane and several light ones over 3
/// workers. The loads end inside the band (or a worker holds only the
/// heavy lane), the lanes stop moving, and everything is ingested once.
#[tokio::test(flavor = "current_thread")]
async fn load_balancing_spreads_weight_and_settles() {
    let (b, c, clk) = setup();
    let mut heavy = Edge::new("c2/heavy", "traces");
    let mut light: Vec<Edge> = (0..8).map(|i| Edge::new(&format!("c{}/l{i}", i % 2), "traces")).collect();
    let mut ws: Vec<W> = (0..3).map(|i| Worker::new(scale_cfg(&format!("w{i}")), b.clone(), c.clone(), clk.clone())).collect();
    let mut n = 0;
    let mut released_at_half = 0;
    for step in 0..1200 {
        n += 1;
        heavy.commit(&b, &format!("h{n}"), 400).await; // 4,000 rows/s at a 100 ms tick
        let i = step % 8;
        light[i].commit(&b, &format!("l{n}"), 20).await; // 25 rows/s per light lane
        for w in ws.iter_mut() {
            let _ = w.step().await;
        }
        clk.0.set(clk.0.get() + 100);
        if step == 600 {
            released_at_half = ws.iter().map(|w| w.stats.lanes_released).sum();
        }
    }
    let released: u64 = ws.iter().map(|w| w.stats.lanes_released).sum();
    assert_eq!(released, released_at_half, "no lane moved in the second minute: settled");
    let held: Vec<Vec<String>> = ws.iter().map(|w| w.held_lanes()).collect();
    let with_heavy = held.iter().position(|h| h.contains(&"c2/heavy/traces".to_string())).expect("the heavy lane is held");
    assert_eq!(held[with_heavy].len(), 1, "the heavy lane's worker holds nothing else: {held:?}");
    let others: Vec<usize> = (0..3).filter(|i| *i != with_heavy).map(|i| held[i].len()).collect();
    assert_eq!(others.iter().sum::<usize>(), 8, "{held:?}");
    assert!(others.iter().all(|x| (3..=5).contains(x)), "the light lanes split evenly: {held:?}");
    run(&mut ws, &clk, 30, 100).await;
    let applied = c.applied.borrow().len();
    assert_eq!(applied, 2 * n, "each object once");
    for i in 1..=n {
        assert_eq!(c.count("otel_traces", &format!("h{i}")), 400);
    }
}

/// A statement with no answer lands late (after the worker gave up on it,
/// up to its fence + budget + slack). The worker neither re-checks nor
/// retries nor releases the lane before it has settled, so nothing is
/// ingested twice; the `ReleaseInFlight` mutant (acting at once) does.
#[tokio::test(flavor = "current_thread")]
async fn an_unanswered_statement_is_waited_out() {
    for mutant in [false, true] {
        let (b, c, clk) = setup();
        c.late_every.set(1);
        c.late_by_ms.set(1000);
        c.slack_ms.set(T.slack_ms);
        let mut e = Edge::new("c1/p1", "traces");
        e.commit(&b, "h0", 5).await;
        let mut cf = cfg("w1");
        if mutant {
            cf.timing.mutation = Mutation::ReleaseInFlight;
        }
        let mut w1 = Worker::new(cf.clone(), b.clone(), c.clone(), clk.clone());
        let _ = w1.step().await; // takes the lane, sends: no answer
        assert_eq!(w1.stats.unsettled, 1);
        c.late_every.set(0);
        // The worker keeps polling; then stops gracefully (release_all).
        for _ in 0..3 {
            clk.0.set(clk.0.get() + 300);
            let _ = w1.step().await;
        }
        w1.release_all().await;
        // Another worker: it may take the lane only once the lease is
        // released (the mutant) or expired (the design).
        let mut w2 = Worker::new(cfg("w2"), b.clone(), c.clone(), clk.clone());
        for _ in 0..80 {
            clk.0.set(clk.0.get() + 250);
            let _ = w2.step().await;
        }
        c.flush_late();
        let n = c.count("otel_traces", "h0");
        if mutant {
            assert_eq!(n, 10, "the mutant re-inserts while the first statement is still to land");
        } else {
            assert_eq!(n, 5, "exactly once");
            assert_eq!(c.landed_late.get(), 1);
            assert!(w1.stats.deferred_unsettled > 0 && w1.stats.retried_missing == 0, "{:?}", w1.stats);
        }
    }
}

/// A statement answered with TIMEOUT_EXCEEDED whose commit was still
/// resolving in Keeper, and lands afterwards (measured on the replicated
/// central: 19 s past max_execution_time). The worker leaves its lane alone
/// until the statement has settled, then verifies: once. The `ErrorSettles`
/// mutant (an error answer ends the statement, the code before the fix)
/// verifies at once, finds the objects missing, inserts them again, and the
/// first statement lands too.
#[tokio::test(flavor = "current_thread")]
async fn an_error_answer_whose_commit_is_still_resolving_is_waited_out() {
    let _trace = otap_s3pq::oscope_trace::covers("DST", &["CAST-2", "H-2", "UCA-5", "LS-2"]);
    for mutant in [false, true] {
        let (b, c, clk) = setup();
        c.late_error_every.set(1);
        c.slack_ms.set(T.slack_ms);
        let mut e = Edge::new("c1/p1", "logs");
        e.commit(&b, "h0", 5).await;
        e.commit(&b, "h1", 7).await;
        let mut cf = cfg("w1");
        if mutant {
            cf.timing.mutation = Mutation::ErrorSettles;
        }
        let mut w = Worker::new(cf, b.clone(), c.clone(), clk.clone());
        let _ = w.step().await; // takes the lane, sends: TIMEOUT_EXCEEDED
        c.late_error_every.set(0);
        for _ in 0..80 {
            clk.0.set(clk.0.get() + 250);
            let _ = w.step().await;
        }
        c.flush_late();
        let (n0, n1) = (c.count("otel_logs", "h0"), c.count("otel_logs", "h1"));
        if mutant {
            assert!(n0 > 5 && n1 > 7, "the mutant inserts again while the first statement is still to land: {n0}, {n1}");
        } else {
            assert_eq!((n0, n1), (5, 7), "exactly once");
            assert_eq!(c.landed_late.get(), 1);
            assert!(w.stats.unsettled == 1 && w.stats.deferred_unsettled > 0 && w.stats.retried_missing == 0, "{:?}", w.stats);
        }
    }
}

/// Which error answers end a statement at once.
#[test]
fn error_answers_that_settle() {
    use super::sql::{error_code, settles_at_once};
    let e = |c: u32, name: &str| format!("clickhouse 500 Internal Server Error: Code: {c}. DB::Exception: x. ({name}) (version 26.10.1.618 (official build))");
    assert_eq!(error_code(&e(159, "TIMEOUT_EXCEEDED")), Some(159));
    for (c, n) in [(159, "TIMEOUT_EXCEEDED"), (999, "KEEPER_EXCEPTION"), (319, "UNKNOWN_STATUS_OF_INSERT"), (242, "TABLE_IS_READ_ONLY"), (499, "S3_ERROR"), (241, "MEMORY_LIMIT_EXCEEDED")] {
        assert!(!settles_at_once(&e(c, n)), "{c} may come with a commit still resolving");
    }
    for (c, n) in [(62, "SYNTAX_ERROR"), (60, "UNKNOWN_TABLE"), (252, "TOO_MANY_PARTS"), (202, "TOO_MANY_SIMULTANEOUS_QUERIES")] {
        assert!(settles_at_once(&e(c, n)), "{c} is raised before anything is written");
    }
    assert!(settles_at_once(&format!("clickhouse 500: Code: 395. DB::Exception: {}: a row's received_at differs", super::sql::RANGE_GUARD)));
    assert!(!settles_at_once("clickhouse: error sending request"), "no code: not provably over");
}

fn at(day: u64, h: u64, m: u64) -> u64 {
    day * DAY_NS + (h * 60 + m) * 60_000_000_000
}

/// Commits an object with an explicit received time (ns).
async fn put_obj(b: &MemBucket, lane: &str, epoch: &str, seq: u64, content: &str, rows: u64, recv: u64) {
    let key = proto::slot_key(&format!("{ROOT}/{lane}"), epoch, seq);
    b.insert(&key, Bytes::from(vec![0u8; 100]), meta_at(epoch, seq, content, rows, recv));
}

/// DECISIONS.md D34: an edge's late parts (`oscope-part: late`) reach
/// central flagged `late` (the `late_part` column) and in statements of
/// their own, bulk objects likewise, each exactly once; the `MixLateParts`
/// mutant puts them together.
#[tokio::test(flavor = "current_thread")]
async fn late_parts_get_statements_of_their_own() {
    for mutant in [false, true] {
        let (b, c, clk) = setup();
        let mut cf = cfg("w1");
        cf.limits.max_objects = 8;
        if mutant {
            cf.timing.mutation = Mutation::MixLateParts;
        }
        let mut w = Worker::new(cf, b.clone(), c.clone(), clk.clone());
        // a split request is a bulk object then its late part, in one lane
        for seq in 0..10u64 {
            let key = proto::slot_key(&format!("{ROOT}/c1/p1/logs"), "E1", seq);
            let mut m = meta_at("E1", seq, &format!("k{seq}"), 3, at(20_020, 10, 0));
            let part = if seq % 2 == 1 { proto::PART_LATE } else { proto::PART_BULK };
            let _ = m.insert(proto::META_PART.to_string(), part.to_string());
            b.insert(&key, Bytes::from(vec![0u8; 100]), m);
        }
        for _ in 0..4 {
            let _ = w.step().await;
            clk.0.set(clk.0.get() + 200);
        }
        for seq in 0..10u64 {
            assert_eq!(c.count("otel_logs", &format!("k{seq}")), 3, "exactly once");
            let late = seq % 2 == 1;
            assert_eq!(c.by_part.borrow().get(&("otel_logs".to_string(), format!("k{seq}"), late)).copied(), Some(3), "k{seq} with late = {late}");
        }
        assert_eq!(c.mixed_statements.get() > 0, mutant, "mixed statements (mutant {mutant})");
    }
}

/// The check's partition range comes from the objects: a batch spanning a
/// day boundary, and objects received days before they are ingested (with
/// an earlier attempt already in central), are checked correctly; the
/// `WallRange` mutant (today's partition, from the worker's clock) ingests
/// the old object twice.
#[tokio::test(flavor = "current_thread")]
async fn the_check_range_follows_the_data() {
    for mutant in [false, true] {
        let (b, c, clk) = setup();
        clk.0.set(at(20_010, 12, 0) / 1_000_000); // the worker's clock: day 20,010, noon
        let mut cf = cfg("w1");
        cf.horizon_ms = Some(3_600_000); // one hour: the day boundary must come from the data
        if mutant {
            cf.timing.mutation = Mutation::WallRange;
        }
        // Spanning a day boundary: 23:59 on day 20,009 and 00:01 on day 20,010.
        put_obj(&b, "c1/p1/logs", "E1", 0, "span-a", 4, at(20_009, 23, 59)).await;
        put_obj(&b, "c1/p1/logs", "E1", 1, "span-b", 4, at(20_010, 0, 1)).await;
        // Old: received five days ago; an earlier attempt (a worker that
        // crashed before its checkpoint write) already put its rows in.
        put_obj(&b, "c1/p2/logs", "E1", 0, "old", 6, at(20_005, 8, 0)).await;
        let old = super::plan::Obj {
            lane: "c1/p2/logs".into(),
            epoch: "E1".into(),
            seq: 0,
            key: proto::slot_key(&format!("{ROOT}/c1/p2/logs"), "E1", 0),
            size: 100,
            content: "old".into(),
            rows: 6,
            received_ns: at(20_005, 8, 0),
            seen_ms: 0,
            announce: 0,
            payloads: Default::default(),
            late: false,
        };
        let k = super::sql::LaneKind::for_signal("logs").unwrap();
        let f = super::sql::Fence { wall_ms: u64::MAX, budget_ms: 1 };
        use super::sql::Central;
        c.insert(&k, &[&old], f, "t", true).await.unwrap();
        // The span batch's first attempt landed only its first object (a partial statement).
        c.partial_every.set(1);
        let mut w = Worker::new(cf, b.clone(), c.clone(), clk.clone());
        for _ in 0..6 {
            let _ = w.step().await;
            c.partial_every.set(0);
            clk.0.set(clk.0.get() + 200);
        }
        let s = &w.stats;
        if mutant {
            assert!(c.count("otel_logs", "old") >= 12, "the mutant misses the day-old rows and inserts them again");
        } else {
            for (x, r) in [("span-a", 4), ("span-b", 4), ("old", 6)] {
                assert_eq!(c.count("otel_logs", x), r, "{x}: {s:?}");
            }
            assert_eq!(c.applied.borrow().iter().filter(|(_, k)| k == "old").count(), 1);
            assert!(s.range_checks > 0 && s.full_checks == 0, "every check used a range: {s:?}");
            assert_eq!(c.full_checks.get(), 0);
        }
    }
}

/// A copy of a request, received later (after an edge restart, in a new
/// epoch), is found by the check within the horizon; an object whose rows
/// don't carry its metadata's received time trips the insert's assertion
/// and is checked over every partition from then on; both exactly once.
#[tokio::test(flavor = "current_thread")]
async fn copies_within_the_horizon_and_lying_metadata() {
    let (b, c, clk) = setup();
    clk.0.set(at(20_020, 10, 0) / 1_000_000);
    let mut cf = cfg("w1");
    cf.horizon_ms = Some(86_400_000);
    let mut w = Worker::new(cf, b.clone(), c.clone(), clk.clone());
    // The original, received 23:50 on day 20,019, ingested.
    put_obj(&b, "c1/p1/traces", "E1", 0, "req", 5, at(20_019, 23, 50)).await;
    for _ in 0..3 {
        let _ = w.step().await;
        clk.0.set(clk.0.get() + 200);
    }
    assert_eq!(c.count("otel_traces", "req"), 5);
    // Its copy: resent after the edge restarted, received 20 hours later.
    put_obj(&b, "c1/p1/traces", "E2", 0, "req", 5, at(20_020, 19, 50)).await;
    // An object whose rows carry received times other than its metadata's (a foreign producer).
    put_obj(&b, "c1/p1/traces", "E2", 1, "liar", 4, at(20_020, 9, 0)).await;
    let _ = c.true_recv.borrow_mut().insert(proto::slot_key(&format!("{ROOT}/c1/p1/traces"), "E2", 1), vec![at(20_018, 9, 0), at(20_019, 9, 0)]);
    for _ in 0..6 {
        let _ = w.step().await;
        clk.0.set(clk.0.get() + 200);
    }
    assert_eq!(c.count("otel_traces", "req"), 5, "the copy was found within the horizon");
    assert_eq!(c.count("otel_traces", "liar"), 4, "exactly once");
    assert_eq!(w.stats.range_guard_failures, 1);
    assert!(w.stats.full_checks > 0, "the liar was verified over every partition");
    // A fresh worker (another holder) doesn't know about the liar: its guarded insert
    // fails again and nothing is written twice.
    let _ = c.rows.borrow_mut().insert(("otel_traces".into(), "liar".into()), 4);
    let ck = w.checkpoint("c1/p1/traces").unwrap().clone();
    assert_eq!(ck.next("E2"), 2);
}

/// The default copy horizon is 3 days: a copy received 2.5 days after its
/// original is found by the check and skipped; one received 4 days after is
/// out of the check's reach and is ingested twice. That second case is what
/// `consume horizon-audit` exists to report (`audit.rs`).
#[tokio::test(flavor = "current_thread")]
async fn the_default_horizon_is_three_days() {
    let (b, c, clk) = setup();
    let cf = cfg("w1");
    assert_eq!(Config::new(ROOT, CTL, "x").horizon_ms, Some(3 * 86_400_000));
    clk.0.set(at(20_024, 12, 0) / 1_000_000);
    let mut w = Worker::new(Config { horizon_ms: Config::new(ROOT, CTL, "x").horizon_ms, ..cf }, b.clone(), c.clone(), clk.clone());
    // Two originals, ingested: one received on day 20,020 at noon, one on day 20,021 at noon.
    put_obj(&b, "c1/p1/traces", "E1", 0, "far", 5, at(20_020, 12, 0)).await;
    put_obj(&b, "c1/p1/traces", "E1", 1, "near", 5, at(20_021, 12, 0)).await;
    for _ in 0..3 {
        let _ = w.step().await;
        clk.0.set(clk.0.get() + 200);
    }
    assert_eq!((c.count("otel_traces", "far"), c.count("otel_traces", "near")), (5, 5));
    // Their copies, resent into a new epoch: "near" 2.5 days later, "far" 4 days later.
    put_obj(&b, "c1/p1/traces", "E2", 0, "near", 5, at(20_024, 0, 0)).await;
    put_obj(&b, "c1/p1/traces", "E2", 1, "far", 5, at(20_024, 12, 0)).await;
    for _ in 0..4 {
        let _ = w.step().await;
        clk.0.set(clk.0.get() + 200);
    }
    assert_eq!(c.count("otel_traces", "near"), 5, "a copy within 3 days is found and skipped");
    assert_eq!(c.count("otel_traces", "far"), 10, "a copy 4 days later is out of the check's reach: ingested twice");
    assert_eq!(w.checkpoint("c1/p1/traces").unwrap().next("E2"), 2);
}

// ---- format v2 ---------------------------------------------------------------------

/// The format marker: created when absent, accepted when it says 2, and a
/// bucket of another format is refused (../FORMAT.md §5).
#[tokio::test(flavor = "current_thread")]
async fn the_format_marker_is_created_once_and_another_format_refused() {
    let (b, _, _) = setup();
    super::ensure_format(&*b, CTL).await.unwrap();
    let (body, _) = b.get(&super::format_key(CTL)).await.unwrap().unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["format"], 2);
    super::ensure_format(&*b, CTL).await.unwrap();
    let other = "r/v1ctl";
    b.insert(&super::format_key(other), Bytes::from(r#"{"format":1}"#), BTreeMap::new());
    assert!(super::ensure_format(&*b, other).await.unwrap_err().contains("format Some(1)"));
}

/// A version-1 key (`{root}/{producer}/{signal}/{epoch}/{seq}`) is not a
/// lane at depth 3: its third segment is an epoch, not a signal. Two clusters
/// with a producer of the same name are two lanes.
#[tokio::test(flavor = "current_thread")]
async fn v1_keys_are_invisible_and_clusters_separate_lanes() {
    let (b, c, clk) = setup();
    b.insert(&proto::slot_key(&format!("{ROOT}/p1/traces"), "E0001", 0), Bytes::from("x"), meta("E0001", 0, "v1", 3));
    let mut a = Edge::new("c1/p1", "traces");
    let mut z = Edge::new("c2/p1", "traces");
    a.commit(&b, "a", 2).await;
    z.commit(&b, "z", 2).await;
    let mut w = worker("w1", &b, &c, &clk);
    for _ in 0..4 {
        let _ = w.step().await;
        clk.0.set(clk.0.get() + 100);
    }
    assert_eq!(w.held_lanes(), vec!["c1/p1/traces", "c2/p1/traces"]);
    assert_eq!((c.count("otel_traces", "a"), c.count("otel_traces", "z"), c.count("otel_traces", "v1")), (2, 2, 0), "rows");
}

// ---- complete_through (../../FORMAT.md §3, ../../model/completeness.qnt) ------------------

/// How the test edge sets an object's `oscope-low`.
#[derive(Clone, Copy, PartialEq)]
enum LowMode {
    /// The design: min(send time, received_at over the edge's custody).
    Custody,
    /// completeness.qnt's `lastReceived` mutant: the object's own received_at.
    OwnReceived,
    /// completeness.qnt's `noBirth`: no birth heartbeats, lanes appear with their first data.
    NoBirth,
    /// The consumer's `maxNotPrefix`: the lane watermark ignores the pending requests.
    WmIgnoresPending,
}

/// One publisher with a durable buffer (custody survives its crashes),
/// sending any buffered request (not oldest first), heartbeating idle lanes,
/// and leaving zombie PUTs behind when it crashes.
struct CustodyEdge {
    producer: String,
    signals: Vec<&'static str>,
    /// (id, signal, received_ns): acknowledged to the sender, not yet seen committed.
    buffer: Vec<(u64, &'static str, u64)>,
    /// Per (signal, writer lane): two writer lanes per signal share the custody
    /// (an exporter's `lanes: 2`), each with its own epoch.
    epoch: HashMap<(&'static str, u8), (String, u64)>,
    n_epochs: u32,
    last_commit: HashMap<&'static str, u64>,
    /// PUTs sent by an earlier incarnation, still in flight.
    zombies: Vec<(String, BTreeMap<String, String>)>,
    mode: LowMode,
}

impl CustodyEdge {
    fn new(producer: &str, signals: Vec<&'static str>, mode: LowMode) -> Self {
        let mut e = CustodyEdge { producer: producer.into(), signals, buffer: Vec::new(), epoch: HashMap::new(), n_epochs: 0, last_commit: HashMap::new(), zombies: Vec::new(), mode };
        e.restart();
        e
    }
    fn restart(&mut self) {
        self.n_epochs += 1;
        for s in self.signals.clone() {
            for l in 0..2u8 {
                let _ = self.epoch.insert((s, l), (format!("E{:04}{}", self.n_epochs, l), 0));
            }
        }
    }
    fn low(&self, now_ns: u64, own: Option<u64>) -> u64 {
        match (self.mode, own) {
            (LowMode::OwnReceived, Some(r)) => r,
            _ => self.buffer.iter().map(|q| q.2).fold(now_ns, u64::min),
        }
    }
    fn meta(&self, s: &'static str, l: u8, content: &str, kind: &str, recv: u64, low: u64) -> BTreeMap<String, String> {
        let (e, n) = &self.epoch[&(s, l)];
        let mut m = meta_at(e, *n, content, u64::from(kind == proto::KIND_DATA), recv);
        let _ = m.insert(proto::META_KIND.into(), kind.into());
        let _ = m.insert(proto::META_LOW.into(), low.to_string());
        m
    }
    /// A create-only PUT at the lane's next slot; `hold`: the PUT stays in
    /// flight, its answer never comes (the caller crashes the edge). Returns
    /// whether it committed (a tombstone moves the lane to a new epoch).
    #[allow(clippy::too_many_arguments)]
    async fn put(&mut self, b: &MemBucket, s: &'static str, l: u8, content: &str, kind: &str, recv: u64, low: u64, hold: bool) -> bool {
        loop {
            let (e, n) = self.epoch[&(s, l)].clone();
            let key = proto::slot_key(&format!("{ROOT}/{}/{s}", self.producer), &e, n);
            let m = self.meta(s, l, content, kind, recv, low);
            if hold {
                self.zombies.push((key, m));
                return false;
            }
            match b.put(&key, Bytes::from(vec![0u8; 10]), Cond::Create, &m).await {
                Put::Ok(_) => {
                    self.epoch.get_mut(&(s, l)).unwrap().1 += 1;
                    return true;
                }
                _ => match b.head(&key).await.unwrap() {
                    Some(h) if proto::Slot::from_meta(&h) == proto::Slot::Tomb => {
                        let _ = self.epoch.insert((s, l), (format!("{e}h{n}"), 0));
                    }
                    Some(_) => self.epoch.get_mut(&(s, l)).unwrap().1 += 1,
                    None => {}
                },
            }
        }
    }
}

/// The derivation, randomized against the real worker and the published
/// watermark (completeness.qnt's `completeSound`): after every publication,
/// every request received before `complete_through` is in central. Two
/// clusters, two lanes each, requests sent in any order, edge crashes that
/// leave zombie PUTs landing late in a superseded epoch (the recomputed
/// minimum dips; the published one holds), heartbeats, lost and partial
/// statements. Returns (violations, regressions seen, the final watermark).
async fn completeness_run(seed: u64, mode: LowMode) -> (Vec<String>, u64, u64) {
    let (b, c, clk) = setup();
    c.partial_every.set(5);
    c.lost_answer_every.set(7);
    let mut rng = Rng(seed * 0x9E37_79B9_7F4A_7C15 + 7);
    let wcfg = super::watermark::WmConfig { skew_ms: 0, ..super::watermark::WmConfig::new(ROOT, CTL) };
    let mut edges = vec![CustodyEdge::new("c1/p0", vec!["traces", "logs"], mode), CustodyEdge::new("c2/p1", vec!["traces", "logs"], mode)];
    let now_ns = |clk: &FakeClock| clk.0.get() * 1_000_000;
    // Births: every lane registered before its edge takes custody.
    if mode != LowMode::NoBirth {
        for e in edges.iter_mut() {
            for s in e.signals.clone() {
                let _ = e.put(&b, s, 0, &format!("beat-{}-{s}-b", e.producer), proto::KIND_BEAT, 0, 0, false).await;
            }
        }
    }
    let mut wc = cfg("w1");
    // A small HEAD budget: a backlog leaves slots unread, in the later epochs.
    wc.max_heads = 3;
    if mode == LowMode::WmIgnoresPending {
        wc.timing.mutation = Mutation::WmIgnoresPending;
    }
    let mut w = Worker::new(wc, b.clone(), c.clone(), clk.clone());
    let mut history: Vec<(String, &'static str, u64, usize)> = Vec::new(); // (content, signal, received_ns, edge)
    let (mut violations, mut regress, mut published) = (Vec::new(), 0u64, 0u64);
    let mut id = 0u64;
    for step in 0..900 {
        let now = now_ns(&clk);
        let i = rng.below(2) as usize;
        match rng.below(18) {
            0..=3 => {
                // a request enters custody
                id += 1;
                let s = if rng.below(2) == 0 { "traces" } else { "logs" };
                edges[i].buffer.push((id, s, now));
                history.push((format!("q{id}"), s, now, i));
            }
            4..=8 if !edges[i].buffer.is_empty() => {
                // any buffered request (not the oldest first), maybe left in flight by a crash
                let k = rng.below(edges[i].buffer.len() as u64) as usize;
                let (qid, s, r) = edges[i].buffer[k];
                let low = edges[i].low(now, Some(r));
                let crash = rng.below(8) == 0;
                let l = rng.below(2) as u8;
                let ok = edges[i].put(&b, s, l, &format!("q{qid}"), proto::KIND_DATA, r, low, crash).await;
                if crash {
                    edges[i].restart();
                } else if ok {
                    edges[i].buffer.remove(k);
                    let _ = edges[i].last_commit.insert(s, now);
                }
            }
            9..=11 if !edges[i].zombies.is_empty() && rng.below(4) == 0 => {
                // a zombie PUT lands (or not: create-only), late
                let (key, m) = edges[i].zombies.remove(0);
                let _ = b.put(&key, Bytes::from(vec![0u8; 10]), Cond::Create, &m).await;
            }
            12 => {
                // heartbeats on the idle lanes
                for s in edges[i].signals.clone() {
                    if edges[i].last_commit.get(s).is_none_or(|t| now >= t + 2_000_000_000) {
                        let low = edges[i].low(now, None);
                        let ok = edges[i].put(&b, s, 0, &format!("beat-{seed}-{step}-{s}"), proto::KIND_BEAT, 0, low, false).await;
                        if ok {
                            let _ = edges[i].last_commit.insert(s, now);
                        }
                    }
                }
            }
            13..=14 => {
                let run = super::watermark::watermark_run(&*b, &wcfg, clk.0.get()).await.unwrap();
                assert!(run.errors.is_empty(), "{:?}", run.errors);
                let d = run.fleet;
                if d.computed_ns < published {
                    regress += 1;
                }
                published = d.complete_through_ns;
                // D29: the finest published value that speaks for each request
                // (its lane's, in its cluster's document) and every coarser
                // one (its signal's and cluster's, fleet-wide and per cluster)
                for (content, s, r, e) in &history {
                    let table = otap_s3pq::Signal::from_name(s).unwrap().table();
                    let (cluster, prod) = edges[*e].producer.split_once('/').unwrap();
                    let cd = run.clusters.iter().find(|x| x.cluster == cluster);
                    let mut bounds = vec![("fleet", published), ("fleet signal", d.signals.get(*s).copied().unwrap_or(d.unlisted_signals_ns))];
                    bounds.extend(d.clusters.get(cluster).map(|v| ("fleet cluster", *v)));
                    if let Some(cd) = cd {
                        bounds.push(("cluster", cd.complete_through_ns));
                        bounds.push(("cluster signal", cd.signals.get(*s).copied().unwrap_or(cd.unlisted_signals_ns)));
                        bounds.extend(cd.lane_wm.get(&format!("{prod}/{s}")).map(|v| ("lane", *v)));
                    }
                    for (what, v) in bounds {
                        if *r < v && c.count(table, content) == 0 {
                            violations.push(format!("seed {seed} step {step}: {content} ({cluster} {s}, received {r}) not ingested below the {what} value {v}"));
                        }
                    }
                }
            }
            _ => {
                let _ = w.step().await;
            }
        }
        clk.0.set(clk.0.get() + 20 + rng.below(300));
    }
    (violations, regress, published)
}

#[tokio::test(flavor = "current_thread")]
async fn complete_through_is_sound_and_advances() {
    let (mut regress, mut advanced) = (0, 0);
    for seed in 1..=20u64 {
        let (v, r, wm) = completeness_run(seed, LowMode::Custody).await;
        assert!(v.is_empty(), "{v:#?}");
        regress += r;
        advanced += u64::from(wm > 1_000_000 * 1_000_000);
    }
    assert!(regress > 0, "a zombie PUT made the recomputed watermark dip (the running max held)");
    assert!(advanced >= 15, "the watermark advanced in most runs: {advanced} of 20");
}

/// D29: one cluster's stalled edge (a request in its custody, never
/// committed) holds the fleet value and its own cluster's, and not another
/// cluster's, which keeps advancing with its heartbeats; the stalled cluster
/// catches up once the request is committed and ingested.
#[tokio::test(flavor = "current_thread")]
async fn a_stalled_cluster_holds_only_its_own_watermark() {
    let (b, c, clk) = setup();
    let wcfg = super::watermark::WmConfig { skew_ms: 0, ..super::watermark::WmConfig::new(ROOT, CTL) };
    let mut e1 = CustodyEdge::new("c1/p0", vec!["logs", "traces"], LowMode::Custody);
    let mut e2 = CustodyEdge::new("c2/p1", vec!["logs"], LowMode::Custody);
    for (e, sigs) in [(&mut e1, vec!["logs", "traces"]), (&mut e2, vec!["logs"])] {
        for s in sigs {
            assert!(e.put(&b, s, 0, &format!("beat-birth-{}-{s}", e.producer), proto::KIND_BEAT, 0, 0, false).await);
        }
    }
    let now_ns = |clk: &FakeClock| clk.0.get() * 1_000_000;
    // c2's edge takes a request into custody and stalls with it
    let r0 = now_ns(&clk);
    e2.buffer.push((1, "logs", r0));
    let mut w = Worker::new(cfg("w1"), b.clone(), c.clone(), clk.clone());
    for i in 0..200u64 {
        if i % 20 == 0 {
            let now = now_ns(&clk);
            for s in ["logs", "traces"] {
                assert!(e1.put(&b, s, 0, &format!("beat-{i}-{s}"), proto::KIND_BEAT, 0, e1.low(now, None), false).await);
            }
        }
        let _ = w.step().await;
        clk.0.set(clk.0.get() + 200);
    }
    let run = super::watermark::watermark_run(&*b, &wcfg, clk.0.get()).await.unwrap();
    assert!(run.errors.is_empty(), "{:?}", run.errors);
    let doc = |c: &str| run.clusters.iter().find(|d| d.cluster == c).unwrap().clone();
    let (d1, d2) = (doc("c1"), doc("c2"));
    let lag_ms = |ns: u64| clk.0.get().saturating_sub(ns / 1_000_000);
    assert!(run.fleet.complete_through_ns <= r0, "the fleet value is held by c2");
    assert!(d2.complete_through_ns <= r0 && d2.signals["logs"] <= r0, "c2 is held by its own edge");
    assert!(lag_ms(d1.complete_through_ns) <= 10_000, "c1 advances: lag {} ms", lag_ms(d1.complete_through_ns));
    assert_eq!(run.fleet.clusters["c1"], d1.complete_through_ns);
    assert!(d1.lane_wm["p0/logs"] >= d1.complete_through_ns && d1.lane_wm["p0/traces"] >= d1.complete_through_ns);
    assert!(d1.holding.iter().all(|l| l.lane.starts_with("c1/")), "a cluster's document names only its lanes: {:?}", d1.holding);
    assert_eq!(d2.holding[0].lane, "c2/p1/logs");
    assert_eq!(run.fleet.holding[0].lane, "c2/p1/logs");
    // the fleet document's signals: logs is held by c2, traces is c1's alone
    assert!(run.fleet.signals["logs"] <= r0 && lag_ms(run.fleet.signals["traces"]) <= 10_000, "{:?}", run.fleet.signals);
    // c2's edge commits its request; once ingested, c2 catches up
    let low = e2.low(now_ns(&clk), Some(r0));
    assert!(e2.put(&b, "logs", 0, "q1", proto::KIND_DATA, r0, low, false).await);
    e2.buffer.clear();
    for i in 0..60u64 {
        if i % 20 == 0 {
            let now = now_ns(&clk);
            assert!(e2.put(&b, "logs", 0, &format!("beat2-{i}"), proto::KIND_BEAT, 0, e2.low(now, None), false).await);
            for s in ["logs", "traces"] {
                assert!(e1.put(&b, s, 0, &format!("beat2-{i}-{s}"), proto::KIND_BEAT, 0, e1.low(now, None), false).await);
            }
        }
        let _ = w.step().await;
        clk.0.set(clk.0.get() + 200);
    }
    assert!(c.count("otel_logs", "q1") > 0, "q1 ingested");
    let run = super::watermark::watermark_run(&*b, &wcfg, clk.0.get()).await.unwrap();
    let d2 = run.clusters.iter().find(|d| d.cluster == "c2").unwrap();
    assert!(d2.complete_through_ns > r0 && run.fleet.complete_through_ns > r0, "c2 and the fleet caught up");
    // --wm-cluster-every: a cluster document written recently is left alone
    // (the fleet document still carries its value)
    let (v1, v2) = (d2.version, run.fleet.version);
    let slow = super::watermark::WmConfig { cluster_every_ms: 60_000, ..wcfg.clone() };
    let again = super::watermark::watermark_run(&*b, &slow, clk.0.get() + 1_000).await.unwrap();
    let d2b = again.clusters.iter().find(|d| d.cluster == "c2").unwrap();
    assert_eq!((d2b.version, again.fleet.version), (v1, v2 + 1));
    // the documents are where FORMAT.md says
    assert!(b.get(&super::watermark::cluster_wm_key(CTL, "c1")).await.unwrap().is_some());
}

/// The mutants the model rejects are caught here as well.
#[tokio::test(flavor = "current_thread")]
async fn complete_through_mutants_break_soundness() {
    let _trace = otap_s3pq::oscope_trace::covers("MU", &["CAST-21", "H-2", "H-4"]);
    for mode in [LowMode::OwnReceived, LowMode::NoBirth, LowMode::WmIgnoresPending] {
        let mut broken = 0;
        for seed in 1..=20u64 {
            broken += usize::from(!completeness_run(seed, mode).await.0.is_empty());
        }
        assert!(broken > 0, "mutant {} went unnoticed", mode as u8);
    }
}

/// Registration and idle lanes: a birth heartbeat makes a lane known (its
/// watermark 0 holds complete_through down) before it has data; heartbeats
/// alone then advance its watermark, without taking the lane off its idle
/// LIST backoff.
#[tokio::test(flavor = "current_thread")]
async fn births_register_lanes_and_heartbeats_advance_idle_ones() {
    let (b, c, clk) = setup();
    let wcfg = super::watermark::WmConfig { skew_ms: 0, ..super::watermark::WmConfig::new(ROOT, CTL) };
    let mut e = CustodyEdge::new("c1/p0", vec!["logs"], LowMode::Custody);
    assert!(e.put(&b, "logs", 0, "beat-birth", proto::KIND_BEAT, 0, 0, false).await);
    let d = super::watermark::watermark_step(&*b, &wcfg, clk.0.get()).await.unwrap();
    assert_eq!((d.lanes, d.complete_through_ns), (1, 0), "a born lane without a watermark holds it at 0");
    let mut cf = cfg("w1");
    cf.backoff = Backoff { after_ms: 1_000, min_ms: 1_000, max_ms: 8_000, jitter: 0.0 };
    cf.poll_ms = 200;
    cf.full_list_ms = 0;
    let mut w = Worker::new(cf, b.clone(), c.clone(), clk.clone());
    let lists0 = w.stats.lane_lists;
    for i in 0..300u64 {
        if i % 25 == 0 {
            let now = clk.0.get() * 1_000_000;
            assert!(e.put(&b, "logs", 0, &format!("beat-{i}"), proto::KIND_BEAT, 0, now, false).await);
        }
        let _ = w.step().await;
        clk.0.set(clk.0.get() + 200);
    }
    let d = super::watermark::watermark_step(&*b, &wcfg, clk.0.get()).await.unwrap();
    let lag_ms = clk.0.get().saturating_sub(d.complete_through_ns / 1_000_000);
    // at most a heartbeat interval (5 s) plus the idle LIST backoff cap (8 s) and a couple of polls behind
    assert!(d.complete_through_ns > 0 && lag_ms <= 16_000, "heartbeats carried the watermark: lag {lag_ms} ms");
    assert!(w.stats.lane_lists - lists0 < 100, "an idle lane with heartbeats backs off: {} LISTs in 300 polls", w.stats.lane_lists - lists0);
    assert_eq!(w.stats.objects_inserted, 0);
}

/// Resource announcements go in before any row of their lane: a lane whose
/// announcement statement fails sits the round out (nothing of it is
/// ingested), other lanes go on, and once the announcements land the rows
/// follow; only the objects that announce anything are read for it.
#[tokio::test(flavor = "current_thread")]
async fn announcements_go_in_before_their_lanes_rows() {
    let (b, c, clk) = setup();
    for (seq, (content, ann)) in [("h0", 1), ("h1", 0), ("h2", 2)].into_iter().enumerate() {
        let key = proto::slot_key(&format!("{ROOT}/c1/p1/logs"), "E0001", seq as u64);
        let mut m = meta("E0001", seq as u64, content, 4);
        let _ = m.insert(proto::META_ANNOUNCE.to_string(), ann.to_string());
        b.insert(&key, Bytes::from(vec![0u8; 100]), m);
    }
    let mut e = Edge::new("c1/p2", "traces");
    let _ = e.commit(&b, "t0", 3).await;
    c.announce_fail_every.set(1); // every announcement statement is refused
    let mut ws = vec![worker("w1", &b, &c, &clk)];
    run(&mut ws, &clk, 3, 100).await;
    assert_eq!(c.count("otel_logs", "h1"), 0, "no row of the lane before its announcements");
    assert_eq!(c.count("otel_traces", "t0"), 3, "other lanes go on");
    assert!(ws[0].stats.announce_deferred > 0 && ws[0].stats.announce_objects == 0, "{:?}", ws[0].stats);
    c.announce_fail_every.set(0);
    run(&mut ws, &clk, 3, 100).await;
    for h in ["h0", "h1", "h2"] {
        assert_eq!(c.count("otel_logs", h), 4, "{h}");
    }
    let ann = c.announced.borrow();
    assert_eq!(ann.len(), 2, "the two announcing objects, and only them: {ann:?}");
    assert_eq!(ws[0].stats.announce_objects, 2);
    assert_eq!(ws[0].checkpoint("c1/p1/logs").unwrap().next("E0001"), 3);
}

/// Payload parts (DECISIONS.md D36, R-L9) go in before any row of their
/// lane, exactly as announcements do: a lane whose payload statement fails
/// sits the round out, other lanes go on, and once the payloads land the
/// rows follow; only the objects that carry payloads are read for it; the
/// dangling check reads only objects whose rows hold references, and finds
/// none. The `RowsBeforePayloads` mutant (the statement after the rows) is
/// caught: rows in central before their payloads, and references dangling.
#[tokio::test(flavor = "current_thread")]
async fn payloads_go_in_before_their_lanes_rows() {
    for mutant in [false, true] {
        let (b, c, clk) = setup();
        // (content, payloads carried, references held): h1 references what
        // h0 carried (the edge's lane cache), h2 carries its own.
        for (seq, (content, carried, refs)) in [("h0", 2, 2), ("h1", 0, 2), ("h2", 1, 3)].into_iter().enumerate() {
            let key = proto::slot_key(&format!("{ROOT}/c1/p1/traces"), "E0001", seq as u64);
            let mut m = meta("E0001", seq as u64, content, 4);
            let _ = m.insert(proto::META_PAYLOADS.to_string(), carried.to_string());
            let _ = m.insert(proto::META_PAYLOAD_REFS.to_string(), refs.to_string());
            b.insert(&key, Bytes::from(vec![0u8; 100]), m);
        }
        let mut e = Edge::new("c1/p2", "logs");
        let _ = e.commit(&b, "l0", 3).await;
        let mut ws = vec![worker("w1", &b, &c, &clk)];
        if mutant {
            ws[0].cfg.timing.mutation = Mutation::RowsBeforePayloads;
            run(&mut ws, &clk, 3, 100).await;
            assert!(c.rows_before_payloads.get() > 0, "the mutant puts rows before their payloads");
            assert!(ws[0].stats.payload_dangling > 0, "and the dangling check sees it: {:?}", ws[0].stats);
            continue;
        }
        c.payload_fail_every.set(1); // every payload statement is refused
        run(&mut ws, &clk, 3, 100).await;
        assert_eq!(c.count("otel_traces", "h1"), 0, "no row of the lane before its payloads");
        assert_eq!(c.count("otel_logs", "l0"), 3, "other lanes go on");
        assert!(ws[0].stats.payload_deferred > 0 && ws[0].stats.payload_objects == 0, "{:?}", ws[0].stats);
        c.payload_fail_every.set(0);
        run(&mut ws, &clk, 3, 100).await;
        for h in ["h0", "h1", "h2"] {
            assert_eq!(c.count("otel_traces", h), 4, "{h}");
        }
        assert_eq!(c.payloads_in.borrow().len(), 2, "the two carrying objects, and only them");
        assert_eq!(ws[0].stats.payload_objects, 2);
        assert_eq!(c.rows_before_payloads.get(), 0);
        assert!(ws[0].stats.dangling_checks > 0 && ws[0].stats.payload_dangling == 0, "{:?}", ws[0].stats);
        assert_eq!(ws[0].checkpoint("c1/p1/traces").unwrap().next("E0001"), 3);
    }
}

// ---- dead-lane retirement (FORMAT.md §3.1, DECISIONS.md D35) ------------------------------

/// Steps the worker `n` times, `dt` ms apart; `beat` commits a heartbeat on
/// each listed (edge, signal) every 10th step, with the edge's custody floor.
async fn steps_with_beats(w: &mut W, b: &MemBucket, clk: &FakeClock, n: u64, beats: &mut [(&mut CustodyEdge, &'static str)]) {
    for i in 0..n {
        if i % 10 == 0 {
            let now = clk.0.get() * 1_000_000;
            for (e, s) in beats.iter_mut() {
                let low = e.low(now, None);
                assert!(e.put(b, s, 0, &format!("beat-{}-{i}", clk.0.get()), proto::KIND_BEAT, 0, low, false).await);
            }
        }
        let _ = w.step().await;
        clk.0.set(clk.0.get() + 200);
    }
}

/// D35 (1): a publisher's orderly close retires its lane once the holder
/// has passed it, on every writer lane; until then (a close on one writer
/// lane only, the other's epoch the newest) the lane holds its cluster. The
/// cluster's `complete_through` then passes the removed publisher, the
/// document names it retired with R, and the other publisher's lane still
/// counts.
#[tokio::test(flavor = "current_thread")]
async fn an_orderly_close_retires_the_lane_and_its_cluster_advances() {
    let (b, c, clk) = setup();
    let wcfg = super::watermark::WmConfig { skew_ms: 0, ..super::watermark::WmConfig::new(ROOT, CTL) };
    let now_ns = |clk: &FakeClock| clk.0.get() * 1_000_000;
    let mut gone = CustodyEdge::new("c1/p0", vec!["logs"], LowMode::Custody);
    let mut live = CustodyEdge::new("c1/p1", vec!["logs"], LowMode::Custody);
    assert!(gone.put(&b, "logs", 0, "birth-p0", proto::KIND_BEAT, 0, 0, false).await);
    assert!(live.put(&b, "logs", 0, "birth-p1", proto::KIND_BEAT, 0, 0, false).await);
    // two requests on the two writer lanes, committed
    for (l, q) in [(0u8, "q1"), (1u8, "q2")] {
        let r = now_ns(&clk);
        let low = gone.low(now_ns(&clk), None);
        assert!(gone.put(&b, "logs", l, q, proto::KIND_DATA, r, low, false).await);
    }
    let mut w = Worker::new(cfg("w1"), b.clone(), c.clone(), clk.clone());
    steps_with_beats(&mut w, &b, &clk, 20, &mut [(&mut live, "logs")]).await;
    // the close on writer lane 0 only: writer lane 1's epoch is the newest
    clk.0.set(clk.0.get() + 50);
    let r0 = now_ns(&clk);
    assert!(gone.put(&b, "logs", 0, "close-0", proto::KIND_CLOSE, 0, gone.low(r0, None), false).await);
    steps_with_beats(&mut w, &b, &clk, 40, &mut [(&mut live, "logs")]).await;
    let ck = w.checkpoint("c1/p0/logs").unwrap().clone();
    assert!(!ck.retired_now(), "a close on one writer lane, another's epoch newer and open: not retired {ck:?}");
    let run = super::watermark::watermark_run(&*b, &wcfg, clk.0.get()).await.unwrap();
    let d1 = run.clusters.iter().find(|d| d.cluster == "c1").unwrap().clone();
    assert!(d1.complete_through_ns <= r0 && d1.retired.is_empty(), "the stopped publisher holds its cluster: {d1:?}");
    // the close on writer lane 1: retired at R = the higher close low
    let r1 = now_ns(&clk);
    assert!(gone.put(&b, "logs", 1, "close-1", proto::KIND_CLOSE, 0, gone.low(r1, None), false).await);
    steps_with_beats(&mut w, &b, &clk, 40, &mut [(&mut live, "logs")]).await;
    let ck = w.checkpoint("c1/p0/logs").unwrap().clone();
    assert!(ck.retired_now() && ck.retired_by == "close" && ck.retired_ns == r1, "{ck:?}");
    assert_eq!(ck.retired_epoch, "E00011");
    assert_eq!(w.stats.lanes_retired, 1);
    assert_eq!((c.count("otel_logs", "q1"), c.count("otel_logs", "q2")), (1, 1));
    let run = super::watermark::watermark_run(&*b, &wcfg, clk.0.get()).await.unwrap();
    let d1 = run.clusters.iter().find(|d| d.cluster == "c1").unwrap().clone();
    assert!(d1.complete_through_ns > r1, "the cluster passed the retired lane: {} <= {r1}", d1.complete_through_ns);
    assert!(clk.0.get().saturating_sub(d1.complete_through_ns / 1_000_000) <= 5_000, "and follows the live publisher");
    assert_eq!(d1.retired, BTreeMap::from([("p0/logs".to_string(), r1)]));
    assert!(d1.holding.iter().all(|l| l.lane == "c1/p1/logs") && run.fleet.retired_lanes == 1, "{d1:?}");
    assert_eq!(d1.lane_wm["p0/logs"], d1.signals["logs"]);
}

/// D35: a retired lane counts again once a later epoch appears (a new
/// incarnation, the same producer); an object below R that central already
/// holds (a replayed copy) is passed as a copy; one it does not hold is
/// quarantined (recorded, never inserted, its slot passed); new requests
/// above R are ingested.
#[tokio::test(flavor = "current_thread")]
async fn a_retired_lane_is_reborn_and_quarantines_below_r() {
    let (b, c, clk) = setup();
    let wcfg = super::watermark::WmConfig { skew_ms: 0, ..super::watermark::WmConfig::new(ROOT, CTL) };
    let now_ns = |clk: &FakeClock| clk.0.get() * 1_000_000;
    let mut e = CustodyEdge::new("c1/p0", vec!["logs"], LowMode::Custody);
    let mut live = CustodyEdge::new("c1/p1", vec!["logs"], LowMode::Custody);
    assert!(e.put(&b, "logs", 0, "birth-p0", proto::KIND_BEAT, 0, 0, false).await);
    let r_q1 = now_ns(&clk);
    assert!(e.put(&b, "logs", 0, "q1", proto::KIND_DATA, r_q1, e.low(r_q1, None), false).await);
    // a request received before the close, never committed: the close
    // below is taken anyway (the operator's or the edge's mistake), and the
    // request is replayed by the next incarnation
    let r_q9 = now_ns(&clk) + 1;
    let r = now_ns(&clk) + 100_000;
    assert!(e.put(&b, "logs", 0, "close-0", proto::KIND_CLOSE, 0, r, false).await);
    // (no clock skew between the incarnations here: the bound is R itself)
    let mut w = Worker::new(Config { quarantine_skew_ms: 0, ..cfg("w1") }, b.clone(), c.clone(), clk.clone());
    steps_with_beats(&mut w, &b, &clk, 30, &mut [(&mut live, "logs")]).await;
    assert!(w.checkpoint("c1/p0/logs").unwrap().retired_now());
    let run = super::watermark::watermark_run(&*b, &wcfg, clk.0.get()).await.unwrap();
    assert_eq!(run.fleet.retired_lanes, 1);
    // the next incarnation: its birth, the replay (q1 a copy, q9 not in central), a new request
    e.restart();
    assert!(e.put(&b, "logs", 0, "birth-p0-2", proto::KIND_BEAT, 0, 0, false).await);
    // a watermark run before the holder saw the birth: the lane counts again (its old watermark)
    let run = super::watermark::watermark_run(&*b, &wcfg, clk.0.get()).await.unwrap();
    assert_eq!(run.fleet.retired_lanes, 0, "a later epoch listed: the lane counts again");
    assert!(e.put(&b, "logs", 0, "q1", proto::KIND_DATA, r_q1, 0, false).await);
    assert!(e.put(&b, "logs", 0, "q9", proto::KIND_DATA, r_q9, 0, false).await);
    let r_q10 = now_ns(&clk) + 200_000;
    assert!(e.put(&b, "logs", 0, "q10", proto::KIND_DATA, r_q10, r_q10, false).await);
    steps_with_beats(&mut w, &b, &clk, 30, &mut [(&mut live, "logs"), (&mut e, "logs")]).await;
    let ck = w.checkpoint("c1/p0/logs").unwrap().clone();
    assert!(!ck.retired_now() && ck.reborn_epoch == "E00020" && ck.retired_ns == r, "{ck:?}");
    assert_eq!(ck.next("E00020"), 7, "every slot passed: birth, q1, q9, q10, three heartbeats");
    assert_eq!((c.count("otel_logs", "q1"), c.count("otel_logs", "q9"), c.count("otel_logs", "q10")), (1, 0, 1));
    assert_eq!((w.stats.below_copies, w.stats.quarantined_objects, w.stats.lanes_reborn), (1, 1, 1), "{:?}", w.stats);
    let (q, _) = super::retire::read(&*b, CTL, "c1/p0/logs").await.unwrap().unwrap();
    assert_eq!(q.objects.len(), 1);
    assert_eq!((q.objects[0].content.as_str(), q.objects[0].received_ns, q.objects[0].seq, q.retired_ns), ("q9", r_q9, 2, r));
    assert_eq!(q.objects[0].key, proto::slot_key(&format!("{ROOT}/c1/p0/logs"), "E00020", 2));
    let run = super::watermark::watermark_run(&*b, &wcfg, clk.0.get()).await.unwrap();
    let d1 = run.clusters.iter().find(|d| d.cluster == "c1").unwrap().clone();
    assert!(d1.retired.is_empty() && d1.lane_wm["p0/logs"] > r_q10, "{d1:?}");
}

/// D35: the quarantine record comes before the checkpoint passes the slot;
/// a record that cannot be written leaves the slot (and the lane) waiting.
#[tokio::test(flavor = "current_thread")]
async fn a_quarantine_that_cannot_be_recorded_holds_its_slot() {
    let (b, c, clk) = setup();
    let mut e = CustodyEdge::new("c1/p0", vec!["logs"], LowMode::Custody);
    assert!(e.put(&b, "logs", 0, "birth", proto::KIND_BEAT, 0, 0, false).await);
    let r = clk.0.get() * 1_000_000 + 1_000;
    assert!(e.put(&b, "logs", 0, "close", proto::KIND_CLOSE, 0, r, false).await);
    let mut w = Worker::new(Config { quarantine_skew_ms: 0, ..cfg("w1") }, b.clone(), c.clone(), clk.clone());
    for _ in 0..10 {
        let _ = w.step().await;
        clk.0.set(clk.0.get() + 200);
    }
    assert!(w.checkpoint("c1/p0/logs").unwrap().retired_now());
    e.restart();
    assert!(e.put(&b, "logs", 0, "birth2", proto::KIND_BEAT, 0, 0, false).await);
    assert!(e.put(&b, "logs", 0, "q9", proto::KIND_DATA, r - 1, 0, false).await);
    *b.faults.borrow_mut() = MemFaults { matching: "quarantine/".into(), drop_every: 1, ..Default::default() };
    for _ in 0..10 {
        let _ = w.step().await;
        clk.0.set(clk.0.get() + 200);
    }
    assert_eq!(w.checkpoint("c1/p0/logs").unwrap().next("E00020"), 1, "the slot waits for its record");
    assert!(w.stats.quarantine_errors > 0 && w.stats.quarantined_objects == 0);
    *b.faults.borrow_mut() = MemFaults::default();
    for _ in 0..5 {
        let _ = w.step().await;
        clk.0.set(clk.0.get() + 200);
    }
    assert_eq!(w.checkpoint("c1/p0/logs").unwrap().next("E00020"), 2);
    assert_eq!((w.stats.quarantined_objects, c.count("otel_logs", "q9")), (1, 0));
}

// ---- consume retire-lane (D35 (2)) ---------------------------------------------------------

fn rcfg(zombie_ms: u64) -> super::retire::RetireCfg {
    super::retire::RetireCfg { root: ROOT.into(), ctl: CTL.into(), zombie_ms, mutation: Mutation::None, dry_run: false }
}

/// D35 (2): `consume retire-lane` refuses without each of its checks, (a)
/// the volume attested deleted with evidence, (b) nothing written for the
/// zombie bound, (c) every slot the lane shows passed by its holder; with
/// them it tombstones the open epoch's head, records the retirement (R =
/// now, the evidence) in the checkpoint and in `{ctl}/retired/…`, and the
/// lane leaves its cluster's minimum.
#[tokio::test(flavor = "current_thread")]
async fn retire_lane_refuses_without_its_evidence_and_retires_with_it() {
    let (b, c, clk) = setup();
    let wcfg = super::watermark::WmConfig { skew_ms: 0, ..super::watermark::WmConfig::new(ROOT, CTL) };
    let now_ns = |clk: &FakeClock| clk.0.get() * 1_000_000;
    let mut dead = CustodyEdge::new("c1/p0", vec!["logs"], LowMode::Custody);
    let mut live = CustodyEdge::new("c1/p1", vec!["logs"], LowMode::Custody);
    assert!(dead.put(&b, "logs", 0, "birth-p0", proto::KIND_BEAT, 0, 0, false).await);
    let r1 = now_ns(&clk);
    assert!(dead.put(&b, "logs", 0, "q1", proto::KIND_DATA, r1, r1, false).await);
    // a request in its custody, its PUT out when it dies (a zombie)
    let r2 = now_ns(&clk) + 1;
    dead.buffer.push((2, "logs", r2));
    assert!(!dead.put(&b, "logs", 0, "q2", proto::KIND_DATA, r2, r1, true).await);
    let mut w = Worker::new(cfg("w1"), b.clone(), c.clone(), clk.clone());
    steps_with_beats(&mut w, &b, &clk, 20, &mut [(&mut live, "logs")]).await;
    let lane = "c1/p0/logs";
    let retire = |ev: &'static str, vol: bool, z: u64| {
        let b = b.clone();
        let clk = clk.clone();
        async move { super::retire::retire_lane(&*b, &rcfg(z), lane, ev, vol, clk.0.get()).await }
    };
    // not a lane; (a) the attestation and its evidence
    assert!(super::retire::retire_lane(&*b, &rcfg(1_000), "c1/p0", "x", true, clk.0.get()).await.unwrap_err().contains("want {cluster}"));
    assert!(retire("x", false, 1_000).await.unwrap_err().contains("(a) the publisher's volume must be deleted"));
    assert!(retire(" ", true, 1_000).await.unwrap_err().contains("(a) --evidence is required"));
    // (b) within the zombie bound of its last object
    assert!(retire("pvc deleted", true, 60_000).await.unwrap_err().contains("(b)"), "within the zombie bound");
    // the zombie lands; past the bound, but not passed by its holder: (c)
    let (key, m) = dead.zombies.pop().unwrap();
    assert!(matches!(b.put(&key, Bytes::from(vec![0u8; 10]), Cond::Create, &m).await, Put::Ok(_)));
    clk.0.set(clk.0.get() + 2_000);
    let err = retire("pvc deleted", true, 1_000).await.unwrap_err();
    assert!(err.contains("(c) the consumer has not passed"), "{err}");
    // the holder ingests the zombie; the lane still holds its cluster (q2's custody... it was committed)
    steps_with_beats(&mut w, &b, &clk, 10, &mut [(&mut live, "logs")]).await;
    assert_eq!(c.count("otel_logs", "q2"), 1, "a zombie that landed before the retirement is ingested");
    let run = super::watermark::watermark_run(&*b, &wcfg, clk.0.get()).await.unwrap();
    let held = run.clusters.iter().find(|d| d.cluster == "c1").unwrap().complete_through_ns;
    assert!(held <= r2 + 1_000_000_000, "the dead lane holds c1: {held}");
    clk.0.set(clk.0.get() + 2_000);
    // a dry run checks and writes nothing
    let dry = super::retire::retire_lane(&*b, &super::retire::RetireCfg { dry_run: true, ..rcfg(1_000) }, lane, "pvc deleted", true, clk.0.get()).await.unwrap();
    assert!(dry.tombstones.is_empty() && !super::coord::CkptDoc::default().retired_now());
    let t = clk.0.get();
    let rec = retire("pvc otap-publisher-0 deleted 02:10Z, node gone (INC-42)", true, 1_000).await.unwrap();
    assert_eq!((rec.r_ns, rec.epoch.as_str(), rec.tombstones.len()), (t * 1_000_000, "E00010", 1), "{rec:?}");
    assert!(rec.last_object_ms > 0 && rec.last_kind == "data");
    // the tombstone at the head, the checkpoint, the record
    let tomb = proto::slot_key(&format!("{ROOT}/{lane}"), "E00010", 3);
    assert_eq!(proto::Slot::from_meta(&b.head(&tomb).await.unwrap().unwrap()), proto::Slot::Tomb);
    let (body, _) = b.get(&format!("{CTL}/ckpt/{lane}.json")).await.unwrap().unwrap();
    let ck: super::coord::CkptDoc = serde_json::from_slice(&body).unwrap();
    assert!(ck.retired_now() && ck.retired_by == "operator" && ck.retired_ns == t * 1_000_000 && ck.retired_evidence.contains("INC-42"), "{ck:?}");
    assert!(b.get(&super::retire::retired_record_key(CTL, lane, t)).await.unwrap().is_some());
    assert!(retire("again", true, 1_000).await.unwrap_err().contains("already retired"));
    // the holder's next checkpoint write fails on the ETag: it drops the
    // lane, takes it again once its own lease has expired (TTL + margin),
    // from the retired document, and closes the tombstoned epoch
    steps_with_beats(&mut w, &b, &clk, 100, &mut [(&mut live, "logs")]).await;
    assert!(w.stats.lanes_lost_cas >= 1, "the retirement's CAS moved the holder off the lane once");
    assert!(w.checkpoint(lane).is_some_and(|c| c.retired_now()), "held again, retired");
    let (body, _) = b.get(&format!("{CTL}/ckpt/{lane}.json")).await.unwrap().unwrap();
    let ck: super::coord::CkptDoc = serde_json::from_slice(&body).unwrap();
    assert!(ck.retired_now() && ck.closed("E00010"), "retired, and its epoch closed at the tombstone: {ck:?}");
    let run = super::watermark::watermark_run(&*b, &wcfg, clk.0.get()).await.unwrap();
    let d1 = run.clusters.iter().find(|d| d.cluster == "c1").unwrap().clone();
    assert_eq!(d1.retired.get("p0/logs"), Some(&(t * 1_000_000)));
    assert!(d1.complete_through_ns > t * 1_000_000, "c1 passed the retired lane: {}", d1.complete_through_ns);
}

/// D35 (2), the model's `opMistake`: the operator retires a lane whose
/// volume was kept after all; a new pod adopts it and replays its custody
/// with the original received_at, below R: quarantined, never ingested
/// (and, the mutant `ingestBelow`, ingested below the published value).
#[tokio::test(flavor = "current_thread")]
async fn a_kept_volume_replayed_after_retire_lane_is_quarantined() {
    for ingest_below in [false, true] {
        let (b, c, clk) = setup();
        let wcfg = super::watermark::WmConfig { skew_ms: 0, ..super::watermark::WmConfig::new(ROOT, CTL) };
        let mut dead = CustodyEdge::new("c1/p0", vec!["logs"], LowMode::Custody);
        let mut live = CustodyEdge::new("c1/p1", vec!["logs"], LowMode::Custody);
        assert!(dead.put(&b, "logs", 0, "birth-p0", proto::KIND_BEAT, 0, 0, false).await);
        let r9 = clk.0.get() * 1_000_000;
        dead.buffer.push((9, "logs", r9)); // in custody on the kept volume, never committed
        let mut cf = cfg("w1");
        if ingest_below {
            cf.timing.mutation = Mutation::IngestBelow;
        }
        let mut w = Worker::new(cf, b.clone(), c.clone(), clk.clone());
        steps_with_beats(&mut w, &b, &clk, 20, &mut [(&mut live, "logs")]).await;
        clk.0.set(clk.0.get() + 5_000);
        let rec = super::retire::retire_lane(&*b, &rcfg(1_000), "c1/p0/logs", "the operator believed the PVC deleted", true, clk.0.get()).await.unwrap();
        // (the holder drops the lane on its next write and takes it again after its lease)
        steps_with_beats(&mut w, &b, &clk, 100, &mut [(&mut live, "logs")]).await;
        assert!(w.checkpoint("c1/p0/logs").is_some_and(|c| c.retired_now()), "held again, retired");
        let run = super::watermark::watermark_run(&*b, &wcfg, clk.0.get()).await.unwrap();
        let published = run.clusters.iter().find(|d| d.cluster == "c1").unwrap().complete_through_ns;
        assert!(published > r9, "c1 published past the kept request: {published} <= {r9}");
        // the kept volume, adopted: a new epoch, its birth, the replay
        dead.restart();
        assert!(dead.put(&b, "logs", 0, "birth-p0-2", proto::KIND_BEAT, 0, 0, false).await);
        assert!(dead.put(&b, "logs", 0, "q9", proto::KIND_DATA, r9, 0, false).await);
        steps_with_beats(&mut w, &b, &clk, 20, &mut [(&mut live, "logs"), (&mut dead, "logs")]).await;
        if ingest_below {
            assert_eq!(c.count("otel_logs", "q9"), 1, "the mutant ingests it below the published {published}");
            continue;
        }
        assert_eq!(c.count("otel_logs", "q9"), 0, "never ingested");
        assert_eq!(w.stats.quarantined_objects, 1);
        let (q, _) = super::retire::read(&*b, CTL, "c1/p0/logs").await.unwrap().unwrap();
        assert_eq!((q.objects[0].content.as_str(), q.objects[0].received_ns, q.retired_ns), ("q9", r9, rec.r_ns));
        let ck = w.checkpoint("c1/p0/logs").unwrap();
        assert!(!ck.retired_now() && ck.reborn_epoch == "E00020", "reborn: the lane counts again from its birth");
    }
}

/// D35 (3): `consume admit` puts a quarantined object into the recovered
/// table (never the main one), once (idempotent by content key), marks it
/// admitted, reports its event window and the published values above its
/// received_at; GC never deletes a quarantined object.
#[tokio::test(flavor = "current_thread")]
async fn admit_recovers_quarantined_objects_once_and_gc_keeps_them() {
    let _trace = otap_s3pq::oscope_trace::covers("DST", &["CAST-48", "H-1", "UCA-6"]);
    let (b, c, clk) = setup();
    let wcfg = super::watermark::WmConfig { skew_ms: 0, ..super::watermark::WmConfig::new(ROOT, CTL) };
    let mut e = CustodyEdge::new("c1/p0", vec!["logs"], LowMode::Custody);
    let mut live = CustodyEdge::new("c1/p1", vec!["logs"], LowMode::Custody);
    assert!(e.put(&b, "logs", 0, "birth", proto::KIND_BEAT, 0, 0, false).await);
    let r = clk.0.get() * 1_000_000 + 1_000;
    assert!(e.put(&b, "logs", 0, "close", proto::KIND_CLOSE, 0, r, false).await);
    let mut w = Worker::new(Config { quarantine_skew_ms: 0, ..cfg("w1") }, b.clone(), c.clone(), clk.clone());
    steps_with_beats(&mut w, &b, &clk, 10, &mut [(&mut live, "logs")]).await;
    assert!(w.checkpoint("c1/p0/logs").unwrap().retired_now());
    // a replay below R, with its event-time range
    e.restart();
    assert!(e.put(&b, "logs", 0, "birth2", proto::KIND_BEAT, 0, 0, false).await);
    let key = proto::slot_key(&format!("{ROOT}/c1/p0/logs"), "E00020", 1);
    let mut m = e.meta("logs", 0, "q9", proto::KIND_DATA, r - 1, 0);
    let _ = m.insert(proto::META_ROWS.into(), "7".into());
    let _ = m.insert(proto::META_MIN_TIME.into(), (3 * 3_600_000_000_000u64 + 5).to_string());
    let _ = m.insert(proto::META_MAX_TIME.into(), (5 * 3_600_000_000_000u64 - 1).to_string());
    b.insert(&key, Bytes::from(vec![0u8; 10]), m);
    e.epoch.get_mut(&("logs", 0)).unwrap().1 += 1;
    steps_with_beats(&mut w, &b, &clk, 20, &mut [(&mut live, "logs"), (&mut e, "logs")]).await;
    assert_eq!((w.stats.quarantined_objects, c.count("otel_logs", "q9")), (1, 0));
    let _ = super::watermark::watermark_run(&*b, &wcfg, clk.0.get()).await.unwrap();
    // a dry run reports and writes nothing
    let dry = super::retire::admit(&*b, &*c, CTL, Some("c1/p0/logs"), true, clk.0.get()).await.unwrap();
    assert_eq!((dry.admitted, c.admits.get(), dry.windows[0].rows), (0, 0, 7));
    let rep = super::retire::admit(&*b, &*c, CTL, Some("c1/p0/logs"), false, clk.0.get()).await.unwrap();
    assert_eq!((rep.admitted, rep.already, c.admits.get()), (1, 0, 1));
    let win = &rep.windows[0];
    assert_eq!((win.cluster.as_str(), win.signal.as_str(), win.table.as_str(), win.objects, win.rows), ("c1", "logs", "otel_logs_recovered", 1, 7));
    assert_eq!((win.event_from_ns, win.event_to_ns, win.hours_total, win.hours.len()), (3 * 3_600_000_000_000 + 5, 5 * 3_600_000_000_000 - 1, 2, 2));
    assert_eq!((win.received_from_ns, win.received_to_ns), (r - 1, r - 1));
    assert!(win.bases_now.iter().any(|x| x.scope == "c1" && x.complete_through_ns > r - 1), "{:?}", win.bases_now);
    assert!(win.bases_now.iter().any(|x| x.scope == "fleet") && win.bases_now.iter().any(|x| x.scope == "c1/logs"));
    assert!(rep.note.contains("no history"));
    assert_eq!(c.recovered.borrow().get(&("otel_logs_recovered".to_string(), "q9".to_string())), Some(&7));
    assert_eq!(c.count("otel_logs", "q9"), 0, "never the main table");
    let (q, _) = super::retire::read(&*b, CTL, "c1/p0/logs").await.unwrap().unwrap();
    assert!(q.objects[0].admitted_wall_ms > 0 && q.objects[0].admitted_rows == 7);
    // again: nothing to do
    let again = super::retire::admit(&*b, &*c, CTL, None, false, clk.0.get()).await.unwrap();
    assert_eq!((again.admitted, again.already, c.admits.get()), (0, 1, 1));
    assert!(super::retire::admit(&*b, &*c, CTL, Some("c1/p9/logs"), false, clk.0.get()).await.unwrap_err().contains("no quarantine"));
    // GC deletes below its horizon, but never the quarantined object
    let g = GcConfig { root: ROOT.into(), ctl: CTL.into(), delay_ms: 1_000, zombie_ms: 3_000, dry_run: false };
    for _ in 0..4 {
        let _ = gc_step(&*b, &g, clk.0.get()).await.unwrap();
        steps_with_beats(&mut w, &b, &clk, 10, &mut [(&mut live, "logs"), (&mut e, "logs")]).await;
    }
    let rep = gc_step(&*b, &g, clk.0.get()).await.unwrap();
    assert!(b.head(&key).await.unwrap().is_some(), "the quarantined object is kept");
    assert!(b.head(&proto::slot_key(&format!("{ROOT}/c1/p0/logs"), "E00020", 0)).await.unwrap().is_none(), "the birth below it was deleted");
    let _ = rep;
}

/// A key that parses to a slot but is not that slot's key (`…/E1/0.parquet`,
/// `…/E1/+1.parquet` for `…/E1/00000000000000000000.parquet`) is not a slot:
/// the commit protocol's create-only PUT makes one object per KEY, so a
/// second spelling of a slot's key would give that slot a second object,
/// which a writer's If-None-Match on the real key never sees. Only the
/// canonical key (`proto::slot_key`) is read, ingested or collected.
#[tokio::test(flavor = "current_thread")]
async fn a_second_spelling_of_a_slot_key_is_not_a_slot() {
    let (b, c, clk) = setup();
    let mut w = worker("w1", &b, &c, &clk);
    let prefix = format!("{ROOT}/c1/p1/traces");
    put_obj(&b, "c1/p1/traces", "E1", 0, "slot0", 3, 0).await;
    b.insert(&format!("{prefix}/E1/0.parquet"), Bytes::from(vec![0u8; 100]), meta("E1", 0, "short0", 4));
    b.insert(&format!("{prefix}/E1/+1.parquet"), Bytes::from(vec![0u8; 100]), meta("E1", 1, "plus1", 6));
    put_obj(&b, "c1/p1/traces", "E1", 1, "slot1", 2, 0).await;
    for _ in 0..6 {
        let _ = w.step().await;
        clk.0.set(clk.0.get() + 200);
    }
    assert_eq!((c.count("otel_traces", "slot0"), c.count("otel_traces", "slot1")), (3, 2), "the canonical slots, once");
    assert_eq!((c.count("otel_traces", "short0"), c.count("otel_traces", "plus1")), (0, 0), "no second spelling ingested");
    assert_eq!(w.checkpoint("c1/p1/traces").unwrap().next("E1"), 2);
    // a second spelling ahead of the writer: slot 2 has only `…/E1/2.parquet`;
    // the lane waits for the real slot 2 and ingests that, never the spelling
    b.insert(&format!("{prefix}/E1/2.parquet"), Bytes::from(vec![0u8; 100]), meta("E1", 2, "ahead2", 5));
    for _ in 0..4 {
        let _ = w.step().await;
        clk.0.set(clk.0.get() + 200);
    }
    assert_eq!(c.count("otel_traces", "ahead2"), 0, "a spelling ahead of the writer is not slot 2");
    put_obj(&b, "c1/p1/traces", "E1", 2, "slot2", 7, 0).await;
    for _ in 0..4 {
        let _ = w.step().await;
        clk.0.set(clk.0.get() + 200);
    }
    assert_eq!((c.count("otel_traces", "slot2"), c.count("otel_traces", "ahead2")), (7, 0));
    assert_eq!(w.checkpoint("c1/p1/traces").unwrap().next("E1"), 3);
}
