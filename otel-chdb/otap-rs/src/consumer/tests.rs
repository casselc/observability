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
    let mut e = Edge::new("p1", "traces");
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
    let ck = ws[0].checkpoint("p1/traces").unwrap().clone();
    assert!(ck.closed("E0001") && ck.next("E0001") == 5, "{ck:?}");
    assert!(!ck.closed("E0002") && ck.next("E0002") == 2);
    let s = &ws[0].stats;
    assert_eq!((s.tombstones_won, s.epochs_closed, s.dedup_skipped), (1, 1, 1), "{s:?}");
    assert!(s.statements <= 2, "5 + 1 objects in at most 2 statements of 4: {}", s.statements);
    // The dead writer (a zombie) tries its next slot: the tombstone halts it.
    let mut z = Edge { producer: "p1".into(), signal: "traces".into(), epoch: "E0001".into(), next: 5, n_epochs: 5, recv_clock: None };
    let (ep, _) = z.commit(&b, "h9", 10).await;
    assert_eq!(ep, "E0006", "halted and moved on");
}

#[tokio::test(flavor = "current_thread")]
async fn gaps_are_never_skipped_nor_tombstoned() {
    let (b, c, clk) = setup();
    let mut e = Edge::new("p1", "logs");
    e.commit(&b, "a", 1).await;
    // slot 1 missing, slot 2 present (a deleted slot, or LIST ahead of HEAD)
    let k2 = proto::slot_key(&e.prefix(), "E0001", 2);
    b.insert(&k2, Bytes::from("x"), meta("E0001", 2, "c", 1));
    let mut e2 = Edge::new("p1", "logs");
    e2.new_epoch(); // a newer epoch exists, so E0001 is superseded
    e2.commit(&b, "d", 1).await;
    let mut ws = vec![worker("w1", &b, &c, &clk)];
    run(&mut ws, &clk, 50, 200).await;
    let ck = ws[0].checkpoint("p1/logs").unwrap().clone();
    assert_eq!((ck.next("E0001"), ck.closed("E0001")), (1, false));
    assert_eq!(c.count("otel_logs", "c"), 0);
    assert_eq!(ws[0].stats.gaps_seen, 1);
    assert!(b.get(&proto::slot_key(&e.prefix(), "E0001", 1)).await.unwrap().is_none(), "no tombstone in the gap");
}

#[tokio::test(flavor = "current_thread")]
async fn takeover_waits_for_expiry_and_fences_the_old_holder() {
    let (b, c, clk) = setup();
    let mut e = Edge::new("p1", "traces");
    e.commit(&b, "h0", 5).await;
    let mut w1 = worker("w1", &b, &c, &clk);
    let mut w2 = worker("w2", &b, &c, &clk);
    let _ = w1.step().await;
    let _ = w2.step().await;
    assert_eq!(w1.held_lanes(), vec!["p1/traces"]);
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
    assert_eq!(w2.held_lanes(), vec!["p1/traces"]);
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
    let mut e1 = Edge::new("p1", "traces");
    let mut e2 = Edge::new("p2", "traces");
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
    let mut e = Edge::new("p1", "logs");
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
    assert_eq!(w1.checkpoint("p1/logs").unwrap().next("E0001"), 1, "not advanced past the fenced object");
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
    let mut e = Edge::new("p1", "traces");
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
    assert_eq!(ws[0].checkpoint("p1/traces").unwrap().next("E0001"), 20);
}

#[tokio::test(flavor = "current_thread")]
async fn series_lane_needs_no_check() {
    let (b, c, clk) = setup();
    let mut e = Edge::new("p1", "metrics_series");
    e.commit(&b, "s0", 3).await;
    e.commit(&b, "s1", 3).await;
    let mut ws = vec![worker("w1", &b, &c, &clk)];
    run(&mut ws, &clk, 2, 100).await;
    assert_eq!(ws[0].stats.series_objects_inserted, 2);
    assert_eq!(ws[0].stats.checks, 0);
    assert_eq!(ws[0].checkpoint("p1/metrics_series").unwrap().next("E0001"), 2);
}

/// Many edge restarts on one lane, with GC retiring closed epochs: the
/// checkpoint keeps only the epochs not retired yet, its floor moves up, the
/// full listing starts after the floor, gc.json stays small, and every batch
/// is ingested exactly once.
#[tokio::test(flavor = "current_thread")]
async fn checkpoint_stays_bounded_across_many_epochs() {
    let (b, c, clk) = setup();
    let mut e = Edge::new("p1", "traces");
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
            let ck = ws[0].checkpoint("p1/traces").unwrap();
            max_epochs = max_epochs.max(ck.epochs.len());
            max_bytes = max_bytes.max(serde_json::to_vec(ck).unwrap().len());
            max_gc = max_gc.max(b.get(&format!("{CTL}/gc.json")).await.unwrap().unwrap().0.len());
        }
    }
    for i in 1..=n {
        assert_eq!(c.count("otel_traces", &format!("h{i}")), 3, "h{i}");
    }
    assert_eq!(c.applied.borrow().len(), n, "each batch applied once");
    let ck = ws[0].checkpoint("p1/traces").unwrap().clone();
    let s = &ws[0].stats;
    // An epoch lives 3 s here (6 × 500 ms per restart) and retires after
    // quiet (2 s) + the GC delay and zombie bound (3 s): about 3 to 4 at once.
    assert!(max_epochs <= 5, "explicit epochs {max_epochs}: {ck:?}");
    assert!(max_bytes < 600 && max_gc < 6000, "checkpoint {max_bytes} B, gc.json {max_gc} B");
    assert!(ck.floor.as_str() > "E0100", "floor {}", ck.floor);
    assert!(s.epochs_compacted >= 110, "{s:?}");
    // The full listing starts after the floor: a whole-lane LIST of the
    // bucket right now returns only the epochs above it.
    let items = b.list(&format!("{ROOT}/p1/traces"), super::coord::floor_start_after(&format!("{ROOT}/p1/traces"), &ck.floor).as_deref()).await.unwrap();
    let epochs: BTreeSet<String> = items.iter().filter_map(|i| proto::parse_slot_key(&format!("{ROOT}/p1/traces"), &i.key)).map(|(e, _)| e).collect();
    assert!(epochs.len() <= 5 && epochs.iter().all(|x| x.as_str() > ck.floor.as_str()), "{epochs:?}");
}

/// Compacting an epoch before it is closed and retired (the model's
/// `earlyCompact`) loses a batch its writer commits afterwards; the design
/// ingests it.
#[tokio::test(flavor = "current_thread")]
async fn early_compaction_would_skip_a_late_batch() {
    for mutant in [false, true] {
        let (b, c, clk) = setup();
        let mut e1 = Edge::new("p1", "logs");
        e1.commit(&b, "a", 2).await;
        e1.commit(&b, "b", 2).await;
        // A second writer (another exporter lane of the edge) opens a newer
        // epoch: E0001 is superseded while its writer is still alive.
        let mut e2 = Edge::new("p1", "logs");
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
        let ck = w.checkpoint("p1/logs").unwrap();
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
    let prefix = format!("{ROOT}/p1/traces");
    // E0001: closed after one batch. E0002: a gap (slot 1 missing), so it
    // stays open for ever. E0003: closed after one batch. E0004: the newest.
    let mut e = Edge::new("p1", "traces");
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
    let ck = ws[0].checkpoint("p1/traces").unwrap().clone();
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
    *b.faults.borrow_mut() = MemFaults { matching: "/ctl/".into(), ambiguous_every: 7, drop_every: 11 };
    c.partial_every.set(5);
    c.lost_answer_every.set(7);
    c.late_every.set(11);
    c.late_by_ms.set(1500);
    c.slack_ms.set(T.slack_ms);
    let mut rng = Rng(seed * 0x9E37_79B9_7F4A_7C15 + 1);
    let mut edges: Vec<Edge> = Vec::new();
    for p in 0..3 {
        for s in ["traces", "logs", "metrics_gauge"] {
            edges.push(if scale { Edge::stamped(&format!("p{p}"), s, &clk) } else { Edge::new(&format!("p{p}"), s) });
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
    let mut busy = Edge::new("busy", "traces");
    let mut idle: Vec<Edge> = (0..9).map(|i| Edge::new(&format!("idle{i}"), "traces")).collect();
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
    let busy_lists = per("busy");
    let idle_lists: u64 = (0..9).map(|i| per(&format!("idle{i}"))).sum::<u64>() / 9;
    assert!(busy_lists >= 1400, "the busy lane is listed every poll: {busy_lists}");
    // 300 s at a 16 s cap: about 35 LISTs per idle lane (2 s of grace at
    // every poll, the doubling, then the cap), not 1,500.
    assert!(idle_lists <= 45, "idle lanes back off: {idle_lists} LISTs each");
    for i in 1..=n {
        assert_eq!(c.count("otel_traces", &format!("b{i}")), 1);
    }
    // A hint: the idle lane's new object is in at the next poll.
    idle[3].commit(&b, "idle3-1", 1).await;
    hints.push("idle3/traces");
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
    hints.push("idle1/traces");
    hints.push("nope/traces");
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
        let mut edges: Vec<Edge> = (0..4).map(|i| Edge::stamped(&format!("p{i}"), "logs", &clk)).collect();
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
    let mut heavy = Edge::new("heavy", "traces");
    let mut light: Vec<Edge> = (0..8).map(|i| Edge::new(&format!("l{i}"), "traces")).collect();
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
    let with_heavy = held.iter().position(|h| h.contains(&"heavy/traces".to_string())).expect("the heavy lane is held");
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
        let mut e = Edge::new("p1", "traces");
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

fn at(day: u64, h: u64, m: u64) -> u64 {
    day * DAY_NS + (h * 60 + m) * 60_000_000_000
}

/// Commits an object with an explicit received time (ns).
async fn put_obj(b: &MemBucket, lane: &str, epoch: &str, seq: u64, content: &str, rows: u64, recv: u64) {
    let key = proto::slot_key(&format!("{ROOT}/{lane}"), epoch, seq);
    b.insert(&key, Bytes::from(vec![0u8; 100]), meta_at(epoch, seq, content, rows, recv));
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
        put_obj(&b, "p1/logs", "E1", 0, "span-a", 4, at(20_009, 23, 59)).await;
        put_obj(&b, "p1/logs", "E1", 1, "span-b", 4, at(20_010, 0, 1)).await;
        // Old: received five days ago; an earlier attempt (a worker that
        // crashed before its checkpoint write) already put its rows in.
        put_obj(&b, "p2/logs", "E1", 0, "old", 6, at(20_005, 8, 0)).await;
        let old = super::plan::Obj {
            lane: "p2/logs".into(),
            epoch: "E1".into(),
            seq: 0,
            key: proto::slot_key(&format!("{ROOT}/p2/logs"), "E1", 0),
            size: 100,
            content: "old".into(),
            rows: 6,
            received_ns: at(20_005, 8, 0),
            seen_ms: 0,
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
    put_obj(&b, "p1/traces", "E1", 0, "req", 5, at(20_019, 23, 50)).await;
    for _ in 0..3 {
        let _ = w.step().await;
        clk.0.set(clk.0.get() + 200);
    }
    assert_eq!(c.count("otel_traces", "req"), 5);
    // Its copy: resent after the edge restarted, received 20 hours later.
    put_obj(&b, "p1/traces", "E2", 0, "req", 5, at(20_020, 19, 50)).await;
    // An object whose rows carry received times other than its metadata's (a foreign producer).
    put_obj(&b, "p1/traces", "E2", 1, "liar", 4, at(20_020, 9, 0)).await;
    let _ = c.true_recv.borrow_mut().insert(proto::slot_key(&format!("{ROOT}/p1/traces"), "E2", 1), vec![at(20_018, 9, 0), at(20_019, 9, 0)]);
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
    let ck = w.checkpoint("p1/traces").unwrap().clone();
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
    put_obj(&b, "p1/traces", "E1", 0, "far", 5, at(20_020, 12, 0)).await;
    put_obj(&b, "p1/traces", "E1", 1, "near", 5, at(20_021, 12, 0)).await;
    for _ in 0..3 {
        let _ = w.step().await;
        clk.0.set(clk.0.get() + 200);
    }
    assert_eq!((c.count("otel_traces", "far"), c.count("otel_traces", "near")), (5, 5));
    // Their copies, resent into a new epoch: "near" 2.5 days later, "far" 4 days later.
    put_obj(&b, "p1/traces", "E2", 0, "near", 5, at(20_024, 0, 0)).await;
    put_obj(&b, "p1/traces", "E2", 1, "far", 5, at(20_024, 12, 0)).await;
    for _ in 0..4 {
        let _ = w.step().await;
        clk.0.set(clk.0.get() + 200);
    }
    assert_eq!(c.count("otel_traces", "near"), 5, "a copy within 3 days is found and skipped");
    assert_eq!(c.count("otel_traces", "far"), 10, "a copy 4 days later is out of the check's reach: ingested twice");
    assert_eq!(w.checkpoint("p1/traces").unwrap().next("E2"), 2);
}
