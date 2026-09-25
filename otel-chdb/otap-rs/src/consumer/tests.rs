//! Worker tests on an in-memory bucket and central with a fake clock:
//! several workers, lease expiry and takeover, zombies, the server-side
//! fence, partial statements, lost answers, GC, and a randomized run.

use super::bucket::{Bucket, Cond, MemBucket, MemFaults, Put};
use super::coord::Timing;
use super::gc::{GcConfig, gc_step};
use super::sql::MemCentral;
use super::worker::{Config, FakeClock, Worker};
use bytes::Bytes;
use otap_s3pq::proto;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::rc::Rc;

const ROOT: &str = "r/edges";
const CTL: &str = "r/ctl";
const T: Timing = Timing { ttl_ms: 9000, margin_ms: 1000, budget_ms: 3000, mutation: super::coord::Mutation::None };

type W = Worker<MemBucket, MemCentral, FakeClock>;

fn cfg(name: &str) -> Config {
    let mut c = Config::new(ROOT, CTL, name);
    c.timing = T;
    c.discover_ms = 0;
    c.full_list_ms = 0;
    c.quiet_ms = 2000;
    c.limits.max_objects = 4;
    c
}

fn worker(name: &str, b: &Rc<MemBucket>, c: &Rc<MemCentral>, clk: &FakeClock) -> W {
    Worker::new(cfg(name), b.clone(), c.clone(), clk.clone())
}

fn meta(epoch: &str, seq: u64, content: &str, rows: u64) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
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
}

impl Edge {
    fn new(producer: &str, signal: &str) -> Self {
        let mut e = Edge { producer: producer.into(), signal: signal.into(), epoch: String::new(), next: 0, n_epochs: 0 };
        e.new_epoch();
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
            let m = meta(&self.epoch, self.next, content, rows);
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
    let mut z = Edge { producer: "p1".into(), signal: "traces".into(), epoch: "E0001".into(), next: 5, n_epochs: 5 };
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
    run(&mut ws, &clk, 5, 100).await;
    for i in 0..20 {
        assert_eq!(c.count("otel_traces", &format!("h{i}")), 7, "h{i}");
    }
    assert_eq!(c.applied.borrow().len(), 20, "each object applied exactly once");
    assert!(ws[0].stats.retried_missing > 0 && ws[0].stats.insert_errors > 0);
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

/// A randomized run: 3 producers × 3 signals, edges restarting (with
/// copies of their last batch in the new epoch), 3 workers taking and
/// losing lanes, crashing (a new incarnation) and pausing past their
/// leases, ambiguous answers on lease and checkpoint writes, statements that
/// half-land or lose their answer, and GC running throughout. At the end,
/// every committed content key is in central exactly once.
#[tokio::test(flavor = "current_thread")]
async fn randomized_fleet() {
    for seed in 1..=12u64 {
        randomized(seed).await;
    }
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

async fn randomized(seed: u64) {
    let (b, c, clk) = setup();
    *b.faults.borrow_mut() = MemFaults { matching: "/ctl/".into(), ambiguous_every: 7, drop_every: 11 };
    c.partial_every.set(5);
    c.lost_answer_every.set(7);
    let mut rng = Rng(seed * 0x9E37_79B9_7F4A_7C15 + 1);
    let mut edges: Vec<Edge> = Vec::new();
    for p in 0..3 {
        for s in ["traces", "logs", "metrics_gauge"] {
            edges.push(Edge::new(&format!("p{p}"), s));
        }
    }
    let mut committed: HashMap<(String, String), u64> = HashMap::new(); // (table, content) -> rows
    let mut last: HashMap<usize, String> = HashMap::new();
    let mut ws: Vec<W> = (0..3).map(|i| worker(&format!("w{i}-0"), &b, &c, &clk)).collect();
    let mut paused: Vec<u64> = vec![0; 3];
    let mut incarn = vec![0u32; 3];
    let gcc = GcConfig { root: ROOT.into(), ctl: CTL.into(), delay_ms: T.ttl_ms + T.margin_ms + 2000, zombie_ms: 60_000, dry_run: false };
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
                ws[i] = worker(&format!("w{i}-{}", incarn[i]), &b, &c, &clk);
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
}
