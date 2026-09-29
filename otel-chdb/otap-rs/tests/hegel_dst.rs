//! Stateful and swarm testing of the consumer fleet with Hegel (HEGEL.md):
//! a sequential `#[hegel::state_machine]` whose rules drive the level-1
//! deterministic simulation (DST.md, `tests/dst/fleet.rs`) one step at a
//! time. Each rule is one move of the environment: a producer commits
//! batches, simulated time advances, a process pauses, is cut off, killed,
//! a discovery LIST is slow, a GC step runs, the fleet heals, or one fault
//! from AMBIGUITY.md's menu hits one process's next request. The fault
//! kinds are separate rules, so Hegel's swarm testing switches each one on
//! or off for a whole test case. The invariants are the simulation's own
//! (no takeover of a live lease, statements inside their lease,
//! neverSkipsCommitted and noCommitAfterClose at every checkpoint write,
//! and here also: no lane dropped for a renewal whose lease is the
//! worker's own), checked after every rule; at the end the fleet heals and
//! must ingest everything exactly once (atMostOnce, onlyCommittedIngested,
//! liveness).
//!
//! The simulation is deterministic, so a failing case replays and Hegel
//! shrinks it to a short rule sequence. It runs on its own thread (a fresh
//! one per test case: seeded `HashMap` order, OS randomness and clocks, as
//! `sim::run` does), and the rules send it closures.
//!
//!   cargo test --release --test hegel_dst -- hegel_dst_fleet
//!   HEGEL_TEST_CASES=2000 cargo test --release --test hegel_dst -- hegel_dst_fleet
//!   HEGEL_DST_MUTANT=backdate_observations cargo test --release --test hegel_dst -- hegel_dst_fleet   # expected to fail
//!   HEGEL_DST_MUTANTS=1 cargo test --release --test hegel_dst -- hegel_dst_finds_mutants --nocapture  # all of them, with statistics

#[path = "../src/consumer/mod.rs"]
#[allow(dead_code, unused_imports, clippy::all)]
mod consumer;
mod dst;
#[path = "dst/fleet.rs"]
mod fleet;

use consumer::coord::Mutation;
use consumer::gc::{GcConfig, gc_step};
use dst::sim::{self, Sim, now_ms, sleep_ms, trace};
use fleet::*;
use hegel::generators as gs;
use hegel::{HealthCheck, Settings, TestCase};
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;

// ---- the case's fixed setup ------------------------------------------------------------------

/// What a test case decides before its first rule.
#[derive(Clone, Debug)]
struct Setup {
    /// Seeds the simulation's own draws (latencies, row counts, poll
    /// jitter) and OS randomness; the rules are Hegel's.
    seed: u64,
    workers: usize,
    producers: usize,
    lat_ms: u64,
    s3_timeout_ms: u64,
    /// Load-balancing mode, received times and the check's partition range.
    scale: bool,
    zombie_ms: u64,
    server_skew_ms: i64,
    worker_skew_ms: Vec<i64>,
    mutant: Mutation,
}

/// The longest a forced late PUT lands after its client gave up (GC's delay covers it).
const LATE_MS: u64 = 1_500;

impl Setup {
    fn draw(tc: &TestCase, mutant: Mutation) -> Setup {
        let margin = T.margin_ms as i64;
        let workers = tc.draw(gs::integers::<usize>().min_value(1).max_value(3));
        let skew = |tc: &TestCase| tc.draw(gs::integers::<i64>().min_value(-margin / 2).max_value(margin / 2));
        Setup {
            seed: tc.draw(gs::integers::<u64>().max_value(1_000)),
            workers,
            producers: tc.draw(gs::integers::<usize>().min_value(1).max_value(2)),
            lat_ms: tc.draw(gs::sampled_from(vec![1u64, 5, 20, 60])),
            s3_timeout_ms: tc.draw(gs::sampled_from(vec![1_000u64, 3_000])),
            scale: tc.draw(gs::booleans()),
            zombie_ms: tc.draw(gs::sampled_from(vec![3_000u64, 60_000])),
            server_skew_ms: skew(tc),
            worker_skew_ms: (0..workers).map(|_| skew(tc)).collect(),
            mutant,
        }
    }

    fn profile(&self) -> Profile {
        let mut p = Profile::calm(self.workers, self.producers, 0);
        p.lat_ms = self.lat_ms;
        p.s3_timeout_ms = self.s3_timeout_ms;
        p.scale = self.scale;
        p.zombie_ms = self.zombie_ms;
        p.put_late_ms = LATE_MS;
        p.server_skew_ms = self.server_skew_ms;
        p.worker_skew_ms = self.worker_skew_ms.clone();
        p
    }
}

// ---- the simulation's thread ------------------------------------------------------------------

/// The world as the simulation thread holds it.
struct Fleet {
    w: Rc<World>,
    slots: RefCell<Vec<Slot>>,
    /// Per lane: its edge driver and process.
    edges: Vec<(tokio::sync::mpsc::UnboundedSender<EdgeCmd>, Rc<Proc>)>,
    gc: Rc<Proc>,
    gcc: GcConfig,
}

type Reply = Result<String, String>;
type Cmd = Box<dyn FnOnce(Rc<Fleet>) -> Pin<Box<dyn Future<Output = Reply>>> + Send>;

/// One test case's simulation, on a thread of its own. Each command runs
/// inside the paused runtime until its future completes; between commands
/// no simulated time passes.
struct SimThread {
    tx: Option<mpsc::Sender<Cmd>>,
    rx: mpsc::Receiver<Reply>,
    handle: Option<std::thread::JoinHandle<(u64, u64)>>,
    dead: Cell<Option<String>>,
}

/// Statistics of the last case (for the mutant report).
static LAST_LINES: AtomicU64 = AtomicU64::new(0);
static LAST_SIM_MS: AtomicU64 = AtomicU64::new(0);

impl SimThread {
    fn start(s: Setup) -> SimThread {
        sim::quiet_panics();
        let (tx, crx) = mpsc::channel::<Cmd>();
        let (rtx, rx) = mpsc::channel::<Reply>();
        let keep = std::env::var_os("DST_TRACE").is_some();
        let handle = std::thread::Builder::new()
            .name(format!("dst-hegel-{}", s.seed))
            .stack_size(64 << 20)
            .spawn(move || {
                sim::enter(s.seed, keep);
                MUTANT.with(|m| m.set(s.mutant));
                let rt = tokio::runtime::Builder::new_current_thread().enable_time().start_paused(true).build().expect("runtime");
                let local = tokio::task::LocalSet::new();
                let wall0 = wall0(s.seed);
                let fleet = local.block_on(&rt, async {
                    sim::start_clock(wall0);
                    let sim = Rc::new(Sim { seed: s.seed, rng: RefCell::new(sim::Rng::new(s.seed)), wall0_ms: wall0 });
                    let p = s.profile();
                    trace(format!("PROFILE {p:?} (hegel)"));
                    let w = World::new(sim, p.clone());
                    let mut edges = Vec::new();
                    for pi in 0..p.producers {
                        for sig in SIGNALS {
                            edges.push(spawn_edge(&w, pi, sig));
                        }
                    }
                    let slots = (0..p.workers).map(|i| spawn_worker(&w, i, 0)).collect();
                    let gcc = GcConfig {
                        root: ROOT.into(),
                        ctl: CTL.into(),
                        delay_ms: T.ttl_ms + T.margin_ms + p.s3_timeout_ms + p.put_late_ms + 2_000,
                        zombie_ms: p.zombie_ms,
                        dry_run: false,
                    };
                    Rc::new(Fleet { w, slots: RefCell::new(slots), edges, gc: Proc::new("gc"), gcc })
                });
                let w = fleet.w.clone();
                otap_s3pq::set_log_sink(Some(Box::new(move |m: &str| {
                    trace(format!("LOG {m}"));
                    w.on_log(m);
                })));
                while let Ok(cmd) = crx.recv() {
                    let f = fleet.clone();
                    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| local.block_on(&rt, cmd(f))));
                    let r = r.unwrap_or_else(|p| {
                        Err(format!(
                            "the simulation panicked: {}",
                            p.downcast_ref::<String>().cloned().or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string())).unwrap_or_default()
                        ))
                    });
                    let stop = r.is_err();
                    if rtx.send(r).is_err() || stop {
                        break;
                    }
                }
                let sim_ms = local.block_on(&rt, async { now_ms() });
                otap_s3pq::set_log_sink(None);
                drop(fleet);
                drop(local);
                drop(rt);
                let (_, _, lines) = sim::leave();
                (lines, sim_ms)
            })
            .expect("spawn");
        SimThread { tx: Some(tx), rx, handle: Some(handle), dead: Cell::new(None) }
    }

    /// Runs `f` in the simulation; a failure (a violated check, a panic in
    /// the simulation) is this case's failure.
    fn run<F, Fut>(&self, f: F) -> String
    where
        F: FnOnce(Rc<Fleet>) -> Fut + Send + 'static,
        Fut: Future<Output = Reply> + 'static,
    {
        if let Some(e) = self.dead.take() {
            self.dead.set(Some(e.clone()));
            FAILED.with(|f| f.set(true));
            panic!("{e}");
        }
        let cmd: Cmd = Box::new(move |fl| Box::pin(f(fl)));
        self.tx.as_ref().expect("sim").send(cmd).expect("the simulation thread is gone");
        match self.rx.recv().expect("the simulation thread is gone") {
            Ok(s) => s,
            Err(e) => {
                self.dead.set(Some(e.clone()));
                FAILED.with(|f| f.set(true));
                panic!("{e}");
            }
        }
    }
}

impl Drop for SimThread {
    fn drop(&mut self) {
        drop(self.tx.take());
        if let Some(h) = self.handle.take() {
            if let Ok((lines, ms)) = h.join() {
                LAST_LINES.store(lines, Ordering::Relaxed);
                LAST_SIM_MS.store(ms, Ordering::Relaxed);
            }
        }
    }
}

/// The invariants checked while the simulation runs, as one verdict.
fn violations(f: &Fleet) -> Reply {
    let v = f.w.violations.borrow();
    if v.is_empty() { Ok(String::new()) } else { Err(format!("invariant violations: {v:#?}")) }
}

/// Every process back to normal: no pause, no cut, no queued fault.
fn heal_procs(f: &Fleet) {
    for s in f.slots.borrow().iter() {
        s.proc.paused_until.set(0);
        s.proc.cut_until.set(0);
        s.proc.slow_list_ms.set(0);
        s.proc.slow_until.set(0);
        s.proc.forced.borrow_mut().clear();
    }
    for (_, p) in &f.edges {
        p.forced.borrow_mut().clear();
    }
    f.gc.forced.borrow_mut().clear();
}

/// Lets the edges commit what they were told to (healed, they always can).
async fn drain_edges(f: &Fleet) -> Reply {
    let t0 = now_ms();
    while f.w.edge_backlog.get() > 0 {
        if now_ms() > t0 + 3_600_000 {
            return Err(format!("edges stuck with {} batches to commit", f.w.edge_backlog.get()));
        }
        sleep_ms(1_000).await;
    }
    Ok(String::new())
}

/// Healed, the fleet ingests everything committed so far (liveness).
async fn heal_and_check(f: &Fleet) -> Reply {
    heal_procs(f);
    trace("HEAL");
    drain_edges(f).await?;
    let q = quiesce(&f.w).await;
    if !complete(&f.w) {
        let (missing, _, _) = final_state(&f.w);
        let (ann_missing, _, _) = announcement_state(&f.w);
        return Err(format!("liveness: healed, no progress for 60 s after {q} ms; not ingested: {missing:?}; announcements not ingested: {ann_missing:?}"));
    }
    violations(f)
}

/// The end: heal, ingest everything, let late statements land, stop, and
/// check the final state.
async fn finish(f: Rc<Fleet>) -> Reply {
    heal_procs(&f);
    f.w.healed.set(true);
    trace("FINISH");
    drain_edges(&f).await?;
    let q = quiesce(&f.w).await;
    sleep_ms(T.ttl_ms + T.slack_ms + T.margin_ms + 1_000).await;
    for s in f.slots.borrow_mut().iter_mut() {
        if let Some(h) = s.handle.take() {
            h.abort();
        }
    }
    sleep_ms(T.ttl_ms + T.slack_ms + 1_000).await;
    trace("END");
    violations(&f)?;
    let (missing, dup, extra) = final_state(&f.w);
    if !dup.is_empty() {
        return Err(format!("atMostOnce: ingested more than once: {dup:?}"));
    }
    if !extra.is_empty() {
        return Err(format!("onlyCommittedIngested: {extra:?}"));
    }
    if !missing.is_empty() {
        return Err(format!("not ingested, no progress for 60 s after {q} ms healed (neverSkipsCommitted / liveness): {missing:?}"));
    }
    let (ann_missing, ann_extra, _) = announcement_state(&f.w);
    if !ann_missing.is_empty() {
        return Err(format!("announcements committed and not ingested, no progress for 60 s after {q} ms healed: {ann_missing:?}"));
    }
    if !ann_extra.is_empty() {
        return Err(format!("announcements ingested and never committed: {ann_extra:?}"));
    }
    Ok(format!("{:?}", f.w.tally.borrow()))
}

// ---- the machine ---------------------------------------------------------------------------------

struct FleetMachine {
    sim: Rc<SimThread>,
    workers: usize,
    lanes: usize,
    steps: Rc<Cell<u64>>,
}

impl FleetMachine {
    fn worker(&self, tc: &TestCase) -> usize {
        tc.draw(gs::integers::<usize>().max_value(self.workers - 1))
    }

    /// A process for a request fault: a worker, an edge, or GC.
    fn target(&self, tc: &TestCase) -> usize {
        tc.draw(gs::integers::<usize>().max_value(self.workers + self.lanes))
    }

    /// Queue `x` for process `t`'s next matching request.
    fn force(&self, tc: &TestCase, t: usize, x: Forced) {
        let workers = self.workers;
        self.step(tc, move |f| async move {
            let p = if t < workers {
                f.slots.borrow()[t].proc.clone()
            } else if t < workers + f.edges.len() {
                f.edges[t - workers].1.clone()
            } else {
                f.gc.clone()
            };
            trace(format!("RULE force {x:?} on {}", p.name));
            p.forced.borrow_mut().push(x);
            Ok(String::new())
        });
    }

    fn force_worker(&self, tc: &TestCase, x: Forced) {
        let i = self.worker(tc);
        self.force(tc, i, x);
    }

    /// One rule's effect `f`, then a little simulated time (drawn, so the
    /// shrinker can take it back to 0) for it to play out.
    fn step<F, Fut>(&self, tc: &TestCase, f: F)
    where
        F: FnOnce(Rc<Fleet>) -> Fut + Send + 'static,
        Fut: Future<Output = Reply> + 'static,
    {
        let then_ms = tc.draw(gs::integers::<u64>().max_value(5_000));
        self.steps.set(self.steps.get() + 1);
        let _ = self.sim.run(move |fl| async move {
            f(fl.clone()).await?;
            sleep_ms(then_ms).await;
            violations(&fl)
        });
    }
}

#[hegel::state_machine]
impl FleetMachine {
    // -- the load --

    /// A producer lane commits `n` batches (create-only slots; an ambiguous
    /// PUT is resolved by HEAD, as the exporter does).
    #[rule]
    fn write(&mut self, tc: TestCase) {
        let lane = tc.draw(gs::integers::<usize>().max_value(self.lanes - 1));
        let n = tc.draw(gs::integers::<u32>().min_value(1).max_value(64));
        self.step(&tc, move |f| async move {
            f.w.edge_backlog.set(f.w.edge_backlog.get() + n as u64);
            let _ = f.edges[lane].0.send(EdgeCmd::Write(n));
            Ok(String::new())
        });
    }

    /// A producer lane's resources change (a rollout: new pods): its next
    /// batches use resources it has never announced, so their objects carry
    /// announcements, which must reach otel_resources before their rows
    /// (sameLane), exactly once (after the ReplacingMergeTree's folding).
    #[rule]
    fn announce(&mut self, tc: TestCase) {
        let lane = tc.draw(gs::integers::<usize>().max_value(self.lanes - 1));
        self.step(&tc, move |f| async move {
            f.w.edge_backlog.set(f.w.edge_backlog.get() + 1);
            let _ = f.edges[lane].0.send(EdgeCmd::NewResources);
            Ok(String::new())
        });
    }

    /// A producer restarts: a new epoch, its last batch resent.
    #[rule]
    fn restart_edge(&mut self, tc: TestCase) {
        let lane = tc.draw(gs::integers::<usize>().max_value(self.lanes - 1));
        self.step(&tc, move |f| async move {
            f.w.edge_backlog.set(f.w.edge_backlog.get() + 1);
            let _ = f.edges[lane].0.send(EdgeCmd::Restart);
            Ok(String::new())
        });
    }

    // -- time --

    /// Simulated time passes: every task runs until then.
    #[rule(weight = 3.0)]
    fn advance(&mut self, tc: TestCase) {
        let ms = tc.draw(gs::integers::<u64>().min_value(1).max_value(30_000));
        self.step(&tc, move |f| async move {
            sleep_ms(ms).await;
            violations(&f)
        });
    }

    // -- processes --

    /// A stop-the-world pause (GC, a frozen VM): nothing sent, no answer taken.
    #[rule]
    fn pause(&mut self, tc: TestCase) {
        let i = self.worker(&tc);
        let ms = tc.draw(gs::integers::<u64>().min_value(500).max_value(T.ttl_ms + 3 * T.margin_ms + 2_000));
        self.step(&tc, move |f| async move {
            let p = f.slots.borrow()[i].proc.clone();
            trace(format!("PAUSE {} for {ms} ms (rule)", p.name));
            p.paused_until.set(now_ms() + ms);
            Ok(String::new())
        });
    }

    /// Cut off the network: requests get no answer.
    #[rule]
    fn cut(&mut self, tc: TestCase) {
        let i = self.worker(&tc);
        let ms = tc.draw(gs::integers::<u64>().min_value(500).max_value(T.ttl_ms + 2_000));
        self.step(&tc, move |f| async move {
            let p = f.slots.borrow()[i].proc.clone();
            trace(format!("CUT {} for {ms} ms (rule)", p.name));
            p.cut_until.set(now_ms() + ms);
            Ok(String::new())
        });
    }

    /// Kill a worker; its next incarnation starts after `delay` (its
    /// statements already sent run on).
    #[rule]
    fn kill(&mut self, tc: TestCase) {
        let i = self.worker(&tc);
        let delay = tc.draw(gs::integers::<u64>().max_value(3_000));
        self.step(&tc, move |f| async move {
            let mut slots = f.slots.borrow_mut();
            trace(format!("KILL {} (rule)", slots[i].proc.name));
            f.w.tally.borrow_mut().kills += 1;
            if let Some(h) = slots[i].handle.take() {
                h.abort();
            }
            let inc = slots[i].inc + 1;
            slots[i] = spawn_worker(&f.w, i, inc);
            slots[i].proc.paused_until.set(now_ms() + delay);
            Ok(String::new())
        });
    }

    /// The worker's next LIST of the heartbeats answers `ms` late: a slow
    /// discovery round (CAST #13's shape).
    #[rule]
    fn slow_list(&mut self, tc: TestCase) {
        let i = self.worker(&tc);
        let ms = tc.draw(gs::integers::<u64>().min_value(1_000).max_value(20_000));
        self.step(&tc, move |f| async move {
            let p = f.slots.borrow()[i].proc.clone();
            trace(format!("SLOW next heartbeat LIST of {} by {ms} ms (rule)", p.name));
            p.slow_list_ms.set(ms);
            Ok(String::new())
        });
    }

    /// The store slows down for one worker: for `for_ms`, every S3 answer
    /// comes `ms` later (throttling, an overloaded gateway). With a backlog
    /// this makes a scan outlast the lease window (CAST #14's shape).
    #[rule]
    fn slow_s3(&mut self, tc: TestCase) {
        let i = self.worker(&tc);
        let ms = tc.draw(gs::integers::<u64>().min_value(50).max_value(300));
        let for_ms = tc.draw(gs::integers::<u64>().min_value(1_000).max_value(60_000));
        self.step(&tc, move |f| async move {
            let p = f.slots.borrow()[i].proc.clone();
            trace(format!("SLOW S3 for {} by {ms} ms per answer for {for_ms} ms (rule)", p.name));
            p.slow_ms.set(ms);
            p.slow_until.set(now_ms() + for_ms);
            Ok(String::new())
        });
    }

    /// One GC step (delete below the horizon, retire closed epochs).
    #[rule]
    fn gc(&mut self, tc: TestCase) {
        self.step(&tc, move |f| async move {
            let b = SimBucket { w: f.w.clone(), p: f.gc.clone() };
            f.w.tally.borrow_mut().gc_runs += 1;
            match gc_step(&b, &f.gcc, f.w.sim.wall()).await {
                Ok(r) => trace(format!("GC {r:?}")),
                Err(e) => trace(format!("GC error {e}")),
            }
            violations(&f)
        });
    }

    /// Everything heals; the fleet must then ingest everything committed so far.
    #[rule]
    fn heal(&mut self, tc: TestCase) {
        self.step(&tc, move |f| async move { heal_and_check(&f).await });
    }

    // -- the fault menu (AMBIGUITY.md), one rule per fault, so swarm testing
    //    switches each on or off for a whole case --

    /// S1-S4: the next PUT never arrives.
    #[rule]
    fn put_drop(&mut self, tc: TestCase) {
        let t = self.target(&tc);
        self.force(&tc, t, Forced::PutDrop);
    }

    /// S1-S4: the next PUT applies, its answer is lost.
    #[rule]
    fn put_lost(&mut self, tc: TestCase) {
        let t = self.target(&tc);
        self.force(&tc, t, Forced::PutLost);
    }

    /// S1-S3: the next conditional PUT applies, then answers 412 (a 5xx and
    /// object_store's retry met our own write; CAST #15's shape).
    #[rule]
    fn put_own_412(&mut self, tc: TestCase) {
        let t = self.target(&tc);
        self.force(&tc, t, Forced::PutOwn412);
    }

    /// S1: the next PUT is stuck and lands after its client gave up.
    #[rule]
    fn put_late(&mut self, tc: TestCase) {
        let t = self.target(&tc);
        let ms = tc.draw(gs::integers::<u64>().max_value(LATE_MS));
        self.force(&tc, t, Forced::PutLate(ms));
    }

    /// S5-S6: the next GET, HEAD or LIST answers 503.
    #[rule]
    fn read_503(&mut self, tc: TestCase) {
        let t = self.target(&tc);
        self.force(&tc, t, Forced::Read503);
    }

    /// C1: the next statement is refused before writing (TOO_MANY_PARTS).
    #[rule]
    fn stmt_settled_err(&mut self, tc: TestCase) {
        self.force_worker(&tc, Forced::StmtSettledErr);
    }

    /// C1: the next insert writes its first object, then fails.
    #[rule]
    fn stmt_partial(&mut self, tc: TestCase) {
        self.force_worker(&tc, Forced::StmtPartial);
    }

    /// C1: the next statement commits, its answer is lost.
    #[rule]
    fn stmt_lost(&mut self, tc: TestCase) {
        self.force_worker(&tc, Forced::StmtLost);
    }

    /// C1/C5: the next statement gets no answer and commits as late as it can.
    #[rule]
    fn stmt_late(&mut self, tc: TestCase) {
        self.force_worker(&tc, Forced::StmtLate);
    }

    /// C1/C5: the next statement answers TIMEOUT_EXCEEDED, then commits.
    #[rule]
    fn stmt_timeout_commit(&mut self, tc: TestCase) {
        self.force_worker(&tc, Forced::StmtTimeoutCommit);
    }

    /// C2: the next count check fails.
    #[rule]
    fn check_err(&mut self, tc: TestCase) {
        self.force_worker(&tc, Forced::CheckErr);
    }

    /// Safety, after every rule: the checks the simulation makes as it runs.
    #[invariant(always_run)]
    fn safety(&mut self, _tc: TestCase) {
        let _ = self.sim.run(|f| async move { violations(&f) });
    }
}

// ---- running it ------------------------------------------------------------------------------------

/// Rules per case (Hegel shortens failing ones).
const STEPS: i64 = 60;

fn mutant_named(name: &str) -> Mutation {
    match name {
        "backdate_observations" => Mutation::BackdateObservations,
        "renew_only_at_insert" => Mutation::RenewOnlyAtInsert,
        "own_412_is_takeover" => Mutation::Own412IsTakeover,
        "no_time_bound" => Mutation::NoTimeBound,
        "no_verify" => Mutation::NoVerify,
        "release_in_flight" => Mutation::ReleaseInFlight,
        "error_settles" => Mutation::ErrorSettles,
        "early_compact" => Mutation::EarlyCompact,
        "" | "none" => Mutation::None,
        other => panic!("unknown HEGEL_DST_MUTANT {other}"),
    }
}

/// Counters over one Hegel run: executions, the first failing one, and the
/// size of the smallest failing one (after shrinking, the minimal example).
static CASES: AtomicU64 = AtomicU64::new(0);
static FIRST_FAIL: AtomicU64 = AtomicU64::new(0);
static FAILS: AtomicU64 = AtomicU64::new(0);
static LAST_FAIL_STEPS: AtomicU64 = AtomicU64::new(0);
static LAST_FAIL_LINES: AtomicU64 = AtomicU64::new(0);
static LAST_FAIL_SIM_MS: AtomicU64 = AtomicU64::new(0);

thread_local! {
    /// Set when this case failed a check (not when Hegel unwinds a case it
    /// abandons: an overrun, a rejected draw).
    static FAILED: Cell<bool> = const { Cell::new(false) };
}

struct CaseGuard {
    n: u64,
    steps: Rc<Cell<u64>>,
}

impl Drop for CaseGuard {
    fn drop(&mut self) {
        if FAILED.with(|f| f.replace(false)) {
            let _ = FIRST_FAIL.compare_exchange(0, self.n, Ordering::Relaxed, Ordering::Relaxed);
            FAILS.fetch_add(1, Ordering::Relaxed);
            // The smallest failure seen (rules, then trace lines) is the
            // shrunk counterexample: shrinking keeps only smaller failures.
            let (steps, lines) = (self.steps.get(), LAST_LINES.load(Ordering::Relaxed));
            let best = (LAST_FAIL_STEPS.load(Ordering::Relaxed), LAST_FAIL_LINES.load(Ordering::Relaxed));
            if best == (0, 0) || (steps, lines) < best {
                LAST_FAIL_STEPS.store(steps, Ordering::Relaxed);
                LAST_FAIL_LINES.store(lines, Ordering::Relaxed);
                LAST_FAIL_SIM_MS.store(LAST_SIM_MS.load(Ordering::Relaxed), Ordering::Relaxed);
            }
        }
    }
}

fn run_case(tc: TestCase, mutant: Mutation) {
    let n = CASES.fetch_add(1, Ordering::Relaxed) + 1;
    let steps = Rc::new(Cell::new(0));
    let _guard = CaseGuard { n, steps: steps.clone() };
    let s = Setup::draw(&tc, mutant);
    tc.note(&format!("{s:?}"));
    let sim = Rc::new(SimThread::start(s.clone()));
    let m = FleetMachine { sim: sim.clone(), workers: s.workers, lanes: s.producers * SIGNALS.len(), steps: steps.clone() };
    hegel::stateful::machine(m).steps(STEPS).run(tc);
    let summary = sim.run(finish);
    if std::env::var_os("DST_VERBOSE").is_some() {
        drop(sim);
        eprintln!(
            "case {n}: {s:?}: {} rules, {} s simulated, {} trace lines; {summary}",
            steps.get(),
            LAST_SIM_MS.load(Ordering::Relaxed) / 1000,
            LAST_LINES.load(Ordering::Relaxed)
        );
    }
}

/// The settings every run here starts from: the profile's (hegel.toml,
/// HEGEL_* variables), and no `too_slow` health check (a case runs a few
/// minutes of simulated time, 50-500 ms of real time).
fn dst_settings() -> Settings {
    Settings::new().suppress_health_check([HealthCheck::TooSlow])
}

/// The fleet under Hegel's rules; `HEGEL_DST_MUTANT` plants one of the
/// worker's known bugs (`coord::Mutation`), for which this must fail.
#[hegel::test(dst_settings())]
fn hegel_dst_fleet(tc: TestCase) {
    let m = mutant_named(&std::env::var("HEGEL_DST_MUTANT").unwrap_or_default());
    run_case(tc, m);
}

/// Each known bug, planted in turn, must be found; prints how many cases it
/// took, the shrunk counterexample's size, and (for comparison) the first
/// DST seed that finds it and that seed's trace. Opt-in: `HEGEL_DST_MUTANTS=1`
/// (`HEGEL_DST_MUTANT_CASES` bounds the search, default 1000).
#[test]
fn hegel_dst_finds_mutants() {
    if std::env::var_os("HEGEL_DST_MUTANTS").is_none() {
        eprintln!("HEGEL_DST_MUTANTS unset: skipped");
        return;
    }
    let budget: u64 = std::env::var("HEGEL_DST_MUTANT_CASES").ok().and_then(|v| v.parse().ok()).unwrap_or(1_000);
    let only = std::env::var("HEGEL_DST_MUTANT").ok();
    let mut survived = Vec::new();
    for name in ["backdate_observations", "renew_only_at_insert", "own_412_is_takeover", "no_time_bound", "no_verify", "release_in_flight", "error_settles", "early_compact"] {
        if only.as_deref().is_some_and(|o| o != name) {
            continue;
        }
        let m = mutant_named(name);
        for c in [&CASES, &FIRST_FAIL, &FAILS, &LAST_FAIL_STEPS, &LAST_FAIL_LINES, &LAST_FAIL_SIM_MS] {
            c.store(0, Ordering::Relaxed);
        }
        let t0 = std::time::Instant::now();
        let settings = dst_settings().test_cases(budget).database(None).derandomize(true).print_blob(false).verbosity(hegel::Verbosity::Quiet);
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| hegel::Hegel::new(|tc| run_case(tc, m)).settings(settings).run()));
        let secs = t0.elapsed().as_secs_f64();
        let msg = r.as_ref().err().map(|p| p.downcast_ref::<String>().cloned().or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string())).unwrap_or_default()).unwrap_or_default();
        // The same bug under the seeded sweep (dst_consumer.rs's fleet), for comparison.
        let dst = (1..=500u64).find_map(|seed| {
            let o = sim::run(seed, wall0(seed), false, move |s| async move {
                MUTANT.with(|c| c.set(m));
                fleet(s).await
            });
            o.failure.map(|_| (seed, o.lines))
        });
        if r.is_ok() {
            survived.push(name);
            eprintln!("mutant {name}: SURVIVED {budget} cases ({secs:.0} s); DST {dst:?}");
            continue;
        }
        eprintln!(
            "mutant {name}: found at case {} of {} executions ({} failing, shrinking) in {secs:.0} s; shrunk to {} rules, {} trace lines, {} s simulated; DST: first failing seed and its trace lines {dst:?}\n  {}",
            FIRST_FAIL.load(Ordering::Relaxed),
            CASES.load(Ordering::Relaxed),
            FAILS.load(Ordering::Relaxed),
            LAST_FAIL_STEPS.load(Ordering::Relaxed),
            LAST_FAIL_LINES.load(Ordering::Relaxed),
            LAST_FAIL_SIM_MS.load(Ordering::Relaxed) / 1000,
            msg.lines().next().unwrap_or("").chars().take(300).collect::<String>()
        );
    }
    assert!(survived.is_empty(), "mutants survived: {survived:?}");
}

/// Nightly run 7's shrunk case, as plain steps (no Hegel blob, so it keeps
/// meaning the same thing when the rules change): 3 workers; a lane
/// commits one batch (its object announces the lane's resource); the edge
/// restarts, resending that batch into E0002/0 (a copy: its rows are
/// already in, its announcement is not); the lane's holder is killed right
/// after its checkpoint write. Another worker can take the lane back only
/// after the dead lease has looked unchanged for ttl + margin (and load
/// balancing lets it), about 11 s later. `finish` judged the fleet done on
/// rows alone and stopped it at 12 s: announcements "committed and not
/// ingested". The fleet was not done; the harness stopped waiting.
#[test]
fn regression_finish_waits_for_a_copys_announcement() {
    let _trace = otap_s3pq::oscope_trace::covers("SM", &["CAST-56"]);
    let s = Setup {
        seed: 0,
        workers: 3,
        producers: 1,
        lat_ms: 1,
        s3_timeout_ms: 1_000,
        scale: true,
        zombie_ms: 3_000,
        server_skew_ms: 0,
        worker_skew_ms: vec![0, 0, 0],
        mutant: Mutation::None,
    };
    let sim = SimThread::start(s);
    let _ = sim.run(|f| async move {
        f.w.edge_backlog.set(f.w.edge_backlog.get() + 1);
        let _ = f.edges[0].0.send(EdgeCmd::Write(1));
        sleep_ms(360).await;
        f.w.edge_backlog.set(f.w.edge_backlog.get() + 1);
        let _ = f.edges[0].0.send(EdgeCmd::Restart);
        sleep_ms(378).await;
        let mut slots = f.slots.borrow_mut();
        trace(format!("KILL {}", slots[0].proc.name));
        if let Some(h) = slots[0].handle.take() {
            h.abort();
        }
        let inc = slots[0].inc + 1;
        slots[0] = spawn_worker(&f.w, 0, inc);
        drop(slots);
        // The case's shape: E0002/0 is a copy, and nobody holds the lane now.
        let (missing, _, _) = final_state(&f.w);
        let (ann_missing, _, _) = announcement_state(&f.w);
        if !missing.is_empty() || ann_missing.len() != 1 {
            return Err(format!("not the nightly-7 shape: rows missing {missing:?}, announcements missing {ann_missing:?}"));
        }
        violations(&f)
    });
    let _ = sim.run(finish);
}
