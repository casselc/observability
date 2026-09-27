//! Deterministic simulation of the consumer fleet (DST.md, level 1): the
//! real `Worker`, `gc_step` and edge writers as concurrent tasks on a paused
//! current-thread runtime, against a simulated bucket and central that add
//! latency, pauses, partitions and the ambiguous outcomes of AMBIGUITY.md,
//! all drawn from one seed. A seed gives one trace, byte for byte
//! (`fleet_is_deterministic`).
//!
//! Checked while it runs (every checkpoint write, every statement landing)
//! and at the end:
//! - **neverSkipsCommitted**: a checkpoint written with `next = n` for an
//!   epoch has every committed slot below `n` fully in central;
//! - **noCommitAfterClose**: no data at or after a closed epoch's tombstone;
//! - **statements stay inside their lease**: a statement is issued only while
//!   its worker's lease is the lane's current one, and lands (commits) before
//!   that lease version changes hands (a takeover, or a release);
//! - **exactly once** at the end: every committed content key in central
//!   with its committed rows (atMostOnce + neverSkipsCommitted + liveness),
//!   nothing uncommitted.
//!
//!   cargo test --release --test dst_consumer -- fleet                  # 40 seeds
//!   DST_SEEDS=2000 cargo test --release --test dst_consumer -- fleet_seeds
//!   DST_SEED=17 DST_TRACE=1 cargo test --release --test dst_consumer -- fleet_seeds --nocapture

#[path = "../src/consumer/mod.rs"]
#[allow(dead_code, unused_imports, clippy::all)]
mod consumer;
mod dst;

use async_trait::async_trait;
use bytes::Bytes;
use consumer::bucket::{Bucket, Cond, Counts, Item, MemBucket, Meta, Put};
use consumer::coord::{CkptDoc, LeaseDoc, Mutation, Timing, join};
use consumer::discovery::Backoff;
use consumer::gc::{GcConfig, gc_step};
use consumer::plan::{CheckRange, DAY_NS, Obj};
use consumer::sql::{Central, Fence, InsertErr, LaneKind, settles_at_once};
use consumer::worker::{BalanceMode, Clock, Config, Worker};
use dst::sim::{self, Sim, now_ms, sleep_ms, sleep_until_ms, trace};
use futures::FutureExt;
use otap_s3pq::proto;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::rc::Rc;

const ROOT: &str = "r/edges";
const CTL: &str = "r/ctl";
const SIGNALS: [&str; 3] = ["traces", "logs", "metrics_gauge"];
/// The tests' compressed timing (consumer/tests.rs), margin ≥ slack.
const T: Timing = Timing { ttl_ms: 9000, margin_ms: 1000, budget_ms: 3000, slack_ms: 1000, mutation: Mutation::None };

// ---- the per-seed configuration ---------------------------------------------------

/// Everything a seed decides up front. Rates are per request (faults) or
/// per second (process events); each is drawn from a small menu, so some
/// seeds are calm and some hostile.
#[derive(Clone, Debug)]
struct Profile {
    workers: usize,
    producers: usize,
    scale: bool,
    zombie_ms: u64,
    run_ms: u64,
    /// One-way network latency, max (uniform from 0).
    lat_ms: u64,
    /// The S3 client's give-up time (no answer).
    s3_timeout_ms: u64,
    /// GET / HEAD / LIST answered 503.
    s3_read_err: f64,
    /// A PUT that never arrives (AMBIGUITY: no answer, not applied).
    put_drop: f64,
    /// A PUT applied, its answer lost (AMBIGUITY: answer lost after the effect).
    put_lost: f64,
    /// A conditional PUT applied, then answered 412: a 5xx / 409 after it
    /// applied, and the client's retry met its own write (AMBIGUITY: S3
    /// conditional PUT).
    put_own_412: f64,
    /// A PUT whose answer was lost lands later still (up to this long after
    /// the client gave up; 0: off).
    put_late_ms: u64,
    /// Switch: LIST doesn't show objects younger than this (0: strongly
    /// consistent LIST, as AWS S3 and SeaweedFS; off by default).
    list_lag_ms: u64,
    ch_timeout_ms: u64,
    ch_check_err: f64,
    /// An insert refused before writing (TOO_MANY_PARTS: settled).
    ch_settled_err: f64,
    /// An insert that writes its first object only, then fails.
    ch_partial: f64,
    /// An insert that lands, its answer lost.
    ch_lost: f64,
    /// An insert with no answer that lands as late as it can (fence + budget + slack).
    ch_late: f64,
    /// An insert answered TIMEOUT_EXCEEDED that commits later anyway
    /// (AMBIGUITY: the Keeper overrun, D9).
    ch_timeout_commit: f64,
    /// The server's clock minus true time.
    server_skew_ms: i64,
    /// Per worker: its wall clock minus true time.
    worker_skew_ms: Vec<i64>,
    /// Process events per second of simulated time.
    pause_rate: f64,
    kill_rate: f64,
    cut_rate: f64,
    edge_restart_rate: f64,
    /// Scripted: (worker, at ms, delay ms): that worker's next LIST of the
    /// heartbeats answers `delay` late.
    slow_list: Option<(usize, u64, u64)>,
    /// Scripted: (worker, at ms, for ms): a pause.
    pause_at: Option<(usize, u64, u64)>,
}

impl Profile {
    fn draw(sim: &Sim) -> Profile {
        let pick_f = |xs: &[f64]| *sim.rng.borrow_mut().pick(xs);
        let pick_u = |xs: &[u64]| *sim.rng.borrow_mut().pick(xs);
        let workers = sim.range(1, 4) as usize;
        let margin = T.margin_ms as i64;
        // |worker − server| ≤ margin: the clock assumption of D9 (coord.rs `Timing::check`).
        let server_skew_ms = sim.range(0, margin as u64) as i64 - margin / 2;
        let worker_skew_ms = (0..workers).map(|_| sim.range(0, margin as u64) as i64 - margin / 2).collect();
        Profile {
            workers,
            producers: sim.range(1, 3) as usize,
            scale: sim.chance(0.5),
            zombie_ms: pick_u(&[3_000, 60_000]),
            run_ms: sim.range(40_000, 120_000),
            lat_ms: pick_u(&[1, 5, 20, 60]),
            s3_timeout_ms: pick_u(&[1_000, 3_000]),
            s3_read_err: pick_f(&[0.0, 0.01, 0.05]),
            put_drop: pick_f(&[0.0, 0.02, 0.08]),
            put_lost: pick_f(&[0.0, 0.02, 0.08]),
            put_own_412: pick_f(&[0.0, 0.02, 0.08]),
            put_late_ms: pick_u(&[0, 0, 1_500]),
            list_lag_ms: if std::env::var_os("DST_LIST_LAG").is_some() { pick_u(&[0, 500, 3_000]) } else { 0 },
            ch_timeout_ms: T.budget_ms + pick_u(&[500, 2_000]),
            ch_check_err: pick_f(&[0.0, 0.02, 0.1]),
            ch_settled_err: pick_f(&[0.0, 0.03]),
            ch_partial: pick_f(&[0.0, 0.05, 0.15]),
            ch_lost: pick_f(&[0.0, 0.05, 0.15]),
            ch_late: pick_f(&[0.0, 0.05, 0.1]),
            ch_timeout_commit: pick_f(&[0.0, 0.05, 0.1]),
            server_skew_ms,
            worker_skew_ms,
            pause_rate: pick_f(&[0.0, 0.01, 0.05]),
            kill_rate: pick_f(&[0.0, 0.01, 0.03]),
            cut_rate: pick_f(&[0.0, 0.01, 0.05]),
            edge_restart_rate: pick_f(&[0.0, 0.02, 0.1]),
            slow_list: None,
            pause_at: None,
        }
    }

    /// No faults at all.
    fn calm(workers: usize, producers: usize, run_ms: u64) -> Profile {
        Profile {
            workers,
            producers,
            scale: false,
            zombie_ms: 60_000,
            run_ms,
            lat_ms: 5,
            s3_timeout_ms: 1_000,
            s3_read_err: 0.0,
            put_drop: 0.0,
            put_lost: 0.0,
            put_own_412: 0.0,
            put_late_ms: 0,
            list_lag_ms: 0,
            ch_timeout_ms: T.budget_ms + 1_000,
            ch_check_err: 0.0,
            ch_settled_err: 0.0,
            ch_partial: 0.0,
            ch_lost: 0.0,
            ch_late: 0.0,
            ch_timeout_commit: 0.0,
            server_skew_ms: 0,
            worker_skew_ms: vec![0; workers],
            pause_rate: 0.0,
            kill_rate: 0.0,
            cut_rate: 0.0,
            edge_restart_rate: 0.0,
            slow_list: None,
            pause_at: None,
        }
    }
}

// ---- the simulated world ------------------------------------------------------------

#[derive(Default)]
struct ChState {
    rows: BTreeMap<(String, String), u64>,
    by_day: BTreeMap<(String, String, u64), u64>,
}

#[derive(Default, Debug)]
struct Tally {
    put_drop: u64,
    put_lost: u64,
    put_own_412: u64,
    put_late: u64,
    read_err: u64,
    cut_reqs: u64,
    stmts: u64,
    fenced: u64,
    settled_err: u64,
    partial: u64,
    lost: u64,
    late: u64,
    timeout_commit: u64,
    landed: u64,
    check_err: u64,
    pauses: u64,
    kills: u64,
    cuts: u64,
    edge_restarts: u64,
    ckpt_checks: u64,
    gc_runs: u64,
    gc_deleted: u64,
}

struct World {
    sim: Rc<Sim>,
    p: Profile,
    mem: MemBucket,
    ch: RefCell<ChState>,
    /// Every data object ever created: (lane, epoch, seq) -> content.
    ever: RefCell<BTreeMap<(String, String, u64), String>>,
    /// (table, content) -> rows, for every committed content key.
    rows_of: RefCell<BTreeMap<(String, String), u64>>,
    violations: RefCell<Vec<String>>,
    tally: RefCell<Tally>,
    stmt_n: Cell<u64>,
    /// Per lane: the lease document last written, and when (simulated ms).
    leases: RefCell<BTreeMap<String, (LeaseDoc, u64)>>,
    /// After the chaos: no new faults (liveness is judged healed).
    healed: Cell<bool>,
}

fn table_of(signal: &str) -> String {
    otap_s3pq::Signal::from_name(signal).expect("signal").table().to_string()
}

/// `r/edges/{producer}/{signal}/{epoch}/{seq}.parquet` -> (lane, epoch, seq).
fn parse_slot(key: &str) -> Option<(String, String, u64)> {
    let rest = key.strip_prefix(ROOT)?.strip_prefix('/')?;
    let parts: Vec<&str> = rest.split('/').collect();
    if parts.len() != 4 {
        return None;
    }
    let seq = parts[3].strip_suffix(".parquet")?.parse().ok()?;
    Some((format!("{}/{}", parts[0], parts[1]), parts[2].to_string(), seq))
}

impl World {
    /// A fault with probability `p`, unless the run is in its healed phase.
    fn fault(&self, p: f64) -> bool {
        !self.healed.get() && self.sim.chance(p)
    }

    fn violation(&self, v: String) {
        trace(format!("VIOLATION {v}"));
        self.violations.borrow_mut().push(format!("t={} {v}", now_ms()));
    }

    fn count(&self, table: &str, content: &str) -> u64 {
        self.ch.borrow().rows.get(&(table.to_string(), content.to_string())).copied().unwrap_or(0)
    }

    fn lease_of(&self, lane: &str) -> Option<LeaseDoc> {
        let key = format!("{}.json", join(&join(CTL, "lease"), lane));
        self.mem.objs.borrow().get(&key).and_then(|o| serde_json::from_slice(&o.body).ok())
    }

    /// The server's clock, ms.
    fn server_now(&self) -> u64 {
        (self.sim.wall() as i64 + self.p.server_skew_ms) as u64
    }

    /// Simulated ms at which the server's clock reads `srv`.
    fn at_server(&self, srv: u64) -> u64 {
        (srv as i64 - self.p.server_skew_ms - self.sim.wall0_ms as i64).max(0) as u64
    }

    /// After a PUT applied: record data objects, check checkpoints.
    fn applied(&self, key: &str, meta: &BTreeMap<String, String>, body: &Bytes) {
        if let Some((lane, epoch, seq)) = parse_slot(key) {
            if meta.get(proto::META_KIND).map(String::as_str) == Some(proto::KIND_DATA) {
                let content = meta.get(proto::META_CONTENT).cloned().unwrap_or_default();
                let _ = self.ever.borrow_mut().insert((lane.clone(), epoch.clone(), seq), content);
                if let Some(ck) = self.ckpt_of(&lane) {
                    if ck.closed(&epoch) && seq >= ck.next(&epoch) {
                        self.violation(format!("noCommitAfterClose: data at {lane} {epoch}/{seq} after its tombstone at {}", ck.next(&epoch)));
                    }
                }
            }
            return;
        }
        let lprefix = join(CTL, "lease");
        if let Some(lane) = key.strip_prefix(&format!("{lprefix}/")).and_then(|r| r.strip_suffix(".json")) {
            let Ok(doc) = serde_json::from_slice::<LeaseDoc>(body) else {
                self.violation(format!("lease {key} does not parse"));
                return;
            };
            // No takeover of a live lease: a lease held by someone else may be
            // taken only once its version has stood unchanged for ttl + margin
            // (the taker's observation starts no earlier than the write).
            if let Some((prev, at)) = self.leases.borrow().get(lane) {
                let age = now_ms() - at;
                if !prev.owner.is_empty() && !doc.owner.is_empty() && doc.owner != prev.owner && age < T.ttl_ms + T.margin_ms {
                    self.violation(format!(
                        "{} took {lane} (epoch {}) from {} (epoch {}), whose lease was written {age} ms ago (< ttl + margin)",
                        doc.owner, doc.epoch, prev.owner, prev.epoch
                    ));
                }
            }
            let _ = self.leases.borrow_mut().insert(lane.to_string(), (doc, now_ms()));
            return;
        }
        let cprefix = join(CTL, "ckpt");
        if let Some(lane) = key.strip_prefix(&format!("{cprefix}/")).and_then(|r| r.strip_suffix(".json")) {
            let Ok(doc) = serde_json::from_slice::<CkptDoc>(body) else {
                self.violation(format!("checkpoint {key} does not parse"));
                return;
            };
            self.check_ckpt(lane, &doc);
        }
    }

    fn ckpt_of(&self, lane: &str) -> Option<CkptDoc> {
        let key = format!("{}.json", join(&join(CTL, "ckpt"), lane));
        self.mem.objs.borrow().get(&key).and_then(|o| serde_json::from_slice(&o.body).ok())
    }

    /// neverSkipsCommitted, at the moment a checkpoint is written.
    fn check_ckpt(&self, lane: &str, doc: &CkptDoc) {
        self.tally.borrow_mut().ckpt_checks += 1;
        let table = table_of(lane.rsplit('/').next().unwrap_or(lane));
        let rows_of = self.rows_of.borrow();
        let ever = self.ever.borrow();
        for (e, pos) in &doc.epochs {
            let lo = (lane.to_string(), e.clone(), 0);
            let hi = (lane.to_string(), e.clone(), pos.next);
            for ((_, _, s), content) in ever.range(lo..hi) {
                let need = rows_of.get(&(table.clone(), content.clone())).copied().unwrap_or(0);
                let have = self.count(&table, content);
                if have < need {
                    self.violation(format!(
                        "neverSkipsCommitted: checkpoint v{} of {lane} moves {e} to {} past slot {s} ({content}), central holds {have}/{need} rows",
                        doc.version, pos.next
                    ));
                }
            }
            if pos.closed {
                if let Some(((_, _, s), c)) = ever.range((lane.to_string(), e.clone(), pos.next)..(lane.to_string(), e.clone(), u64::MAX)).next() {
                    self.violation(format!("noCommitAfterClose: {lane} {e} closed at {} with data at {s} ({c})", pos.next));
                }
            }
        }
    }

    /// A statement lands: its rows go in, if its lanes' lease versions are
    /// still the ones it was issued under.
    fn land(&self, n: u64, table: &str, objs: &[Obj], leases: &[(String, u64)], repair: bool) {
        for (lane, ep) in leases {
            let cur = self.lease_of(lane);
            if cur.as_ref().map(|d| d.epoch) != Some(*ep) {
                self.violation(format!(
                    "statement #{n} lands on {lane} after its lease epoch {ep} changed hands (now {:?})",
                    cur.map(|d| (d.owner, d.epoch))
                ));
            }
        }
        self.tally.borrow_mut().landed += 1;
        let mut ch = self.ch.borrow_mut();
        for o in objs {
            let have = ch.rows.get(&(table.to_string(), o.content.clone())).copied().unwrap_or(0);
            let add = if repair { o.rows.saturating_sub(have) } else { o.rows };
            *ch.rows.entry((table.to_string(), o.content.clone())).or_default() += add;
            if add > 0 {
                *ch.by_day.entry((table.to_string(), o.content.clone(), o.received_ns / DAY_NS)).or_default() += add;
            }
            trace(format!("CH #{n} lands {table} {} +{add} rows (now {})", o.content, have + add));
        }
    }
}

// ---- processes -------------------------------------------------------------------------

/// A process's fate: paused (a stop-the-world pause: nothing sent, no answer
/// taken) or cut off the network until a time.
struct Proc {
    name: String,
    paused_until: Cell<u64>,
    cut_until: Cell<u64>,
    /// The next LIST of the heartbeats answers this much later (a slow request).
    slow_list_ms: Cell<u64>,
}

impl Proc {
    fn new(name: &str) -> Rc<Proc> {
        Rc::new(Proc { name: name.into(), paused_until: Cell::new(0), cut_until: Cell::new(0), slow_list_ms: Cell::new(0) })
    }
    async fn gate(&self) {
        while now_ms() < self.paused_until.get() {
            sleep_until_ms(self.paused_until.get()).await;
        }
    }
    fn cut(&self) -> bool {
        now_ms() < self.cut_until.get()
    }
}

#[derive(Clone)]
struct SimClock {
    skew_ms: i64,
    wall0: u64,
}

impl Clock for SimClock {
    fn mono(&self) -> u64 {
        1_000_000 + now_ms()
    }
    fn wall(&self) -> u64 {
        (self.wall0 as i64 + now_ms() as i64 + self.skew_ms) as u64
    }
}

// ---- the bucket ------------------------------------------------------------------------

struct SimBucket {
    w: Rc<World>,
    p: Rc<Proc>,
}

impl SimBucket {
    /// One request: the pause gate, the network (a cut or a drop: no answer
    /// by the client's timeout), `effect` at arrival, the answer back.
    async fn req<T: std::fmt::Debug>(&self, what: &str, drop: bool, lose: bool, effect: impl FnOnce() -> T) -> Result<T, String> {
        let w = &self.w;
        self.p.gate().await;
        let deadline = now_ms() + w.p.s3_timeout_ms;
        if self.p.cut() || drop {
            if self.p.cut() {
                w.tally.borrow_mut().cut_reqs += 1;
            }
            sleep_until_ms(deadline).await;
            trace(format!("{} {what} -> no answer (not delivered)", self.p.name));
            return Err("injected: no answer (request not delivered)".into());
        }
        sleep_ms(w.sim.range(0, w.p.lat_ms)).await;
        let r = effect();
        if lose || self.p.cut() {
            sleep_until_ms(deadline.max(now_ms())).await;
            trace(format!("{} {what} -> {r:?}, answer lost", self.p.name));
            return Err("injected: no answer (answer lost)".into());
        }
        sleep_ms(w.sim.range(0, w.p.lat_ms)).await;
        self.p.gate().await;
        trace(format!("{} {what} -> {r:?}", self.p.name));
        Ok(r)
    }

    fn read_fault(&self) -> bool {
        let f = self.w.fault(self.w.p.s3_read_err);
        if f {
            self.w.tally.borrow_mut().read_err += 1;
        }
        f
    }
}

#[async_trait(?Send)]
impl Bucket for SimBucket {
    async fn get(&self, key: &str) -> Result<Option<(Bytes, String)>, String> {
        if self.read_fault() {
            let _ = self.req(&format!("GET {key}"), false, false, || "503").await;
            return Err("injected: 503 Slow Down".into());
        }
        let r = self.req(&format!("GET {key}"), false, false, || self.w.mem.get(key).now_or_never().expect("mem").expect("mem")).await?;
        Ok(r)
    }

    async fn put(&self, key: &str, body: Bytes, cond: Cond<'_>, meta: &BTreeMap<String, String>) -> Put {
        let w = self.w.clone();
        let drop = w.fault(w.p.put_drop);
        let lose = !drop && w.fault(w.p.put_lost);
        let own412 = !drop && !lose && !matches!(cond, Cond::None) && w.fault(w.p.put_own_412);
        let late = drop && w.p.put_late_ms > 0 && w.sim.chance(0.5);
        {
            let mut t = w.tally.borrow_mut();
            t.put_drop += (drop && !late) as u64;
            t.put_lost += lose as u64;
            t.put_own_412 += own412 as u64;
            t.put_late += late as u64;
        }
        let what = format!("PUT {key} {cond:?}");
        let cond_owned: Option<Option<String>> = match cond {
            Cond::None => None,
            Cond::Create => Some(None),
            Cond::IfMatch(e) => Some(Some(e.to_string())),
        };
        let apply = {
            let w = w.clone();
            let key = key.to_string();
            let meta = meta.clone();
            let body = body.clone();
            move || {
                w.mem.clock.set(w.sim.wall());
                let c = match &cond_owned {
                    None => Cond::None,
                    Some(None) => Cond::Create,
                    Some(Some(e)) => Cond::IfMatch(e),
                };
                let r = w.mem.put(&key, body.clone(), c, &meta).now_or_never().expect("mem");
                if let Put::Ok(_) = &r {
                    w.applied(&key, &meta, &body);
                }
                r
            }
        };
        if late {
            // The request is stuck on the way and lands after the client gave up.
            let delay = w.p.s3_timeout_ms + w.sim.range(0, w.p.put_late_ms);
            let what2 = what.clone();
            let name = self.p.name.clone();
            std::mem::drop(tokio::task::spawn_local(async move {
                sleep_ms(delay).await;
                let r = apply();
                trace(format!("{name} {what2} -> {r:?} (landed late, after the client gave up)"));
            }));
            let _ = self.req(&what, true, false, || ()).await;
            return Put::Unknown("injected: no answer (the PUT lands later)".into());
        }
        match self.req(&what, drop, lose, apply).await {
            Err(e) => Put::Unknown(e),
            Ok(Put::Ok(_)) if own412 => {
                trace(format!("{} {what} -> 412 (it applied; a 5xx, then the retry met our own write)", self.p.name));
                Put::Conflict
            }
            Ok(r) => r,
        }
    }

    async fn head(&self, key: &str) -> Result<Option<Meta>, String> {
        if self.read_fault() {
            let _ = self.req(&format!("HEAD {key}"), false, false, || "503").await;
            return Err("injected: 503 Slow Down".into());
        }
        // Sorted for the trace (a HashMap's order is seeded, but would shift with every new map).
        let r = self
            .req(&format!("HEAD {key}"), false, false, || {
                self.w.mem.head(key).now_or_never().expect("mem").expect("mem").map(|m| m.into_iter().collect::<BTreeMap<_, _>>())
            })
            .await?;
        Ok(r.map(|m| m.into_iter().collect()))
    }

    async fn list(&self, prefix: &str, start_after: Option<&str>) -> Result<Vec<Item>, String> {
        if self.read_fault() {
            let _ = self.req(&format!("LIST {prefix}"), false, false, || "503").await;
            return Err("injected: 503 Slow Down".into());
        }
        let lag = self.w.p.list_lag_ms;
        let what = format!("LIST {prefix} after {start_after:?}");
        let r = self
            .req(&what, false, false, || {
                let all = self.w.mem.list(prefix, start_after).now_or_never().expect("mem").expect("mem");
                let wall = self.w.sim.wall();
                all.into_iter().filter(|i| lag == 0 || i.modified_ms + lag <= wall || parse_slot(&i.key).is_none()).collect::<Vec<_>>()
            })
            .await?;
        if prefix.ends_with("/workers") && self.p.slow_list_ms.get() > 0 {
            let d = self.p.slow_list_ms.replace(0);
            trace(format!("{} {what}: its answer is {d} ms late", self.p.name));
            sleep_ms(d).await;
        }
        Ok(r)
    }

    async fn list_dirs(&self, prefix: &str) -> Result<Vec<String>, String> {
        if self.read_fault() {
            let _ = self.req(&format!("LISTDIRS {prefix}"), false, false, || "503").await;
            return Err("injected: 503 Slow Down".into());
        }
        self.req(&format!("LISTDIRS {prefix}"), false, false, || self.w.mem.list_dirs(prefix).now_or_never().expect("mem").expect("mem")).await
    }

    async fn delete(&self, keys: &[String]) -> Result<usize, String> {
        let r = self
            .req(&format!("DELETE {} keys", keys.len()), false, false, || {
                for k in keys {
                    trace(format!("  delete {k}"));
                }
                self.w.mem.delete(keys).now_or_never().expect("mem")
            })
            .await?;
        self.w.tally.borrow_mut().gc_deleted += keys.len() as u64;
        r
    }

    fn counts(&self) -> &Counts {
        &self.w.mem.counts
    }

    fn object_url(&self, key: &str) -> String {
        format!("http://s3/{key}")
    }

    fn path_of(&self, key: &str) -> String {
        format!("b/{key}")
    }
}

// ---- central -------------------------------------------------------------------------------

struct SimCentral {
    w: Rc<World>,
    p: Rc<Proc>,
}

#[derive(Clone, Copy, Debug)]
enum Outcome {
    Normal,
    SettledErr,
    Partial,
    Lost,
    Late,
    TimeoutCommit,
}

fn no_answer(msg: &str) -> InsertErr {
    InsertErr { msg: msg.into(), settled: false, answered: false, range: false }
}

impl SimCentral {
    fn draw(&self, repair: bool) -> Outcome {
        let (w, p) = (&self.w, &self.w.p);
        let o = if w.fault(p.ch_settled_err) {
            Outcome::SettledErr
        } else if !repair && w.fault(p.ch_partial) {
            Outcome::Partial
        } else if w.fault(p.ch_lost) {
            Outcome::Lost
        } else if w.fault(p.ch_late) {
            Outcome::Late
        } else if w.fault(p.ch_timeout_commit) {
            Outcome::TimeoutCommit
        } else {
            Outcome::Normal
        };
        let mut t = w.tally.borrow_mut();
        match o {
            Outcome::SettledErr => t.settled_err += 1,
            Outcome::Partial => t.partial += 1,
            Outcome::Lost => t.lost += 1,
            Outcome::Late => t.late += 1,
            Outcome::TimeoutCommit => t.timeout_commit += 1,
            Outcome::Normal => {}
        }
        o
    }

    /// A statement (an insert, or a row repair): issued now, run on the
    /// server as its own task (a killed client doesn't stop it), landing and
    /// answering as `draw` says.
    async fn statement(&self, k: &LaneKind, objs: Vec<Obj>, fence: Fence, repair: bool) -> Result<(), InsertErr> {
        let w = self.w.clone();
        let n = w.stmt_n.get() + 1;
        w.stmt_n.set(n);
        w.tally.borrow_mut().stmts += 1;
        // Issued inside the lease: the lane's current lease is this worker's.
        let mut leases = Vec::new();
        let lanes: BTreeSet<&str> = objs.iter().map(|o| o.lane.as_str()).collect();
        for lane in lanes {
            match w.lease_of(lane) {
                Some(d) if d.owner == self.p.name => leases.push((lane.to_string(), d.epoch)),
                other => w.violation(format!(
                    "statement #{n} from {} for {lane} issued while the lease is {:?}",
                    self.p.name,
                    other.map(|d| (d.owner, d.epoch))
                )),
            }
        }
        let contents: Vec<&str> = objs.iter().map(|o| o.content.as_str()).collect();
        let what = format!("CH #{n} {} {} {contents:?} fence {}", if repair { "REPAIR" } else { "INSERT" }, k.table, fence.wall_ms);
        self.p.gate().await;
        if self.p.cut() {
            w.tally.borrow_mut().cut_reqs += 1;
            sleep_ms(w.p.ch_timeout_ms).await;
            trace(format!("{} {what} -> no answer (not delivered)", self.p.name));
            return Err(no_answer("injected: no answer (not delivered)"));
        }
        let outcome = self.draw(repair);
        trace(format!("{} {what} sent ({outcome:?})", self.p.name));
        let (tx, rx) = tokio::sync::oneshot::channel::<Result<(), InsertErr>>();
        let table = k.table.clone();
        let req_lat = w.sim.range(0, w.p.lat_ms);
        // Mostly quick, sometimes close to the budget.
        let exec = if w.sim.chance(0.05) { w.sim.range(300, T.budget_ms * 3 / 4) } else { w.sim.range(10, 300) };
        let latest_srv = fence.wall_ms + fence.budget_ms + T.slack_ms;
        let w2 = w.clone();
        drop(tokio::task::spawn_local(async move {
            let w = w2;
            sleep_ms(req_lat).await;
            if w.server_now() > fence.wall_ms {
                w.tally.borrow_mut().fenced += 1;
                trace(format!("CH #{n} fenced (server {} > fence {})", w.server_now(), fence.wall_ms));
                let _ = tx.send(Ok(()));
                return;
            }
            match outcome {
                Outcome::Normal => {
                    sleep_ms(exec).await;
                    w.land(n, &table, &objs, &leases, repair);
                    let _ = tx.send(Ok(()));
                }
                Outcome::SettledErr => {
                    let msg = "clickhouse 500: Code: 252. DB::Exception: Too many parts (TOO_MANY_PARTS)".to_string();
                    let _ = tx.send(Err(InsertErr { settled: settles_at_once(&msg), answered: true, range: false, msg }));
                }
                Outcome::Partial => {
                    sleep_ms(exec).await;
                    w.land(n, &table, &objs[..1], &leases, repair);
                    let _ = tx.send(Err(InsertErr { msg: "injected: the statement died after its first part".into(), settled: true, answered: true, range: false }));
                }
                Outcome::Lost => {
                    sleep_ms(exec).await;
                    w.land(n, &table, &objs, &leases, repair);
                    drop(tx);
                }
                Outcome::Late => {
                    let at = w.at_server(latest_srv);
                    sleep_until_ms(w.sim.range(now_ms(), at.max(now_ms()))).await;
                    w.land(n, &table, &objs, &leases, repair);
                    drop(tx);
                }
                Outcome::TimeoutCommit => {
                    sleep_ms(fence.budget_ms).await;
                    let msg = "clickhouse 500: Code: 159. DB::Exception: Timeout exceeded: elapsed 3000 ms, maximum: 3000 ms. (TIMEOUT_EXCEEDED)".to_string();
                    let _ = tx.send(Err(InsertErr { settled: settles_at_once(&msg), answered: true, range: false, msg }));
                    let at = w.at_server(latest_srv);
                    sleep_until_ms(w.sim.range(now_ms(), at.max(now_ms()))).await;
                    w.land(n, &table, &objs, &leases, repair);
                }
            }
        }));
        let r = match tokio::time::timeout(std::time::Duration::from_millis(w.p.ch_timeout_ms), rx).await {
            Ok(Ok(r)) => r,
            _ => Err(no_answer("injected: no answer (timeout)")),
        };
        sleep_ms(w.sim.range(0, w.p.lat_ms)).await;
        self.p.gate().await;
        let r = if self.p.cut() { Err(no_answer("injected: no answer (cut off)")) } else { r };
        trace(format!("{} CH #{n} -> {:?}", self.p.name, r.as_ref().map_err(|e| (&e.msg, e.settled, e.answered))));
        r
    }
}

#[async_trait(?Send)]
impl Central for SimCentral {
    async fn ensure(&self, _k: &LaneKind) -> Result<(), String> {
        Ok(())
    }

    fn ranged(&self, _k: &LaneKind) -> bool {
        true
    }

    async fn counts(&self, k: &LaneKind, contents: &[&str], range: Option<CheckRange>) -> Result<HashMap<String, u64>, String> {
        let w = &self.w;
        self.p.gate().await;
        let what = format!("CH COUNT {} {contents:?} range {:?}", k.table, range.map(|r| (r.lo_ns / DAY_NS, r.hi_ns / DAY_NS)));
        if self.p.cut() || w.fault(w.p.ch_check_err) {
            w.tally.borrow_mut().check_err += 1;
            sleep_ms(w.p.ch_timeout_ms).await;
            trace(format!("{} {what} -> no answer", self.p.name));
            return Err("clickhouse: injected: timeout".into());
        }
        sleep_ms(w.sim.range(0, w.p.lat_ms)).await;
        let snap: BTreeMap<String, u64> = {
            let ch = w.ch.borrow();
            contents
                .iter()
                .filter_map(|c| {
                    let n = match range {
                        None => ch.rows.get(&(k.table.clone(), c.to_string())).copied().unwrap_or(0),
                        Some(r) => ch
                            .by_day
                            .range((k.table.clone(), c.to_string(), r.lo_ns / DAY_NS)..=(k.table.clone(), c.to_string(), r.hi_ns / DAY_NS))
                            .map(|(_, n)| *n)
                            .sum(),
                    };
                    (n > 0).then(|| (c.to_string(), n))
                })
                .collect()
        };
        sleep_ms(w.sim.range(0, w.p.lat_ms)).await;
        self.p.gate().await;
        trace(format!("{} {what} -> {snap:?}", self.p.name));
        Ok(snap.into_iter().collect())
    }

    async fn insert(&self, k: &LaneKind, objs: &[&Obj], fence: Fence, _token: &str, _guard: bool) -> Result<(), InsertErr> {
        self.statement(k, objs.iter().map(|o| (*o).clone()).collect(), fence, false).await
    }

    async fn repair(&self, k: &LaneKind, obj: &Obj, fence: Fence, _token: &str) -> Result<(), InsertErr> {
        self.statement(k, vec![obj.clone()], fence, true).await
    }
}

// ---- edges ------------------------------------------------------------------------------------

/// A writer lane as the exporter runs it: create-only slots, HEAD on an
/// ambiguous answer, a new epoch on a tombstone or a restart (resending
/// its last batch, which keeps its received time).
struct Edge {
    producer: String,
    signal: String,
    epoch: String,
    n_epochs: u32,
    next: u64,
    last: Option<(String, u64, u64)>,
}

impl Edge {
    fn prefix(&self) -> String {
        format!("{ROOT}/{}/{}", self.producer, self.signal)
    }
    fn new_epoch(&mut self) {
        self.n_epochs += 1;
        self.epoch = format!("E{:04}", self.n_epochs);
        self.next = 0;
    }
    async fn commit(&mut self, b: &SimBucket, content: &str, rows: u64, recv_ns: u64) {
        loop {
            let key = proto::slot_key(&self.prefix(), &self.epoch, self.next);
            let mut m = BTreeMap::new();
            for (k, v) in [
                (proto::META_KIND, proto::KIND_DATA.to_string()),
                (proto::META_EPOCH, self.epoch.clone()),
                (proto::META_SEQ, self.next.to_string()),
                (proto::META_CONTENT, content.to_string()),
                (proto::META_ROWS, rows.to_string()),
            ] {
                let _ = m.insert(k.to_string(), v);
            }
            if recv_ns > 0 {
                let _ = m.insert(proto::META_RECEIVED.to_string(), recv_ns.to_string());
            }
            match b.put(&key, Bytes::from_static(&[0u8; 64]), Cond::Create, &m).await {
                Put::Ok(_) => {
                    self.next += 1;
                    return;
                }
                _ => loop {
                    match b.head(&key).await {
                        Ok(Some(h)) => {
                            match proto::Slot::from_meta(&h) {
                                proto::Slot::Tomb => self.new_epoch(),
                                proto::Slot::Data { epoch, content: c } if epoch == self.epoch && c == content => {
                                    self.next += 1;
                                    return; // ours: it applied
                                }
                                _ => self.next += 1,
                            }
                            break;
                        }
                        Ok(None) => break, // not applied (or not yet): write it again
                        Err(_) => sleep_ms(200).await,
                    }
                },
            }
        }
    }
}

// ---- the scenario -------------------------------------------------------------------------

thread_local! {
    /// A deliberate bug in the workers (`coord::Mutation`), for `fleet_catches_mutants`.
    static MUTANT: Cell<Mutation> = const { Cell::new(Mutation::None) };
}

fn worker_cfg(name: &str, scale: bool) -> Config {
    let mut c = Config::new(ROOT, CTL, name);
    c.timing = T;
    c.timing.mutation = MUTANT.with(|m| m.get());
    c.discover_ms = 500;
    c.lanes_every_ms = 2_000;
    c.full_list_ms = 5_000;
    c.quiet_ms = 2_000;
    c.limits.max_objects = 4;
    c.backoff = Backoff::off();
    c.poll_ms = 200;
    c.balance.mode = BalanceMode::Count;
    if scale {
        c.backoff = Backoff { after_ms: 1_000, min_ms: 500, max_ms: 8_000, jitter: 0.2 };
        c.linger_ms = 300;
        c.balance.mode = BalanceMode::Load;
        c.balance.min_hold_ms = 2_000;
        c.balance.loads_every_ms = 1_000;
        c.balance.window_ms = 10_000;
        c.balance.base_weight = 1.0;
        c.horizon_ms = Some(86_400_000);
    }
    c
}

struct Slot {
    proc: Rc<Proc>,
    handle: Option<tokio::task::JoinHandle<()>>,
    inc: u32,
}

fn spawn_worker(w: &Rc<World>, i: usize, inc: u32) -> Slot {
    let name = format!("w{i}-{inc}");
    let proc = Proc::new(&name);
    let b = Rc::new(SimBucket { w: w.clone(), p: proc.clone() });
    let c = Rc::new(SimCentral { w: w.clone(), p: proc.clone() });
    let clock = SimClock { skew_ms: w.p.worker_skew_ms[i], wall0: w.sim.wall0_ms };
    let mut wk = Worker::new(worker_cfg(&name, w.p.scale), b, c, clock);
    let sim = w.sim.clone();
    let p2 = proc.clone();
    trace(format!("START {name}"));
    let handle = tokio::task::spawn_local(async move {
        loop {
            p2.gate().await;
            let _ = wk.step().await;
            let poll = wk.cfg.poll_ms + sim.range(0, 100);
            let sleep = match wk.next_wake() {
                Some(at) => poll.min(at.saturating_sub(1_000_000 + now_ms()).max(5)),
                None => poll,
            };
            sleep_ms(sleep).await;
        }
    });
    Slot { proc, handle: Some(handle), inc }
}

async fn fleet(sim: Rc<Sim>) -> String {
    let p = Profile::draw(&sim);
    fleet_with(sim, p).await
}

async fn fleet_with(sim: Rc<Sim>, p: Profile) -> String {
    trace(format!("PROFILE {p:?}"));
    let w = Rc::new(World {
        sim: sim.clone(),
        p: p.clone(),
        mem: MemBucket::default(),
        ch: RefCell::new(ChState::default()),
        ever: RefCell::new(BTreeMap::new()),
        rows_of: RefCell::new(BTreeMap::new()),
        violations: RefCell::new(Vec::new()),
        tally: RefCell::new(Tally::default()),
        stmt_n: Cell::new(0),
        leases: RefCell::new(BTreeMap::new()),
        healed: Cell::new(false),
    });
    let stop = Rc::new(Cell::new(false));
    // Edges.
    let mut edge_tasks = Vec::new();
    let n_content = Rc::new(Cell::new(0u64));
    for pi in 0..p.producers {
        for s in SIGNALS {
            let w = w.clone();
            let stop = stop.clone();
            let n_content = n_content.clone();
            let name = format!("edge-p{pi}-{s}");
            edge_tasks.push(tokio::task::spawn_local(async move {
                let b = SimBucket { w: w.clone(), p: Proc::new(&name) };
                let mut e = Edge { producer: format!("p{pi}"), signal: s.to_string(), epoch: String::new(), n_epochs: 0, next: 0, last: None };
                e.new_epoch();
                let table = table_of(s);
                let gap = w.sim.range(100, 2_000);
                while !stop.get() {
                    sleep_ms(w.sim.range(gap / 2, gap * 3 / 2)).await;
                    if stop.get() {
                        break;
                    }
                    if w.sim.chance(w.p.edge_restart_rate * gap as f64 / 1000.0) {
                        // A restart: the old epoch is abandoned (its head stays
                        // free: the consumer closes it), the unacked batch is
                        // resent into a new one with its received time.
                        w.tally.borrow_mut().edge_restarts += 1;
                        trace(format!("{name} RESTART"));
                        e.new_epoch();
                        if let Some((c, rows, recv)) = e.last.clone() {
                            e.commit(&b, &c, rows, recv).await;
                        }
                        continue;
                    }
                    n_content.set(n_content.get() + 1);
                    let c = format!("c{}", n_content.get());
                    let rows = w.sim.range(1, 9);
                    let recv = if w.p.scale { w.sim.wall() * 1_000_000 } else { 0 };
                    let _ = w.rows_of.borrow_mut().insert((table.clone(), c.clone()), rows);
                    e.commit(&b, &c, rows, recv).await;
                    e.last = Some((c, rows, recv));
                }
            }));
        }
    }
    // Workers.
    let mut slots: Vec<Slot> = (0..p.workers).map(|i| spawn_worker(&w, i, 0)).collect();
    // GC.
    let gc_task = {
        let w = w.clone();
        tokio::task::spawn_local(async move {
            let b = SimBucket { w: w.clone(), p: Proc::new("gc") };
            let gcc = GcConfig {
                root: ROOT.into(),
                ctl: CTL.into(),
                // lease ttl + margin + the longest a PUT can be in flight
                delay_ms: T.ttl_ms + T.margin_ms + w.p.s3_timeout_ms + w.p.put_late_ms + 2_000,
                zombie_ms: w.p.zombie_ms,
                dry_run: false,
            };
            loop {
                sleep_ms(w.sim.range(1_000, 5_000)).await;
                w.tally.borrow_mut().gc_runs += 1;
                match gc_step(&b, &gcc, w.sim.wall()).await {
                    Ok(r) => trace(format!("GC {r:?}")),
                    Err(e) => trace(format!("GC error {e}")),
                }
            }
        })
    };
    // Chaos, while the edges write.
    let t_end = now_ms() + p.run_ms;
    while now_ms() < t_end {
        sleep_ms(1_000).await;
        if let Some((i, at, d)) = p.slow_list {
            if now_ms() >= at && now_ms() < at + 1_000 {
                trace(format!("SLOW next heartbeat LIST of {} by {d} ms", slots[i].proc.name));
                slots[i].proc.slow_list_ms.set(d);
            }
        }
        if let Some((i, at, d)) = p.pause_at {
            if now_ms() >= at && now_ms() < at + 1_000 {
                trace(format!("PAUSE {} for {d} ms (scripted)", slots[i].proc.name));
                slots[i].proc.paused_until.set(now_ms() + d);
            }
        }
        for (i, slot) in slots.iter_mut().enumerate() {
            let now = now_ms();
            if sim.chance(p.pause_rate) {
                let d = sim.range(500, T.ttl_ms + 3 * T.margin_ms + 2_000);
                slot.proc.paused_until.set(now + d);
                w.tally.borrow_mut().pauses += 1;
                trace(format!("PAUSE {} for {d} ms", slot.proc.name));
            }
            if sim.chance(p.cut_rate) {
                let d = sim.range(500, T.ttl_ms + 2_000);
                slot.proc.cut_until.set(now + d);
                w.tally.borrow_mut().cuts += 1;
                trace(format!("CUT {} for {d} ms", slot.proc.name));
            }
            if sim.chance(p.kill_rate) {
                w.tally.borrow_mut().kills += 1;
                trace(format!("KILL {}", slot.proc.name));
                if let Some(h) = slot.handle.take() {
                    h.abort();
                }
                sleep_ms(sim.range(0, 3_000)).await;
                let inc = slot.inc + 1;
                *slot = spawn_worker(&w, i, inc);
            }
        }
    }
    // Quiesce: the edges stop, every process runs, until everything is in.
    stop.set(true);
    for h in edge_tasks {
        let _ = h.await;
    }
    for s in &slots {
        s.proc.paused_until.set(0);
        s.proc.cut_until.set(0);
    }
    w.healed.set(true);
    trace("QUIESCE (healed: no new faults)");
    let complete = |w: &World| w.rows_of.borrow().iter().all(|((t, c), r)| w.count(t, c) >= *r);
    let q0 = now_ms();
    while !complete(&w) && now_ms() < q0 + 300_000 {
        sleep_ms(1_000).await;
    }
    let quiesce_ms = now_ms() - q0;
    // Late statements can still land: wait them out, then stop everyone.
    sleep_ms(T.ttl_ms + T.slack_ms + T.margin_ms + 1_000).await;
    for s in slots.iter_mut() {
        if let Some(h) = s.handle.take() {
            h.abort();
        }
    }
    gc_task.abort();
    sleep_ms(T.ttl_ms + T.slack_ms + 1_000).await;
    trace("END");
    // The final state.
    let rows_of = w.rows_of.borrow().clone();
    let mut missing = Vec::new();
    let mut dup = Vec::new();
    for ((t, c), r) in &rows_of {
        let have = w.count(t, c);
        if have < *r {
            let at: Vec<String> = w
                .ever
                .borrow()
                .iter()
                .filter(|(_, cc)| *cc == c)
                .map(|((l, e, s), _)| {
                    let ck = w.ckpt_of(l);
                    format!("{l} {e}/{s} (ckpt next {:?}{})", ck.as_ref().map(|k| k.next(e)), if ck.as_ref().is_some_and(|k| k.closed(e)) { " closed" } else { "" })
                })
                .collect();
            missing.push(format!("{t}/{c} {have}/{r} at {at:?}"));
        } else if have > *r {
            dup.push(format!("{t}/{c} {have}/{r}"));
        }
    }
    let extra: Vec<String> = w.ch.borrow().rows.keys().filter(|k| !rows_of.contains_key(*k)).map(|(t, c)| format!("{t}/{c}")).collect();
    let v = w.violations.borrow().clone();
    let tally = format!("{:?}", w.tally.borrow());
    trace(format!("TALLY {tally}"));
    let summary = format!(
        "workers {} producers {} scale {} objects {} contents {} quiesce {} ms; {tally}",
        p.workers,
        p.producers,
        p.scale,
        w.ever.borrow().len(),
        rows_of.len(),
        quiesce_ms
    );
    assert!(v.is_empty(), "invariant violations: {v:#?}\n{summary}");
    assert!(dup.is_empty(), "atMostOnce: ingested more than once: {dup:?}\n{summary}");
    assert!(extra.is_empty(), "onlyCommittedIngested: {extra:?}\n{summary}");
    assert!(missing.is_empty(), "not ingested after {quiesce_ms} ms of quiet (neverSkipsCommitted / liveness): {missing:?}\n{summary}");
    summary
}

/// Wall clock at the start: some seeds start three minutes before a UTC
/// midnight (the check's partition range crosses a day).
fn wall0(seed: u64) -> u64 {
    let day = 20_000 + seed % 7;
    if seed % 2 == 0 { day * 86_400_000 - 180_000 } else { day * 86_400_000 + 3_600_000 }
}

// ---- the tests ---------------------------------------------------------------------------

/// The overrides are really in the path: in a simulation, OS randomness
/// (HashMap order, rand::random) is a function of the seed, and std's
/// clocks read simulated time.
#[test]
fn sim_self_test() {
    let order = |seed: u64| {
        sim::run(seed, 1_700_000_000_000, true, |_sim| async move {
            let m: HashMap<u32, ()> = (0..64).map(|i| (i, ())).collect();
            let order: Vec<u32> = m.keys().copied().collect();
            let r: u64 = rand::random();
            let i0 = std::time::Instant::now();
            let s0 = std::time::SystemTime::now();
            sleep_ms(90_000).await;
            let di = i0.elapsed().as_millis();
            let ds = s0.elapsed().unwrap().as_millis();
            let wall = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis();
            trace(format!("{order:?} {r} {di} {ds} {wall}"));
            format!("{order:?} {r} {di} {ds} {wall}")
        })
    };
    let before = sim::SEEDED_CALLS.load(std::sync::atomic::Ordering::Relaxed);
    let (a, b, c) = (order(1), order(1), order(2));
    assert!(a.failure.is_none(), "{:?}", a.failure);
    assert_eq!(a.summary, b.summary, "same seed, same HashMap order and randomness");
    assert_eq!(a.trace, b.trace);
    assert_ne!(a.summary, c.summary, "another seed, another order");
    assert!(a.summary.ends_with(&format!(" 90000 90000 {}", 1_700_000_000_000u64 + 90_000)), "std clocks read simulated time: {}", a.summary);
    assert!(sim::SEEDED_CALLS.load(std::sync::atomic::Ordering::Relaxed) > before, "getrandom served from the seed");
}

/// The fleet over many seeds (`DST_SEEDS`, `DST_SEED_BASE`, `DST_SEED`).
#[test]
fn fleet_seeds() {
    let seeds = sim::seeds(40);
    let t0 = std::time::Instant::now();
    let out = sim::sweep("fleet_seeds", "dst_consumer", &seeds, wall0, || fleet);
    let lines: u64 = out.iter().map(|o| o.lines).sum();
    eprintln!("DST fleet: {} seeds passed, {lines} trace lines, {:.1} s", out.len(), t0.elapsed().as_secs_f64());
}

/// Regression (found by seeds 20 and 34): a worker whose discovery round
/// is slow (here: its LIST of the heartbeats answers 15 s late; a process
/// pause does the same) used to record the lease ETags it then listed as
/// first seen when the round started. A renewal made during the slow round
/// looked unchanged for longer than it was, and the worker took a live lease
/// from its holder (`worker.rs` `heartbeat_and_leases`).
#[test]
fn a_slow_discovery_round_does_not_backdate_lease_observations() {
    let o = sim::run(7, wall0(7), true, |sim| async move {
        let mut p = Profile::calm(2, 1, 40_000);
        p.slow_list = Some((1, 20_000, 15_000));
        fleet_with(sim, p).await
    });
    if let Some(f) = &o.failure {
        let path = sim::trace_dir().join("dst-slow-discovery.trace");
        let _ = std::fs::write(&path, &o.trace);
        panic!("{f}\n  trace: {}", path.display());
    }
    assert!(o.trace.contains("its answer is 15000 ms late"), "the slow LIST happened");
}

/// Regression (found by ~10% of the first 200 seeds): a worker with a
/// backlog on several lanes HEADs slot after slot, lane after lane, with no
/// renewal in between; when that takes longer than the lease window (slow
/// S3, many lanes: in production 75 s of HEADs, e.g. 40 lanes × 256 slots
/// at 20 ms), every lease lapses before the insert, the HEAD cache goes with
/// them, and the next step starts over: nothing is ever ingested. Now the
/// step renews between lanes and a lane stops HEADing once its renewal is
/// due (`worker.rs` `step`, `scan_inner`).
#[test]
fn a_backlog_longer_than_the_lease_window_is_still_ingested() {
    let o = sim::run(11, wall0(11), true, |sim| async move {
        let mut p = Profile::calm(1, 3, 40_000);
        p.lat_ms = 60;
        p.pause_at = Some((0, 10_000, 20_000));
        fleet_with(sim, p).await
    });
    if let Some(f) = &o.failure {
        let path = sim::trace_dir().join("dst-backlog.trace");
        let _ = std::fs::write(&path, &o.trace);
        panic!("{}\n  trace: {}", &f[..f.len().min(600)], path.display());
    }
}

/// The harness finds the model's mutants (`coord::Mutation`, the bugs the
/// Quint models were checked against): each must fail some seed.
#[test]
fn fleet_catches_mutants() {
    let n = std::env::var("DST_MUTANT_SEEDS").ok().and_then(|v| v.parse().ok()).unwrap_or(150u64);
    for (name, m) in [
        ("no_time_bound", Mutation::NoTimeBound),
        ("no_verify", Mutation::NoVerify),
        ("release_in_flight", Mutation::ReleaseInFlight),
        ("error_settles", Mutation::ErrorSettles),
        ("early_compact", Mutation::EarlyCompact),
    ] {
        let t0 = std::time::Instant::now();
        let caught = (1..=n).find_map(|seed| {
            let o = sim::run(seed, wall0(seed), false, move |sim| async move {
                MUTANT.with(|c| c.set(m));
                fleet(sim).await
            });
            o.failure.map(|f| (seed, f))
        });
        match caught {
            Some((seed, f)) => {
                let first = f.lines().find(|l| l.contains("t=") || l.contains("not ingested") || l.contains("once")).unwrap_or(&f);
                eprintln!("mutant {name}: caught by seed {seed} in {:.1} s: {}", t0.elapsed().as_secs_f64(), &first.trim()[..first.trim().len().min(200)]);
            }
            None => panic!("mutant {name} survived {n} seeds"),
        }
    }
}

/// Determinism: each seed twice, traces compared byte for byte.
#[test]
fn fleet_is_deterministic() {
    let n = std::env::var("DST_META_SEEDS").ok().and_then(|v| v.parse().ok()).unwrap_or(4u64);
    let base = std::env::var("DST_SEED_BASE").ok().and_then(|v| v.parse().ok()).unwrap_or(1000u64);
    for seed in base..base + n {
        let a = sim::run(seed, wall0(seed), true, fleet);
        let b = sim::run(seed, wall0(seed), true, fleet);
        assert!(a.lines > 1000, "seed {seed}: a trace worth comparing ({} lines)", a.lines);
        if a.trace != b.trace || a.failure != b.failure {
            let dir = sim::trace_dir();
            let (pa, pb) = (dir.join(format!("dst-meta-{seed}-a.trace")), dir.join(format!("dst-meta-{seed}-b.trace")));
            let _ = std::fs::write(&pa, &a.trace);
            let _ = std::fs::write(&pb, &b.trace);
            let first = a.trace.lines().zip(b.trace.lines()).position(|(x, y)| x != y);
            panic!(
                "seed {seed}: two runs differ (first at line {first:?}): diff {} {}\n  repro: DST_SEED_BASE={seed} DST_META_SEEDS=1 cargo test --release --test dst_consumer -- fleet_is_deterministic",
                pa.display(),
                pb.display()
            );
        }
        eprintln!("DST meta seed {seed}: identical, {} lines, {} ({})", a.lines, &a.trace_hash[..16], a.failure.as_deref().unwrap_or("passed"));
    }
}
