//! The level-1 world of the deterministic simulation (DST.md): simulated S3
//! (`SimBucket`) and ClickHouse (`SimCentral`) over one `World`, edge
//! writers, workers and GC as tasks on a paused current-thread runtime, the
//! fault menu (per-seed rates, and one-shot faults a driver injects), and
//! the invariants checked while it runs. Shared by `tests/dst_consumer.rs`
//! (seeded sweeps) and `tests/hegel_dst.rs` (Hegel's stateful and swarm
//! tests drive the same world rule by rule).
//!
//! Mounted by each test binary with `#[path = "dst/fleet.rs"] mod fleet;`,
//! next to `mod consumer` and `mod dst`.

#![allow(dead_code, clippy::type_complexity)]

use crate::consumer::bucket::{Bucket, Cond, Counts, Item, MemBucket, Meta, Put};
use crate::consumer::coord::{CkptDoc, LeaseDoc, Mutation, Timing, join};
use crate::consumer::discovery::Backoff;
use crate::consumer::gc::{GcConfig, gc_step};
use crate::consumer::plan::{CheckRange, DAY_NS, Obj};
use crate::consumer::sql::{Central, Fence, InsertErr, LaneKind, settles_at_once};
use crate::consumer::worker::{BalanceMode, Clock, Config, Worker};
use crate::dst::sim::{Sim, now_ms, sleep_ms, sleep_until_ms, trace};
use async_trait::async_trait;
use bytes::Bytes;
use futures::FutureExt;
use otap_s3pq::proto;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::rc::Rc;
pub const ROOT: &str = "r/edges";
pub const CTL: &str = "r/ctl";
pub const SIGNALS: [&str; 3] = ["traces", "logs", "metrics_gauge"];
/// The tests' compressed timing (consumer/tests.rs), margin ≥ slack.
pub const T: Timing = Timing { ttl_ms: 9000, margin_ms: 1000, budget_ms: 3000, slack_ms: 1000, mutation: Mutation::None };

// ---- the per-seed configuration ---------------------------------------------------

/// Everything a seed decides up front. Rates are per request (faults) or
/// per second (process events); each is drawn from a small menu, so some
/// seeds are calm and some hostile.
#[derive(Clone, Debug)]
pub struct Profile {
    pub workers: usize,
    pub producers: usize,
    pub scale: bool,
    pub zombie_ms: u64,
    pub run_ms: u64,
    /// One-way network latency, max (uniform from 0).
    pub lat_ms: u64,
    /// The S3 client's give-up time (no answer).
    pub s3_timeout_ms: u64,
    /// GET / HEAD / LIST answered 503.
    pub s3_read_err: f64,
    /// A PUT that never arrives (AMBIGUITY: no answer, not applied).
    pub put_drop: f64,
    /// A PUT applied, its answer lost (AMBIGUITY: answer lost after the effect).
    pub put_lost: f64,
    /// A conditional PUT applied, then answered 412: a 5xx / 409 after it
    /// applied, and the client's retry met its own write (AMBIGUITY: S3
    /// conditional PUT).
    pub put_own_412: f64,
    /// A PUT whose answer was lost lands later still (up to this long after
    /// the client gave up; 0: off).
    pub put_late_ms: u64,
    /// Switch: LIST doesn't show objects younger than this (0: strongly
    /// consistent LIST, as AWS S3 and SeaweedFS; off by default).
    pub list_lag_ms: u64,
    pub ch_timeout_ms: u64,
    pub ch_check_err: f64,
    /// An insert refused before writing (TOO_MANY_PARTS: settled).
    pub ch_settled_err: f64,
    /// An insert that writes its first object only, then fails.
    pub ch_partial: f64,
    /// An insert that lands, its answer lost.
    pub ch_lost: f64,
    /// An insert with no answer that lands as late as it can (fence + budget + slack).
    pub ch_late: f64,
    /// An insert answered TIMEOUT_EXCEEDED that commits later anyway
    /// (AMBIGUITY: the Keeper overrun, D9).
    pub ch_timeout_commit: f64,
    /// The server's clock minus true time.
    pub server_skew_ms: i64,
    /// Per worker: its wall clock minus true time.
    pub worker_skew_ms: Vec<i64>,
    /// Process events per second of simulated time.
    pub pause_rate: f64,
    pub kill_rate: f64,
    pub cut_rate: f64,
    pub edge_restart_rate: f64,
    /// Scripted: (worker, at ms, delay ms): that worker's next LIST of the
    /// heartbeats answers `delay` late.
    pub slow_list: Option<(usize, u64, u64)>,
    /// Scripted: (worker, at ms, for ms): a pause.
    pub pause_at: Option<(usize, u64, u64)>,
}

impl Profile {
    pub fn draw(sim: &Sim) -> Profile {
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
    pub fn calm(workers: usize, producers: usize, run_ms: u64) -> Profile {
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
pub struct ChState {
    pub rows: BTreeMap<(String, String), u64>,
    pub by_day: BTreeMap<(String, String, u64), u64>,
    /// otel_resources: resource -> physical rows landed (ReplacingMergeTree
    /// keeps one per (resource, object); the count shows the retries).
    pub announced: BTreeMap<String, u64>,
    /// The announcing objects whose announcement landed.
    pub announced_objs: BTreeSet<(String, String, u64)>,
    /// llm_payloads (DECISIONS.md D36): payload -> physical rows landed, and
    /// the carrying objects whose payload part landed.
    pub payloads: BTreeMap<String, u64>,
    pub payload_objs: BTreeSet<(String, String, u64)>,
}

#[derive(Default, Debug)]
pub struct Tally {
    pub put_drop: u64,
    pub put_lost: u64,
    pub put_own_412: u64,
    pub put_late: u64,
    pub read_err: u64,
    pub cut_reqs: u64,
    pub stmts: u64,
    pub fenced: u64,
    pub settled_err: u64,
    pub partial: u64,
    pub lost: u64,
    pub late: u64,
    pub timeout_commit: u64,
    pub landed: u64,
    /// Rows landed from late parts (D34).
    pub late_rows: u64,
    pub check_err: u64,
    pub pauses: u64,
    pub kills: u64,
    pub cuts: u64,
    pub edge_restarts: u64,
    /// Late renewals of their own that workers adopted (CAST-74).
    pub lease_adopted: u64,
    /// Late takes of their own that workers adopted (CAST-83).
    pub take_adopted: u64,
    pub ckpt_checks: u64,
    pub gc_runs: u64,
    pub gc_deleted: u64,
    /// Objects committed with an announcement; announcement statements; rows
    /// that landed after their resource's announcement (checked).
    pub announcing_objs: u64,
    pub ann_stmts: u64,
    pub rows_after_ann: u64,
    /// Objects committed carrying a payload; payload statements; rows that
    /// landed after the payload they reference (checked, R-L9).
    pub carrying_objs: u64,
    pub payload_stmts: u64,
    pub rows_after_payload: u64,
}

pub struct World {
    pub sim: Rc<Sim>,
    pub p: Profile,
    pub mem: MemBucket,
    pub ch: RefCell<ChState>,
    /// Every data object ever created: (lane, epoch, seq) -> content.
    pub ever: RefCell<BTreeMap<(String, String, u64), String>>,
    /// (table, content) -> rows, for every committed content key.
    pub rows_of: RefCell<BTreeMap<(String, String), u64>>,
    pub violations: RefCell<Vec<String>>,
    pub tally: RefCell<Tally>,
    pub stmt_n: Cell<u64>,
    /// Per lane: the lease document last written, and when (simulated ms).
    pub leases: RefCell<BTreeMap<String, (LeaseDoc, u64)>>,
    /// After the chaos: no new faults (liveness is judged healed).
    pub healed: Cell<bool>,
    /// Content keys handed out by `spawn_edge` drivers.
    pub n_content: Cell<u64>,
    /// Batches (and restarts) sent to `spawn_edge` drivers and not committed yet.
    pub edge_backlog: Cell<u64>,
    /// Per (process, lease key): the body of its last lease PUT, and whether
    /// a read of that key failed since (for `on_log`'s lane-drop check).
    pub lease_puts: RefCell<BTreeMap<(String, String), (Bytes, bool)>>,
    /// Per (process, lease key): what its last GET of that key returned
    /// (None: it failed, or no object), for `on_log`'s late-renewal check.
    pub lease_reads: RefCell<BTreeMap<(String, String), Option<Bytes>>>,
    /// Takes that landed after their worker gave up on them (a late PUT
    /// that replaced another epoch's lease with one naming its sender):
    /// (process, lease key, body), for `on_log`'s late-take check.
    pub late_takes: RefCell<BTreeSet<(String, String, Bytes)>>,
    /// The resource each content key's rows use (traces, logs).
    pub res_of: RefCell<BTreeMap<String, String>>,
    /// Committed data objects that announce a resource: slot -> resource.
    pub ann_of: RefCell<BTreeMap<(String, String, u64), String>>,
    /// Committed data objects that carry a payload: slot -> payload.
    pub pay_of: RefCell<BTreeMap<(String, String, u64), String>>,
    /// Committed content keys that are late parts (`oscope-part: late`,
    /// DECISIONS.md D31): their rows must land with `late`, and never in a
    /// statement with bulk objects (D34).
    pub late_of: RefCell<BTreeSet<String>>,
}

/// Whether an edge commits a content key as the late part of a split
/// request: one in five, by the key alone, so a retry or a resend agrees.
pub fn sim_late(content: &str) -> bool {
    !NO_LATE.with(|c| c.get()) && content.bytes().map(u64::from).sum::<u64>() % 5 == 0
}

thread_local! {
    /// No late parts at all (the fleet as before D34): regressions found on
    /// that fleet replay their seed with it.
    pub static NO_LATE: Cell<bool> = const { Cell::new(false) };
}

/// A sim object's resource (S3 metadata; the real object carries it in
/// `resource_announce` and `resource_id`).
pub const META_SIM_RESOURCE: &str = "sim-resource";

/// A sim object's payload reference (the real object's rows carry it in
/// `payload_refs`, its content in `payloads`): each content key references
/// one payload, named after its resource, so objects share payloads the way
/// a resent conversation shares its messages (DECISIONS.md D36).
pub const META_SIM_PAYLOAD: &str = "sim-payload";

/// The payload a content key's rows reference.
pub fn payload_of_res(res: &str) -> String {
    format!("pl-{res}")
}

/// A fault a driver injects into one process's next matching request
/// (`tests/hegel_dst.rs`); the per-seed rates of `Profile` are independent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Forced {
    /// The next PUT never arrives (no answer, not applied).
    PutDrop,
    /// The next PUT applies; its answer is lost.
    PutLost,
    /// The next conditional PUT applies, then answers 412 (a 5xx after it
    /// applied and object_store's retry met our own write).
    PutOwn412,
    /// The next PUT is stuck on the way: no answer, and it lands this many
    /// ms after the client gave up.
    PutLate(u64),
    /// The same for the next lease PUT only (a renewal, CAST-74's shape;
    /// `PutLate` mostly meets heartbeats).
    LeasePutLate(u64),
    /// The same for the next take only (a lease PUT naming its sender that
    /// replaces another epoch's lease, or creates one: CAST-83's shape).
    LeaseTakeLate(u64),
    /// The next GET, HEAD or LIST answers 503.
    Read503,
    /// The next statement: refused before writing (TOO_MANY_PARTS).
    StmtSettledErr,
    /// The next insert writes its first object only, then fails.
    StmtPartial,
    /// The next statement lands; its answer is lost.
    StmtLost,
    /// The next statement gets no answer and lands as late as it can.
    StmtLate,
    /// The next statement answers TIMEOUT_EXCEEDED and commits later anyway.
    StmtTimeoutCommit,
    /// The next count check fails.
    CheckErr,
}

impl Forced {
    fn is_put(self) -> bool {
        matches!(self, Forced::PutDrop | Forced::PutLost | Forced::PutOwn412 | Forced::PutLate(_) | Forced::LeasePutLate(_) | Forced::LeaseTakeLate(_))
    }
    fn is_stmt(self) -> bool {
        matches!(self, Forced::StmtSettledErr | Forced::StmtPartial | Forced::StmtLost | Forced::StmtLate | Forced::StmtTimeoutCommit)
    }
}

pub fn table_of(signal: &str) -> String {
    otap_s3pq::Signal::from_name(signal).expect("signal").table().to_string()
}

/// `r/edges/{cluster}/{producer}/{signal}/{epoch}/{seq}.parquet` (format v2) -> (lane, epoch, seq).
pub fn parse_slot(key: &str) -> Option<(String, String, u64)> {
    let rest = key.strip_prefix(ROOT)?.strip_prefix('/')?;
    let parts: Vec<&str> = rest.split('/').collect();
    if parts.len() != 5 {
        return None;
    }
    let seq = parts[4].strip_suffix(".parquet")?.parse().ok()?;
    Some((parts[..3].join("/"), parts[3].to_string(), seq))
}

impl World {
    /// A fault with probability `p`, unless the run is in its healed phase.
    pub fn fault(&self, p: f64) -> bool {
        !self.healed.get() && self.sim.chance(p)
    }

    pub fn violation(&self, v: String) {
        trace(format!("VIOLATION {v}"));
        self.violations.borrow_mut().push(format!("t={} {v}", now_ms()));
    }

    pub fn count(&self, table: &str, content: &str) -> u64 {
        self.ch.borrow().rows.get(&(table.to_string(), content.to_string())).copied().unwrap_or(0)
    }

    pub fn lease_of(&self, lane: &str) -> Option<LeaseDoc> {
        let key = format!("{}.json", join(&join(CTL, "lease"), lane));
        self.mem.objs.borrow().get(&key).and_then(|o| serde_json::from_slice(&o.body).ok())
    }

    /// The server's clock, ms.
    pub fn server_now(&self) -> u64 {
        (self.sim.wall() as i64 + self.p.server_skew_ms) as u64
    }

    /// Simulated ms at which the server's clock reads `srv`.
    pub fn at_server(&self, srv: u64) -> u64 {
        (srv as i64 - self.p.server_skew_ms - self.sim.wall0_ms as i64).max(0) as u64
    }

    /// After a PUT applied: record data objects, check checkpoints.
    pub fn applied(&self, key: &str, meta: &BTreeMap<String, String>, body: &Bytes) {
        if let Some((lane, epoch, seq)) = parse_slot(key) {
            if meta.get(proto::META_KIND).map(String::as_str) == Some(proto::KIND_DATA) {
                let content = meta.get(proto::META_CONTENT).cloned().unwrap_or_default();
                if meta.get(proto::META_PART).map(String::as_str) == Some(proto::PART_LATE) {
                    let _ = self.late_of.borrow_mut().insert(content.clone());
                }
                let _ = self.ever.borrow_mut().insert((lane.clone(), epoch.clone(), seq), content);
                if meta.get(proto::META_ANNOUNCE).is_some_and(|n| n != "0") {
                    if let Some(r) = meta.get(META_SIM_RESOURCE) {
                        self.tally.borrow_mut().announcing_objs += 1;
                        let _ = self.ann_of.borrow_mut().insert((lane.clone(), epoch.clone(), seq), r.clone());
                    }
                }
                if meta.get(proto::META_PAYLOADS).is_some_and(|n| n != "0") {
                    if let Some(p) = meta.get(META_SIM_PAYLOAD) {
                        self.tally.borrow_mut().carrying_objs += 1;
                        let _ = self.pay_of.borrow_mut().insert((lane.clone(), epoch.clone(), seq), p.clone());
                    }
                }
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

    pub fn ckpt_of(&self, lane: &str) -> Option<CkptDoc> {
        let key = format!("{}.json", join(&join(CTL, "ckpt"), lane));
        self.mem.objs.borrow().get(&key).and_then(|o| serde_json::from_slice(&o.body).ok())
    }

    /// neverSkipsCommitted, at the moment a checkpoint is written.
    pub fn check_ckpt(&self, lane: &str, doc: &CkptDoc) {
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
    pub fn land(&self, n: u64, table: &str, objs: &[Obj], leases: &[(String, u64)], kind: Stmt) {
        let repair = kind == Stmt::Repair;
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
        if kind == Stmt::Announce {
            for o in objs {
                let slot = (o.lane.clone(), o.epoch.clone(), o.seq);
                match self.ann_of.borrow().get(&slot) {
                    Some(r) => {
                        *ch.announced.entry(r.clone()).or_default() += 1;
                        let _ = ch.announced_objs.insert(slot);
                        trace(format!("CH #{n} announces {r} ({} {}/{})", o.lane, o.epoch, o.seq));
                    }
                    None => self.violation(format!("statement #{n} announces from {} {}/{}, which announces nothing", o.lane, o.epoch, o.seq)),
                }
            }
            return;
        }
        if kind == Stmt::Payloads {
            for o in objs {
                let slot = (o.lane.clone(), o.epoch.clone(), o.seq);
                match self.pay_of.borrow().get(&slot) {
                    Some(p) => {
                        *ch.payloads.entry(p.clone()).or_default() += 1;
                        let _ = ch.payload_objs.insert(slot);
                        trace(format!("CH #{n} payload {p} ({} {}/{})", o.lane, o.epoch, o.seq));
                    }
                    None => self.violation(format!("statement #{n} inserts payloads from {} {}/{}, which carries none", o.lane, o.epoch, o.seq)),
                }
            }
            return;
        }
        // D34: a statement's objects are all late parts or all bulk, and each
        // lands with its object's part (the `late_part` column's value).
        if objs.iter().any(|o| objs.first().is_some_and(|f| o.late != f.late)) {
            self.violation(format!("statement #{n} mixes late parts and bulk objects: {:?}", objs.iter().map(|o| (&o.content, o.late)).collect::<Vec<_>>()));
        }
        for o in objs {
            if o.late != self.late_of.borrow().contains(&o.content) {
                self.violation(format!("latePart: rows of {} land with late = {}, their object says otherwise", o.content, o.late));
            }
            if o.late {
                self.tally.borrow_mut().late_rows += o.rows;
            }
            // sameLane: a row reaches central only after the announcement of
            // its resource (entityCatalog.qnt ingestRow).
            if let Some(r) = self.res_of.borrow().get(&o.content) {
                if ch.announced.contains_key(r) {
                    self.tally.borrow_mut().rows_after_ann += 1;
                } else {
                    self.violation(format!("sameLane: rows of {} ({table}, {} {}/{}) land before the announcement of {r}", o.content, o.lane, o.epoch, o.seq));
                }
            }
            // payloadsFirst (R-L9): a row reaches central only after the
            // payload it references (carried by its object or an earlier one
            // of its lane's epoch).
            if let Some(r) = self.res_of.borrow().get(&o.content) {
                let p = payload_of_res(r);
                if ch.payloads.contains_key(&p) {
                    self.tally.borrow_mut().rows_after_payload += 1;
                } else {
                    self.violation(format!("payloadsFirst: rows of {} ({table}, {} {}/{}) land before their payload {p}", o.content, o.lane, o.epoch, o.seq));
                }
            }
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
pub struct Proc {
    pub name: String,
    pub paused_until: Cell<u64>,
    pub cut_until: Cell<u64>,
    /// The next LIST of the heartbeats answers this much later (a slow request).
    pub slow_list_ms: Cell<u64>,
    /// Until this time, every S3 request's answer is `slow_ms` later (a
    /// throttled or overloaded store; tests/hegel_dst.rs `slow_s3`).
    pub slow_until: Cell<u64>,
    pub slow_ms: Cell<u64>,
    /// One-shot faults for this process's next matching requests, in order.
    pub forced: RefCell<Vec<Forced>>,
}

impl Proc {
    pub fn new(name: &str) -> Rc<Proc> {
        Rc::new(Proc { name: name.into(), paused_until: Cell::new(0), cut_until: Cell::new(0), slow_list_ms: Cell::new(0), slow_until: Cell::new(0), slow_ms: Cell::new(0), forced: RefCell::new(Vec::new()) })
    }
    pub async fn gate(&self) {
        while now_ms() < self.paused_until.get() {
            sleep_until_ms(self.paused_until.get()).await;
        }
    }
    pub fn cut(&self) -> bool {
        now_ms() < self.cut_until.get()
    }
    /// Takes the first queued fault `pick` accepts.
    pub fn take(&self, pick: impl Fn(Forced) -> bool) -> Option<Forced> {
        let mut f = self.forced.borrow_mut();
        let i = f.iter().position(|x| pick(*x))?;
        let x = f.remove(i);
        trace(format!("{} FORCED {x:?}", self.name));
        Some(x)
    }
}

#[derive(Clone)]
pub struct SimClock {
    pub skew_ms: i64,
    pub wall0: u64,
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

pub struct SimBucket {
    pub w: Rc<World>,
    pub p: Rc<Proc>,
}

impl SimBucket {
    /// One request: the pause gate, the network (a cut or a drop: no answer
    /// by the client's timeout), `effect` at arrival, the answer back.
    pub async fn req<T: std::fmt::Debug>(&self, what: &str, drop: bool, lose: bool, effect: impl FnOnce() -> T) -> Result<T, String> {
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
        if self.p.slow_until.get() > 0 && now_ms() < self.p.slow_until.get() {
            sleep_ms(self.p.slow_ms.get()).await;
        }
        self.p.gate().await;
        trace(format!("{} {what} -> {r:?}", self.p.name));
        Ok(r)
    }

    pub fn read_fault(&self) -> bool {
        if self.p.take(|x| x == Forced::Read503).is_some() {
            self.w.tally.borrow_mut().read_err += 1;
            return true;
        }
        let f = self.w.fault(self.w.p.s3_read_err);
        if f {
            self.w.tally.borrow_mut().read_err += 1;
        }
        f
    }

    async fn get_inner(&self, key: &str) -> Result<Option<(Bytes, String)>, String> {
        if self.read_fault() {
            let _ = self.req(&format!("GET {key}"), false, false, || "503").await;
            return Err("injected: 503 Slow Down".into());
        }
        let r = self.req(&format!("GET {key}"), false, false, || self.w.mem.get(key).now_or_never().expect("mem").expect("mem")).await?;
        Ok(r)
    }

    async fn put_inner(&self, key: &str, body: Bytes, cond: Cond<'_>, meta: &BTreeMap<String, String>) -> Put {
        let w = self.w.clone();
        let conditional = !matches!(cond, Cond::None);
        let lease = key.starts_with(&format!("{CTL}/lease/"));
        // A take: a lease naming its sender, over another epoch's lease or none.
        let take = lease && is_take(&w.mem, key, &body);
        let forced = self.p.take(|x| {
            x.is_put()
                && (x != Forced::PutOwn412 || conditional)
                && (lease || !matches!(x, Forced::LeasePutLate(_)))
                && (take || !matches!(x, Forced::LeaseTakeLate(_)))
        });
        let (drop, lose, own412, late, forced_late) = match forced {
            Some(Forced::PutDrop) => (true, false, false, false, None),
            Some(Forced::PutLost) => (false, true, false, false, None),
            Some(Forced::PutOwn412) => (false, false, true, false, None),
            Some(Forced::PutLate(ms) | Forced::LeasePutLate(ms) | Forced::LeaseTakeLate(ms)) => (true, false, false, true, Some(ms)),
            _ => {
                let drop = w.fault(w.p.put_drop);
                let lose = !drop && w.fault(w.p.put_lost);
                let own412 = !drop && !lose && conditional && w.fault(w.p.put_own_412);
                let late = drop && w.p.put_late_ms > 0 && w.sim.chance(0.5);
                (drop, lose, own412, late, None)
            }
        };
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
            let delay = w.p.s3_timeout_ms + forced_late.unwrap_or_else(|| w.sim.range(0, w.p.put_late_ms));
            let what2 = what.clone();
            let name = self.p.name.clone();
            let (w2, key2, body2) = (w.clone(), key.to_string(), body.clone());
            std::mem::drop(tokio::task::spawn_local(async move {
                sleep_ms(delay).await;
                let take = is_take(&w2.mem, &key2, &body2);
                let r = apply();
                if take && matches!(r, Put::Ok(_)) {
                    let _ = w2.late_takes.borrow_mut().insert((name.clone(), key2, body2));
                }
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
}

#[async_trait(?Send)]
impl Bucket for SimBucket {
    async fn get(&self, key: &str) -> Result<Option<(Bytes, String)>, String> {
        let r = self.get_inner(key).await;
        if r.is_err() {
            if let Some(e) = self.w.lease_puts.borrow_mut().get_mut(&(self.p.name.clone(), key.to_string())) {
                e.1 = true;
            }
        }
        if key.starts_with(&format!("{CTL}/lease/")) {
            let got = r.as_ref().ok().and_then(|o| o.as_ref().map(|(b, _)| b.clone()));
            let _ = self.w.lease_reads.borrow_mut().insert((self.p.name.clone(), key.to_string()), got);
        }
        r
    }

    async fn put(&self, key: &str, body: Bytes, cond: Cond<'_>, meta: &BTreeMap<String, String>) -> Put {
        if key.starts_with(&format!("{CTL}/lease/")) {
            let _ = self.w.lease_puts.borrow_mut().insert((self.p.name.clone(), key.to_string()), (body.clone(), false));
        }
        self.put_inner(key, body, cond, meta).await
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

pub struct SimCentral {
    pub w: Rc<World>,
    pub p: Rc<Proc>,
}

#[derive(Clone, Copy, Debug)]
pub enum Outcome {
    Normal,
    SettledErr,
    Partial,
    Lost,
    Late,
    TimeoutCommit,
}

/// What a statement is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stmt {
    Insert,
    Repair,
    Announce,
    Payloads,
}

pub fn no_answer(msg: &str) -> InsertErr {
    InsertErr { msg: msg.into(), settled: false, answered: false, range: false }
}

impl SimCentral {
    pub fn draw(&self, repair: bool) -> Outcome {
        let (w, p) = (&self.w, &self.w.p);
        let forced = self.p.take(|x| x.is_stmt() && !(repair && x == Forced::StmtPartial));
        let o = if let Some(f) = forced {
            match f {
                Forced::StmtSettledErr => Outcome::SettledErr,
                Forced::StmtPartial => Outcome::Partial,
                Forced::StmtLost => Outcome::Lost,
                Forced::StmtLate => Outcome::Late,
                _ => Outcome::TimeoutCommit,
            }
        } else if w.fault(p.ch_settled_err) {
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
    pub async fn statement(&self, k: &LaneKind, objs: Vec<Obj>, fence: Fence, kind: Stmt) -> Result<(), InsertErr> {
        let repair = kind == Stmt::Repair;
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
        let what = format!("CH #{n} {kind:?} {} {contents:?} fence {}", k.table, fence.wall_ms);
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
                // An insert's fence selects nothing; an announcement's raises (sql.rs FENCED).
                let r = if kind == Stmt::Announce || kind == Stmt::Payloads {
                    let msg = format!("clickhouse 500: Code: 395. DB::Exception: {}: fenced. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO)", crate::consumer::sql::FENCED);
                    Err(InsertErr { settled: settles_at_once(&msg), answered: true, range: false, msg })
                } else {
                    Ok(())
                };
                let _ = tx.send(r);
                return;
            }
            match outcome {
                Outcome::Normal => {
                    sleep_ms(exec).await;
                    w.land(n, &table, &objs, &leases, kind);
                    let _ = tx.send(Ok(()));
                }
                Outcome::SettledErr => {
                    let msg = "clickhouse 500: Code: 252. DB::Exception: Too many parts (TOO_MANY_PARTS)".to_string();
                    let _ = tx.send(Err(InsertErr { settled: settles_at_once(&msg), answered: true, range: false, msg }));
                }
                Outcome::Partial => {
                    sleep_ms(exec).await;
                    w.land(n, &table, &objs[..1], &leases, kind);
                    let _ = tx.send(Err(InsertErr { msg: "injected: the statement died after its first part".into(), settled: true, answered: true, range: false }));
                }
                Outcome::Lost => {
                    sleep_ms(exec).await;
                    w.land(n, &table, &objs, &leases, kind);
                    drop(tx);
                }
                Outcome::Late => {
                    let at = w.at_server(latest_srv);
                    sleep_until_ms(w.sim.range(now_ms(), at.max(now_ms()))).await;
                    w.land(n, &table, &objs, &leases, kind);
                    drop(tx);
                }
                Outcome::TimeoutCommit => {
                    sleep_ms(fence.budget_ms).await;
                    let msg = "clickhouse 500: Code: 159. DB::Exception: Timeout exceeded: elapsed 3000 ms, maximum: 3000 ms. (TIMEOUT_EXCEEDED)".to_string();
                    let _ = tx.send(Err(InsertErr { settled: settles_at_once(&msg), answered: true, range: false, msg }));
                    let at = w.at_server(latest_srv);
                    sleep_until_ms(w.sim.range(now_ms(), at.max(now_ms()))).await;
                    w.land(n, &table, &objs, &leases, kind);
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
        if self.p.cut() || self.p.take(|x| x == Forced::CheckErr).is_some() || w.fault(w.p.ch_check_err) {
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
        self.statement(k, objs.iter().map(|o| (*o).clone()).collect(), fence, Stmt::Insert).await
    }

    async fn repair(&self, k: &LaneKind, obj: &Obj, fence: Fence, _token: &str) -> Result<(), InsertErr> {
        self.statement(k, vec![obj.clone()], fence, Stmt::Repair).await
    }

    async fn announce(&self, k: &LaneKind, objs: &[&Obj], fence: Fence, _token: &str) -> Result<(), InsertErr> {
        self.w.tally.borrow_mut().ann_stmts += 1;
        self.statement(k, objs.iter().map(|o| (*o).clone()).collect(), fence, Stmt::Announce).await
    }

    async fn payloads(&self, k: &LaneKind, objs: &[&Obj], fence: Fence, _token: &str) -> Result<(), InsertErr> {
        self.w.tally.borrow_mut().payload_stmts += 1;
        self.statement(k, objs.iter().map(|o| (*o).clone()).collect(), fence, Stmt::Payloads).await
    }

    async fn dangling(&self, _k: &LaneKind, objs: &[&Obj]) -> Result<Vec<(String, u64)>, String> {
        let w = &self.w;
        let ch = w.ch.borrow();
        let res = w.res_of.borrow();
        Ok(objs
            .iter()
            .filter(|o| res.get(&o.content).is_some_and(|r| !ch.payloads.contains_key(&payload_of_res(r))))
            .map(|o| (o.key.clone(), 1))
            .collect())
    }
}

// ---- edges ------------------------------------------------------------------------------------

/// A writer lane as the exporter runs it: create-only slots, HEAD on an
/// ambiguous answer, a new epoch on a tombstone or a restart (resending
/// its last batch, which keeps its received time).
pub struct Edge {
    pub producer: String,
    pub signal: String,
    pub epoch: String,
    pub n_epochs: u32,
    pub next: u64,
    pub last: Option<(String, u64, u64)>,
    /// Resources announced in this epoch (traces, logs): the exporter's
    /// `AnnounceCache`, marked only once the announcing object committed.
    pub cache: BTreeSet<String>,
    /// The resource pool's generation (`EdgeCmd::NewResources`).
    pub res_gen: u64,
    /// Payloads sent in this epoch: the exporter's `PayloadCache`, marked
    /// only once the carrying object committed.
    pub pcache: BTreeSet<String>,
}

/// Whether a signal's objects carry resources.
pub fn has_resources(signal: &str) -> bool {
    signal == "traces" || signal == "logs"
}

impl Edge {
    pub fn prefix(&self) -> String {
        format!("{ROOT}/{}/{}", self.producer, self.signal)
    }
    pub fn new_epoch(&mut self) {
        self.n_epochs += 1;
        self.epoch = format!("E{:04}", self.n_epochs);
        self.next = 0;
        self.cache.clear(); // a new epoch announces everything again
        self.pcache.clear(); // and carries every payload again
    }
    /// A batch's resource: one of three per pool generation, per producer.
    pub fn resource(&self, w: &World) -> String {
        format!("{}-g{}-r{}", self.producer, self.res_gen, w.sim.range(0, 3))
    }
    pub async fn commit(&mut self, b: &SimBucket, content: &str, rows: u64, recv_ns: u64) {
        let res = if has_resources(&self.signal) { b.w.res_of.borrow().get(content).cloned() } else { None };
        loop {
            let key = proto::slot_key(&self.prefix(), &self.epoch, self.next);
            // Decided per attempt, for this slot's epoch.
            let announce = res.as_ref().filter(|r| !self.cache.contains(*r)).cloned();
            let pay = res.as_ref().map(|r| payload_of_res(r));
            let carry = pay.as_ref().filter(|p| !self.pcache.contains(*p)).cloned();
            let mut m = BTreeMap::new();
            if let Some(r) = &res {
                let _ = m.insert(META_SIM_RESOURCE.to_string(), r.clone());
                let _ = m.insert(proto::META_ANNOUNCE.to_string(), if announce.is_some() { "1" } else { "0" }.to_string());
                let _ = m.insert(META_SIM_PAYLOAD.to_string(), payload_of_res(r));
                let _ = m.insert(proto::META_PAYLOADS.to_string(), if carry.is_some() { "1" } else { "0" }.to_string());
                let _ = m.insert(proto::META_PAYLOAD_REFS.to_string(), "1".to_string());
            }
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
            if has_resources(&self.signal) {
                let part = if sim_late(content) { proto::PART_LATE } else { proto::PART_BULK };
                let _ = m.insert(proto::META_PART.to_string(), part.to_string());
            }
            match b.put(&key, Bytes::from_static(&[0u8; 64]), Cond::Create, &m).await {
                Put::Ok(_) => {
                    self.next += 1;
                    self.cache.extend(announce);
                    self.pcache.extend(carry);
                    return;
                }
                _ => loop {
                    match b.head(&key).await {
                        Ok(Some(h)) => {
                            match proto::Slot::from_meta(&h) {
                                proto::Slot::Tomb => self.new_epoch(),
                                proto::Slot::Data { epoch, content: c } if epoch == self.epoch && c == content => {
                                    self.next += 1;
                                    // ours: it applied, and so did what it announced
                                    if h.get(proto::META_ANNOUNCE).is_some_and(|n| n != "0") {
                                        self.cache.extend(h.get(META_SIM_RESOURCE).cloned());
                                    }
                                    if h.get(proto::META_PAYLOADS).is_some_and(|n| n != "0") {
                                        self.pcache.extend(h.get(META_SIM_PAYLOAD).cloned());
                                    }
                                    return;
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
    pub static MUTANT: Cell<Mutation> = const { Cell::new(Mutation::None) };
}

pub fn worker_cfg(name: &str, scale: bool) -> Config {
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

pub struct Slot {
    pub proc: Rc<Proc>,
    pub handle: Option<tokio::task::JoinHandle<()>>,
    pub inc: u32,
}

pub fn spawn_worker(w: &Rc<World>, i: usize, inc: u32) -> Slot {
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

pub async fn fleet(sim: Rc<Sim>) -> String {
    let p = Profile::draw(&sim);
    fleet_with(sim, p).await
}

pub async fn fleet_with(sim: Rc<Sim>, p: Profile) -> String {
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
        n_content: Cell::new(0),
        edge_backlog: Cell::new(0),
        lease_puts: RefCell::new(BTreeMap::new()),
        lease_reads: RefCell::new(BTreeMap::new()),
        late_takes: RefCell::new(BTreeSet::new()),
        res_of: RefCell::new(BTreeMap::new()),
        ann_of: RefCell::new(BTreeMap::new()),
        pay_of: RefCell::new(BTreeMap::new()),
        late_of: RefCell::new(BTreeSet::new()),
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
                let mut e = Edge { producer: format!("c{}/p{pi}", pi % 2), signal: s.to_string(), epoch: String::new(), n_epochs: 0, next: 0, last: None, cache: BTreeSet::new(), res_gen: 0, pcache: BTreeSet::new() };
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
                    if has_resources(s) {
                        let _ = w.res_of.borrow_mut().insert(c.clone(), e.resource(&w));
                    }
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
    let quiesce_ms = quiesce(&w).await;
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
    let (missing, dup, extra) = final_state(&w);
    let (ann_missing, ann_extra, _) = announcement_state(&w);
    let pay_missing = payload_state(&w);
    let rows_of = w.rows_of.borrow().clone();
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
    assert!(missing.is_empty(), "not ingested, no progress for 60 s after {quiesce_ms} ms healed (neverSkipsCommitted / liveness): {missing:?}\n{summary}");
    assert!(ann_missing.is_empty(), "announcements committed and not ingested, no progress for 60 s after {quiesce_ms} ms healed: {ann_missing:?}\n{summary}");
    assert!(ann_extra.is_empty(), "announcements ingested and never committed: {ann_extra:?}\n{summary}");
    assert!(pay_missing.is_empty(), "payload parts committed and not ingested: {pay_missing:?}\n{summary}");
    summary
}

/// Payload parts: every committed carrying object's payload landed.
pub fn payload_state(w: &World) -> Vec<String> {
    let pay_of = w.pay_of.borrow();
    let ch = w.ch.borrow();
    pay_of.iter().filter(|(slot, _)| !ch.payload_objs.contains(*slot)).map(|((l, e, s), p)| format!("{p} from {l} {e}/{s}")).collect()
}

/// Every committed content key has all its rows in central, and every
/// committed announcement and payload part has landed (`announcement_state`,
/// `payload_state`). All three, because a lane's pending object can be a
/// copy whose rows are already in (an edge restart resends its last batch
/// into the new epoch) while its announcement or payloads are not: judging
/// the fleet done on rows alone ended the wait before a dead holder's lane
/// was taken back (nightly run 7).
pub fn complete(w: &World) -> bool {
    w.rows_of.borrow().iter().all(|((t, c), r)| w.count(t, c) >= *r) && {
        let ch = w.ch.borrow();
        w.ann_of.borrow().keys().all(|slot| ch.announced_objs.contains(slot))
            && w.pay_of.borrow().keys().all(|slot| ch.payload_objs.contains(slot))
    }
}

/// Liveness: healed, the fleet must keep making progress until it is
/// done. A backlog may take a while (one worker, slow S3); a minute with
/// no new rows while something is missing is a failure. Returns how long
/// it ran (simulated ms); `complete` says whether it got there.
pub async fn quiesce(w: &World) -> u64 {
    let q0 = now_ms();
    // Progress: new rows, or a newly landed announcement.
    let total = |w: &World| {
        let ch = w.ch.borrow();
        ch.rows.values().sum::<u64>() + ch.announced_objs.len() as u64
    };
    let (mut last, mut last_at) = (total(w), q0);
    while !complete(w) && now_ms() < q0 + 3_600_000 && now_ms() < last_at + 60_000 {
        sleep_ms(1_000).await;
        if total(w) != last {
            (last, last_at) = (total(w), now_ms());
        }
    }
    now_ms() - q0
}

/// The end state against what was committed: (missing, duplicated, not
/// committed) content keys; atMostOnce + onlyCommittedIngested +
/// neverSkipsCommitted hold when all three are empty.
pub fn final_state(w: &World) -> (Vec<String>, Vec<String>, Vec<String>) {
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
    (missing, dup, extra)
}

/// Announcements, exactly once: every committed announcement landed (its
/// resource is in otel_resources), and nothing else did. Physical copies
/// (a retried statement, an object's copy in a later epoch) are the
/// ReplacingMergeTree's to fold; they are counted, not failed. Returns
/// (missing, extra, physical duplicates).
pub fn announcement_state(w: &World) -> (Vec<String>, Vec<String>, u64) {
    let ann_of = w.ann_of.borrow();
    let ch = w.ch.borrow();
    let missing: Vec<String> =
        ann_of.iter().filter(|(slot, _)| !ch.announced_objs.contains(*slot)).map(|((l, e, s), r)| format!("{r} from {l} {e}/{s}")).collect();
    let want: BTreeSet<&String> = ann_of.values().collect();
    let extra: Vec<String> = ch.announced.keys().filter(|r| !want.contains(r)).cloned().collect();
    let dups = ch.announced.values().sum::<u64>() - ch.announced_objs.len() as u64;
    (missing, extra, dups)
}

/// A command for an edge driver (`spawn_edge`).
#[derive(Clone, Copy, Debug)]
pub enum EdgeCmd {
    /// Commit this many new batches, one after another.
    Write(u32),
    /// Restart: a new epoch, the last batch resent into it (its received
    /// time kept); the old epoch's head stays free for the consumer to close.
    Restart,
    /// New resources (a rollout): the next batches use resources this lane
    /// has never announced.
    NewResources,
}

/// A writer lane driven by commands instead of its own clock: the same
/// commit rule as `fleet_with`'s edges.
pub fn spawn_edge(w: &Rc<World>, producer: usize, signal: &str) -> (tokio::sync::mpsc::UnboundedSender<EdgeCmd>, Rc<Proc>) {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<EdgeCmd>();
    let w = w.clone();
    let s = signal.to_string();
    let proc = Proc::new(&format!("edge-p{producer}-{s}"));
    let p2 = proc.clone();
    drop(tokio::task::spawn_local(async move {
        let name = p2.name.clone();
        let b = SimBucket { w: w.clone(), p: p2 };
        let mut e = Edge { producer: format!("c{}/p{producer}", producer % 2), signal: s.clone(), epoch: String::new(), n_epochs: 0, next: 0, last: None, cache: BTreeSet::new(), res_gen: 0, pcache: BTreeSet::new() };
        e.new_epoch();
        let table = table_of(&s);
        while let Some(cmd) = rx.recv().await {
            match cmd {
                EdgeCmd::Write(n) => {
                    for _ in 0..n {
                        w.n_content.set(w.n_content.get() + 1);
                        let c = format!("c{}", w.n_content.get());
                        let rows = w.sim.range(1, 9);
                        let recv = if w.p.scale { w.sim.wall() * 1_000_000 } else { 0 };
                        let _ = w.rows_of.borrow_mut().insert((table.clone(), c.clone()), rows);
                        if has_resources(&s) {
                            let _ = w.res_of.borrow_mut().insert(c.clone(), e.resource(&w));
                        }
                        e.commit(&b, &c, rows, recv).await;
                        e.last = Some((c, rows, recv));
                        w.edge_backlog.set(w.edge_backlog.get() - 1);
                    }
                }
                EdgeCmd::NewResources => {
                    e.res_gen += 1;
                    trace(format!("{name} NEW RESOURCES g{}", e.res_gen));
                    w.edge_backlog.set(w.edge_backlog.get() - 1);
                }
                EdgeCmd::Restart => {
                    w.tally.borrow_mut().edge_restarts += 1;
                    trace(format!("{name} RESTART"));
                    e.new_epoch();
                    if let Some((c, rows, recv)) = e.last.clone() {
                        e.commit(&b, &c, rows, recv).await;
                    }
                    w.edge_backlog.set(w.edge_backlog.get() - 1);
                }
            }
        }
    }));
    (tx, proc)
}

impl World {
    /// A fresh world for `p` (no tasks yet).
    pub fn new(sim: Rc<Sim>, p: Profile) -> Rc<World> {
        Rc::new(World {
            sim,
            p,
            mem: MemBucket::default(),
            ch: RefCell::new(ChState::default()),
            ever: RefCell::new(BTreeMap::new()),
            rows_of: RefCell::new(BTreeMap::new()),
            violations: RefCell::new(Vec::new()),
            tally: RefCell::new(Tally::default()),
            stmt_n: Cell::new(0),
            leases: RefCell::new(BTreeMap::new()),
            healed: Cell::new(false),
            n_content: Cell::new(0),
        edge_backlog: Cell::new(0),
            lease_puts: RefCell::new(BTreeMap::new()),
            lease_reads: RefCell::new(BTreeMap::new()),
            late_takes: RefCell::new(BTreeSet::new()),
            res_of: RefCell::new(BTreeMap::new()),
            ann_of: RefCell::new(BTreeMap::new()),
            pay_of: RefCell::new(BTreeMap::new()),
            late_of: RefCell::new(BTreeSet::new()),
        })
    }

    /// A consumer log line, as it is written (install with
    /// `otap_s3pq::set_log_sink`). Checks that a worker drops a lane for a
    /// failed renewal only when the lease is not its own: when the store
    /// holds exactly the doc its last lease PUT sent and no read of the
    /// lease failed since, the drop was spurious (it could have read the
    /// lease back: CAST #15, a 412 for our own write taken as a takeover).
    ///
    /// And (STPA.md CAST-74, `noLateLeaseStall`) that a worker never gives a
    /// lane up, by a lapse or a failed renewal, after reading back a later
    /// renewal of its own in the store (its owner, the epoch it held, a
    /// higher beat) that is still there: it could have adopted it, and the
    /// lane, its own in the store, idles until that version expires.
    pub fn on_log(&self, line: &str) {
        let Some(rest) = line.strip_prefix("consumer ") else { return };
        let Some((worker, msg)) = rest.split_once(": ") else { return };
        if msg.contains("landed late") && msg.ends_with("adopting it") {
            if msg.contains("our take") {
                self.tally.borrow_mut().take_adopted += 1;
            } else {
                self.tally.borrow_mut().lease_adopted += 1;
            }
        }
        let Some(m) = msg.strip_prefix("lease ") else { return };
        // noLateTakeStall (STPA.md CAST-83): a worker that finds the store
        // naming it holder of a lane it does not hold, where the stored lease
        // is a take of its own that landed after it gave up on it, could have
        // adopted it; the lane, its own in the store, idles until it expires.
        if let Some((lane, r)) = m.split_once(": the store names us holder (") {
            let key = format!("{}.json", join(&join(CTL, "lease"), lane));
            let stored = self.mem.objs.borrow().get(&key).map(|o| o.body.clone());
            // (only if the store still holds the very version the worker read:
            // its read may have been answered just before a late landing)
            let v = r.split_once(')').map_or(r, |x| x.0);
            let seen = stored.as_ref().and_then(|b| serde_json::from_slice::<LeaseDoc>(b).ok()).is_some_and(|d| v == format!("epoch {}, beat {}", d.epoch, d.beat));
            if let Some(cur) = stored.filter(|_| seen) {
                if self.late_takes.borrow().contains(&(worker.to_string(), key.clone(), cur)) {
                    self.violation(format!("noLateTakeStall: {worker} left {lane} idle although the store holds its own take ({v}) that landed late"));
                }
            }
            return;
        }
        let (lane, held, failed) = if let Some((lane, r)) = m.split_once(": renewal failed (") {
            (lane, r.strip_suffix("), dropping the lane"), true)
        } else if let Some((lane, r)) = m.split_once(" lapsed by our clock (") {
            (lane, r.strip_suffix("): dropping it"), false)
        } else {
            return;
        };
        // "epoch E, beat B": the version the worker held
        let held = held.and_then(|h| {
            let (e, b) = h.strip_prefix("epoch ")?.split_once(", beat ")?;
            Some((e.parse::<u64>().ok()?, b.parse::<u64>().ok()?))
        });
        let key = format!("{}.json", join(&join(CTL, "lease"), lane));
        // (it held the lane: a late take of its own stored now is one it
        // adopted or heard, then let lapse or lost; not a stall of CAST-83's)
        self.late_takes.borrow_mut().retain(|(w, k, _)| !(w == worker && *k == key));
        let stored = self.mem.objs.borrow().get(&key).map(|o| o.body.clone());
        if failed {
            let sent = self.lease_puts.borrow().get(&(worker.to_string(), key.clone())).cloned();
            if let (Some((body, read_failed)), Some(cur)) = (sent, stored.clone()) {
                if cur == body && !read_failed {
                    self.violation(format!("{worker} dropped {lane} although the lease in the store is the one it just wrote (a spurious lane loss)"));
                }
            }
        }
        let read = self.lease_reads.borrow().get(&(worker.to_string(), key.clone())).cloned().flatten();
        let doc = stored.as_ref().and_then(|b| serde_json::from_slice::<LeaseDoc>(b).ok());
        if let (Some((e, b)), Some(d), Some(cur), Some(read)) = (held, doc, stored, read) {
            if d.owner == worker && d.epoch == e && d.beat > b && read == cur {
                self.violation(format!(
                    "noLateLeaseStall: {worker} gave up {lane} (held epoch {e}, beat {b}) after reading back its own later renewal (beat {}) in the store",
                    d.beat
                ));
            }
        }
    }
}

/// Is a PUT of `body` to `key` a take: a lease naming its sender (not a
/// release) over another epoch's lease, or over none?
fn is_take(mem: &MemBucket, key: &str, body: &Bytes) -> bool {
    let Ok(d) = serde_json::from_slice::<LeaseDoc>(body) else { return false };
    let before = mem.objs.borrow().get(key).and_then(|o| serde_json::from_slice::<LeaseDoc>(&o.body).ok());
    !d.owner.is_empty() && before.is_none_or(|b| b.epoch != d.epoch)
}

/// Wall clock at the start: some seeds start three minutes before a UTC
/// midnight (the check's partition range crosses a day).
pub fn wall0(seed: u64) -> u64 {
    let day = 20_000 + seed % 7;
    if seed % 2 == 0 { day * 86_400_000 - 180_000 } else { day * 86_400_000 + 3_600_000 }
}

