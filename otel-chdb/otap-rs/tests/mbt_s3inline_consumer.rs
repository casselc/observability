//! Model-based test of the consumer fleet against
//! ../model/s3InlineConsumer.qnt (instance `s3InlineConsumerDesign`), with
//! quint-connect.
//!
//! The log part (writers, S3, network) is s3Inline's, driven by the shared
//! driver of `mbt_s3inline.rs` (`tests/common/s3inline.rs`). The consumer
//! part replays the model's worker, central and GC actions through the
//! consumer's own decision code (`src/consumer/`):
//!
//! | model | implementation |
//! |---|---|
//! | `wAcquire` | `Observer::may_take` must allow it; `coord::take`; the checkpoint fence (`CkptDoc::bumped`) |
//! | `wRenew`, `wLapse` | `coord::renew`; `Held::lapsed` must agree |
//! | `wCheck`, `wSend` | `Held::may_start` must allow it; `plan::verdict` picks the absent objects; the statement carries `Held::fence_wall_ms` |
//! | `cApply`, `cDrop` | central: rows land only by the fence (the server-side `WHERE now64() <= fence`) |
//! | `wAdvance` | the verify (`plan::verdict`), `plan::advance_to`, the checkpoint CAS by ETag |
//! | `wTomb`, `wSeeTomb` | a create-only tombstone in the bucket, `CkptDoc::close`, `plan::found` |
//! | `gc` | `gc::doomed` must pick exactly the model's slots (the one below the position stays) |
//! | `wCrash` | the process's state (held lease, observer) is gone |
//! | `wRelease` | `coord::may_act` must allow it (no statement of the worker can still land: `Held::settled_by`); `coord::release` |
//! | `newDay`, `senderResend`, the partitions | an object's received time is its request's custody day, stamped once per incarnation (an edge replay keeps it; a sender's resend is new custody, a later day); the check and the verify read the partitions `plan::check_range` / `plan::own_range` give (the verify's recount over the horizon included) |
//! | series actions | not driven (the edge's cache is `series.rs`; see the model) |
//! | `wListReq`, `wListAnswer` (instance `designSlow`) | a discovery LIST: the answer's lease version goes to `Observer::observe` dated when the answer came; `takeable` is then `Observer::may_take` on the version the LIST showed |
//! | `wHead` (`designSlow`) | a HEAD costs a tick; `Held::renew_due` must be false (the scan stops once the renewal is due) |
//! | `wRenewSend`, `wAdvanceSend`, `wCasLand`, `wCasLose`, `wCasAnswer` (`designSlow`) | the lease renewal (`coord::renew`) and the checkpoint write as PUT If-Match: applied only if the ETag still matches; on a 412 or no answer the doc is read back and the lane kept iff it equals ours (`write_lease` / `write_ckpt` since 034f577), or still has the ETag the write was conditional on (a lost request: kept on the old window, and written again; `NotWritten::Unchanged` since 2026-09-27, found by this driver, seed 0x29e8aebd) |
//! | `wCasTimeout`, `wRefresh` (`designSlow`, `designLate`; STPA.md CAST-50) | a write applied after the reader's check: the answer times out while the PUT is in flight, the read-back finds the ETag it was conditional on (`NotWritten::Unchanged`), and a checkpoint write marks the lane `ckpt_unsure`; the PUT may apply later, unheard. At its next step an unsure worker reads the checkpoint back and takes it iff `CkptDoc::ours_landed_late` (`refresh_own_ckpt`, 397456b) |
//! | `wLeaseRefresh`, and a renewal's `wCasTimeout` / `wCasAnswer` (the same instances; STPA.md CAST-74) | a renewal read back unchanged goes to `lease_unsure` (the model's `pend`, projected as the renewals' send times; its beat from `coord::renew_after`); a read-back of the lease (at `maintain`'s start, or after a timeout or 412) that shows one of them adopts it iff `coord::own_late_renewal` (`Held::landed`, the stored ETag), else the doubt ends or the lane goes; a lapse must find none of them stored (the code reads back first) |
//! | `wTakeSend`, a take's `wCasLand`, `wTakeAnswer`, `wTakeTimeout`, `wTakeRefresh` (`designSlow`, `designSlowQuiet`, `designLate`; STPA.md CAST-83) | the take as PUT If-Match on the version the observer judged expired (`coord::take_after`: a beat above the takes still pending on it); a 200 or a read-back equal to it holds the lane (`install_take`: the checkpoint fence, or a release if the take's window is already over); a read-back still showing the old version keeps the take in `take_unsure` (the model's `tpend`, projected as the takes' lease records); a read-back (at the step's start, or after a later take's timeout or 412) that shows one of them adopts it iff `coord::own_late_take`, any other version ends the doubt |
//!
//! After every step the implementation's state is projected onto the
//! model's variables (the log, the lease, each worker's lease view,
//! checkpoint view, phase and objects, the checkpoint, central, statements
//! in flight) and compared, together with what each worker's clock allows
//! it now: `takeable`, `mayStart` and `lapsed` per worker, which the model
//! computes from its own formulas and the implementation from `coord.rs`,
//! and `mayRelease`; and central's rows per payload and day.
//!
//! Two instances are replayed: `s3InlineConsumerDesign` (the full hostile
//! environment) and `designQuiet` (no writer faults, no series lane), where
//! most steps go to the workers; and `designDays` and `designSlow` (the
//! same with durations: slow LISTs, HEADs that cost time, ambiguous lease
//! and checkpoint writes, and writes that land after their read-back);
//! and `designLate` (ambiguous and late writes without the slow LISTs and
//! HEADs, where random runs reach a late checkpoint write, GC and the
//! worker's read-back).
//!
//! **Checkpoint compaction** (../model/s3InlineConsumerCompact.qnt, the same
//! consumer plus GC retirement, a floor and compaction; instances
//! `compactDesign` and `compactQuiet`) is replayed by the same driver:
//!
//! | model | implementation |
//! |---|---|
//! | `gcRetire` | `gc::doomed` with `retire` must pick every key the epoch has left, tombstone included |
//! | `wCompact` | `CkptDoc::compact` (known epochs, GC's retired set), the checkpoint CAS; the new floor must be the model's |
//! | the guards on `wCheck`, `wTomb`, `wSeeTomb` | `coord::above_floor` must hold for the epoch against the worker's view |
//!
//! and after every step, beside the rest, the floor, the retired set, each
//! worker's view of the floor, and **which epochs compaction may drop
//! now** (`compactable`): the model's rule (closed and retired) against what
//! `CkptDoc::compact` would drop, run on a copy of the checkpoint.
//!
//!   cargo test --release --test mbt_s3inline_consumer -- --nocapture
//!   OTAPRS_CONSUMER_MUTANT=no_time_bound|no_verify|early_compact|release_in_flight|wall_range|late_renewal_lost|late_take_lost cargo test --release --test mbt_s3inline_consumer   # must fail

#[path = "../src/consumer/mod.rs"]
#[allow(dead_code, unused_imports)]
mod consumer;
mod common;

use common::s3inline::*;
use consumer::coord::{self, CkptDoc, EpochPos, Held, LeaseDoc, Mutation, Observer, Timing};
use consumer::plan::{self, DAY_NS, Found, Verdict};
use otap_s3pq::proto;
use quint_connect::*;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet, HashMap};

// The model's design instances: 2 epochs, 3 slots, TTL 6, MARGIN 1,
// BUDGET 1, SLACK 1; days 0..1 and a copy horizon of 1 day.
const EPOCHS: i64 = 2;
const SLOTS: i64 = 3;
const LANE: &str = "mbt/lane";
const MAX_DAY: i64 = 1;
const HORIZON_DAYS: u64 = 1;

fn timing() -> Timing {
    Timing {
        ttl_ms: 6,
        margin_ms: 1,
        budget_ms: 1,
        slack_ms: 1,
        mutation: match std::env::var("OTAPRS_CONSUMER_MUTANT").as_deref() {
            Ok("no_time_bound") => Mutation::NoTimeBound,
            Ok("no_verify") => Mutation::NoVerify,
            Ok("early_compact") => Mutation::EarlyCompact,
            Ok("release_in_flight") => Mutation::ReleaseInFlight,
            Ok("wall_range") => Mutation::WallRange,
            Ok("late_renewal_lost") => Mutation::LateRenewalLost,
            Ok("late_take_lost") => Mutation::LateTakeLost,
            _ => Mutation::None,
        },
    }
}

/// A model day as a received time: noon of that day (ns).
fn day_ns(d: i64) -> u64 {
    d as u64 * DAY_NS + DAY_NS / 2
}

// ---- the model's types ------------------------------------------------------------

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct CkptM {
    version: i64,
    #[serde(rename = "leaseEpoch")]
    lease_epoch: i64,
    next: BTreeMap<i64, i64>,
    closed: BTreeMap<i64, bool>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "tag")]
pub enum WPhaseM {
    WIdle,
    WChecked,
    WSent,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct ObjM {
    epoch: i64,
    slot: i64,
    payload: i64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct WorkerM {
    holds: bool,
    inc: i64,
    #[serde(rename = "leaseEpoch")]
    lease_epoch: i64,
    sent: i64,
    seen: i64,
    view: CkptM,
    phase: WPhaseM,
    epoch: i64,
    objs: BTreeSet<ObjM>,
    pending: BTreeSet<ObjM>,
    /// The version the worker's LISTs last showed (not compared: it only
    /// feeds the model's `takeable`, which is).
    #[serde(default)]
    obs: Option<LeaseM>,
    /// A checkpoint write of its own timed out and read back unchanged
    /// (worker.rs `ckpt_unsure`). Absent in the compaction model: false.
    #[serde(default)]
    unsure: bool,
    /// The send times of its renewals given up on (worker.rs
    /// `lease_unsure`). Absent in the compaction model: none.
    #[serde(default)]
    pend: BTreeSet<i64>,
    /// The takes it gave up on (worker.rs `take_unsure`), as lease records.
    /// Absent in the compaction model: none.
    #[serde(default)]
    tpend: BTreeSet<LeaseM>,
    /// The version they were conditional on (not compared: the code keeps
    /// its ETag, and uses it only as the model does, to tell a changed lease).
    #[serde(default = "nolease")]
    tbase: LeaseM,
}

fn nolease() -> LeaseM {
    LeaseM { owner: -1, epoch: -1, sent: -1 }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(tag = "tag")]
pub enum CasKindM {
    CRenew,
    CAdvance,
    CTake,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(tag = "tag")]
pub enum CasStM {
    CPending,
    CApplied,
    CRejected,
    CLost,
    CLate,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "tag")]
pub enum CasAnsM {
    A200,
    ANone,
    A412,
}

/// A lease / checkpoint write in flight or unanswered (the model's `Write`).
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct WriteM {
    worker: i64,
    inc: i64,
    kind: CasKindM,
    st: CasStM,
    /// CAdvance: the checkpoint version it is conditional on (0 for CRenew).
    base: i64,
    /// CRenew: the renewed lease (its `sent` tells two renewals apart).
    lease: LeaseM,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct StmtM {
    worker: i64,
    inc: i64,
    objs: BTreeSet<ObjM>,
    fence: i64,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct LeaseM {
    owner: i64,
    epoch: i64,
    sent: i64,
}

#[derive(Deserialize)]
struct Raw {
    #[serde(rename = "s3InlineConsumer::L::log", alias = "s3InlineConsumerCompact::L::log")]
    l_log: BTreeMap<i64, BTreeMap<i64, Entry>>,
    #[serde(rename = "s3InlineConsumer::L::writers", alias = "s3InlineConsumerCompact::L::writers")]
    l_writers: BTreeMap<i64, Writer>,
    #[serde(rename = "s3InlineConsumer::L::lease", alias = "s3InlineConsumerCompact::L::lease")]
    l_lease: i64,
    #[serde(rename = "s3InlineConsumer::L::queue", alias = "s3InlineConsumerCompact::L::queue")]
    l_queue: BTreeSet<i64>,
    #[serde(rename = "s3InlineConsumer::L::acked", alias = "s3InlineConsumerCompact::L::acked")]
    l_acked: BTreeSet<i64>,
    #[serde(rename = "s3InlineConsumer::L::inflight", alias = "s3InlineConsumerCompact::L::inflight")]
    l_inflight: BTreeSet<Req>,
    #[serde(rename = "s3InlineConsumer::L::responses", alias = "s3InlineConsumerCompact::L::responses")]
    l_responses: BTreeSet<Resp>,
    #[serde(rename = "s3InlineConsumerDesign::s3InlineConsumer::time", alias = "designQuiet::s3InlineConsumer::time", alias = "designDays::s3InlineConsumer::time", alias = "designSlow::s3InlineConsumer::time", alias = "designSlowQuiet::s3InlineConsumer::time", alias = "designLate::s3InlineConsumer::time",
        alias = "compactDesign::s3InlineConsumerCompact::time", alias = "compactQuiet::s3InlineConsumerCompact::time")]
    time: i64,
    #[serde(rename = "s3InlineConsumerDesign::s3InlineConsumer::lease", alias = "designQuiet::s3InlineConsumer::lease", alias = "designDays::s3InlineConsumer::lease", alias = "designSlow::s3InlineConsumer::lease", alias = "designSlowQuiet::s3InlineConsumer::lease", alias = "designLate::s3InlineConsumer::lease",
        alias = "compactDesign::s3InlineConsumerCompact::lease", alias = "compactQuiet::s3InlineConsumerCompact::lease")]
    lease: LeaseM,
    #[serde(rename = "s3InlineConsumerDesign::s3InlineConsumer::workers", alias = "designQuiet::s3InlineConsumer::workers", alias = "designDays::s3InlineConsumer::workers", alias = "designSlow::s3InlineConsumer::workers", alias = "designSlowQuiet::s3InlineConsumer::workers", alias = "designLate::s3InlineConsumer::workers",
        alias = "compactDesign::s3InlineConsumerCompact::workers", alias = "compactQuiet::s3InlineConsumerCompact::workers")]
    workers: BTreeMap<i64, WorkerM>,
    #[serde(rename = "s3InlineConsumerDesign::s3InlineConsumer::ckpt", alias = "designQuiet::s3InlineConsumer::ckpt", alias = "designDays::s3InlineConsumer::ckpt", alias = "designSlow::s3InlineConsumer::ckpt", alias = "designSlowQuiet::s3InlineConsumer::ckpt", alias = "designLate::s3InlineConsumer::ckpt",
        alias = "compactDesign::s3InlineConsumerCompact::ckpt", alias = "compactQuiet::s3InlineConsumerCompact::ckpt")]
    ckpt: CkptM,
    #[serde(rename = "s3InlineConsumerDesign::s3InlineConsumer::central", alias = "designQuiet::s3InlineConsumer::central", alias = "designDays::s3InlineConsumer::central", alias = "designSlow::s3InlineConsumer::central", alias = "designSlowQuiet::s3InlineConsumer::central", alias = "designLate::s3InlineConsumer::central",
        alias = "compactDesign::s3InlineConsumerCompact::central", alias = "compactQuiet::s3InlineConsumerCompact::central")]
    central: BTreeMap<i64, i64>,
    #[serde(rename = "s3InlineConsumerDesign::s3InlineConsumer::stmts", alias = "designQuiet::s3InlineConsumer::stmts", alias = "designDays::s3InlineConsumer::stmts", alias = "designSlow::s3InlineConsumer::stmts", alias = "designSlowQuiet::s3InlineConsumer::stmts", alias = "designLate::s3InlineConsumer::stmts",
        alias = "compactDesign::s3InlineConsumerCompact::stmts", alias = "compactQuiet::s3InlineConsumerCompact::stmts")]
    stmts: BTreeSet<StmtM>,
    // s3InlineConsumer only (absent: no partitions in the instance)
    #[serde(default, rename = "s3InlineConsumerDesign::s3InlineConsumer::crows", alias = "designQuiet::s3InlineConsumer::crows",
        alias = "designDays::s3InlineConsumer::crows", alias = "designSlow::s3InlineConsumer::crows", alias = "designSlowQuiet::s3InlineConsumer::crows", alias = "designLate::s3InlineConsumer::crows")]
    crows: Option<BTreeMap<i64, BTreeMap<i64, i64>>>,
    #[serde(default, rename = "s3InlineConsumerDesign::s3InlineConsumer::day", alias = "designQuiet::s3InlineConsumer::day",
        alias = "designDays::s3InlineConsumer::day", alias = "designSlow::s3InlineConsumer::day", alias = "designSlowQuiet::s3InlineConsumer::day", alias = "designLate::s3InlineConsumer::day")]
    day: i64,
    // s3InlineConsumerCompact only (absent: no compaction in the instance)
    #[serde(default, rename = "compactDesign::s3InlineConsumerCompact::floor", alias = "compactQuiet::s3InlineConsumerCompact::floor")]
    floor: i64,
    #[serde(default, rename = "compactDesign::s3InlineConsumerCompact::retired", alias = "compactQuiet::s3InlineConsumerCompact::retired")]
    retired: BTreeSet<i64>,
    #[serde(default, rename = "compactDesign::s3InlineConsumerCompact::viewFloor", alias = "compactQuiet::s3InlineConsumerCompact::viewFloor")]
    view_floor: Option<BTreeMap<i64, i64>>,
    // The instances with durations only (present: SLOW_OBS and CAS_AMBIG are on)
    #[serde(default, rename = "designSlow::s3InlineConsumer::writes", alias = "designSlowQuiet::s3InlineConsumer::writes")]
    writes: Option<BTreeSet<WriteM>>,
    // designLate: ambiguous writes without slow observation
    #[serde(default, rename = "designLate::s3InlineConsumer::writes")]
    late_writes: Option<BTreeSet<WriteM>>,
}

/// What each worker's clock allows it now, and what compaction may drop.
#[derive(Debug, PartialEq, Eq)]
pub struct Allowed {
    takeable: BTreeMap<i64, bool>,
    may_start: BTreeMap<i64, bool>,
    lapsed: BTreeMap<i64, bool>,
    may_release: BTreeMap<i64, bool>,
    compactable: BTreeSet<i64>,
}

#[derive(Debug, PartialEq, Eq, Deserialize)]
#[serde(from = "Raw")]
pub struct Spec {
    log: BTreeMap<i64, BTreeMap<i64, Entry>>,
    writers: BTreeMap<i64, Writer>,
    l_lease: i64,
    queue: BTreeSet<i64>,
    acked: BTreeSet<i64>,
    inflight: BTreeSet<Req>,
    responses: BTreeSet<Resp>,
    time: i64,
    lease: LeaseM,
    workers: BTreeMap<i64, WorkerM>,
    ckpt: CkptM,
    central: BTreeMap<i64, i64>,
    /// Central's rows per (payload, day), nonzero only (a model without
    /// partitions: every row on day 0).
    crows: BTreeMap<(i64, i64), i64>,
    day: i64,
    stmts: BTreeSet<StmtM>,
    floor: i64,
    retired: BTreeSet<i64>,
    view_floor: BTreeMap<i64, i64>,
    /// Lease / checkpoint writes in flight or unanswered: (worker, inc,
    /// kind, state, the checkpoint version it is conditional on, the
    /// renewal's sent time).
    writes: BTreeSet<(i64, i64, CasKindM, CasStM, i64, i64)>,
    allowed: Allowed,
}

impl From<Raw> for Spec {
    fn from(r: Raw) -> Self {
        // The model's formulas (s3InlineConsumer.qnt: takeable, mayStart, lapsed).
        let t = timing();
        let (ttl, m, b, sl) = (t.ttl_ms as i64, t.margin_ms as i64, t.budget_ms as i64, t.slack_ms as i64);
        // (SLOW_OBS: only the version its LISTs showed, and a take is a CAS on it)
        let slow = r.writes.is_some();
        let allowed = Allowed {
            takeable: r
                .workers
                .iter()
                .map(|(w, x)| (*w, !x.holds && (!slow || x.obs == Some(r.lease)) && (r.lease.owner == 0 || r.time >= x.seen + ttl + m)))
                .collect(),
            may_start: r.workers.iter().map(|(w, x)| (*w, x.holds && r.time + b <= x.sent + ttl - m)).collect(),
            lapsed: r.workers.iter().map(|(w, x)| (*w, x.holds && r.time >= x.sent + ttl - m)).collect(),
            // s3InlineConsumer's mayRelease (the design: RELEASE_GUARD)
            may_release: r
                .workers
                .iter()
                .map(|(w, x)| {
                    let current = r.lease == LeaseM { owner: *w, epoch: x.lease_epoch, sent: x.sent };
                    let settled = r.stmts.iter().filter(|q| q.worker == *w && q.inc == x.inc).all(|q| r.time > q.fence + m + b + sl);
                    (*w, x.holds && current && settled)
                })
                .collect(),
            // s3InlineConsumerCompact's dropsOf (the design: closed and retired, above the floor)
            compactable: (1..=EPOCHS).filter(|e| *e > r.floor && r.ckpt.closed[e] && r.retired.contains(e)).collect(),
        };
        let view_floor = r.view_floor.unwrap_or_else(|| r.workers.keys().map(|w| (*w, 0)).collect());
        let crows = match r.crows {
            Some(c) => c.iter().flat_map(|(p, ds)| ds.iter().filter(|(_, n)| **n > 0).map(|(d, n)| ((*p, *d), *n))).collect(),
            None => r.central.iter().filter(|(_, n)| **n > 0).map(|(p, n)| ((*p, 0), *n)).collect(),
        };
        let writes = r.writes.or(r.late_writes).unwrap_or_default().iter().map(|q| (q.worker, q.inc, q.kind, q.st, q.base, q.lease.sent)).collect();
        let workers = r.workers.into_iter().map(|(w, x)| (w, WorkerM { obs: None, tbase: nolease(), ..x })).collect();
        Spec {
            log: r.l_log,
            writers: r.l_writers,
            l_lease: r.l_lease,
            queue: r.l_queue,
            acked: r.l_acked,
            inflight: r.l_inflight,
            responses: r.l_responses,
            time: r.time,
            lease: r.lease,
            workers,
            ckpt: r.ckpt,
            central: r.central,
            crows,
            day: r.day,
            stmts: r.stmts,
            floor: r.floor,
            retired: r.retired,
            view_floor,
            writes,
            allowed,
        }
    }
}

// ---- the driver ----------------------------------------------------------------------

#[derive(Clone, Debug)]
struct Worker {
    holds: bool,
    inc: i64,
    lease_epoch: i64,
    sent: i64,
    held: Option<Held>,
    obs: Observer,
    view: CkptDoc,
    view_etag: u64,
    phase: WPhaseM,
    epoch: i64,
    objs: Vec<ObjM>,
    pending: Vec<ObjM>,
    /// (designSlow) What its last LIST answer showed: None, never listed;
    /// Some(None), no lease object; Some(Some(etag)).
    listed: Option<Option<String>>,
    /// (designSlow) HEADs made per epoch above its checkpoint view.
    scanned: BTreeMap<i64, u64>,
    /// worker.rs `ckpt_unsure`: a checkpoint write of ours may still land.
    unsure: bool,
    /// worker.rs `lease_unsure`: renewals of ours that may still land.
    lease_unsure: Vec<Held>,
    /// worker.rs `take_unsure`: takes of ours that may still land, and the
    /// ETag of the version they were conditional on.
    take_unsure: Vec<Held>,
    take_base: Option<String>,
}

/// A lease or checkpoint PUT If-Match of the driver's, in flight or unanswered.
#[derive(Clone, Debug)]
struct PendW {
    worker: i64,
    inc: i64,
    kind: CasKindM,
    st: CasStM,
    /// CRenew: the renewed doc, the ETag it is conditional on, and (applied) its new ETag.
    lease: Option<LeaseDoc>,
    prev_etag: Option<String>,
    etag: Option<String>,
    /// CAdvance: the new checkpoint, the view's ETag it is conditional on, (applied) its new ETag.
    ckpt: Option<CkptDoc>,
    base_etag: u64,
    ckpt_etag: u64,
    epoch: i64,
    from: u64,
    /// When the PUT was sent (the window counts from it).
    sent: i64,
    /// CAdvance: the version of the view it is conditional on (the model's `base`).
    base_version: i64,
}

impl Worker {
    fn new() -> Self {
        Worker {
            holds: false,
            inc: 0,
            lease_epoch: 0,
            sent: 0,
            held: None,
            obs: Observer::default(),
            view: CkptDoc::new(LANE),
            view_etag: 0,
            phase: WPhaseM::WIdle,
            epoch: 0,
            objs: Vec::new(),
            pending: Vec::new(),
            listed: None,
            scanned: BTreeMap::new(),
            unsure: false,
            lease_unsure: Vec::new(),
            take_unsure: Vec::new(),
            take_base: None,
        }
    }
    /// It no longer holds the lane (its HEADs go with it), and has no take pending.
    fn drop_lane(&mut self) {
        self.idle();
        self.holds = false;
        self.held = None;
        self.scanned.clear();
        self.unsure = false;
        self.lease_unsure.clear();
        self.take_unsure.clear();
        self.take_base = None;
    }
    /// HEADs above the new checkpoint are kept (a slot there never changes).
    fn keep_heads(&mut self, e: i64, from: u64, n: u64) {
        let c = self.scanned.entry(e).or_default();
        *c = c.saturating_sub(n - from);
    }
    fn idle(&mut self) {
        self.phase = WPhaseM::WIdle;
        self.epoch = 0;
        self.objs.clear();
        self.pending.clear();
    }
}

#[derive(Default)]
pub struct ConsumerDriver {
    l: S3InlineDriver,
    time: i64,
    lease: Option<(LeaseDoc, String)>,
    ckpt: CkptDoc,
    ckpt_etag: u64,
    workers: BTreeMap<i64, Worker>,
    /// (seen-since time per worker, for the projection: when its observer
    /// first saw the current lease version)
    seen: BTreeMap<i64, i64>,
    central: BTreeMap<i64, i64>,
    /// Rows per (payload, day).
    crows: BTreeMap<(i64, i64), i64>,
    /// The calendar.
    day: i64,
    /// Each payload's custody day, and the received day each incarnation
    /// stamped a payload's objects with (the model's pDay, oDay).
    pday: BTreeMap<i64, i64>,
    oday: BTreeMap<(i64, i64), i64>,
    stmts: Vec<StmtM>,
    /// Per statement in flight (same order): the code's `Held::settled_by`.
    settle: Vec<u64>,
    etags: u64,
    /// Epochs GC removed entirely (gc.json's `retired`).
    retired: BTreeSet<String>,
    /// The instance has durations (`designSlow`): workers see lease
    /// versions only by listing, and lease / checkpoint writes are ambiguous.
    slow: bool,
    writes: Vec<PendW>,
}

fn ename(e: i64) -> String {
    epoch_name(e)
}

impl ConsumerDriver {
    fn now(&self) -> u64 {
        self.time as u64
    }

    fn etag(&mut self) -> String {
        self.etags += 1;
        format!("\"e{}\"", self.etags)
    }

    fn init(&mut self) {
        self.l.prefix = "mbt/traces".into();
        self.l.init();
        self.time = 0;
        self.lease = None;
        self.ckpt = CkptDoc::new(LANE);
        self.ckpt_etag = 0;
        self.workers = [1, 2].into_iter().map(|w| (w, Worker::new())).collect();
        self.seen = [(1, 0), (2, 0)].into_iter().collect();
        self.central = PAYLOADS.into_iter().map(|p| (p, 0)).collect();
        self.crows.clear();
        self.day = 0;
        self.pday = PAYLOADS.into_iter().map(|p| (p, 0)).collect();
        self.oday.clear();
        self.stmts.clear();
        self.settle.clear();
        self.retired.clear();
        self.writes.clear();
    }

    /// Incarnation e takes payload p: its objects carry p's custody day
    /// (the edge keeps received_at across replays), stamped once.
    fn stamp(&mut self, e: i64, p: i64) {
        let d = self.pday[&p];
        let _ = self.oday.entry((e, p)).or_insert(d);
    }

    /// The received day of an object of the model.
    fn day_of(&self, o: &ObjM) -> i64 {
        *self.oday.get(&(o.epoch, o.payload)).expect("a stamped object")
    }

    /// An object of the model as the code sees it: received on its stamped day.
    fn obj(&self, o: &ObjM) -> plan::Obj {
        plan::Obj {
            lane: LANE.into(),
            epoch: ename(o.epoch),
            seq: o.slot as u64,
            key: proto::slot_key(&self.l.prefix, &ename(o.epoch), o.slot as u64),
            size: 1,
            content: format!("p{}", o.payload),
            rows: 1,
            received_ns: day_ns(self.day_of(o)),
            seen_ms: 0,
            announce: 0,
            payloads: Default::default(),
            late: false,
        }
    }

    /// What central shows of payload p in a range (None: every partition).
    fn visible(&self, p: i64, r: Option<plan::CheckRange>) -> u64 {
        (0..=MAX_DAY)
            .filter(|d| r.is_none_or(|r| r.covers(day_ns(*d))))
            .map(|d| self.crows.get(&(p, d)).copied().unwrap_or(0) as u64)
            .sum()
    }

    fn wall_ns(&self) -> u64 {
        day_ns(self.day)
    }

    /// The pre-check's count for an object (`plan::check_range`).
    fn precheck(&self, o: &ObjM) -> u64 {
        let x = self.obj(o);
        let r = plan::check_range(&[&x], &Default::default(), Some(HORIZON_DAYS * DAY_NS), timing().mutation, self.wall_ns());
        self.visible(o.payload, r)
    }

    /// The verify's count: the statement's own range, then over the horizon if short.
    fn verify(&self, o: &ObjM) -> u64 {
        let x = self.obj(o);
        let own = plan::own_range(&[&x], &Default::default(), timing().mutation, self.wall_ns());
        let n = self.visible(o.payload, own);
        if matches!(plan::verdict(1, n), Verdict::Absent | Verdict::Partial(_)) {
            n.max(self.precheck(o))
        } else {
            n
        }
    }

    /// Whether the code lets worker w release its lane now.
    fn may_release(&self, w: i64) -> bool {
        let t = timing();
        let x = &self.workers[&w];
        let Some(h) = &x.held else { return false };
        let current = self.lease.as_ref().is_some_and(|(_, e)| *e == h.etag);
        let until = self.stmts.iter().zip(&self.settle).filter(|(q, _)| q.worker == w && q.inc == x.inc).map(|(_, u)| *u).max();
        x.holds && current && coord::may_act(until, self.now(), &t)
    }

    fn release(&mut self, w: i64) {
        let now = self.now();
        assert!(self.may_release(w), "the model releases worker {w}'s lane at {now}; the code may not (a statement may still land)");
        let h = self.workers[&w].held.clone().expect("holds");
        let doc = coord::release(&h.doc, now);
        let etag = self.etag();
        self.lease = Some((doc, etag.clone()));
        self.observe_all(&etag);
        self.w(w).drop_lane();
    }

    /// The epochs a full listing shows the worker (every epoch the writers
    /// started), and what compaction would drop now, on a copy.
    fn known(&self) -> Vec<String> {
        (1..=self.l.lease.min(EPOCHS)).map(ename).collect()
    }
    fn compactable(&self) -> BTreeSet<i64> {
        let mut c = self.ckpt.clone();
        c.compact(&self.known(), &self.retired, timing().mutation).iter().map(|e| epoch_num(e)).collect()
    }

    /// The worker only ever sees epochs above its view's floor (the code's
    /// discovery filters every listing with `coord::above_floor`).
    fn assert_above_floor(&self, w: i64, e: i64) {
        let f = &self.workers[&w].view.floor;
        assert!(coord::above_floor(&ename(e), f), "the model lets worker {w} act on epoch {e}, at or below its floor {f:?}");
    }

    fn gc_retire(&mut self, e: i64) {
        let pos = EpochPos { next: self.ckpt.next(&ename(e)), closed: self.ckpt.closed(&ename(e)), ..Default::default() };
        assert!(pos.closed, "GC retires an epoch the checkpoint hasn't closed");
        let dir = format!("{}/{}/", self.l.prefix, ename(e));
        let keys: Vec<String> = self.l.s3.keys().filter(|k| k.starts_with(&dir)).cloned().collect();
        let (del, _) = consumer::gc::doomed(&self.l.prefix, &keys, &pos, true);
        assert_eq!(del, keys, "retiring {e} must remove every key it has left");
        for k in del {
            let _ = self.l.s3.remove(&k);
        }
        let _ = self.retired.insert(ename(e));
    }

    /// (The new floor is compared with the model's after the step.)
    fn compact(&mut self, w: i64) {
        assert_eq!(self.ckpt_etag, self.workers[&w].view_etag, "the model compacts; the checkpoint CAS would fail");
        let known = self.known();
        let dropped = self.ckpt.compact(&known, &self.retired, timing().mutation);
        assert!(!dropped.is_empty(), "the model compacts; the code drops nothing");
        let le = self.workers[&w].lease_epoch as u64;
        self.ckpt = self.ckpt.bumped(le);
        self.ckpt_etag += 1;
        let (ck, ce) = (self.ckpt.clone(), self.ckpt_etag);
        let y = self.w(w);
        y.view = ck;
        y.view_etag = ce;
        // (a write of ours succeeded: an earlier one can only meet a 412)
        y.unsure = false;
    }

    /// Every worker sees a new lease version now (designSlow: only by listing).
    fn observe_all(&mut self, etag: &str) {
        if self.slow {
            return;
        }
        let now = self.now();
        for (w, x) in self.workers.iter_mut() {
            x.obs.observe(LANE, Some(etag), now);
            let _ = self.seen.insert(*w, self.time);
        }
    }

    fn w(&mut self, w: i64) -> &mut Worker {
        self.workers.get_mut(&w).expect("worker")
    }

    fn slot(&self, e: i64, s: i64) -> Option<Found> {
        let key = proto::slot_key(&self.l.prefix, &ename(e), s as u64);
        self.l.s3.get(&key).map(plan::found)
    }

    fn acquire(&mut self, w: i64) {
        let t = timing();
        let now = self.now();
        let may = self.may_take(w);
        assert!(may, "the model lets worker {w} take the lane at {now}; its observer wouldn't");
        assert!(self.own_late_take(w).is_none(), "the model takes; the code adopts its own late take first");
        let doc = coord::take(LANE, self.lease.as_ref().map(|l| &l.0), &format!("w{w}"), t.ttl_ms, now);
        let etag = self.etag();
        self.lease = Some((doc.clone(), etag.clone()));
        self.observe_all(&etag);
        self.gain(w, Held { doc, etag, sent_ms: now, sent_wall_ms: now });
    }

    /// Worker `w` learns that the store holds `h`, a take of its own (a 200,
    /// a read-back equal to it, or its adoption: worker.rs `install_take`).
    /// Inside the take's window it holds the lane from the take's send time
    /// and fences the checkpoint; past it, it gives the lane back at once (a
    /// release CAS on `h`).
    fn gain(&mut self, w: i64, h: Held) {
        let t = timing();
        let now = self.now();
        if h.lapsed(now, &t) {
            if self.lease.as_ref().map(|l| &l.1) == Some(&h.etag) {
                let doc = coord::release(&h.doc, now);
                let etag = self.etag();
                self.lease = Some((doc, etag.clone()));
                self.observe_all(&etag);
            }
            self.w(w).drop_lane();
            return;
        }
        // the checkpoint fence
        self.ckpt = self.ckpt.bumped(h.doc.epoch);
        self.ckpt_etag += 1;
        let (ck, ce) = (self.ckpt.clone(), self.ckpt_etag);
        let x = self.w(w);
        x.drop_lane();
        x.holds = true;
        x.lease_epoch = h.doc.epoch as i64;
        x.sent = h.sent_ms as i64;
        x.held = Some(h);
        x.view = ck;
        x.view_etag = ce;
    }

    /// The stored lease, if it is one of worker `w`'s takes given up on
    /// (`coord::own_late_take`), held as the code adopts it (`Held::landed`).
    fn own_late_take(&self, w: i64) -> Option<Held> {
        if timing().mutation == Mutation::LateTakeLost {
            return None;
        }
        let x = &self.workers[&w];
        if x.holds {
            return None;
        }
        let (doc, etag) = self.lease.as_ref()?;
        coord::own_late_take(doc, &format!("w{w}"), &x.take_unsure).map(|p| p.landed(etag))
    }

    /// A take read back unchanged is kept (worker.rs try_take's
    /// `LeaseMiss::Unchanged`); those kept for another version are dead.
    fn pend_take(&mut self, w: i64, p: &PendW) {
        if timing().mutation == Mutation::LateTakeLost {
            return;
        }
        let x = self.w(w);
        if x.take_unsure.is_empty() || x.take_base != p.prev_etag {
            x.take_unsure.clear();
            x.take_base = p.prev_etag.clone();
        }
        x.take_unsure.push(Held { doc: p.lease.clone().expect("doc"), etag: String::new(), sent_ms: p.sent as u64, sent_wall_ms: p.sent as u64 });
    }

    /// The take's PUT If-Match (on the version the observer judged expired,
    /// or a create) goes out; its window will count from now.
    fn take_send(&mut self, w: i64) {
        let t = timing();
        let now = self.now();
        assert!(self.may_take(w), "the model sends worker {w}'s take at {now}; its observer wouldn't");
        assert!(self.own_late_take(w).is_none(), "the model sends a take; the code adopts its own late take first");
        let base = self.lease.as_ref().map(|l| l.1.clone());
        let x = &self.workers[&w];
        let pending = if !x.take_unsure.is_empty() && x.take_base == base { &x.take_unsure[..] } else { &[][..] };
        let doc = coord::take_after(LANE, self.lease.as_ref().map(|l| &l.0), &format!("w{w}"), t.ttl_ms, now, pending);
        let p = PendW {
            worker: w,
            inc: x.inc,
            kind: CasKindM::CTake,
            st: CasStM::CPending,
            lease: Some(doc),
            prev_etag: base,
            etag: None,
            ckpt: None,
            base_etag: 0,
            ckpt_etag: 0,
            epoch: 0,
            from: 0,
            sent: self.time,
            base_version: 0,
        };
        self.writes.push(p);
    }

    /// The take's answer (worker.rs try_take / `write_lease`): a 200 or a
    /// read-back equal to it holds the lane; a read-back still showing the
    /// version it was conditional on keeps it pending; one showing an
    /// earlier take of ours adopts that; anything else ends the doubt.
    fn take_answer(&mut self, q: &WriteM, ans: CasAnsM) {
        let i = self.write_index(q);
        let p = self.writes.remove(i);
        let doc = p.lease.clone().expect("doc");
        let read_back = self.lease.as_ref().filter(|(d, _)| *d == doc).map(|(_, e)| e.clone());
        if ans == CasAnsM::A200 {
            assert_eq!(p.st, CasStM::CApplied, "a 200 for a take that did not apply");
        }
        if ans == CasAnsM::A200 || read_back.is_some() {
            let etag = if ans == CasAnsM::A200 { p.etag.clone() } else { read_back }.expect("applied");
            self.gain(p.worker, Held { doc, etag, sent_ms: p.sent as u64, sent_wall_ms: p.sent as u64 });
            return;
        }
        self.take_miss(&p);
    }

    /// No take of ours installed by this answer (worker.rs
    /// `LeaseMiss::Unchanged` / `LeaseMiss::Other`).
    fn take_miss(&mut self, p: &PendW) {
        if self.lease.as_ref().map(|l| &l.1) == p.prev_etag.as_ref() {
            self.pend_take(p.worker, p);
            return;
        }
        match self.own_late_take(p.worker) {
            Some(h) => self.gain(p.worker, h),
            None => {
                let x = self.w(p.worker);
                x.take_unsure.clear();
                x.take_base = None;
            }
        }
    }

    /// The take's answer times out while it is in flight; the read-back is
    /// resolved as any unanswered take's (`take_miss`), and the PUT may
    /// apply later, unheard.
    fn take_timeout(&mut self, q: &WriteM) {
        let i = self.write_index(q);
        assert_eq!(self.writes[i].st, CasStM::CPending, "a timeout of a take that is no longer in flight");
        let p = self.writes[i].clone();
        self.writes[i].st = CasStM::CLate;
        self.take_miss(&p);
    }

    /// `refresh_own_takes`: a worker with takes it gave up on reads the
    /// lease back at the start of its step (the model: only once it
    /// changed); one of them is adopted, anything else ends the doubt.
    fn take_refresh(&mut self, w: i64) {
        let x = &self.workers[&w];
        assert!(!x.holds && !x.take_unsure.is_empty(), "the model reads the lease back for worker {w}; the code has no take pending");
        assert_ne!(self.lease.as_ref().map(|l| l.1.clone()), x.take_base, "the model reads back a changed lease; the driver's is unchanged");
        match self.own_late_take(w) {
            Some(h) => self.gain(w, h),
            None => {
                let x = self.w(w);
                x.take_unsure.clear();
                x.take_base = None;
            }
        }
    }

    fn renew(&mut self, w: i64) {
        let t = timing();
        let now = self.now();
        let h = self.workers[&w].held.clone().expect("holds");
        assert!(!h.lapsed(now, &t), "the model renews; the worker's window is over");
        assert_eq!(self.lease.as_ref().map(|l| &l.1), Some(&h.etag), "the renewal's CAS would fail");
        let doc = coord::renew(&h.doc, now);
        let etag = self.etag();
        self.lease = Some((doc.clone(), etag.clone()));
        self.observe_all(&etag);
        let time = self.time;
        let x = self.w(w);
        x.sent = time;
        x.held = Some(Held { doc, etag, sent_ms: now, sent_wall_ms: now });
    }

    fn lapse(&mut self, w: i64) {
        let t = timing();
        let now = self.now();
        let x = self.w(w);
        assert!(x.held.as_ref().is_some_and(|h| h.lapsed(now, &t)), "the model lapses worker {w}; its clock says the window is open");
        // (`maintain` reads the lease back first while renewals may land)
        let adoptable = self.own_late(w).is_some();
        assert!(!adoptable, "the model lapses worker {w}; the code's read-back finds its own late renewal and adopts it");
        self.w(w).drop_lane();
    }

    /// The stored lease, if it is one of worker `w`'s renewals given up on
    /// (`coord::own_late_renewal`), held as the code adopts it (`Held::landed`).
    fn own_late(&self, w: i64) -> Option<Held> {
        if timing().mutation == Mutation::LateRenewalLost {
            return None;
        }
        let x = &self.workers[&w];
        let (doc, etag) = self.lease.as_ref()?;
        coord::own_late_renewal(doc, &x.held.as_ref()?.doc, &x.lease_unsure).map(|p| p.landed(etag))
    }

    /// Worker `w` adopts `h` (its own late renewal).
    fn adopt(&mut self, w: i64, h: Held) {
        let x = self.w(w);
        x.sent = h.sent_ms as i64;
        x.held = Some(h);
        x.lease_unsure.clear();
    }

    /// `refresh_own_lease`: a holder with renewals it gave up on reads the
    /// lease back (the model: only once it changed); one of them is adopted,
    /// anything else ends the doubt.
    fn lease_refresh(&mut self, w: i64) {
        let x = &self.workers[&w];
        assert!(x.holds && !x.lease_unsure.is_empty(), "the model reads the lease back for worker {w}; the code has no renewal pending");
        assert_ne!(self.lease.as_ref().map(|l| &l.1), x.held.as_ref().map(|h| &h.etag), "the model reads back a changed lease; the driver's is unchanged");
        match self.own_late(w) {
            Some(h) => self.adopt(w, h),
            None => self.w(w).lease_unsure.clear(),
        }
    }

    fn check(&mut self, w: i64, e: i64, k: i64) {
        let t = timing();
        let now = self.now();
        assert!(self.workers[&w].held.as_ref().is_some_and(|h| h.may_start(now, &t)), "the model checks; the worker may not start");
        self.assert_above_floor(w, e);
        let s0 = self.workers[&w].view.next(&ename(e)) as i64;
        let mut objs = Vec::new();
        for s in s0..s0 + k {
            let Some(Found::Data { content, .. }) = self.slot(e, s) else { panic!("wCheck on a non-data slot") };
            objs.push(ObjM { epoch: e, slot: s, payload: payload_of(&content) });
        }
        // The pre-check: absent ones only, one object per content key (the first).
        let mut pending: Vec<ObjM> = Vec::new();
        for o in &objs {
            let absent = plan::verdict(1, self.precheck(o)) == Verdict::Absent;
            if absent && !pending.iter().any(|p| p.payload == o.payload) {
                pending.push(*o);
            }
        }
        let x = self.w(w);
        x.phase = WPhaseM::WChecked;
        x.epoch = e;
        x.objs = objs;
        x.pending = pending;
    }

    fn send(&mut self, w: i64) {
        let t = timing();
        let now = self.now();
        let x = self.workers[&w].clone();
        let h = x.held.as_ref().expect("holds");
        assert!(h.may_start(now, &t), "the model sends; the worker may not start");
        if !x.pending.is_empty() {
            let fence = h.fence_wall_ms(&t).min(1000) as i64; // the model's NEVER is 1000
            self.stmts.push(StmtM { worker: w, inc: x.inc, objs: x.pending.iter().copied().collect(), fence });
            self.settle.push(h.settled_by(&t));
        }
        self.w(w).phase = WPhaseM::WSent;
    }

    fn take_stmt(&mut self, q: &StmtM) -> StmtM {
        let i = self.stmts.iter().position(|s| s == q).expect("a statement in flight");
        let _ = self.settle.remove(i);
        self.stmts.remove(i)
    }

    fn c_apply(&mut self, q: StmtM, sub: BTreeSet<ObjM>) {
        let t = timing();
        let s = self.take_stmt(&q);
        // The server-side fence (`WHERE now64() <= fence`) plus the budget and
        // the commit's slack: the latest a statement can land.
        assert!(self.time <= s.fence + (t.budget_ms + t.slack_ms) as i64, "the model lands a statement after fence + budget + slack");
        for p in sub.iter().map(|o| o.payload).collect::<BTreeSet<_>>() {
            *self.central.get_mut(&p).expect("payload") += 1;
        }
        for o in &sub {
            *self.crows.entry((o.payload, self.day_of(o))).or_default() += 1;
        }
    }

    fn advance(&mut self, w: i64) {
        let t = timing();
        let x = self.workers[&w].clone();
        assert!(!self.stmts.iter().any(|s| s.worker == w && s.inc == x.inc), "advance with its statement in flight");
        let e = ename(x.epoch);
        // The verify: an object is done if central holds its batch.
        let done: HashMap<i64, bool> = x
            .objs
            .iter()
            .map(|o| {
                let v = plan::verdict(1, self.verify(o));
                (o.slot, t.mutation == Mutation::NoVerify || matches!(v, Verdict::Present | Verdict::Over(_)))
            })
            .collect();
        let seqs: Vec<u64> = x.objs.iter().map(|o| o.slot as u64).collect();
        let n = plan::advance_to(x.view.next(&e), &seqs, |s| done[&(s as i64)]);
        if self.ckpt_etag == x.view_etag {
            self.ckpt.advance(&e, n);
            self.ckpt = self.ckpt.bumped(x.lease_epoch as u64);
            self.ckpt_etag += 1;
            let (ck, ce) = (self.ckpt.clone(), self.ckpt_etag);
            let from = x.view.next(&e);
            let y = self.w(w);
            y.idle();
            y.keep_heads(x.epoch, from, n);
            y.view = ck;
            y.view_etag = ce;
        } else {
            // The CAS failed: another worker took the lane.
            self.w(w).drop_lane();
        }
    }

    fn close(&mut self, w: i64, e: i64, s: i64) {
        assert_eq!(self.ckpt_etag, self.workers[&w].view_etag, "the model closes; the checkpoint CAS would fail");
        self.ckpt.close(&ename(e), s as u64);
        let le = self.workers[&w].lease_epoch as u64;
        self.ckpt = self.ckpt.bumped(le);
        self.ckpt_etag += 1;
        let (ck, ce) = (self.ckpt.clone(), self.ckpt_etag);
        let y = self.w(w);
        y.view = ck;
        y.view_etag = ce;
        // (a write of ours succeeded: an earlier one can only meet a 412)
        y.unsure = false;
    }

    fn tomb(&mut self, w: i64, e: i64) {
        self.assert_above_floor(w, e);
        let s = self.workers[&w].view.next(&ename(e)) as i64;
        assert!(self.slot(e, s).is_none(), "the model tombstones a slot that isn't free");
        let key = proto::slot_key(&self.l.prefix, &ename(e), s as u64);
        let mut m = HashMap::new();
        let _ = m.insert(proto::META_KIND.to_string(), proto::KIND_TOMB.to_string());
        let _ = m.insert(proto::META_EPOCH.to_string(), ename(e));
        let _ = self.l.s3.insert(key, m);
        self.close(w, e, s);
    }

    fn see_tomb(&mut self, w: i64, e: i64) {
        self.assert_above_floor(w, e);
        let s = self.workers[&w].view.next(&ename(e)) as i64;
        assert_eq!(self.slot(e, s), Some(Found::Tomb));
        self.close(w, e, s);
    }

    fn crash(&mut self, w: i64) {
        let time = self.time;
        let slow = self.slow;
        let x = self.w(w);
        x.drop_lane();
        x.inc += 1;
        // A new process: nothing observed yet; it sees the current lease
        // from now (designSlow: from its first LIST). A write it had in
        // flight may still apply (an orphan).
        x.obs = Observer::default();
        x.listed = None;
        let now = time as u64;
        if let Some((_, etag)) = self.lease.clone().filter(|_| !slow) {
            self.w(w).obs.observe(LANE, Some(&etag), now);
        }
        let _ = self.seen.insert(w, time);
    }

    /// Would the code take the lane now? Its observer must allow the
    /// current version (`Observer::may_take`); designSlow: and its LISTs
    /// must have shown that version (the take is a CAS on it; the model
    /// folds try_take's GET into the take).
    fn may_take(&self, w: i64) -> bool {
        let t = timing();
        let x = &self.workers[&w];
        let listed = !self.slow || x.listed == Some(self.lease.as_ref().map(|l| l.1.clone()));
        listed
            && match &self.lease {
                None => true,
                Some((doc, etag)) => x.obs.may_take(LANE, etag, doc, self.now(), t.margin_ms),
            }
    }

    /// A LIST answer arrives: the lease version it shows goes to the
    /// observer now, when the answer came (worker.rs since 6b1a5f5); a
    /// version the worker has already seen keeps its first-seen time.
    fn list_answer(&mut self, w: i64) {
        let now = self.now();
        let cur = self.lease.as_ref().map(|l| l.1.clone());
        let x = self.w(w);
        if x.listed.as_ref() == Some(&cur) {
            return;
        }
        x.obs.observe(LANE, cur.as_deref(), now);
        let since = match cur.as_deref() {
            Some(e) => now - x.obs.unchanged_for(LANE, e, now).expect("just observed"),
            None => now,
        };
        x.listed = Some(cur);
        let _ = self.seen.insert(w, since as i64);
    }

    /// A HEAD of the next slot above the view: it takes a tick. The design
    /// stops a scan once the renewal is due.
    fn head(&mut self, w: i64, e: i64) {
        let now = self.now();
        let x = &self.workers[&w];
        let h = x.held.as_ref().expect("holds");
        assert!(!h.renew_due(now), "the model HEADs at {now}; the code's renewal is due (Held::renew_due): its scan stops there");
        self.assert_above_floor(w, e);
        let s = x.view.next(&ename(e)) + x.scanned.get(&e).copied().unwrap_or(0);
        assert!(matches!(self.slot(e, s as i64), Some(Found::Data { .. })), "the model HEADs epoch {e} slot {s}: not data");
        *self.w(w).scanned.entry(e).or_default() += 1;
        self.time += 1;
    }

    /// The driver's write the model's `q` is: the same worker, process,
    /// kind and state, and (a worker may have a late write beside a new
    /// one) the same base version or renewal time.
    fn write_index(&self, q: &WriteM) -> usize {
        self.writes
            .iter()
            .position(|p| {
                (p.worker, p.inc, p.kind, p.st) == (q.worker, q.inc, q.kind, q.st)
                    && match p.kind {
                        CasKindM::CRenew => p.sent == q.lease.sent,
                        // (two takes of one worker in one tick, on different
                        // versions, differ by their epoch: CAST-73's lesson)
                        CasKindM::CTake => p.sent == q.lease.sent && p.lease.as_ref().is_some_and(|d| d.epoch as i64 == q.lease.epoch),
                        CasKindM::CAdvance => p.base_version == q.base,
                    }
            })
            .unwrap_or_else(|| panic!("no write in flight like the model's {q:?}"))
    }

    /// The renewal's PUT If-Match goes out; the window will count from now.
    fn renew_send(&mut self, w: i64) {
        let t = timing();
        let now = self.now();
        let x = &self.workers[&w];
        let h = x.held.clone().expect("holds");
        assert!(!h.lapsed(now, &t), "the model renews; the worker's window is over");
        assert_eq!(self.lease.as_ref().map(|l| &l.1), Some(&h.etag), "the model renews a lease that is no longer the worker's");
        let p = PendW {
            worker: w,
            inc: x.inc,
            kind: CasKindM::CRenew,
            st: CasStM::CPending,
            lease: Some(if t.mutation == Mutation::LateRenewalLost { coord::renew(&h.doc, now) } else { coord::renew_after(&h.doc, &x.lease_unsure, now) }),
            prev_etag: Some(h.etag),
            etag: None,
            ckpt: None,
            base_etag: 0,
            ckpt_etag: 0,
            epoch: 0,
            from: 0,
            sent: self.time,
            base_version: 0,
        };
        self.writes.push(p);
    }

    /// The checkpoint's PUT If-Match (on its view's ETag) past the verified done prefix.
    fn advance_send(&mut self, w: i64) {
        let t = timing();
        let x = self.workers[&w].clone();
        assert!(!self.stmts.iter().any(|s| s.worker == w && s.inc == x.inc), "advance with its statement in flight");
        let e = ename(x.epoch);
        let done: HashMap<i64, bool> = x
            .objs
            .iter()
            .map(|o| {
                let v = plan::verdict(1, self.verify(o));
                (o.slot, t.mutation == Mutation::NoVerify || matches!(v, Verdict::Present | Verdict::Over(_)))
            })
            .collect();
        let seqs: Vec<u64> = x.objs.iter().map(|o| o.slot as u64).collect();
        let n = plan::advance_to(x.view.next(&e), &seqs, |s| done[&(s as i64)]);
        let mut new = x.view.clone();
        new.advance(&e, n);
        let p = PendW {
            worker: w,
            inc: x.inc,
            kind: CasKindM::CAdvance,
            st: CasStM::CPending,
            lease: None,
            prev_etag: None,
            etag: None,
            ckpt: Some(new.bumped(x.lease_epoch as u64)),
            base_etag: x.view_etag,
            ckpt_etag: 0,
            epoch: x.epoch,
            from: x.view.next(&e),
            sent: self.time,
            base_version: x.view.version as i64,
        };
        self.writes.push(p);
    }

    /// The store takes the write iff its If-Match still holds. Nobody hears
    /// the answer to a dead process's write.
    fn cas_land(&mut self, q: &WriteM) {
        let i = self.write_index(q);
        let mut p = self.writes.remove(i);
        let ok = match p.kind {
            CasKindM::CRenew | CasKindM::CTake => self.lease.as_ref().map(|l| &l.1) == p.prev_etag.as_ref(),
            CasKindM::CAdvance => self.ckpt_etag == p.base_etag,
        };
        if ok {
            match p.kind {
                CasKindM::CRenew | CasKindM::CTake => {
                    let etag = self.etag();
                    self.lease = Some((p.lease.clone().expect("doc"), etag.clone()));
                    self.observe_all(&etag);
                    p.etag = Some(etag);
                }
                CasKindM::CAdvance => {
                    self.ckpt = p.ckpt.clone().expect("doc");
                    self.ckpt_etag += 1;
                    p.ckpt_etag = self.ckpt_etag;
                }
            }
        }
        // (a late write's worker read back before it applied: nobody hears it)
        let heard = self.workers[&p.worker].inc == p.inc && p.st != CasStM::CLate;
        p.st = if ok { CasStM::CApplied } else { CasStM::CRejected };
        if heard {
            self.writes.insert(i, p);
        }
    }

    fn cas_lose(&mut self, q: &WriteM) {
        let i = self.write_index(q);
        if self.workers[&q.worker].inc == q.inc && q.st != CasStM::CLate {
            self.writes[i].st = CasStM::CLost;
        } else {
            let _ = self.writes.remove(i);
        }
    }

    /// The answer, and the code's rule for it (worker.rs `write_lease`,
    /// `write_ckpt` since 034f577): a 200 is ours; a 412 (another's write,
    /// or object_store's retry meeting our own) or no answer is resolved by
    /// reading the doc back, and the lane is kept iff it equals ours, or
    /// the object still has the ETag the write was conditional on (our
    /// request was lost: `NotWritten::Unchanged`, since 2026-09-27; the
    /// worker keeps its old window and writes again).
    fn cas_answer(&mut self, q: &WriteM, ans: CasAnsM) {
        assert_ne!(q.kind, CasKindM::CTake, "a take's answer is wTakeAnswer");
        let i = self.write_index(q);
        let p = self.writes.remove(i);
        let read_back = match p.kind {
            CasKindM::CRenew | CasKindM::CTake => self.lease.as_ref().filter(|(d, _)| Some(d) == p.lease.as_ref()).map(|(_, e)| e.clone()),
            CasKindM::CAdvance => (Some(&self.ckpt) == p.ckpt.as_ref()).then(|| format!("{}", self.ckpt_etag)),
        };
        let keep = match ans {
            CasAnsM::A200 => {
                assert_eq!(p.st, CasStM::CApplied, "a 200 for a write that did not apply");
                true
            }
            CasAnsM::ANone | CasAnsM::A412 => read_back.is_some(),
        };
        let unchanged = match p.kind {
            CasKindM::CRenew | CasKindM::CTake => self.lease.as_ref().map(|l| &l.1) == p.prev_etag.as_ref(),
            CasKindM::CAdvance => self.ckpt_etag == p.base_etag,
        };
        if !keep && unchanged {
            return;
        }
        // (the read-back shows an earlier renewal of ours, landed late: adopted)
        if !keep && p.kind == CasKindM::CRenew {
            if let Some(h) = self.own_late(p.worker) {
                self.adopt(p.worker, h);
                return;
            }
        }
        let ce = self.ckpt_etag;
        let x = self.w(p.worker);
        if !keep {
            x.drop_lane();
            return;
        }
        match p.kind {
            CasKindM::CRenew | CasKindM::CTake => {
                let doc = p.lease.expect("doc");
                // (the 200's ETag, or the one read back)
                let etag = if ans == CasAnsM::A200 { p.etag } else { read_back }.expect("applied");
                x.sent = p.sent;
                x.held = Some(Held { doc, etag, sent_ms: p.sent as u64, sent_wall_ms: p.sent as u64 });
                // (the ones given up on were conditional on the version this replaced)
                x.lease_unsure.clear();
            }
            CasKindM::CAdvance => {
                let ck = p.ckpt.expect("doc");
                let n = ck.next(&ename(p.epoch));
                x.idle();
                x.keep_heads(p.epoch, p.from, n);
                x.view = ck;
                // (the 200's ETag, or the one read back: `write_ckpt` takes the
                // stored ETag when the doc equals ours, which since LATE_CAS
                // may be an identical earlier write of ours that landed late,
                // this one rejected)
                x.view_etag = if ans == CasAnsM::A200 { p.ckpt_etag } else { ce };
                x.unsure = false;
            }
        }
    }

    /// The answer times out while the PUT is still in flight, and the doc
    /// is read back before the store applies it (STPA.md CAST-50). The
    /// code's rule (`write_lease` / `write_ckpt`): still the ETag the write
    /// was conditional on is `NotWritten::Unchanged`; the lane is kept (a
    /// renewal on its old window; a checkpoint write ends the step and sets
    /// `ckpt_unsure`). Anything else: the lane is dropped. The PUT stays in
    /// flight, and may apply later, unheard.
    fn cas_timeout(&mut self, q: &WriteM) {
        assert_ne!(q.kind, CasKindM::CTake, "a take's timeout is wTakeTimeout");
        let i = self.write_index(q);
        assert_eq!(self.writes[i].st, CasStM::CPending, "a timeout of a write that is no longer in flight");
        let p = self.writes[i].clone();
        self.writes[i].st = CasStM::CLate;
        let unchanged = match p.kind {
            CasKindM::CRenew | CasKindM::CTake => self.lease.as_ref().map(|l| &l.1) == p.prev_etag.as_ref(),
            CasKindM::CAdvance => self.ckpt_etag == p.base_etag,
        };
        if !unchanged && p.kind == CasKindM::CRenew {
            if let Some(h) = self.own_late(p.worker) {
                self.adopt(p.worker, h);
                return;
            }
        }
        let late_lost = timing().mutation == Mutation::LateRenewalLost;
        let x = self.w(p.worker);
        match (unchanged, p.kind) {
            (false, _) => x.drop_lane(),
            (true, CasKindM::CRenew | CasKindM::CTake) => {
                if !late_lost {
                    let doc = p.lease.expect("doc");
                    x.lease_unsure.push(Held { doc, etag: String::new(), sent_ms: p.sent as u64, sent_wall_ms: p.sent as u64 });
                }
            }
            (true, CasKindM::CAdvance) => {
                x.idle();
                x.unsure = true;
            }
        }
    }

    /// `refresh_own_ckpt`: an unsure worker reads the checkpoint back at
    /// the start of its step. Its own late write (`CkptDoc::ours_landed_late`)
    /// is taken, with the HEADs below it dropped; anything else newer ends
    /// the doubt. (The model reads back only when the checkpoint changed.)
    fn refresh(&mut self, w: i64) {
        let (ck, ce) = (self.ckpt.clone(), self.ckpt_etag);
        let x = self.w(w);
        assert!(x.holds && x.unsure, "the model reads back for worker {w}; the code's lane isn't unsure");
        assert_ne!(ce, x.view_etag, "the model reads back a changed checkpoint; the driver's is unchanged");
        let le = x.held.as_ref().expect("holds").doc.epoch;
        if ck.ours_landed_late(le, &x.view) {
            for e in 1..=EPOCHS {
                let (from, n) = (x.view.next(&ename(e)), ck.next(&ename(e)));
                x.keep_heads(e, from, n.max(from));
            }
            x.idle();
            x.view = ck;
            x.view_etag = ce;
        }
        x.unsure = false;
    }

    fn gc(&mut self, e: i64, doomed_m: BTreeSet<i64>) {
        let n = self.ckpt.next(&ename(e));
        let closed = self.ckpt.closed(&ename(e));
        let dir = format!("{}/{}/", self.l.prefix, ename(e));
        let keys: Vec<String> = self.l.s3.keys().filter(|k| k.starts_with(&dir)).cloned().collect();
        let (del, _) = consumer::gc::doomed(&self.l.prefix, &keys, &EpochPos { next: n, closed, ..Default::default() }, false);
        let doomed: BTreeSet<i64> = del.iter().filter_map(|k| proto::parse_slot_key(&self.l.prefix, k)).map(|(_, s)| s as i64).collect();
        assert_eq!(doomed, doomed_m, "GC's choice of slots differs");
        for k in del {
            let _ = self.l.s3.remove(&k);
        }
    }
}

impl Driver for ConsumerDriver {
    type State = Spec;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => self.init(),
            step => self.init(), // the Rust evaluator's state-0 label (see mbt_s3inline.rs)
            lStartPush(e: i64, p: i64) => {
                self.l.start_push(e, p);
                self.stamp(e, p);
            },
            lSend(e: i64) => self.l.send(e),
            lTimeout(e: i64) => self.l.timeout(e),
            lResolve(e: i64) => self.l.resolve(e),
            lSwitchPayload(e: i64, p: i64) => {
                self.l.switch_payload(e, p);
                self.stamp(e, p);
            },
            lReceive(e: i64, q: Resp) => self.l.receive(e, q),
            lNewIncarnation(z: bool) => self.l.new_incarnation(z),
            lApply(q: Req) => self.l.apply(q),
            lLose(q: Req) => self.l.lose(q),
            tick => self.time += 1,
            newDay => self.day += 1,
            senderResend(p: i64) => { let _ = self.pday.insert(p, self.day); },
            wRelease(w: i64) => self.release(w),
            wAcquire(w: i64) => self.acquire(w),
            wRenew(w: i64) => self.renew(w),
            wLapse(w: i64) => self.lapse(w),
            wCheck(w: i64, e: i64, k: i64) => self.check(w, e, k),
            wSend(w: i64) => self.send(w),
            wAdvance(w: i64) => self.advance(w),
            wTomb(w: i64, e: i64) => self.tomb(w, e),
            wSeeTomb(w: i64, e: i64) => self.see_tomb(w, e),
            wCrash(w: i64) => self.crash(w),
            cApply(q: StmtM, sub: BTreeSet<ObjM>) => self.c_apply(q, sub),
            cDrop(q: StmtM) => { let _ = self.take_stmt(&q); },
            gc(e: i64) => {
                // The model's doomed set, recomputed from the model's rule to
                // compare with the implementation's `gc::doomed`.
                let n = self.ckpt.next(&ename(e)) as i64;
                // (GC_KEEP: the slot just below the position stays until retirement.)
                let doomed = (0..SLOTS).filter(|s| *s + 1 < n && matches!(self.slot(e, *s), Some(Found::Data { .. }))).collect();
                self.gc(e, doomed)
            },
            gcRetire(e: i64) => self.gc_retire(e),
            wListReq(w: i64) => { let _ = w; },
            wListAnswer(w: i64) => self.list_answer(w),
            wHead(w: i64, e: i64) => self.head(w, e),
            wRenewSend(w: i64) => self.renew_send(w),
            wAdvanceSend(w: i64) => self.advance_send(w),
            wCasLand(q: WriteM) => self.cas_land(&q),
            wCasLose(q: WriteM) => self.cas_lose(&q),
            wCasAnswer(q: WriteM, ans: CasAnsM) => self.cas_answer(&q, ans),
            wCasTimeout(q: WriteM) => self.cas_timeout(&q),
            wRefresh(w: i64) => self.refresh(w),
            wLeaseRefresh(w: i64) => self.lease_refresh(w),
            wTakeSend(w: i64) => self.take_send(w),
            wTakeAnswer(q: WriteM, ans: CasAnsM) => self.take_answer(&q, ans),
            wTakeTimeout(q: WriteM) => self.take_timeout(&q),
            wTakeRefresh(w: i64) => self.take_refresh(w),
            wCompact(w: i64) => self.compact(w),
            sPush => {},
            sResend => {},
            sLand => {},
            sLose => {},
            sAnnounce => {},
            sRestart => {},
        })
    }
}

fn ckpt_m(c: &CkptDoc) -> CkptM {
    CkptM {
        version: c.version as i64,
        lease_epoch: c.lease_epoch as i64,
        next: (1..=EPOCHS).map(|e| (e, c.next(&ename(e)) as i64)).collect(),
        closed: (1..=EPOCHS).map(|e| (e, c.closed(&ename(e)))).collect(),
    }
}

impl State<ConsumerDriver> for Spec {
    fn from_driver(d: &ConsumerDriver) -> Result<Self> {
        let l = d.l.project();
        let t = timing();
        let now = d.now();
        let lease = match &d.lease {
            None => LeaseM { owner: 0, epoch: 0, sent: 0 },
            Some((doc, _)) => LeaseM {
                owner: doc.owner.trim_start_matches('w').parse().unwrap_or(0),
                epoch: doc.epoch as i64,
                sent: doc.wall_ms as i64,
            },
        };
        let workers = d
            .workers
            .iter()
            .map(|(w, x)| {
                (*w, WorkerM {
                    holds: x.holds,
                    inc: x.inc,
                    lease_epoch: x.lease_epoch,
                    sent: x.sent,
                    seen: d.seen[w],
                    view: ckpt_m(&x.view),
                    phase: x.phase,
                    epoch: x.epoch,
                    objs: x.objs.iter().copied().collect(),
                    pending: x.pending.iter().copied().collect(),
                    obs: None,
                    unsure: x.unsure,
                    pend: x.lease_unsure.iter().map(|h| h.sent_ms as i64).collect(),
                    tpend: x
                        .take_unsure
                        .iter()
                        .map(|h| LeaseM { owner: *w, epoch: h.doc.epoch as i64, sent: h.doc.wall_ms as i64 })
                        .collect(),
                    tbase: nolease(),
                })
            })
            .collect();
        // What each worker's own code allows it now.
        let allowed = Allowed {
            takeable: d.workers.iter().map(|(w, x)| (*w, !x.holds && d.may_take(*w))).collect(),
            may_start: d.workers.iter().map(|(w, x)| (*w, x.holds && x.held.as_ref().is_some_and(|h| h.may_start(now, &t)))).collect(),
            lapsed: d.workers.iter().map(|(w, x)| (*w, x.holds && x.held.as_ref().is_some_and(|h| h.lapsed(now, &t)))).collect(),
            may_release: d.workers.keys().map(|w| (*w, d.may_release(*w))).collect(),
            compactable: d.compactable(),
        };
        let keep = |m: &BTreeMap<i64, BTreeMap<i64, Entry>>| -> BTreeMap<i64, BTreeMap<i64, Entry>> {
            m.iter().filter(|(e, _)| **e <= EPOCHS).map(|(e, s)| (*e, s.iter().filter(|(s, _)| **s < SLOTS).map(|(a, b)| (*a, *b)).collect())).collect()
        };
        Ok(Spec {
            log: keep(&l.log),
            writers: l.writers.into_iter().filter(|(e, _)| *e <= EPOCHS).collect(),
            l_lease: l.lease,
            queue: l.queue,
            acked: l.acked,
            inflight: l.inflight,
            responses: l.responses,
            time: d.time,
            lease,
            workers,
            ckpt: ckpt_m(&d.ckpt),
            central: d.central.clone(),
            crows: d.crows.iter().filter(|(_, n)| **n > 0).map(|(k, n)| (*k, *n)).collect(),
            day: d.day,
            stmts: d.stmts.iter().cloned().collect(),
            floor: epoch_num(&d.ckpt.floor),
            retired: d.retired.iter().map(|e| epoch_num(e)).collect(),
            view_floor: d.workers.iter().map(|(w, x)| (*w, epoch_num(&x.view.floor))).collect(),
            writes: d
                .writes
                .iter()
                .map(|p| match p.kind {
                    CasKindM::CRenew | CasKindM::CTake => (p.worker, p.inc, p.kind, p.st, 0, p.sent),
                    CasKindM::CAdvance => (p.worker, p.inc, p.kind, p.st, p.base_version, -1),
                })
                .collect(),
            allowed,
        })
    }
}

#[quint_run(spec = "../model/s3InlineConsumer.qnt", main = "s3InlineConsumerDesign", max_samples = 300, max_steps = 60)]
fn s3inline_consumer_design_simulation() -> impl Driver {
    ConsumerDriver::default()
}

/// The design with no writer faults and no series lane: the steps go to
/// the workers (checks, statements, verifies, takeovers, releases, GC).
/// (600 traces: since the take's pending set joined each worker's state,
/// CAST-83, 1000 of 80 steps exhaust quint's heap: nightly run 51.)
#[quint_run(spec = "../model/s3InlineConsumer.qnt", main = "designQuiet", max_samples = 600, max_steps = 80)]
fn s3inline_consumer_quiet_simulation() -> impl Driver {
    ConsumerDriver::default()
}

/// Writer faults without the series lane: copies of a request in a later
/// epoch, received on a later day, against the check's partition range.
/// (`designDays`: `designCopies` at this driver's TTL of 6.)
#[quint_run(spec = "../model/s3InlineConsumer.qnt", main = "designDays", max_samples = 500, max_steps = 80)]
fn s3inline_consumer_copies_simulation() -> impl Driver {
    ConsumerDriver::default()
}

/// Checkpoint compaction (../model/s3InlineConsumerCompact.qnt): GC retires
/// closed epochs, the holder compacts them out of the checkpoint, and the
/// floor moves up; everything else as in `s3InlineConsumerDesign`.
#[quint_run(spec = "../model/s3InlineConsumerCompact.qnt", main = "compactDesign", max_samples = 300, max_steps = 60)]
fn s3inline_consumer_compact_design_simulation() -> impl Driver {
    ConsumerDriver::default()
}

/// The same with no writer faults and no series lane.
#[quint_run(spec = "../model/s3InlineConsumerCompact.qnt", main = "compactQuiet", max_samples = 1000, max_steps = 80)]
fn s3inline_consumer_compact_quiet_simulation() -> impl Driver {
    ConsumerDriver::default()
}

/// Durations (2026-09-27; ../model/TEMPLATE.md "slow observation", "step
/// duration", "own-write conflict on retry"): workers see lease versions
/// only through LISTs answered later, HEADs cost time against the window,
/// and lease renewals and checkpoint writes are request, effect and an
/// answer that may be lost or a 412 for our own write. (`designSlow`:
/// `designDays`' environment with the three behaviours on.)
#[quint_run(spec = "../model/s3InlineConsumer.qnt", main = "designSlow", max_samples = 500, max_steps = 80)]
fn s3inline_consumer_slow_simulation() -> impl Driver {
    ConsumerDriver { slow: true, ..Default::default() }
}

/// The same with no writer faults: the steps go to the workers (LISTs,
/// HEADs, checks, statements, ambiguous renewals and checkpoint writes).
/// (500 traces: 1000 of 80 steps exhaust quint's 5 GB heap.)
#[quint_run(spec = "../model/s3InlineConsumer.qnt", main = "designSlowQuiet", max_samples = 500, max_steps = 80)]
fn s3inline_consumer_slow_quiet_simulation() -> impl Driver {
    ConsumerDriver { slow: true, ..Default::default() }
}

/// A write applied after the reader's check (STPA.md CAST-50): lease and
/// checkpoint writes whose answer may time out while they are in flight
/// and apply after the worker read the doc back, without the slow LISTs and
/// HEADs, so that random runs reach a late checkpoint write, GC and the
/// worker's read-back (`designLate`).
#[quint_run(spec = "../model/s3InlineConsumer.qnt", main = "designLate", max_samples = 500, max_steps = 80)]
fn s3inline_consumer_late_simulation() -> impl Driver {
    ConsumerDriver::default()
}
