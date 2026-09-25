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
//! | `gc` | `gc::doomed` must pick exactly the model's slots |
//! | `wCrash` | the process's state (held lease, observer) is gone |
//! | series actions | not driven (the edge's cache is `series.rs`; see the model) |
//!
//! After every step the implementation's state is projected onto the
//! model's variables (the log, the lease, each worker's lease view,
//! checkpoint view, phase and objects, the checkpoint, central, statements
//! in flight) and compared, together with what each worker's clock allows
//! it now: `takeable`, `mayStart` and `lapsed` per worker, which the model
//! computes from its own formulas and the implementation from `coord.rs`.
//!
//! Two instances are replayed: `s3InlineConsumerDesign` (the full hostile
//! environment) and `designQuiet` (no writer faults, no series lane), where
//! most steps go to the workers.
//!
//!   cargo test --release --test mbt_s3inline_consumer -- --nocapture
//!   OTAPRS_CONSUMER_MUTANT=no_time_bound|no_verify cargo test --release --test mbt_s3inline_consumer   # must fail

#[path = "../src/consumer/mod.rs"]
#[allow(dead_code, unused_imports)]
mod consumer;
mod common;

use common::s3inline::*;
use consumer::coord::{self, CkptDoc, EpochPos, Held, LeaseDoc, Mutation, Observer, Timing};
use consumer::plan::{self, Found, Verdict};
use otap_s3pq::proto;
use quint_connect::*;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet, HashMap};

// The model's design instances: 2 epochs, 3 slots, TTL 6, MARGIN 1, BUDGET 1.
const EPOCHS: i64 = 2;
const SLOTS: i64 = 3;
const LANE: &str = "mbt/lane";

fn timing() -> Timing {
    Timing {
        ttl_ms: 6,
        margin_ms: 1,
        budget_ms: 1,
        mutation: match std::env::var("OTAPRS_CONSUMER_MUTANT").as_deref() {
            Ok("no_time_bound") => Mutation::NoTimeBound,
            Ok("no_verify") => Mutation::NoVerify,
            _ => Mutation::None,
        },
    }
}

// ---- the model's types ------------------------------------------------------------

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
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
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct StmtM {
    worker: i64,
    inc: i64,
    objs: BTreeSet<ObjM>,
    fence: i64,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
pub struct LeaseM {
    owner: i64,
    epoch: i64,
    sent: i64,
}

#[derive(Deserialize)]
struct Raw {
    #[serde(rename = "s3InlineConsumer::L::log")]
    l_log: BTreeMap<i64, BTreeMap<i64, Entry>>,
    #[serde(rename = "s3InlineConsumer::L::writers")]
    l_writers: BTreeMap<i64, Writer>,
    #[serde(rename = "s3InlineConsumer::L::lease")]
    l_lease: i64,
    #[serde(rename = "s3InlineConsumer::L::queue")]
    l_queue: BTreeSet<i64>,
    #[serde(rename = "s3InlineConsumer::L::acked")]
    l_acked: BTreeSet<i64>,
    #[serde(rename = "s3InlineConsumer::L::inflight")]
    l_inflight: BTreeSet<Req>,
    #[serde(rename = "s3InlineConsumer::L::responses")]
    l_responses: BTreeSet<Resp>,
    #[serde(rename = "s3InlineConsumerDesign::s3InlineConsumer::time", alias = "designQuiet::s3InlineConsumer::time")]
    time: i64,
    #[serde(rename = "s3InlineConsumerDesign::s3InlineConsumer::lease", alias = "designQuiet::s3InlineConsumer::lease")]
    lease: LeaseM,
    #[serde(rename = "s3InlineConsumerDesign::s3InlineConsumer::workers", alias = "designQuiet::s3InlineConsumer::workers")]
    workers: BTreeMap<i64, WorkerM>,
    #[serde(rename = "s3InlineConsumerDesign::s3InlineConsumer::ckpt", alias = "designQuiet::s3InlineConsumer::ckpt")]
    ckpt: CkptM,
    #[serde(rename = "s3InlineConsumerDesign::s3InlineConsumer::central", alias = "designQuiet::s3InlineConsumer::central")]
    central: BTreeMap<i64, i64>,
    #[serde(rename = "s3InlineConsumerDesign::s3InlineConsumer::stmts", alias = "designQuiet::s3InlineConsumer::stmts")]
    stmts: BTreeSet<StmtM>,
}

/// What each worker's clock allows it now.
#[derive(Debug, PartialEq, Eq)]
pub struct Allowed {
    takeable: BTreeMap<i64, bool>,
    may_start: BTreeMap<i64, bool>,
    lapsed: BTreeMap<i64, bool>,
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
    stmts: BTreeSet<StmtM>,
    allowed: Allowed,
}

impl From<Raw> for Spec {
    fn from(r: Raw) -> Self {
        // The model's formulas (s3InlineConsumer.qnt: takeable, mayStart, lapsed).
        let t = timing();
        let (ttl, m, b) = (t.ttl_ms as i64, t.margin_ms as i64, t.budget_ms as i64);
        let allowed = Allowed {
            takeable: r.workers.iter().map(|(w, x)| (*w, !x.holds && (r.lease.owner == 0 || r.time >= x.seen + ttl + m))).collect(),
            may_start: r.workers.iter().map(|(w, x)| (*w, x.holds && r.time + b <= x.sent + ttl - m)).collect(),
            lapsed: r.workers.iter().map(|(w, x)| (*w, x.holds && r.time >= x.sent + ttl - m)).collect(),
        };
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
            workers: r.workers,
            ckpt: r.ckpt,
            central: r.central,
            stmts: r.stmts,
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
        }
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
    stmts: Vec<StmtM>,
    etags: u64,
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
        self.stmts.clear();
    }

    /// Every worker sees a new lease version now.
    fn observe_all(&mut self, etag: &str) {
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
        let may = match &self.lease {
            None => true,
            Some((doc, etag)) => self.workers[&w].obs.may_take(LANE, etag, doc, now, t.margin_ms),
        };
        assert!(may, "the model lets worker {w} take the lane at {now}; its observer wouldn't");
        let doc = coord::take(LANE, self.lease.as_ref().map(|l| &l.0), &format!("w{w}"), t.ttl_ms, now);
        let etag = self.etag();
        self.lease = Some((doc.clone(), etag.clone()));
        self.observe_all(&etag);
        // the checkpoint fence
        self.ckpt = self.ckpt.bumped(doc.epoch);
        self.ckpt_etag += 1;
        let (ck, ce, time) = (self.ckpt.clone(), self.ckpt_etag, self.time);
        let x = self.w(w);
        x.idle();
        x.holds = true;
        x.lease_epoch = doc.epoch as i64;
        x.sent = time;
        x.held = Some(Held { doc, etag, sent_ms: now, sent_wall_ms: now });
        x.view = ck;
        x.view_etag = ce;
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
        x.holds = false;
        x.held = None;
        x.idle();
    }

    fn check(&mut self, w: i64, e: i64, k: i64) {
        let t = timing();
        let now = self.now();
        assert!(self.workers[&w].held.as_ref().is_some_and(|h| h.may_start(now, &t)), "the model checks; the worker may not start");
        let s0 = self.workers[&w].view.next(&ename(e)) as i64;
        let mut objs = Vec::new();
        for s in s0..s0 + k {
            let Some(Found::Data { content, .. }) = self.slot(e, s) else { panic!("wCheck on a non-data slot") };
            objs.push(ObjM { epoch: e, slot: s, payload: payload_of(&content) });
        }
        // The pre-check: absent ones only, one object per content key (the first).
        let mut pending: Vec<ObjM> = Vec::new();
        for o in &objs {
            let absent = plan::verdict(1, self.central[&o.payload] as u64) == Verdict::Absent;
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
        }
        self.w(w).phase = WPhaseM::WSent;
    }

    fn take_stmt(&mut self, q: &StmtM) -> StmtM {
        let i = self.stmts.iter().position(|s| s == q).expect("a statement in flight");
        self.stmts.remove(i)
    }

    fn c_apply(&mut self, q: StmtM, sub: BTreeSet<ObjM>) {
        let s = self.take_stmt(&q);
        // The server-side fence: `WHERE now64() <= fence`.
        assert!(self.time <= s.fence, "the model lands a statement after its fence");
        for p in sub.iter().map(|o| o.payload).collect::<BTreeSet<_>>() {
            *self.central.get_mut(&p).expect("payload") += 1;
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
                let v = plan::verdict(1, self.central[&o.payload] as u64);
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
            let y = self.w(w);
            y.idle();
            y.view = ck;
            y.view_etag = ce;
        } else {
            // The CAS failed: another worker took the lane.
            let y = self.w(w);
            y.idle();
            y.holds = false;
            y.held = None;
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
    }

    fn tomb(&mut self, w: i64, e: i64) {
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
        let s = self.workers[&w].view.next(&ename(e)) as i64;
        assert_eq!(self.slot(e, s), Some(Found::Tomb));
        self.close(w, e, s);
    }

    fn crash(&mut self, w: i64) {
        let time = self.time;
        let x = self.w(w);
        x.idle();
        x.holds = false;
        x.held = None;
        x.inc += 1;
        // A new process: nothing observed yet; it sees the current lease from now.
        x.obs = Observer::default();
        let now = time as u64;
        if let Some((_, etag)) = self.lease.clone() {
            self.w(w).obs.observe(LANE, Some(&etag), now);
        }
        let _ = self.seen.insert(w, time);
    }

    fn gc(&mut self, e: i64, doomed_m: BTreeSet<i64>) {
        let n = self.ckpt.next(&ename(e));
        let closed = self.ckpt.closed(&ename(e));
        let dir = format!("{}/{}/", self.l.prefix, ename(e));
        let keys: Vec<String> = self.l.s3.keys().filter(|k| k.starts_with(&dir)).cloned().collect();
        let (del, _) = consumer::gc::doomed(&self.l.prefix, &keys, &EpochPos { next: n, closed }, false);
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
            lStartPush(e: i64, p: i64) => self.l.start_push(e, p),
            lSend(e: i64) => self.l.send(e),
            lTimeout(e: i64) => self.l.timeout(e),
            lResolve(e: i64) => self.l.resolve(e),
            lSwitchPayload(e: i64, p: i64) => self.l.switch_payload(e, p),
            lReceive(e: i64, q: Resp) => self.l.receive(e, q),
            lNewIncarnation(z: bool) => self.l.new_incarnation(z),
            lApply(q: Req) => self.l.apply(q),
            lLose(q: Req) => self.l.lose(q),
            tick => self.time += 1,
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
                let doomed = (0..SLOTS).filter(|s| *s < n && matches!(self.slot(e, *s), Some(Found::Data { .. }))).collect();
                self.gc(e, doomed)
            },
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
                })
            })
            .collect();
        // What each worker's own code allows it now.
        let allowed = Allowed {
            takeable: d
                .workers
                .iter()
                .map(|(w, x)| {
                    let may = match &d.lease {
                        None => true,
                        Some((doc, etag)) => x.obs.may_take(LANE, etag, doc, now, t.margin_ms),
                    };
                    (*w, !x.holds && may)
                })
                .collect(),
            may_start: d.workers.iter().map(|(w, x)| (*w, x.holds && x.held.as_ref().is_some_and(|h| h.may_start(now, &t)))).collect(),
            lapsed: d.workers.iter().map(|(w, x)| (*w, x.holds && x.held.as_ref().is_some_and(|h| h.lapsed(now, &t)))).collect(),
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
            stmts: d.stmts.iter().cloned().collect(),
            allowed,
        })
    }
}

#[quint_run(spec = "../model/s3InlineConsumer.qnt", main = "s3InlineConsumerDesign", max_samples = 300, max_steps = 60)]
fn s3inline_consumer_design_simulation() -> impl Driver {
    ConsumerDriver::default()
}

/// The design with no writer faults and no series lane: the steps go to
/// the workers (checks, statements, verifies, takeovers, GC).
#[quint_run(spec = "../model/s3InlineConsumer.qnt", main = "designQuiet", max_samples = 1000, max_steps = 80)]
fn s3inline_consumer_quiet_simulation() -> impl Driver {
    ConsumerDriver::default()
}
