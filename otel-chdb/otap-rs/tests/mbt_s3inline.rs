//! Model-based test of the commit protocol against ../model/s3Inline.qnt
//! (instance `s3InlineDesign`: ambiguous and late S3 writes, zombie writers,
//! consumer crashes), with quint-connect.
//!
//! quint-connect runs `quint run --mbt` on the model, and replays every
//! trace step by step: each model action becomes a call into this crate's
//! `proto::Lane` (the writer) or `proto::Consumer`, with the outcome the
//! model chose injected (200, 412, no answer, a late or lost request, a
//! crash, a zombie). S3 is an in-memory bucket with create-only puts, holding
//! the same user metadata `runner::append` writes and read back through
//! `Slot::from_meta`; central is a count per content key. After every step
//! the implementation's state is projected onto the model's variables (what
//! each slot holds, each writer's phase, slot and batch, the queue and its
//! acks, the consumer's checkpoints, closed epochs and phase, what central
//! holds, the requests in flight) and compared.
//!
//!   cargo test --release --test mbt_s3inline -- --nocapture          # quint on PATH
//!   OTAPRS_MUTANT=retry_new_key|no_halt|no_check_central cargo test ... # must fail
//!   QUINT_SEED=0x5eed cargo test ...                                   # reproduce

use otap_s3pq::proto::{self, CPhase, Consumer, Lane, Mutation, Phase, PutOutcome, Slot, Step as LStep};
use quint_connect::*;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet, HashMap};

const MAX_EPOCH: i64 = 3;
const MAX_SLOT: i64 = 4;
const PAYLOADS: [i64; 2] = [1, 2];
const PREFIX: &str = "mbt/traces";

// ---- the model's types ----------------------------------------------------------

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(tag = "tag")]
enum Kind {
    Free,
    Data,
    Tomb,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
struct Entry {
    kind: Kind,
    epoch: i64,
    payload: i64,
}

const FREE: Entry = Entry { kind: Kind::Free, epoch: 0, payload: 0 };
fn data(e: i64, p: i64) -> Entry {
    Entry { kind: Kind::Data, epoch: e, payload: p }
}
fn tomb(e: i64) -> Entry {
    Entry { kind: Kind::Tomb, epoch: e, payload: 0 }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "tag")]
enum WPhase {
    WIdle,
    WReady,
    WWaiting,
    WUnresolved,
    WHalted,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
struct Writer {
    alive: bool,
    phase: WPhase,
    #[serde(rename = "nextSlot")]
    next_slot: i64,
    entry: Entry,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
struct Req {
    epoch: i64,
    slot: i64,
    entry: Entry,
    #[serde(rename = "sawFree")]
    saw_free: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
struct Resp {
    epoch: i64,
    slot: i64,
    entry: Entry,
    ok: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "tag")]
enum CPhaseM {
    CIdle,
    CChecked,
    CInserted,
    CTombWait,
    CTombUnresolved,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
struct ConsumerM {
    phase: CPhaseM,
    epoch: i64,
    slot: i64,
    #[serde(rename = "sawPresent")]
    saw_present: bool,
}

/// The model variables the implementation is checked against (all but the
/// history variables `everLog` and `events`).
#[derive(Debug, Deserialize, PartialEq, Eq)]
struct SpecState {
    #[serde(rename = "s3InlineDesign::s3Inline::log")]
    log: BTreeMap<i64, BTreeMap<i64, Entry>>,
    #[serde(rename = "s3InlineDesign::s3Inline::writers")]
    writers: BTreeMap<i64, Writer>,
    #[serde(rename = "s3InlineDesign::s3Inline::lease")]
    lease: i64,
    #[serde(rename = "s3InlineDesign::s3Inline::queue")]
    queue: BTreeSet<i64>,
    #[serde(rename = "s3InlineDesign::s3Inline::acked")]
    acked: BTreeSet<i64>,
    #[serde(rename = "s3InlineDesign::s3Inline::inflight")]
    inflight: BTreeSet<Req>,
    #[serde(rename = "s3InlineDesign::s3Inline::responses")]
    responses: BTreeSet<Resp>,
    #[serde(rename = "s3InlineDesign::s3Inline::ckpt")]
    ckpt: BTreeMap<i64, i64>,
    #[serde(rename = "s3InlineDesign::s3Inline::closed")]
    closed: BTreeMap<i64, bool>,
    #[serde(rename = "s3InlineDesign::s3Inline::consumer")]
    consumer: ConsumerM,
    #[serde(rename = "s3InlineDesign::s3Inline::central")]
    central: BTreeMap<i64, i64>,
}

// ---- the driver -------------------------------------------------------------------

fn epoch_name(e: i64) -> String {
    format!("E{e}")
}
fn epoch_num(s: &str) -> i64 {
    s.trim_start_matches('E').parse().unwrap_or(0)
}
fn content(p: i64) -> String {
    format!("P{p}")
}
fn payload_of(c: &str) -> i64 {
    c.trim_start_matches('P').parse().unwrap_or(0)
}

fn mutation() -> Mutation {
    match std::env::var("OTAPRS_MUTANT").as_deref() {
        Ok("retry_new_key") => Mutation::RetryNewKey,
        Ok("no_halt") => Mutation::NoHalt,
        Ok("no_check_central") => Mutation::NoCheckCentral,
        _ => Mutation::None,
    }
}

#[derive(Default)]
struct S3InlineDriver {
    lanes: BTreeMap<i64, Lane>,
    alive: BTreeMap<i64, bool>,
    lease: i64,
    queue: BTreeSet<i64>,
    acked: BTreeSet<i64>,
    /// The bucket: key -> user metadata (the body doesn't matter here).
    s3: BTreeMap<String, HashMap<String, String>>,
    /// The network: requests on their way to S3, answers on their way back.
    inflight: BTreeSet<Req>,
    responses: BTreeSet<Resp>,
    consumer: Consumer,
    central: BTreeMap<i64, i64>,
}

impl S3InlineDriver {
    fn key(e: i64, s: i64) -> String {
        proto::slot_key(PREFIX, &epoch_name(e), s as u64)
    }

    /// HEAD.
    fn head(&self, e: i64, s: i64) -> Slot {
        self.s3.get(&Self::key(e, s)).map(Slot::from_meta).unwrap_or(Slot::Free)
    }

    fn lane(&mut self, e: i64) -> &mut Lane {
        self.lanes.get_mut(&e).expect("a lane for every started epoch")
    }

    /// The queue learns that a batch is committed (acked, dequeued).
    fn learned(&mut self, c: &str) {
        let p = payload_of(c);
        self.queue.remove(&p);
        self.acked.insert(p);
    }

    fn after_lane_step(&mut self, e: i64, st: LStep) {
        match st {
            // The lane committed (or found) its own batch: the queue acks it.
            LStep::Committed { content, .. } => self.learned(&content),
            LStep::LearnedOther { content, .. } => self.learned(&content),
            LStep::Head => {
                // After a 412 the lane reads the slot at once.
                let slot = self.lanes[&e].next as i64;
                let found = self.head(e, slot);
                let st2 = self.lane(e).on_head(&found);
                self.after_lane_step(e, st2);
            }
            LStep::Put | LStep::Halted | LStep::Inconsistent => {}
        }
    }

    fn init(&mut self) {
        let m = mutation();
        *self = Self::default();
        let mut l = Lane::new(epoch_name(1));
        l.mutation = m;
        self.lanes.insert(1, l);
        self.alive.insert(1, true);
        self.lease = 1;
        self.queue = PAYLOADS.into_iter().collect();
        self.central = PAYLOADS.into_iter().map(|p| (p, 0)).collect();
        self.consumer.mutation = m;
    }

    fn new_incarnation(&mut self, zombie: bool) {
        self.lease += 1;
        let ne = self.lease;
        let mut l = Lane::new(epoch_name(ne));
        l.mutation = mutation();
        self.lanes.insert(ne, l);
        if !zombie {
            for v in self.alive.values_mut() {
                *v = false;
            }
        }
        self.alive.insert(ne, true);
    }

    fn start_push(&mut self, e: i64, p: i64) {
        let known = self.lane(e).start(&content(p));
        assert!(known.is_none(), "the model only pushes queued payloads");
    }

    fn send(&mut self, e: i64) {
        let slot = self.lanes[&e].next as i64;
        let p = payload_of(self.lanes[&e].entry.as_deref().unwrap_or(""));
        let saw_free = !self.s3.contains_key(&Self::key(e, slot));
        self.lane(e).sent();
        self.inflight.insert(Req { epoch: e, slot, entry: data(e, p), saw_free });
    }

    /// S3 applies a request: create-only, decided atomically here.
    fn apply(&mut self, q: Req) {
        assert!(self.inflight.remove(&q), "apply of a request not in flight");
        let key = Self::key(q.epoch, q.slot);
        let wins = !self.s3.contains_key(&key);
        if wins {
            let mut m = HashMap::new();
            match q.entry.kind {
                Kind::Tomb => {
                    m.insert(proto::META_KIND.into(), proto::KIND_TOMB.into());
                }
                _ => {
                    m.insert(proto::META_KIND.into(), proto::KIND_DATA.into());
                    m.insert(proto::META_CONTENT.into(), content(q.entry.payload));
                }
            }
            m.insert(proto::META_EPOCH.into(), epoch_name(q.epoch));
            self.s3.insert(key, m);
        }
        self.responses.insert(Resp { epoch: q.epoch, slot: q.slot, entry: q.entry, ok: wins });
    }

    fn lose(&mut self, q: Req) {
        assert!(self.inflight.remove(&q));
    }

    fn receive(&mut self, e: i64, r: Resp) {
        assert!(self.responses.remove(&r));
        let st = self.lane(e).on_put(if r.ok { PutOutcome::Ok } else { PutOutcome::Exists });
        self.after_lane_step(e, st);
    }

    fn timeout(&mut self, e: i64) {
        let _ = self.lane(e).on_put(PutOutcome::Unknown);
    }

    fn resolve(&mut self, e: i64) {
        let slot = self.lanes[&e].next as i64;
        let found = self.head(e, slot);
        let st = self.lane(e).on_head(&found);
        self.after_lane_step(e, st);
    }

    fn switch_payload(&mut self, e: i64, p: i64) {
        self.start_push(e, p);
    }

    fn c_check(&mut self, e: i64) {
        let s = self.consumer.next_slot(&epoch_name(e)) as i64;
        let Slot::Data { content, .. } = self.head(e, s) else { panic!("cCheck on a non-data slot") };
        let in_central = self.central[&payload_of(&content)] > 0;
        self.consumer.check(&epoch_name(e), &content, in_central);
    }

    fn c_insert(&mut self) {
        let (e, s) = self.consumer.cur.clone().unwrap();
        let now = self.head(epoch_num(&e), s as i64);
        if let proto::Insert::Rows { content } = self.consumer.insert(&now) {
            *self.central.get_mut(&payload_of(&content)).unwrap() += 1;
        }
    }

    fn c_tomb(&mut self, e: i64) {
        let s = self.consumer.tomb(&epoch_name(e)) as i64;
        self.inflight.insert(Req { epoch: e, slot: s, entry: tomb(e), saw_free: true });
    }

    fn c_tomb_receive(&mut self, r: Resp) {
        assert!(self.responses.remove(&r));
        if self.consumer.on_tomb_put(if r.ok { PutOutcome::Ok } else { PutOutcome::Exists }).is_none() {
            let found = self.head(r.epoch, r.slot);
            let resend = self.consumer.on_tomb_head(&found);
            assert!(!resend, "a 412 means the slot is taken");
        }
    }

    fn c_tomb_resolve(&mut self) {
        let (e, s) = self.consumer.cur.clone().unwrap();
        let (e, s) = (epoch_num(&e), s as i64);
        if self.consumer.on_tomb_head(&self.head(e, s)) {
            self.inflight.insert(Req { epoch: e, slot: s, entry: tomb(e), saw_free: true });
        }
    }
}

impl Driver for S3InlineDriver {
    type State = SpecState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => self.init(),
            // quint's Rust evaluator (v0.6.0) labels the initial state of some
            // traces `step` instead of `init` (only ever state 0; the
            // TypeScript backend labels them all `init`). A `step` anywhere
            // else would reset the driver, and the state check would fail.
            step => self.init(),
            newIncarnation(z: bool) => self.new_incarnation(z),
            startPush(e: i64, p: i64) => self.start_push(e, p),
            send(e: i64) => self.send(e),
            apply(q: Req) => self.apply(q),
            lose(q: Req) => self.lose(q),
            receive(e: i64, r: Resp) => self.receive(e, r),
            timeout(e: i64) => self.timeout(e),
            resolve(e: i64) => self.resolve(e),
            switchPayload(e: i64, p: i64) => self.switch_payload(e, p),
            cCheck(e: i64) => self.c_check(e),
            cInsert => self.c_insert(),
            cAdvance => self.consumer.advance(),
            cSeeTomb(e: i64) => self.consumer.see_tomb(&epoch_name(e)),
            cTomb(e: i64) => self.c_tomb(e),
            cTombReceive(r: Resp) => self.c_tomb_receive(r),
            cTombTimeout => { let _ = self.consumer.on_tomb_put(PutOutcome::Unknown); },
            cTombResolve => self.c_tomb_resolve(),
            cCrash => self.consumer.crash(),
        })
    }
}

impl State<S3InlineDriver> for SpecState {
    fn from_driver(d: &S3InlineDriver) -> Result<Self> {
        let mut log = BTreeMap::new();
        for e in 1..=MAX_EPOCH {
            let mut slots = BTreeMap::new();
            for s in 0..MAX_SLOT {
                let x = match d.head(e, s) {
                    Slot::Free => FREE,
                    Slot::Tomb => tomb(e),
                    Slot::Data { epoch, content } => data(epoch_num(&epoch), payload_of(&content)),
                };
                slots.insert(s, x);
            }
            log.insert(e, slots);
        }
        let mut writers = BTreeMap::new();
        for e in 1..=MAX_EPOCH {
            let w = match d.lanes.get(&e) {
                None => Writer { alive: false, phase: WPhase::WIdle, next_slot: 0, entry: FREE },
                Some(l) => Writer {
                    alive: d.alive[&e],
                    phase: match l.phase {
                        Phase::Idle => WPhase::WIdle,
                        Phase::Ready => WPhase::WReady,
                        Phase::Waiting => WPhase::WWaiting,
                        Phase::Unresolved => WPhase::WUnresolved,
                        Phase::Halted => WPhase::WHalted,
                    },
                    next_slot: l.next as i64,
                    entry: l.entry.as_deref().map(|c| data(e, payload_of(c))).unwrap_or(FREE),
                },
            };
            writers.insert(e, w);
        }
        let c = &d.consumer;
        let (ce, cs) = c.cur.as_ref().map(|(e, s)| (epoch_num(e), *s as i64)).unwrap_or((0, 0));
        Ok(SpecState {
            log,
            writers,
            lease: d.lease,
            queue: d.queue.clone(),
            acked: d.acked.clone(),
            inflight: d.inflight.clone(),
            responses: d.responses.clone(),
            ckpt: (1..=MAX_EPOCH).map(|e| (e, c.next_slot(&epoch_name(e)) as i64)).collect(),
            closed: (1..=MAX_EPOCH).map(|e| (e, c.is_closed(&epoch_name(e)))).collect(),
            consumer: ConsumerM {
                phase: match c.phase {
                    CPhase::Idle => CPhaseM::CIdle,
                    CPhase::Checked => CPhaseM::CChecked,
                    CPhase::Inserted => CPhaseM::CInserted,
                    CPhase::TombWait => CPhaseM::CTombWait,
                    CPhase::TombUnresolved => CPhaseM::CTombUnresolved,
                },
                epoch: ce,
                slot: cs,
                saw_present: c.saw_present,
            },
            central: d.central.clone(),
        })
    }
}

#[quint_run(spec = "../model/s3Inline.qnt", main = "s3InlineDesign", max_samples = 300, max_steps = 60)]
fn s3inline_design_simulation() -> impl Driver {
    S3InlineDriver::default()
}
