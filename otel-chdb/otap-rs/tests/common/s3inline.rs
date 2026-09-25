//! The s3Inline model's types and a driver for one log (writer lanes,
//! bucket, network, consumer, central), shared by the model-based tests of
//! `s3Inline.qnt` (`mbt_s3inline.rs`) and of `s3InlineMetrics.qnt`
//! (`mbt_s3inline_metrics.rs`, two logs and the request level on top).
#![allow(dead_code)]

use otap_s3pq::proto::{self, CPhase, Consumer, Lane, Mutation, Phase, PutOutcome, Slot, Step as LStep};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet, HashMap};

pub const MAX_EPOCH: i64 = 3;
pub const MAX_SLOT: i64 = 4;
pub const PAYLOADS: [i64; 2] = [1, 2];

// ---- the model's types ----------------------------------------------------------

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(tag = "tag")]
pub enum Kind {
    Free,
    Data,
    Tomb,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Entry {
    pub kind: Kind,
    pub epoch: i64,
    pub payload: i64,
}

pub const FREE: Entry = Entry { kind: Kind::Free, epoch: 0, payload: 0 };
pub fn data(e: i64, p: i64) -> Entry {
    Entry { kind: Kind::Data, epoch: e, payload: p }
}
pub fn tomb(e: i64) -> Entry {
    Entry { kind: Kind::Tomb, epoch: e, payload: 0 }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "tag")]
pub enum WPhase {
    WIdle,
    WReady,
    WWaiting,
    WUnresolved,
    WHalted,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
pub struct Writer {
    pub alive: bool,
    pub phase: WPhase,
    #[serde(rename = "nextSlot")]
    pub next_slot: i64,
    pub entry: Entry,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Req {
    pub epoch: i64,
    pub slot: i64,
    pub entry: Entry,
    #[serde(rename = "sawFree")]
    pub saw_free: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Resp {
    pub epoch: i64,
    pub slot: i64,
    pub entry: Entry,
    pub ok: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "tag")]
pub enum CPhaseM {
    CIdle,
    CChecked,
    CInserted,
    CTombWait,
    CTombUnresolved,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
pub struct ConsumerM {
    pub phase: CPhaseM,
    pub epoch: i64,
    pub slot: i64,
    #[serde(rename = "sawPresent")]
    pub saw_present: bool,
}

/// One log's model variables (all but the history variables `everLog` and
/// `events`), as the implementation projects them.
#[derive(Debug, Deserialize, PartialEq, Eq, Clone)]
pub struct LogState {
    pub log: BTreeMap<i64, BTreeMap<i64, Entry>>,
    pub writers: BTreeMap<i64, Writer>,
    pub lease: i64,
    pub queue: BTreeSet<i64>,
    pub acked: BTreeSet<i64>,
    pub inflight: BTreeSet<Req>,
    pub responses: BTreeSet<Resp>,
    pub ckpt: BTreeMap<i64, i64>,
    pub closed: BTreeMap<i64, bool>,
    pub consumer: ConsumerM,
    pub central: BTreeMap<i64, i64>,
}

// ---- the driver -------------------------------------------------------------------

pub fn epoch_name(e: i64) -> String {
    format!("E{e}")
}
pub fn epoch_num(s: &str) -> i64 {
    s.trim_start_matches('E').parse().unwrap_or(0)
}
pub fn content(p: i64) -> String {
    format!("P{p}")
}
pub fn payload_of(c: &str) -> i64 {
    c.trim_start_matches('P').parse().unwrap_or(0)
}

pub fn mutation() -> Mutation {
    match std::env::var("OTAPRS_MUTANT").as_deref() {
        Ok("retry_new_key") => Mutation::RetryNewKey,
        Ok("no_halt") => Mutation::NoHalt,
        Ok("no_check_central") => Mutation::NoCheckCentral,
        Ok("ack_on_any") => Mutation::AckOnAny,
        _ => Mutation::None,
    }
}

#[derive(Default)]
pub struct S3InlineDriver {
    /// The log's key prefix (one per signal namespace).
    pub prefix: String,
    pub lanes: BTreeMap<i64, Lane>,
    pub alive: BTreeMap<i64, bool>,
    pub lease: i64,
    pub queue: BTreeSet<i64>,
    pub acked: BTreeSet<i64>,
    /// The bucket: key -> user metadata (the body doesn't matter here).
    pub s3: BTreeMap<String, HashMap<String, String>>,
    /// The network: requests on their way to S3, answers on their way back.
    pub inflight: BTreeSet<Req>,
    pub responses: BTreeSet<Resp>,
    pub consumer: Consumer,
    pub central: BTreeMap<i64, i64>,
}

impl S3InlineDriver {
    pub fn key(&self, e: i64, s: i64) -> String {
        proto::slot_key(&self.prefix, &epoch_name(e), s as u64)
    }

    /// HEAD.
    pub fn head(&self, e: i64, s: i64) -> Slot {
        self.s3.get(&self.key(e, s)).map(Slot::from_meta).unwrap_or(Slot::Free)
    }

    pub fn lane(&mut self, e: i64) -> &mut Lane {
        self.lanes.get_mut(&e).expect("a lane for every started epoch")
    }

    /// The queue learns that a batch is committed (acked, dequeued).
    pub fn learned(&mut self, c: &str) {
        let p = payload_of(c);
        self.queue.remove(&p);
        self.acked.insert(p);
    }

    pub fn after_lane_step(&mut self, e: i64, st: LStep) {
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

    pub fn init(&mut self) {
        let m = mutation();
        let prefix = std::mem::take(&mut self.prefix);
        *self = Self { prefix, ..Self::default() };
        let mut l = Lane::new(epoch_name(1));
        l.mutation = m;
        self.lanes.insert(1, l);
        self.alive.insert(1, true);
        self.lease = 1;
        self.queue = PAYLOADS.into_iter().collect();
        self.central = PAYLOADS.into_iter().map(|p| (p, 0)).collect();
        self.consumer.mutation = m;
    }

    pub fn new_incarnation(&mut self, zombie: bool) {
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

    pub fn start_push(&mut self, e: i64, p: i64) {
        let known = self.lane(e).start(&content(p));
        assert!(known.is_none(), "the model only pushes queued payloads");
    }

    pub fn send(&mut self, e: i64) {
        let slot = self.lanes[&e].next as i64;
        let p = payload_of(self.lanes[&e].entry.as_deref().unwrap_or(""));
        let saw_free = !self.s3.contains_key(&self.key(e, slot));
        self.lane(e).sent();
        self.inflight.insert(Req { epoch: e, slot, entry: data(e, p), saw_free });
    }

    /// S3 applies a request: create-only, decided atomically here.
    pub fn apply(&mut self, q: Req) {
        assert!(self.inflight.remove(&q), "apply of a request not in flight");
        let key = self.key(q.epoch, q.slot);
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

    pub fn lose(&mut self, q: Req) {
        assert!(self.inflight.remove(&q));
    }

    pub fn receive(&mut self, e: i64, r: Resp) {
        assert!(self.responses.remove(&r));
        let st = self.lane(e).on_put(if r.ok { PutOutcome::Ok } else { PutOutcome::Exists });
        self.after_lane_step(e, st);
    }

    pub fn timeout(&mut self, e: i64) {
        let _ = self.lane(e).on_put(PutOutcome::Unknown);
    }

    pub fn resolve(&mut self, e: i64) {
        let slot = self.lanes[&e].next as i64;
        let found = self.head(e, slot);
        let st = self.lane(e).on_head(&found);
        self.after_lane_step(e, st);
    }

    pub fn switch_payload(&mut self, e: i64, p: i64) {
        self.start_push(e, p);
    }

    pub fn c_check(&mut self, e: i64) {
        let s = self.consumer.next_slot(&epoch_name(e)) as i64;
        let Slot::Data { content, .. } = self.head(e, s) else { panic!("cCheck on a non-data slot") };
        let in_central = self.central[&payload_of(&content)] > 0;
        self.consumer.check(&epoch_name(e), &content, in_central);
    }

    pub fn c_insert(&mut self) {
        let (e, s) = self.consumer.cur.clone().unwrap();
        let now = self.head(epoch_num(&e), s as i64);
        if let proto::Insert::Rows { content } = self.consumer.insert(&now) {
            *self.central.get_mut(&payload_of(&content)).unwrap() += 1;
        }
    }

    pub fn c_tomb(&mut self, e: i64) {
        let s = self.consumer.tomb(&epoch_name(e)) as i64;
        self.inflight.insert(Req { epoch: e, slot: s, entry: tomb(e), saw_free: true });
    }

    pub fn c_tomb_receive(&mut self, r: Resp) {
        assert!(self.responses.remove(&r));
        if self.consumer.on_tomb_put(if r.ok { PutOutcome::Ok } else { PutOutcome::Exists }).is_none() {
            let found = self.head(r.epoch, r.slot);
            let resend = self.consumer.on_tomb_head(&found);
            assert!(!resend, "a 412 means the slot is taken");
        }
    }

    pub fn c_tomb_resolve(&mut self) {
        let (e, s) = self.consumer.cur.clone().unwrap();
        let (e, s) = (epoch_num(&e), s as i64);
        if self.consumer.on_tomb_head(&self.head(e, s)) {
            self.inflight.insert(Req { epoch: e, slot: s, entry: tomb(e), saw_free: true });
        }
    }
}

impl S3InlineDriver {
    /// The implementation's state, as the model's variables of one log.
    pub fn project(&self) -> LogState {
        let d = self;
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
        LogState {
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
        }
    }
}

