//! Dead-lane retirement, simulated (DECISIONS.md D35, FORMAT.md §3.1): the
//! lifecycle of `../model/retirement.qnt` driven by a seeded random walk
//! against the real consumer (`Worker`, `watermark_run`, and `consume
//! retire-lane`'s checks) on the in-memory bucket and central.
//!
//! One cluster, two publishers (`c1/p1`, `c1/p2`), one signal (`logs`).
//! Each publisher receives requests into its custody, sends each to its
//! lane's head (the PUT in flight may land, be lost, or land after its
//! writer died: a zombie), acks it once seen committed, beats, closes in
//! order (drained: the close's low is its empty custody's floor, the
//! clock), dies without a close (custody left on its volume, a PUT maybe
//! in flight), has its volume deleted (its custody lost, acknowledged:
//! only once no PUT of it is in flight), or is adopted by a new incarnation
//! of the same producer (a later epoch: its birth, then the volume's
//! custody replayed with each request's original received_at). The
//! operator retires a dead lane with `retire_lane` (its evidence: the
//! volume deleted, `OpMode::Evidence`; or not, `OpMode::Mistake`).
//!
//! Checked at every publication and at the end, as the model's properties:
//! - **completeSound**: every request received below the published value
//!   (the cluster's and the fleet's) is in central, lost with its volume,
//!   or quarantined;
//! - **noLateBelow**: no request enters central with a received_at below a
//!   value published before it did;
//! - **quarantineOnlyOnMistake**: the quarantine stays empty unless an
//!   operator retired a lane whose custody was not empty, or a close was
//!   committed with custody left (the edge mutant);
//! - **wmBounded**: the published value never passes the clock.
//!
//! Mutants (`Knobs`): the consumer's `Mutation::{RetireStale, StaysRetired,
//! IngestBelow, RetireInFlight}` and the edge's `close_undrained`, each
//! caught by some seed (`dst_consumer.rs` `retirement_catches_mutants`).

#![allow(dead_code)]

use crate::consumer::bucket::{Bucket, Cond, MemBucket, Put};
use crate::consumer::coord::{Mutation, Timing};
use crate::consumer::discovery::Backoff;
use crate::consumer::sql::MemCentral;
use crate::consumer::watermark::{WmConfig, watermark_run};
use crate::consumer::worker::{BalanceMode, Config, FakeClock, Worker};
use bytes::Bytes;
use otap_s3pq::proto;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

pub const ROOT: &str = "r/edges";
pub const CTL: &str = "r/ctl";
const T: Timing = Timing { ttl_ms: 9000, margin_ms: 1000, budget_ms: 3000, slack_ms: 1000, mutation: Mutation::None };
/// GC's zombie bound here: a PUT of a dead process lands within it.
pub const ZOMBIE_MS: u64 = 6_000;

/// What the operator's retirement rests on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpMode {
    /// No operator retirement.
    Off,
    /// Only once the volume is deleted (the design).
    Evidence,
    /// Also while the volume is kept (the model's `opMistake`): safe, its
    /// replay quarantined.
    Mistake,
}

#[derive(Clone, Copy, Debug)]
pub struct Knobs {
    pub mutation: Mutation,
    /// The edge commits its close with requests still in custody.
    pub close_undrained: bool,
    pub op: OpMode,
    pub steps: usize,
}

impl Knobs {
    pub fn design() -> Knobs {
        Knobs { mutation: Mutation::None, close_undrained: false, op: OpMode::Evidence, steps: 400 }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Life {
    Up,
    Closed,
    Dead,
}

struct Req {
    id: u64,
    lane: usize,
    r_ns: u64,
}

/// A PUT on its way: (key, metadata, lane, the writer's incarnation).
struct InFlight {
    key: String,
    meta: BTreeMap<String, String>,
    lane: usize,
    inc: u32,
    /// When it was sent: a PUT lands within `PUT_LIFETIME_MS` or never
    /// (the request lifetime the zombie bound covers).
    sent_ms: u64,
}

/// The longest a PUT can be in flight (below `ZOMBIE_MS`, as GC's delay).
pub const PUT_LIFETIME_MS: u64 = 3_000;

struct Pub {
    producer: String,
    life: Life,
    since_ms: u64,
    inc: u32,
    epoch: String,
    seq: u64,
    /// The request whose PUT is out (the writer waits for it).
    busy: Option<(u64, String)>,
    /// Custody (on the volume): request ids.
    buffer: Vec<u64>,
    epochs: u32,
}

/// What a run saw.
#[derive(Debug, Default)]
pub struct Outcome {
    pub violations: Vec<String>,
    pub witnesses: BTreeSet<&'static str>,
    pub summary: String,
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
    fn chance(&mut self, p: f64) -> bool {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64 <= p
    }
}

struct World {
    b: Rc<MemBucket>,
    c: Rc<MemCentral>,
    clk: FakeClock,
    k: Knobs,
    pubs: Vec<Pub>,
    reqs: BTreeMap<u64, Req>,
    next_id: u64,
    puts: Vec<InFlight>,
    lost: BTreeSet<u64>,
    /// The highest cluster value published so far (ns).
    published: u64,
    /// Requests seen in central, and the published value when first seen.
    ingested: BTreeSet<u64>,
    /// When each request was first seen in central (ms): the history's
    /// steps (D29 amendment) are checked against it (`historySound`).
    noticed: BTreeMap<u64, u64>,
    /// The history steps of the fleet's and c1's documents seen so far.
    hist_steps: Vec<(&'static str, crate::consumer::wmhistory::HistStep)>,
    mistake: bool,
    undrained_close: bool,
    out: Outcome,
}

fn content(id: u64) -> String {
    format!("q{id}")
}

impl World {
    fn now_ms(&self) -> u64 {
        self.clk.0.get()
    }
    fn now_ns(&self) -> u64 {
        self.now_ms() * 1_000_000
    }
    fn prefix(&self, l: usize) -> String {
        format!("{ROOT}/c1/{}/logs", self.pubs[l].producer)
    }
    fn lane_id(&self, l: usize) -> String {
        format!("c1/{}/logs", self.pubs[l].producer)
    }
    /// The custody floor: the oldest received_at in custody but `except`, or now.
    fn floor(&self, l: usize, except: Option<u64>) -> u64 {
        self.pubs[l].buffer.iter().filter(|id| Some(**id) != except).map(|id| self.reqs[id].r_ns).fold(self.now_ns(), u64::min)
    }
    fn meta(&self, l: usize, kind: &str, content: &str, rows: u64, r_ns: u64, low: u64) -> BTreeMap<String, String> {
        let p = &self.pubs[l];
        let mut m = BTreeMap::new();
        for (k, v) in [
            (proto::META_KIND, kind.to_string()),
            (proto::META_EPOCH, p.epoch.clone()),
            (proto::META_SEQ, p.seq.to_string()),
            (proto::META_CONTENT, content.to_string()),
            (proto::META_ROWS, rows.to_string()),
            (proto::META_LOW, low.to_string()),
        ] {
            let _ = m.insert(k.to_string(), v);
        }
        if r_ns > 0 {
            let _ = m.insert(proto::META_RECEIVED.to_string(), r_ns.to_string());
        }
        m
    }
    /// A slot committed synchronously (births, heartbeats, closes: zero
    /// bytes, no custody). False: the head was taken (a tombstone: the
    /// writer halts and goes on in a new epoch; the caller retries).
    async fn commit_now(&mut self, l: usize, kind: &str, low: u64) -> bool {
        let tag = format!("{kind}-{}-{}", self.pubs[l].inc, self.now_ms());
        let m = self.meta(l, kind, &tag, 0, 0, low);
        let key = proto::slot_key(&self.prefix(l), &self.pubs[l].epoch, self.pubs[l].seq);
        match self.b.put(&key, Bytes::new(), Cond::Create, &m).await {
            Put::Ok(_) => {
                self.pubs[l].seq += 1;
                true
            }
            _ => {
                self.halt(l).await;
                false
            }
        }
    }
    /// The head is taken: a tombstone (halt, a new epoch) or anything else (move on).
    async fn halt(&mut self, l: usize) {
        let key = proto::slot_key(&self.prefix(l), &self.pubs[l].epoch, self.pubs[l].seq);
        match self.b.head(&key).await.ok().flatten() {
            Some(h) if proto::Slot::from_meta(&h) == proto::Slot::Tomb => self.new_epoch(l),
            Some(_) => self.pubs[l].seq += 1,
            None => {}
        }
    }
    fn new_epoch(&mut self, l: usize) {
        let p = &mut self.pubs[l];
        p.epochs += 1;
        // Epochs sort by start (a timestamp, then random bits).
        p.epoch = format!("E{:012}-{}", self.clk.0.get(), p.epochs);
        p.seq = 0;
        p.busy = None;
    }

    // ---- the publisher -------------------------------------------------------------
    fn receive(&mut self, l: usize) {
        if self.pubs[l].life != Life::Up || self.next_id > 40 {
            return;
        }
        let id = self.next_id;
        self.next_id += 1;
        let _ = self.reqs.insert(id, Req { id, lane: l, r_ns: self.now_ns() });
        self.pubs[l].buffer.push(id);
    }
    fn send(&mut self, l: usize, pick: u64) {
        let p = &self.pubs[l];
        if p.life != Life::Up || p.busy.is_some() || p.buffer.is_empty() {
            return;
        }
        let id = p.buffer[(pick % p.buffer.len() as u64) as usize];
        let low = self.floor(l, Some(id));
        let m = self.meta(l, proto::KIND_DATA, &content(id), 1, self.reqs[&id].r_ns, low);
        let key = proto::slot_key(&self.prefix(l), &self.pubs[l].epoch, self.pubs[l].seq);
        self.puts.push(InFlight { key: key.clone(), meta: m, lane: l, inc: self.pubs[l].inc, sent_ms: self.now_ms() });
        self.pubs[l].busy = Some((id, key));
    }
    async fn land(&mut self, i: usize) {
        let f = self.puts.remove(i);
        let dead = self.pubs[f.lane].life != Life::Up || self.pubs[f.lane].inc != f.inc;
        if let Put::Ok(_) = self.b.put(&f.key, Bytes::from(vec![0u8; 10]), Cond::Create, &f.meta).await {
            if dead {
                let _ = self.out.witnesses.insert("zombie landed");
            }
        }
    }
    fn lose(&mut self, i: usize) {
        let _ = self.puts.remove(i);
    }
    /// The writer sees its PUT's slot: ours (ack: out of custody), a
    /// tombstone (halt, a new epoch; the request is resent there), or free
    /// with nothing in flight (resend).
    async fn resolve(&mut self, l: usize) {
        let Some((id, key)) = self.pubs[l].busy.clone() else { return };
        if self.pubs[l].life != Life::Up {
            return;
        }
        match self.b.head(&key).await.ok().flatten() {
            Some(h) if h.get(proto::META_CONTENT) == Some(&content(id)) => {
                self.pubs[l].buffer.retain(|x| *x != id);
                self.pubs[l].busy = None;
                self.pubs[l].seq += 1;
            }
            Some(h) if proto::Slot::from_meta(&h) == proto::Slot::Tomb => self.new_epoch(l),
            Some(_) => {
                self.pubs[l].busy = None;
                self.pubs[l].seq += 1;
            }
            None if !self.puts.iter().any(|p| p.key == key) => self.pubs[l].busy = None,
            None => {}
        }
    }
    async fn beat(&mut self, l: usize) {
        if self.pubs[l].life == Life::Up && self.pubs[l].busy.is_none() {
            let low = self.floor(l, None);
            let _ = self.commit_now(l, proto::KIND_BEAT, low).await;
        }
    }
    async fn close(&mut self, l: usize) {
        let p = &self.pubs[l];
        if p.life != Life::Up || p.busy.is_some() || (!p.buffer.is_empty() && !self.k.close_undrained) {
            return;
        }
        let undrained = !p.buffer.is_empty();
        let low = self.floor(l, None);
        if self.commit_now(l, proto::KIND_CLOSE, low).await {
            self.undrained_close |= undrained;
            let now = self.now_ms();
            let p = &mut self.pubs[l];
            p.life = Life::Closed;
            p.since_ms = now;
            p.busy = None;
        }
    }
    fn die(&mut self, l: usize) {
        if self.pubs[l].life == Life::Up {
            self.pubs[l].life = Life::Dead;
            self.pubs[l].since_ms = self.now_ms();
        }
    }
    fn delete_volume(&mut self, l: usize) {
        let p = &self.pubs[l];
        // Only once no PUT of the dead process can land (the runbook: the pod
        // gone longer than a request lifetime).
        let wait = self.k.mutation != Mutation::RetireInFlight; // the model's ZOMBIE_WAIT
        if p.life == Life::Up || p.buffer.is_empty() || (wait && self.puts.iter().any(|f| f.lane == l)) {
            return;
        }
        let ids = std::mem::take(&mut self.pubs[l].buffer);
        self.lost.extend(ids);
        let _ = self.out.witnesses.insert("lost with its volume");
    }
    /// A new incarnation mounts the volume: a later epoch, its birth first
    /// (low: its custody floor), then it replays the custody.
    async fn adopt(&mut self, l: usize) {
        if self.pubs[l].life == Life::Up || self.pubs[l].epochs >= 4 {
            return;
        }
        self.pubs[l].inc += 1;
        self.new_epoch(l);
        self.pubs[l].life = Life::Up;
        self.pubs[l].since_ms = self.now_ms();
        let low = self.floor(l, None);
        while !self.commit_now(l, proto::KIND_BEAT, low).await {}
        let _ = self.out.witnesses.insert("adopted");
    }

    // ---- the operator ----------------------------------------------------------------
    /// `consume retire-lane` on a dead lane: with the evidence (the volume
    /// deleted) or, `OpMode::Mistake`, with the volume kept; the tool's own
    /// checks (b) and (c) decide the rest.
    async fn operator(&mut self, l: usize) {
        let p = &self.pubs[l];
        let attested = match self.k.op {
            OpMode::Off => return,
            OpMode::Evidence => p.buffer.is_empty(),
            OpMode::Mistake => true,
        };
        // The operator also confirms the process gone longer than a request
        // lifetime (the pod deleted: retire-lane's (b) sees only the lane's
        // last object); the mutant does not wait.
        let gone = self.now_ms() >= p.since_ms + PUT_LIFETIME_MS || self.k.mutation == Mutation::RetireInFlight;
        if p.life != Life::Dead || !attested || !gone {
            return;
        }
        let kept = !p.buffer.is_empty();
        let cfg = crate::consumer::retire::RetireCfg {
            root: ROOT.into(),
            ctl: CTL.into(),
            zombie_ms: ZOMBIE_MS,
            mutation: self.k.mutation,
            dry_run: false,
        };
        let lane = self.lane_id(l);
        if crate::consumer::retire::retire_lane(&*self.b, &cfg, &lane, "sim: volume deleted", true, self.now_ms()).await.is_ok() {
            self.mistake |= kept;
            let _ = self.out.witnesses.insert(if kept { "operator retired a kept volume" } else { "operator retired" });
        }
    }
    /// PUTs past their lifetime never land.
    fn expire(&mut self) {
        let now = self.now_ms();
        self.puts.retain(|f| now < f.sent_ms + PUT_LIFETIME_MS);
    }

    // ---- the reader: checks --------------------------------------------------------------
    async fn quarantined(&self) -> BTreeSet<u64> {
        let mut out = BTreeSet::new();
        for l in 0..self.pubs.len() {
            if let Ok(Some((d, _))) = crate::consumer::retire::read(&*self.b, CTL, &self.lane_id(l)).await {
                for q in d.objects {
                    if let Some(id) = q.content.strip_prefix('q').and_then(|x| x.parse().ok()) {
                        let _ = out.insert(id);
                    }
                }
            }
        }
        out
    }
    /// After a worker step: requests newly in central below a value published before.
    fn check_ingest(&mut self) {
        for (id, r) in &self.reqs {
            if !self.ingested.contains(id) && self.c.count("otel_logs", &content(*id)) > 0 {
                let _ = self.ingested.insert(*id);
                let _ = self.noticed.insert(*id, self.clk.0.get());
                if r.r_ns < self.published {
                    self.out.violations.push(format!(
                        "noLateBelow: q{id} (lane {}, received {}) ingested after {} was published",
                        r.lane, r.r_ns, self.published
                    ));
                }
            }
        }
    }
    async fn publish(&mut self) {
        let wcfg = WmConfig { skew_ms: 0, mutation: self.k.mutation, ..WmConfig::new(ROOT, CTL) };
        let Ok(run) = watermark_run(&*self.b, &wcfg, self.now_ms()).await else { return };
        let cl = run.clusters.iter().find(|d| d.cluster == "c1").map_or(0, |d| d.complete_through_ns);
        let v = cl.max(run.fleet.complete_through_ns);
        if v > self.now_ns() {
            self.out.violations.push(format!("wmBounded: {v} > now {}", self.now_ns()));
        }
        if run.clusters.iter().any(|d| !d.retired.is_empty()) && v > self.published {
            let _ = self.out.witnesses.insert("published past a retired lane");
        }
        self.published = self.published.max(v);
        for (scope, h) in std::iter::once(("fleet", &run.fleet.history)).chain(run.clusters.iter().filter(|d| d.cluster == "c1").map(|d| ("c1", &d.history))) {
            for st in h.sealing.iter().chain(std::iter::once(&h.open)).flat_map(|x| x.steps.iter()) {
                if !self.hist_steps.iter().any(|(s0, x)| *s0 == scope && x == st) {
                    self.hist_steps.push((scope, st.clone()));
                }
            }
        }
        self.check_sound().await;
    }
    async fn check_sound(&mut self) {
        let quar = self.quarantined().await;
        for (id, r) in &self.reqs {
            let ok = self.c.count("otel_logs", &content(*id)) > 0 || self.lost.contains(id) || quar.contains(id);
            if r.r_ns < self.published && !ok {
                self.out.violations.push(format!(
                    "completeSound: q{id} (lane {}, received {}) below the published {} is not in central, lost or quarantined",
                    r.lane, r.r_ns, self.published
                ));
            }
        }
        // historySound: every history step's value was true by its stamp:
        // each request below it was in central by then (lost or quarantined
        // ones aside, as above; an operator's mistake voids it as it voids
        // completeSound)
        let mistake = self.mistake;
        for (scope, st) in self.hist_steps.iter().filter(|_| !mistake) {
            for (id, r) in &self.reqs {
                if r.r_ns < st.ct_ns && !self.lost.contains(id) && !quar.contains(id) && self.noticed.get(id).is_none_or(|t| *t > st.at_ms) {
                    self.out.violations.push(format!(
                        "historySound: the {scope} history says {} by {} ms, but q{id} (lane {}, received {}) was in central only at {:?}",
                        st.ct_ns, st.at_ms, r.lane, r.r_ns, self.noticed.get(id)
                    ));
                }
            }
        }
        if !self.hist_steps.is_empty() {
            let _ = self.out.witnesses.insert("history recorded");
        }
        if !quar.is_empty() && !self.mistake && !self.undrained_close {
            self.out.violations.push(format!("quarantineOnlyOnMistake: {quar:?} quarantined without a mistake"));
        }
        if !quar.is_empty() {
            let _ = self.out.witnesses.insert("quarantined");
        }
    }
}

fn worker_cfg(m: Mutation) -> Config {
    let mut c = Config::new(ROOT, CTL, "w1");
    c.timing = Timing { mutation: m, ..T };
    c.discover_ms = 0;
    c.lanes_every_ms = 0;
    c.full_list_ms = 0;
    c.quiet_ms = 2_000;
    c.backoff = Backoff::off();
    c.poll_ms = 0;
    c.balance.mode = BalanceMode::Count;
    c.quarantine_skew_ms = 0;
    c
}

/// One seed.
pub async fn run(seed: u64, k: Knobs) -> Outcome {
    let clk = FakeClock::default();
    clk.0.set(1_000_000);
    let c = Rc::new(MemCentral { clock: clk.0.clone(), ..Default::default() });
    let b = Rc::new(MemBucket { clock: clk.0.clone(), ..Default::default() });
    let mut rng = Rng(seed.wrapping_mul(0x2545_f491_4f6c_dd1d) ^ 0x5eed);
    let pubs = (1..=2)
        .map(|i| Pub { producer: format!("p{i}"), life: Life::Up, since_ms: 0, inc: 1, epoch: String::new(), seq: 0, busy: None, buffer: Vec::new(), epochs: 0 })
        .collect();
    let mut w = World {
        b: b.clone(),
        c: c.clone(),
        clk: clk.clone(),
        k,
        pubs,
        reqs: BTreeMap::new(),
        next_id: 1,
        puts: Vec::new(),
        lost: BTreeSet::new(),
        published: 0,
        ingested: BTreeSet::new(),
        noticed: BTreeMap::new(),
        hist_steps: Vec::new(),
        mistake: false,
        undrained_close: false,
        out: Outcome::default(),
    };
    for l in 0..2 {
        w.new_epoch(l);
        while !w.commit_now(l, proto::KIND_BEAT, 0).await {}
    }
    let mut worker = Worker::new(worker_cfg(k.mutation), b.clone(), c.clone(), clk.clone());
    for _ in 0..k.steps {
        let l = rng.below(2) as usize;
        match rng.below(100) {
            0..=14 => w.receive(l),
            15..=26 => w.send(l, rng.next()),
            27..=36 if { w.expire(); !w.puts.is_empty() } => {
                let i = rng.below(w.puts.len() as u64) as usize;
                if rng.chance(0.85) { w.land(i).await } else { w.lose(i) }
            }
            37..=46 => w.resolve(l).await,
            47..=52 => w.beat(l).await,
            53..=55 => w.close(l).await,
            56..=57 => w.die(l),
            58..=59 => w.delete_volume(l),
            60..=62 => w.adopt(l).await,
            63..=66 => w.operator(l).await,
            67..=84 => {
                let _ = worker.step().await;
                w.check_ingest();
            }
            85..=91 => w.publish().await,
            _ => {
                let dt = 100 + rng.below(1_500);
                clk.0.set(clk.0.get() + dt);
            }
        }
    }
    // Wind down: every live publisher commits and closes; in-flight PUTs
    // land; the worker catches up; a last publication.
    for _ in 0..200 {
        for l in 0..2 {
            w.send(l, 0);
            w.resolve(l).await;
        }
        w.expire();
        while !w.puts.is_empty() {
            w.land(0).await;
        }
        let _ = worker.step().await;
        w.check_ingest();
        clk.0.set(clk.0.get() + 300);
    }
    w.publish().await;
    let retired = |l: usize| worker.checkpoint(&w.lane_id(l)).is_some_and(|c| c.retired_ns > 0);
    if (0..2).any(retired) {
        let _ = w.out.witnesses.insert("a lane retired");
    }
    if (0..2).any(|l| worker.checkpoint(&w.lane_id(l)).is_some_and(|c| !c.reborn_epoch.is_empty())) && !w.ingested.is_empty() {
        let _ = w.out.witnesses.insert("reborn");
    }
    w.out.summary = format!(
        "seed {seed}: {} requests, {} ingested, {} lost, published {}, stats {:?}",
        w.reqs.len(),
        w.ingested.len(),
        w.lost.len(),
        w.published,
        (worker.stats.lanes_retired, worker.stats.lanes_reborn, worker.stats.quarantined_objects, worker.stats.below_copies)
    );
    w.out
}
