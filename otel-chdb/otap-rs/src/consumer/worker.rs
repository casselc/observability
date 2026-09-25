//! The consumer worker: one process of a fleet sharing the lanes.
//!
//! Each `step` (one poll):
//! 1. **Leases.** A held lease past its window (own monotonic clock) is
//!    dropped; one due is renewed by CAS. Every `discover_ms` the worker
//!    lists the lanes, beats its heartbeat, lists the workers (liveness by
//!    ETag change) and the leases, then evens the load: it releases above
//!    its fair share and takes free, released or expired lanes below it.
//!    Taking a lane rewrites its checkpoint with the new lease epoch (the
//!    fence against the previous holder's checkpoint writes).
//! 2. **Discovery.** Per held lane and open epoch, LIST StartAfter the
//!    checkpoint's key. The newest epoch's LIST runs lane-wide, so it also
//!    returns epochs created since; a periodic LIST with delimiter catches
//!    one that sorts earlier (a producer whose clock stepped back).
//! 3. **Slots.** HEAD each consecutive slot from the checkpoint: data (its
//!    content key and committed row count) or a tombstone (the epoch is
//!    closed there). The scan stops at the first free slot. If a later slot
//!    is listed (a gap) it never skips it and never tombstones it. A
//!    superseded epoch whose head stays free for `quiet_ms` gets a
//!    create-only tombstone race.
//! 4. **Ingest,** per table, across lanes: one projection check for all
//!    content keys; present ones are skipped (a copy in another epoch, or a
//!    retry); absent ones are grouped into statements (`plan::group`); each
//!    statement starts only inside every contributing lease's window and
//!    carries the server-side fence. After it, the same check verifies;
//!    missing objects are inserted again one by one, partial ones get a
//!    row repair. The series lane has no check: re-inserting is harmless.
//! 5. **Checkpoints.** Per lane, the position moves past the slots that are
//!    done, in order, and a tombstone at the new position closes the epoch;
//!    the write is a CAS. A failed CAS means another worker took the lane:
//!    it is dropped at once.

use super::bucket::{Bucket, Cond, Put};
use super::coord::{self, CkptDoc, Held, Lane, LeaseDoc, Observer, Timing, join};
use super::plan::{self, Found, Limits, Obj, Verdict};
use super::sql::{Central, Fence, LaneKind};
use bytes::Bytes;
use otap_s3pq::proto;
use serde::Serialize;
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::rc::Rc;

pub trait Clock {
    fn mono(&self) -> u64;
    fn wall(&self) -> u64;
}

pub struct RealClock;
impl Clock for RealClock {
    fn mono(&self) -> u64 {
        super::mono_ms()
    }
    fn wall(&self) -> u64 {
        super::wall_ms()
    }
}

/// A test clock: monotonic = wall = the cell, moved by hand.
#[derive(Clone, Default)]
pub struct FakeClock(pub Rc<Cell<u64>>);
impl Clock for FakeClock {
    fn mono(&self) -> u64 {
        self.0.get()
    }
    fn wall(&self) -> u64 {
        self.0.get()
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    /// The data root: lanes are `{root}/{producer}/{signal}` (depth 2) or `{root}/{signal}` (1).
    pub root: String,
    pub depth: usize,
    /// The control prefix: leases, checkpoints, heartbeats, GC state.
    pub ctl: String,
    /// Only these signals (empty: every known one).
    pub signals: Vec<String>,
    pub worker: String,
    pub timing: Timing,
    pub discover_ms: u64,
    pub full_list_ms: u64,
    pub quiet_ms: u64,
    pub limits: Limits,
    /// HEADs per lane per step (bounds a step after a long outage).
    pub max_heads: usize,
    pub max_lanes: usize,
    /// Take every free lane regardless of other workers (a one-shot run).
    pub solo: bool,
    pub verbose: bool,
}

impl Config {
    pub fn new(root: &str, ctl: &str, worker: &str) -> Self {
        Config {
            root: root.trim_matches('/').into(),
            depth: 2,
            ctl: ctl.trim_matches('/').into(),
            signals: Vec::new(),
            worker: worker.into(),
            timing: Timing { ttl_ms: 30_000, margin_ms: 2_000, budget_ms: 10_000, mutation: coord::Mutation::None },
            discover_ms: 2_000,
            full_list_ms: 30_000,
            quiet_ms: 30_000,
            limits: Limits::default(),
            max_heads: 256,
            max_lanes: usize::MAX,
            solo: false,
            verbose: false,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Stats {
    pub steps: u64,
    pub objects_inserted: u64,
    pub rows_inserted: u64,
    pub series_objects_inserted: u64,
    pub dedup_skipped: u64,
    pub statements: u64,
    pub statement_objects: u64,
    pub insert_errors: u64,
    pub retried_missing: u64,
    pub repaired_partial: u64,
    pub over_count: u64,
    pub fenced_by_server: u64,
    pub deferred_by_lease: u64,
    pub checks: u64,
    pub tombstones_won: u64,
    pub tombstones_lost_to_data: u64,
    pub epochs_closed: u64,
    pub gaps_seen: u64,
    pub head_missing: u64,
    pub lanes_taken: u64,
    pub lanes_released: u64,
    pub lanes_lapsed: u64,
    pub lanes_lost_cas: u64,
    pub renewals: u64,
    pub ckpt_writes: u64,
    pub errors: u64,
    /// Receive (edge) to insert returned, ms.
    #[serde(skip)]
    pub visible_ms: Vec<f64>,
}

type SlotId = (String, String, u64);

struct LaneState {
    lane: Lane,
    held: Held,
    ckpt: CkptDoc,
    ckpt_etag: String,
    known: BTreeSet<String>,
    last_full: Option<u64>,
    /// Per epoch: when a new slot (or the epoch) was last seen.
    last_seen: HashMap<String, u64>,
}

/// One epoch's work this step.
struct EpochWork {
    lane: String,
    epoch: String,
    next: u64,
    data: Vec<u64>,
    tomb: Option<u64>,
}

pub struct Worker<B: Bucket, C: Central, K: Clock> {
    pub cfg: Config,
    pub bucket: Rc<B>,
    pub central: Rc<C>,
    pub clock: K,
    pub stats: Stats,
    lanes: BTreeMap<String, Lane>,
    held: BTreeMap<String, LaneState>,
    obs: Observer,
    lease_etags: HashMap<String, String>,
    workers_obs: Observer,
    live_workers: usize,
    last_discover: Option<u64>,
    ensured: HashSet<String>,
    beat: u64,
    logged_gaps: HashSet<SlotId>,
}

fn log(cfg: &Config, msg: &str) {
    otap_s3pq::log(&format!("consumer {}: {msg}", cfg.worker));
}

impl<B: Bucket, C: Central, K: Clock> Worker<B, C, K> {
    pub fn new(cfg: Config, bucket: Rc<B>, central: Rc<C>, clock: K) -> Self {
        Worker {
            cfg,
            bucket,
            central,
            clock,
            stats: Stats::default(),
            lanes: BTreeMap::new(),
            held: BTreeMap::new(),
            obs: Observer::default(),
            lease_etags: HashMap::new(),
            workers_obs: Observer::default(),
            live_workers: 1,
            last_discover: None,
            ensured: HashSet::new(),
            beat: 0,
            logged_gaps: HashSet::new(),
        }
    }

    pub fn held_lanes(&self) -> Vec<String> {
        self.held.keys().cloned().collect()
    }

    pub fn checkpoint(&self, lane: &str) -> Option<&CkptDoc> {
        self.held.get(lane).map(|l| &l.ckpt)
    }

    /// One poll. Returns whether any slot was consumed or any epoch closed.
    pub async fn step(&mut self) -> bool {
        self.stats.steps += 1;
        self.maintain().await;
        let now = self.clock.mono();
        if self.last_discover.is_none_or(|t| now >= t + self.cfg.discover_ms) {
            self.discover().await;
            self.last_discover = Some(now);
            self.balance().await;
        }
        let ids: Vec<String> = self.held.keys().cloned().collect();
        let mut objs = Vec::new();
        let mut work = Vec::new();
        for id in &ids {
            if let Err(e) = self.scan(id, &mut objs, &mut work).await {
                self.stats.errors += 1;
                log(&self.cfg, &format!("scan {id}: {e}"));
            }
        }
        let done = self.ingest(objs).await;
        let mut progressed = false;
        for id in &ids {
            progressed |= self.advance(id, &work, &done).await;
        }
        progressed
    }

    // ---- leases ------------------------------------------------------------------

    async fn maintain(&mut self) {
        let t = self.cfg.timing;
        let ids: Vec<String> = self.held.keys().cloned().collect();
        for id in ids {
            let now = self.clock.mono();
            let h = self.held[&id].held.clone();
            if h.lapsed(now, &t) {
                self.stats.lanes_lapsed += 1;
                log(&self.cfg, &format!("lease {id} lapsed by our clock (epoch {}): dropping it", h.doc.epoch));
                let _ = self.held.remove(&id);
                continue;
            }
            if !h.renew_due(now) {
                continue;
            }
            let lane = self.held[&id].lane.clone();
            let doc = coord::renew(&h.doc, self.clock.wall());
            match self.write_lease(&lane, &doc, Some(&h.etag)).await {
                Some(held) => {
                    self.stats.renewals += 1;
                    if let Some(ls) = self.held.get_mut(&id) {
                        ls.held = held;
                    }
                }
                None => {
                    // Taken over (or unresolvable): stop at once. A lost answer
                    // that actually applied is harmless: the lease expires.
                    self.stats.lanes_lost_cas += 1;
                    log(&self.cfg, &format!("lease {id}: renewal failed, dropping the lane"));
                    let _ = self.held.remove(&id);
                }
            }
        }
    }

    /// PUT a lease doc (If-Match `etag`, or create), resolving a lost answer
    /// by reading it back. The window starts when the PUT was sent.
    async fn write_lease(&self, lane: &Lane, doc: &LeaseDoc, etag: Option<&str>) -> Option<Held> {
        let key = lane.lease_key(&self.cfg.ctl);
        let body = Bytes::from(serde_json::to_vec(doc).expect("lease json"));
        let (sent_ms, sent_wall_ms) = (self.clock.mono(), self.clock.wall());
        let cond = etag.map_or(Cond::Create, Cond::IfMatch);
        let etag = match self.bucket.put(&key, body, cond, &BTreeMap::new()).await {
            Put::Ok(e) => e,
            Put::Conflict => return None,
            Put::Unknown(_) => match self.bucket.get(&key).await {
                Ok(Some((b, e))) if serde_json::from_slice::<LeaseDoc>(&b).ok().as_ref() == Some(doc) => e,
                _ => return None,
            },
        };
        Some(Held { doc: doc.clone(), etag, sent_ms, sent_wall_ms })
    }

    async fn discover(&mut self) {
        let b = self.bucket.clone();
        // lanes
        let mut lanes = BTreeMap::new();
        let producers: Vec<String> = if self.cfg.depth >= 2 {
            match b.list_dirs(&self.cfg.root).await {
                Ok(p) => p.into_iter().filter(|p| !p.starts_with('_')).collect(),
                Err(e) => {
                    self.stats.errors += 1;
                    log(&self.cfg, &format!("discover: {e}"));
                    return;
                }
            }
        } else {
            vec![String::new()]
        };
        for p in producers {
            let dir = if p.is_empty() { self.cfg.root.clone() } else { join(&self.cfg.root, &p) };
            match b.list_dirs(&dir).await {
                Ok(sigs) => {
                    for s in sigs {
                        if s.starts_with('_') || LaneKind::for_signal(&s).is_none() {
                            continue;
                        }
                        if !self.cfg.signals.is_empty() && !self.cfg.signals.contains(&s) {
                            continue;
                        }
                        let l = Lane { producer: p.clone(), signal: s };
                        let _ = lanes.insert(l.id(), l);
                    }
                }
                Err(e) => {
                    self.stats.errors += 1;
                    log(&self.cfg, &format!("discover {dir}: {e}"));
                }
            }
        }
        self.lanes = lanes;
        // heartbeat, and who else is alive
        self.beat += 1;
        let hb = serde_json::json!({"worker": self.cfg.worker, "beat": self.beat, "wall_ms": self.clock.wall()});
        let wprefix = join(&self.cfg.ctl, "workers");
        let _ = b.put(&format!("{wprefix}/{}.json", self.cfg.worker), Bytes::from(hb.to_string()), Cond::None, &BTreeMap::new()).await;
        let now = self.clock.mono();
        let t = self.cfg.timing;
        if let Ok(items) = b.list(&wprefix, None).await {
            let mut live = 0;
            let mut dead = Vec::new();
            let wall = self.clock.wall();
            for it in items {
                let id = it.key.clone();
                let etag = it.etag.clone().unwrap_or_default();
                self.workers_obs.observe(&id, Some(&etag), now);
                let quiet = self.workers_obs.unchanged_for(&id, &etag, now).unwrap_or(0);
                // (LastModified is the store's clock: good enough to discount a
                // long-dead heartbeat at once. This only balances load.)
                let stale = wall.saturating_sub(it.modified_ms) > 3 * t.ttl_ms;
                if id.ends_with(&format!("/{}.json", self.cfg.worker)) || (quiet < t.ttl_ms + t.margin_ms && !stale) {
                    live += 1;
                } else if quiet > 10 * t.ttl_ms || wall.saturating_sub(it.modified_ms) > 10 * t.ttl_ms {
                    dead.push(id);
                }
            }
            self.live_workers = if self.cfg.solo { 1 } else { live.max(1) };
            let _ = b.delete(&dead).await;
        }
        // leases
        let lprefix = join(&self.cfg.ctl, "lease");
        if let Ok(items) = b.list(&lprefix, None).await {
            let mut m = HashMap::new();
            for it in items {
                if let Some(l) = Lane::from_ctl_key(&lprefix, &it.key) {
                    let _ = m.insert(l.id(), it.etag.unwrap_or_default());
                }
            }
            for id in self.lanes.keys() {
                self.obs.observe(id, m.get(id).map(String::as_str), now);
            }
            self.lease_etags = m;
        }
    }

    async fn balance(&mut self) {
        let share = coord::fair_share(self.lanes.len(), self.live_workers).min(self.cfg.max_lanes);
        // Above the fair share: give one lane back per round (hysteresis).
        if self.held.len() > share {
            if let Some(id) = self.held.keys().next_back().cloned() {
                let ls = self.held.remove(&id).expect("held");
                let doc = coord::release(&ls.held.doc, self.clock.wall());
                let _ = self.write_lease(&ls.lane, &doc, Some(&ls.held.etag)).await;
                self.stats.lanes_released += 1;
                if self.cfg.verbose {
                    log(&self.cfg, &format!("released {id} (share {share}, workers {})", self.live_workers));
                }
            }
            return;
        }
        let now = self.clock.mono();
        let t = self.cfg.timing;
        let candidates: Vec<String> = self.lanes.keys().filter(|id| !self.held.contains_key(*id)).cloned().collect();
        for id in candidates {
            if self.held.len() >= share {
                break;
            }
            let lane = self.lanes[&id].clone();
            let prev = match self.lease_etags.get(&id) {
                None => None,
                Some(_) => {
                    // Below the share only: read it (it may be released, or
                    // expired by our observation of its ETag).
                    match self.bucket.get(&lane.lease_key(&self.cfg.ctl)).await {
                        Ok(Some((body, etag))) => {
                            self.obs.observe(&id, Some(&etag), now);
                            let Ok(doc) = serde_json::from_slice::<LeaseDoc>(&body) else { continue };
                            // (A lease this worker let lapse still names it as
                            // owner: it is taken again like anyone else's, after
                            // expiry. Skipping "our own" leases here orphaned
                            // them while this worker lived: the soak's finding.)
                            if !self.obs.may_take(&id, &etag, &doc, now, t.margin_ms) {
                                continue;
                            }
                            Some((doc, etag))
                        }
                        Ok(None) => None,
                        Err(_) => continue,
                    }
                }
            };
            let doc = coord::take(&id, prev.as_ref().map(|p| &p.0), &self.cfg.worker, t.ttl_ms, self.clock.wall());
            let Some(held) = self.write_lease(&lane, &doc, prev.as_ref().map(|p| p.1.as_str())).await else { continue };
            // Fence the checkpoint: rewrite it under our lease epoch.
            match self.fence_checkpoint(&lane, doc.epoch).await {
                Some((ck, etag)) => {
                    self.stats.lanes_taken += 1;
                    if self.cfg.verbose {
                        log(&self.cfg, &format!("took {id} (lease epoch {}, share {share})", doc.epoch));
                    }
                    let known = ck.epochs.keys().cloned().collect();
                    let _ = self.held.insert(
                        id.clone(),
                        LaneState { lane, held, ckpt: ck, ckpt_etag: etag, known, last_full: None, last_seen: HashMap::new() },
                    );
                }
                None => {
                    let rel = coord::release(&doc, self.clock.wall());
                    let _ = self.write_lease(&lane, &rel, Some(&held.etag)).await;
                }
            }
        }
    }

    async fn fence_checkpoint(&self, lane: &Lane, lease_epoch: u64) -> Option<(CkptDoc, String)> {
        let key = lane.ckpt_key(&self.cfg.ctl);
        for _ in 0..3 {
            let (cur, etag) = match self.bucket.get(&key).await {
                Ok(Some((b, e))) => (serde_json::from_slice::<CkptDoc>(&b).ok()?, Some(e)),
                Ok(None) => (CkptDoc::new(&lane.id()), None),
                Err(_) => return None,
            };
            let new = cur.bumped(lease_epoch);
            if let Some(e) = self.write_ckpt(&key, &new, etag.as_deref()).await {
                return Some((new, e));
            }
        }
        None
    }

    async fn write_ckpt(&self, key: &str, doc: &CkptDoc, etag: Option<&str>) -> Option<String> {
        let body = Bytes::from(serde_json::to_vec(doc).expect("ckpt json"));
        match self.bucket.put(key, body, etag.map_or(Cond::Create, Cond::IfMatch), &BTreeMap::new()).await {
            Put::Ok(e) => Some(e),
            Put::Conflict => None,
            Put::Unknown(_) => match self.bucket.get(key).await {
                Ok(Some((b, e))) if serde_json::from_slice::<CkptDoc>(&b).ok().as_ref() == Some(doc) => Some(e),
                _ => None,
            },
        }
    }

    /// Gives every lane back and removes the heartbeat (a graceful stop).
    pub async fn release_all(&mut self) {
        let hb = format!("{}/{}.json", join(&self.cfg.ctl, "workers"), self.cfg.worker);
        let _ = self.bucket.delete(&[hb]).await;
        let ids: Vec<String> = self.held.keys().cloned().collect();
        for id in ids {
            let ls = self.held.remove(&id).expect("held");
            let doc = coord::release(&ls.held.doc, self.clock.wall());
            let _ = self.write_lease(&ls.lane, &doc, Some(&ls.held.etag)).await;
        }
    }

    // ---- discovery and slots ---------------------------------------------------------

    async fn scan(&mut self, id: &str, objs: &mut Vec<Obj>, work: &mut Vec<EpochWork>) -> Result<(), String> {
        let b = self.bucket.clone();
        let cfg = self.cfg.clone();
        let now = self.clock.mono();
        let ls = self.held.get_mut(id).expect("held");
        let prefix = ls.lane.data_prefix(&cfg.root);
        if ls.last_full.is_none_or(|t| now >= t + cfg.full_list_ms) {
            for e in b.list_dirs(&prefix).await? {
                let _ = ls.known.insert(e);
            }
            ls.last_full = Some(now);
        }
        let newest = ls.known.iter().next_back().cloned();
        let mut listed: BTreeMap<String, Vec<plan::Listed>> = BTreeMap::new();
        let parse = |items: Vec<super::bucket::Item>, listed: &mut BTreeMap<String, Vec<plan::Listed>>| {
            for it in items {
                if let Some((e, s)) = proto::parse_slot_key(&prefix, &it.key) {
                    listed.entry(e).or_default().push((s, it.key, it.size));
                }
            }
        };
        // StartAfter the checkpoint's previous slot; for slot 0, `{epoch}/0`,
        // which sorts before every slot key. (Not `{epoch}/`: SeaweedFS takes
        // a StartAfter naming a directory as "after that whole directory".)
        let after = |e: &str, next: u64| -> String {
            match next.checked_sub(1) {
                Some(s) => proto::slot_key(&prefix, e, s),
                None => format!("{}/0", join(&prefix, e)),
            }
        };
        for e in ls.known.iter().filter(|e| !ls.ckpt.closed(e) && Some(*e) != newest.as_ref()) {
            let items = b.list(&join(&prefix, e), Some(&after(e, ls.ckpt.next(e)))).await?;
            parse(items, &mut listed);
        }
        // The newest epoch, lane-wide: its new slots plus any newer epochs.
        let sa = newest.as_ref().map(|e| {
            if ls.ckpt.closed(e) { proto::slot_key(&prefix, e, ls.ckpt.next(e)) } else { after(e, ls.ckpt.next(e)) }
        });
        let items = b.list(&prefix, sa.as_deref()).await?;
        if std::env::var_os("OTAPRS_CONSUMER_DEBUG").is_some() {
            log(&cfg, &format!("scan {id}: prefix {prefix} after {sa:?}: {} keys, first {:?}", items.len(), items.first().map(|i| &i.key)));
        }
        parse(items, &mut listed);
        for e in listed.keys() {
            let _ = ls.known.insert(e.clone());
        }
        let newest = ls.known.iter().next_back().cloned();
        let open: Vec<String> = ls.known.iter().filter(|e| !ls.ckpt.closed(e)).cloned().collect();
        let mut heads = 0;
        for e in open {
            let next = ls.ckpt.next(&e);
            let sc = plan::scan_epoch(&e, next, listed.get(&e).map(Vec::as_slice).unwrap_or(&[]));
            let seen = ls.last_seen.entry(e.clone()).or_insert(now);
            if !sc.run.is_empty() {
                *seen = now;
            }
            let quiet_for = now.saturating_sub(*seen);
            if let Some(g) = sc.gap_then {
                if self.logged_gaps.insert((id.to_string(), e.clone(), sc.head())) {
                    self.stats.gaps_seen += 1;
                    log(&cfg, &format!("gap: {id}/{e} slot {} is free but slot {g} is listed; waiting (never skipped, never tombstoned)", sc.head()));
                }
            }
            let mut w = EpochWork { lane: id.to_string(), epoch: e.clone(), next, data: Vec::new(), tomb: None };
            for (seq, key, size) in &sc.run {
                if heads >= cfg.max_heads {
                    break;
                }
                heads += 1;
                match b.head(key).await? {
                    None => {
                        self.stats.head_missing += 1;
                        break;
                    }
                    Some(meta) => match plan::found(&meta) {
                        Found::Tomb => {
                            w.tomb = Some(*seq);
                            break;
                        }
                        Found::Data { content, rows, received_ns } => {
                            w.data.push(*seq);
                            objs.push(Obj {
                                lane: id.to_string(),
                                epoch: e.clone(),
                                seq: *seq,
                                key: key.clone(),
                                size: *size,
                                content,
                                rows,
                                received_ns,
                            });
                        }
                    },
                }
            }
            if sc.run.is_empty() && plan::may_tomb(&sc, newest.as_ref() == Some(&e), quiet_for, cfg.quiet_ms) {
                match tombstone(&*b, &prefix, &e, next).await {
                    TombResult::Closed(won) => {
                        if won {
                            self.stats.tombstones_won += 1;
                        }
                        w.tomb = Some(next);
                        if cfg.verbose {
                            log(&cfg, &format!("closing {id}/{e} at slot {next} (tombstone{})", if won { ", ours" } else { "" }));
                        }
                    }
                    TombResult::LostToData => {
                        self.stats.tombstones_lost_to_data += 1;
                        log(&cfg, &format!("tombstone {id}/{e}/{next} lost to a late batch: ingesting it next"));
                    }
                    TombResult::Unresolved(why) => {
                        self.stats.errors += 1;
                        log(&cfg, &format!("tombstone {id}/{e}/{next}: {why}"));
                    }
                }
            }
            if !w.data.is_empty() || w.tomb.is_some() {
                work.push(w);
            }
        }
        Ok(())
    }

    // ---- ingest ------------------------------------------------------------------

    async fn ensure(&mut self, k: &LaneKind) -> bool {
        if self.ensured.contains(&k.signal) {
            return true;
        }
        match self.central.ensure(k).await {
            Ok(()) => {
                let _ = self.ensured.insert(k.signal.clone());
                true
            }
            Err(e) => {
                self.stats.errors += 1;
                log(&self.cfg, &format!("create table for {}: {e}", k.signal));
                false
            }
        }
    }

    async fn ingest(&mut self, objs: Vec<Obj>) -> HashSet<SlotId> {
        let mut done: HashSet<SlotId> = HashSet::new();
        let mut by: BTreeMap<String, Vec<Obj>> = BTreeMap::new();
        for o in objs {
            let sig = self.held.get(&o.lane).map(|l| l.lane.signal.clone()).unwrap_or_default();
            by.entry(sig).or_default().push(o);
        }
        for (sig, list) in by {
            let Some(k) = LaneKind::for_signal(&sig) else { continue };
            if !self.ensure(&k).await {
                continue;
            }
            if !k.counted {
                for g in plan::group(list, &self.cfg.limits) {
                    self.maintain().await;
                    let refs: Vec<&Obj> = g.iter().collect();
                    let Some(fence) = self.window(&refs) else { continue };
                    let keys: Vec<&str> = g.iter().map(|o| o.key.as_str()).collect();
                    let token = plan::token(&k.signal, &keys);
                    self.stats.statements += 1;
                    self.stats.statement_objects += g.len() as u64;
                    match self.central.insert(&k, &refs, fence, &token).await {
                        Ok(()) if self.clock.wall() <= fence.wall_ms + fence.budget_ms => {
                            for o in &g {
                                self.stats.series_objects_inserted += 1;
                                let _ = done.insert((o.lane.clone(), o.epoch.clone(), o.seq));
                            }
                        }
                        // Answered after the fence could have dropped it: not
                        // known to have landed; the next holder re-inserts
                        // (harmless for this table).
                        Ok(()) => self.stats.fenced_by_server += g.len() as u64,
                        Err(e) => {
                            self.stats.insert_errors += 1;
                            log(&self.cfg, &format!("insert {}: {e}", k.signal));
                        }
                    }
                }
                continue;
            }
            // Every slot's content, for mapping results back to slots.
            let slots: Vec<(SlotId, String)> =
                list.iter().map(|o| ((o.lane.clone(), o.epoch.clone(), o.seq), o.content.clone())).collect();
            // One object per content key: a copy in another epoch rides on it.
            let mut primary: Vec<Obj> = Vec::new();
            let mut seen: HashSet<String> = HashSet::new();
            let mut copies = 0u64;
            for o in list {
                if seen.insert(o.content.clone()) {
                    primary.push(o)
                } else {
                    copies += 1
                }
            }
            let contents: Vec<&str> = primary.iter().map(|o| o.content.as_str()).collect();
            self.stats.checks += 1;
            let pre = match self.central.counts(&k, &contents).await {
                Ok(m) => m,
                Err(e) => {
                    self.stats.errors += 1;
                    log(&self.cfg, &format!("check {}: {e}", k.signal));
                    continue;
                }
            };
            let mut ok: HashSet<String> = HashSet::new();
            let mut absent = Vec::new();
            for o in primary {
                match plan::verdict(o.rows, pre.get(&o.content).copied().unwrap_or(0)) {
                    Verdict::Present => {
                        self.stats.dedup_skipped += 1;
                        let _ = ok.insert(o.content.clone());
                    }
                    Verdict::Over(h) => {
                        self.stats.over_count += 1;
                        log(&self.cfg, &format!("{} holds {h} rows of {} (committed {}): a duplicate got in", k.table, o.content, o.rows));
                        let _ = ok.insert(o.content.clone());
                    }
                    Verdict::Partial(h) => {
                        if self.repair(&k, &o, h).await {
                            let _ = ok.insert(o.content.clone());
                        }
                    }
                    Verdict::Absent => absent.push(o),
                }
            }
            for g in plan::group(absent, &self.cfg.limits) {
                self.insert_group(&k, g, &mut ok).await;
            }
            self.stats.dedup_skipped += copies;
            for (slot, c) in slots {
                if ok.contains(&c) {
                    let _ = done.insert(slot);
                }
            }
        }
        done
    }

    /// The fence for a statement over these objects, if every contributing
    /// lease can cover it (start now, finish by `budget`, inside the window).
    fn window(&mut self, objs: &[&Obj]) -> Option<Fence> {
        let t = self.cfg.timing;
        let now = self.clock.mono();
        let mut fence = u64::MAX;
        for o in objs {
            match self.held.get(&o.lane) {
                Some(ls) if ls.held.may_start(now, &t) => fence = fence.min(ls.held.fence_wall_ms(&t)),
                _ => {
                    self.stats.deferred_by_lease += objs.len() as u64;
                    return None;
                }
            }
        }
        Some(Fence { wall_ms: fence, budget_ms: t.budget_ms })
    }

    /// One statement for a group of absent objects, then the verify: what is
    /// complete is done; what is missing goes again alone; what is partial
    /// gets a row repair.
    async fn insert_group(&mut self, k: &LaneKind, g: Vec<Obj>, ok: &mut HashSet<String>) {
        // A long step must not starve the renewals.
        self.maintain().await;
        // Only objects whose lease can cover the statement.
        let t = self.cfg.timing;
        let now = self.clock.mono();
        let (g, late): (Vec<Obj>, Vec<Obj>) =
            g.into_iter().partition(|o| self.held.get(&o.lane).is_some_and(|l| l.held.may_start(now, &t)));
        self.stats.deferred_by_lease += late.len() as u64;
        if g.is_empty() {
            return;
        }
        let refs: Vec<&Obj> = g.iter().collect();
        let Some(fence) = self.window(&refs) else { return };
        let keys: Vec<&str> = g.iter().map(|o| o.key.as_str()).collect();
        let token = plan::token(&k.signal, &keys);
        self.stats.statements += 1;
        self.stats.statement_objects += g.len() as u64;
        let res = self.central.insert(k, &refs, fence, &token).await;
        if let Err(e) = &res {
            self.stats.insert_errors += 1;
            log(&self.cfg, &format!("insert {} ({} objects): {e}; verifying", k.signal, g.len()));
        }
        if self.cfg.timing.mutation == coord::Mutation::NoVerify {
            for o in &g {
                self.inserted(o, ok);
            }
            return;
        }
        let after = match self.central.counts(k, &g.iter().map(|o| o.content.as_str()).collect::<Vec<_>>()).await {
            Ok(m) => m,
            Err(e) => {
                self.stats.errors += 1;
                log(&self.cfg, &format!("verify {}: {e}", k.signal));
                return;
            }
        };
        self.stats.checks += 1;
        let mut missing = Vec::new();
        for o in g {
            match plan::verdict(o.rows, after.get(&o.content).copied().unwrap_or(0)) {
                Verdict::Present => self.inserted(&o, ok),
                Verdict::Over(h) => {
                    self.stats.over_count += 1;
                    log(&self.cfg, &format!("{} holds {h} rows of {} (committed {}) after insert", k.table, o.content, o.rows));
                    let _ = ok.insert(o.content.clone());
                }
                Verdict::Partial(h) => {
                    if self.repair(k, &o, h).await {
                        self.inserted(&o, ok);
                    }
                }
                Verdict::Absent => missing.push(o),
            }
        }
        if missing.is_empty() {
            return;
        }
        if res.is_ok() && self.clock.wall() > fence.wall_ms {
            // The statement reached the server after its fence: a no-op by design.
            self.stats.fenced_by_server += missing.len() as u64;
            log(&self.cfg, &format!("{} objects fenced by the server (statement sent after the lease window)", missing.len()));
            return;
        }
        // Repair only the missing batches: each alone, then verify.
        for o in missing {
            let Some(fence) = self.window(&[&o]) else { return };
            let token = format!("{}/retry", plan::token(&k.signal, &[&o.key]));
            self.stats.statements += 1;
            self.stats.statement_objects += 1;
            self.stats.retried_missing += 1;
            if let Err(e) = self.central.insert(k, &[&o], fence, &token).await {
                self.stats.insert_errors += 1;
                log(&self.cfg, &format!("retry {}: {e}", o.key));
            }
            self.stats.checks += 1;
            if let Ok(m) = self.central.counts(k, &[&o.content]).await {
                if plan::verdict(o.rows, m.get(&o.content).copied().unwrap_or(0)) == Verdict::Present {
                    self.inserted(&o, ok);
                }
            }
        }
    }

    fn inserted(&mut self, o: &Obj, ok: &mut HashSet<String>) {
        self.stats.objects_inserted += 1;
        self.stats.rows_inserted += o.rows;
        if o.received_ns > 0 && self.stats.visible_ms.len() < 1_000_000 {
            self.stats.visible_ms.push(self.clock.wall() as f64 - o.received_ns as f64 / 1e6);
        }
        let _ = ok.insert(o.content.clone());
    }

    /// Inserts the missing row ordinals of a partial batch, then verifies.
    async fn repair(&mut self, k: &LaneKind, o: &Obj, have: u64) -> bool {
        let Some(fence) = self.window(&[o]) else { return false };
        self.stats.repaired_partial += 1;
        self.stats.statements += 1;
        let token = format!("{}/repair/{have}", o.content);
        log(&self.cfg, &format!("{} holds {have} of {} rows of {}: repairing", k.table, o.rows, o.content));
        if let Err(e) = self.central.repair(k, o, fence, &token).await {
            self.stats.insert_errors += 1;
            log(&self.cfg, &format!("repair {}: {e}", o.key));
        }
        self.stats.checks += 1;
        matches!(self.central.counts(k, &[&o.content]).await, Ok(m) if m.get(&o.content).copied() == Some(o.rows))
    }

    // ---- checkpoints ---------------------------------------------------------------

    async fn advance(&mut self, id: &str, work: &[EpochWork], done: &HashSet<SlotId>) -> bool {
        let Some(ls) = self.held.get(id) else { return false };
        let mut doc = ls.ckpt.clone();
        let mut changed = false;
        let mut closed = 0;
        for w in work.iter().filter(|w| w.lane == id) {
            let n = plan::advance_to(w.next, &w.data, |s| done.contains(&(id.to_string(), w.epoch.clone(), s)));
            if n > doc.next(&w.epoch) {
                doc.advance(&w.epoch, n);
                changed = true;
            }
            if w.tomb == Some(n) && !doc.closed(&w.epoch) {
                doc.close(&w.epoch, n);
                closed += 1;
                changed = true;
            }
        }
        if !changed {
            return false;
        }
        let new = doc.bumped(ls.held.doc.epoch);
        let key = ls.lane.ckpt_key(&self.cfg.ctl);
        let etag = ls.ckpt_etag.clone();
        self.stats.ckpt_writes += 1;
        match self.write_ckpt(&key, &new, Some(&etag)).await {
            Some(e) => {
                self.stats.epochs_closed += closed;
                let ls = self.held.get_mut(id).expect("held");
                ls.ckpt = new;
                ls.ckpt_etag = e;
                true
            }
            None => {
                self.stats.lanes_lost_cas += 1;
                log(&self.cfg, &format!("checkpoint {id}: CAS failed (another worker took the lane); dropping it"));
                let _ = self.held.remove(id);
                false
            }
        }
    }

    pub fn stats_json(&self) -> serde_json::Value {
        let mut v = serde_json::to_value(&self.stats).expect("stats");
        let mut lat = self.stats.visible_ms.clone();
        lat.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let pct = |p: f64| lat.get(((lat.len() as f64 - 1.0) * p).round() as usize).copied().unwrap_or(0.0);
        let m = v.as_object_mut().expect("object");
        let _ = m.insert("worker".into(), self.cfg.worker.clone().into());
        let _ = m.insert("held".into(), self.held_lanes().into());
        let _ = m.insert("lanes_known".into(), self.lanes.len().into());
        let _ = m.insert("live_workers".into(), self.live_workers.into());
        let _ = m.insert("s3".into(), serde_json::to_value(self.bucket.counts().snap()).expect("counts"));
        let _ = m.insert("visible_n".into(), lat.len().into());
        let _ = m.insert("visible_ms_p50".into(), pct(0.5).into());
        let _ = m.insert("visible_ms_p90".into(), pct(0.9).into());
        let _ = m.insert("visible_ms_p99".into(), pct(0.99).into());
        let _ = m.insert("visible_ms_max".into(), pct(1.0).into());
        v
    }
}

pub enum TombResult {
    /// The epoch is closed at the slot (true: our PUT won; false: a tombstone was already there).
    Closed(bool),
    /// A late batch got the slot first: ingest it.
    LostToData,
    Unresolved(String),
}

/// Races a create-only tombstone into `{prefix}/{epoch}/{seq}`.
pub async fn tombstone<B: Bucket + ?Sized>(b: &B, prefix: &str, epoch: &str, seq: u64) -> TombResult {
    let key = proto::slot_key(prefix, epoch, seq);
    let mut meta = BTreeMap::new();
    let _ = meta.insert(proto::META_KIND.to_string(), proto::KIND_TOMB.to_string());
    let _ = meta.insert(proto::META_EPOCH.to_string(), epoch.to_string());
    for _ in 0..3 {
        let conflict = match b.put(&key, Bytes::new(), Cond::Create, &meta).await {
            Put::Ok(_) => return TombResult::Closed(true),
            Put::Conflict => true,
            Put::Unknown(_) => false,
        };
        match b.head(&key).await {
            Ok(Some(m)) => {
                return match plan::found(&m) {
                    // Ours landed (answer lost), or another worker's.
                    Found::Tomb => TombResult::Closed(!conflict),
                    Found::Data { .. } => TombResult::LostToData,
                };
            }
            Ok(None) if conflict => return TombResult::Unresolved("412, then the HEAD found nothing".into()),
            Ok(None) => continue, // not landed: send it again
            Err(e) => return TombResult::Unresolved(e),
        }
    }
    TombResult::Unresolved("no answer".into())
}
