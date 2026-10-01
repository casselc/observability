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
//!    returns epochs created since; a periodic full LIST of the lane from
//!    its checkpoint's floor (StartAfter `{floor}/~`) catches one that sorts
//!    earlier (a producer whose clock stepped back) and, after it, the
//!    checkpoint is compacted: epochs GC has retired leave it
//!    (`CkptDoc::compact`), so neither the checkpoint nor that LIST grows
//!    with the lane's history.
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
//!
//! For fleet scale (README "Consumer at fleet scale"):
//! - **Idle-lane backoff** (`discovery::Backoff`): a lane whose LIST found
//!   nothing to do is LISTed again after a doubling, jittered wait; any work
//!   found puts it back on every poll. **Hints** (`discovery::Hints`, e.g. S3
//!   event notifications) cut the wait short; LIST stays the truth.
//! - **Linger**: a table with fewer pending objects than a statement holds
//!   waits up to `linger_ms` (from the first object's HEAD) for more. HEADs
//!   are cached per slot, so waiting costs no second HEAD.
//! - **Load-based balancing** (`coord::pick_release`, `take_by_load`): lane
//!   weight = a base + recent rows/s, exchanged through the heartbeats.
//! - **The check's partition range** (`plan::own_range`, `check_range`):
//!   derived from the objects' own `received_at`, asserted row by row by the
//!   insert, widened by the copy horizon for the pre-check.
//! - **Unsettled statements**: an insert with no answer may still land until
//!   `Held::settled_by`; until then its lanes are neither checked, verified,
//!   retried nor released (the model's "a worker acts once its statement is
//!   gone").

use super::bucket::{Bucket, Cond, Put};
use super::coord::{self, CkptDoc, Ewma, Held, Lane, LeaseDoc, Mutation, Observer, Timing, join};
use super::discovery::{Backoff, Hints, Jitter};
use super::plan::{self, CheckRange, Found, Limits, Obj, Verdict};
use super::sql::{Central, Fence, InsertErr, LaneKind};
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
    /// The data root: lanes are `{root}/{cluster}/{producer}/{signal}`
    /// (depth 3, format v2, the default); `depth` is the number of key
    /// segments of a lane (2: `{root}/{producer}/{signal}`, 1:
    /// `{root}/{signal}`, kept for mechanism tests).
    pub root: String,
    pub depth: usize,
    /// The control prefix: leases, checkpoints, heartbeats, GC state.
    pub ctl: String,
    /// Only these signals (empty: every known one).
    pub signals: Vec<String>,
    pub worker: String,
    pub timing: Timing,
    pub discover_ms: u64,
    /// The poll period: a busy lane is LISTed once per poll, even when the
    /// worker wakes sooner (a linger ending, an idle lane's backoff ending).
    pub poll_ms: u64,
    pub full_list_ms: u64,
    pub quiet_ms: u64,
    pub limits: Limits,
    /// HEADs per lane per step (bounds a step after a long outage).
    pub max_heads: usize,
    pub max_lanes: usize,
    /// Take every free lane regardless of other workers (a one-shot run).
    pub solo: bool,
    pub verbose: bool,
    /// How often the lane directories are listed (one LIST per producer);
    /// leases and heartbeats are listed every `discover_ms`.
    pub lanes_every_ms: u64,
    /// Per-lane LIST backoff when a lane is idle.
    pub backoff: Backoff,
    /// Wait up to this long (from the oldest pending object's HEAD) for a
    /// table's statement to fill (0: off).
    pub linger_ms: u64,
    pub balance: Balance,
    /// The count check's copy horizon: None reads every partition; Some(h)
    /// reads the partitions of the objects' own `received_at` ± h. A copy of
    /// a request received more than h after its original is not found, and
    /// is ingested twice: `consume horizon-audit` reports such copies
    /// (`audit.rs`). Default 3 days (`DEFAULT_HORIZON_MS`).
    pub horizon_ms: Option<u64>,
    /// A retired lane's quarantine bound for objects of a later epoch is
    /// R minus this (ms): a later incarnation may run on a node whose clock
    /// is behind the closer's (`../../FORMAT.md` §3.1). The edges' clock
    /// skew, as the watermark's `--wm-skew`.
    pub quarantine_skew_ms: u64,
}

/// The default copy horizon of the count check (`--check-horizon`): 3 days.
/// A resend from an edge's durable buffer after an outage, or a sender's
/// retry, must be received within it of its original.
pub const DEFAULT_HORIZON_MS: u64 = 3 * 86_400_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BalanceMode {
    /// Fair share by lane count (⌈lanes / workers⌉).
    Count,
    /// By weight: a base per lane plus its recent rows/s.
    Load,
}

#[derive(Clone, Copy, Debug)]
pub struct Balance {
    pub mode: BalanceMode,
    /// Nothing moves while a worker's load is within target × (1 ± h).
    pub hysteresis: f64,
    /// A lane's weight = base + its recent rows/s.
    pub base_weight: f64,
    /// The rate's time constant.
    pub window_ms: u64,
    /// A lane isn't given back sooner than this after it was taken.
    pub min_hold_ms: u64,
    /// How often the other workers' loads are read (their heartbeats: one GET each).
    pub loads_every_ms: u64,
}

impl Default for Balance {
    fn default() -> Self {
        Balance { mode: BalanceMode::Load, hysteresis: 0.2, base_weight: 50.0, window_ms: 60_000, min_hold_ms: 30_000, loads_every_ms: 10_000 }
    }
}

impl Config {
    pub fn new(root: &str, ctl: &str, worker: &str) -> Self {
        Config {
            root: root.trim_matches('/').into(),
            depth: 3,
            ctl: ctl.trim_matches('/').into(),
            signals: Vec::new(),
            worker: worker.into(),
            timing: Timing::production(),
            discover_ms: 2_000,
            poll_ms: 1_000,
            full_list_ms: 30_000,
            quiet_ms: 30_000,
            limits: Limits::default(),
            max_heads: 256,
            max_lanes: usize::MAX,
            solo: false,
            verbose: false,
            lanes_every_ms: 30_000,
            backoff: Backoff::default(),
            linger_ms: 0,
            balance: Balance::default(),
            horizon_ms: Some(DEFAULT_HORIZON_MS),
            quarantine_skew_ms: 5_000,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Stats {
    pub steps: u64,
    pub objects_inserted: u64,
    pub rows_inserted: u64,
    pub series_objects_inserted: u64,
    /// Resource announcements: statements, objects whose announcements
    /// landed, and objects held back a round because their lane's did not
    /// surely land.
    pub announce_statements: u64,
    pub announce_objects: u64,
    pub announce_deferred: u64,
    /// Payload parts (DECISIONS.md D36): statements, objects whose payloads
    /// landed, objects held back a round because their lane's did not
    /// surely land; dangling checks and the references they found dangling.
    pub payload_statements: u64,
    pub payload_objects: u64,
    pub payload_deferred: u64,
    pub dangling_checks: u64,
    pub payload_dangling: u64,
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
    /// Checkpoint writes of ours that landed after reading back unchanged,
    /// taken back by `refresh_own_ckpt`.
    pub ckpt_late_taken: u64,
    /// Lease renewals of ours that landed after reading back unchanged,
    /// adopted (`refresh_own_lease`, or on the next renewal's 412).
    pub lease_late_taken: u64,
    /// Takes of ours that landed after reading back unchanged: adopted
    /// (`refresh_own_takes`, try_take's read, or a later take's 412), or,
    /// found after their window, given back at once (`take_late_released`).
    pub take_late_taken: u64,
    pub take_late_released: u64,
    /// Epochs dropped from checkpoints by compaction (retired by GC).
    pub epochs_compacted: u64,
    /// Full listings of a lane (from its floor).
    pub full_lists: u64,
    /// gc.json reads (for compaction).
    pub gc_reads: u64,
    /// The largest checkpoint this worker wrote: bytes, explicit epochs.
    pub ckpt_bytes_max: u64,
    pub ckpt_epochs_max: u64,
    pub errors: u64,
    /// LISTs of held lanes' data (not discovery), and polls a lane skipped
    /// because it was backing off.
    pub lane_lists: u64,
    pub lists_skipped: u64,
    /// Per lane: its data LISTs (for LISTs per lane per month).
    pub lists_by_lane: BTreeMap<String, u64>,
    /// Hints received, lanes they woke early, new lanes they named.
    pub hints: u64,
    pub hint_wakeups: u64,
    pub hint_new_lanes: u64,
    /// Objects held back a poll by the linger (counted per poll).
    pub linger_deferred: u64,
    /// Statements with no answer (unsettled), and objects held back while
    /// their lane had one.
    pub unsettled: u64,
    pub deferred_unsettled: u64,
    /// Checks restricted to a partition range, over every partition, and
    /// recounts over the horizon after a restricted verify fell short.
    pub range_checks: u64,
    pub full_checks: u64,
    pub range_recounts: u64,
    /// Inserts whose range assertion fired (their objects are checked over every partition from then on).
    pub range_guard_failures: u64,
    /// Heartbeat reads (the other workers' loads).
    pub load_reads: u64,
    /// Retirement (`../../FORMAT.md` §3.1): lanes this worker retired by
    /// their publisher's close, retired lanes it saw reborn (a later epoch),
    /// objects it quarantined (below a retired lane's R, not in central),
    /// objects below R it found already in central (copies: passed), and
    /// quarantine document writes that failed (the slots wait).
    pub lanes_retired: u64,
    pub lanes_reborn: u64,
    pub quarantined_objects: u64,
    pub quarantined_rows: u64,
    pub below_copies: u64,
    pub quarantine_errors: u64,
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
    /// Epochs above the checkpoint's floor this worker knows of.
    known: BTreeSet<String>,
    last_full: Option<u64>,
    /// A full listing ran since the last compaction.
    compact_due: bool,
    /// Per epoch: when a new slot (or the epoch) was last seen.
    last_seen: HashMap<String, u64>,
    /// The idle backoff: the current (unjittered) wait, 0 when busy, and
    /// when the lane is LISTed next (monotonic ms).
    idle_ms: u64,
    next_list_at: u64,
    /// When a LIST of it last found work (monotonic ms).
    last_busy: u64,
    /// HEADs of slots above the checkpoint (immutable: create-only), with
    /// when each was first HEADed.
    heads: HashMap<(String, u64), (Found, u64)>,
    /// A statement with no answer may land until then (monotonic ms).
    unsettled_until: Option<u64>,
    taken_at: u64,
    /// Rows ingested (the load).
    load: Ewma,
    /// The watermark the last full listing computed (ns, wall ms), for the
    /// next checkpoint write.
    pending_wm: Option<(u64, u64)>,
    /// This step's full listing: the highest slot listed per epoch (for the
    /// close proof, `CkptDoc::close_proof`).
    full_listed: Option<BTreeMap<String, u64>>,
    /// A later epoch than the retired one, seen by a listing (a rebirth).
    pending_reborn: Option<String>,
    /// A checkpoint write of ours got no answer and read back unchanged
    /// (`NotWritten::Unchanged`): it may still land. Until a later write of
    /// ours succeeds (after which it can only meet a 412) the stored
    /// checkpoint may be ahead of `ckpt`, and GC deletes by the stored one;
    /// `scan` reads it back first (`refresh_own_ckpt`).
    ckpt_unsure: bool,
    /// Renewals of ours that got no answer and read back unchanged
    /// (`NotWritten::Unchanged`): each may still land. Until one is found
    /// stored (and adopted) or a later renewal succeeds (after which they can
    /// only meet a 412), `maintain` reads the lease back before its lapse
    /// check and its renewal (`refresh_own_lease`). Each is the `Held` it
    /// would have installed (no ETag: none was seen); the beats differ
    /// (`coord::renew_after`).
    lease_unsure: Vec<Held>,
}

impl LaneState {
    fn new(lane: Lane, held: Held, ckpt: CkptDoc, ckpt_etag: String, now: u64, load: Ewma) -> Self {
        let known = ckpt.epochs.keys().cloned().collect();
        LaneState {
            lane,
            held,
            ckpt,
            ckpt_etag,
            known,
            last_full: None,
            compact_due: false,
            last_seen: HashMap::new(),
            idle_ms: 0,
            next_list_at: now,
            last_busy: now,
            heads: HashMap::new(),
            unsettled_until: None,
            taken_at: now,
            load,
            pending_wm: None,
            full_listed: None,
            pending_reborn: None,
            ckpt_unsure: false,
            lease_unsure: Vec::new(),
        }
    }
}

/// One epoch's work this step.
struct EpochWork {
    lane: String,
    epoch: String,
    next: u64,
    data: Vec<u64>,
    /// Heartbeats among `data`: done without an insert.
    beats: Vec<u64>,
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
    /// gc.json's retired epochs per lane, and when they were read (mono ms).
    gc_retired: Option<(u64, BTreeMap<String, BTreeSet<String>>)>,
    pub hints: Option<Rc<dyn Hints>>,
    /// Lanes learnt from hints since the last lane listing.
    hinted: HashSet<String>,
    last_lanes: Option<u64>,
    jitter: Jitter,
    /// Content keys whose range assertion failed: always checked over every partition.
    unranged: HashSet<String>,
    /// The other live workers' loads (heartbeat key -> load), the rates of
    /// the lanes they hold, and when they were read.
    peer_loads: HashMap<String, f64>,
    peer_lanes: HashMap<String, Vec<String>>,
    lane_rates: HashMap<String, f64>,
    last_loads: Option<u64>,
    /// Since when (mono) each lane no live worker's heartbeat names has been so.
    unheld_since: HashMap<String, u64>,
    /// When the worker next has something to do (a linger ending, a lane's
    /// backoff ending): the caller may sleep until then instead of a whole poll.
    wake_at: Option<u64>,
    /// This step's objects below a retired lane's bound (quarantine
    /// candidates), and those the ingest found absent from central: they are
    /// recorded in `{ctl}/quarantine/{lane}.json`, never inserted.
    below: HashSet<SlotId>,
    quarantined: Vec<Obj>,
    /// Takes of ours that got no answer and read back the version they were
    /// conditional on (`LeaseMiss::Unchanged`): each may still land, and the
    /// store then names us holder of a lane we do not hold. Per lane: that
    /// version's ETag (None: no lease object yet) and the `Held` each take
    /// would have installed (no ETag: none was seen; distinct beats,
    /// `coord::take_after`). Until one is found stored (and adopted) or the
    /// lease is seen at another version (then they can only fail),
    /// each step starts by reading the lease back (`refresh_own_takes`), and try_take
    /// looks for them before it judges the lane someone else's.
    take_unsure: BTreeMap<String, (Option<String>, Vec<Held>)>,
}

fn log(cfg: &Config, msg: &str) {
    otap_s3pq::log(&format!("consumer {}: {msg}", cfg.worker));
}

impl<B: Bucket, C: Central, K: Clock> Worker<B, C, K> {
    pub fn new(cfg: Config, bucket: Rc<B>, central: Rc<C>, clock: K) -> Self {
        let jitter = Jitter::new(&cfg.worker);
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
            gc_retired: None,
            hints: None,
            hinted: HashSet::new(),
            last_lanes: None,
            jitter,
            unranged: HashSet::new(),
            peer_loads: HashMap::new(),
            peer_lanes: HashMap::new(),
            lane_rates: HashMap::new(),
            last_loads: None,
            unheld_since: HashMap::new(),
            wake_at: None,
            below: HashSet::new(),
            quarantined: Vec::new(),
            take_unsure: BTreeMap::new(),
        }
    }

    /// When the worker next has something to do (monotonic ms), if sooner than a poll.
    pub fn next_wake(&self) -> Option<u64> {
        self.wake_at
    }

    fn wake(&mut self, at: u64) {
        self.wake_at = Some(self.wake_at.map_or(at, |w| w.min(at)));
    }

    /// A lane's weight for balancing: the base plus its recent rows/s.
    fn weight(&self, id: &str, now: u64) -> f64 {
        let b = &self.cfg.balance;
        let rate = match self.held.get(id) {
            Some(ls) => ls.load.rate(now, b.window_ms),
            None => self.lane_rates.get(id).copied().unwrap_or(0.0),
        };
        b.base_weight + rate
    }

    fn my_load(&self, now: u64) -> f64 {
        self.held.keys().map(|id| self.weight(id, now)).sum()
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
        self.wake_at = None;
        self.below.clear();
        self.quarantined.clear();
        // (once a step: a take pending on a version nobody replaces costs a
        // GET per poll until someone takes the lane, us included)
        self.refresh_own_takes().await;
        self.maintain().await;
        let now = self.clock.mono();
        if self.last_discover.is_none_or(|t| now >= t + self.cfg.discover_ms) {
            self.discover().await;
            self.last_discover = Some(now);
            self.balance().await;
        }
        self.take_hints().await;
        let step_start = now;
        let ids: Vec<String> = self.held.keys().cloned().collect();
        let mut objs = Vec::new();
        let mut work = Vec::new();
        for id in &ids {
            // Renew between lanes: a long scan (a backlog, slow HEADs) must
            // not outlast the leases (tests/dst_consumer.rs `a_backlog…`).
            if self.cfg.timing.mutation != Mutation::RenewOnlyAtInsert {
                self.maintain().await;
            }
            let now = self.clock.mono();
            let due = self.held.get(id).is_some_and(|l| now >= l.next_list_at);
            if !due {
                self.stats.lists_skipped += 1;
                continue;
            }
            let (n_objs, n_work) = (objs.len(), work.len());
            match self.scan(id, &mut objs, &mut work).await {
                Ok(new_epoch) => {
                    // Busy (anything to ingest or close, or a new epoch): LIST
                    // again next poll. Idle: back off.
                    // (Heartbeats alone keep an idle lane idle.)
                    let busy = objs.len() > n_objs || work[n_work..].iter().any(|w| w.tomb.is_some()) || new_epoch;
                    let (b, r) = (self.cfg.backoff, self.jitter.next());
                    if let Some(ls) = self.held.get_mut(id) {
                        if busy {
                            ls.last_busy = now;
                        }
                        if !b.enabled() {
                            ls.idle_ms = 0;
                            ls.next_list_at = step_start;
                        } else if busy || now < ls.last_busy + b.after_ms {
                            ls.idle_ms = 0;
                            ls.next_list_at = step_start + self.cfg.poll_ms - self.cfg.poll_ms / 10;
                        } else {
                            ls.idle_ms = b.next_base(ls.idle_ms);
                            ls.next_list_at = now + b.jittered(ls.idle_ms, r);
                        }
                    }
                }
                Err(e) => {
                    self.stats.errors += 1;
                    log(&self.cfg, &format!("scan {id}: {e}"));
                }
            }
        }
        let later: Vec<u64> = self.held.values().map(|l| l.next_list_at).filter(|t| *t > now).collect();
        if let Some(t) = later.into_iter().min() {
            self.wake(t);
        }
        let done = self.ingest(objs).await;
        let mut progressed = false;
        for id in &ids {
            progressed |= self.advance(id, &work, &done).await;
        }
        progressed
    }

    /// Hints: wake the named held lanes now; learn lanes not listed yet.
    async fn take_hints(&mut self) {
        let Some(h) = self.hints.clone() else { return };
        let now = self.clock.mono();
        for id in h.poll().await {
            self.stats.hints += 1;
            if let Some(ls) = self.held.get_mut(&id) {
                if ls.next_list_at > now {
                    ls.next_list_at = now;
                    ls.idle_ms = 0;
                    ls.last_busy = now;
                    self.stats.hint_wakeups += 1;
                }
            } else if !self.lanes.contains_key(&id) {
                if id.split('/').count() != self.cfg.depth || id.split('/').any(|p| p.is_empty() || p.starts_with('_')) {
                    continue;
                }
                let lane = match id.rsplit_once('/') {
                    Some((p, s)) => Lane { producer: p.to_string(), signal: s.to_string() },
                    None => Lane { producer: String::new(), signal: id.clone() },
                };
                let wanted = self.cfg.signals.is_empty() || self.cfg.signals.contains(&lane.signal);
                if LaneKind::for_signal(&lane.signal).is_some() && wanted {
                    self.stats.hint_new_lanes += 1;
                    let _ = self.hinted.insert(id.clone());
                    let _ = self.lanes.insert(id, lane);
                }
            }
        }
    }

    // ---- leases ------------------------------------------------------------------

    async fn maintain(&mut self) {
        let t = self.cfg.timing;
        let ids: Vec<String> = self.held.keys().cloned().collect();
        for id in ids {
            // A renewal of ours may have landed after we gave up on it: read
            // the lease back before judging the window by the older version.
            if self.held.get(&id).is_some_and(|l| !l.lease_unsure.is_empty()) {
                self.refresh_own_lease(&id).await;
            }
            let now = self.clock.mono();
            let h = self.held[&id].held.clone();
            if h.lapsed(now, &t) {
                self.stats.lanes_lapsed += 1;
                log(&self.cfg, &format!("lease {id} lapsed by our clock (epoch {}, beat {}): dropping it", h.doc.epoch, h.doc.beat));
                let _ = self.held.remove(&id);
                continue;
            }
            if !h.renew_due(now) {
                continue;
            }
            let lane = self.held[&id].lane.clone();
            let late_lost = t.mutation == Mutation::LateRenewalLost;
            let doc = if late_lost { coord::renew(&h.doc, self.clock.wall()) } else { coord::renew_after(&h.doc, &self.held[&id].lease_unsure, self.clock.wall()) };
            // (the instant `write_lease` counts the window from)
            let (sent_ms, sent_wall_ms) = (self.clock.mono(), self.clock.wall());
            match self.write_lease(&lane, &doc, Some(&h.etag)).await {
                Ok(held) => {
                    self.stats.renewals += 1;
                    if let Some(ls) = self.held.get_mut(&id) {
                        ls.held = held;
                        // (the ones we gave up on were conditional on the
                        // version this one replaced: they can only meet a 412)
                        ls.lease_unsure.clear();
                    }
                }
                // Our request has not applied: the lease is still the
                // version we hold, on its old window (the lapse check above
                // still counts from it). It may apply later all the same (a
                // write applied after our read-back, STPA.md CAST-74): until
                // we know, the next maintain reads the lease back first. And
                // it retries.
                Err(LeaseMiss::Unchanged) => {
                    log(&self.cfg, &format!("lease {id}: renewal not applied (yet), still ours; retrying"));
                    if let Some(ls) = self.held.get_mut(&id).filter(|_| !late_lost) {
                        ls.lease_unsure.push(Held { doc, etag: String::new(), sent_ms, sent_wall_ms });
                    }
                }
                Err(LeaseMiss::Other(stored)) => {
                    // The read-back shows an earlier renewal of ours that
                    // landed late (this one met its 412): the lane is ours.
                    let ls = self.held.get_mut(&id).expect("held");
                    let own = stored.as_ref().filter(|_| !late_lost).and_then(|(d, e)| coord::own_late_renewal(d, &ls.held.doc, &ls.lease_unsure).map(|p| p.landed(e)));
                    if let Some(held) = own {
                        self.stats.lease_late_taken += 1;
                        log(&self.cfg, &format!("lease {id}: our earlier renewal landed late (beat {}); adopting it", held.doc.beat));
                        ls.held = held;
                        ls.lease_unsure.clear();
                        continue;
                    }
                    // Taken over (or unresolvable): stop at once.
                    self.stats.lanes_lost_cas += 1;
                    log(&self.cfg, &format!("lease {id}: renewal failed (epoch {}, beat {}), dropping the lane", h.doc.epoch, h.doc.beat));
                    let _ = self.held.remove(&id);
                }
            }
        }
    }

    /// Reads the lane's lease back while renewals of ours may still land
    /// (`lease_unsure`), and adopts it if it is one of them
    /// (`coord::own_late_renewal`: our owner id, our lease epoch, one of the
    /// beats we sent). Without this a renewal that landed after its
    /// read-back left the holder counting from the older version: it lapsed
    /// early, or dropped the lane when its next renewal met a 412, and the
    /// lane, its own in the store, idled until the late version was ttl +
    /// margin old (STPA.md CAST-74; `a_lease_renewal_landing_late_is_adopted`).
    async fn refresh_own_lease(&mut self, id: &str) {
        let Some(ls) = self.held.get(id) else { return };
        let key = ls.lane.lease_key(&self.cfg.ctl);
        let got = self.bucket.get(&key).await;
        let Some(ls) = self.held.get_mut(id) else { return };
        match got {
            Ok(Some((_, e))) if e == ls.held.etag => {} // not landed (yet)
            Ok(Some((body, e))) => {
                let doc = serde_json::from_slice::<LeaseDoc>(&body).ok();
                match doc.as_ref().and_then(|d| coord::own_late_renewal(d, &ls.held.doc, &ls.lease_unsure)).map(|p| p.landed(&e)) {
                    Some(held) => {
                        log(&self.cfg, &format!("lease {id}: our renewal landed late (beat {}); adopting it", held.doc.beat));
                        self.stats.lease_late_taken += 1;
                        ls.held = held;
                        ls.lease_unsure.clear();
                    }
                    // Not ours: the lane changed hands; the next renewal's CAS
                    // drops it (or the window ends first).
                    None => ls.lease_unsure.clear(),
                }
            }
            _ => {} // unreadable or gone: try again next time
        }
    }

    /// PUT a lease doc (If-Match `etag`, or create), resolving a lost answer
    /// by reading it back. The window starts when the PUT was sent.
    async fn write_lease(&self, lane: &Lane, doc: &LeaseDoc, etag: Option<&str>) -> Result<Held, LeaseMiss> {
        let key = lane.lease_key(&self.cfg.ctl);
        let body = Bytes::from(serde_json::to_vec(doc).expect("lease json"));
        let (sent_ms, sent_wall_ms) = (self.clock.mono(), self.clock.wall());
        let cond = etag.map_or(Cond::Create, Cond::IfMatch);
        // A 412 is not proof that someone else wrote: when a PUT applied and
        // its answer was an error (a 5xx, a 409 on AWS), object_store retries
        // it with the old ETag and meets our own new one (AMBIGUITY.md, S3
        // conditional PUT). Both a 412 and no answer are resolved by reading
        // the lease back: a doc equal to ours (our worker, epoch, beat and
        // wall time) was written by us.
        let etag = match self.bucket.put(&key, body, cond, &BTreeMap::new()).await {
            Put::Ok(e) => e,
            Put::Conflict if self.cfg.timing.mutation == Mutation::Own412IsTakeover => return Err(LeaseMiss::Other(None)),
            r @ (Put::Conflict | Put::Unknown(_)) => match self.bucket.get(&key).await {
                Ok(Some((b, e))) => match serde_json::from_slice::<LeaseDoc>(&b).ok() {
                    Some(d) if d == *doc => {
                        if r == Put::Conflict {
                            log(&self.cfg, &format!("lease {}: 412, but the lease is ours (our earlier attempt applied)", lane.id()));
                        }
                        e
                    }
                    _ if etag == Some(e.as_str()) => return Err(LeaseMiss::Unchanged),
                    // (what is stored instead: the caller may know it for one of its own)
                    d => return Err(LeaseMiss::Other(d.map(|d| (d, e)))),
                },
                // (a create: still no lease object, the version it was conditional on)
                Ok(None) if etag.is_none() => return Err(LeaseMiss::Unchanged),
                _ => return Err(LeaseMiss::Other(None)),
            },
        };
        Ok(Held { doc: doc.clone(), etag, sent_ms, sent_wall_ms })
    }

    async fn discover(&mut self) {
        let b = self.bucket.clone();
        let now = self.clock.mono();
        if self.last_lanes.is_none_or(|t| now >= t + self.cfg.lanes_every_ms) {
            if self.list_lanes().await {
                self.last_lanes = Some(now);
            }
        }
        self.heartbeat_and_leases(&b).await;
    }

    /// The lane directories: one LIST of the root, one per cluster, one per
    /// producer (`list_lane_parents`).
    async fn list_lanes(&mut self) -> bool {
        let b = self.bucket.clone();
        let mut lanes = BTreeMap::new();
        let producers = match list_lane_parents(&*b, &self.cfg.root, self.cfg.depth).await {
            Ok(p) => p,
            Err(e) => {
                self.stats.errors += 1;
                log(&self.cfg, &format!("discover: {e}"));
                return false;
            }
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
        // A lane a hint named since the last listing stays even if this
        // listing doesn't show it yet (LIST lag).
        for id in std::mem::take(&mut self.hinted) {
            if let Some(l) = self.lanes.get(&id) {
                let _ = lanes.entry(id).or_insert_with(|| l.clone());
            }
        }
        self.lanes = lanes;
        true
    }

    async fn heartbeat_and_leases(&mut self, b: &Rc<B>) {
        // heartbeat (with this worker's load and its lanes' rates), and who else is alive
        self.beat += 1;
        let now = self.clock.mono();
        let win = self.cfg.balance.window_ms;
        let rates: BTreeMap<String, f64> =
            self.held.iter().map(|(id, l)| (id.clone(), (l.load.rate(now, win) * 1000.0).round() / 1000.0)).collect();
        let hb = serde_json::json!({"worker": self.cfg.worker, "beat": self.beat, "wall_ms": self.clock.wall(),
            "load": (self.my_load(now) * 1000.0).round() / 1000.0, "lanes": rates});
        let wprefix = join(&self.cfg.ctl, "workers");
        let mine = format!("{wprefix}/{}.json", self.cfg.worker);
        let _ = b.put(&mine, Bytes::from(hb.to_string()), Cond::None, &BTreeMap::new()).await;
        let t = self.cfg.timing;
        let read_loads = self.cfg.balance.mode == BalanceMode::Load
            && self.last_loads.is_none_or(|x| now >= x + self.cfg.balance.loads_every_ms);
        let round_start = now;
        let backdate = t.mutation == Mutation::BackdateObservations;
        if let Ok(items) = b.list(&wprefix, None).await {
            // Observed now, not when the round started: a new ETag must not
            // be backdated by however long the requests (or a pause) took.
            let now = if backdate { round_start } else { self.clock.mono() };
            let mut live = 0;
            let mut dead = Vec::new();
            let mut peers = Vec::new();
            let wall = self.clock.wall();
            for it in items {
                let id = it.key.clone();
                let etag = it.etag.clone().unwrap_or_default();
                self.workers_obs.observe(&id, Some(&etag), now);
                let quiet = self.workers_obs.unchanged_for(&id, &etag, now).unwrap_or(0);
                // (LastModified is the store's clock: good enough to discount a
                // long-dead heartbeat at once. This only balances load.)
                let stale = wall.saturating_sub(it.modified_ms) > 3 * t.ttl_ms;
                if id == mine {
                    live += 1;
                } else if quiet < t.ttl_ms + t.margin_ms && !stale {
                    live += 1;
                    peers.push(id);
                } else if quiet > 10 * t.ttl_ms || wall.saturating_sub(it.modified_ms) > 10 * t.ttl_ms {
                    dead.push(id);
                }
            }
            self.live_workers = if self.cfg.solo { 1 } else { live.max(1) };
            let _ = b.delete(&dead).await;
            self.peer_loads.retain(|k, _| peers.contains(k));
            self.peer_lanes.retain(|k, _| peers.contains(k));
            if read_loads {
                self.last_loads = Some(now);
                for id in peers {
                    self.stats.load_reads += 1;
                    if let Ok(Some((body, _))) = b.get(&id).await {
                        if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&body) {
                            let _ = self.peer_loads.insert(id.clone(), v["load"].as_f64().unwrap_or(0.0));
                            let mut theirs = Vec::new();
                            if let Some(m) = v["lanes"].as_object() {
                                for (lane, r) in m {
                                    let _ = self.lane_rates.insert(lane.clone(), r.as_f64().unwrap_or(0.0));
                                    theirs.push(lane.clone());
                                }
                            }
                            let _ = self.peer_lanes.insert(id, theirs);
                        }
                    }
                }
            }
        }
        // leases
        let lprefix = join(&self.cfg.ctl, "lease");
        if let Ok(items) = b.list(&lprefix, None).await {
            // As above, and here it is safety: a renewal first listed now but
            // recorded as seen at the round's start would look unchanged for
            // longer than it was, and `try_take` would take a live lease
            // (tests/dst_consumer.rs `a_slow_discovery_round…`).
            let now = if backdate { round_start } else { self.clock.mono() };
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
        match self.cfg.balance.mode {
            BalanceMode::Count => self.balance_count().await,
            BalanceMode::Load => self.balance_load().await,
        }
    }

    /// May this held lane be given back now: held long enough (load mode),
    /// and no statement of it can still land (`coord::may_act`).
    fn releasable(&self, ls: &LaneState, now: u64, min_hold: u64) -> bool {
        now >= ls.taken_at + min_hold && coord::may_act(ls.unsettled_until, now, &self.cfg.timing)
    }

    async fn release_lane(&mut self, id: &str, why: &str) {
        let Some(ls) = self.held.remove(id) else { return };
        let doc = coord::release(&ls.held.doc, self.clock.wall());
        let _ = self.write_lease(&ls.lane, &doc, Some(&ls.held.etag)).await;
        // Its rate goes with it, for whoever takes it next.
        let now = self.clock.mono();
        let _ = self.lane_rates.insert(id.to_string(), ls.load.rate(now, self.cfg.balance.window_ms));
        self.stats.lanes_released += 1;
        if self.cfg.verbose {
            log(&self.cfg, &format!("released {id} ({why}, workers {})", self.live_workers));
        }
    }

    /// Fair share by lane count.
    async fn balance_count(&mut self) {
        let share = coord::fair_share(self.lanes.len(), self.live_workers).min(self.cfg.max_lanes);
        let now = self.clock.mono();
        // Above the fair share: give one lane back per round (hysteresis).
        if self.held.len() > share {
            let id = self.held.iter().rev().find(|(_, l)| self.releasable(l, now, 0)).map(|(id, _)| id.clone());
            if let Some(id) = id {
                self.release_lane(&id, &format!("share {share}")).await;
            }
            return;
        }
        let candidates: Vec<String> = self.lanes.keys().filter(|id| !self.held.contains_key(*id)).cloned().collect();
        for id in candidates {
            if self.held.len() >= share {
                break;
            }
            let _ = self.try_take(&id).await;
        }
    }

    /// By load: weights, a target and a band (`coord::pick_release`, `take_by_load`).
    async fn balance_load(&mut self) {
        let now = self.clock.mono();
        let b = self.cfg.balance;
        let h = b.hysteresis;
        let weights: Vec<f64> = self.lanes.keys().map(|id| self.weight(id, now)).collect();
        let target = coord::load_target(&weights, self.live_workers);
        if self.held.len() > self.cfg.max_lanes {
            let id = self.held.iter().rev().find(|(_, l)| self.releasable(l, now, 0)).map(|(id, _)| id.clone());
            if let Some(id) = id {
                self.release_lane(&id, "max lanes").await;
            }
            return;
        }
        let mine: Vec<(String, f64, bool)> = self
            .held
            .iter()
            .map(|(id, l)| (id.clone(), self.weight(id, now), self.releasable(l, now, b.min_hold_ms)))
            .collect();
        if let Some(id) = coord::pick_release(&mine, target, h) {
            let why = format!("load {:.0} > target {target:.0} × {:.2}", self.my_load(now), 1.0 + h);
            self.release_lane(&id, &why).await;
            return;
        }
        // Lanes another live worker's heartbeat says it holds aren't read.
        let peer_held: HashSet<&String> = self.peer_lanes.values().flatten().collect();
        let mut candidates: Vec<String> =
            self.lanes.keys().filter(|id| !self.held.contains_key(*id) && !peer_held.contains(id)).cloned().collect();
        let unheld: HashSet<String> = candidates.iter().cloned().collect();
        self.unheld_since.retain(|id, _| unheld.contains(id));
        for id in &candidates {
            let _ = self.unheld_since.entry(id.clone()).or_insert(now);
        }
        // Heaviest first: they are the hardest to place.
        candidates.sort_by(|a, b| self.weight(b, now).total_cmp(&self.weight(a, now)).then_with(|| a.cmp(b)));
        let me = format!("{}/{}.json", join(&self.cfg.ctl, "workers"), self.cfg.worker);
        for id in candidates {
            if self.held.len() >= self.cfg.max_lanes {
                break;
            }
            let my = self.my_load(now);
            let least = self.peer_loads.iter().all(|(k, l)| (my, &me) < (*l, k));
            let orphaned = self.unheld_since.get(&id).is_some_and(|t| now >= t + 3 * b.loads_every_ms);
            if !coord::take_by_load(my, self.weight(&id, now), target, h, least, orphaned) {
                continue;
            }
            let _ = self.try_take(&id).await;
        }
    }

    /// Takes a lane if its lease is free, released, or expired by our
    /// observation; then fences its checkpoint. Returns whether it did.
    async fn try_take(&mut self, id: &str) -> bool {
        let t = self.cfg.timing;
        let asked = self.clock.mono();
        let Some(lane) = self.lanes.get(id).cloned() else { return false };
        let prev = match self.lease_etags.get(id) {
            None => None,
            Some(_) => {
                // Read it (it may be released, or expired by our observation of its ETag).
                match self.bucket.get(&lane.lease_key(&self.cfg.ctl)).await {
                    Ok(Some((body, etag))) => {
                        // When the answer came, not when we asked (as in heartbeat_and_leases).
                        let now = if t.mutation == Mutation::BackdateObservations { asked } else { self.clock.mono() };
                        self.obs.observe(id, Some(&etag), now);
                        let Ok(doc) = serde_json::from_slice::<LeaseDoc>(&body) else { return false };
                        // A take of ours we gave up on, stored: the lane is
                        // ours. Adopted before the lane is judged someone
                        // else's (a version first seen now, not expired).
                        if let Some(held) = self.late_take(id, Some((&doc, &etag))) {
                            return self.install_take(id, lane, held, true).await;
                        }
                        if doc.owner == self.cfg.worker {
                            // (a lease of ours we let lapse or lost; or, before
                            // CAST-83's fix, a take of ours that landed unheard)
                            log(&self.cfg, &format!("lease {id}: the store names us holder (epoch {}, beat {}), but not as a take of ours to adopt; taken like anyone's once it expires", doc.epoch, doc.beat));
                        }
                        // (A lease this worker let lapse still names it as
                        // owner: it is taken again like anyone else's, after
                        // expiry. Skipping "our own" leases here orphaned
                        // them while this worker lived: the soak's finding.)
                        if !self.obs.may_take(id, &etag, &doc, now, t.margin_ms) {
                            return false;
                        }
                        Some((doc, etag))
                    }
                    Ok(None) => None,
                    Err(_) => return false,
                }
            }
        };
        let base = prev.as_ref().map(|p| p.1.clone());
        // (a beat above the takes of ours still pending on this same version)
        let pending = self.take_unsure.get(id).filter(|(b, _)| *b == base).map_or(&[][..], |(_, v)| &v[..]);
        let doc = coord::take_after(id, prev.as_ref().map(|p| &p.0), &self.cfg.worker, t.ttl_ms, self.clock.wall(), pending);
        // (the instant `write_lease` counts the window from)
        let (sent_ms, sent_wall_ms) = (self.clock.mono(), self.clock.wall());
        match self.write_lease(&lane, &doc, base.as_deref()).await {
            Ok(held) => self.install_take(id, lane, held, false).await,
            // Our take has not applied: the lease is still the version we
            // took it from. It may apply later all the same (a write
            // applied after our read-back, STPA.md CAST-83): the store would
            // then name us holder of a lane we do not hold, and nobody would
            // work it until that version expired. Until we know, each
            // step reads the lease back (`refresh_own_takes`). The same when
            // the read-back itself failed (`Other(None)`): the take may have
            // applied, or may still.
            Err(LeaseMiss::Unchanged | LeaseMiss::Other(None)) => {
                if t.mutation != Mutation::LateTakeLost {
                    let e = self.take_unsure.entry(id.to_string()).or_insert_with(|| (base.clone(), Vec::new()));
                    if e.0 != base {
                        // (kept for a version that is gone: they can only fail)
                        *e = (base, Vec::new());
                    }
                    e.1.push(Held { doc, etag: String::new(), sent_ms, sent_wall_ms });
                }
                false
            }
            // The read-back shows an earlier take of ours that landed late
            // (this one met its 412): the lane is ours. Anything else:
            // someone else has it.
            Err(LeaseMiss::Other(Some((d, e)))) => match self.late_take(id, Some((&d, &e))) {
                Some(held) => self.install_take(id, lane, held, true).await,
                None => false,
            },
        }
    }

    /// What a read-back of lane `id`'s lease (`stored`: its doc and ETag,
    /// None: no lease object) says about takes of ours given up on
    /// (`take_unsure`): one of them, stored, is returned to be adopted
    /// (`coord::own_late_take`, held with the stored ETag); the version they
    /// were conditional on, still stored, keeps them pending; any other
    /// version ends the doubt (their CAS can only fail now).
    fn late_take(&mut self, id: &str, stored: Option<(&LeaseDoc, &str)>) -> Option<Held> {
        let (base, pending) = self.take_unsure.get(id)?;
        if stored.map(|(_, e)| e) == base.as_deref() {
            return None;
        }
        let own = stored.and_then(|(d, e)| coord::own_late_take(d, &self.cfg.worker, pending).map(|p| p.landed(e)));
        let _ = self.take_unsure.remove(id);
        own
    }

    /// Reads back, at the start of a step, the lease of every lane with
    /// takes of ours still pending (`take_unsure`), and adopts one found
    /// stored (`late_take`). Without
    /// this a take that landed after its read-back left the store naming us
    /// holder of a lane we did not hold, and nobody, ourselves included,
    /// could take it before that version was ttl + margin old (STPA.md
    /// CAST-83; `a_lease_take_landing_late_is_adopted`).
    async fn refresh_own_takes(&mut self) {
        let ids: Vec<String> = self.take_unsure.keys().cloned().collect();
        for id in ids {
            // (a lane a listing missed this time keeps its takes pending)
            let Some(lane) = self.lanes.get(&id).cloned() else { continue };
            let got = match self.bucket.get(&lane.lease_key(&self.cfg.ctl)).await {
                Ok(Some((body, e))) => match serde_json::from_slice::<LeaseDoc>(&body) {
                    Ok(d) => Some((d, e)),
                    Err(_) => continue, // unreadable: try again next time
                },
                Ok(None) => None,
                Err(_) => continue,
            };
            if let Some(held) = self.late_take(&id, got.as_ref().map(|(d, e)| (d, e.as_str()))) {
                let _ = self.install_take(&id, lane, held, true).await;
            }
        }
    }

    /// We hold `held`, a take of ours the store holds (a 200, a read-back
    /// equal to it, or `late` the adoption of one we gave up on): fence the
    /// checkpoint and hold the lane from the take's send time. The same
    /// bookkeeping on every path (STPA.md CAST-75's lesson). A take whose
    /// window is already over (its answer, or its landing, came later than
    /// ttl − margin after it was sent) would lapse at once and leave the
    /// lane, ours in the store, idle until it expired: it is given back at
    /// once instead (a release CAS on it: anyone may take the lane now).
    async fn install_take(&mut self, id: &str, lane: Lane, held: Held, late: bool) -> bool {
        let t = self.cfg.timing;
        let _ = self.take_unsure.remove(id);
        if late {
            self.stats.take_late_taken += 1;
            log(&self.cfg, &format!("lease {id}: our take landed late (epoch {}, beat {}); adopting it", held.doc.epoch, held.doc.beat));
        }
        if held.lapsed(self.clock.mono(), &t) {
            self.stats.take_late_released += 1;
            log(&self.cfg, &format!("lease {id}: our take's window is already over (epoch {}); giving it back", held.doc.epoch));
            self.give_back(id, &lane, held).await;
            return false;
        }
        // Fence the checkpoint: rewrite it under our lease epoch.
        match self.fence_checkpoint(&lane, held.doc.epoch).await {
            Some((ck, etag)) => {
                self.stats.lanes_taken += 1;
                if self.cfg.verbose {
                    log(&self.cfg, &format!("took {id} (lease epoch {}, workers {})", held.doc.epoch, self.live_workers));
                }
                let now = self.clock.mono();
                let win = self.cfg.balance.window_ms;
                let load = Ewma::seeded(self.lane_rates.get(id).copied().unwrap_or(0.0), now, win);
                let _ = self.held.insert(id.to_string(), LaneState::new(lane, held, ck, etag, now, load));
                let _ = self.unheld_since.remove(id);
                true
            }
            None => {
                self.give_back(id, &lane, held).await;
                false
            }
        }
    }

    /// Releases `held`, a take of ours the store holds that we will not
    /// work. If the release itself is not known to have applied (no answer
    /// and our take still stored, or an unreadable read-back), the take is
    /// kept pending, with no base: the next read-back that finds it stored
    /// adopts it again and gives it back again. Forgotten here, the store
    /// would name us holder of a lane we do not hold until it expired
    /// (hegel nightly 51: the release of a take found after its window was
    /// dropped).
    async fn give_back(&mut self, id: &str, lane: &Lane, held: Held) {
        let rel = coord::release(&held.doc, self.clock.wall());
        if let Err(LeaseMiss::Unchanged | LeaseMiss::Other(None)) = self.write_lease(lane, &rel, Some(&held.etag)).await {
            log(&self.cfg, &format!("lease {id}: giving our take back got no answer; it stays pending"));
            let _ = self.take_unsure.insert(id.to_string(), (None, vec![Held { etag: String::new(), ..held }]));
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
            if let Ok(e) = self.write_ckpt(&key, &new, etag.as_deref()).await {
                return Some((new, e));
            }
        }
        None
    }

    async fn write_ckpt(&self, key: &str, doc: &CkptDoc, etag: Option<&str>) -> Result<String, NotWritten> {
        let body = Bytes::from(serde_json::to_vec(doc).expect("ckpt json"));
        // As write_lease: a 412 may be our own write behind an error answer.
        // A checkpoint equal to ours (our lease epoch, its version) is ours.
        match self.bucket.put(key, body, etag.map_or(Cond::Create, Cond::IfMatch), &BTreeMap::new()).await {
            Put::Ok(e) => Ok(e),
            Put::Conflict if self.cfg.timing.mutation == Mutation::Own412IsTakeover => Err(NotWritten::Other),
            r @ (Put::Conflict | Put::Unknown(_)) => match self.bucket.get(key).await {
                Ok(Some((b, e))) if serde_json::from_slice::<CkptDoc>(&b).ok().as_ref() == Some(doc) => {
                    if r == Put::Conflict {
                        log(&self.cfg, &format!("{key}: 412, but the checkpoint is ours (our earlier attempt applied)"));
                    }
                    Ok(e)
                }
                Ok(Some((_, e))) if etag == Some(e.as_str()) => Err(NotWritten::Unchanged),
                _ => Err(NotWritten::Other),
            },
        }
    }

    /// Gives every lane back and removes the heartbeat (a graceful stop).
    /// A lane with an unsettled statement is not released: its lease is left
    /// to expire, which takes longer than the statement can still land.
    pub async fn release_all(&mut self) {
        let hb = format!("{}/{}.json", join(&self.cfg.ctl, "workers"), self.cfg.worker);
        let _ = self.bucket.delete(&[hb]).await;
        let now = self.clock.mono();
        let ids: Vec<String> = self.held.keys().cloned().collect();
        for id in ids {
            let ls = self.held.remove(&id).expect("held");
            if !coord::may_act(ls.unsettled_until, now, &self.cfg.timing) {
                log(&self.cfg, &format!("{id}: a statement may still land; leaving the lease to expire"));
                continue;
            }
            let doc = coord::release(&ls.held.doc, self.clock.wall());
            let _ = self.write_lease(&ls.lane, &doc, Some(&ls.held.etag)).await;
        }
    }

    // ---- discovery and slots ---------------------------------------------------------

    /// LISTs a held lane from its checkpoint and HEADs its consecutive slots.
    /// Returns whether it found an epoch it didn't know.
    async fn scan(&mut self, id: &str, objs: &mut Vec<Obj>, work: &mut Vec<EpochWork>) -> Result<bool, String> {
        let b = self.bucket.clone();
        let cfg = self.cfg.clone();
        let now = self.clock.mono();
        if self.held.get(id).is_some_and(|ls| ls.ckpt_unsure) {
            self.refresh_own_ckpt(id).await;
        }
        let lists_before = b.counts().list.get();
        let res = self.scan_inner(id, objs, work, &b, &cfg, now).await;
        let n = b.counts().list.get() - lists_before;
        self.stats.lane_lists += n;
        *self.stats.lists_by_lane.entry(id.to_string()).or_default() += n;
        res
    }

    /// Reads the lane's checkpoint back after a write of ours that may have
    /// landed late, and takes it if it is ours: written under our lease
    /// epoch (a takeover writes a higher one) and a later version than the
    /// one we hold. Without this the scan restarted from the older version
    /// while GC, reading the stored one, deleted the slots in between: the
    /// lane waited at a "gap" forever (dst_consumer seed 504836, nightly
    /// 2026-09-29; `a_checkpoint_write_landing_late_is_taken_back`).
    async fn refresh_own_ckpt(&mut self, id: &str) {
        let Some(ls) = self.held.get(id) else { return };
        let key = ls.lane.ckpt_key(&self.cfg.ctl);
        let got = self.bucket.get(&key).await;
        let Some(ls) = self.held.get_mut(id) else { return };
        match got {
            Ok(Some((_, e))) if e == ls.ckpt_etag => {} // not landed (yet)
            Ok(Some((body, e))) => match serde_json::from_slice::<CkptDoc>(&body) {
                Ok(doc) if doc.ours_landed_late(ls.held.doc.epoch, &ls.ckpt) => {
                    log(&self.cfg, &format!("checkpoint {id}: our write landed late (version {}); taking it", doc.version));
                    self.stats.ckpt_late_taken += 1;
                    // Forget what that write compacted, as `advance` does when
                    // its write is answered: epochs leave a checkpoint only by
                    // compaction (closed, and retired: GC deleted every key),
                    // and compaction moves the floor only past a contiguous run
                    // from the bottom. An epoch dropped above the floor (an
                    // older one not retired yet) and still in `known` read as
                    // open at slot 0 with nothing listed: the scan tombstoned
                    // it again and the checkpoint closed it at 0, after its
                    // slots had been ingested (dst_consumer seed 4709496,
                    // nightly run 47; `a_late_checkpoint_that_compacted_an_epoch_does_not_reopen_it`).
                    let floor = doc.floor.clone();
                    let dropped: Vec<String> = ls.ckpt.epochs.keys().filter(|ep| !doc.epochs.contains_key(*ep)).cloned().collect();
                    let keep = |ep: &String| coord::above_floor(ep, &floor) && !dropped.contains(ep);
                    ls.known.retain(|ep| keep(ep));
                    ls.last_seen.retain(|ep, _| keep(ep));
                    for ep in doc.epochs.keys() {
                        let _ = ls.known.insert(ep.clone());
                    }
                    ls.heads.retain(|(ep, s), _| ls.known.contains(ep) && !doc.closed(ep) && *s >= doc.next(ep));
                    ls.ckpt = doc;
                    ls.ckpt_etag = e;
                    ls.ckpt_unsure = false;
                }
                // Not ours: the lane changed hands; the next write's CAS drops it.
                _ => ls.ckpt_unsure = false,
            },
            _ => {} // unreadable or gone: try again next step
        }
    }

    async fn scan_inner(
        &mut self,
        id: &str,
        objs: &mut Vec<Obj>,
        work: &mut Vec<EpochWork>,
        b: &Rc<B>,
        cfg: &Config,
        now: u64,
    ) -> Result<bool, String> {
        let ls = self.held.get_mut(id).expect("held");
        let prefix = ls.lane.data_prefix(&cfg.root);
        let floor = ls.ckpt.floor.clone();
        let mut listed: BTreeMap<String, Vec<plan::Listed>> = BTreeMap::new();
        let parse = |items: Vec<super::bucket::Item>, listed: &mut BTreeMap<String, Vec<plan::Listed>>| {
            for it in items {
                if let Some((e, s)) = proto::parse_slot_key(&prefix, &it.key) {
                    // At or below the floor: retired (see CkptDoc::compact).
                    if coord::above_floor(&e, &floor) {
                        listed.entry(e).or_default().push((s, it.key, it.size));
                    }
                }
            }
        };
        let full = ls.last_full.is_none_or(|t| now >= t + cfg.full_list_ms);
        if full {
            // The whole lane above the floor: every epoch not retired, and
            // their slots (so no per-epoch LIST this step). A flat LIST: an
            // epoch with no key left isn't returned (a delimiter LIST on
            // SeaweedFS still returns a directory whose objects are all
            // deleted), and it starts after the floor, so its cost follows
            // the epochs not retired yet, not the lane's history.
            let items = b.list(&prefix, coord::floor_start_after(&prefix, &floor).as_deref()).await?;
            parse(items, &mut listed);
            ls.last_full = Some(now);
            ls.compact_due = true;
            self.stats.full_lists += 1;
        } else {
            let newest = ls.known.iter().next_back().cloned();
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
            let sa = match newest.as_ref() {
                Some(e) if ls.ckpt.closed(e) => Some(proto::slot_key(&prefix, e, ls.ckpt.next(e))),
                Some(e) => Some(after(e, ls.ckpt.next(e))),
                None => coord::floor_start_after(&prefix, &floor),
            };
            let items = b.list(&prefix, sa.as_deref()).await?;
            if std::env::var_os("OTAPRS_CONSUMER_DEBUG").is_some() {
                log(&cfg, &format!("scan {id}: prefix {prefix} after {sa:?}: {} keys, first {:?}", items.len(), items.first().map(|i| &i.key)));
            }
            parse(items, &mut listed);
        }
        let mut new_epoch = false;
        for e in listed.keys() {
            new_epoch |= ls.known.insert(e.clone());
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
            let mut w = EpochWork { lane: id.to_string(), epoch: e.clone(), next, data: Vec::new(), beats: Vec::new(), tomb: None };
            for (seq, key, size) in &sc.run {
                if heads >= cfg.max_heads {
                    break;
                }
                // Once the renewal is due, leave the rest for the next step
                // (the HEADs so far are kept): the lease is renewed before
                // the next lane or the insert, not after it has lapsed.
                if !ls.heads.contains_key(&(e.clone(), *seq))
                    && ls.held.renew_due(self.clock.mono())
                    && cfg.timing.mutation != Mutation::RenewOnlyAtInsert
                {
                    break;
                }
                heads += 1;
                // A slot above the checkpoint never changes (create-only), so
                // its HEAD is kept until the checkpoint passes it.
                let (found, seen_ms) = match ls.heads.get(&(e.clone(), *seq)) {
                    Some((f, t)) => (f.clone(), *t),
                    None => match b.head(key).await? {
                        None => {
                            self.stats.head_missing += 1;
                            break;
                        }
                        Some(meta) => {
                            let f = plan::found(&meta);
                            let _ = ls.heads.insert((e.clone(), *seq), (f.clone(), now));
                            (f, now)
                        }
                    },
                };
                match found {
                    Found::Tomb => {
                        w.tomb = Some(*seq);
                        break;
                    }
                    Found::Beat { .. } | Found::Close { .. } => {
                        w.data.push(*seq);
                        w.beats.push(*seq);
                    }
                    Found::Data { content, rows, received_ns, announce, payloads, late, .. } => {
                        w.data.push(*seq);
                        // A retired lane's object below its bound: a
                        // quarantine candidate (../../FORMAT.md §3.1). The
                        // series table holds definitions no answer counts,
                        // idempotent by series: never quarantined.
                        if cfg.timing.mutation != Mutation::IngestBelow
                            && LaneKind::for_signal(&ls.lane.signal).is_some_and(|k| k.counted)
                            && ls.ckpt.quarantines(&e, received_ns, cfg.quarantine_skew_ms.saturating_mul(1_000_000))
                        {
                            let _ = self.below.insert((id.to_string(), e.clone(), *seq));
                        }
                        objs.push(Obj {
                            announce,
                            payloads,
                            late,
                            lane: id.to_string(),
                            epoch: e.clone(),
                            seq: *seq,
                            key: key.clone(),
                            size: *size,
                            content,
                            rows,
                            received_ns,
                            seen_ms,
                        });
                    }
                }
            }
            if sc.run.is_empty() && plan::may_tomb(&sc, newest.as_ref() == Some(&e), quiet_for, cfg.quiet_ms) {
                match tombstone(&**b, &prefix, &e, next).await {
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
        // The lane's watermark, from a listing of the whole lane
        // (../../FORMAT.md §3): the max low passed before this LIST, capped
        // by every request the LIST shows above the checkpoint.
        // A retired lane with a later epoch listed: a new incarnation's
        // birth; it counts again from there.
        if cfg.timing.mutation != Mutation::StaysRetired {
            ls.pending_reborn = ls.ckpt.rebirth(ls.known.iter());
        }
        if full {
            ls.full_listed = Some(listed.iter().map(|(e, v)| (e.clone(), v.iter().map(|l| l.0).max().unwrap_or(0))).collect());
            let pending: Vec<Vec<Option<Found>>> = ls
                .known
                .iter()
                .filter(|e| !ls.ckpt.closed(e))
                .map(|e| {
                    let next = ls.ckpt.next(e);
                    let mut seqs: Vec<u64> = listed.get(e).map(|v| v.iter().map(|l| l.0).filter(|s| *s >= next).collect()).unwrap_or_default();
                    seqs.sort_unstable();
                    seqs.iter().map(|s| ls.heads.get(&(e.clone(), *s)).map(|(f, _)| f.clone())).collect()
                })
                .collect();
            let pending = if cfg.timing.mutation == Mutation::WmIgnoresPending { Vec::new() } else { pending };
            if let Some(wm) = plan::lane_wm(ls.ckpt.max_low_ns, &pending) {
                ls.pending_wm = Some((wm, self.clock.wall()));
            }
        }
        Ok(new_epoch)
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
        let now = self.clock.mono();
        let t = self.cfg.timing;
        let mut by: BTreeMap<String, Vec<Obj>> = BTreeMap::new();
        let mut waits = Vec::new();
        for o in objs {
            let Some(ls) = self.held.get(&o.lane) else { continue };
            // A lane whose statement may still land: nothing of it is checked
            // or inserted before that statement has settled.
            if !coord::may_act(ls.unsettled_until, now, &t) {
                self.stats.deferred_unsettled += 1;
                waits.extend(ls.unsettled_until.map(|u| u + 1));
                continue;
            }
            by.entry(ls.lane.signal.clone()).or_default().push(o);
        }
        for w in waits {
            self.wake(w);
        }
        for (sig, list) in by {
            let Some(k) = LaneKind::for_signal(&sig) else { continue };
            if self.lingers(&list, now) {
                self.stats.linger_deferred += list.len() as u64;
                continue;
            }
            if !self.ensure(&k).await {
                continue;
            }
            if !k.counted {
                for g in plan::group_parts(list, &self.cfg.limits, self.cfg.timing.mutation != Mutation::MixLateParts) {
                    self.maintain().await;
                    let refs: Vec<&Obj> = g.iter().collect();
                    let Some(fence) = self.window(&refs) else { continue };
                    let keys: Vec<&str> = g.iter().map(|o| o.key.as_str()).collect();
                    let token = plan::token(&k.signal, &keys);
                    self.stats.statements += 1;
                    self.stats.statement_objects += g.len() as u64;
                    match self.central.insert(&k, &refs, fence, &token, false).await {
                        Ok(()) if self.clock.wall() <= fence.wall_ms + fence.budget_ms => {
                            for o in &g {
                                self.stats.series_objects_inserted += 1;
                                self.add_load(o);
                                let _ = done.insert((o.lane.clone(), o.epoch.clone(), o.seq));
                            }
                        }
                        // Answered after the fence could have dropped it: not
                        // known to have landed; the next holder re-inserts
                        // (harmless for this table, as is a lost answer).
                        Ok(()) => self.stats.fenced_by_server += g.len() as u64,
                        Err(e) => {
                            self.stats.insert_errors += 1;
                            log(&self.cfg, &format!("insert {}: {e}", k.signal));
                        }
                    }
                }
                continue;
            }
            // Below a retired lane's bound (../../FORMAT.md §3.1): never
            // inserted. A content key any of whose slots is below goes
            // this way with all of them.
            let below_contents: HashSet<String> = list
                .iter()
                .filter(|o| self.below.contains(&(o.lane.clone(), o.epoch.clone(), o.seq)))
                .map(|o| o.content.clone())
                .collect();
            let (below, list): (Vec<Obj>, Vec<Obj>) = list.into_iter().partition(|o| below_contents.contains(&o.content));
            if !below.is_empty() {
                self.settle_below(&k, below, &mut done).await;
            }
            // Announcements before rows: an object's announcements must
            // have landed before any row of its lane goes in this round.
            let list = if k.announce { self.announce_first(&k, list).await } else { list };
            // Payloads before rows (R-L9, AMBIGUITY X22): likewise, an
            // object's payload part must have landed before any row of its
            // lane goes in this round. The mutant sends them after the rows.
            let rows_first = self.cfg.timing.mutation == coord::Mutation::RowsBeforePayloads;
            let list = if k.announce && !rows_first { self.payloads_first(&k, list).await } else { list };
            if list.is_empty() {
                continue;
            }
            let late_payloads: Vec<Obj> = if k.announce && rows_first { list.clone() } else { Vec::new() };
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
            // The pre-check finds earlier attempts of these objects (in their
            // own range) and copies of them received within the horizon.
            let range = self.check_range_of(&k, &primary.iter().collect::<Vec<_>>());
            let pre = match self.counts(&k, &contents, range).await {
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
            for g in plan::group_parts(absent, &self.cfg.limits, self.cfg.timing.mutation != Mutation::MixLateParts) {
                self.insert_group(&k, g, &mut ok).await;
            }
            if !late_payloads.is_empty() {
                let _ = self.payloads_first(&k, late_payloads).await;
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

    /// Objects below a retired lane's bound: a copy of what central already
    /// holds (every row of its content key) is passed like any copy; the
    /// rest are quarantined (`self.quarantined`, recorded by `advance`
    /// before the checkpoint passes them), never inserted.
    async fn settle_below(&mut self, k: &LaneKind, below: Vec<Obj>, done: &mut HashSet<SlotId>) {
        let mut contents: Vec<&str> = below.iter().map(|o| o.content.as_str()).collect();
        contents.sort_unstable();
        contents.dedup();
        let range = self.check_range_of(k, &below.iter().collect::<Vec<_>>());
        let have = match self.counts(k, &contents, range).await {
            Ok(m) => m,
            Err(e) => {
                self.stats.errors += 1;
                log(&self.cfg, &format!("check {} (below a retirement): {e}", k.signal));
                return;
            }
        };
        for o in below {
            match plan::verdict(o.rows, have.get(&o.content).copied().unwrap_or(0)) {
                Verdict::Present | Verdict::Over(_) => {
                    self.stats.below_copies += 1;
                    let _ = done.insert((o.lane.clone(), o.epoch.clone(), o.seq));
                }
                Verdict::Partial(_) | Verdict::Absent => self.quarantined.push(o),
            }
        }
    }

    /// Inserts the resource announcements these objects carry (`oscope-announce`
    /// > 0, traces and logs) into `otel_resources`, before any of their rows:
    /// the rows that use a resource come in the object that announces it or
    /// after it in its lane's epoch, so ingesting a lane's announcements
    /// first means no row reaches central before the announcement of its
    /// resource, and a row is exact once the dictionaries have loaded
    /// (`../../model/entityCatalog.qnt`, `sameLane`: exactAfterLag). A lane
    /// whose announcement statement did not surely land (an error, no
    /// answer, answered past its fence, no lease window) sits this round
    /// out; announcements are idempotent, so the retry is harmless.
    async fn announce_first(&mut self, k: &LaneKind, list: Vec<Obj>) -> Vec<Obj> {
        let ann: Vec<Obj> = list.iter().filter(|o| o.announce > 0).cloned().collect();
        if ann.is_empty() {
            return list;
        }
        let mut held: HashSet<String> = HashSet::new();
        for g in plan::group(ann, &self.cfg.limits) {
            self.maintain().await;
            let refs: Vec<&Obj> = g.iter().collect();
            let Some(fence) = self.window(&refs) else {
                held.extend(g.iter().map(|o| o.lane.clone()));
                continue;
            };
            let keys: Vec<&str> = g.iter().map(|o| o.key.as_str()).collect();
            let token = plan::token(&format!("{}-announce", k.signal), &keys);
            self.stats.announce_statements += 1;
            match self.central.announce(k, &refs, fence, &token).await {
                Ok(()) if self.clock.wall() <= fence.wall_ms + fence.budget_ms => self.stats.announce_objects += g.len() as u64,
                Ok(()) => {
                    self.stats.fenced_by_server += g.len() as u64;
                    held.extend(g.iter().map(|o| o.lane.clone()));
                }
                Err(e) => {
                    self.stats.insert_errors += 1;
                    log(&self.cfg, &format!("announce {}: {e}", k.signal));
                    // No answer, or one that may still commit: like any
                    // statement, it must settle inside the lease that sent it
                    // (found by dst_consumer: a TIMEOUT_EXCEEDED announcement
                    // landed after the lane changed hands).
                    if !self.settled(&e) {
                        self.unsettle(&refs, &e);
                    }
                    held.extend(g.iter().map(|o| o.lane.clone()));
                }
            }
        }
        if held.is_empty() {
            return list;
        }
        let (keep, wait): (Vec<Obj>, Vec<Obj>) = list.into_iter().partition(|o| !held.contains(&o.lane));
        self.stats.announce_deferred += wait.len() as u64;
        keep
    }

    /// Inserts the payload parts these objects carry (`oscope-payloads` > 0,
    /// traces and logs) into `llm_payloads`, before any of their rows
    /// (R-L9, AMBIGUITY X22): a row references payloads carried by its own
    /// object or by an earlier object of its lane's epoch (the edge's
    /// per-lane cache marks a payload sent only once its object committed),
    /// so ingesting a lane's payloads first means no row reaches central
    /// before the content it references. Exactly `announce_first`'s
    /// discipline: a lane whose payload statement did not surely land sits
    /// this round out; payloads are content-addressed, so a retry is
    /// harmless.
    async fn payloads_first(&mut self, k: &LaneKind, list: Vec<Obj>) -> Vec<Obj> {
        let carrying: Vec<Obj> = list.iter().filter(|o| o.payloads.carried > 0).cloned().collect();
        if carrying.is_empty() {
            return list;
        }
        let mut held: HashSet<String> = HashSet::new();
        for g in plan::group(carrying, &self.cfg.limits) {
            self.maintain().await;
            let refs: Vec<&Obj> = g.iter().collect();
            let Some(fence) = self.window(&refs) else {
                held.extend(g.iter().map(|o| o.lane.clone()));
                continue;
            };
            let keys: Vec<&str> = g.iter().map(|o| o.key.as_str()).collect();
            let token = plan::token(&format!("{}-payloads", k.signal), &keys);
            self.stats.payload_statements += 1;
            match self.central.payloads(k, &refs, fence, &token).await {
                Ok(()) if self.clock.wall() <= fence.wall_ms + fence.budget_ms => self.stats.payload_objects += g.len() as u64,
                Ok(()) => {
                    self.stats.fenced_by_server += g.len() as u64;
                    held.extend(g.iter().map(|o| o.lane.clone()));
                }
                Err(e) => {
                    self.stats.insert_errors += 1;
                    log(&self.cfg, &format!("payloads {}: {e}", k.signal));
                    // Like any statement, it must settle inside the lease
                    // that sent it (the announcement's CAST 23 lesson).
                    if !self.settled(&e) {
                        self.unsettle(&refs, &e);
                    }
                    held.extend(g.iter().map(|o| o.lane.clone()));
                }
            }
        }
        if held.is_empty() {
            return list;
        }
        let (keep, wait): (Vec<Obj>, Vec<Obj>) = list.into_iter().partition(|o| !held.contains(&o.lane));
        self.stats.payload_deferred += wait.len() as u64;
        keep
    }

    /// The dangling check (R-L9, AMBIGUITY X23): after these objects' rows
    /// landed, their references with no payload of their day in central are
    /// counted (`consumer_payload_dangling_total`) and logged per object; a
    /// row's reference then resolves to "missing" at read, never to empty
    /// content. Only objects whose rows hold references are read. A failed
    /// check is counted, not retried: it changes nothing that is stored.
    async fn check_dangling(&mut self, k: &LaneKind, objs: &[&Obj]) {
        let with: Vec<&Obj> = objs.iter().copied().filter(|o| o.payloads.refs > 0).collect();
        if with.is_empty() || !k.announce {
            return;
        }
        self.stats.dangling_checks += 1;
        match self.central.dangling(k, &with).await {
            Ok(per) => {
                for (key, n) in per {
                    self.stats.payload_dangling += n;
                    log(&self.cfg, &format!("{key}: {n} payload references dangle (no payload of their day in central)"));
                }
            }
            Err(e) => {
                self.stats.errors += 1;
                log(&self.cfg, &format!("dangling check {}: {e}", k.signal));
            }
        }
    }

    /// Whether a table's pending objects wait for more (the linger): the
    /// statement isn't full, the oldest was first HEADed less than
    /// `linger_ms` ago, and every lane can still start a statement then.
    fn lingers(&mut self, list: &[Obj], now: u64) -> bool {
        let l = self.cfg.linger_ms;
        if l == 0 || plan::fills(list, &self.cfg.limits) {
            return false;
        }
        let oldest = list.iter().map(|o| o.seen_ms).min().unwrap_or(now);
        if now >= oldest + l {
            return false;
        }
        let t = self.cfg.timing;
        if list.iter().any(|o| self.held.get(&o.lane).is_none_or(|ls| !ls.held.may_start(oldest + l, &t))) {
            return false;
        }
        self.wake(oldest + l);
        true
    }

    /// The pre-check's partition range for these objects (None: every partition).
    fn check_range_of(&self, k: &LaneKind, objs: &[&Obj]) -> Option<CheckRange> {
        if !self.central.ranged(k) {
            return None;
        }
        let h = self.cfg.horizon_ms.map(|h| h.saturating_mul(1_000_000));
        plan::check_range(objs, &self.unranged, h, self.cfg.timing.mutation, self.clock.wall().saturating_mul(1_000_000))
    }

    /// The range an insert of these objects (guarded) wrote into.
    fn own_range_of(&self, k: &LaneKind, objs: &[&Obj]) -> Option<CheckRange> {
        if !self.central.ranged(k) || self.cfg.horizon_ms.is_none() {
            return None;
        }
        plan::own_range(objs, &self.unranged, self.cfg.timing.mutation, self.clock.wall().saturating_mul(1_000_000))
    }

    /// Whether an insert of these objects asserts their rows' `received_at`.
    fn guarded(&self, k: &LaneKind, objs: &[&Obj]) -> bool {
        self.central.ranged(k) && objs.iter().all(|o| o.received_ns > 0 && !self.unranged.contains(&o.content))
    }

    async fn counts(&mut self, k: &LaneKind, contents: &[&str], range: Option<CheckRange>) -> Result<HashMap<String, u64>, String> {
        self.stats.checks += 1;
        if range.is_some() {
            self.stats.range_checks += 1;
        } else {
            self.stats.full_checks += 1;
        }
        self.central.counts(k, contents, range).await
    }

    /// The verify's counts: first in the partitions the statement wrote
    /// (`range`), then, for any object that falls short there, over the
    /// check's horizon (a count over fewer partitions can only be lower, so
    /// a complete one is final; a short one is recounted before anything is
    /// inserted again).
    async fn verify_counts(&mut self, k: &LaneKind, objs: &[&Obj], range: Option<CheckRange>) -> Result<HashMap<String, u64>, String> {
        let contents: Vec<&str> = objs.iter().map(|o| o.content.as_str()).collect();
        let mut m = self.counts(k, &contents, range).await?;
        if range.is_some() {
            let short: Vec<&Obj> = objs
                .iter()
                .copied()
                .filter(|o| matches!(plan::verdict(o.rows, m.get(&o.content).copied().unwrap_or(0)), Verdict::Absent | Verdict::Partial(_)))
                .collect();
            if !short.is_empty() {
                let wide = self.check_range_of(k, &short);
                if wide != range {
                    self.stats.range_recounts += 1;
                    let c: Vec<&str> = short.iter().map(|o| o.content.as_str()).collect();
                    for (key, n) in self.counts(k, &c, wide).await? {
                        let e = m.entry(key).or_default();
                        *e = (*e).max(n);
                    }
                }
            }
        }
        Ok(m)
    }

    /// Whether nothing of a failed statement can land any more (the
    /// `ErrorSettles` mutant: any error answer).
    fn settled(&self, e: &InsertErr) -> bool {
        e.settled || (self.cfg.timing.mutation == Mutation::ErrorSettles && e.answered)
    }

    /// A statement over these objects got no answer: it may land until each
    /// lane's `settled_by`. Until then those lanes are left alone.
    fn unsettle(&mut self, objs: &[&Obj], e: &InsertErr) {
        let t = self.cfg.timing;
        self.stats.unsettled += 1;
        let mut until = 0;
        for o in objs {
            if let Some(ls) = self.held.get_mut(&o.lane) {
                let u = ls.held.settled_by(&t);
                ls.unsettled_until = Some(ls.unsettled_until.map_or(u, |x| x.max(u)));
                until = until.max(u);
            }
        }
        log(&self.cfg, &format!("{} objects: {e}; leaving their lanes alone until they have settled (mono {until})", objs.len()));
        self.wake(until + 1);
    }

    /// The fence for a statement over these objects, if every contributing
    /// lease can cover it (start now, finish by `budget`, inside the window)
    /// and none of their lanes has an unsettled statement.
    fn window(&mut self, objs: &[&Obj]) -> Option<Fence> {
        let t = self.cfg.timing;
        let now = self.clock.mono();
        let mut fence = u64::MAX;
        for o in objs {
            match self.held.get(&o.lane) {
                Some(ls) if ls.held.may_start(now, &t) && coord::may_act(ls.unsettled_until, now, &t) => {
                    fence = fence.min(ls.held.fence_wall_ms(&t))
                }
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
        let (g, late): (Vec<Obj>, Vec<Obj>) = g
            .into_iter()
            .partition(|o| self.held.get(&o.lane).is_some_and(|l| l.held.may_start(now, &t) && coord::may_act(l.unsettled_until, now, &t)));
        self.stats.deferred_by_lease += late.len() as u64;
        if g.is_empty() {
            return;
        }
        let refs: Vec<&Obj> = g.iter().collect();
        let Some(fence) = self.window(&refs) else { return };
        let keys: Vec<&str> = g.iter().map(|o| o.key.as_str()).collect();
        let token = plan::token(&k.signal, &keys);
        let guard = self.guarded(k, &refs);
        self.stats.statements += 1;
        self.stats.statement_objects += g.len() as u64;
        let res = self.central.insert(k, &refs, fence, &token, guard).await;
        let mut range_err = false;
        match &res {
            Err(e) if !self.settled(e) => {
                self.stats.insert_errors += 1;
                self.unsettle(&refs, e);
                return;
            }
            Err(e) => {
                self.stats.insert_errors += 1;
                if e.range {
                    // Some object's rows don't carry its metadata's received
                    // time: nothing of the statement was written. Its objects
                    // are checked over every partition from now on.
                    range_err = true;
                    self.stats.range_guard_failures += 1;
                    for o in &g {
                        let _ = self.unranged.insert(o.content.clone());
                    }
                }
                log(&self.cfg, &format!("insert {} ({} objects): {e}; verifying", k.signal, g.len()));
            }
            Ok(()) => {}
        }
        if self.cfg.timing.mutation == coord::Mutation::NoVerify {
            for o in &g {
                self.inserted(o, ok);
            }
            return;
        }
        let range = if guard && !range_err { self.own_range_of(k, &refs) } else { None };
        let after = match self.verify_counts(k, &refs, range).await {
            Ok(m) => m,
            Err(e) => {
                self.stats.errors += 1;
                log(&self.cfg, &format!("verify {}: {e}", k.signal));
                return;
            }
        };
        let mut missing = Vec::new();
        let mut landed: Vec<Obj> = Vec::new();
        for o in g {
            match plan::verdict(o.rows, after.get(&o.content).copied().unwrap_or(0)) {
                Verdict::Present => {
                    self.inserted(&o, ok);
                    if o.payloads.refs > 0 {
                        landed.push(o);
                    }
                }
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
        if !landed.is_empty() {
            self.check_dangling(k, &landed.iter().collect::<Vec<_>>()).await;
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
            let Some(fence) = self.window(&[&o]) else { continue };
            let token = format!("{}/retry", plan::token(&k.signal, &[&o.key]));
            let guard = self.guarded(k, &[&o]);
            self.stats.statements += 1;
            self.stats.statement_objects += 1;
            self.stats.retried_missing += 1;
            let mut range_err = false;
            match self.central.insert(k, &[&o], fence, &token, guard).await {
                Err(e) if !self.settled(&e) => {
                    self.stats.insert_errors += 1;
                    self.unsettle(&[&o], &e);
                    continue;
                }
                Err(e) => {
                    self.stats.insert_errors += 1;
                    if e.range {
                        range_err = true;
                        self.stats.range_guard_failures += 1;
                        let _ = self.unranged.insert(o.content.clone());
                    }
                    log(&self.cfg, &format!("retry {}: {e}", o.key));
                }
                Ok(()) => {}
            }
            let range = if guard && !range_err { self.own_range_of(k, &[&o]) } else { None };
            if let Ok(m) = self.verify_counts(k, &[&o], range).await {
                if plan::verdict(o.rows, m.get(&o.content).copied().unwrap_or(0)) == Verdict::Present {
                    self.inserted(&o, ok);
                    self.check_dangling(k, &[&o]).await;
                }
            }
        }
    }

    fn add_load(&mut self, o: &Obj) {
        let now = self.clock.mono();
        let win = self.cfg.balance.window_ms;
        if let Some(ls) = self.held.get_mut(&o.lane) {
            ls.load.add(o.rows as f64, now, win);
        }
    }

    fn inserted(&mut self, o: &Obj, ok: &mut HashSet<String>) {
        self.stats.objects_inserted += 1;
        self.stats.rows_inserted += o.rows;
        self.add_load(o);
        if o.received_ns > 0 && self.stats.visible_ms.len() < 1_000_000 {
            self.stats.visible_ms.push(self.clock.wall() as f64 - o.received_ns as f64 / 1e6);
        }
        let _ = ok.insert(o.content.clone());
    }

    /// Inserts the missing row ordinals of a partial batch, then verifies.
    /// (The repair's `row_ordinal NOT IN` reads every partition, so it never
    /// adds a row that is anywhere already.)
    async fn repair(&mut self, k: &LaneKind, o: &Obj, have: u64) -> bool {
        let Some(fence) = self.window(&[o]) else { return false };
        self.stats.repaired_partial += 1;
        self.stats.statements += 1;
        let token = format!("{}/repair/{have}", o.content);
        log(&self.cfg, &format!("{} holds {have} of {} rows of {}: repairing", k.table, o.rows, o.content));
        if let Err(e) = self.central.repair(k, o, fence, &token).await {
            self.stats.insert_errors += 1;
            if !self.settled(&e) {
                self.unsettle(&[o], &e);
                return false;
            }
            log(&self.cfg, &format!("repair {}: {e}", o.key));
        }
        let range = self.own_range_of(k, &[o]);
        matches!(self.verify_counts(k, &[o], range).await, Ok(m) if m.get(&o.content).copied() == Some(o.rows))
    }

    // ---- checkpoints ---------------------------------------------------------------

    async fn advance(&mut self, id: &str, work: &[EpochWork], done: &HashSet<SlotId>) -> bool {
        // Compaction, after a full listing: needs gc.json's retired epochs.
        let compact = self.held.get(id).is_some_and(|l| l.compact_due);
        let retired = if compact { self.retired(id).await } else { None };
        // Quarantined objects of this lane: recorded before the checkpoint
        // passes their slots (../../FORMAT.md §3.1).
        let q: Vec<Obj> = self.quarantined.iter().filter(|o| o.lane == id).cloned().collect();
        let mut quarantined: HashSet<SlotId> = HashSet::new();
        if !q.is_empty() {
            self.quarantined.retain(|o| o.lane != id);
            let r = self.held.get(id).map_or(0, |l| l.ckpt.retired_ns);
            match super::retire::record(&*self.bucket, &self.cfg.ctl, id, r, &q, self.clock.wall()).await {
                Ok(n) => {
                    self.stats.quarantined_objects += n as u64;
                    self.stats.quarantined_rows += q.iter().map(|o| o.rows).sum::<u64>();
                    for o in &q {
                        let _ = quarantined.insert((o.lane.clone(), o.epoch.clone(), o.seq));
                        log(
                            &self.cfg,
                            &format!("QUARANTINED {id}/{}/{} ({} rows, received {} < retired {r}): not ingested; see consume admit", o.epoch, o.seq, o.rows, o.received_ns),
                        );
                    }
                }
                Err(e) => {
                    self.stats.quarantine_errors += 1;
                    log(&self.cfg, &format!("quarantine {id}: {e}; its slots wait"));
                }
            }
        }
        let wall = self.clock.wall();
        let mutation = self.cfg.timing.mutation;
        let quiet_ms = self.cfg.quiet_ms;
        let now = self.clock.mono();
        let Some(ls) = self.held.get_mut(id) else { return false };
        ls.compact_due = false;
        let mut doc = ls.ckpt.clone();
        let mut changed = false;
        let mut closed = 0;
        for w in work.iter().filter(|w| w.lane == id) {
            let n = plan::advance_to(w.next, &w.data, |s| {
                let slot = (id.to_string(), w.epoch.clone(), s);
                w.beats.contains(&s) || done.contains(&slot) || quarantined.contains(&slot)
            });
            if n > doc.next(&w.epoch) {
                // The highest low passed (ingested and verified, or a
                // heartbeat), and whether the last one passed is a close.
                let mut close_low = 0;
                for s in doc.next(&w.epoch)..n {
                    close_low = 0;
                    if let Some((f, _)) = ls.heads.get(&(w.epoch.clone(), s)) {
                        doc.max_low_ns = doc.max_low_ns.max(f.low_ns());
                        if let Found::Close { low_ns } = f {
                            close_low = *low_ns;
                        }
                    }
                }
                doc.advance(&w.epoch, n);
                if let Some(p) = doc.epochs.get_mut(&w.epoch) {
                    p.close_low = close_low;
                }
                changed = true;
            }
            if w.tomb == Some(n) && !doc.closed(&w.epoch) {
                doc.close(&w.epoch, n);
                closed += 1;
                changed = true;
            }
        }
        // Rebirth: a retired lane with a later epoch counts again.
        if let Some(e) = ls.pending_reborn.take() {
            if doc.retired_now() {
                log(&self.cfg, &format!("lane {id} reborn: epoch {e} after its retirement in {} (R {}); it counts again", doc.retired_epoch, doc.retired_ns));
                doc.reborn_epoch = e;
                self.stats.lanes_reborn += 1;
                changed = true;
            }
        }
        // Retirement by the publisher's orderly close, from this step's full
        // listing (../../FORMAT.md §3.1): the lane leaves every minimum.
        if let Some(listed) = ls.full_listed.take() {
            let newest_quiet = ls.known.iter().next_back().and_then(|e| ls.last_seen.get(e)).is_some_and(|t| now.saturating_sub(*t) >= quiet_ms);
            let m = if mutation == Mutation::RetireStale && !newest_quiet { Mutation::None } else { mutation };
            if !doc.retired_now() {
                if let Some(p) = doc.close_proof(&listed, m, wall.saturating_mul(1_000_000)) {
                    log(&self.cfg, &format!("lane {id} retired by its publisher's close in {} at R {}: out of complete_through's minimum until a later epoch", p.epoch, p.r_ns));
                    doc.retire(&p.epoch, p.r_ns, "close", "", wall);
                    self.stats.lanes_retired += 1;
                    changed = true;
                }
            }
        }
        if let Some((wm, at)) = ls.pending_wm.take() {
            // (A watermark that did not move costs no write.)
            if wm != doc.wm_ns {
                doc.wm_ns = wm;
                doc.wm_wall_ms = at;
                changed = true;
            }
        }
        let mut dropped = Vec::new();
        if let Some(r) = &retired {
            dropped = doc.compact(ls.known.iter(), r, self.cfg.timing.mutation);
            changed |= !dropped.is_empty();
        }
        if !changed {
            return false;
        }
        let new = doc.bumped(ls.held.doc.epoch);
        let key = ls.lane.ckpt_key(&self.cfg.ctl);
        let etag = ls.ckpt_etag.clone();
        self.stats.ckpt_writes += 1;
        let size = serde_json::to_vec(&new).map_or(0, |v| v.len() as u64);
        self.stats.ckpt_bytes_max = self.stats.ckpt_bytes_max.max(size);
        self.stats.ckpt_epochs_max = self.stats.ckpt_epochs_max.max(new.epochs.len() as u64);
        match self.write_ckpt(&key, &new, Some(&etag)).await {
            Ok(e) => {
                self.stats.epochs_closed += closed;
                self.stats.epochs_compacted += dropped.len() as u64;
                if self.cfg.verbose && !dropped.is_empty() {
                    log(&self.cfg, &format!("compacted {id}: {} retired epochs dropped, floor {}", dropped.len(), new.floor));
                }
                let ls = self.held.get_mut(id).expect("held");
                let floor = new.floor.clone();
                ls.known.retain(|e| coord::above_floor(e, &floor) && !dropped.contains(e));
                ls.last_seen.retain(|e, _| coord::above_floor(e, &floor) && !dropped.contains(e));
                // HEADs the checkpoint has passed aren't needed again.
                ls.heads.retain(|(ep, s), _| ls.known.contains(ep) && !new.closed(ep) && *s >= new.next(ep));
                ls.ckpt = new;
                ls.ckpt_etag = e;
                ls.ckpt_unsure = false;
                true
            }
            // Our request has not applied: the checkpoint is the version we
            // hold, and our lease still fences it. The next step advances.
            // It may still land (a request the store applies after the
            // client gave up): `scan` reads the checkpoint back until then.
            Err(NotWritten::Unchanged) => {
                log(&self.cfg, &format!("checkpoint {id}: write not applied, still ours; retrying"));
                if let Some(ls) = self.held.get_mut(id) {
                    ls.ckpt_unsure = true;
                }
                false
            }
            Err(NotWritten::Other) => {
                self.stats.lanes_lost_cas += 1;
                log(&self.cfg, &format!("checkpoint {id}: CAS failed (another worker took the lane); dropping it"));
                let _ = self.held.remove(id);
                false
            }
        }
    }

    /// gc.json's retired epochs of one lane, read at most once per
    /// `full_list_ms` (None: unreadable; compaction waits).
    async fn retired(&mut self, id: &str) -> Option<BTreeSet<String>> {
        let now = self.clock.mono();
        if self.gc_retired.as_ref().is_none_or(|(t, _)| now >= t + self.cfg.full_list_ms) {
            self.stats.gc_reads += 1;
            let key = join(&self.cfg.ctl, "gc.json");
            let r = match self.bucket.get(&key).await {
                Ok(Some((body, _))) => match serde_json::from_slice::<super::gc::GcDoc>(&body) {
                    Ok(d) => d.retired,
                    Err(e) => {
                        self.stats.errors += 1;
                        log(&self.cfg, &format!("{key}: {e}"));
                        return None;
                    }
                },
                Ok(None) => BTreeMap::new(),
                Err(e) => {
                    self.stats.errors += 1;
                    log(&self.cfg, &format!("{key}: {e}"));
                    return None;
                }
            };
            self.gc_retired = Some((now, r));
        }
        self.gc_retired.as_ref().map(|(_, m)| m.get(id).cloned().unwrap_or_default())
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
        let now = self.clock.mono();
        let weights: Vec<f64> = self.lanes.keys().map(|id| self.weight(id, now)).collect();
        let _ = m.insert("load".into(), ((self.my_load(now) * 10.0).round() / 10.0).into());
        let _ = m.insert("load_target".into(), ((coord::load_target(&weights, self.live_workers) * 10.0).round() / 10.0).into());
        let _ = m.insert("objects_per_statement".into(), (self.stats.statement_objects as f64 / self.stats.statements.max(1) as f64).into());
        let _ = m.insert("visible_n".into(), lat.len().into());
        let _ = m.insert("visible_ms_p50".into(), pct(0.5).into());
        let _ = m.insert("visible_ms_p90".into(), pct(0.9).into());
        let _ = m.insert("visible_ms_p99".into(), pct(0.99).into());
        let _ = m.insert("visible_ms_max".into(), pct(1.0).into());
        v
    }
}

/// The directories a lane's signal sits under, relative to `root`: every
/// `{cluster}/{producer}` at depth 3 (one LIST of the root, one per
/// cluster), every `{producer}` at 2, `""` at 1. Names starting with `_`
/// are control prefixes, never lanes.
pub async fn list_lane_parents<B: Bucket + ?Sized>(b: &B, root: &str, depth: usize) -> Result<Vec<String>, String> {
    let mut parents = vec![String::new()];
    for _ in 1..depth.max(1) {
        let mut next = Vec::new();
        for p in &parents {
            let dir = if p.is_empty() { root.to_string() } else { join(root, p) };
            for d in b.list_dirs(&dir).await? {
                if !d.starts_with('_') && !d.is_empty() {
                    next.push(if p.is_empty() { d } else { format!("{p}/{d}") });
                }
            }
        }
        parents = next;
    }
    Ok(parents)
}

pub enum TombResult {
    /// The epoch is closed at the slot (true: our PUT won; false: a tombstone was already there).
    Closed(bool),
    /// A late batch got the slot first: ingest it.
    LostToData,
    Unresolved(String),
}

/// Why a lease or checkpoint PUT If-Match did not take (read back after a
/// 412 or no answer).
/// Why a lease PUT did not install our doc (`write_lease`).
#[derive(Debug, Clone, PartialEq, Eq)]
enum LeaseMiss {
    /// The version we held is still stored: our request has not applied
    /// (it may still: `LaneState::lease_unsure`, `Worker::take_unsure`).
    Unchanged,
    /// Anything else, with what the read-back found (a doc and its ETag),
    /// if it could read one.
    Other(Option<(LeaseDoc, String)>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NotWritten {
    /// The object still has the ETag we sent in If-Match: our request did
    /// not apply (lost, or not landed yet; if it lands later it is ours,
    /// and a retry on the same ETag gets a 412). Before 2026-09-27 this
    /// dropped a lane that was still ours (MBT `designSlow`, seed
    /// 0x29e8aebd): it idled until our own lease expired.
    Unchanged,
    /// Another doc (a takeover), no object, or no answer to the read.
    Other,
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
                    Found::Data { .. } | Found::Beat { .. } | Found::Close { .. } => TombResult::LostToData,
                };
            }
            Ok(None) if conflict => return TombResult::Unresolved("412, then the HEAD found nothing".into()),
            Ok(None) => continue, // not landed: send it again
            Err(e) => return TombResult::Unresolved(e),
        }
    }
    TombResult::Unresolved("no answer".into())
}
