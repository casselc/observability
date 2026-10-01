//! `complete_through`: the consumer's published watermark (`../../FORMAT.md`
//! §3, `../../model/completeness.qnt`). Run beside GC (`consume gc`, or
//! alone: `consume watermark`), safe to run from several processes.
//!
//! Each lane's holder records the lane's watermark in its checkpoint
//! (`CkptDoc::wm_ns`, computed at every full listing: `plan::lane_wm`). A run
//! here:
//!
//! 1. notes the wall clock `t_list` and LISTs the lane directories;
//! 2. reads every listed lane's checkpoint: its `wm_ns`, 0 when it has no
//!    checkpoint or no watermark yet (a lane is registered by its first
//!    object, the edge's birth heartbeat, before it takes custody; a lane
//!    that exists counts from then on);
//! 3. computes `min(t_list - skew, min over lanes)`: a lane born after the
//!    LIST has every `received_at` above its birth, which is after
//!    `t_list` up to the edges' clock skew;
//! 4. publishes the running max of that and the value already published,
//!    by CAS on `{ctl}/watermark.json`: the recomputed minimum dips when a
//!    zombie PUT lands late (completeness.qnt `wRegress`), the published
//!    one never goes back. Every request with `received_at` below it is
//!    ingested, and stays so; new requests arrive above it.
//!
//! A lane whose watermark lags the wall clock by more than `stale_ms` is
//! named in the document (`stale`) and in the metrics: an edge that is
//! offline, or wedged with data in custody, holds the watermark for every
//! alert window, and that is paged, never skipped.
//!
//! **Per cluster, per signal, per lane (D29).** The same minimum over a
//! subset of the lanes is sound for the requests of that subset: a lane's
//! watermark speaks only for its own requests, and a lane of the subset born
//! after the LIST is above the cap like any other. So each run also
//! publishes, as running maxes:
//!
//! - in `watermark.json`: `clusters` (each listed cluster's value),
//!   `signals` (each signal's value over the fleet) and
//!   `unlisted_signals_ns` (a signal no listed lane carries: the cap);
//! - one document per cluster, `{ctl}/watermark/{cluster}.json` (CAS), with
//!   the cluster's value, its per-signal values and every lane's published
//!   value (`lane_wm`, keyed `{producer}/{signal}`), plus its holding and
//!   stale lanes. A reader scoped to some clusters needs only their
//!   documents (prefix ABAC, D18), and one cluster's stalled lane holds only
//!   its own cluster's values.
//!
//! **Retired lanes (D35, `../../FORMAT.md` §3.1).** A lane whose checkpoint
//! says retired (its publisher's orderly close, or `consume retire-lane`)
//! counts as +inf in every minimum here, until a later epoch appears: this
//! run LISTs the lane's epochs after reading its checkpoint, and a later one
//! (a new incarnation's birth, committed before it takes custody) puts the
//! lane back with its checkpoint's watermark. A birth this LIST did not show
//! was committed after `t_list`, so its requests are above the cap. The
//! cluster document names the retired lanes with their R (`retired`).
//!
//! **History (D29 amendment 2026-10-01, `wmhistory`).** Each document also
//! holds its scope's open hour of steps (`history`): after the GET inside
//! the CAS, the clock plus `skew_ms` stamps the values written; an hour is
//! frozen by the CAS that first writes a step in a later hour, then sealed
//! create-only as `{ctl}/watermark-history/{scope}/{YYYY-MM-DD}T{HH}.json`
//! and dropped from the document by a second CAS.
//!
//! Every published value is floored by the coarser one (a lane's >= its
//! signal's in its cluster >= its cluster's >= the fleet's): a coarser value
//! is sound for every subset, and a sound value stays sound (ingested stays
//! ingested), which is also why a running max is sound.

use super::bucket::{Bucket, Cond, Put};
use super::coord::{CkptDoc, Lane, Mutation, epoch_key, join};
use super::sql::LaneKind;
use super::worker::list_lane_parents;
use super::wmhistory::{self, HistMutation, HistStep, History};
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub struct WmConfig {
    pub root: String,
    pub ctl: String,
    pub depth: usize,
    /// The edges' clock skew against this process's (ms).
    pub skew_ms: u64,
    /// A lane whose watermark lags the wall clock by more is stale (ms).
    pub stale_ms: u64,
    /// How many of the lowest lanes the document names (`holding`).
    pub holding: usize,
    /// Publish `{ctl}/watermark/{cluster}.json` too (D29).
    pub per_cluster: bool,
    /// Write a cluster's document at most this often (ms; 0: every run).
    pub cluster_every_ms: u64,
    /// A deliberate bug for the simulation (`Mutation::StaysRetired`: a
    /// retired lane is never put back); `None` in production.
    pub mutation: Mutation,
    /// Keep the history (`wmhistory`; `--no-wm-history`: off, the documents
    /// keep what they have).
    pub history: bool,
    /// At most one history step per this long (ms), and only on a change.
    pub history_every_ms: u64,
    /// The history's deliberate bugs (the model's mutants); `None` in production.
    pub hist_mutation: HistMutation,
}

impl WmConfig {
    pub fn new(root: &str, ctl: &str) -> Self {
        WmConfig { root: root.into(), ctl: ctl.into(), depth: 3, skew_ms: 5_000, stale_ms: 300_000, holding: 5, per_cluster: true, cluster_every_ms: 0, mutation: Mutation::None, history: true, history_every_ms: 60_000, hist_mutation: HistMutation::None }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct LaneWm {
    pub lane: String,
    pub wm_ns: u64,
    pub lag_s: f64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct WmDoc {
    pub format: u32,
    /// +1 per write.
    pub version: u64,
    /// The published watermark: a running max (ns).
    pub complete_through_ns: u64,
    /// This run's minimum before the running max (ns).
    pub computed_ns: u64,
    pub wall_ms: u64,
    /// `t_list - skew` (ns).
    pub list_cap_ns: u64,
    pub lanes: usize,
    /// The lowest lanes: what holds the watermark back.
    pub holding: Vec<LaneWm>,
    /// Lanes whose watermark lags the wall clock by more than `stale_after_s`.
    pub stale: Vec<LaneWm>,
    pub stale_after_s: u64,
    /// Each listed cluster's published value (running max, >= the fleet's).
    #[serde(default)]
    pub clusters: BTreeMap<String, u64>,
    /// Each listed signal's published value over the fleet (running max,
    /// >= the fleet's).
    #[serde(default)]
    pub signals: BTreeMap<String, u64>,
    /// The value of a signal no listed lane carries (the cap; running max).
    #[serde(default)]
    pub unlisted_signals_ns: u64,
    /// Listed lanes that are retired (+inf in every minimum, D35).
    #[serde(default)]
    pub retired_lanes: usize,
    /// The open hour of the fleet's history, and hours frozen for sealing
    /// (`wmhistory`, D29 amendment 2026-10-01).
    #[serde(default, skip_serializing_if = "History::is_empty")]
    pub history: History,
}

/// `{ctl}/watermark/{cluster}.json` (D29).
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct ClusterWmDoc {
    pub format: u32,
    /// +1 per write.
    pub version: u64,
    pub cluster: String,
    /// The cluster's published value: every request of the cluster received
    /// before it is ingested (running max, >= the fleet's).
    pub complete_through_ns: u64,
    /// This run's minimum over the cluster's lanes, capped (ns).
    pub computed_ns: u64,
    pub wall_ms: u64,
    pub list_cap_ns: u64,
    pub lanes: usize,
    /// Per signal, over the cluster's lanes of it (running max, >= the cluster's).
    pub signals: BTreeMap<String, u64>,
    /// A signal none of the cluster's listed lanes carries.
    pub unlisted_signals_ns: u64,
    /// Every listed lane's published value, keyed `{producer}/{signal}`
    /// (running max, >= its signal's).
    pub lane_wm: BTreeMap<String, u64>,
    pub holding: Vec<LaneWm>,
    pub stale: Vec<LaneWm>,
    pub stale_after_s: u64,
    /// The cluster's retired lanes (D35), keyed `{producer}/{signal}`: R
    /// (ns). Such a lane is +inf in every minimum; its `lane_wm` entry is
    /// its signal's value.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub retired: BTreeMap<String, u64>,
    /// The open hour of the cluster's history, and hours frozen for sealing.
    #[serde(default, skip_serializing_if = "History::is_empty")]
    pub history: History,
}

pub fn wm_key(ctl: &str) -> String {
    join(ctl, "watermark.json")
}

/// `{ctl}/watermark/{cluster}.json`.
pub fn cluster_wm_key(ctl: &str, cluster: &str) -> String {
    join(ctl, &format!("watermark/{cluster}.json"))
}

/// A lane id `{cluster}/{producer…}/{signal}` split into (cluster, the rest
/// after the cluster, signal).
pub fn split_lane(id: &str) -> (&str, &str, &str) {
    let (cluster, rest) = id.split_once('/').unwrap_or(("", id));
    let signal = id.rsplit_once('/').map_or(id, |x| x.1);
    (cluster, rest, signal)
}

fn cap_of(wall_ms: u64, cfg: &WmConfig) -> u64 {
    wall_ms.saturating_sub(cfg.skew_ms).saturating_mul(1_000_000)
}

/// A retired lane's watermark in the maps here: +inf (`RETIRED`).
pub const RETIRED: u64 = u64::MAX;

/// (the minimum over `lanes` capped by `cap`, the lowest `holding` lanes,
/// the stale ones). A retired lane (`RETIRED`) is +inf: in no minimum, never
/// holding, never stale.
fn summarize<'a>(lanes: impl Iterator<Item = (&'a String, &'a u64)>, cap: u64, wall_ms: u64, cfg: &WmConfig) -> (u64, Vec<LaneWm>, Vec<LaneWm>) {
    let lag = |wm: u64| (wall_ms as f64 / 1e3 - wm as f64 / 1e9).max(0.0);
    let mut by: Vec<LaneWm> = lanes.filter(|(_, w)| **w != RETIRED).map(|(l, w)| LaneWm { lane: l.clone(), wm_ns: *w, lag_s: lag(*w) }).collect();
    let computed = by.iter().map(|l| l.wm_ns).fold(cap, u64::min);
    by.sort_by(|a, b| a.wm_ns.cmp(&b.wm_ns).then(a.lane.cmp(&b.lane)));
    let stale = by.iter().filter(|l| l.lag_s * 1e3 > cfg.stale_ms as f64).cloned().collect();
    by.truncate(cfg.holding);
    (computed, by, stale)
}

/// Per signal, the minimum over `lanes` (ids ending in `/{signal}`) capped by `cap`.
fn per_signal<'a>(lanes: impl Iterator<Item = (&'a String, &'a u64)>, cap: u64) -> BTreeMap<String, u64> {
    let mut out: BTreeMap<String, u64> = BTreeMap::new();
    for (l, w) in lanes {
        let e = out.entry(split_lane(l).2.to_string()).or_insert(cap);
        *e = (*e).min(*w);
    }
    out
}

/// The running max of each signal's value: a signal the previous document
/// did not list takes its unlisted value (or the previous overall value) as
/// its floor, and every one is floored by `ct`, which is sound for them all.
fn signals_max(prev: &BTreeMap<String, u64>, prev_unlisted: u64, prev_ct: u64, now: BTreeMap<String, u64>, ct: u64) -> BTreeMap<String, u64> {
    now.into_iter()
        .map(|(s, v)| {
            let p = prev.get(&s).copied().unwrap_or(prev_unlisted.max(prev_ct));
            (s, p.max(ct).max(v))
        })
        .collect()
}

/// The pure part: `lanes` is every listed lane's watermark (0: none yet;
/// `RETIRED`: a retired lane).
pub fn compute(prev: u64, lanes: &BTreeMap<String, u64>, wall_ms: u64, cfg: &WmConfig) -> WmDoc {
    compute_doc(&WmDoc { complete_through_ns: prev, ..Default::default() }, lanes, wall_ms, cfg)
}

/// The fleet document, from the previous one (for the running maxes).
pub fn compute_doc(prev: &WmDoc, lanes: &BTreeMap<String, u64>, wall_ms: u64, cfg: &WmConfig) -> WmDoc {
    let cap = cap_of(wall_ms, cfg);
    let (computed, holding, stale) = summarize(lanes.iter(), cap, wall_ms, cfg);
    let ct = prev.complete_through_ns.max(computed);
    let mut by_cluster: BTreeMap<&str, u64> = BTreeMap::new();
    for (l, w) in lanes {
        let e = by_cluster.entry(split_lane(l).0).or_insert(cap);
        *e = (*e).min(*w);
    }
    // A cluster the previous document did not name: the previous fleet
    // value (sound for every subset) is its floor.
    let clusters = by_cluster
        .into_iter()
        .map(|(c, v)| (c.to_string(), prev.clusters.get(c).copied().unwrap_or(prev.complete_through_ns).max(ct).max(v)))
        .collect();
    WmDoc {
        format: otap_s3pq::proto::FORMAT_VERSION,
        version: 0,
        complete_through_ns: ct,
        computed_ns: computed,
        wall_ms,
        list_cap_ns: cap,
        lanes: lanes.len(),
        holding,
        stale,
        stale_after_s: cfg.stale_ms / 1000,
        clusters,
        signals: signals_max(&prev.signals, prev.unlisted_signals_ns, prev.complete_through_ns, per_signal(lanes.iter(), cap), ct),
        unlisted_signals_ns: prev.unlisted_signals_ns.max(ct).max(cap),
        retired_lanes: lanes.values().filter(|w| **w == RETIRED).count(),
        history: prev.history.clone(),
    }
}

/// One cluster's document: `lanes` are the cluster's lanes (full ids);
/// `floor` is a value already sound for the cluster (the fleet document's
/// value for it, just published); `retired`: R of its retired lanes (full
/// ids; their `lanes` entry is `RETIRED`).
pub fn compute_cluster(
    prev: &ClusterWmDoc,
    cluster: &str,
    lanes: &BTreeMap<String, u64>,
    retired: &BTreeMap<String, u64>,
    floor: u64,
    wall_ms: u64,
    cfg: &WmConfig,
) -> ClusterWmDoc {
    let cap = cap_of(wall_ms, cfg);
    let (computed, holding, stale) = summarize(lanes.iter(), cap, wall_ms, cfg);
    let ct = prev.complete_through_ns.max(floor).max(computed);
    let signals = signals_max(&prev.signals, prev.unlisted_signals_ns, prev.complete_through_ns, per_signal(lanes.iter(), cap), ct);
    let lane_wm = lanes
        .iter()
        .map(|(l, w)| {
            let (_, rest, s) = split_lane(l);
            let p = prev.lane_wm.get(rest).copied().unwrap_or(0);
            // A retired lane: its signal's value (sound for it: the lane
            // holds nothing below R, and R is below every value published).
            let own = if *w == RETIRED { 0 } else { *w };
            (rest.to_string(), p.max(signals[s]).max(own))
        })
        .collect();
    ClusterWmDoc {
        format: otap_s3pq::proto::FORMAT_VERSION,
        version: 0,
        cluster: cluster.to_string(),
        complete_through_ns: ct,
        computed_ns: computed,
        wall_ms,
        list_cap_ns: cap,
        lanes: lanes.len(),
        signals,
        unlisted_signals_ns: prev.unlisted_signals_ns.max(ct).max(cap),
        lane_wm,
        holding,
        stale,
        stale_after_s: cfg.stale_ms / 1000,
        retired: lanes
            .keys()
            .filter_map(|l| retired.get(l).map(|r| (split_lane(l).1.to_string(), *r)))
            .collect(),
        history: prev.history.clone(),
    }
}

/// Every lane the data root shows, with its checkpoint's watermark
/// (`RETIRED` for a retired lane no later epoch of which is listed), and
/// the retired lanes' R.
pub async fn lane_wms<B: Bucket + ?Sized>(b: &B, cfg: &WmConfig) -> Result<BTreeMap<String, u64>, String> {
    lane_wms_retired(b, cfg).await.map(|x| x.0)
}

pub async fn lane_wms_retired<B: Bucket + ?Sized>(b: &B, cfg: &WmConfig) -> Result<(BTreeMap<String, u64>, BTreeMap<String, u64>), String> {
    let mut out = BTreeMap::new();
    let mut retired = BTreeMap::new();
    for p in list_lane_parents(b, &cfg.root, cfg.depth).await? {
        let dir = if p.is_empty() { cfg.root.clone() } else { join(&cfg.root, &p) };
        for s in b.list_dirs(&dir).await? {
            if s.starts_with('_') || LaneKind::for_signal(&s).is_none() {
                continue;
            }
            let lane = Lane { producer: p.clone(), signal: s };
            let wm = match b.get(&lane.ckpt_key(&cfg.ctl)).await? {
                Some((body, _)) => {
                    let c = serde_json::from_slice::<CkptDoc>(&body).map_err(|e| format!("{}: {e}", lane.id()))?;
                    // Retired, and no later epoch listed (a birth this LIST
                    // doesn't show committed after t_list): +inf.
                    let reborn = c.retired_now()
                        && cfg.mutation != Mutation::StaysRetired
                        && b.list_dirs(&lane.data_prefix(&cfg.root)).await?.iter().any(|e| epoch_key(e) > epoch_key(&c.retired_epoch));
                    if c.retired_now() && !reborn {
                        let _ = retired.insert(lane.id(), c.retired_ns);
                        RETIRED
                    } else {
                        c.wm_ns
                    }
                }
                None => 0,
            };
            let _ = out.insert(lane.id(), wm);
        }
    }
    Ok((out, retired))
}

/// A document written by CAS with a version.
trait Versioned: Serialize + DeserializeOwned + Default + Clone {
    fn version(&self) -> u64;
    fn set_version(&mut self, v: u64);
    fn history(&self) -> &History;
    fn set_history(&mut self, h: History);
    /// The values a history step records.
    fn step(&self, at_ms: u64) -> HistStep;
}
impl Versioned for WmDoc {
    fn version(&self) -> u64 {
        self.version
    }
    fn set_version(&mut self, v: u64) {
        self.version = v;
    }
    fn history(&self) -> &History {
        &self.history
    }
    fn set_history(&mut self, h: History) {
        self.history = h;
    }
    fn step(&self, at_ms: u64) -> HistStep {
        HistStep { at_ms, ct_ns: self.complete_through_ns, signals: self.signals.clone(), unlisted_ns: self.unlisted_signals_ns }
    }
}
impl Versioned for ClusterWmDoc {
    fn version(&self) -> u64 {
        self.version
    }
    fn set_version(&mut self, v: u64) {
        self.version = v;
    }
    fn history(&self) -> &History {
        &self.history
    }
    fn set_history(&mut self, h: History) {
        self.history = h;
    }
    fn step(&self, at_ms: u64) -> HistStep {
        HistStep { at_ms, ct_ns: self.complete_through_ns, signals: self.signals.clone(), unlisted_ns: self.unlisted_signals_ns }
    }
}

/// `next` with the history step added: stamped with `clock()` read here,
/// after the document's GET (`wmhistory::stamp`), so after everything the
/// values depend on was observed.
fn with_history<'a, D: Versioned>(
    cfg: &'a WmConfig,
    wall_ms: u64,
    clock: &'a dyn Fn() -> u64,
    next: impl Fn(&D) -> Option<D> + 'a,
) -> impl Fn(&D) -> Option<D> + 'a {
    move |prev: &D| {
        let mut d = next(prev)?;
        if cfg.history {
            let at = wmhistory::stamp(clock(), wall_ms, cfg.skew_ms, cfg.hist_mutation);
            d.set_history(wmhistory::advance(prev.history(), d.step(at), cfg.history_every_ms, cfg.hist_mutation));
        }
        Some(d)
    }
}

/// Seals a document's frozen hours (create-only), then drops the sealed ones
/// from it by CAS. Returns (hours sealed, conflicts, pending).
async fn seal_history<B: Bucket + ?Sized, D: Versioned>(b: &B, cfg: &WmConfig, key: &str, scope: &str, doc: &D) -> (usize, Vec<String>, Vec<String>) {
    if doc.history().sealing.is_empty() {
        return (0, vec![], vec![]);
    }
    let s = wmhistory::seal(b, &cfg.ctl, scope, doc.history()).await;
    let mut gone = s.done.clone();
    gone.extend(&s.conflicts);
    let conflicts = s.conflicts.iter().map(|h| format!("{}: stored with other content (kept)", wmhistory::hour_key(&cfg.ctl, scope, *h))).collect();
    let mut pending = s.pending;
    if !gone.is_empty() {
        let unseal = |prev: &D| {
            wmhistory::without_sealed(prev.history(), &gone).map(|h| {
                let mut d = prev.clone();
                d.set_history(h);
                d
            })
        };
        if let Err(e) = cas(b, key, unseal).await {
            pending.push(e);
        }
    }
    (s.done.len(), conflicts, pending)
}

/// Read-modify-write by CAS on the ETag: `next` gets the document there
/// (the default when absent) and returns the one to write, or `None` to
/// leave it as it is (returned as read).
async fn cas<B: Bucket + ?Sized, D: Versioned>(b: &B, key: &str, next: impl Fn(&D) -> Option<D>) -> Result<D, String> {
    for _ in 0..4 {
        let (prev, etag) = match b.get(key).await? {
            Some((body, e)) => (serde_json::from_slice::<D>(&body).map_err(|e| format!("{key}: {e}"))?, Some(e)),
            None => (D::default(), None),
        };
        let Some(mut doc) = next(&prev) else {
            return Ok(prev);
        };
        doc.set_version(prev.version() + 1);
        let body = Bytes::from(serde_json::to_vec(&doc).expect("json"));
        let cond = etag.as_deref().map_or(Cond::Create, Cond::IfMatch);
        match b.put(key, body, cond, &BTreeMap::new()).await {
            Put::Ok(_) => return Ok(doc),
            // Another publisher wrote first, or the answer was lost: read
            // again and take the max over what is there (idempotent).
            Put::Conflict | Put::Unknown(_) => continue,
        }
    }
    Err(format!("{key}: no CAS won in 4 tries"))
}

/// What one run published.
#[derive(Clone, Debug, Default)]
pub struct WmRun {
    pub fleet: WmDoc,
    /// The per-cluster documents written (or, written less than
    /// `cluster_every_ms` ago, left as they were).
    pub clusters: Vec<ClusterWmDoc>,
    /// Per-cluster documents that could not be written (the fleet one was):
    /// their readers keep the previous value, which stays sound.
    pub errors: Vec<String>,
    /// History hours sealed by this run (every scope).
    pub hist_sealed: usize,
    /// History hours found stored with other content (kept, never rewritten).
    pub hist_conflicts: Vec<String>,
    /// History hours left frozen for the next run (a lost answer, an error).
    pub hist_pending: Vec<String>,
}

/// One run: compute and publish the fleet document, then each listed
/// cluster's. `wall_ms` is read before the LIST; the history's stamps read
/// `wall_ms` again (a clock that does not move during the run: tests).
pub async fn watermark_run<B: Bucket + ?Sized>(b: &B, cfg: &WmConfig, wall_ms: u64) -> Result<WmRun, String> {
    watermark_run_at(b, cfg, wall_ms, &|| wall_ms).await
}

/// `watermark_run` with the clock the history's stamps read (`clock`, read
/// after each document's GET; production: the wall clock).
pub async fn watermark_run_at<B: Bucket + ?Sized>(b: &B, cfg: &WmConfig, wall_ms: u64, clock: &dyn Fn() -> u64) -> Result<WmRun, String> {
    let (lanes, retired) = lane_wms_retired(b, cfg).await?;
    let fkey = wm_key(&cfg.ctl);
    let fleet = cas(b, &fkey, with_history(cfg, wall_ms, clock, |prev: &WmDoc| Some(compute_doc(prev, &lanes, wall_ms, cfg)))).await?;
    let mut run = WmRun { fleet, ..Default::default() };
    let (n, c, p) = seal_history(b, cfg, &fkey, wmhistory::FLEET, &run.fleet).await;
    run.hist_sealed += n;
    run.hist_conflicts.extend(c);
    run.hist_pending.extend(p);
    if !cfg.per_cluster {
        return Ok(run);
    }
    let mut by_cluster: BTreeMap<&str, BTreeMap<String, u64>> = BTreeMap::new();
    for (l, w) in &lanes {
        let _ = by_cluster.entry(split_lane(l).0).or_default().insert(l.clone(), *w);
    }
    for (c, ls) in by_cluster {
        // FORMAT.md §1's names: a key segment, never a path.
        if !otap_s3pq::proto::valid_name(c) {
            run.errors.push(format!("cluster {c:?}: not a valid name; no per-cluster document"));
            continue;
        }
        let floor = run.fleet.clusters.get(c).copied().unwrap_or(run.fleet.complete_through_ns);
        // At most one write per `cluster_every_ms`: the fleet document
        // carries each cluster's value every run; the cluster's own adds its
        // per-signal and per-lane values.
        let next = |prev: &ClusterWmDoc| {
            let recent = prev.wall_ms > 0 && wall_ms < prev.wall_ms.saturating_add(cfg.cluster_every_ms);
            (!recent).then(|| compute_cluster(prev, c, &ls, &retired, floor, wall_ms, cfg))
        };
        let ckey = cluster_wm_key(&cfg.ctl, c);
        match cas(b, &ckey, with_history(cfg, wall_ms, clock, next)).await {
            Ok(d) => {
                let (n, cf, p) = seal_history(b, cfg, &ckey, c, &d).await;
                run.hist_sealed += n;
                run.hist_conflicts.extend(cf);
                run.hist_pending.extend(p);
                run.clusters.push(d);
            }
            Err(e) => run.errors.push(e),
        }
    }
    Ok(run)
}

/// One run; returns the fleet document (the per-cluster ones are written
/// too: `watermark_run` reports them).
pub async fn watermark_step<B: Bucket + ?Sized>(b: &B, cfg: &WmConfig, wall_ms: u64) -> Result<WmDoc, String> {
    watermark_run(b, cfg, wall_ms).await.map(|r| r.fleet)
}

#[cfg(test)]
mod tests {
    use super::*;

    const fn ms(x: u64) -> u64 {
        x * 1_000_000
    }

    #[test]
    fn the_minimum_over_lanes_capped_by_the_list_time_and_a_running_max() {
        let cfg = WmConfig { skew_ms: 5_000, stale_ms: 60_000, ..WmConfig::new("r", "c") };
        let mut l = BTreeMap::new();
        let _ = l.insert("c1/p1/traces".to_string(), ms(90_000));
        let _ = l.insert("c1/p1/logs".to_string(), ms(99_000));
        let d = compute(0, &l, 100_000, &cfg);
        assert_eq!((d.computed_ns, d.complete_through_ns, d.list_cap_ns), (ms(90_000), ms(90_000), ms(95_000)));
        assert_eq!(d.holding[0].lane, "c1/p1/traces");
        assert!(d.stale.is_empty());
        // a zombie copy makes a lane dip: the published value holds
        let _ = l.insert("c1/p1/traces".to_string(), ms(80_000));
        let d2 = compute(d.complete_through_ns, &l, 101_000, &cfg);
        assert_eq!((d2.computed_ns, d2.complete_through_ns), (ms(80_000), ms(90_000)));
        // a lane with no watermark yet holds it at 0, and is stale
        let _ = l.insert("c2/p9/traces".to_string(), 0);
        let d3 = compute(0, &l, 200_000, &cfg);
        assert_eq!(d3.computed_ns, 0);
        assert_eq!(d3.stale.iter().map(|x| x.lane.as_str()).collect::<Vec<_>>(), vec!["c2/p9/traces", "c1/p1/traces", "c1/p1/logs"]);
        // no lanes: the list time
        assert_eq!(compute(0, &BTreeMap::new(), 100_000, &cfg).complete_through_ns, ms(95_000));
    }

    /// D29: a cluster's and a signal's values are the minimum over their own
    /// lanes; a stalled cluster holds the fleet value and its own, not
    /// another cluster's; each is a running max floored by the coarser one.
    #[test]
    fn per_cluster_and_per_signal_values_see_only_their_lanes() {
        let cfg = WmConfig { skew_ms: 5_000, stale_ms: 60_000, ..WmConfig::new("r", "c") };
        let mut l = BTreeMap::new();
        let _ = l.insert("c1/p1/traces".to_string(), ms(90_000));
        let _ = l.insert("c1/p1/logs".to_string(), ms(94_000));
        let _ = l.insert("c1/p2/logs".to_string(), ms(92_000));
        let _ = l.insert("c2/p9/traces".to_string(), ms(10_000)); // stalled
        let d = compute_doc(&WmDoc::default(), &l, 100_000, &cfg);
        assert_eq!(d.complete_through_ns, ms(10_000));
        assert_eq!(d.clusters, BTreeMap::from([("c1".into(), ms(90_000)), ("c2".into(), ms(10_000))]));
        assert_eq!(d.signals, BTreeMap::from([("logs".into(), ms(92_000)), ("traces".into(), ms(10_000))]));
        assert_eq!(d.unlisted_signals_ns, ms(95_000), "a signal with no lane: the cap");
        let lanes1: BTreeMap<String, u64> = l.iter().filter(|(k, _)| k.starts_with("c1/")).map(|(k, v)| (k.clone(), *v)).collect();
        let c1 = compute_cluster(&ClusterWmDoc::default(), "c1", &lanes1, &BTreeMap::new(), d.clusters["c1"], 100_000, &cfg);
        assert_eq!((c1.complete_through_ns, c1.lanes), (ms(90_000), 3));
        assert_eq!(c1.signals, BTreeMap::from([("logs".into(), ms(92_000)), ("traces".into(), ms(90_000))]));
        assert_eq!(c1.lane_wm["p1/logs"], ms(94_000));
        assert_eq!(c1.lane_wm["p2/logs"], ms(92_000));
        assert_eq!(c1.holding[0].lane, "c1/p1/traces");
        // the next run: c1's traces lane dips (a zombie copy); every value holds
        let _ = l.insert("c1/p1/traces".to_string(), ms(50_000));
        let _ = l.insert("c1/p1/logs".to_string(), ms(99_000));
        let d2 = compute_doc(&d, &l, 101_000, &cfg);
        assert_eq!((d2.clusters["c1"], d2.signals["traces"], d2.signals["logs"]), (ms(90_000), ms(10_000), ms(92_000)));
        let lanes1: BTreeMap<String, u64> = l.iter().filter(|(k, _)| k.starts_with("c1/")).map(|(k, v)| (k.clone(), *v)).collect();
        let c1b = compute_cluster(&c1, "c1", &lanes1, &BTreeMap::new(), d2.clusters["c1"], 101_000, &cfg);
        assert_eq!((c1b.complete_through_ns, c1b.signals["traces"], c1b.lane_wm["p1/traces"]), (ms(90_000), ms(90_000), ms(90_000)));
        assert_eq!(c1b.lane_wm["p1/logs"], ms(99_000));
        // c2 recovers: the fleet moves to c1's value; c2 takes the fleet as a floor
        let _ = l.insert("c2/p9/traces".to_string(), ms(100_000));
        let d3 = compute_doc(&d2, &l, 102_000, &cfg);
        assert_eq!((d3.complete_through_ns, d3.clusters["c2"]), (ms(50_000), ms(97_000)));
        // a new cluster starts from the previous fleet value, a new signal
        // from the previous unlisted value (its lanes were born after that LIST)
        let _ = l.insert("c3/p0/metrics_series".to_string(), ms(1_000));
        let d4 = compute_doc(&d3, &l, 103_000, &cfg);
        assert_eq!((d4.clusters["c3"], d4.signals["metrics_series"]), (ms(50_000), ms(97_000)));
    }

    /// D35 (FORMAT.md §3.1): a retired lane is +inf in every minimum
    /// (fleet, cluster, signal), never holding or stale; its cluster's
    /// document names it with R, and its `lane_wm` is its signal's value. A
    /// cluster whose every lane is retired follows the cap.
    #[test]
    fn a_retired_lane_is_in_no_minimum() {
        let cfg = WmConfig { skew_ms: 5_000, stale_ms: 60_000, ..WmConfig::new("r", "c") };
        let mut l = BTreeMap::new();
        let _ = l.insert("c1/p0/traces".to_string(), ms(90_000));
        let _ = l.insert("c1/p1/traces".to_string(), RETIRED);
        let _ = l.insert("c1/p1/logs".to_string(), RETIRED);
        let _ = l.insert("c2/p9/logs".to_string(), RETIRED);
        let d = compute_doc(&WmDoc::default(), &l, 100_000, &cfg);
        assert_eq!((d.computed_ns, d.complete_through_ns, d.retired_lanes), (ms(90_000), ms(90_000), 3));
        assert_eq!(d.clusters, BTreeMap::from([("c1".into(), ms(90_000)), ("c2".into(), ms(95_000))]));
        assert_eq!(d.signals, BTreeMap::from([("logs".into(), ms(95_000)), ("traces".into(), ms(90_000))]));
        assert_eq!(d.holding.iter().map(|x| x.lane.as_str()).collect::<Vec<_>>(), vec!["c1/p0/traces"]);
        assert!(d.stale.is_empty());
        let lanes1: BTreeMap<String, u64> = l.iter().filter(|(k, _)| k.starts_with("c1/")).map(|(k, v)| (k.clone(), *v)).collect();
        let r = BTreeMap::from([("c1/p1/traces".to_string(), ms(42_000)), ("c1/p1/logs".to_string(), ms(42_000)), ("c2/p9/logs".to_string(), 7)]);
        let c1 = compute_cluster(&ClusterWmDoc::default(), "c1", &lanes1, &r, d.clusters["c1"], 100_000, &cfg);
        assert_eq!(c1.complete_through_ns, ms(90_000));
        assert_eq!(c1.retired, BTreeMap::from([("p1/logs".to_string(), ms(42_000)), ("p1/traces".to_string(), ms(42_000))]));
        assert_eq!((c1.lane_wm["p1/traces"], c1.lane_wm["p1/logs"]), (ms(90_000), ms(95_000)), "a retired lane's lane_wm is its signal's value");
        assert_eq!(c1.signals["logs"], ms(95_000), "a signal whose lanes are all retired follows the cap");
        // no lane of the cluster counts: the cap
        let lanes2: BTreeMap<String, u64> = l.iter().filter(|(k, _)| k.starts_with("c2/")).map(|(k, v)| (k.clone(), *v)).collect();
        let c2 = compute_cluster(&ClusterWmDoc::default(), "c2", &lanes2, &r, d.clusters["c2"], 100_000, &cfg);
        assert_eq!((c2.complete_through_ns, c2.retired["p9/logs"]), (ms(95_000), 7));
    }

    #[test]
    fn lane_ids_split_into_cluster_producer_and_signal() {
        assert_eq!(split_lane("c1/p1/traces"), ("c1", "p1/traces", "traces"));
        assert_eq!(split_lane("c1/a/b/logs"), ("c1", "a/b/logs", "logs"));
        assert_eq!(cluster_wm_key("r/_consumer", "c1"), "r/_consumer/watermark/c1.json");
    }
}
