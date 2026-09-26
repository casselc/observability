//! Coordination on S3 with conditional writes, sans-IO: a lease object and a
//! checkpoint object per lane (one producer's one signal), both CAS'd by
//! ETag (`PUT If-Match`), created with `If-None-Match: *`.
//!
//! **The lease** (`{ctl}/lease/{lane}.json`) carries an owner, a fencing
//! `epoch` (+1 on every change of owner), a `beat` (+1 on every renewal, so
//! its ETag changes) and a TTL. Expiry is judged by the OBSERVER's own
//! monotonic clock, never by comparing wall clocks: a lease whose ETag a
//! worker has seen unchanged for `ttl + margin` is expired. The holder, in
//! turn, counts its time from the moment it SENT the write that installed
//! its current version, so its window always starts before any observer's:
//!
//!   holder:   safe until  sent + ttl − margin
//!   observer: may take at first_seen(version) + ttl + margin  (first_seen ≥ applied ≥ sent)
//!
//! The two windows are 2·margin apart whatever the clocks' offsets; only
//! their rates must agree to within margin / ttl.
//!
//! **The time bound on inserts.** S3 can't fence a ClickHouse INSERT, so a
//! holder starts a statement only if it can finish inside its window:
//! `now + budget ≤ safe_until`, with `max_execution_time = budget` on the
//! statement. A process paused between that check and the send (a GC pause,
//! SIGSTOP) would still send late, so the statement also carries a
//! server-side fence, `WHERE now64(3) <= fence_wall` with
//! `fence_wall = sent_wall + ttl − margin − budget`: evaluated on
//! ClickHouse's clock, it turns a late statement into a no-op. That one
//! needs the holder's and the server's wall clocks within `margin`.
//!
//! **The checkpoint** (`{ctl}/ckpt/{lane}.json`) holds, per epoch, the next
//! slot to ingest and whether a tombstone closed the epoch there. It
//! carries the lease epoch of its writer; a new holder rewrites it at
//! takeover before anything else, so every later CAS by the previous holder
//! fails on the ETag.
//!
//! **Compaction** keeps the checkpoint bounded however many epochs a lane
//! has had. An epoch leaves the explicit set once it is *retired*: closed
//! here (a tombstone at `next`, every slot below it ingested and verified)
//! AND deleted entirely by GC (its tombstone too, which GC does only once
//! the epoch has been closed for the zombie bound). A retired epoch has no
//! key left and no writer that could make one, so nothing about it needs
//! remembering. The `floor` is the highest retired epoch below every epoch
//! still known (in key order): discovery lists from it (`LIST StartAfter
//! {floor}/~`) and ignores anything at or below it. See `CkptDoc::compact`.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LeaseDoc {
    pub lane: String,
    /// "" when released.
    pub owner: String,
    /// The fencing token: +1 on every change of owner (take or release).
    pub epoch: u64,
    /// +1 on every renewal.
    pub beat: u64,
    pub ttl_ms: u64,
    /// The writer's wall clock at the write (informational only).
    pub wall_ms: u64,
}

impl LeaseDoc {
    pub fn released(&self) -> bool {
        self.owner.is_empty()
    }
}

/// A new lease for `me`, replacing `prev` (none: the lane's first lease).
pub fn take(lane: &str, prev: Option<&LeaseDoc>, me: &str, ttl_ms: u64, wall: u64) -> LeaseDoc {
    LeaseDoc {
        lane: lane.to_string(),
        owner: me.to_string(),
        epoch: prev.map_or(1, |p| p.epoch + 1),
        beat: 0,
        ttl_ms,
        wall_ms: wall,
    }
}

pub fn renew(doc: &LeaseDoc, wall: u64) -> LeaseDoc {
    LeaseDoc { beat: doc.beat + 1, wall_ms: wall, ..doc.clone() }
}

/// Gives the lane up (the holder has nothing in flight): anyone may take it at once.
pub fn release(doc: &LeaseDoc, wall: u64) -> LeaseDoc {
    LeaseDoc { owner: String::new(), epoch: doc.epoch + 1, beat: 0, wall_ms: wall, ..doc.clone() }
}

/// A lease this worker holds.
#[derive(Clone, Debug)]
pub struct Held {
    pub doc: LeaseDoc,
    pub etag: String,
    /// Monotonic ms at which the write that installed `doc` was sent.
    pub sent_ms: u64,
    /// Wall ms at the same moment (for the server-side fence).
    pub sent_wall_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Timing {
    pub ttl_ms: u64,
    /// Safety margin on both sides of the window (clock rate, scheduling).
    pub margin_ms: u64,
    /// The longest an INSERT may run (`max_execution_time`).
    pub budget_ms: u64,
    /// How long a statement's commit can outlive `max_execution_time`: on a
    /// replicated table, one Keeper request (`operation_timeout_ms`, 10 s by
    /// default; central-replicated/README.md §A time-bound caveat). The
    /// margin must cover it (`check`), and a statement whose answer was lost
    /// is treated as possibly landing until `Held::settled_by`.
    pub slack_ms: u64,
    /// A deliberate bug, for the model-based test (never set in production).
    pub mutation: Mutation,
}

/// Consumer mutations (../model/s3InlineConsumer.qnt's `noTimeBound`,
/// `noVerify`), as `proto::Mutation` is for the writer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Mutation {
    #[default]
    None,
    /// Workers ignore their lease windows; statements carry no fence.
    NoTimeBound,
    /// The checkpoint moves past a statement's objects without verifying them.
    NoVerify,
    /// Compaction drops every superseded epoch (not the newest known),
    /// closed and retired or not (../model/s3InlineConsumerCompact.qnt's
    /// `earlyCompact`).
    EarlyCompact,
    /// A lane is released (or re-checked) while a statement whose answer was
    /// lost may still land (../model/s3InlineConsumer.qnt's `releaseInFlight`).
    ReleaseInFlight,
    /// The check's partition range comes from the worker's wall clock (today),
    /// not from the objects' data (the model's `wallRange`).
    WallRange,
}

/// The smallest lease margin a production worker accepts: a replicated
/// central's Keeper request can outlive `max_execution_time` by
/// `operation_timeout_ms` (10 s), and the margin must cover that
/// (DECISIONS.md risk 5c). `--allow-short-margin` overrides it (tests).
pub const MIN_MARGIN_MS: u64 = 10_000;

impl Timing {
    /// Sanity: a statement must fit inside a lease with room to renew, and
    /// the margin must cover how long a commit can outlive its time limit.
    ///
    /// Why margin ≥ slack: a statement sent under a lease version written at
    /// `sent` starts on the server by the fence, `sent + ttl − margin −
    /// budget` on the server's clock, which may be up to `margin` behind ours
    /// (the clock assumption); it runs at most `budget` and commits at most
    /// `slack` later: it has landed by `sent + ttl + slack` on our clock. No
    /// other worker may take the lane before `sent + ttl + margin`.
    pub fn check(&self) -> Result<(), String> {
        if self.budget_ms + 2 * self.margin_ms + self.ttl_ms / 3 > self.ttl_ms {
            return Err(format!(
                "lease ttl {} ms is too short for budget {} ms + 2 × margin {} ms + a renewal at ttl/3 (need ttl ≥ {} ms)",
                self.ttl_ms,
                self.budget_ms,
                self.margin_ms,
                (self.budget_ms + 2 * self.margin_ms) * 3 / 2
            ));
        }
        if self.slack_ms > self.margin_ms {
            return Err(format!(
                "lease margin {} ms is shorter than the commit slack {} ms: a statement could land after another worker took the lane",
                self.margin_ms, self.slack_ms
            ));
        }
        Ok(())
    }

    /// `check`, plus the production floor on the margin (`MIN_MARGIN_MS`)
    /// unless `allow_short` (tests with compressed timing).
    pub fn check_production(&self, allow_short: bool) -> Result<(), String> {
        self.check()?;
        if !allow_short && self.margin_ms < MIN_MARGIN_MS {
            return Err(format!(
                "lease margin {} ms is below the minimum {} ms: on a replicated central one Keeper request can \
                 outlive max_execution_time by operation_timeout_ms (10 s), and the margin must cover it \
                 (DECISIONS.md D9, risk 5c). Raise --margin (and --ttl), or pass --allow-short-margin for a test.",
                self.margin_ms, MIN_MARGIN_MS
            ));
        }
        Ok(())
    }

    /// The production defaults: ttl 45 s, margin 10 s, budget 10 s, slack 10 s
    /// (45 = 1.5 × (10 + 2 × 10): a statement plus both margins fit in the
    /// two-thirds of the TTL left after a renewal is due).
    pub const fn production() -> Timing {
        Timing { ttl_ms: 45_000, margin_ms: 10_000, budget_ms: 10_000, slack_ms: 10_000, mutation: Mutation::None }
    }
}

impl Held {
    pub fn safe_until(&self, t: &Timing) -> u64 {
        (self.sent_ms + self.doc.ttl_ms).saturating_sub(t.margin_ms)
    }

    /// May a statement start now and finish (by `budget`) inside the window?
    pub fn may_start(&self, now: u64, t: &Timing) -> bool {
        t.mutation == Mutation::NoTimeBound || now + t.budget_ms <= self.safe_until(t)
    }

    /// Wall-clock deadline ClickHouse checks: rows are read only if the
    /// statement starts by then, and it runs at most `budget` after.
    pub fn fence_wall_ms(&self, t: &Timing) -> u64 {
        if t.mutation == Mutation::NoTimeBound {
            return u64::MAX;
        }
        (self.sent_wall_ms + self.doc.ttl_ms).saturating_sub(t.margin_ms + t.budget_ms)
    }

    pub fn renew_due(&self, now: u64) -> bool {
        now >= self.sent_ms + self.doc.ttl_ms / 3
    }

    /// Past the window: the lease must be treated as lost (even if nobody took it).
    pub fn lapsed(&self, now: u64, t: &Timing) -> bool {
        t.mutation != Mutation::NoTimeBound && now >= self.safe_until(t)
    }

    /// On our monotonic clock, the latest a statement sent under this lease
    /// version can still land: its fence plus the server clock's allowed lag
    /// (margin), its budget and the commit slack: `sent + ttl + slack`. A
    /// statement whose answer was lost (and whose KILL wasn't confirmed) is
    /// treated as possibly landing until then: its lane is neither checked
    /// again nor released before (`may_act`). With `slack ≤ margin` this is
    /// never after the earliest takeover, `sent + ttl + margin`.
    pub fn settled_by(&self, t: &Timing) -> u64 {
        self.sent_ms + self.doc.ttl_ms + t.slack_ms
    }
}

/// May a worker act on a lane again (check it, insert, release it) given the
/// deadline of a statement whose outcome it doesn't know (`None`: none)?
/// The model's rule: a worker acts only once its own statements are gone.
pub fn may_act(unsettled_until: Option<u64>, now: u64, t: &Timing) -> bool {
    t.mutation == Mutation::ReleaseInFlight || unsettled_until.is_none_or(|u| now > u)
}

/// What a worker has observed of the leases it doesn't hold: per lane, the
/// ETag and when it first saw it (monotonic ms).
#[derive(Clone, Debug, Default)]
pub struct Observer {
    seen: HashMap<String, (String, u64)>,
}

impl Observer {
    /// Record the lane's lease ETag as of `now` (None: no lease object).
    pub fn observe(&mut self, lane: &str, etag: Option<&str>, now: u64) {
        match etag {
            None => {
                let _ = self.seen.remove(lane);
            }
            Some(e) => match self.seen.get(lane) {
                Some((old, _)) if old == e => {}
                _ => {
                    let _ = self.seen.insert(lane.to_string(), (e.to_string(), now));
                }
            },
        }
    }

    /// How long this version has been seen unchanged.
    pub fn unchanged_for(&self, lane: &str, etag: &str, now: u64) -> Option<u64> {
        self.seen.get(lane).filter(|(e, _)| e == etag).map(|(_, t)| now.saturating_sub(*t))
    }

    /// May this worker take the lane? A released lease: yes. Otherwise only
    /// once its version has stayed unchanged for ttl + margin on our clock.
    pub fn may_take(&self, lane: &str, etag: &str, doc: &LeaseDoc, now: u64, margin_ms: u64) -> bool {
        doc.released() || self.unchanged_for(lane, etag, now).is_some_and(|d| d >= doc.ttl_ms + margin_ms)
    }
}

/// Lanes per worker: ceil(lanes / live workers), at least 1.
pub fn fair_share(lanes: usize, workers: usize) -> usize {
    lanes.div_ceil(workers.max(1)).max(1)
}

// ---- load-based balancing (sans-IO) -------------------------------------------------
//
// A lane's weight is a base (what holding any lane costs: its LISTs, its
// renewals) plus its recent rows/s (an EWMA). A worker's load is the sum of
// its lanes' weights; the target is the total weight of every known lane
// over the live workers. Inside target × (1 ± h) nothing moves; above it a
// worker gives back one lane per round, the heaviest that doesn't take it
// below the band; below it, it takes free or expired lanes that keep it
// inside the band. Leases are untouched: a take is still `Observer::may_take`
// plus the lease CAS and the checkpoint fence, and a release (`release`)
// happens only once no statement of the lane can still land (`may_act`).

/// An exponentially weighted moving sum; `rate` is per second over `window_ms`.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Ewma {
    v: f64,
    at: u64,
}

impl Ewma {
    fn decayed(&self, now: u64, window_ms: u64) -> f64 {
        if window_ms == 0 {
            return self.v;
        }
        self.v * (-(now.saturating_sub(self.at) as f64) / window_ms as f64).exp()
    }
    pub fn add(&mut self, x: f64, now: u64, window_ms: u64) {
        self.v = self.decayed(now, window_ms) + x;
        self.at = self.at.max(now);
    }
    pub fn rate(&self, now: u64, window_ms: u64) -> f64 {
        self.decayed(now, window_ms) / (window_ms.max(1) as f64 / 1000.0)
    }
    /// An EWMA that currently reads `rate` (a lane taken over: its previous holder's figure).
    pub fn seeded(rate: f64, now: u64, window_ms: u64) -> Ewma {
        Ewma { v: rate * window_ms as f64 / 1000.0, at: now }
    }
}

/// The target load per worker: the water level. A lane heavier than an
/// even split can't be split, so it gets a worker to itself and leaves the
/// split: repeatedly, the heaviest lane above (remaining weight / remaining
/// workers) is set aside with one worker. The rest share what is left.
pub fn load_target(weights: &[f64], workers: usize) -> f64 {
    let mut w: Vec<f64> = weights.to_vec();
    w.sort_by(|a, b| b.total_cmp(a));
    let (mut total, mut n) = (w.iter().sum::<f64>(), workers.max(1));
    for x in &w {
        if n > 1 && *x > total / n as f64 {
            total -= x;
            n -= 1;
        } else {
            break;
        }
    }
    total / n as f64
}

/// Which held lane to give back, if any: `mine` is (lane, weight,
/// releasable now). Only above the band, never the last lane, and never one
/// that would leave this worker below the band (that lane would come back).
pub fn pick_release(mine: &[(String, f64, bool)], target: f64, h: f64) -> Option<String> {
    let load: f64 = mine.iter().map(|m| m.1).sum();
    if mine.len() <= 1 || load <= target * (1.0 + h) {
        return None;
    }
    mine.iter()
        .filter(|m| m.2 && load - m.1 >= target * (1.0 - h))
        .max_by(|a, b| a.1.total_cmp(&b.1).then_with(|| b.0.cmp(&a.0)))
        .map(|m| m.0.clone())
}

/// Whether a worker at `load` takes an unheld lane of weight `w`: if it
/// stays inside the band; or, if it is the least loaded live worker (so
/// some worker always takes a lane nobody holds), while below the target;
/// or if the lane has been unheld too long (stale load figures).
pub fn take_by_load(load: f64, w: f64, target: f64, h: f64, least: bool, orphaned: bool) -> bool {
    load + w <= target * (1.0 + h) || (least && load < target) || orphaned
}

// ---- the checkpoint ------------------------------------------------------------

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct EpochPos {
    /// The next slot to ingest (every slot below it is ingested or a copy).
    pub next: u64,
    /// A tombstone sits at `next`: the epoch is over.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub closed: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct CkptDoc {
    pub lane: String,
    /// The lease epoch of the worker that wrote it.
    pub lease_epoch: u64,
    /// +1 per write.
    pub version: u64,
    /// Every epoch at or below it (in key order, `above_floor`) is retired:
    /// closed, ingested and verified up to its tombstone, and deleted by GC.
    /// Discovery starts after it. "" (the default): none.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub floor: String,
    /// The epochs not retired yet (and never one at or below the floor).
    pub epochs: BTreeMap<String, EpochPos>,
}

/// Whether `epoch` is above `floor` in key order: `{epoch}/` sorts after
/// `{floor}/~`, the StartAfter a full listing uses. (For names of one
/// length, as the edges mint them, this is plain string order.)
pub fn above_floor(epoch: &str, floor: &str) -> bool {
    floor.is_empty() || epoch_key(epoch) > format!("{floor}/~")
}

/// An epoch's position in LIST order.
pub fn epoch_key(epoch: &str) -> String {
    format!("{epoch}/")
}

/// The StartAfter that lists every key of the epochs above `floor`.
pub fn floor_start_after(prefix: &str, floor: &str) -> Option<String> {
    (!floor.is_empty()).then(|| format!("{}/~", join(prefix, floor)))
}

impl CkptDoc {
    pub fn new(lane: &str) -> Self {
        Self { lane: lane.to_string(), ..Default::default() }
    }

    /// Checkpoint compaction (run after a full listing of the lane).
    ///
    /// `known`: every epoch above the floor the worker knows of (the
    /// listing's and the checkpoint's); `retired`: the epochs GC has deleted
    /// entirely (gc.json). Drops each explicit epoch that is closed here AND
    /// retired, and moves the floor up to the highest dropped epoch that is
    /// below every epoch left. Returns the dropped epochs.
    ///
    /// Why this is safe (README "Checkpoint compaction"):
    /// - closed at `next` means every slot below the tombstone was ingested
    ///   and verified (the checkpoint only moves past verified slots);
    /// - retired means GC deleted every key, tombstone included, and did so
    ///   only once the epoch had been closed for the zombie bound, so no
    ///   writer of it is left to write again;
    /// - so the epoch has nothing to ingest, and nothing to list: forgetting
    ///   it loses nothing. The floor only passes a contiguous run of such
    ///   epochs, so a listing from it misses only them;
    /// - a NEW epoch is minted by its edge at its first write, with a
    ///   timestamped name, so it sorts above every epoch closed by then
    ///   unless the producer's clock stepped back by more than the zombie
    ///   bound (see the README for what that costs).
    pub fn compact<'a>(
        &mut self,
        known: impl IntoIterator<Item = &'a String>,
        retired: &std::collections::BTreeSet<String>,
        mutation: Mutation,
    ) -> Vec<String> {
        let mut all: std::collections::BTreeSet<String> =
            known.into_iter().filter(|e| above_floor(e, &self.floor)).cloned().collect();
        all.extend(self.epochs.keys().cloned());
        let newest = all.iter().max_by_key(|e| epoch_key(e)).cloned();
        let drop: Vec<String> = match mutation {
            Mutation::EarlyCompact => all.iter().filter(|e| Some(*e) != newest.as_ref()).cloned().collect(),
            _ => self.epochs.iter().filter(|(e, p)| p.closed && retired.contains(*e)).map(|(e, _)| e.clone()).collect(),
        };
        if drop.is_empty() {
            return drop;
        }
        for e in &drop {
            let _ = self.epochs.remove(e);
        }
        // The floor: the highest dropped epoch below every epoch left.
        let low = all.iter().filter(|e| !drop.contains(e)).map(|e| epoch_key(e)).min();
        if let Some(f) = drop.iter().filter(|e| low.as_ref().is_none_or(|l| epoch_key(e) < *l)).max_by_key(|e| epoch_key(e)) {
            if above_floor(f, &self.floor) {
                self.floor = f.clone();
            }
        }
        drop
    }
    pub fn next(&self, epoch: &str) -> u64 {
        self.epochs.get(epoch).map_or(0, |p| p.next)
    }
    pub fn closed(&self, epoch: &str) -> bool {
        self.epochs.get(epoch).is_some_and(|p| p.closed)
    }
    /// Moves an epoch's position forward (never back).
    pub fn advance(&mut self, epoch: &str, next: u64) {
        let p = self.epochs.entry(epoch.to_string()).or_default();
        debug_assert!(!p.closed || next <= p.next, "no progress past a tombstone");
        p.next = p.next.max(next);
    }
    pub fn close(&mut self, epoch: &str, at: u64) {
        let p = self.epochs.entry(epoch.to_string()).or_default();
        debug_assert_eq!(p.next, at, "a tombstone is closed at the checkpoint");
        p.closed = true;
    }
    /// The next version, written by the holder of lease epoch `lease_epoch`.
    pub fn bumped(&self, lease_epoch: u64) -> CkptDoc {
        CkptDoc { lease_epoch, version: self.version + 1, ..self.clone() }
    }
}

// ---- lane names and keys ---------------------------------------------------------

/// A lane: one producer's one signal, `{root}/{producer}/{signal}`
/// (or `{root}/{signal}` when producers aren't separated by prefix).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lane {
    pub producer: String,
    pub signal: String,
}

impl Lane {
    pub fn id(&self) -> String {
        if self.producer.is_empty() { self.signal.clone() } else { format!("{}/{}", self.producer, self.signal) }
    }
    pub fn data_prefix(&self, root: &str) -> String {
        join(root, &self.id())
    }
    pub fn lease_key(&self, ctl: &str) -> String {
        format!("{}.json", join(&join(ctl, "lease"), &self.id()))
    }
    pub fn ckpt_key(&self, ctl: &str) -> String {
        format!("{}.json", join(&join(ctl, "ckpt"), &self.id()))
    }
    /// The inverse of `lease_key` / `ckpt_key`, given the kind's prefix.
    pub fn from_ctl_key(prefix: &str, key: &str) -> Option<Lane> {
        let rest = key.strip_prefix(prefix.trim_end_matches('/'))?.strip_prefix('/')?.strip_suffix(".json")?;
        Some(match rest.rsplit_once('/') {
            Some((p, s)) => Lane { producer: p.to_string(), signal: s.to_string() },
            None => Lane { producer: String::new(), signal: rest.to_string() },
        })
    }
}

pub fn join(a: &str, b: &str) -> String {
    let a = a.trim_end_matches('/');
    if a.is_empty() { b.to_string() } else { format!("{a}/{b}") }
}

/// A random suffix (the worker's incarnation).
pub fn nonce() -> String {
    format!("{:08x}", rand::random::<u32>())
}

#[cfg(test)]
mod tests {
    use super::*;

    const T: Timing = Timing { ttl_ms: 9000, margin_ms: 1000, budget_ms: 3000, slack_ms: 1000, mutation: Mutation::None };

    #[test]
    fn windows_never_overlap() {
        // The holder sent its write at mono 100 (its clock); the store applied
        // it at some later time; an observer first saw it at its own mono 5000.
        let doc = take("p/traces", None, "w1", T.ttl_ms, 1_000_000);
        let h = Held { doc: doc.clone(), etag: "e1".into(), sent_ms: 100, sent_wall_ms: 1_000_000 };
        assert_eq!(h.safe_until(&T), 100 + 9000 - 1000);
        assert!(h.may_start(100 + 5000, &T));
        assert!(!h.may_start(100 + 5001, &T), "a statement must end by safe_until");
        assert_eq!(h.fence_wall_ms(&T), 1_000_000 + 9000 - 4000);
        let mut o = Observer::default();
        o.observe("p/traces", Some("e1"), 5000);
        assert!(!o.may_take("p/traces", "e1", &doc, 5000 + 9999, T.margin_ms));
        assert!(o.may_take("p/traces", "e1", &doc, 5000 + 10_000, T.margin_ms));
        // Even if the observer saw it at the instant it was sent (same clock),
        // its takeover comes 2 × margin after the holder's window closed.
        let mut o2 = Observer::default();
        o2.observe("p/traces", Some("e1"), 100);
        let first_take = 100 + T.ttl_ms + T.margin_ms;
        assert!(o2.may_take("p/traces", "e1", &doc, first_take, T.margin_ms));
        assert_eq!(first_take - h.safe_until(&T), 2 * T.margin_ms);
    }

    #[test]
    fn a_renewal_restarts_the_observer() {
        let doc = take("l", None, "w1", T.ttl_ms, 0);
        let mut o = Observer::default();
        o.observe("l", Some("e1"), 0);
        o.observe("l", Some("e1"), 8000);
        o.observe("l", Some("e2"), 9000); // renewed
        assert!(!o.may_take("l", "e2", &doc, 12_000, T.margin_ms));
        assert!(o.may_take("l", "e2", &doc, 19_000, T.margin_ms));
        // an etag the observer never saw can't be judged expired
        assert!(!o.may_take("l", "e3", &doc, 1_000_000, T.margin_ms));
        // released: at once
        let r = release(&doc, 0);
        assert!(r.released() && r.epoch == 2);
        assert!(o.may_take("l", "e9", &r, 0, T.margin_ms));
        assert_eq!(take("l", Some(&r), "w2", 1, 0).epoch, 3);
    }

    #[test]
    fn timing_checks() {
        assert!(T.check().is_ok());
        assert!(Timing { ttl_ms: 3000, margin_ms: 500, budget_ms: 2000, slack_ms: 0, mutation: Mutation::None }.check().is_err());
        // the margin must cover the commit slack
        let e = Timing { slack_ms: 1001, ..T }.check().unwrap_err();
        assert!(e.contains("shorter than the commit slack"), "{e}");
        // the production floor: refused below 10 s unless overridden
        let e = T.check_production(false).unwrap_err();
        assert!(e.contains("below the minimum 10000 ms") && e.contains("--allow-short-margin"), "{e}");
        assert!(T.check_production(true).is_ok());
        // the defaults comply; the old ones (30 s / 2 s / 10 s) don't
        assert!(Timing::production().check_production(false).is_ok());
        let old = Timing { ttl_ms: 30_000, margin_ms: 2_000, budget_ms: 10_000, slack_ms: 2_000, mutation: Mutation::None };
        assert!(old.check().is_ok() && old.check_production(false).is_err());
        // D9's "TTL 30 s and margin ≥ 10 s" doesn't fit a 10 s budget
        let d9 = Timing { ttl_ms: 30_000, margin_ms: 10_000, ..Timing::production() };
        assert!(d9.check().unwrap_err().contains("need ttl ≥ 45000 ms"));
        // a lost statement has settled before anyone may take the lane over
        let h = Held { doc: take("l", None, "w", T.ttl_ms, 0), etag: "e".into(), sent_ms: 100, sent_wall_ms: 0 };
        assert!(h.settled_by(&T) <= 100 + T.ttl_ms + T.margin_ms);
        assert!(!may_act(Some(h.settled_by(&T)), h.settled_by(&T), &T) && may_act(Some(h.settled_by(&T)), h.settled_by(&T) + 1, &T));
        assert!(may_act(None, 0, &T));
        assert_eq!(fair_share(7, 3), 3);
        assert_eq!(fair_share(0, 3), 1);
        assert_eq!(fair_share(4, 0), 4);
    }

    #[test]
    fn load_balancing_decisions() {
        let l = |n: &str, w: f64| (n.to_string(), w, true);
        // one heavy lane and small ones: the heavy lane is never given back
        // alone, and a release never drops the worker below the band
        assert_eq!(pick_release(&[l("h", 100.0)], 55.0, 0.2), None, "the last lane stays");
        assert_eq!(pick_release(&[l("h", 100.0), l("s", 10.0)], 55.0, 0.2).as_deref(), Some("s"));
        assert_eq!(pick_release(&[l("a", 30.0), l("b", 30.0)], 55.0, 0.2), None, "inside the band");
        assert_eq!(pick_release(&[l("a", 40.0), l("b", 30.0), l("c", 5.0)], 50.0, 0.2).as_deref(), Some("b"));
        assert_eq!(pick_release(&[("a".into(), 40.0, false), l("b", 40.0)], 50.0, 0.2).as_deref(), Some("b"), "not releasable: kept");
        assert_eq!(pick_release(&[("a".into(), 40.0, false), ("b".into(), 40.0, false)], 50.0, 0.2), None);
        // the target sets a lane heavier than an even split aside with its own worker
        assert_eq!(load_target(&[4001.0, 26.0, 26.0, 26.0, 26.0], 3), 52.0);
        assert_eq!(load_target(&[10.0, 10.0, 10.0], 3), 10.0);
        assert_eq!(load_target(&[100.0, 100.0, 1.0], 2), 100.5);
        assert_eq!(load_target(&[5.0], 0), 5.0);
        // takes: inside the band; the least loaded below the target; an orphan always
        assert!(take_by_load(30.0, 20.0, 50.0, 0.2, false, false));
        assert!(!take_by_load(50.0, 20.0, 50.0, 0.2, false, false));
        assert!(take_by_load(40.0, 100.0, 50.0, 0.2, true, false));
        assert!(!take_by_load(50.0, 100.0, 50.0, 0.2, true, false));
        assert!(take_by_load(90.0, 100.0, 50.0, 0.2, false, true));
        // the moving rate
        let w = 10_000;
        let mut e = Ewma::default();
        for t in 0..600 {
            e.add(100.0, t * 100, w); // 1,000 rows/s for a minute
        }
        assert!((e.rate(60_000, w) - 1000.0).abs() < 25.0, "{}", e.rate(60_000, w));
        assert!(e.rate(60_000 + 5 * w, w) < 10.0, "decays when idle");
        let s = Ewma::seeded(250.0, 7, w);
        assert!((s.rate(7, w) - 250.0).abs() < 1e-9);
    }

    #[test]
    fn checkpoint_moves_forward_only() {
        let mut c = CkptDoc::new("l");
        c.advance("E1", 3);
        c.advance("E1", 2);
        assert_eq!(c.next("E1"), 3);
        c.close("E1", 3);
        assert!(c.closed("E1") && !c.closed("E2"));
        let c2 = c.bumped(7);
        assert_eq!((c2.version, c2.lease_epoch), (1, 7));
        let j = serde_json::to_string(&c2).unwrap();
        assert_eq!(serde_json::from_str::<CkptDoc>(&j).unwrap(), c2);
    }

    fn set(xs: &[&str]) -> std::collections::BTreeSet<String> {
        xs.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn compaction_drops_closed_and_retired_epochs_only() {
        let mut c = CkptDoc::new("l");
        for (e, n) in [("E1", 3), ("E2", 2), ("E3", 5), ("E4", 1), ("E6", 4)] {
            c.advance(e, n);
        }
        c.close("E1", 3);
        c.close("E2", 2);
        c.close("E4", 1);
        c.close("E6", 4);
        // E5: listed, no entry yet. E3: open. E6: closed, not retired yet.
        let known = set(&["E1", "E2", "E3", "E4", "E5", "E6"]);
        let d = c.compact(&known, &set(&["E1", "E2", "E4", "E3x"]), Mutation::None);
        assert_eq!(d, vec!["E1", "E2", "E4"], "closed and retired");
        // The floor stops below the open E3; E4 (retired above it) goes anyway.
        assert_eq!(c.floor, "E2");
        assert_eq!(c.epochs.keys().cloned().collect::<Vec<_>>(), vec!["E3", "E6"]);
        assert!(c.compact(&known, &set(&["E1", "E2", "E4"]), Mutation::None).is_empty(), "idempotent");
        // E3 closes and retires. The worker now knows E3, E5 (listed, no
        // entry) and E6: the floor passes E3 and stops below E5; E6 goes too.
        c.close("E3", 5);
        let d = c.compact(&set(&["E3", "E5", "E6"]), &set(&["E3", "E6"]), Mutation::None);
        assert_eq!((d, c.floor.as_str()), (vec!["E3".to_string(), "E6".to_string()], "E3"));
        assert!(c.epochs.is_empty());
        // At or below the floor: ignored; the floor never moves back.
        assert!(!above_floor("E3", "E3") && !above_floor("E1", "E3") && above_floor("E5", "E3") && above_floor("E1", ""));
        let d = c.compact(&set(&["E1", "E5"]), &set(&["E1"]), Mutation::None);
        assert!(d.is_empty() && c.floor == "E3");
        assert_eq!(floor_start_after("r/p/t", "E3").as_deref(), Some("r/p/t/E3/~"));
        assert_eq!(floor_start_after("r/p/t", ""), None);
        // Key order, not string order: `E1-x/` sorts before `E1/~`, as in a LIST.
        assert!(!above_floor("E1-x", "E1") && above_floor("E10", "E1"));
        // The mutant drops every epoch but the newest, open or not.
        let mut m = CkptDoc::new("l");
        m.advance("E1", 2);
        m.advance("E2", 1);
        let d = m.compact(&set(&["E1", "E2"]), &set(&[]), Mutation::EarlyCompact);
        assert_eq!((d, m.floor.as_str()), (vec!["E1".to_string()], "E1"));
        // (serde: an old checkpoint without a floor reads; an empty floor isn't written)
        let old: CkptDoc = serde_json::from_str(r#"{"lane":"l","lease_epoch":1,"version":2,"epochs":{}}"#).unwrap();
        assert!(old.floor.is_empty() && !serde_json::to_string(&old).unwrap().contains("floor"));
    }

    #[test]
    fn lane_keys() {
        let l = Lane { producer: "edge-1".into(), signal: "traces".into() };
        assert_eq!(l.data_prefix("r/edges"), "r/edges/edge-1/traces");
        assert_eq!(l.lease_key("r/ctl"), "r/ctl/lease/edge-1/traces.json");
        assert_eq!(Lane::from_ctl_key("r/ctl/lease", &l.lease_key("r/ctl")), Some(l.clone()));
        let flat = Lane { producer: String::new(), signal: "logs".into() };
        assert_eq!(flat.ckpt_key("c"), "c/ckpt/logs.json");
        assert_eq!(Lane::from_ctl_key("c/ckpt", "c/ckpt/logs.json"), Some(flat));
    }
}
