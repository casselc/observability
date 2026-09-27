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

use super::bucket::{Bucket, Cond, Put};
use super::coord::{CkptDoc, Lane, join};
use super::sql::LaneKind;
use super::worker::list_lane_parents;
use bytes::Bytes;
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
}

impl WmConfig {
    pub fn new(root: &str, ctl: &str) -> Self {
        WmConfig { root: root.into(), ctl: ctl.into(), depth: 3, skew_ms: 5_000, stale_ms: 300_000, holding: 5 }
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
}

pub fn wm_key(ctl: &str) -> String {
    join(ctl, "watermark.json")
}

/// The pure part: `lanes` is every listed lane's watermark (0: none yet).
pub fn compute(prev: u64, lanes: &BTreeMap<String, u64>, wall_ms: u64, cfg: &WmConfig) -> WmDoc {
    let cap = wall_ms.saturating_sub(cfg.skew_ms).saturating_mul(1_000_000);
    let computed = lanes.values().copied().fold(cap, u64::min);
    let lag = |wm: u64| (wall_ms as f64 / 1e3 - wm as f64 / 1e9).max(0.0);
    let mut by: Vec<LaneWm> = lanes.iter().map(|(l, w)| LaneWm { lane: l.clone(), wm_ns: *w, lag_s: lag(*w) }).collect();
    by.sort_by(|a, b| a.wm_ns.cmp(&b.wm_ns).then(a.lane.cmp(&b.lane)));
    let stale = by.iter().filter(|l| l.lag_s * 1e3 > cfg.stale_ms as f64).cloned().collect();
    WmDoc {
        format: otap_s3pq::proto::FORMAT_VERSION,
        version: 0,
        complete_through_ns: prev.max(computed),
        computed_ns: computed,
        wall_ms,
        list_cap_ns: cap,
        lanes: lanes.len(),
        holding: by.into_iter().take(cfg.holding).collect(),
        stale,
        stale_after_s: cfg.stale_ms / 1000,
    }
}

/// Every lane the data root shows, with its checkpoint's watermark.
pub async fn lane_wms<B: Bucket + ?Sized>(b: &B, cfg: &WmConfig) -> Result<BTreeMap<String, u64>, String> {
    let mut out = BTreeMap::new();
    for p in list_lane_parents(b, &cfg.root, cfg.depth).await? {
        let dir = if p.is_empty() { cfg.root.clone() } else { join(&cfg.root, &p) };
        for s in b.list_dirs(&dir).await? {
            if s.starts_with('_') || LaneKind::for_signal(&s).is_none() {
                continue;
            }
            let lane = Lane { producer: p.clone(), signal: s };
            let wm = match b.get(&lane.ckpt_key(&cfg.ctl)).await? {
                Some((body, _)) => serde_json::from_slice::<CkptDoc>(&body).map_err(|e| format!("{}: {e}", lane.id()))?.wm_ns,
                None => 0,
            };
            let _ = out.insert(lane.id(), wm);
        }
    }
    Ok(out)
}

/// One run: compute and publish. `wall_ms` is read before the LIST.
pub async fn watermark_step<B: Bucket + ?Sized>(b: &B, cfg: &WmConfig, wall_ms: u64) -> Result<WmDoc, String> {
    let lanes = lane_wms(b, cfg).await?;
    let key = wm_key(&cfg.ctl);
    for _ in 0..4 {
        let (prev, etag) = match b.get(&key).await? {
            Some((body, e)) => (serde_json::from_slice::<WmDoc>(&body).map_err(|e| format!("{key}: {e}"))?, Some(e)),
            None => (WmDoc::default(), None),
        };
        let mut doc = compute(prev.complete_through_ns, &lanes, wall_ms, cfg);
        doc.version = prev.version + 1;
        let body = Bytes::from(serde_json::to_vec(&doc).expect("json"));
        let cond = etag.as_deref().map_or(Cond::Create, Cond::IfMatch);
        match b.put(&key, body, cond, &BTreeMap::new()).await {
            Put::Ok(_) => return Ok(doc),
            // Another publisher wrote first, or the answer was lost: read
            // again and take the max over what is there (idempotent).
            Put::Conflict | Put::Unknown(_) => continue,
        }
    }
    Err(format!("{key}: no CAS won in 4 tries"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_minimum_over_lanes_capped_by_the_list_time_and_a_running_max() {
        let cfg = WmConfig { skew_ms: 5_000, stale_ms: 60_000, ..WmConfig::new("r", "c") };
        let ms = |x: u64| x * 1_000_000;
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
}
