//! GC: a separate step (`consume gc`), safe to run at any time, from any
//! number of processes, concurrently with the workers.
//!
//! Every run reads every lane's checkpoint and appends it, with the time, as
//! a *mark* to `{ctl}/gc.json` (CAS'd). It then deletes, per lane and epoch,
//! the data slots below the position of the newest mark that is at least
//! `delay` old: the horizon trails the checkpoints by a lease length plus a
//! request lifetime (../model/S3NATIVE.md, "Readers and GC").
//!
//! - **A lease length:** a worker that lost its lease without knowing it may
//!   still be reading slots above the checkpoint it last saw; those are
//!   above any horizon at least a lease old.
//! - **A request lifetime:** deleting a key re-opens it to `If-None-Match`
//!   (../awss3, "Deleting slots re-opens them"). A slot is deleted only once
//!   it has been ingested for longer than a PUT can stay in flight, so a
//!   late copy of that same PUT can't re-create it. If something re-creates
//!   a slot below a checkpoint anyway, it is never ingested (the consumer
//!   only moves forward) and the next run deletes it.
//! - **Tombstones and the free head of a live epoch are never deleted by
//!   the horizon:** a position is the first slot NOT consumed, and only
//!   slots below it go. A zombie writer's next PUT goes to its head, so the
//!   tombstone there is what halts it. A closed epoch is removed entirely
//!   (tombstone included) only once it has been closed for `zombie_ms`, a
//!   bound on how long a fenced writer process can live.
//!
//! Deleting is idempotent and every horizon is safe on its own, so two GCs
//! racing is harmless; the loser's CAS on gc.json fails and its mark is lost.

use super::bucket::{Bucket, Cond, Put};
use super::coord::{CkptDoc, EpochPos, Lane, join};
use bytes::Bytes;
use otap_s3pq::proto;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Mark {
    pub wall_ms: u64,
    pub lanes: BTreeMap<String, BTreeMap<String, EpochPos>>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct GcDoc {
    pub version: u64,
    pub marks: Vec<Mark>,
    /// Per lane and epoch: every data slot below this is deleted.
    #[serde(default)]
    pub deleted_below: BTreeMap<String, BTreeMap<String, u64>>,
    /// Closed epochs removed entirely.
    #[serde(default)]
    pub retired: BTreeMap<String, BTreeSet<String>>,
}

#[derive(Clone, Debug)]
pub struct GcConfig {
    pub root: String,
    pub ctl: String,
    /// lease ttl + margin + the longest a PUT can be in flight.
    pub delay_ms: u64,
    /// How long a closed epoch (its tombstone) is kept.
    pub zombie_ms: u64,
    pub dry_run: bool,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct GcReport {
    pub lanes: usize,
    pub marks: usize,
    /// Age of the horizon mark (none yet: nothing deleted).
    pub horizon_age_ms: Option<u64>,
    pub deleted_data: usize,
    pub deleted_tombstones: usize,
    pub epochs_retired: usize,
    pub kept_at_or_above_horizon: usize,
    pub cas_conflict: bool,
}

/// The newest mark at least `age_ms` old at `now`.
pub fn horizon(marks: &[Mark], now: u64, age_ms: u64) -> Option<&Mark> {
    marks.iter().filter(|m| m.wall_ms + age_ms <= now).max_by_key(|m| m.wall_ms)
}

/// Which keys of an epoch's LIST to delete under the horizon position `pos`
/// (retire: the whole closed epoch, tombstone included).
pub fn doomed(prefix: &str, keys: &[String], pos: &EpochPos, retire: bool) -> (Vec<String>, usize) {
    let mut out = Vec::new();
    let mut tombs = 0;
    for k in keys {
        if let Some((_, s)) = proto::parse_slot_key(prefix, k) {
            if s < pos.next {
                out.push(k.clone());
            } else if retire && pos.closed && s == pos.next {
                out.push(k.clone());
                tombs += 1;
            }
        }
    }
    (out, tombs)
}

pub async fn gc_step<B: Bucket + ?Sized>(b: &B, cfg: &GcConfig, now: u64) -> Result<GcReport, String> {
    let mut rep = GcReport::default();
    let gkey = join(&cfg.ctl, "gc.json");
    let (mut doc, etag) = match b.get(&gkey).await? {
        Some((body, e)) => (serde_json::from_slice::<GcDoc>(&body).map_err(|e| format!("gc.json: {e}"))?, Some(e)),
        None => (GcDoc::default(), None),
    };
    // Today's positions.
    let cprefix = join(&cfg.ctl, "ckpt");
    let mut mark = Mark { wall_ms: now, lanes: BTreeMap::new() };
    for it in b.list(&cprefix, None).await? {
        let Some(lane) = Lane::from_ctl_key(&cprefix, &it.key) else { continue };
        if let Some((body, _)) = b.get(&it.key).await? {
            let ck: CkptDoc = serde_json::from_slice(&body).map_err(|e| format!("{}: {e}", it.key))?;
            let _ = mark.lanes.insert(lane.id(), ck.epochs);
        }
    }
    rep.lanes = mark.lanes.len();
    doc.marks.push(mark);
    let h = horizon(&doc.marks, now, cfg.delay_ms).cloned();
    let z = horizon(&doc.marks, now, cfg.zombie_ms).cloned();
    rep.horizon_age_ms = h.as_ref().map(|m| now - m.wall_ms);
    if let Some(h) = &h {
        for (lane_id, epochs) in &h.lanes {
            let lane = match lane_id.rsplit_once('/') {
                Some((p, s)) => Lane { producer: p.into(), signal: s.into() },
                None => Lane { producer: String::new(), signal: lane_id.clone() },
            };
            let prefix = lane.data_prefix(&cfg.root);
            for (epoch, pos) in epochs {
                if doc.retired.get(lane_id).is_some_and(|r| r.contains(epoch)) {
                    continue;
                }
                // Closed for at least zombie_ms: the zombie bound mark shows it closed at the same slot.
                let retire = pos.closed
                    && z.as_ref().and_then(|m| m.lanes.get(lane_id)).and_then(|e| e.get(epoch)).is_some_and(|p| p.closed && p.next == pos.next);
                let below = doc.deleted_below.get(lane_id).and_then(|m| m.get(epoch)).copied().unwrap_or(0);
                if pos.next <= below && !retire {
                    continue;
                }
                let keys: Vec<String> = b.list(&join(&prefix, epoch), None).await?.into_iter().map(|i| i.key).collect();
                let (del, tombs) = doomed(&prefix, &keys, pos, retire);
                rep.kept_at_or_above_horizon += keys.len() - del.len();
                if !cfg.dry_run {
                    let _ = b.delete(&del).await?;
                    let _ = doc.deleted_below.entry(lane_id.clone()).or_default().insert(epoch.clone(), pos.next);
                    if retire {
                        let _ = doc.retired.entry(lane_id.clone()).or_default().insert(epoch.clone());
                        rep.epochs_retired += 1;
                    }
                }
                rep.deleted_data += del.len() - tombs;
                rep.deleted_tombstones += tombs;
            }
        }
    }
    // Keep the marks a later run can still use: the zombie-bound one and newer.
    if let Some(z) = &z {
        doc.marks.retain(|m| m.wall_ms >= z.wall_ms);
    }
    rep.marks = doc.marks.len();
    if cfg.dry_run {
        return Ok(rep);
    }
    doc.version += 1;
    let body = Bytes::from(serde_json::to_vec(&doc).expect("gc json"));
    let cond = etag.as_deref().map_or(Cond::Create, Cond::IfMatch);
    rep.cas_conflict = !matches!(b.put(&gkey, body, cond, &BTreeMap::new()).await, Put::Ok(_));
    Ok(rep)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::bucket::MemBucket;

    fn m(w: u64, next: u64, closed: bool) -> Mark {
        let mut e = BTreeMap::new();
        let _ = e.insert("E1".to_string(), EpochPos { next, closed });
        let mut lanes = BTreeMap::new();
        let _ = lanes.insert("p/traces".to_string(), e);
        Mark { wall_ms: w, lanes }
    }

    #[test]
    fn horizon_trails() {
        let marks = vec![m(100, 1, false), m(200, 5, false), m(300, 9, false)];
        assert_eq!(horizon(&marks, 250, 100).map(|m| m.wall_ms), Some(100));
        assert_eq!(horizon(&marks, 300, 100).map(|m| m.wall_ms), Some(200));
        assert!(horizon(&marks, 150, 100).is_none());
        let keys: Vec<String> = (0..6).map(|s| proto::slot_key("r/p/traces", "E1", s)).collect();
        let (d, t) = doomed("r/p/traces", &keys, &EpochPos { next: 3, closed: true }, false);
        assert_eq!((d.len(), t), (3, 0), "the tombstone at 3 stays");
        let (d, t) = doomed("r/p/traces", &keys, &EpochPos { next: 3, closed: true }, true);
        assert_eq!((d.len(), t), (4, 1));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn gc_deletes_behind_the_horizon_only() {
        let b = MemBucket::default();
        let prefix = "r/p/traces";
        for s in 0..6 {
            b.insert(&proto::slot_key(prefix, "E1", s), Bytes::from("x"), BTreeMap::new());
        }
        let mut ck = CkptDoc::new("p/traces");
        ck.advance("E1", 4);
        b.insert("c/ckpt/p/traces.json", Bytes::from(serde_json::to_vec(&ck).unwrap()), BTreeMap::new());
        let cfg = GcConfig { root: "r".into(), ctl: "c".into(), delay_ms: 1000, zombie_ms: 10_000, dry_run: false };
        let r = gc_step(&b, &cfg, 1000).await.unwrap();
        assert_eq!((r.deleted_data, r.horizon_age_ms), (0, None), "no mark is old enough yet");
        // the checkpoint moves on; the horizon is still the first mark (next = 4)
        ck.advance("E1", 6);
        b.insert("c/ckpt/p/traces.json", Bytes::from(serde_json::to_vec(&ck).unwrap()), BTreeMap::new());
        let r = gc_step(&b, &cfg, 2000).await.unwrap();
        assert_eq!(r.deleted_data, 4);
        assert_eq!(b.keys().iter().filter(|k| k.starts_with(prefix)).count(), 2);
        // idempotent, and nothing re-listed for an unchanged position
        let r = gc_step(&b, &cfg, 2500).await.unwrap();
        assert_eq!(r.deleted_data, 0);
        let r = gc_step(&b, &cfg, 3500).await.unwrap();
        assert_eq!(r.deleted_data, 2, "the second mark (next = 6) is old enough now");
        let doc: GcDoc = serde_json::from_slice(&b.get("c/gc.json").await.unwrap().unwrap().0).unwrap();
        assert!(doc.marks.len() >= 2 && doc.version == 4);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn closed_epochs_retire_after_the_zombie_bound() {
        let b = MemBucket::default();
        let prefix = "r/p/traces";
        for s in 0..3 {
            b.insert(&proto::slot_key(prefix, "E1", s), Bytes::from("x"), BTreeMap::new());
        }
        let mut tm = BTreeMap::new();
        let _ = tm.insert(proto::META_KIND.to_string(), proto::KIND_TOMB.to_string());
        b.insert(&proto::slot_key(prefix, "E1", 3), Bytes::new(), tm);
        let mut ck = CkptDoc::new("p/traces");
        ck.advance("E1", 3);
        ck.close("E1", 3);
        b.insert("c/ckpt/p/traces.json", Bytes::from(serde_json::to_vec(&ck).unwrap()), BTreeMap::new());
        let cfg = GcConfig { root: "r".into(), ctl: "c".into(), delay_ms: 100, zombie_ms: 1000, dry_run: false };
        let _ = gc_step(&b, &cfg, 0).await.unwrap();
        let r = gc_step(&b, &cfg, 200).await.unwrap();
        assert_eq!((r.deleted_data, r.deleted_tombstones), (3, 0), "the tombstone stays while a zombie may live");
        let r = gc_step(&b, &cfg, 1200).await.unwrap();
        assert_eq!((r.deleted_tombstones, r.epochs_retired), (1, 1));
        assert!(b.keys().iter().all(|k| !k.starts_with(prefix)));
    }
}
