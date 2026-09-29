//! Dead-lane retirement (`../../FORMAT.md` §3.1, DECISIONS.md D35,
//! `../../model/retirement.qnt`): the quarantine document, written by a
//! lane's holder before its checkpoint passes a quarantined slot, and
//! `consume retire-lane` (`retire_lane`), an operator's retirement of a lane
//! whose publisher died without a close.
//!
//! - **Retirement by an orderly close** is the worker's (`worker.rs`
//!   `advance`, `CkptDoc::close_proof`).
//! - **Quarantine.** An object of a retired lane received below the bound R
//!   (`CkptDoc::quarantines`) that central does not already hold is never
//!   inserted: it is recorded in `{ctl}/quarantine/{lane}.json` (slot,
//!   content key, rows, received_at), counted
//!   (`consumer_quarantined_objects_total`, paged), and its slot is passed.
//!   An object below R that central already holds (a copy: an adopted
//!   volume replaying what was committed before the close) is passed like
//!   any copy.

use super::bucket::{Bucket, Cond, Put};
use super::coord::join;
use super::plan::Obj;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// One quarantined object.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Quarantined {
    /// The slot's key, epoch and seq.
    pub key: String,
    pub epoch: String,
    pub seq: u64,
    /// `oscope-content`, `oscope-rows`, `oscope-received`.
    pub content: String,
    pub rows: u64,
    pub received_ns: u64,
    /// When it was quarantined (wall ms).
    pub at_wall_ms: u64,
    /// `consume admit`: when it was admitted into the recovered table (wall
    /// ms; 0: not yet), and the rows that table holds for it.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub admitted_wall_ms: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub admitted_rows: u64,
}

fn is_zero(x: &u64) -> bool {
    *x == 0
}

/// `{ctl}/quarantine/{lane}.json`.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct QuarantineDoc {
    /// `{cluster}/{producer}/{signal}`.
    pub lane: String,
    /// +1 per write.
    pub version: u64,
    /// The lane's R when the last object was added (ns).
    pub retired_ns: u64,
    /// In slot order of arrival; one entry per slot.
    pub objects: Vec<Quarantined>,
}

pub fn quarantine_key(ctl: &str, lane: &str) -> String {
    join(ctl, &format!("quarantine/{lane}.json"))
}

pub fn quarantine_prefix(ctl: &str) -> String {
    join(ctl, "quarantine")
}

/// Adds `objs` (one lane's) to its quarantine document by CAS, once per
/// slot (a retry, or another holder's earlier write, adds nothing).
/// Returns the objects the document now holds that were not there before.
pub async fn record<B: Bucket + ?Sized>(b: &B, ctl: &str, lane: &str, retired_ns: u64, objs: &[Obj], wall_ms: u64) -> Result<usize, String> {
    let key = quarantine_key(ctl, lane);
    for _ in 0..4 {
        let (mut doc, etag) = match b.get(&key).await? {
            Some((body, e)) => (serde_json::from_slice::<QuarantineDoc>(&body).map_err(|e| format!("{key}: {e}"))?, Some(e)),
            None => (QuarantineDoc { lane: lane.to_string(), ..Default::default() }, None),
        };
        let have: std::collections::BTreeSet<(String, u64)> = doc.objects.iter().map(|q| (q.epoch.clone(), q.seq)).collect();
        let new: Vec<Quarantined> = objs
            .iter()
            .filter(|o| !have.contains(&(o.epoch.clone(), o.seq)))
            .map(|o| Quarantined {
                key: o.key.clone(),
                epoch: o.epoch.clone(),
                seq: o.seq,
                content: o.content.clone(),
                rows: o.rows,
                received_ns: o.received_ns,
                at_wall_ms: wall_ms,
                ..Default::default()
            })
            .collect();
        if new.is_empty() {
            return Ok(0);
        }
        let n = new.len();
        doc.objects.extend(new);
        doc.retired_ns = doc.retired_ns.max(retired_ns);
        doc.version += 1;
        let body = Bytes::from(serde_json::to_vec_pretty(&doc).expect("json"));
        let cond = etag.as_deref().map_or(Cond::Create, Cond::IfMatch);
        match b.put(&key, body, cond, &BTreeMap::new()).await {
            Put::Ok(_) => return Ok(n),
            // Lost or raced: read again; the slots already there are skipped.
            Put::Conflict | Put::Unknown(_) => continue,
        }
    }
    Err(format!("{key}: no CAS won in 4 tries"))
}

/// Reads a lane's quarantine document (None: none).
pub async fn read<B: Bucket + ?Sized>(b: &B, ctl: &str, lane: &str) -> Result<Option<(QuarantineDoc, String)>, String> {
    let key = quarantine_key(ctl, lane);
    match b.get(&key).await? {
        Some((body, e)) => Ok(Some((serde_json::from_slice(&body).map_err(|e| format!("{key}: {e}"))?, e))),
        None => Ok(None),
    }
}

// ---- consume retire-lane: an operator's retirement, with evidence ----------------------

/// `consume retire-lane`'s settings.
#[derive(Clone, Debug)]
pub struct RetireCfg {
    pub root: String,
    pub ctl: String,
    /// GC's zombie bound (`--zombie`): no PUT of a process gone this long can
    /// still land. Must exceed the edges' heartbeat interval (a live
    /// publisher writes at least that often).
    pub zombie_ms: u64,
    /// `Mutation::RetireInFlight` skips checks (b) and (c) and the seal
    /// (the model's `retireInFlight`, which has no tombstone; simulation
    /// only). The seal alone would stop a zombie PUT at the epoch's head:
    /// the implementation has both defenses.
    pub mutation: super::coord::Mutation,
    /// Check only: run (a), (b) and (c) and report, write nothing.
    pub dry_run: bool,
}

/// What a retirement did (printed, and kept in `{ctl}/retired/…`).
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Retirement {
    pub lane: String,
    /// R (ns): the retirement's time, after the zombie bound.
    pub r_ns: u64,
    /// The epoch retired (the lane's newest).
    pub epoch: String,
    pub evidence: String,
    /// The operator's attestation (`--volume-deleted`): the publisher's
    /// volume is deleted, its custody lost.
    pub volume_deleted: bool,
    pub wall_ms: u64,
    /// The lane's newest object: when it was written (the store's clock,
    /// ms), its kind and its `oscope-low`. Every request of the dead
    /// publisher received at or after that low may be among the ones lost
    /// with its volume.
    pub last_object_ms: u64,
    pub last_kind: String,
    pub last_low_ns: u64,
    /// Tombstones written at the heads of the epochs left open.
    pub tombstones: Vec<String>,
    /// Who ran it (`$USER`), for the record.
    pub by: String,
}

pub fn retired_record_key(ctl: &str, lane: &str, wall_ms: u64) -> String {
    join(ctl, &format!("retired/{lane}/{wall_ms}.json"))
}

/// `consume retire-lane` (`../../FORMAT.md` §3.1, DECISIONS.md D35): retires
/// the lane of a publisher that died without a close, at R = now, only if
///
/// - (a) the operator attests that its volume is deleted (`volume_deleted`)
///   and gives the evidence (recorded in the checkpoint and in
///   `{ctl}/retired/{lane}/{wall_ms}.json`); the tool cannot see Kubernetes;
/// - (b) the lane's newest object is older than `zombie_ms` (the store's
///   LastModified against this clock): no PUT of the dead process can still
///   land, and R is strictly after its death;
/// - (c) the lane's checkpoint has passed every slot the lane shows (a
///   zombie PUT that landed before is ingested, not quarantined).
///
/// Then it tombstones the head of every epoch the checkpoint has not closed
/// (as the consumer does for a superseded epoch: nothing of it can land
/// after), and records the retirement in the checkpoint by CAS. `Err` is a
/// refusal (nothing written, or tombstones only), with the reason.
pub async fn retire_lane<B: Bucket + ?Sized>(
    b: &B,
    cfg: &RetireCfg,
    lane_id: &str,
    evidence: &str,
    volume_deleted: bool,
    now_ms: u64,
) -> Result<Retirement, String> {
    use super::coord::{CkptDoc, Lane, Mutation, above_floor, epoch_key, floor_start_after};
    let parts: Vec<&str> = lane_id.split('/').collect();
    let [cluster, producer, signal] = parts[..] else {
        return Err(format!("refused: --lane {lane_id:?}: want {{cluster}}/{{producer}}/{{signal}}"));
    };
    if !otap_s3pq::proto::valid_name(cluster) || !otap_s3pq::proto::valid_name(producer) || super::sql::LaneKind::for_signal(signal).is_none() {
        return Err(format!("refused: --lane {lane_id:?}: not a lane (FORMAT.md §1 names, a known signal)"));
    }
    // (a) the operator's attestation
    if !volume_deleted {
        return Err("refused: (a) the publisher's volume must be deleted first (its custody lost, acknowledged): \
             pass --volume-deleted once it is, with --evidence saying how you know. Retiring a lane whose volume is kept \
             quarantines whatever it replays later (FORMAT.md §3.1)"
            .into());
    }
    if evidence.trim().is_empty() {
        return Err("refused: (a) --evidence is required: what shows the publisher and its volume are gone (the PVC deletion, the node's loss)".into());
    }
    let lane = Lane { producer: format!("{cluster}/{producer}"), signal: signal.to_string() };
    let ckey = lane.ckpt_key(&cfg.ctl);
    let Some((body, _)) = b.get(&ckey).await? else {
        return Err(format!("refused: (c) {lane_id} has no checkpoint: the consumer has passed nothing of it yet"));
    };
    let ck: CkptDoc = serde_json::from_slice(&body).map_err(|e| format!("{ckey}: {e}"))?;
    if ck.retired_now() {
        return Err(format!("refused: {lane_id} is already retired (by {}, R {}, epoch {})", ck.retired_by, ck.retired_ns, ck.retired_epoch));
    }
    let prefix = lane.data_prefix(&cfg.root);
    let items = b.list(&prefix, floor_start_after(&prefix, &ck.floor).as_deref()).await?;
    let mut listed: BTreeMap<String, u64> = BTreeMap::new();
    let mut last: Option<&super::bucket::Item> = None;
    for it in &items {
        if let Some((e, s)) = otap_s3pq::proto::parse_slot_key(&prefix, &it.key) {
            if above_floor(&e, &ck.floor) {
                let m = listed.entry(e).or_insert(s);
                *m = (*m).max(s);
                if last.is_none_or(|l| it.modified_ms > l.modified_ms) {
                    last = Some(it);
                }
            }
        }
    }
    let Some(last) = last else {
        return Err(format!("refused: (b) nothing of {lane_id} is listed: this tool cannot see when it last wrote"));
    };
    let in_flight = cfg.mutation == Mutation::RetireInFlight;
    // (b) gone longer than a request lifetime
    let quiet_until = last.modified_ms.saturating_add(cfg.zombie_ms);
    if now_ms <= quiet_until && !in_flight {
        return Err(format!(
            "refused: (b) {lane_id} last wrote {} ms ago ({}), within the zombie bound {} ms: a PUT of its process may still land. \
             Retry after {} (Unix ms), with the process confirmed gone",
            now_ms.saturating_sub(last.modified_ms),
            last.key,
            cfg.zombie_ms,
            quiet_until + 1
        ));
    }
    // (c) every slot the lane shows passed by its holder
    for (e, max) in &listed {
        if !ck.closed(e) && *max >= ck.next(e) && !in_flight {
            return Err(format!(
                "refused: (c) the consumer has not passed {lane_id}/{e} slot {} (listed up to {max}): let it ingest first; a zombie PUT that landed must be ingested, not quarantined",
                ck.next(e)
            ));
        }
    }
    let meta = b.head(&last.key).await?.unwrap_or_default();
    if cfg.dry_run {
        return Ok(Retirement {
            lane: lane_id.to_string(),
            r_ns: now_ms.saturating_mul(1_000_000),
            epoch: listed.keys().chain(ck.epochs.keys()).max_by_key(|e| epoch_key(e)).cloned().unwrap_or_default(),
            evidence: evidence.to_string(),
            volume_deleted,
            wall_ms: now_ms,
            last_object_ms: last.modified_ms,
            last_kind: meta.get(otap_s3pq::proto::META_KIND).cloned().unwrap_or_default(),
            last_low_ns: meta.get(otap_s3pq::proto::META_LOW).and_then(|v| v.parse().ok()).unwrap_or(0),
            tombstones: Vec::new(),
            by: std::env::var("USER").unwrap_or_default(),
        });
    }
    // Seal: a tombstone at the head of every epoch not closed yet.
    let mut epochs: Vec<String> = listed.keys().cloned().collect();
    epochs.extend(ck.epochs.keys().cloned());
    epochs.sort_by_key(|e| epoch_key(e));
    epochs.dedup();
    let newest = epochs.last().cloned().unwrap_or_default();
    let mut tombs = Vec::new();
    for e in epochs.iter().filter(|e| !ck.closed(e) && !in_flight) {
        let at = ck.next(e);
        match super::worker::tombstone(b, &prefix, e, at).await {
            super::worker::TombResult::Closed(_) => tombs.push(format!("{e}/{at}")),
            super::worker::TombResult::LostToData => {
                return Err(format!("refused: a slot appeared at {lane_id}/{e}/{at} while retiring: its publisher is alive (tombstones written: {tombs:?})"));
            }
            super::worker::TombResult::Unresolved(why) => {
                return Err(format!("could not tombstone {lane_id}/{e}/{at}: {why}; nothing retired (tombstones written: {tombs:?})"));
            }
        }
    }
    let r_ns = now_ms.saturating_mul(1_000_000);
    let rec = Retirement {
        lane: lane_id.to_string(),
        r_ns,
        epoch: newest.clone(),
        evidence: evidence.to_string(),
        volume_deleted,
        wall_ms: now_ms,
        last_object_ms: last.modified_ms,
        last_kind: meta.get(otap_s3pq::proto::META_KIND).cloned().unwrap_or_default(),
        last_low_ns: meta.get(otap_s3pq::proto::META_LOW).and_then(|v| v.parse().ok()).unwrap_or(0),
        tombstones: tombs,
        by: std::env::var("USER").unwrap_or_default(),
    };
    // The retirement in the checkpoint, by CAS (a holder's next write then
    // fails on the ETag and it takes the lane again from this document).
    let mut done = false;
    for _ in 0..4 {
        let Some((body, etag)) = b.get(&ckey).await? else { return Err(format!("{ckey}: gone")) };
        let mut doc: CkptDoc = serde_json::from_slice(&body).map_err(|e| format!("{ckey}: {e}"))?;
        if doc.retired_now() && doc.retired_by == "operator" && doc.retired_wall_ms == now_ms {
            done = true;
            break;
        }
        doc.retire(&newest, r_ns, "operator", evidence, now_ms);
        doc.version += 1;
        match b.put(&ckey, Bytes::from(serde_json::to_vec(&doc).expect("json")), Cond::IfMatch(&etag), &BTreeMap::new()).await {
            Put::Ok(_) => {
                done = true;
                break;
            }
            Put::Conflict | Put::Unknown(_) => continue,
        }
    }
    if !done {
        return Err(format!("{ckey}: no CAS won in 4 tries; the lane's epochs are tombstoned, not retired: run it again"));
    }
    let key = retired_record_key(&cfg.ctl, lane_id, now_ms);
    let _ = b.put(&key, Bytes::from(serde_json::to_vec_pretty(&rec).expect("json")), Cond::Create, &BTreeMap::new()).await;
    Ok(rec)
}
