//! Dead-lane retirement (`../../FORMAT.md` §3.1, DECISIONS.md D35,
//! `../../model/retirement.qnt`): the quarantine document, written by a
//! lane's holder before its checkpoint passes a quarantined slot.
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
