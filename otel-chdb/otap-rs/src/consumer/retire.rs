//! Dead-lane retirement (`../../FORMAT.md` §3.1, DECISIONS.md D35,
//! `../../model/retirement.qnt`): the quarantine document, written by a
//! lane's holder before its checkpoint passes a quarantined slot, and
//! `consume retire-lane` (`retire_lane`), an operator's retirement of a lane
//! whose publisher died without a close.
//!
//! - **Retirement by an orderly close** is the worker's (`worker.rs`
//!   `advance`, `CkptDoc::close_proof`).
//! - **`consume admit`** (`admit`): the quarantined objects into the
//!   recovered tables (`sql::Recover`, `{table}_recovered`), never the main
//!   ones, with a report of the windows and published values they fall in.
//! - **Quarantine.** An object of a retired lane received below the bound R
//!   (`CkptDoc::quarantines`) that central does not already hold is never
//!   inserted: it is recorded in `{ctl}/quarantine/{lane}.json` (slot,
//!   content key, rows, received_at), counted
//!   (`consumer_quarantined_objects_total`, paged), and its slot is passed.
//!   An object below R that central already holds (a copy: an adopted
//!   volume replaying what was committed before the close) is passed like
//!   any copy.

use super::wmhistory;
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

// ---- consume admit: quarantined objects into the recovered tables ------------------------

/// Every object key listed in a quarantine document: GC keeps them (they
/// are the quarantine's evidence and `consume admit`'s source).
pub async fn quarantined_keys<B: Bucket + ?Sized>(b: &B, ctl: &str) -> Result<std::collections::BTreeSet<String>, String> {
    let mut out = std::collections::BTreeSet::new();
    for (_, doc) in quarantine_docs(b, ctl, None).await? {
        out.extend(doc.objects.into_iter().map(|q| q.key));
    }
    Ok(out)
}

/// The quarantine documents (one lane's, or every lane's): (lane, doc).
pub async fn quarantine_docs<B: Bucket + ?Sized>(b: &B, ctl: &str, lane: Option<&str>) -> Result<Vec<(String, QuarantineDoc)>, String> {
    let lanes: Vec<String> = match lane {
        Some(l) => vec![l.to_string()],
        None => {
            let prefix = quarantine_prefix(ctl);
            b.list(&prefix, None)
                .await?
                .into_iter()
                .filter_map(|it| it.key.strip_prefix(&format!("{prefix}/")).and_then(|k| k.strip_suffix(".json")).map(str::to_string))
                .collect()
        }
    };
    let mut out = Vec::new();
    for l in lanes {
        if let Some((d, _)) = read(b, ctl, &l).await? {
            out.push((l, d));
        }
    }
    Ok(out)
}

/// One (cluster, signal)'s share of an admit report.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdmitWindow {
    pub cluster: String,
    pub signal: String,
    /// The recovered table the rows went to (or would, with --dry-run).
    pub table: String,
    pub objects: u64,
    pub rows: u64,
    /// The rows' event-time range (ns; the objects' `oscope-min-time` /
    /// `oscope-max-time`): alert and query windows over it were evaluated
    /// without these rows.
    pub event_from_ns: u64,
    pub event_to_ns: u64,
    /// The hours (UTC, Unix s) that range touches, at most 48 listed.
    pub hours: Vec<u64>,
    pub hours_total: u64,
    /// Their received_at range (ns).
    pub received_from_ns: u64,
    pub received_to_ns: u64,
    /// The published `complete_through` values above the lowest of those
    /// received_at: the bases (D30) they fall below; each with, from the
    /// history (D29 amendment 2026-10-01), when it first passed the rows.
    pub bases_now: Vec<AdmitBasis>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdmitBasis {
    /// `fleet`, `cluster`, `cluster/signal`.
    pub scope: String,
    pub complete_through_ns: u64,
    /// The watermark document's version (+1 per write) and wall time.
    pub version: u64,
    pub wall_ms: u64,
    /// From the history (`wmhistory`): the last step whose value was at or
    /// below the rows' lowest received_at, and the first above it (wall ms).
    /// A basis of this scope minted before `last_below_ms` cannot be missing
    /// the rows; one minted after `first_above_ms` is; between the two the
    /// history's resolution (`--wm-history-every`) cannot say. `None`: the
    /// history does not reach that far (or was off).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_below_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_above_ms: Option<u64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdmitReport {
    pub dry_run: bool,
    /// Objects admitted now, already admitted before, and gone from S3.
    pub admitted: u64,
    pub already: u64,
    pub missing: Vec<String>,
    pub windows: Vec<AdmitWindow>,
    pub note: String,
}

/// `consume admit` (D35 (3), `../../FORMAT.md` §3.1): puts the quarantined
/// objects of one lane (or every lane) into the recovered tables (`r`),
/// never the main ones, idempotent by content key; marks each admitted in
/// its quarantine document; and reports, per cluster and signal, the event
/// windows the rows fall in and the published values above their
/// received_at. With `dry_run`, the report alone.
pub async fn admit<B: Bucket + ?Sized, R: super::sql::Recover + ?Sized>(
    b: &B,
    r: &R,
    ctl: &str,
    lane: Option<&str>,
    dry_run: bool,
    now_ms: u64,
) -> Result<AdmitReport, String> {
    use otap_s3pq::proto;
    let mut rep = AdmitReport { dry_run, ..Default::default() };
    let mut windows: BTreeMap<(String, String), AdmitWindow> = BTreeMap::new();
    let docs = quarantine_docs(b, ctl, lane).await?;
    if let (Some(l), true) = (lane, docs.is_empty()) {
        return Err(format!("{l}: no quarantine document ({})", quarantine_key(ctl, l)));
    }
    for (lane_id, doc) in docs {
        let (cluster, rest) = lane_id.split_once('/').unwrap_or(("", &lane_id));
        let signal = rest.rsplit('/').next().unwrap_or(rest).to_string();
        let Some(k) = super::sql::LaneKind::for_signal(&signal) else { continue };
        let mut done: BTreeMap<(String, u64), u64> = BTreeMap::new();
        for q in &doc.objects {
            if q.admitted_wall_ms > 0 {
                rep.already += 1;
                continue;
            }
            let Some(meta) = b.head(&q.key).await? else {
                rep.missing.push(q.key.clone());
                continue;
            };
            let num = |key: &str| meta.get(key).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);
            let obj = Obj {
                lane: lane_id.clone(),
                epoch: q.epoch.clone(),
                seq: q.seq,
                key: q.key.clone(),
                size: 0,
                content: q.content.clone(),
                rows: q.rows,
                received_ns: q.received_ns,
                seen_ms: 0,
                announce: 0,
                payloads: Default::default(),
                late: super::plan::is_late_part(&meta),
            };
            let rows = if dry_run { q.rows } else { r.admit(&k, &obj, &lane_id).await? };
            if !dry_run {
                let _ = done.insert((q.epoch.clone(), q.seq), rows);
                rep.admitted += 1;
            }
            let w = windows.entry((cluster.to_string(), signal.clone())).or_insert_with(|| AdmitWindow {
                cluster: cluster.to_string(),
                signal: signal.clone(),
                table: r.recovered_table(&k),
                event_from_ns: u64::MAX,
                received_from_ns: u64::MAX,
                ..Default::default()
            });
            w.objects += 1;
            w.rows += rows;
            w.event_from_ns = w.event_from_ns.min(num(proto::META_MIN_TIME));
            w.event_to_ns = w.event_to_ns.max(num(proto::META_MAX_TIME));
            w.received_from_ns = w.received_from_ns.min(q.received_ns);
            w.received_to_ns = w.received_to_ns.max(q.received_ns);
        }
        if !done.is_empty() {
            mark_admitted(b, ctl, &lane_id, &done, now_ms).await?;
        }
    }
    // What the rows would have changed: the windows, and the values published above them.
    let fleet: Option<super::watermark::WmDoc> = get_doc(b, &super::watermark::wm_key(ctl)).await?;
    for ((cluster, signal), mut w) in windows {
        let hour = 3_600_000_000_000u64;
        if w.event_to_ns >= w.event_from_ns {
            let (h0, h1) = (w.event_from_ns / hour, w.event_to_ns / hour);
            w.hours_total = h1 - h0 + 1;
            w.hours = (h0..=h1).take(48).map(|h| h * 3_600).collect();
        }
        let above = |v: u64| v > w.received_from_ns;
        let x = w.received_from_ns;
        if let Some(f) = &fleet {
            if above(f.complete_through_ns) {
                let (last_below_ms, first_above_ms) = passed(b, ctl, wmhistory::FLEET, &f.history, x, None).await?;
                w.bases_now.push(AdmitBasis {
                    scope: "fleet".into(),
                    complete_through_ns: f.complete_through_ns,
                    version: f.version,
                    wall_ms: f.wall_ms,
                    last_below_ms,
                    first_above_ms,
                });
            }
        }
        if let Some(c) = get_doc::<_, super::watermark::ClusterWmDoc>(b, &super::watermark::cluster_wm_key(ctl, &cluster)).await? {
            let sig = [signal.clone()];
            for (scope, v, sigs) in [(cluster.clone(), c.complete_through_ns, None), (format!("{cluster}/{signal}"), c.signals.get(&signal).copied().unwrap_or(0), Some(&sig[..]))] {
                if above(v) {
                    let (last_below_ms, first_above_ms) = passed(b, ctl, &cluster, &c.history, x, sigs).await?;
                    w.bases_now.push(AdmitBasis { scope, complete_through_ns: v, version: c.version, wall_ms: c.wall_ms, last_below_ms, first_above_ms });
                }
            }
        }
        rep.windows.push(w);
    }
    rep.note = "Every basis issued for these scopes at or above the rows' lowest received_at reads without them: from the history \
        (D29 amendment), each listed scope's value was at or below it until last_below_ms and above it from first_above_ms (wall ms; \
        absent where the history does not reach), so bases minted between those times may or may not, and after first_above_ms do. Alert windows over the listed event times were evaluated without these rows: \
        re-check them by hand; nothing is re-evaluated automatically. The rows are only in the recovered tables, read only by a query \
        that asks for them (the query service's \"recovered\": true), never mixed into the main tables."
        .into();
    Ok(rep)
}

/// How many hours of history `consume admit` reads per scope (a fortnight).
const ADMIT_HISTORY_HOURS: u64 = 24 * 14;

/// From a scope's history: the last step at or below `x` (ns) and the first
/// above it (wall ms), searching from wall time x (the history cannot pass x
/// before it: its values are at most the LIST time).
async fn passed<B: Bucket + ?Sized>(b: &B, ctl: &str, scope: &str, h: &wmhistory::History, x: u64, signals: Option<&[String]>) -> Result<(Option<u64>, Option<u64>), String> {
    let from_ms = x / 1_000_000;
    let to_ms = h.open.hour_ms.max(from_ms);
    let objs = wmhistory::load_hours(b, ctl, scope, from_ms, to_ms, ADMIT_HISTORY_HOURS).await?;
    let (below, above) = wmhistory::first_above(h, x, signals, from_ms, ADMIT_HISTORY_HOURS, |hh| objs.get(&hh).cloned());
    Ok((below.map(|s| s.at_ms), above.map(|s| s.at_ms)))
}

async fn get_doc<B: Bucket + ?Sized, D: serde::de::DeserializeOwned>(b: &B, key: &str) -> Result<Option<D>, String> {
    match b.get(key).await? {
        Some((body, _)) => serde_json::from_slice(&body).map(Some).map_err(|e| format!("{key}: {e}")),
        None => Ok(None),
    }
}

/// Marks slots admitted in a lane's quarantine document (CAS).
async fn mark_admitted<B: Bucket + ?Sized>(b: &B, ctl: &str, lane: &str, done: &BTreeMap<(String, u64), u64>, now_ms: u64) -> Result<(), String> {
    let key = quarantine_key(ctl, lane);
    for _ in 0..4 {
        let Some((mut doc, etag)) = read(b, ctl, lane).await? else { return Err(format!("{key}: gone")) };
        for q in doc.objects.iter_mut() {
            if let Some(rows) = done.get(&(q.epoch.clone(), q.seq)) {
                if q.admitted_wall_ms == 0 {
                    q.admitted_wall_ms = now_ms;
                    q.admitted_rows = *rows;
                }
            }
        }
        doc.version += 1;
        match b.put(&key, Bytes::from(serde_json::to_vec_pretty(&doc).expect("json")), Cond::IfMatch(&etag), &BTreeMap::new()).await {
            Put::Ok(_) => return Ok(()),
            Put::Conflict | Put::Unknown(_) => continue,
        }
    }
    Err(format!("{key}: no CAS won in 4 tries (the rows are in the recovered table; admit again to mark them)"))
}
