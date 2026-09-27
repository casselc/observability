//! The production-shaped central consumer (importer) for the manifest-less
//! layout of format v2 (`../../FORMAT.md`):
//! `{root}/{cluster}/{producer}/{signal}/{epoch}/{seq:020d}.parquet`. A lane
//! is `{cluster}/{producer}/{signal}` (`coord::Lane`: `producer` holds the
//! segments above the signal, `cluster/producer` at `depth` 3).
//!
//! - `coord`:  the lane lease and checkpoint documents, and every decision
//!   about them (take, renew, release, the insert time bound, the fair
//!   share), sans-IO (../model/S3NATIVE.md, "Consumer"); checkpoint
//!   compaction (`CkptDoc::compact`: retired epochs leave the checkpoint,
//!   a per-lane floor bounds discovery; ../model/s3InlineConsumerCompact.qnt).
//! - `plan`:   the per-epoch scan from the checkpoint (runs, gaps, the dead
//!   head), object metadata, the check / verify verdicts and grouping
//!   objects into statements, sans-IO.
//! - `bucket`: what the consumer needs from S3 (GET with ETag, conditional
//!   PUT, LIST StartAfter, HEAD, DELETE), over object_store with request
//!   counters, and an in-memory bucket with faults for tests.
//! - `sql`:    lane kinds (signal -> table, s3() structure, columns) and the
//!   ClickHouse statements: batched `INSERT … SELECT FROM s3({k1,k2,…})`
//!   with the single-block settings, a server-side lease fence, the
//!   projection check, row repair; plus an in-memory central for tests.
//! - `worker`: the loop: discover lanes, hold leases, scan, ingest, verify,
//!   advance checkpoints, close dead epochs with tombstones.
//! - `discovery`: fleet-scale discovery, sans-IO: the per-lane idle LIST
//!   backoff, and the `Hints` seam for event-driven discovery (S3 event
//!   notifications), with LIST kept as the reconciliation.
//! - `gc`:     the separate GC step: delete slots below a horizon that trails
//!   the checkpoints by a lease length plus a request-lifetime delay, and
//!   retire closed epochs after the zombie bound (which lets the workers
//!   compact them out of their checkpoints).
//! - `audit`:  the horizon audit: finds copies ingested twice because they
//!   were received more than the check's copy horizon after their original
//!   (content keys in two partitions beyond the check's reach), off the
//!   ingest path (`consume gc --audit-every`, `consume horizon-audit`).
//! - `watermark`: `complete_through`: the minimum of the lanes' watermarks
//!   (each lane's holder computes its own at every full listing), published
//!   as a running max in `{ctl}/watermark.json` (`consume gc`, `consume
//!   watermark`), with the lanes holding it back and the stale ones.
//! - `metrics`: Prometheus text for the worker, GC and the audit, and a
//!   minimal HTTP endpoint (`--metrics-addr`).
//!
//! The module is mounted by `src/bin/consume.rs` and by the tests with
//! `#[path]`, so it depends on the library only through `otap_s3pq::…`.

#![allow(dead_code)]

pub mod audit;
pub mod bucket;
pub mod coord;
pub mod discovery;
pub mod gc;
pub mod metrics;
pub mod plan;
pub mod sql;
pub mod watermark;
pub mod worker;

/// `{ctl}/format.json`: the on-disk format this bucket is in (`../../FORMAT.md` §5).
pub fn format_key(ctl: &str) -> String {
    coord::join(ctl, "format.json")
}

/// Checks the bucket's format marker, creating it (create-only) when there is
/// none: `Err` when it names another format (a version-1 bucket is drained
/// or purged before version 2 is deployed on it) or can't be read.
pub async fn ensure_format<B: bucket::Bucket + ?Sized>(b: &B, ctl: &str) -> Result<(), String> {
    let key = format_key(ctl);
    let want = otap_s3pq::proto::FORMAT_VERSION as u64;
    for _ in 0..3 {
        match b.get(&key).await? {
            Some((body, _)) => {
                let v: serde_json::Value = serde_json::from_slice(&body).map_err(|e| format!("{key}: {e}"))?;
                return match v["format"].as_u64() {
                    Some(f) if f == want => Ok(()),
                    f => Err(format!("{key} says format {f:?}; this consumer reads format {want} (../FORMAT.md §5)")),
                };
            }
            None => {
                let doc = serde_json::json!({"format": want, "layout": "{cluster}/{producer}/{signal}/{epoch}/{seq:020d}.parquet"});
                let body = bytes::Bytes::from(doc.to_string());
                // Create-only; a 412 or a lost answer is settled by reading it back.
                let _ = b.put(&key, body, bucket::Cond::Create, &std::collections::BTreeMap::new()).await;
            }
        }
    }
    Err(format!("{key}: could not create or read the format marker"))
}

/// Milliseconds on a monotonic clock (the lease time bound is measured on it).
pub fn mono_ms() -> u64 {
    use std::sync::OnceLock;
    static START: OnceLock<std::time::Instant> = OnceLock::new();
    START.get_or_init(std::time::Instant::now).elapsed().as_millis() as u64
}

/// Milliseconds since the Unix epoch (wall clock: the server-side fence and GC marks).
pub fn wall_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// A process's CPU time (user + system), ms.
pub fn cpu_ms() -> f64 {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let tv = |t: libc::timeval| t.tv_sec as f64 * 1e3 + t.tv_usec as f64 / 1e3;
    tv(ru.ru_utime) + tv(ru.ru_stime)
}

#[cfg(test)]
mod tests;
