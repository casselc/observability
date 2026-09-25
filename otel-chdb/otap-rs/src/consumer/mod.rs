//! The production-shaped central consumer (importer) for the manifest-less
//! layout `{root}/{producer}/{signal}/{epoch}/{seq:020d}.parquet`.
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
//! - `gc`:     the separate GC step: delete slots below a horizon that trails
//!   the checkpoints by a lease length plus a request-lifetime delay, and
//!   retire closed epochs after the zombie bound (which lets the workers
//!   compact them out of their checkpoints).
//!
//! The module is mounted by `src/bin/consume.rs` and by the tests with
//! `#[path]`, so it depends on the library only through `otap_s3pq::…`.

#![allow(dead_code)]

pub mod bucket;
pub mod coord;
pub mod gc;
pub mod plan;
pub mod sql;
pub mod worker;

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
