//! otap-s3pq: an otap-dataflow exporter that publishes ClickStack-shaped
//! Parquet batches to S3 with the manifest-less, create-only commit
//! protocol, acknowledging upstream only once a batch's commit is resolved.
//!
//! - `flatten`: OTLP bytes / OTAP records → ClickStack rows (column buffers)
//! - `render`:  contrib-compatible value rendering
//! - `encode`:  Parquet (and Arrow IPC) encoding
//! - `proto`:   the commit protocol's writer lane and consumer (sans-IO)
//! - `runner`:  the protocol's I/O loop
//! - `store`:   S3 via object_store, credentials, an in-memory store
//! - `exporter`: the otap-dataflow node (`urn:otel:exporter:s3pq`)
//! - `batch`:   one request → content hash + flattened columns → encoded slot object
//! - `central`: the ClickHouse side of the consumer

pub mod batch;
pub mod central;
pub mod columns;
pub mod encode;
pub mod exporter;
pub mod flatten;
pub mod proto;
pub mod render;
pub mod runner;
pub mod schema;
pub mod store;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Signal {
    Traces,
    Logs,
}

impl Signal {
    pub fn name(self) -> &'static str {
        match self {
            Signal::Traces => "traces",
            Signal::Logs => "logs",
        }
    }
}

/// Minimal stderr logging (the engine's own telemetry macros need its
/// component scope; this crate keeps to plain lines).
pub fn log(msg: &str) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    eprintln!("{}.{:03} otap-s3pq: {msg}", now.as_secs(), now.subsec_millis());
}
