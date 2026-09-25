//! otap-s3pq: an otap-dataflow exporter that publishes ClickStack-shaped
//! Parquet batches to S3 with the manifest-less, create-only commit
//! protocol, acknowledging upstream only once a batch's commit is resolved.
//!
//! - `flatten`: OTLP bytes / OTAP records → ClickStack rows (column buffers)
//! - `metrics`: the same for metrics: one row set per metric type
//! - `gosort`:  Go's `slices.SortFunc`, for contrib's metrics map ordering
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
pub mod gosort;
pub mod metrics;
pub mod proto;
pub mod render;
pub mod runner;
pub mod schema;
pub mod store;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Signal {
    Traces,
    Logs,
    /// Metrics: one signal per metric type, so each type is its own table,
    /// its own object and its own log (lane, epoch, slots).
    MetricsGauge,
    MetricsSum,
    MetricsHistogram,
    MetricsExpHistogram,
    MetricsSummary,
}

impl Signal {
    pub const ALL: [Signal; 7] = [
        Signal::Traces,
        Signal::Logs,
        Signal::MetricsGauge,
        Signal::MetricsSum,
        Signal::MetricsHistogram,
        Signal::MetricsExpHistogram,
        Signal::MetricsSummary,
    ];
    /// The metric types, in the order a request's objects are listed.
    pub const METRICS: [Signal; 5] = [
        Signal::MetricsGauge,
        Signal::MetricsSum,
        Signal::MetricsHistogram,
        Signal::MetricsExpHistogram,
        Signal::MetricsSummary,
    ];

    /// The signal namespace: the S3 path segment, the content-key domain,
    /// and `x-amz-meta-oscope-signal`.
    pub fn name(self) -> &'static str {
        match self {
            Signal::Traces => "traces",
            Signal::Logs => "logs",
            Signal::MetricsGauge => "metrics_gauge",
            Signal::MetricsSum => "metrics_sum",
            Signal::MetricsHistogram => "metrics_histogram",
            Signal::MetricsExpHistogram => "metrics_exponential_histogram",
            Signal::MetricsSummary => "metrics_summary",
        }
    }

    pub fn from_name(s: &str) -> Option<Signal> {
        Signal::ALL.into_iter().find(|x| x.name() == s)
    }

    /// The contrib clickhouseexporter's default table name.
    pub fn table(self) -> &'static str {
        match self {
            Signal::Traces => "otel_traces",
            Signal::Logs => "otel_logs",
            Signal::MetricsGauge => "otel_metrics_gauge",
            Signal::MetricsSum => "otel_metrics_sum",
            Signal::MetricsHistogram => "otel_metrics_histogram",
            Signal::MetricsExpHistogram => "otel_metrics_exponential_histogram",
            Signal::MetricsSummary => "otel_metrics_summary",
        }
    }

    pub fn is_metrics(self) -> bool {
        !matches!(self, Signal::Traces | Signal::Logs)
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
