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
//! - `creds`:   shared config/credentials profiles, AssumeRole chaining, SigV4 for STS
//! - `exporter`: the otap-dataflow node (`urn:otel:exporter:s3pq`)
//! - `commit_metrics`: `s3pq_commit_outcomes_total{outcome}`, the exporter's commit outcomes
//! - `batch`:   one request → content hash + flattened columns → encoded slot object
//! - `series`:  metrics layout B: narrow points + series objects, edge series ids
//! - `resource`: resource_id (the entity catalog's content address) and the announcement cache
//! - `central`: the ClickHouse side of the consumer

pub mod batch;
pub mod central;
pub mod columns;
pub mod commit_metrics;
pub mod creds;
pub mod encode;
pub mod exporter;
pub mod flatten;
pub mod late;
pub mod gosort;
pub mod metrics;
pub mod proto;
pub mod render;
pub mod resource;
pub mod runner;
pub mod schema;
pub mod series;
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
    /// Layout B (`series`): narrow points objects keyed by an edge-computed
    /// series id, one namespace per points table, and the series objects.
    /// Gauge and sum share `metrics_number_points` unless the exporter's
    /// `series.merge_number_points` is off.
    MetricsNumberPoints,
    MetricsGaugePoints,
    MetricsSumPoints,
    MetricsHistogramPoints,
    MetricsExpHistogramPoints,
    MetricsSummaryPoints,
    /// The series objects: keyed by the object's own content hash, not the
    /// request's, because what a request announces depends on the cache.
    MetricsSeries,
}

impl Signal {
    pub const ALL: [Signal; 14] = [
        Signal::Traces,
        Signal::Logs,
        Signal::MetricsGauge,
        Signal::MetricsSum,
        Signal::MetricsHistogram,
        Signal::MetricsExpHistogram,
        Signal::MetricsSummary,
        Signal::MetricsNumberPoints,
        Signal::MetricsGaugePoints,
        Signal::MetricsSumPoints,
        Signal::MetricsHistogramPoints,
        Signal::MetricsExpHistogramPoints,
        Signal::MetricsSummaryPoints,
        Signal::MetricsSeries,
    ];
    /// Layout B's namespaces (`series.rs`).
    pub const SERIES_LAYOUT: [Signal; 7] = [
        Signal::MetricsNumberPoints,
        Signal::MetricsGaugePoints,
        Signal::MetricsSumPoints,
        Signal::MetricsHistogramPoints,
        Signal::MetricsExpHistogramPoints,
        Signal::MetricsSummaryPoints,
        Signal::MetricsSeries,
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
            Signal::MetricsNumberPoints => "metrics_number_points",
            Signal::MetricsGaugePoints => "metrics_gauge_points",
            Signal::MetricsSumPoints => "metrics_sum_points",
            Signal::MetricsHistogramPoints => "metrics_histogram_points",
            Signal::MetricsExpHistogramPoints => "metrics_exponential_histogram_points",
            Signal::MetricsSummaryPoints => "metrics_summary_points",
            Signal::MetricsSeries => "metrics_series",
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
            Signal::MetricsNumberPoints => "otel_metrics_number_points",
            Signal::MetricsGaugePoints => "otel_metrics_gauge_points",
            Signal::MetricsSumPoints => "otel_metrics_sum_points",
            Signal::MetricsHistogramPoints => "otel_metrics_histogram_points",
            Signal::MetricsExpHistogramPoints => "otel_metrics_exponential_histogram_points",
            Signal::MetricsSummaryPoints => "otel_metrics_summary_points",
            Signal::MetricsSeries => "otel_metrics_series",
        }
    }

    pub fn is_metrics(self) -> bool {
        !matches!(self, Signal::Traces | Signal::Logs)
    }

    /// One of layout B's namespaces (points or series).
    pub fn is_series_layout(self) -> bool {
        Signal::SERIES_LAYOUT.contains(&self)
    }
}

thread_local! {
    static LOG_SINK: std::cell::RefCell<Option<Box<dyn Fn(&str)>>> = const { std::cell::RefCell::new(None) };
}

/// Sends this thread's `log` lines to `sink` instead of stderr (`None`:
/// back to stderr). The deterministic simulation tests (tests/dst_*.rs) put
/// the lines into their trace, stamped with simulated time.
pub fn set_log_sink(sink: Option<Box<dyn Fn(&str)>>) {
    LOG_SINK.with(|s| *s.borrow_mut() = sink);
}

/// Minimal stderr logging (the engine's own telemetry macros need its
/// component scope; this crate keeps to plain lines).
pub fn log(msg: &str) {
    if LOG_SINK.try_with(|s| s.borrow().as_ref().map(|f| f(msg)).is_some()).unwrap_or(false) {
        return;
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    eprintln!("{}.{:03} otap-s3pq: {msg}", now.as_secs(), now.subsec_millis());
}
