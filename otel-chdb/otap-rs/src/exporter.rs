//! `urn:otel:exporter:s3pq`: the otap-dataflow node.
//!
//! Per request: content hash and flatten (on the pipeline thread), then
//! append to a lane's log (encode for the slot, create-only PUT, HEAD on a
//! 412 or no answer). The request is ACKed only when the commit is resolved
//! as ours (or found committed); NACKed (retryable) when the outcome is still
//! unknown, and permanently when the data can't be encoded. With the OTLP
//! receiver's `wait_for_result`, the client's response follows the commit.
//!
//! Metrics: a request becomes one object per non-empty metric type
//! (`metrics_gauge`, `metrics_sum`, ...), each appended to its own type's lane
//! and log, concurrently. The request is ACKed only when every object has
//! committed (`proto::request_verdict`); otherwise it is NACKed as a whole,
//! and the retry finds the parts that did commit in their lanes' known set.
//!
//! Lanes: one log per (signal, lane). A request goes to lane
//! `hash(content) mod lanes`, so a retry meets the lane, and the unresolved
//! slot, of its first attempt. Lanes append concurrently; one lane appends
//! one batch at a time.

use crate::batch::{Encoder, Flat, Format, Input};
use crate::encode::ParquetOptions;
use crate::flatten::Envelope;
use crate::proto::{self, Lane, PartOutcome, Ref, Verdict};
use crate::runner::{self, AppendError, EncodedCache, Stats, Timeouts};
use crate::series::{MetricsLayout, SeriesOptions};
use crate::store::{S3Config, S3Store};
use crate::Signal;
use async_trait::async_trait;
use futures::StreamExt;
use futures::future::LocalBoxFuture;
use futures::stream::FuturesUnordered;
use linkme::distributed_slice;
use otel_arrow_dfe_config::SignalType;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_engine::config::ExporterConfig;
use otel_arrow_dfe_engine::context::PipelineContext;
use otel_arrow_dfe_engine::control::{AckMsg, NackCause, NackMsg, NodeControlMsg};
use otel_arrow_dfe_engine::error::{Error, ExporterErrorKind};
use otel_arrow_dfe_engine::exporter::ExporterWrapper;
use otel_arrow_dfe_engine::local::exporter::{EffectHandler, Exporter};
use otel_arrow_dfe_engine::message::{ExporterInbox, Message};
use otel_arrow_dfe_engine::node::NodeId;
use otel_arrow_dfe_engine::terminal_state::TerminalState;
use otel_arrow_dfe_engine::{ConsumerEffectHandlerExtension, ExporterFactory};
use otel_arrow_dfe_otap::OTAP_EXPORTER_FACTORIES;
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_pdata::{OtapArrowRecords, OtlpProtoBytes, PayloadData, TryIntoWithOptions};
use serde::Deserialize;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Instant, SystemTime};

pub const S3PQ_EXPORTER_URN: &str = "urn:otel:exporter:s3pq";

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OtlpPath {
    /// Walk the OTLP protobuf bytes directly (zero-copy views).
    #[default]
    Direct,
    /// Convert OTLP to OTAP records first (upstream's encoder), then walk those.
    ViaOtap,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The bucket and the data root (`{root}` in `../FORMAT.md`): objects go
    /// under `{root}/{cluster}/{producer_id}/{signal}/`.
    pub s3: S3Config,
    /// The cluster this publisher serves: the first key segment, which
    /// write access is scoped by (DECISIONS.md D18). Required.
    pub cluster: String,
    /// Unique in the cluster and stable per durable buffer (the pod name).
    pub producer_id: String,
    #[serde(default = "one")]
    pub lanes: usize,
    #[serde(default)]
    pub format: Format,
    #[serde(default)]
    pub otlp_path: OtlpPath,
    #[serde(default)]
    pub parquet: ParquetOptions,
    /// Metrics as layout B (`series_table`, the default: points tables plus
    /// a series table, `series.rs`) or as the contrib exporter's five tables
    /// (`clickstack_tables`).
    #[serde(default)]
    pub metrics_layout: MetricsLayout,
    #[serde(default)]
    pub series: SeriesOptions,
    /// Write each batch's stats line to stderr.
    #[serde(default)]
    pub verbose: bool,
    /// What holds this publisher's custody (`../FORMAT.md` §2, `oscope-low`).
    #[serde(default)]
    pub custody: Custody,
    #[serde(default)]
    pub heartbeat: HeartbeatConfig,
    /// Resource announcements (`resource.rs`, `../FORMAT.md` §2).
    #[serde(default)]
    pub resources: crate::resource::ResourceOptions,
    /// The late split (`late.rs`, DECISIONS.md D31): a traces or logs
    /// request with rows more than this older (event time) than its newest
    /// row is committed as two objects, the bulk and the late rows, each
    /// with its own tight time range; 0s never splits. Default 15m: keep it
    /// above the fleet's clock skew. The Go edge's `late_split_after`.
    #[serde(default = "default_late_split_after", with = "humantime_serde")]
    pub late_split_after: std::time::Duration,
}

fn default_late_split_after() -> std::time::Duration {
    std::time::Duration::from_secs(15 * 60)
}

/// Where the requests this exporter publishes wait before it has them.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Custody {
    /// Nothing: a sender waits for the commit (ack after commit), so custody
    /// is the requests in the exporter's hands.
    #[default]
    Exporter,
    /// A durable buffer (Quiver) in front: it publishes its floor on this
    /// thread (`otel_arrow_dfe_otap::custody`, patches/0006); until it has,
    /// every object's low is 0.
    DurableBuffer,
}

/// Heartbeat slots (`../FORMAT.md` §2): a birth heartbeat per registered
/// lane at start, then one per lane idle for `interval`.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HeartbeatConfig {
    /// 0: no heartbeats at all, births included (benchmarks only: the
    /// consumer's complete_through then cannot pass this producer).
    #[serde(with = "humantime_serde")]
    pub interval: std::time::Duration,
    /// How long the exporter waits for its births before it takes requests
    /// (it keeps trying after that).
    #[serde(with = "humantime_serde")]
    pub birth_timeout: std::time::Duration,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        HeartbeatConfig { interval: std::time::Duration::from_secs(30), birth_timeout: std::time::Duration::from_secs(30) }
    }
}

/// The lanes a publisher registers (heartbeats), by metrics layout
/// (`../FORMAT.md` §1): every lane it can ever write.
pub fn registered_signals(layout: MetricsLayout, merge_number_points: bool) -> Vec<Signal> {
    let mut v = vec![Signal::Traces, Signal::Logs];
    match layout {
        MetricsLayout::ClickstackTables => v.extend(Signal::METRICS),
        MetricsLayout::SeriesTable => v.extend(Signal::SERIES_LAYOUT.into_iter().filter(|s| match s {
            Signal::MetricsNumberPoints => merge_number_points,
            Signal::MetricsGaugePoints | Signal::MetricsSumPoints => !merge_number_points,
            _ => true,
        })),
    }
    v
}

/// An object's `oscope-low` (ns): the lowest of the send time, the
/// `received_at` of every request in the exporter's hands, and the durable
/// buffer's published floor (0 while a configured buffer has published
/// none). Its own request is included: lower, so still sound.
pub fn custody_low(now_ns: u64, in_hands_min: Option<u64>, published: Option<u64>, custody: Custody) -> u64 {
    let mut low = now_ns;
    if let Some(m) = in_hands_min {
        low = low.min(m);
    }
    match (published, custody) {
        (Some(p), _) => low.min(p),
        (None, Custody::DurableBuffer) => 0,
        (None, Custody::Exporter) => low,
    }
}

fn one() -> usize {
    1
}

pub fn validate_config(v: &serde_json::Value) -> Result<(), otel_arrow_dfe_config::error::Error> {
    let c: Config = serde_json::from_value(v.clone()).map_err(|e| {
        otel_arrow_dfe_config::error::Error::InvalidUserConfig { error: e.to_string() }
    })?;
    if c.s3.url.is_empty() || c.producer_id.is_empty() || c.cluster.is_empty() {
        return Err(otel_arrow_dfe_config::error::Error::InvalidUserConfig {
            error: "s3.url, cluster and producer_id are required".into(),
        });
    }
    for (what, v) in [("cluster", &c.cluster), ("producer_id", &c.producer_id)] {
        if !proto::valid_name(v) {
            return Err(otel_arrow_dfe_config::error::Error::InvalidUserConfig {
                error: format!("{what} {v:?}: want [a-z0-9]([a-z0-9._-]{{0,61}}[a-z0-9])? (a key segment, ../FORMAT.md)"),
            });
        }
    }
    Ok(())
}

#[allow(unsafe_code)]
#[distributed_slice(OTAP_EXPORTER_FACTORIES)]
pub static S3PQ_EXPORTER: ExporterFactory<OtapPdata> = ExporterFactory {
    name: S3PQ_EXPORTER_URN,
    create:
        |pipeline: PipelineContext,
         node: NodeId,
         node_config: Arc<NodeUserConfig>,
         exporter_config: &ExporterConfig,
         _capabilities: &otel_arrow_dfe_engine::capability::registry::Capabilities| {
            let config: Config = serde_json::from_value(node_config.config.clone()).map_err(|e| {
                otel_arrow_dfe_config::error::Error::InvalidUserConfig { error: e.to_string() }
            })?;
            let outcomes = crate::commit_metrics::CommitOutcomes::register(&pipeline);
            Ok(ExporterWrapper::local(S3pqExporter { config, outcomes }, node, node_config, exporter_config))
        },
    validate_config,
    context_declarations: None,
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
};

pub struct S3pqExporter {
    config: Config,
    /// `s3pq_commit_outcomes_total{outcome}` (`commit_metrics.rs`).
    outcomes: crate::commit_metrics::CommitOutcomes,
}

struct LaneState {
    lane: Lane,
    cache: EncodedCache,
    /// The resources announced in this lane's epoch (traces, logs).
    ann: crate::resource::AnnounceCache,
}

/// A request's outcome: one entry per object.
type Done = (OtapPdata, Vec<(Signal, PartOutcome)>, Instant, usize);

struct Shared {
    store: S3Store,
    encoder: RefCell<Encoder>,
    stats: Stats,
    timeouts: Timeouts,
    producer: String,
    cluster: String,
    lanes: HashMap<(Signal, usize), Rc<tokio::sync::Mutex<LaneState>>>,
    lanes_per_signal: usize,
    prefixes: HashMap<Signal, String>,
    verbose: bool,
    custody: Custody,
    /// received_at of the requests being committed (a multiset).
    in_hands: RefCell<std::collections::BTreeMap<u64, usize>>,
    /// When each lane last committed anything (heartbeats are for idle lanes).
    last_commit: RefCell<HashMap<Signal, Instant>>,
    resources: crate::resource::ResourceOptions,
    /// `late_split_after` in ns (0: never split).
    late_split_ns: u64,
    /// NACKs sent upstream (a durable buffer takes them back into custody).
    nacks_sent: std::cell::Cell<u64>,
}

impl Shared {
    fn low_now(&self) -> u64 {
        let m = self.in_hands.borrow().keys().next().copied();
        custody_low(now_ns(), m, otel_arrow_dfe_otap::custody::current(), self.custody)
    }
    fn hold(&self, r: u64) {
        *self.in_hands.borrow_mut().entry(r).or_default() += 1;
    }
    fn release(&self, r: u64) {
        let mut h = self.in_hands.borrow_mut();
        if let Some(n) = h.get_mut(&r) {
            *n -= 1;
            if *n == 0 {
                let _ = h.remove(&r);
            }
        }
    }
}

/// Commits a heartbeat to the signal's first lane: a zero-byte slot with
/// `oscope-kind: beat` and `oscope-low` (`../FORMAT.md` §2).
async fn heartbeat(sh: Rc<Shared>, s: Signal) -> (Signal, Result<Ref, String>) {
    let low = sh.low_now();
    let r = empty_slot(&sh, s, 0, proto::KIND_BEAT, low).await;
    if r.is_ok() {
        let _ = sh.last_commit.borrow_mut().insert(s, Instant::now());
    }
    (s, r)
}

/// Whether the exporter may commit its orderly close at shutdown
/// (`../FORMAT.md` §3.1, DECISIONS.md D35): its custody is empty. Without a
/// buffer, custody is the requests in its hands, all resolved by then;
/// behind a durable buffer, the buffer's shutdown drain must have handed
/// every bundle over (`patches/0006`: `custody::drained`) and handled every
/// NACK the exporter sent (a NACK it never saw leaves a bundle in custody).
/// Heartbeats off (no births): no close either.
pub fn may_close(custody: Custody, heartbeats: bool, all_acked: bool, buffer_drained: bool, nacks_sent: u64, nacks_handled: u64) -> bool {
    heartbeats
        && all_acked
        && match custody {
            Custody::Exporter => true,
            Custody::DurableBuffer => buffer_drained && nacks_sent == nacks_handled,
        }
}

/// A zero-byte slot (a heartbeat or a close) in writer lane `i` of `s`, with
/// `oscope-low` = `low`, by the lane's create-only slot protocol.
async fn empty_slot(sh: &Shared, s: Signal, i: usize, kind: &str, low: u64) -> Result<Ref, String> {
    let lane = sh.lanes[&(s, i)].clone();
    let mut g = lane.lock().await;
    let st = &mut *g;
    let content = format!("{kind}-{:016x}", rand::random::<u64>());
    let mut encode = |_r: &Ref| {
        let mut meta = std::collections::BTreeMap::new();
        for (k, v) in [
            (proto::META_KIND, kind.to_string()),
            (proto::META_FORMAT, proto::FORMAT_VERSION.to_string()),
            (proto::META_CLUSTER, sh.cluster.clone()),
            (proto::META_SIGNAL, s.name().to_string()),
            (proto::META_ROWS, "0".to_string()),
            (proto::META_LOW, low.to_string()),
        ] {
            let _ = meta.insert(k.to_string(), v);
        }
        Ok(runner::Encoded { body: bytes::Bytes::new(), content_type: "application/octet-stream", meta })
    };
    runner::append(&mut st.lane, &mut st.cache, &sh.store, &sh.prefixes[&s], &sh.producer, &content, &mut encode, &sh.timeouts, &sh.stats)
        .await
        .map_err(|e| e.to_string())
}

/// The orderly close: one `oscope-kind: close` slot in every writer lane
/// of every registered signal that has an epoch (a writer lane never
/// written has none), with `oscope-low` = `low` (the empty custody's floor:
/// now). Each is an ordinary create-only slot; a lane that fails is
/// reported and left without a close (it stays stale: safe). Returns
/// (closed, failed).
async fn close_lanes(sh: &Shared, registered: &[Signal], low: u64) -> (usize, usize) {
    let mut jobs = Vec::new();
    for s in registered {
        for i in 0..sh.lanes_per_signal {
            let named = sh.lanes[&(*s, i)].try_lock().map_or(true, |g| !g.lane.epoch.is_empty());
            if named {
                jobs.push(async move { (*s, i, empty_slot(sh, *s, i, proto::KIND_CLOSE, low).await) });
            }
        }
    }
    let mut out = (0, 0);
    for (s, i, r) in futures::future::join_all(jobs).await {
        match r {
            Ok(r) => {
                out.0 += 1;
                if sh.verbose {
                    crate::log(&format!("close {} lane {i}: {}/{}", s.name(), r.epoch, r.seq));
                }
            }
            Err(e) => {
                out.1 += 1;
                crate::log(&format!("close {} lane {i}: {e} (the lane stays open: safe)", s.name()));
            }
        }
    }
    out
}

fn signal_of(t: SignalType) -> Signal {
    match t {
        SignalType::Traces => Signal::Traces,
        SignalType::Logs => Signal::Logs,
        // The request's objects are per type; the type is decided per metric.
        SignalType::Metrics => Signal::MetricsGauge,
    }
}

fn now_ns() -> u64 {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_nanos() as u64
}

/// A request's `received_at`: when it entered the edge's durable custody.
///
/// Behind a durable buffer (configs/edge-durable.yaml, edge-publisher.yaml)
/// that is the ingestion time of the request's WAL entry, which Quiver
/// persists with the bundle (patches/0003) and hands over as the pdata's
/// `ingestion_time`: every delivery of the bundle carries the same value, a
/// NACK's retry and a replay after a restart included. A replay of a request
/// whose commit landed but whose ACK was lost therefore lands in the
/// original's `toDate(received_at)` partition, where the consumer's count
/// check finds the original, however long the edge was down.
///
/// Without a buffer (configs/edge.yaml) custody passes only with the commit
/// (the client gets its answer after it), so the exporter's own receive
/// time is the custody time: `now`.
///
/// - The content key is not affected: it hashes the request's bytes, and the
///   time travels beside them, in the pdata's context.
/// - A batch processor in front of the buffer (edge-publisher.yaml) makes a
///   request of several senders' requests: its time is the WAL write of the
///   batch, when custody of all of them began (each sender's answer follows
///   that write).
/// - It is the edge's wall clock at the WAL write, as trusted as `now` was;
///   nothing re-reads the clock later, so a clock step after the write
///   changes neither the value nor a replay's copy of it. A pre-1970 or
///   missing time falls back to `now`.
/// - Every object of the request (metrics: one per type, and the series
///   object) and every row of each carries this one value, in the rows and
///   in the object's metadata, as the consumer's range guard requires.
fn received_ns(ingestion_time: Option<SystemTime>, now: impl FnOnce() -> u64) -> u64 {
    ingestion_time
        .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
        .map(|d| d.as_nanos() as u64)
        .filter(|&ns| ns > 0)
        .unwrap_or_else(now)
}

/// Content hash + flattened columns for each of a request's objects; a
/// traces or logs request with late rows is two objects (`late.rs`).
fn prepare(sh: &Shared, pdata: &OtapPdata, path: OtlpPath) -> Result<Vec<Flat>, String> {
    let flats = prepare_whole(sh, pdata, path)?;
    if sh.late_split_ns == 0 || signal_of(pdata.signal_type()).is_metrics() {
        return Ok(flats);
    }
    let enc = sh.encoder.borrow();
    let mut out = Vec::with_capacity(flats.len() + 1);
    for f in flats {
        // The parts' keys: over the request's bytes when it has them (both
        // OTLP paths, as the Go edge), else over the part's rows.
        let parts = match pdata.payload_ref().data() {
            PayloadData::OtlpBytes(b) => {
                let bytes: &[u8] = match b {
                    OtlpProtoBytes::ExportTracesRequest(x)
                    | OtlpProtoBytes::ExportLogsRequest(x)
                    | OtlpProtoBytes::ExportMetricsRequest(x) => x,
                };
                enc.split_late(f, sh.late_split_ns, |ns, _| crate::batch::content_hash_named(ns, bytes))
            }
            PayloadData::OtapArrowRecords(_) => enc.split_late(f, sh.late_split_ns, crate::batch::content_hash_cols_named),
        };
        out.extend(parts.map_err(|e| e.0)?);
    }
    Ok(out)
}

fn prepare_whole(sh: &Shared, pdata: &OtapPdata, path: OtlpPath) -> Result<Vec<Flat>, String> {
    let signal = signal_of(pdata.signal_type());
    let mut enc = sh.encoder.borrow_mut();
    match pdata.payload_ref().data() {
        PayloadData::OtlpBytes(b) => {
            let (bytes, input) = match b {
                OtlpProtoBytes::ExportTracesRequest(x) | OtlpProtoBytes::ExportLogsRequest(x) => {
                    (x, Input::Otlp(signal, x))
                }
                OtlpProtoBytes::ExportMetricsRequest(x) => (x, Input::OtlpMetrics(x)),
            };
            match path {
                OtlpPath::Direct => enc.flatten_all(&input).map_err(|e| e.0),
                OtlpPath::ViaOtap => {
                    let recs: OtapArrowRecords = b.clone().try_into_with_default().map_err(|e| format!("{e}"))?;
                    let input = if signal.is_metrics() { Input::OtapMetrics(&recs) } else { Input::Otap(signal, &recs) };
                    let mut fs = enc.flatten_all(&input).map_err(|e| e.0)?;
                    // Keep the request's content keys, so both paths dedup alike
                    // (a series object is keyed by its own content on both).
                    for f in fs.iter_mut().filter(|f| f.signal != Signal::MetricsSeries) {
                        f.content = crate::batch::content_hash_otlp(f.signal, bytes);
                    }
                    Ok(fs)
                }
            }
        }
        PayloadData::OtapArrowRecords(r) => {
            // From an OTAP receiver the parent ids are still in the transport-
            // optimized (delta, quasi-delta) encoding, which the views don't
            // undo: without this every span's attribute lookup matches
            // thousands of rows (found with the Go otelarrow producer:
            // ~9,000 attributes per span and a 13 GB RSS on metrics). The
            // record batches are Arc'd, so the clone is shallow.
            let mut r = r.clone();
            r.decode_transport_optimized_ids().map_err(|e| format!("decode OTAP ids: {e}"))?;
            let input = if signal.is_metrics() { Input::OtapMetrics(&r) } else { Input::Otap(signal, &r) };
            enc.flatten_all(&input).map_err(|e| e.0)
        }
    }
}

/// Appends one object to its signal's lane.
async fn commit_one(sh: Rc<Shared>, flat: Flat, received_ns: u64) -> (Signal, PartOutcome) {
    let started = Instant::now();
    let n = sh.lanes_per_signal;
    let idx = if n <= 1 {
        0
    } else {
        u64::from_str_radix(&flat.content[..16], 16).unwrap_or(0) as usize % n
    };
    let lane = sh.lanes[&(flat.signal, idx)].clone();
    let mut g = lane.lock().await;
    let st = &mut *g;
    let prefix = &sh.prefixes[&flat.signal];
    let producer = sh.producer.clone();
    // Announcements (traces, logs): decided for the slot's epoch, from the
    // lane's cache, when the object is encoded; marked once it committed.
    let opts = &sh.resources;
    let window = opts.window_of(received_ns);
    let announced: RefCell<Vec<u64>> = RefCell::new(Vec::new());
    let ann = &st.ann;
    let mut encode = |r: &Ref| {
        let env = Envelope {
            producer: producer.clone(),
            epoch: r.epoch.clone(),
            batch: r.seq,
            received_ns,
        };
        let wants = |id: u64| opts.announce && ann.wants(&r.epoch, id, window);
        let (mut o, ids) = sh.encoder.borrow().encode_announcing(&flat, &env, &wants).map_err(|e| e.0)?;
        *announced.borrow_mut() = ids;
        let _ = o.meta.insert(proto::META_FORMAT.to_string(), proto::FORMAT_VERSION.to_string());
        let _ = o.meta.insert(proto::META_CLUSTER.to_string(), sh.cluster.clone());
        // Computed when the object is encoded for its slot, and cached with
        // its bytes: a resend into the slot carries the same value.
        let _ = o.meta.insert(proto::META_LOW.to_string(), sh.low_now().to_string());
        Ok(o)
    };
    let res = runner::append(
        &mut st.lane,
        &mut st.cache,
        &sh.store,
        prefix,
        &sh.producer,
        &flat.content,
        &mut encode,
        &sh.timeouts,
        &sh.stats,
    )
    .await;
    // The one new rule of layout B: a series counts as announced only once
    // the series object that carried it has committed (here, as ours or
    // found committed), in that lane's epoch.
    if let (Ok(r), false) = (&res, flat.announce.is_empty()) {
        sh.encoder.borrow_mut().series_announced(&flat.announce, &r.epoch);
    }
    // The same rule for resources: announced once the object that carried
    // them has committed (entityCatalog.qnt announcedAfterCommit). An object
    // found committed without being encoded here marks nothing (they are
    // announced again: harmless).
    if let Ok(r) = &res {
        let ids = announced.take();
        if !ids.is_empty() {
            st.ann.announced(&r.epoch, &ids, window, opts.cache_size);
        }
    }
    if sh.verbose {
        match &res {
            Ok(r) => crate::log(&format!(
                "{} rows={} content={} -> {}/{} in {:?}",
                flat.signal.name(),
                flat.stats.rows,
                flat.content,
                r.epoch,
                r.seq,
                started.elapsed()
            )),
            Err(e) => crate::log(&format!("{} content={}: {e}", flat.signal.name(), flat.content)),
        }
    }
    if res.is_ok() {
        let _ = sh.last_commit.borrow_mut().insert(flat.signal, Instant::now());
    }
    let out = match res {
        Ok(r) => PartOutcome::Committed(r),
        Err(AppendError::Encode(e)) => PartOutcome::Rejected(e),
        Err(e @ AppendError::Unresolved(_)) => PartOutcome::Unresolved(e.to_string()),
    };
    (flat.signal, out)
}

/// Commits every object of a request, concurrently (each in its own lane).
fn commit(sh: Rc<Shared>, pdata: OtapPdata, flats: Vec<Flat>, received_ns: u64) -> LocalBoxFuture<'static, Done> {
    Box::pin(async move {
        let started = Instant::now();
        let rows = flats.iter().map(|f| f.stats.rows).sum();
        sh.hold(received_ns);
        let parts = if flats.iter().any(|f| f.split.is_some()) {
            // A split request's parts share a lane: bulk, then late, in
            // order, as the Go edge appends them (late.rs).
            let mut v = Vec::with_capacity(flats.len());
            for f in flats {
                v.push(commit_one(sh.clone(), f, received_ns).await);
            }
            v
        } else {
            futures::future::join_all(flats.into_iter().map(|f| commit_one(sh.clone(), f, received_ns))).await
        };
        sh.release(received_ns);
        (pdata, parts, started, rows)
    })
}

#[async_trait(?Send)]
impl Exporter<OtapPdata> for S3pqExporter {
    async fn start(
        self: Box<Self>,
        mut inbox: ExporterInbox<OtapPdata>,
        effect_handler: EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        let cfg = self.config;
        let mut outcomes = self.outcomes;
        let store = cfg.s3.build().map_err(|e| Error::ExporterError {
            exporter: effect_handler.exporter_id(),
            kind: ExporterErrorKind::Configuration,
            error: e.to_string(),
            source_detail: String::new(),
        })?;
        let mut lanes = HashMap::new();
        let mut prefixes = HashMap::new();
        for s in Signal::ALL {
            let _ = prefixes.insert(s, proto::lane_prefix(&store.prefix, &cfg.cluster, &cfg.producer_id, s.name()));
            for i in 0..cfg.lanes.max(1) {
                let _ = lanes.insert(
                    (s, i),
                    Rc::new(tokio::sync::Mutex::new(LaneState {
                        // Named at its first write (runner::append).
                        lane: Lane::new(String::new()),
                        cache: EncodedCache::default(),
                        ann: Default::default(),
                    })),
                );
            }
        }
        crate::log(&format!(
            "exporter start: {} lanes, epochs (named at each lane's first write) {:?}",
            cfg.lanes,
            lanes.iter().map(|(k, v)| (k.0.name(), k.1, v.try_lock().map(|g| g.lane.epoch.clone()).unwrap_or_default())).collect::<Vec<_>>()
        ));
        let sh = Rc::new(Shared {
            timeouts: Timeouts { put: cfg.s3.put_timeout, head: cfg.s3.head_timeout },
            store,
            encoder: RefCell::new(
                Encoder::new(cfg.parquet.clone(), cfg.format).with_metrics_layout(cfg.metrics_layout, cfg.series.clone()),
            ),
            stats: Stats::default(),
            producer: cfg.producer_id.clone(),
            cluster: cfg.cluster.clone(),
            lanes,
            lanes_per_signal: cfg.lanes.max(1),
            prefixes,
            verbose: cfg.verbose,
            custody: cfg.custody,
            in_hands: RefCell::new(Default::default()),
            last_commit: RefCell::new(HashMap::new()),
            resources: cfg.resources.clone(),
            late_split_ns: cfg.late_split_after.as_nanos().min(u64::MAX as u128) as u64,
            nacks_sent: std::cell::Cell::new(0),
        });
        // Heartbeats: the births first (every lane this publisher can write
        // is registered before it takes a request, up to birth_timeout),
        // then one per lane idle for the interval.
        let beat_every = cfg.heartbeat.interval;
        let registered = if beat_every.is_zero() {
            Vec::new()
        } else {
            registered_signals(cfg.metrics_layout, cfg.series.merge_number_points)
        };
        let mut beats: FuturesUnordered<LocalBoxFuture<'static, (Signal, Result<Ref, String>)>> = FuturesUnordered::new();
        let mut beating: std::collections::HashSet<Signal> = std::collections::HashSet::new();
        if !registered.is_empty() {
            let until = tokio::time::Instant::now() + cfg.heartbeat.birth_timeout;
            let mut born = std::collections::HashSet::new();
            while born.len() < registered.len() && tokio::time::Instant::now() < until {
                let round: Vec<_> = registered.iter().filter(|s| !born.contains(*s)).map(|s| heartbeat(sh.clone(), *s)).collect();
                for (s, r) in futures::future::join_all(round).await {
                    match r {
                        Ok(_) => {
                            let _ = born.insert(s);
                        }
                        Err(e) => crate::log(&format!("birth heartbeat {}: {e}", s.name())),
                    }
                }
                if born.len() < registered.len() {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
            }
            crate::log(&format!("births: {} of {} lanes registered", born.len(), registered.len()));
        }
        let mut next_beat = tokio::time::Instant::now() + beat_every / 2;
        let max_in_flight = cfg.lanes.max(1) * 2;
        let mut in_flight: FuturesUnordered<LocalBoxFuture<'static, Done>> = FuturesUnordered::new();

        let finish = |d: Done, eh: &EffectHandler<OtapPdata>| {
            let (pdata, parts, _started, _rows) = d;
            let eh = eh.clone();
            let sh = sh.clone();
            async move {
                if !matches!(proto::request_verdict(parts.iter().map(|(_, o)| o)), Verdict::Ack) {
                    sh.nacks_sent.set(sh.nacks_sent.get() + 1);
                }
                match proto::request_verdict(parts.iter().map(|(_, o)| o)) {
                    Verdict::Ack => eh.notify_ack(AckMsg::new(pdata)).await,
                    // The data can't be encoded: a client error (OTLP 400 / INVALID_ARGUMENT).
                    Verdict::Reject(e) => {
                        eh.notify_nack(NackMsg::new_permanent_with_cause(e, pdata, NackCause::Refused)).await
                    }
                    Verdict::Retry(e) => {
                        if parts.len() > 1 {
                            let done: Vec<&str> = parts
                                .iter()
                                .filter(|(_, o)| matches!(o, PartOutcome::Committed(_)))
                                .map(|(s, _)| s.name())
                                .collect();
                            crate::log(&format!("nack: {} of {} objects committed ({done:?}); {e}", done.len(), parts.len()));
                        }
                        eh.notify_nack(NackMsg::new(e, pdata)).await
                    }
                }
            }
        };

        loop {
            let accepting = in_flight.len() < max_in_flight;
            let msg = tokio::select! {
                biased;
                Some(d) = in_flight.next(), if !in_flight.is_empty() => {
                    finish(d, &effect_handler).await?;
                    continue;
                }
                Some((s, r)) = beats.next(), if !beats.is_empty() => {
                    let _ = beating.remove(&s);
                    if let Err(e) = r {
                        crate::log(&format!("heartbeat {}: {e}", s.name()));
                    }
                    continue;
                }
                _ = tokio::time::sleep_until(next_beat), if !registered.is_empty() => {
                    next_beat = tokio::time::Instant::now() + beat_every / 2;
                    let due: Vec<Signal> = registered
                        .iter()
                        .copied()
                        .filter(|s| !beating.contains(s))
                        .filter(|s| sh.last_commit.borrow().get(s).is_none_or(|t| t.elapsed() >= beat_every))
                        .collect();
                    for s in due {
                        let _ = beating.insert(s);
                        beats.push(Box::pin(heartbeat(sh.clone(), s)));
                    }
                    continue;
                }
                m = inbox.recv_when(accepting) => m?,
            };
            match msg {
                Message::Control(NodeControlMsg::Shutdown { deadline, .. }) => {
                    let until = tokio::time::Instant::from_std(deadline);
                    while !in_flight.is_empty() {
                        match tokio::time::timeout_at(until, in_flight.next()).await {
                            Ok(Some(d)) => finish(d, &effect_handler).await?,
                            _ => break,
                        }
                    }
                    // The orderly close (../FORMAT.md §3.1): only with the
                    // custody empty, heartbeats settled first (a heartbeat
                    // left in flight must not land after the close), and
                    // within the shutdown budget; otherwise no close, and the
                    // lanes stay stale (safe).
                    // (Without a buffer a NACKed request is its sender's
                    // again; behind one, may_close counts the NACKs.)
                    let all_acked = in_flight.is_empty();
                    while !beats.is_empty() {
                        match tokio::time::timeout_at(until, beats.next()).await {
                            Ok(Some(_)) => {}
                            _ => break,
                        }
                    }
                    let ok = beats.is_empty()
                        && may_close(
                            cfg.custody,
                            !registered.is_empty(),
                            all_acked,
                            otel_arrow_dfe_otap::custody::drained(),
                            sh.nacks_sent.get(),
                            otel_arrow_dfe_otap::custody::nacks_handled(),
                        );
                    if ok {
                        match tokio::time::timeout_at(until, close_lanes(&sh, &registered, now_ns())).await {
                            Ok((n, 0)) => crate::log(&format!("close: {n} writer lanes closed (custody empty)")),
                            Ok((n, f)) => crate::log(&format!("close: {n} writer lanes closed, {f} failed (those stay open)")),
                            Err(_) => crate::log("close: the shutdown deadline passed (the lanes not closed stay open)"),
                        }
                    } else {
                        crate::log(&format!(
                            "close: none (custody {:?}, all acked {all_acked}, buffer drained {}, nacks sent {} handled {}, heartbeats settled {})",
                            cfg.custody,
                            otel_arrow_dfe_otap::custody::drained(),
                            sh.nacks_sent.get(),
                            otel_arrow_dfe_otap::custody::nacks_handled(),
                            beats.is_empty()
                        ));
                    }
                    let s = &sh.stats;
                    crate::log(&format!(
                        "exporter stop: committed={} resolved_own={} resent={} learned_other={} halted={} known_skipped={} unresolved={} inconsistent={} encodes={} puts={} heads={} abandoned={}",
                        s.committed.get(), s.resolved_own.get(), s.resent.get(), s.learned_other.get(),
                        s.halted.get(), s.known_skipped.get(), s.unresolved.get(), s.inconsistent.get(),
                        s.encodes.get(), s.puts.get(), s.heads.get(),
                        in_flight.len()
                    ));
                    return Ok(TerminalState::new(deadline, Vec::<otel_arrow_dfe_telemetry::metrics::MetricSetSnapshot>::new()));
                }
                Message::Control(NodeControlMsg::CollectTelemetry { mut metrics_reporter }) => {
                    let _ = outcomes.report(&sh.stats, &mut metrics_reporter);
                }
                Message::Control(_) => {}
                Message::PData(pdata) => {
                    if pdata.is_empty() {
                        effect_handler.notify_ack(AckMsg::new(pdata)).await?;
                        continue;
                    }
                    let received_ns = received_ns(pdata.ingestion_time(), now_ns);
                    let t_prep = Instant::now();
                    let prepared = prepare(&sh, &pdata, cfg.otlp_path);
                    if sh.verbose {
                        crate::log(&format!(
                            "prepare ({}): {:?}",
                            if matches!(pdata.payload_ref().data(), PayloadData::OtapArrowRecords(_)) { "otap" } else { "otlp" },
                            t_prep.elapsed()
                        ));
                    }
                    match prepared {
                        // A metrics request without data points: nothing to write.
                        Ok(flats) if flats.is_empty() => effect_handler.notify_ack(AckMsg::new(pdata)).await?,
                        Ok(flats) => in_flight.push(commit(sh.clone(), pdata, flats, received_ns)),
                        Err(e) => {
                            crate::log(&format!("rejecting request: {e}"));
                            effect_handler
                                .notify_nack(NackMsg::new_permanent_with_cause(e, pdata, NackCause::Refused))
                                .await?;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn oscope_low_is_the_custody_floor() {
        // nothing held: the send time
        assert_eq!(custody_low(100, None, None, Custody::Exporter), 100);
        // requests in the exporter's hands (its own included)
        assert_eq!(custody_low(100, Some(40), None, Custody::Exporter), 40);
        // a buffer's published floor, older than anything in hand
        assert_eq!(custody_low(100, Some(40), Some(25), Custody::DurableBuffer), 25);
        // a configured buffer that has not published yet: 0 (sound; holds the watermark)
        assert_eq!(custody_low(100, Some(40), None, Custody::DurableBuffer), 0);
        // a floor published where none was configured is still honoured
        assert_eq!(custody_low(100, None, Some(60), Custody::Exporter), 60);
    }

    /// D35: the close only with the custody empty (FORMAT.md §3.1).
    #[test]
    fn the_close_needs_an_empty_custody() {
        // without a buffer: every request in hand resolved
        assert!(may_close(Custody::Exporter, true, true, false, 3, 0));
        assert!(!may_close(Custody::Exporter, true, false, false, 0, 0), "a request still in flight");
        assert!(!may_close(Custody::Exporter, false, true, false, 0, 0), "no heartbeats, no births: no close");
        // behind a durable buffer: its drain handed everything over, and it saw every NACK
        assert!(may_close(Custody::DurableBuffer, true, true, true, 2, 2));
        assert!(!may_close(Custody::DurableBuffer, true, true, false, 0, 0), "the buffer's drain did not finish");
        assert!(!may_close(Custody::DurableBuffer, true, true, true, 3, 2), "a NACK the buffer never handled: its bundle is still in custody");
        assert!(!may_close(Custody::DurableBuffer, true, false, true, 0, 0));
    }

    #[test]
    fn registered_lanes_follow_the_layout() {
        let names = |v: Vec<Signal>| v.into_iter().map(|s| s.name()).collect::<Vec<_>>();
        assert_eq!(
            names(registered_signals(MetricsLayout::SeriesTable, true)),
            ["traces", "logs", "metrics_number_points", "metrics_histogram_points", "metrics_exponential_histogram_points", "metrics_summary_points", "metrics_series"]
        );
        assert!(names(registered_signals(MetricsLayout::SeriesTable, false)).contains(&"metrics_gauge_points"));
        assert_eq!(registered_signals(MetricsLayout::ClickstackTables, true).len(), 7);
    }

    #[test]
    fn received_at_is_the_custody_time_when_the_buffer_has_one() {
        let _trace = crate::oscope_trace::covers("P2C", &["CAST-1", "H-2", "UCA-2", "LS-1"]);
        let at = SystemTime::UNIX_EPOCH + Duration::from_nanos(1_700_000_000_123_456_789);
        // A replay days later reports the WAL write, not the redelivery.
        assert_eq!(received_ns(Some(at), || 1_800_000_000_000_000_000), 1_700_000_000_123_456_789);
    }

    #[test]
    fn received_at_falls_back_to_now_without_a_buffer() {
        assert_eq!(received_ns(None, || 42), 42);
        assert_eq!(received_ns(Some(SystemTime::UNIX_EPOCH), || 42), 42);
    }

    #[test]
    fn received_at_rides_beside_the_bytes() {
        // The time is context, not payload: the same request with and without
        // it has the same content key.
        let bytes = b"\x0a\x02\x0a\x00".to_vec();
        let mut a = OtapPdata::new_todo_context(
            OtlpProtoBytes::ExportTracesRequest(bytes::Bytes::from(bytes.clone())).into(),
        );
        let b = a.clone();
        a.set_ingestion_time(SystemTime::UNIX_EPOCH + Duration::from_secs(1));
        let key = |p: &OtapPdata| match p.payload_ref().data() {
            PayloadData::OtlpBytes(OtlpProtoBytes::ExportTracesRequest(x)) => {
                crate::batch::content_hash_otlp(Signal::Traces, x)
            }
            _ => unreachable!(),
        };
        assert_eq!(key(&a), key(&b));
        assert_eq!(a.ingestion_time(), Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1)));
        assert_eq!(b.ingestion_time(), None);
    }
}
