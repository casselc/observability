//! One request → its content hash, its flattened columns, and the encoded
//! object for a given slot.

use crate::encode::{self, ParquetOptions};
use crate::flatten::{Envelope, LogsBuf, Stats, TracesBuf, record_batch};
use crate::metrics::MetricsBuf;
use crate::proto;
use crate::runner::Encoded;
use crate::resource::Covered;
use crate::schema::Schemas;
use crate::series::{MetricsLayout, SeriesBuf, SeriesOptions};
use crate::Signal;
use arrow::array::ArrayRef;
use bytes::Bytes;
use otel_arrow_dfe_pdata::OtapArrowRecords;
use otel_arrow_dfe_pdata::views::otap::{OtapLogsView, OtapMetricsView, OtapTracesView};
use otel_arrow_dfe_pdata::views::otlp::bytes::logs::RawLogsData;
use otel_arrow_dfe_pdata::views::otlp::bytes::metrics::RawMetricsData;
use otel_arrow_dfe_pdata::views::otlp::bytes::traces::RawTraceData;
use serde::Deserialize;
use std::collections::BTreeMap;

pub const PARQUET_CONTENT_TYPE: &str = "application/vnd.apache.parquet";
pub const ARROW_CONTENT_TYPE: &str = "application/vnd.apache.arrow.file";

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Format {
    #[default]
    Parquet,
    Arrow,
}

/// The input of one batch.
pub enum Input<'a> {
    /// A serialized ExportTraceServiceRequest / ExportLogsServiceRequest.
    Otlp(Signal, &'a [u8]),
    /// OTAP Arrow record batches.
    Otap(Signal, &'a OtapArrowRecords),
    /// A serialized ExportMetricsServiceRequest: one object per metric type.
    OtlpMetrics(&'a [u8]),
    /// OTAP metrics record batches.
    OtapMetrics(&'a OtapArrowRecords),
}

/// A flattened batch: the content columns (no envelope yet) and stats.
pub struct Flat {
    pub signal: Signal,
    pub content: String,
    pub cols: Vec<ArrayRef>,
    pub stats: Stats,
    /// A series object's new series (id, cache window): mark them announced
    /// once this object has committed (`Encoder::series_announced`).
    pub announce: Vec<(u64, i32)>,
    /// Traces and logs: the object's distinct resources, each with the first
    /// row that uses it (`resource.rs`); the rows' ids are the last content
    /// column.
    pub resources: Vec<(Covered, u32)>,
    /// Set on the two objects of a request split by event time (`late.rs`,
    /// DECISIONS.md D31): which part, and the bound in ns.
    pub split: Option<(crate::late::Part, u64)>,
    /// Traces and logs: the request's payloads (`offload.rs`), each with the
    /// first row (walk order) that references it; the rows' references are
    /// the `payload_refs` content column.
    pub payloads: Vec<crate::offload::Payload>,
    /// What the offloader did to this object's values.
    pub offload: crate::offload::OffloadStats,
}

impl Flat {
    /// A flat without resources or payloads (metrics).
    pub fn plain(signal: Signal, content: String, cols: Vec<ArrayRef>, stats: Stats, announce: Vec<(u64, i32)>) -> Self {
        Flat { signal, content, cols, stats, announce, resources: Vec::new(), split: None, payloads: Vec::new(), offload: Default::default() }
    }
}

/// Content hash of an OTLP request: BLAKE3 over "{signal}\0{protobuf}",
/// 128 bits in hex. The same idea as ../awss3/inline's SHA-256 key (and the
/// same length); BLAKE3 because SHA-256 without SHA-NI costs ~5 ms per
/// 10k-span request here. A client's retry resends the same bytes, so it
/// hashes the same in any process.
pub fn content_hash_otlp(signal: Signal, bytes: &[u8]) -> String {
    content_hash_named(signal.name(), bytes)
}

/// `content_hash_otlp` under any namespace name (a split part's, `late.rs`):
/// BLAKE3 over "{name}\0{protobuf}", parquetgo `commit.ContentHash`.
pub fn content_hash_named(name: &str, bytes: &[u8]) -> String {
    let mut h = blake3::Hasher::new();
    let _ = h.update(name.as_bytes());
    let _ = h.update(&[0]);
    let _ = h.update(bytes);
    hex::encode(&h.finalize().as_bytes()[..16])
}

/// Content hash of flattened rows (for OTAP input, which has no canonical
/// bytes): BLAKE3 over every content column's buffers.
pub fn content_hash_cols(signal: Signal, cols: &[ArrayRef]) -> String {
    content_hash_cols_named(signal.name(), cols)
}

/// `content_hash_cols` under any namespace name (a split part's, `late.rs`).
pub fn content_hash_cols_named(name: &str, cols: &[ArrayRef]) -> String {
    fn feed(h: &mut blake3::Hasher, d: &arrow::array::ArrayData) {
        let _ = h.update(&(d.len() as u64).to_le_bytes());
        for b in d.buffers() {
            let _ = h.update(&(b.len() as u64).to_le_bytes());
            let _ = h.update(b.as_slice());
        }
        for c in d.child_data() {
            feed(h, c);
        }
    }
    let mut h = blake3::Hasher::new();
    let _ = h.update(b"rows:");
    let _ = h.update(name.as_bytes());
    for c in cols {
        feed(&mut h, &c.to_data());
    }
    hex::encode(&h.finalize().as_bytes()[..16])
}

/// A trace or log batch in (ServiceName, Timestamp) order, and its plan.
pub fn sort_batch(
    rb: &arrow::array::RecordBatch,
    o: &encode::SortOptions,
) -> Result<(encode::SortPlan, arrow::array::RecordBatch), arrow::error::ArrowError> {
    use arrow::array::{BinaryArray, TimestampNanosecondArray};
    let col = |name: &str| {
        rb.column_by_name(name)
            .ok_or_else(|| arrow::error::ArrowError::SchemaError(format!("no {name} column")))
    };
    let svc = col("ServiceName")?;
    let ts = col("Timestamp")?;
    let svc = svc
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| arrow::error::ArrowError::SchemaError("ServiceName is not Binary".into()))?;
    let ts = ts
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .ok_or_else(|| arrow::error::ArrowError::SchemaError("Timestamp is not ns".into()))?;
    let plan = encode::sort_plan(svc, ts, o);
    let idx = arrow::array::UInt32Array::from(plan.perm.clone());
    let sorted = arrow::compute::take_record_batch(rb, &idx)?;
    Ok((plan, sorted))
}

/// Reusable per-signal state: schemas and column buffers.
pub struct Encoder {
    pub traces_sc: Schemas,
    pub logs_sc: Schemas,
    /// One per metric type, in `Signal::METRICS` order.
    pub metrics_sc: Vec<Schemas>,
    traces: TracesBuf,
    logs: LogsBuf,
    metrics: MetricsBuf,
    /// Layout B (`series.rs`): its encoder and series cache, and schemas.
    series: SeriesBuf,
    series_sc: Vec<Schemas>,
    pub layout: MetricsLayout,
    pub opts: ParquetOptions,
    pub format: Format,
    /// The payload offloader's policy and the edge's cluster (`offload.rs`);
    /// None: values stay inline.
    pub offload: Option<(std::sync::Arc<crate::offload::Policy>, String)>,
    /// The current request's `received_at` (the payload hash's day).
    received_ns: u64,
}

/// What an object carries for its slot, decided when it is encoded:
/// the resources it announces and the payloads it carries.
pub struct Carried {
    pub announced: Vec<u64>,
    pub payloads: Vec<[u8; 16]>,
}

#[derive(Debug)]
pub struct EncodeError(pub String);
impl std::fmt::Display for EncodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl Encoder {
    pub fn new(opts: ParquetOptions, format: Format) -> Self {
        Self {
            traces_sc: Schemas::new(Signal::Traces),
            logs_sc: Schemas::new(Signal::Logs),
            metrics_sc: Signal::METRICS.iter().map(|s| Schemas::new(*s)).collect(),
            traces: TracesBuf::default(),
            logs: LogsBuf::default(),
            metrics: MetricsBuf::default(),
            series: SeriesBuf::new(SeriesOptions::default()),
            series_sc: Signal::SERIES_LAYOUT.iter().map(|s| crate::series::schemas(*s, &SeriesOptions::default())).collect(),
            layout: MetricsLayout::ClickstackTables,
            opts,
            format,
            offload: None,
            received_ns: 0,
        }
    }

    /// Offloads large values by reference (`offload.rs`) under `opts`,
    /// hashed for tenants of `cluster`; `opts.enabled` false: no offloading.
    pub fn with_offload(mut self, opts: crate::offload::OffloadOptions, cluster: &str) -> Self {
        self.offload = opts.enabled.then(|| (std::sync::Arc::new(crate::offload::Policy::new(opts)), cluster.to_string()));
        self
    }

    /// `flatten_all` for a request received (entered custody) at
    /// `received_ns`: the day of the payload hashes.
    pub fn flatten_all_at(&mut self, input: &Input<'_>, received_ns: u64) -> Result<Vec<Flat>, EncodeError> {
        self.received_ns = received_ns;
        self.flatten_all(input)
    }

    fn offloader(&self) -> Option<crate::offload::Offloader> {
        self.offload.as_ref().map(|(p, c)| crate::offload::Offloader::new(p.clone(), c, self.received_ns))
    }

    /// Metrics as layout B (`series_table`) with these options, or as the
    /// ClickStack tables. The series cache starts empty.
    pub fn with_metrics_layout(mut self, layout: MetricsLayout, o: SeriesOptions) -> Self {
        self.layout = layout;
        self.series_sc = Signal::SERIES_LAYOUT.iter().map(|s| crate::series::schemas(*s, &o)).collect();
        self.series = SeriesBuf::new(o);
        self
    }

    pub fn series_options(&self) -> &SeriesOptions {
        &self.series.opts
    }

    /// Marks a committed series object's series as announced, in its epoch.
    pub fn series_announced(&mut self, ids: &[(u64, i32)], epoch: &str) {
        self.series.announced(ids, epoch);
    }

    pub fn series_cache_len(&self) -> usize {
        self.series.cache_len()
    }

    pub fn schemas(&self, s: Signal) -> &Schemas {
        if let Some(i) = Signal::SERIES_LAYOUT.iter().position(|x| *x == s) {
            return &self.series_sc[i];
        }
        match s {
            Signal::Traces => &self.traces_sc,
            Signal::Logs => &self.logs_sc,
            m => &self.metrics_sc[Signal::METRICS.iter().position(|x| *x == m).expect("a metric type")],
        }
    }

    /// Walks any input into its objects' columns: one `Flat` for traces or
    /// logs, one per non-empty metric type for metrics (none for a request
    /// without data points).
    pub fn flatten_all(&mut self, input: &Input<'_>) -> Result<Vec<Flat>, EncodeError> {
        let e = |m: &dyn std::fmt::Display| EncodeError(m.to_string());
        match input {
            Input::OtlpMetrics(b) if self.layout == MetricsLayout::SeriesTable => {
                let v = RawMetricsData::try_new(b).map_err(|x| e(&x))?;
                self.series.fill(&v).map_err(|x| EncodeError(x.0))?;
                Ok(self.series_flats(|sig, _| content_hash_otlp(sig, b)))
            }
            Input::OtapMetrics(r) if self.layout == MetricsLayout::SeriesTable => {
                let v = OtapMetricsView::try_from(*r).map_err(|x| e(&x))?;
                self.series.fill(&v).map_err(|x| EncodeError(x.0))?;
                Ok(self.series_flats(content_hash_cols))
            }
            Input::OtlpMetrics(b) => {
                let v = RawMetricsData::try_new(b).map_err(|x| e(&x))?;
                self.metrics.fill(&v).map_err(|x| EncodeError(x.0))?;
                Ok(self.metrics_flats(|sig, _| content_hash_otlp(sig, b)))
            }
            Input::OtapMetrics(r) => {
                let v = OtapMetricsView::try_from(*r).map_err(|x| e(&x))?;
                self.metrics.fill(&v).map_err(|x| EncodeError(x.0))?;
                Ok(self.metrics_flats(content_hash_cols))
            }
            other => Ok(vec![self.flatten(other)?]),
        }
    }

    fn metrics_flats(&mut self, key: impl Fn(Signal, &[ArrayRef]) -> String) -> Vec<Flat> {
        let mut out = Vec::new();
        for (i, sig) in Signal::METRICS.into_iter().enumerate() {
            let stats = self.metrics.stats(sig);
            // Always take the columns, so the buffers are reset for the next request.
            let cols = self.metrics.content_arrays(sig, &self.metrics_sc[i]);
            if stats.rows > 0 {
                let content = key(sig, &cols);
                out.push(Flat::plain(sig, content, cols, stats, Vec::new()));
            }
        }
        out
    }

    /// Layout B's objects: the non-empty points objects, keyed like the
    /// ClickStack ones (`key`: the request's hash per namespace), then the
    /// series object if any series is new, keyed by its own content.
    fn series_flats(&mut self, key: impl Fn(Signal, &[ArrayRef]) -> String) -> Vec<Flat> {
        let mut out = Vec::new();
        let mut sigs = self.series.opts.point_signals();
        sigs.push(Signal::MetricsSeries);
        for sig in sigs {
            let stats = self.series.stats(sig);
            let i = Signal::SERIES_LAYOUT.iter().position(|x| *x == sig).expect("layout B");
            let cols = self.series.content_arrays(sig, &self.series_sc[i]);
            if stats.rows == 0 {
                continue;
            }
            if sig == Signal::MetricsSeries {
                let content = content_hash_cols(sig, &cols);
                out.push(Flat::plain(sig, content, cols, stats, self.series.take_new()));
            } else {
                let content = key(sig, &cols);
                out.push(Flat::plain(sig, content, cols, stats, Vec::new()));
            }
        }
        out
    }

    /// Walks the input into columns.
    pub fn flatten(&mut self, input: &Input<'_>) -> Result<Flat, EncodeError> {
        let e = |m: &dyn std::fmt::Display| EncodeError(m.to_string());
        self.traces.off = self.offloader();
        self.logs.off = self.offloader();
        let (signal, stats, cols, resources, off) = match input {
            Input::Otlp(Signal::Traces, b) => {
                let v = RawTraceData::try_new(b).map_err(|x| e(&x))?;
                let st = self.traces.fill(&v);
                (Signal::Traces, st, self.traces.content_arrays(&self.traces_sc), self.traces.take_resources(), self.traces.off.take())
            }
            Input::Otlp(Signal::Logs, b) => {
                let v = RawLogsData::try_new(b).map_err(|x| e(&x))?;
                let st = self.logs.fill(&v);
                (Signal::Logs, st, self.logs.content_arrays(&self.logs_sc), self.logs.take_resources(), self.logs.off.take())
            }
            Input::Otap(Signal::Traces, r) => {
                let v = OtapTracesView::try_from(*r).map_err(|x| e(&x))?;
                let st = self.traces.fill(&v);
                (Signal::Traces, st, self.traces.content_arrays(&self.traces_sc), self.traces.take_resources(), self.traces.off.take())
            }
            Input::Otap(Signal::Logs, r) => {
                let v = OtapLogsView::try_from(*r).map_err(|x| e(&x))?;
                let st = self.logs.fill(&v);
                (Signal::Logs, st, self.logs.content_arrays(&self.logs_sc), self.logs.take_resources(), self.logs.off.take())
            }
            _ => return Err(EncodeError("metrics input has one object per type: use flatten_all".into())),
        };
        let (payloads, offload) = match off {
            Some(mut o) => (o.take_payloads(), o.stats),
            None => (Vec::new(), Default::default()),
        };
        let content = match input {
            Input::Otlp(s, b) => content_hash_otlp(*s, b),
            Input::Otap(s, _) => content_hash_cols(*s, &cols),
            _ => unreachable!(),
        };
        Ok(Flat { signal, content, cols, stats, announce: Vec::new(), resources, split: None, payloads, offload })
    }

    /// The object for one slot: envelope added, encoded, described. Every
    /// resource of a trace or log object is announced (no cache).
    pub fn encode(&self, f: &Flat, env: &Envelope) -> Result<Encoded, EncodeError> {
        self.encode_carrying(f, env, &|_| true, &|_| true).map(|(o, _)| o)
    }

    /// The object's rows, unsorted: the content columns, `resource_announce`
    /// (traces and logs: the resources `wants` names), `payloads` (the
    /// payloads `wants_payload` names, `offload.rs`) and the envelope; and
    /// what the object carries.
    pub fn rows(&self, f: &Flat, env: &Envelope, wants: &dyn Fn(u64) -> bool, wants_payload: &dyn Fn(&[u8; 16]) -> bool) -> (arrow::array::RecordBatch, Carried) {
        let sc = self.schemas(f.signal);
        let mut cols = f.cols.clone();
        let mut carried = Carried { announced: Vec::new(), payloads: Vec::new() };
        if matches!(f.signal, Signal::Traces | Signal::Logs) {
            let mut at: Vec<u32> = Vec::new();
            let mut which: Vec<usize> = Vec::new();
            for (i, (c, row)) in f.resources.iter().enumerate() {
                if wants(c.id) {
                    at.push(*row);
                    which.push(i);
                    carried.announced.push(c.id);
                }
            }
            cols.push(announce_column(&f.resources, &at, &which, f.stats.rows, &sc.entries));
            let mut m = crate::columns::Map::default();
            m.clear();
            let mut next = f.payloads.iter().filter(|p| wants_payload(&p.hash)).peekable();
            for r in 0..f.stats.rows as u32 {
                while let Some(p) = next.next_if(|p| p.first_row == r) {
                    let mut hx = [0u8; 32];
                    hex::encode_to_slice(p.hash, &mut hx).expect("32 hex digits");
                    m.keys.push(&hx);
                    m.vals.push(&p.content);
                    carried.payloads.push(p.hash);
                }
                m.commit();
            }
            cols.push(m.take(&sc.entries));
        }
        (record_batch(sc, cols, f.stats.rows, env), carried)
    }

    /// `encode`, announcing (traces and logs) the resources `wants` names:
    /// their covered sets go into `resource_announce` on the first row of
    /// each, and `oscope-announce` counts them. Returns the announced ids,
    /// to mark once the object has committed (`resource::AnnounceCache`).
    pub fn encode_announcing(&self, f: &Flat, env: &Envelope, wants: &dyn Fn(u64) -> bool) -> Result<(Encoded, Vec<u64>), EncodeError> {
        self.encode_carrying(f, env, wants, &|_| true).map(|(o, c)| (o, c.announced))
    }

    /// `encode_announcing`, also carrying (traces and logs) the payloads
    /// `wants_payload` names in `payloads`, on the first row that references
    /// each (walk order), counted in `oscope-payloads`; `oscope-payload-refs`
    /// counts the distinct references of the object's rows. Returns what
    /// the object carries, to mark once it has committed
    /// (`offload::PayloadCache`).
    pub fn encode_carrying(&self, f: &Flat, env: &Envelope, wants: &dyn Fn(u64) -> bool, wants_payload: &dyn Fn(&[u8; 16]) -> bool) -> Result<(Encoded, Carried), EncodeError> {
        let sc = self.schemas(f.signal);
        let has_resources = matches!(f.signal, Signal::Traces | Signal::Logs);
        let (rb, carried) = self.rows(f, env, wants, wants_payload);
        let mut meta = BTreeMap::new();
        if has_resources {
            let _ = meta.insert(proto::META_ANNOUNCE.to_string(), carried.announced.len().to_string());
            let _ = meta.insert(proto::META_PAYLOADS.to_string(), carried.payloads.len().to_string());
            let _ = meta.insert(proto::META_PAYLOAD_REFS.to_string(), f.payloads.len().to_string());
        }
        for (k, v) in [
            (proto::META_PRODUCER, env.producer.clone()),
            (proto::META_SIGNAL, f.signal.name().to_string()),
            (proto::META_SCHEMA, sc.version.to_string()),
            (proto::META_ROWS, f.stats.rows.to_string()),
            (proto::META_MIN_TIME, f.stats.min_ts.to_string()),
            (proto::META_MAX_TIME, f.stats.max_ts.to_string()),
            (proto::META_RECEIVED, env.received_ns.to_string()),
        ] {
            let _ = meta.insert(k.to_string(), v);
        }
        if let Some((part, after_ns)) = f.split {
            let _ = meta.insert(proto::META_PART.to_string(), part.name().to_string());
            let _ = meta.insert(proto::META_LATE_AFTER.to_string(), after_ns.to_string());
        }
        // The footer carries the whole description, the slot identity and
        // the content key included, so the object stands on its own.
        let mut footer = meta.clone();
        for (k, v) in [
            (proto::META_KIND, proto::KIND_DATA.to_string()),
            (proto::META_EPOCH, env.epoch.clone()),
            (proto::META_SEQ, env.batch.to_string()),
            (proto::META_CONTENT, f.content.clone()),
        ] {
            let _ = footer.insert(k.to_string(), v);
        }
        let mut out = Vec::with_capacity(512 << 10);
        let sorted = self.opts.sort.enabled() && matches!(f.signal, Signal::Traces | Signal::Logs);
        let content_type = match self.format {
            Format::Parquet if sorted => {
                // Rows by (ServiceName, Timestamp), cut into row groups by
                // service (`encode::sort_plan`); a pure function of the rows,
                // so a retry's object is byte-identical. row_ordinal moves
                // with its row: it still names the row's place in the request.
                let (plan, rb) = sort_batch(&rb, &self.opts.sort).map_err(|x| EncodeError(x.to_string()))?;
                let _ = footer.insert(encode::META_SORT.to_string(), plan.footer_value(&self.opts.sort));
                encode::parquet_groups(sc, &self.opts, &rb, &plan.groups, plan.max_services, &footer, &mut out)
                    .map_err(|x| EncodeError(x.to_string()))?;
                PARQUET_CONTENT_TYPE
            }
            Format::Parquet => {
                encode::parquet(sc, &self.opts, &rb, &footer, &mut out).map_err(|x| EncodeError(x.to_string()))?;
                PARQUET_CONTENT_TYPE
            }
            Format::Arrow => {
                encode::arrow_ipc(&rb, &mut out).map_err(|x| EncodeError(x.to_string()))?;
                ARROW_CONTENT_TYPE
            }
        };
        Ok((Encoded { body: Bytes::from(out), content_type, meta }, carried))
    }
}

/// `resource_announce`: per row, the covered set of the resource announced
/// on it (`at[j]` is the row of `resources[which[j]]`), else an empty map.
fn announce_column(resources: &[(Covered, u32)], at: &[u32], which: &[usize], rows: usize, entries: &arrow::datatypes::FieldRef) -> ArrayRef {
    let mut m = crate::columns::Map::default();
    m.clear();
    let mut by_row: Vec<(u32, usize)> = at.iter().copied().zip(which.iter().copied()).collect();
    by_row.sort_unstable();
    let mut next = by_row.iter().peekable();
    for r in 0..rows as u32 {
        while let Some(&&(row, i)) = next.peek() {
            if row != r {
                break;
            }
            for (k, v) in &resources[i].0.pairs {
                m.keys.push(k);
                m.vals.push(v);
            }
            let _ = next.next();
        }
        m.commit();
    }
    m.take(entries)
}
