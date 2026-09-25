//! One request → its content hash, its flattened columns, and the encoded
//! object for a given slot.

use crate::encode::{self, ParquetOptions};
use crate::flatten::{Envelope, LogsBuf, Stats, TracesBuf, record_batch};
use crate::metrics::MetricsBuf;
use crate::proto;
use crate::runner::Encoded;
use crate::schema::{SCHEMA_VERSION, Schemas};
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
}

/// Content hash of an OTLP request: BLAKE3 over "{signal}\0{protobuf}",
/// 128 bits in hex. The same idea as ../awss3/inline's SHA-256 key (and the
/// same length); BLAKE3 because SHA-256 without SHA-NI costs ~5 ms per
/// 10k-span request here. A client's retry resends the same bytes, so it
/// hashes the same in any process.
pub fn content_hash_otlp(signal: Signal, bytes: &[u8]) -> String {
    let mut h = blake3::Hasher::new();
    let _ = h.update(signal.name().as_bytes());
    let _ = h.update(&[0]);
    let _ = h.update(bytes);
    hex::encode(&h.finalize().as_bytes()[..16])
}

/// Content hash of flattened rows (for OTAP input, which has no canonical
/// bytes): BLAKE3 over every content column's buffers.
pub fn content_hash_cols(signal: Signal, cols: &[ArrayRef]) -> String {
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
    let _ = h.update(signal.name().as_bytes());
    for c in cols {
        feed(&mut h, &c.to_data());
    }
    hex::encode(&h.finalize().as_bytes()[..16])
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
    pub opts: ParquetOptions,
    pub format: Format,
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
            opts,
            format,
        }
    }

    pub fn schemas(&self, s: Signal) -> &Schemas {
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
                out.push(Flat { signal: sig, content, cols, stats });
            }
        }
        out
    }

    /// Walks the input into columns.
    pub fn flatten(&mut self, input: &Input<'_>) -> Result<Flat, EncodeError> {
        let e = |m: &dyn std::fmt::Display| EncodeError(m.to_string());
        let (signal, stats, cols) = match input {
            Input::Otlp(Signal::Traces, b) => {
                let v = RawTraceData::try_new(b).map_err(|x| e(&x))?;
                let st = self.traces.fill(&v);
                (Signal::Traces, st, self.traces.content_arrays(&self.traces_sc))
            }
            Input::Otlp(Signal::Logs, b) => {
                let v = RawLogsData::try_new(b).map_err(|x| e(&x))?;
                let st = self.logs.fill(&v);
                (Signal::Logs, st, self.logs.content_arrays(&self.logs_sc))
            }
            Input::Otap(Signal::Traces, r) => {
                let v = OtapTracesView::try_from(*r).map_err(|x| e(&x))?;
                let st = self.traces.fill(&v);
                (Signal::Traces, st, self.traces.content_arrays(&self.traces_sc))
            }
            Input::Otap(Signal::Logs, r) => {
                let v = OtapLogsView::try_from(*r).map_err(|x| e(&x))?;
                let st = self.logs.fill(&v);
                (Signal::Logs, st, self.logs.content_arrays(&self.logs_sc))
            }
            _ => return Err(EncodeError("metrics input has one object per type: use flatten_all".into())),
        };
        let content = match input {
            Input::Otlp(s, b) => content_hash_otlp(*s, b),
            Input::Otap(s, _) => content_hash_cols(*s, &cols),
            _ => unreachable!(),
        };
        Ok(Flat { signal, content, cols, stats })
    }

    /// The object for one slot: envelope added, encoded, described.
    pub fn encode(&self, f: &Flat, env: &Envelope) -> Result<Encoded, EncodeError> {
        let sc = self.schemas(f.signal);
        let rb = record_batch(sc, f.cols.clone(), f.stats.rows, env);
        let mut meta = BTreeMap::new();
        for (k, v) in [
            (proto::META_PRODUCER, env.producer.clone()),
            (proto::META_SIGNAL, f.signal.name().to_string()),
            (proto::META_SCHEMA, SCHEMA_VERSION.to_string()),
            (proto::META_ROWS, f.stats.rows.to_string()),
            (proto::META_MIN_TIME, f.stats.min_ts.to_string()),
            (proto::META_MAX_TIME, f.stats.max_ts.to_string()),
            (proto::META_RECEIVED, env.received_ns.to_string()),
        ] {
            let _ = meta.insert(k.to_string(), v);
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
        let content_type = match self.format {
            Format::Parquet => {
                encode::parquet(sc, &self.opts, &rb, &footer, &mut out).map_err(|x| EncodeError(x.to_string()))?;
                PARQUET_CONTENT_TYPE
            }
            Format::Arrow => {
                encode::arrow_ipc(&rb, &mut out).map_err(|x| EncodeError(x.to_string()))?;
                ARROW_CONTENT_TYPE
            }
        };
        Ok(Encoded { body: Bytes::from(out), content_type, meta })
    }
}
