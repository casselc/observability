//! The published schema: the ClickStack `otel_traces` / `otel_logs` row shape
//! with plain types (as the chdb exporter's structure and parquetgo's
//! `TracesSchema` / `LogsSchema`), plus the envelope columns. Every column is
//! required; DateTime64(9) is TIMESTAMP(NANOS, UTC); Map(String, String) is a
//! MAP with a `key_value` group; each Nested sub-column is its own LIST of
//! `element`; UInt8/16/32/64 are unsigned integers.
//!
//! The Arrow arrays carry strings as `Binary` (see columns.rs); the Parquet
//! schema is derived from the same schema with `Utf8`, so the file says STRING.

use crate::columns::map_entries;
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit};
use parquet::arrow::ArrowSchemaConverter;
use parquet::schema::types::SchemaDescriptor;
use std::sync::Arc;

pub const SCHEMA_VERSION: u16 = 1;

pub fn ts_type() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
}

/// A ClickHouse `DateTime` (the metrics tables' timestamps), as ClickHouse's
/// own Parquet writer types it: TIMESTAMP(MILLIS, UTC) holding whole seconds
/// (../parquetgo/METRICS_SCHEMA.md, tag `dt`).
pub fn dt_type() -> DataType {
    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
}

fn col(name: &str, t: DataType) -> Field {
    Field::new(name, t, false)
}

fn list(t: DataType) -> DataType {
    DataType::List(Arc::new(Field::new("element", t, false)))
}

pub fn map_type(s: &DataType) -> DataType {
    DataType::Map(map_entries(s), false)
}

fn envelope(s: &DataType) -> Vec<Field> {
    vec![
        col("producer_id", s.clone()),
        col("producer_epoch", s.clone()),
        col("batch_id", DataType::UInt64),
        col("row_ordinal", DataType::UInt32),
        col("received_at", ts_type()),
        col("schema_version", DataType::UInt16),
    ]
}

pub fn traces(s: &DataType) -> Schema {
    let mut f = vec![
        col("Timestamp", ts_type()),
        col("TraceId", s.clone()),
        col("SpanId", s.clone()),
        col("ParentSpanId", s.clone()),
        col("TraceState", s.clone()),
        col("SpanName", s.clone()),
        col("SpanKind", s.clone()),
        col("ServiceName", s.clone()),
        col("ResourceAttributes", map_type(s)),
        col("ScopeName", s.clone()),
        col("ScopeVersion", s.clone()),
        col("SpanAttributes", map_type(s)),
        col("Duration", DataType::UInt64),
        col("StatusCode", s.clone()),
        col("StatusMessage", s.clone()),
        col("Events.Timestamp", list(ts_type())),
        col("Events.Name", list(s.clone())),
        col("Events.Attributes", list(map_type(s))),
        col("Links.TraceId", list(s.clone())),
        col("Links.SpanId", list(s.clone())),
        col("Links.TraceState", list(s.clone())),
        col("Links.Attributes", list(map_type(s))),
    ];
    f.extend(envelope(s));
    Schema::new(f)
}

pub fn logs(s: &DataType) -> Schema {
    let mut f = vec![
        col("Timestamp", ts_type()),
        col("TraceId", s.clone()),
        col("SpanId", s.clone()),
        col("TraceFlags", DataType::UInt8),
        col("SeverityText", s.clone()),
        col("SeverityNumber", DataType::UInt8),
        col("ServiceName", s.clone()),
        col("Body", s.clone()),
        col("ResourceSchemaUrl", s.clone()),
        col("ResourceAttributes", map_type(s)),
        col("ScopeSchemaUrl", s.clone()),
        col("ScopeName", s.clone()),
        col("ScopeVersion", s.clone()),
        col("ScopeAttributes", map_type(s)),
        col("LogAttributes", map_type(s)),
        col("EventName", s.clone()),
    ];
    f.extend(envelope(s));
    Schema::new(f)
}

/// The contrib clickhouseexporter's metrics tables (v0.161.0,
/// `internal/sqltemplates/metrics_*_table.sql`), column for column and in
/// the same order, with plain types: `LowCardinality(String)` is a string,
/// `DateTime` is TIMESTAMP(MILLIS) holding the second clickhouse-go stores
/// (`metrics::dt_ms`), `Boolean` is BOOLEAN, and each Nested sub-column is
/// its own LIST. This is ../parquetgo/METRICS_SCHEMA.md.
pub fn metrics(signal: crate::Signal, s: &DataType) -> Schema {
    use crate::Signal as S;
    let f64l = || list(DataType::Float64);
    let mut f = vec![
        col("ResourceAttributes", map_type(s)),
        col("ResourceSchemaUrl", s.clone()),
        col("ScopeName", s.clone()),
        col("ScopeVersion", s.clone()),
        col("ScopeAttributes", map_type(s)),
        col("ScopeDroppedAttrCount", DataType::UInt32),
        col("ScopeSchemaUrl", s.clone()),
        col("ServiceName", s.clone()),
        col("MetricName", s.clone()),
        col("MetricDescription", s.clone()),
        col("MetricUnit", s.clone()),
        col("Attributes", map_type(s)),
        col("StartTimeUnix", dt_type()),
        col("TimeUnix", dt_type()),
    ];
    let exemplars = || {
        vec![
            col("Exemplars.FilteredAttributes", list(map_type(s))),
            col("Exemplars.TimeUnix", list(dt_type())),
            col("Exemplars.Value", f64l()),
            col("Exemplars.SpanId", list(s.clone())),
            col("Exemplars.TraceId", list(s.clone())),
        ]
    };
    match signal {
        S::MetricsGauge | S::MetricsSum => {
            f.push(col("Value", DataType::Float64));
            f.push(col("Flags", DataType::UInt32));
            f.extend(exemplars());
            if signal == S::MetricsSum {
                f.push(col("AggregationTemporality", DataType::Int32));
                f.push(col("IsMonotonic", DataType::Boolean));
            }
        }
        S::MetricsHistogram => {
            f.push(col("Count", DataType::UInt64));
            f.push(col("Sum", DataType::Float64));
            f.push(col("BucketCounts", list(DataType::UInt64)));
            f.push(col("ExplicitBounds", f64l()));
            f.extend(exemplars());
            f.push(col("Flags", DataType::UInt32));
            f.push(col("Min", DataType::Float64));
            f.push(col("Max", DataType::Float64));
            f.push(col("AggregationTemporality", DataType::Int32));
        }
        S::MetricsExpHistogram => {
            f.push(col("Count", DataType::UInt64));
            f.push(col("Sum", DataType::Float64));
            f.push(col("Scale", DataType::Int32));
            f.push(col("ZeroCount", DataType::UInt64));
            f.push(col("PositiveOffset", DataType::Int32));
            f.push(col("PositiveBucketCounts", list(DataType::UInt64)));
            f.push(col("NegativeOffset", DataType::Int32));
            f.push(col("NegativeBucketCounts", list(DataType::UInt64)));
            f.extend(exemplars());
            f.push(col("Flags", DataType::UInt32));
            f.push(col("Min", DataType::Float64));
            f.push(col("Max", DataType::Float64));
            f.push(col("AggregationTemporality", DataType::Int32));
        }
        S::MetricsSummary => {
            f.push(col("Count", DataType::UInt64));
            f.push(col("Sum", DataType::Float64));
            f.push(col("ValueAtQuantiles.Quantile", f64l()));
            f.push(col("ValueAtQuantiles.Value", f64l()));
            f.push(col("Flags", DataType::UInt32));
        }
        S::Traces | S::Logs => unreachable!("not a metrics signal"),
        s => unreachable!("{s:?} is layout B: series::schemas"),
    }
    f.extend(envelope(s));
    Schema::new(f)
}

/// Both forms of one signal's schema.
pub struct Schemas {
    /// What the arrays are built as (strings as Binary).
    pub arrow: SchemaRef,
    /// The published Parquet schema (strings as STRING).
    pub parquet: SchemaDescriptor,
    pub entries: FieldRef,
    pub ts_elem: FieldRef,
    pub str_elem: FieldRef,
    pub map_elem: FieldRef,
    pub dt_elem: FieldRef,
    pub f64_elem: FieldRef,
    pub u64_elem: FieldRef,
    pub u32_elem: FieldRef,
    /// Leaf columns written without a dictionary, besides `HIGH_CARDINALITY`.
    pub plain: Vec<Vec<String>>,
    /// Leaf columns written DELTA_BINARY_PACKED (no dictionary).
    pub delta: Vec<Vec<String>>,
}

impl Schemas {
    pub fn new(signal: crate::Signal) -> Self {
        if signal.is_series_layout() {
            return crate::series::schemas(signal, &crate::series::SeriesOptions::default());
        }
        let (b, u) = match signal {
            crate::Signal::Traces => (traces(&DataType::Binary), traces(&DataType::Utf8)),
            crate::Signal::Logs => (logs(&DataType::Binary), logs(&DataType::Utf8)),
            m => (metrics(m, &DataType::Binary), metrics(m, &DataType::Utf8)),
        };
        let parquet = ArrowSchemaConverter::new()
            .convert(&u)
            .expect("published schema converts to Parquet");
        Self::with(Arc::new(b), parquet, Vec::new())
    }

    /// From an Arrow schema (strings as Binary) and the published Parquet schema.
    pub fn with(arrow: SchemaRef, parquet: SchemaDescriptor, plain: Vec<Vec<String>>) -> Self {
        Self {
            arrow,
            parquet,
            plain,
            delta: Vec::new(),
            entries: map_entries(&DataType::Binary),
            ts_elem: Arc::new(Field::new("element", ts_type(), false)),
            str_elem: Arc::new(Field::new("element", DataType::Binary, false)),
            map_elem: Arc::new(Field::new("element", map_type(&DataType::Binary), false)),
            dt_elem: Arc::new(Field::new("element", dt_type(), false)),
            f64_elem: Arc::new(Field::new("element", DataType::Float64, false)),
            u64_elem: Arc::new(Field::new("element", DataType::UInt64, false)),
            u32_elem: Arc::new(Field::new("element", DataType::UInt32, false)),
        }
    }
}

/// Leaf columns that are unique or nearly so per row: written without a
/// dictionary (parquetgo's `HighCardinality`). Paths that a signal's schema
/// doesn't have are ignored by the writer.
pub const HIGH_CARDINALITY: &[&[&str]] = &[
    &["Value"],
    &["Sum"],
    &["Exemplars.TimeUnix", "list", "element"],
    &["Exemplars.Value", "list", "element"],
    &["Exemplars.SpanId", "list", "element"],
    &["Exemplars.TraceId", "list", "element"],
    &["Timestamp"],
    &["TraceId"],
    &["SpanId"],
    &["ParentSpanId"],
    &["row_ordinal"],
    &["Events.Timestamp", "list", "element"],
];
