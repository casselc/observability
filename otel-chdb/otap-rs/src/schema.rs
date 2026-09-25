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
}

impl Schemas {
    pub fn new(signal: crate::Signal) -> Self {
        let (b, u) = match signal {
            crate::Signal::Traces => (traces(&DataType::Binary), traces(&DataType::Utf8)),
            crate::Signal::Logs => (logs(&DataType::Binary), logs(&DataType::Utf8)),
        };
        let parquet = ArrowSchemaConverter::new()
            .convert(&u)
            .expect("published schema converts to Parquet");
        Self {
            arrow: Arc::new(b),
            parquet,
            entries: map_entries(&DataType::Binary),
            ts_elem: Arc::new(Field::new("element", ts_type(), false)),
            str_elem: Arc::new(Field::new("element", DataType::Binary, false)),
            map_elem: Arc::new(Field::new("element", map_type(&DataType::Binary), false)),
        }
    }
}

/// Leaf columns that are unique or nearly so per row: written without a
/// dictionary (parquetgo's `HighCardinality`).
pub const HIGH_CARDINALITY: &[&[&str]] = &[
    &["Timestamp"],
    &["TraceId"],
    &["SpanId"],
    &["ParentSpanId"],
    &["row_ordinal"],
    &["Events.Timestamp", "list", "element"],
];
