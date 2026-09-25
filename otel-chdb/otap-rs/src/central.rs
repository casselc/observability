//! The central side: the consumer's ClickHouse statements.
//!
//! The target is the ClickStack-typed table plus one column, `content_key`
//! (the batch's content hash, constant per batch), with:
//! - an aggregating projection content_key → count(), so the check before
//!   insert reads a few hundred bytes, not the table (../model/FASTPATH.md §4);
//! - a batch-constant partition key, `toDate(received_at)`, so every insert
//!   is one part and atomic;
//! - a pinned nonzero `non_replicated_deduplication_window`, and inserts
//!   formed as one block (`ONE_BLOCK`), so the dedup token also protects an
//!   insert retried after a crash.

use arrow::datatypes::DataType;

pub const ENV_STRUCTURE: &str = ", producer_id String, producer_epoch String, batch_id UInt64, row_ordinal UInt32, received_at DateTime64(9), schema_version UInt16";

pub const TRACES_STRUCTURE: &str = "Timestamp DateTime64(9), TraceId String, SpanId String, ParentSpanId String, TraceState String, SpanName String, SpanKind String, ServiceName String, ResourceAttributes Map(String, String), ScopeName String, ScopeVersion String, SpanAttributes Map(String, String), Duration UInt64, StatusCode String, StatusMessage String, `Events.Timestamp` Array(DateTime64(9)), `Events.Name` Array(String), `Events.Attributes` Array(Map(String, String)), `Links.TraceId` Array(String), `Links.SpanId` Array(String), `Links.TraceState` Array(String), `Links.Attributes` Array(Map(String, String))";

pub const LOGS_STRUCTURE: &str = "Timestamp DateTime64(9), TraceId String, SpanId String, TraceFlags UInt8, SeverityText String, SeverityNumber UInt8, ServiceName String, Body String, ResourceSchemaUrl String, ResourceAttributes Map(String, String), ScopeSchemaUrl String, ScopeName String, ScopeVersion String, ScopeAttributes Map(String, String), LogAttributes Map(String, String), EventName String";

pub const TRACES_COLS: &str = "Timestamp, TraceId, SpanId, ParentSpanId, TraceState, SpanName, SpanKind, ServiceName, ResourceAttributes, ScopeName, ScopeVersion, SpanAttributes, Duration, StatusCode, StatusMessage, `Events.Timestamp`, `Events.Name`, `Events.Attributes`, `Links.TraceId`, `Links.SpanId`, `Links.TraceState`, `Links.Attributes`";

pub const LOGS_COLS: &str = "Timestamp, TraceId, SpanId, TraceFlags, SeverityText, SeverityNumber, ServiceName, Body, ResourceSchemaUrl, ResourceAttributes, ScopeSchemaUrl, ScopeName, ScopeVersion, ScopeAttributes, LogAttributes, EventName";

pub const ENV_COLS: &str = ", producer_id, producer_epoch, batch_id, row_ordinal, received_at, schema_version";

const CENTRAL_ENV: &str = ", producer_id LowCardinality(String), producer_epoch LowCardinality(String), batch_id UInt64, row_ordinal UInt32, received_at DateTime64(9), schema_version UInt16, content_key LowCardinality(String)";

/// Settings that make an `INSERT … SELECT FROM s3()` of one batch one block
/// (FASTPATH.md §4, fp.py's ONE_BLOCK), including the Parquet reader's own
/// chunking limits.
pub const ONE_BLOCK: &[(&str, &str)] = &[
    ("max_threads", "1"),
    ("max_insert_threads", "1"),
    ("max_block_size", "1048576"),
    ("max_insert_block_size", "1048576"),
    ("min_insert_block_size_rows", "0"),
    ("min_insert_block_size_bytes", "0"),
    ("input_format_parquet_max_block_size", "1048576"),
    ("input_format_parquet_prefer_block_bytes", "4294967296"),
];

pub fn structure(signal: crate::Signal) -> String {
    match signal {
        crate::Signal::Traces => format!("{TRACES_STRUCTURE}{ENV_STRUCTURE}"),
        crate::Signal::Logs => format!("{LOGS_STRUCTURE}{ENV_STRUCTURE}"),
        m => metrics_structure(m),
    }
}

pub fn cols(signal: crate::Signal) -> String {
    match signal {
        crate::Signal::Traces => format!("{TRACES_COLS}{ENV_COLS}"),
        crate::Signal::Logs => format!("{LOGS_COLS}{ENV_COLS}"),
        m => content_cols(m) + ENV_COLS,
    }
}

/// A metrics type's content columns (no envelope), quoted, in table order.
pub fn content_cols(signal: crate::Signal) -> String {
    let sc = crate::schema::metrics(signal, &DataType::Utf8);
    let n = sc.fields().len() - 6;
    sc.fields()[..n].iter().map(|f| format!("`{}`", f.name())).collect::<Vec<_>>().join(", ")
}

/// The object's ClickHouse type for an Arrow type (the s3() structure).
fn plain_type(t: &DataType) -> String {
    match t {
        DataType::Utf8 | DataType::Binary => "String".into(),
        DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, _) => "DateTime".into(),
        DataType::Timestamp(..) => "DateTime64(9)".into(),
        DataType::Map(..) => "Map(String, String)".into(),
        DataType::List(f) => format!("Array({})", plain_type(f.data_type())),
        DataType::Boolean => "Bool".into(),
        other => format!("{other:?}"), // UInt32, UInt64, Int32, Float64, ...
    }
}

/// The s3() structure of a metrics object, envelope included.
pub fn metrics_structure(signal: crate::Signal) -> String {
    let sc = crate::schema::metrics(signal, &DataType::Utf8);
    sc.fields().iter().map(|f| format!("`{}` {}", f.name(), plain_type(f.data_type()))).collect::<Vec<_>>().join(", ")
}

/// The contrib exporter's column type (v0.161.0 DDL, codecs and indexes left out).
fn contrib_type(name: &str, t: &DataType) -> String {
    match t {
        DataType::Utf8 if name == "ServiceName" || name == "MetricName" => "LowCardinality(String)".into(),
        DataType::Utf8 => "String".into(),
        DataType::Timestamp(..) => "DateTime".into(),
        DataType::Map(..) => "Map(LowCardinality(String), String)".into(),
        DataType::List(f) => format!("Array({})", contrib_type(name, f.data_type())),
        DataType::Boolean => "Bool".into(),
        other => format!("{other:?}"),
    }
}

/// The contrib exporter's table body for a metrics type: the same columns,
/// types and Nested groups as `metrics_*_table.sql`.
pub fn contrib_metrics_columns(signal: crate::Signal) -> String {
    let sc = crate::schema::metrics(signal, &DataType::Utf8);
    let n = sc.fields().len() - 6;
    let mut out: Vec<String> = Vec::new();
    let mut nested: Option<(String, Vec<String>)> = None;
    for f in &sc.fields()[..n] {
        if let Some((group, sub)) = f.name().split_once('.') {
            let DataType::List(e) = f.data_type() else { unreachable!() };
            let item = format!("{sub} {}", contrib_type(sub, e.data_type()));
            match &mut nested {
                Some((g, items)) if g == group => items.push(item),
                _ => {
                    if let Some((g, items)) = nested.take() {
                        out.push(format!("{g} Nested ({})", items.join(", ")));
                    }
                    nested = Some((group.to_string(), vec![item]));
                }
            }
            continue;
        }
        if let Some((g, items)) = nested.take() {
            out.push(format!("{g} Nested ({})", items.join(", ")));
        }
        out.push(format!("{} {}", f.name(), contrib_type(f.name(), f.data_type())));
    }
    if let Some((g, items)) = nested.take() {
        out.push(format!("{g} Nested ({})", items.join(", ")));
    }
    out.join(",\n  ")
}

pub fn create_table(table: &str, signal: crate::Signal) -> String {
    if signal.is_metrics() {
        return format!(
            "CREATE TABLE IF NOT EXISTS {table} ({}{CENTRAL_ENV},
  PROJECTION by_content (SELECT content_key, count() GROUP BY content_key))
ENGINE = MergeTree PARTITION BY toDate(received_at)
ORDER BY (ServiceName, MetricName, toStartOfHour(TimeUnix), cityHash64(Attributes), TimeUnix)
SETTINGS non_replicated_deduplication_window = 1000",
            contrib_metrics_columns(signal)
        );
    }
    let body = match signal {
        crate::Signal::Traces => "(Timestamp DateTime64(9), TraceId String, SpanId String, ParentSpanId String, TraceState String,
  SpanName LowCardinality(String), SpanKind LowCardinality(String), ServiceName LowCardinality(String),
  ResourceAttributes Map(LowCardinality(String), String), ScopeName String, ScopeVersion String,
  SpanAttributes Map(LowCardinality(String), String), Duration UInt64, StatusCode LowCardinality(String), StatusMessage String,
  Events Nested (Timestamp DateTime64(9), Name LowCardinality(String), Attributes Map(LowCardinality(String), String)),
  Links Nested (TraceId String, SpanId String, TraceState String, Attributes Map(LowCardinality(String), String))",
        crate::Signal::Logs => "(Timestamp DateTime64(9), TraceId String, SpanId String, TraceFlags UInt8, SeverityText LowCardinality(String),
  SeverityNumber UInt8, ServiceName LowCardinality(String), Body String, ResourceSchemaUrl LowCardinality(String),
  ResourceAttributes Map(LowCardinality(String), String), ScopeSchemaUrl LowCardinality(String), ScopeName String,
  ScopeVersion LowCardinality(String), ScopeAttributes Map(LowCardinality(String), String),
  LogAttributes Map(LowCardinality(String), String), EventName String",
        _ => unreachable!(),
    };
    let order = match signal {
        crate::Signal::Traces => "(ServiceName, SpanName, toDateTime(Timestamp))",
        crate::Signal::Logs => "(ServiceName, Timestamp)",
        _ => unreachable!(),
    };
    format!(
        "CREATE TABLE IF NOT EXISTS {table} {body}{CENTRAL_ENV},
  PROJECTION by_content (SELECT content_key, count() GROUP BY content_key))
ENGINE = MergeTree PARTITION BY toDate(received_at) ORDER BY {order}
SETTINGS non_replicated_deduplication_window = 1000"
    )
}

// ---- metrics layout B (../metrics-layout/sql/b_tables.sql) -------------------------
//
// The objects' structures and the select lists come from `series.rs`
// (`structure`, `insert_select`); these are the consumer's central tables.

/// A points table (`metrics_number_points`, `metrics_*_points`) for an
/// object `structure` from `series::structure`: the same columns with
/// central types (LowCardinality names, B's codecs), plus `content_key` and
/// the content projection, partitioned by the batch-constant
/// `toDate(received_at)`.
pub fn points_create_table(table: &str, structure: &str) -> String {
    let mut cols = Vec::new();
    for c in split_top(structure) {
        let c = c.trim();
        let (name, ty) = c.split_once(' ').unwrap_or((c, "String"));
        let bare = name.trim_matches('`');
        let ty = match (bare, ty) {
            ("MetricName" | "ServiceName" | "producer_id" | "producer_epoch", _) => "LowCardinality(String)".to_string(),
            (_, "Array(Map(String, String))") => "Array(Map(LowCardinality(String), String))".to_string(),
            (_, t) => t.to_string(),
        };
        let codec = match bare {
            "TimeUnix" | "received_at" => "DoubleDelta, ZSTD(1)",
            "StartTimeUnix" => "Delta(4), ZSTD(1)",
            "batch_id" | "Sum" => "Delta, ZSTD(1)",
            "Count" => "DoubleDelta, ZSTD(1)",
            _ => "ZSTD(1)",
        };
        cols.push(format!("    {name} {ty} CODEC({codec})"));
    }
    format!(
        "CREATE TABLE IF NOT EXISTS {table}
(
{},
    content_key LowCardinality(String),
    PROJECTION by_content (SELECT content_key, count() GROUP BY content_key)
)
ENGINE = MergeTree
PARTITION BY toDate(received_at)
ORDER BY (MetricName, ServiceName, series_id, TimeUnix)
SETTINGS non_replicated_deduplication_window = 1000, index_granularity = 8192",
        cols.join(",\n")
    )
}

/// Splits a structure at its top-level commas.
fn split_top(s: &str) -> Vec<&str> {
    let (mut out, mut depth, mut start, mut tick) = (Vec::new(), 0i32, 0, false);
    for (i, c) in s.char_indices() {
        match c {
            '`' => tick = !tick,
            '(' if !tick => depth += 1,
            ')' if !tick => depth -= 1,
            ',' if !tick && depth == 0 => {
                out.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    out.push(&s[start..]);
    out
}

/// The series table: an AggregatingMergeTree where every non-key column is
/// a function of `series_id`, so inserting an object twice changes nothing
/// after a merge. That is why the series lane needs no count check.
pub fn series_create_table(table: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {table}
(
    series_id UInt64,
    MetricType Enum8('gauge' = 0, 'sum' = 1, 'histogram' = 2, 'exponential_histogram' = 3, 'summary' = 4),
    MetricName LowCardinality(String) CODEC(ZSTD(1)),
    MetricDescription String CODEC(ZSTD(1)),
    MetricUnit LowCardinality(String) CODEC(ZSTD(1)),
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ResourceSchemaUrl LowCardinality(String) CODEC(ZSTD(1)),
    ScopeName LowCardinality(String) CODEC(ZSTD(1)),
    ScopeVersion LowCardinality(String) CODEC(ZSTD(1)),
    ScopeAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeDroppedAttrCount UInt32 CODEC(ZSTD(1)),
    ScopeSchemaUrl LowCardinality(String) CODEC(ZSTD(1)),
    Attributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    AggregationTemporality Int32 CODEC(ZSTD(1)),
    IsMonotonic Bool CODEC(ZSTD(1)),
    ExplicitBounds Array(Float64) CODEC(ZSTD(1)),
    FirstSeen SimpleAggregateFunction(min, DateTime) CODEC(ZSTD(1)),
    LastSeen SimpleAggregateFunction(max, DateTime) CODEC(ZSTD(1))
)
ENGINE = AggregatingMergeTree
ORDER BY (MetricName, ServiceName, series_id)
SETTINGS index_granularity = 1024, allow_dimensions_outside_sorting_key = 1, non_replicated_deduplication_window = 1000"
    )
}

/// Single-quoted ClickHouse string literal.
pub fn sq(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
}

/// A minimal ClickHouse HTTP client.
pub struct ClickHouse {
    pub url: String,
    pub http: reqwest::Client,
    pub user: Option<(String, String)>,
}

impl ClickHouse {
    pub fn new(url: &str) -> Self {
        Self { url: url.trim_end_matches('/').to_string(), http: reqwest::Client::new(), user: None }
    }

    pub async fn query(&self, sql: &str, settings: &[(&str, &str)]) -> Result<String, String> {
        let mut r = self.http.post(format!("{}/", self.url)).query(settings).body(sql.to_string());
        if let Some((u, p)) = &self.user {
            r = r.basic_auth(u, Some(p));
        }
        let resp = r.send().await.map_err(|e| format!("clickhouse: {e}"))?;
        let status = resp.status();
        let text = resp.text().await.map_err(|e| format!("clickhouse: {e}"))?;
        if !status.is_success() {
            return Err(format!("clickhouse {status}: {}", text.trim()));
        }
        Ok(text.trim().to_string())
    }
}
