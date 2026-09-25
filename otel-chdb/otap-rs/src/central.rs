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
    }
}

pub fn cols(signal: crate::Signal) -> String {
    match signal {
        crate::Signal::Traces => format!("{TRACES_COLS}{ENV_COLS}"),
        crate::Signal::Logs => format!("{LOGS_COLS}{ENV_COLS}"),
    }
}

pub fn create_table(table: &str, signal: crate::Signal) -> String {
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
    };
    let order = match signal {
        crate::Signal::Traces => "(ServiceName, SpanName, toDateTime(Timestamp))",
        crate::Signal::Logs => "(ServiceName, Timestamp)",
    };
    format!(
        "CREATE TABLE IF NOT EXISTS {table} {body}{CENTRAL_ENV},
  PROJECTION by_content (SELECT content_key, count() GROUP BY content_key))
ENGINE = MergeTree PARTITION BY toDate(received_at) ORDER BY {order}
SETTINGS non_replicated_deduplication_window = 1000"
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
