-- The consumer's otel_traces / otel_logs BEFORE the ClickStack 2.39.1 alignment
-- (otap-rs/src/central.rs create_table at 85c5932): no codecs, no skip indexes,
-- no materialized columns, no rollup; logs sorted (ServiceName, Timestamp).
-- The 'before' side of the schema comparison in ../README.md. {db} is the database.
CREATE TABLE IF NOT EXISTS {db}.otel_traces (Timestamp DateTime64(9), TraceId String, SpanId String, ParentSpanId String, TraceState String,
  SpanName LowCardinality(String), SpanKind LowCardinality(String), ServiceName LowCardinality(String),
  ResourceAttributes Map(LowCardinality(String), String), ScopeName String, ScopeVersion String,
  SpanAttributes Map(LowCardinality(String), String), Duration UInt64, StatusCode LowCardinality(String), StatusMessage String,
  Events Nested (Timestamp DateTime64(9), Name LowCardinality(String), Attributes Map(LowCardinality(String), String)),
  Links Nested (TraceId String, SpanId String, TraceState String, Attributes Map(LowCardinality(String), String)), producer_id LowCardinality(String), producer_epoch LowCardinality(String), batch_id UInt64, row_ordinal UInt32, received_at DateTime64(9), schema_version UInt16, content_key LowCardinality(String),
  PROJECTION by_content (SELECT content_key, count() GROUP BY content_key))
ENGINE = MergeTree PARTITION BY toDate(received_at) ORDER BY (ServiceName, SpanName, toDateTime(Timestamp))
SETTINGS non_replicated_deduplication_window = 1000;
CREATE TABLE IF NOT EXISTS {db}.otel_logs (Timestamp DateTime64(9), TraceId String, SpanId String, TraceFlags UInt8, SeverityText LowCardinality(String),
  SeverityNumber UInt8, ServiceName LowCardinality(String), Body String, ResourceSchemaUrl LowCardinality(String),
  ResourceAttributes Map(LowCardinality(String), String), ScopeSchemaUrl LowCardinality(String), ScopeName String,
  ScopeVersion LowCardinality(String), ScopeAttributes Map(LowCardinality(String), String),
  LogAttributes Map(LowCardinality(String), String), EventName String, producer_id LowCardinality(String), producer_epoch LowCardinality(String), batch_id UInt64, row_ordinal UInt32, received_at DateTime64(9), schema_version UInt16, content_key LowCardinality(String),
  PROJECTION by_content (SELECT content_key, count() GROUP BY content_key))
ENGINE = MergeTree PARTITION BY toDate(received_at) ORDER BY (ServiceName, Timestamp)
SETTINGS non_replicated_deduplication_window = 1000;
