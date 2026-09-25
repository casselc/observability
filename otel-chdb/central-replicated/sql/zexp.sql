CREATE DATABASE IF NOT EXISTS zexp;
CREATE TABLE IF NOT EXISTS zexp.otel_traces_zc (Timestamp DateTime64(9), TraceId String, SpanId String, ParentSpanId String, TraceState String,
  SpanName LowCardinality(String), SpanKind LowCardinality(String), ServiceName LowCardinality(String),
  ResourceAttributes Map(LowCardinality(String), String), ScopeName String, ScopeVersion String,
  SpanAttributes Map(LowCardinality(String), String), Duration UInt64, StatusCode LowCardinality(String), StatusMessage String,
  Events Nested (Timestamp DateTime64(9), Name LowCardinality(String), Attributes Map(LowCardinality(String), String)),
  Links Nested (TraceId String, SpanId String, TraceState String, Attributes Map(LowCardinality(String), String)), producer_id LowCardinality(String), producer_epoch LowCardinality(String), batch_id UInt64, row_ordinal UInt32, received_at DateTime64(9), schema_version UInt16, content_key LowCardinality(String),
  PROJECTION by_content (SELECT content_key, count() GROUP BY content_key))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/zexp/otel_traces_zc', '{replica}') PARTITION BY toDate(received_at) ORDER BY (ServiceName, SpanName, toDateTime(Timestamp))
TTL toDateTime(received_at) + INTERVAL 100 YEAR TO VOLUME 'cold', toDateTime(received_at) + INTERVAL 200 YEAR DELETE
SETTINGS storage_policy = 'tiered_zc', allow_remote_fs_zero_copy_replication = 1;
CREATE TABLE IF NOT EXISTS zexp.otel_traces_own (Timestamp DateTime64(9), TraceId String, SpanId String, ParentSpanId String, TraceState String,
  SpanName LowCardinality(String), SpanKind LowCardinality(String), ServiceName LowCardinality(String),
  ResourceAttributes Map(LowCardinality(String), String), ScopeName String, ScopeVersion String,
  SpanAttributes Map(LowCardinality(String), String), Duration UInt64, StatusCode LowCardinality(String), StatusMessage String,
  Events Nested (Timestamp DateTime64(9), Name LowCardinality(String), Attributes Map(LowCardinality(String), String)),
  Links Nested (TraceId String, SpanId String, TraceState String, Attributes Map(LowCardinality(String), String)), producer_id LowCardinality(String), producer_epoch LowCardinality(String), batch_id UInt64, row_ordinal UInt32, received_at DateTime64(9), schema_version UInt16, content_key LowCardinality(String),
  PROJECTION by_content (SELECT content_key, count() GROUP BY content_key))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/zexp/otel_traces_own', '{replica}') PARTITION BY toDate(received_at) ORDER BY (ServiceName, SpanName, toDateTime(Timestamp))
TTL toDateTime(received_at) + INTERVAL 100 YEAR TO VOLUME 'cold', toDateTime(received_at) + INTERVAL 200 YEAR DELETE
SETTINGS storage_policy = 'tiered_own', allow_remote_fs_zero_copy_replication = 0;
