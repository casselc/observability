-- The consumer's central otel_traces (otap-rs/sql/otel_traces.sql: ClickStack
-- 2.39.1's table with text indexes and materialized columns, plus the edge
-- envelope, content_key and the by_content projection, PARTITION BY
-- toDate(received_at), the dedup window). Only the table: its key-value
-- rollup and materialized view are not part of the merge benchmark.
-- Before the schema alignment (otel-chdb(schema) commits) this file held the
-- contrib exporter's bloom-filter DDL (chdbexporter/schema.go); the results in
-- ../README.md and ../../clean/block3 were measured on that.
-- {partition}, {ttl} and {settings} are filled in by merges.py.
CREATE TABLE {db}.otel_traces
(
    `Timestamp` DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    `TraceId` String CODEC(ZSTD(1)),
    `SpanId` String CODEC(ZSTD(1)),
    `ParentSpanId` String CODEC(ZSTD(1)),
    `TraceState` String CODEC(ZSTD(1)),
    `SpanName` LowCardinality(String) CODEC(ZSTD(1)),
    `SpanKind` LowCardinality(String) CODEC(ZSTD(1)),
    `ServiceName` LowCardinality(String) CODEC(ZSTD(1)),
    `ResourceAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `ScopeName` String CODEC(ZSTD(1)),
    `ScopeVersion` String CODEC(ZSTD(1)),
    `SpanAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `Duration` UInt64 CODEC(ZSTD(1)),
    `StatusCode` LowCardinality(String) CODEC(ZSTD(1)),
    `StatusMessage` String CODEC(ZSTD(1)),
    `Events.Timestamp` Array(DateTime64(9)) CODEC(ZSTD(1)),
    `Events.Name` Array(LowCardinality(String)) CODEC(ZSTD(1)),
    `Events.Attributes` Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
    `Links.TraceId` Array(String) CODEC(ZSTD(1)),
    `Links.SpanId` Array(String) CODEC(ZSTD(1)),
    `Links.TraceState` Array(String) CODEC(ZSTD(1)),
    `Links.Attributes` Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
    `__hdx_materialized_rum.sessionId` String MATERIALIZED ResourceAttributes['rum.sessionId'] CODEC(ZSTD(1)),
    `SampleRate` UInt64 MATERIALIZED greatest(toUInt64OrZero(SpanAttributes['SampleRate']), 1) CODEC(T64, ZSTD(1)),
    `ResourceAttributeItems` Array(String) ALIAS arrayMap((arr) -> concat(arr.1, '=', arr.2), ResourceAttributes::Array(Tuple(String, String))),
    `SpanAttributeItems` Array(String) ALIAS arrayMap((arr) -> concat(arr.1, '=', arr.2), SpanAttributes::Array(Tuple(String, String))),
    `producer_id` LowCardinality(String) CODEC(ZSTD(1)),
    `producer_epoch` LowCardinality(String) CODEC(ZSTD(1)),
    `batch_id` UInt64 CODEC(ZSTD(1)),
    `row_ordinal` UInt32 CODEC(ZSTD(1)),
    `received_at` DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    `schema_version` UInt16 CODEC(ZSTD(1)),
    `content_key` LowCardinality(String) CODEC(ZSTD(1)),
    INDEX idx_trace_id TraceId TYPE text(tokenizer = 'array'),
    INDEX idx_rum_session_id __hdx_materialized_rum.sessionId TYPE text(tokenizer = 'array'),
    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE text(tokenizer = 'array'),
    INDEX idx_res_attr_items ResourceAttributeItems TYPE text(tokenizer = 'array'),
    INDEX idx_span_attr_key mapKeys(SpanAttributes) TYPE text(tokenizer = 'array'),
    INDEX idx_span_attr_items SpanAttributeItems TYPE text(tokenizer = 'array'),
    INDEX idx_duration Duration TYPE minmax GRANULARITY 1,
    INDEX idx_lower_span_name SpanName TYPE text(tokenizer = 'splitByNonAlpha', preprocessor=lower(SpanName)),
    PROJECTION by_content (SELECT content_key, count() GROUP BY content_key)
)
ENGINE = MergeTree
PARTITION BY {partition}
ORDER BY (ServiceName, SpanName, toDateTime(Timestamp))
{ttl}
SETTINGS non_replicated_deduplication_window = 1000, index_granularity = 8192, ttl_only_drop_parts = 1{settings};
