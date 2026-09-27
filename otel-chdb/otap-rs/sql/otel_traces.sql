-- The consumer's central otel_traces: ClickStack 2.39.1's own table
-- (hyperdx@885d30c docker/otel-collector/schema/seed/00005_otel_traces.sql,
-- the ClickHouse >= 26.2 variant with text indexes) and its key-value rollup
-- (00007_otel_traces_rollups.sql), column for column, codec for codec, index
-- for index, with the consumer's additions (+) and deviations (~):
--
--  + the edge envelope (producer_id … schema_version), content_key and the
--    by_content projection: the consumer's exactly-once count check
--    (FASTPATH.md §4; src/consumer/sql.rs `counts`);
--  + non_replicated_deduplication_window = 1000 on the table AND on the
--    rollup table: the dedup token of a retried insert (central.rs
--    `ONE_BLOCK`). On the rollup it is what makes the view's block of an
--    exact retry deduplicated too (26.10 runs the view even when the source
--    block is a duplicate, and dedups its block by a token derived from the
--    statement's, deduplicate_blocks_in_dependent_materialized_views = 1,
--    only if the target has a window); without it an exact retry counts twice
--    in the rollup [M];
--  ~ PARTITION BY toDate(received_at), not toDate(Timestamp): batch-constant,
--    so one object is one part (atomic), and the count check can prune on
--    `_partition_value` (src/consumer/plan.rs). ClickStack's key would split
--    an object across days and turn the range check off;
--  ~ no TTL: ClickStack's is `toDate(Timestamp) + ${TRACES_TTL}` (720h by
--    default). Retention is the operator's: `TTL toDateTime(received_at) +
--    INTERVAL n DAY` drops whole partitions under ttl_only_drop_parts, as
--    ../../central-replicated/scripts/ddl.py does.
--
-- {table} is the fully qualified table (db.otel_traces); the rollup is named
-- after it as HyperDX's source auto-detection expects (`<table>_kv_rollup_15m`).
-- Statements are separated by a line ending in ';'. central.rs splits them.
CREATE TABLE IF NOT EXISTS {table}
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
PARTITION BY toDate(received_at)
ORDER BY (ServiceName, SpanName, toDateTime(Timestamp))
SETTINGS non_replicated_deduplication_window = 1000, index_granularity = 8192, ttl_only_drop_parts = 1;

CREATE TABLE IF NOT EXISTS {table}_kv_rollup_15m
(
    `Timestamp` DateTime,
    `ColumnIdentifier` LowCardinality(String),
    `Key` LowCardinality(String),
    `Value` String,
    `count` UInt64,
    INDEX idx_count_minmax count TYPE minmax GRANULARITY 1,
    INDEX idx_timestamp_minmax Timestamp TYPE minmax GRANULARITY 1
)
ENGINE = SummingMergeTree
PARTITION BY toDate(Timestamp)
ORDER BY (ColumnIdentifier, Key, Timestamp, Value)
SETTINGS non_replicated_deduplication_window = 1000, index_granularity = 8192, ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS {table}_kv_rollup_15m_mv TO {table}_kv_rollup_15m
AS WITH elements AS (
    SELECT 'NativeColumn' AS ColumnIdentifier, toStartOfFifteenMinutes(Timestamp) AS Timestamp, 'ServiceName' AS Key, CAST(ServiceName AS String) AS Value FROM {table}
    UNION ALL
    SELECT 'NativeColumn' AS ColumnIdentifier, toStartOfFifteenMinutes(Timestamp) AS Timestamp, 'SpanName' AS Key, CAST(SpanName AS String) AS Value FROM {table}
    UNION ALL
    SELECT 'NativeColumn' AS ColumnIdentifier, toStartOfFifteenMinutes(Timestamp) AS Timestamp, 'SpanKind' AS Key, CAST(SpanKind AS String) AS Value FROM {table}
    UNION ALL
    SELECT 'NativeColumn' AS ColumnIdentifier, toStartOfFifteenMinutes(Timestamp) AS Timestamp, 'StatusCode' AS Key, CAST(StatusCode AS String) AS Value FROM {table}
    UNION ALL
    SELECT 'NativeColumn' AS ColumnIdentifier, toStartOfFifteenMinutes(Timestamp) AS Timestamp, 'ScopeName' AS Key, CAST(ScopeName AS String) AS Value FROM {table}
    UNION ALL
    SELECT 'NativeColumn' AS ColumnIdentifier, toStartOfFifteenMinutes(Timestamp) AS Timestamp, 'ScopeVersion' AS Key, CAST(ScopeVersion AS String) AS Value FROM {table}
)
SELECT Timestamp, ColumnIdentifier, Key, Value, count() AS count FROM elements
GROUP BY Timestamp, ColumnIdentifier, Key, Value;
