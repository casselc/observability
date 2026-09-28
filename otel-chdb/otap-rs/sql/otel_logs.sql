-- The consumer's central otel_logs: ClickStack 2.39.1's own table
-- (hyperdx@885d30c docker/otel-collector/schema/seed/00002_otel_logs.sql,
-- the ClickHouse >= 26.2 variant with text indexes) and its key-value rollup
-- (00006_otel_logs_rollups.sql), with the consumer's additions and the same
-- deliberate deviations as otel_traces.sql (envelope, resource_id, content_key and the
-- by_content projection, the dedup window on the table and the rollup table;
-- PARTITION BY toDate(received_at) instead of toDate(Timestamp); no TTL; no
-- idx_res_attr_key / idx_scope_attr_key / idx_log_attr_key, for their insert
-- cost: HyperDX finds map keys through the *_attr_items indexes instead).
-- The sort key is ClickStack's.
-- enable_block_number_column / enable_block_offset_column are ClickStack's:
-- HyperDX identifies a row by partition key + primary key + _block_number +
-- _block_offset when both are on (DBRowTable.tsx).
CREATE TABLE IF NOT EXISTS {table}
(
    `Timestamp` DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    `TraceId` String CODEC(ZSTD(1)),
    `SpanId` String CODEC(ZSTD(1)),
    `TraceFlags` UInt8,
    `SeverityText` LowCardinality(String) CODEC(ZSTD(1)),
    `SeverityNumber` UInt8,
    `ServiceName` LowCardinality(String) CODEC(ZSTD(1)),
    `Body` String CODEC(ZSTD(1)),
    `ResourceSchemaUrl` LowCardinality(String) CODEC(ZSTD(1)),
    `ResourceAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `ScopeSchemaUrl` LowCardinality(String) CODEC(ZSTD(1)),
    `ScopeName` String CODEC(ZSTD(1)),
    `ScopeVersion` LowCardinality(String) CODEC(ZSTD(1)),
    `ScopeAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `LogAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `EventName` String CODEC(ZSTD(1)),
    `__hdx_materialized_k8s.cluster.name` LowCardinality(String) MATERIALIZED ResourceAttributes['k8s.cluster.name'] CODEC(ZSTD(1)),
    `__hdx_materialized_k8s.container.name` LowCardinality(String) MATERIALIZED ResourceAttributes['k8s.container.name'] CODEC(ZSTD(1)),
    `__hdx_materialized_k8s.deployment.name` LowCardinality(String) MATERIALIZED ResourceAttributes['k8s.deployment.name'] CODEC(ZSTD(1)),
    `__hdx_materialized_k8s.namespace.name` LowCardinality(String) MATERIALIZED ResourceAttributes['k8s.namespace.name'] CODEC(ZSTD(1)),
    `__hdx_materialized_k8s.node.name` LowCardinality(String) MATERIALIZED ResourceAttributes['k8s.node.name'] CODEC(ZSTD(1)),
    `__hdx_materialized_k8s.pod.name` LowCardinality(String) MATERIALIZED ResourceAttributes['k8s.pod.name'] CODEC(ZSTD(1)),
    `__hdx_materialized_k8s.pod.uid` LowCardinality(String) MATERIALIZED ResourceAttributes['k8s.pod.uid'] CODEC(ZSTD(1)),
    `__hdx_materialized_deployment.environment.name` LowCardinality(String) MATERIALIZED ResourceAttributes['deployment.environment.name'] CODEC(ZSTD(1)),
    `ResourceAttributeItems` Array(String) ALIAS arrayMap((arr) -> concat(arr.1, '=', arr.2), ResourceAttributes::Array(Tuple(String, String))),
    `ScopeAttributeItems` Array(String) ALIAS arrayMap((arr) -> concat(arr.1, '=', arr.2), ScopeAttributes::Array(Tuple(String, String))),
    `LogAttributeItems` Array(String) ALIAS arrayMap((arr) -> concat(arr.1, '=', arr.2), LogAttributes::Array(Tuple(String, String))),
    `resource_id` UInt64 CODEC(ZSTD(1)),
    `producer_id` LowCardinality(String) CODEC(ZSTD(1)),
    `producer_epoch` LowCardinality(String) CODEC(ZSTD(1)),
    `batch_id` UInt64 CODEC(ZSTD(1)),
    `row_ordinal` UInt32 CODEC(ZSTD(1)),
    `received_at` DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    `schema_version` UInt16 CODEC(ZSTD(1)),
    `content_key` LowCardinality(String) CODEC(ZSTD(1)),
    INDEX idx_trace_id TraceId TYPE text(tokenizer = 'array'),
    INDEX idx_res_attr_items ResourceAttributeItems TYPE text(tokenizer = 'array'),
    INDEX idx_scope_attr_items ScopeAttributeItems TYPE text(tokenizer = 'array'),
    INDEX idx_log_attr_items LogAttributeItems TYPE text(tokenizer = 'array'),
    INDEX idx_lower_body lower(Body) TYPE text(tokenizer = 'splitByNonAlpha'),
    PROJECTION by_content (SELECT content_key, count() GROUP BY content_key)
)
ENGINE = MergeTree
PARTITION BY toDate(received_at)
ORDER BY (toStartOfFiveMinutes(Timestamp), ServiceName, Timestamp)
SETTINGS non_replicated_deduplication_window = 1000, index_granularity = 8192, ttl_only_drop_parts = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

-- The key-value rollup has a `cluster` column (the row's k8s.cluster.name;
-- owner decision 2026-09-28, DECISIONS.md D33): ClickStack's has none, so it
-- could be served only to callers with every cluster. `cluster` ends the
-- sort key (SummingMergeTree sums per cluster; the primary key is
-- ClickStack's), which is what an existing table can be altered to:
-- migrate_kv_rollup_cluster.sql migrates tables created before it.
CREATE TABLE IF NOT EXISTS {table}_kv_rollup_15m
(
    `Timestamp` DateTime,
    `ColumnIdentifier` LowCardinality(String),
    `Key` LowCardinality(String),
    `Value` String,
    `cluster` LowCardinality(String),
    `count` UInt64,
    INDEX idx_count_minmax count TYPE minmax GRANULARITY 1,
    INDEX idx_timestamp_minmax Timestamp TYPE minmax GRANULARITY 1
)
ENGINE = SummingMergeTree
PARTITION BY toDate(Timestamp)
ORDER BY (ColumnIdentifier, Key, Timestamp, Value, cluster)
PRIMARY KEY (ColumnIdentifier, Key, Timestamp, Value)
SETTINGS non_replicated_deduplication_window = 1000, index_granularity = 8192, ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS {table}_attr_kv_rollup_15m_mv TO {table}_kv_rollup_15m
AS WITH elements AS (
    SELECT 'NativeColumn' AS ColumnIdentifier, toStartOfFifteenMinutes(Timestamp) AS Timestamp, 'SeverityText' AS Key, CAST(SeverityText AS String) AS Value, CAST(`__hdx_materialized_k8s.cluster.name` AS String) AS cluster FROM {table}
    UNION ALL
    SELECT 'NativeColumn' AS ColumnIdentifier, toStartOfFifteenMinutes(Timestamp) AS Timestamp, 'ServiceName' AS Key, CAST(ServiceName AS String) AS Value, CAST(`__hdx_materialized_k8s.cluster.name` AS String) AS cluster FROM {table}
    UNION ALL
    SELECT 'NativeColumn' AS ColumnIdentifier, toStartOfFifteenMinutes(Timestamp) AS Timestamp, 'ScopeName' AS Key, CAST(ScopeName AS String) AS Value, CAST(`__hdx_materialized_k8s.cluster.name` AS String) AS cluster FROM {table}
    UNION ALL
    SELECT 'NativeColumn' AS ColumnIdentifier, toStartOfFifteenMinutes(Timestamp) AS Timestamp, 'ScopeVersion' AS Key, CAST(ScopeVersion AS String) AS Value, CAST(`__hdx_materialized_k8s.cluster.name` AS String) AS cluster FROM {table}
    UNION ALL
    SELECT 'NativeColumn' AS ColumnIdentifier, toStartOfFifteenMinutes(Timestamp) AS Timestamp, 'ResourceSchemaUrl' AS Key, CAST(ResourceSchemaUrl AS String) AS Value, CAST(`__hdx_materialized_k8s.cluster.name` AS String) AS cluster FROM {table}
    UNION ALL
    SELECT 'NativeColumn' AS ColumnIdentifier, toStartOfFifteenMinutes(Timestamp) AS Timestamp, 'ScopeSchemaUrl' AS Key, CAST(ScopeSchemaUrl AS String) AS Value, CAST(`__hdx_materialized_k8s.cluster.name` AS String) AS cluster FROM {table}
    UNION ALL
    SELECT 'NativeColumn' AS ColumnIdentifier, toStartOfFifteenMinutes(Timestamp) AS Timestamp, '__hdx_materialized_k8s.cluster.name' AS Key, CAST(`__hdx_materialized_k8s.cluster.name` AS String) AS Value, CAST(`__hdx_materialized_k8s.cluster.name` AS String) AS cluster FROM {table}
    UNION ALL
    SELECT 'NativeColumn' AS ColumnIdentifier, toStartOfFifteenMinutes(Timestamp) AS Timestamp, '__hdx_materialized_k8s.container.name' AS Key, CAST(`__hdx_materialized_k8s.container.name` AS String) AS Value, CAST(`__hdx_materialized_k8s.cluster.name` AS String) AS cluster FROM {table}
    UNION ALL
    SELECT 'NativeColumn' AS ColumnIdentifier, toStartOfFifteenMinutes(Timestamp) AS Timestamp, '__hdx_materialized_k8s.deployment.name' AS Key, CAST(`__hdx_materialized_k8s.deployment.name` AS String) AS Value, CAST(`__hdx_materialized_k8s.cluster.name` AS String) AS cluster FROM {table}
    UNION ALL
    SELECT 'NativeColumn' AS ColumnIdentifier, toStartOfFifteenMinutes(Timestamp) AS Timestamp, '__hdx_materialized_k8s.namespace.name' AS Key, CAST(`__hdx_materialized_k8s.namespace.name` AS String) AS Value, CAST(`__hdx_materialized_k8s.cluster.name` AS String) AS cluster FROM {table}
    UNION ALL
    SELECT 'NativeColumn' AS ColumnIdentifier, toStartOfFifteenMinutes(Timestamp) AS Timestamp, '__hdx_materialized_k8s.node.name' AS Key, CAST(`__hdx_materialized_k8s.node.name` AS String) AS Value, CAST(`__hdx_materialized_k8s.cluster.name` AS String) AS cluster FROM {table}
    UNION ALL
    SELECT 'NativeColumn' AS ColumnIdentifier, toStartOfFifteenMinutes(Timestamp) AS Timestamp, '__hdx_materialized_k8s.pod.name' AS Key, CAST(`__hdx_materialized_k8s.pod.name` AS String) AS Value, CAST(`__hdx_materialized_k8s.cluster.name` AS String) AS cluster FROM {table}
    UNION ALL
    SELECT 'NativeColumn' AS ColumnIdentifier, toStartOfFifteenMinutes(Timestamp) AS Timestamp, '__hdx_materialized_k8s.pod.uid' AS Key, CAST(`__hdx_materialized_k8s.pod.uid` AS String) AS Value, CAST(`__hdx_materialized_k8s.cluster.name` AS String) AS cluster FROM {table}
    UNION ALL
    SELECT 'NativeColumn' AS ColumnIdentifier, toStartOfFifteenMinutes(Timestamp) AS Timestamp, '__hdx_materialized_deployment.environment.name' AS Key, CAST(`__hdx_materialized_deployment.environment.name` AS String) AS Value, CAST(`__hdx_materialized_k8s.cluster.name` AS String) AS cluster FROM {table}
)
SELECT Timestamp, ColumnIdentifier, Key, Value, cluster, count() AS count FROM elements
GROUP BY Timestamp, ColumnIdentifier, Key, Value, cluster;
