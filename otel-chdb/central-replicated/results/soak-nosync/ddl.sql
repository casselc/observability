CREATE DATABASE IF NOT EXISTS rsoak_rsoak1790376918;
CREATE TABLE IF NOT EXISTS rsoak_rsoak1790376918.otel_traces (Timestamp DateTime64(9), TraceId String, SpanId String, ParentSpanId String, TraceState String,
  SpanName LowCardinality(String), SpanKind LowCardinality(String), ServiceName LowCardinality(String),
  ResourceAttributes Map(LowCardinality(String), String), ScopeName String, ScopeVersion String,
  SpanAttributes Map(LowCardinality(String), String), Duration UInt64, StatusCode LowCardinality(String), StatusMessage String,
  Events Nested (Timestamp DateTime64(9), Name LowCardinality(String), Attributes Map(LowCardinality(String), String)),
  Links Nested (TraceId String, SpanId String, TraceState String, Attributes Map(LowCardinality(String), String)), producer_id LowCardinality(String), producer_epoch LowCardinality(String), batch_id UInt64, row_ordinal UInt32, received_at DateTime64(9), schema_version UInt16, content_key LowCardinality(String),
  PROJECTION by_content (SELECT content_key, count() GROUP BY content_key))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/rsoak_rsoak1790376918/otel_traces', '{replica}') PARTITION BY toDate(received_at) ORDER BY (ServiceName, SpanName, toDateTime(Timestamp))
TTL toDateTime(received_at) + INTERVAL 3 MINUTE TO VOLUME 'cold', toDateTime(received_at) + INTERVAL 1 DAY DELETE
SETTINGS storage_policy = 'tiered_zc', allow_remote_fs_zero_copy_replication = 1, old_parts_lifetime = 30;
CREATE TABLE IF NOT EXISTS rsoak_rsoak1790376918.otel_logs (Timestamp DateTime64(9), TraceId String, SpanId String, TraceFlags UInt8, SeverityText LowCardinality(String),
  SeverityNumber UInt8, ServiceName LowCardinality(String), Body String, ResourceSchemaUrl LowCardinality(String),
  ResourceAttributes Map(LowCardinality(String), String), ScopeSchemaUrl LowCardinality(String), ScopeName String,
  ScopeVersion LowCardinality(String), ScopeAttributes Map(LowCardinality(String), String),
  LogAttributes Map(LowCardinality(String), String), EventName String, producer_id LowCardinality(String), producer_epoch LowCardinality(String), batch_id UInt64, row_ordinal UInt32, received_at DateTime64(9), schema_version UInt16, content_key LowCardinality(String),
  PROJECTION by_content (SELECT content_key, count() GROUP BY content_key))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/rsoak_rsoak1790376918/otel_logs', '{replica}') PARTITION BY toDate(received_at) ORDER BY (ServiceName, Timestamp)
TTL toDateTime(received_at) + INTERVAL 3 MINUTE TO VOLUME 'cold', toDateTime(received_at) + INTERVAL 1 DAY DELETE
SETTINGS storage_policy = 'tiered_zc', allow_remote_fs_zero_copy_replication = 1, old_parts_lifetime = 30;
CREATE TABLE IF NOT EXISTS rsoak_rsoak1790376918.otel_metrics_series
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
    LastSeen SimpleAggregateFunction(max, DateTime) CODEC(ZSTD(1)),
    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_attr_value mapValues(Attributes) TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/rsoak_rsoak1790376918/otel_metrics_series', '{replica}')
ORDER BY (MetricName, ServiceName, series_id)
TTL LastSeen + INTERVAL 3 MINUTE TO VOLUME 'cold', LastSeen + INTERVAL 1 DAY DELETE
SETTINGS storage_policy = 'tiered_zc', allow_remote_fs_zero_copy_replication = 1, old_parts_lifetime = 30, index_granularity = 1024, allow_dimensions_outside_sorting_key = 1;
CREATE TABLE IF NOT EXISTS rsoak_rsoak1790376918.otel_metrics_number_points
(
    MetricName LowCardinality(String) CODEC(ZSTD(1)),
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    series_id UInt64 CODEC(ZSTD(1)),
    MetricType Enum8('gauge' = 0, 'sum' = 1) CODEC(ZSTD(1)),
    StartTimeUnix DateTime CODEC(Delta(4), ZSTD(1)),
    TimeUnix DateTime CODEC(DoubleDelta, ZSTD(1)),
    Value Float64 CODEC(Delta, ZSTD(1)),
    Flags UInt32 CODEC(ZSTD(1)),
    `Exemplars.FilteredAttributes` Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
    `Exemplars.TimeUnix` Array(DateTime) CODEC(ZSTD(1)),
    `Exemplars.Value` Array(Float64) CODEC(ZSTD(1)),
    `Exemplars.SpanId` Array(String) CODEC(ZSTD(1)),
    `Exemplars.TraceId` Array(String) CODEC(ZSTD(1)),
    producer_id LowCardinality(String) CODEC(ZSTD(1)),
    producer_epoch LowCardinality(String) CODEC(ZSTD(1)),
    batch_id UInt64 CODEC(Delta, ZSTD(1)),
    row_ordinal UInt32 CODEC(ZSTD(1)),
    received_at DateTime64(9) CODEC(DoubleDelta, ZSTD(1)),
    schema_version UInt16 CODEC(ZSTD(1)),
    content_key LowCardinality(String),
    PROJECTION by_content (SELECT content_key, count() GROUP BY content_key)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/rsoak_rsoak1790376918/otel_metrics_number_points', '{replica}')
PARTITION BY toDate(received_at)
ORDER BY (MetricName, ServiceName, series_id, TimeUnix)
TTL toDateTime(received_at) + INTERVAL 3 MINUTE TO VOLUME 'cold', toDateTime(received_at) + INTERVAL 1 DAY DELETE
SETTINGS storage_policy = 'tiered_zc', allow_remote_fs_zero_copy_replication = 1, old_parts_lifetime = 30, index_granularity = 8192;
CREATE TABLE IF NOT EXISTS rsoak_rsoak1790376918.otel_metrics_histogram_points
(
    MetricName LowCardinality(String) CODEC(ZSTD(1)),
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    series_id UInt64 CODEC(ZSTD(1)),
    StartTimeUnix DateTime CODEC(Delta(4), ZSTD(1)),
    TimeUnix DateTime CODEC(DoubleDelta, ZSTD(1)),
    Count UInt64 CODEC(DoubleDelta, ZSTD(1)),
    Sum Float64 CODEC(Delta, ZSTD(1)),
    BucketCounts Array(UInt64) CODEC(ZSTD(1)),
    Min Float64 CODEC(ZSTD(1)),
    Max Float64 CODEC(ZSTD(1)),
    Flags UInt32 CODEC(ZSTD(1)),
    `Exemplars.FilteredAttributes` Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
    `Exemplars.TimeUnix` Array(DateTime) CODEC(ZSTD(1)),
    `Exemplars.Value` Array(Float64) CODEC(ZSTD(1)),
    `Exemplars.SpanId` Array(String) CODEC(ZSTD(1)),
    `Exemplars.TraceId` Array(String) CODEC(ZSTD(1)),
    producer_id LowCardinality(String) CODEC(ZSTD(1)),
    producer_epoch LowCardinality(String) CODEC(ZSTD(1)),
    batch_id UInt64 CODEC(Delta, ZSTD(1)),
    row_ordinal UInt32 CODEC(ZSTD(1)),
    received_at DateTime64(9) CODEC(DoubleDelta, ZSTD(1)),
    schema_version UInt16 CODEC(ZSTD(1)),
    content_key LowCardinality(String),
    PROJECTION by_content (SELECT content_key, count() GROUP BY content_key)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/rsoak_rsoak1790376918/otel_metrics_histogram_points', '{replica}')
PARTITION BY toDate(received_at)
ORDER BY (MetricName, ServiceName, series_id, TimeUnix)
TTL toDateTime(received_at) + INTERVAL 3 MINUTE TO VOLUME 'cold', toDateTime(received_at) + INTERVAL 1 DAY DELETE
SETTINGS storage_policy = 'tiered_zc', allow_remote_fs_zero_copy_replication = 1, old_parts_lifetime = 30, index_granularity = 8192;
CREATE TABLE IF NOT EXISTS rsoak_rsoak1790376918.otel_metrics_exponential_histogram_points
(
    MetricName LowCardinality(String) CODEC(ZSTD(1)),
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    series_id UInt64 CODEC(ZSTD(1)),
    StartTimeUnix DateTime CODEC(Delta(4), ZSTD(1)),
    TimeUnix DateTime CODEC(DoubleDelta, ZSTD(1)),
    Count UInt64 CODEC(ZSTD(1)),
    Sum Float64 CODEC(ZSTD(1)),
    Scale Int32 CODEC(ZSTD(1)),
    ZeroCount UInt64 CODEC(ZSTD(1)),
    PositiveOffset Int32 CODEC(ZSTD(1)),
    PositiveBucketCounts Array(UInt64) CODEC(ZSTD(1)),
    NegativeOffset Int32 CODEC(ZSTD(1)),
    NegativeBucketCounts Array(UInt64) CODEC(ZSTD(1)),
    Min Float64 CODEC(ZSTD(1)),
    Max Float64 CODEC(ZSTD(1)),
    Flags UInt32 CODEC(ZSTD(1)),
    `Exemplars.FilteredAttributes` Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
    `Exemplars.TimeUnix` Array(DateTime) CODEC(ZSTD(1)),
    `Exemplars.Value` Array(Float64) CODEC(ZSTD(1)),
    `Exemplars.SpanId` Array(String) CODEC(ZSTD(1)),
    `Exemplars.TraceId` Array(String) CODEC(ZSTD(1)),
    producer_id LowCardinality(String) CODEC(ZSTD(1)),
    producer_epoch LowCardinality(String) CODEC(ZSTD(1)),
    batch_id UInt64 CODEC(Delta, ZSTD(1)),
    row_ordinal UInt32 CODEC(ZSTD(1)),
    received_at DateTime64(9) CODEC(DoubleDelta, ZSTD(1)),
    schema_version UInt16 CODEC(ZSTD(1)),
    content_key LowCardinality(String),
    PROJECTION by_content (SELECT content_key, count() GROUP BY content_key)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/rsoak_rsoak1790376918/otel_metrics_exponential_histogram_points', '{replica}')
PARTITION BY toDate(received_at)
ORDER BY (MetricName, ServiceName, series_id, TimeUnix)
TTL toDateTime(received_at) + INTERVAL 3 MINUTE TO VOLUME 'cold', toDateTime(received_at) + INTERVAL 1 DAY DELETE
SETTINGS storage_policy = 'tiered_zc', allow_remote_fs_zero_copy_replication = 1, old_parts_lifetime = 30, index_granularity = 8192;
CREATE TABLE IF NOT EXISTS rsoak_rsoak1790376918.otel_metrics_summary_points
(
    MetricName LowCardinality(String) CODEC(ZSTD(1)),
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    series_id UInt64 CODEC(ZSTD(1)),
    StartTimeUnix DateTime CODEC(Delta(4), ZSTD(1)),
    TimeUnix DateTime CODEC(DoubleDelta, ZSTD(1)),
    Count UInt64 CODEC(DoubleDelta, ZSTD(1)),
    Sum Float64 CODEC(Delta, ZSTD(1)),
    `ValueAtQuantiles.Quantile` Array(Float64) CODEC(ZSTD(1)),
    `ValueAtQuantiles.Value` Array(Float64) CODEC(ZSTD(1)),
    Flags UInt32 CODEC(ZSTD(1)),
    producer_id LowCardinality(String) CODEC(ZSTD(1)),
    producer_epoch LowCardinality(String) CODEC(ZSTD(1)),
    batch_id UInt64 CODEC(Delta, ZSTD(1)),
    row_ordinal UInt32 CODEC(ZSTD(1)),
    received_at DateTime64(9) CODEC(DoubleDelta, ZSTD(1)),
    schema_version UInt16 CODEC(ZSTD(1)),
    content_key LowCardinality(String),
    PROJECTION by_content (SELECT content_key, count() GROUP BY content_key)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/rsoak_rsoak1790376918/otel_metrics_summary_points', '{replica}')
PARTITION BY toDate(received_at)
ORDER BY (MetricName, ServiceName, series_id, TimeUnix)
TTL toDateTime(received_at) + INTERVAL 3 MINUTE TO VOLUME 'cold', toDateTime(received_at) + INTERVAL 1 DAY DELETE
SETTINGS storage_policy = 'tiered_zc', allow_remote_fs_zero_copy_replication = 1, old_parts_lifetime = 30, index_granularity = 8192;
