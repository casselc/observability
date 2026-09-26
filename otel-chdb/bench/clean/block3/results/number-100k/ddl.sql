CREATE TABLE mrg_number_100k.otel_metrics_number_points
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
ENGINE = MergeTree
PARTITION BY toDate(received_at)
ORDER BY (MetricName, ServiceName, series_id, TimeUnix)
SETTINGS non_replicated_deduplication_window = 1000, index_granularity = 8192, old_parts_lifetime = 5, cleanup_delay_period = 1, cleanup_delay_period_random_add = 1;
