CREATE TABLE mrg_series_100k.otel_metrics_series
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
ENGINE = AggregatingMergeTree
ORDER BY (MetricName, ServiceName, series_id)
-- Every non-key column is a function of series_id (it is in the hash).
SETTINGS index_granularity = 1024, allow_dimensions_outside_sorting_key = 1, old_parts_lifetime = 5, cleanup_delay_period = 1, cleanup_delay_period_random_add = 1;
