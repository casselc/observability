-- Layout B: series-deduplicated. {db} is the database.
-- Codecs: picked with sql/codecs.sh on one hour of this data (README,
-- "Codecs"); only GA codecs (ALP was 8% smaller on gauges but is beta).
-- One series table (attribute maps once per series) and one narrow points
-- table per metric type. The points tables carry the importer's envelope
-- like the A tables do; row_ordinal is kept for the count check but see
-- README ("envelope") for dropping it from storage.

CREATE TABLE {db}.otel_metrics_series
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
SETTINGS index_granularity = 1024, allow_dimensions_outside_sorting_key = 1;

CREATE TABLE {db}.otel_metrics_gauge_points
(
    MetricName LowCardinality(String) CODEC(ZSTD(1)),
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    series_id UInt64 CODEC(ZSTD(1)),
    StartTimeUnix DateTime CODEC(Delta(4), ZSTD(1)),
    TimeUnix DateTime CODEC(DoubleDelta, ZSTD(1)),
    Value Float64 CODEC(Gorilla, ZSTD(1)),
    Flags UInt32 CODEC(ZSTD(1)),
    `Exemplars.TimeUnix` Array(DateTime) CODEC(ZSTD(1)),
    `Exemplars.Value` Array(Float64) CODEC(ZSTD(1)),
    `Exemplars.SpanId` Array(String) CODEC(ZSTD(1)),
    `Exemplars.TraceId` Array(String) CODEC(ZSTD(1)),
    producer_id LowCardinality(String) CODEC(ZSTD(1)),
    producer_epoch LowCardinality(String) CODEC(ZSTD(1)),
    batch_id UInt64 CODEC(Delta, ZSTD(1)),
    row_ordinal UInt32 CODEC(ZSTD(1)),
    received_at DateTime64(9) CODEC(DoubleDelta, ZSTD(1)),
    schema_version UInt16 CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toDate(received_at)
ORDER BY (MetricName, ServiceName, series_id, TimeUnix)
SETTINGS non_replicated_deduplication_window = 1000, index_granularity = 8192;

CREATE TABLE {db}.otel_metrics_sum_points
(
    MetricName LowCardinality(String) CODEC(ZSTD(1)),
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    series_id UInt64 CODEC(ZSTD(1)),
    StartTimeUnix DateTime CODEC(Delta(4), ZSTD(1)),
    TimeUnix DateTime CODEC(DoubleDelta, ZSTD(1)),
    Value Float64 CODEC(Delta, ZSTD(1)),
    Flags UInt32 CODEC(ZSTD(1)),
    `Exemplars.TimeUnix` Array(DateTime) CODEC(ZSTD(1)),
    `Exemplars.Value` Array(Float64) CODEC(ZSTD(1)),
    `Exemplars.SpanId` Array(String) CODEC(ZSTD(1)),
    `Exemplars.TraceId` Array(String) CODEC(ZSTD(1)),
    producer_id LowCardinality(String) CODEC(ZSTD(1)),
    producer_epoch LowCardinality(String) CODEC(ZSTD(1)),
    batch_id UInt64 CODEC(Delta, ZSTD(1)),
    row_ordinal UInt32 CODEC(ZSTD(1)),
    received_at DateTime64(9) CODEC(DoubleDelta, ZSTD(1)),
    schema_version UInt16 CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toDate(received_at)
ORDER BY (MetricName, ServiceName, series_id, TimeUnix)
SETTINGS non_replicated_deduplication_window = 1000, index_granularity = 8192;


CREATE TABLE {db}.otel_metrics_histogram_points
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
    `Exemplars.TimeUnix` Array(DateTime) CODEC(ZSTD(1)),
    `Exemplars.Value` Array(Float64) CODEC(ZSTD(1)),
    `Exemplars.SpanId` Array(String) CODEC(ZSTD(1)),
    `Exemplars.TraceId` Array(String) CODEC(ZSTD(1)),
    producer_id LowCardinality(String) CODEC(ZSTD(1)),
    producer_epoch LowCardinality(String) CODEC(ZSTD(1)),
    batch_id UInt64 CODEC(Delta, ZSTD(1)),
    row_ordinal UInt32 CODEC(ZSTD(1)),
    received_at DateTime64(9) CODEC(DoubleDelta, ZSTD(1)),
    schema_version UInt16 CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toDate(received_at)
ORDER BY (MetricName, ServiceName, series_id, TimeUnix)
SETTINGS non_replicated_deduplication_window = 1000, index_granularity = 8192;

CREATE TABLE {db}.otel_metrics_exponential_histogram_points
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
    `Exemplars.TimeUnix` Array(DateTime) CODEC(ZSTD(1)),
    `Exemplars.Value` Array(Float64) CODEC(ZSTD(1)),
    `Exemplars.SpanId` Array(String) CODEC(ZSTD(1)),
    `Exemplars.TraceId` Array(String) CODEC(ZSTD(1)),
    producer_id LowCardinality(String) CODEC(ZSTD(1)),
    producer_epoch LowCardinality(String) CODEC(ZSTD(1)),
    batch_id UInt64 CODEC(Delta, ZSTD(1)),
    row_ordinal UInt32 CODEC(ZSTD(1)),
    received_at DateTime64(9) CODEC(DoubleDelta, ZSTD(1)),
    schema_version UInt16 CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toDate(received_at)
ORDER BY (MetricName, ServiceName, series_id, TimeUnix)
SETTINGS non_replicated_deduplication_window = 1000, index_granularity = 8192;

CREATE TABLE {db}.otel_metrics_summary_points
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
    schema_version UInt16 CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toDate(received_at)
ORDER BY (MetricName, ServiceName, series_id, TimeUnix)
SETTINGS non_replicated_deduplication_window = 1000, index_granularity = 8192;
