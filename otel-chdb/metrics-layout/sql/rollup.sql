-- 5-minute rollups (min/max/sum/count/last per series per 5 min), built for
-- A and B from the loaded 6 h so their cost and size can be compared. In
-- production each would be a materialized view on the points/contrib table
-- (the SELECTs below are the MV bodies); here they run once as INSERT…SELECT
-- over the whole table, which is the same work minus Parquet parsing.
-- Run with: costprobe -db <db> -f sql/rollup.sql

-- ---- B: keyed by series_id; attributes stay in the series table ----
CREATE TABLE {db}.rollup5m_number_b
(
    MetricName LowCardinality(String) CODEC(ZSTD(1)),
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    series_id UInt64 CODEC(ZSTD(1)),
    t DateTime CODEC(DoubleDelta, ZSTD(1)),
    vmin SimpleAggregateFunction(min, Float64) CODEC(Gorilla, ZSTD(1)),
    vmax SimpleAggregateFunction(max, Float64) CODEC(Gorilla, ZSTD(1)),
    vsum SimpleAggregateFunction(sum, Float64) CODEC(Gorilla, ZSTD(1)),
    n SimpleAggregateFunction(sum, UInt64) CODEC(ZSTD(1)),
    vlast AggregateFunction(argMax, Float64, DateTime) CODEC(ZSTD(1))
)
ENGINE = AggregatingMergeTree ORDER BY (MetricName, ServiceName, series_id, t);

INSERT INTO {db}.rollup5m_number_b
SELECT MetricName, ServiceName, series_id, toStartOfFiveMinutes(TimeUnix) AS t,
       min(Value), max(Value), sum(Value), count(), argMaxState(Value, TimeUnix)
FROM {db}.otel_metrics_gauge_points GROUP BY MetricName, ServiceName, series_id, t;

INSERT INTO {db}.rollup5m_number_b
SELECT MetricName, ServiceName, series_id, toStartOfFiveMinutes(TimeUnix) AS t,
       min(Value), max(Value), sum(Value), count(), argMaxState(Value, TimeUnix)
FROM {db}.otel_metrics_sum_points GROUP BY MetricName, ServiceName, series_id, t;

-- Cumulative histograms: the last point of each 5 min is the rollup (any
-- window's increase is last - last of the previous window).
CREATE TABLE {db}.rollup5m_histogram_b
(
    MetricName LowCardinality(String) CODEC(ZSTD(1)),
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    series_id UInt64 CODEC(ZSTD(1)),
    t DateTime CODEC(DoubleDelta, ZSTD(1)),
    last AggregateFunction(argMax, Tuple(UInt64, Float64, Array(UInt64)), DateTime) CODEC(ZSTD(1))
)
ENGINE = AggregatingMergeTree ORDER BY (MetricName, ServiceName, series_id, t);

INSERT INTO {db}.rollup5m_histogram_b
SELECT MetricName, ServiceName, series_id, toStartOfFiveMinutes(TimeUnix) AS t,
       argMaxState((Count, Sum, BucketCounts), TimeUnix)
FROM {db}.otel_metrics_histogram_points GROUP BY MetricName, ServiceName, series_id, t;

-- ---- A: the contrib tables have no series key, so the rollup has to carry
-- the maps (once per series per 5 min) to stay queryable by attribute ----
CREATE TABLE {db}.rollup5m_number_a
(
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    MetricName LowCardinality(String) CODEC(ZSTD(1)),
    h UInt64 CODEC(ZSTD(1)),
    t DateTime CODEC(DoubleDelta, ZSTD(1)),
    ResourceAttributes SimpleAggregateFunction(any, Map(String, String)) CODEC(ZSTD(1)),
    Attributes SimpleAggregateFunction(any, Map(String, String)) CODEC(ZSTD(1)),
    vmin SimpleAggregateFunction(min, Float64) CODEC(Gorilla, ZSTD(1)),
    vmax SimpleAggregateFunction(max, Float64) CODEC(Gorilla, ZSTD(1)),
    vsum SimpleAggregateFunction(sum, Float64) CODEC(Gorilla, ZSTD(1)),
    n SimpleAggregateFunction(sum, UInt64) CODEC(ZSTD(1)),
    vlast AggregateFunction(argMax, Float64, DateTime) CODEC(ZSTD(1))
)
ENGINE = AggregatingMergeTree ORDER BY (ServiceName, MetricName, h, t)
SETTINGS allow_dimensions_outside_sorting_key = 1;

INSERT INTO {db}.rollup5m_number_a
SELECT ServiceName, MetricName, cityHash64(ResourceAttributes, Attributes) AS h, toStartOfFiveMinutes(TimeUnix) AS t,
       any(ResourceAttributes), any(Attributes), min(Value), max(Value), sum(Value), count(), argMaxState(Value, TimeUnix)
FROM {db}.otel_metrics_gauge GROUP BY ServiceName, MetricName, h, t;

INSERT INTO {db}.rollup5m_number_a
SELECT ServiceName, MetricName, cityHash64(ResourceAttributes, Attributes) AS h, toStartOfFiveMinutes(TimeUnix) AS t,
       any(ResourceAttributes), any(Attributes), min(Value), max(Value), sum(Value), count(), argMaxState(Value, TimeUnix)
FROM {db}.otel_metrics_sum GROUP BY ServiceName, MetricName, h, t;

CREATE TABLE {db}.rollup5m_histogram_a
(
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    MetricName LowCardinality(String) CODEC(ZSTD(1)),
    h UInt64 CODEC(ZSTD(1)),
    t DateTime CODEC(DoubleDelta, ZSTD(1)),
    ResourceAttributes SimpleAggregateFunction(any, Map(String, String)) CODEC(ZSTD(1)),
    Attributes SimpleAggregateFunction(any, Map(String, String)) CODEC(ZSTD(1)),
    ExplicitBounds SimpleAggregateFunction(any, Array(Float64)) CODEC(ZSTD(1)),
    last AggregateFunction(argMax, Tuple(UInt64, Float64, Array(UInt64)), DateTime) CODEC(ZSTD(1))
)
ENGINE = AggregatingMergeTree ORDER BY (ServiceName, MetricName, h, t)
SETTINGS allow_dimensions_outside_sorting_key = 1;

INSERT INTO {db}.rollup5m_histogram_a
SELECT ServiceName, MetricName, cityHash64(ResourceAttributes, Attributes) AS h, toStartOfFiveMinutes(TimeUnix) AS t,
       any(ResourceAttributes), any(Attributes), any(ExplicitBounds), argMaxState((Count, Sum, BucketCounts), TimeUnix)
FROM {db}.otel_metrics_histogram GROUP BY ServiceName, MetricName, h, t;

OPTIMIZE TABLE {db}.rollup5m_number_b FINAL;
OPTIMIZE TABLE {db}.rollup5m_histogram_b FINAL;
OPTIMIZE TABLE {db}.rollup5m_number_a FINAL;
OPTIMIZE TABLE {db}.rollup5m_histogram_a FINAL;
