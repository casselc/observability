-- The proposed metric-name helper for layout B (DECISIONS.md §4 risk 4): one
-- row per (type, hour, metric, service) seen, kept by materialized views on
-- the points tables. {db} is B's database. Measured in ../README.md; HyperDX
-- 2.39.1 cannot use it (its picker reads the source's own metric table), so
-- this stays a measurement aid until HyperDX can be pointed at a names table
-- (or at its own planned `seriesTable`).
CREATE TABLE IF NOT EXISTS {db}.otel_metrics_names
(
    MetricType Enum8('gauge' = 0, 'sum' = 1, 'histogram' = 2, 'exponential_histogram' = 3, 'summary' = 4),
    Hour DateTime,
    MetricName LowCardinality(String),
    ServiceName LowCardinality(String)
)
ENGINE = ReplacingMergeTree
PARTITION BY toDate(Hour)
ORDER BY (MetricType, Hour, MetricName, ServiceName);

CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.otel_metrics_names_number_mv TO {db}.otel_metrics_names AS
SELECT DISTINCT toString(p.MetricType) AS MetricType, toStartOfHour(p.TimeUnix) AS Hour, p.MetricName AS MetricName, p.ServiceName AS ServiceName
FROM {db}.otel_metrics_number_points AS p;

CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.otel_metrics_names_histogram_mv TO {db}.otel_metrics_names AS
SELECT DISTINCT 'histogram' AS MetricType, toStartOfHour(TimeUnix) AS Hour, MetricName, ServiceName
FROM {db}.otel_metrics_histogram_points;

CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.otel_metrics_names_exp_histogram_mv TO {db}.otel_metrics_names AS
SELECT DISTINCT 'exponential_histogram' AS MetricType, toStartOfHour(TimeUnix) AS Hour, MetricName, ServiceName
FROM {db}.otel_metrics_exponential_histogram_points;

CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.otel_metrics_names_summary_mv TO {db}.otel_metrics_names AS
SELECT DISTINCT 'summary' AS MetricType, toStartOfHour(TimeUnix) AS Hour, MetricName, ServiceName
FROM {db}.otel_metrics_summary_points;

-- Backfill (the MVs only see new inserts): run once after creating them.
INSERT INTO {db}.otel_metrics_names
SELECT DISTINCT toString(MetricType), toStartOfHour(TimeUnix), MetricName, ServiceName FROM {db}.otel_metrics_number_points;
INSERT INTO {db}.otel_metrics_names
SELECT DISTINCT 'histogram', toStartOfHour(TimeUnix), MetricName, ServiceName FROM {db}.otel_metrics_histogram_points;
INSERT INTO {db}.otel_metrics_names
SELECT DISTINCT 'exponential_histogram', toStartOfHour(TimeUnix), MetricName, ServiceName FROM {db}.otel_metrics_exponential_histogram_points;
INSERT INTO {db}.otel_metrics_names
SELECT DISTINCT 'summary', toStartOfHour(TimeUnix), MetricName, ServiceName FROM {db}.otel_metrics_summary_points;
