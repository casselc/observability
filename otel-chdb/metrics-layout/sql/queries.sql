-- Dashboard queries, per layout. cmd/queries runs every block for the 1 h
-- and the full (6 h) window. Placeholders: {db} the A/B database, {vdb} the
-- compatibility views over B, {tsdb} the TimeSeries database, {from} {to}
-- (DateTime literals), {fromMs} {toMs} (ms), {step} (seconds).
-- A block starts with "-- @ <query> <layout>".
--
-- Q1 rate of a counter for one service, by pod
-- Q2 p99 of a histogram for one service
-- Q3 top-10 pods by a gauge (whole fleet)
-- Q4 filter on a resource attribute (k8s.namespace.name): request rate by service
-- H1/H2 are HyperDX's own SQL (renderChartConfig snapshots: "single sum
-- metric" with a group-by, "histogram quantile with grouping") with this
-- dataset's names, run against the contrib tables (A) and the views (BV).

-- @ Q1 A
SELECT pod, t, sum(r) AS rate FROM (
  SELECT any(ResourceAttributes['k8s.pod.name']) AS pod, toStartOfInterval(TimeUnix, INTERVAL {step} SECOND) AS t,
         (argMax(Value, TimeUnix) - argMin(Value, TimeUnix)) / greatest(dateDiff('second', min(TimeUnix), max(TimeUnix)), 1) AS r
  FROM {db}.otel_metrics_sum
  WHERE ServiceName = 'checkout' AND MetricName = 'http.server.request.count' AND TimeUnix >= {from} AND TimeUnix < {to}
  GROUP BY cityHash64(ResourceAttributes, Attributes), t)
GROUP BY pod, t ORDER BY pod, t

-- @ Q1 B
SELECT s.pod, t, sum(r) AS rate FROM (
  SELECT series_id, toStartOfInterval(TimeUnix, INTERVAL {step} SECOND) AS t,
         (argMax(Value, TimeUnix) - argMin(Value, TimeUnix)) / greatest(dateDiff('second', min(TimeUnix), max(TimeUnix)), 1) AS r
  FROM {db}.otel_metrics_sum_points
  WHERE MetricName = 'http.server.request.count' AND ServiceName = 'checkout' AND TimeUnix >= {from} AND TimeUnix < {to}
  GROUP BY series_id, t) AS p
INNER JOIN (SELECT DISTINCT series_id, ResourceAttributes['k8s.pod.name'] AS pod FROM {db}.otel_metrics_series
            WHERE MetricName = 'http.server.request.count' AND ServiceName = 'checkout') AS s USING series_id
GROUP BY s.pod, t ORDER BY s.pod, t

-- @ Q1 BV
SELECT pod, t, sum(r) AS rate FROM (
  SELECT any(ResourceAttributes['k8s.pod.name']) AS pod, toStartOfInterval(TimeUnix, INTERVAL {step} SECOND) AS t,
         (argMax(Value, TimeUnix) - argMin(Value, TimeUnix)) / greatest(dateDiff('second', min(TimeUnix), max(TimeUnix)), 1) AS r
  FROM {vdb}.otel_metrics_sum
  WHERE ServiceName = 'checkout' AND MetricName = 'http.server.request.count' AND TimeUnix >= {from} AND TimeUnix < {to}
  GROUP BY cityHash64(ResourceAttributes, Attributes), t)
GROUP BY pod, t ORDER BY pod, t

-- @ Q1 C
SELECT * FROM prometheusQueryRange({tsdb}.ts, 'sum by (k8s_pod_name) (rate(http_server_request_count_total{service_name="checkout"}[{step}s]))', toDateTime64({from}, 3) + {step}, toDateTime64({to}, 3), {step})

-- @ Q2 A
SELECT t, bounds[indexOf(arrayMap(c -> c >= 0.99 * arraySum(b), arrayCumSum(b)), 1)] AS p99 FROM (
  SELECT t, any(eb) AS bounds, sumForEach(d) AS b FROM (
    SELECT toStartOfInterval(TimeUnix, INTERVAL {step} SECOND) AS t, any(ExplicitBounds) AS eb,
           arrayMap((x, y) -> toInt64(x) - toInt64(y), argMax(BucketCounts, TimeUnix), argMin(BucketCounts, TimeUnix)) AS d
    FROM {db}.otel_metrics_histogram
    WHERE ServiceName = 'checkout' AND MetricName = 'http.server.request.duration' AND TimeUnix >= {from} AND TimeUnix < {to}
    GROUP BY cityHash64(ResourceAttributes, Attributes), t)
  GROUP BY t) ORDER BY t

-- @ Q2 B
SELECT t, bounds[indexOf(arrayMap(c -> c >= 0.99 * arraySum(b), arrayCumSum(b)), 1)] AS p99 FROM (
  SELECT t, any(s.ExplicitBounds) AS bounds, sumForEach(d) AS b FROM (
    SELECT series_id, toStartOfInterval(TimeUnix, INTERVAL {step} SECOND) AS t,
           arrayMap((x, y) -> toInt64(x) - toInt64(y), argMax(BucketCounts, TimeUnix), argMin(BucketCounts, TimeUnix)) AS d
    FROM {db}.otel_metrics_histogram_points
    WHERE MetricName = 'http.server.request.duration' AND ServiceName = 'checkout' AND TimeUnix >= {from} AND TimeUnix < {to}
    GROUP BY series_id, t) AS p
  ANY INNER JOIN (SELECT series_id, ExplicitBounds FROM {db}.otel_metrics_series
                  WHERE MetricName = 'http.server.request.duration' AND ServiceName = 'checkout') AS s USING series_id
  GROUP BY t) ORDER BY t

-- @ Q2 BV
SELECT t, bounds[indexOf(arrayMap(c -> c >= 0.99 * arraySum(b), arrayCumSum(b)), 1)] AS p99 FROM (
  SELECT t, any(eb) AS bounds, sumForEach(d) AS b FROM (
    SELECT toStartOfInterval(TimeUnix, INTERVAL {step} SECOND) AS t, any(ExplicitBounds) AS eb,
           arrayMap((x, y) -> toInt64(x) - toInt64(y), argMax(BucketCounts, TimeUnix), argMin(BucketCounts, TimeUnix)) AS d
    FROM {vdb}.otel_metrics_histogram
    WHERE ServiceName = 'checkout' AND MetricName = 'http.server.request.duration' AND TimeUnix >= {from} AND TimeUnix < {to}
    GROUP BY cityHash64(ResourceAttributes, Attributes), t)
  GROUP BY t) ORDER BY t

-- @ Q2 C
SELECT * FROM prometheusQueryRange({tsdb}.ts, 'histogram_quantile(0.99, sum by (le) (rate(http_server_request_duration_bucket{service_name="checkout"}[{step}s])))', toDateTime64({from}, 3) + {step}, toDateTime64({to}, 3), {step})

-- @ Q3 A
SELECT ResourceAttributes['k8s.pod.name'] AS pod, avg(Value) AS v
FROM {db}.otel_metrics_gauge
WHERE MetricName = 'process.memory.usage' AND TimeUnix >= {from} AND TimeUnix < {to}
GROUP BY pod ORDER BY v DESC LIMIT 10

-- @ Q3 B
SELECT s.pod, avg(v) AS v FROM (
  SELECT series_id, avg(Value) AS v FROM {db}.otel_metrics_gauge_points
  WHERE MetricName = 'process.memory.usage' AND TimeUnix >= {from} AND TimeUnix < {to}
  GROUP BY series_id) AS p
INNER JOIN (SELECT DISTINCT series_id, ResourceAttributes['k8s.pod.name'] AS pod FROM {db}.otel_metrics_series
            WHERE MetricName = 'process.memory.usage') AS s USING series_id
GROUP BY s.pod ORDER BY v DESC LIMIT 10

-- @ Q3 BV
SELECT ResourceAttributes['k8s.pod.name'] AS pod, avg(Value) AS v
FROM {vdb}.otel_metrics_gauge
WHERE MetricName = 'process.memory.usage' AND TimeUnix >= {from} AND TimeUnix < {to}
GROUP BY pod ORDER BY v DESC LIMIT 10

-- @ Q3 C
SELECT * FROM prometheusQuery({tsdb}.ts, 'topk(10, avg by (k8s_pod_name) (avg_over_time(process_memory_usage[{window}s])))', toDateTime64({to}, 3))

-- @ Q4 A
SELECT ServiceName, t, sum(r) AS rate FROM (
  SELECT any(ServiceName) AS ServiceName, toStartOfInterval(TimeUnix, INTERVAL {step} SECOND) AS t,
         (argMax(Value, TimeUnix) - argMin(Value, TimeUnix)) / greatest(dateDiff('second', min(TimeUnix), max(TimeUnix)), 1) AS r
  FROM {db}.otel_metrics_sum
  WHERE MetricName = 'http.server.request.count' AND ResourceAttributes['k8s.namespace.name'] = 'payments'
    AND TimeUnix >= {from} AND TimeUnix < {to}
  GROUP BY cityHash64(ResourceAttributes, Attributes), t)
GROUP BY ServiceName, t ORDER BY ServiceName, t

-- @ Q4 B
SELECT ServiceName, t, sum(r) AS rate FROM (
  SELECT ServiceName, series_id, toStartOfInterval(TimeUnix, INTERVAL {step} SECOND) AS t,
         (argMax(Value, TimeUnix) - argMin(Value, TimeUnix)) / greatest(dateDiff('second', min(TimeUnix), max(TimeUnix)), 1) AS r
  FROM {db}.otel_metrics_sum_points
  WHERE MetricName = 'http.server.request.count' AND TimeUnix >= {from} AND TimeUnix < {to}
    AND (ServiceName, series_id) IN (SELECT ServiceName, series_id FROM {db}.otel_metrics_series
                      WHERE MetricName = 'http.server.request.count' AND ResourceAttributes['k8s.namespace.name'] = 'payments')
  GROUP BY ServiceName, series_id, t)
GROUP BY ServiceName, t ORDER BY ServiceName, t

-- @ Q4 BV
SELECT ServiceName, t, sum(r) AS rate FROM (
  SELECT any(ServiceName) AS ServiceName, toStartOfInterval(TimeUnix, INTERVAL {step} SECOND) AS t,
         (argMax(Value, TimeUnix) - argMin(Value, TimeUnix)) / greatest(dateDiff('second', min(TimeUnix), max(TimeUnix)), 1) AS r
  FROM {vdb}.otel_metrics_sum
  WHERE MetricName = 'http.server.request.count' AND ResourceAttributes['k8s.namespace.name'] = 'payments'
    AND TimeUnix >= {from} AND TimeUnix < {to}
  GROUP BY cityHash64(ResourceAttributes, Attributes), t)
GROUP BY ServiceName, t ORDER BY ServiceName, t

-- @ Q4 C
SELECT * FROM prometheusQueryRange({tsdb}.ts, 'sum by (service_name) (rate(http_server_request_count_total{k8s_namespace_name="payments"}[{step}s]))', toDateTime64({from}, 3) + {step}, toDateTime64({to}, 3), {step})

-- @ H1 A
WITH Source AS (
  SELECT *, cityHash64(ScopeAttributes, ResourceAttributes, Attributes) AS AttributesHash,
    IF(AggregationTemporality = 1, Value,
       greatest(Value - lagInFrame(toNullable(Value), 1, NULL) OVER (PARTITION BY AttributesHash ORDER BY TimeUnix), 0)) AS Rate,
    IF(AggregationTemporality = 1,
       SUM(Value) OVER (PARTITION BY AttributesHash ORDER BY TimeUnix ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), Value) AS Sum
  FROM {db}.otel_metrics_sum
  WHERE (TimeUnix >= toStartOfInterval({from}, INTERVAL {step} second) - INTERVAL {step} second AND TimeUnix <= toStartOfInterval({to}, INTERVAL {step} second) + INTERVAL {step} second)
    AND ((MetricName = 'http.server.request.count')) AND (ServiceName = 'checkout')),
Bucketed AS (
  SELECT `__hdx_time_bucket2`, AttributesHash, Rate, Sum, ResourceAttributes, ResourceSchemaUrl, ScopeName, ScopeVersion, ScopeAttributes,
         ScopeDroppedAttrCount, ScopeSchemaUrl, ServiceName, MetricName, MetricDescription, MetricUnit, Attributes, StartTimeUnix, Flags,
         AggregationTemporality, IsMonotonic
  FROM (
    SELECT toStartOfInterval(toDateTime(TimeUnix), INTERVAL {step} second) AS `__hdx_time_bucket2`, AttributesHash,
      sum(Source.Rate) AS Rate, argMax(Source.Sum, TimeUnix) AS Sum,
      any(ResourceAttributes) AS ResourceAttributes, any(ResourceSchemaUrl) AS ResourceSchemaUrl, any(ScopeName) AS ScopeName,
      any(ScopeVersion) AS ScopeVersion, any(ScopeAttributes) AS ScopeAttributes, any(ScopeDroppedAttrCount) AS ScopeDroppedAttrCount,
      any(ScopeSchemaUrl) AS ScopeSchemaUrl, any(ServiceName) AS ServiceName, any(MetricName) AS MetricName,
      any(MetricDescription) AS MetricDescription, any(MetricUnit) AS MetricUnit, any(Attributes) AS Attributes,
      any(StartTimeUnix) AS StartTimeUnix, any(Flags) AS Flags, any(AggregationTemporality) AS AggregationTemporality,
      any(IsMonotonic) AS IsMonotonic
    FROM Source GROUP BY AttributesHash, `__hdx_time_bucket2` ORDER BY AttributesHash, `__hdx_time_bucket2`))
SELECT sum(toFloat64OrDefault(toString(Rate))) AS "Value", ResourceAttributes['k8s.pod.name'] AS pod,
       toStartOfInterval(toDateTime(`__hdx_time_bucket2`), INTERVAL {step} second) AS `__hdx_time_bucket`
FROM Bucketed WHERE (`__hdx_time_bucket2` >= {from} AND `__hdx_time_bucket2` <= {to})
GROUP BY pod, `__hdx_time_bucket` ORDER BY `__hdx_time_bucket`
SETTINGS optimize_read_in_order = 0, cast_keep_nullable = 1

-- @ H2 A
WITH source AS (
  SELECT MetricName, ExplicitBounds, toStartOfInterval(toDateTime(TimeUnix), INTERVAL {step} second) AS `__hdx_time_bucket`, group,
         sumForEach(deltas) as rates
  FROM (
    SELECT TimeUnix, MetricName, ExplicitBounds, group, attr_hash,
      any(attr_hash) OVER prev_row AS prev_attr_hash,
      count() OVER prev_row = 0 OR prev_attr_hash != attr_hash AS is_first_series_point,
      any(bounds_hash) OVER prev_row AS prev_bounds_hash,
      any(counts) OVER prev_row AS prev_counts,
      counts,
      multiIf(AggregationTemporality = 2 AND is_first_series_point, arrayWithConstant(length(counts), toInt64(0)),
              AggregationTemporality = 1 OR bounds_hash != prev_bounds_hash OR arrayExists((x) -> x.2 < x.1, arrayZip(prev_counts, counts)), counts,
              counts - prev_counts) AS deltas
    FROM (
      SELECT TimeUnix, MetricName, AggregationTemporality, ExplicitBounds, ResourceAttributes, Attributes, [ServiceName] as group,
             cityHash64(ScopeAttributes, ResourceAttributes, Attributes) AS attr_hash, cityHash64(ExplicitBounds) AS bounds_hash,
             CAST(BucketCounts AS Array(Int64)) counts
      FROM {db}.otel_metrics_histogram
      WHERE (TimeUnix >= toStartOfInterval({from}, INTERVAL {step} second) - INTERVAL {step} second AND TimeUnix <= toStartOfInterval({to}, INTERVAL {step} second) + INTERVAL {step} second)
        AND ((MetricName = 'http.server.request.duration')) AND (ServiceName = 'checkout')
      ORDER BY group, attr_hash, TimeUnix ASC)
    WINDOW prev_row AS (PARTITION BY group ORDER BY attr_hash, TimeUnix ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING))
  GROUP BY `__hdx_time_bucket`, MetricName, group, ExplicitBounds ORDER BY `__hdx_time_bucket`),
points AS (SELECT `__hdx_time_bucket`, group, MetricName, arrayZipUnaligned(arrayCumSum(rates), ExplicitBounds) as point, length(point) as n FROM source),
metrics AS (
  SELECT `__hdx_time_bucket`, group, MetricName, point[n].1 AS total, 0.99 * total AS rank,
    arrayFirstIndex(x -> if(x.1 > rank, 1, 0), point) AS upper_idx, point[upper_idx].1 AS upper_count,
    ifNull(point[upper_idx].2, inf) AS upper_bound,
    CASE WHEN upper_idx > 1 THEN point[upper_idx - 1].2 WHEN point[upper_idx].2 > 0 THEN 0 ELSE inf END AS lower_bound,
    if (lower_bound = 0, 0, point[upper_idx - 1].1) AS lower_count,
    CASE WHEN upper_bound = inf THEN point[upper_idx - 1].2 WHEN lower_bound = inf THEN point[1].2
         ELSE lower_bound + (upper_bound - lower_bound) * ((rank - lower_count) / (upper_count - lower_count)) END AS "Value"
  FROM points WHERE length(point) > 1 AND total > 0)
SELECT `__hdx_time_bucket`, group, "Value" FROM metrics WHERE (`__hdx_time_bucket` >= {from} AND `__hdx_time_bucket` <= {to})
SETTINGS short_circuit_function_evaluation = 'force_enable', optimize_read_in_order = 0, cast_keep_nullable = 1
