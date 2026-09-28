-- Real-telemetry measurements on the mirror central (real-cluster-telemetry.md §3).
-- Run by measure.sh, one statement per `-- name:` block, with {db}, {from}
-- and {to} (DateTime strings, UTC) substituted; each result lands in
-- $OUT/<name>.tsv. Every rate is by received_at (the edge's custody clock),
-- so it is what central ingests, after the agents' sampling.

-- name: rates
-- rows/s per signal and cluster over the window: the calculator's
-- spans/logs/points per second (divide by pods and nodes from rates/collect.py).
SELECT 'traces' AS signal, ResourceAttributes['k8s.cluster.name'] AS cluster, count() / dateDiff('second', toDateTime('{from}'), toDateTime('{to}')) AS rows_per_s
FROM {db}.otel_traces WHERE received_at >= '{from}' AND received_at < '{to}' GROUP BY cluster
UNION ALL
SELECT 'logs', ResourceAttributes['k8s.cluster.name'], count() / dateDiff('second', toDateTime('{from}'), toDateTime('{to}'))
FROM {db}.otel_logs WHERE received_at >= '{from}' AND received_at < '{to}' GROUP BY 2
UNION ALL
SELECT 'metric_points', '', count() / dateDiff('second', toDateTime('{from}'), toDateTime('{to}'))
FROM merge('{db}', '^otel_metrics_.*_points$') WHERE received_at >= '{from}' AND received_at < '{to}'
FORMAT TSVWithNames;

-- name: rates_by_hour
-- the diurnal shape: rows per second per hour and signal (peak / average for the calculator's basis).
SELECT toStartOfHour(received_at) AS h, 'traces' AS signal, count() / 3600 AS rows_per_s FROM {db}.otel_traces
WHERE received_at >= '{from}' AND received_at < '{to}' GROUP BY h
UNION ALL
SELECT toStartOfHour(received_at), 'logs', count() / 3600 FROM {db}.otel_logs
WHERE received_at >= '{from}' AND received_at < '{to}' GROUP BY 1
ORDER BY 1, 2 FORMAT TSVWithNames;

-- name: bytes_per_row
-- stored bytes per row after merges (bSpan, bLog, bPointB, bSeries), active parts only.
-- Run it after OPTIMIZE ... FINAL on a closed day's partition (measure.sh optimize) for the merged figure.
SELECT table, sum(rows) AS total_rows, sum(data_compressed_bytes) AS compressed, sum(data_uncompressed_bytes) AS uncompressed,
       round(sum(data_compressed_bytes) / sum(rows), 2) AS bytes_per_row,
       round(sum(bytes_on_disk) / sum(rows), 2) AS disk_bytes_per_row, count() AS parts, uniqExact(partition) AS partitions
FROM system.parts WHERE database = '{db}' AND active AND rows > 0 GROUP BY table ORDER BY table FORMAT TSVWithNames;

-- name: bytes_by_column
-- where the bytes go, per table: the resource map and its index are what the entity schema removes.
SELECT table, name, round(sum(data_compressed_bytes) / greatest(any(t.rows), 1), 2) AS bytes_per_row,
       round(100 * sum(data_compressed_bytes) / any(t.total), 1) AS pct
FROM system.columns
JOIN (SELECT table, sum(rows) AS rows, sum(data_compressed_bytes) AS total FROM system.parts
      WHERE database = '{db}' AND active GROUP BY table) AS t USING table
WHERE database = '{db}' AND table IN ('otel_traces', 'otel_logs')
GROUP BY table, name HAVING pct >= 0.5 ORDER BY table, pct DESC FORMAT TSVWithNames;

-- name: index_bytes
SELECT table, name, type, round(data_compressed_bytes / 1e6, 1) AS mb FROM system.data_skipping_indices
WHERE database = '{db}' ORDER BY data_compressed_bytes DESC FORMAT TSVWithNames;

-- name: wire_bytes
-- Parquet bytes per row on the wire (bPq, bPqPointB): from the objects still in the bucket.
-- measure.sh fills {s3} (the edges' root glob with credentials) or skips this block.
SELECT splitByChar('/', _path)[-3] AS signal, count() AS objects, sum(_size) AS bytes, sum(n) AS rows, round(sum(_size) / sum(n), 1) AS bytes_per_row
FROM (SELECT _path, any(_size) AS _size, count() AS n FROM s3({s3}, 'Parquet') GROUP BY _path) GROUP BY signal ORDER BY signal FORMAT TSVWithNames;

-- name: resource_keys
-- (traces are a 10% sample: rand() % 10; logs are all rows)
-- resource attribute keys: how many rows carry each, distinct values (cardinality), and
-- whether the entity catalog covers it (resource_id's covered set: controller rid.CoveredKeys + k8s.pod.label.*).
WITH ['cloud.account.id','cloud.availability.zone','cloud.platform','cloud.provider','cloud.region','container.image.name',
      'container.image.tag','deployment.environment.name','host.id','host.name','host.type','k8s.cluster.name','k8s.cluster.uid',
      'k8s.container.name','k8s.cronjob.name','k8s.daemonset.name','k8s.deployment.name','k8s.job.name','k8s.namespace.name',
      'k8s.node.name','k8s.node.uid','k8s.pod.name','k8s.pod.start_time','k8s.pod.uid','k8s.replicaset.name',
      'k8s.statefulset.name','service.name'] AS covered
SELECT signal, k, count() AS rows, uniq(v) AS distinct_values, round(avg(length(v)), 1) AS avg_len,
       has(covered, k) OR startsWith(k, 'k8s.pod.label.') AS is_covered
FROM (
  SELECT 'traces' AS signal, ResourceAttributes AS m FROM {db}.otel_traces WHERE received_at >= '{from}' AND received_at < '{to}' AND rand() % 10 = 0
  UNION ALL
  SELECT 'logs', ResourceAttributes FROM {db}.otel_logs WHERE received_at >= '{from}' AND received_at < '{to}'
) ARRAY JOIN mapKeys(m) AS k, mapValues(m) AS v
GROUP BY signal, k ORDER BY signal, rows DESC FORMAT TSVWithNames;

-- name: attr_shape
-- map sizes per row: resource, span/log attributes; the synthetic data carried 12-21 resource keys (risk 2).
SELECT 'traces' AS signal, quantiles(0.5, 0.9, 0.99)(length(ResourceAttributes)) AS res_keys,
       quantiles(0.5, 0.9, 0.99)(length(SpanAttributes)) AS attr_keys,
       round(avg(byteSize(ResourceAttributes)), 1) AS res_bytes_raw, round(avg(byteSize(SpanAttributes)), 1) AS attr_bytes_raw,
       uniq(SpanName) AS span_names, uniq(ServiceName) AS services
FROM {db}.otel_traces WHERE received_at >= '{from}' AND received_at < '{to}'
UNION ALL
SELECT 'logs', quantiles(0.5, 0.9, 0.99)(length(ResourceAttributes)), quantiles(0.5, 0.9, 0.99)(length(LogAttributes)),
       round(avg(byteSize(ResourceAttributes)), 1), round(avg(byteSize(LogAttributes)), 1), uniq(SeverityText), uniq(ServiceName)
FROM {db}.otel_logs WHERE received_at >= '{from}' AND received_at < '{to}'
FORMAT TSVWithNames;

-- name: attr_keys_top
-- span and log attribute keys by frequency and cardinality (the rollup view and the items index pay per key).
SELECT 'traces' AS signal, k, count() AS rows, uniq(v) AS distinct_values
FROM (SELECT SpanAttributes AS m FROM {db}.otel_traces WHERE received_at >= '{from}' AND received_at < '{to}' AND rand() % 10 = 0)
ARRAY JOIN mapKeys(m) AS k, mapValues(m) AS v GROUP BY k ORDER BY rows DESC LIMIT 100
UNION ALL
SELECT 'logs', k, count(), uniq(v)
FROM (SELECT LogAttributes AS m FROM {db}.otel_logs WHERE received_at >= '{from}' AND received_at < '{to}')
ARRAY JOIN mapKeys(m) AS k, mapValues(m) AS v GROUP BY k ORDER BY 3 DESC LIMIT 100
FORMAT TSVWithNames;

-- name: resource_ids
-- resource_id distribution: distinct resources, rows per resource (skew), resources per hour (churn), map
-- shapes per resource. resource_id is on every trace/log row since the edges compute it (D21).
SELECT signal, count() AS resources, sum(n) AS rows,
       quantiles(0.5, 0.9, 0.99, 0.999)(n) AS rows_per_resource_q, max(n) AS max_rows_one_resource,
       round(max(n) / sum(n), 4) AS top_share
FROM (
  SELECT 'traces' AS signal, resource_id, count() AS n FROM {db}.otel_traces WHERE received_at >= '{from}' AND received_at < '{to}' GROUP BY resource_id
  UNION ALL
  SELECT 'logs', resource_id, count() FROM {db}.otel_logs WHERE received_at >= '{from}' AND received_at < '{to}' GROUP BY resource_id
) GROUP BY signal FORMAT TSVWithNames;

-- name: resource_churn
-- new resource_ids per hour (first seen), against live ones: the catalog's write rate and the dictionary size.
SELECT h, uniqExactIf(resource_id, first = h) AS new_resources, uniqExact(resource_id) AS live_resources
FROM (
  SELECT resource_id, toStartOfHour(received_at) AS h, min(toStartOfHour(received_at)) OVER (PARTITION BY resource_id) AS first
  FROM (SELECT resource_id, received_at FROM {db}.otel_traces WHERE received_at >= '{from}' AND received_at < '{to}'
        UNION ALL SELECT resource_id, received_at FROM {db}.otel_logs WHERE received_at >= '{from}' AND received_at < '{to}')
) GROUP BY h ORDER BY h FORMAT TSVWithNames;

-- name: resource_id_zero
-- rows without a resource_id (0): resources the edge could not hash, or an edge older than D21.
SELECT 'traces' AS signal, countIf(resource_id = 0) AS zero, count() AS rows FROM {db}.otel_traces WHERE received_at >= '{from}' AND received_at < '{to}'
UNION ALL
SELECT 'logs', countIf(resource_id = 0), count() FROM {db}.otel_logs WHERE received_at >= '{from}' AND received_at < '{to}'
FORMAT TSVWithNames;

-- name: series
-- layout B: series per pod and churn (active series, the calculator's series inputs), new series per hour.
SELECT uniqExact(series_id) AS series, uniqExact(ResourceAttributes['k8s.pod.name']) AS pods,
       round(uniqExact(series_id) / greatest(uniqExact(ResourceAttributes['k8s.pod.name']), 1), 1) AS series_per_pod,
       round(avg(length(Attributes)), 1) AS attr_keys, round(avg(length(ResourceAttributes)), 1) AS res_keys
FROM {db}.otel_metrics_series WHERE LastSeen >= '{from}' FORMAT TSVWithNames;

-- name: objects
-- edge objects: rows per object and objects per second per signal (fixedMs is charged per object).
SELECT 'traces' AS signal, uniqExact(content_key) AS objects, count() / uniqExact(content_key) AS rows_per_object,
       uniqExact(content_key) / dateDiff('second', toDateTime('{from}'), toDateTime('{to}')) AS objects_per_s
FROM {db}.otel_traces WHERE received_at >= '{from}' AND received_at < '{to}'
UNION ALL
SELECT 'logs', uniqExact(content_key), count() / uniqExact(content_key), uniqExact(content_key) / dateDiff('second', toDateTime('{from}'), toDateTime('{to}'))
FROM {db}.otel_logs WHERE received_at >= '{from}' AND received_at < '{to}'
FORMAT TSVWithNames;
