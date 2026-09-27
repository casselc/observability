#!/usr/bin/env bash
# Synthetic edge objects for the lake-ui spike: ClickStack-shaped traces and
# logs, layout-B gauge points, named like lanes
# ({producer}/{signal}/{epoch}/{seq:020d}.parquet), plus a snapshot manifest
# (per-object min/max Timestamp, cluster, services) and a trace-id maplet.
# 4 clusters x 2 publishers; 5 services per cluster; 10k rows per object.
# Usage: gen.sh OUT_DIR   (needs `clickhouse` on PATH or $CH)
set -euo pipefail
OUT=${1:?out dir}; CH=${CH:-clickhouse}
T0=1790510400   # 2026-09-27 12:00:00 UTC
mkdir -p "$OUT"
q() { "$CH" local --max_threads 1 --output_format_parquet_row_group_size 10000 \
      --output_format_parquet_compression_method zstd -q "$1"; }
for p in 0 1 2 3 4 5 6 7; do
  c=$((p / 2))
  for s in 0 1 2 3 4 5; do
    f="$OUT/pub-$p/traces/e1/$(printf %020d $s).parquet"; mkdir -p "$(dirname "$f")"
    q "SELECT
      fromUnixTimestamp64Nano(toInt64(($T0 + $s*600) * 1e9 + number * 60000000)) AS Timestamp,
      lower(hex(MD5(concat('$p-$s-', toString(intDiv(number, 10)))))) AS TraceId,
      substring(lower(hex(MD5(concat('$p-$s-s', toString(number))))), 1, 16) AS SpanId,
      if(number % 10 = 0, '', substring(lower(hex(MD5(concat('$p-$s-s', toString(number - 1))))), 1, 16)) AS ParentSpanId,
      '' AS TraceState,
      ['GET /api/item','POST /api/cart','db.query','cache.get','rpc.Charge'][number % 5 + 1] AS SpanName,
      ['Server','Client','Internal'][number % 3 + 1] AS SpanKind,
      concat('svc-$c-', toString(intDiv(number, 10) % 5)) AS ServiceName,
      map('k8s.cluster.name','cluster-$c','k8s.namespace.name','shop',
          'k8s.pod.name', concat('svc-$c-', toString(intDiv(number, 10) % 5), '-pod-', toString(number % 3)),
          'k8s.node.name', concat('node-$c-', toString(number % 7)), 'service.version','1.4.2',
          'host.arch','amd64','os.type','linux','telemetry.sdk.language','go') AS ResourceAttributes,
      'otel-go' AS ScopeName, '1.31.0' AS ScopeVersion,
      map('http.method', ['GET','POST'][number % 2 + 1], 'http.route', concat('/api/v1/item/', toString(number % 50)),
          'http.status_code', if(number % 97 = 0, '500', '200'), 'user.id', toString(cityHash64(number, $p, $s) % 100000)) AS SpanAttributes,
      cityHash64(number, $p, $s) % 5000000 AS Duration,
      if(number % 97 = 0, 'Error', 'Unset') AS StatusCode,
      if(number % 97 = 0, 'upstream timeout', '') AS StatusMessage,
      fromUnixTimestamp64Milli(toInt64(($T0 + $s*600 + 601) * 1000)) AS received_at
    FROM numbers(10000) INTO OUTFILE '$f' TRUNCATE FORMAT Parquet"
  done
  for s in 0 1; do
    f="$OUT/pub-$p/logs/e1/$(printf %020d $s).parquet"; mkdir -p "$(dirname "$f")"
    q "SELECT
      fromUnixTimestamp64Nano(toInt64(($T0 + $s*1800) * 1e9 + number * 180000000)) AS Timestamp,
      lower(hex(MD5(concat('$p-$s-', toString(intDiv(number, 10)))))) AS TraceId,
      substring(lower(hex(MD5(concat('$p-$s-s', toString(number))))), 1, 16) AS SpanId,
      if(number % 53 = 0, 'ERROR', 'INFO') AS SeverityText, if(number % 53 = 0, 17, 9) AS SeverityNumber,
      concat('svc-$c-', toString(intDiv(number, 10) % 5)) AS ServiceName,
      concat('request ', ['completed','started','failed','retrying'][number % 4 + 1], ' order=',
             toString(cityHash64(number, $p, $s) % 1000000), ' latency_ms=', toString(number % 900)) AS Body,
      map('k8s.cluster.name','cluster-$c','k8s.namespace.name','shop',
          'k8s.pod.name', concat('svc-$c-', toString(intDiv(number, 10) % 5), '-pod-', toString(number % 3)),
          'k8s.node.name', concat('node-$c-', toString(number % 7))) AS ResourceAttributes,
      map('logger', 'app', 'thread', toString(number % 8)) AS LogAttributes,
      fromUnixTimestamp64Milli(toInt64(($T0 + $s*1800 + 1801) * 1000)) AS received_at
    FROM numbers(10000) INTO OUTFILE '$f' TRUNCATE FORMAT Parquet"
  done
  f="$OUT/pub-$p/metrics_gauge/e1/$(printf %020d 0).parquet"; mkdir -p "$(dirname "$f")"
  q "SELECT cityHash64($p, number % 500) AS series_id,
      ['cpu.usage','mem.rss','http.server.active','queue.depth'][number % 4 + 1] AS MetricName,
      concat('svc-$c-', toString(number % 5)) AS ServiceName,
      fromUnixTimestamp64Nano(toInt64(($T0 + intDiv(number, 500) * 60) * 1e9)) AS TimeUnix,
      (cityHash64(number, $p) % 10000) / 100.0 AS Value
    FROM numbers(20000) INTO OUTFILE '$f' TRUNCATE FORMAT Parquet"
done
# Snapshot manifest: what a sealer's snapshot + the entity catalog give a
# planner before any data read (here computed from the objects themselves).
"$CH" local --max_threads 2 -q "
SELECT replaceOne(_path, '$OUT/', '') AS path, _size AS bytes, splitByChar('/', _path)[-3] AS signal,
       any(ResourceAttributes['k8s.cluster.name']) AS cluster,
       groupUniqArray(ServiceName) AS services,
       toUnixTimestamp64Milli(min(Timestamp)) AS ts_min, toUnixTimestamp64Milli(max(Timestamp)) AS ts_max,
       count() AS rows
FROM file('$OUT/pub-*/{traces,logs}/e1/*.parquet', Parquet) GROUP BY _path, _size ORDER BY path
FORMAT JSONEachRow" > "$OUT/manifest.jsonl"
# Trace-id maplet (lake P2 shape): trace_id -> object list, here for traces+logs.
"$CH" local --max_threads 2 -q "
SELECT TraceId, groupUniqArray(replaceOne(_path, '$OUT/', '')) AS paths
FROM file('$OUT/pub-*/{traces,logs}/e1/*.parquet', Parquet) GROUP BY TraceId FORMAT JSONEachRow" > "$OUT/maplet.jsonl"
du -sh "$OUT"; wc -l "$OUT"/manifest.jsonl "$OUT"/maplet.jsonl
