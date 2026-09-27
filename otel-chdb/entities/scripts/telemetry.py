#!/usr/bin/env python3
"""Traces and logs on the synthetic fleet (fleet.py), with the row shapes of
../../hyperdx/scripts/hdxgen (HTTP server spans, DB and downstream client
spans, exceptions on errors; access logs, app logs tied to spans, agent and
sidecar logs, job logs), generated inside ClickHouse into staging tables that
carry every schema's columns at once:

  ResourceAttributes  (a) the full map, keys sorted: covered set + residual
  resource_id         (b) content hash of the covered set (sql/resources.sql)
  ResourceResidual    (b) what the catalog doesn't cover (telemetry.sdk.*,
                      process.runtime.name, service.instance.id, custom
                      attributes of 15% of workloads) plus, inside the grace
                      window, the whole covered set
  resource_id_full    (c) content hash of the whole map (edge-announced)

Resources are every (pod version, telemetry container) alive in the window,
rows spread by workload traffic weight. Rates are scaled down from the mid
scenario (--spans / --logs in total over --hours); each row keeps its
resource's lifetime, so pods starting, relabelled and deleted inside the
window produce the race rows:

  grace     rows within --grace s of their resource version's first sighting:
            the edge sends the full covered set in ResourceResidual
  late      --late of the rows of pods deleted in the window, stamped up to
            30 s after the deletion (a terminating pod's last logs)
  withheld  resources the controller never wrote (--withheld of pods): the
            catalog lacks them until `race.py` plays the catch-up

Objects: rows go to one of 3 publishers per cluster (by pod uid), ordered by
time, cut every 10,000 rows (the edge batch); `obj`, producer_id, batch_id,
row_ordinal, received_at and content_key follow from that.

  telemetry.py [--cat ent_cat] [--db ent_src] [--spans 1000000] [--logs 1000000] [--hours 2]
"""
import argparse, sys, time
import chlib as c

ap = argparse.ArgumentParser()
ap.add_argument("--cat", default="ent_cat")
ap.add_argument("--db", default="ent_src")
ap.add_argument("--spans", type=int, default=1_000_000)
ap.add_argument("--logs", type=int, default=1_000_000)
ap.add_argument("--hours", type=float, default=2)
ap.add_argument("--end", default="2026-09-27 00:00:00")
ap.add_argument("--grace", type=int, default=120)
ap.add_argument("--late", type=float, default=0.01)
ap.add_argument("--withheld", type=int, default=200, help="1 in N pods is withheld from the catalog")
a = ap.parse_args()

W1 = f"toDateTime64('{a.end}', 3, 'UTC')"
W0 = f"({W1} - toIntervalSecond({int(a.hours * 3600)}))"
t0 = time.time()


def step(msg, sql, **kw):
    c.q(sql, **kw)
    print(f"{time.time() - t0:6.0f}s {msg}", flush=True)


c.q(f"CREATE DATABASE IF NOT EXISTS {a.db}")
# 1. resources alive in the window (plus an hour before), from the catalog tables
c.q(f"DROP TABLE IF EXISTS {a.db}.res_all SYNC")
c.q(f"CREATE TABLE {a.db}.res_all AS {a.cat}.resources ENGINE = MergeTree ORDER BY resource_id")
ins = c.statements(f"{c.SQL}/resources.sql", db=a.cat,
                   where=f"p.valid_from < {W1} AND p.valid_to > {W0} - toIntervalHour(1)")[0]
step("resources in window", ins.replace(f"INSERT INTO {a.cat}.resources", f"INSERT INTO {a.db}.res_all"))
step("catalog resources (all but the withheld)",
     f"INSERT INTO {a.cat}.resources SELECT * FROM {a.db}.res_all WHERE cityHash64(pod_uid) % {a.withheld} != 0")

# 2. per resource: weights, residual, lifetime
c.q(f"DROP TABLE IF EXISTS {a.db}.win_res SYNC")
step("win_res", f"""
CREATE TABLE {a.db}.win_res ENGINE = MergeTree ORDER BY resource_id AS
SELECT r.resource_id AS resource_id, r.attrs AS attrs, r.pod_uid AS pod_uid, r.container AS container,
       r.valid_from AS valid_from, r.valid_to AS valid_to, p.pod_end AS pod_end, w.kind AS kind,
       r.container = w.containers[1].1 AS is_main,
       CAST(if(r.container = 'istio-proxy', map(), mapConcat(w.residual, map('service.instance.id',
           lower(concat(hex(reinterpretAsFixedString(cityHash64(r.pod_uid, r.container, 1))), hex(reinterpretAsFixedString(cityHash64(r.pod_uid, r.container, 2)))))))), 'Map(LowCardinality(String), String)') AS residual,
       CAST(mapSort(mapConcat(r.attrs, residual)), 'Map(LowCardinality(String), String)') AS full_attrs,
       xxh3(concat('res.v1\\0', arrayStringConcat(arrayMap(x -> concat(x.1, '\\0', x.2, '\\0'),
           arraySort(arrayFilter(x -> x.2 != '', CAST(full_attrs, 'Array(Tuple(String, String))'))))))) AS resource_id_full,
       greatest(r.valid_from, {W0}) AS lo, least(r.valid_to, {W1}) AS hi,
       greatest(toFloat64(dateDiff('millisecond', lo, hi)), 0) / ({a.hours} * 3600000) AS frac,
       w.weight * frac * multiIf(w.kind IN ('deployment', 'statefulset') AND is_main, 1.0, w.kind = 'cronjob', 0.3, 0.0) AS w_span,
       frac * multiIf(w.kind = 'daemonset', 0.4, r.container = 'istio-proxy', 0.5 * w.weight, w.kind = 'cronjob', 1.0, w.weight) AS w_log,
       cityHash64(r.pod_uid) % {a.withheld} = 0 AS withheld
FROM {a.db}.res_all AS r
INNER JOIN (SELECT pod_uid, valid_from, pod_end, wl_key FROM {a.cat}.pods WHERE valid_from < {W1} AND valid_to > {W0}) AS p
    ON p.pod_uid = r.pod_uid AND p.valid_from = r.valid_from
INNER JOIN {a.cat}.workloads AS w ON w.wl_key = p.wl_key
WHERE r.valid_from < {W1} AND r.valid_to > {W0}
""")

TOT = c.rows(f"SELECT sum(w_span) AS s, sum(w_log) AS l, count() AS n FROM {a.db}.win_res")[0]
print("resources", TOT, flush=True)

ROUTES = "['/api/v1/orders','/api/v1/items','/api/v1/users','/healthz','/api/v1/search','/api/v2/cart','/internal/sync','/api/v1/payments','/api/v1/accounts/{id}','/graphql']"
TABLES = "['orders','order_items','users','sessions','inventory','payments','audit_log','products']"
LANG_SCOPE = "map('java','io.opentelemetry.spring-webmvc-6.0','go','go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp','nodejs','@opentelemetry/instrumentation-http','python','opentelemetry.instrumentation.flask','dotnet','OpenTelemetry.Instrumentation.AspNetCore')"


def common_select(total_w, n_total, wcol, seed):
    """Rows per resource ~ weight share; Timestamp inside [lo, hi), plus late rows after a deletion."""
    return f"""
    SELECT r.*, i,
        cityHash64(r.resource_id, i, {seed}) AS h,
        toUInt32(floor(r.{wcol} / {total_w} * {n_total} + (cityHash64(r.resource_id, {seed}) % 1000) / 1000)) AS n,
        (r.pod_end = r.valid_to AND r.valid_to < {W1} AND (h % 10000) < {int(a.late * 10000)}) AS late,
        if(late, r.valid_to + toIntervalMillisecond(h % 30000),
           r.lo + toIntervalMillisecond(intDiv(h, 7) % greatest(toUInt64(dateDiff('millisecond', r.lo, r.hi)), 1))) + toIntervalNanosecond(h % 1000000) AS ts,
        ts < r.valid_from + toIntervalSecond({a.grace}) AS grace
    FROM {a.db}.win_res AS r
    ARRAY JOIN range(toUInt32(floor(r.{wcol} / {total_w} * {n_total} + (cityHash64(r.resource_id, {seed}) % 1000) / 1000))) AS i
    WHERE r.{wcol} > 0"""


ENVELOPE = """
    CAST(concat(attrs['k8s.cluster.name'], '-pub-', toString(cityHash64(pod_uid) % 3)), 'LowCardinality(String)') AS producer_id,
    CAST('e1', 'LowCardinality(String)') AS producer_epoch"""

for sig, n_total, wcol, seed in (("traces", a.spans, "w_span", 11), ("logs", a.logs, "w_log", 13)):
    c.q(f"DROP TABLE IF EXISTS {a.db}.{sig}_gen SYNC")
    if sig == "traces":
        body = f"""
        SELECT ts AS Timestamp,
          cityHash64(h, 1) % {max(1, a.spans // 6)} AS trace_no,
          lower(concat(hex(reinterpretAsFixedString(cityHash64(trace_no, 1))), hex(reinterpretAsFixedString(cityHash64(trace_no, 2))))) AS TraceId,
          lower(hex(reinterpretAsFixedString(cityHash64(h, 3)))) AS SpanId,
          if(h % 10 < 7, lower(hex(reinterpretAsFixedString(cityHash64(h, 4)))), '') AS ParentSpanId,
          '' AS TraceState,
          ['Server', 'Client', 'Internal'][1 + (intDiv(h, 13) % 10 < 5 ? 0 : (intDiv(h, 13) % 10 < 9 ? 1 : 2))] AS SpanKind,
          {ROUTES}[1 + intDiv(h, 17) % 10] AS route,
          ['GET', 'GET', 'GET', 'POST', 'PUT', 'DELETE'][1 + intDiv(h, 19) % 6] AS method,
          {TABLES}[1 + intDiv(h, 23) % 8] AS tbl,
          intDiv(h, 29) % 100 < 3 AS err,
          multiIf(SpanKind = 'Server', concat(method, ' ', route),
                  SpanKind = 'Client', if(intDiv(h, 31) % 2 = 0, concat('SELECT ', tbl), concat(method, ' ', {ROUTES}[1 + intDiv(h, 37) % 10])),
                  concat(['process', 'serialize', 'validate', 'compute'][1 + intDiv(h, 41) % 4], ' ', tbl)) AS SpanName,
          attrs['service.name'] AS ServiceName,
          {LANG_SCOPE}[residual['telemetry.sdk.language']] AS ScopeName,
          '2.10.0' AS ScopeVersion,
          CAST(multiIf(SpanKind = 'Server', map('http.request.method', method, 'http.route', route, 'url.path', replaceOne(route, '{{id}}', toString(intDiv(h, 43) % 100000)),
                        'http.response.status_code', if(err, '500', ['200', '200', '200', '201', '204', '404'][1 + intDiv(h, 47) % 6]),
                        'user.id', concat('user-', toString(intDiv(h, 53) % 20000)), 'network.protocol.version', '1.1'),
                      SpanKind = 'Client' AND startsWith(SpanName, 'SELECT'), map('db.system', 'postgresql', 'db.operation.name', 'SELECT', 'db.collection.name', tbl,
                        'server.address', concat(attrs['k8s.namespace.name'], '-db.', attrs['k8s.namespace.name'], '.svc'), 'server.port', '5432'),
                      SpanKind = 'Client', map('http.request.method', method, 'server.address', concat(['payments', 'catalog', 'identity', 'search'][1 + intDiv(h, 59) % 4], '-api'),
                        'http.response.status_code', if(err, '503', '200')),
                      map('cart.items', toString(intDiv(h, 61) % 12))), 'Map(LowCardinality(String), String)') AS SpanAttributes,
          toUInt64(pow(2, 14 + intDiv(h, 67) % 14) * (1 + (intDiv(h, 71) % 100) / 100)) AS Duration,
          CAST(if(err, 'Error', if(SpanKind = 'Server', 'Ok', 'Unset')), 'LowCardinality(String)') AS StatusCode,
          if(err, ['upstream connect error', 'deadline exceeded', 'card declined', 'connection reset by peer'][1 + intDiv(h, 73) % 4], '') AS StatusMessage,
          if(err, [ts + toIntervalNanosecond(Duration - 1000)], []) AS `Events.Timestamp`,
          CAST(if(err, ['exception'], []), 'Array(LowCardinality(String))') AS `Events.Name`,
          CAST(if(err, [map('exception.type', ['java.io.IOException', 'context.DeadlineExceeded', 'PaymentDeclinedError', 'ECONNRESET'][1 + intDiv(h, 73) % 4],
                              'exception.message', StatusMessage, 'exception.stacktrace', concat('at ', ServiceName, '.handler(', route, ':', toString(intDiv(h, 79) % 400), ')'))], []),
               'Array(Map(LowCardinality(String), String))') AS `Events.Attributes`,
          CAST([], 'Array(String)') AS `Links.TraceId`, CAST([], 'Array(String)') AS `Links.SpanId`,
          CAST([], 'Array(String)') AS `Links.TraceState`, CAST([], 'Array(Map(LowCardinality(String), String))') AS `Links.Attributes`,"""
    else:
        body = f"""
        SELECT ts AS Timestamp,
          cityHash64(h, 1) % {max(1, a.spans // 6)} AS trace_no,
          intDiv(h, 83) % 10 < 6 AND kind != 'daemonset' AND container != 'istio-proxy' AS linked,
          if(linked, lower(concat(hex(reinterpretAsFixedString(cityHash64(trace_no, 1))), hex(reinterpretAsFixedString(cityHash64(trace_no, 2))))), '') AS TraceId,
          if(linked, lower(hex(reinterpretAsFixedString(cityHash64(h, 3)))), '') AS SpanId,
          toUInt8(linked) AS TraceFlags,
          intDiv(h, 89) % 100 AS sv,
          CAST(multiIf(sv < 12, 'DEBUG', sv < 84, 'INFO', sv < 95, 'WARN', 'ERROR'), 'LowCardinality(String)') AS SeverityText,
          toUInt8(multiIf(sv < 12, 5, sv < 84, 9, sv < 95, 13, 17)) AS SeverityNumber,
          attrs['service.name'] AS ServiceName,
          {ROUTES}[1 + intDiv(h, 17) % 10] AS route,
          toString(intDiv(h, 97) % 900 + 3) AS ms,
          multiIf(
            container = 'istio-proxy',
              concat('[', formatDateTime(ts, '%Y-%m-%dT%H:%i:%S.000Z', 'UTC'), '] "', ['GET', 'POST'][1 + intDiv(h, 19) % 2], ' ', route, ' HTTP/1.1" ',
                     if(sv >= 95, '503', '200'), ' - via_upstream - "-" 0 ', toString(intDiv(h, 101) % 9000), ' ', ms, ' ', ms, ' "-" "okhttp/4.12.0" "',
                     lower(hex(reinterpretAsFixedString(cityHash64(h, 5)))), '" "', ServiceName, ':8080" "10.', toString(intDiv(h, 103) % 250), '.', toString(intDiv(h, 107) % 250),
                     '.', toString(intDiv(h, 109) % 250), ':8080" inbound|8080||'),
            kind = 'daemonset',
              [concat('[info] [input:tail:tail.0] inotify_fs_add(): inode=', toString(intDiv(h, 113) % 9000000), ' watch_fd=', toString(intDiv(h, 127) % 900),
                      ' name=/var/log/containers/', ServiceName, '.log'),
               concat('Syncing iptables rules took ', ms, 'ms'),
               concat('Everything is ready. Begin running and processing data. batch=', toString(intDiv(h, 131) % 100000)),
               concat('ipamd: assigned IP to pod, ip=10.', toString(intDiv(h, 103) % 250), '.', toString(intDiv(h, 107) % 250), '.', toString(intDiv(h, 109) % 250))][1 + intDiv(h, 137) % 4],
            kind = 'cronjob',
              [concat('job started, batch ', toString(intDiv(h, 139) % 1000)), concat('processed ', toString(intDiv(h, 149) % 100000), ' records'),
               concat('job finished in ', ms, 's'), concat('skipping partition ', toString(intDiv(h, 151) % 64), ': already up to date')][1 + intDiv(h, 157) % 4],
            sv >= 95,
              [concat('card declined for order ', toString(intDiv(h, 163) % 1000000)), concat('upstream connect error or disconnect/reset before headers, retries=', toString(intDiv(h, 167) % 5)),
               concat('java.io.IOException: Connection reset by peer at ', ServiceName, '.Client.call(Client.java:', toString(intDiv(h, 173) % 900), ')'),
               concat('deadline exceeded after ', ms, 'ms calling ', ['payments', 'catalog', 'identity'][1 + intDiv(h, 179) % 3], '-api')][1 + intDiv(h, 181) % 4],
            [concat(['GET', 'POST', 'PUT'][1 + intDiv(h, 19) % 3], ' ', route, ' -> ', ['200', '200', '201', '404'][1 + intDiv(h, 47) % 4], ' in ', ms, 'ms'),
             concat('order ', toString(intDiv(h, 163) % 1000000), ' placed by user-', toString(intDiv(h, 53) % 20000)),
             concat('cache miss for key product:', toString(intDiv(h, 191) % 50000)),
             concat('slow query on ', {TABLES}[1 + intDiv(h, 23) % 8], ' took ', ms, 'ms'),
             concat('handled ', route)][1 + intDiv(h, 193) % 5]) AS Body,
          CAST('', 'LowCardinality(String)') AS ResourceSchemaUrl,
          CAST('', 'LowCardinality(String)') AS ScopeSchemaUrl,
          if(kind = 'daemonset' OR container = 'istio-proxy', '', {LANG_SCOPE}[residual['telemetry.sdk.language']]) AS ScopeName,
          CAST(if(ScopeName = '', '', '2.10.0'), 'LowCardinality(String)') AS ScopeVersion,
          CAST(map(), 'Map(LowCardinality(String), String)') AS ScopeAttributes,
          CAST(if(kind = 'daemonset' OR container = 'istio-proxy',
                  map('log.file.path', concat('/var/log/pods/', attrs['k8s.namespace.name'], '_', attrs['k8s.pod.name'], '_', pod_uid, '/', container, '/0.log'), 'log.iostream', 'stdout'),
                  if(linked, map('http.route', route, 'code.function', concat(ServiceName, '.handle')), map('code.function', concat(ServiceName, '.run')))),
               'Map(LowCardinality(String), String)') AS LogAttributes,
          '' AS EventName,"""
    step(f"{sig}_gen", f"""
CREATE TABLE {a.db}.{sig}_gen ENGINE = MergeTree ORDER BY (producer_id, Timestamp) AS
WITH base AS ({common_select(TOT['s' if sig == 'traces' else 'l'], n_total, wcol, seed)})
SELECT * EXCEPT (trace_no{', route, method, tbl, err' if sig == 'traces' else ', linked, sv, route, ms'}, full_attrs),
       full_attrs AS ResourceAttributes,
       CAST(if(grace, mapSort(mapConcat(attrs, residual)), mapSort(residual)), 'Map(LowCardinality(String), String)') AS ResourceResidual,
       {ENVELOPE}
FROM ({body}
          resource_id, resource_id_full, full_attrs, attrs, residual, grace, late, withheld, kind, container, pod_uid
        FROM base)
""", max_threads=1, max_block_size=512, min_insert_block_size_rows=100000, min_insert_block_size_bytes=64000000, max_insert_threads=1)
    # objects: 10k rows per publisher in time order (one small window query per publisher)
    c.q(f"DROP TABLE IF EXISTS {a.db}.{sig} SYNC")
    c.q(f"""CREATE TABLE {a.db}.{sig} ENGINE = MergeTree ORDER BY (obj, row_ordinal) AS
            SELECT * EXCEPT (pod_uid, attrs, residual), toUInt64(0) AS batch_id, toUInt32(0) AS row_ordinal, toUInt16(1) AS schema_version,
                   toUInt32(0) AS obj, CAST('', 'LowCardinality(String)') AS content_key, Timestamp AS received_at FROM {a.db}.{sig}_gen LIMIT 0""")
    obj0 = 0
    for pid in c.one(f"SELECT DISTINCT producer_id FROM {a.db}.{sig}_gen ORDER BY producer_id").splitlines():
        c.q(f"""INSERT INTO {a.db}.{sig}
            SELECT * EXCEPT (rn), max(Timestamp) OVER (PARTITION BY batch_id) + toIntervalSecond(2) AS received_at
            FROM (SELECT * EXCEPT (pod_uid, attrs, residual), toUInt64(intDiv(rn, 10000)) AS batch_id, toUInt32(rn % 10000) AS row_ordinal,
                         toUInt16(1) AS schema_version, toUInt32({obj0} + intDiv(rn, 10000)) AS obj, concat('ck-', producer_id, '-', toString(batch_id)) AS content_key
                  FROM (SELECT *, row_number() OVER (ORDER BY Timestamp) - 1 AS rn FROM {a.db}.{sig}_gen WHERE producer_id = '{pid}'))""",
            max_threads=1)
        obj0 = int(c.one(f"SELECT max(obj) + 1 FROM {a.db}.{sig}"))
    print(f"{time.time() - t0:6.0f}s {sig} objects {obj0}", flush=True)
    c.q(f"DROP TABLE {a.db}.{sig}_gen SYNC")
    print(c.one(f"SELECT count(), max(obj) + 1, countIf(grace), countIf(late), countIf(withheld), uniqExact(resource_id) FROM {a.db}.{sig}"), flush=True)
print("done", f"{time.time() - t0:.0f}s")
