#!/usr/bin/env python3
"""The statements the proxy is checked and timed on, as JSON lines
{id, set, scenario, query, params, db}:

  captured   every statement HyperDX 2.39.1 sent in ../../../hyperdx's
             captures (results/schema-replay.json, schema3-replay.json: 799,
             three schema sides), pointed at variant c (the database
             parameter -> rw_c), their time parameters shifted so that each
             statement's window ends where this data ends, and hdxgen's
             values replaced by this data's (a pod, a route, a db system)
  scenarios  (and scenarios_a: the same for the ClickStack table rw_a, as
             HyperDX renders them there)
             the entities spike's 15 query shapes (../../scripts/queries.py)
             as HyperDX 2.39.1 renders them for variant c (a ClickStack table
             whose ResourceAttributes has no items index: map filters with
             indexHint(mapContains), text-index full text, discovery from the
             rollup), with HyperDX's own query parameters for the table and
             the time bounds; plus the rollup-less value discovery (a scan)
  matrix     every filter form on covered, residual and absent keys, the race
             values (pods the catalog doesn't know, with and without the
             grace-window residual), group-bys and selects of every key level

  corpus.py [--out DIR]    writes DIR/{captured,scenarios,matrix}.jsonl
"""
import argparse, json, os, re, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "scripts"))
import chlib as c  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--out", default=os.environ.get("RW_SCRATCH", "/tmp"))
ap.add_argument("--db", default="rw_c")
ap.add_argument("--cat", default="rw_cat")
a = ap.parse_args()
DB, CAT = a.db, a.cat
HDX = os.path.join(HERE, "..", "..", "..", "hyperdx", "results")
T1 = int(time.mktime(time.strptime("2026-09-27 00:00:00", "%Y-%m-%d %H:%M:%S")) - time.timezone) * 1000
T0 = T1 - 2 * 3600 * 1000
TL = T1 - 15 * 60 * 1000

POD = c.one(f"SELECT ResourceAttributes['k8s.pod.name'] AS p FROM rw_a.otel_logs WHERE SeverityText = 'ERROR' GROUP BY p ORDER BY count() DESC, p LIMIT 1")
NS = "payments-core"
SVC = c.one("SELECT ServiceName FROM rw_a.otel_traces GROUP BY ServiceName ORDER BY count() DESC, ServiceName LIMIT 1")
TRACE = c.one("SELECT TraceId FROM rw_a.otel_traces GROUP BY TraceId ORDER BY count() DESC, TraceId LIMIT 1")
ROW_TS, ROW_SVC = c.one("SELECT toString(Timestamp), ServiceName FROM rw_a.otel_logs WHERE SeverityText = 'ERROR' ORDER BY Timestamp DESC LIMIT 1 OFFSET 7").split("\t")
# race value: a pod the catalog doesn't know whose rows carry the covered set in the residual (grace window)
GRACE_POD = c.one(f"SELECT ResourceResidual['k8s.pod.name'] AS p FROM {DB}.otel_logs WHERE mapContains(ResourceResidual, 'k8s.pod.name') "
                  f"AND NOT dictHas('{CAT}.d_res', resource_id) GROUP BY p ORDER BY count() DESC, p LIMIT 1")
VALUES = dict(pod=POD, ns=NS, svc=SVC, trace=TRACE, grace_pod=GRACE_POD)


def write(name, rows):
    p = os.path.join(a.out, f"{name}.jsonl")
    with open(p, "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    print(name, len(rows), "->", p, flush=True)


# ---- captured ------------------------------------------------------------
SUBST = [("frontend-f778-0", POD), ("/api/cart", "/api/v1/orders"), ("%redis%", "%postgres%")]


def captured():
    out = []
    for f in ("schema-replay.json", "schema3-replay.json"):
        d = json.load(open(os.path.join(HDX, f)))
        for i, s in enumerate(d["statements"]):
            q, params = s["query"], dict(s["params"])
            for k, v in list(params.items()):
                if v in ("hdx_old", "hdx_new", "hdx_full", "hdx_b", "hdx_stock"):
                    params[k] = DB
            ms = [int(v) for v in params.values() if re.fullmatch(r"17\d{11}", v)]
            if ms:
                shift = T1 - max(ms)
                for k, v in list(params.items()):
                    if re.fullmatch(r"17\d{11}", v):
                        params[k] = str(int(v) + shift)
            for x, y in SUBST:
                q = q.replace(x, y)
            out.append(dict(id=f"{f.split('.')[0]}#{i}", set="captured", scenario=s["scenario"], side=s["side"], query=q, params=params, db="default"))
    return out


# ---- scenarios (queries.py's shapes, HyperDX's rendering for variant c) ----
class R:
    """Renders with HyperDX's parameters: {HYPERDX_PARAM_n:Type}. side "c"
    (variant c) or "a" (the ClickStack table, rw_a: items index,
    materialized k8s columns, text-index discovery)."""

    def __init__(self, side="c"):
        self.params, self.n, self.side = {}, 0, side

    def p(self, v, t):
        self.n += 1
        name = f"HYPERDX_PARAM_{self.n}"
        self.params[name] = str(v)
        return "{" + name + ":" + t + "}"

    def table(self, t):
        return f"{self.p(DB if self.side == 'c' else 'rw_a', 'Identifier')}.{self.p(t, 'Identifier')}"

    def ms(self, x):
        return f"fromUnixTimestamp64Milli({self.p(x, 'Int64')})"


def trange(r, s, lo=T0, hi=T1):
    t = f"(Timestamp >= {r.ms(lo)} AND Timestamp <= {r.ms(hi)})"
    if s == "logs":
        t += f"AND(toStartOfFiveMinutes(Timestamp) >= toStartOfFiveMinutes({r.ms(lo)}) AND toStartOfFiveMinutes(Timestamp) <= toStartOfFiveMinutes({r.ms(hi)}))"
    return t


def res_eq(k, v, side="c"):
    if side == "a":
        return f"(has(`ResourceAttributeItems`, concat('{k}', '=', '{v}')))"
    return f"(`ResourceAttributes`['{k}'] = '{v}' AND indexHint(mapContains(`ResourceAttributes`, '{k}')))"


def res_ilike(k, v):
    return f"(`ResourceAttributes`['{k}'] ILIKE '%{v}%' AND indexHint(mapContains(`ResourceAttributes`, '{k}')))"


def fulltext():
    return "(hasAllTokens(lower(Body), lower('card declined')) AND (lower(Body) LIKE lower('%card declined%')))"


def logs_list(r, where, lo=TL):
    return (f"SELECT Timestamp,ServiceName,SeverityText,Body,received_at,toDate(received_at),toStartOfFiveMinutes(Timestamp),_block_number,_block_offset "
            f"FROM {r.table('otel_logs')} WHERE {trange(r, 'logs', lo)} {('AND ' + where) if where else ''} "
            f"ORDER BY (toStartOfFiveMinutes(Timestamp), Timestamp) DESC LIMIT {r.p(200, 'Int32')} OFFSET {r.p(0, 'Int32')} \nFORMAT JSONCompactEachRowWithNamesAndTypes")


def hist(r, s, where, by="SeverityText"):
    return (f"SELECT count(),{by},toStartOfInterval(toDateTime(Timestamp), INTERVAL 5 minute) AS `__hdx_time_bucket` FROM {r.table('otel_' + s)} "
            f"WHERE {trange(r, s)} {('AND ' + where) if where else ''} GROUP BY {by},toStartOfInterval(toDateTime(Timestamp), INTERVAL 5 minute) AS `__hdx_time_bucket` "
            f"ORDER BY toStartOfInterval(toDateTime(Timestamp), INTERVAL 5 minute) AS `__hdx_time_bucket` LIMIT {r.p(100000, 'Int32')} \nFORMAT JSON")


def traces_list(r, where, lo=TL):
    return (f"SELECT Timestamp,ServiceName,StatusCode,round(Duration/1e6),SpanName,received_at,toDate(received_at),toDateTime(Timestamp) "
            f"FROM {r.table('otel_traces')} WHERE {trange(r, 'traces', lo)} {('AND ' + where) if where else ''} "
            f"ORDER BY (toDateTime(Timestamp), Timestamp) DESC LIMIT {r.p(200, 'Int32')} OFFSET {r.p(0, 'Int32')} \nFORMAT JSONCompactEachRowWithNamesAndTypes")


RES_KEYS = ["k8s.pod.name", "k8s.namespace.name", "k8s.deployment.name", "service.version", "k8s.node.name", "cloud.region",
            "k8s.cluster.name", "host.name", "deployment.environment.name", "service.name"]


def parts_filter(r, t):
    lo, hi = r.ms(T0), r.ms(T1)
    lo2, hi2, lo3, hi3 = r.ms(T0), r.ms(T1), r.ms(T0), r.ms(T1)
    return (f"part_name IN (SELECT name FROM system.parts WHERE database = {r.p('rw_a', 'String')} AND table = {r.p(t, 'String')} AND active = 1 AND "
            f"((min_time >= {lo} AND min_time <= {hi}) OR (max_time <= {hi2} AND max_time >= {lo2}) OR (min_time <= {lo3} AND max_time >= {hi3})))")


def keys(r, s):
    if r.side == "a":
        return (f"SELECT splitByString('=', token)[1] AS key FROM mergeTreeTextIndex({r.p('rw_a', 'String')}, {r.p('otel_' + s, 'String')}, 'idx_res_attr_items') "
                f"WHERE {parts_filter(r, 'otel_' + s)} GROUP BY key HAVING key != '' LIMIT 1000 \nFORMAT JSON")
    return (f"SELECT Key FROM {r.table('otel_' + s + '_kv_rollup_15m')} WHERE ColumnIdentifier = {r.p('ResourceAttributes', 'String')} "
            f"AND Timestamp >= toStartOfFifteenMinutes({r.ms(T0)}) AND Timestamp <= toStartOfFifteenMinutes({r.ms(T1)}) GROUP BY Key ORDER BY sum(count) DESC LIMIT 1000 \nFORMAT JSON")


def values(r, s):
    if r.side == "a":
        sw = " OR ".join(f"startsWith(token, '{k}=')" for k in RES_KEYS)
        return (f"SELECT * FROM (SELECT 'ResourceAttributes' as column, substring(token, 1, position(token, '=') - 1) AS key, "
                f"groupUniqArray(20)(substring(token, position(token, '=') + 1)) AS value FROM mergeTreeTextIndex({r.p('rw_a', 'String')}, {r.p('otel_' + s, 'String')}, 'idx_res_attr_items') "
                f"WHERE {parts_filter(r, 'otel_' + s)} AND ({sw}) AND substring(token, position(token, '=') + 1) != '' GROUP BY column, key) \nFORMAT JSON")
    kl = ",".join(f"'{k}'" for k in RES_KEYS)
    return (f"SELECT * FROM (SELECT ColumnIdentifier, Key, groupUniqArray({r.p(20, 'Int32')})(Value) as Values FROM {r.table('otel_' + s + '_kv_rollup_15m')} "
            f"WHERE ((ColumnIdentifier = {r.p('ResourceAttributes', 'String')} AND Key IN ({kl}))) AND Timestamp >= toStartOfFifteenMinutes({r.ms(T0)}) "
            f"AND Timestamp <= toStartOfFifteenMinutes({r.ms(T1)}) AND notEmpty(Value) GROUP BY ColumnIdentifier, Key ORDER BY ColumnIdentifier, Key) \nFORMAT JSON")


def values_scan(r, s):
    sel = ", ".join(f"ResourceAttributes['{k}'] as param{i}" for i, k in enumerate(RES_KEYS))
    agg = ", ".join(f"groupUniqArray(20)(param{i}) AS param{i}" for i in range(len(RES_KEYS)))
    return (f"WITH sampledData AS (SELECT {sel} FROM {r.table('otel_' + s)} WHERE (Timestamp >= {r.ms(T0)} AND Timestamp <= {r.ms(T1)}) "
            f"LIMIT {r.p(3000000, 'Int32')}) SELECT {agg} FROM sampledData \nFORMAT JSON")


def row_detail(r):
    return (f"SELECT *,Timestamp AS \"__hdx_timestamp\",Body AS \"__hdx_body\",ServiceName AS \"__hdx_service_name\","
            f"ResourceAttributes AS \"__hdx_resource_attributes\",LogAttributes AS \"__hdx_event_attributes\" FROM {r.table('otel_logs')} "
            f"WHERE (Timestamp=parseDateTime64BestEffort('{ROW_TS}', 9) AND ServiceName='{ROW_SVC}' AND SeverityText='ERROR') LIMIT {r.p(1, 'Int32')} \nFORMAT JSON")


def waterfall(r):
    return [f"SELECT SpanName AS \"Body\",Timestamp AS \"Timestamp\",SpanId AS \"SpanId\",ServiceName AS \"ServiceName\",(Duration) / 1e9 AS \"Duration\","
            f"ParentSpanId AS \"ParentSpanId\",StatusCode AS \"StatusCode\",SpanAttributes AS \"SpanAttributes\",Events AS \"SpanEvents\" "
            f"FROM {r.table('otel_traces')} WHERE {trange(r, 'traces')} AND (TraceId = '{TRACE}') LIMIT {r.p(100000, 'Int32')} \nFORMAT JSONCompactEachRowWithNamesAndTypes",
            f"SELECT Timestamp AS \"Timestamp\",`ResourceAttributes`['service.name'] AS \"serviceName\",ParentSpanId AS \"parentSpanId\" "
            f"FROM {r.table('otel_traces')} WHERE {trange(r, 'traces')} AND (TraceId = '{TRACE}') LIMIT {r.p(100000, 'Int32')} \nFORMAT JSONCompactEachRowWithNamesAndTypes",
            f"SELECT Body AS \"Body\",Timestamp AS \"Timestamp\",SpanId AS \"SpanId\",ServiceName AS \"ServiceName\",SeverityText AS \"SeverityText\" "
            f"FROM {r.table('otel_logs')} WHERE {trange(r, 'logs')} AND (TraceId = '{TRACE}') LIMIT {r.p(100000, 'Int32')} \nFORMAT JSONCompactEachRowWithNamesAndTypes"]


def scenario_list(side="c"):
    err = "(SeverityText ILIKE '%ERROR%')"
    pod = res_eq("k8s.pod.name", POD, side)
    ns = res_eq("k8s.namespace.name", NS, side)
    ns_col = "`__hdx_materialized_k8s.namespace.name`" if side == "a" else "`ResourceAttributes`['k8s.namespace.name']"
    java = res_ilike("telemetry.sdk.language", "java")
    dbs = "(`SpanAttributes`['db.system'] ILIKE '%postgres%' AND indexHint(mapContains(`SpanAttributes`, 'db.system')))"
    svc = f"(ServiceName = '{SVC}')"
    S = {
        "logs-all": lambda r: [hist(r, "logs", ""), logs_list(r, "")],
        "logs-res-attr (pod = , + ERROR)": lambda r: [hist(r, "logs", f"({pod} AND {err})"), logs_list(r, f"({pod} AND {err})", lo=T0)],
        "logs-namespace (ns =)": lambda r: [hist(r, "logs", ns), logs_list(r, ns)],
        "logs-service (ServiceName =)": lambda r: [hist(r, "logs", svc), logs_list(r, svc)],
        "logs-fulltext": lambda r: [hist(r, "logs", fulltext()), logs_list(r, fulltext(), lo=T0)],
        "logs-group-by-namespace": lambda r: [f"SELECT {ns_col} AS g, count() FROM {r.table('otel_logs')} WHERE {trange(r, 'logs')} "
                                              f"GROUP BY g ORDER BY count() DESC, g LIMIT 20 \nFORMAT JSON"],
        "logs-row-detail": lambda r: [row_detail(r)],
        "traces-all": lambda r: [hist(r, "traces", "", by="StatusCode"), traces_list(r, "")],
        "traces-res-attr (pod =)": lambda r: [hist(r, "traces", pod, by="StatusCode"), traces_list(r, pod, lo=T0)],
        "traces-residual (sdk ILIKE java, db.system)": lambda r: [hist(r, "traces", f"({java} AND {dbs})", by="StatusCode"), traces_list(r, f"({java} AND {dbs})")],
        "traces-group-by-deployment (p95)": lambda r: [f"SELECT `ResourceAttributes`['k8s.deployment.name'] AS g, quantile(0.95)(Duration) FROM {r.table('otel_traces')} "
                                                       f"WHERE {trange(r, 'traces')} AND StatusCode = 'Error' GROUP BY g ORDER BY 2 DESC, g LIMIT 20 \nFORMAT JSON"],
        "trace-waterfall (by TraceId)": waterfall,
        "discovery: resource keys": lambda r: [keys(r, "logs"), keys(r, "traces")],
        "discovery: resource values (10 keys)": lambda r: [values(r, "logs"), values(r, "traces")],
        "discovery: values by scanning (no rollup)": lambda r: [values_scan(r, "logs")],
    }
    if side == "a":
        del S["discovery: values by scanning (no rollup)"]
    out = []
    for name, fn in S.items():
        r = R(side)
        for i, q in enumerate(fn(r)):
            out.append(dict(id=f"{name}#{i}", set="scenarios" if side == "c" else "scenarios_a", scenario=name, query=q, params=dict(r.params), db="default"))
    return out


# ---- matrix ----------------------------------------------------------------
def matrix():
    one = lambda sql: c.one(sql)
    t = f"{DB}.otel_logs"
    val = {k: one(f"SELECT ResourceAttributes['{k}'] AS v FROM {t} WHERE v != '' GROUP BY v ORDER BY count() DESC, v LIMIT 1")
           for k in ("k8s.pod.name", "k8s.namespace.name", "k8s.deployment.name", "k8s.statefulset.name", "service.version", "k8s.pod.uid",
                     "k8s.container.name", "container.image.tag", "cloud.region", "k8s.node.name", "k8s.pod.start_time",
                     "telemetry.sdk.language", "service.instance.id", "git.commit.sha", "process.runtime.name")}
    label = one(f"SELECT k FROM (SELECT arrayJoin(mapKeys(ResourceAttributes)) AS k FROM {t} LIMIT 200000) WHERE k LIKE 'k8s.pod.label.%' GROUP BY k ORDER BY count() DESC, k LIMIT 1")
    if label:
        val[label] = one(f"SELECT ResourceAttributes['{label}'] AS v FROM {t} WHERE v != '' GROUP BY v ORDER BY count() DESC, v LIMIT 1")
    val["nope.absent"] = "x"
    grace_ns = one(f"SELECT ResourceResidual['k8s.namespace.name'] FROM {t} WHERE mapContains(ResourceResidual, 'k8s.namespace.name') "
                   f"AND NOT dictHas('{CAT}.d_res', resource_id) LIMIT 1")
    rows = []
    n = [0]

    def add(sig, where, kind):
        n[0] += 1
        r = R()
        q = (f"SELECT count(), sum(cityHash64(Timestamp, ServiceName, {'SpanId' if sig == 'traces' else 'Body'})) FROM {r.table('otel_' + sig)} "
             f"WHERE {trange(r, sig)} AND ({where}) \nFORMAT JSON")
        rows.append(dict(id=f"matrix#{n[0]}", set="matrix", scenario=kind, query=q, params=dict(r.params), db="default"))

    ra = lambda k: f"ResourceAttributes['{k}']"
    for sig in ("logs", "traces"):
        for k, v in val.items():
            prefix = v[:3]
            forms = [(f"{ra(k)} = '{v}'", "="), (f"{ra(k)} != '{v}'", "!="), (f"{ra(k)} IN ('{v}', 'zz')", "IN"),
                     (f"{ra(k)} NOT IN ('{v}')", "NOT IN"), (f"{ra(k)} LIKE '{prefix}%'", "LIKE"), (f"{ra(k)} ILIKE '%{v[-3:].upper()}%'", "ILIKE"),
                     (f"{ra(k)} NOT ILIKE '{prefix}%'", "NOT ILIKE"), (f"{ra(k)} = ''", "= ''"), (f"{ra(k)} != ''", "!= ''"),
                     (f"{ra(k)} LIKE '%'", "LIKE %"), (f"mapContains(ResourceAttributes, '{k}')", "exists"),
                     (f"NOT mapContains(`ResourceAttributes`, '{k}')", "NOT exists"), (f"NOT ({ra(k)} = '{v}')", "NOT (=)"),
                     (f"'{v}' = {ra(k)}", "reversed ="), (f"({ra(k)} = '{v}' OR ServiceName = '{SVC}')", "= OR native"),
                     (f"notEmpty({ra(k)}) = 1", "notEmpty (key:*)"), (f"empty({ra(k)})", "empty")]
            for w, kind in forms:
                add(sig, w, kind)
        # the race: a pod the catalog doesn't know yet (grace rows carry it in the residual)
        if GRACE_POD:
            add(sig, f"{ra('k8s.pod.name')} = '{GRACE_POD}'", "race: grace pod =")
            add(sig, f"{ra('k8s.pod.name')} ILIKE '%{GRACE_POD[-6:]}%'", "race: grace pod ILIKE")
        if grace_ns:
            add(sig, f"{ra('k8s.namespace.name')} = '{grace_ns}'", "race: grace namespace =")
        # parameters as values (HyperDX's SQL mode can send them)
        r = R()
        pv = r.p(val["k8s.namespace.name"], "String")
        q = (f"SELECT count() FROM {r.table('otel_' + sig)} WHERE {trange(r, sig)} AND ResourceAttributes['k8s.namespace.name'] = {pv} \nFORMAT JSON")
        n[0] += 1
        rows.append(dict(id=f"matrix#{n[0]}", set="matrix", scenario="param value", query=q, params=dict(r.params), db="default"))
    # group-bys / selects of every level, and an unaliased select (the column name must stay)
    for sig in ("logs", "traces"):
        for k in list(val) + ["k8s.cluster.name", "cloud.availability.zone", "host.name", "deployment.environment.name", "service.name"]:
            r = R()
            q = (f"SELECT ResourceAttributes['{k}'] AS g, count() AS n FROM {r.table('otel_' + sig)} WHERE {trange(r, sig)} "
                 f"GROUP BY g ORDER BY n DESC, g LIMIT 50 \nFORMAT JSON")
            n[0] += 1
            rows.append(dict(id=f"matrix#{n[0]}", set="matrix", scenario="group by", query=q, params=dict(r.params), db="default"))
        r = R()
        q = (f"SELECT `ResourceAttributes`['k8s.namespace.name'], count() FROM {r.table('otel_' + sig)} WHERE {trange(r, sig)} "
             f"GROUP BY ResourceAttributes['k8s.namespace.name'] ORDER BY 2 DESC, 1 LIMIT 10 \nFORMAT JSON")
        n[0] += 1
        rows.append(dict(id=f"matrix#{n[0]}", set="matrix", scenario="unaliased select", query=q, params=dict(r.params), db="default"))
    return rows


if __name__ == "__main__":
    os.makedirs(a.out, exist_ok=True)
    write("captured", captured())
    write("scenarios", scenario_list())
    write("scenarios_a", scenario_list("a"))
    write("matrix", matrix())
    with open(os.path.join(a.out, "values.json"), "w") as f:
        json.dump(VALUES, f)
    print(VALUES)
