#!/usr/bin/env python3
"""HyperDX-shaped query latency, (a) against (b)/(c), on the loaded databases.

The statements are the shapes HyperDX 2.39.1 sent in ../../hyperdx
(results/schema-replay.json, captured through chproxy), with this data's
values, in four renderings:

  a     ent_a.otel_*: the ClickStack table; what HyperDX sends for it
        (text-index items filters, hasAllTokens, mergeTreeTextIndex key and
        value discovery, the materialized k8s columns for logs)
  view  ent_b2.otel_* (the view): what HyperDX sends for a table it sees no
        skip index, sort key or materialized column on (the pre-alignment
        side of the capture): map filters, hasToken + LIKE, and, because the
        view has a `<name>_kv_rollup_15m`, key/value discovery from the rollup
  c     ent_b2.otel_*_rid: the table with ResourceAttributes as an ALIAS; what
        HyperDX sends for a ClickStack table whose ResourceAttributes has no
        items index: (a)'s statements except map filters on
        ResourceAttributes, and resource discovery from the rollup
  rw    c, with every ResourceAttributes filter / group key rewritten to the
        catalog (resource_id IN (SELECT ... FROM ent_cat.resource_kv ...),
        ResourceResidual[...] for keys the catalog does not cover, a level
        dictionary for group keys): what a HyperDX change, or a rewriting
        proxy, would send

Each statement is run once to warm up, then ROUNDS times with the renderings
interleaved; server time = median X-ClickHouse-Summary elapsed_ns; rows and
bytes read from the same summary. use_query_condition_cache = 0 (HyperDX's
repeated predicates would otherwise be answered from the cache).

  queries.py [--rounds 5] [--out ../results/queries.jsonl]
"""
import argparse, json, statistics, time
import chlib as c

ap = argparse.ArgumentParser()
ap.add_argument("--rounds", type=int, default=5)
ap.add_argument("--modes", default="a,view,c,rw")
ap.add_argument("--only", default="")
ap.add_argument("--out", default=f"{c.HERE}/../results/queries.jsonl")
a = ap.parse_args()
MODES = a.modes.split(",")
T1 = int(time.mktime(time.strptime("2026-09-27 00:00:00", "%Y-%m-%d %H:%M:%S")) - time.timezone) * 1000
T0 = T1 - 2 * 3600 * 1000
TL = T1 - 15 * 60 * 1000  # a result page: HyperDX's first window
CAT = "ent_cat"

# sample values from the data
POD = c.one("SELECT ResourceAttributes['k8s.pod.name'] AS p FROM ent_a.otel_logs WHERE SeverityText = 'ERROR' GROUP BY p ORDER BY count() DESC LIMIT 1")
NS = "payments-core"
SVC = c.one("SELECT ServiceName FROM ent_a.otel_traces GROUP BY ServiceName ORDER BY count() DESC LIMIT 1")
TRACE = c.one("SELECT TraceId FROM ent_a.otel_traces GROUP BY TraceId ORDER BY count() DESC LIMIT 1")
ROW_TS, ROW_SVC = c.one("SELECT toString(Timestamp), ServiceName FROM ent_a.otel_logs WHERE SeverityText = 'ERROR' ORDER BY Timestamp DESC LIMIT 1 OFFSET 7").split("\t")

TABLE = {"a": "ent_a.otel_{s}", "view": "ent_b2.otel_{s}", "c": "ent_b2.otel_{s}_rid", "rw": "ent_b2.otel_{s}_rid"}
ROLLUP = {"a": "ent_a.otel_{s}_kv_rollup_15m", "view": "ent_b2.otel_{s}_kv_rollup_15m", "c": "ent_b2.otel_{s}_kv_rollup_15m", "rw": "ent_b2.otel_{s}_kv_rollup_15m"}


def ms(x):
    return f"fromUnixTimestamp64Milli({x})"


def trange(mode, s, lo=T0, hi=T1):
    t = f"(Timestamp >= {ms(lo)} AND Timestamp <= {ms(hi)})"
    if s == "logs" and mode != "view":  # HyperDX adds the sort key's time bucket when it can see the sort key
        t += f" AND (toStartOfFiveMinutes(Timestamp) >= toStartOfFiveMinutes({ms(lo)}) AND toStartOfFiveMinutes(Timestamp) <= toStartOfFiveMinutes({ms(hi)}))"
    return t


def res_eq(mode, k, v):
    if mode == "a":
        return f"(has(`ResourceAttributeItems`, concat('{k}', '=', '{v}')))"
    if mode == "rw":
        return f"(resource_id IN (SELECT resource_id FROM {CAT}.resource_kv WHERE Key = '{k}' AND Value = '{v}'))"
    return f"(`ResourceAttributes`['{k}'] = '{v}' AND indexHint(mapContains(`ResourceAttributes`, '{k}')))"


def res_ilike(mode, k, v, residual=False):
    if mode == "rw":
        if residual:
            return f"(ResourceResidual['{k}'] ILIKE '%{v}%')"
        return f"(resource_id IN (SELECT resource_id FROM {CAT}.resource_kv WHERE Key = '{k}' AND Value ILIKE '%{v}%'))"
    return f"(`ResourceAttributes`['{k}'] ILIKE '%{v}%' AND indexHint(mapContains(`ResourceAttributes`, '{k}')))"


def res_col(mode, s, k):
    if mode == "a" and s == "logs" and k in ("k8s.namespace.name", "k8s.pod.name", "k8s.deployment.name"):
        return f"`__hdx_materialized_{k}`"  # HyperDX's materialized-column rewrite (the column exists in (a) logs)
    if mode == "rw":
        lvl = {"k8s.namespace.name": ("d_ns", "ns_key"), "k8s.deployment.name": ("d_wl", "wl_key")}[k]
        return (f"JSONExtractString(dictGet('{CAT}.{lvl[0]}', 'attrs', dictGet('{CAT}.d_pod', '{lvl[1]}', "
                f"dictGet('{CAT}.d_res', 'pod_key', resource_id))), '{k}')")
    return f"`ResourceAttributes`['{k}']"


def fulltext(mode):
    if mode == "view":
        return "(hasToken(lower(Body), lower('card')) AND hasToken(lower(Body), lower('declined')) AND (lower(Body) LIKE lower('%card declined%')))"
    return "(hasAllTokens(lower(Body), lower('card declined')) AND (lower(Body) LIKE lower('%card declined%')))"


def logs_list(mode, where, lo=TL):
    extra = ",toStartOfFiveMinutes(Timestamp),_block_number,_block_offset" if mode != "view" else ""
    order = "(toStartOfFiveMinutes(Timestamp), Timestamp) DESC" if mode != "view" else "Timestamp DESC"
    return (f"SELECT Timestamp,ServiceName,SeverityText,Body,received_at,toDate(received_at){extra} FROM {TABLE[mode].format(s='logs')} "
            f"WHERE {trange(mode, 'logs', lo)} {('AND ' + where) if where else ''} ORDER BY {order} LIMIT 200")


def hist(mode, s, where, by="SeverityText"):
    return (f"SELECT count(),{by},toStartOfInterval(toDateTime(Timestamp), INTERVAL 5 minute) AS `__hdx_time_bucket` FROM {TABLE[mode].format(s=s)} "
            f"WHERE {trange(mode, s)} {('AND ' + where) if where else ''} GROUP BY {by},`__hdx_time_bucket` ORDER BY `__hdx_time_bucket` LIMIT 100000")


def traces_list(mode, where, lo=TL):
    return (f"SELECT Timestamp,ServiceName,StatusCode,round(Duration/1e6),SpanName,received_at,toDate(received_at),toDateTime(Timestamp) "
            f"FROM {TABLE[mode].format(s='traces')} WHERE {trange(mode, 'traces', lo)} {('AND ' + where) if where else ''} "
            f"ORDER BY (toDateTime(Timestamp), Timestamp) DESC LIMIT 200")


RES_KEYS = ["k8s.pod.name", "k8s.namespace.name", "k8s.deployment.name", "service.version", "k8s.node.name", "cloud.region",
            "k8s.cluster.name", "host.name", "deployment.environment.name", "service.name"]


def parts_filter(db, t):
    return (f"part_name IN (SELECT name FROM system.parts WHERE database = '{db}' AND table = '{t}' AND active = 1 AND "
            f"((min_time >= {ms(T0)} AND min_time <= {ms(T1)}) OR (max_time <= {ms(T1)} AND max_time >= {ms(T0)}) OR (min_time <= {ms(T0)} AND max_time >= {ms(T1)})))")


def keys(mode, s):
    if mode == "a":
        db, t = TABLE[mode].format(s=s).split(".")
        return (f"SELECT splitByString('=', token)[1] AS key FROM mergeTreeTextIndex('{db}', '{t}', 'idx_res_attr_items') "
                f"WHERE {parts_filter(db, t)} GROUP BY key HAVING key != '' LIMIT 1000")
    return (f"SELECT Key FROM {ROLLUP[mode].format(s=s)} WHERE ColumnIdentifier = 'ResourceAttributes' "
            f"AND Timestamp >= toStartOfFifteenMinutes({ms(T0)}) AND Timestamp <= toStartOfFifteenMinutes({ms(T1)}) GROUP BY Key ORDER BY sum(count) DESC LIMIT 1000")


def values(mode, s):
    if mode == "a":
        db, t = TABLE[mode].format(s=s).split(".")
        sw = " OR ".join(f"startsWith(token, '{k}=')" for k in RES_KEYS)
        return (f"SELECT * FROM (SELECT 'ResourceAttributes' as column, substring(token, 1, position(token, '=') - 1) AS key, "
                f"groupUniqArray(20)(substring(token, position(token, '=') + 1)) AS value FROM mergeTreeTextIndex('{db}', '{t}', 'idx_res_attr_items') "
                f"WHERE {parts_filter(db, t)} AND ({sw}) AND substring(token, position(token, '=') + 1) != '' GROUP BY column, key)")
    kl = ",".join(f"'{k}'" for k in RES_KEYS)
    return (f"SELECT * FROM (SELECT ColumnIdentifier, Key, groupUniqArray(20)(Value) as Values FROM {ROLLUP[mode].format(s=s)} "
            f"WHERE ((ColumnIdentifier = 'ResourceAttributes' AND Key IN ({kl}))) AND Timestamp >= toStartOfFifteenMinutes({ms(T0)}) "
            f"AND Timestamp <= toStartOfFifteenMinutes({ms(T1)}) AND notEmpty(Value) GROUP BY ColumnIdentifier, Key ORDER BY ColumnIdentifier, Key)")


def values_scan(mode, s):
    """HyperDX's fallback when no rollup serves a column: sample the table (DEFAULT_METADATA_MAX_ROWS_TO_READ 3e6)."""
    sel = ", ".join(f"`ResourceAttributes`['{k}'] as param{i}" for i, k in enumerate(RES_KEYS))
    agg = ", ".join(f"groupUniqArray(20)(param{i}) AS param{i}" for i in range(len(RES_KEYS)))
    return f"WITH sampledData AS (SELECT {sel} FROM {TABLE[mode].format(s=s)} WHERE {trange(mode, s)} LIMIT 3000000) SELECT {agg} FROM sampledData"


def row_detail(mode):
    t = TABLE[mode].format(s="logs")
    return (f"SELECT *,Timestamp AS \"__hdx_timestamp\",Body AS \"__hdx_body\",ServiceName AS \"__hdx_service_name\","
            f"ResourceAttributes AS \"__hdx_resource_attributes\",LogAttributes AS \"__hdx_event_attributes\" FROM {t} "
            f"WHERE (Timestamp=parseDateTime64BestEffort('{ROW_TS}', 9) AND ServiceName='{ROW_SVC}' AND SeverityText='ERROR') LIMIT 1")


def waterfall(mode):
    return [f"SELECT SpanName AS \"Body\",Timestamp AS \"Timestamp\",SpanId AS \"SpanId\",ServiceName AS \"ServiceName\",(Duration) / 1e9 AS \"Duration\","
            f"ParentSpanId AS \"ParentSpanId\",StatusCode AS \"StatusCode\",SpanAttributes AS \"SpanAttributes\",Events AS \"SpanEvents\" "
            f"FROM {TABLE[mode].format(s='traces')} WHERE {trange(mode, 'traces')} AND (TraceId = '{TRACE}') LIMIT 100000",
            f"SELECT Timestamp AS \"Timestamp\",`ResourceAttributes`['service.name'] AS \"serviceName\",ParentSpanId AS \"parentSpanId\" "
            f"FROM {TABLE[mode].format(s='traces')} WHERE {trange(mode, 'traces')} AND (TraceId = '{TRACE}') LIMIT 100000",
            f"SELECT Body AS \"Body\",Timestamp AS \"Timestamp\",SpanId AS \"SpanId\",ServiceName AS \"ServiceName\",SeverityText AS \"SeverityText\" "
            f"FROM {TABLE[mode].format(s='logs')} WHERE {trange(mode, 'logs')} AND (TraceId = '{TRACE}') LIMIT 100000"]


def scenarios(mode):
    err = "(SeverityText ILIKE '%ERROR%')"
    pod = res_eq(mode, "k8s.pod.name", POD)
    ns_l = res_eq(mode, "k8s.namespace.name", NS)
    java = res_ilike(mode, "telemetry.sdk.language", "java", residual=True)
    svc = f"(ServiceName = '{SVC}')"
    return {
        "logs-all": [hist(mode, "logs", ""), logs_list(mode, "")],
        "logs-res-attr (pod = , + ERROR)": [hist(mode, "logs", f"({pod} AND {err})"), logs_list(mode, f"({pod} AND {err})", lo=T0)],
        "logs-namespace (ns =)": [hist(mode, "logs", ns_l), logs_list(mode, ns_l)],
        "logs-service (ServiceName =)": [hist(mode, "logs", svc), logs_list(mode, svc)],
        "logs-fulltext": [hist(mode, "logs", fulltext(mode)), logs_list(mode, fulltext(mode), lo=T0)],
        "logs-group-by-namespace": [f"SELECT {res_col(mode, 'logs', 'k8s.namespace.name')} AS g, count() FROM {TABLE[mode].format(s='logs')} "
                                    f"WHERE {trange(mode, 'logs')} GROUP BY g ORDER BY count() DESC LIMIT 20"],
        "logs-row-detail": [row_detail(mode)],
        "traces-all": [hist(mode, "traces", "", by="StatusCode"), traces_list(mode, "")],
        "traces-res-attr (pod =)": [hist(mode, "traces", pod, by="StatusCode"), traces_list(mode, pod, lo=T0)],
        "traces-residual (sdk ILIKE java, db.system)": [
            hist(mode, "traces", f"({java} AND (`SpanAttributes`['db.system'] ILIKE '%postgres%' AND indexHint(mapContains(`SpanAttributes`, 'db.system'))))", by="StatusCode"),
            traces_list(mode, f"({java} AND (`SpanAttributes`['db.system'] ILIKE '%postgres%' AND indexHint(mapContains(`SpanAttributes`, 'db.system'))))")],
        "traces-group-by-deployment (p95)": [f"SELECT {res_col(mode, 'traces', 'k8s.deployment.name')} AS g, quantile(0.95)(Duration) FROM {TABLE[mode].format(s='traces')} "
                                             f"WHERE {trange(mode, 'traces')} AND StatusCode = 'Error' GROUP BY g ORDER BY 2 DESC LIMIT 20"],
        "trace-waterfall (by TraceId)": waterfall(mode),
        "discovery: resource keys": [keys(mode, "logs"), keys(mode, "traces")],
        "discovery: resource values (10 keys)": [values(mode, "logs"), values(mode, "traces")],
        **({"discovery: values by scanning (no rollup)": [values_scan(mode, "logs")]} if mode in ("view", "c") else {}),
    }


def run(sql):
    c.q(sql, fmt="Null", use_query_condition_cache=0, max_execution_time=120)
    s = c.q.last_summary
    return int(s.get("elapsed_ns", 0)) / 1e6, int(s.get("read_rows", 0)), int(s.get("read_bytes", 0))


def main():
    rec = dict(ts=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), load_before=c.load(), values=dict(pod=POD, ns=NS, svc=SVC, trace=TRACE))
    per = {m: scenarios(m) for m in MODES}
    names = [n for n in per[MODES[0]] if not a.only or a.only in n]
    for n in per["view"] if "view" in per else []:
        if n not in names and (not a.only or a.only in n):
            names.append(n)
    res = {}
    for n in names:
        for m in MODES:
            for i, sql in enumerate(per[m].get(n, [])):
                try:
                    run(sql)  # warm-up
                except RuntimeError as e:
                    res.setdefault(n, {}).setdefault(m, []).append(dict(error=str(e)[:300]))
        for r in range(a.rounds):
            order = MODES[r % len(MODES):] + MODES[:r % len(MODES)]
            for m in order:
                for i, sql in enumerate(per[m].get(n, [])):
                    lst = res.setdefault(n, {}).setdefault(m, [])
                    while len(lst) <= i:
                        lst.append(dict(ms=[], rows=0, bytes=0, sql=sql[:4000]))
                    if "error" in lst[i]:
                        continue
                    t, rr, rb = run(sql)
                    lst[i]["ms"].append(t)
                    lst[i]["rows"], lst[i]["bytes"] = rr, rb
        line = [n]
        for m in MODES:
            st = res[n].get(m, [])
            if not st:
                line.append(f"{m}: -")
                continue
            if any("error" in x for x in st):
                line.append(f"{m}: ERROR")
                continue
            tot = sum(statistics.median(x["ms"]) for x in st)
            rows = sum(x["rows"] for x in st)
            line.append(f"{m}: {tot:.1f} ms {rows:,} rows")
        print(" | ".join(line), flush=True)
    rec["results"] = {n: {m: [dict(ms=round(statistics.median(x["ms"]), 2) if x.get("ms") else None, ms_all=x.get("ms"), rows=x.get("rows"),
                                   bytes=x.get("bytes"), error=x.get("error"), sql=x.get("sql")) for x in st] for m, st in d.items()} for n, d in res.items()}
    rec["load_after"] = c.load()
    with open(a.out, "a") as f:
        f.write(json.dumps(rec) + "\n")


if __name__ == "__main__":
    main()
