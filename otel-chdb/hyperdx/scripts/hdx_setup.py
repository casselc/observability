#!/usr/bin/env python3
"""Configure a fresh HyperDX (API on --api, default http://localhost:18800: the session cookie is set for Domain=localhost):
register the first user, add the ClickHouse connection, and the sources:

  Logs, Traces                 hdx_b.otel_logs / hdx_b.otel_traces (the consumer's tables)
  Metrics (layout B views)     hdx_b.otel_metrics_*  (views over the series table, D7)
  Metrics (stock)              hdx_stock.otel_metrics_* (ClickStack's own tables, same rows)

With --schema old=DB,full=DB,new=DB (the schema comparison, ../README.md §Schema; any subset,
or OLD_DB,NEW_DB), instead of the above, a Logs and a Traces source per side:
  old   the consumer's DDL before the ClickStack alignment
  full  ClickStack 2.39.1's full DDL (../sql/clickstack_full_*.sql)
  new   the consumer's (../../otap-rs/sql/otel_*.sql: option 2, without the attr-key indexes)
full and new declare their key-value rollups as the sources' metadataMaterializedViews (what
HyperDX's source form auto-detects from <table>_kv_rollup_15m). The ids go to
hdx_ids_<side>.json (keys Logs, Traces, for hdx_ui.js).

Idempotent: logs in if the team exists, and only adds what is missing.
Prints the ids as JSON (also written to hdx_ids.json next to this script's cwd).

  hdx_setup.py [--email E --password P] [--ch http://127.0.0.1:18124]
"""
import argparse, json, sys
import requests

p = argparse.ArgumentParser()
p.add_argument("--api", default="http://localhost:18800")
p.add_argument("--email", default="eval@example.com")
p.add_argument("--password", default="Hdx-eval-2026!")
p.add_argument("--ch", default="http://127.0.0.1:18124", help="ClickHouse as HyperDX sees it: chproxy.py in front of :18123")
p.add_argument("--b", default="hdx_b")
p.add_argument("--stock", default="hdx_stock")
p.add_argument("--schema", default="", help="old=DB,full=DB,new=DB: the schema comparison's sources only")
a = p.parse_args()

s = requests.Session()


def call(method, path, **kw):
    r = s.request(method, a.api + path, **kw)
    if r.status_code >= 400:
        sys.exit(f"{method} {path}: {r.status_code} {r.text[:500]}")
    return r.json() if r.text and r.headers.get("content-type", "").startswith("application/json") else r.text


if not call("GET", "/installation")["isTeamExisting"]:
    call("POST", "/register/password", json={"email": a.email, "password": a.password, "confirmPassword": a.password})
r = s.post(a.api + "/login/password", json={"email": a.email, "password": a.password}, allow_redirects=False)
if r.status_code >= 400:
    sys.exit(f"login: {r.status_code} {r.text[:300]}")
# requests' cookie jar drops a cookie set for Domain=localhost: carry it by hand
s.headers["Cookie"] = "connect.sid=" + r.headers["Set-Cookie"].split("connect.sid=")[1].split(";")[0]
me = call("GET", "/me")
team = call("GET", "/team")

conns = call("GET", "/connections")
conn = next((c for c in conns if c["name"] == "central"), None)
if not conn:
    cid = call("POST", "/connections", json={"name": "central", "host": a.ch, "username": "default", "password": ""})["id"]
else:
    cid = conn["id"]
    if conn["host"] != a.ch:
        call("PUT", f"/connections/{cid}", json={**conn, "host": a.ch, "password": ""})

sources = {x["name"]: x for x in call("GET", "/sources")}


def ensure(src):
    if src["name"] in sources:
        return sources[src["name"]]["id"]
    out = call("POST", "/sources", json=src)
    return out.get("id") or out.get("_id")


ids = {"connection": cid, "team": team.get("_id") or team.get("id"), "accessKey": me.get("accessKey")}


def logs_src(name, db, extra=None):
    return {
        "kind": "log", "name": name, "connection": cid,
        "from": {"databaseName": db, "tableName": "otel_logs"},
        "timestampValueExpression": "Timestamp", "displayedTimestampValueExpression": "Timestamp",
        "defaultTableSelectExpression": "Timestamp,ServiceName,SeverityText,Body",
        "serviceNameExpression": "ServiceName", "severityTextExpression": "SeverityText", "bodyExpression": "Body",
        "eventAttributesExpression": "LogAttributes", "resourceAttributesExpression": "ResourceAttributes",
        "traceIdExpression": "TraceId", "spanIdExpression": "SpanId", "implicitColumnExpression": "Body",
        **(extra or {}),
    }


def traces_src(name, db, logs_id, extra=None):
    return {
        "kind": "trace", "name": name, "connection": cid,
        "from": {"databaseName": db, "tableName": "otel_traces"},
        "timestampValueExpression": "Timestamp", "displayedTimestampValueExpression": "Timestamp",
        "defaultTableSelectExpression": "Timestamp,ServiceName,StatusCode,round(Duration/1e6),SpanName",
        "durationExpression": "Duration", "durationPrecision": 9, "traceIdExpression": "TraceId",
        "spanIdExpression": "SpanId", "parentSpanIdExpression": "ParentSpanId", "spanNameExpression": "SpanName",
        "spanKindExpression": "SpanKind", "statusCodeExpression": "StatusCode", "statusMessageExpression": "StatusMessage",
        "serviceNameExpression": "ServiceName", "resourceAttributesExpression": "ResourceAttributes",
        "eventAttributesExpression": "SpanAttributes", "spanEventsValueExpression": "Events",
        "implicitColumnExpression": "SpanName", "logSourceId": logs_id,
        **(extra or {}),
    }


if a.schema:
    pairs = [x.split("=") for x in a.schema.split(",")] if "=" in a.schema else list(zip(("old", "new"), a.schema.split(",")))
    labels = {"old": "pre-alignment", "full": "ClickStack full DDL", "new": "consumer DDL, option 2"}
    for side, db in pairs:
        label, mv = labels[side], side != "old"
        rollup = lambda t: {"metadataMaterializedViews": {"kvRollupTable": f"{t}_kv_rollup_15m", "granularity": "15 minute"}} if mv else {}
        side_ids = {"connection": cid}
        side_ids["Logs"] = ensure(logs_src(f"Logs ({label})", db, rollup("otel_logs")))
        side_ids["Traces"] = ensure(traces_src(f"Traces ({label})", db, side_ids["Logs"], rollup("otel_traces")))
        cur = next(x for x in call("GET", "/sources") if x["id"] == side_ids["Logs"])
        cur["traceSourceId"] = side_ids["Traces"]
        call("PUT", f"/sources/{side_ids['Logs']}", json=cur)
        json.dump(side_ids, open(f"hdx_ids_{side}.json", "w"), indent=1)
        ids[side] = side_ids
    print(json.dumps(ids, indent=1))
    sys.exit(0)

ids["Logs"] = ensure(logs_src("Logs", a.b))
ids["Traces"] = ensure(traces_src("Traces", a.b, ids["Logs"]))
tables = {"gauge": "otel_metrics_gauge", "sum": "otel_metrics_sum", "histogram": "otel_metrics_histogram",
          "exponential histogram": "otel_metrics_exponential_histogram", "summary": "otel_metrics_summary"}
for name, db in (("Metrics (layout B views)", a.b), ("Metrics (stock)", a.stock)):
    ids[name] = ensure({
        "kind": "metric", "name": name, "connection": cid,
        "from": {"databaseName": db, "tableName": ""}, "timestampValueExpression": "TimeUnix",
        "resourceAttributesExpression": "ResourceAttributes", "serviceNameExpression": "ServiceName",
        "metricTables": tables, "logSourceId": ids["Logs"],
    })
# link logs/traces to the views' metric source (the one under test)
for n, extra in (("Logs", {"traceSourceId": ids["Traces"], "metricSourceId": ids["Metrics (layout B views)"]}),
                 ("Traces", {"metricSourceId": ids["Metrics (layout B views)"]})):
    cur = next(x for x in call("GET", "/sources") if x["id"] == ids[n])
    cur.update(extra)
    call("PUT", f"/sources/{ids[n]}", json=cur)
print(json.dumps(ids, indent=1))
json.dump(ids, open("hdx_ids.json", "w"), indent=1)
