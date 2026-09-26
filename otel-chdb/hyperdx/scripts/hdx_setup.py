#!/usr/bin/env python3
"""Configure a fresh HyperDX (API on --api, default http://localhost:18800: the session cookie is set for Domain=localhost):
register the first user, add the ClickHouse connection, and the sources:

  Logs, Traces                 hdx_b.otel_logs / hdx_b.otel_traces (the consumer's tables)
  Metrics (layout B views)     hdx_b.otel_metrics_*  (views over the series table, D7)
  Metrics (stock)              hdx_stock.otel_metrics_* (ClickStack's own tables, same rows)

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
ids["Logs"] = ensure({
    "kind": "log", "name": "Logs", "connection": cid,
    "from": {"databaseName": a.b, "tableName": "otel_logs"},
    "timestampValueExpression": "Timestamp", "displayedTimestampValueExpression": "Timestamp",
    "defaultTableSelectExpression": "Timestamp,ServiceName,SeverityText,Body",
    "serviceNameExpression": "ServiceName", "severityTextExpression": "SeverityText", "bodyExpression": "Body",
    "eventAttributesExpression": "LogAttributes", "resourceAttributesExpression": "ResourceAttributes",
    "traceIdExpression": "TraceId", "spanIdExpression": "SpanId", "implicitColumnExpression": "Body",
})
ids["Traces"] = ensure({
    "kind": "trace", "name": "Traces", "connection": cid,
    "from": {"databaseName": a.b, "tableName": "otel_traces"},
    "timestampValueExpression": "Timestamp", "displayedTimestampValueExpression": "Timestamp",
    "defaultTableSelectExpression": "Timestamp,ServiceName,StatusCode,round(Duration/1e6),SpanName",
    "durationExpression": "Duration", "durationPrecision": 9, "traceIdExpression": "TraceId",
    "spanIdExpression": "SpanId", "parentSpanIdExpression": "ParentSpanId", "spanNameExpression": "SpanName",
    "spanKindExpression": "SpanKind", "statusCodeExpression": "StatusCode", "statusMessageExpression": "StatusMessage",
    "serviceNameExpression": "ServiceName", "resourceAttributesExpression": "ResourceAttributes",
    "eventAttributesExpression": "SpanAttributes", "spanEventsValueExpression": "Events",
    "implicitColumnExpression": "SpanName", "logSourceId": ids["Logs"],
})
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
