#!/usr/bin/env python3
"""Dashboards and alerts for the HyperDX evaluation, through HyperDX's API:

  * a generic webhook to http://127.0.0.1:18890/hook (webhook_sink.py; HyperDX
    must run with WEBHOOK_HOSTNAME_ALLOWLIST=127.0.0.1);
  * the same six-tile dashboard twice, on the layout-B views and on the stock
    tables (gauge avg by service, counter increase by route, histogram p95 by
    route, exponential histogram p50, log count by severity, span p95);
  * alerts, each on both metric sources: a tile alert on the gauge tile and on
    the counter tile (1m interval, thresholds that fire), and a saved-search
    alert on ERROR logs.

  hdx_dash.py [--api http://localhost:18800] (reads hdx_ids.json; writes hdx_dash.json)
"""
import argparse, json, sys
import requests

p = argparse.ArgumentParser()
p.add_argument("--api", default="http://localhost:18800")
p.add_argument("--email", default="eval@example.com")
p.add_argument("--password", default="Hdx-eval-2026!")
p.add_argument("--hook", default="http://127.0.0.1:18890/hook")
a = p.parse_args()
ids = json.load(open("hdx_ids.json"))
s = requests.Session()
r = s.post(a.api + "/login/password", json={"email": a.email, "password": a.password}, allow_redirects=False)
s.headers["Cookie"] = "connect.sid=" + r.headers["Set-Cookie"].split("connect.sid=")[1].split(";")[0]


def call(method, path, **kw):
    r = s.request(method, a.api + path, **kw)
    if r.status_code >= 400:
        sys.exit(f"{method} {path}: {r.status_code} {r.text[:800]}")
    return r.json() if r.text else None


hooks = call("GET", "/webhooks", params={"service": "generic"})
hooks = hooks.get("data", hooks) if isinstance(hooks, dict) else hooks
hook = next((h for h in hooks if h.get("url") == a.hook), None)
if not hook:
    out = call("POST", "/webhooks", json={"name": "sink", "service": "generic", "url": a.hook,
                                          "body": '{"title":"{{title}}","body":"{{body}}","link":"{{link}}"}'})
    hook = out.get("data", out)
hook_id = hook.get("_id") or hook.get("id")


def sel(aggFn, metricName, metricType, **kw):
    return {"aggFn": aggFn, "metricName": metricName, "metricType": metricType, "valueExpression": "Value",
            "aggCondition": "", "aggConditionLanguage": "lucene", **kw}


def tile(tid, x, y, name, source, select, groupBy="", where="", display="line"):
    return {"id": tid, "x": x, "y": y, "w": 12, "h": 10, "config": {
        "name": name, "source": source, "displayType": display, "select": select, "groupBy": groupBy,
        "where": where, "whereLanguage": "lucene", "granularity": "auto"}}


def dashboard(name, msrc):
    return {"name": name, "tags": ["hdx-eval"], "tiles": [
        tile("cpu", 0, 0, "CPU by service", msrc, [sel("avg", "container.cpu.utilization", "gauge")], "ServiceName"),
        tile("reqs", 12, 0, "Requests (increase) by route", msrc, [sel("increase", "http.server.request.count", "sum")], "Attributes['http.route']"),
        tile("p95", 0, 10, "HTTP p95 by route", msrc, [sel("quantile", "http.server.request.duration", "histogram", level=0.95)], "Attributes['http.route']"),
        tile("rpc", 12, 10, "RPC p50 (exp. histogram)", msrc, [sel("quantile", "rpc.server.duration", "exponential histogram", level=0.5)], "ServiceName"),
        tile("logs", 0, 20, "Logs by severity", ids["Logs"], [{"aggFn": "count", "valueExpression": "", "aggCondition": "", "aggConditionLanguage": "lucene"}], "SeverityText"),
        tile("spans", 12, 20, "Span p95 by service", ids["Traces"], [{"aggFn": "quantile", "level": 0.95, "valueExpression": "Duration", "aggCondition": "", "aggConditionLanguage": "lucene"}], "ServiceName"),
    ]}


existing = {d["name"]: d for d in call("GET", "/dashboards")}
out = {"webhook": hook_id}
for key, name, msrc in (("b", "hdx-eval: layout B views", ids["Metrics (layout B views)"]),
                        ("stock", "hdx-eval: stock tables", ids["Metrics (stock)"])):
    if name in existing:
        did = existing[name]["id"]
    else:
        d = call("POST", "/dashboards", json=dashboard(name, msrc))
        did = d.get("id") or d.get("_id")
    out[f"dashboard_{key}"] = did
    for tid, thr, ttype in (("cpu", 0.1, "above"), ("reqs", 1, "above")):
        body = {"source": "tile", "dashboardId": did, "tileId": tid, "interval": "1m", "threshold": thr,
                "thresholdType": ttype, "channel": {"type": "webhook", "webhookId": hook_id},
                "name": f"{key} {tid}"}
        existing_alerts = call("GET", "/alerts").get("data", [])
        if not any(al.get("name") == body["name"] for al in existing_alerts):
            out[f"alert_{key}_{tid}"] = call("POST", "/alerts", json=body)["data"].get("_id")
# a saved-search alert on ERROR logs
ss = [x for x in call("GET", "/saved-search") if x.get("name") == "hdx-eval errors"]
if not ss:
    ss = [call("POST", "/saved-search", json={"name": "hdx-eval errors", "select": "", "where": "SeverityText:ERROR",
                                              "whereLanguage": "lucene", "source": ids["Logs"], "tags": []})]
out["saved_search"] = ss[0].get("_id") or ss[0].get("id")
if not any(al.get("name") == "logs errors" for al in call("GET", "/alerts").get("data", [])):
    out["alert_logs"] = call("POST", "/alerts", json={
        "source": "saved_search", "savedSearchId": out["saved_search"], "interval": "1m", "threshold": 0,
        "thresholdType": "above", "channel": {"type": "webhook", "webhookId": hook_id}, "name": "logs errors"})["data"].get("_id")
print(json.dumps(out, indent=1))
json.dump(out, open("hdx_dash.json", "w"), indent=1)
