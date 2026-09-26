#!/usr/bin/env python3
"""Run queries.promql against a Prometheus-compatible API and save rates.json.

    rates/collect.py --prom http://prometheus.monitoring:9090 [--selector 'cluster="prod-1"'] \
        [--bearer TOKEN | --header 'X-Scope-OrgID: tenant'] [--ca ca.pem] [--heavy] [--out rates.json]

    # through kubectl, without exposing Prometheus:
    kubectl -n monitoring port-forward svc/prometheus-operated 9090 &
    rates/collect.py --prom http://127.0.0.1:9090

For each block it tries the alternatives in order and keeps the first that
returns data. Every block is evaluated three ways:
  now     the query at the current instant (rates over --window, default 1h)
  avg     rates over the whole --range (default 1d); gauges averaged over it
  peak    the maximum over --range of the query at a 5m window
so the calculator can be fed the average (default) or the peak.
Standard library only.
"""
import argparse
import json
import os
import re
import ssl
import sys
import time
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))


def parse_blocks(path):
    blocks, cur = [], None
    for raw in open(path, encoding="utf-8"):
        line = raw.rstrip("\n")
        m = re.match(r"#\s*(name|help|kind|heavy):\s*(.*)$", line)
        if m:
            k, v = m.group(1), m.group(2).strip()
            if k == "name":
                cur = {"name": v, "help": "", "kind": "gauge", "heavy": False, "queries": []}
                blocks.append(cur)
            elif cur is not None:
                cur[k] = (v == "yes") if k == "heavy" else v
            continue
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if cur is not None:
            cur["queries"].append(line.strip())
    return blocks


def substitute(q, window, sel):
    inner = sel.strip().lstrip(",")
    q = q.replace("{{SEL}}", "{" + inner + "}")
    q = q.replace("{SEL}", ("," + inner) if inner else "")
    return q.replace("{W}", window)


class Prom:
    def __init__(self, url, headers, ca, timeout):
        self.url = url.rstrip("/")
        self.headers = headers
        self.ctx = ssl.create_default_context(cafile=ca) if ca else None
        self.timeout = timeout
        self.calls = 0

    def query(self, q):
        self.calls += 1
        data = urllib.parse.urlencode({"query": q}).encode()
        req = urllib.request.Request(self.url + "/api/v1/query", data=data, headers=self.headers)
        req.add_header("Content-Type", "application/x-www-form-urlencoded")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout, context=self.ctx) as r:
                body = json.load(r)
        except urllib.error.HTTPError as e:
            try:
                body = json.load(e)
            except Exception:
                return None, f"HTTP {e.code}"
        except Exception as e:  # network, timeout
            return None, str(e)
        if body.get("status") != "success":
            return None, body.get("error", "error")
        res = body["data"]["result"]
        rtype = body["data"]["resultType"]
        if rtype == "scalar":
            return [{"metric": {}, "value": float(res[1])}], None
        out = []
        for s in res:
            try:
                v = float(s["value"][1])
            except (KeyError, ValueError):
                continue
            if v != v:  # NaN
                continue
            out.append({"metric": s.get("metric", {}), "value": v})
        return out, None


def wrap(q, kind, variant, window, rng, sel):
    if variant == "now":
        return substitute(q, window, sel)
    if variant == "avg":
        if kind == "rate":
            return substitute(q, rng, sel)
        return f"avg_over_time(({substitute(q, window, sel)})[{rng}:15m])"
    # peak
    return f"max_over_time(({substitute(q, '5m', sel)})[{rng}:5m])"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--prom", default=os.environ.get("PROM_URL", ""), help="Prometheus / Thanos / Mimir base URL (PROM_URL)")
    ap.add_argument("--queries", default=os.path.join(HERE, "queries.promql"))
    ap.add_argument("--selector", default="", help='extra label matcher, e.g. cluster="prod-1"')
    ap.add_argument("--window", default="1h", help="rate window for 'now'")
    ap.add_argument("--range", default="1d", help="range for 'avg' and 'peak'")
    ap.add_argument("--bearer", default=os.environ.get("PROM_TOKEN", ""), help="bearer token (PROM_TOKEN)")
    ap.add_argument("--header", action="append", default=[], help="extra header 'Name: value' (repeatable)")
    ap.add_argument("--ca", default="", help="CA bundle for https")
    ap.add_argument("--heavy", action="store_true", help="also run blocks marked heavy (count by pod over all series)")
    ap.add_argument("--no-peak", action="store_true", help="skip the peak (subquery) variant")
    ap.add_argument("--timeout", type=float, default=120)
    ap.add_argument("--out", default="rates.json")
    a = ap.parse_args()
    if not a.prom:
        ap.error("--prom (or PROM_URL) is required")
    headers = {}
    if a.bearer:
        headers["Authorization"] = "Bearer " + a.bearer
    for h in a.header:
        k, _, v = h.partition(":")
        headers[k.strip()] = v.strip()
    prom = Prom(a.prom, headers, a.ca or None, a.timeout)
    out = {"prom": a.prom, "selector": a.selector, "window": a.window, "range": a.range,
           "collected_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "blocks": {}}
    variants = ["now", "avg"] + ([] if a.no_peak else ["peak"])
    for b in parse_blocks(a.queries):
        if b["heavy"] and not a.heavy:
            out["blocks"][b["name"]] = {"help": b["help"], "kind": b["kind"], "skipped": "heavy (use --heavy)"}
            continue
        rec = {"help": b["help"], "kind": b["kind"], "tried": []}
        chosen = None
        for i, q in enumerate(b["queries"]):
            res, err = prom.query(wrap(q, b["kind"], "now", a.window, a.range, a.selector))
            rec["tried"].append({"alt": i, "query": substitute(q, a.window, a.selector), "error": err,
                                 "series": None if res is None else len(res)})
            if res:
                chosen = (i, q, res)
                break
        if chosen is None:
            rec["value"] = None
            out["blocks"][b["name"]] = rec
            print(f"  {b['name']:24} no data", file=sys.stderr)
            continue
        i, q, res = chosen
        rec["alt"] = i
        rec["query"] = substitute(q, a.window, a.selector)
        vector = len(res) > 1 or any(res[0]["metric"])
        for v in variants:
            r = res if v == "now" else prom.query(wrap(q, b["kind"], v, a.window, a.range, a.selector))[0]
            if r is None:
                continue
            if vector:
                rec[v] = [{"labels": s["metric"], "value": s["value"]} for s in r]
            else:
                rec[v] = r[0]["value"] if r else None
        rec["value"] = rec.get("now")
        out["blocks"][b["name"]] = rec
        shown = rec["now"] if not vector else f"{len(res)} series"
        print(f"  {b['name']:24} {shown}  (alt {i})", file=sys.stderr)
    out["queries_sent"] = prom.calls
    with open(a.out, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)
    print(f"wrote {a.out} ({prom.calls} queries)", file=sys.stderr)


if __name__ == "__main__":
    main()
