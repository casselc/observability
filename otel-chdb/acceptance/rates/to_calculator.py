#!/usr/bin/env python3
"""Turn rates.json (collect.py) and/or sample.json (kubectl_sample.py) into the
sizing calculator's telemetry-rate inputs, and print them.

    rates/to_calculator.py rates.json [--sample sample.json] [--basis avg|peak|now]
                           [--spans-source tempo|jaeger|otelcol] [--interval 30] [--json]

Calculator inputs (central-sizing.html, "Telemetry rate" and "Fleet"):
  spansPod    spans/s per pod, after sampling
  logsPod     log lines/s per pod (container logs)
  logsNode    log lines/s per node (kubelet, runtime, journald)
  seriesPod   active series per pod (app metrics + cAdvisor)
  seriesNode  active series per node (node-exporter, kubelet, kube-state-metrics, control plane)
  interval    export (scrape) interval, s
  clusters, pods, nodes (pods and nodes are PER CLUSTER in the calculator)

It prints a table (value, how it was derived, the alternatives it saw), the
values as JSON, and a one-line snippet to paste into the browser console on
the calculator page, which stores them the way the page itself does
(localStorage "central-sizing-v9") and reloads.
"""
import argparse
import json
import sys

KEY = "central-sizing-v9"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("rates", nargs="?", help="rates.json from collect.py")
    ap.add_argument("--sample", help="sample.json from kubectl_sample.py (used where rates.json has nothing)")
    ap.add_argument("--basis", choices=["avg", "peak", "now"], default="avg",
                    help="avg over the range (default; the calculator adds its own headroom), the peak, or now")
    ap.add_argument("--spans-source", choices=["tempo", "jaeger", "otelcol"], default=None,
                    help="force the span source (default: tempo, then jaeger, then otelcol receivers)")
    ap.add_argument("--interval", type=float, default=None, help="override the export interval (s)")
    ap.add_argument("--json", action="store_true", help="print only the JSON")
    a = ap.parse_args()
    if not a.rates and not a.sample:
        ap.error("give rates.json and/or --sample sample.json")

    blocks = json.load(open(a.rates))["blocks"] if a.rates else {}
    sample = json.load(open(a.sample)) if a.sample else {}
    notes, derived = [], {}

    def val(name, basis=None):
        rec = blocks.get(name) or {}
        for b in (basis or a.basis, "now"):
            v = rec.get(b)
            if isinstance(v, (int, float)):
                return float(v)
        return None

    def first(*names):
        for n in names:
            v = val(n)
            if v is not None:
                return n, v
        return None, None

    # Fleet.
    clusters = val("clusters", "now") or 1.0
    # The fleet is today's (now); only the rates follow --basis, so a peak is
    # peak telemetry per current pod, not a ratio of two peaks.
    pods = val("pods", "now") or sample.get("pods_total")
    nodes = val("nodes", "now") or sample.get("nodes_total")
    if not pods or not nodes:
        sys.exit("need pod and node counts (rates.json blocks pods/nodes, or sample.json)")
    derived["clusters"] = (clusters, "count by (cluster)" if val("clusters", "now") else "1 (no cluster label)")
    derived["pods"] = (pods / clusters, f"{pods:.0f} running pods / {clusters:.0f} clusters")
    derived["nodes"] = (nodes / clusters, f"{nodes:.0f} nodes / {clusters:.0f} clusters")

    # Interval.
    iv = a.interval or val("scrape_interval_s", "now")
    derived["interval"] = (iv or 30.0, "--interval" if a.interval else ("median scrape interval" if iv else "default 30 (no data)"))

    # Series.
    parts = {n: val(n) for n in ("series_cadvisor", "series_node_exporter", "series_kubelet", "series_ksm", "series_control_plane")}
    total_name, total = first("series_scraped", "head_series")
    if total is not None:
        known = sum(v for v in parts.values() if v)
        app = max(0.0, total - known)
        cad = parts["series_cadvisor"] or 0.0
        node_side = sum(parts[n] or 0.0 for n in ("series_node_exporter", "series_kubelet", "series_ksm", "series_control_plane"))
        derived["seriesPod"] = ((app + cad) / pods, f"({total_name} {total:.0f} − infra {known:.0f} = app {app:.0f}, + cAdvisor {cad:.0f}) / {pods:.0f} pods")
        derived["seriesNode"] = (node_side / nodes, f"(node-exporter + kubelet + kube-state-metrics + control plane = {node_side:.0f}) / {nodes:.0f} nodes")
        missing = [n for n, v in parts.items() if v is None]
        if missing:
            notes.append(f"no data for {', '.join(missing)}: those series are counted as app series (per pod)")
        if total_name == "head_series":
            notes.append("series from prometheus_tsdb_head_series: an HA pair counts every series twice; halve if so")
        p50 = val("series_per_pod_p50", "now")
        if p50:
            notes.append(f"direct count by pod (heavy): median {p50:.0f}, p90 {val('series_per_pod_p90', 'now') or 0:.0f} series per pod")
    elif val("otel_metric_points") is not None:
        pts = val("otel_metric_points")
        s = pts * derived["interval"][0]
        derived["seriesPod"] = (s / pods, f"OTel metric points {pts:.0f}/s × interval {derived['interval'][0]:.0f} s / {pods:.0f} pods (all attributed to pods)")
        derived["seriesNode"] = (0.0, "unknown from OTel point rates; set by hand (node-exporter ≈ 500-1,500 per node)")
    elif sample:
        cad = sample.get("cadvisor_series_per_pod") or 0.0
        app = sample.get("app_series_per_pod") or 0.0
        derived["seriesPod"] = (cad + app, f"kubectl sample: cAdvisor {cad:.0f} + app {app:.0f} per pod")
        kub = sample.get("kubelet_series_per_node")
        derived["seriesNode"] = ((kub or 0.0), f"kubectl sample: kubelet {kub or 0:.0f} per node; node-exporter and kube-state-metrics NOT included")
        notes.append("series per node from the kubectl sample misses node-exporter and kube-state-metrics: add them by hand")

    # Logs.
    src = None
    for s in ("fluentbit", "vector", "promtail", "otelcol"):
        pod_v, node_v = val(f"logs_{s}_pod"), val(f"logs_{s}_node")
        if pod_v is not None:
            src = s
            derived["logsPod"] = (pod_v / pods, f"{s} container-log inputs {pod_v:.1f} lines/s / {pods:.0f} pods")
            if node_v is not None:
                derived["logsNode"] = (node_v / nodes, f"{s} journal inputs {node_v:.1f} lines/s / {nodes:.0f} nodes")
            else:
                notes.append(f"{s}: no node (journal) log input found; logsNode left at the calculator's value")
            break
    if src is None and val("logs_loki") is not None:
        v = val("logs_loki")
        derived["logsPod"] = (v / pods, f"Loki lines {v:.1f}/s / {pods:.0f} pods (includes node logs)")
        notes.append("Loki's total is not split by source: node logs are inside logsPod; set logsNode to 0 or split by hand")
    elif src is None and sample.get("log_lines_per_pod_per_s", {}).get("mean") is not None:
        m = sample["log_lines_per_pod_per_s"]
        derived["logsPod"] = (m["mean"], f"kubectl logs sample of {m['n']} pods: mean (p50 {m['p50']:.2f}, p90 {m['p90']:.2f})")
    bpl = None
    if val("logs_fluentbit_bytes") and val("logs_fluentbit_pod"):
        bpl = val("logs_fluentbit_bytes") / val("logs_fluentbit_pod")
    elif val("logs_loki_bytes") and val("logs_loki"):
        bpl = val("logs_loki_bytes") / val("logs_loki")
    elif sample.get("log_bytes_per_line"):
        bpl = sample["log_bytes_per_line"]
    if bpl:
        notes.append(f"raw log line ≈ {bpl:.0f} bytes (the calculator's stored bytes/log is after compression: keep its default unless you measure central)")

    # Spans.
    order = {"tempo": ["spans_tempo"], "jaeger": ["spans_jaeger"], "otelcol": ["spans_otelcol"]}
    names = order[a.spans_source] if a.spans_source else ["spans_tempo", "spans_jaeger", "spans_otelcol"]
    sname, sv = first(*names)
    if sv is not None:
        derived["spansPod"] = (sv / pods, f"{sname} {sv:.1f} spans/s / {pods:.0f} pods")
        if sname == "spans_otelcol":
            notes.append("spans counted at OTel collector receivers include every tier (agent → gateway): check "
                         "spans_otelcol_by_job and rerun with a --selector for one tier if there are two")
        if val("spans_tempo_bytes") and sname == "spans_tempo":
            notes.append(f"Tempo wire bytes per span ≈ {val('spans_tempo_bytes') / sv:.0f} (OTLP protobuf; attribute-heavy spans run 300-1,000)")

    order_out = ["clusters", "pods", "nodes", "spansPod", "logsPod", "logsNode", "seriesPod", "seriesNode", "interval"]
    vals = {}
    for k in order_out:
        if k in derived:
            v = derived[k][0]
            vals[k] = round(v) if k in ("clusters", "pods", "nodes", "seriesPod", "seriesNode", "interval") else round(v, 2)
    if a.json:
        print(json.dumps(vals))
        return
    print(f"Calculator inputs (basis: {a.basis})\n")
    print(f"  {'input':11} {'value':>10}  derived from")
    for k in order_out:
        if k in derived:
            print(f"  {k:11} {vals[k]:>10}  {derived[k][1]}")
        else:
            print(f"  {k:11} {'(keep)':>10}  no data: keep the calculator's value")
    if notes:
        print("\nNotes:")
        for n in notes:
            print("  - " + n)
    js = ("(function(){var k=%r,s={};try{s=JSON.parse(localStorage.getItem(k)||'{}')||{}}catch(e){}"
          "var v=%s;for(var x in v)s[x]=String(v[x]);localStorage.setItem(k,JSON.stringify(s));location.reload()})()"
          % (KEY, json.dumps(vals)))
    print("\nJSON:\n  " + json.dumps(vals))
    print("\nPaste into the browser console on the calculator page (sets the fields and reloads):\n  " + js)


if __name__ == "__main__":
    main()
