#!/usr/bin/env python3
"""Fallback rate sampling with kubectl only (no Prometheus, no log-shipper metrics).

    rates/kubectl_sample.py [--pods 30] [--nodes 3] [--since 10m] [--context CTX] [--out sample.json]

What it measures, on a random sample:
  log lines/s and bytes/line per pod   kubectl logs --since=10m --all-containers | count
  cAdvisor series per pod              GET /api/v1/nodes/N/proxy/metrics/cadvisor, ÷ running pods on N
  kubelet series per node              GET /api/v1/nodes/N/proxy/metrics
  app series per pod                   GET /api/v1/namespaces/NS/pods/P:PORT/proxy/PATH for pods
                                       annotated prometheus.io/scrape=true
Node (journal) log rates need a privileged debug pod and are NOT sampled here:
  kubectl debug node/N -it --image=busybox -- chroot /host sh -c \
      'journalctl --since "-10min" -o cat | wc -l'          # ÷ 600 = lines/s per node
Needs RBAC for pods/log, nodes/proxy and pods/proxy (get). Standard library only.
"""
import argparse
import json
import os
import random
import statistics
import subprocess
import sys

KUBECTL = os.environ.get("KUBECTL", "kubectl")


def kc(args, ctx, timeout=60):
    cmd = [KUBECTL] + (["--context", ctx] if ctx else []) + args
    try:
        p = subprocess.run(cmd, capture_output=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return None, "timeout"
    if p.returncode != 0:
        return None, p.stderr.decode(errors="replace").strip()[:200]
    return p.stdout, None


def series_count(text):
    return sum(1 for l in text.decode(errors="replace").splitlines() if l and not l.startswith("#"))


def seconds(s):
    units = {"s": 1, "m": 60, "h": 3600}
    return int(s[:-1]) * units[s[-1]]


def q(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, max(0, int(round(p * len(xs))) - 1))] if xs else None


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pods", type=int, default=30, help="pods to sample for logs and app metrics")
    ap.add_argument("--nodes", type=int, default=3, help="nodes to sample for cAdvisor and kubelet series")
    ap.add_argument("--since", default="10m")
    ap.add_argument("--context", default="")
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--out", default="sample.json")
    a = ap.parse_args()
    rnd = random.Random(a.seed)
    since_s = seconds(a.since)

    raw, err = kc(["get", "pods", "-A", "--field-selector", "status.phase=Running", "-o", "json"], a.context, 120)
    if raw is None:
        sys.exit(f"kubectl get pods: {err}")
    pods = json.loads(raw)["items"]
    raw, err = kc(["get", "nodes", "-o", "json"], a.context)
    if raw is None:
        sys.exit(f"kubectl get nodes: {err}")
    nodes = [n["metadata"]["name"] for n in json.loads(raw)["items"]]
    per_node = {}
    for p in pods:
        per_node[p["spec"].get("nodeName", "")] = per_node.get(p["spec"].get("nodeName", ""), 0) + 1
    out = {"pods_total": len(pods), "nodes_total": len(nodes), "since": a.since, "errors": []}
    print(f"{len(pods)} running pods on {len(nodes)} nodes", file=sys.stderr)

    # Logs per pod.
    lps, bpl = [], []
    for p in rnd.sample(pods, min(a.pods, len(pods))):
        ns, name = p["metadata"]["namespace"], p["metadata"]["name"]
        body, err = kc(["logs", "-n", ns, name, "--all-containers", "--since=" + a.since, "--ignore-errors"], a.context)
        if body is None:
            out["errors"].append(f"logs {ns}/{name}: {err}")
            continue
        n = body.count(b"\n")
        lps.append(n / since_s)
        if n:
            bpl.append(len(body) / n)
    out["log_lines_per_pod_per_s"] = {"n": len(lps), "mean": statistics.fmean(lps) if lps else None,
                                      "p50": q(lps, .5), "p90": q(lps, .9), "max": max(lps) if lps else None}
    out["log_bytes_per_line"] = statistics.fmean(bpl) if bpl else None

    # cAdvisor and kubelet series per node.
    cad, kub = [], []
    for n in rnd.sample(nodes, min(a.nodes, len(nodes))):
        body, err = kc(["get", "--raw", f"/api/v1/nodes/{n}/proxy/metrics/cadvisor"], a.context)
        if body is None:
            out["errors"].append(f"cadvisor {n}: {err}")
        elif per_node.get(n):
            cad.append(series_count(body) / per_node[n])
        body, err = kc(["get", "--raw", f"/api/v1/nodes/{n}/proxy/metrics"], a.context)
        if body is None:
            out["errors"].append(f"kubelet {n}: {err}")
        else:
            kub.append(series_count(body))
    out["cadvisor_series_per_pod"] = statistics.fmean(cad) if cad else None
    out["kubelet_series_per_node"] = statistics.fmean(kub) if kub else None

    # App metrics of annotated pods.
    ann = [p for p in pods if p["metadata"].get("annotations", {}).get("prometheus.io/scrape") == "true"]
    app = []
    for p in rnd.sample(ann, min(a.pods, len(ann))):
        an = p["metadata"]["annotations"]
        port = an.get("prometheus.io/port", "9090")
        path = an.get("prometheus.io/path", "/metrics")
        ns, name = p["metadata"]["namespace"], p["metadata"]["name"]
        body, err = kc(["get", "--raw", f"/api/v1/namespaces/{ns}/pods/{name}:{port}/proxy{path}"], a.context)
        if body is None:
            out["errors"].append(f"metrics {ns}/{name}: {err}")
            continue
        app.append(series_count(body))
    out["annotated_pods_fraction"] = len(ann) / len(pods) if pods else None
    # Series per annotated pod, and spread over all pods (most pods expose none).
    out["app_series_per_annotated_pod"] = statistics.fmean(app) if app else None
    out["app_series_per_pod"] = (statistics.fmean(app) * len(ann) / len(pods)) if app and pods else None
    with open(a.out, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)
    print(json.dumps({k: v for k, v in out.items() if k != "errors"}, indent=2))
    if out["errors"]:
        print(f"{len(out['errors'])} errors (in {a.out}); first: {out['errors'][0]}", file=sys.stderr)


if __name__ == "__main__":
    main()
