#!/usr/bin/env python3
"""Latency: the 15 HyperDX query shapes, and the proxy's own cost.

  bench.py scenarios [--rounds 5]
      Per scenario (corpus.py's scenarios / scenarios_a), five ways:
        a            the ClickStack table (rw_a), HyperDX's rendering for it, direct
        c            variant c, HyperDX's rendering, direct (the ALIAS column)
        c+rw direct  c, rewritten (exact mode), sent directly (no proxy)
        c+proxy      c through the proxy, exact mode
        c+proxy cat  c through the proxy, catalog mode
      Server ms = X-ClickHouse-Summary elapsed_ns (it passes through the
      proxy), wall ms = the client's request time; median of ROUNDS after a
      warm-up, the ways interleaved and rotated per round.

  bench.py overhead [--rounds 3]
      Every captured statement (corpus captured.jsonl) direct and through the
      exact-mode proxy, interleaved: the wall-time difference for the
      statements the proxy forwards unchanged is the forwarding cost; the
      proxy's log gives parse + rewrite time for every statement. The
      proxy's RSS and CPU are read from /proc before and after.

  bench.py hop [--rounds 500]
      The hop alone: `SELECT 1` (a pass-through statement), a rewritten
      statement that reads no rows (a filter on an empty time range), and a
      5 MB response, each direct and through the exact-mode proxy,
      interleaved: wall-time difference p50 / p90 / p99, and the proxy's CPU
      per request.

All three start their own proxies (serve, :18125 exact and :18126 catalog) and
stop them at the end. Results: ../results/bench.jsonl
"""
import argparse, http.client, json, os, signal, statistics, subprocess, sys, time, urllib.error, urllib.parse, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "scripts"))
import chlib as c  # noqa: E402

SCR = os.environ.get("RW_SCRATCH", "/tmp")
ap = argparse.ArgumentParser()
ap.add_argument("what", choices=["scenarios", "overhead", "hop"])
ap.add_argument("--rounds", type=int, default=5)
ap.add_argument("--corpus", default=os.path.join(SCR, "rwc"))
ap.add_argument("--bin", default=os.path.join(SCR, "rwproxy-target", "rwproxy"))
ap.add_argument("--config", default=os.path.join(SCR, "rwproxy.json"))
ap.add_argument("--label", default="")
ap.add_argument("--out", default=os.path.join(HERE, "..", "results", "bench.jsonl"))
a = ap.parse_args()
SETTINGS = dict(max_threads="2", max_memory_usage="3000000000", use_query_condition_cache="0", max_execution_time="300",
                output_format_write_statistics="0")
PROXIES = {"exact": "http://127.0.0.1:18125", "catalog": "http://127.0.0.1:18126"}


def start_proxies():
    procs = {}
    for mode, url in PROXIES.items():
        log = os.path.join(SCR, f"rwproxy-{mode}.jsonl")
        if os.path.exists(log):
            os.remove(log)
        cfg = json.load(open(a.config))
        cfg["log"] = log
        path = os.path.join(SCR, f"rwproxy-{mode}.json")
        json.dump(cfg, open(path, "w"))
        p = subprocess.Popen([a.bin, "serve", "-config", path, "-mode", mode, "-listen", url.split("//")[1]],
                             stdout=open(os.path.join(SCR, f"rwproxy-{mode}.out"), "w"), stderr=subprocess.STDOUT, start_new_session=True)
        procs[mode] = (p, log)
    for url in PROXIES.values():
        for _ in range(100):
            try:
                urllib.request.urlopen(url + "/__rw/stats", timeout=1).read()
                break
            except OSError:
                time.sleep(0.1)
    return procs


def proc_stats(pid):
    st = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
    ticks = os.sysconf("SC_CLK_TCK")
    rss = hwm = 0
    for l in open(f"/proc/{pid}/status"):
        if l.startswith("VmRSS"):
            rss = int(l.split()[1])
        if l.startswith("VmHWM"):
            hwm = int(l.split()[1])
    return dict(cpu_s=(int(st[11]) + int(st[12])) / ticks, rss_kb=rss, hwm_kb=hwm)


CONNS = {}


def run(base, q, params, db="default"):
    """One statement over a kept-alive connection per endpoint (as HyperDX's
    clients do), timed from sending to the last byte."""
    qs = dict(SETTINGS)
    qs.update({"param_" + k: v for k, v in (params or {}).items()})
    qs["database"] = db
    host = base.split("//")[1]
    for attempt in (0, 1):
        conn = CONNS.get(host)
        if conn is None:
            conn = CONNS[host] = http.client.HTTPConnection(host, timeout=600)
        t = time.perf_counter()
        try:
            conn.request("POST", "/?" + urllib.parse.urlencode(qs), body=q.encode())
            r = conn.getresponse()
            body = r.read()
        except (http.client.HTTPException, OSError):
            conn.close()
            CONNS.pop(host, None)
            if attempt:
                raise
            continue
        wall = (time.perf_counter() - t) * 1000
        summ = json.loads(r.getheader("X-ClickHouse-Summary") or "{}")
        return dict(status=r.status, wall=wall, server=int(summ.get("elapsed_ns", 0)) / 1e6, rows=int(summ.get("read_rows", 0)), bytes=len(body))


def load(name):
    return [json.loads(l) for l in open(os.path.join(a.corpus, f"{name}.jsonl"))]


def rewrite(sts, mode):
    p = subprocess.run([a.bin, "rewrite", "-config", a.config, "-mode", mode], input="\n".join(json.dumps(s) for s in sts) + "\n",
                       capture_output=True, text=True, check=True)
    return [json.loads(l) for l in p.stdout.splitlines()]


def scenarios():
    c_sts, a_sts = load("scenarios"), load("scenarios_a")
    rw = rewrite(c_sts, "exact")
    ways = {
        "a": lambda s, i: (c.CH, a_by[s][i]),
        "c": lambda s, i: (c.CH, c_by[s][i]),
        "c+rw direct": lambda s, i: (c.CH, dict(c_by[s][i], query=rw_by[s][i])),
        "c+proxy": lambda s, i: (PROXIES["exact"], c_by[s][i]),
        "c+proxy cat": lambda s, i: (PROXIES["catalog"], c_by[s][i]),
    }
    c_by, a_by, rw_by = {}, {}, {}
    for s, r in zip(c_sts, rw):
        c_by.setdefault(s["scenario"], []).append(s)
        rw_by.setdefault(s["scenario"], []).append(r["sql"] or s["query"])
    for s in a_sts:
        a_by.setdefault(s["scenario"], []).append(s)
    res = {}
    names = list(c_by)
    wn = list(ways)
    for n in names:
        for w in wn:
            if w == "a" and n not in a_by:
                continue
            for i in range(len(c_by[n]) if w != "a" else len(a_by[n])):
                base, st = ways[w](n, i)
                run(base, st["query"], st["params"])  # warm-up
        for rd in range(a.rounds):
            order = wn[rd % len(wn):] + wn[:rd % len(wn)]
            for w in order:
                if w == "a" and n not in a_by:
                    continue
                for i in range(len(c_by[n]) if w != "a" else len(a_by[n])):
                    base, st = ways[w](n, i)
                    r = run(base, st["query"], st["params"])
                    lst = res.setdefault(n, {}).setdefault(w, {}).setdefault(i, [])
                    lst.append(r)
        line = [n]
        for w in wn:
            d = res[n].get(w)
            if not d:
                line.append(f"{w}: -")
                continue
            srv = sum(statistics.median(x["server"] for x in lst) for lst in d.values())
            wall = sum(statistics.median(x["wall"] for x in lst) for lst in d.values())
            bad = sum(1 for lst in d.values() for x in lst if x["status"] != 200)
            line.append(f"{w}: {srv:.1f}/{wall:.1f}" + (f" ERR{bad}" if bad else ""))
        print(" | ".join(line), flush=True)
    out = {}
    for n, d in res.items():
        out[n] = {}
        for w, per in d.items():
            out[n][w] = dict(statements=len(per), server_ms=round(sum(statistics.median(x["server"] for x in lst) for lst in per.values()), 2),
                             wall_ms=round(sum(statistics.median(x["wall"] for x in lst) for lst in per.values()), 2),
                             rows=sum(lst[-1]["rows"] for lst in per.values()), errors=sum(1 for lst in per.values() for x in lst if x["status"] != 200),
                             server_all=[[round(x["server"], 2) for x in lst] for lst in per.values()],
                             wall_all=[[round(x["wall"], 2) for x in lst] for lst in per.values()])
    return out


def overhead():
    sts = load("captured")
    rw = rewrite(sts, "exact")
    per = [dict(direct=[], proxy=[], rewritten=r["rewritten"]) for r in rw]
    for rd in range(a.rounds):
        for i, s in enumerate(sts):
            order = ("direct", "proxy") if rd % 2 == 0 else ("proxy", "direct")
            for w in order:
                base = c.CH if w == "direct" else PROXIES["exact"]
                per[i][w].append(run(base, s["query"], s["params"], s["db"])["wall"])
    pas = sorted(statistics.median(p["proxy"]) - statistics.median(p["direct"]) for p in per if not p["rewritten"])
    q = lambda xs, f: xs[int(f * (len(xs) - 1))]
    return dict(statements=len(sts), passthrough=len(pas), forward_overhead_ms=dict(p50=round(q(pas, .5), 3), p90=round(q(pas, .9), 3),
                p99=round(q(pas, .99), 3), mean=round(statistics.mean(pas), 3)),
                direct_wall_ms_p50=round(q(sorted(statistics.median(p["direct"]) for p in per), .5), 3))


def hop():
    cases = {
        "SELECT 1 (pass-through)": dict(query="SELECT 1", params={}),
        "rewritten, no rows": dict(query="SELECT count() FROM {db:Identifier}.{t:Identifier} WHERE Timestamp < '2000-01-01' AND "
                                         "ResourceAttributes['k8s.namespace.name'] = 'payments-core' AND "
                                         "indexHint(mapContains(ResourceAttributes, 'k8s.namespace.name'))",
                                   params=dict(db="rw_c", t="otel_logs")),
        "5 MB response (pass-through)": dict(query="SELECT Body, TraceId, SpanId FROM rw_c.otel_logs LIMIT 50000 FORMAT JSONEachRow", params={}),
    }
    # the rewritten case is sent directly as the proxy would send it, so the
    # difference is the proxy alone, not the catalog subquery
    rw = rewrite([dict(query=st["query"], params=st["params"], db="default") for st in cases.values()], "exact")
    for st, r in zip(cases.values(), rw):
        st["direct"] = r["sql"] or st["query"]
    out = {}
    for name, st in cases.items():
        n = a.rounds if "5 MB" not in name else max(a.rounds // 10, 20)
        d, p, size = [], [], 0
        cpu0 = proc_stats(PROCS["exact"][0].pid)["cpu_s"]
        for i in range(n):
            for w in (("direct", "proxy") if i % 2 == 0 else ("proxy", "direct")):
                r = run(c.CH, st["direct"], st["params"]) if w == "direct" else run(PROXIES["exact"], st["query"], st["params"])
                (d if w == "direct" else p).append(r["wall"])
                size = r["bytes"]
        cpu = proc_stats(PROCS["exact"][0].pid)["cpu_s"] - cpu0
        diff = sorted(y - x for x, y in zip(d, p))
        q = lambda xs, f: xs[int(f * (len(xs) - 1))]
        out[name] = dict(n=n, response_bytes=size, direct_ms_p50=round(statistics.median(d), 3), proxy_ms_p50=round(statistics.median(p), 3),
                         diff_ms=dict(p50=round(q(diff, .5), 3), p90=round(q(diff, .9), 3), p99=round(q(diff, .99), 3)),
                         proxy_cpu_us_per_request=round(cpu * 1e6 / n, 1))
        print(name, out[name], flush=True)
    return out


PROCS = {}


def main():
    procs = start_proxies()
    PROCS.update(procs)
    before = {m: proc_stats(p.pid) for m, (p, _) in procs.items()}
    rec = dict(what=a.what, label=a.label, ts=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), load_before=c.load(), rounds=a.rounds)
    try:
        rec["results"] = {"scenarios": scenarios, "overhead": overhead, "hop": hop}[a.what]()
        rec["load_after"] = c.load()
        after = {m: proc_stats(p.pid) for m, (p, _) in procs.items()}
        stats = {m: json.loads(urllib.request.urlopen(u + "/__rw/stats").read()) for m, u in PROXIES.items()}
        rec["proxy"] = {m: dict(requests=stats[m]["requests"], rewritten=stats[m]["rewritten"], fallbacks=stats[m]["fallbacks"],
                                reasons=stats[m]["reasons"], rewrite_us=stats[m]["rewrite_us"],
                                cpu_s=round(after[m]["cpu_s"] - before[m]["cpu_s"], 3), rss_kb=after[m]["rss_kb"], hwm_kb=after[m]["hwm_kb"],
                                cpu_us_per_request=round((after[m]["cpu_s"] - before[m]["cpu_s"]) * 1e6 / max(stats[m]["requests"], 1), 1))
                        for m in PROXIES}
    finally:
        for p, _ in procs.values():
            os.killpg(p.pid, signal.SIGTERM)
    print(json.dumps({k: v for k, v in rec.items() if k != "results"}, indent=1))
    if a.what != "scenarios":
        print(json.dumps(rec["results"], indent=1))
    with open(a.out, "a") as f:
        f.write(json.dumps(rec) + "\n")


if __name__ == "__main__":
    main()
