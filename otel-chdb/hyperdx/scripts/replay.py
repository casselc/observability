#!/usr/bin/env python3
"""Replays the SQL HyperDX sent (chproxy.jsonl) that reads the metric tables,
against layout B's views and against the stock ClickStack tables, and compares
the latency and the results.

Every statement whose parameters or text name `B_DB` or `STOCK_DB` together
with an otel_metrics_* table is run twice per round, once with the database
swapped to each (identifier parameters and quoted literals), directly on
ClickHouse (not through the proxy) with the settings HyperDX sent. The server
time (X-ClickHouse-Summary elapsed_ns) is the median of ROUNDS runs, after one
warm-up, the two sides interleaved. Results are compared as the parsed `data` rows (as a multiset when the
statement has no ORDER BY).

  replay.py chproxy.jsonl [--rounds 5] [--tags REGEX] [--out replay.json] [--md replay.md]
"""
import argparse, collections, json, re, statistics, sys, urllib.parse, urllib.request

ap = argparse.ArgumentParser()
ap.add_argument("log")
ap.add_argument("--ch", default="http://127.0.0.1:18123")
ap.add_argument("--b", default="hdx_b")
ap.add_argument("--stock", default="hdx_stock")
ap.add_argument("--rounds", type=int, default=5)
ap.add_argument("--tags", default="")
ap.add_argument("--out", default="replay.json")
ap.add_argument("--md", default="replay.md")
a = ap.parse_args()

TABLE = re.compile(r"otel_metrics_\w+")


def swap(rec, dst):
    src = a.stock if dst == a.b else a.b
    params = {k: (dst if v == src else v) for k, v in rec["params"].items()}
    q = rec["query"].replace(f"'{src}'", f"'{dst}'").replace(f"`{src}`.", f"`{dst}`.").replace(f" {src}.", f" {dst}.")
    return q, params


def run(query, params, settings):
    qs = {f"param_{k}": v for k, v in params.items()}
    qs.update({k: v for k, v in settings.items() if k not in ("enable_http_compression",)})
    qs["use_query_cache"] = "0"
    url = a.ch + "/?" + urllib.parse.urlencode(qs)
    req = urllib.request.Request(url, data=query.encode(), method="POST")
    try:
        with urllib.request.urlopen(req, timeout=300) as r:
            body = r.read()
            summ = json.loads(r.headers.get("X-ClickHouse-Summary") or "{}")
            return {"ok": True, "body": body, "ms": int(summ.get("elapsed_ns", 0)) / 1e6,
                    "read_rows": int(summ.get("read_rows", 0)), "read_bytes": int(summ.get("read_bytes", 0))}
    except urllib.error.HTTPError as e:
        return {"ok": False, "error": e.read().decode(errors="replace")[:600], "ms": 0}


def rows(res, ordered):
    if not res["ok"]:
        return None
    try:
        d = json.loads(res["body"])["data"]
    except Exception:
        lines = [l for l in res["body"].decode(errors="replace").split("\n") if l]
        d = lines
    d = [json.dumps(x, sort_keys=True) for x in d]
    return d if ordered else sorted(d)


recs = [json.loads(l) for l in open(a.log)]
seen, todo = set(), []
for r in recs:
    if a.tags and not re.search(a.tags, r["tag"]):
        continue
    blob = r["query"] + " " + json.dumps(r["params"])
    if not TABLE.search(blob) or not (a.b in blob or a.stock in blob):
        continue
    if re.search(r"system\.(tables|columns|data_skipping_indices|parts)\b|DESCRIBE|SHOW CREATE", r["query"]) and "mergeTreeIndex" not in r["query"]:
        continue
    key = (r["tag"].rsplit("-", 1)[0] if r["tag"].endswith(("-b", "-stock")) else r["tag"], swap(r, a.b)[0], json.dumps(swap(r, a.b)[1], sort_keys=True))
    if key in seen:
        continue
    seen.add(key)
    todo.append(r)

out = []
for r in todo:
    ordered = bool(re.search(r"ORDER BY", r["query"], re.I))
    res = {"b": [], "stock": []}
    sides = (("b", a.b), ("stock", a.stock))
    for side, db in sides:
        q, p = swap(r, db)
        run(q, p, r["settings"])  # warm-up
    for _ in range(a.rounds):  # interleaved, so that load from the shared box hits both sides alike
        for side, db in sides:
            q, p = swap(r, db)
            res[side].append(run(q, p, r["settings"]))
    b, s = res["b"], res["stock"]
    rb, rs = rows(b[0], ordered), rows(s[0], ordered)
    item = {
        "tag": r["tag"], "query": r["query"], "params": r["params"],
        "b_ms": statistics.median(x["ms"] for x in b), "stock_ms": statistics.median(x["ms"] for x in s),
        "b_rows_read": b[0].get("read_rows"), "stock_rows_read": s[0].get("read_rows"),
        "b_bytes_read": b[0].get("read_bytes"), "stock_bytes_read": s[0].get("read_bytes"),
        "b_error": None if b[0]["ok"] else b[0]["error"], "stock_error": None if s[0]["ok"] else s[0]["error"],
        "same": rb == rs, "n_rows": len(rb) if rb is not None else None,
        "orig_status": r["status"], "orig_ms": r["ms"], "orig_error": r.get("error"),
    }
    out.append(item)
    short = re.sub(r"\s+", " ", r["query"])[:110]
    print(f"{item['tag']:32s} b {item['b_ms']:8.1f} ms  stock {item['stock_ms']:8.1f} ms  same={item['same']}"
          f"{'  B-ERR ' + item['b_error'][:120] if item['b_error'] else ''}{'  S-ERR ' + item['stock_error'][:120] if item['stock_error'] else ''}"
          f"  | {short}", flush=True)

json.dump(out, open(a.out, "w"), indent=1)
# per scenario totals
by = collections.OrderedDict()
for it in out:
    t = re.sub(r"-(b|stock)(-search|-catalog)?$", r"\2", it["tag"])
    g = by.setdefault(t, {"n": 0, "b": 0.0, "s": 0.0, "maxb": 0.0, "maxs": 0.0, "diff": 0, "berr": 0, "serr": 0})
    g["n"] += 1
    g["b"] += it["b_ms"]
    g["s"] += it["stock_ms"]
    g["maxb"] = max(g["maxb"], it["b_ms"])
    g["maxs"] = max(g["maxs"], it["stock_ms"])
    g["diff"] += 0 if it["same"] else 1
    g["berr"] += 1 if it["b_error"] else 0
    g["serr"] += 1 if it["stock_error"] else 0
with open(a.md, "w") as f:
    f.write("| scenario | statements | views ms (sum) | stock ms (sum) | ratio | slowest views / stock ms | results differ | errors views / stock |\n")
    f.write("|---|---:|---:|---:|---:|---:|---:|---:|\n")
    for t, g in by.items():
        ratio = g["b"] / g["s"] if g["s"] else float("nan")
        f.write(f"| {t} | {g['n']} | {g['b']:.1f} | {g['s']:.1f} | {ratio:.2f} | {g['maxb']:.1f} / {g['maxs']:.1f} | {g['diff']} | {g['berr']} / {g['serr']} |\n")
print(open(a.md).read())
