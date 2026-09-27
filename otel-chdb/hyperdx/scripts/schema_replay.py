#!/usr/bin/env python3
"""The schema comparison (../README.md §Schema): replays the SQL HyperDX sent for
each side (chproxy.jsonl, tags old<N>-<scenario> / new<N>-<scenario>, from
hdx_ui.js runs against the pre-alignment and the ClickStack-DDL sources) on
that side's own database, as a user of each pays for it: HyperDX renders
different SQL for the two tables (text-index key discovery, items filters,
hasAllTokens full text, the key-value rollup on the new one).

Every statement naming OLD_DB or NEW_DB (text or parameters), deduplicated per
scenario, is run once to warm up and then ROUNDS times, the sides interleaved;
server time is the median X-ClickHouse-Summary elapsed_ns. The search
statements (the result list: ORDER BY Timestamp DESC ... LIMIT) are also
compared by result between the sides.

  schema_replay.py chproxy.jsonl [--ch http://127.0.0.1:18723] [--round 1] [--rounds 5] [--md out.md] [--out out.json]
"""
import argparse, collections, json, re, statistics, urllib.error, urllib.parse, urllib.request

ap = argparse.ArgumentParser()
ap.add_argument("log")
ap.add_argument("--ch", default="http://127.0.0.1:18723")
ap.add_argument("--old", default="hdx_old")
ap.add_argument("--new", default="hdx_new")
ap.add_argument("--round", default="1", help="which UI round's statements to replay")
ap.add_argument("--rounds", type=int, default=5)
ap.add_argument("--out", default="schema_replay.json")
ap.add_argument("--md", default="schema_replay.md")
a = ap.parse_args()

FEATURES = [  # (name, regex over query + params): what HyperDX's statement relies on
    ("text-index key discovery (mergeTreeTextIndex)", r"mergeTreeTextIndex"),
    ("parts overlap (system.parts, for the above)", r"FROM system\.parts|system\.parts"),
    ("kv rollup", r"kv_rollup_15m"),
    ("items filter (has(*AttributeItems))", r"AttributeItems"),
    ("full text: hasAllTokens", r"hasAllTokens"),
    ("full text: hasToken (bloom-filter era)", r"hasToken\("),
    ("key discovery by sampling the map (sampledKeys)", r"sampledKeys"),
    ("value discovery by sampling (groupUniqArray)", r"groupUniqArray"),
]


def run(query, params, settings):
    qs = {f"param_{k}": v for k, v in params.items()}
    qs.update({k: v for k, v in settings.items() if k not in ("enable_http_compression",)})
    qs["use_query_cache"] = "0"
    qs["use_query_condition_cache"] = "0"
    req = urllib.request.Request(a.ch + "/?" + urllib.parse.urlencode(qs), data=query.encode(), method="POST")
    try:
        with urllib.request.urlopen(req, timeout=300) as r:
            body = r.read()
            s = json.loads(r.headers.get("X-ClickHouse-Summary") or "{}")
            return {"ok": True, "body": body, "ms": int(s.get("elapsed_ns", 0)) / 1e6, "read_rows": int(s.get("read_rows", 0)),
                    "read_bytes": int(s.get("read_bytes", 0))}
    except urllib.error.HTTPError as e:
        return {"ok": False, "error": e.read().decode(errors="replace")[:400], "ms": 0, "read_rows": 0, "read_bytes": 0}


recs = [json.loads(l) for l in open(a.log) if l.strip()]
todo = collections.OrderedDict()  # (scenario, side) -> [records]
seen = set()
for r in recs:
    m = re.match(rf"(old|new){a.round}-(.+)$", r.get("tag", ""))
    if not m:
        continue
    side, scn = m.groups()
    db = a.old if side == "old" else a.new
    blob = r["query"] + " " + json.dumps(r.get("params", {}))
    if db not in blob:
        continue
    key = (scn, side, r["query"], json.dumps(r.get("params", {}), sort_keys=True))
    if key in seen:
        continue
    seen.add(key)
    todo.setdefault((scn, side), []).append(r)

scenarios = list(dict.fromkeys(s for s, _ in todo))
res = {}
for rnd in range(a.rounds + 1):  # round 0 warms up
    for scn in scenarios:
        for side in (("old", "new") if rnd % 2 else ("new", "old")):
            for i, r in enumerate(todo.get((scn, side), [])):
                x = run(r["query"], r.get("params", {}), r.get("settings", {}))
                if rnd:
                    res.setdefault((scn, side, i), []).append(x)

out, table = [], []
feat_tot = collections.defaultdict(lambda: [0, 0.0, 0])
for scn in scenarios:
    row = {"scenario": scn}
    search = {}
    for side in ("old", "new"):
        n, ms, rr, err = 0, 0.0, 0, 0
        feats = collections.Counter()
        for i, r in enumerate(todo.get((scn, side), [])):
            xs = res[(scn, side, i)]
            med = statistics.median(x["ms"] for x in xs)
            n += 1
            ms += med
            rr += xs[0]["read_rows"]
            err += 0 if xs[0]["ok"] else 1
            blob = r["query"] + " " + json.dumps(r.get("params", {}))
            for name, pat in FEATURES:
                if re.search(pat, blob):
                    feats[name] += 1
                    t = feat_tot[(side, name)]
                    t[0] += 1
                    t[1] += med
                    t[2] += xs[0]["read_rows"]
            if re.search(r"ORDER BY .*Timestamp.* DESC", r["query"]) and "JSONCompactEachRowWithNamesAndTypes" in r["query"] and not r["query"].lstrip().startswith("EXPLAIN") and xs[0]["ok"]:
                # keyed by the window and the page (the Int64 / Int32 parameters' values)
                wkey = tuple(sorted(v for k, v in r.get("params", {}).items() if re.search(rf"\{{{k}:(Int64|Int32)\}}", r["query"])))
                search.setdefault(side, {})[wkey] = xs[0]["body"]
            out.append({"scenario": scn, "side": side, "query": r["query"], "params": r.get("params", {}), "ms": med,
                        "read_rows": xs[0]["read_rows"], "read_bytes": xs[0]["read_bytes"], "error": None if xs[0]["ok"] else xs[0]["error"]})
        row[side] = {"n": n, "ms": ms, "read_rows": rr, "errors": err, "features": dict(feats)}
    # the result-list statements of both sides for the same window and page: their rows, on the
    # columns both select (the new side adds the sort key and _block_number / _block_offset)
    same, compared, nrows = 0, 0, 0
    for wkey in set(search.get("old", {})) & set(search.get("new", {})):
        parsed = {}
        for side in ("old", "new"):
            lines = [json.loads(l) for l in search[side][wkey].decode().splitlines() if l.strip()]
            parsed[side] = (lines[0], lines[2:])  # JSONCompactEachRowWithNamesAndTypes
        common = [c for c in parsed["old"][0] if c in parsed["new"][0]]
        proj = {side: sorted(json.dumps([r[names.index(c)] for c in common]) for r in rows) for side, (names, rows) in parsed.items()}
        compared += 1
        same += proj["old"] == proj["new"]
        nrows += len(proj["new"])
    row["search_same"] = f"{same} of {compared}"
    row["search_rows"] = nrows
    table.append(row)

json.dump({"scenarios": table, "statements": out}, open(a.out, "w"), indent=1)
with open(a.md, "w") as f:
    f.write("| scenario | statements old / new | server ms old / new (sum of medians) | ratio | rows read old / new | search result same | errors |\n")
    f.write("|---|---:|---:|---:|---:|---|---:|\n")
    T = {"old": [0, 0.0, 0], "new": [0, 0.0, 0]}
    for r in table:
        o, n = r["old"], r["new"]
        for s in ("old", "new"):
            T[s][0] += r[s]["n"]; T[s][1] += r[s]["ms"]; T[s][2] += r[s]["read_rows"]
        f.write(f"| {r['scenario']} | {o['n']} / {n['n']} | {o['ms']:.0f} / {n['ms']:.0f} | {n['ms'] / o['ms']:.2f} | "
                f"{o['read_rows']:,} / {n['read_rows']:,} | {r['search_same']} ({r['search_rows']} rows) | {o['errors']} / {n['errors']} |\n")
    f.write(f"| **all** | {T['old'][0]} / {T['new'][0]} | **{T['old'][1]:.0f} / {T['new'][1]:.0f}** | {T['new'][1] / T['old'][1]:.2f} | "
            f"{T['old'][2]:,} / {T['new'][2]:,} | | |\n\n")
    f.write("| statement kind | old: n, ms, rows read | new: n, ms, rows read |\n|---|---:|---:|\n")
    for name, _ in FEATURES:
        o, n = feat_tot[("old", name)], feat_tot[("new", name)]
        f.write(f"| {name} | {o[0]}, {o[1]:.0f}, {o[2]:,} | {n[0]}, {n[1]:.0f}, {n[2]:,} |\n")
print(open(a.md).read())
