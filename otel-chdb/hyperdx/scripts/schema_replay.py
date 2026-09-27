#!/usr/bin/env python3
"""The schema comparison (../README.md §Schema): replays the SQL HyperDX sent for
each schema (chproxy.jsonl, tags <prefix>-<scenario> from hdx_ui.js runs) on
that schema's own database, as a user of each pays for it: HyperDX renders
different SQL for different tables (text-index key discovery, items filters,
hasAllTokens full text, the key-value rollup).

  --sides NAME:TAGPREFIX:CAPTURE_DB:TARGET_DB[:opt2],...
      default old:old1:hdx_old:hdx_old,new:new1:hdx_new:hdx_new (the two-way run)

A side replays the statements tagged TAGPREFIX-* that name CAPTURE_DB, with
CAPTURE_DB replaced by TARGET_DB. `opt2` derives what HyperDX 2.39.1 sends for
a table without the idx_*_attr_key (mapKeys) text indexes from what it sent for
the full DDL: the only statement that depends on those indexes is map key
discovery (common-utils/src/core/metadata.ts getMapKeys, hyperdx@885d30c),
which without a key index reads the map's `*AttributeItems` index and splits
its tokens at the ALIAS column's separator ('='):

  SELECT token AS key FROM mergeTreeTextIndex(db, t, 'idx_X_attr_key') ...
  -> SELECT splitByString('=', token)[1] AS key FROM mergeTreeTextIndex(db, t, 'idx_X_attr_items') ...

(query rendering, value discovery and filters use only the items index:
queryParser.ts passes textIndexInfo.kv; metadata.ts getAllKeyValues checks .kv.)

--shift-ms moves every epoch-millisecond parameter within a day of the capture
by that much (data generated later than the capture).

Every statement is run once to warm up and then ROUNDS times, the sides
interleaved in rotating order; server time is the median X-ClickHouse-Summary
elapsed_ns. The result-list statements (ORDER BY ... Timestamp DESC, JSONCompact
EachRowWithNamesAndTypes) are compared with the first side's for the same
window and page, on the columns both select.

  schema_replay.py chproxy.jsonl [--ch URL] [--sides ...] [--shift-ms N] [--rounds 5] [--md out.md] [--out out.json]
"""
import argparse, collections, json, re, statistics, urllib.error, urllib.parse, urllib.request

ap = argparse.ArgumentParser()
ap.add_argument("log")
ap.add_argument("--ch", default="http://127.0.0.1:18723")
ap.add_argument("--sides", default="old:old1:hdx_old:hdx_old,new:new1:hdx_new:hdx_new")
ap.add_argument("--shift-ms", type=int, default=0)
ap.add_argument("--rounds", type=int, default=5)
ap.add_argument("--setting", action="append", default=[], help="k=v added to every statement (e.g. max_memory_usage)")
ap.add_argument("--out", default="schema_replay.json")
ap.add_argument("--md", default="schema_replay.md")
a = ap.parse_args()
SIDES = []
for spec in a.sides.split(","):
    p = spec.split(":")
    SIDES.append({"name": p[0], "tag": p[1], "cap": p[2], "db": p[3], "opt2": len(p) > 4 and p[4] == "opt2"})
NAMES = [s["name"] for s in SIDES]
EXTRA = dict(kv.split("=", 1) for kv in a.setting)

FEATURES = [  # (name, regex over query + params): what HyperDX's statement relies on
    ("map key discovery: mergeTreeTextIndex on a mapKeys index", r"idx_\w+_attr_key\b"),
    ("map key discovery: mergeTreeTextIndex on an items index (split at '=')", r"splitByString\([^)]*token\)\[1\] AS key"),
    ("map values: mergeTreeTextIndex on an items index", r"groupUniqArray\(\{HYPERDX_PARAM_\w+:Int32\}\)\(substring\(token"),
    ("parts overlap (system.parts, for the above)", r"FROM system\.parts"),
    ("kv rollup", r"kv_rollup_15m"),
    ("items filter (has(*AttributeItems))", r"AttributeItems"),
    ("full text: hasAllTokens", r"hasAllTokens"),
    ("full text: hasToken (no text index)", r"hasToken\("),
    ("map key discovery by sampling the map (sampledKeys)", r"sampledKeys"),
    ("value discovery by sampling (groupUniqArray over rows)", r"sampledData|groupUniqArray\(\d+\)\(param"),
]


def to_side(r, side):
    q, params = r["query"], dict(r.get("params", {}))
    params = {k: (side["db"] if v == side["cap"] else v) for k, v in params.items()}
    q = q.replace(f"'{side['cap']}'", f"'{side['db']}'").replace(f"`{side['cap']}`.", f"`{side['db']}`.").replace(f" {side['cap']}.", f" {side['db']}.")
    if a.shift_ms:
        for k, v in params.items():
            if re.fullmatch(r"1\d{12}", str(v)):
                params[k] = str(int(v) + a.shift_ms)
    if side["opt2"] and q.lstrip().startswith("SELECT token AS key") and "mergeTreeTextIndex" in q:
        idx = next(k for k, v in params.items() if re.fullmatch(r"idx_\w+_attr_key", str(v)))
        params[idx] = params[idx][: -len("_attr_key")] + "_attr_items"
        params["HYPERDX_PARAM_kvsep"] = "="
        q = q.replace("SELECT token AS key", "SELECT splitByString({HYPERDX_PARAM_kvsep:String}, token)[1] AS key", 1)
    return q, params


def run(query, params, settings):
    qs = {f"param_{k}": v for k, v in params.items()}
    qs.update({k: v for k, v in settings.items() if k not in ("enable_http_compression",)})
    qs.update(EXTRA)
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
todo = collections.OrderedDict()  # (scenario, side name) -> [(query, params, settings, original)]
seen = set()
for r in recs:
    for side in SIDES:
        m = re.match(rf"{re.escape(side['tag'])}-(.+)$", r.get("tag", ""))
        if not m or side["cap"] not in r["query"] + " " + json.dumps(r.get("params", {})):
            continue
        scn = m.group(1)
        q, p = to_side(r, side)
        key = (scn, side["name"], q, json.dumps(p, sort_keys=True))
        if key in seen:
            continue
        seen.add(key)
        todo.setdefault((scn, side["name"]), []).append((q, p, r.get("settings", {}), r))

scenarios = list(dict.fromkeys(s for s, _ in todo))
res = {}
for rnd in range(a.rounds + 1):  # round 0 warms up
    order = NAMES[rnd % len(NAMES):] + NAMES[:rnd % len(NAMES)]
    for scn in scenarios:
        for name in order:
            for i, (q, p, st, _) in enumerate(todo.get((scn, name), [])):
                x = run(q, p, st)
                if rnd:
                    res.setdefault((scn, name, i), []).append(x)


def result_lists(scn, name):
    out = {}
    for i, (q, p, st, _) in enumerate(todo.get((scn, name), [])):
        x = res[(scn, name, i)][0]
        if re.search(r"ORDER BY .*Timestamp.* DESC", q) and "JSONCompactEachRowWithNamesAndTypes" in q and not q.lstrip().startswith("EXPLAIN") and x["ok"]:
            wkey = tuple(sorted(v for k, v in p.items() if re.search(rf"\{{{k}:(Int64|Int32)\}}", q)))
            lines = [json.loads(l) for l in x["body"].decode().splitlines() if l.strip()]
            out[wkey] = (lines[0], lines[2:])
    return out


out, table = [], []
feat_tot = collections.defaultdict(lambda: [0, 0.0, 0])
for scn in scenarios:
    row = {"scenario": scn}
    for name in NAMES:
        n, ms, rr, err = 0, 0.0, 0, 0
        for i, (q, p, st, _) in enumerate(todo.get((scn, name), [])):
            xs = res[(scn, name, i)]
            med = statistics.median(x["ms"] for x in xs)
            n += 1
            ms += med
            rr += xs[0]["read_rows"]
            err += 0 if xs[0]["ok"] else 1
            blob = q + " " + json.dumps(p)
            for fname, pat in FEATURES:
                if re.search(pat, blob):
                    t = feat_tot[(name, fname)]
                    t[0] += 1
                    t[1] += med
                    t[2] += xs[0]["read_rows"]
            out.append({"scenario": scn, "side": name, "query": q, "params": p, "ms": med, "read_rows": xs[0]["read_rows"],
                        "read_bytes": xs[0]["read_bytes"], "error": None if xs[0]["ok"] else xs[0]["error"],
                        "body": xs[0]["body"].decode(errors="replace")[:2000] if xs[0]["ok"] and "mergeTreeTextIndex" in q else None})
        row[name] = {"n": n, "ms": ms, "read_rows": rr, "errors": err}
    base = result_lists(scn, NAMES[0])
    for name in NAMES[1:]:
        other = result_lists(scn, name)
        same = compared = nrows = 0
        for wkey in set(base) & set(other):
            common = [c for c in base[wkey][0] if c in other[wkey][0]]
            proj = [sorted(json.dumps([r[names.index(c)] for c in common]) for r in rows) for names, rows in (base[wkey], other[wkey])]
            compared += 1
            same += proj[0] == proj[1]
            nrows += len(proj[1])
        row[f"same_{name}"] = f"{same} of {compared} ({nrows} rows)"
    table.append(row)

json.dump({"sides": SIDES, "scenarios": table, "statements": out}, open(a.out, "w"), indent=1)
with open(a.md, "w") as f:
    f.write(f"| scenario | statements {' / '.join(NAMES)} | server ms {' / '.join(NAMES)} (sum of medians) | rows read {' / '.join(NAMES)} | "
            + " | ".join(f"result lists {n} = {NAMES[0]}" for n in NAMES[1:]) + " | errors |\n")
    f.write("|---|---:|---:|---:|" + "---|" * (len(NAMES) - 1) + "---:|\n")
    T = {n: [0, 0.0, 0] for n in NAMES}
    for r in table:
        for n in NAMES:
            T[n][0] += r[n]["n"]; T[n][1] += r[n]["ms"]; T[n][2] += r[n]["read_rows"]
        cells = [" / ".join(str(r[n]["n"]) for n in NAMES), " / ".join(f"{r[n]['ms']:.0f}" for n in NAMES),
                 " / ".join(f"{r[n]['read_rows']:,}" for n in NAMES)] + [r[f"same_{n}"] for n in NAMES[1:]] + \
                [" / ".join(str(r[n]["errors"]) for n in NAMES)]
        f.write(f"| {r['scenario']} | " + " | ".join(cells) + " |\n")
    f.write(f"| **all** | {' / '.join(str(T[n][0]) for n in NAMES)} | **{' / '.join(f'{T[n][1]:.0f}' for n in NAMES)}** | "
            f"{' / '.join(f'{T[n][2]:,}' for n in NAMES)} |" + " |" * (len(NAMES) - 1) + " |\n\n")
    f.write(f"| statement kind | {' | '.join(n + ': n, ms, rows read' for n in NAMES)} |\n|---|" + "---:|" * len(NAMES) + "\n")
    for fname, _ in FEATURES:
        f.write(f"| {fname} | " + " | ".join(f"{feat_tot[(n, fname)][0]}, {feat_tot[(n, fname)][1]:.0f}, {feat_tot[(n, fname)][2]:,}" for n in NAMES) + " |\n")
print(open(a.md).read())
