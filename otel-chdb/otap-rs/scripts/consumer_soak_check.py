#!/usr/bin/env python3
"""consumer_soak_check.py OUT DB: the soak's verdict.

1. Every committed batch is in central exactly once: for each content key
   of a committed data object (the audit, `committed.jsonl`), central holds
   exactly its committed row count (a copy of the same request in another
   epoch has the same key, and must not add rows).
2. Nothing uncommitted is ingested: every content key in central is a
   committed object's.
3. End to end, per request the senders were acked: traces and logs rows by
   `soak.req`; metrics points per request through its series (layout B), and
   every points row's series is in the series table (the attribution rule).
4. The workers' counters, summed over incarnations.
"""
import collections
import glob
import json
import os
import sys
import urllib.request

OUT, DB = sys.argv[1], sys.argv[2]
CH = os.environ.get("CH", "http://127.0.0.1:18123")
TABLES = {
    "traces": "otel_traces",
    "logs": "otel_logs",
    "metrics_number_points": "otel_metrics_number_points",
    "metrics_histogram_points": "otel_metrics_histogram_points",
    "metrics_exponential_histogram_points": "otel_metrics_exponential_histogram_points",
    "metrics_summary_points": "otel_metrics_summary_points",
}


def ch(q):
    r = urllib.request.urlopen(urllib.request.Request(CH + "/", data=q.encode()), timeout=120)
    return r.read().decode()


def rows(q):
    return [l.split("\t") for l in ch(q + " FORMAT TSV").splitlines() if l]


committed = collections.defaultdict(dict)  # signal -> content -> rows
objects = collections.Counter()
tombs = 0
epochs = set()
copies = collections.Counter()
for line in open(os.path.join(OUT, "committed.jsonl")):
    o = json.loads(line)
    epochs.add((o["lane"], o["signal"], o["epoch"]))
    if o.get("kind") == "tomb":
        tombs += 1
        continue
    objects[o["signal"]] += 1
    c = committed[o["signal"]]
    if o["content"] in c:
        copies[o["signal"]] += 1
    c[o["content"]] = o["rows"]

ok = True
print(f"committed objects: {sum(objects.values())} ({dict(objects)}); tombstones {tombs}; epochs {len(epochs)}; "
      f"cross-epoch copies {sum(copies.values())} ({dict(copies)})")
for sig, table in TABLES.items():
    central = {c: int(n) for c, n in rows(f"SELECT content_key, count() FROM {DB}.{table} GROUP BY content_key")}
    com = committed.get(sig, {})
    missing = [c for c in com if central.get(c, 0) == 0]
    partial = [c for c in com if 0 < central.get(c, 0) < com[c]]
    dup = [c for c in com if central.get(c, 0) > com[c]]
    uncommitted = [c for c in central if c not in com]
    good = not (missing or partial or dup or uncommitted)
    ok &= good
    print(f"{'PASS' if good else 'FAIL'} {sig}: committed batches {len(com)}, central batches {len(central)}, "
          f"rows {sum(central.values())} (committed {sum(com.values())}); missing {len(missing)}, partial {len(partial)}, "
          f"duplicated {len(dup)}, uncommitted {len(uncommitted)}")

# ---- per request (the senders' 2xx) --------------------------------------------------
acked = []
for f in glob.glob(os.path.join(OUT, "acked-edge-*.jsonl")):
    acked += [json.loads(l) for l in open(f)]
dropped = [a for a in acked if a["attempts"] < 0]
by = collections.defaultdict(list)
for a in acked:
    by[a["signal"]].append(a)
for sig in ("traces", "logs"):
    got = dict((k, int(n)) for k, n in rows(
        f"SELECT ResourceAttributes['soak.req'], count() FROM {DB}.otel_{sig} GROUP BY 1"))
    bad = [a["id"] for a in by[sig] if got.get(a["id"], 0) != a["rows"]]
    good = not bad
    ok &= good
    print(f"{'PASS' if good else 'FAIL'} {sig} requests acked {len(by[sig])}: rows exactly once for {len(by[sig]) - len(bad)}"
          + (f"; wrong: {bad[:5]}" if bad else ""))
series = f"(SELECT series_id, any(ResourceAttributes['soak.req']) AS req FROM {DB}.otel_metrics_series GROUP BY series_id)"
per = collections.defaultdict(dict)
for t, want in (("number_points", 2), ("histogram_points", 1), ("exponential_histogram_points", 1), ("summary_points", 1)):
    for req, n in rows(f"SELECT s.req, count() FROM {DB}.otel_metrics_{t} AS p INNER JOIN {series} AS s USING (series_id) GROUP BY s.req"):
        per[t][req] = (int(n), want)
bad = []
for a in by["metrics"]:
    for t in per:
        n, want = per[t].get(a["id"], (0, 0))
        if n != want * a["points"]["gauge"]:
            bad.append((a["id"], t, n))
orphans = {t: int(rows(f"SELECT count() FROM {DB}.otel_metrics_{t} WHERE series_id NOT IN (SELECT series_id FROM {DB}.otel_metrics_series)")[0][0])
           for t in ("number_points", "histogram_points", "exponential_histogram_points", "summary_points")}
good = not bad and not any(orphans.values())
ok &= good
print(f"{'PASS' if good else 'FAIL'} metrics requests acked {len(by['metrics'])}: points exactly once per request through its series for "
      f"{len(by['metrics']) - len({b[0] for b in bad})}; points without a series row: {orphans}" + (f"; wrong: {bad[:5]}" if bad else ""))
print(f"requests acked {len(acked)} (dropped as permanent: {len(dropped)}), attempts {sum(abs(a['attempts']) for a in acked)}")

# ---- the workers' counters -----------------------------------------------------------
tot = collections.Counter()
s3 = collections.Counter()
lat = []
incs = 0
for f in sorted(glob.glob(os.path.join(OUT, "w*.stats.json"))):
    try:
        s = json.load(open(f))
    except Exception:
        continue
    incs += 1
    for k, v in s.items():
        if isinstance(v, (int, float)) and not isinstance(v, bool) and not k.startswith("visible") and k != "live_workers":
            tot[k] += v
    s3.update(s.get("s3", {}))
keys = ["objects_inserted", "series_objects_inserted", "rows_inserted", "dedup_skipped", "statements", "statement_objects",
        "insert_errors", "retried_missing", "repaired_partial", "over_count", "fenced_by_server", "deferred_by_lease",
        "tombstones_won", "tombstones_lost_to_data", "epochs_closed", "gaps_seen", "head_missing", "lanes_taken",
        "lanes_released", "lanes_lapsed", "lanes_lost_cas", "renewals", "ckpt_writes", "errors", "cpu_ms", "elapsed_ms"]
print(f"workers: {incs} incarnations; " + ", ".join(f"{k} {round(tot[k], 1)}" for k in keys))
print(f"worker S3 requests: {dict(s3)}")
gc = [json.loads(l) for l in open(os.path.join(OUT, "gc.log")) if l.startswith("{")]
print(f"gc: {len(gc)} runs, deleted data {sum(g['gc']['deleted_data'] for g in gc)}, tombstones {sum(g['gc']['deleted_tombstones'] for g in gc)}, "
      f"epochs retired {sum(g['gc']['epochs_retired'] for g in gc)}, CAS conflicts {sum(g['gc']['cas_conflict'] for g in gc)}")
print("VERDICT:", "PASS" if ok and tot["over_count"] == 0 else "FAIL")
