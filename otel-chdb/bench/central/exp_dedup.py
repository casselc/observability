"""Idempotent retries of INSERT ... SELECT with insert_deduplication_token into
a non-replicated MergeTree (non_replicated_deduplication_window = 1000), from
each source. A retry repeats the same statement with the same token, possibly
under different execution settings (threads, block size), and, for a live
native source, after the writer has merged the parts being read.

Needs the 'natural' generator running with -control 127.0.0.1:17001."""
import json, sys, time, uuid
import requests
import os
from common import *

out = open(sys.argv[1], "w")
meta = json.load(open(os.path.join(RESULTS, "sources.json")))
ep = meta["r_nat"]["epoch"]
CTL = "http://127.0.0.1:17001"
T = "central.dedup_t"
q(f"DROP TABLE IF EXISTS {T} SYNC")
q(CENTRAL_DDL.replace("central.otel_traces", T))


def count():
    return int(q(f"SELECT count() FROM {T}"))


def ins(sel, token=None, **extra):
    s = {"use_query_condition_cache": 0, **extra}
    if token:
        s["insert_deduplication_token"] = token
    _, _, summ, _ = ch(f"INSERT INTO {T} ({COLS}) {sel}", s)
    return int(summ.get("written_rows", 0))


def record(**kw):
    out.write(json.dumps({"exp": "dedup", **kw}) + "\n")
    out.flush()
    print(kw, flush=True)


ids = list(range(21, 31))
SOURCES = {"parquet": parquet_select(parquet_url("natural", ep, ids), ep, ids),
           "native-3parts": native_select("r_nat", ep, ids),
           "native-60parts": native_select("r_unm", meta["r_unm"]["epoch"], ids)}
RETRIES = [("same settings", {}),
           ("max_threads=1, max_insert_threads=1", {"max_threads": 1, "max_insert_threads": 1}),
           ("max_block_size=10000", {"max_block_size": 10000, "min_insert_block_size_rows": 10000}),
           ("max_threads=16", {"max_threads": 16, "max_insert_threads": 16})]
for label, sel in SOURCES.items():
    q(f"TRUNCATE TABLE {T} SYNC")
    tok = "tok-" + uuid.uuid4().hex
    w = ins(sel, tok)
    record(source=label, step="first attempt", written=w, total=count())
    for rl, s in RETRIES:
        w = ins(sel, tok, **s)
        record(source=label, step="retry: " + rl, written=w, total=count())
    # No token at all: is an INSERT SELECT retry deduplicated by block hash?
    q(f"TRUNCATE TABLE {T} SYNC")
    ins(sel)
    ins(sel)
    record(source=label, step="no token, run twice", written=None, total=count())

# Live native source: the writer merges between attempt and retry.
ctl = lambda path, **p: requests.get(CTL + path, params=p, timeout=600).text
ctl("/sql", q="SYSTEM STOP MERGES")
stmts = attach("r_live", meta["r_nat"]["manifest"], refresh=1)
q("DROP DATABASE IF EXISTS r_live SYNC")
for s in stmts[:2]:
    q(s)
before = int(q(f"SELECT max(batch_id) FROM r_live.{TABLE}"))
ctl("/push", n="10")
new = list(range(before + 1, before + 11))
time.sleep(3)
parts0 = q(f"SELECT count() FROM system.parts WHERE active AND database = 'r_live' AND table = '{TABLE}'")
sel = native_select("r_live", ep, new)
q(f"TRUNCATE TABLE {T} SYNC")
tok = "tok-" + uuid.uuid4().hex
w = ins(sel, tok)
record(source="native-live", step=f"first attempt ({parts0} parts in reader)", written=w, total=count())
w = ins(sel, tok)
record(source="native-live", step="retry, no merge in between", written=w, total=count())
ctl("/sql", q="SYSTEM START MERGES")
ctl("/optimize")
time.sleep(4)
parts1 = q(f"SELECT count() FROM system.parts WHERE active AND database = 'r_live' AND table = '{TABLE}'")
w = ins(sel, tok)
record(source="native-live", step=f"retry after writer OPTIMIZE FINAL ({parts1} parts in reader)", written=w, total=count())
w = ins(sel, tok, max_threads=1, max_insert_threads=1)
record(source="native-live", step="retry after merge, max_threads=1", written=w, total=count())
q("DROP DATABASE r_live SYNC")
