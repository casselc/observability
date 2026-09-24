"""Can a native retry be made merge-proof? Same as the live case in
exp_dedup.py, but the SELECT is made stable with ORDER BY batch_id,
row_ordinal. Also times that ORDER BY on the 50-batch sets."""
import json, sys, time, uuid
import requests
import os
from common import *
out = open(sys.argv[1], "w")
meta = json.load(open(os.path.join(RESULTS, "sources.json")))
ep = meta["r_nat"]["epoch"]
CTL = "http://127.0.0.1:17001"
T = "central.dedup_t"
ctl = lambda path, **p: requests.get(CTL + path, params=p, timeout=600).text
def count(): return int(q(f"SELECT count() FROM {T}"))
def ins(sel, token, **extra):
    ch(f"INSERT INTO {T} ({COLS}) {sel}", {"use_query_condition_cache": 0, "insert_deduplication_token": token, **extra})
def record(**kw):
    out.write(json.dumps({"exp": "dedup2", **kw}) + "\n"); out.flush(); print(kw, flush=True)

ctl("/sql", q="SYSTEM STOP MERGES")
stmts = attach("r_live", meta["r_nat"]["manifest"], refresh=1)
q("DROP DATABASE IF EXISTS r_live SYNC")
for s in stmts[:2]:
    q(s)
time.sleep(2)
before = int(q(f"SELECT max(batch_id) FROM r_live.{TABLE}"))
ctl("/push", n="10")
new = list(range(before + 1, before + 11))
time.sleep(3)
p0 = q(f"SELECT count() FROM system.parts WHERE active AND database = 'r_live' AND table = '{TABLE}'")
sel = native_select("r_live", ep, new) + " ORDER BY batch_id, row_ordinal"
q(f"TRUNCATE TABLE {T} SYNC")
tok = uuid.uuid4().hex
ins(sel, tok)
record(step=f"first attempt, ORDER BY batch_id, row_ordinal ({p0} parts)", total=count())
ctl("/sql", q="SYSTEM START MERGES")
ctl("/optimize")
time.sleep(4)
p1 = q(f"SELECT count() FROM system.parts WHERE active AND database = 'r_live' AND table = '{TABLE}'")
ins(sel, tok)
record(step=f"retry after writer OPTIMIZE FINAL ({p1} parts)", total=count())
ins(sel, tok, max_threads=16, max_insert_threads=16)
record(step="retry after merge, max_threads=16", total=count())
q("DROP DATABASE r_live SYNC")

# Cost of the ORDER BY: 50 batches from r_opt and from Parquet, insert, 3 runs each.
pid = server_pid()
ids = list(range(1, 51))
for run in range(3):
    for label, s in (("native-1part", native_select("r_opt", meta["r_opt"]["epoch"], ids)),
                     ("parquet", parquet_select(parquet_url("natural", ep, ids), ep, ids))):
        for order in (False, True):
            q(f"TRUNCATE TABLE {T} SYNC"); time.sleep(0.5)
            sql = f"INSERT INTO {T} ({COLS}) {s}" + (" ORDER BY batch_id, row_ordinal" if order else "")
            d = run_measured(pid, sql, {"use_query_condition_cache": 0, "insert_deduplication_token": uuid.uuid4().hex})
            record(step="orderby-cost", source=label, order=order, run=run, wall_s=d["wall_s"],
                   qcpu_s=(d["UserTimeMicroseconds"] + d["SystemTimeMicroseconds"]) / 1e6)
