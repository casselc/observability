"""Does async_insert honour insert_deduplication_token on a non-replicated
MergeTree, and does it agree with the importer's INSERT ... SELECT FROM s3()?

Every case gets a fresh table (so the dedup log starts empty), merges stopped
so parts show how blocks were formed. Output: results/dedup.jsonl."""
import threading
from fp import *
from common import STRUCTURE, COLS
from paths import *

log = Log("dedup.jsonl")
# --- A/B: async + token, twice, wait=1 and wait=0 ------------------------------
for wait in (1, 0):
    for fmt in ("Parquet", "Native"):
        t = fresh()
        tok = token()
        fast(t, 1, tok, wait=wait, fmt=fmt); flush()
        fast(t, 1, tok, wait=wait, fmt=fmt); flush()
        rec(log, f"async twice, same token, wait={wait}, {fmt}", t, 8000)
    # the same with the legacy switch spelled out (should change nothing in 26.10)
    t = fresh(); tok = token()
    for _ in range(2):
        fast(t, 1, tok, wait=wait, extra={"deduplicate_insert": "backward_compatible_choice", "async_insert_deduplicate": 0}); flush()
    rec(log, f"async twice, same token, wait={wait}, legacy async_insert_deduplicate=0", t, None)
    t = fresh(); tok = token()
    for _ in range(2):
        fast(t, 1, tok, wait=wait, extra={"deduplicate_insert": "backward_compatible_choice", "async_insert_deduplicate": 1}); flush()
    rec(log, f"async twice, same token, wait={wait}, legacy async_insert_deduplicate=1", t, 8000)

# Window 0 (the MergeTree default): nothing is deduplicated.
t = fresh(extra_settings="non_replicated_deduplication_window = 0")
tok = token()
fast(t, 1, tok); fast(t, 1, tok)
rec(log, "async twice, same token, window=0", t, 16000)

# No token: content hash of the entry.
t = fresh()
fast(t, 1, None); fast(t, 1, None)
rec(log, "async twice, no token", t, None)

# --- C/D/E: async and importer with the same token ---------------------------
t = fresh(); tok = token()
fast(t, 1, tok); importer(t, 1, tok)
rec(log, "async(wait=1) then importer, same token, one-block settings", t, 8000)

t = fresh(); tok = token()
importer(t, 1, tok); fast(t, 1, tok)
rec(log, "importer then async(wait=1), same token", t, 8000)

t = fresh(); tok = token()
fast(t, 1, tok, fmt="Native"); importer(t, 1, tok)
rec(log, "async Native then importer, same token", t, 8000)

# E: the async entry is still in the buffer when the importer commits.
t = fresh(); tok = token()
slow = {"async_insert_use_adaptive_busy_timeout": 0, "async_insert_busy_timeout_ms": 3000,
        "async_insert_busy_timeout_max_ms": 3000}
fast(t, 1, tok, wait=0, extra=slow)
visible_before = count(t)
importer(t, 1, tok)
time.sleep(3.5); flush()
rec(log, "async(wait=0) buffered, importer commits first, then flush", t, 8000, visible_before_importer=visible_before)

# Default importer settings (threads, squashing) vs one-block.
t = fresh(); tok = token()
fast(t, 1, tok); importer(t, 1, tok, one_block=False)
rec(log, "async then importer with default settings", t, 8000)
t = fresh(); tok = token()
fast(t, 1, tok); importer(t, 1, tok, one_block=False, extra={"max_block_size": 1000, "max_threads": 4, "max_insert_threads": 4, "min_insert_block_size_rows": 1000, "min_insert_block_size_bytes": 0})
rec(log, "async then importer forced into 1000-row blocks", t, None)
t = fresh(); tok = token()
importer(t, 1, tok, one_block=False, extra={"max_block_size": 1000, "max_threads": 4, "max_insert_threads": 4, "min_insert_block_size_rows": 1000, "min_insert_block_size_bytes": 0})
fast(t, 1, tok)
rec(log, "importer in 1000-row blocks, then async", t, None)

# --- H: a batch spanning two partitions ---------------------------------------
t = fresh(); tok = token()
fast(t, 100, tok); importer(t, 100, tok)
rec(log, "two-partition batch: async then importer, same token", t, 8000)
t = fresh(); tok = token()
importer(t, 100, tok); fast(t, 100, tok)
rec(log, "two-partition batch: importer then async, same token", t, 8000)

# --- I: one flush combining several exporters' entries --------------------------
t = fresh()
toks = {i: token() for i in (1, 2, 3)}
slow1 = {"async_insert_use_adaptive_busy_timeout": 0, "async_insert_busy_timeout_ms": 1500,
         "async_insert_busy_timeout_max_ms": 1500}
th = [threading.Thread(target=fast, args=(t, i, toks[i]), kwargs={"wait": 1, "extra": slow1}) for i in (1, 2, 3)]
[x.start() for x in th]; [x.join() for x in th]
rec(log, "three exporters, wait=1, one busy window", t, 24000)
# now a duplicate (batch 2) and a new batch (4) in one flush
th = [threading.Thread(target=fast, args=(t, 2, toks[2]), kwargs={"wait": 1, "extra": slow1}),
      threading.Thread(target=fast, args=(t, 4, token()), kwargs={"wait": 1, "extra": slow1})]
[x.start() for x in th]; [x.join() for x in th]
rec(log, "dup of batch 2 + new batch 4 in one flush", t, 32000)
per = q(f"SELECT groupArray((batch_id, c)) FROM (SELECT batch_id, count() c FROM {DB}.{t} GROUP BY batch_id ORDER BY batch_id)")
log(case="per-batch rows after combined flush", per=per)

# --- J: all-or-nothing per entry ----------------------------------------------
t = fresh()
bad = body(5)[: len(body(5)) // 2]           # a truncated Parquet object
res = {}
def go(name, data_i, payload=None):
    s = {"async_insert": 1, "wait_for_async_insert": 1, "insert_deduplication_token": token(), **slow1}
    res[name] = ch(f"INSERT INTO {DB}.{t} ({COLS}) FORMAT Parquet", settings=s,
                   data=payload if payload is not None else body(data_i), check=False)[3]
th = [threading.Thread(target=go, args=("good", 6)), threading.Thread(target=go, args=("truncated", 5, bad))]
[x.start() for x in th]; [x.join() for x in th]
rec(log, "good + truncated Parquet entries in one busy window", t, 8000, status=res)

# Row-format entry with one bad row in the middle: all or nothing?
t2 = fresh("t_small")
rows = "\n".join(json.dumps({"Timestamp": "2026-09-24 12:00:00", "TraceId": f"t{i}", "SpanId": "s", "producer_id": "x",
                              "producer_epoch": "e", "batch_id": 9, "row_ordinal": i}) for i in range(100))
bad_rows = rows.replace('"row_ordinal": 50}', '"row_ordinal": "notanumber"}')
st = ch(f"INSERT INTO {DB}.{t2} (Timestamp, TraceId, SpanId, producer_id, producer_epoch, batch_id, row_ordinal) FORMAT JSONEachRow",
        settings={"async_insert": 1, "wait_for_async_insert": 1, "insert_deduplication_token": token()},
        data=bad_rows.encode(), check=False)
rec(log, "JSONEachRow entry with one bad row (row 50 of 100)", t2, 0, status=st[3], err=st[0][:160])

# --- K: the client's view on timeout --------------------------------------------
t = fresh(); tok = token()
slow5 = {"async_insert_use_adaptive_busy_timeout": 0, "async_insert_busy_timeout_ms": 5000,
         "async_insert_busy_timeout_max_ms": 5000, "wait_for_async_insert_timeout": 1}
txt, wall, _, st = fast(t, 1, tok, wait=1, extra=slow5, check=False)
n0 = count(t)
time.sleep(5.5)
n1 = count(t)
log(case="wait=1, wait_for_async_insert_timeout=1s < busy timeout 5s", status=st, wall_s=round(wall, 2),
    err=txt[:200], rows_at_error=n0, rows_after_flush=n1)
txt, wall, _, st = fast(t, 1, tok, wait=1)
rec(log, "client retries the timed-out insert with the same token", t, 8000, status=st)

# --- L: visibility lag with wait=0 ------------------------------------------------
t = fresh()
for busy in (200, 1000):
    tok = token()
    ex = {"async_insert_use_adaptive_busy_timeout": 0, "async_insert_busy_timeout_ms": busy,
          "async_insert_busy_timeout_max_ms": busy}
    before = count(t)
    t0 = time.perf_counter()
    _, wall, _, _ = fast(t, 2, tok, wait=0, extra=ex)
    seen = None
    while time.perf_counter() - t0 < 10:
        if count(t) > before:
            seen = time.perf_counter() - t0
            break
        time.sleep(0.02)
    log(case=f"wait=0 visibility, busy_timeout={busy}ms", ack_s=round(wall, 4), visible_after_s=round(seen, 3) if seen else None)
    t0 = time.perf_counter()
    _, wall, _, _ = fast(t, 3 if busy == 200 else 4, token(), wait=1, extra=ex)
    log(case=f"wait=1 ack latency, busy_timeout={busy}ms", ack_s=round(wall, 3))
