"""Does async_insert apply to INSERT ... SELECT FROM s3() (the "central pulls
the committed object" fast path)? Output: results/pull.jsonl."""
from fp import *
from paths import *

log = Log("pull.jsonl")
t = fresh(); tok = token()
slow = {"async_insert": 1, "wait_for_async_insert": 0, "async_insert_use_adaptive_busy_timeout": 0,
        "async_insert_busy_timeout_ms": 5000, "async_insert_busy_timeout_max_ms": 5000}
t0 = time.perf_counter()
importer(t, 3, tok, extra=slow)
log(case="INSERT SELECT FROM s3 with async_insert=1, wait=0, busy 5 s", returned_s=round(time.perf_counter() - t0, 3),
    rows_visible_on_return=count(t), pending_async_entries=q("SELECT count() FROM system.asynchronous_inserts"))
