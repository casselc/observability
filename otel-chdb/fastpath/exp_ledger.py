"""A per-batch ledger fed by a materialized view on the target: is it exact
under dedup, and can it disagree with the target when one side fails?
Output: results/ledger.jsonl."""
import threading
from fp import *
from paths import *

log = Log("ledger.jsonl")


def with_ledger(constraint=None, mv_throw_batch=None):
    t = fresh()
    if constraint:
        q(f"ALTER TABLE {DB}.{t} ADD CONSTRAINT c CHECK {constraint}")
    q(f"DROP TABLE IF EXISTS {DB}.ledger SYNC")
    q(f"DROP VIEW IF EXISTS {DB}.ledger_mv SYNC")
    q(f"""CREATE TABLE {DB}.ledger (producer_id LowCardinality(String), producer_epoch LowCardinality(String),
          batch_id UInt64, rows UInt64, ingested_at DateTime64(3) DEFAULT now64(3))
          ENGINE = MergeTree ORDER BY (producer_id, producer_epoch, batch_id)""")
    thr = f"throwIf(batch_id = {mv_throw_batch}, 'mv failure') + " if mv_throw_batch else ""
    q(f"""CREATE MATERIALIZED VIEW {DB}.ledger_mv TO {DB}.ledger AS
          SELECT producer_id, producer_epoch, {thr}batch_id AS batch_id, count() AS rows
          FROM {DB}.{t} GROUP BY producer_id, producer_epoch, batch_id""")
    return t


def ledger_rows():
    return q(f"SELECT groupArray((batch_id, rows)) FROM (SELECT batch_id, rows FROM {DB}.ledger ORDER BY batch_id, rows)")


t = with_ledger(); tok = token()
fast(t, 1, tok); fast(t, 1, tok); importer(t, 1, tok)
log(case="async, async retry, importer (same token)", target=count(t), ledger=ledger_rows())

t = with_ledger(); tok = token()
fast(t, 100, tok); importer(t, 100, tok)
log(case="two-partition batch: async then importer", target=count(t), ledger=ledger_rows(), parts=parts(t))

t = with_ledger()
slow1 = {"async_insert_use_adaptive_busy_timeout": 0, "async_insert_busy_timeout_ms": 1500, "async_insert_busy_timeout_max_ms": 1500}
toks = {i: token() for i in (1, 2, 3)}
th = [threading.Thread(target=fast, args=(t, i, toks[i]), kwargs={"extra": slow1}) for i in (1, 2, 3)]
[x.start() for x in th]; [x.join() for x in th]
th = [threading.Thread(target=fast, args=(t, 2, toks[2]), kwargs={"extra": slow1}),
      threading.Thread(target=fast, args=(t, 4, token()), kwargs={"extra": slow1})]
[x.start() for x in th]; [x.join() for x in th]
log(case="combined flushes: 1,2,3 then dup 2 + new 4", target=count(t), ledger=ledger_rows(), parts=parts(t))

# The view fails after (or while) the target is written.
for path in ("async", "importer"):
    t = with_ledger(mv_throw_batch=7); tok = token()
    r = fast(t, 7, tok, check=False) if path == "async" else importer(t, 7, tok, check=False)
    log(case=f"MV throws for batch 7 ({path})", status=r[3], err=r[0][:120], target=count(t), ledger=ledger_rows())

# The target rejects the block (a CHECK constraint): does the ledger still get a row?
for path in ("async", "importer"):
    t = with_ledger(constraint="batch_id != 8"); tok = token()
    r = fast(t, 8, tok, check=False) if path == "async" else importer(t, 8, tok, check=False)
    log(case=f"target constraint rejects batch 8 ({path})", status=r[3], err=r[0][:120], target=count(t), ledger=ledger_rows())
