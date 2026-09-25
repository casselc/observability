"""What does the importer's "is batch X already ingested?" cost at central on a
realistically sized otel_traces target? Also the cost of the alternatives:
a full importer insert, and a token-only re-insert that dedup drops.

Target: 10M spans (1,250 batches x 8,000, 50 producers, ~7 days, 7 partitions)
with three ways to answer the check:
  - the raw envelope columns (full scan of producer_id/epoch/batch_id);
  - the same with the batch's event-time range from its commit record;
  - a bloom_filter skip index on batch_id;
  - an aggregating projection (producer, epoch, batch) -> count(): a ledger
    stored inside each part, so it is exactly as atomic as the data;
  - a ledger table fed by a materialized view.
Output: results/check.jsonl."""
import random, statistics
from fp import *
from paths import *
from common import STRUCTURE, COLS
from setup import make_target

log = Log("check.jsonl")
N_BATCH = int(os.environ.get("N_BATCH", "1250"))
T = "big"
NOCACHE = {"use_query_condition_cache": 0, "use_query_cache": 0}

make_target(T)
q(f"ALTER TABLE {DB}.{T} ADD INDEX idx_batch batch_id TYPE bloom_filter(0.01) GRANULARITY 1")
q(f"""ALTER TABLE {DB}.{T} ADD PROJECTION p_batch
      (SELECT producer_id, producer_epoch, batch_id, count(), min(Timestamp), max(Timestamp)
       GROUP BY producer_id, producer_epoch, batch_id)""")
q(f"DROP TABLE IF EXISTS {DB}.big_ledger SYNC")
q(f"DROP VIEW IF EXISTS {DB}.big_ledger_mv SYNC")
q(f"""CREATE TABLE {DB}.big_ledger (producer_id LowCardinality(String), producer_epoch LowCardinality(String),
      batch_id UInt64, rows UInt64) ENGINE = ReplacingMergeTree ORDER BY (producer_id, producer_epoch, batch_id)""")
q(f"""CREATE MATERIALIZED VIEW {DB}.big_ledger_mv TO {DB}.big_ledger AS
      SELECT producer_id, producer_epoch, batch_id, count() AS rows FROM {DB}.{T}
      GROUP BY producer_id, producer_epoch, batch_id""")

cols = [c.strip("`") for c in COLS.split(", ")]
sel = []
for c in cols:
    sel.append({"Timestamp": "Timestamp + toIntervalSecond(n * 480) AS Timestamp",
                "producer_id": "concat('p', toString(n % 50)) AS producer_id",
                "producer_epoch": "'e1' AS producer_epoch",
                "batch_id": "toUInt64(n) AS batch_id",
                "TraceId": "concat(TraceId, toString(n)) AS TraceId"}.get(c, f"`{c}`"))
src = f"s3('{SEED}', '{KEY}', '{SECRET}', 'Parquet', '{STRUCTURE}') AS s"
t0 = time.perf_counter()
step = 125  # batches per INSERT, so parts look like a stream of inserts, merged
for lo in range(0, N_BATCH, step):
    q(f"INSERT INTO {DB}.{T} ({COLS}) SELECT {', '.join(sel)} FROM {src} "
      f"CROSS JOIN (SELECT number AS n FROM numbers({lo}, {min(step, N_BATCH - lo)})) AS b",
      settings={"max_partitions_per_insert_block": 1000})
build_s = time.perf_counter() - t0
info = q(f"""SELECT count(), sum(rows), formatReadableSize(sum(bytes_on_disk)), uniqExact(partition)
             FROM system.parts WHERE database='{DB}' AND table='{T}' AND active FORMAT TSV""")
proj = q(f"""SELECT formatReadableSize(sum(bytes_on_disk)) FROM system.projection_parts
             WHERE database='{DB}' AND table='{T}' AND active FORMAT TSV""")
idx = q(f"""SELECT formatReadableSize(sum(data_compressed_bytes)) FROM system.data_skipping_indices
             WHERE database='{DB}' AND table='{T}' AND name='idx_batch' FORMAT TSV""")
log(case="target built", parts_rows_size_partitions=info, projection_size=proj, bloom_index_size=idx,
    build_s=round(build_s, 1))

random.seed(7)
probe = random.sample(range(N_BATCH), 20)
rng = {n: q(f"SELECT min(Timestamp), max(Timestamp) FROM {DB}.{T} WHERE batch_id = {n} FORMAT TSV",
             settings=NOCACHE).split("\t") for n in probe}


def key(n):
    return f"producer_id = 'p{n % 50}' AND producer_epoch = 'e1' AND batch_id = {n}"


VARIANTS = {
    "raw columns, no index (projection and skip index disabled)":
        (lambda n: f"SELECT count() FROM {DB}.{T} WHERE {key(n)}",
         {"optimize_use_projections": 0, "use_skip_indexes": 0}),
    "raw columns + event-time range from the commit record":
        (lambda n: f"SELECT count() FROM {DB}.{T} WHERE {key(n)} AND Timestamp BETWEEN '{rng[n][0]}' AND '{rng[n][1]}'",
         {"optimize_use_projections": 0, "use_skip_indexes": 0}),
    "bloom_filter skip index on batch_id":
        (lambda n: f"SELECT count() FROM {DB}.{T} WHERE {key(n)}", {"optimize_use_projections": 0}),
    "aggregating projection (producer, epoch, batch) -> count()":
        (lambda n: f"SELECT count() FROM {DB}.{T} WHERE {key(n)}", {"force_optimize_projection": 1}),
    "ledger table (MV), FINAL":
        (lambda n: f"SELECT count() FROM {DB}.big_ledger FINAL WHERE {key(n)}", {}),
}
for name, (mk, s) in VARIANTS.items():
    walls, rows, bytes_, results = [], [], [], set()
    for n in probe:
        txt, wall, summ, _ = ch(mk(n), settings={**NOCACHE, **s})
        walls.append(wall); rows.append(int(summ.get("read_rows", 0))); bytes_.append(int(summ.get("read_bytes", 0)))
        results.add(txt.strip())
    log(case="check one batch", variant=name, wall_ms_median=round(1000 * statistics.median(walls), 2),
        wall_ms_max=round(1000 * max(walls), 2), read_rows_median=statistics.median(rows),
        read_bytes_median=statistics.median(bytes_), answers=sorted(results))

# The importer checks a whole poll's worth of batches at once.
keys = ",".join(f"('p{n % 50}', 'e1', {n})" for n in random.sample(range(N_BATCH), 100))
for name, sql, s in [
    ("projection, 100 batches in one query",
     f"SELECT producer_id, producer_epoch, batch_id, count() FROM {DB}.{T} WHERE (producer_id, producer_epoch, batch_id) IN ({keys}) GROUP BY ALL",
     {"force_optimize_projection": 1}),
    ("ledger, 100 batches in one query",
     f"SELECT producer_id, producer_epoch, batch_id, max(rows) FROM {DB}.big_ledger WHERE (producer_id, producer_epoch, batch_id) IN ({keys}) GROUP BY ALL", {}),
    ("raw columns, 100 batches in one query",
     f"SELECT producer_id, producer_epoch, batch_id, count() FROM {DB}.{T} WHERE (producer_id, producer_epoch, batch_id) IN ({keys}) GROUP BY ALL",
     {"optimize_use_projections": 0, "use_skip_indexes": 0}),
]:
    walls = []
    for _ in range(5):
        txt, wall, summ, _ = ch(sql, settings={**NOCACHE, **s})
        walls.append(wall)
    log(case="check 100 batches", variant=name, wall_ms_median=round(1000 * statistics.median(walls), 2),
        read_rows=int(summ.get("read_rows", 0)), read_bytes=int(summ.get("read_bytes", 0)),
        found=len(txt.strip().splitlines()))

# For scale: the importer's full insert of one committed batch, and a
# token-only re-insert that dedup drops (option e), into the big target.
for label, tok_fn in [("full importer insert (new token)", token), ("token-only re-insert (dedup drops it)", None)]:
    walls, cpu = [], []
    tok0 = token()
    if tok_fn is None:
        importer(T, 2, tok0)
    for _ in range(5):
        tk = tok_fn() if tok_fn else tok0
        _, wall, summ, _ = importer(T, 2, tk)
        walls.append(wall)
    log(case="importer insert, one 8k batch", variant=label, wall_ms_median=round(1000 * statistics.median(walls), 1),
        wall_ms_all=[round(1000 * w, 1) for w in walls], written_rows=summ.get("written_rows"))
log(case="rows after insert probes", rows=count(T))
