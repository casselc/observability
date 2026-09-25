"""A batch bigger than the Parquet reader's default chunk (~16 MB decoded):
default importer settings vs the single-block settings, against one async
entry with the same token. Output: results/bigbatch.jsonl."""
from fp import *
from paths import *
from common import STRUCTURE, COLS
import paths

log = Log("bigbatch.jsonl")
key = "edge-1/e1/%020d.parquet" % 200
paths.KEYS[200] = key
q(f"INSERT INTO FUNCTION s3('{PREFIX}/{key}', '{KEY}', '{SECRET}', 'Parquet', '{STRUCTURE}') "
  f"SELECT s.* REPLACE (toUInt64(200) AS batch_id, toUInt32(row_ordinal + 8000 * n) AS row_ordinal) "
  f"FROM s3('{PREFIX}/edge-1/e1/{1:020d}.parquet', '{KEY}', '{SECRET}', 'Parquet', '{STRUCTURE}') AS s "
  f"CROSS JOIN (SELECT number AS n FROM numbers(8)) AS x",
  settings={"s3_truncate_on_insert": 1, "output_format_parquet_row_group_size": 1000000})

size = len(body(200))
for label, one in [("importer default settings", False), ("importer single-block settings", True)]:
    t = fresh(); tok = token()
    fast(t, 200, tok)
    after_fast = parts(t)
    importer(t, 200, tok, one_block=one)
    log(case=f"64k-row batch ({size/1e6:.1f} MB Parquet): async entry, then {label}", rows=count(t),
        parts_after_async=after_fast, parts=parts(t))
