"""Block formation: does the dedup agreement between the fast path (one async
entry) and the importer survive an importer insert that forms several blocks?
Output: results/blocks.jsonl."""
from fp import *
from paths import *

SPLIT = {"input_format_parquet_max_block_size": 1000, "max_block_size": 1000, "max_insert_block_size": 1000,
         "min_insert_block_size_rows": 0, "min_insert_block_size_bytes": 0, "use_strict_insert_block_limits": 1,
         "max_threads": 1, "max_insert_threads": 1}
blog = Log("blocks.jsonl")

t = fresh(); tok = token()
importer(t, 1, tok, one_block=False, extra=SPLIT)
blog(case="importer split into 1000-row blocks: parts formed", parts=parts(t), rows=count(t))
importer(t, 1, tok, one_block=False, extra=SPLIT)
blog(case="  ...retried with the same split", rows=count(t))
importer(t, 1, tok, one_block=True)
blog(case="  ...retried as one block", rows=count(t))
fast(t, 1, tok)
blog(case="  ...then the async fast path, same token", rows=count(t))

t = fresh(); tok = token()
fast(t, 1, tok)
importer(t, 1, tok, one_block=False, extra=SPLIT)
blog(case="async first, then importer split into 1000-row blocks", rows=count(t), parts=parts(t))

# Multi-row-group Parquet read by the importer with default settings.
t = fresh(); tok = token()
fast(t, 1, tok)
importer(t, 1, tok, one_block=False, extra={"input_format_parquet_max_block_size": 1000})
blog(case="async first, then importer with only input_format_parquet_max_block_size=1000", rows=count(t), parts=parts(t))

# The async side parses the entry with the same Parquet reader: does ITS chunking matter?
PSPLIT = {"input_format_parquet_max_block_size": 1000}
t = fresh(); tok = token()
fast(t, 1, tok, extra=PSPLIT)
blog(case="async entry parsed in 1000-row chunks: parts formed", parts=parts(t), rows=count(t))
importer(t, 1, tok, one_block=True)
blog(case="  ...then importer as one block", rows=count(t), parts=parts(t))
fast(t, 1, tok)
blog(case="  ...then async parsed as one chunk", rows=count(t), parts=parts(t))

# The reader's byte threshold alone (prefer_block_bytes 1 MB) on the importer.
t = fresh(); tok = token()
fast(t, 1, tok)
importer(t, 1, tok, one_block=False, extra={"input_format_parquet_prefer_block_bytes": 1 << 20})
blog(case="async first, then importer with prefer_block_bytes=1MB (else default)", rows=count(t), parts=parts(t))
bytes8k = q(f"SELECT sum(data_uncompressed_bytes) FROM system.parts WHERE database='{DB}' AND table='{t}' AND active")
blog(case="uncompressed bytes of the table (2 copies of the batch if duplicated)", bytes=bytes8k)
