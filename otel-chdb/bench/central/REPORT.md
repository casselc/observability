# Central ingest: native parts vs Parquet

**Verdict:** move data to central as Parquet read through `s3()`. At realistic
cycle sizes the two paths cost the same to ingest, because writing the central
MergeTree table dominates. Parquet reads less per cycle, costs nothing while
idle, keeps retries idempotent, and cuts the edge's S3 writes by about 90%.
Keep native tables at the edge for local queries if you want them, but on local
disk.

Labels: **[M]** measured here · **[R]** from the otel-chdb README, not
re-measured · **[E]** estimate. Everything ran on one 4-vCPU box: ClickHouse
26.10.1, chDB 26.7.3, SeaweedFS 4.47 on localhost, so there's no network
latency. Batches were 8,000 spans each. `run.sh` reproduces all of it in about
20 minutes; raw data is in `results/` (run 1) and `results/run2/` (a full
re-run with the same conclusions), and each run's `summary.md` has the full
tables.

## Measured results [M]

The central table has the same columns and skip indexes, a different sort key
(`ServiceName, SpanName, toStartOfHour(Timestamp), TraceId`), and
`non_replicated_deduplication_window = 1000`. Every insert carried a dedup
token. ClickHouse 26.x's query condition cache (on by default) made repeated
runs look pruned when they weren't, so it was turned off for all runs.

### Full `INSERT … SELECT` into central

Medians with [min–max]. "Parts" is the layout of the 60-batch source
generation.

| batches | source | runs | wall (s) | k rows/s | query CPU (s) | S3 requests | S3 MB read |
|---|---|---|---|---|---|---|---|
| 1 | native, 1 / 3 / 60 parts | 3 | 0.082 / 0.110 / 0.116 | 69–97 | 0.12–0.15 | 19 / 29 / 121 GET | 7.49 / 6.81 / 2.39 |
| 1 | Parquet | 3 | **0.059** [0.055–0.059] | 137 | 0.06 | 1 GET + 1 HEAD | **0.26** |
| 10 | native, 1 / 3 / 60 parts | 3 | 0.419 / 0.422 / 0.384 | 190–208 | 0.46–0.52 | 21 / 29 / 130 GET | 10.4 / 8.6 / 3.9 |
| 10 | Parquet | 3 | 0.381 [0.364–0.437] | 210 | 0.45 | 10 GET + 10 HEAD | 2.63 |
| 50 | native, 1 part | 9 | 1.49 [1.44–2.30] | 269 | 1.78 [1.68–2.59] | 21 GET | 17.1 |
| 50 | native, 60 parts | 9 | 1.66 [1.29–2.14] | 241 | 1.94 | 170 GET | 10.4 |
| 50 | Parquet | 9 | 1.72 [1.28–1.94] | 233 | 2.12 [1.65–2.38] | 50 GET + 50 HEAD | 13.2 |
| 50 from 10 producers | native (10 tables) / Parquet | 9 | 1.63 / 1.69 | 245 / 237 | 1.89 / 2.05 | 150 GET / 50 + 50 | 10.0 / 13.2 |

- **Write-bound:** the central insert costs about 5 µs/row on either path. At
  50 batches the difference between sources is inside the run-to-run spread.
  Parquet uses about 10% more query CPU at 50 batches in both full runs.
- **Parquet type conversion:** converting to the target's
  `LowCardinality`/`Map` types adds about 0.4 µs/row (0.44 → 0.59 s of CPU
  per 400k rows, 5 runs).
- **Storage:** Parquet is about 2.2× bigger than merged native parts
  (15.8 MB against 7.2 MB for 480k spans).

### Selectivity

- **Merged parts:** `batch_id` isn't in the sort key, so selecting 1 batch
  reads the whole 7.2 MB merged part. Parquet reads 0.26 MB.
- **Unmerged parts:** every part must be opened, about 2 GETs per part.
- **Event-time hint:** adding the manifest's time range helps unmerged parts
  (4 GETs, 0.24 MB). On merged parts it increased bytes read, from 7.5 to
  17.9 MB.
- **Live 240-batch generation (6–11 parts), newest 3 batches:** native reads
  0.71 MB [0.64–3.26] against 0.79 MB for Parquet.
- **Same generation, 3 old batches:** native reads 8.0 MB [7.8–8.8] against
  0.79 MB.

### Standing costs

- **Attaching a reader table (cold):**
  - 0.06–0.34 s per table.
  - 57–662 GET and 10–77 LIST, depending on the number of parts and leftover
    objects.
- **Refresh polling (`refresh_parts_interval = 1`, one 30 s window each):**
  - About 3 LIST plus 1 GET per part directory, per table, per refresh.
  - At 5 parts that's 3.0 LIST/s and 6.0 GET/s for one table, and
    29.1 LIST/s and 58.2 GET/s for ten.
  - Server CPU is about +5.7 ms per second per table.
  - A 5 s refresh cuts this by 5×.
  - Parquet has no standing cost. Its `s3()` call with a `{a,b,…}` object list
    issues no LIST at all.
- **Manifest discovery (both paths):** 2 LIST per producer-generation prefix,
  plus 1 GET per manifest.

### Retries with dedup tokens

Same results in both full runs:

| retry condition | Parquet | native |
|---|---|---|
| same settings or different thread counts | deduplicated | deduplicated |
| different block size | partial duplicates (+64k of 80k) | partial duplicates |
| no token | duplicates | duplicates |
| writer merged the source parts in between | not applicable (objects never change) | **all 80k rows inserted again** |
| same, with `ORDER BY batch_id, row_ordinal` added | not applicable | still duplicated, and 2.5× the CPU |

The response header's `written_rows` reports 80,000 even when a retry was
fully deduplicated, so it can't be used to detect dedup.

### Reader lifetime and cleanup

- **Readers must keep refreshing.** A native reader with refresh 0 on a live
  generation broke about 14 min after the writer merged: `count()` still
  answered 480,000 from metadata, but any data read failed with "key does not
  exist". The README's `old_parts_lifetime` finding therefore applies to
  every native reader of an active generation.
- **Objects leak on exit.** A writer that exits within `old_parts_lifetime` of
  a merge never deletes the merged-away parts.
  - With `seal_optimize` and exit, the main table's prefix held 1,814 objects
    and 36.5 MB for a 7.2 MB live part.
  - Refresh then costs about 70 GET/s instead of about 4.
  - The default seal path makes this leak certain.
- **Version coupling:** native parts carry version-specific files such as
  `statistics.packed`, so an older central can't be assumed to read a newer
  edge's parts.

## Pros and cons

**Native attached tables**
- **Pros:**
  - Better compression.
  - Efficient reads when most of a generation is wanted.
  - Edge data can be queried from any server without ingesting it.
  - Never shows partial batches [R].
- **Cons:**
  - Polling cost per table, per node.
  - Reads grow with generation size.
  - Dedup tokens break when the writer merges.
  - The attached table can lag the manifest by up to one refresh, so the
    worker must check row counts per batch. This is reasoned from the README's
    ~1 s visibility, not measured.
  - Reader lifetime hazards and leaked objects.
  - Thousands of disk and table entries in the catalog.
  - Edge and central versions must be pinned together.
  - About 11× the edge's S3 writes (51 against 4.7 per batch) [R].
  - Not usable directly by a lakehouse.

**Parquet via `s3()`**
- **Pros:**
  - Reads exactly the named objects.
  - Nothing to attach or poll, and any worker can run any cycle.
  - Retries stay idempotent if the object list and block settings are fixed.
  - Cheaper edge.
  - Independent of ClickHouse version.
  - A newer schema's added columns read as defaults.
  - Can feed a lakehouse.
  - Safe to delete once checkpointed.
- **Cons:**
  - About 2× the bytes.
  - About 7% of insert CPU spent on type conversion.
  - One HEAD per object.
  - Many small objects if batches are small.
  - Nanosecond timestamps and Map columns are untested with Databricks.

## Recommendation, and when it flips

Parquet as the only thing moved to central, with native tables kept on the
edge's local disk if local queries matter. That costs about 145 ms per batch
locally plus about 5 S3 writes [R].

It flips toward native only if all of these hold:
- tens of producers rather than thousands;
- the central reads most of each generation at a time (backfills,
  whole-generation loads);
- edge and central versions are pinned and upgraded together;
- idempotency comes from the ledger (check before insert, a Replacing target,
  or sealed-only reads) rather than dedup tokens.

Needing to query edge data centrally without ingesting it is the other reason,
and even then native can be used for that alone while ingest stays on Parquet.

## Estimated, not measured

- **Real-S3 costs** for 500 producers each sending one 10k-span batch every
  10 s, at us-east-1 list prices [E]:
  - Edge PUTs: about $33k/month native against about $3k Parquet.
  - Polling at a 1 s refresh: about $19k LIST plus $4k GET per month, and
    about 2.9 cores of central CPU. At a 10 s refresh it's about $1.9k plus
    $0.4k.
- **Real-S3 latency** [E]: about 10–30 ms per GET and about 51 PUTs per
  native edge batch.
- **Scaling:** polling beyond 10 tables and server restart time with a large
  catalog of attached tables are extrapolations.
- **Edge throughput and write counts** come from the README.
- **Real event-time overlap:** the synthetic batches are perfectly
  time-ordered, which is the best case for native pruning.
- **Two causes are inferred, not isolated:** that native pruning on merged
  parts comes from automatic statistics, and that merges break dedup because
  the dedup ids include source-part identity.
- **Not tested at all:** Databricks/Spark reading these Parquet types, a
  replicated or multi-node central, and logs (everything here is traces).
