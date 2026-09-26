# Background merge CPU on the central ClickHouse

Labels: **[M]** measured here · **[E]** estimate or extrapolation.
Every run used ClickHouse 26.10.1 on the shared 4-vCPU box, with the consumer's
statement shape. Raw per-merge and per-insert records are in `results/<run>/`.

## Numbers for the calculator

The calculator currently models merges as **1.5× insert CPU [E]**. That is
too low wherever a partition takes more than a few hundred parts, and it grows
with partition size. Merge CPU scales with **rows × merge levels**, not with
inserted objects, so it should be charged per row and not on the fixed
per-object cost.

Merge CPU as a multiple of the same statements' insert CPU. "End" is where the
run stopped. N is the number of inserted parts in the partition (a day's
statements per table per replica). Projections use the model checked in
[Extrapolation](#how-far-the-extrapolation-holds).

| table | rows / statement | merge ÷ insert, measured at end [M] | parts reached | N = 1e3 [E] | **N = 1e4 [E]** | N = 1e5 [E] | merge µs per inserted row, N = 1e4 [E] |
|---|---|---|---|---|---|---|---|
| **traces** (otel_traces + envelope), random ids | 10k | 2.65× | 2,131 | 2.6× | **3.0×** | 3.6× | 19.6 |
| | 100k | 0.99× (run cut short) | 132 | 1.6× | **2.2×** | 2.8× | 11.7 |
| traces, the pool's testgen ids | 10k | 2.85× | 2,400 | 2.9× | 3.2× | 3.8× | 19.4 |
| | 100k | 1.69× | 519 | 1.9× | **2.6×** | 3.2× | 12.2 |
| **logs** (otel_logs + envelope) | 10k | 3.45× | 2,400 | 3.5× | **4.0×** | 4.9× | 19.8 |
| | 100k | 1.59× | 420 | 1.9× | **2.6×** | 3.4× | 9.7 |
| **metrics, series layout: number points** (gauge+sum) | 10k | 3.14× | 3,600 | 3.1× | 3.5× | 4.3× | 10.5 |
| | 80k | 3.28× | 588 | 3.6× | **5.0×** | 6.4× | 5.9 |
| histogram points | 10k | 1.76× | 2,700 | 1.8× | 2.0× | 2.5× | 10.0 |
| | 96k | 1.97× | 294 | 2.4× | 3.1× | 3.8× | 7.4 |
| exponential histogram points | 10k | 1.80× | 900 | 1.8× | 2.4× | 3.1× | 15.7 |
| | 100k | 1.26× | 118 | 1.9× | 2.6× | 3.3× | 13.5 |
| summary points | 10k | 1.70× | 1,800 | 1.7× | 2.2× | 2.8× | 8.6 |
| | 100k | 1.45× | 177 | 2.2× | 3.2× | 4.2× | 8.1 |
| series table (AggregatingMergeTree) | 12.5k | 0.87× | 450 | – | – | – | 12 per series row (few rows) |
| | 100k | 0.20× | 59 | – | – | – | 2.8 per series row |
| **metrics blend**, calculator weights (40 sum, 30 gauge, 20 hist, 5 exp, 5 summary) | 10k | 2.57× | – | 2.6× | 2.9× | 3.6× | 10.5 |
| | 100k | 2.46× | – | 2.9× | **3.9×** | 5.0× | 6.7 |
| ClickStack otel_metrics_sum (for comparison) | 52k | 1.85× | 192 | 2.5× | 3.5× | 4.5× | 28.2 (histogram's slope) |
| ClickStack otel_metrics_histogram | 16k | 2.22× | 840 | 2.3× | 3.1× | 3.9× | 35.4 |
| | 96k | 1.04× | 96 | 1.9× | 2.7× | 3.5× | 29.3 |

Insert CPU per row in the same runs [M] (µs):

| | traces | logs | number pts | histogram | exp. hist | summary | metrics blend | ClickStack sum / histogram |
|---|---|---|---|---|---|---|---|---|
| 10k / statement | 6.50 | 4.95 | 3.00 | 4.88 | 6.40 | 3.94 | 3.59 | – / 11.5 |
| 100k / statement | 5.32 | 3.65 | 1.19 | 2.39 | 5.19 | 2.54 | 1.69 | 8.0 / 11.0 |

These match the calculator's measured insert inputs: 5 µs per span, 1.15 µs
per series-layout point, and 7.85 µs per ClickStack point [M]. For N below the
parts a run reached, the table repeats the value measured at the end.

**Recommendation [E].**

- **Replace the single 1.5× with a per-row merge cost.** The consumer batches
  up to 200k rows per statement, and a table's daily partition on one replica
  takes about 10⁴ statements (e.g. 1.2 statements/s at 100k rows is 10⁵ parts
  a day; 0.1/s is 10⁴). At that size:
  - **spans and logs: about 11 µs per row, 2.5× their insert CPU.** The range
    is 2–3.5× between 10³ and 10⁵ parts per partition. With 10k-row
    statements it is 3–4× (about 20 µs per row).
  - **metric points, series layout: about 6.5 µs per point.** That is about 4×
    the blended per-point insert CPU measured here, and 5–6× the calculator's
    1.15 µs marginal point cost. With 10k-row statements it is about 10.5 µs.
  - **metric points, ClickStack tables: about 25–35 µs per point**, about 3×
    their insert CPU.
  - **series table:** it merges into itself and receives few rows, so leave it
    out.
- **If the calculator keeps one multiplier, use 2.5×** for traces and logs,
  applied to their per-row insert CPU. For metrics, apply about 5× to the
  per-point CPU only, not to the fixed per-object CPU. Merges don't scale with
  the object count, and the fixed cost is most of a metrics insert at small
  flushes.
- **Batch the consumer's statements large.** At 100k rows, merges cost about
  half as much per row as at 10k (11.7 against 19.6 µs for traces at 10⁴ parts),
  on top of the lower insert cost. A tenfold bigger statement removes about
  one merge level and makes the first merge vertical instead of horizontal.
- **Keep the defaults.** Wide parts, a different selector and disabling
  vertical merges were all equal or worse (see [Settings](#settings-effects)).
  An hourly partition cuts merge CPU by about 20% at 1,100 parts [M], and
  about 1.5 levels at production size [E]. That is a real lever if queries
  tolerate 24× more partitions.

## Write amplification [M]

Rows read by merges per inserted row ("rewrites"), and compressed bytes merges
wrote per compressed byte inserted, at the end of each run. Every factor of 10
more parts in the partition adds about the slope shown (fitted on the 10k
runs, 900–3,600 parts).

| table | run | parts | rewrites per inserted row | merge bytes written per inserted byte | stored B/row | extra rewrites per 10× parts | µs per rewritten row, upper levels |
|---|---|---|---|---|---|---|---|
| traces, random ids | traces-10k | 2,131 | 3.47 | 2.91 | 42.3 | 1.13 | 3.20 |
| | traces-100k | 132 | 1.69 | 1.66 | 40.9 | 1.13 | 3.03 |
| traces, testgen ids | traces-10k-tg | 2,400 | 4.65 | 2.19 | 11.5 | 1.38 | 2.61 |
| | traces-100k-tg | 519 | 3.32 | 3.09 | 9.4 | 1.38 | 2.33 |
| logs | logs-10k | 2,400 | 4.19 | 3.75 | 22.1 | 1.38 | 3.12 |
| | logs-100k | 420 | 2.53 | 2.57 | 18.9 | 1.38 | 2.02 |
| number points | metrics-10k | 3,600 | 7.55 | 2.58 | 2.33 | 1.97 | 1.21 |
| | metrics-100k | 588 | 4.36 | 0.55 | 1.07 | 1.97 | 0.83 |
| histogram points | metrics-10k | 2,700 | 4.35 | 1.50 | 9.5 | 1.39 | 1.69 |
| | metrics-100k | 294 | 3.37 | 1.32 | 7.3 | 1.39 | 1.25 |
| exp. histogram points | metrics-10k | 900 | 3.75 | 2.17 | 19.1 | 1.48 | 2.70 |
| | metrics-100k | 118 | 2.48 | 1.12 | 17.0 | 1.48 | 2.45 |
| summary points | metrics-10k | 1,800 | 5.80 | 2.25 | 4.2 | 2.44 | 1.05 |
| | metrics-100k | 177 | 3.33 | 1.36 | 9.2 | 2.44 | 1.04 |
| series table | metrics-10k / 100k | 450 / 59 | 2.23 / 1.12 | 1.35 / 0.19 | 38.9 | – | – |
| ClickStack sum | clickstack-100k | 192 | 2.50 | 1.98 | 20.2 | – | – |
| ClickStack histogram | clickstack-10k / 100k | 840 / 96 | 3.24 / 1.72 | 1.93 / 1.27 | 29.7 / 30.9 | 1.37 | 6.6 |

- **Small parts in bytes are rewritten more.** Number points store 1–2 bytes
  per row, so a 10k-row part is about 23 KB. The Simple selector merges parts
  that small with a low fan-in: about 3 parts per level, against about 8 for
  traces. Number points gain about 2 rewrites per decade of parts; traces gain
  about 1.1. Each rewrite of a number point is cheap (0.8–1.2 µs), so their
  merge ÷ insert ratio ends up the highest of the points tables.
- **The first merge of 10k-row compact parts is the dearest, per row.** It is
  horizontal. For traces it costs 8.3 µs per row read, against about 3.1 µs
  for the vertical merges above it. With 100k-row parts the first merge is
  already vertical (3.2 µs) [M, from the size classes in `results/runs.md`].
- **Byte amplification can be below 1** (number points at 80k rows: 0.55).
  A merged part compresses better than the fresh parts it replaces, so the
  bytes merges write are fewer than the bytes inserts wrote.

## Time to steady state [M]

A partition that keeps growing never reaches a flat steady state. Every
roughly 4× more parts adds a size level. The first merge of each output size
(output rows ÷ statement rows) finished at:

| run | 4–16× | 16–64× | 64–256× | 256–1024× | 1024–4096× |
|---|---|---|---|---|---|
| traces-10k | 2 s / 10 parts | 10 s / 41 | 40 s / 161 | 204 s / 814 | – |
| traces-100k-tg | 7 s / 7 | 27 s / 27 | 102 s / 102 | 529 s / 519 | – |
| logs-10k | 2 s / 8 | 5 s / 20 | 39 s / 157 | 161 s / 645 | – |
| logs-100k | 7 s / 7 | 28 s / 28 | 137 s / 138 | – | – |
| metrics-10k, number | 1 s / 6 | 4 s / 18 | 18 s / 72 | 90 s / 361 | 603 s / 2,413 |
| metrics-100k, number | 6 s / 6 | 18 s / 18 | 130 s / 130 | 337 s / 337 | – |

- **Levels are set by part count, not by time.** A level appears at about 6,
  20, 80, 350–800 and 2,400 parts, whatever the insert size and rate.
- **The per-minute ratio settles within 2–3 minutes (100–700 parts).** After
  that, a minute's merge ÷ insert moves within a band, with a spike whenever
  a top-level merge lands. Per minute, from minute 3 (min–median–max):
  - traces-10k: 2.3–2.8–3.4;
  - logs-10k: 3.0–3.6–4.1;
  - number-10k: 2.7–3.0–5.2;
  - number-100k: 2.7–3.5–5.3.

  The cumulative ratio keeps creeping up by the slope above per decade of parts.
- **Active parts stay few:** at the end of inserts the partitions held 4–31
  active parts, spread over levels 0–16 (`results/steady.md`).

## Settings effects [M]

Traces, 10k-row statements, 4 per second, compared at the same 1,100 parts
(`results/settings.md`):

| setting | rewrites per row | merge µs per inserted row | insert µs/row | merge ÷ insert | insert + merge µs/row |
|---|---|---|---|---|---|
| defaults (traces-10k) | 3.20 | 16.3 | 6.50 | 2.51× | 22.8 |
| `min_bytes_for_wide_part = 0` (every part wide) | 3.35 | 16.1 | 8.27 | 1.95× | 24.4 (+7%) |
| `merge_selector_algorithm = 'StochasticSimple'` | 3.36 | 15.6 | 6.18 | 2.52× | 21.8 (−5%) |
| `enable_vertical_merge_algorithm = 0` | 3.67 | 24.9 | 6.31 | 3.94× | 31.2 (+37%) |
| hourly partition (`toStartOfHour(received_at)`, time 30× faster: 480 parts per partition) | 2.72 | 13.0 | 6.00 | 2.17× | 19.0 (−17%) |

- **Compact parts** (the default below 10 MB) are right for these statement
  sizes. Wide parts cost 27% more at insert and save nothing in merges.
- **StochasticSimple** is within noise of Simple.
- **Vertical merges** save about a third of merge CPU. They start at 131,072
  output rows with 11+ columns, which every table here has.
- **Partitioning finer** caps the levels: a partition of 480 parts had 2.7
  rewrites per row, where one of 1,100 had 3.2. Per day [E]: an hour's
  partition holds 1/24 of the parts, which is about 1.4 decades. That saves
  about 1.5 rewrites for traces and 2.8 for number points, at the price of 24×
  the partitions.

## TTL costs

The day structure is scaled 360× (`run_ttl.sh`, `run_ttl_move.sh`):
- `received_at` is the wall clock, because TTL is evaluated against it;
- a "day" partition is `toStartOfInterval(received_at, 4 min)`;
- a one-day TTL is 4 minutes;
- `merge_with_ttl_timeout` of 14,400 s becomes 40 s.

Each run is 12 minutes (3 "days") at 4 statements/s (TTL DELETE) or
2 statements/s (TTL MOVE) of 10k rows.

| case | table | TTL work [M] | CPU per inserted row [M] | ÷ insert CPU [M] |
|---|---|---|---|---|
| **TTL DELETE, ClickStack's form**: `TTL <partition's day> + 1 day`, `ttl_only_drop_parts = 1` | number points | 4 `TTLDropMerge`, 0 rows read: whole parts dropped | ≈ 0 (0.01 s against 246 s of regular merges) | ≈ 0 |
| **TTL DELETE, row level**: `TTL toDateTime(received_at) + 1 day`, `ttl_only_drop_parts = 0` | number points | 15 `TTLDeleteMerge`, 1.86 rows read per inserted row, 0.38 µs per row read | 0.70 µs (regular merges: 8.4) | **0.24×** (+8% on merges) |
| **TTL MOVE to a second local disk**, each day's partition once a day old | traces | 31 merged parts, 493 MB; `Move` threads 0.66 s: **1.3 ms CPU per MB moved** | 0.06 µs | **0.01×** |
| **TTL MOVE to S3** (SeaweedFS disk) | traces | 22 parts, 502 MB; `Move` threads 0.54 s plus about 7.2 s more in the upload thread pool than the local run: **about 15 ms per MB** [E, by difference] | about 0.65 µs [E] | **about 0.1×** [E] |

- **ClickStack's own TTL costs nothing in merges.** Its TTL is day-granular
  on a day partition with `ttl_only_drop_parts = 1`, so expired data leaves
  as whole parts.
- **A row-level TTL rewrites every partition while it expires.** A day
  partition is rewritten about 6 times (the day ÷ `merge_with_ttl_timeout`),
  each time smaller. Here that cost 0.24× insert CPU. For the calculator's
  "raw metrics kept N days", keep it day-granular and drop-only.
- **TTL MOVE is a copy, not a merge.** part_log has no ProfileEvents for
  `MovePart` (all zero), so its CPU comes from the `Move` (and, for S3, upload)
  threads in /proc. It is proportional to the stored bytes moved: 1.3 ms/MB
  to local disk, about 15 ms/MB to S3. At 40–150 B per span, an S3 move is
  0.6–2.3 µs per row, 0.1–0.45× a 5 µs insert [E]. A local move is a tenth of
  that.
- **Pitfall found: `move_factor`.** The first TTL MOVE attempt
  (`results/movefactor-*`) used the policies' default `move_factor = 0.1`.
  The shared disk was over 90% full, so ClickHouse wrote and moved **every
  new part** to the cold volume at once, and all merges then ran there.
  - Merging on S3 cost 5.9 µs per row read, against 5.0 on the cold local
    disk and 4.9 on the hot disk: +19% [M].
  - The `Move` threads spent 13.1 s on 926 small parts to S3, against 4.3 s
    for 915 parts to local disk: 28 against 9 ms/MB.
  - Set `move_factor = 0` (or keep the hot volume under 90% full) when the
    older tier is meant to be TTL-driven.

## How far the extrapolation holds

Rewrites per row R(N) and merge CPU per inserted row M(N) after N inserted
parts follow `a + b·ln N` closely [M]. `validate_fit.py` fits the first
quarter of a run and predicts the end:

- **Fitted on 300 or more parts, the 4× prediction lands within −2% to +18%**
  of the measured M, and mostly overestimates.
- **Fitted on 100–130 parts, it is off by −26% to +27%.**
- **Fitted on 64 parts, it is useless.**

Where the fit can be made, the slopes of the ~10k and ~100k runs of one table
agree (traces 1.13 per decade in both, logs 1.38 against 1.35, histogram 1.39
against 1.37). So `proj.py` projects each run as:

```
M(N) = M(N_end) + b · ln(N / N_end) · c
```

- `b` is the rewrite slope from the table's 10k run (900–3,600 parts).
- `c` is the run's own µs per row in merges whose output is at least 16× the
  statement, the levels further merges will be.

The projections to 10⁴ and 10⁵ parts are therefore 4–80× beyond what was
measured [E].

Two things bound them:
- Parts stop merging at `max_bytes_to_merge_at_max_space_in_pool` (150 GB).
  A day of 10⁴ traces statements at 100k rows is about 40 GB at these bytes
  per row, so the cap doesn't bind.
- The partition stops growing at the day boundary.

## Method

**Server.** The shared server on :18123 runs with the embedded default
config, which has no `system.part_log` and no `system.query_log`. Reconfiguring
it was off limits, so every run went to a private instance started from the
same binary: `clickhouse` 26.10.1.618, HTTP :18623, data in the session
scratchpad.
- Its config (`server-config.xml` here) adds only `part_log`, `query_log`
  and two storage policies for the TTL MOVE runs (`move_factor = 0`).
- All merge settings are defaults: Simple selector, base 5, 16 pool threads,
  `min_bytes_for_wide_part` 10 MB, vertical merges from 131,072 rows.
- The one change is `old_parts_lifetime = 30` on the test tables. That only
  frees disk sooner.
- Each run had its own database, dropped at the end, and the instance was
  stopped afterwards.

**Tables** (`sql/`, and `merges.py` `ddl()`); the DDL of every run is in its
`results/<run>/ddl.sql`:
- **traces and logs:** the edge's ClickStack DDL (`chdbexporter/schema.go`),
  with every codec, skip index and sort key, plus what the consumer adds
  (`otap-rs/src/central.rs`): the envelope columns, `content_key`, the
  `by_content` projection and `PARTITION BY toDate(received_at)`.
- **metrics:** the tables of `otap-rs/sql/series_tables.sql`, with
  `content_key` and the projection added as `central::series_layout_create_table`
  does:
  - `otel_metrics_number_points` (gauge and sum merged);
  - histogram, exponential histogram and summary points;
  - `otel_metrics_series`.
- **ClickStack metrics tables:** `metrics-layout` `central.ADDL` (contrib
  DDL plus the envelope), with `content_key` and the projection.

**Statements.** Every statement is the consumer's `INSERT … SELECT … FROM
s3('…/{k1,k2,…}', 'Parquet', structure)` with:
- `ONE_BLOCK` plus squashing (one part per statement);
- `insert_deduplication_token`;
- the fence `WHERE`;
- `content_key` from `_file`.

It reads edge objects from SeaweedFS, and several objects per statement
where the consumer would batch them:
- **traces and logs:** 1 object of 10k rows, or 10 objects for 100k.
- **number points:** the gauge and sum objects of one edge batch, 10k or 80k rows.
- **histogram:** 5 × 2k or 6 × 16k rows.
- **exp. histogram and summary:** 5 or 50 objects of 2k rows.

**Data.**
- **Traces and logs:** the 10 testgen objects (10k rows each) that the Rust
  exporter wrote for `otap-rs/scripts/central_bench.py`, copied to
  `merges/pool/`. They are replayed with timestamps shifted to the insert
  time, and the envelope is set per statement.
  - testgen's ids vary in a few bytes only, so by default each row gets fresh
    random trace, span and parent ids in the SELECT (42 B/span stored,
    against 9–11 with the pool's ids).
  - The `-tg` runs keep the pool's ids. Random ids make a merged row about
    30% dearer (3.1 against 2.4 µs) and inserts about 10% dearer.
- **Metrics:** the `metrics-layout` fleet (k8s-shaped, 20 services × 10 pods,
  100k series), encoded by the Go layout-B edge encoder (`seriesenc`) and
  parquetgo, by `mgen/`:
  - 24 rounds at 25 pods per batch (`small`), 24 rounds at 200 pods per batch
    (`big`, also layout A), and 50 rounds of exp. histogram and summary
    (`big50`);
  - 165 MB of Parquet, replayed in cycles with `TimeUnix` shifted per cycle.
- `received_at` is a synthetic clock that starts on 2026-09-20 and runs at
  wall speed (`--time-scale` for the hourly-partition run), so every run
  stays in one daily partition.

**Rates.**
- 10k statements: 4 per second per table (metrics: 4 number, 3 histogram,
  2 summary, 1 exp. histogram, 0.5 series).
- 100k statements: 1 per second for traces and logs; 1, 0.5, 0.3, 0.2 and
  0.1 for the metrics tables.
- 3 concurrent workers per table.
- Every run held its target rate (`results/runs.md`, `stmt/s`).
- Runs went one signal at a time, 7–15 minutes each, or until the database
  reached 0.5–0.9 GB.

**Attribution.**
- **Merge CPU:** the `OSCPUVirtualTimeMicroseconds` of each `MergeParts`
  row in `system.part_log`, which 26.10 records per merge along with
  `UserTimeMicroseconds`, `SystemTimeMicroseconds` and the merge stage times.
- **Insert CPU:** the same counter on each INSERT's `system.query_log` row.
- **Cross-check:** every run also sampled the CPU of the server's
  `MergeMutate` threads from `/proc/<pid>/task/*/stat`. part_log accounted
  for 93–98.5% of it in every complete run. The rest is `system.*` log-table
  merges and merges still running at the cut. (traces-100k: 84%, because a
  big merge was in flight when free disk stopped the run.)
- The merge counts include merges finished during the 60–120 s drain after
  the last statement ("with drain"). "Ingest window" in `results/runs.md`
  counts only those that finished while statements were being sent.
- The `by_content` projection is 0.1–2.4% of merge time.

## Caveats

- **The box was busy:** load average about 20 on 4 vCPUs during most runs,
  from the other agent's servers. CPU is attributed per thread, so other load
  isn't counted. But contention inflates CPU time per unit of work, typically
  by 10–30% [E]. Insert and merge ran under the same conditions, so the
  ratios are less affected than the µs.
- **Partitions reached 60–3,600 parts, not a day's worth.** The disk budget
  (0.5–0.9 GB per run, free space on the shared disk down to 2.5 GB at times)
  set the limit. Values past the parts reached are [E].
- **traces-100k with random ids was cut at 132 parts** when free disk
  dropped. Its projection leans on traces-100k-tg (519 parts, the same
  selector behaviour) and on the 10k run's slope.
- **Synthetic data:**
  - testgen traces and logs have 3 services and a handful of span names.
    Real resource maps (about 20 k8s attributes) are larger per row and would
    raise both insert and merge µs. The ratio moves less (random against
    testgen ids: 2.65× against 2.85× at 10k).
  - The metrics pool repeats every 12 synthetic minutes, so counters step
    back once per cycle.
- **One replica, local SSD, SeaweedFS on localhost.** A replicated table
  merges on every replica, as the calculator already charges.
  `execute_merges_on_single_replica_time_threshold` or zero-copy replication
  can trade that CPU for network; not tested.
- **The rate isn't "several parts per second" at 100k rows:** traces and logs
  ran at 1 statement per second. What drives merges is parts per partition
  and bytes per part, not wall-clock rate. The level-appearance table shows
  levels at the same part counts in 10k and 100k runs.
- **The per-row merge cost depends on bytes per row.** The ratio to insert is
  the more portable number; the µs figures are for this data.

## Files

- `merges.py`: the driver. One run: tables, statement size, rates, drain,
  settings/TTL/partition options. It writes `results/<run>/`:
  - `args.json`, `run.json`, `ddl.sql`;
  - `part_log.jsonl.gz` and `query_log.jsonl.gz`: the raw per-merge and
    per-insert rows with their ProfileEvents;
  - `inserts.jsonl.gz` and `samples.jsonl.gz`: the part layout, merge-thread
    CPU and free disk every 5 s;
  - `summary.json`.
- `analyze.py` builds the per-table summary (`results/runs.md`). `fit.py`
  fits the log curve, `proj.py` makes the calculator table
  (`results/projection.md`), `validate_fit.py` checks the extrapolation
  (`results/validate_fit.txt`), `steady.py` gives the level timings
  (`results/steady.md`), and `compare.py` compares runs at equal N
  (`results/settings.md`).
- `run_tl.sh`, `run_tl2.sh`, `run_metrics.sh`, `run_settings.sh`,
  `run_a.sh`, `run_ttl.sh` and `run_ttl_move.sh` are the runs as executed; the
  logs are `results/run_*.log`.
  - The TTL MOVE runs of `run_ttl.sh` are kept as `results/movefactor-*`
    (see the `move_factor` pitfall).
  - `run_ttl_part1.log` is their log.
  - A first `run_ttl.sh` with a synthetic clock expired every row on insert,
    because TTL runs on the wall clock. It was stopped and redone; nothing
    from it is used.
- `mgen/` is the metrics pool generator (fleet → seriesenc / parquetgo → S3).
- `sql/otel_traces.sql` and `sql/otel_logs.sql` are the central DDL.
- `server-config.xml` is the private instance's config.

Reproduce:
1. Start the stack (`start-services.sh`) and a server with `server-config.xml`.
2. Copy the pool:
   - the testgen traces and logs objects into `otel/merges/pool/{traces,logs}/NN.parquet`;
   - the metrics pools with `go run ./mgen -prefix merges/pool/small -rounds 24 -pods-per-batch 25`,
     then `… big -pods-per-batch 200 -a`,
     then `… big50 -rounds 50 -pods-per-batch 200 -only exponential_histogram,summary`.
3. Run the `run_*.sh` scripts, then `analyze.py`, `fit.py` and `proj.py`.
