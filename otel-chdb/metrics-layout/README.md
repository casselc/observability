# metrics-layout: a cheaper metrics layout than the otel_metrics_* tables

Labels: **[M]** measured here · **[D]** docs or source · **[E]** estimate.

## Verdict

**Use layout B: series-deduplicated tables, with the series id computed at
the edge.** The edge sends two kinds of objects:

- narrow per-type **points** objects: series id, time, values;
- a **series** object that carries the attribute maps. A series goes into it
  only the first time the edge sees it in an hour.

Central stores the maps once per series. The points tables hold only numbers.

Compared with the ClickStack tables (A) on the same data, B cuts:

- stored bytes by **4.1×**;
- central insert CPU per point by **7×**, not counting the fixed per-object
  cost;
- edge encode CPU by **5.7×**;
- the Parquet sent over the wire by **1.6×**.

It is lossless. The compatibility views over B return **exactly** A's rows:
72 M rows, count and hash equal.

**Sizing calculator numbers for B.** Blends use the calculator's weights:
40% sum, 30% gauge, 20% histogram, 5% exponential histogram, 5% summary.

| calculator input | B | A (re-measured, same data) | per type (B) |
| --- | --- | --- | --- |
| **central CPU per point** | **1.15 µs + 16 ms per object** [M]. At 10k-point objects that is **2.8 µs** [E, from the fit]. With 20k-point mixed edge batches (5 objects) it was **5.0 µs** [M] | 7.85 µs + 18 ms per object [M]. 20k-point batches: **12.0 µs** [M] | marginal µs/point: sum 0.72, gauge 0.64, histogram 1.8, exp. histogram 2.7, summary 1.3. Series rows add 0.11 [M] |
| **stored bytes per point** (after merges, with the envelope) | **6.3 B** [M] | **26.0 B** [M] | sum 1.36, gauge 1.45, histogram 13.8, exp. histogram 38.1, summary 11.7. The series table adds about 40 B per series per retention period [M] |
| **edge CPU per point** | **1.65 µs** encode [M], about **2.0 µs** with the PUT [E: pubbench's +0.3] | 9.4 µs encode [M] | – |
| **Parquet bytes per point** | **24.1 B** [M] | 38.1 B [M] | sum 11.2, gauge 11.5, histogram 53.6, exp. histogram 68.0, summary 38.4. The series object adds 0.15 [M] |

**Watch the fixed cost per object.** It is now the largest central cost.

- Every `INSERT … SELECT FROM s3()` costs **about 16–18 ms of server CPU**
  however few rows the object has, for A and B alike [M]. Roughly half of it
  is opening the Parquet object and half is creating the part.
- With one flush every 30 s per node, the user's fleet sends about 667
  objects/s: 4k nodes × 5 types / 30 s. That is:
  - B: 667 × 16 ms ≈ **11 cores** of fixed cost, plus 1.3 M × 1.15 µs ≈
    **1.5 cores** for the points;
  - A: about **22 cores** [E, from the measured fit].
- B's schema makes the fix cheap:
  - put gauge and sum in one points table (their schemas are identical);
  - send exponential-histogram and summary objects only when there are some;
  - flush every 60 s, or through a gateway tier.
- That makes B about **4 cores** in total [E]. A stays at about 16 cores,
  because its per-point cost doesn't shrink.

**What B costs.**

- **UI.** HyperDX works unchanged through one VIEW per type, and the rows are
  identical. HyperDX's own chart SQL runs as fast or faster through the views
  than on A: 656 ms against 747 ms CPU for a 1 h sum chart.
- **What gets slower through the views:**
  - filtering on a resource attribute: 2.1× the CPU of A;
  - the metric-name picker: HyperDX's primary-index fast path refuses a
    view, so it scans (1.26 s against 0.08 s);
  - the contrib exporter can't write to the views.
- **Queries written for B** are 1.5–20× cheaper than A.
- **Complexity:**
  - a new edge encoder, prototyped here: about 400 lines;
  - a per-epoch series cache;
  - a sixth lane (`metrics_series`) whose importer needs no count check,
    because the series table is idempotent;
  - the compatibility views.

  The manifest-less commit design needs **one new rule and no new
  mechanism**: mark a series as announced only once its series object has
  committed. See the last section.

**The alternatives lose.**

- **B-central** (keep today's edge, compute the id in ClickHouse) is
  *worse* than A at ingest: 24 µs/point [M]. Without an edge change the maps
  still have to be parsed, and a series row is written per point.
- **ClickHouse's TimeSeries engine (C)** works in 26.10 behind
  `allow_experimental_time_series_table`, and PromQL works too
  (`prometheusQuery*` behind `enable_time_series_aggregate_functions`). But:
  - ingest is **60 µs** per OTel point [M];
  - it stores floats only, so a histogram becomes 17 classic `le` series;
  - it keeps a second "recent samples" copy for 4 days: 21.2 B/point
    inside that window, 10.3 B/point after it [M].
- **The Prometheus TSDB (D)** as the reference:
  - native histograms: 4.2 µs and 7.2 B per OTel point;
  - classic histograms: 11.2 µs and 15.8 B [M].

  B beats both on stored bytes and on marginal ingest CPU. D's queries are
  faster.

## The data

`fleet/` generates it; `cmd/genstat` prints its shape.

**Shape**:

- 20 services × 10 pods = 200 pods on 14 nodes, 8 namespaces;
- 21 resource attributes per pod, as `k8sattributes` and
  `resourcedetection` leave them: `service.*`, `k8s.*` (pod, uid, node,
  deployment, replicaset, namespace, cluster, container), `container.*`,
  `host.name`, `cloud.*`, `os.type`, `telemetry.sdk.*`;
- 5 scopes and 45 metric names;
- 500 series per pod: 260 counters, 140 gauges, 80 explicit histograms with
  11–16 buckets, 10 exponential histograms and 10 summaries;
- 1–5 point attributes per series.

**Size**:

- **100,000 live series** (120,000 over the run, because 20% of services roll
  out once);
- a 30 s interval for **6 h**: 720 rounds, **72 M points**. By type:
  - 37.4 M sum;
  - 20.2 M gauge;
  - 11.5 M histogram;
  - 1.44 M exponential histogram;
  - 1.44 M summary.

**Scaled down** from the 200k series asked for. At 200k series the first
bulk load took free disk from 12 GB to 2.6 GB. Most of that was A's merge
leftovers and SeaweedFS garbage. So the load was stopped and rerun at 100k.

Per-point numbers don't depend on the series count: each series still has
720 points. The series table's share per point does depend on it, but it is
0.07 B.

**Values** evolve like real ones:

- **counters** grow by Poisson increments, from heavy-tailed per-series
  rates with a daily swing. Error and rare-route counters stay flat for long
  stretches;
- **gauges** random walk, stick, burst or stay constant (40 "config" gauges);
- **histograms** add Poisson bucket counts from a per-series log-normal
  latency. Their cumulative min and max move only on a new extreme;
- **exemplars** are on 5% of `http.server.request.duration` points;
- **timestamps**: each pod exports at its own offset, with jitter;
- **rollouts** change the pod name, uid, instance id, version and start
  time, and restart the counters.

**The rows are the same everywhere.** The same generator stream feeds A, B,
C and D.

- C ran on 3 services (10.8 M points) and D on 4 (14.4 M points), to fit the
  disk.
- Their per-point numbers are comparable. Their fleet-wide queries (Q3, Q4)
  cover less data, so they aren't.

## The layouts

**A**:

- The contrib clickhouseexporter v0.161.0's own DDL. `cmd/contribddl` has the
  exporter create it; it is in `sql/contrib/`.
- The envelope is added and the table partitioned by
  `toDate(received_at)`, exactly as `../parquetgo/compare` does it (see
  `central.ADDL`).
- The edge objects are `parquetgo.PGEncoder.Metrics` with the default
  options, bloom filters on.

**B** (`sql/b_tables.sql`, `seriesenc/`, `central.BInsert`):

- **The series table** is `otel_metrics_series`: an `AggregatingMergeTree`
  ordered by `(MetricName, ServiceName, series_id)`.
  - It holds every non-point field of the contrib row: the maps, the scope,
    description, unit, temporality, monotonicity and `ExplicitBounds`.
  - `FirstSeen` and `LastSeen` are `SimpleAggregateFunction(min/max)`.
- **Five narrow points tables** are ordered by
  `(MetricName, ServiceName, series_id, TimeUnix)` and partitioned by
  `toDate(received_at)`.
  - MetricName and ServiceName cost about 0.003 B/point there. They give
    locality and let HyperDX-style filters prune by key.
  - Each has the envelope.
- **Codecs** were chosen with `sql/codecs.sh` on 1 h of this data
  (`results/codecs.tsv`); bytes/point:

  | column | chosen | ZSTD(1) | notes |
  | --- | --- | --- | --- |
  | sum `Value` | `Delta, ZSTD` | 1.83 → **1.20** | |
  | gauge `Value` | `Gorilla, ZSTD` | 1.38 → **1.29** | ALP 1.18, but ALP is beta in 26.10 |
  | histogram `Sum` | `Delta, ZSTD` | 6.96 → **6.26** | |
  | histogram `Count` | `DoubleDelta, ZSTD` | 1.23 → **0.86** | |
  | `BucketCounts` | `ZSTD(1)` | **6.47** | T64 7.3, Delta 8.1, DoubleDelta 13.3 |
  | `batch_id`, `received_at` | `Delta` / `DoubleDelta` | 1.1 and 0.57 → about 0.02 | |
  | `TimeUnix` | `DoubleDelta` | 0.06 | |

- **Series id (v1)**: xxh3-64 over a length-prefixed canonical encoding of
  every non-point field. It is two-level, so the resource and scope part is
  hashed once per ScopeMetrics. `seriesenc/seriesenc.go` specifies it
  exactly.
- **Series cache.** A series is announced once per hour window of
  `TimeUnix`, and again after every epoch change, because a new epoch
  starts with an empty cache.
- **Maps travel as key and value arrays.** Central builds the Map with
  `mapFromArrays`. `parquet-go` would write Go maps in random order.
- **Compatibility views** are in `sql/b_compat_views.sql`: points
  `ANY LEFT JOIN` series `USING (MetricName, ServiceName, series_id)`, with
  the contrib column names, types and order.

**B-central (Bc)**:

- The edge is unchanged and sends A's objects.
- Each object goes into a `Null` table with two materialized views: one to
  the points table and one to the series table (`central.BcDDL`).
- The series id is `cityHash64` of the same fields.

**C**:

- `ENGINE = TimeSeries` with the 26.10 defaults: version 6, `ALP` on
  values, a samples table plus a "recent samples" table with a 4-day TTL,
  and a tags `AggregatingMergeTree` (`cmd/loadts`).
- `promconv/` converts OTel to Prometheus samples with full-fidelity labels:
  every resource and point attribute, the same series as B.
  - Classic histograms become `_bucket{le}`, `_sum` and `_count`.
  - Exponential histograms become only `_sum` and `_count`, because the
    engine stores floats only.
- The edge writes one Parquet row per sample. Central runs
  `INSERT INTO ts (metric_name, tags, samples) SELECT …, [(timestamp, value)] FROM s3()`.

**D**:

- `github.com/prometheus/prometheus/tsdb` v0.303.0 (Prometheus 3.3),
  appended through its Go API with a series-ref cache, then `CompactHead`
  into one block (`promtsdb/`).
- There are two variants:
  - classic `le` series;
  - native histograms: custom buckets (NHCB) for explicit histograms, and
    exponential native histograms (gauge type, because the data is delta).
- **VictoriaMetrics was not built.** Its docs claim about 0.4–1 B per sample
  on production data [D]; not checked.

## Method

**Central CPU** is the server's own per-query ProfileEvents:
`OSCPUVirtualTimeMicroseconds` for the query's thread group, read over the
native protocol (`central/native.go`).

- It excludes merges and other people's queries.
- The server has no `query_log` configured, and `system.events` deltas would
  have included merges.
- Every insert is the importer's
  `INSERT … SETTINGS <single-block> , insert_deduplication_token SELECT … FROM s3('<object>')`,
  one object per insert, from SeaweedFS.

**Three object sizes** fit fixed plus marginal cost per type
(`results/fit.py`):

| run | pods per edge batch | points per batch | objects |
| --- | --- | --- | --- |
| `load-meas-5k` | 10 | 5k | 5 per batch |
| `load-meas-20k` | 40 | 20k | 5 per batch |
| `load-bulk` | 200 | 100k | 5 per batch, 720 batches |

The bulk run is the one that fills the tables used for storage and queries.

**Stored bytes** are `data_compressed_bytes` from `system.parts` after
`OPTIMIZE FINAL`. `bytes_on_disk` is within 0.5%.

**Edge CPU** (`cmd/edgebench`):

- getrusage around each encoder call only, GC included;
- 3 runs × 130 rounds of 20k-point batches, crossing an hourly cache window;
- the runs agree within 1%.

**Queries** (`cmd/queries`, `sql/queries.sql`):

- median of 5 runs after a warm-up;
- server CPU from ProfileEvents, and rows and bytes read from the progress
  packets;
- `use_query_cache = 0`, page cache warm.

**The box** has 4 vCPUs shared with other work, at load 1–3. Compare CPU, not
wall time.

## Comparison

### Ingest and storage

| | A: contrib tables | **B: series-dedup, edge id** | Bc: series-dedup, central id | C: TimeSeries engine | D: Prometheus TSDB, classic | D: Prometheus TSDB, native |
| --- | --- | --- | --- | --- | --- | --- |
| central CPU, marginal µs/point | 7.85 [M] | **1.04, +0.11 for series rows** [M] | – | – | – | – |
| central CPU, fixed ms/object | 18.3 [M] | **16.3** [M] | – | – | – | – |
| central CPU, all-in µs/point, 20k-point batches | 12.0 [M] | **5.0** [M] | 24.2 [M] | – | – | – |
| central CPU, all-in µs/point, 100k-point batches | 8.8 [M] | **1.9** [M] | – | 60.3 (54.6k samples/object) [M] | 11.2 (5.8 convert + 5.4 append) [M] | 4.2 (1.6 + 2.5) [M] |
| compaction CPU, µs/point | not measured | not measured | – | not measured | 0.64 [M] | 0.30 [M] |
| stored B/point, blend | 26.0 [M] | **6.3** [M] | same as B [E] | 21.2 within 4 days, 10.3 after [M] | 15.8 [M] | 7.2 [M] |
| stored B/point: sum / gauge / histogram / exp. histogram / summary | 20.4 / 19.1 / 38.1 / 57.3 / 33.8 [M] | **1.36 / 1.45 / 13.8 / 38.1 / 11.7** [M] | – | – | 4.33 B per sample, 3.64 samples per point [M] | 6.7 B per sample, 1.08 samples per point [M] |
| series or index | in every row: ResourceAttributes alone is 15.1 B/point | 39.6 B per series row: 0.07 B/point over 6 h [M], 0.02 over 24 h [E] | same as B | tags table: 0.5 B/point over 6 h [M] | index: 0.47 B/sample [M] | index: 0.46 B/sample [M] |
| edge CPU, µs/point | 9.4 [M] | **1.65** [M] | 9.4 (A's) | 28.6, naive per-sample label arrays [M] | – | – |
| Parquet B/point, blend | 38.1 [M] | **24.1** [M] | 38.1 | 92.9 [M] | – | – |
| histograms | as OTLP | as OTLP, bounds in the series row | as OTLP | classic `le` series only; exp. histograms lose their buckets | classic `le`: 17 series each | native, both kinds |
| UI (HyperDX) | native | views, identical rows | views | no | no (Grafana) | no |

Notes on the table:

- **The previous measurements were** 8.2 µs, 25 B and 7.8 µs, with 12
  resource attributes and less realistic values. Here A has 21 resource
  attributes: `ResourceAttributes` is 15.1 B/point, up from 8.9. The other
  columns shrank on realistic values.
- **Where B's bytes go:**
  - counters: `Value` 1.15, `TimeUnix` 0.06, `series_id` 0.03, envelope
    0.1;
  - histograms: `Sum` 6.1 and `BucketCounts` 5.9, together 87%;
  - exponential histograms: noisy per-point `Sum`, `Min` and `Max` (delta
    temporality).
- **Where A's go:** `ResourceAttributes` 15.1 B/point, then `Attributes` 0.8–2.1,
  and `ScopeName`, `MetricUnit`, `MetricDescription` and
  `ResourceSchemaUrl` at about 0.27 each (a Map or String column per row).
- **The fixed cost per object**, in `results/fixedcost.txt`, for a 5,600-row
  B gauge object:
  - `SELECT * FROM s3()`: 1–3 ms;
  - `INSERT` into a `Null` table: 10–14 ms;
  - `INSERT` into the MergeTree: 14–19 ms.
  - Ten objects in one `INSERT` through a `{..}` glob cost 12 ms each without
    squashing and **7.5–9.3 ms** each with squashing (one part).

  So grouping objects halves the fixed cost at best. But a squashed group has
  an unstable dedup identity (see `../parquetgo` README), so the importer
  would then rely on the count check alone.
- **C's 60 µs/point** is 16.6 µs per sample:
  - one tags row per sample until merges collapse them: 25 M raw rows were
    2.1 GB of parts during the load;
  - the second write to "recent samples".
- **Merge CPU** was not measured for any layout. B merges about 4× fewer
  bytes than A [E].

### Queries

The queries are:

- **Q1**: rate of `http.server.request.count` for service `checkout`, by pod.
- **Q2**: p99 of `http.server.request.duration` for `checkout`.
- **Q3**: top 10 pods by `process.memory.usage`, fleet-wide.
- **Q4**: request rate by service for `k8s.namespace.name = payments`.
- **H1** and **H2**: HyperDX's own generated SQL for a sum chart grouped by
  pod, and for a histogram p99. They are taken from its `renderChartConfig`
  snapshots at hyperdx@885d30c and run on A's tables and B's views.

Steps are 1 min for the 1 h window and 5 min for the 6 h one.

Cells are **server CPU ms / MB read** [M]:

| query | window | A | B | B via view | C (3 services) | D classic (4 services) | D native (4 services) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Q1 | 1h | 109 / 31.8 MB | **36 / 6.5 MB** | 85 / 7.1 MB | 63 / 13.6 MB | 11 | 12 |
| Q1 | 6h | 424 / 173.7 MB | **43 / 6.5 MB** | 329 / 7.1 MB | 96 / 29.0 MB | 33 | 35 |
| Q2 | 1h | 170 / 36.0 MB | **111 / 31.1 MB** | 174 / 31.6 MB | 330 / 101.2 MB | 129 | 55 |
| Q2 | 6h | 505 / 183.4 MB | **149 / 31.1 MB** | 406 / 31.6 MB | 757 / 269.7 MB | 368 | 142 |
| Q3 | 1h | 191 / 6.1 MB | **9.3 / 3.1 MB** | 15 / 3.9 MB | 16 / 5.4 MB | 1.0 | 1.0 |
| Q3 | 6h | 200 / 6.1 MB | **11 / 3.1 MB** | 56 / 3.9 MB | 14 / 6.1 MB | 2.7 | 2.8 |
| Q4 | 1h | 397 / 95.3 MB | **148 / 19.6 MB** | 852 / 132.5 MB | 58 / 13.2 MB | 11 | 11 |
| Q4 | 6h | 1939 / 527.9 MB | **185 / 19.6 MB** | 3079 / 132.5 MB | 93 / 28.1 MB | 31 | 33 |
| H1 (HyperDX sum) | 1h | 747 / 60.7 MB | – | **656 / 7.1 MB** | – | – | – |
| H1 | 6h | 1542 / 177.2 MB | – | **1359 / 7.1 MB** | – | – | – |
| H2 (HyperDX p99) | 1h | 198 / 61.5 MB | – | **162 / 31.6 MB** | – | – | – |
| H2 | 6h | 876 / 186.0 MB | – | **719 / 31.6 MB** | – | – | – |

- Wall times, rows read and per-run values are in `results/queries-*.jsonl`
  and `results/promtsdb-*.json`. `results/tables.py` prints this table.
- **The B queries join the series table.**
  - Q1 and Q3 read the matching series (pod name) with a `DISTINCT` subquery
    and join on `series_id`.
  - Q2 takes `ExplicitBounds` from the series row.
  - Q4 filters `(ServiceName, series_id) IN (series WHERE
    ResourceAttributes['k8s.namespace.name'] = 'payments')`. The plain
    `series_id IN` form can't use the key, because ServiceName sits between
    MetricName and series_id in it, and read 5.8 M rows instead of 0.9 M.
- **B's 1 h and 6 h read the same rows**, because time is the last key
  column and a series' 6 h fits in one granule. With a day per partition, a
  1 h B query reads the day's points for the matched series. That is still
  small (about 1.4 B/point), but it grows with the window A prunes by hour.
  At 24 h retention:
  - A: 24 h costs about 4× the 6 h figure, and 1 h stays the same [E];
  - B: both 1 h and 24 h cost about 4× the 6 h figure, for example Q1 about
    170 ms CPU [E].
- **For retention of weeks, add a coarse time bucket to B's key.** For
  example, `toStartOfInterval(TimeUnix, INTERVAL 1 DAY)` before `series_id`
  [E, not measured].
- **C and D ran on 3 and 4 services.** Q1 and Q2 touch one service, so they
  compare directly. Q3 and Q4 scale with the fleet: times 6.7 for C and 5 for
  D to reach 20 services [E].
- **D is an in-process PromQL engine over one block.** Its CPU is the Go
  process's. It shows how cheap a purpose-built TSDB is for these queries,
  not what a Prometheus server with HTTP would cost.
- **C's `histogram_quantile` works in 26.10.** Q2 on C reads 3–9 M rows,
  because every `le` bucket is its own series.

## HyperDX (ClickStack UI) compatibility

**What works** [M]:

- `sql/b_compat_views.sql` presents `otel_metrics_{gauge,sum,histogram,exponential_histogram,summary}`
  over B with the contrib names, types and column order.
- A HyperDX metrics source points its per-type table names at the views'
  database.
- **Correctness:**
  - `EXCEPT ALL` in both directions is empty on the 800k-row pilot, for all
    five types.
  - On the full 72 M rows, the counts are equal for all five types, and
    `sum(cityHash64(<all columns>))` is equal for gauge and histogram.
- **HyperDX's own chart SQL** (window functions over
  `cityHash64(ScopeAttributes, ResourceAttributes, Attributes)`, `SELECT *`)
  costs **12–18% less CPU** through the views than on A. It reads 7–31 MB
  against 61–186 MB:
  - A reads every map of every point;
  - the view materializes the maps only through the join.

**What gets worse or breaks** [M unless marked]:

- **Filtering on a map through the view** (Q4-BV) is 2.1× A's CPU at 1 h and
  1.6× at 6 h. A's bloom-filter skip indexes on the map columns don't reach
  through the join, and the join builds the right side from every series
  row of the metric.
  - Queries written for B are fine (Q4-B is 2.7–10× cheaper than A).
  - A HyperDX-native B source would fix this. That is a code change in
    HyperDX: filter the series table first.
- **The metric-name picker and value autocomplete.** HyperDX reads distinct
  key values with `mergeTreeIndex(db, table)`, and `assertIndexReadable`
  rejects anything that isn't a MergeTree.
  - Through a view it falls back to a scan: `SELECT DISTINCT MetricName`
    over 1 h costs 1,260 ms and 37.6 M rows, against 78 ms and 7.8 M rows on
    A.
  - Map-key discovery (`groupUniqArrayArray(keys)` over 3 M rows) is about
    the same: 2.7 s against 3.3 s.
  - Mitigation [E]: a small MergeTree of `(MetricName, ServiceName)`
    maintained from the series table.
- **No writes through the views.** A contrib exporter writing directly to
  ClickHouse can't coexist on the same tables. All metrics must come
  through the edge path, or through a B-shaped writer.
- **HyperDX's rollup acceleration** looks for `MaterializedView` engines on
  the source table. That machinery doesn't apply to views [D:
  `metadata.ts`], so B's rollups (below) would be queried by our own
  dashboards, not by HyperDX.
- **Exemplar FilteredAttributes.** The prototype doesn't carry
  `Exemplars.FilteredAttributes`: the view returns empty maps. Adding them is
  one more list column. They were empty in A too, for this data.
- **Late series.** A point whose series row hasn't been ingested yet appears
  with empty maps: `ANY LEFT JOIN`. An inner join would hide it instead.
  With `join_use_nulls = 1` in a user profile, the view's Map columns would
  have to become Nullable, which Map can't be [E].
- **A dictionary instead of the join.** `dictGet` per row for five maps
  costs at least what the join does. It was not measured, because HyperDX's
  `SELECT *` would pull every attribute anyway.
- **A parameterized view doesn't help,** because HyperDX doesn't pass
  parameters.

## Downsampling (5-minute min / max / sum / count / last per series)

`sql/rollup.sql` builds each rollup once over the full 6 h, as the body of
the materialized view it would be. Results are in `results/rollup-*`:

| | build CPU, µs per raw point [M] | stored, B per rollup row [M] | B per raw point [M] | query |
| --- | --- | --- | --- | --- |
| **B**, number: `AggregatingMergeTree` on `(MetricName, ServiceName, series_id, 5-min)` | **0.41** | 9.4 | **0.94** | join the series table as the points queries do |
| **B**, histogram: `argMaxState(Count, Sum, BucketCounts)` (cumulative, so "last" is the rollup) | **1.71** | 27.3 | 2.7 | as above |
| A, number: key `cityHash64(ResourceAttributes, Attributes)`; both maps carried as `SimpleAggregateFunction(any)` so it stays filterable | 8.8 | 42.4 | 4.2 | self-contained |
| A, histogram | 11.9 | 62.0 | 6.2 | self-contained |
| C: TimeSeries | no rollup support [D]. An MV on the inner samples table keyed by `(id, 5-min)` is B's number rollup with `id` for `series_id` [E] | | | PromQL over it needs a second TimeSeries table |
| D: Prometheus | none in the TSDB [D]: Thanos and Mimir compactors, or VictoriaMetrics Enterprise | | | |

- **As a materialized view at ingest,** B's rollup adds at most 0.4–1.7 µs
  per point, and less without the read [E].
- **A's rollup work is dominated by reading and hashing the maps.** As an MV
  that shrinks, but the maps must still be hashed and carried [E: 2–4
  µs/point].
- **Retention.** Keep B's series table longer than the points: TTL on
  `LastSeen`. The rollups need the series rows for their whole retention.

## What B needs in the edge and the importer

**Edge** (`seriesenc/` is the prototype):

1. **Series id and points.** Per point: sort the point attributes, hash them
   with the per-scope prefix (xxh3), look the id up in the cache, and append
   a narrow row.
2. **Series rows only for new series.** They are built only for series not
   announced in the current window (hourly, on `TimeUnix`). In steady state
   that is 1 series row per 120 points: 0.15 B/point of Parquet and 0.11
   µs/point of central CPU [M].
3. **Cache lifetime.** The cache belongs to a producer epoch: a new epoch,
   after a restart or a new log, starts empty and re-announces everything.
   - Memory: about 16 B per series, so about 10k series per node is trivial.
4. **The one new ordering rule: `Announced()` only after the series object
   commits.**
   - Suppose a request's series object fails or is ambiguous, and the
     request is retried in the same epoch. It must re-announce, and it will,
     because the cache wasn't updated.
   - If the cache were updated first, a retried request would send points
     whose series row never lands: permanently unattributed points.
5. **Points and series can commit concurrently.** The ack waits for all of a
   request's objects, as `PushMetrics` does today.
   - Suppose the edge crashes after the points commit but before the series
     object does. The request is NACKed and resent into a new epoch, with an
     empty cache, so the series row is re-announced.
   - So "series before points" is not needed for safety. It would only
     shorten the window in which points show without attributes.
6. **Fewer objects per request.** The fixed central cost (about 16 ms per
   object) argues for:
   - one points object for gauge and sum, whose schemas are identical;
   - one for histograms;
   - exponential histograms and summaries only when present.

   Each Parquet object also has a fixed cost at the edge.

**Importer**:

- **Points lanes** are unchanged: exact-key `INSERT … SELECT`, the
  single-block settings, the dedup token, and the count check against
  `row_ordinal` (FASTPATH).
  - `row_ordinal` costs 0.01 B/point in B's order.
  - With the per-batch-constant partition, a batch never lands partly, so
    the stored `row_ordinal` could be dropped and computed at insert only
    for the count [E].
- **The series lane** (`metrics_series`) needs **no count check and no
  dedup.** The series table is an `AggregatingMergeTree` with `min`/`max`
  and otherwise identical rows, so inserting an object twice changes
  nothing after a merge. The importer can re-insert on any doubt.
- **Query-side tolerance of late series.** A points object may be ingested
  before its series object, because the lanes are independent. Queries
  written for B use an inner join: the point is invisible for seconds. The
  views use a left join: the point shows with empty maps for seconds.

**The manifest-less design** (`../awss3/README.md`, `../model/s3Inline.qnt`,
`../model/s3InlineMetrics.qnt`) needs nothing new in its mechanism:

- **`metrics_series` is one more namespace,** with its own epoch, slots,
  create-only PUT, tombstones and consumer checkpoint.
  - `s3InlineMetrics` already composes N lanes behind a request-level ack.
  - Its `reqAckedImpliesAllCommitted` covers "points acked only when the
    series object committed" as it stands.
- **The series lane's content key must be the object's own hash,** not
  `(request hash, signal)`. The series object's content depends on the cache
  state, so a retry of the same request can produce a different series
  object, or none.
- **What the model would need is a new invariant plus a mutation** [E, not
  written]:
  - the invariant: every ingested point's series id has an ingested series
    row, eventually, from the same producer;
  - the mutation: update the cache before the series commit resolves as
    ours. It should violate the invariant through the
    ambiguous-PUT-then-retry path.
  - This is a model of the cache, not of the log, so it would be a small
    extension of `s3InlineMetrics`.
- **Retention of the log is unchanged.** The series table's TTL (above) is
  central-only.

## Gaps

**Scale:**

- The data is 100k series over 6 h, not 200k, because of disk. The 24 h
  query numbers are extrapolated [E].
- C and D ran on 3 and 4 services.
- There was no real fleet. Values are modelled, not recorded, and compression
  depends on value entropy. Treat stored bytes as ±50% (the same caveat as
  `../parquetgo`).
  - Real gauges with long flat runs would favour B further.
  - Dense real histograms would raise both A and B.

**CPU not measured:**

- merge CPU, and ingest under concurrent merges (the bulk run had merges
  running; the fit mixes it with quieter runs);
- the edge PUT, which pubbench measured at +0.3 µs;
- B-central's series-row deduplication, for example filtering against a
  `Set`: Bc wrote every row.

**Code not written or not measured:**

- **Merging gauge and sum into one points object and table** is recommended
  but not built. Its 4-core estimate is arithmetic on the measured fit.
- **Series id collisions.** It is 64-bit: the chance of any collision among
  10⁹ series ever seen is about 3% [E]. A collision merges two series'
  attributes. A 128-bit id would cost about 0.03 B/point stored (the column
  is 0.03 B/point now) and 8 B/point of Parquet [E].
- **Parquet size.** `series_id` is 8 random bytes per point in the edge
  object, the largest part of B's 11 B/point for counters. A dense per-epoch
  ordinal with the id in the series object would shrink it, but central
  would then have to map ordinals back to ids [E].
- **Exemplar `FilteredAttributes`** are not carried by B's prototype.
- **HyperDX was not run live.** Its SQL was taken from its own test
  snapshots, and its metadata fallback read from `metadata.ts`.
- **VictoriaMetrics** was not built.
- **C through remote-write** was not measured. Its HTTP handler needs server
  config, which this spike must not change. C's edge encoder
  (`cmd/loadts`) is naive, so its 28.6 µs/point is not a fair edge number
  for C.
- **The `TimeSeries` engine and `ALP`** are experimental or beta in 26.10.

## Files

| path | what |
| --- | --- |
| `fleet/` | the dataset generator |
| `seriesenc/` | B's edge encoder (series id, cache, points and series Parquet), with a determinism, cache and window test |
| `central/` | DDL, `s3()` structures, the importer's statements for A, B and Bc; ch-go native client with ProfileEvents; S3 |
| `promconv/` | OTel to Prometheus samples, for C and D |
| `promtsdb/` | D: the Prometheus TSDB bench, its own `go.mod` |
| `cmd/load` | the edge encode, PUT and central insert pipeline for A, B and Bc (`-pods-per-batch`, `-services`, `-rounds`) |
| `cmd/loadts` | C |
| `cmd/edgebench` | edge encode CPU and Parquet size, A against B |
| `cmd/queries` | the query suite |
| `cmd/costprobe` | one-off statement costs |
| `cmd/contribddl` | has the contrib exporter create its DDL |
| `cmd/genstat` | the dataset's shape |
| `cmd/s3clean` | deletes this directory's objects |
| `sql/contrib/` | the contrib v0.161.0 DDL (A's source) |
| `sql/b_tables.sql` | B's tables |
| `sql/b_compat_views.sql` | the compatibility views |
| `sql/queries.sql` | Q1–Q4, H1 and H2 per layout |
| `sql/rollup.sql` | the 5-minute rollups |
| `sql/codecs.sh` | the codec experiment |
| `sql/fixedcost.sh` | the per-object cost breakdown |
| `results/` | raw results (`*.jsonl`, `*.tsv`, `*.txt`) and `fit.py` / `tables.py`, which derive the tables above |

To reproduce (the server and SeaweedFS as in `../parquetgo`):

```sh
go run ./cmd/load -db ml_main -services 20 -rounds 720 -workers 2        # A + B tables
go run ./cmd/load -db ml_m20k -services 20 -rounds 40 -pods-per-batch 40 -workers 1
go run ./cmd/load -db ml_bc -layouts A,Bc -services 20 -rounds 40 -pods-per-batch 40 -workers 1
go run ./cmd/edgebench -services 8 -rounds 130 -pods-per-batch 40
go run ./cmd/loadts -db ml_ts -services 3 -rounds 720
(cd promtsdb && go run . -services 4 -rounds 720 -dir /tmp/prom [-native])
go run ./cmd/queries -db ml_main -vdb ml_view -tsdb ml_ts        # after creating the views
```
