# hyperdx: a real HyperDX against layout B's views (DECISIONS.md §4 risk 4)

HyperDX (ClickStack's UI) had never been run against the compatibility views
that present layout B ([D7](../DECISIONS.md)) as ClickStack's `otel_metrics_*`
tables; its SQL had been taken from its test snapshots. This directory runs
the released HyperDX against a local central fed by the real pipeline, and
records what works, what differs and what is slow.

Labels: **[M]** measured here, on the shared 4-vCPU dev box (another agent's
ClickHouse soak ran alongside: load average 2–4), ClickHouse 26.10.1.618 and
SeaweedFS on localhost. **[D]** read in HyperDX's source at the pinned commit.
**[E]** estimated.

## Result

**Everything a user does in HyperDX works through the views, with the same
numbers as on the stock tables.** Nothing HyperDX sent failed: 3,390 statements,
all `SELECT`, `WITH`, `DESCRIBE` or `EXPLAIN`, no errors, no writes [M]. Charts
of every metric type HyperDX supports, group-bys and filters on attributes and
resource attributes, dashboards, and alerts give the same results on both
(alerts fired for the same groups with the same values).

**One thing will break at fleet scale: the metric-name picker.** HyperDX
2.39.1 lists names from the primary index (`mergeTreeIndex()`), which a view
cannot serve, so on the views it falls back to a `GROUP BY MetricName` over
every point of the last 1–3 days. That is 15.6× the stock cost here (264 ms
against 17 ms at 6 M points) and it grows with the points in the window
(about 44 ns per point on this box [M]), so it passes HyperDX's 60 s
`max_execution_time` at about 1.4 G points in the window [E], where the mid
scenario writes 110 G points a day. The proposed `(MetricName, ServiceName)`
helper answers the same question in 16 ms [M], but **HyperDX cannot be pointed
at it**: the picker reads the source's own metric table.

The rest is a constant factor, not a failure: through the views, a chart
explorer page costs **2.95×** the server time of the stock tables (11.1 s
against 3.8 s over 17 scenarios) [M], of which the chart queries themselves
are 1.46×, the picker 11.7× and HyperDX's metadata probes 3.0×. Dashboards
and alerts do not load the picker: alert evaluation costs 1.05× [M].

Of the four degradations DECISIONS.md expected: the **picker** is confirmed
and is worse than recorded (it scales with retention in the window, not
1.26 s flat); **map filters** are confirmed (3.3× on a selective
resource-attribute filter); **no writes** is confirmed and costs nothing,
since HyperDX never writes; **no rollup acceleration** does not apply,
because HyperDX 2.39.1 has no rollup acceleration for metric sources at all
(only log and trace sources take `materializedViews`) [D].

## What was run

| | version | how |
|---|---|---|
| HyperDX | **2.39.1** (latest release, 2026-09-19; `hyperdx/hyperdx:2.39.1`, `sha256:619783ca66d6c81f72a0106f4013d65865ac42e064491b19c0019ffec268f316`); source read at `hyperdx@885d30c` (`CODE_VERSION=2.39.1`) | Docker, host networking, the app + API + alert-task image (149 MB compressed, 622 MB on disk), pointed at our ClickHouse; not the all-in-one image (549 MB, bundles its own ClickHouse) |
| MongoDB (HyperDX's app state) | 5.0.32-focal (`sha256:3b6c281e…cf9fd7`), the version HyperDX's compose file pins | Docker, host networking, `:27117` |
| ClickHouse | 26.10.1.618, the shared server on `:18123` | HyperDX's connection goes through `scripts/chproxy.py` (`:18124`), because the server runs without a config file and has no `system.query_log` |
| Pipeline | Rust edge `otap-s3pq` and consumer `consume`, release builds of `../otap-rs` from the shared target directory (2026-09-26, HEAD `b0dfa71`) | two edges: layout B, and ClickStack tables for the comparison |
| Browser | Playwright + Chromium 1194, headless | `scripts/hdx_ui.js` |

Docker runs inside the dev container: `dockerd --data-root <scratch>
--exec-root /run/hdxd --iptables=false --ip6tables=false --bridge=none
--storage-driver=overlay2`, containers on `--network host`. The exec root has
to be short (containerd's socket path is limited to 104 bytes); pulls went
through the agent proxy without extra configuration.

### Data

`scripts/hdxgen` sends OTLP/HTTP protobuf to the edges: a five-service shop
(`frontend → checkout → payment, inventory; cart`), with traces that cross
services, logs tied to spans, and metrics of every OTLP type in the
temporalities SDKs send:

| metric | type | temporality |
|---|---|---|
| `container.cpu.utilization`, `container.memory.working_set`, `system.cpu.utilization{state}`, `app.custom.metric.NNN` | gauge | |
| `http.server.request.count{http.route, http.response.status_code}` | sum, monotonic | cumulative |
| `app.orders.placed{payment.method}` | sum, monotonic | delta |
| `db.client.connections.usage{state}` | sum, up-down | cumulative |
| `http.server.request.duration{http.route}` (with exemplars) | histogram | cumulative |
| `rpc.server.duration` | exponential histogram, scale 3 | delta |
| `jvm.gc.pause{gc}` | summary | cumulative |

Every metrics request goes to both edges (`-also-metrics`), so both
databases get the same points through the real edge → S3 → consumer path:

- `hdx_b`: the consumer's `otel_traces`, `otel_logs` and layout B, and the
  views of `../otap-rs/sql/series_views.sql` over it (views in the same
  database);
- `hdx_stock`: ClickStack 2.39.1's own metric tables
  (`sql/stock_metrics.sql`: HyperDX's `00003_otel_metrics.sql` with the
  consumer's envelope columns), filled by the consumer from the
  `clickstack_tables` edge.

Two loads: **small**, 10 pods, 3 h of backfill every 30 s then live (21 k
spans, 7 k logs, 60 k points); **big**, 200 more pods (`k8s.cluster.name=big`)
with 30 extra gauge names each, 6 h (**6.04 M points**, 39 metric names,
8.4 k series). The views return exactly the stock rows: count and
`sum(cityHash64(all columns))` equal for all five types, on both loads [M]
(`big`: gauge 5,066,800, sum 622,272, histogram 237,056, exponential
histogram 59,264, summary 59,264 rows).

## Findings

| # | area | result | cause |
|---|---|---|---|
| 1 | Log search: all, full text, `LogAttributes.*`, `ResourceAttributes.*` + severity, SQL `WHERE` | **works** | not the views (consumer tables) |
| 2 | Trace search (status, service, resource and span attributes); **trace waterfall** with correlated logs; **service map**; Services page | **works** | not the views |
| 3 | Metric sources on the views (created through the API; HyperDX validates metric tables by column names) | **works** | |
| 4 | Gauge charts: avg / max, group by `Attributes[…]`, `ResourceAttributes[…]`, `ServiceName`, Lucene and SQL filters on resource attributes | **works, same results** | results differ run to run on *either* database: HyperDX takes `last_value(Value)` per series and bucket with no order, so it is nondeterministic (the same statement on the stock tables alone gave up to 6 different results over 6 runs) [M]. HyperDX bug, not the views |
| 5 | Sum: cumulative counter (`sum`, `increase`), delta counter, up-down counter | **works, same results** | HyperDX turns a non-monotonic cumulative sum into a clamped rate (`greatest(Value - lag, 0)`) on both [D, M]: a HyperDX semantics issue |
| 6 | Histogram p95 / count, exponential histogram p50 | **works, same results** | |
| 7 | Summary | **not supported by HyperDX**, on both: "no query support for metric type=summary"; summaries are not offered in the picker [M, D] | HyperDX 2.39.1 |
| 8 | Metric-name picker | **works, differs, slow; breaks at scale** (above; §Picker) | views: `assertIndexReadable` refuses a View ("engine View is not a MergeTree", a console error, then the exhaustive fallback) [D, M] |
| 9 | Picker completeness | **differs, in the views' favour**: on the small load the stock picker omitted `app.orders.placed` and `db.client.connections.usage` (a name that sits inside one granule is not in the sparse index); the views' exhaustive list had all names [M]. On the big load both listed all 38 | HyperDX's index browse is a documented subset |
| 10 | Metric catalog ("Browse metrics"), attribute helper panel | **works**, 2–3× slower | the join (§Latency) |
| 11 | Dashboards: 6 tiles (gauge, counter increase, histogram p95, exp. histogram p50, log count, span p95) | **works, same** | dashboards do not mount the picker |
| 12 | Alerts: tile alerts on a gauge and a counter tile (1 min), saved-search alert on ERROR logs, webhook | **works, same**: 100 / 100 gauge and 160 / 160 counter notifications for the same groups and values; 7 gauge values differ in the first decimal, which is finding 4 [M] | the alert task queries from the API server (Node client), 1.05× the stock cost |
| 13 | Writes | **none**: HyperDX sent no `INSERT` or DDL [M] | the views' "no writes" costs nothing to HyperDX |
| 14 | Rollup acceleration | **not applicable**: metric sources have no `materializedViews` or `metadataMaterializedViews` in 2.39.1; those are for log and trace sources [D] | |
| 15 | Kubernetes page | runs without errors on the views, no data (the dataset has no `k8s.*` metrics) | |
| 16 | Heatmap on a histogram metric | not exercised (the URL config tried was not one HyperDX accepts) | |
| 17 | Late series | not re-measured; `ANY LEFT JOIN` gives a point with empty maps until its series row lands, which HyperDX would group under empty attribute values for a few seconds | views (known, D7) |

Found on the way, **not the views**:

- ~~**The consumer's traces and logs tables are not ClickStack 2.39.1's
  schema.**~~ **Resolved** (§Schema): the consumer's DDL is now HyperDX
  2.39.1's own (`../otap-rs/sql/otel_{traces,logs}.sql`), with its text
  indexes, materialized columns, logs sort key and key-value rollup, **minus
  the four `idx_*_attr_key` (mapKeys) indexes** (option 2, the owner's
  choice, for insert cost). HyperDX takes its fast paths on it, live: key
  discovery through the items indexes, **0.12–0.14× the server time** of
  the old tables over nine log and trace scenarios at 3 M rows per signal,
  the same as the full DDL, the same results. The price: insert CPU 2.4–2.6×
  per span or log with the rollup (full DDL 2.9–3.5×), merges 2.0×.
- **HyperDX is growing its own series table.** 2.39.1 carries a
  feature-flagged "unified metrics series table" (`seriesTable` on a metric
  source, enabled by a team flag `isMetricsSeriesTableEnabled`; required
  columns `Date, MetricName, SeriesHash, ServiceName, MetricType,
  Temporality, ResourceAttributes, Attributes`, and a `SeriesHash UInt64` on
  every metric table to join on) [D: `app/src/source.ts`,
  `useMetricsSeriesTableAvailability.ts`]. At `885d30c` only the source form
  and its validation exist; no query uses it. It is close to layout B (a
  per-day series table joined on a 64-bit hash), so it is the route to a
  HyperDX-native B, and worth tracking.

## Latency

`scripts/replay.py` re-runs every statement HyperDX sent (from the proxy's
log) on both databases, 7 rounds, the two sides interleaved, median server
time; `scripts/summarize.py` sums them per scenario as a user of each source
pays: the statements sent for the views source timed on the views, against
those sent for the stock source timed on the stock tables (HyperDX renders
different SQL for the two: it adds `toStartOfHour(TimeUnix)` primary-key
filters on the stock tables). Big load, range 16:58–22:58 (6 h) [M]:

| scenario | chart query views / stock ms | metric names views / stock ms | metadata views / stock ms | page total views / stock ms | ratio |
|---|---:|---:|---:|---:|---:|
| gauge avg | 92 / 100 | 245 / 19 | 159 / 58 | 496 / 177 | 2.80 |
| gauge, group by `Attributes['state']` | 232 / 148 | 244 / 20 | 371 / 83 | 847 / 251 | 3.37 |
| gauge, group by pod | 129 / 152 | 250 / 19 | 188 / 64 | 567 / 234 | 2.42 |
| gauge, Lucene filter on one pod | **92 / 28** | 233 / 19 | 191 / 58 | 516 / 105 | 4.92 |
| gauge, SQL filter on `cloud.region` | 93 / 174 | 269 / 19 | 190 / 59 | 552 / 253 | 2.19 |
| gauge, filter on `k8s.cluster.name` | 102 / 108 | 240 / 21 | 189 / 147 | 532 / 276 | 1.93 |
| cumulative counter, sum | 427 / 232 | 277 / 21 | 530 / 91 | 1,235 / 344 | 3.59 |
| cumulative counter, increase | **796 / 338** | 253 / 19 | 495 / 89 | 1,544 / 445 | 3.47 |
| delta counter | 104 / 69 | 237 / 19 | 153 / 63 | 494 / 151 | 3.27 |
| up-down counter | 89 / 58 | 225 / 22 | 155 / 66 | 469 / 146 | 3.21 |
| histogram p95 | 352 / 257 | 232 / 20 | 183 / 89 | 767 / 366 | 2.09 |
| histogram count | 226 / 125 | 252 / 19 | 185 / 92 | 663 / 236 | 2.81 |
| exponential histogram p50 | 196 / 194 | 256 / 21 | 145 / 68 | 596 / 283 | 2.11 |
| summary (HyperDX refuses) | – | 257 / 18 | 98 / 50 | 354 / 68 | 5.18 |
| picker open / search / catalog | – | 394 / 20, 115 / 44 | 236 / 67, –, 649 / 208 | | 3.93, 2.60, 3.12 |
| **17 scenarios** | **3,044 / 2,084 (1.46×)** | **3,978 / 341 (11.7×)** | **4,118 / 1,352 (3.0×)** | **11,140 / 3,778** | **2.95** |
| alert task (same SQL both, 20 statements) | | | | 737 / 701 | 1.05 |

(`results/per-scenario.md`; the same-SQL comparison is
`results/replay-same-sql.md`.) What the numbers say:

- **Chart queries** are close, and cheaper through the views when they read
  every map: the views read 5 MB where the stock tables read 35 MB for the
  same gauge chart (`SELECT *` reads every map of every point on the stock
  tables; the view reads them once per series). They are slower where the
  stock tables prune: a selective resource-attribute filter (3.3×: the stock
  tables' bloom-filter indexes and hour key reach the maps, the view filters
  after the join) and the cumulative counter charts (1.8–2.4×: HyperDX's
  window functions over the join's output).
- **Metadata probes** (unit and description `LIMIT 1`, distinct attribute
  sets, map-key samples; HyperDX sends 10–15 per chart page) cost 13–35 ms
  against 4–11 ms. Without a time filter they read ~185 k rows through the
  view against ~1 k on the stock tables: the join builds the metric's whole
  series set and the left side is read in parallel blocks before `LIMIT`
  stops it. Bounded by the read parallelism and the metric's series count,
  not by retention [M].
- **Metric names** are the outlier (§Picker).

UI wall time per page (3.3–5.5 s) is dominated by the driver's settle waits
and was the same for both sources.

## Picker

`scripts/picker_bench.py`, the four kinds the picker lists, 24 h window
(HyperDX widens a sub-day range to one day and caps it at three [D:
`clampCatalogDateRange`]), big load, median of 9 [M]:

| path | ms | rows read | names |
|---|---:|---:|---:|
| **views: HyperDX's exhaustive `GROUP BY MetricName`** (what 2.39.1 runs on the views, on every chart-explorer mount) | **263.5** | 11,727,780 | 38 |
| views, searching (`ILIKE '%http%'`) | 110.6 | 11,710,908 | 2 |
| **stock: HyperDX's `mergeTreeIndex()` browse** | **16.9** | 777 | 38 |
| stock, exhaustive (what stock pays when searching) | 45.7 | 5,986,900 | 38 |
| stock, searching | 36.9 | 5,986,900 | 2 |
| B's points tables directly, no join | 60.4 | 6,394,736 | 38 |
| **helper `otel_metrics_names`** (`sql/metric_picker.sql`) | **15.9** | 5,160 | 38 |

- Through the views it is 15.6× the stock browse, and the join is most of it
  (4.4× the points tables alone; the gauge and sum views also both scan
  `otel_metrics_number_points`, because `MetricType` is not in its sort key).
- **It scales with the points in the window:** ≈ 44 ns per point here, so
  HyperDX's `max_execution_time` of 60 s is reached at ≈ 1.4 G points in the
  window [E, linear], and the mid scenario (1.27 M points/s) writes that in
  18 minutes. The picker's window is at least a day. Searching has the same
  shape on the stock tables too (≈ 6 ns per point), so a HyperDX user typing
  a name hits the same wall on stock at about 7× the volume; browsing on
  stock stays at the cost of reading the index.
- **The helper can't be used by HyperDX 2.39.1.** The picker reads
  `metricTables[kind]`, the same table as the charts, and only a MergeTree
  with `MetricName` in its key passes the index path. Using it needs a
  HyperDX change (a names table on the metric source, or its planned
  `seriesTable`), not a view change. Nothing in the views can make the join
  skip: ClickHouse 26.10 keeps an `ANY LEFT JOIN` whose right columns are
  unused (`query_plan_convert_join_to_in` does not apply to a LEFT join) [M].
- The helper as built: one row per (type, hour, metric, service), a
  `ReplacingMergeTree` fed by one materialized view per points table, 1,296
  rows for the big load.

## Views: no change

Nothing broke, and no result differs because of the views, so
`../otap-rs/sql/series_views.sql` is unchanged and the view conformance was
not re-run for it. Every result difference seen is explained by HyperDX
itself: its unordered `last_value` for gauges (finding 4) and, for the
counter and histogram charts, the live edge (the two consumers ingest the
live stream a moment apart; the same statements re-run give equal results).

## Schema: ClickStack 2.39.1's traces and logs DDL

The finding above, resolved: `../otap-rs/src/central.rs` `create_table` now
returns HyperDX 2.39.1's `otel_traces` / `otel_logs`
(`docker/otel-collector/schema/seed/00002_otel_logs.sql`,
`00005_otel_traces.sql`, the ClickHouse ≥ 26.2 variants; checked out at
`885d30c`, `CODE_VERSION=2.39.1`), from `../otap-rs/sql/otel_*.sql`, and
`create_rollups` their key-value rollup (`00006`, `00007`). On a server,
`system.columns` (names, types, MATERIALIZED / ALIAS expressions, codecs),
`system.data_skipping_indices` and the materialized views equal the seed's,
column for column, but for the four mapKeys indexes option 2 drops [M]; the
full DDL is kept as `sql/clickstack_full_*.sql` for the comparison. At 2.39.1 the seed has **one** rollup per signal, the
key-value one (`*_kv_rollup_15m`); the key rollup HyperDX can also read
(`keyRollupTable`) is not created by it, and HyperDX's source form only
auto-detects the key-value one [D: `source.ts`].

What the consumer keeps on top, and why:

| | ClickStack 2.39.1 | consumer | why |
|---|---|---|---|
| envelope, `content_key`, `PROJECTION by_content` | – | added | the count check and repair (exactly-once) |
| `PARTITION BY` | `toDate(Timestamp)` | `toDate(received_at)` | batch-constant: an object is one part, the count check prunes on `_partition_value` |
| `non_replicated_deduplication_window` | – | 1000 on the table **and on the rollup table** | the dedup token of an exact retry; on the rollup table because 26.10 runs the view for a deduplicated block and dedups the view's block only if its target has a window: without it an exact retry counted twice in the rollup [M] |
| TTL | `toDate(Timestamp) + 30 days` (tables and rollups) | none | retention is the operator's; `toDateTime(received_at) + n` drops whole partitions (`central-replicated/scripts/ddl.py`) |
| `idx_res_attr_key`, `idx_span_attr_key` / `idx_scope_attr_key`, `idx_log_attr_key` (text indexes on `mapKeys()`) | yes | **dropped** (option 2) | insert cost; HyperDX finds map keys through the `*_attr_items` indexes instead (§Option 2) |

`create_rollups` is not yet run by the consumer: `ensure()` issues one DDL
statement per lane (`src/consumer/sql.rs`). Until it runs the rollup
statements after the table, HyperDX falls back to scanning for the native
columns' values (the text-index paths work without the rollup).

The contrib exporter v0.161.0 (what "ClickStack-shaped" rows were defined
against) differs from the seed: `__otel_materialized_*` instead of
`__hdx_materialized_*`; logs `idx_*_attr_value` on `mapValues` instead of
the `*AttributeItems` ALIAS columns and their items indexes; traces with
bloom filters only (no text indexes, no `SampleRate` or rum columns), Nested
declared as such; no rollups (a `trace_id_ts` lookup table instead); no TTL
unless configured; no `enable_block_number_column`. Columns and types are the
same, so the edge's objects fit both.

**HyperDX takes its fast paths** (the first pass: pre-alignment against the full DDL) [M]. Both schemas as HyperDX sources
(`scripts/hdx_setup.py --schema`), the same data through two real consumers
(the consumer's INSERT unchanged), the UI driven through nine log and trace
scenarios, and each side's SQL replayed on 3.0 M spans and 3.0 M logs
(`results/schema-replay.md`):

| | pre-alignment | ClickStack DDL |
|---|---|---|
| map key discovery | `sampledKeys` scans of the map (23 statements, 5.7 M rows) | `mergeTreeTextIndex` on `idx_*_attr_items` (34 statements, 3 k rows) |
| filter on an attribute | `Map['k'] = v` scan | `has(*AttributeItems, 'k=v')`: 16 of 383 granules |
| full text | `hasToken` ×2 + `LIKE`, no index | `hasAllTokens`: 6 of 383 granules (`idx_lower_body`) |
| native-column values | `groupUniqArray` scans (13 statements, 9.7 s) | the key-value rollup + fewer scans (0.9 s) |
| **server time, 9 scenarios** | **11.9 s** (17.0 s in a second run) | **2.1 s** (2.3 s): 0.18× (0.14×) |
| rows read | 100 M | 49 M |

Every result-list statement both sides sent for the same window returns the
same rows. On the small data alone (23 k rows) the new schema is slower
(6.9 s against 5.1 s, 475 statements against 402): the fast paths are extra
statements (`system.parts`, `mergeTreeTextIndex`, the rollup) that pay off
only once scans cost more than they do.

**What the full DDL costs** (the first pass, before option 2; `results/schema-insert.md`; private server, load 1.6–2.3;
one-object 10k-row statements as `bench/clean` block 2, same objects, 7
reps interleaved; median [range]):

| | pre-alignment | ClickStack DDL, table only | + rollup view (what ships) |
|---|---:|---:|---:|
| insert µs/span | 3.52 [3.05–4.06] | 13.62 [12.88–15.00] | **15.17** [13.89–16.88] |
| insert µs/log | 3.10 [2.97–3.34] | 6.96 [6.48–7.07] | **9.47** [9.05–10.13] |
| stored B/span (after OPTIMIZE FINAL; of it indexes) | 23.7 | 19.2 (7.5) | 19.2 |
| stored B/log | 15.0 | 15.2 (7.2) | 15.2 |
| merge µs/span, 20 parts → 1 | 2.61 | 5.38 | 4.89 |
| merge µs/log | 3.20 | 5.32 | 4.79 |

- The insert cost is the text indexes (spans: 4.0 → 13.6 µs without them /
  with them; logs 3.7 → 6.2), the rollup view (+1.5 per span, +2.5 per log:
  14 `UNION ALL` branches for logs), and ZSTD (+0.4). No single index
  dominates (+0.4–1.5 µs each, alone), but they are not additive: without
  the two `mapKeys` indexes a span costs 7.0, without the two items indexes
  6.6 [M]. HyperDX's key discovery can use either (`getMapKeys`: the key
  index first, then the items index split at `=`) [D], so dropping the
  `idx_*_attr_key` indexes is the cheapest deviation if the CPU matters (done: §Option 2).
- ZSTD(1) halves the data (spans 23.7 → 11.4 B without indexes), and the
  text indexes add it back for logs; TraceId and the attribute items are
  the big indexes. On hdxgen's richer rows (10 resource attributes) spans
  store 79.5 → 57.9 B and logs 79.6 → 80.0 B.
- Merges cost 1.6–2.0× per row, most of it ZSTD (without text indexes and
  codecs: 2.6 / 3.7; with ZSTD only: 4.1 / 4.9).
- ClickStack's partition key would have cost nothing on queries here: over
  7 days, the same HyperDX-shaped statements read the same marks with
  either key (`results/schema-partition.txt`), because both sort keys start
  (logs) or end (traces) with time and the primary index prunes the other
  days' parts.
- The count check still reads the projection (`ReadFromMergeTree
  (by_content)`) and prunes on `_partition_value` (2 of 6 parts read) on
  the new tables [M]; `correctness.py` (32 checks incl. the rollup's
  counts) passes, and the same objects give the same stored rows in every
  written column, old table and new [M].

### Option 2: without the mapKeys indexes

The owner's choice after the first comparison below: ClickStack 2.39.1's DDL
minus `idx_res_attr_key`, `idx_span_attr_key`, `idx_scope_attr_key` and
`idx_log_attr_key`. HyperDX 2.39.1 uses a mapKeys text index for one thing,
map key discovery (`getMapKeys`); with none it reads the map's items index
(`*AttributeItems`, `k=v` tokens) and splits the tokens at `=`; query
rendering, value discovery and attribute filters use only the items index
[D: `metadata.ts` 978–1027, `queryParser.ts` 1750–1766, 1992].

**Live, HyperDX takes that path** [M]: HyperDX 2.39.1 in Docker against
three databases holding the same rows (count and hash of every written
column equal): the pre-alignment DDL, the full ClickStack DDL, option 2; the
nine scenarios twice per schema. Every one of the 46 key-discovery
statements it sent for the full DDL (`SELECT token AS key FROM
mergeTreeTextIndex(…, 'idx_*_attr_key')`) it sent for option 2 as `SELECT
splitByString('=', token)[1] AS key FROM mergeTreeTextIndex(…,
'idx_*_attr_items')`, and they return the same keys for every map. The rest
of its SQL is identical. Each side's statements replayed on its own database
(3.0 M spans and logs; `results/schema3-replay.md`, shared server,
max_threads 2):

| | pre-alignment | full ClickStack | option 2 |
|---|---:|---:|---:|
| server time, 9 scenarios | 17.0 s | 2.12 s (0.12×) | 2.29 s (2.12 s without one extra histogram the UI sent in that run) |
| rows read | 114 M | 44 M | 48 M |
| map key discovery (23 statements) | 23 map scans: 648 ms, 26 M rows | mapKeys index: 128 ms, 334 index rows | items index: 146 ms, 56 k index rows |
| full text (`hasAllTokens`, 9) | `hasToken`: 263 ms, 9.2 M rows | 111 ms, 456 k rows | 110 ms, 409 k rows |
| attribute filters (`has(*AttributeItems)`, 23) | – | 268 ms | 256 ms |
| result lists equal to pre-alignment | | 26 of 26 | 26 of 26 |

**What it costs**, three ways (`results/schema3-insert.md`; shared server,
loaded box, 1-min load 5.6–20; one-object statements, 6 reps interleaved;
median µs per span / log; merge by clickhouse-local on the same 20–21 parts):

| | pre-alignment | full ClickStack + rollup | option 2 + rollup (ships) | option 2, table only |
|---|---:|---:|---:|---:|
| insert, bench-v objects (10k rows) | 3.37 / 3.06 | 12.81 / 7.48 | **9.11 / 7.12** | 6.22 / 4.98 |
| insert, hdxgen objects (9.6k spans, 3.2k logs, 10 resource attributes) | 3.89 / 7.20 | 9.96 / 25.20 | **8.07 / 21.06** | 5.47 / 12.45 |
| merge, bench-v | 2.40 / 2.84 | 5.40 / 4.74 | 4.85 / 5.00 | |
| merge, hdxgen | 2.10 / 4.72 | 5.14 / 9.14 | 4.77 / 8.01 | |
| stored B, bench-v | 23.7 / 15.0 | 19.2 / 15.2 | 18.9 / 15.2 | |
| stored B, hdxgen | 82.1 / 68.8 | 61.9 / 66.4 | 61.8 / 66.4 | |

- Option 2 takes a fifth to a third off the full DDL's insert cost for
  spans (−19% hdxgen, −29% bench-v) and 5–16% for logs. The spread is wide (a busy box:
  full-DDL spans 8.4–15.3 µs), and the first pass (private server, idle
  box) put the full DDL higher (13.6 µs/span table only) and the mapKeys
  indexes' share at half of it.
- The rollup view costs +2.6–2.9 µs/span, and for logs +2.1 µs at 10k rows
  but +8.6 µs at 3.2k rows per object: mostly a per-statement cost (the
  view's 14 `UNION ALL` branches), which larger objects dilute.
- Merges are 2.0× and nearly all ZSTD and the items indexes; the mapKeys
  indexes are small (0–0.3 B/row, low-cardinality keys), so dropping them
  changes neither merges nor bytes much.
- Sizing (the calculator's weights, 0.75 spans + 0.25 logs, scaled to its
  idle-box baseline): `usRow` 3.43 → **9.0** (×2.40–2.62; table only 6.2;
  full DDL 12.0), `mergeRow` 10.1 → **20.1** (×1.95–2.02; full 21.6),
  `bSpan` ×0.75–0.80, `bLog` ×0.97–1.01. Mid scenario: 91 → **151 vCPU**
  (3 × 2 nodes; full DDL 169), 992 → 846 TB with `bSpan` 64 / `bLog` 61.
- `correctness.py` passes on the option-2 tables (32 of 32, rollup counts
  included).

Reproduce (three-way): `scripts/schema_load.py` (or `scripts/run.sh
schema-dbs schema-edge schema-consumers schema-gen`), `scripts/run.sh
dockerd hyperdx chproxy schema-setup`, `hdx_ui.js` per side (`IDS=hdx_ids_<side>.json
TAG_PREFIX=<side>1-`), `scripts/schema_replay.py chproxy.jsonl --sides
old:old1:hdx_old:hdx_old,full:full1:hdx_full:hdx_full,new:new1:hdx_new:hdx_new`,
`scripts/schema_bench.py` (`CPU_SOURCE=client` on a server without
`query_log`) and `scripts/schema_merge.py`. The first, two-way run:
`hdx_ui.js` with `IDS=hdx_ids_old.json TAG_PREFIX=old1-` and
`IDS=hdx_ids_new.json TAG_PREFIX=new1-` (`'^(logs-|traces-|trace-waterfall)'`),
`scripts/schema_replay.py chproxy.jsonl`, and `scripts/schema_bench.py`.

## Reproduce

```sh
W=<scratch>/hdx B=$W/bin                      # otap-s3pq and consume from ../otap-rs (cargo build --release),
(cd scripts/hdxgen && go build -o $B/hdxgen .) # and hdxgen
W=$W B=$B scripts/run.sh dockerd
docker pull hyperdx/hyperdx:2.39.1 && docker pull mongo:5.0.32-focal
W=$W B=$B scripts/run.sh hyperdx chproxy sink edges consumers gen
W=$W B=$B scripts/run.sh setup                 # views, picker helper, HyperDX user/sources, dashboards, alerts
W=$W B=$B scripts/run.sh big                   # then wait for both consumers to catch up
cd $W
NODE_PATH=$(npm root -g) IDS=hdx_ids.json OUT=ui node <this dir>/scripts/hdx_ui.js                 # 3 h range
NODE_PATH=$(npm root -g) FROM_MS=… TO_MS=… TAG_PREFIX=big- POD=big-payment-d889-3 OUT=ui-big \
  node <this dir>/scripts/hdx_ui.js '^(picker|m-)'
python3 <this dir>/scripts/replay.py chproxy.jsonl --rounds 7 --out replay.json --md replay.md
python3 <this dir>/scripts/summarize.py replay.json --exclude 'INTERVAL 1 minute'
python3 <this dir>/scripts/picker_bench.py --from-ms … --to-ms …
W=$W scripts/run.sh down                      # and DROP DATABASE hdx_b, hdx_stock; purge hdx-otel
```

Disk: the two images take 1.4 GB, Mongo and the logs a few MB, the two
databases 150 MB for the big load.

## Files

- `scripts/hdxgen/`: the OTLP generator (Go, pdata).
- `scripts/run.sh`: the stack, step by step.
- `scripts/setup_db.py`: stock tables, the views over B, the picker helper.
- `scripts/hdx_setup.py`, `scripts/hdx_dash.py`: HyperDX user, connection,
  sources, dashboards, webhook, alerts, through HyperDX's API.
- `scripts/chproxy.py`: the logging proxy standing in for `system.query_log`.
- `scripts/hdx_ui.js`: the headless UI driver (tags each scenario in the proxy).
- `scripts/replay.py`, `scripts/summarize.py`, `scripts/picker_bench.py`:
  timing and result comparison.
- `scripts/schema_replay.py`, `scripts/schema_bench.py`,
  `scripts/schema_merge.py`, `scripts/schema_load.py`,
  `sql/pre_alignment_traces_logs.sql`, `sql/clickstack_full_*.sql`: the
  schema comparison (§Schema).
- `scripts/webhook_sink.py`: the alert webhook sink.
- `sql/stock_metrics.sql`: ClickStack 2.39.1's metric tables with the
  consumer's envelope columns. `sql/metric_picker.sql`: the helper.
- `results/`: `schema3-replay.md` / `.json`, `schema3-insert.md` (three
  ways, §Option 2), `schema-replay.md` / `.json`, `schema-insert.md`,
  `schema-partition.txt` (§Schema), `per-scenario.md`, `replay-same-sql.md` (big load),
  `replay-same-sql-small.md`, `picker.md`, `ui.json` (per scenario: console
  errors, errors shown, picker options), `alerts.json` (the notifications).
- `screens/`: `picker-b.png` (the picker through the views),
  `picker-b-catalog.png`, `m-gauge-where-res-b.png`,
  `m-sum-cumulative-increase-b.png`, `m-hist-p95-b.png`,
  `m-exphist-p50-b.png`, `m-summary-b.png` (HyperDX's summary error),
  `dashboard-b.png`, `alerts.png`, `logs-res-attr.png`,
  `trace-waterfall.png`, `service-map.png`.
