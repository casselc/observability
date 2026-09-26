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

- **The consumer's traces and logs tables are not ClickStack 2.39.1's
  schema.** They have the columns, but not the text indexes
  (`idx_lower_body`, `idx_*_attr_items`), the `__hdx_materialized_*` columns,
  the key / key-value rollup tables and their materialized views, or the
  logs sort key `(toStartOfFiveMinutes(Timestamp), ServiceName, Timestamp)`
  (`otap-rs/src/central.rs` `create_table`). HyperDX works on them, falling
  back to scans for full-text search and filter-value discovery. Aligning the
  DDL with `docker/otel-collector/schema/seed/0000{2,5,6,7}_*.sql` is a
  consumer change.
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
- `scripts/webhook_sink.py`: the alert webhook sink.
- `sql/stock_metrics.sql`: ClickStack 2.39.1's metric tables with the
  consumer's envelope columns. `sql/metric_picker.sql`: the helper.
- `results/`: `per-scenario.md`, `replay-same-sql.md` (big load),
  `replay-same-sql-small.md`, `picker.md`, `ui.json` (per scenario: console
  errors, errors shown, picker options), `alerts.json` (the notifications).
- `screens/`: `picker-b.png` (the picker through the views),
  `picker-b-catalog.png`, `m-gauge-where-res-b.png`,
  `m-sum-cumulative-increase-b.png`, `m-hist-p95-b.png`,
  `m-exphist-p50-b.png`, `m-summary-b.png` (HyperDX's summary error),
  `dashboard-b.png`, `alerts.png`, `logs-res-attr.png`,
  `trace-waterfall.png`, `service-map.png`.
