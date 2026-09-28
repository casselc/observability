# research: Mosaic (vgplot + DuckDB-WASM) for the lake UI's analytical views

The owner asked (2026-09-28): "look at the mosaic project as a possible
duckdb option, https://github.com/uwdata/mosaic". This note evaluates Mosaic
against what the lake UI already is ([`../lakeui/`](../lakeui/README.md), D24:
a static page on `/v1/plan`, hyparquet range reads, X8's re-plan rules,
completeness on every view) and against what [lake-ui.md](lake-ui.md) found
about DuckDB-WASM (§1 findings 2–3: presigned GET URLs answer HEAD with 403;
DuckDB-WASM reads HTTP objects whole). A working spike was built and
measured: [`../lakeui/mosaic/`](../lakeui/mosaic/README.md).

Labels, as in lake-ui.md:

- **[D]**: from a cited source (Mosaic's repository at commit `3c6c5f6`,
  2026-09-27, release 0.31.0; npm registry queried 2026-09-28);
- **[M]**: measured here: shared 4-vCPU box (other agents building),
  SeaweedFS and ClickHouse 26.10 on localhost, headless Chromium from
  Playwright 1.56.1, DuckDB-WASM 1.33.1-dev57.0 (DuckDB v1.5.4). Three full
  e2e runs; the numbers quoted are the last run's, with the spread where it
  matters. Localhost says nothing about a WAN; byte counts do transfer;
- **[E]**: my estimate.

## 1. Summary and recommendation

**What Mosaic is** [D]: a coordinator (`@uwdata/mosaic-core`) that owns every
query of a set of "clients" (charts, inputs, tables), links them with
*selections* (brush, click, crossfilter), consolidates and caches their
queries, and, for aggregate charts under an interval or point selection,
builds **pre-aggregated tables** ("materialized views", the "data cube
indexes") so a brush is answered from a small table instead of the base
data. `vgplot` is its grammar of graphics on Observable Plot. Queries are
DuckDB SQL built by `@uwdata/mosaic-sql`; results come back as Arrow IPC.
Connectors: DuckDB-WASM in the page, or a DuckDB server over WebSocket/HTTP
(Node, Python, Go, Rust implementations in the repo).

**What the spike found:**

1. **Mosaic runs on DuckDB-WASM with data we load ourselves** [M]. lakeui's
   range reader reads only the needed column chunks of the planned objects;
   the rows become Arrow IPC (flechette, which Mosaic already ships) and are
   inserted with `insertArrowFromIPCStream`. DuckDB never sees a URL, needs no
   extension, and X8 (re-plan before expiry, 403 means re-plan, "not read"
   rather than partial) stays in lakeui's tested state machine. Mosaic only
   ever queries two local tables.
2. **Letting DuckDB fetch the URLs still reads every object whole** [M]:
   8 GETs, all `200`, 941,409 B = 100 % of the planned bytes, against 23
   ranged `206` GETs and 180,840 B (19.2 %) for the range reader, same
   tables. The HEAD shim changes nothing (DuckDB 1.5.4 never HEADs these
   URLs: it GETs them whole); with `reliableHeadRequests` and no full reads,
   DuckDB fails to open the files **without sending a single request**. So
   lake-ui.md finding 3 still holds at DuckDB 1.5.4.
3. **DuckDB's own Parquet reader loses the nanoseconds** [M]: the edges'
   `Timestamp` (INT64, TIMESTAMP NANOS, `isAdjustedToUTC=1`) is read as
   `TIMESTAMP WITH TIME ZONE`, i.e. µs; `read_parquet` has no option to keep
   ns. Every column of the two paths' tables matched except `ts_ns`. D24
   requires exact ns for completeness; the range path keeps them.
4. **Cross-filtering is exact, and completeness fits as data** [M]. After
   every brush and click, each chart's drawn total equalled an independent
   SQL count under that chart's filter (asserted without pre-aggregation;
   with it, 0 differences in three full runs, one 2-row difference in a
   development run: §4.3). The completeness layer is ordinary vgplot marks
   over arrays (a hatched band from the first unsettled bucket, a
   settled-through rule), and bars stacked by a per-row state column decided
   by lakeui's `completeness.js` before loading. An unknown watermark greys
   all four charts.
5. **Pre-aggregation is what makes big tables interactive, and it has two
   sharp edges** [M]. Brush latency on the time chart stays at 21–37 ms
   median from 21 k to 12.7 M rows with pre-aggregation, against 43 → 342 ms
   without; the price is an activation (building the cube tables when the
   pointer enters a chart) that grows to 1.4 s at 12.7 M rows, and memory.
   The edges: cube tables are named by a hash of their SQL and created
   `IF NOT EXISTS`, so **after a reload of different rows under the same
   table names the cubes answer for the old rows** (B showed 9,000 where
   3,000 was right); and brush edges are rounded to the pixel.
6. **Its connector cannot sit on `/v1/query`** [M][D]. Mosaic sends DuckDB
   SQL (`epoch_ms`, `**`, base-10 `log`, `DESC … GROUP BY ALL`), and needs
   `exec` statements (`CREATE SCHEMA`, `CREATE TABLE … AS`, `DROP SCHEMA …
   CASCADE`) for its cubes. Of the 58 distinct statements the spike's
   dashboard sent, ClickHouse ran 14 (10 `DESC`, 4 plain selects); every
   cube statement failed; `log(100)` is 2 in DuckDB and 4.6 in ClickHouse, a
   silent difference. `/v1/query` takes one scoped SELECT and answers JSON.
7. **Cost of the engine** [M]: 8.6 MB gzip (37.6 MB raw) to download and
   0.9–1.2 s to start DuckDB-WASM on localhost, before the first chart;
   lakeui's whole page is about 73 KB gzip. First chart 1.6 s (range mode)
   against 43–76 ms for lakeui's own log-volume view over the same window.

**Recommendation** (proposed as D28, not decided):

1. **Adopt Mosaic for the lake UI's analytical views, as an opt-in view
   loaded on demand**: cross-filtered breakdowns over a planned window (log
   volume by severity × service/pod, span latency distributions, error
   rates by service, per-pod comparisons, and later dashboards over
   rollups), where a person brushes and clicks repeatedly over the same rows.
   There, loading once (0.18 MB here) and answering every brush locally
   (0 bytes, 15–40 ms) beats re-planning and re-reading per brush (lakeui:
   82 KB and 68 ms for one brushed sub-window of this data).
2. **Feed it only through the range reader** (`?mode=range` of the spike):
   never let DuckDB fetch the URLs (finding 2: whole objects; finding 3: µs;
   X8 rules 1–2 cannot be kept per object inside one DuckDB statement).
3. **Keep hyparquet-only for search, trace by id and the current three
   views**: they need a filter, a count and a few rows, which need no SQL
   engine, and they must stay fast on a cold page (no 8.6 MB engine).
4. **Rules for the Mosaic view** (all implemented in the spike): decide
   completeness in JavaScript before loading and pass states as columns; drop
   the cube schema and clear the coordinator's cache on every load; hand
   Mosaic our own DuckDB instance (its default `wasmConnector()` fetches
   jsDelivr); cap what a plan may load into the page (§4.4: 1.6 GB of DuckDB
   memory at 12.7 M rows; out of memory at the ≈3.1 GiB wasm32 ceiling).
5. **No Mosaic server connector on the query service.** If server-side
   Mosaic is ever wanted (bigger than a browser), it is a DuckDB server in
   the reader tier with per-viewer cube schemas, not an adapter on
   `/v1/query`; not proposed now.

## 2. What Mosaic is and needs from a data source [D]

Versions: `@uwdata/vgplot`, `mosaic-core`, `mosaic-sql`, `mosaic-plot`,
`mosaic-inputs` **0.31.0** (2026-08-25; 0.24 → 0.31 in five months: young
and moving, pre-1.0); `mosaic-core` pins `@duckdb/duckdb-wasm`
**1.33.1-dev57.0** (npm's `latest` tag, DuckDB **v1.5.4** [M]), `@uwdata/flechette`
2.5.0 (Arrow), `@observablehq/plot` 0.6.17, `d3` 7.9.0. Repository at
`3c6c5f6` (2026-09-27).

- **The connector contract** (`packages/mosaic/core/src/connectors/Connector.ts`):
  `query({type: 'arrow' | 'exec', sql})` returns Arrow IPC bytes (or nothing
  for `exec`). That is all Mosaic needs from a data source: a SQL engine
  that speaks its dialect, can create tables (for cubes), and returns Arrow.
- **The DuckDB-WASM connector** (`connectors/wasm.ts`): with no instance
  given, `initDatabase()` uses `duckdb.getJsDelivrBundles()`, i.e. a CDN;
  given `{duckdb, connection}`, it uses ours. Results bypass DuckDB-WASM's
  Arrow JS decoding (`useUnsafe` → `runQuery` → IPC bytes).
- **The REST/socket connectors** (`rest.ts`, `socket.ts`) POST `{type, sql}`
  as JSON to a DuckDB server (`packages/server/duckdb-server*`); the Go
  server's README warns it permits every origin, including side-effecting
  `exec` over GET.
- **SQL dialect**: `mosaic-sql` has a generic and a DuckDB code generator
  (`sql/src/visit/codegen/{sql,duckdb}.ts`) and nothing else; bins use
  `time_bucket`, time scales `epoch_ms`, log scales `log`/`**`; field
  metadata comes from `DESC <query>` and `DESC <table>` (`util/field-info.ts`).
- **Pre-aggregation** (`core/src/preagg/PreAggregator.ts`): for a client
  whose query is group-by + supported aggregates, under a selection whose
  active clause is an interval (pixel-binned: `floor(scale(x) · pixelSize)`)
  or a point clause, Mosaic creates
  `CREATE TABLE IF NOT EXISTS mosaic.preagg_<fnv(create SQL)>` (`temp: false`)
  grouped by the client's keys and the binned active column, and answers
  updates from it. Tables are built when the pointer enters a view
  (`why-mosaic`). The docs call the schema "a persistent cache … may be used
  across sessions" and say `dropSchema()` "may be needed if the original
  tables have updated data".
- **Caching**: the `QueryManager` caches results by SQL text.
- **Licence**: Mosaic and flechette BSD-3-Clause; DuckDB-WASM MIT (its
  bundled apache-arrow 17 Apache-2.0); Observable Plot and d3 ISC. No
  copyleft; attribution in the bundle (`legalComments: 'eof'`).

## 3. The spike

[`../lakeui/mosaic/`](../lakeui/mosaic/README.md): four charts over two
DuckDB-WASM tables (`logs`, `spans`), cross-filtered: log volume by severity
over event time (brush), logs by pod (click), span latency on a log scale
(brush), spans by pod (filtered by both). Both tables have the same `b0`
(lakeui's bucket start) and `pod` columns, so one crossfilter selection
filters both. It imports lakeui's modules unchanged; the only change outside
the directory is `LUI_RICH_SPANS=1` in the rig (log-normal span durations
and spans in the late batch; unset, the rig's data is as before).

**Completeness** (R-S1, R-S2; D26's two clocks). Mosaic plots are specs of
marks, and a mark's data may be a plain array, so the layer is marks: a
`rect` over `[first unsettled bucket, window end)` with a hatch pattern, a
`ruleX` and `text` at settled-through, and `fill: 'cstate'` with an ordinal
scale whose range is `[colour, url(#hatch), url(#grey)]` on the other
charts. The states are columns (`cstate` per row, `bstate` per bucket),
computed by lakeui's `completeness.js` in BigInt ns before the rows reach
DuckDB. SQL never re-derives the rule; a property test checks the band
starts exactly at lakeui's first unsettled bucket.

**Loading** (`?mode=`): `range` (lakeui's engine → columns → Arrow IPC →
`insertArrowFromIPCStream`), `url` (register each presigned URL under a
plain name, `CREATE TABLE … AS SELECT … FROM read_parquet([...])`, re-plan
on any error, at most 3), `url-shim` (+ lake-ui's HEAD shim in DuckDB's
worker), `&fs=head` (trust HEAD, no full reads).

## 4. Measurements [M]

Data: the rig's window (`truth.at` … `truth.late_to`, 21 min), token of
cluster `lui-a`: 18,600 log rows (600 of them late, past settled-through)
and 2,574 spans (120 late), in 4 + 4 planned objects of 744,326 + 197,083 B.

### 4.1 Bytes and time to first chart

| path | GETs | bytes fetched | of planned | engine start | data (plan + read + load) | charts | first chart |
|---|---|---|---|---|---|---|---|
| Mosaic, **range reader → Arrow** | 23, all `206` | 180,840 | 19.2 % | 937 ms | 1,394 ms | 119 ms | **1,599 ms** |
| Mosaic, **DuckDB reads URLs** | 8, all `200` | 941,409 | 100 % | 1,132 ms | 1,728 ms | 103 ms | 1,902 ms |
| … + HEAD shim | 8, all `200` | 941,409 | 100 % | – | 1,960 ms | – | – |
| … + shim + trusted HEAD | **0** | 0 | – | – | fails: "Failed to open file" ×4 plans per signal | – | "not read" |
| **lakeui, hyparquet only**, log-volume view (count + histogram) | 8 | 92,520 | 12.4 % of the logs | none | 43–76 ms | (SVG) | 43–76 ms |

- The range path reads more than lakeui's own view because the dashboard
  needs more: `ServiceName` and `ResourceAttributes` (the pod) besides
  `Timestamp` and `SeverityText`, and the span columns. Per signal: logs
  117,933 B (15.8 %) in 12 GETs, spans 62,907 B (31.9 %) in 11 GETs.
- The "data" column includes engine start (DuckDB is created on the first
  load); Arrow insert took 111–167 ms per table.
- Engine start and the first chart are localhost numbers: over a WAN add the
  download of 8.6 MB gzip once per browser (1.4 s at 50 Mbit/s [E]).

### 4.2 Bundle

| file | raw | gzip |
|---|---|---|
| `vendor/mosaic.js` (esbuild, minified: vgplot, mosaic-core/sql/plot/inputs, Plot, d3 subset, DuckDB-WASM's JS API with apache-arrow 17, flechette) | 864,399 | 269,791 |
| `duckdb-browser-eh.worker.js` | 773,223 | 189,256 |
| `duckdb-eh.wasm` | 35,913,747 | 8,128,896 |
| `parquet.duckdb_extension.wasm` (URL mode only) | 3,218,307 | 743,153 |
| **range mode total** | **37.6 MB** | **8.59 MB** |
| lakeui's page (hyparquet + fzstd + src), for scale | 285 KB | ≈73 KB |

Inside `mosaic.js` (minified bytes): Observable Plot 199 KB, apache-arrow
186 KB (pulled in by DuckDB-WASM's API although Mosaic reads IPC bytes),
mosaic-plot 66 KB, flechette 46 KB, mosaic-sql 39 KB, duckdb-wasm JS 32 KB,
mosaic-core 31 KB, vgplot 29 KB, d3 modules the rest. The wasm is 94 % of
the download.

### 4.3 Interaction latency and memory

Brush on the time chart (8 moves) and on the latency chart, click on a pod;
median / max per move, ms. Tables scaled by `n` copies of the loaded rows
(same distributions); `duckdb` is `duckdb_memory()` after the run.

| rows (logs + spans) | pre-agg: activation | pre-agg: time brush | pre-agg: pod click | no pre-agg: time brush | no pre-agg: pod click | latency brush (pre-agg / none) | DuckDB memory |
|---|---|---|---|---|---|---|---|
| 21 k (1×) | 45 ms, 3 tables | 37 / 58 | 49 | 43 / 67 | 74 | 23 / 16 | 7.9 MB |
| 423 k (20×) | 93 ms | 30 / 34 | 63 | 57 / 72 | 113 | 21 / 19 | 70 MB |
| 2.1 M (100×) | 295 ms | 29 / 33 | 186 | 105 / 208 | 124 | 14 / 22 | 285 MB |
| 6.4 M (300×) | 593 ms | 32 / 267 | 674 | 229 / 280 | 337 | 23 / 49 | 821 MB |
| 12.7 M (600×) | 1,442 ms | 21 / 99 | 955 | 342 / 628 | 631 | 17 / 70 | 1,625 MB |

- **Pre-aggregation keeps a brush at ≈20–40 ms whatever the size**; without
  it, a time brush grows linearly (343 ms median at 12.7 M rows). Across the
  three runs the 600× no-pre-agg median was 342–463 ms and the pre-agg one
  21–27 ms.
- **A click is not cheap either way at scale**: a point selection on B
  re-filters A, C and D, and with pre-aggregation the click builds new cubes
  for the new active clause (0.9 s at 12.7 M rows).
- **Memory**: DuckDB's own accounting grows ≈128 bytes per row here; the JS
  heap stays at 13–21 MB (Chromium's coarse `performance.memory`). A first
  attempt that accidentally built ≈40 M rows died with "Out of Memory Error:
  could not allocate block … (3.1 GiB/3.1 GiB used)": the wasm32 ceiling.
- **Pixel rounding**: with pre-aggregation the brush's edges are rounded to
  the active chart's pixel bins, so a chart can count the rows of an edge
  pixel differently from the exact predicate: once, in a development run,
  spans-by-pod showed 510 where the exact count was 508 after a latency
  brush; 0 differences in three full runs. Without pre-aggregation every
  check was exact.
- lakeui's way to answer the same time brush is to plan and read again:
  **81,906 B and 68 ms** for the brushed sub-window (3 objects). Mosaic
  answers from memory with no request.

### 4.4 Correctness checks (the e2e, 7 tests, all passing)

- Range mode: log rows = ClickHouse's count for the window + the 600 late
  rows; rows with `cstate = complete` = ClickHouse's count before
  settled-through; exactly the late batch is `incomplete`; spans likewise.
  The page's byte count equals the counting tap's.
- URL mode: every column fingerprint equal to range mode's except `ts_ns`
  (µs); really expired URLs (60 s TTL, the page's plan cache made to believe
  them valid): 2 × `403` at the tap, one re-plan per signal, same tables.
  DuckDB reports the 403 only as "Failed to open file", and it reads all
  objects inside one statement, so X8 rule 1 (no read starts past
  `replan_after`) can only be checked before the statement, and rule 2 can
  only say "the statement failed", not which objects.
- Stale cubes (`?nodrop=1`, a fleet token, the same window loaded for
  `lui-a` then `lui-b`): after the reload the brushed charts showed lui-a's
  numbers (B 9,000, C and D 1,236) where lui-b's were right (3,000 and 628);
  with the schema dropped on load, all equal.
- Unknown watermark: every row and bucket `unknown`, grey band over the
  whole time axis, grey bars, banner "Unknown".

## 5. Could the connector sit on the query service? [M][D]

What it would take, and why not:

- **Dialect.** The 58 distinct statements of the spike's dashboard (load,
  cubes, brushes, a click), replayed on ClickHouse 26.10 over empty tables
  of the same columns: `DESC` 10/10 ran (but ClickHouse answers `name,
  type, …`, not DuckDB's `column_name, column_type, null`, so Mosaic would
  read no types [E]); plain selects 4/6 (the latency bins failed on `**`);
  cube creation 0/7 (`CREATE SCHEMA`); cube reads 0/35; `DROP SCHEMA …
  CASCADE` failed. `epoch_ms` does not exist in ClickHouse; `log` is base 10
  in DuckDB and natural in ClickHouse (same text, different numbers). A
  translating connector would have to rewrite Mosaic's AST to ClickHouse or
  add a ClickHouse code generator to `mosaic-sql`.
- **Cubes are writes.** `/v1/query` takes one SELECT (D22) and refuses
  everything else; cube tables would have to live in ClickHouse per viewer
  (and be scoped, audited, and garbage-collected), or pre-aggregation be
  off (then every brush is a round trip to ClickHouse: the HyperDX path).
- **Results.** `/v1/query` answers ClickHouse's `FORMAT JSON` with the label
  beside it; Mosaic wants Arrow IPC. A connector can convert (flechette's
  `tableFromArrays`), at a cost Mosaic's docs themselves warn about.
- **The label.** The connector interface returns bytes only, and the
  coordinator hands clients a table: there is no channel for "complete
  through T" per result. It would travel beside Mosaic: the connector keeps
  the label of each statement it forwarded (keyed by SQL), and the page
  draws the worst one. With cubes, a cube's rows carry the label of the
  statement that built it, however old: the connector would need to rebuild
  cubes when the label moves.

The practical server-side form is Mosaic's own DuckDB server in the reader
tier, reading the planned objects in-region (where whole-object reads are
cheap) with a cube schema per viewer and session. That is a new stateful
service with the scoping and audit work of D22 over again [E]; not proposed.

## 6. Risks

1. **Young, fast-moving dependency**: 0.x, a release every 2–4 weeks; the
   DuckDB-WASM it pins is a dev build (the npm `latest` tag). Pin both;
   vendor by `npm run vendor` from the lockfile; re-run the e2e on upgrade.
2. **Cube staleness is silent** (§4.4): the rule "drop the schema on every
   load" must be kept by whoever wires a new view; the e2e guards it.
3. **Memory and engine size**: 8.6 MB per browser once, 1.6 GB of wasm heap
   at 12.7 M rows, a hard ceiling near 3.1 GiB. Cap plans loaded into the
   Mosaic view [E: a few million rows], and send bigger windows to the
   reader tier or to rollups.
4. **Precision** if anyone lets DuckDB read the objects (µs), and **pixel
   rounding** at brush edges with pre-aggregation: neither is visible to a
   person; both matter to an exact count.
5. **Two engines in one UI** (hyparquet for search/trace, DuckDB for the
   analytical view): the plan, X8 and completeness code stays single (the
   spike reuses lakeui's modules), but there are two ways to draw a chart.

## 7. Next steps (if D28 is accepted)

1. Move the spike's loader (`columns.js`, `arrow.js`, `overlay.js`) next to
   lakeui's modules and give the lake UI an "Explore" view that loads
   `vendor/mosaic.js` and DuckDB only when opened.
2. A row cap per load, and a "too big: narrow the window or use the reader
   tier" refusal, measured on a throttled link (`tc`) with L2-sized files.
3. Rollup-backed tiles (D15) as Mosaic tables, where pre-aggregation at the
   source and Mosaic's cubes meet.
4. Re-test DuckDB-WASM's own reader (ranges, ns) at each minor release.

## 8. Sources

| What | Where |
|---|---|
| Mosaic repository, release 0.31.0 | https://github.com/uwdata/mosaic (commit `3c6c5f6`, 2026-09-27); npm `@uwdata/vgplot` 0.31.0 (2026-08-25) |
| Connector contract, DuckDB-WASM connector, REST | `packages/mosaic/core/src/connectors/{Connector,wasm,rest}.ts`; docs `api/core/connectors.md` |
| Pre-aggregation | `packages/mosaic/core/src/preagg/PreAggregator.ts`; docs `why-mosaic/index.md` ("build pre-aggregated data tables when the mouse cursor enters a view") |
| SQL dialect | `packages/mosaic/sql/src/visit/codegen/duckdb.ts`, `functions/datetime.ts` (`time_bucket`, `epoch_ms`), `core/src/util/field-info.ts` (`DESC`) |
| Selections | `packages/mosaic/core/src/Selection.ts` (`predicate(client, noSkip)`) |
| Go DuckDB server (origins, `exec`) | `packages/server/duckdb-server-go/README.md` |
| Licences | `LICENSE` (BSD-3-Clause); package manifests in `node_modules` |
| DuckDB-WASM | npm `@duckdb/duckdb-wasm` 1.33.1-dev57.0 (`latest`), 1.33.1-dev64.0 (`next`) |
