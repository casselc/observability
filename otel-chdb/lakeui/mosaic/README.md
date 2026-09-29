# lakeui/mosaic: a Mosaic spike beside the lake UI

A spike, not a product: can [Mosaic](https://github.com/uwdata/mosaic)
(vgplot, its coordinator, DuckDB-WASM, cross-filtering, pre-aggregated
"data cube" tables) serve the lake UI's analytical views over `/v1/plan`?
The evaluation, the numbers and the recommendation are in
[`../../research/mosaic.md`](../../research/mosaic.md); the proposal is D28
in [`../../DECISIONS.md`](../../DECISIONS.md). Nothing in `../src/` was changed:
this page imports lakeui's modules (plan client, range reader, re-plan state
machine, completeness math, OIDC) as they are.

Labels: **[M]** measured here (shared 4-vCPU box, SeaweedFS and ClickHouse
26.10 on localhost, headless Chromium from Playwright 1.56.1), **[E]** estimate.

## What it is

One page (`index.html`, `src/app.js`) with four cross-filtered charts over two
DuckDB-WASM tables, `logs` and `spans`:

| chart | mark | interaction |
|---|---|---|
| A log volume over event time, stacked by severity | `rectY` over lakeui's buckets (`b0`, `b1`) | brush → `$cf` (crossfilter) |
| B logs by pod, stacked by row state | `barX` | click a pod → `$cf` |
| C span latency (log scale), stacked by row state | `rectY` + `bin('dur_ms')` | brush → `$lat` |
| D spans by pod, stacked by row state | `barX` | filtered by `$cf ∧ $lat` |

Both tables carry the same `b0` and `pod` columns, so one crossfilter
selection filters logs and spans alike.

**Completeness on every chart.** The state of every row (`cstate`) and bucket
(`bstate`) is decided in JavaScript, in exact BigInt nanoseconds, by lakeui's
`completeness.js`, before anything reaches DuckDB; SQL never re-derives the
rule. Mosaic marks accept plain arrays as data, so the layer is ordinary
marks (`src/overlay.js`): on the time chart a hatched band from the first
unsettled bucket to the window's end and a "settled through" rule; on the
others each bar is stacked by `cstate` with patterned fills (hatched =
incomplete, grey = unknown). An unknown watermark greys everything.

**Two ways to fill the tables** (`?mode=`):

- `range` (default): `/v1/plan` → lakeui's engine (range reads of only the
  needed column chunks, X8's re-plan rules) → columns (`src/columns.js`) →
  Arrow IPC with flechette (`src/arrow.js`) → `insertArrowFromIPCStream`.
  DuckDB never sees a URL and needs no extension.
- `url`: DuckDB reads the presigned URLs itself (`read_parquet`, the parquet
  extension from `vendor/ext`, `src/urlsql.js`); `url-shim` adds lake-ui's
  HEAD shim to DuckDB's worker, and `&fs=head` trusts HEAD and forbids
  whole-object reads.

Other switches: `?preagg=0` turns Mosaic's pre-aggregation off; `?nodrop=1`
keeps the previous load's cube tables (to show why they must be dropped).

## Run it

```
npm ci --ignore-scripts && (cd .. && npm ci --ignore-scripts)
npm run vendor               # vendor/: esbuild bundle, DuckDB-WASM eh files, the parquet extension (SHA-256 pinned)
npm test                     # 6 node tests (DuckDB-WASM runs in Node for the Arrow test)
QS_IT_BIN=<dir with otelcol-s3pq and consume> npm run e2e   # 7 Playwright tests, ~3.5 min
```

By hand: `LUI_RICH_SPANS=1 go run ./integration/lakeuirig` in `../../query`,
open `<page>/mosaic/index.html`, sign in, paste the printed `truth.at` …
`truth.late_to`. `LUI_RICH_SPANS=1` gives the rig's filler spans log-normal
durations and the late batch spans (unset, its data is as before).

`vendor/` is built, not committed (40 MB of wasm). No CDN at runtime:
`src/app.js` hands Mosaic a DuckDB instance built from `vendor/duckdb/`
(Mosaic's own `wasmConnector()` would fetch jsDelivr's bundles) and points
`custom_extension_repository` at `vendor/ext`.

## Tests

- **Unit** (`test/mosaic.test.js`): the columns loaded from lakeui's real
  edge-object fixtures equal a brute-force read and carry lakeui's row and
  bucket states (the boundary is placed 1 ns after a real row, so an
  off-by-one fails); unknown and complete labels; Arrow IPC into DuckDB-WASM
  (the Node build of the same wasm) keeps exact ns sums, states, doubles and
  types, and an empty load still creates the table; a property (2,000 runs):
  the completeness band starts exactly at the first bucket lakeui calls
  unsettled; the URL mode's SQL carries exact BigInt literals. Two mutants
  (band at `s` instead of its bucket; a row state off by 1 ns) each fail.
  The tail (AMBIGUITY.md #10 (b)): range mode's columns draw a tail part's
  rows and their buckets incomplete; URL mode's SQL, run in DuckDB-WASM over
  a basis file and a tail file it wrote, gives per bucket the same rows,
  tail rows and states as the range reader's columns (mutant `bool_and`
  for `bool_or` fails).
- **Browser** (`e2e/mosaic.spec.mjs`, against the real stack): both modes
  plan at a basis with its tail (D30 amendment): range mode's tables equal
  ClickHouse's counts (the basis part) plus the late batch (the tail),
  exactly the late rows are incomplete, every GET is a ranged 206 and the tap's bytes equal the
  page's; URL mode builds the same tables column by column (timestamps at
  µs, see below) from whole-object GETs, the tail's rows marked from
  `read_parquet`'s `filename` (a tail object the planner could not date
  stays in the tail here: DuckDB does not read its footer, so its rows are
  drawn incomplete, the weaker claim); really expired URLs (403: the plan
  answers replayed past their expiry, since tail plans are never cached)
  re-plan in URL mode too; the shim + trusted-HEAD mode fails to open files and shows
  "not read"; after every brush and click, each chart's drawn total equals an
  independent SQL count under its filter (asserted without pre-aggregation,
  recorded with it); brush latency with and without pre-aggregation at 1×,
  20×, 100×, 300×, 600× the loaded rows; kept cube tables answer for the old
  rows; an unknown watermark greys all four charts (no basis can be issued:
  planned unpinned, and said so); Mosaic's statements
  replayed on ClickHouse.

## Found here

- **DuckDB drops the nanoseconds.** It reads the edges' `Timestamp` (INT64,
  TIMESTAMP NANOS, `isAdjustedToUTC`) as `TIMESTAMP WITH TIME ZONE`, which is
  µs; `read_parquet` has no option to keep them (1.5.4). URL mode decides a
  row's state on the late end of its µs (`ts_ns + 999`), so it errs toward
  "incomplete". The range path keeps ns.
- **Cube tables outlive the data.** Mosaic names a pre-aggregated table by a
  hash of its SQL and creates it `IF NOT EXISTS` in schema `mosaic`; reload
  different rows under the same table names (a re-plan that brings late
  objects, another cluster) and the cube answers for the old rows. The page
  drops the schema and clears the coordinator's cache on every load; the e2e
  shows the wrong counts with `?nodrop=1`.
- **`vg.highlight` breaks on an aggregate** whose group keys do not include
  the selection's column: it selects the predicate as a column (a DuckDB
  binder error). Not used.
- **`Selection.predicate(client)` skips the active clause** for its own
  source client; checks use `predicate(client, true)`.
