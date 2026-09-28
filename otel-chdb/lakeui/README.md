# lakeui: the lake-first UI, first slice

A static browser app that reads the lake the way
[`../research/lake-ui.md`](../research/lake-ui.md) recommends (option b,
hybrid): it asks the query service for a **plan** (`POST /v1/plan`,
[`../query/README.md`](../query/README.md) §2.2), then **range-reads the
planned objects straight from S3** with presigned URLs, and shows every
answer with its completeness. It replaces nothing: the spike in
[`../lake-ui/`](../lake-ui/) stays as it was. Decision: D24 in
[`../DECISIONS.md`](../DECISIONS.md).

Labels as elsewhere: **[M]** measured here (shared 4-vCPU box, SeaweedFS 4.47,
ClickHouse 26.10, Chromium 141 headless from Playwright 1.56.1, all on
localhost), **[E]** estimate.

## What works

- **Sign-in**: OIDC authorization code + PKCE (S256) from the page, no client
  secret; the access token is the query service's bearer token (the service
  verifies it; the page only shows its claims and expiry). A pasted token is a
  development fallback.
- **Plan → range reads**: for each planned object the page reads the footer
  with one tail read sized from the plan's `size` (8 KiB; no HEAD, which a GET
  URL answers with 403), then only the column chunks the query needs, for the
  row groups whose statistics can match. Every byte is counted per object.
  Engine: **hyparquet 1.31.2 + fzstd 0.1.1, vendored** under `vendor/` (no CDN;
  `npm run vendor:check` proves `vendor/` is what `package-lock.json` pins).
  No DuckDB-WASM in this slice: the three views need a filter, a count, a
  top-N and bucket aggregates, which are a few lines each over hyparquet's
  columns, and DuckDB-WASM reads HTTP objects whole (lake-ui finding 3) and
  costs 9–11 MB of engine; it can be fed the fetched bytes later if a view
  needs SQL.
- **Views** (all over a time range and a cluster, or every cluster in the
  token's scope):
  - **Logs**: `Body` contains (case-insensitive), severities, count, a volume
    histogram, the newest N rows. Two passes: first `Timestamp` +
    `SeverityText` (+ `Body` only if the text filter needs it) for counts and
    candidates; then `Body`/`ServiceName` only from the objects holding the
    rows shown. With N = 0 the second pass doesn't run.
  - **Trace by id**: `TraceId` (with row-group pruning on its statistics), then
    the span columns only where it matched; a waterfall and a table.
  - **Metric**: a ClickStack-layout gauge (`metrics_gauge` lane):
    `MetricName`, `TimeUnix`, `Value`, `ServiceName`, bucketed averages per
    service.
- **Completeness on every view** (STPA R-S1, R-S2; X8 rule 3): a banner with
  the source, `complete_through` and the plan; rows, histogram buckets, trace
  spans and chart points at or after `incomplete_from` are drawn incomplete
  (hatched bars, dashed line, open markers, a shaded region, a labelled
  complete-through marker, "incomplete" in the row); a bucket that straddles
  the boundary counts as incomplete. `completeness: unknown` (no, a stale or an
  unreadable watermark) draws everything grey-hatched and says so. A plan
  with `start_complete: false` (GC truncated a lane) makes the result
  incomplete.
- **Re-plan (X8 rules 1–2)** in `src/runner.js`: a plan is reused only before
  `replan_after`; no read *starts* at or after it; any failed read (a 403
  above all) re-plans; re-plans are bounded (3), and then the page shows
  **which objects it lacks and no result** — a partial read is never drawn as
  an answer, and a failed fetch is never an empty result. Reads already done
  are kept by key across re-plans (slots are create-only). A refused plan
  (401/403/413) is rendered as a refusal, with the service's reason.

## What doesn't (yet)

- **No snapshots / as-of**: the service plans from a LIST (no sealer); a later
  plan may include objects this one didn't (rule 4). The page says "no
  snapshot".
- **Namespace-scoped viewers are refused** by the service
  (`namespace_scope_needs_filtering_reader`): raw objects hold every
  namespace. They need `/v1/query` (not wired into this UI).
- **No trace-id maplet or term index**: trace by id and text search read the
  `TraceId`/`Body` column of every planned object. On raw edge objects that
  is most of the object (below); on large compacted files the row-group
  pruning pays off, on small raw slots it cannot.
- **Page index / offset index** reads are off (hyparquet's `usePageIndex`),
  and there is one row group per edge object, so pruning inside an object is
  by row group only.
- Per-cluster `complete_through`: the plan's watermark is the fleet minimum
  (query README §7 item 6), so a one-cluster view can be more conservative
  than it needs to be.
- Only Chromium is tested; no WAN or throttled-link numbers; no metric
  catalog (the gauge name is typed); no histogram/sum metric types.
- `config.json` in this directory is an example; a deployment serves its own
  (`query_url`, `issuer`, `client_id`, `clusters`, `metric`). The IdP must
  allow the page's origin (CORS on discovery and token) and register the
  redirect URI; the bucket needs CORS for `GET`/`HEAD`, header `Range`,
  exposing `Content-Range`, `Content-Length`, `ETag` (query README §2.2).

## Layout

| path | what |
|---|---|
| `index.html`, `app.css`, `src/app.js` | the page (DOM wiring only) |
| `src/ns.js` | RFC 3339 ↔ BigInt ns; JSON with exact `*_ns` integers (they exceed 2^53) |
| `src/oidc.js` | discovery, PKCE, code exchange, claims |
| `src/planclient.js` | `/v1/plan`: request, answer checks (a plan it cannot trust is refused, not read partly), error kinds, the plan cache |
| `src/rangereader.js` | hyparquet `AsyncBuffer` over one presigned URL: `Range` GETs, size checks, byte counting, typed errors |
| `src/parquet.js` | footer, row-group pruning (a truncated string max never prunes a value that extends it), column reads, BigInt timestamps |
| `src/completeness.js` | segments, row/bucket/result states, banner words |
| `src/runner.js` | the re-plan state machine |
| `src/queries.js`, `src/engine.js` | the three views' reads and merges; plan → read → merge → label |
| `src/charts.js` | SVG histogram, line chart, waterfall |
| `test/` | `node:test` + fast-check, with three real edge objects as fixtures |
| `e2e/` | Playwright against the real stack (`../query/integration/lakeuirig`) |

## Tests

```
npm ci && npm test           # 31 unit and property tests, ~7 s
npm run vendor:check         # vendor/ == the pinned packages
QS_IT_BIN=<dir with otelcol-s3pq and consume> npm run e2e   # ClickHouse :18123, SeaweedFS :18333 (ci/services.sh)
```

**Unit and property tests** (`npm test`, per push in CI as `lakeui`):

- `completeness`: examples, and properties over arbitrary labels: segments
  tile the window in order; nothing after `complete_through` is ever
  complete; unknown makes everything unknown; row and bucket states agree with
  the segments and a complete bucket holds only complete rows; a result is
  complete iff every bucket is, the start is intact and nothing is missing.
- `runner`: 403 → re-plan → re-read with the new URL; no read starts past
  `replan_after`; persistent 403s end `failed` naming the keys; a refused
  re-plan ends `failed` with the refusal. A **property** over random plan
  lifetimes, 403s, network errors, refusals, concurrency and read times
  (1,500 runs): no read starts at or after its plan's `replan_after`; plan
  calls ≤ max re-plans + 1; `ok` ⇔ every object of the reported plan was read
  successfully; `failed` names what it lacks. Mutants (the per-object expiry
  check removed; a failed result reported ok; one extra re-plan) each fail the runner tests.
- `planclient`: exact ns over the wire (a double that lost precision is
  refused), untrustworthy plans refused, each HTTP refusal its kind, the cache.
- `rangereader`: exact ranges, containment cache, a store that ignores
  `Range`, 403/404/network/short/size-mismatch are errors, never short buffers.
- `queries`: against real Go-edge objects, served by a fake ranged store, log
  search equals a brute-force scan (count, newest rows) for random windows,
  texts and severities (property); a count-only query reads < 35 % of each
  object; trace by id and the gauge sum equal brute force; all-403 ends
  failed; one expired URL re-plans.
- `ns`: format ∘ parse is the identity on ns (property); the source-text
  reviver and the fallback agree (property).

**Browser test** (`npm run e2e`, nightly as `lakeui-e2e`, 9 tests, ~2 min of
which 60 s waits for real URL expiry). `../query/integration/lakeuirig`
brings up: its own bucket `lui-…` (with CORS) and database `lui_…`; the Go
edge for clusters `lui-a` (2 pods) and `lui-b` publishing three batches of
logs, spans and gauge points; the Rust consumer ingesting them and publishing
the watermark; a **late batch** for `lui-a` published after the watermark
(so past `complete_through`); the query service built as `cmd/queryd` builds
it (URL TTL 60 s, replan margin 20 s) with the test issuer from
`internal/auth/authtest` plus an authorization-code + PKCE front; a counting
pass-through in front of SeaweedFS that the URLs are signed for; the page. It
removes everything on SIGTERM.

Results [M] (one run, 2026-09-28):

| check | lake UI | ClickHouse |
|---|---|---|
| logs, window closed before `complete_through`: all / `ERROR` + `status=500` / a needle | 18,000 / 508 / 18, banner **complete** | 18,000 / 508 / 18 |
| logs, window past it | 18,600, banner **incomplete**; the 600 late rows (and every bucket after the marker) drawn incomplete | 18,000 (none after `complete_through`) |
| trace by id (two traces, spans in 3 objects each) | 12, 6 | 12, 6 |
| gauge `lui.queue.depth`, points / sum | 360 / 20714.98 | 360 / 20714.98 |
| fleet token, cluster `lui-b` | 6,000 | 6,000 |
| `lui-a` token asking for `lui-b` | **refused** `cluster_not_in_scope` (no count shown) | – |
| namespace-scoped token | **refused** `namespace_scope_needs_filtering_reader` | – |
| URLs really expired (60 s TTL), page clock behind | 4 × 403 from SeaweedFS (seen by the tap), 1 re-plan, same 18,600 | – |
| plan past `replan_after` | new plan before any read, no 403 | – |
| every object GET reset | 3 re-plans (4 plan calls), then **"Not read"** listing the 4 objects, no count | – |
| watermark removed | banner **unknown**, every bucket and row unknown | – |

**Bytes** [M], counted by the page and, identically, by the pass-through
(range GETs only, all 206, no HEAD):

| query | objects | fetched / planned | per object |
|---|---|---|---|
| count of `WARN` logs, no rows (narrow) | 4 | 59,754 / 743,979 B = **8.0 %** | 19.1 KB of 237.7 KB (8.0 %), 2.4 KB of 30.8 KB (7.9 %); **one GET each** (footer tail + `Timestamp` + `SeverityText`) |
| logs, all, newest 50 rows | 3 | 228,982 / 713,160 B = 32 % | + `Body`/`ServiceName` of the objects holding the 50 rows |
| logs with a text filter | 3 | 572,042 / 713,160 B = 80 % | `Body` is 72 % of a log object: text search without a term index reads it |
| trace by id | 3 | 130–155 KB / 169 KB = 77–92 % | `TraceId` is ~47 % of a 56 KB raw trace object; the maplet (lake P2) is what makes this cheap |
| gauge chart | 3 | 28,969 / 29,398 B = 98.5 % | 9.8 KB objects are mostly footer: fetch small objects whole (lake-ui §7) |

Latency on localhost: 50–120 ms per query including the plan [M]; says
nothing about a WAN.

## Running it by hand

```
cd ../query && QS_IT_BIN=… LUI_KEEP= go run ./integration/lakeuirig   # prints LAKEUI_RIG_READY {…"page": "http://127.0.0.1:…/"…}
```

Open the printed page, sign in as `alice` (cluster `lui-a`), `sre` (fleet) or
`shop` (namespace-scoped: refused), and pick "custom" with the printed
`truth.at` … `truth.late_to` window.
