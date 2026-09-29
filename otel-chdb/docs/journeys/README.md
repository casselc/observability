# User journeys: the lake UI, step by step

Short stories a newcomer would want explained, each one told with the real
application: the lake UI ([`../../lakeui/`](../../lakeui/README.md), D24)
signed in through the query service, reading objects the Go edge wrote and the
Rust consumer ingested, on the lake UI rig
([`../../query/integration/lakeuirig`](../../query/integration/lakeuirig/main.go)).
Nothing here is mocked or drawn by hand. Every picture is a screenshot a
Playwright test took **after its assertions for that step passed**, so each
journey is also a test: if the application stops behaving as a page says, the
nightly `journeys` job fails, and the pictures are not refreshed.

| journey | the question | shows | demonstrates |
|---|---|---|---|
| [1. Is this data complete?](complete.md) | Can I trust this count? | the banner, settled buckets vs the hatched tail, a window that is complete and equals ClickHouse, a watermark gone | STPA R-S1, R-S2, H-2, H-5; D24, D26, D29, D30; AMBIGUITY #10 (b) |
| [2. Find a trace](trace.md) | Where is this request, and what did finding it cost? | a trace's waterfall; the lake index ruling objects out; bytes before/after; an unindexed object read, not missed | D27, FORMAT.md §7, R-S1 |
| [3. Not your cluster](scope.md) | What happens when I ask for data I may not see? | a refusal with its reason (never an empty result), a namespace-scoped user refused, the fleet user answered | STPA R-S8, H-6, H-2; D22, D38 |
| [4. Links expire](expiry.md) | What if my page sits open past its URLs' lifetime? | real 403s from the store, a re-plan, the same answer; persistent failures shown as "not read", never as data | AMBIGUITY X8, D24, D30, R-S2, H-2 |
| [5. Late data](late.md) | A row stamped long ago arrives now: where does it go? | a held basis whose part stays fixed while the tail grows; an old, settled bucket turning incomplete; the late rows labelled | D26, D30 (the tail), D31, R-S1, R-S2, H-5, CAST row 26 |
| [6. Cross-filter (**spike**)](mosaic.md) | Could the analytical views cross-filter and keep the marks? | the Mosaic spike's four charts with the tail hatched, a time brush, a pod click, every total exact | D28 (**proposed, not adopted**), R-S1, R-S2, H-2 (the check CAST row 33 added) |

## How each journey is built

- **One rig for all six**, started once by the suite's global setup exactly
  as the lake UI's own e2e starts it: its own bucket and database (prefix
  `jny`), two clusters (`lui-a`: 2 pods, `lui-b`), three batches of logs,
  spans and gauge points ingested by the consumer and covered by the
  watermark, then a **late batch** for `lui-a` received after it (so it is
  the tail of every basis; with `LUI_RICH_SPANS`, spans in it too, for the
  spike's latency chart), URL lifetime 60 s. Users: `alice` (cluster
  `lui-a`), `shop` (`lui-a`, namespace `shop`), `sre` (fleet, via group).
  The journeys run in order, one Playwright test each; journey 5 adds data,
  so only the spike (6) runs after it.
- **Every step asserts, then shoots.** The assertions compare with
  ClickHouse (`central`) where central has the rows, with the rig's truth
  (the late batch's size, the trace ids) otherwise, and with the counting
  pass-through in front of SeaweedFS for bytes and status codes.
- **Pictures are deterministic where the rig allows**: viewport 1200 × 820
  at scale 1, DejaVu Sans / DejaVu Sans Mono forced, animations and the caret
  off, each step clipped to the region it is about. The rig's data is seeded,
  but it is written at the current time, so the **times in the pictures
  change from run to run**; counts, shapes and states do not (the late
  batch's size, the rig's batches and traces are fixed).
- **One GIF per journey**: every step's full viewport with the step's
  region outlined and a caption bar, assembled with ffmpeg.

## Storyboards

Each step: what the picture shows · what the test asserts first.

### 1. Is this data complete? ([page](complete.md), `01-complete.spec.mjs`)

Alice has an incident on cluster `lui-a` and searches its logs for the last
20 minutes. Before she reads a number, she needs to know whether the number
can still change.

1. **Signed in** — the header after the code + PKCE sign-in: `alice
   (clusters lui-a)` and the token's lifetime · the IdP flow completed; the
   page shows the token's claims.
2. **A window that runs to now** — the banner (incomplete, complete through
   T, settled through T − max_lateness, "600 row(s) received after the
   basis"), the stats line (the basis, the tail's objects), the histogram:
   settled buckets solid, the tail's buckets hatched, the "settled through"
   marker · status ok, completeness partial; basis part = central's count;
   tail rows = the late batch (600); every complete bucket ends before
   settled-through; one marker.
3. **The rows** — the newest rows each tagged `incomplete` · the 50 shown
   are all tail rows, all incomplete.
4. **Close the window at settled-through** — banner complete, every bucket
   solid · count = ClickHouse's for the same window; every bucket complete.
5. **The service cannot tell** — the watermark removed: banner unknown,
   everything grey-hatched, "not at a basis: no basis could be issued" ·
   completeness unknown; unpinned (`basis_unverifiable`); every row and
   bucket unknown. (The watermark is restored after the step.)

### 2. Find a trace ([page](trace.md), `02-trace.spec.mjs`)

A support ticket carries a trace id. Alice pastes it in; the trace lives in
one object of the three the window holds.

1. **Without the index** — the waterfall and the stats line: 3 objects
   planned, the bytes fetched · spans = ClickHouse's count for the id.
2. **With the index** — the same waterfall; stats: "1 narrowed, 1 not
   indexed (read whole), 2 ruled out" (the unindexed one is the tail's span
   object), fewer bytes, fewer range GETs · same spans; index pruned > 0;
   bytes fetched strictly fewer; the page's byte count = the pass-through's.
3. **A text search the index has not caught up with** — a word from one
   batch: the late object is read whole ("not indexed"), never skipped · count
   ≥ central's (the late batch is lake-only); scan > 0.
4. **After an indexer pass** — the same search: nothing "not indexed", the
   same count · scan = 0; bytes ≤ before.

### 3. Not your cluster ([page](scope.md), `03-scope.spec.mjs`)

Alice's token names cluster `lui-a`. She picks `lui-b` from the list anyway.

1. **Refused, with the reason** — the banner: "The query service refused
   this plan (`cluster_not_in_scope`) … This is a refusal, not an empty
   result." · status refused, reason `cluster_not_in_scope`, no count
   element on the page.
2. **A namespace-scoped user** — `shop` (namespace `shop` in `lui-a`) asks
   for logs: refused `namespace_scope_needs_filtering_reader` (raw objects
   hold every namespace) · reason as named, no count.
3. **The fleet user** — `sre` asks for `lui-b`: answered · count = ClickHouse's
   count for `lui-b`.

### 4. Links expire ([page](expiry.md), `04-expiry.spec.mjs`)

The plan's URLs live 60 s here (300 s by default). A page that holds a plan
past that must not turn a 403 into "no data".

1. **A plan and its answer** — the banner and the stats line; the read log
   opened: the plan, its reads · status ok.
2. **The same plan, after its URLs expired** — (the test waits until the
   URLs really expire, then replays the recorded plan answer as a browser
   with a slow clock would hold it) the read log: `read_error … 403`, then
   `replan read_error`; the banner: the same count · real 403s from the
   store (seen by the pass-through too), one re-plan, same count, same
   tail, same basis, a new request id.
3. **The store stays unreachable** — every read reset: three re-plans, then
   the banner "failed … No result is shown" and the list of objects not
   read · status failed; 4 plan calls; missing = every object; no count.

### 5. Late data ([page](late.md), `05-late.spec.mjs`)

Alice holds her basis (the checkbox) so that re-running gives her the same
answer while she writes up the incident. Then a pod that was cut off comes
back and delivers rows stamped 18 minutes ago.

1. **Held basis, before** — the histogram with the bucket two minutes into
   the window solid (settled long ago); stats: the basis part answered from
   the cache · basis kept; every object of the basis part cached; only the
   tail read again (pass-through).
2. **The late rows arrive** — `/rig/more` sends 200 rows for `lui-a` with
   event times two minutes into the window; the same query at the held
   basis: that old bucket is now hatched, the banner counts more rows
   after the basis · same basis, same `objects_hash`, basis part = central's
   18,000; tail = 600 + 200; the bucket incomplete although its time is long
   before settled-through; central still has none of them.
3. **Zoom to the late rows** — the window around those two minutes: rows of
   the basis (complete) interleaved with the late rows (tagged incomplete) ·
   the late rows are exactly the tail rows; all the others complete.

### 6. Cross-filter, with the completeness marks: a spike ([page](mosaic.md), `06-mosaic.spec.mjs`)

Labelled as a spike everywhere it appears: D28 is proposed, not adopted.
The Mosaic page loads the same plan (basis and tail) through the lake UI's
range reader into DuckDB-WASM and draws four cross-filtered charts.

1. **Loaded** — log volume by severity with the hatched band and the
   "settled through" rule; logs by pod, span latency and spans by pod with
   hatched (tail) parts · the basis part = central's count; incomplete rows
   = the tail's rows; the band, the rule, hatched bars drawn; every chart's
   total = an independent SQL count.
2. **Brush a time range** — the other charts follow · chart B filtered;
   every total exact (pre-aggregation off).
3. **Click a pod** — the span charts filtered by time and pod · the spans
   chart's filter names the pod; every total exact.

## Not shown, and why

- **HyperDX with the completeness banner** (D25, D33): the fork cannot be
  built on this machine (disk), and a screenshot of it would have to be
  faked. The banner component has no standalone render (no Storybook in the
  fork; rendering it in jsdom needs the monorepo's dependencies, the same
  install that does not fit). Left out.
- **Mosaic's URL mode, pre-aggregation and scale** (DuckDB reading the
  presigned URLs itself; brush latency up to 11 M rows): measured by the
  spike's own nightly e2e (`lakeui-mosaic-e2e`) and reported in
  [`../../research/mosaic.md`](../../research/mosaic.md); journey 6 shows
  only the range-reader path, exact.
- **Metric charts**: the gauge view works and is tested by the lake UI's
  e2e, but it tells no story the log view does not already tell.

## Regenerating

The pictures are outputs, never edited by hand. The committed ones are
the `journeys` artifact of nightly run
[18](https://github.com/casselc/observability/actions/runs/36545540422) (`render.sh` on the CI runner at
`e27d67a`, the commit before the one that added them): on the development machine the shared
SeaweedFS was read-only (the disk under its 1 % `minFreeSpace`), so no rig
could start there. Either way of refreshing them is the same script.
**Sizes** (PNG as 8-bit palette images, GIFs 900 px wide, 2.5 s a step):
2.14 MB in all: 22 PNGs, 634 KB (4–52 KB each); six GIFs, 1.50 MB
(`complete.gif` 535 KB with five steps, the others 132–276 KB).

To refresh them locally:

```
cd otel-chdb/lakeui && npm ci
QS_IT_BIN=<dir with otelcol-s3pq and consume> e2e/journeys/render.sh
```

Journey 6 also needs the spike's `vendor/` (`cd lakeui/mosaic && npm ci &&
npm run vendor`, ~40 MB, not committed); `JOURNEYS_MOSAIC=0` skips it.
`render.sh` runs the journeys against a fresh rig (ClickHouse
`:18123`, SeaweedFS `:18333`, as `ci/services.sh` starts them), writes each
step's PNG under `img/<journey>/`, re-encodes them as palette PNGs and builds
`img/<journey>.gif` (ffmpeg). A step's picture is taken only after its
assertions pass, and `img/` is replaced only when every journey passed; a
failure leaves it as it was and fails the script. The nightly `journeys`
job runs the same script and uploads the pictures as an artifact; it does
not commit them. To take the pictures from a run instead, unpack its
`journeys` artifact's `journeys-img/` over `img/`.
