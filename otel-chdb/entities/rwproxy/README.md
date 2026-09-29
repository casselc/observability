# rwproxy: HyperDX's resource-attribute SQL, rewritten onto the entity catalog

A spike, following [`../README.md`](../README.md) (the entity catalog). There,
rows carry `resource_id` + `ResourceResidual`, and `ResourceAttributes` is an
`ALIAS` column on the table (variant **c**) rebuilt from dictionaries. The
catalog pays off only if HyperDX's resource-attribute filters and group-bys
become catalog lookups: unmodified, they are 10–170× slower than on the
ClickStack table (§4.2 there). Its `rw` mode simulated that rewrite by hand.
This directory builds the rewrite as a proxy between HyperDX and ClickHouse,
checks it statement by statement, and measures it.

Labels as in the parent: **[M]** measured here (the shared 4-vCPU dev box,
loaded by other agents; load averages given with each table; ClickHouse
26.10.1.618, queries capped at `max_threads = 2` and 3 GB), **[D]** read in a
source (HyperDX at `885d30c`, `CODE_VERSION=2.39.1`), **[E]** estimate.

## 1. Result

- **It works, and it is exact.** A Go HTTP proxy parses every statement with
  a ClickHouse-grammar parser (799 / 799 of HyperDX 2.39.1's captured
  statements), rewrites `ResourceAttributes['k']` filters, group-bys and
  selects on variant-c tables into `resource_id IN (SELECT resource_id FROM
  resource_kv …)` and dictionary value expressions, splices the result into
  the original text, and passes everything else through byte for byte. In
  **exact** mode every rewritten statement returned what the `ALIAS` column
  returns (hash-equal response bodies, `FORMAT JSON` metadata included) on
  1,452 statements × 2 catalog states: HyperDX's captures, the parent's 15
  shapes and a 632-statement matrix of filter forms, residual keys and race
  rows (§4). **Catalog** mode (`resource_id IN` alone) is exact once the
  catalog is complete and misses the grace-window rows of unknown resources
  before that, as designed.
- **It recovers the catalog's cost.** Through the proxy, the parent's 14
  shapes take 900 ms (catalog) / 1,159 ms (exact) against 1,316 ms on the
  ClickStack table and 14,163 ms on the unrewritten `ALIAS` (§6). Resource
  filters in catalog mode are within 1.1–1.4× of ClickStack; exact mode is
  3.5–5.9× there, still 2–29× faster than the `ALIAS`.
- **The hop is cheap.** Parse + rewrite p50 48 µs / p99 0.87 ms in process
  (rewritten statements p50 146 µs); added wall time per HyperDX statement
  p50 0.5 ms, p90 2 ms on a loaded box; 10–42 MB RSS; ~1 ms CPU per request
  in this container (0.1–0.2 ms in process) (§5).
- **HyperDX live was not run** (memory and disk below the bar); its captured
  SQL was replayed through the proxy instead (§7).
- **Recommendation**: the proxy in exact mode now, fail-open behind a load
  balancer; catalog mode once the announcement lane lands; the same rewrite
  proposed upstream in HyperDX's `metadata.ts` / `queryParser.ts` /
  `renderChartConfig.ts` (§9).
- **Through the query service** (2026-09-28, DECISIONS.md D33): in front of
  the HyperDX adapter, all 69 of its non-EXPLAIN rewrites of the captured
  statements answer, for fleet and cluster- or namespace-scoped tokens,
  equal to ClickHouse with the service's filters (before: 0). The service
  allows the catalog's dictionaries by name and guards every lookup per
  caller (another cluster's `resource_id` reads as absent), and serves
  `resource_kv` scoped by two columns `setup.py kv` now writes, `cluster`
  and `namespace` ([`../../query/README.md`](../../query/README.md) §3,
  §8.4).

## 2. Design

```
HyperDX (browser: @clickhouse/client-web via the API's /clickhouse-proxy;
         API + alert task: @clickhouse/client)          HTTP only
   │  POST /?param_HYPERDX_PARAM_…=…&query_id=…  body: the statement
   ▼
rwproxy :18125 ── parse (ClickHouse grammar) ── find SELECTs on a catalog table
   │             ── replace ResourceAttributes['k'] uses by catalog lookups
   │             ── re-parse the result; anything unknown → forward as is
   ▼
ClickHouse :8123 ── response streamed back unchanged (headers, compression, progress)
```

- **Transport.** HyperDX talks to ClickHouse only over HTTP: the browser uses
  `@clickhouse/client-web` through the API's `/clickhouse-proxy`, the API and
  alert task `@clickhouse/client` (Node); `createNativeClient` in
  `common-utils/src/clickhouse/node.ts` is that HTTP client, not the native
  protocol [D]. So an HTTP proxy in place of the connection's host sees every
  statement. It reads the statement from the body (what HyperDX sends), the
  `query` URL parameter, or a `multipart/form-data` body, which
  `@clickhouse/client-web` sends when the parameters outgrow its URL budget
  (`api/src/routers/api/__tests__/clickhouseProxy.int.test.ts`) [D]; a gzip
  request body is decompressed. A request with both a URL `query` and a body
  (an `INSERT` with its data) is forwarded untouched. The response is copied
  back as it streams, with ClickHouse's headers (`X-ClickHouse-Summary`,
  progress, exception code) and encoding. Credentials pass through.
- **Parser: ClickHouse's grammar, in Go.** The candidates, on the 799
  statements HyperDX 2.39.1 sent in `../../hyperdx`'s captures [M]:

  | parser | parses | fails on |
  |---|---:|---|
  | Rust `sqlparser` 0.59 / 0.63, `ClickHouseDialect`, as sent | 229 / 799 | `{HYPERDX_PARAM_n:Identifier}` (every table name) |
  | same, placeholders masked (below) | 659 / 799 | `GROUP BY … AS alias` (all 90 histograms, the statements that matter most), `DESCRIBE … FORMAT JSON` |
  | Go `github.com/AfterShip/clickhouse-sql-parser` v0.5.6, as sent | 127 / 799 | the placeholders |
  | **same, placeholders masked** | **799 / 799** | – |

  Both parse map access, lambdas, `WITH`, `SETTINGS`, parametric aggregates
  (`groupUniqArray(20)(x)`), `EXPLAIN ESTIMATE` and `FORMAT`; only the
  ClickHouse-specific grammar takes HyperDX's aliased group keys. Hence Go.
  ClickHouse itself (`EXPLAIN AST`, `formatQuery`) parses everything by
  definition, but costs a round trip per statement (or a process per
  statement with `clickhouse-local`) and returns no source positions; it is
  used here only as the judge (§4).
- **Masking.** Each `{name:Type}` placeholder is replaced by an identifier of
  the same length (`p______…`) before parsing, so every byte offset stays
  valid; the parameter's value (from `param_name`) resolves the database and
  table a `SELECT` reads.
- **Splice, don't re-print.** The parser only finds spans; the output is the
  original text with those spans replaced. Everything else (quoting, spacing,
  the placeholders, `FORMAT`, `SETTINGS`) goes to ClickHouse byte for byte.
  The parser's `End()` points at a node's closing delimiter; `start()` /
  `end()` in `rewrite.go` turn that into exact half-open spans and refuse
  node kinds they don't know. The spliced statement is parsed again; if it
  doesn't parse, the original is sent.
- **Scopes.** Every `SELECT` whose `FROM` is exactly one configured table
  (`rw_c.otel_logs`, `rw_c.otel_traces`: variant c) is a scope; its own
  clauses (select list, `WITH` expressions, `PREWHERE`, `WHERE`, `GROUP BY`,
  `HAVING`, `ORDER BY`, `LIMIT BY`) are rewritten, nested `SELECT`s are
  scopes of their own (HyperDX's `WITH sampledData AS (SELECT … FROM table)`
  is one). Joins, a table alias qualifying the column, and a select alias
  that shadows `ResourceAttributes` are left alone.
- **Configuration** (`rwproxy.json`, written by `scripts/config.py` from the
  catalog): the tables, the catalog's `(Key, Value, resource_id)` table
  (`rw_cat.resource_kv`, sorted by `(Key, Value)`), the covered key list
  (re-read every 30 s from `resource_kv`), and for each covered key one
  **value expression** over the normalized dictionaries: the level that
  holds the key (cluster, node, namespace, workload, pod, container), e.g.
  `if(dictHas('rw_cat.d_res', {rid}), JSONExtractString(dictGet('rw_cat.d_ns',
  'attrs', dictGet('rw_cat.d_pod', 'ns_key', dictGet('rw_cat.d_res',
  'pod_key', {rid})))), 'k8s.namespace.name'), '')`. `config.py` checks each
  expression against the `ALIAS` column on every row of both tables before
  writing it (40 of 40 keys equal on 894,106 rows [M]); a key held by two
  levels would get none.

### 2.1 The rewrite rules

The `ALIAS` column is `mapUpdate(covered(resource_id) if the catalog knows
it else {}, ResourceResidual)`: for a key k the row reads the residual's
value if the residual has k, else the catalog's, else `''`. The rules
preserve exactly that. `R` is `ResourceResidual`, `id` is `resource_id`,
`KV(cond)` is `SELECT resource_id FROM rw_cat.resource_kv WHERE Key = k AND
cond`.

| HyperDX sends (on a variant-c table) | rewritten to (mode **exact**) | mode **catalog** |
|---|---|---|
| `ResourceAttributes['k'] OP v`, OP in `=` `!=` `<>` `IN (…)` `NOT IN (…)` `LIKE` `ILIKE` `NOT LIKE` `NOT ILIKE`, v a literal, a literal list, or a `{p:String}` parameter; also `v = ResourceAttributes['k']` | `if(mapContains(R, k), R[k] OP v, id IN (KV(Value OP v)))` | `id IN (KV(Value OP v))` for a covered k, `R[k] OP v` otherwise |
| … when `'' OP v` is true (`!= 'x'`, `= ''`, `NOT IN`, `NOT ILIKE 'a%'`, `LIKE '%'`): rows the catalog doesn't know read `''` | `if(…, …, id NOT IN (KV(NOT (Value OP v))))` | `id NOT IN (KV(NOT (Value OP v)))` |
| `mapContains(ResourceAttributes, k)` (and `NOT …`) | `mapContains(R, k) OR id IN (KV(1))` | `id IN (KV(1))` / `mapContains(R, k)` |
| `notEmpty(ResourceAttributes['k'])` (Lucene `k:*`), `empty(…)` | as `!= ''`, `= ''` | same |
| `indexHint(mapContains(ResourceAttributes, k))` (HyperDX adds it to every map filter) | `1` (`indexHint` is 1 for every row; no index reads the ALIAS) | same |
| `ResourceAttributes['k']` anywhere else in the scope: select list, `GROUP BY`, `ORDER BY`, a comparison with a non-constant | `if(mapContains(R, k), R[k], <k's value expression>)`; an unaliased select item gets `` AS `arrayElement(ResourceAttributes, 'k')` ``, ClickHouse's own name for it, so result column names don't change | same |
| … for a key outside the covered list | `R[k]` | same |
| a column standing for a key (`"materialized"` in the config, e.g. `__hdx_materialized_k8s.pod.name`) | as `ResourceAttributes['k']` | same |

- **exact** keeps the `ALIAS` column's semantics whatever the catalog knows;
  it relies on one thing only, that `resource_kv` holds what `d_res` holds.
  Its catalog condition sits inside an `if`, so it isn't a primary-key
  condition: ClickHouse reads `resource_id` and `ResourceResidual` over the
  time range, but builds no maps.
- **catalog** is `id IN (…)` alone: a primary-key condition on variant c's
  sort key, the parent's `rw` mode. It is exact once the catalog knows every
  resource the rows name. Before that, rows the edge sent inside the grace
  window (the covered set in the residual) of a resource the catalog doesn't
  know yet are missed by a filter on a covered key; §4 counts them. With the
  edge announcement lane (parent §6.3) the catalog knows every resource the
  edge saw, and catalog mode is exact.
- The key list only decides `R[k]` against the value expression, and in
  catalog mode `R[k] OP v` against `id IN`; a key the catalog gains is picked
  up within `refresh_seconds`.

### 2.2 What passes through untouched

- statements with no configured table in a single-table `FROM`: `system.*`,
  `DESCRIBE`, the key-value rollups (discovery is already catalog-served, parent
  §3.5), `mergeTreeTextIndex(...)`, `KILL QUERY`;
- `SELECT`s on the table that don't mention `ResourceAttributes`;
- the whole map: `SELECT *, ResourceAttributes AS __hdx_resource_attributes`
  (the row panel: one row), `mapKeys(ResourceAttributes)`: the `ALIAS`
  builds it, ~4 µs per row;
- `ResourceAttributes['k'] = <expression>` (non-constant right side): only
  the access becomes a value expression;
- `ResourceAttributes[<non-literal key>]`, qualified `t.ResourceAttributes`;
- anything that fails to parse (none of the captures), an `INSERT` with
  data, a non-gzip compressed request body, a body over 32 MB.

The proxy logs each such statement with its reason (`log` in the config)
and counts them at `/__rw/stats`.

## 3. How it was run

The entities spike's databases had been dropped; `scripts/setup.py`
rebuilds them under `rw_*` with the spike's own scripts (the catalog name
patched), smaller where it doesn't matter:

| | here | the parent |
|---|---|---|
| fleet (`fleet.py`) | 20 clusters, **2 days** of SCD2 history (158 k pod versions) | 90 days (4.72 M) |
| telemetry (`telemetry.py`), loaded (`load.py a,c`, 60 objects per signal) | 442,690 spans, 451,416 logs, 2 h | 497,218 / 478,320 |
| `resource_kv` | 2.43 M rows, 76.6 k resources | 2.42 M, 76 k |
| variants | `rw_a` (ClickStack 2.39.1 tables, consumer DDL at HEAD), `rw_c` (variant c: `resource_id` + `ResourceResidual`, `ResourceAttributes` an `ALIAS`) | the same |

The history length only changes the dictionaries' size (the window's
resources are the same kind); query shapes, row counts and `resource_kv` are
comparable to the parent's.

**Two catalog states** (`setup.py race`, `setup.py catchup`):

- **race**: 1 pod in 200 withheld from the catalog (the controller never saw
  it, as in the parent), **and** every resource whose rows carry the covered
  set in the residual (the edge's grace window: brand-new pods) taken out of
  the catalog too, since the controller hasn't written a brand-new pod yet.
  3,607 log and 3,680 span rows name a resource the catalog doesn't know;
  350 / 274 of them carry its covered set in the residual. `resource_kv` is
  rebuilt from the same state (what the catalog knows).
- **after catch-up**: the controller has written everything; dictionaries
  reloaded, `resource_kv` rebuilt.

**Statements** (`scripts/corpus.py`):

| set | statements | what |
|---|---:|---|
| captured | 799 | every statement HyperDX 2.39.1 sent in `../../hyperdx`'s captures (three schema sides: pre-alignment "old", full ClickStack, option 2), the database parameter pointed at `rw_c`, time parameters shifted onto this data's window, hdxgen's pod / route / db-system values replaced by this data's |
| scenarios | 27 (15 shapes) | the parent's 15 query shapes (`../scripts/queries.py`) as HyperDX 2.39.1 renders them for variant c (a ClickStack table without a resource items index: map filters with `indexHint(mapContains)`, `hasAllTokens` full text, discovery from the rollup), with HyperDX's own `{HYPERDX_PARAM_n:Type}` parameters; and `scenarios_a`, the same for `rw_a` as HyperDX renders them there (items index, `__hdx_materialized_k8s.*`, `mergeTreeTextIndex` discovery) |
| matrix | 632 | every filter form of §2.1 on 17 keys (pod, namespace, deployment, statefulset, version, uid, container, image tag, region, node, start time, a pod label; residual keys `telemetry.sdk.language`, `service.instance.id`, `git.commit.sha`, `process.runtime.name`; an absent key) × both signals; the race values (a pod and a namespace known only from grace rows); a `{p:String}` value; group-bys of 22 keys over every level; an unaliased select |

## 4. Correctness

`scripts/correct.py`: every statement of the three sets goes through
`rwproxy rewrite`. One that isn't rewritten is forwarded byte for byte, so
there is nothing to check. Each rewritten one and its original run against
ClickHouse with the same parameters and settings (`max_threads = 2`,
`output_format_write_statistics = 0`, `use_query_condition_cache = 0`), and
the response bodies are compared: **equal** (byte-equal, including the
`FORMAT JSON` metadata, so result column names are checked too),
**equal-rows** (the same lines in another order: a histogram ordered only by
bucket, the waterfall's unordered `LIMIT`), **estimate** (`EXPLAIN
ESTIMATE`: the rewritten statement reads other granules by design; HyperDX
uses it for its row-count hint), or **MISMATCH** [M]:

| state | set | stmts | mode | rewritten | equal | equal-rows | estimate | **MISMATCH** |
|---|---|---:|---|---:|---:|---:|---:|---:|
| race | captured | 799 | exact | 82 | 69 | 0 | 13 | **0** |
| race | scenarios | 27 | exact | 12 | 11 | 1 | 0 | **0** |
| race | matrix | 632 | exact | 632 | 632 | 0 | 0 | **0** |
| race | captured | 799 | catalog | 82 | 69 | 0 | 13 | **0** |
| race | scenarios | 27 | catalog | 12 | 9 | 2 | 0 | **1** |
| race | matrix | 632 | catalog | 632 | 369 | 0 | 0 | **263** |
| after catch-up | captured | 799 | exact | 82 | 69 | 0 | 13 | **0** |
| after catch-up | scenarios | 27 | exact | 12 | 11 | 1 | 0 | **0** |
| after catch-up | matrix | 632 | exact | 632 | 632 | 0 | 0 | **0** |
| after catch-up | captured | 799 | catalog | 82 | 69 | 0 | 13 | **0** |
| after catch-up | scenarios | 27 | catalog | 12 | 12 | 0 | 0 | **0** |
| after catch-up | matrix | 632 | catalog | 632 | 632 | 0 | 0 | **0** |

- **Exact mode returns what the `ALIAS` returns on every statement, in both
  catalog states**: 1,452 rewritten statements (82 + 12 + 632, twice), no
  mismatch. That includes the residual keys (`telemetry.sdk.language ILIKE`,
  `service.instance.id`, `git.commit.sha`, which the catalog never holds),
  an absent key, every `''`-matching form, `{p:String}` values, group-bys of
  22 keys on every level, and the race rows: filters on a pod and a
  namespace that exist only in grace-window rows of resources the catalog
  doesn't know (246 spans of one pod).
- **Catalog mode is exact once the catalog is complete** (after catch-up:
  0 mismatches), and before that it is wrong exactly where §2.1 says: 263 of
  the 632 matrix statements and one scenario (`k8s.namespace.name =`),
  all filters on a covered key, off by up to 350 log / 274 span rows, the
  grace-window rows of unknown resources. Group-bys and selects (the value
  path, shared with exact mode) and residual-key filters were exact in both
  states.
- The captured statements' values were replaced by this data's (a pod, a
  region, a route), so their filters select rows. The 13 estimates are
  `EXPLAIN ESTIMATE` on resource filters; their other statements (the list
  and the histogram of the same search) are among the 69 equal.
- The rewrite's premise, that `resource_kv` says what `d_res` says, held
  here because both were rebuilt from the same `res_index` at each state
  change; §8 is what happens when they drift.

## 5. Overhead

Three measurements, all on the loaded box [M]:

**Parse + rewrite, in process** (`rwproxy bench -n 200`, every statement
200 times, interleaved; `results/rewrite-bench.json`):

| statements | n | p50 µs | p90 µs | p99 µs |
|---|---:|---:|---:|---:|
| captured (799), all | 159,800 | 48 | 174 | 866 |
| … passed through (717) | 143,400 | 44 | 114 | 556 |
| … rewritten (82) | 16,400 | 146 | 633 | 2,148 |
| scenarios (27), rewritten (12) | 2,400 | 145 | 393 | 1,065 |

33 KB allocated per statement (captured), 55 KB (scenarios). Parsing
dominates; the long statements (HyperDX's 30-column `sampledData`
discovery) are the p99. Inside the running proxy, under the benchmarks'
load, the same step logged p50 130–225 µs and p99 0.9–1.3 ms
(`/__rw/stats`).

**The hop** (`bench.py hop`: direct and through the proxy, interleaved, one
kept-alive connection each; load 4.0–4.3):

| request | n | direct p50 ms | through the proxy p50 ms | added p50 / p90 / p99 ms | proxy CPU µs per request |
|---|---:|---:|---:|---:|---:|
| `SELECT 1` (passed through) | 500 | 1.62 | 1.98 | 0.37 / 0.96 / 4.85 | 660 |
| a rewritten filter that reads no rows (sent directly already rewritten) | 500 | 7.77 | 8.29 | 0.51 / 2.18 / 6.32 | 900 |
| a 5.7 MB response (passed through) | 50 | 19.9 | 21.5 | 2.25 / 5.91 / 6.48 | 4,800 |

**Per statement, HyperDX's own** (`bench.py overhead`: all 799 captured
statements through the running proxy in exact mode and directly,
interleaved, 3 rounds, the median per statement; load 4.2 1-min, 20–26
5/15-min after another agent's memory-pressure episode):

| | n | p50 | p90 | p99 |
|---|---:|---:|---:|---:|
| added wall time per passed-through statement (proxy − direct), ms | 717 | 0.54 | 2.0 | 20.6 |
| parse + rewrite inside the proxy (`/__rw/stats`, every request), µs | 2,397 | 129 | 320 | 879 |

Direct wall time was 7.4 ms p50. The p99 of the added time is the loaded
box's scheduling noise (the same statement's direct rounds spread as much);
an earlier run of the same benchmark gave 0.52 / 2.2 / 14.9 ms. Of the 2,397
requests the proxy rewrote 246 (82 × 3), passed 2,151 through (1,035 with no
`ResourceAttributes`, 1,086 on no configured table, 30 `unhandled`: the whole
map), took **0 fallbacks**, and used 2.5 s of CPU (1.0 ms per request) with
36 MB RSS, 42 MB peak.

**CPU and memory.** RSS 10 MB idle, 15–27 MB while serving the benchmarks,
37–42 MB after streaming the 5.7 MB responses (a 64 KB copy buffer per
in-flight request, pooled). CPU per request was 0.66 ms for `SELECT 1` and
0.9 ms for a rewritten statement **in this container**, where system calls
are expensive: a profile of the running proxy is two-thirds `read`/`write`
syscalls, `futex` wake-ups and the scheduler, and the same request path
in-process (`go test -bench BenchmarkProxy`: client, proxy and a fake
upstream in one process, kept-alive) takes 0.10 ms per round trip passed
through and 0.19 ms rewritten, client and upstream included. At the mid
scenario's HyperDX load (tens of statements per second per user) that is
well under one core [E]. The first version allocated its 64 KB buffer per
request and ran the collector every few dozen requests; its background
sweeper took 75% of the CPU (3.4× the round-trip time), fixed with a buffer
pool and `GOGC=800` (the heap is a few MB).

## 6. End-to-end latency

`scripts/bench.py scenarios`, after catch-up: each of the 15 shapes five
ways, server time (`X-ClickHouse-Summary` `elapsed_ns`, which passes through
the proxy), median of 7 rounds after a warm-up, the ways interleaved and
rotated per round, one kept-alive connection per endpoint (as HyperDX's
clients keep theirs). **Loaded box**, load average 4.0–4.4 (1-min), while
other agents' jobs ran [M]:

| scenario | stmts | (a) ClickStack table, direct | (c) `ALIAS`, direct | (c) through the proxy, **exact** | (c) through the proxy, **catalog** | proxy hop, ms |
|---|---:|---:|---:|---:|---:|---:|
| logs: all (histogram + page) | 2 | 33.2 | 33.3 | 33.9 | 34.5 | +0.5 |
| logs: `k8s.pod.name =` + ERROR | 2 | 38.5 | 303.2 | 136.2 | **42.0** | −1.3 |
| logs: `k8s.namespace.name =` | 2 | 29.1 | 2,820.3 | 105.6 | **39.3** | +0.7 |
| logs: `ServiceName =` | 2 | 37.2 | 33.3 | 39.6 | 40.2 | +0.8 |
| logs: full text | 2 | 100.6 | 88.8 | 93.2 | 83.7 | +2.8 |
| logs: group by namespace | 1 | 18.7 | 1,904.7 | 120.8 | 142.6 | +0.3 |
| logs: row detail panel | 1 | 22.9 | 22.1 | 21.6 | 21.7 | +0.8 |
| traces: all | 2 | 49.6 | 55.5 | 52.8 | 52.8 | +1.7 |
| traces: `k8s.pod.name =` | 2 | 22.6 | 3,916.1 | 133.8 | **30.0** | +0.2 |
| traces: residual key `ILIKE` + span attribute | 2 | 571.8 | 4,720.5 | **205.9** | **201.4** | +1.6 |
| traces: p95 by deployment (errors) | 1 | 257.0 | 121.0 | **75.2** | **73.5** | +0.8 |
| trace waterfall by TraceId | 3 | 61.0 | 34.7 | 33.6 | 35.4 | +2.9 |
| discovery: resource keys | 2 | 46.5 | 39.3 | 39.4 | 36.2 | +1.0 |
| discovery: values of 10 resource keys | 2 | 27.6 | 70.1 | 67.7 | 66.4 | +1.1 |
| **14 shapes** | 26 | **1,316** | **14,163** | **1,159** | **900** | |
| discovery: values by scanning (no rollup) | 1 | – | 1,835.5 | 859.0 | 820.4 | +0.9 |

(`results/bench.jsonl`; `scripts/summarize.py scenarios` prints it with the
fifth way, the rewritten statement sent directly: 1,144 ms for the 14 shapes.)

- **Through the proxy, variant c costs what the ClickStack table costs, or
  less**: 900 ms (catalog mode) and 1,159 ms (exact) against (a)'s 1,316 ms
  and the `ALIAS` column's 14,163 ms. The parent measured the hand-written
  `rw` rendering at 766 against 1,340 on a differently loaded box.
- **Catalog mode is within 1.1–1.4× of (a) on the resource filters** (pod,
  namespace: 30–42 ms against 23–39): `resource_id IN (…)` is a key
  condition on c's sort key. **Exact mode is 3.5–5.9× (a)** there (105–136
  ms): its `if(mapContains(ResourceResidual, k), …, resource_id IN (…))`
  isn't one, so it reads `resource_id` and `ResourceResidual` over the whole
  range; still 2.2–29× faster than the `ALIAS`. A skip index doesn't rescue
  it: with a bloom filter on `mapKeys(ResourceResidual)` and one on
  `resource_id`, the OR form `resource_id IN (…) OR (mapContains(R, k) AND
  R[k] = v)` read 287 k of 443 k rows (74 ms against 83), because grace-window
  rows land in nearly every granule (a few per 8,192 rows even at 0.03%)
  [M, a copy of the traces table, dropped].
- **Where (a) must read the map, the rewrite wins**: a residual-key filter
  (`telemetry.sdk.language ILIKE`) reads the small residual instead of (a)'s
  37-key map (2.8×), and the p95 by deployment reads one level dictionary per
  row instead of the map (3.4×).
- **Group by namespace stays the one shape slower than (a)**: 121–143 ms
  against 19, which (a) answers from the materialized
  `__hdx_materialized_k8s.namespace.name` column; the rewrite makes one
  dictionary chain per row (`d_res` → `d_pod` → `d_ns` + `JSONExtract`), 16×
  cheaper than rebuilding the map, and still 6.5–7.6× (a).
- Value discovery by scan (HyperDX's fallback when the rollup can't serve a
  column) halves: the sampled `ResourceAttributes['k']` become ten value
  expressions instead of one map build.
- Statements the proxy passes through cost the same through it (server times
  within the run's noise, ±5 ms), and the proxy adds **a median 0.5 ms per statement** (−1.3 to +2.9 ms per
  scenario, within noise) (the last column: wall minus
  server time through the proxy, less the same sent directly).

## 7. HyperDX live

**Not run.** The task's bar was MemAvailable ≥ 6 GB and ≥ 5 GB of disk free
after the images (1.4 GB, `../../hyperdx/README.md`). Free disk was
3.4–4.9 GB during this spike (other agents' CI, and this spike's databases),
so pulling the images would have taken the box below the 4 GB floor; free
memory also swung between 1.7 and 13 GB while another agent's Kubernetes
simulators ran. Instead:

- **HyperDX's own statements went through the proxy**: all 799 captured
  statements, with their `{HYPERDX_PARAM_n:Type}` parameters, `FORMAT`
  clauses and settings as URL parameters, three times each, through the
  running proxy and directly (§5); the 27 scenario statements seven times
  each (§6). No request failed in the proxy; no fallback was taken.
- **HyperDX's transport** is covered by tests against a fake upstream
  (`proxy_test.go`): the statement in the body (what `@clickhouse/client`
  sends), in the URL, gzip-compressed, and in a `multipart/form-data` body
  (what `@clickhouse/client-web` sends when its parameters outgrow the URL;
  also checked against the real ClickHouse through the proxy: same result as
  direct); the response's bytes, encoding and `X-ClickHouse-*` headers
  unchanged; an `INSERT` with data untouched; the fallback.
- What a live run would add: the browser's own mix and timing, the alert
  task, and HyperDX's reaction to `EXPLAIN ESTIMATE` returning smaller
  estimates (it only shows them). To do it: `../../hyperdx/scripts/run.sh
  dockerd hyperdx`, then point the connection's host at the proxy
  (`http://127.0.0.1:18125`) and the log and trace sources at
  `rw_c.otel_logs` / `rw_c.otel_traces` with `kvRollupTable` set (parent
  §5).

## 8. Failure modes

| failure | what happens | why it is safe, or what to do |
|---|---|---|
| a statement the proxy can't parse, or a shape it doesn't know | forwarded unchanged, logged with its reason | the `ALIAS` column makes the original statement correct on variant c: only slower |
| the spliced statement doesn't parse | the original is sent (`reparse`) | same |
| the rewritten statement fails before any byte is streamed (e.g. `resource_kv` missing, a permission on the catalog database) | the proxy re-sends the original (`"fallback": true`; `TestProxyFallback`) and counts it | same; alert on `/__rw/stats` `fallbacks` |
| it fails after streaming began | the client sees ClickHouse's error, as it would directly | ClickHouse's own semantics |
| **the proxy is down** | HyperDX has one host per connection and no failover: its queries fail | run ≥ 2 replicas behind a Service, and put a load balancer in front whose **backup** is ClickHouse itself (HAProxy `backup`, Envoy priority 1): fail-open to direct, correct and slow. Nothing about correctness depends on the proxy being there |
| `resource_kv` and `d_res` disagree (the catalog's two views refreshed at different times) | exact mode follows `resource_kv` for rows whose residual lacks the key, the `ALIAS` follows `d_res`: they differ for resources in one and not the other | a resource's kv rows are immutable (content address), so the controller should write them in the same batch as its `res_index` row; then `resource_kv` is ahead of the dictionary by its `LIFETIME` (30–60 s, parent §3.6), and the rewrite finds a new resource's rows *before* the `ALIAS` shows its attributes |
| catalog mode before the catalog knows a resource | grace-window rows of that resource (covered set in the residual) are missed by a filter on a covered key (§4 counts them) | use exact mode until the announcement lane (parent §6.3) makes the catalog complete |
| the covered key list is stale (a new label key) | the value path reads a key the catalog now has from the residual only; catalog mode filters it on the residual | re-read from `resource_kv` every `refresh_seconds` (30); exact-mode filters don't depend on the list |
| cancellation | HyperDX cancels by closing the request or by `KILL QUERY WHERE query_id = …` | the proxy cancels its upstream request with the client's; `KILL QUERY` passes through; `query_id` is kept |
| credentials | the proxy forwards HyperDX's headers and sees them | it runs next to HyperDX, with no TLS termination in this spike |

## 9. The alternative: a resource lookup table in HyperDX

The other place to do the rewrite is HyperDX itself, which already has the
pieces: it detects index-backed map columns from table metadata and renders
`has(ResourceAttributeItems, concat(k, '=', v))` instead of `Map['k'] = v`,
and it rewrites map accesses to materialized columns with a SQL AST pass.
A **resource lookup** would be a third optimisation of the same kind [D, at
`885d30c`]:

1. **Detection, no source-form change.** `common-utils/src/core/metadata.ts`:
   a `getResourceLookup({databaseName, tableName})` beside
   `getMaterializedColumnsLookupTable` (l. 644) and `getMapColumnTextIndexes`:
   the table has `resource_id` and `ResourceResidual`, `ResourceAttributes`
   is an `ALIAS`, and a lookup table exists by convention
   (`<table>_resource_kv`, or named in the table's `COMMENT`), the way
   `app/src/source.ts` (l. 400–430) finds `<table>_kv_rollup_15m`. An
   explicit `resourceLookup: {table, idExpression, residualExpression}` on
   `LogSourceSchema` / `TraceSourceSchema` (`common-utils/src/types.ts`
   l. 2448 / 2494) would instead have to be threaded through every chart
   config the way `useTextIndexForImplicitColumn` is (the chart-config schema,
   l. 1672 / 1777, and ~20 call sites in `app/src`).
2. **Lucene filters** (`common-utils/src/queryParser.ts`,
   `CustomSchemaSQLSerializerV2`): the constructor (l. 1392) starts a
   `resourceLookupPromise` next to `textIndexInfoLookupPromise`;
   `buildColumnExpressionFromField` (l. 1681), on the `Map` prefix match
   (l. 1746), attaches `resourceLookup` for the resource map as it attaches
   `textIndexInfo`; `getColumnForField` (l. 1934) passes it on; and `eq`
   (l. 516) gets a branch before the items-index one (l. 548): `(if(mapContains(R,
   k), R[k] = v, resource_id IN (SELECT resource_id FROM <lookup> WHERE Key =
   k AND Value = v)))`, with the `''` cases as in §2.1. The same in
   `fieldSearch` (l. 1451, the `ILIKE '%v%'` path) and `isNotNull` (l. 606,
   `k:*`). ~150 lines with tests (`__tests__/queryParser.test.ts`).
3. **SQL filters, sidebar facets and group-bys**
   (`common-utils/src/core/renderChartConfig.ts`): `fastifySQL` (l. 244)
   already parses the rendered `WHERE` (`renderWhereExpressionStr`,
   l. 1228–1295: SQL-language conditions and `filtersToQuery`'s
   `ResourceAttributes['k'] IN (…)` from `filters.ts` l. 49) and each select
   and group-by expression (`renderSelectList`, l. 730–830) with
   node-sql-parser (PostgreSQL dialect, `SETTINGS` stripped), and swaps
   `Map['k']` for a materialized column. A second pass beside it would swap
   predicates for the lookup and values for the level expression. ~200
   lines with tests (`core/__tests__/renderChartConfig.test.ts`).
4. **Discovery**: nothing; the catalog's rows in `<table>_kv_rollup_15m`
   already serve keys and values (parent §3.5).

| | proxy (this) | HyperDX change |
|---|---|---|
| covers | every statement to ClickHouse: search, charts, dashboards, alerts, raw-SQL charts, any other client (Grafana, scripts) | what HyperDX renders from structured config: Lucene, sidebar filters, builder group-bys. **Not** raw-SQL charts or a user's SQL `WHERE` beyond what node-sql-parser (PostgreSQL dialect) understands; `fastifySQL` swallows its parse errors and leaves the SQL as is |
| parser | ClickHouse grammar, 799 / 799 of HyperDX's statements | renders from semantics for Lucene (no parsing); node-sql-parser for the SQL paths |
| correctness risk | reverse-engineers statements; bounded by the pass-through rule and the fallback | knows the source; tested by HyperDX's snapshot tests |
| cost | one hop: §5 | none |
| operations | a component to run, replicate, monitor; sees credentials | none, once released |
| schema coupling | the proxy's config (`config.py` from the catalog) | a HyperDX feature for one custom schema, which upstream has to accept; the metadata detection keeps it opt-in |
| time to use | now | a feature request, review, a release |

**Recommendation.** Use the proxy now, in **exact** mode, behind a load
balancer that falls back to ClickHouse; move to **catalog** mode when the
announcement lane makes the catalog complete. Propose the HyperDX change
upstream in the metadata-detected form (1–3 above), since it removes the hop
for the common paths; keep the proxy for raw SQL and non-HyperDX clients
until then.

## Files

- `main.go`: the command (`serve`, `rewrite`, `bench`); `proxy.go`: the
  HTTP proxy (statement extraction, forwarding, streaming, fallback,
  `/__rw/stats`, covered-key refresh); `rewrite.go`: masking, parsing,
  scopes, spans and the rules of §2.1; `config.go`.
- `rewrite_test.go`, `proxy_test.go`: the rules, scopes, parameters,
  pass-through, and the transport (body, URL, gzip, multipart, `INSERT`,
  fallback) against a fake upstream; `go test ./...` needs no ClickHouse.
- `scripts/setup.py`: the data (§3) and the catalog states; `config.py`: the
  proxy's configuration from the catalog, value expressions checked against
  the `ALIAS`; `corpus.py`: the three statement sets; `correct.py`: §4;
  `bench.py`: §5, §6; `summarize.py`: the tables.
- `results/correctness.jsonl`, `results/bench.jsonl`,
  `results/rewrite-bench.json` (in-process parse + rewrite).

Reproduce (ClickHouse on `:18123`, `CH_CLIENT` = a clickhouse binary):

```sh
export RW_SCRATCH=<scratch dir> CH_CLIENT=<clickhouse>
go build -o $RW_SCRATCH/rwproxy-target/rwproxy .       # Go 1.27
cd scripts
python3 setup.py fleet telemetry index dicts load kv race  # ~3 min; then drop rw_src.traces / logs if disk is short
python3 config.py --out $RW_SCRATCH/rwproxy.json
python3 corpus.py --out $RW_SCRATCH/rwc
python3 correct.py --label race
python3 setup.py catchup
python3 bench.py scenarios ; python3 bench.py overhead ; python3 bench.py hop
python3 correct.py --label "after catch-up"
$RW_SCRATCH/rwproxy-target/rwproxy bench -config $RW_SCRATCH/rwproxy.json -n 200 $RW_SCRATCH/rwc/captured.jsonl
python3 setup.py drop
```

Everything lived in databases `rw_*`; they were dropped at the end.
