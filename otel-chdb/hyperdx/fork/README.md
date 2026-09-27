# HyperDX fork: patch series

The owner's UI decision (DECISIONS.md, "Owner decisions, 2026-09-27"):
start with a HyperDX fork. The fork is kept here as a patch series against
upstream **HyperDX at `885d30c`** (`hyperdxio/hyperdx`, "Update VOUCHED list
(#3210)"), not as a copy of the tree. Nothing here has been sent upstream.

| Patch | What | Register row |
|---|---|---|
| [`0001-no-silent-partial-results.patch`](patches/0001-no-silent-partial-results.patch) | Every query the HyperDX client sends pins the eleven `*_overflow_mode` settings to `throw`; a source's `querySettings` can no longer set one; the `break`/`any` settings are gone from the call sites, except a flagged read sample for suggestion lists | AMBIGUITY.md X7 (H-2 in the UI), C2's hazard in the UI and alerts |

## Apply and build

```sh
git clone https://github.com/hyperdxio/hyperdx && cd hyperdx
git checkout 885d30c
git am /path/to/otel-chdb/hyperdx/fork/patches/*.patch
yarn install            # yarn 4 (corepack), as upstream's README
yarn build:common-utils # the app and api import common-utils from dist/
yarn workspace @hyperdx/common-utils ci:unit   # jest, includes the new tests
yarn workspace @hyperdx/app ci:unit
yarn workspace @hyperdx/api ci:int             # needs ClickHouse and Mongo (upstream's docker-compose.ci.yml)
```

**What was checked here (2026-09-27):** the series applies with `git am`
to a clean `885d30c`; every touched `.ts`/`.tsx` file parses
(`typescript.transpileModule`, no diagnostics); the touched tests are
formatted with the repository's Prettier config. **Not run:** `yarn
install`, the type check and the jest suites. The monorepo's
dependencies need more disk than the build box had to spare (it keeps
at least 3 GB free); run the commands above before relying on the patch.

## 0001: no silent partial results

**The hazard** (AMBIGUITY.md X7, C2): a ClickHouse limit
(`max_execution_time`, `max_rows_to_read`, `max_rows_to_group_by`,
`max_result_rows`, ...) with its overflow mode at `break` (`any` for
GROUP BY) returns the rows read so far with HTTP 200 and nothing to say
so. At `885d30c` HyperDX asked for that on its metadata queries, value
distributions and raw SQL tables, and passed a source's `querySettings`
into the SQL `SETTINGS` clause unfiltered, so a source (or a server or user
profile) could do the same to charts, search pages and **alert
evaluations**. A missing filter value reads as "no such value", a metric
missing from the catalog as "not reported", an alert window as quiet.

**The mechanism:**

- `common-utils/src/core/overflow.ts`: `NO_PARTIAL_RESULTS`, the eleven
  modes (the consumer's `sql.rs` list: `timeout`, `timeout_leaf`, `read`,
  `read_leaf`, `result`, `group_by`, `set`, `join`, `sort`, `distinct`,
  `transfer`) as `throw`.
- `BaseClickhouseClient.processClickhouseSettings` spreads them **last**,
  after the call's own settings, on every query of the node and browser
  clients: charts, search, raw SQL, alerts (`checkAlerts` goes through
  `queryChartConfig`), metadata, MCP tools. HTTP parameters override a
  server or user profile, so the profile can't make a result partial
  either.
- `joinQuerySettings` drops any `*_overflow_mode` from a source's
  `querySettings`. They go into the SQL `SETTINGS` clause, which ClickHouse
  applies over the HTTP parameters, so the pin alone would not hold.
- Limits stay. A query that reaches one fails, and the UI shows the error
  it already shows for a failed query.

**Per call site** (all at `885d30c`):

| Call site | Was | Now | Why |
|---|---|---|---|
| `metadata.ts` `getMapKeys` (2 queries) | `timeout_overflow_mode: 'break'`, 15 s | limit kept, **throw** | a key list cut at 15 s reads as "no such key" |
| `metadata.ts` `getMetadataMVKeyValues` | `timeout 'break'`, 15 s | limit kept, **throw** (its failure was, and is, isolated to that strategy) | a short value list |
| `metadata.ts` `getKeyValues` (the filter panel's values, AI summaries) | `timeout 'break'`, 15 s | limit kept, **throw** | the X7 case: a missing value reads as absent |
| `metadata.ts` `getValuesDistribution` | `group_by_overflow_mode: 'any'` at `limit × 10` groups | limit kept, **throw** | `any` counts an arbitrary subset of the values and shows its shares as the real ones |
| `useMetricCatalog.ts` | `timeout 'break'` | limit kept, **throw** | a metric missing from the catalog reads as not reported |
| `useFetchMetricAttributeValues.tsx` | `timeout 'break'`, 60 s | limit kept, **throw** | a short attribute value list |
| `useOffsetPaginatedQuery.tsx` (raw SQL tables) | `result_overflow_mode: 'break'` at `max_result_rows` (≤ 10,000) | **throw**; the error names the limit (add a `LIMIT`) | `break` showed the first rows as the whole result, without saying so |
| builder search pages, charts, alerts (`queryChartConfig`, `checkAlerts`, alert message samples) | nothing set; a source's `querySettings` or the profile could set `break` | **throw** pinned; source overflow modes dropped | alerts must never evaluate a partial window |
| MCP `listSources` preview client, `listMetrics`, `describeMetric` (3) | `timeout 'break'` | **throw**; previews are best-effort and already caught, so a slow one is omitted instead of attached short | an agent reads a short list as complete |
| `metadata.ts` `getJSONKeys`, `getMapValues`, `getMapTextIndexKeyValues`, `getTextIndexKeyValues` | `read_overflow_mode: 'break'` at `max_rows_to_read` (3M) | **kept**, as a flagged sample: `QueryInputs.allowSampledRead` lets only the two read modes through the pin; every other mode stays `throw`. `getJSONKeys`' inner `SETTINGS timeout_overflow_mode = 'break'` (2 s) is left as it was: it is in the SQL, outside the pin | these are suggestion vocabularies (typeahead keys and values) that sample by design; making them fail past 3M rows would break suggestions on any real table. They feed no chart, search result or alert |

**Open (X7 stays *partly*):** the four sampled vocabularies are marked in
code (`allowSampledRead`) but not yet **in the UI**: a suggestion list
does not say it is a sample. That, and the `complete_through` banner (R-S1,
R-S2), are the next patches. `getAllKeyValues` still drops a failed
strategy's keys with a console warning (upstream behaviour): a facet whose
query failed shows no values rather than an error. User-written raw SQL can
still say `SETTINGS … = 'break'` itself; that is the author's choice, in
the query text.

**Tests in the patch:** `clickhouse.test.ts` (the pin overrides a caller's
`break`/`any`; `allowSampledRead` lets only a read sample through; the
default-settings expectations now include the pins), `utils.test.ts`
(`joinQuerySettings` drops overflow modes), `metadata.test.ts` and
`useOffsetPaginatedQuery.test.tsx` (expectations updated),
`renderChartConfig.int.test.ts` (a source's `result_overflow_mode=break`
no longer reaches the SQL, and the over-limit query fails; its old
snapshot removed).

## Regenerating the series

The series was made from a clone at `885d30c` with one commit per patch:

```sh
git format-patch --no-signature -o otel-chdb/hyperdx/fork/patches 885d30c..HEAD
```
