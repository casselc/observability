# HyperDX fork: patch series

The owner's UI decision (DECISIONS.md, "Owner decisions, 2026-09-27"):
start with a HyperDX fork. The fork is kept here as a patch series against
upstream **HyperDX at `885d30c`** (`hyperdxio/hyperdx`, "Update VOUCHED list
(#3210)"), not as a copy of the tree. Nothing here has been sent upstream.

| Patch | What | Register row |
|---|---|---|
| [`0001-no-silent-partial-results.patch`](patches/0001-no-silent-partial-results.patch) | Every query the HyperDX client sends pins the eleven `*_overflow_mode` settings to `throw`; a source's `querySettings` can no longer set one; the `break`/`any` settings are gone from the call sites, except a flagged read sample for suggestion lists | AMBIGUITY.md X7 (H-2 in the UI), C2's hazard in the UI and alerts |
| [`0002-query-service-token-and-completeness-banner.patch`](patches/0002-query-service-token-and-completeness-banner.patch) | HyperDX's ClickHouse traffic goes through the query service's adapter with the user's own token (browser path: the API's proxy forwards it; server path: a service token file), and every page shows the worst completeness label of its results in a banner; a refused text-index key lookup falls through instead of returning no keys; fixes 0001's one missed test expectation | R-S1, R-S2, R-S8 in the UI; X7; D25 |
| [`0004-user-token-on-server-side-queries-and-labelled-samples.patch`](patches/0004-user-token-on-server-side-queries-and-labelled-samples.patch) | Server-side queries a user started (MCP tools, the external API, the API's own routes) carry that user's token, never the service identity, which is left to the alert task; the query service's labelled samples (typeahead) are recorded as samples and shown on a banner line of their own; fixes 0003's one missed test expectation | D33 (owner decisions 2026-09-28); R-S8 server-side; X7, X18 |
| [`0003-one-basis-per-dashboard-refresh.patch`](patches/0003-one-basis-per-dashboard-refresh.patch) | Every statement of a page load or dashboard refresh carries one basis group (`X-Otel-Basis-Group`); the adapter pins one basis per group, so the panels read the same rows while data arrives and only a refresh moves them; the API proxy forwards only well-formed basis headers; the banner says which basis the page read at (one, several, or up to now) | D30 (research/bitemporal.md §3) |

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

**What was checked (2026-09-28, 0001 + 0002).** The full `yarn install` did
not fit (3.6 GB free, 2.5 GB to keep), so each touched package was checked
against the exact dependency versions of `yarn.lock`, installed with npm
next to the clone (231 MB for common-utils, 152 MB for the banner):

| package | type check | tests |
|---|---|---|
| `@hyperdx/common-utils` | **`tsc --noEmit -p tsconfig.json`, the whole package: clean** | **jest, all 39 unit suites: 2,665 tests pass**, the new `completeness.test.ts` (10) and 0001's tests included. The run found one of 0001's expectations missing the pinned modes (`clickhouse.test.ts`, "should only apply available optimization settings"): fixed in 0002 |
| `@hyperdx/app` | `CompletenessBanner.tsx` and its test: clean (strict, `react-jsx`); `pages/_app.tsx`: parses only (`transpileModule`) | `CompletenessBanner.test.tsx` (4) passes under jest + jsdom with a minimal `renderWithMantine` (MantineProvider), not the app's own jest setup |
| `@hyperdx/api` | `utils/queryServiceToken.ts` and its test: clean; `config.ts`, `routers/api/clickhouseProxy.ts`: parse only | `queryServiceToken.test.ts` (3) passes |

Not run: ESLint (import order), the app's and the API's full type checks and
suites, `ci:int`, a built HyperDX. The series applies to a clean `885d30c`
with `git apply` and gives exactly the tree that was tested. Before relying
on it, run the commands above on a machine with room for `yarn install`.

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
does not say it is a sample (still open; the `complete_through` banner is
patch 0002, and through the query service a sample past `max_rows_to_read`
fails instead: query README §8.5). `getAllKeyValues` still drops a failed
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

## 0002: through the query service, with a completeness banner

**Why.** The query service (`../../query/`, D22) scopes every statement to
the caller's clusters and namespaces and labels every result with its
`complete_through` (R-S1, R-S2, R-S8). HyperDX reached ClickHouse directly,
with the connection's shared credentials, and showed no completeness. The
adapter (`query/cmd/hdxadapter`, query README §8, D25) speaks ClickHouse's
HTTP interface to HyperDX and `/v1/query` to the service; this patch is the
HyperDX side. It changes HyperDX as little as the adapter allows: the
connection's host points at the adapter, and three small hooks.

**The token.**

- *Browser → API → adapter.* HyperDX runs behind an OIDC proxy (e.g.
  oauth2-proxy with `--pass-access-token`), which puts the user's token in a
  request header. With `HDX_QUERY_SERVICE_TOKEN_HEADER=x-forwarded-access-token`
  the API's `/clickhouse-proxy` forwards it as `Authorization: Bearer` to the
  connection's host (`utils/queryServiceToken.ts`: exactly one token-shaped
  value, or nothing). The browser client already strips its own
  Authorization (`standardModeFetch`). Unset: upstream behaviour.
- *Server side* (alert task, the API's own queries, MCP tools: the node
  client). `HDX_QUERY_SERVICE_TOKEN_FILE` names a file with the service
  identity's token, re-read per query; it goes in the query's
  `auth: {access_token}`. A per-query `http_headers` Authorization does not
  work: `@clickhouse/client` 1.23 overwrites it with its Basic header (found
  by the adapter's integration test with the real client). An empty file is
  an error. These queries run as that identity, not as the user who asked
  (MCP, external API): a limit, see query README §8.5.
- The token must carry what the service maps to clusters, namespaces and the
  `query` role (query README §4); its audience is the service's.

**The label** (`common-utils/src/clickhouse/completeness.ts`).
`BaseClickhouseClient.query` records every result's `X-Otel-*` headers under
the statement's text (a refreshed chart replaces its own entry; the time
range is a parameter, so the text is stable). A result without the headers
is **`unlabelled`**, a strange value **`unknown`**; nothing without a label
reads as complete. Schema answers (`X-Otel-Source: metadata`) are not
recorded. The store lives on `globalThis`, so every bundled copy of the
module shares it.

**The banner** (`app/src/components/CompletenessBanner.tsx`, mounted in
`_app.tsx`, reset on every route change): the worst label of the page's
results.

| worst | banner |
|---|---|
| `complete` | gray: "Complete through *T*" (the earliest `complete_through` on the page) |
| `partial` | yellow: "Partial — data from *T* on may still arrive (complete through *T'*): counts over that range are partial" |
| `unknown` | red: "Unknown — watermark *stale/missing/error*", with the service's note |
| `unlabelled` | red: "Unknown — not labelled: some results did not come through the query service" |

Per-chart marking (drawing the region after `incomplete_from` as
incomplete in each chart, R-S2) is not in this patch: the banner is
page-wide. The label is the service's: `complete` means complete in custody
time (see the CAST row on late rows).

**Text-index key discovery.** The service refuses `mergeTreeTextIndex`
(its tokens are every cluster's). At `885d30c` a failed text-index key
lookup returned an empty key list (a filter panel with no keys, silently);
with the patch it falls through to the rollup and scan paths.

**Deployment.** Connection host = the adapter (`http://hdxadapter:18191`),
username/password anything (ignored); API env
`HDX_QUERY_SERVICE_TOKEN_HEADER`, alert task / API env
`HDX_QUERY_SERVICE_TOKEN_FILE`; the adapter's `query_url` = the service. The
optional entity rewrite proxy sits between the API and the adapter (its
rewritten statements are refused by the service today: query README §8.5).

## 0003: one basis per dashboard refresh (D30)

**Why.** Every panel of a dashboard is its own statement, and before 0003
each ran up to "now": panels loaded a second apart disagreed about the
most recent data, and a panel re-run showed different numbers for the same
range. The query service can answer **at a basis** (a named custody time
per cluster: rows received before it, `../../research/bitemporal.md` §3,
D30); an answer at a basis never changes while new data arrives.

**What.**

- `common-utils/src/clickhouse/completeness.ts`: the store keeps a **basis
  group** per page load and searched time range (`pinBasis(key)`: the same
  key on the same page load is the same group, so a re-render changes
  nothing; a new range, including a refresh's new range end, or a new page
  is a new group; `reset()` on route change unpins). Every label records the
  basis its result was computed at (`X-Otel-Basis`, `X-Otel-At-Basis`,
  `X-Otel-Basis-Info`), and the summary counts the page's bases and the
  results read up to now.
- `common-utils/src/clickhouse/browser.ts`: every statement carries
  `X-Otel-Basis-Group` while a group is pinned.
- `api/src/routers/api/clickhouseProxy.ts`, `utils/queryServiceToken.ts`:
  the proxy forwards only a well-formed `X-Otel-Basis` (a `b1.…` token or
  `latest`) or `X-Otel-Basis-Group`, and drops anything else.
- `app/src/DBDashboardPage.tsx`: pins the group during render (before the
  panels' queries start in their effects) from the searched time range.
- `app/src/components/CompletenessBanner.tsx`: a second line: "Pinned to
  one basis for this refresh (rows received before T): the panels agree
  and do not change until you refresh", or a warning when the page read at
  several bases or some results up to now.

**The adapter side** (`query/internal/hdxadapter/basis.go`): the first
statement of a group mints a basis from the service's `POST /v1/basis` (the
caller's whole scope, every signal), keyed by the caller's token and the
group; the refresh's concurrent statements wait for that one mint. A basis
the service stops accepting (`basis_invalid` after a restart with a
per-process key, `basis_expired`) is re-minted once. An explicit
`X-Otel-Basis` is passed through as is.

**Checked (2026-09-28).** The three patches apply with `git am` to a clean
`885d30c` and give the tree that was checked; the touched files parse
(`transpileModule`) and match the repository's Prettier config;
`completeness.ts` and `queryServiceToken.ts` type-check under `strict`
(TypeScript 6.0.2, with a one-line stub for each of their two type
imports); the new tests' assertions (`completeness.test.ts`,
`queryServiceToken.test.ts`, `CompletenessBanner.test.tsx`'s
`basisMessage`) pass against the transpiled modules with React and Mantine
stubbed. **Not run:** `yarn install`, jest itself, the app's and the API's
full type checks (the box had under 1 GB free while this was built), a
built HyperDX.

## 0004: the user's token server-side, and labelled samples (D33)

**Why.** Through 0003, every query HyperDX's API sent itself (MCP tools, the
external API, AI summaries, Prometheus, alert previews) ran as the one
service identity of `HDX_QUERY_SERVICE_TOKEN_FILE`: the query service scoped
and audited it as the service, not as the person who asked (owner decision
2026-09-28: user-started server-side calls carry the user's token). And the
query service now answers HyperDX's typeahead samples as labelled samples
(`completeness: sample`), which the banner read as `unknown`.

**What.**

- `common-utils/src/clickhouse/node.ts`: `runAsQueryServiceUser(token, fn)`
  (an `AsyncLocalStorage`, one per process whatever the bundle) marks work
  a user started; `queryServiceAuth()` sends that user's token on every
  query made inside it, however deep and asynchronous. **A query started by
  a user whose token did not reach the API fails; it is never sent as the
  service identity.** Outside any user's work (the alert task, usage
  stats) the service token file is used, as before.
- `api/src/middleware/queryServiceUser.ts`, mounted in `api-app.ts` before
  every router: each request's work runs as its user, with the token from
  `HDX_QUERY_SERVICE_TOKEN_HEADER` (the header the OIDC proxy sets; the same
  one 0002's `/clickhouse-proxy` forwards). **MCP and external API clients**
  authenticate to HyperDX with an access key in `Authorization`, so they
  send their OIDC token in that header beside it (e.g.
  `X-Forwarded-Access-Token: <token>`). HyperDX does not check that the
  token's subject is the access key's user: the service scopes by the token.
- `common-utils/src/clickhouse/completeness.ts`: `X-Otel-Completeness:
  sample` and `X-Otel-Sample` are read into a `sample` label (rows read, the
  bound, whether reading stopped at it). Samples are kept out of the page's
  worst label (a sampled suggestion list says nothing about the charts) and
  counted apart (`samples`, `samplesAtBound`, `sampleRowsRead`).
- `app/src/components/CompletenessBanner.tsx`: a line of its own whenever
  the page has a sample: "One suggestion list on this page is a sample:
  stopped reading at the row bound (up to N rows read), so values beyond it
  are not listed", or, under the bound, "read as a sample … not labelled
  complete"; a page with only samples shows a yellow "Sampled suggestions"
  banner. Never hidden, never complete.
- Fixes 0003's `completeness.test.ts` ("reads the adapter headers" missed
  `atBasis: false`): found by running the suite, which 0003's checks did
  not.

**Checked (2026-09-28).** The four patches apply with `git am` to a clean
`885d30c` and give the tree that was tested. With each touched package's
dependencies installed at `yarn.lock`'s versions next to the clone (npm; no
`yarn install`, disk):

| package | type check | tests |
|---|---|---|
| `@hyperdx/common-utils` | **`tsc --noEmit -p tsconfig.json`, the whole package: clean** | **jest, all 39 unit suites: 2,674 tests pass** (the new ones: samples read and summarized; the user's token used, a user's work without one refused, the service identity outside; the context followed through async code; the node client's `auth` per context) |
| `@hyperdx/app` | `CompletenessBanner.tsx` and its test: clean (strict, `react-jsx`, the real `@mantine/core` and testing-library types); it found the switch missing the new `sample` case | `CompletenessBanner.test.tsx` (9) passes under jest + jsdom with a minimal `renderWithMantine` |
| `@hyperdx/api` | `middleware/queryServiceUser.ts`, `utils/queryServiceToken.ts`, `config.ts` and their tests: clean (strict, a one-declaration stub for the common-utils import); `api-app.ts`: parses only | `queryServiceUser.test.ts` (3: the header's token reaches a query made later in the request; no token, or not one token, fails; outside a request, the service identity) and `queryServiceToken.test.ts` (5) pass, against common-utils' sources |

The touched files are formatted with the repository's Prettier (3.3.3).
Not run: ESLint, the app's and the API's full type checks and suites,
`ci:int` (Mongo, ClickHouse), an MCP client against a built HyperDX.
