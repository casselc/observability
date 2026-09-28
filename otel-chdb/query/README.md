# query: the query service both UIs sit on

The first slice of the service that the HyperDX fork and the lake-first UI
share ([`../research/lake-ui.md`](../research/lake-ui.md) §3, "Shared pieces";
[`../DECISIONS.md`](../DECISIONS.md) "Owner decisions, 2026-09-27" and D22):

- **`POST /v1/query`**: SQL on central ClickHouse, constrained to our tables,
  parsed and rebuilt from the tree, scoped to the caller's clusters and
  namespaces, with the caller's limits and every overflow mode pinned to
  `throw`;
- **`POST /v1/plan`**: the lake's plan call: the objects of a signal and
  window in the caller's clusters, each with a presigned GET URL, its size
  and its time range;
- every answer carries its **source** and that source's
  **`complete_through`** (custody time), the **`max_lateness`** policy that
  bridges it to event time, and marks what extends past it (STPA R-S1, R-S2,
  CAST row 26); a windowed answer counts its **late rows** (received more than
  `max_lateness` after their event time);
- **OIDC** bearer tokens (RS256, the issuer's JWKS), claims mapped to
  clusters, namespaces and roles, **deny by default**; every decision is
  **audited** before anything runs (R-S8);
- `/metrics` and `/healthz`.

Labels as elsewhere: **[M]** measured here (the shared 4-vCPU box, SeaweedFS
4.47 and ClickHouse 26.10.1 on localhost), **[D]** read in a source, **[E]**
estimate.

**Why Go.** The rewrite proxy ([`../entities/rwproxy/`](../entities/rwproxy/))
already chose the ClickHouse-grammar parser
[`github.com/AfterShip/clickhouse-sql-parser`](https://github.com/AfterShip/clickhouse-sql-parser)
v0.5.6: it parses 799 of 799 of HyperDX's statements where Rust's
`sqlparser` parses 229 (rwproxy README §2). This service uses the same parser
and version, the AWS SDK the controller uses, `golang-jwt/jwt/v5` for token
checks, and `pgregory.net/rapid` v1.2.0 for property tests.

## 1. Running it

```
go build ./cmd/queryd
QS_CH_PASSWORD=… ./queryd -config queryd.example.json
```

[`queryd.example.json`](queryd.example.json) is the whole configuration.
Endpoints and secrets can be set or overridden in the environment:
`QS_LISTEN`, `QS_OIDC_ISSUER`, `QS_OIDC_AUDIENCE`, `QS_OIDC_JWKS_URL`,
`QS_AUDIT_PATH`, `QS_CH_URL`, `QS_CH_USER`, `QS_CH_DATABASE`, the password
in `QS_CH_PASSWORD` (or the variable `central.password_env` names),
`QS_S3_ENDPOINT`, `QS_S3_PUBLIC_ENDPOINT`, `QS_S3_BUCKET`, `QS_S3_REGION`,
`QS_S3_KEY` / `QS_S3_SECRET` (otherwise the AWS SDK's default chain: web
identity, instance role, `AWS_*`), `QS_LAKE_ROOT`, `QS_LAKE_CTL`,
`QS_CATALOG_DB`, `QS_LAKE_ENABLED`, and the completeness policy
`QS_MAX_LATENESS_S` (`watermark.max_lateness_s`, default 60) and
`QS_COUNT_LATE` (`watermark.count_late`, default true), §2.1. An audit path and a bucket are
required: the service does not start without somewhere to record decisions,
or without `{ctl}/watermark.json` to label results with.

**The ClickHouse user** is read-only and can read only the served tables:

```sql
CREATE USER otel_query_ro IDENTIFIED WITH sha256_password BY '…' SETTINGS readonly = 2;
GRANT SELECT ON otel.otel_logs TO otel_query_ro;
GRANT SELECT ON otel.otel_traces TO otel_query_ro;
-- catalog-scoped tables and R-S5: the catalog's resources view (and what it reads), ingest_log
GRANT SELECT ON entities.resources TO otel_query_ro;
GRANT SELECT ON entities.ingest_log TO otel_query_ro;
-- the metadata tables (scope "metadata", §3), for the HyperDX adapter (§8).
-- 26.10 needs a grant for data_skipping_indices; the rest are readable by
-- every user, and every one of them shows only what the user may see
GRANT SELECT ON system.tables, system.columns, system.data_skipping_indices, system.settings,
  system.table_engines, system.databases TO otel_query_ro;
-- HyperDX finds a rollup through its materialized view's definition: SHOW, not SELECT
GRANT SHOW TABLES ON otel.otel_logs_attr_kv_rollup_15m_mv TO otel_query_ro;
GRANT SHOW TABLES ON otel.otel_traces_kv_rollup_15m_mv TO otel_query_ro;
```

`readonly = 2` lets the service set settings per statement (the limits, the
filters) and nothing else; `readonly` itself cannot be changed [M]. The
grants are the second fence behind the allow-list (§3).

## 2. API

Every endpoint under `/v1/` takes `POST` with a JSON body and an
`Authorization: Bearer <OIDC token>` header, answers JSON with
`Cache-Control: no-store`, and answers CORS preflights for the configured
origins (`cors_origins`). Every answer has a `request_id`, which is the audit
record's and ClickHouse's `query_id`.

### 2.1 `POST /v1/query` (role `query`)

```json
{"sql": "SELECT ServiceName, count() FROM otel_logs GROUP BY ServiceName",
 "window": {"from": "2026-09-28T12:00:00Z", "to": "2026-09-28T13:00:00Z"}}
```

`window` is optional; `from`/`to` are RFC 3339 or integer nanoseconds. When
it is given, every table read with a `time_column` is restricted to
`[from, to)` by the service (§3), so the completeness label describes the
rows the statement could see.

`output` is optional: output-format settings that change how values are
written, never which rows, from an allow-list (`OutputSettings`: today only
`date_time_output_format` = `simple` | `iso` | `unix_timestamp`, which HyperDX
needs as `iso`). Any other name or value is **400 `bad_output`** and nothing
runs.

```json
{"request_id": "5c0f…",
 "source": "central",
 "complete_through": "2026-09-28T12:59:31.2Z", "complete_through_ns": 1790…,
 "max_lateness_s": 60, "settled_through": "2026-09-28T12:58:31.2Z", "settled_through_ns": 1790…,
 "completeness": "partial", "partial": true,
 "incomplete_from": "2026-09-28T12:58:31.2Z", "incomplete_from_ns": 1790…,
 "watermark": {"status": "ok", "key": "edges/_consumer/watermark.json", "age_s": 12.1, "lag_s": 28.8,
               "fetched_at": "…", "holding": [{"lane": "prod-eu-1/pub-0/logs", "wm_ns": 1790…, "lag_s": 28.8}]},
 "catalog": {"status": "ok", "clusters": {"prod-eu-1": {"last_put": "…", "lag_s": 41.0, "status": "ok"}}},
 "late": {"max_lateness_s": 60, "status": "counted", "rows": 0, "tables": {"otel.otel_logs": 0}},
 "query": {"hash": "9e1d…", "tables": ["otel.otel_logs"], "sql": "SELECT ServiceName, count() FROM otel.otel_logs GROUP BY ServiceName",
           "scoped": true, "window": {"from_ns": …, "to_ns": …}, "rows_read": 5120, "elapsed_ms": 18.2},
 "result": { ClickHouse's FORMAT JSON: meta, data, rows, statistics }}
```

- **Two clocks** (STPA CAST row 26, D26). `complete_through` is **custody
  time**: every row *received* (`received_at`, the edge's custody stamp)
  before it is in central. A `window` is **event time** (the rows'
  `Timestamp`). `max_lateness_s` is the policy that bridges them: a row is
  assumed received within `max_lateness` of its event time, so event time
  is settled through `settled_through` = `complete_through − max_lateness`.
- **`completeness`**: `complete` (the window ends at or before
  `settled_through`, i.e. `complete_through ≥ to + max_lateness`, and the
  watermark is current), `partial` (it extends past it: draw from
  `incomplete_from` = `settled_through`, clamped to the window, on as
  incomplete, count over it as partial), or `unknown` (the watermark is
  missing, unreadable or stale: **nothing in the result may be read as
  settled**). A statement without a window runs up to now, so it is always
  `partial` or `unknown`. Until 2026-09-28 a window was `complete` once
  `complete_through ≥ to`: a row with its event time in the window, still at
  an edge when the window closed, was missing from a result labelled complete.
- **`late`**: rows later than the policy are **counted, not hidden**. For a
  windowed statement the service runs one more statement after it, under the
  same filters (scope and window) and limits: per table read that has a
  `received_column` (table config; `received_at` in the consumer's schema,
  every central table has it per row), `count()` of rows with `received_at > time_column +
  max_lateness`. `status`: `counted` (`rows`, per table in `tables`;
  `uncounted` lists tables without a received column), `no_window` (an
  unbounded statement is not counted), `not_measured` (no table read has
  both columns), `disabled` (`watermark.count_late: false`), `error` (the
  count failed; the result stands, `rows` is null). Late rows are *in* this
  result; a non-zero count on a `complete` window means a result over it
  served before they came lacked them, and that `max_lateness` is too
  short for this data (the policy to change, not the code).
- **`watermark`**: the document's own freshness: `status` (`ok`, `stale`:
  the consumer last published more than `watermark.max_age_s` ago, `missing`,
  `error`: unreadable, or the last good copy is older than `max_age_s`),
  `age_s` (now − the document's `wall_ms`), `lag_s` (now −
  `complete_through`), and the lanes holding it back or stale, **cut to the
  caller's clusters** (another cluster's lane names are not disclosed).
- **`catalog`** (R-S5): per cluster in the caller's scope, the newest entity
  object the aggregator has ingested (`ingest_log`) and its age; `lagging`
  past `catalog.lag_max_s`, `missing` for a cluster with no entry,
  `unavailable` when it cannot be read, `not_configured` without a catalog.
- **Errors**: `{"error": reason, "detail": …, "request_id": …}`:

  | status | `error` | when |
  |---|---|---|
  | 401 | `no_token`, `bad_token` | no bearer token; a bad signature, issuer, audience, expiry, algorithm, or no subject |
  | 403 | `role_missing`, `empty_scope`, `bad_scope_value` | the token grants no `query` role / no cluster / a name outside FORMAT.md's pattern |
  | 403 | `table_not_allowed`, `table_function`, `settings_clause`, `format_clause`, `denied_function`, `in_table`, `not_select`, `scope_unenforceable` | the statement is outside the allow-list (§3) |
  | 400 | `parse_error`, `statement_count`, `too_long`, `bad_window`, `query_param`, `bad_identifier` | malformed |
  | 422 | `limit_exceeded:<NAME>` | a pinned limit fired (TOO_MANY_ROWS, TIMEOUT_EXCEEDED, MEMORY_LIMIT_EXCEEDED, …): **the statement failed; nothing was shortened** |
  | 429 | `too_many_concurrent` | the caller's `max_concurrent` statements are running |
  | 503 | `audit_unavailable`, `catalog_unavailable` | the decision could not be recorded, so nothing ran; a catalog-scoped table and no catalog answer |
  | 403 | `central_access_denied` | ClickHouse's grants refused what the allow-list passed: the two disagree (alert on it) |
  | 502 | `central_error` | ClickHouse unreachable or another error |

### 2.2 `POST /v1/plan` (role `plan`)

```json
{"signal": "logs", "from": "2026-09-28T12:00:00Z", "to": "2026-09-28T13:00:00Z", "clusters": ["prod-eu-1"]}
```

`clusters` is optional (default: every cluster in the token's scope); a
named cluster outside the scope is **refused (403 `cluster_not_in_scope`),
never silently dropped**. `signal` is a lane namespace (FORMAT.md §1).

```json
{"request_id": "…", "source": "lake", "signal": "logs", "clusters": ["prod-eu-1"],
 "from": "…", "to": "…", "complete_through": "…", "max_lateness_s": 60, "settled_through": "…",
 "completeness": "partial", "partial": true, "incomplete_from": "…", "late_objects": 0,
 "watermark": {…},
 "snapshot": null, "snapshot_note": "no sealer snapshots exist yet: planned from a LIST of the v2 lanes at listed_at",
 "listed_at": "…", "start_complete": true,
 "expires_at": "2026-09-28T13:05:00Z", "replan_after": "2026-09-28T13:04:00Z", "url_ttl_s": 300,
 "objects": [{"url": "https://lake.example.com/otel/edges/prod-eu-1/pub-0/logs/20260928T…Z-1a2b3c4d/00000000000000000042.parquet?X-Amz-…",
              "size": 498123, "key": "…", "cluster": "prod-eu-1", "producer": "pub-0", "epoch": "…", "seq": 42,
              "last_modified": "…", "min_time_ns": …, "max_time_ns": …, "rows": 10000, "refined": true}],
 "total_bytes": 498123, "unrefined": 0, "mismatched": 0, "objects_hash": "…",
 "rules": ["…"]}
```

**How it plans (no sealer yet).** The sealer and its snapshots don't exist
([`../research/central-optional.md`](../research/central-optional.md) §5), so
the planner **lists the v2 lanes directly**: for each cluster,
`{root}/{cluster}/` → producers → `{root}/{cluster}/{producer}/{signal}/`
(cluster-first keys, FORMAT.md §1, so a cluster's objects are exactly a
prefix). It skips empty slots (heartbeats, tombstones) and objects written
before `from − skew_s` (an event is received no earlier than its time minus
the skew: this bounds *early* receipt; late receipt never drops an object,
which is kept by its rows' event-time range), then HEADs the rest (up to `max_heads`, 16 at a time) for
`oscope-kind`, `oscope-min-time`, `oscope-max-time`, `oscope-rows`,
`oscope-cluster` and `oscope-received`, and keeps data objects whose rows
overlap the window. An object received more than `max_lateness` after its
earliest row is marked `late` (and counted in `late_objects`): late data,
visible in the lake too. An
object whose `oscope-cluster` names another cluster than its prefix is never
planned (`mismatched`, and a metric). Past the HEAD budget, objects are
planned on their LIST entry alone (`refined: false`: a superset, never a
loss). More than `max_objects` objects is **413 `plan_too_large`**: narrow
the window or clusters, or use `/v1/query`. Windows are at most
`max_window_s` (24 h).

**`complete_through` for the lake.** The plan reads the same
`{ctl}/watermark.json` **before** it lists: every request with
`received_at` below it was committed before the document was written, so
before the LIST, which is strongly consistent on SeaweedFS and AWS
(AMBIGUITY.md S6). **GC** deletes slots the consumer has ingested (D12): when
`{ctl}/gc.json` shows deletions in a planned lane and the window starts
before that lane's oldest remaining object, the plan says
`start_complete: false`, names the lanes in `gc_truncated_lanes`, and is
`partial`: the older rows are in central. The label is the same function as
`/v1/query`'s: event time settles at `complete_through − max_lateness`.

**Namespaces.** A raw lane object holds every namespace of its cluster, and
a presigned URL grants the whole object (research/lake-ui.md §6.1), so a
**namespace-restricted token is refused** (403
`namespace_scope_needs_filtering_reader`) and reads through `/v1/query`,
whose scope is per row. Namespace-partitioned compacted files (lake-ui §6.4)
would lift this for older data.

**The rules the plan carries (AMBIGUITY.md X8)**, in every plan's `rules`:

1. Each URL is valid until `expires_at`. **Re-plan before `replan_after`**
   (`expires_at` − `replan_margin_s`, 60 s, cut to half the URL lifetime so a
   plan is never stale when issued); never start reading an object
   whose URL expires within the margin.
2. **A 403 (or any error) on a planned object means re-plan, never "no
   data"**: a query that could not read every planned object is incomplete
   and must say which objects it lacks.
3. Rows with an event time at or after `incomplete_from`
   (`complete_through − max_lateness`) may still arrive: draw that region as
   incomplete, and count over it as partial. Objects marked `late` arrived
   after that policy.
4. `snapshot` is null: this plan lists lanes directly; a later plan may
   include objects this one did not.

URL lifetime is `url_ttl_s` (default 300 s, clamped to 60–900 s): there is
no revocation before expiry (lake-ui §6.5). A URL presigned for GET answers
**HEAD with 403** (SigV4 binds the method: [M] in the integration test, and
lake-ui finding 2); the plan carries `size` so a range reader never needs a
HEAD, and engines that HEAD need lake-ui's shim. The bucket needs CORS for
the UI's origin: `GET`, `HEAD`, header `Range`, exposing `Content-Range`,
`Content-Length`, `ETag` (lake-ui §2.2). Presigned URLs are signed for
`s3.public_endpoint` (the host the browser reaches; SigV4 signs the host).

**Index filters (D27).** A plan may carry `"trace_id": "<32 hex>"` (signals
`traces` and `logs`) and/or `"terms": ["…"]` (signal `logs`: each a
case-insensitive substring of `Body` in JavaScript's `toLowerCase` sense;
all must match; at most 8, each 1–256 bytes). The service resolves them
against the lake index of the plan's clusters (FORMAT.md §7; §9 below):
objects the index rules out are **not planned**; a covered object that may
match is planned with `"index": "hit", "row_groups": [0, …]`; every other
object (not indexed yet, index unreadable or corrupt, over the per-plan
index budget) with `"index": "scan"`: read it whole, as without a filter.
The plan's `index` block says what happened:

```json
"index": {"trace_id": "…", "constraints": ["suffix:timeout", "prefix:acct"], "segments": 1, "requests": 3,
          "bytes": 13698, "cache_hits": 0, "covered": 3, "scan": 1, "pruned": 2, "errors": [], "elapsed_ms": 4,
          "rule": "Objects marked index=hit hold matches only in row_groups; …"}
```

An index can only remove what cannot match; the page still tests every row.
Config `lake.index`: `disabled`, `max_mb_per_plan` (64), `cache_mb` (128).
Metrics `qs_plan_index_objects_total{outcome}`, `qs_plan_index_errors_total`,
`qs_plan_index_bytes_total`. Refusals: 400 `bad_trace_id`, `bad_filter`.

No Iceberg-REST `loadTable` yet: it needs the sealer's metadata.

### 2.3 `/healthz`, `/metrics`

`GET /healthz` is liveness: 200 with the watermark's status and age (a stale
watermark labels results; it does not stop them). `GET /metrics`
(Prometheus text):

| metric | |
|---|---|
| `qs_requests_total{endpoint,code}` | every answer |
| `qs_denials_total{endpoint,reason}` | 401, 403, 400, 413, 429 by reason |
| `qs_planned_objects_total`, `qs_planned_bytes_total` | what plans handed out |
| `qs_plan_mismatched_objects_total` | objects whose metadata names another cluster than their prefix (never planned; should stay 0) |
| `qs_query_rows_read_total` | rows central read for allowed statements |
| `qs_clickhouse_errors_total{code}` | ClickHouse errors (limits included), `transport` for no answer |
| `qs_audit_errors_total{event}` | audit writes that failed (a failed `decision` refused its request) |
| `qs_results_total{source,completeness}` | results by label: a rising `unknown` is the pipeline's watermark failing |
| `qs_late_results_total{source,completeness}` | results holding rows (query) or objects (plan) later than `max_lateness`: rising on `complete`, the policy is too short |
| `qs_late_count_errors_total` | late-row counts that failed (the result was served with `late.status: error`) |
| `qs_max_lateness_seconds` | the policy the labels are made with |
| `qs_watermark_age_seconds`, `qs_complete_through_seconds`, `qs_watermark_status{status}` | the watermark as the service sees it |

No credentials appear in any answer except the presigned URLs themselves.

## 3. SQL: parsed, checked, rebuilt; scope as table filters (R-S9)

`internal/sqlscope`:

1. **Parse** with the ClickHouse grammar. Exactly one statement, and it must
   be a `SELECT` (with `WITH`, subqueries, joins, `UNION`/`EXCEPT`/`INTERSECT`).
2. **Check** every node, found by a reflective walk over the whole tree (so
   a node kind the parser's own walker skips is still seen):
   - every table is an allow-listed `db.table` or a CTE visible at that
     point (the SELECT's own `WITH` and its ancestors'; a set-operation
     branch does not see the first branch's `WITH`, so an ambiguous name is
     refused, never read as a table); **no table functions** (`s3`, `url`,
     `merge`, `remote`, `cluster`, `numbers`, …);
   - no `SETTINGS` (anywhere, subqueries included) and no `FORMAT`: limits and
     output are the service's;
   - no functions that read objects by name or stall the server
     (`dictGet*`, `joinGet*`, `file`, `getSetting*`, `sleep*`, …); no
     `x IN table_name`; no query parameters; no quoted identifier with a
     backslash (ClickHouse and the parser lex those differently).
3. **Rebuild** the statement from the tree (the parser's formatter), with
   every table qualified (`otel_logs` → `otel.otel_logs`, so ClickHouse's
   default database never decides). Comments do not survive. The rebuilt
   text is **parsed and checked again** and must be a fixed point of
   format-then-parse and read the same tables; otherwise the statement is
   refused (`roundtrip`).
4. **Scope.** For each table read, a predicate built as a tree from the
   token's attributes and the table's configured expressions:
   - `columns`: `<cluster_expr> IN ('prod-a', …) AND <namespace_expr> IN (…)`
     (e.g. ClickStack's `` `__hdx_materialized_k8s.cluster.name` `` on
     `otel_logs`, `ResourceAttributes['k8s.cluster.name']` on `otel_traces`);
   - `catalog`: `resource_id IN (…)` over the ids the entity catalog resolves
     for the caller's clusters and namespaces (`resources.attrs`); an empty
     answer reads no row, no answer refuses the statement;
   - `fleet`: no per-row scope; only a caller with every cluster and
     namespace may read the table;
   - `metadata`: a table of the `system` database whose rows are schema, not
     telemetry (`system.tables`, `columns`, `data_skipping_indices`,
     `settings`, `table_engines`, `databases`); no per-row scope, any caller
     with the `query` role. ClickHouse cuts what they show to the tables the
     read-only user may see (the served ones). Configuring a table outside
     `system`, or one with a time column, as `metadata` is refused at start.
     **Only allow-listed columns** are served: each metadata table has
     `columns` (default `sqlscope.MetadataColumns`: what HyperDX reads), and
     every read of it, anywhere in the statement, is rebuilt as `(SELECT
     <columns> FROM system.t) AS t`, so `SELECT *` expands to the allow-list
     and naming `data_paths`, `metadata_path`, `uuid` or any other column is
     ClickHouse's `UNKNOWN_IDENTIFIER` (400 `central_rejected`). The rebuilt
     text is checked to read each metadata table only as the FROM of its
     projection (`metadata_unprojected` otherwise). `system.tables` serves
     `database, name, table, engine, is_temporary, create_table_query,
     engine_full, as_select, partition_key, sorting_key, primary_key,
     sampling_key, total_rows, comment` (ClickHouse masks the secrets in the
     DDL columns: `[HIDDEN]` [M]); `system.databases` `name, engine,
     comment` (not `data_path`, `metadata_path`, `engine_full`);
   - with a `window`, `time_column >= … AND time_column < …`.
   Cluster names must match FORMAT.md's `[a-z0-9]([a-z0-9._-]{0,61}[a-z0-9])?`
   and namespaces Kubernetes' label pattern, or the token is refused.

   The predicate is **not spliced into the statement's `WHERE`**. It is
   passed as ClickHouse's `additional_table_filters` setting, per table.
   Measured on 26.10 [M]: ClickHouse applies it to **every read of that
   table**: in subqueries, CTEs, both sides of a self-join, `IN (SELECT …)`,
   through a view, with projections (the filtered count stays right when a
   projection could have answered), and as a `PREWHERE` that uses the
   primary key. A `SELECT` alias cannot shadow it (`SELECT 'qb' AS
   `` `__hdx_materialized_k8s.cluster.name` `` … still reads only `qa`'s
   rows, in the integration test), which a `WHERE` predicate would not
   survive. It does not apply to `merge()` (the allow-list refuses table
   functions). The caller's text cannot reach the setting: `SETTINGS` is
   refused and settings travel in the URL.
5. **Run** as the read-only user with the caller's limits
   (`max_execution_time`, `max_rows_to_read`, `max_bytes_to_read`,
   `max_result_rows`, `max_result_bytes`, `max_memory_usage`; `default`,
   raised by any group in `limits.groups`) and **all eleven
   `*_overflow_mode` settings pinned to `throw`** (the HyperDX fork's patch
   0001 pins the same eleven: AMBIGUITY.md X7, C2), so a limit fails the
   statement (422) instead of shortening it. `wait_end_of_query=1`: an
   error is an HTTP status, never a 200 followed by an exception (C7).
   `query_id` is the request id and `log_comment` names the caller, so
   `system.query_log` joins the audit log. At most `max_concurrent`
   statements per subject run at once (429).

**Three fences.** The allow-list is the first. The second is ClickHouse's
grants: the user can read only the served tables (in the integration test
it can neither write nor read `system.users` [M]). The third is the table
filter, which holds even if the parser and ClickHouse read a text
differently: a statement that somehow reaches an allowed table still reads
only the caller's rows.

Cost: parse, check, rebuild, re-parse and filter build take **~99 µs** per
statement (`BenchmarkPrepareFinish`, a 150-byte aggregate) [M].

**Known refusals of valid SQL** (fail closed): `x IN (WITH … SELECT …)`
does not parse in v0.5.6 (found by the property test); a CTE used in a
later `UNION` branch is refused as an unknown table.

## 4. Authentication and authorization (R-S8)

- **Tokens**: RS256 (RS384/RS512 if configured; never `none` or HMAC),
  checked against the issuer's JWKS (from `jwks_url` or the discovery
  document, whose `issuer` must match), `iss`, `aud`, `exp` (required),
  `nbf`, with `leeway_s`. Keys shorter than 2048 bits are ignored. The key
  set is re-read every `jwks_refresh_s` and on an unknown `kid` (at most
  once per `jwks_min_refresh_s`); a failed refresh keeps the previous set.
- **Claims → attributes** (`claims`): a subject claim (required), and
  clusters, namespaces and roles read from claims of those names (a list, or
  one string separated by commas or spaces) and from **groups**
  (`group_grants`: a group name → clusters, namespaces, roles). `"*"` means
  all. Only the roles `query` and `plan` exist; anything else in a roles
  claim is dropped.
- **Deny by default**: a token that grants no role can do nothing; no
  cluster is `empty_scope`; no namespace grants no namespace (so every
  scoped read is refused) unless a claim or group grants `"*"`.
- **The read side of D18's ABAC.** The write-side attributes in
  [`../deploy/iam/`](../deploy/iam/) are per cluster (`${aws:PrincipalTag/cluster}`
  on AWS, one identity per cluster elsewhere); the read side keeps the same
  unit: a token's clusters are the `{cluster}` segments of FORMAT.md's keys
  and the values of `k8s.cluster.name`. Those must agree per deployment (the
  edge's `cluster` setting and the collector's `k8s.cluster.name`
  attribute). The service holds the only credentials that read the bucket;
  viewers get presigned URLs. Session-tagged STS signing (lake-ui §6.3) as
  defence in depth on AWS is not built.
- **Audit** (`internal/audit`): one JSON line per decision, `event:
  "decision"`, **written before anything runs or any URL is returned**; if
  it cannot be written the request is refused (503) and nothing runs. It
  carries the subject, groups, roles, the scope applied, the client address,
  the input's and the rebuilt statement's hashes, the rebuilt SQL (8 KiB),
  the tables and the number of filters; for plans the signal, the object
  count, bytes, a hash over every planned key and the first 50 keys, and the
  URLs' expiry. An `outcome` line follows each allowed statement (status,
  rows read, elapsed, error). The file is opened `O_APPEND`, mode 0600, and
  fsynced per record (`audit.fsync`). `Shipper` is the interface for
  sending records elsewhere (a SIEM, a separate bucket); `File.StartShipping`
  feeds it in batches with retries, and the file stays the record of truth.
  The store's own access log records each GET under the service's key; the
  plan's audit line (keys, expiry, subject) is what links them to a person
  (lake-ui §5.4).

## 5. Requirements

| | Requirement | Status |
|---|---|---|
| R-S1 | every result carries its source and complete-through | **met for this service's answers**: `source`, `complete_through`, the watermark's freshness; on every `/v1/query` and `/v1/plan` answer. **HyperDX**: the adapter (§8) returns the label in `X-Otel-*` headers and the fork's patch 0002 shows it as a banner (built and unit-tested; not run in a live HyperDX) |
| R-S2 | windows not yet complete drawn as incomplete, counts marked partial | **met at the API**: `partial`, `incomplete_from`, `completeness`, in event time (`complete_through − max_lateness`, CAST row 26); a missing, unreadable or stale watermark is `unknown`, never `complete` (unit tests for each); rows later than `max_lateness` are counted (`late`). Per-bucket marking is the UI's |
| R-S3 | alerts evaluate only up to complete-through; a failed evaluation pages | **not this service's**: the alert evaluator can use `completeness`/`incomplete_from` to pick its windows (X5) |
| R-S5 | views show catalog lag and rows without an entity match | **partly**: catalog lag per cluster from the aggregator's `ingest_log`; rows without an entity match are not counted yet |
| R-S8 | reads role-scoped by cluster and namespace; every query audit-logged | **met for this service**: deny by default, per-row scope on central, per-object (cluster) scope on the lake, namespace-restricted plans refused; every decision audited before it acts. HyperDX reaches it through the adapter (§8) with the user's own token once the fork's patch 0002 is deployed; the adapter holds no ClickHouse credentials |
| R-S9 | SQL only from a parsed tree, per-user limits | **met**: rebuilt from the tree and checked twice; scope as table filters; per-caller limits and concurrency; overflow modes pinned |
| X5 | alert evaluation | **open**; this service's labels are the input the evaluator needs |
| X6 | paging | **open** (no code) |
| X7 | UI queries: partial results with HTTP 200 | **handled for this service**: every overflow mode `throw`, `wait_end_of_query`, a limit is a 422; unknown completeness is labelled |
| X8 | presigned URL expiry | **partly**: the plan carries expiry, `replan_after` and the rules; the lake UI ([`../lakeui/`](../lakeui/README.md)) follows them, tested in Chromium against this service |

## 6. Tests

```
go test ./...                                    # unit + property tests (the integration test skips)
QS_IT_BIN=<dir with otelcol-s3pq and consume> go test ./integration -v
```

- **`internal/sqlscope`**: accepted statements and their rebuilt text; 28
  refusals (system tables, other databases, `s3`/`url`/`merge`/`remote`/
  `cluster`, `SETTINGS` in a subquery, `FORMAT`, DDL, DML, `SET`, two
  statements, `dictGet`/`joinGet`/`getSetting`/`sleep`/`file`, `IN table`,
  query parameters, a backslash identifier, CTE visibility across `UNION`
  and out of a subquery); filters for two tables and a join; catalog scope;
  fleet tables; bad scope values; windows; hashes. **Property tests**
  (`rapid`): random statements from allowed and forbidden pieces are
  accepted exactly when no forbidden piece was used, and an accepted one
  parses back to itself, reads only allow-listed tables (each qualified),
  has no table function or `SETTINGS`, has a filter per table and keeps
  every string literal byte for byte (20,000 cases in 2.5 s [M]); any string
  a token carries as a cluster is refused or lands as exactly that literal.
  Removing the table-function check fails the property on its first case.
- **`internal/auth`**: good token; wrong audience, issuer, expired, no
  `exp`, `nbf` in the future, forged with our `kid`, HS256 signed with a
  public-key-shaped secret, `alg: none`, garbage; key rotation refetches the
  JWKS once; claims mapping denies by default.
- **`internal/completeness`**: labels for closed, open, after and unbounded
  windows; caching; missing, unreadable, garbage and stale documents are
  `unknown`; a good copy ages out when the store stops answering. CAST row
  26's regression (`TestLateRowNotComplete`: a row received after its
  window's end keeps the window partial until `complete_through ≥ end +
  max_lateness`; with `max_lateness` 0, D22's rule, it was complete) and a
  **rapid property over rows whose receive time diverges from their event
  time** (from 5 s early to twice `max_lateness` late): a window labelled
  complete holds every row with its event time in it received before its end
  + `max_lateness`; a partial one every such row before `incomplete_from`.
  With the label computed as before the fix the property fails at once.
- **`internal/sqlscope`** (metadata, late): every read of a metadata table
  is projected to its allow-list (`*`, aliases, subqueries, joins, `IN`,
  idempotent), config refusals, and a rapid property that the rebuilt text
  reads metadata tables only through their projection; the late-row count
  statement (only tables with both columns, metadata excluded).
- **`internal/server`** (fakes for ClickHouse and S3, the local issuer): 401
  and 403 are audited and run nothing; the filter, overflow modes and limits
  reach ClickHouse; the label; refusals; an audit failure refuses both
  endpoints; a missing watermark; the concurrency limit; a limit error is
  422; plans: scope, heartbeats, old objects and a cross-cluster object
  excluded, expiry, rules, audit; cross-cluster and namespace plans denied;
  fleet plans and GC truncation; CORS; metrics. The label's
  `max_lateness_s`, `settled_through` and `incomplete_from`; a window
  ending 20 s before `complete_through` is partial, one ending
  `max_lateness` before it complete; the late count runs under the
  statement's filters, limits and `query_id`-late, reports per table and
  uncounted tables, `no_window`, `disabled` and a failed count (the result
  stands); plans mark late objects.
- **`integration`** [M] (97 s, 80 of them the late-row story): the Go edge (`otelcol-s3pq`,
  [`../conformance/go-edge.yaml`](../conformance/go-edge.yaml)) publishes
  logs and spans for clusters `qa` and `qb` (two namespaces each) to
  SeaweedFS; the Rust consumer ingests them into ClickHouse and publishes
  `watermark.json`; a read-only user and a stand-in catalog (the edges'
  announcements as `resources`) are created. Then: fleet 12 logs, `qa` 5,
  `qa` asking for `qb` 0, `qa` with an alias shadowing the scope column 5,
  through a CTE and a self-join 5, `qa/shop` 3; traces scoped by the catalog
  3 / 2 / 8; refusals; the read-only user cannot write; `max_result_rows =
  2` fails with 422; the label reads the real document (`ok`, lag ≈ 15 s) and
  a window closed before `complete_through − max_lateness` is `complete`,
  with its 5 rows counted late (the fixture stamps them 10 minutes before
  sending). **Late rows** (CAST row 26), with both edges beating between
  steps: `complete_through` passes `t0` (3 s past it) while a row with event
  time `t0 − 1 s` has not been sent: `[t0 − 1 min, t0)` is `partial` from
  `complete_through − 60 s` (D22's rule said `complete`, with 0 rows); the
  row goes through the real Go edge 20 s after its event time (within
  `max_lateness`): 1 row, still `partial`, `late.rows` 0; a row stamped
  15 minutes back lands in an old window that was `complete` with 0 rows:
  still `complete`, 1 row, `late.rows` 1 (counted, not silent); once
  `complete_through ≥ t0 + 60 s`: `complete`, 1 row. The plan returns only `qa/`
  objects, each GET 200 with the planned size and a Parquet header, HEAD
  with the same URL 403; `qa` asking for `qb` is 403, `qa/shop` is 403; the
  fleet plans both clusters; 42 audit lines. Everything is named `qs-…` /
  `qs_…` and removed. Nightly in CI (`query-integration`).

## 7. What's next

1. **The HyperDX fork through the service**: built (§8, the fork's patch
   0002). Left: what §8.5 lists (EXPLAIN estimates, text-index discovery,
   performance settings, CSV, the rewrite proxy's dictionaries), and a run of
   the patched HyperDX itself.
2. **The lake UI on `/v1/plan`**: first slice built in
   [`../lakeui/`](../lakeui/README.md) (hyparquet range reads with the plan's
   sizes, re-plan before `replan_after`, a 403 as re-plan, the incomplete
   region drawn after `incomplete_from`); next: snapshots, the maplet and term
   index in plans, namespace-scoped viewers through `/v1/query`.
3. **The alert evaluator** (X5, R-S3; built, D23): evaluate only windows
   labelled `complete` with `complete_through ≥ end + max_lateness +
   lateness` (D26), page on
   `unknown` for longer than a bound and on a failed evaluation, and never
   read "no data" as OK; paging per X6.
4. **The sealer's snapshots** replace the LIST (snapshot ids in plans,
   as-of reads, an Iceberg-REST `loadTable`), and per-file `resource_id`
   ranges make catalog-scoped lake plans possible.
5. **R-S5's second half**: count rows whose `resource_id` the catalog lacks
   (`resource_evidence`), per result or as a metric.
6. **Per-cluster `complete_through`**: the document carries only the
   minimum over every lane and the lanes holding it; a restricted caller's
   label is conservative (another cluster's stall holds it back). The
   consumer could publish a watermark per cluster.
7. **Rate limits** beyond concurrency (statements per minute, bytes read per
   hour), and a cost estimate (`EXPLAIN ESTIMATE`) before cold statements
   (SEC-5).

## 8. The HyperDX adapter (`cmd/hdxadapter`, `internal/hdxadapter`)

The owner chose "HyperDX fork first" (DECISIONS.md, owner decisions
2026-09-27); D25 records this design. HyperDX talks to ClickHouse only over
the HTTP interface (`@clickhouse/client` in the API and alert task,
`@clickhouse/client-web` in the browser through the API's
`/clickhouse-proxy`) [D]. The adapter speaks enough of that interface for
those two clients and sends every statement to `POST /v1/query` **with the
caller's own bearer token**. It holds **no ClickHouse credentials**: what
reaches ClickHouse is what the service parsed, allow-listed, rebuilt, scoped
and audited.

```
browser ─ HyperDX app ─ API /clickhouse-proxy ──┐   (fork 0002: user's token → Authorization: Bearer)
alert task / API / MCP (node client) ───────────┤   (fork 0002: HDX_QUERY_SERVICE_TOKEN_FILE → auth.access_token)
                                                ▼
                          [rwproxy, optional: entity rewrite, same wire]
                                                ▼
hdxadapter :18191 ── bind params · strip FORMAT · DESCRIBE/SHOW → system SELECT · derive window
                                                ▼  POST /v1/query {sql, window?, output?}  Bearer <user token>
queryd ── parse · allow-list · rebuild · scope (additional_table_filters) · audit · limits
                                                ▼
ClickHouse (read-only user)            answer: ClickHouse's format + X-Otel-* label headers
```

Run: `go build ./cmd/hdxadapter && ./hdxadapter -config hdxadapter.example.json`
(`HDXA_LISTEN`, `HDXA_QUERY_URL`, `HDXA_TOKEN_HEADER` override). HyperDX's
connection host is the adapter; its username/password are ignored (never
forwarded). `time_columns` mirrors the service's `central.tables`
(used only to derive windows, below); `token_header` lets the adapter take the
token from a header other than `Authorization` (oauth2-proxy's
`X-Forwarded-Access-Token`) when HyperDX runs without the fork's forwarding.

### 8.1 What it does with each kind of statement

| HyperDX sends | where | the adapter | why |
|---|---|---|---|
| `SELECT` / `WITH … SELECT` on the served tables | charts, search, traces, sessions, raw SQL, alerts | binds parameters, strips the trailing `FORMAT`, sends it; renders the answer in the requested format | the service's normal path |
| `SELECT … FROM system.tables / data_skipping_indices / settings / table_engines` | `metadata.ts` schema discovery, `getSettings`, `getServerVersion`-style probes | as a SELECT; the service serves these six system tables as scope `metadata` (§3) | schema, not rows; ClickHouse's grants cut them to the served tables |
| `DESCRIBE [TABLE] db.table` | `metadata.ts` `getColumns` | rewritten to `SELECT name, type, default_kind AS default_type, … FROM system.columns WHERE database = … AND table = … ORDER BY position` (DESCRIBE's column names and order; `ttl_expression` always `''`); **zero rows → UNKNOWN_TABLE (60)**, never an empty column list | the service takes SELECT only; answers compared equal to DESCRIBE on all 50 captured statements [M] |
| `SHOW DATABASES`, `SHOW TABLES FROM db` | the source form's pickers (`app/src/clickhouse.ts`) | rewritten to SELECTs on `system.databases` / `system.tables` | as above |
| `EXPLAIN …` (`ESTIMATE`, `indexes=1`) | row-count hint (`useExplainQuery`), MV choice (`testChartConfigValidity`), benchmark page | **refused** (403, ACCESS_DENIED) | its estimates count every cluster's granules; HyperDX already treats a failed EXPLAIN as "no estimate" / "not valid" (the raw table is used) [D] |
| `mergeTreeTextIndex(…)` (with `system.parts`) | map-key discovery through a text index | **refused by the service** (`table_function`) | the index's tokens are every cluster's, unscoped; the fork's patch 0002 falls through to the rollup / scan path instead of an empty key list |
| `cluster(…)`, `timeSeries…(…)`, other table functions | clustered metadata, metric tables | refused by the service | table functions are outside the allow-list |
| a statement with `SETTINGS` in its text | a source's `querySettings` (`joinQuerySettings`), `getJSONKeys` | refused by the service (`settings_clause`) | limits and settings are the service's |
| URL settings (`max_execution_time`, `max_rows_to_read`, overflow modes, optimizer switches, …) | every query | **dropped**, named in `X-Otel-Dropped-Settings`; only `date_time_output_format` goes on, as the service's `output` | the caller's limits are the service's (R-S9); the overflow modes are pinned there |
| `INSERT`, a statement in both URL and body, `KILL QUERY`, other statements | (HyperDX never writes [M, earlier eval]) | refused | read-only |
| formats `JSON`, `JSONEachRow`, `JSONCompact`, `JSONCompactEachRow[WithNames[AndTypes]]` | all of HyperDX's but two | rendered from the service's FORMAT JSON answer (values byte-for-byte as ClickHouse wrote them, column order and duplicate names kept) | |
| `CSV`, `TabSeparated*`, `Null`, no FORMAT and no `default_format` | alert message samples (`checkAlerts/template.ts`), the connection test, the benchmark page | refused (UNKNOWN_FORMAT, 73) | the service answers JSON only; rendering CSV from JSON values is not exact |
| multipart bodies (`use_multipart_params_auto`), gzip bodies, `/ping` | the clients | handled | |
| no bearer token (a Basic header is not one) | | 401 AUTHENTICATION_FAILED (516) | deny by default |

Every refusal is a ClickHouse-shaped error (`Code: N. DB::Exception: … (NAME)`,
`X-ClickHouse-Exception-Code`) that `@clickhouse/client` parses into a
`ClickHouseError` with that code and type [M, node client in the integration
test]; the service's reason travels in the message and in `X-Otel-Refusal`.
A pinned limit keeps ClickHouse's code (158 TOO_MANY_ROWS, 159, 241, …).

### 8.2 Parameter binding

`{name:Type}` placeholders are found by a scanner that skips strings, quoted
identifiers and comments, **masked** by identifiers, the statement is parsed,
and each mask is **replaced in the tree** by a node built from the decoded
value; the text sent is the tree formatted. A value's text is never pasted
into the statement.

| type | decoded as | becomes | refused |
|---|---|---|---|
| `String` | ClickHouse's escaped format, as the client sends it (`DecodeString`) | a string literal (`\\`, `\'`, control bytes, DEL and invalid UTF-8 as `\xHH`) | `\x` without two hex digits (ClickHouse makes up a byte), a trailing `\`, a raw tab or newline (ClickHouse refuses) |
| `Identifier` | raw: **one** identifier (`a.b` is one name, as ClickHouse reads it [M]) | a backquoted identifier | empty, `` ` ``, `"`, `\`, NUL, CR/LF, invalid UTF-8 |
| `Int8…Int64`, `UInt8…UInt64` | `[+-]digits` | `toInt64(n)` etc. (the type kept) | anything else; **out of range** (ClickHouse wraps `99999999999` to an Int32 [M]; the adapter refuses) |
| `Float32`, `Float64` | decimal | `toFloat64('…')` | `inf`, `nan`, hex floats |
| anything else (`Array(…)`, `Nullable(…)`, `DateTime…`) | | | refused (HyperDX 2.39.1 sends only the six above [D, `chSql`]) |

A mask as a function name, a value type where only a name can stand
(`FROM {t:String}`), or a mask the walk could not replace is refused. The
scanner refuses what ClickHouse's lexer and the parser's read differently:
a quoted identifier with a backslash or a doubled quote (the parser reads
`` `a``b` `` as `a AS b`), `#` comments, `$heredoc$`, nested comments.
**Measured against ClickHouse 26.10:** every printable ASCII byte and every
control byte after a backslash, and 3,000 random values: the bound literal
reads, on the server, exactly as ClickHouse's own substitution
(`TestBindMatchesClickHouse`). This found two rules the first decoder had
wrong (a raw tab/newline is an error; a backslash before a control byte is
dropped) [M].

### 8.3 The label and the window

On every answer (except schema answers, `X-Otel-Source: metadata`):
`X-Otel-Source`, `X-Otel-Completeness` (`complete` | `partial` | `unknown`),
`X-Otel-Complete-Through`, `X-Otel-Incomplete-From`,
`X-Otel-Watermark-Status`, `X-Otel-Watermark-Lag-S`, `X-Otel-Watermark-Note`,
`X-Otel-Max-Lateness-S`, `X-Otel-Settled-Through` (event time:
`complete_through − max_lateness`), `X-Otel-Late-Rows` (a count, or
`no_window` / `not_measured` / `disabled` / `error`),
`X-Otel-Window-From` / `-To`, `X-Otel-Request-Id` (the audit record),
`X-Otel-Dropped-Settings`; exposed for CORS. The fork records them per
statement and shows the worst on the page (fork README, patch 0002).

**Window.** The service restricts every read of a table with a time column
to the window it is given, and labels against it. The adapter derives the
window from the statement's own bounds only when that restriction **changes
no row**: every SELECT that reads a served table directly reads exactly one
(no join), its top-level `WHERE` conjuncts bound the table's time column on
both sides (`>=`/`>`/`<=`/`<` `fromUnixTimestamp64Milli(n)`: `<= t` becomes
`to = t + 1 ns`), and every such SELECT has the same bounds; a table the
adapter has no time column for, an `OR`, one bound, different bounds in a
subquery: no window, and the label covers everything up to now (`partial` or
`unknown`, never `complete`). 350 of the 620 answered captured statements
get a window [M]. The label inherits the service's semantics: a derived
window is complete only once `complete_through ≥ its end + max_lateness`
(CAST row 26, D26), and its late rows are counted. Schema answers carry the
metadata scope's allow-listed columns only (§3): `SELECT * FROM
system.tables` answers 14 columns, not ClickHouse's 42.

### 8.4 Tests

- `internal/hdxadapter` (`go test`): decoding (every escape as measured);
  hostile strings (`'; DROP …`, `\' OR 1=1`, `{p:String}`, `*/`, NUL, invalid
  UTF-8, the mask name …) each land as exactly one literal that reads back as
  the value, in a statement of unchanged shape; hostile identifiers (`a.b`,
  `x; DROP TABLE y`, `otel_logs FINAL`, `(SELECT 1)`) stay one quoted name;
  integers (`1 OR 1=1`, overflow, hex, exponent) and floats; placeholder
  positions; FORMAT stripping (not from strings or comments); DESCRIBE/SHOW
  with hostile names; windows (15 cases); a property test (any bytes, sent as
  the client sends them, bind to one literal with that value); the HTTP
  handler against a fake service (token, multipart, gzip, settings dropped,
  every service refusal mapped to a code the client's own error pattern
  parses, DESCRIBE of an unknown table); the live differential test above
  (skips without ClickHouse); the 799 captured statements prepare or are
  refused only as EXPLAIN.
- `integration/hdxadapter` (`HDXA_IT=1 go test ./integration/hdxadapter`, 35 s
  on the shared box; nightly `hdxadapter-integration`): three databases with
  the capture's schema sides (pre-alignment, option 2, full ClickStack) and the
  same 20,000 logs and spans each (clusters `qa`/`qb`, the capture's values),
  the service wired as `queryd` wires it (real ClickHouse, read-only user,
  in-memory watermark), the adapter in front. **All 799 statements HyperDX
  2.39.1 sent** go through the adapter as `@clickhouse/client` sends them
  (`param_*`, HyperDX's settings, a fleet token), and each answer is compared
  with ClickHouse's own answer to the same statement and parameters [M]:

  | outcome | statements |
  |---|---:|
  | equal (meta, rows, order) | **617** (538 non-empty; all 50 DESCRIBEs) |
  | equal as row sets (order ties differ) | 3 (search pages whose `ORDER BY` leaves ties; since the replay stamps `received_at`, the rows span more partitions) |
  | refused: `EXPLAIN` | 77 |
  | refused by the service: `mergeTreeTextIndex` (table function) | 102 |
  | mismatch | **0** |

  Per statement: [`../hyperdx/results/adapter-replay.jsonl`](../hyperdx/results/adapter-replay.jsonl)
  (`HDXA_IT_OUT`). Schema answers are compared on the columns both have:
  the 77 `SELECT * FROM system.tables` answers withhold 29 of ClickHouse's
  columns (`data_paths`, `metadata_path`, `uuid`, `storage_policy`, byte
  and part counts, dependencies, …) and add the alias `table`, which a
  subquery's `*` includes; every served system table answers `SELECT *`
  with exactly its allow-list, and `data_paths` / `metadata_path` /
  `data_path` by name are errors [M]. The replay's rows carry `received_at`
  0–90 s after `Timestamp`: of the 350 windowed answers, 254 report late
  rows, 96 none; the 48 without a window say `no_window`.

  The same statements with a token for cluster `qa` only: **532 equal** to
  ClickHouse's answer with the service's scope filter applied by hand (130 of
  them narrower than the fleet's answer), 38 refused (`scope_unenforceable`:
  the key-value rollups have no cluster column and are `fleet`), 0 different.
  Labels: 143 `complete` and 207 `partial` with a derived window, 48 `partial`
  without; each checked against the window's end + `max_lateness` and
  `complete_through`.
  Latency per statement through adapter and service p50 15.1 ms / p90 27.3 ms,
  ClickHouse direct 6.8 / 11.4 ms (in process, loaded shared box; the
  late-row count is one more statement per windowed answer; before it,
  9.9 / 16.9 ms) [M]. With
  `HDXA_IT_NODE_MODULES` (a `node_modules` holding `@clickhouse/client`
  1.23.0-head.fae5998.1, HyperDX's pin) the real node client runs against the
  adapter: ping, a typed-parameter query in JSON with the label headers and
  ISO timestamps, a streamed JSONCompactEachRowWithNamesAndTypes, a 20 KB
  parameter that forces multipart, and EXPLAIN / `system.parts` / no-token
  errors parsed as `ClickHouseError` 497 / 497 / 516 [M]. It found that the
  node client **overwrites a per-query `Authorization` header with its Basic
  header**; the token has to go in the query's `auth: {access_token}` (the
  fork does so).

### 8.5 HyperDX features under the service's rules

| feature | under the service | what it needs |
|---|---|---|
| search, charts, dashboards, traces, sessions, raw SQL, alerts' queries | work (the 620) | – |
| the row-count hint in search (`EXPLAIN ESTIMATE`) | not shown | a scoped estimate: the service could run `EXPLAIN ESTIMATE` over the rebuilt statement with the filters, or return `rows_read` of a `LIMIT 0` probe; or drop the hint |
| materialized-view choice by estimate (`testChartConfigValidity`) | every MV reads as "not valid": the raw table is used | as above; today HyperDX 2.39.1 has no MVs on metric sources, and the logs/traces rollups are the kv tables below |
| map-key discovery through a text index (option 2's `*_attr_items`, full ClickStack's `*_attr_key`) | refused; with patch 0002 falls through to the rollup / sampled scan (before 0002: an empty key list, silently) | a scoped key list: the key-value rollup with a cluster column, or keys per resource from the entity catalog |
| key/value rollups (`*_kv_rollup_15m`) | fleet callers only (no cluster column: `fleet`); restricted callers are refused and fall back to the scan | a cluster (and namespace) column in the rollup, then scope `columns` |
| typeahead samples that stop at `max_rows_to_read` (`allowSampledRead`, patch 0001) | fail past the limit (the service pins `read_overflow_mode = throw`) | a labelled sample mode in the service (a `sample: true` request that answers `completeness: sample`), or a smaller window |
| performance settings (lazy materialization, top-k skip indexes, `use_skip_indexes_on_data_read`, …) | dropped | an allow-list of settings that change speed but not rows, next to `OutputSettings` |
| a source's `querySettings` | the statement is refused | drop them for sources served by the service, or the same allow-list |
| JSON-type columns (`getJSONKeys` puts `SETTINGS` in its SQL) | key discovery refused | not used by our schema (Map columns) |
| alert message sample rows (`format: 'CSV'`) | refused | ask for JSONEachRow in the fork, or let the service return CSV |
| the connection form's Test button (`/?query=SELECT 1` without a token) | fails (401) | none (use the service's `/healthz`) |
| the benchmark page (`EXPLAIN indexes=1`, `format: 'NULL'`) | fails | none |
| `cluster(…)` metadata (a cluster name on the connection) | refused | the service reads the local node's system tables |
| the entity rewrite proxy in front (`entities/rwproxy`) | its 82 rewritten statements: 13 EXPLAIN, **23 refused for `dictHas`/`dictGet`, 46 for reading `rw_cat.resource_kv`**: 0 pass [M, offline through `sqlscope`] | the service must allow the catalog's dictionaries by name and `resource_kv` with scope `catalog`; until then run rwproxy only for fleet callers or not at all |
| server-side queries (alerts, the API's own, MCP) | run as the service identity of `HDX_QUERY_SERVICE_TOKEN_FILE`, not as the user who asked | pass the user's token through the API for user-initiated server-side calls (MCP, external API) |

## 9. The lake index (`cmd/lakeindex`, `internal/lakeidx`)

D27; the format is [`../FORMAT.md`](../FORMAT.md) §7. The indexer follows
every lane of the configured clusters' `traces` and `logs`, reads each new
object once and writes create-only segments under the cluster's own
`{root}/{cluster}/_index/v1/`. The planner reads them (§2.2).

```
LAKEIDX_S3_KEY=… LAKEIDX_S3_SECRET=… lakeindex -endpoint http://127.0.0.1:18333 -bucket otel -root edges \
    [-clusters a,b] [-signals traces,logs] [-interval 30s] [-once] [-metrics :9464]
```

One process per cluster (with [`../deploy/iam/indexer.json`](../deploy/iam/indexer.json):
read its cluster, write only its `_index/`) or one for the fleet; any number
may run at once (segments are content-addressed, the progress document is a
CAS hint). Each pass: LIST the cluster's lanes; skip slots below the
progress document's per-epoch `next`; group the rest by the hour of their
`LastModified`; subtract what the hour's segments already cover; build one
L0 segment per ≤ 256 objects (`-max-segment-objects`); write it
`If-None-Match: *`; advance the progress; then merge each hour that ended
more than 10 minutes ago (`-merge-after`) into one L1 when no single segment
covers it. Metrics: `lakeidx_passes_total`, `lakeidx_segments_written_total`,
`lakeidx_objects_indexed_total`, `lakeidx_rows_indexed_total`,
`lakeidx_source_bytes_read_total`, `lakeidx_segment_bytes_total`,
`lakeidx_put_conflicts_total`, `lakeidx_errors_total`,
`lakeidx_lag_seconds{cluster,signal}`, `lakeidx_last_pass_ok`.

**Tokenizer.** Terms are what the lake UI's substring search sees:
JavaScript `toLowerCase`, then ASCII letters, digits and `_` are word
characters and everything else (any non-ASCII character too) separates.
Only U+0130 (İ → `i` + U+0307) and U+212A (Kelvin → `k`) lowercase to
anything ASCII in the browser, so Go folds without Unicode tables;
`../lakeui/test/fold.test.js` scans every code point in the engine to keep
that true and writes `internal/lakeidx/testdata/fold_vectors.json`, which
`TestTokensAgreeWithTheBrowserVectors` reads. No normalization (neither side
normalizes); invalid UTF-8 is U+FFFD, a separator, on both sides. A search
text becomes constraints (whole token, token prefix, token suffix, inside a
token) that every matching body satisfies (FORMAT.md §7.3).

**Tests** (`go test ./internal/lakeidx ./internal/lake`): rapid properties:
resolution ⊇ brute force (no false negatives) over generated segments with
tiny blocks, frequent-term cuts and long tokens, and over real Parquet
objects through the indexer; the constraint rule holds on every matching
body (20,000 cases [M]); a single flipped byte anywhere in a segment never
narrows an answer; a restarted indexer converges under lost requests and
lost answers with no duplicate keys; two concurrent indexers with objects
arriving in between; hour merges; unindexable objects; a corrupt segment is
refused, then rebuilt. Planner: filters narrow, unindexed objects scan, a
corrupt or unreadable segment scans and is reported, refusals, and a
superset property over generated lakes. `LAKEIDX_MEASURE=1 go test -run
Measure ./internal/lakeidx` prints sizes and build costs over the lake UI's
real edge objects.
