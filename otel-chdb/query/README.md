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
  **`complete_through`**, and marks what extends past it (STPA R-S1, R-S2);
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
`QS_CATALOG_DB`, `QS_LAKE_ENABLED`. An audit path and a bucket are
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

```json
{"request_id": "5c0f…",
 "source": "central",
 "complete_through": "2026-09-28T12:59:31.2Z", "complete_through_ns": 1790…,
 "completeness": "partial", "partial": true,
 "incomplete_from": "2026-09-28T12:59:31.2Z", "incomplete_from_ns": 1790…,
 "watermark": {"status": "ok", "key": "edges/_consumer/watermark.json", "age_s": 12.1, "lag_s": 28.8,
               "fetched_at": "…", "holding": [{"lane": "prod-eu-1/pub-0/logs", "wm_ns": 1790…, "lag_s": 28.8}]},
 "catalog": {"status": "ok", "clusters": {"prod-eu-1": {"last_put": "…", "lag_s": 41.0, "status": "ok"}}},
 "query": {"hash": "9e1d…", "tables": ["otel.otel_logs"], "sql": "SELECT ServiceName, count() FROM otel.otel_logs GROUP BY ServiceName",
           "scoped": true, "window": {"from_ns": …, "to_ns": …}, "rows_read": 5120, "elapsed_ms": 18.2},
 "result": { ClickHouse's FORMAT JSON: meta, data, rows, statistics }}
```

- **`completeness`**: `complete` (the window ends at or before
  `complete_through` and the watermark is current), `partial` (it extends
  past it: draw from `incomplete_from` on as incomplete, count over it as
  partial), or `unknown` (the watermark is missing, unreadable or stale:
  **nothing in the result may be read as settled**). A statement without a
  window runs up to now, so it is always `partial` or `unknown`.
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
 "from": "…", "to": "…", "complete_through": "…", "completeness": "partial", "partial": true, "incomplete_from": "…",
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
the skew), then HEADs the rest (up to `max_heads`, 16 at a time) for
`oscope-kind`, `oscope-min-time`, `oscope-max-time`, `oscope-rows` and
`oscope-cluster`, and keeps data objects whose rows overlap the window. An
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
`partial`: the older rows are in central.

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
3. Rows after `complete_through` may still arrive: draw that region as
   incomplete, and count over it as partial.
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
| R-S1 | every result carries its source and complete-through | **met for this service's answers**: `source`, `complete_through`, the watermark's freshness; on every `/v1/query` and `/v1/plan` answer. The UIs must still show it (the banner is not built) |
| R-S2 | windows not yet complete drawn as incomplete, counts marked partial | **met at the API**: `partial`, `incomplete_from`, `completeness`; a missing, unreadable or stale watermark is `unknown`, never `complete` (unit tests for each). Per-bucket marking is the UI's |
| R-S3 | alerts evaluate only up to complete-through; a failed evaluation pages | **not this service's**: the alert evaluator can use `completeness`/`incomplete_from` to pick its windows (X5) |
| R-S5 | views show catalog lag and rows without an entity match | **partly**: catalog lag per cluster from the aggregator's `ingest_log`; rows without an entity match are not counted yet |
| R-S8 | reads role-scoped by cluster and namespace; every query audit-logged | **met for this service**: deny by default, per-row scope on central, per-object (cluster) scope on the lake, namespace-restricted plans refused; every decision audited before it acts. HyperDX still reaches ClickHouse directly until the fork routes through this service |
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
  `unknown`; a good copy ages out when the store stops answering.
- **`internal/server`** (fakes for ClickHouse and S3, the local issuer): 401
  and 403 are audited and run nothing; the filter, overflow modes and limits
  reach ClickHouse; the label; refusals; an audit failure refuses both
  endpoints; a missing watermark; the concurrency limit; a limit error is
  422; plans: scope, heartbeats, old objects and a cross-cluster object
  excluded, expiry, rules, audit; cross-cluster and namespace plans denied;
  fleet plans and GC truncation; CORS; metrics.
- **`integration`** [M] (17.5 s): the Go edge (`otelcol-s3pq`,
  [`../conformance/go-edge.yaml`](../conformance/go-edge.yaml)) publishes
  logs and spans for clusters `qa` and `qb` (two namespaces each) to
  SeaweedFS; the Rust consumer ingests them into ClickHouse and publishes
  `watermark.json`; a read-only user and a stand-in catalog (the edges'
  announcements as `resources`) are created. Then: fleet 12 logs, `qa` 5,
  `qa` asking for `qb` 0, `qa` with an alias shadowing the scope column 5,
  through a CTE and a self-join 5, `qa/shop` 3; traces scoped by the catalog
  3 / 2 / 8; refusals; the read-only user cannot write; `max_result_rows =
  2` fails with 422; the label reads the real document (`ok`, lag ≈ 15 s) and
  a window closed before it is `complete`; the plan returns only `qa/`
  objects, each GET 200 with the planned size and a Parquet header, HEAD
  with the same URL 403; `qa` asking for `qb` is 403, `qa/shop` is 403; the
  fleet plans both clusters; 32 audit lines. Everything is named `qs-…` /
  `qs_…` and removed. Nightly in CI (`query-integration`).

## 7. What's next

1. **The HyperDX fork through the service.** Point the fork's ClickHouse
   connection at a small adapter that forwards its HTTP statements to
   `/v1/query` with the user's token (the fork's API holds the session), and
   render the label as the `complete_through` banner. The fork's
   `{HYPERDX_PARAM_…}` placeholders need binding before parsing (rwproxy's
   masking shows how), and the entity rewrite (rwproxy) belongs before the
   scope step in the same pipeline.
2. **The lake UI on `/v1/plan`**: first slice built in
   [`../lakeui/`](../lakeui/README.md) (hyparquet range reads with the plan's
   sizes, re-plan before `replan_after`, a 403 as re-plan, the incomplete
   region drawn after `incomplete_from`); next: snapshots, the maplet and term
   index in plans, namespace-scoped viewers through `/v1/query`.
3. **The alert evaluator** (X5, R-S3): evaluate only windows that end at or
   before `complete_through` with `completeness: complete`, page on
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
