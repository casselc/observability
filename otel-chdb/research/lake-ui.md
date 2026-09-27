# research: a lake-first browser UI — engines in the page, a share endpoint, and what the HyperDX fork keeps

A feasibility spike and design note. The owner decided on 2026-09-27 (see
[../DECISIONS.md](../DECISIONS.md), "Owner decisions, 2026-09-27"):

- a 15–40 s near-tail and alerts about 5 minutes behind are acceptable;
- the UI is **(1) a HyperDX fork first, and (2) a dedicated lake-first UI as
  well**, possibly with DuckDB or chDB in WebAssembly querying in the browser.

This note is about (2), and about where it meets (1). It builds on:

- [central-optional.md](central-optional.md): the sealer and its create-only
  Iceberg snapshots over edge objects, as-of reads, `complete_through`
  (§5.4), the term LSM (§6), the stateless reader tier;
- [../lake/DESIGN.md](../lake/DESIGN.md): compaction (P1), the trace-id
  maplet (P2), rollups;
- [../entities/README.md](../entities/README.md) and
  [../entities/rwproxy/README.md](../entities/rwproxy/README.md): the SCD2
  entity catalog, `resource_id`, and the SQL-rewriting proxy in front of
  ClickHouse;
- [../model/completeness.qnt](../model/completeness.qnt): `complete_through`
  needs `x-amz-meta-oscope-low`, heartbeat and birth slots; R-S1 and R-S3.

A prototype was built and run: [`../lake-ui/`](../lake-ui/) (§7).

Labels, as elsewhere:

- **[D]**: from a cited source (read 2026-09-27; package registries queried
  the same day);
- **[E]**: my estimate;
- **[M]**: measured here, on the shared 4-vCPU box (two other agents
  building), SeaweedFS 4.47 on localhost, headless Chromium from
  Playwright 1.56.1. Local latencies say nothing about a WAN; byte counts
  do transfer.

## 1. Summary and recommendation

**What the spike found:**

1. **A browser can query our edge Parquet directly, and a share endpoint
   can keep every credential out of it.** The page asks a small server for
   a plan; the server resolves the viewer's scope (an OIDC token's claims)
   and the query's scope (entity catalog, snapshot manifest, trace maplet)
   to an object list, and returns one presigned GET URL per object. The
   page then reads S3 directly. It ran end to end in the prototype
   [M §7]: trace by id, service search, pod logs, a log scan and a metric
   chart, each in 0.1–0.9 s over 0.25–23 MB on localhost. The endpoint
   refused a missing token (401) and a service outside the token's
   clusters (403), and cut a fleet-wide query down to the token's cluster.
2. **Presigned URLs break the engines' HEAD probe.** A SigV4 query-string
   signature binds the method, so a URL presigned for GET answers HEAD
   with 403 (SeaweedFS [M]; AWS behaves the same [D]). chdb-wasm's `url()`
   then fails ("Cannot find out file size") [M]. A 40-line worker shim that
   turns that HEAD into `GET Range: bytes=0-0` fixes it
   ([`head-shim.js`](../lake-ui/head-shim.js)) [M]. The share endpoint
   already knows every object's size, which is all a range reader needs.
   Delta Sharing returns `size` per file for the same reason [D].
3. **DuckDB-WASM reads every HTTP object whole.** In 1.32.0 (DuckDB 1.4.3)
   and 1.33.1-dev64 (DuckDB 1.5.5), a one-column count over a 17.8 MB,
   8-row-group file fetched all 17.8 MB in one GET, even from a
   Range-capable plain server [M]. Its own docs say so [D]. Forcing range
   mode (`reliableHeadRequests`, no full reads) failed to open files [M].
   Its HTTP reads are synchronous XHR in one worker, so they are
   sequential. **For raw edge objects (about 0.5 MB) this doesn't matter;
   for compacted L2 files it decides the design.**
4. **Range reads work from JS.** hyparquet 1.31.2 (pure JS, 95 KB gzipped
   with the zstd codecs), given the size from the plan, read the same
   17.8 MB file's one column with **15 GETs and 0.53 MB, in 399 ms**
   [M]: 34× fewer bytes than DuckDB-WASM. chdb-wasm with the shim moved
   11–16 MB (1 MB read-ahead) [M].
5. **chDB in the browser exists now.** chdb-wasm 0.4.0 (2026-08-10,
   ClickHouse 26.5.1.1) ran in headless Chromium: init 1.5 s from
   localhost, `url()` works, Parquet via `putFile` or `url()` [M]. But it
   is a 158 MB wasm (29 MB gzipped), needs Memory64, and jsDelivr refuses
   to serve it (150 MB package limit) [M]. It is young (4 releases in 7
   weeks) [D].
6. **The entity catalog is what makes client-side reads affordable.** A
   `k8s.*` or service filter becomes a cluster, and a pod becomes a
   cluster and a lifetime, before any data is read: 4× fewer objects in
   the prototype (48 → 12) [M], 20× at the mid scenario [E]. Without it,
   and without the maplet and term index, fleet-wide shapes are scans that
   no browser should run (§4).

**Recommendation.**

1. **Build the lake-first UI as option (b), hybrid**: a **share/plan
   endpoint** (stateless, next to the query service) decides and signs; the
   **browser reads the planned objects directly** for the interactive
   shapes (trace by id, a pod's or service's recent rows, rollup-backed
   charts and dashboards, as-of views); **big scans go to the stateless
   ClickHouse reader tier** and come back as results. Pure client-side
   (a) is a mode of (b) for small plans; server-only (c) is its fallback.
2. **Engine: a JS range reader for planning-sized reads, DuckDB-WASM for
   SQL over the fetched bytes.** Use hyparquet (or parquet-wasm) to fetch
   footers and only the needed column chunks, with sizes from the plan;
   use DuckDB-WASM (1.32.0 stable, eh bundle, self-hosted with its
   extensions) as the in-page SQL engine over those bytes and over
   rollups. Revisit DuckDB-WASM's own HTTP reader when it range-reads
   presigned objects. Keep chdb-wasm as the candidate for **ClickHouse SQL
   parity with the HyperDX fork**, and re-test it in six months.
3. **The share endpoint is the policy decision point.** It maps OIDC
   claims (cluster, namespace, role) through the entity catalog to objects,
   signs only those, and audits every plan. Expose it as an **Iceberg REST
   catalog with a presigned-URL plan call** for our UI, and optionally as a
   **Delta Sharing endpoint** for external tools (§5). Native storage ABAC
   (STS session tags) is defence in depth on AWS only; SeaweedFS has partial
   STS and Nutanix Objects none (§6).
4. **Lay the store out so that access is enforceable by prefix**: tenant
   boundary first (§6.3). A presigned URL grants a whole object, so
   namespace isolation needs objects partitioned by namespace (compactor),
   or a server-side reader that filters.
5. **Share with the HyperDX fork**: the query service, the entity catalog
   API, the share endpoint and the `complete_through` banner (§3).

**The options:**

| # | Option | Verdict |
|---|---|---|
| a | **Pure client-side**: browser reads the snapshot manifest and index segments, prunes, range-GETs row groups | **Only for small plans** (≤ about 50 objects or 20 MB). It needs the manifest, maplet and term segments readable by the browser, which widens what a viewer can list; every index lookup is a WAN round trip, and the browser holds 6 HTTP/1.1 connections per host [E]. |
| b | **Hybrid**: plan on a server (auth, catalog, indexes, signing), read in the browser; big scans on the reader tier | **Recommended.** Interactive shapes in 0.3–2 s [E] with 1–20 MB per query; credentials stay on the server; the same plan call serves as-of, degraded and air-gapped modes. |
| c | **Server-only**: the reader tier runs every query, the browser renders | **Fallback and the HyperDX fork's mode.** Least exposure, least client work; every query costs reader CPU, and a UI that only renders can't work offline or on a detached site. |

## 2. Engines in the browser

### 2.1 Verdicts

| Engine | Version (date) [D: npm/PyPI] | Reads our Parquet over HTTP | Range reads of presigned objects | Iceberg in the browser | Size (gzip) | Verdict |
|---|---|---|---|---|---|---|
| **DuckDB-WASM** | 1.32.0 stable (2025-12-16; DuckDB 1.4.3 [M]); 1.33.1-dev64.0 (2026-07-28; DuckDB 1.5.5 [M]) | yes: traces, logs, layout-B points, `Map` columns [M] | **no**: whole-object GETs in both builds, even where HEAD works [M]; "reading through the built-in httpfs extension may currently download the whole file" [D] | `iceberg` extension loads in wasm_eh for 1.4.3 and 1.5.5 [M]; documented for REST catalogs (S3 Tables) [D: blog 2025-12-16]; `iceberg_scan` on a metadata file not tried | engine 34 MB (7.7 MB); `parquet` is an **extension** (0.7 MB), `icu` 2.0 MB, `iceberg` 0.4 MB [M] | **Use as the in-page SQL engine** over fetched bytes and small objects. Self-host the bundle and extensions. The threaded (COI) bundle of 1.32.0 failed to link extensions ("mismatch in shared state of memory") [M]: pin the eh bundle. |
| **chdb-wasm** | 0.4.0 (2026-08-10; ClickHouse 26.5.1.1 [M]); first release 2026-06-24 [D] | yes: `putFile` + `file()`, or `url()` [M] | **with the HEAD shim**: 19–33 GETs, 11–16 MB of 17.8 MB [M]; without it, `url()` fails on a presigned URL [M] | ClickHouse `icebergS3`/`iceberg` functions not tried in wasm | 158 MB (29 MB) per bundle, st and mt [M]; jsDelivr refuses the package (> 150 MB) [M] | **Candidate for SQL parity with the HyperDX fork** (same dialect, same functions). Too heavy and too young to be the default now. Needs Memory64: Chrome 133+, Firefox 134+ (later versions without a flag), Safari behind [D: caniuse, SpiderMonkey blog]. |
| **hyparquet** (+ hyparquet-compressors) | 1.31.2 (2026-09-27) | yes, with zstd codecs [M] | **yes**: footer + column chunks; 15 GETs, 0.53 MB of 17.8 MB [M]; no HEAD needed when the size is known | no | 95 KB [M] | **Use as the range reader** that fetches only what a query needs, feeding DuckDB-WASM or the UI directly. No SQL: filters are ours. |
| parquet-wasm | 0.9.0 (2026-09-25) | yes [D] | yes, async reader with byte ranges [D] | no | about 1–2 MB [E; npm 16 MB unpacked across several builds] | Alternative to hyparquet if Arrow output matters. Not run. |
| Apache Arrow JS | 21.2.0 (2026-07-21) | IPC only, not Parquet | – | no | – | Transport between engine and charts only. |
| DataFusion-WASM | `datafusion-wasm` 0.3.1 (2024-12-22) | not tried | not tried | no | about 10 MB [E; npm 42 MB unpacked] | **No**: stale bindings. |
| Polars | no official wasm build; `@pola-rs/browser` is an alpha [D] | – | – | – | – | **No.** |

Memory: wasm32 caps a tab's engine at 4 GB, and browsers may cap it lower
[D: DuckDB-WASM docs]. Memory64 raises the ceiling to 16 GB in Chromium
[D: SciChart]; only chdb-wasm is built for it. The UI should never need
that much: a plan that implies more than about 200 MB goes to the reader
tier (§4).

### 2.2 What each needs from the store and the page

- **CORS on the bucket:** `GET`, `HEAD`; allowed header `Range`; exposed
  headers `Content-Range`, `Content-Length`, `ETag` (and `Accept-Ranges`).
  SeaweedFS 4.47 honoured `PutBucketCors` and answered preflights with
  exactly that [M]. Nutanix Objects documents per-bucket CORS
  (Objects 4.3–5.3) [D]. AWS S3 supports it [E].
- **COOP/COEP** (`same-origin` / `require-corp`) only for threaded bundles
  (SharedArrayBuffer) [D]. Under COEP every cross-origin fetch needs CORS,
  which the bucket already gives [M]. The prototype used the single-threaded
  eh bundle and worked with or without isolation.
- **Self-hosting:** the engine, workers and every extension it autoloads
  (`parquet`, `icu`, `iceberg`, `avro`, `json`) mirrored under one origin
  and `SET custom_extension_repository` [D][M]. That is the air-gapped
  mode anyway. In this container headless Chromium couldn't use jsDelivr
  (the egress proxy's CA isn't trusted by Chromium here), while curl
  reached it (200) [M], so the whole run used self-hosted copies from npm.
- **The HEAD problem** (finding 2): any engine that HEADs before reading
  needs the shim, a separately presigned HEAD URL it can't use, or a
  same-origin gateway that re-signs. Ours: the plan carries `size`; the
  range reader never HEADs; the shim covers engines that do.

### 2.3 Can they read Iceberg as of a snapshot, directly?

- The sealer's table is Iceberg with create-only `v{N}.metadata.json`
  (central-optional §5.1). Reading it in the browser means: metadata JSON
  (small), manifest list and manifests (Avro), then data files. DuckDB-WASM
  has an `iceberg` extension that loads in wasm [M] and is documented for
  REST catalogs [D]; hyparquet and chDB would need our own manifest reader.
- **Don't make the browser plan.** Manifests list every file in the
  snapshot, so handing them to a viewer discloses the whole table's layout,
  and a 30 s-cadence table has 120–360 live tail manifests per hour
  (central-optional §5.3). The share endpoint reads them (cached, they are
  immutable) and returns only the viewer's files for the requested
  snapshot, with the snapshot id and its `complete_through`.

## 3. The lake-first UI and the HyperDX fork

**What each owns:**

| | HyperDX fork (first) | Lake-first UI (second) |
|---|---|---|
| Data it reads | central ClickHouse, or the stateless reader tier over `icebergS3` (central-optional §4), through the rewrite proxy | the lake: planned edge objects, compacted files, rollups, as of a snapshot |
| Latency class | < 1 s on central; 1–5 s on readers | 0.3–2 s for planned shapes [E]; hands big scans to the readers |
| Search, full text, trace waterfall, alerts UI | **yes**: HyperDX's strengths; the fork adds the entity rewrite (rwproxy) and the term-index rewrite | trace by id, needle search through the term index, recent rows for a service or pod |
| **As of a snapshot** | only through `iceberg_snapshot_id` on the readers (central-optional §5.2) | **native**: every page pins one snapshot id; "show me what was known at 14:02" |
| **Degraded mode** (central down) | fails over to the reader tier at warm latency | unaffected: needs S3 and the share endpoint only |
| **Older ranges** (beyond the hot 1–7 days) | slow scans on readers | planned reads of compacted files and rollups |
| **Offline / air-gapped sites** | needs its API, MongoDB and a ClickHouse endpoint on site | a static bundle + the share endpoint + the site's bucket; can run against a copied snapshot with no server at all (a "frozen" export, central-optional §1 item 2) |
| Metrics dashboards | yes (layout-B views, rollups) | tiles over published rollups |

**Shared pieces (build once):**

1. **The query service**: prefers central, falls back to the lake, and
   labels every result with its source and that source's
   `complete_through` (R-S1 in `model/completeness.qnt`). The fork calls
   it through the proxy; the lake UI calls its plan API.
2. **The entity catalog API**: `resolve(scope, t0, t1) → {clusters,
   namespaces, resource_ids, lifetimes}`. The rwproxy already turns
   `ResourceAttributes['k'] = 'v'` into catalog lookups for ClickHouse; the
   lake UI uses the same resolution to choose objects.
3. **The share endpoint** (§5): OIDC in, a signed object list out, one
   audit line per decision.
4. **The `complete_through` banner.** Every view shows, per source, the
   watermark it read and "data after W may be incomplete". R-S3 (the alert
   evaluator only evaluates windows that end at or before W, pages on a
   stalled watermark, never reads "no data" as OK) is the evaluator's; the
   UI's part is to never draw the region after W as settled. The model
   requires `x-amz-meta-oscope-low`, heartbeat slots and birth slots before
   W is sound (completeness.qnt header); until the edge writes them, the
   banner must say "approximate".
5. **The STPA requirements.** Only R-S1, R-S3 and R-S6 are written in this
   repository (the Quint headers); the STPA document itself isn't
   committed. As the owner names them here: R-S1..R-S5 cover result
   labelling, watermarks and alerting, R-S7 and SEC-1..3 the write-side
   isolation of §6.3, and R-S8 auditing every data access. **Commit the
   STPA table** so the UI's acceptance tests can cite requirement text.

**Order of work.** The fork first (it has users on day one); the lake UI's
first release is the three shapes the fork does worst on the lake: as-of,
trace by id across old ranges, and a pod's logs over days.

## 4. Query shapes at the mid scenario

Mid scenario (lake/DESIGN §1): 90 edge objects/s (60 traces, 20 logs, 10
metrics), about 0.5 MB per trace or log object, 20 clusters × 3
publishers, 6.1 TB/day. All [E] unless marked; (a) pure client, (b) hybrid,
(c) server-only. Browser ↔ store 30–100 ms RTT, 50–100 MB/s.

| Shape | Pruning before any data read | (a) client: GETs / bytes / latency | (b) hybrid: GETs / bytes / latency | (c) server | Prototype [M] (localhost, DuckDB-WASM 1.32.0) |
|---|---|---|---|---|---|
| **Search 1 h fleet-wide** (a needle, newest 50 rows + a count histogram) | term LSM: 20 clusters × L0/L1 segments of the hour → 1–20 files; histogram from the log-count rollup | about 450 GETs (400 segment + 2–40 row ranges) / 10–20 MB / **3–8 s** (6 connections) | plan on server (segments cached there) → 2–40 range GETs / 2–10 MB / **1–2 s** | 1–3 s (central-optional §6.4) / 50 KB to the browser | log scan over 16 objects (no index): 5.0 MB, 431 ms |
| same, **without** a term index | none | 216k objects/h, about 108 GB: **not in a browser** | → reader tier | minutes (fan-out) | – |
| **Trace by id** | P2 maplet (+ tail L0 maplets) → 5–20 objects (routing puts a trace's services in several lanes) | tail maplets = 240 GETs; rows 5–20 objects × 0.5 MB whole = 2.5–10 MB / **1–3 s** | maplet on server → 5–20 URLs / 2.5–10 MB raw, 1–5 MB from L2 with ranges / **0.3–1 s** | 0.3–1 s | maplet → 1 object, 0.48 MB, **105 ms**; no maplet → 48 objects, 22.9 MB, **663 ms** |
| **A pod's logs over 7 days** (newest 100 + volume) | entity catalog: pod → cluster + lifetime (20×); L2 sorted by (ServiceName, resource_id, Timestamp) with per-file min/max `resource_id` in the manifest | tail hour: 3.6k objects of the cluster, 1.8 GB: **no**; L2 only: 2–5 MB first page | server prunes by manifest + page index → first page 1–2 MB, **1–2 s**; scrolling 7 days about 40 MB projected, streamed; histogram from a (resource_id, 5 min) count rollup, about 50 KB | 1–4 s per 24 h scanned | 4 objects (cluster), 1.25 MB, **129 ms** |
| **Metric chart, 24 h** | layout B: MetricName + series set from the series table; D15 5-minute rollup | rollup: 1–2 GETs / 0.1–0.3 MB / **0.2–0.5 s**; raw points sorted by (MetricName, series): about 13 MB | same, planned | < 1 s (aggregates central) | 2 objects, 0.25 MB, **26 ms** warm, 649 ms cold (loads the 6.5 MB `icu` extension) |
| **Dashboard, 8 tiles, 24 h** | rollups only | 16–40 GETs / 1–3 MB / **0.5–2 s**, + 9–11 MB engine and extensions once per browser | same | < 1 s | – |

**Cost per query [E, list prices]:**

- **GETs**: at most about 500 per shape: $0.0002 at $0.0004 per 1,000
  (the price behind central-optional's $94/month for 7.8 M GETs/day).
  10k cold pages a day at about 100 GETs is about $12/month.
- **Egress is the real cost of (a) and (b) on AWS**: bytes go to the
  viewer, not to an in-region reader. At about $0.09/GB, 10 MB per query
  is about $0.001; 10k queries a day about $270/month. On Nutanix and
  inside a VPC or over Direct Connect there is none. (c) sends only
  results.
- **Engine download** is 9–11 MB compressed per browser, once, cached.

**Latency caveats [E]:**

- DuckDB-WASM's reads are synchronous XHR: sequential. 48 objects took
  663 ms on localhost [M]; at 80 ms RTT that is about 4–6 s. The range
  reader (hyparquet) is async: 6 parallel requests per host on HTTP/1.1,
  many more on HTTP/2 (a CDN or gateway in front of the store; direct S3
  endpoints are commonly HTTP/1.1 [E: re:Post]).
- Every browser-side index lookup is a WAN round trip. That is the main
  argument for (b): the plan is one call.

## 5. Sharing protocols: OIDC → snapshot metadata + short-lived storage access

**The pattern the owner asked about** (Delta Sharing's "open sharing"): the
viewer authenticates with OIDC to a sharing or catalog server, which returns
the table's metadata as of a version or time, and short-lived access to the
files: presigned URLs per file, vended credentials, or remote signing.

### 5.1 The protocols and servers (checked 2026-09-27)

| | **Delta Sharing** | **Iceberg REST catalog** |
|---|---|---|
| Version | protocol in `delta-io/delta-sharing`; Python client 1.4.2 (2026-08-10) [D: PyPI] | spec in `apache/iceberg`; servers below |
| Auth | bearer token, `basic`, `oauth_client_credentials` in the profile file [D: PROTOCOL.md]. **OIDC token federation** (U2M and M2M, the recipient's IdP issues the JWT, a federation policy checks issuer, audiences and subject claim) is a Databricks feature, documented 2026-09-11 [D]; nothing documents it for the open-source reference server [E] | OAuth2 (client credentials, token exchange) per the spec [E, spec not re-read]; servers add OIDC (Polaris, Lakekeeper) |
| As of | `version` or `timestamp` in the query-table request; `startingVersion`/`endingVersion` for changes [D] | snapshot id or `snapshot-log` from `loadTable`; the engine picks the snapshot |
| Server-side pruning | `predicateHints` (SQL subset), `jsonPredicateHints` (partition columns), `limitHint` [D] | none in `loadTable`; the **scan-planning** endpoints (plan-table-scan) let a server plan [E, spec not re-read] |
| What the client gets | per file: **`url` (presigned https), `size`, `stats`, `partitionValues`, `expirationTimestamp`**; `refreshToken` to refresh URLs mid-snapshot [D] | `storage-credentials` / `config` with **vended** temporary credentials (`X-Iceberg-Access-Delegation: vended-credentials`), or **remote signing** (a signer endpoint signs each request) [D]; no presigned-URL mode |
| Browser fit | **direct**: a file list with URLs and sizes is exactly what a range reader needs (no HEAD, no keys) | vended credentials put an S3 key pair in the page (short-lived, prefix-scoped, but a key); remote signing keeps keys out but needs an engine that implements it |

**Servers:**

- **Apache Polaris 1.5.0** (Maven, 2026-05-14) [D]: credential vending
  via STS; `stsUnavailable: true` for S3-compatible stores without STS,
  which then vends no credentials; a bug (Feb 2026, 1.3.0) vended anyway
  on NetApp S3 [D: issue #3742].
- **Lakekeeper 0.13.4** (2026-09-10, Rust, Apache-2.0) [D]: vended
  credentials and remote signing for S3, GCS, ADLS and on-prem
  S3-compatible stores; configurable STS endpoint per storage profile;
  OpenFGA authorization; emits `signer.uri` for Iceberg 1.11+ clients [D].
- **Unity Catalog OSS** (client 0.6.0, 2026-08-20) [D: PyPI]: v0.4.0
  added storage credentials and location-scoped vending (AWS STS, Azure
  SAS, GCS) [D]; its Iceberg REST clients can't refresh vended credentials,
  so access dies at the STS session's expiry (issue #1885) [D].
- **Every one of them assumes STS for AWS-style vending.** Our stores
  are SeaweedFS (partial STS) and Nutanix Objects (none; §6.2).

### 5.2 Who can consume what

| Client | Delta Sharing | Iceberg REST + vended creds | Iceberg REST + remote signing | Our plan call (URLs + sizes) |
|---|---|---|---|---|
| DuckDB-WASM | no client [E]; the file list can be fed to `read_parquet` through a JS shim (whole-object reads) | `iceberg` extension against REST catalogs in wasm [D: 2025-12-16 blog], keys in the page | not claimed [E] | **yes** (prototype) |
| chdb-wasm | via shim + `url()` (HEAD shim) | not tried | not tried | **yes** (prototype, with the shim) |
| hyparquet / parquet-wasm | a 30-line shim: request, then range-read with `size` | no (no S3 signing) | no | **yes** (prototype) |
| Spark, pandas, Power BI, Tableau, DuckDB native | **yes** (delta-sharing connectors; Power BI and Tableau named by Databricks [D]) | yes (Spark, Trino, DuckDB) [E] | Spark/Iceberg Java [E] | no |
| ClickHouse | no [E] | `icebergS3` with a REST `DataLakeCatalog` (Glue, Unity, Hive, REST) [D: central-optional §10] | no [E] | via `s3('{…}')` with the list |

### 5.3 Our fit

1. **Primary: our own share endpoint, shaped as an Iceberg REST catalog
   plus a "plan with presigned URLs" call.** The sealer already writes
   Iceberg; the endpoint serves `loadTable` for the reader tier and our
   tools, and a plan call (Delta Sharing's query-table shape: snapshot or
   timestamp, predicate hints, `limitHint`) that returns, for the viewer's
   scope only: `{snapshot_id, complete_through, files: [{url, size,
   row_groups?, stats}], expires}`. **Presigned URLs are portable** to
   SeaweedFS and Nutanix, which may lack STS; they need only the store's
   own SigV4 and CORS [M].
2. **Optional: a Delta Sharing endpoint for external tools** (Power BI,
   pandas, Spark) over the same plan logic. Delta Sharing wants a Delta
   log; our table is Iceberg, so this is a translation layer (version =
   snapshot sequence, files = the snapshot's data files). Worth it only if
   external consumers are asked for.
3. **The entity catalog resolves the viewer's claims.** OIDC claims
   (clusters, namespaces, roles) → allowed `(cluster, namespace)` pairs →
   allowed prefixes and, for mixed objects, a verdict that the plan must
   go to the filtering reader (§6.4).
4. **Mechanics measured or to decide:**
   - **URL lifetime vs long queries**: the prototype signs for 300 s. A
     plan lives as long as a page needs it; re-plan (or a `refreshToken`,
     as Delta Sharing) when a URL is within a minute of expiry. Keep it
     short: there is no revocation before expiry (§6.5).
   - **Batched signing**: signing is a local HMAC; a plan of 1–48 objects
     took 5–65 ms end to end including catalog work and JSON [M]. A 5k-URL
     plan is well under a second [E]; cap plans at a few thousand objects
     and send bigger ones to the reader tier.
   - **CORS** and the **HEAD shim**: §2.2.
   - **Audit**: one line per plan decision (subject, query, allow/deny,
     objects, bytes); the prototype wrote 136 lines, 13 of them denials
     [M]. The store's own access log records each GET under the signer's
     key, not the viewer; the plan's audit line is what links the two
     (R-S8).

## 6. Security: ABAC, session credentials and the prefix layout

### 6.1 What a presigned URL can and can't enforce

- **It grants one object, one method, until it expires.** Whatever is in
  the object is readable: **no row-level isolation within an object.**
- Raw edge objects are per publisher, and publishers are per cluster
  (DECISIONS pipeline picture), so **a cluster boundary is enforceable by
  object; a namespace boundary is not**, because one raw object holds many
  namespaces.
- **Redaction happens at the edge or in the compactor, never in the
  browser**: the browser has the bytes.

### 6.2 Native storage ABAC: what exists (checked 2026-09-27)

| Mechanism | AWS [D] | SeaweedFS 4.x [D] | Nutanix Objects 5.x [D] |
|---|---|---|---|
| OIDC → temporary credentials (`AssumeRoleWithWebIdentity`) | yes | **yes** (`weed/iam/sts`, claim-based role mapping) | **no**: "IAM/STS functionality is not supported by Nutanix Objects 4.3 and 5.0" (Veeam) |
| Session tags from OIDC claims (`https://aws.amazon.com/tags` → `aws:PrincipalTag/*`) | yes; ≤ 50 tags, keys ≤ 128 and values ≤ 256 characters; the role trust policy needs `sts:TagSession`; single-valued | **no** `sts:TagSession` | no |
| Policy variables in resource ARNs, e.g. `arn:aws:s3:::lake/${aws:PrincipalTag/cluster}/*`, and `s3:prefix` conditions on `ListBucket` | yes | **partly**: `${jwt:<claim>}` variables for STS sessions, merged 2026-01-22 (PR #8082) | no |
| Session policies when vending | yes; inline ≤ 2,048 characters in plaintext, and they count against the session token's size | [E] not documented | no |
| **S3 Access Grants** (directory identities → prefixes, `GetDataAccess` vends STS credentials, 15 min–12 h, ≤ 100k grants per region) | yes | no | no |
| `s3:ExistingObjectTag` conditions | yes, for object reads; **not for List**, not for PUT/DELETE; tags cost $0.0065 per 10,000 per month | [E] no | no |

So native ABAC is **AWS-only defence in depth**. On-prem it isn't there,
and even on AWS it stops at prefixes.

### 6.3 A prefix layout that makes access enforceable

**Tenant boundary first, in every key, for reads and writes alike.** The
write side is proposed in [../DECISIONS.md](../DECISIONS.md) under D18,
"Write-side ABAC" (2026-09-27, for STPA R-S7 and SEC-1..3); this note uses
the same layout and adds the lake's read side:

```
{root}/{cluster}/{producer}/{signal}/{epoch}/{seq:020d}.parquet    raw edge slots (D3); today {root}/{producer}/…
{entities}/{cluster}/…                                              entity lanes, one writer per cluster controller
{lake}/{cluster}/{namespace}/{signal}/date={d}/hour={h}/…           compacted L2/L3 files (compactor), per namespace
{lake}/{cluster}/{signal}/l1/…                                      L1 (5 min), per cluster (§6.4)
{lake}/{cluster}/_index/{signal}/{level}/…                          term segments and maplets, per cluster
{lake}/_meta/{table}/metadata/v{N:020d}.metadata.json               snapshots and manifests (sealer)
{ctl}/…                                                              leases, checkpoints, tombstones, GC marks
```

**Write side** (as D18 proposes):

- **edge publisher**: `PutObject` under `{root}/${cluster}/*` only,
  create-only by `If-None-Match: *`; no delete; no access to `{ctl}`.
- **entity controller**: `PutObject` under `{entities}/${cluster}/*` only.
- **consumer, sealer, compactor**: read lanes; write and delete `{ctl}` and
  `{lake}`; delete lanes only as GC (D12).

**Read side** (this note):

- **viewer**: no native credentials at all on-prem; presigned GETs from
  the share endpoint, which signs only keys under the viewer's
  `{cluster}` (and `{namespace}` for compacted data).
- **on AWS, defence in depth**: the share endpoint signs with a role whose
  policy is itself tag-scoped (`{lake}/${aws:PrincipalTag/cluster}/*`),
  assumed per request batch with the viewer's claims as session tags, so a
  bug in the endpoint's catalog logic can't sign outside the viewer's
  clusters. That is two STS calls' latency per plan [E], cached per viewer
  for the session.

**What §6.2 adds to D18's portability line:** SeaweedFS has STS with OIDC
and `${jwt:<claim>}` policy variables (PR #8082), but no session tags, so
`${aws:PrincipalTag/cluster}` has to become `${jwt:cluster}` there, or one
identity per cluster; Nutanix Objects has no STS, so one access key per
cluster with a prefix-scoped bucket policy (to verify on a cluster) is the
only native option.

### 6.4 Namespace-level isolation

Two ways, pick per deployment:

1. **Partition by namespace in the compactor** (`{lake}/{cluster}/{namespace}/…`):
   presigned URLs then enforce it for everything older than the raw tail
   (about 1 h). The tail stays cluster-scoped: namespace-restricted
   viewers see compacted data only, or read the tail through (2).
2. **A server-side filtering reader**: the reader tier runs the query with
   `resource_id IN (allowed)` and returns rows. Exact at any age; costs
   reader CPU per query.

The cost of (1) [E]: more, smaller files per hour. At 20 clusters × about
30 namespaces with data, 600 L2 files per signal-hour instead of 20; fine
for L2 (hour) and L3 (day), too many for L1 (5 min), so L1 stays per
cluster.

### 6.5 Limits to accept

- **No revocation before expiry** for presigned URLs or STS sessions.
  Keep URL TTLs to minutes (300 s in the prototype) and OIDC sessions
  short; revoke at the share endpoint (the next plan is refused).
- **Replay of a leaked URL** works until expiry, from anywhere, unless the
  store supports source-IP or VPC conditions on the signing identity (AWS
  does, on-prem stores likely not [E]).
- **Timing and size side channels**: object sizes and counts in a plan
  leak volume per cluster; acceptable inside one organisation.
- **The share endpoint is now security-critical**: it holds the signing
  keys (or the role that signs). Run it stateless, with the keys in the
  platform's secret store, and audit to a separate sink.

## 7. The prototype

**What was built** ([`../lake-ui/`](../lake-ui/)):

- `gen.sh`: 72 synthetic edge objects with `clickhouse local` 26.10:
  48 trace objects (ClickStack columns incl. `ResourceAttributes` and
  `SpanAttributes` maps, 10k spans, about 478 KB each, zstd), 16 log
  objects (about 312 KB), 8 layout-B gauge objects; 4 clusters × 2
  publishers, lane-shaped keys (`pub-N/{signal}/e1/{seq:020d}.parquet`);
  plus a snapshot manifest (per object: cluster, services, min/max
  `Timestamp`, rows) and a trace-id maplet, standing in for the sealer's
  snapshot, lake P2 and the entity catalog.
- `signer.py`: the **share endpoint** (stdlib only): `GET /plan` checks an
  HS256 JWT (standing in for the IdP's RS256 + JWKS; `aud`, `exp`, a
  `clusters` claim), refuses a service outside the token's clusters,
  resolves scope → objects (claims → maplet → entity catalog → time zone
  map), presigns each (SigV4 query string, 300 s), audits, and serves the
  page with COOP/COEP. Keys come from the environment; the page never sees
  them.
- `index.html`: DuckDB-WASM in the page; registers each presigned URL under
  a plain name (DuckDB reads `?` in a path as a glob) and runs seven
  shapes; a hyparquet path with range reads.
- `head-shim.js`, `chdb-worker.js`: the HEAD shim, and chdb-wasm's worker
  with it installed first.
- `drive.mjs`: Playwright driver; `s3tap.py`: a logging pass-through in
  front of SeaweedFS that counts every request, range and byte (SigV4
  signs the Host header, so presign against the tap's port).

**Results [M]** (shared box, load 1–7; SeaweedFS on localhost, CORS set
with `PutBucketCors`):

| Shape | Objects (bytes) | DuckDB-WASM 1.32.0 (1.4.3): query ms, 1st / 2nd | 1.33.1-dev64 (1.5.5): 1st / 2nd |
|---|---|---|---|
| trace by id, maplet | 1 (0.48 MB) | 105 / 21 | 163 / 22 |
| trace by id, no maplet | 48 (22.9 MB) | 663 / 842 | 645 / 717 |
| service search, aggregate (entity catalog → cluster) | 12 (5.7 MB) | 323 / 149 | 317 / 103 |
| service search, latest 50 rows | 12 (5.7 MB) | 264 / 256 | 175 / 206 |
| pod logs, newest 100 (entity catalog → cluster) | 4 (1.25 MB) | 129 / 49 | 176 / 162 |
| log needle, all log objects | 16 (5.0 MB) | 431 / 207 | 196 / 213 |
| metric chart, one service | 2 (0.25 MB) | 649 (loads `icu`) / 26 | 581 / 16 |
| hyparquet: trace by id, no maplet | 48 (22.9 MB; 48 range GETs) | 955 | 964 |
| hyparquet: service search | 12 (5.7 MB; 12 range GETs) | 401 | 427 |

- **Every DuckDB read was a whole-object GET** (the tap saw 156 full
  GETs, 67.8 MB, in the 1.32.0 run, and no Range header) [M]. The "2nd"
  pass re-plans (new URLs within the same second can hit the browser's
  HTTP cache, which some rows did).
- **hyparquet's range reads fetched whole objects here** because its
  default footer prefetch (512 KB) exceeds a 0.5 MB edge object [M]:
  consistent with central-optional §8.4, raw slots are cheaper fetched
  whole. **On a 17.8 MB, 8-row-group file** (a stand-in for a compacted
  L2 file), one column's count:

| Engine | GETs | Bytes | ms |
|---|---|---|---|
| DuckDB-WASM 1.32.0, presigned or plain URL | 1 | 17.78 MB | 656 |
| hyparquet 1.31.2, size from the plan | 15 | 0.53 MB | 399 |
| chdb-wasm 0.4.0 `url()`, no shim | – | fails: HEAD 403 → "Cannot find out file size" | – |
| chdb-wasm 0.4.0 `url()` + HEAD shim | 19–33 | 11.3–15.9 MB | 2,324–3,584 |

- **Engine start** (localhost, cached nothing): DuckDB-WASM eh 1.0–1.3 s
  (1.4–1.9 s for the dev build); chdb-wasm 1.5 s [M]. Over a WAN add the
  download: 9–11 MB for DuckDB with extensions, 29 MB for chdb-wasm [M
  sizes; E time].
- **chdb-wasm via `putFile`**: 12 objects fetched in 88 ms, the service
  aggregate in 186 ms, same answer as DuckDB [M].
- **Authorization**: no token → 401; token for cluster-2 asking for a
  cluster-1 service → 403; the same token asking fleet-wide for logs got 4
  objects (its cluster) instead of 16 [M].
- **The `iceberg` extension loads** in wasm_eh for 1.4.3 and 1.5.5 [M]; no
  Iceberg table was read (the sealer isn't built).

**Not done:** a WAN or throttled-network run; Iceberg metadata as of a
snapshot in the browser; DuckDB-WASM's threaded bundle (its extensions
failed to link in 1.32.0); Firefox and Safari; memory at scale.

**Reproduce:** `gen.sh OUT` (needs `clickhouse`); upload `OUT/pub-*` to a
bucket and `signer.py cors BUCKET ORIGIN`; `s3tap.py 18334
localhost:18333 LOG`; start `signer.py serve 18190` with `S3_ENDPOINT`
set to the tap, `LAKEUI_DATA=OUT`, `LAKEUI_VENDOR` (the self-hosted
DuckDB bundle under `ddb/`, its extensions under `ext/`, hyparquet and
the dev bundle under `ddbdev/`, chdb-wasm's `dist/` under `chdb/`) and
`LAKEUI_JWT_SECRET`; then `LAKEUI_TOKEN=$(signer.py mint alice
cluster-0,…) LAKEUI_SRC=local LAKEUI_BUNDLE=eh LAKEUI_HYPARQUET=1 node
drive.mjs TRACE_ID`. The scratch bucket `lakeui` (29 MB) was left on the
local SeaweedFS.

## 8. Prior art (checked 2026-09-27)

| System / fact | What | Source (date) |
|---|---|---|
| DuckDB-WASM | stable 1.32.0 (2025-12-16), dev 1.33.1-dev64.0 (2026-07-28); DuckDB 1.5.5 is the current native stable | [npm](https://www.npmjs.com/package/@duckdb/duckdb-wasm); [overview](https://duckdb.org/docs/current/clients/wasm/overview) |
| DuckDB-WASM extensions | subset in wasm; `INSTALL` isn't durable; signed; `custom_extension_repository`; httpfs is a JS reimplementation, CORS required | [extensions](https://duckdb.org/docs/current/clients/wasm/extensions); [deploying](https://duckdb.org/docs/current/clients/wasm/deploying_duckdb_wasm) |
| Iceberg in DuckDB-WASM | REST catalogs from the browser (S3 Tables), networking through the JS stack | [blog](https://duckdb.org/2025/12/16/iceberg-in-the-browser) (2025-12-16) |
| chdb-wasm | ClickHouse as wasm, worker API, Memory64, st and mt bundles; shell at wasm.chdb.io | [npm](https://www.npmjs.com/package/chdb-wasm) (0.4.0, 2026-08-10); [announcement](https://x.com/chdb_io/status/2072230596227797361) |
| hyparquet, parquet-wasm | JS / Rust-wasm Parquet readers with async range reads | [hyparquet](https://github.com/hyparam/hyparquet) (1.31.2, 2026-09-27); [parquet-wasm](https://github.com/kylebarron/parquet-wasm) (0.9.0, 2026-09-25) |
| Memory64 | Chrome 133, Firefox 134 (later without a flag), Safari behind | [caniuse](https://caniuse.com/wf-wasm-memory64); [SpiderMonkey](https://spidermonkey.dev/blog/2025/01/15/is-memory64-actually-worth-using.html) (2025-01-15) |
| Polars in wasm | not officially supported | [@pola-rs/browser](https://www.npmjs.com/package/@pola-rs/browser) |
| Delta Sharing protocol | bearer / basic / OAuth client credentials; `version`, `timestamp`; `predicateHints`, `jsonPredicateHints`, `limitHint`; file `url`, `size`, `stats`, `expirationTimestamp`; `refreshToken` | [PROTOCOL.md](https://github.com/delta-io/delta-sharing/blob/main/PROTOCOL.md); client [1.4.2](https://pypi.org/project/delta-sharing/) (2026-08-10) |
| Delta Sharing OIDC federation | U2M and M2M, recipient IdP issues the JWT, federation policy; Databricks | [docs](https://docs.databricks.com/aws/en/opensharing/create-recipient-oidc-fed) (2026-09-11) |
| Iceberg REST vending | `X-Iceberg-Access-Delegation: vended-credentials`, `storage-credentials`, remote signing | [issue #11118](https://github.com/apache/iceberg/issues/11118); [Snowflake doc](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration-vended-credentials) |
| Apache Polaris | 1.5.0; `stsUnavailable` for S3-compatible stores | [Maven](https://search.maven.org/artifact/org.apache.polaris/polaris-server) (2026-05-14); [issue #3742](https://github.com/apache/polaris/issues/3742) |
| Lakekeeper | 0.13.4; vending and remote signing incl. on-prem S3; STS endpoint per profile; OpenFGA | [release notes](https://docs.lakekeeper.io/about/release-notes/) (2026-09-10) |
| Unity Catalog OSS | vending since 0.4.0; REST clients can't refresh vended credentials | [issue #1885](https://github.com/unitycatalog/unitycatalog/issues/1885); client [0.6.0](https://pypi.org/project/unitycatalog-client/) (2026-08-20) |
| AWS session tags | from OIDC `https://aws.amazon.com/tags`; ≤ 50; 128/256 characters; session policy ≤ 2,048 characters; `sts:TagSession` | [IAM docs](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_session-tags.html); [ABAC with JWT tags](https://aws.amazon.com/blogs/security/saas-tenant-isolation-with-abac-using-aws-sts-support-for-tags-in-jwt/) |
| S3 Access Grants | directory identities → prefixes; `GetDataAccess`; 15 min–12 h; 100k grants | [GetDataAccess](https://docs.aws.amazon.com/AmazonS3/latest/API/API_control_GetDataAccess.html); [directory identities](https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-grants-directory-ids.html) |
| S3 object tags | `s3:ExistingObjectTag`; $0.0065 per 10,000 tags per month (2025-03) | [tagging and policies](https://docs.aws.amazon.com/AmazonS3/latest/userguide/tagging-and-policies.html); [price](https://aws.amazon.com/about-aws/whats-new/2025/03/amazon-s3-reduces-pricing-object-tagging) |
| SeaweedFS STS, policy variables | STS with OIDC; `${jwt:…}` variables (PR #8082, 2026-01-22); no `sts:TagSession`; CORS operations | [sts package](https://pkg.go.dev/github.com/seaweedfs/seaweedfs/weed/iam/sts); [#8037](https://github.com/seaweedfs/seaweedfs/issues/8037); [#8082](https://github.com/seaweedfs/seaweedfs/pull/8082); [S3 API](https://github.com/seaweedfs/seaweedfs/wiki/Amazon-S3-API) |
| Nutanix Objects | per-bucket CORS (4.3–5.3); IAM/STS not supported (4.3, 5.0) | [CORS](https://portal.nutanix.com/page/documents/details?targetId=Objects-v5_1:top-configure-cors-t.html); [Veeam](https://www.veeam.com/sys976) |

## 9. Risks

1. **DuckDB-WASM's whole-object reads** make it wrong for compacted
   files. Mitigation: the range reader in front; watch upstream.
2. **chdb-wasm is 7 weeks old** and 158 MB; its read-ahead moved 63–90%
   of a file for a one-column query [M].
3. **Egress cost on AWS** for browser reads (§4); on-prem none.
4. **The share endpoint is a new security-critical service** (§6.5).
5. **Namespace isolation needs the compactor to partition by namespace**
   or a filtering reader (§6.4); the raw tail is cluster-scoped only.
6. **`complete_through` isn't sound until the edge writes
   `oscope-low`, heartbeats and birth slots** (completeness.qnt); the
   banner must say "approximate" until then.
7. **Nutanix**: no STS; bucket-policy prefix support and CORS behaviour
   unverified on a real cluster; the same conditional-write risk as
   everything else (DECISIONS risk 1).
8. **Browser memory**: wasm32 4 GB per tab at best; plans must be capped.

## 10. Next steps

1. **Plan API, for real**, inside the query service: Iceberg `loadTable`
   for readers plus the presigned plan call; reads the sealer's snapshot
   (once Q1 exists), the maplet and term segments, the entity catalog;
   returns `{snapshot_id, complete_through, files[{url, size,
   row_groups}]}`. RS256 + JWKS, not HS256.
2. **Range reader + DuckDB-WASM** in the page: hyparquet fetches the
   planned column chunks, DuckDB-WASM (eh, self-hosted, pinned) queries
   them as Arrow; measure on a 100 ms RTT link with `tc` and on L2-sized
   files.
3. **Write-side prefix policy** (D18's proposal): put the cluster in the
   key after `{root}` and give the edge role create-only PUT there (a
   change in both edges' key templates and the consumer's LIST prefixes);
   test the policies on SeaweedFS with `${jwt:…}` variables and, when a
   cluster is available, on Nutanix.
4. **Compactor partitions L2/L3 by namespace**, and publishes a
   (resource_id, 5 min) log-count rollup and per-file `resource_id` ranges
   for the pod-logs shape.
5. **Commit the STPA table** (R-S1..R-S8, SEC-1..3) so the UI's
   acceptance tests cite it; add a UI requirement: nothing after
   `complete_through` is drawn as settled.
6. **Re-test chdb-wasm** at its next minor release for range reads without
   the shim and a smaller bundle; if it holds, it gives the lake UI the
   HyperDX fork's SQL dialect.
7. **Delta Sharing endpoint**: only if an external consumer asks.
