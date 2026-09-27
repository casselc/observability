# research: central ClickHouse as an optimisation — snapshots, embedded readers and object-store indexes

A research note. The owner's question: **make the central ClickHouse cluster a
performance optimisation, not something most functionality depends on.** A
reader may not be able to live-tail, but it can pick an almost arbitrarily
recent **snapshot** and query as of it, as with a frozen DuckLake. Nothing
was built. One small probe was run (§9). The note builds on:

- [../DECISIONS.md](../DECISIONS.md): D3 (create-only slots, epochs,
  tombstones), D7 (layout B), D8–D13 (the consumer), D16 (routing), D17
  (lake), D19 (`received_at`), §3 (sizing);
- [../lake/DESIGN.md](../lake/DESIGN.md): the compactor (P1), the trace-id
  maplet (P2), cubes, and why per-object sidecars don't prune;
- [incremental-views.md](incremental-views.md): option E, a
  source-replicated central with one consumer group per replica;
- [../hyperdx/README.md](../hyperdx/README.md): what HyperDX sends, and the
  ClickStack DDL's cost (91 → **151 vCPU**);
- [../bench/sorting/README.md](../bench/sorting/README.md): edge sorting and
  service routing;
- `../entities/` (in progress, uncommitted): the SCD2 entity catalog
  (cluster → node → namespace → workload → pod) and `resource_id`.

Labels, as in lake/DESIGN.md:

- **[D]**: from a cited source (web sources read 2026-09-27);
- **[E]**: my estimate;
- **[M]**: measured. Spike numbers are quoted from the READMEs. The §9 probe
  numbers come from this note, on synthetic data and a shared box.

## 1. Summary and recommendation

**Where central's money goes today** (mid scenario: 2.07 M rows/s, 7.8 M
edge objects/day, about 6.1 TB/day of edge Parquet, 90 days):

- **151 vCPU (3 × 2 nodes) and 846 TB** with ClickStack 2.39.1's DDL,
  option 2 [D: hyperdx/README §Option 2]. It was 91 vCPU and 992 TB with
  the earlier DDL.
- **The +60 vCPU is almost all search indexing on traces and logs.** Text
  indexes, the key-value rollup view and ZSTD take insert from 3.43 to
  9.0 µs/row and merges from 10.1 to 20.1 µs/row [M: hyperdx/README].
- **ClickHouse pays that cost twice over:** once per replica, and again at
  every merge level, because a merge rebuilds the skip indexes of the part
  it writes.
- **Metrics in layout B are cheap:** 1.18 µs/point insert and 4.1 merge
  [M: D7], about 7 vCPU per replica.

**So "central optional" is mostly a question about traces and logs.** Can
search, trace lookup and the HyperDX UI run over object storage at an
acceptable latency? The answer depends on four things:

1. **Snapshots are cheap to add and reuse D3 unchanged.**
   - A **sealer** is a consumer group whose sink is a table log instead of
     ClickHouse.
   - It follows the lanes and deduplicates by content key from HEAD
     metadata alone, without reading object bodies.
   - Every 10–30 s it commits an Iceberg snapshot over the committed raw
     slots, which stay where they are, with a create-only
     `v{N:020d}.metadata.json`.
   - **The metadata file is the commit and the checkpoint.** Two sealers
     can't both commit N+1. "As of T" is `iceberg_timestamp_ms` in
     ClickHouse (since 25.4) or DuckDB time travel [D].
   - The compactor from lake P1 hands its rewrites to the sealer, so the
     table has one committer.
2. **DuckLake's frozen mode proves the reader model but doesn't fit the
   writer's cadence.**
   - A frozen DuckLake is a read-only DuckDB catalog file on S3 or HTTPS.
     It worked in the probe: create-only PUT, attach, `AT (VERSION => k)`
     [M §9].
   - But publishing means rewriting the whole catalog file. The catalog
     grows by about **0.9 KB per registered file** [M §9]. An hour of raw
     slots (324k files) would mean republishing about 300 MB every seal.
   - ClickHouse has no DuckLake reader (issue #103709 is open) [D].
   - Use it only as an optional per-day **export for DuckDB users** over
     compacted files.
3. **Readers don't need a stateful central; HyperDX needs a ClickHouse
   endpoint.**
   - The endpoint can be a **stateless ClickHouse reader tier**: the
     ordinary server with no MergeTree data and no Keeper, only `icebergS3`
     tables named and typed like ClickStack's, plus the filesystem and
     Parquet-metadata caches. It scales horizontally and can scale to zero.
   - chDB (v4.4.0, 2026-09, ClickHouse core 26.7) and DuckDB 1.5.5 fit
     fan-out jobs and tools, but can't be HyperDX's backend: chDB's HTTP
     server is archived (`chdb-server-bak`) [D].
4. **Full-text search on the lake is possible, and far cheaper than
   central's text indexes, if the index is a time-partitioned LSM of
   immutable segments.**
   - The segments map term → (object, row block) and are merged
     30 s → 5 min → hour → day. They are **built per cluster next to the
     data, not per object at query time**.
   - This changes lake/DESIGN's verdict for **needle** terms: ids, error
     codes, rare words. A per-object index can't prune when it is probed at
     query time, but it can when many objects are merged into one segment.
   - It doesn't change the verdict for common predicates (service,
     severity). Those need routing, locality compaction and zone maps.
   - **Probe** [M §9, synthetic logs]: 238k rows in 30 objects give 215k
     distinct terms, and only 128 of them occur in every object. Postings
     at object granularity are **14% of the edge bytes with trace ids and
     4.6% without**. Trace ids belong in the P2 maplet. A needle trace id
     hit 2 of 30 objects.
   - **Estimated cost at the mid scenario: 2–4 vCPU** [E], against about
     60 vCPU for central's text indexes.

**The options:**

| # | Option | Verdict |
|---|---|---|
| A | Central as today (ClickHouse is the truth; raw slots deleted after ingest) | Works, fastest, **151 vCPU + Keeper, 846 TB**. Everything stops when it is down, though no data is lost: it waits in the lanes. |
| B | **Central as cache + lake** (the lake is the truth; ClickHouse holds 1–7 days and can be rebuilt from the lake) | **Yes, as the migration path.** It keeps < 1 s for hot data. Storage halves. **Compute saves little unless central also drops its text indexes** (then about 100–110 vCPU [E]). |
| C | **Lake + stateless readers, no central for traces and logs** (small aggregates-only ClickHouse optional) | **Recommended target.** About 45–100 vCPU in total [E], mostly elastic readers and a stateless compactor. Search 1–5 s warm, trace lookup under 1 s with the maplet, freshness 15–40 s. HyperDX is degraded in known places (§4). |
| D | Federated per-cluster lakes and readers (data stays in each cluster's bucket or site) | **Only if residency or per-site isolation requires it.** It is C times 20 deployments, with a per-reader floor and partial results by design. Build C so that D is a configuration of it: per-cluster tables and a `Distributed` table over the readers. |

**Recommendation.**

1. **Adopt C as the target, and reach it through B.**
   - Build the sealer (§5) as a consumer group with an Iceberg sink.
   - Add lake P1 (compaction) and P2 (trace maplet) on top, then the term
     LSM (§6).
   - Point HyperDX at a stateless ClickHouse reader tier.
   - Keep today's central as the hot cache (B) until C's warm latency is
     measured against HyperDX.
2. **Keep a small ClickHouse for aggregates**, never for raw traces or logs:
   - span metrics and log counts (lake option 2a);
   - D15's 5-minute rollups;
   - raw layout-B metrics, if metric charts must stay under 1 s;
   - the metric-name helper.

   That is about 10–40 vCPU [E]. It is what makes alerts and dashboards
   fast and deterministic. **Losing it degrades them to the lake's rollup
   tables, about 5 minutes behind.** It is still an optimisation.
3. **Prototype cheaply first (§11):** Q1 sealer + as-of reads, Q2 HyperDX on
   a stateless reader, Q3 the term LSM on hdxgen data. Each is days, not
   weeks, and each has a stop condition.

## 2. The capability matrix

"Warm" means compacted lake files with indexes; "tail" means raw slots not
yet compacted (the last ≤ 1 h). Latencies are [E] unless marked.

| Capability | A: central as today | B: central as cache + lake | C: lake + stateless readers | D: federated per cluster |
|---|---|---|---|---|
| **Structured search** (service, status, attributes) | < 1 s [M: hyperdx 9 scenarios 2.1–2.3 s server time in total] | hot < 1 s; older 1–4 s per 24 h (lake P1) | warm 1–4 s per 24 h; tail: service routing leaves about 3.6k objects/h per service, so 15 min is about 900 GETs, **2–5 s** | as C, in parallel per cluster; a query without a cluster filter fans out to 20 |
| **Full-text / needle** | < 1 s (`hasAllTokens` on `idx_lower_body`) | hot < 1 s; cold only with term segments | **1–3 s** with the term LSM (§6); minutes without it (fan-out scan) | as C; 20 segment lookups in parallel |
| **Trace by id** | < 1 s | hot < 1 s; cold 0.3–1 s with the P2 maplet | 0.3–1 s (maplet) + tail L0 maplets, about 1 s | 20 maplet lookups in parallel, about 1 s |
| **Metric charts** | < 1 s (layout B views) | same, if metrics stay in central | from the aggregates central (< 1 s); or lake metrics sorted by (MetricName, series), **1–5 s** | as C, or a per-site aggregates central |
| **Dashboards** | < 1 s | < 1 s (rollups in central) | < 1 s from the aggregates central; **1–2 s** from rollup tables published in snapshots | as C |
| **Alerts** | HyperDX alert task every minute [M] | same | on the aggregates central: same. **Lake only:** each evaluation reads the tail (fleet logs over 5 min are 6k objects), so evaluate on L1 files or rollups, **5 min lag**; `complete_through` makes it deterministic (§5.4) | per cluster, plus a global roll-up |
| **HyperDX UI** | full [M] | full on hot; cold degraded (below) | works through `icebergS3` tables with ClickStack's columns, **degraded:** no `mergeTreeTextIndex` / `mergeTreeIndex` fast paths, so pickers and key discovery scan unless helper tables are published; rollups only if published; untested (Q2) | as C, behind `Distributed` with `skip_unavailable_shards` (partial results) |
| **Live tail** | 0.2–1 s visibility [M: D6] | same | **near-tail: 15–40 s** (edge batch fill about 1 s + seal cadence 10–30 s + planning) | same |
| **Retention (90 d)** | TTL drop by day; **846–992 TB**, a copy per replica | hot 15–106 TB + lake about 400 TB | lake about 400 TB + indexes 15–25 TB | same bytes, per-cluster buckets and policies |
| **Central down** | UI down; ingest waits in the lanes, no loss | falls back to warm latencies | nothing depends on it; the aggregates central's loss moves alerts to lake rollups | one site down → its data missing, flagged |
| **Consistency** | what has been ingested (per replica) | hot/cold boundary must be checked (lake §2.5) | **repeatable as-of snapshot**; a page pins one snapshot id | per-cluster snapshots; no global cut unless a root is published (§8.1) |

## 3. Options in detail

### A. Central as today

Already measured in DECISIONS.md.

**Cost [D: hyperdx/README; calculator]:**

- 151 vCPU on 6 nodes of 32 vCPU, plus 3 Keeper nodes;
- 846 TB, a copy per replica: **about $19.9k/month on S3, $38k on local
  HDD**;
- edge PUTs about $1.2k/month.

**Risk:** central holds the only queryable copy, and Keeper couples both
replicas' failures (D9, D13).

### B. Central as cache + lake (the migration path)

**How.**

- **The consumer keeps feeding ClickHouse with a short TTL (1–7 days).**
- **A second consumer group, the sealer (§5), and the lake compactor
  (lake P1) make the lake the durable copy.**
- **Rebuilding central** is a consumer group replaying the retained raw
  slots or the compacted lake files; the content-key check makes that
  idempotent (D11).
- **The raw-slot GC floor** is the minimum over every group, as in
  incremental-views option E.

**What "cache" buys:**

- storage: hot 15 TB (1 day) plus about 400 TB in the lake, against
  846–992 TB;
- a central that can be lost and rebuilt;
- **no compute saving, unless central also sheds the text indexes.** The
  lake's term segments then serve full text beyond the hot window, and
  central's insert goes back towards the table-only 6.2 µs/row
  [M: hyperdx/README]: **about 100–110 vCPU** [E].

**HyperDX failover** [E]:

- HyperDX has one connection per source, so failover lives **behind** it:
  a proxy (like `hyperdx/scripts/chproxy.py`) that routes to central, or,
  when central is unhealthy, to the reader tier.
- On the reader tier, `otel_traces` and `otel_logs` are views over hot ∪
  lake with the same names and columns.
- The UI keeps working at warm latency, and the fast paths fall back to
  scans.

**Risks:**

- the hot/cold boundary (lake §2.5);
- deduplication in two places;
- two costs to run until central shrinks.

### C. Lake + stateless readers (the target)

**The pieces:**

| Piece | What | Cost at the mid scenario [E] |
|---|---|---|
| Sealer (§5) | consumer group → Iceberg snapshots over raw slots every 10–30 s; dedup by content key from HEAD | ≤ 1 vCPU; HEADs already done by the consumer today (7.8 M/day ≈ $94/month); 3 PUTs per seal ≈ $1–8/month |
| Compactor (lake P1) | raw → L1 (5 min) → L2 (1 h) sorted files; hands the sealer the replace | 15–20 vCPU; 7.8 M GETs/day ≈ $94/month |
| Trace maplet (lake P2) | trace_id → (file, row group), hourly then daily | ≤ 2 vCPU; about 27 GB/day |
| Term LSM (§6) | term → (object/file, row block), per cluster | 2–4 vCPU; 15–25 TB over 90 days |
| Rollup and helper tables | kv rollup 15 min, metric names, span metrics, 5-min rollups, published per seal or per L1 | < 1 vCPU; < 1 GB/day |
| Reader tier | stateless `clickhouse-server` × N (no Keeper, no MergeTree), filesystem cache on local NVMe | the calculator's query load is 24.8 vCPU on MergeTree; on the lake assume **1.5–2.5×** that, **about 16–64 vCPU, elastic** |
| Aggregates central (optional) | 2 × 8–16 vCPU ClickHouse: span metrics, rollups, optionally raw layout-B metrics | 8–40 vCPU |

**Totals [E]:**

- **Compute:** about **45–100 vCPU**, most of it stateless or elastic,
  against 151 vCPU plus Keeper.
- **Storage:** about 400 TB lake plus about 20 TB indexes, **about
  $9.7k/month**, against $19.9k (S3) or $38k (HDD).
- **Query S3 requests:**
  - a HyperDX page is 10–20 statements × about 200 GETs cold, so about
    4k GETs ≈ $0.0016 [E];
  - 10k cold pages a day ≈ **$500/month**, less with the filesystem
    cache;
  - alerts on L1 files (100 rules × 100 GETs per minute) ≈ **$170/month**.

**Maturity and licences [D]:**

- ClickHouse (Apache-2.0): Iceberg read with time travel since 25.4.
  **Known silent-wrong-result bugs in pruning** (#119173, #118371,
  #120986; lake §2.3).
- iceberg-rust 0.10 (July 2026, Apache-2.0): `StaticTable` reads from a
  metadata file with no catalog. Writing without a catalog is ours to do:
  we write the Avro manifests and the metadata JSON (lake P1 already plans
  that).
- DuckDB 1.5.5 (MIT), with its iceberg and ducklake extensions.

**Risks:**

1. HyperDX has never run against `icebergS3` sources, and its fast paths
   are MergeTree-specific (Q2 decides).
2. Iceberg metadata churn at a 10–30 s cadence (§5.3).
3. ClickHouse Iceberg pruning bugs.
4. Warm latency is seconds: users will feel it on pages that send 10–20
   statements.
5. Everything depends on the store's CAS (DECISIONS risk 1, Nutanix).

### D. Federated per-cluster

**How.**

- Each cluster (or Nutanix site) keeps its lanes, sealer, compactor, term
  LSM and a small reader in its own bucket.
- The region runs a front reader whose tables are
  `Distributed(cluster_readers, …)` over the per-cluster readers' `icebergS3`
  tables. `Distributed` needs no Keeper.
- HyperDX talks to the front reader.
- `skip_unavailable_shards = 1` turns a lost site into partial results. The
  UI can't show that, so a "sites answered" banner needs a HyperDX change,
  or a proxy-side warning.

**Fit:**

- data residency and per-site egress;
- a lost site doesn't take others down;
- services are mostly cluster-local, so per-cluster locality compaction
  loses little.

**Cost [E]:**

- the same bytes;
- compute of C plus a floor of about 2 vCPU per cluster reader
  (+40 vCPU for 20 clusters);
- 20 of everything to operate;
- a cross-cluster trace is 20 maplet lookups.

**The entity catalog** (`../entities/`) gives a free pre-filter: `k8s.*`
attributes resolve to a cluster, so a query is routed to one site
(§8.4).

**Verdict:** a configuration of C, not a separate design. Build C with
per-cluster tables from the start.

## 4. HyperDX without a stateful central

What HyperDX 2.39.1 does, from [../hyperdx/README.md](../hyperdx/README.md),
and what happens on a reader tier over `icebergS3` [E unless marked]:

| HyperDX mechanism | On MergeTree [M/D] | On `icebergS3` / views | Mitigation |
|---|---|---|---|
| Search SQL (`WHERE`, `ORDER BY Timestamp DESC LIMIT`) | fast, pruned by the primary index | works; pruned by Iceberg partitions and min/max, and the Parquet page index on compacted files | locality compaction (lake P1) |
| Full text `hasAllTokens(lower(Body), …)` | `idx_lower_body` text index: 6 of 383 granules | **open:** whether 26.x evaluates `hasAllTokens` without a text index or refuses it; if it evaluates, it is a scan | term LSM behind a proxy rewrite, or a HyperDX hook (§6.5) |
| Map key discovery `mergeTreeTextIndex(…, 'idx_*_attr_items')` | 3 k index rows | **fails** (not a MergeTree), so HyperDX falls back to `sampledKeys` scans (seen on the old schema [M]) | publish a small keys table; needs a HyperDX change to use it |
| Native-column values via the `*_kv_rollup_15m` table | 0.9 s over 9 scenarios [M] | **works if the rollup is published as a table with the same name and columns** (HyperDX reads it by name) | the compactor emits the kv rollup per 15 min, mergeable [D: incremental-views §3] |
| Metric-name picker `mergeTreeIndex()` | 17 ms [M] | fails on views today [M] | the aggregates central keeps metrics as MergeTree, or HyperDX's `seriesTable` flag (hyperdx/README) |
| Trace waterfall (TraceId + time window) | fast | with the maplet: only through a proxy that turns `TraceId = x` into `s3('{k1,k2}')`; without it, bloom per row group over the window | lake P2 + proxy |
| Alerts | same SQL every minute | works; cost as in §2 | the aggregates central |
| Writes | none [M] | none needed | – |

**The proxy is the lever.** HyperDX's SQL is regular enough to recognise
three shapes (`TraceId = '…'`, `hasAllTokens(…)`, and time windows) and
rewrite them into index lookups plus exact file lists. `chproxy.py` already
sits in that position. That keeps HyperDX unpatched, at the price of a
component that must track HyperDX's SQL; pin the version.

## 5. Snapshot / "as of" design that reuses D3

### 5.1 The sealer is a consumer group with a table-log sink

It follows every lane exactly as the consumer does (D8), with its own
`{ctl}/group-lake/` leases and heartbeat. It is option E's "another group",
except that its sink is S3.

**Per round (every Δ = 10–30 s):**

1. `LIST StartAfter` each owned lane's position. Idle lanes back off as in
   D8.
2. `HEAD` each new slot for its `x-amz-meta-oscope-*`: content key, rows,
   `received_at`, and min/max `Timestamp` if the edge adds it (it doesn't
   today; one small edge change). Tombstones close epochs as in D3.
3. **Dedup:**
   - A content key already in the dedup set means the slot is recorded as
     *skipped* and never added.
   - The set is SlateDB or CAS'd hourly shards (§7), with a TTL of the
     copy horizon, 3 days (D11).
   - **Bodies are never read.**
4. Write an Iceberg manifest listing the new slots as data files, by
   absolute path, with per-file stats from metadata. Then write a manifest
   list. Both are create-only; nothing references them yet.
5. **Commit:** `PUT If-None-Match: *` of
   `metadata/v{N+1:020d}.metadata.json`. Its snapshot summary carries:
   - `frontier`: per lane, (epoch, last seq) and closed epochs;
   - `skipped`: the content-key copies;
   - `complete_through` (§5.4);
   - references to index segments (§6) and to the rollup tables' snapshot
     ids.
6. A 412 means another sealer (a zombie or a racing twin) committed N+1.
   Re-read it, rebase if its frontier is behind ours, else stop.

**The metadata file is the commit and the checkpoint.** There is no
separate checkpoint CAS: a restarted sealer reads the latest metadata and
resumes from its frontier.

- The lease is for liveness and load, **not for safety**. Safety is
  create-only on N+1, the s3Inline pattern with one writer per key, which
  is already modelled.
- The Quint model is the consumer model with the sink swapped:
  `atMostOnce` becomes "each content key in at most one live snapshot";
  `neverSkipsCommitted` becomes "every committed slot is in a snapshot or
  in `skipped`".
- **New mutants:**
  - `commitBeforeManifest` (a reader sees a missing manifest);
  - `gcReferencedSlot` (a slot deleted while a retained snapshot names
    it);
  - `rebaseLosesSkipped`.

**One committer per table.**

- The compactor (lake P1) writes L1/L2 files and the maplet and term
  segments, then drops a create-only **proposal**:
  `{lake}/proposals/{hour}/{gen}.json` = {remove: raw slots, add: files,
  indexes}.
- The sealer applies proposals in its next commit.
- Sealer and compactor never race on N+1, and a proposal is idempotent:
  a proposal whose removes are already gone is skipped.
- This is how Delta and Iceberg handle concurrent writers, minus the
  conflict retries.

**Sharding.**

- One sealer per region is enough at about 90 objects/s. Registration,
  metadata only, cost about 7.8 ms per file even in DuckDB
  [M §9: `ducklake_add_data_files` including a footer GET], so it is well
  under a core with parallel HEADs.
- For D, run one sealer per cluster, each with its own table.

### 5.2 How a reader picks "as of T"

| Reader | Latest | As of T | Cost |
|---|---|---|---|
| ClickHouse `icebergS3` | lexicographically last `*.metadata.json` without a catalog [D: lake §2.3] (a LIST per plan unless cached) | `SETTINGS iceberg_timestamp_ms = T` or `iceberg_snapshot_id` (25.4+; `system.iceberg_history`) [D] | metadata JSON + manifest list + new manifests; immutable, so cached by path |
| DuckDB iceberg | metadata path | snapshot by id or timestamp (verify the parameter names in 1.5.x) | same |
| iceberg-rust / DataFusion 55 | `StaticTable::from_metadata_file` [D] | pick the snapshot from the log | same |
| Our tools (trace lookup, term search, fan-out) | `LIST metadata/ StartAfter last` (1 LIST) | read `v{N}` whose summary time ≤ T; N by binary search on zero-padded names, or a tiny `time-index` object per hour | 1–3 GETs |

**Pinning a page:** the proxy (§4) or the tool records the snapshot id at
the first statement and sends every later statement with
`iceberg_snapshot_id`. Every panel then sees the same data, which central
can't offer today.

### 5.3 Cadence against file count [E]

At 90 objects/s. Stats limited to `Timestamp`, `ServiceName` and
`received_at`, about 150–300 B per manifest entry. Raw slots live about 1 h
before a compaction proposal replaces them.

| Seal cadence Δ | 10 s | 30 s | 5 min |
|---|---|---|---|
| snapshots/day | 8,640 | 2,880 | 288 |
| new data files per commit | 900 | 2,700 | 27,000 |
| manifest bytes per commit | 0.15–0.3 MB | 0.4–0.8 MB | 4–8 MB |
| PUTs/day (manifest, list, metadata) ≈ $/month | 26k ≈ $4 | 8.6k ≈ $1.3 | 0.9k ≈ $0.1 |
| live manifests before merging (1 h of tail) | 360 | 120 | 12 |
| reader poll: new bytes to plan per poll | 1 manifest + list | same | same |
| freshness (batch fill about 1 s + Δ/2 + plan) | about 7 s | about 17 s | about 2.5 min |

**What keeps it bounded:**

- **Expire snapshots** older than the as-of window (default 1 h, plus the
  reader grace). Without that, the metadata JSON's snapshot log grows
  about 1 KB per snapshot: 8.6k/day is 8.6 MB of JSON.
- **Merge the current hour's manifests** every few minutes (Iceberg
  rewrite-manifests), so there are ≤ 10 live tail manifests per hour.
- **Partition by `hour(Timestamp)` and cluster,** so a 15-minute query
  opens one hour's tail manifests.

**Why not DuckLake here:** the catalog is a single DuckDB file that has to
be rewritten per seal. It grew **+2.6 MB per 3,000 registered files,
about 0.9 KB/file** [M §9], so 324k live tail files would be about 300 MB
per republish.

**Why not Delta:** it fits the protocol equally well (put-if-absent
`_delta_log/{v:020d}.json`; delta-rs uses conditional puts [D: lake
§2.3]). Iceberg is preferred for ClickHouse's transform pruning and its
documented time travel. A Delta log of add actions with absolute paths is
the fallback if ClickHouse mishandles absolute Iceberg paths outside the
table location (open question 2).

### 5.4 `complete_through`: a watermark for deterministic alerts

- A snapshot can say "every request that entered edge custody before W is
  in me, or is a known skipped copy". Here W is the minimum over live
  lanes of the lane's last `received_at` seen, and idle lanes are counted
  as caught up to the sealer's LIST time, since a lane is ordered and
  gap-free (D3).
- **The exceptions are exactly D19's:** a buffer replay after an outage
  lands with its original, older `received_at`. So W is sound except for
  replays, and each replay is counted (`late_to_snapshot_total`), as
  D11's horizon audit counts late copies.
- An alert on window [t, t+5 min) evaluates on the first snapshot with
  W ≥ t + 5 min + slack. It gets **the same answer every time,** which
  central can't give today.

### 5.5 GC with snapshots

The rules extend lake §2.0:

- a raw slot is deleted only below min(consumer groups, sealer frontier of
  the **oldest retained snapshot that references it**, compactor);
- a replaced raw slot stays until every snapshot naming it has expired,
  plus the reader grace;
- the existing `--delay`, `--zombie` and tombstone rules and the
  keep-the-slot-below-the-position rule are unchanged (D12);
- **as-of window × tail bytes is the extra storage:** 1 h is about
  250 GB, about $6/month.

### 5.6 Edge-produced micro-lakes and a merge-tree of manifests (evaluated)

**The idea:** each publisher writes its own per-epoch manifest, and a
merge tree of manifests builds the global one.

**Verdict: no. The lane already is the micro-lake.**

- The slot names are deterministic (`{producer}/{signal}/{epoch}/{seq}`),
  and an epoch is gap-free, so **a frontier alone names every file**.
- An edge manifest would be a second PUT per batch, the shape D3 rejected
  (F1–F3).
- The merge tree exists anyway: lanes → per-cluster sealer → (for D) a
  region root.

**The part worth keeping: frontier-only snapshots.**

- A snapshot of just `{lane: (epoch, seq)}` plus `skipped` is a few KB.
  It can be published every second.
- A planner expands it to exact keys and hands them to ClickHouse
  `s3('{k1,…}')`, which issues no LIST [M: D10].
- No engine understands it natively, so it serves our own tools, live
  tail and fan-out workers, not HyperDX.

## 6. Edge / per-cluster full-text indexing: a time-partitioned index LSM

### 6.1 What lake/DESIGN found, and what batching changes

**lake/DESIGN §2.1 found:**

- every service appears in every object of its cluster's publishers, so a
  per-object sidecar says "maybe" for any service predicate;
- probing per-object filters is 72k blooms an hour;
- Loki abandoned free-text blooms (lake §3.2).

**All of that holds for query-time probing and for common terms.**

**Batching changes it for rare terms:**

- Merge many objects' term lists into one segment per (cluster, window),
  and the query becomes one dictionary lookup per segment, returning the
  few objects that contain the term.
- Common terms (service names, `GET`, `error`) prune nothing. Drop the
  postings of any term present in more than X% of a segment's objects;
  the probe had **128 of 215k terms in every object**. Those predicates go
  to routing, locality compaction and zone maps.
- This is an exact inverted index, not a bloom. It doesn't saturate when
  merged, and merging sorted term lists is a streaming merge, with no
  re-tokenising.
- **Per (service, hour) batching adds little:** with routing, a service is
  already one lane. Batching per (cluster, time) is what makes segments
  large enough to be worth a lookup.

### 6.2 Design

| Level | Scope | Built by | Contents | Count |
|---|---|---|---|---|
| L0 | per cluster, per seal (30 s) | per-cluster **indexer**: reads each new object once (GET + decode), tokenizes | FST term dictionary → postings of (object ordinal, row-block bitmap of 1,024 rows); a trace-id maplet L0; a hotcache footer (≤ 1% of the segment) | 20 × 120/h |
| L1 | per cluster, 5 min | indexer: k-way merge of 10 L0s | same | 20 × 12/h |
| L2 | per cluster, 1 h | compactor with lake P1: **remaps** postings from raw (object, row) to (L2 file, row group) through the compaction permutation, with no re-tokenising | same, keyed to L2 files | 20/h |
| L3 | per cluster, 1 day | streaming merge of 24 L2s | same | 20/day |

**What is indexed** (the same items HyperDX's ClickStack DDL indexes):

- log `Body` tokens (lowercased, split on non-word characters, as
  `idx_lower_body`);
- `LogAttributes` and `SpanAttributes` `k=v` items;
- selected span strings (`StatusMessage`, `exception.message`,
  `db.statement` prefix).

**Not indexed:** TraceId (the P2 maplet does that), and terms above the
frequency cut.

**Commit:** segments are unreferenced files until the sealer's next
snapshot names them (§5.1). GC follows snapshot expiry. Readers find a
window's segments from the snapshot summary.

**Where to tokenize: the exporter or a per-cluster indexer?**

| | In the edge exporter (a sidecar in the object's tail, lake §2.1 layout) | **Per-cluster indexer** (recommended first) |
|---|---|---|
| CPU [E] | tokenize about 1.5 µs/log, items about 0.5 µs/span → **about 0.6 vCPU fleet (+11% on the 5.3 vCPU Rust edge)**; the L0 merge then reads only the tails (5% of bytes) | GET + decode about 1 µs/row, plus tokenize → **about 2 vCPU fleet**, stateless; shares the compactor's read when co-located |
| edge budget (4 µs/span today) | +0.5–1.5 µs/row on the hot path; +5% object bytes for logs [M §9 ratio] | 0 |
| format lock-in | every index change is an edge rollout, in two edges (Rust and Go, D1) | the indexer's code only |
| index lag | the seal cadence | the seal cadence + one GET |

**Verdict:** start in the indexer. Move tokenizing into the exporter only
if the indexer's reads become the bottleneck. At the mid scenario they
aren't: the compactor already reads every object.

### 6.3 Numbers [E, scaled from §9 M]

Probe ratios, synthetic logs: postings at object granularity are 10.8 B per
row with trace ids and 3.5 B without; about 1 posting per row without trace
ids. Row-block postings add about 14%.

| | Logs (200k/s) | Spans, attribute items only (600k/s) |
|---|---|---|
| postings/s (L0) | about 200k (no trace ids) | about 300–600k [E: 0.5–1 per span for items that are not everywhere] |
| L0 bytes/day | about 60 GB (3.5 B/row), **about 4–5% of 0.86 TB/day** | about 50–100 GB, **2–4% of 2.6 TB/day** |
| after the L2/L3 merge | lower: rare terms dominate, and hour and day dictionaries share prefixes | same |
| 90 days | about 5 TB | about 5–9 TB |
| build (indexer) | about 0.6 vCPU | about 1–1.5 vCPU |
| merges (about 50 ns/posting × 3 levels) | about 0.03 vCPU | about 0.1 vCPU |

**Compared with central:** ClickStack's text indexes and rollup cost about
+5.6 µs/row insert and +10 µs/row merge, per replica. At 800k rows/s that
is **about 25 vCPU raw, and most of the 60 vCPU once headroom and the
second replica are counted** [D: hyperdx/README; E]. The LSM wins because:

- its granularity is objects and row blocks, not positions per granule;
- it is built once, not per replica;
- its levels merge postings; they don't rewrite data.

The synthetic data is optimistic (templated bodies). Real bodies with
ids in free text push the dictionary up: assume **2–3×** until Q3 measures
hdxgen or real data.

### 6.4 What a query does

**Example:** `hasAllTokens(Body, 'timeout', 'acct-7731')`, 24 h, no service
filter.

1. **Snapshot:** latest, or as of T. Its summary lists the window's
   segments: 20 L3 (or 20 × 24 L2), plus the current hour's L1/L0 per
   cluster.
2. **Per segment:** hotcache (cached) → 1 range GET for the term block of
   each term → intersect postings → (file, row group) list. Segments in
   parallel: 20 clusters × about 2 GETs for L3, plus the tail's about
   200 L0/L1 × 2 GETs.
3. **Rows:** hand the exact list to ClickHouse or chDB,
   `s3('{f1,…}')` with row-group ranges and `WHERE` re-checking the
   predicate, or to DuckDB `read_parquet([...])`. Rare terms touch
   **1–20 files → 2–40 range GETs.**
4. **Total:** about **50–500 GETs, 1–3 s** [E], against minutes for a
   24 h scan of about 0.5 TB of Body (lake §2.5).

**For HyperDX:** the proxy (§4) rewrites the `hasAllTokens` statement into
a `_path IN (…)` form, or runs step 3 itself.

### 6.5 The entity catalog and topology as a pre-filter

- `k8s.pod.name`, `k8s.node.name`, `k8s.namespace.name` or
  `k8s.deployment.name` in a query → the SCD2 catalog (`../entities/sql/catalog.sql`)
  gives the cluster (and the time the pod existed) → **only that cluster's
  segments and files: 20× less work before any index is touched.**
- With service routing (D16), service → publisher is the hash ring. The
  sealer records the ring version per snapshot, so service → lane is
  metadata too.
- **A node → lane map** needs the gateway to route by node, or a sidecar
  that records which lanes each node's agents hit. Worth it only for
  node-heavy queries (open question 7).

## 7. Object-store-native KV / LSM: SlateDB and alternatives

| System | What it is [D, 2026-09] | As our index store | Verdict |
|---|---|---|---|
| **SlateDB** 0.16.0 (latest; Apache-2.0; Commonhaus) | embedded LSM on object storage; single writer by a formally verified manifest-fencing protocol; many readers (`DbReader`, which polls the manifest; checkpoints are metadata-only and cheap); compaction embedded or as separate distributed workers (0.15); transactions, merge operator; Go, Java, Python, Node bindings; WAL iterator for CDC (0.16) | **Not for postings or trace maplets:** they are append-only and time-partitioned, and static segments merged by level avoid WAL, memtable and compaction write amplification. **Yes for small mutable catalogs:** the content-key dedup set (7.8 M keys/day, TTL 3 days), the series catalog (38 M active, `LastSeen` updates), the entity catalog, the snapshot time index. One writer per DB, so one per cluster or shard. | **Adopt for the dedup set when the sealer is built;** it replaces lake §2.0's CAS'd hourly shards. Pre-1.0: pin it. Needs the same conditional-write guarantees as D3 (Nutanix, risk 1). |
| **Tonbo** | embedded LSM with Parquet SSTables on S3, manifest by CAS; active (commits July 2026) | Parquet SSTs are readable by any engine; less mature than SlateDB | watch |
| **Lance** (format + LanceDB) | table format with versions; scalar, FTS (inverted) and vector indexes on object storage; Polaris integration Jan 2026 | would mean a second data format; its FTS is per table version, not per time window | no, beyond borrowing ideas |
| **tantivy** 0.26.2 / **Quickwit** 0.9 | inverted index, splits with a hotcache; Quickwit indexes at **7.5 MB/s per core with 4 GB RAM per core**, and searchers want 8 GB per core on S3 [D: Quickwit sizing] | a full doc store and positions; at our 200k logs/s × about 300 B that is about 8 indexer cores for logs alone, before spans | use the tantivy crate for FST and postings code if convenient; no Quickwit metastore |
| **Tempo** vParquet + sharded blooms; **Loki** TSDB index + dataobj index objects | lake §3.1 | blooms keyed by trace-id prefix; index objects separate from data | the maplet (P2) and our segments already follow the shapes |
| **Iceberg Puffin** | theta NDV and deletion vectors only (lake §2.3) | custom blobs would be read by our code only | put segment references in snapshot summaries, not Puffin |

## 8. Other ideas, evaluated

1. **The frozen root: one global snapshot pointer for D.**
   - A region root `root/v{N}.json` = {cluster → snapshot id}, create-only.
     Gives a consistent cross-cluster "as of".
   - It costs one more writer and a lagging cluster holds the root back.
   - **Verdict:** only if cross-site consistency is asked for. Default:
     per-cluster "as of T" by timestamp, which is consistent enough for
     observability.
2. **Kv-rollup, metric-name helper and span metrics as snapshot-published
   tables.**
   - All are mergeable aggregates (incremental-views §3), so the compactor
     emits them per 5 minutes, and the sealer commits them with the same
     snapshot.
   - HyperDX reads the kv rollup by table name [D: hyperdx/README]. The
     picker needs a HyperDX change either way.
   - **Verdict: yes.** Cheap, and it removes the worst lake degradation.
3. **Zone maps + bloom per (service, hour) as a tiny catalog.**
   - Iceberg manifests already carry per-file min/max. With routing, the
     ring already answers "which lane holds service X".
   - A separate catalog adds nothing over manifests plus the ring history.
   - **Verdict: no separate catalog.** Put the ring version in snapshot
     summaries.
4. **Parquet page index + ranged reads.**
   - Rust edge objects have no page index today (D1); compacted L2 files
     must have one.
   - ClickHouse needs `remote_filesystem_read_method = 'read'`,
     `remote_read_min_bytes_for_seek = 0` and the metadata cache to range
     read [M: bench/sorting].
   - **Verdict: yes for L2, not for raw slots** (about 1 MB objects are
     cheaper fetched whole).
5. **Arrow Flight / ADBC federation.**
   - chDB 4.3 added an ADBC extra; DataFusion speaks Flight SQL.
   - HyperDX speaks only ClickHouse, so federation for HyperDX is
     ClickHouse `Distributed` / `remote()` over readers (native protocol).
   - **Verdict:** Flight only for our own tools, if at all.
6. **Serverless fan-out for the cold tier (Lambda, K8s jobs).**
   - A 30-day unindexed log scan is about 26 TB. At about 150 MB/s per
     worker × 1,000 workers that is **about 3 minutes, about $6 of
     compute (2 GB workers) plus about $1.3 of GETs (8 MB ranges)** per
     query [E, list prices].
   - Nutanix has no Lambda; a K8s job pool of chDB or DuckDB workers does
     the same at the cluster's own cost.
   - **Verdict:** the "cold, minutes" tier, for rare forensic queries,
     behind a tool rather than HyperDX.
7. **chDB fleets as the cache.**
   - chDB has the filesystem cache and the same SQL, but no server mode
     that HyperDX can use, and no sharing between processes.
   - **Verdict:** stateless `clickhouse-server` readers instead. chDB goes
     in workers and tools.
8. **Materialise only aggregates centrally.** This is recommendation 2:
   - span metrics (+4.5k exponential-histogram points/s, lake §2.2);
   - 5-minute rollups (127k rows/s at most);
   - optionally all layout-B metrics (about 7 vCPU per replica plus
     headroom).

   **Verdict: yes.** It is the cheap part of central, and it makes alerts
   and dashboards fast.
9. **Source-replicated readers (incremental-views option E) meet the
   lake.** With a lake as the truth, E's second replica is simply the
   stateless reader tier. **E becomes unnecessary for traces and logs,**
   and remains the Keeper-less answer for the aggregates central.

## 9. The probe (run, small)

**Environment:**

- DuckDB **1.5.5**, ducklake `d8a1881e`, fts `6814ec9`, httpfs, in a
  scratch venv;
- local SeaweedFS 4.47;
- 30 log objects from `ent-objects/a/logs/` (entity experiment,
  synthetic): 237,977 rows, **18.2 MB as the edge wrote them**, re-encoded
  by DuckDB (snappy) to 56.2 MB and copied to a scratch bucket;
- shared, loaded box; timings are indicative only;
- the bucket and the scratch directory were deleted afterwards.

| Step | Result [M] |
|---|---|
| Register the 30 objects in place with `ducklake_add_data_files`, 3 per commit (10 "seals") | 0.4–0.8 s in total; each commit is a snapshot |
| `AT (VERSION => v)` | v1 0, v4 76,936, v7 144,432, v10 207,977 rows; the growth matches the seals |
| Tiny insert (5 rows) | **inlined** in the catalog: still 30 files (default inlining limit 10 rows [D]) |
| Freeze: PUT the catalog file with `If-None-Match: *` | 200; a second create-only PUT: **412**, so the D3 commit pattern works for catalog files |
| Fresh reader `ATTACH 'ducklake:s3://…/00000000000000000011.ducklake' (READ_ONLY)` | 0.03 s; latest 237,982 rows; `AT (VERSION => 4)` 76,936; `INSERT` refused (read-only) |
| Needle `TraceId = x` on the frozen lake | **30 of 30 files read** (random ids have no min/max pruning), 2 rows; a `Timestamp` filter read 15 of 30 files |
| Catalog growth | 4.73 MB at 30 files → **7.35 MB at 3,030 files over 110 snapshots: about 0.9 KB per file**; 7.8 ms per registration including the footer GET; `count(*)` over 3,030 files from metadata in 0.02 s |
| Term postings (Body tokens + TraceId + `LogAttributes` k=v) | 2.30 M occurrences, **214,938 distinct terms** (158k in one object only, 128 in all 30); postings at object granularity **2.56 MB (14% of edge bytes)**, with 1,024-row blocks 2.92 MB; **without trace ids or everywhere-terms 0.84 MB (4.6%)**; a needle trace id → objects [15, 17] |
| Build cost in DuckDB SQL (2 threads) | 5.2 µs/row for tokenize + group (an upper bound; Rust should be about 1 µs) |
| DuckDB `fts` extension | 4.3 µs/row build; BM25 works; but it indexes **a DuckDB table** (a copy of the data: a 110 MB database file for 18 MB of Parquet), isn't updated by inserts, and can't index Parquet in place, so it is **not a lake index** |

**Conclusions:**

- **Frozen-catalog readers and as-of queries work as advertised**, and
  create-only commits apply unchanged.
- **Per-file catalog cost and whole-file republishing** rule DuckLake out
  for a 10–30 s tail, and keep it for sealed days.
- **Postings are small once trace ids and everywhere-terms are removed.**

## 10. Prior art and facts (checked 2026-09-27)

| System / fact | What | Source (date) |
|---|---|---|
| DuckLake 1.0 | production release, catalog in PostgreSQL, SQLite, MySQL or DuckDB; 1.1 expected Sept 2026 | [ducklake.select 1.0](https://ducklake.select/2026/04/13/ducklake-10/) (2026-04-13); [FAQ](https://ducklake.select/faq) |
| Frozen DuckLake | read-only DuckDB catalog file on S3 or HTTPS; single periodic writer; 10.8 B rows / 4,030 files built in about 22 min | [blog](https://ducklake.select/2025/10/24/frozen-ducklake/) (2025-10-24); [public DuckLake guide](https://ducklake.select/docs/stable/duckdb/guides/public_ducklake_on_object_storage) |
| `ducklake_add_data_files` | registers Parquet in place; **ownership passes to DuckLake** (compaction and cleanup may delete the file) | [docs](https://ducklake.select/docs/stable/duckdb/metadata/adding_files) |
| DuckLake inlining, maintenance | inlining on by default at 10 rows, `ducklake_flush_inlined_data`; `merge_adjacent_files`, `expire_snapshots`, `cleanup_old_files`, `rewrite_data_files`, `CHECKPOINT`; nothing is deleted without expiry | [inlining](https://ducklake.select/docs/stable/duckdb/advanced_features/data_inlining), [maintenance](https://ducklake.select/docs/stable/duckdb/maintenance/recommended_maintenance) |
| ClickHouse and DuckLake | no support; feature request open | [#103709](https://github.com/ClickHouse/ClickHouse/issues/103709); [DataLakeCatalog docs](https://clickhouse.com/docs/engines/database-engines/datalakecatalog) (Glue, Unity, Hive, REST) |
| ClickHouse Iceberg time travel | `iceberg_timestamp_ms` / `iceberg_snapshot_id` (25.4+); `system.iceberg_history` | [iceberg function docs](https://clickhouse.com/docs/reference/functions/table-functions/iceberg); [PR #77439](https://github.com/ClickHouse/ClickHouse/pull/77439); [PR #71072](https://github.com/ClickHouse/ClickHouse/pull/71072) |
| iceberg-rust 0.10 | 254 PRs, Mar–Jul 2026; `StaticTable` read-only from a metadata file | [release blog](https://iceberg.apache.org/blog/apache-iceberg-rust-0.10.0-release/); [StaticTable](https://rust.iceberg.apache.org/api/iceberg/table/struct.StaticTable.html) |
| DuckDB | 1.5.x stable (1.5.5 in the probe); Iceberg v3 and deletion vectors in 1.5.3 | [1.5.0](https://duckdb.org/2026/03/09/announcing-duckdb-150); [iceberg 1.5.3](https://duckdb.org/2026/05/29/new-iceberg-features) (2026-05-29) |
| DuckDB `fts` | indexes a table; not updated automatically | [docs](https://duckdb.org/docs/current/core_extensions/full_text_search) |
| chDB | v4.4.0 (2026-09-11, chdb-core ≥ 26.7, `chdb.durable`); v4.3.0 ADBC extra; the HTTP server repo is archived as `chdb-server-bak` | [releases](https://github.com/chdb-io/chdb/releases); [chdb-server-bak](https://github.com/chdb-io/chdb-server-bak) |
| DataFusion | 55.1.0 (Sept 2026) | [release issue #24462](https://github.com/apache/datafusion/issues/24462) |
| SlateDB | 0.16.0 latest (distributed compaction, WAL iterator); Apache-2.0; Commonhaus; verified manifest fencing; `DbReader` checkpoints | [site](https://slatedb.io/); [releases](https://github.com/slatedb/slatedb/releases); [checkpoints](https://slatedb.io/docs/design/checkpoints/) |
| Tonbo | Parquet SSTs on S3, manifest CAS; last commit 2026-07 | [GitHub](https://github.com/tonbo-io/tonbo) |
| Lance | table format with FTS inverted index; Polaris integration | [FTS spec](https://lance.org/format/index/scalar/fts/); [Polaris blog](https://polaris.apache.org/blog/2026/01/06/apache-polaris-and-lance-bringing-ai-native-storage-to-the-open-multimodal-lakehouse/) (2026-01-06) |
| tantivy, Quickwit | tantivy 0.26.2; Quickwit sizing 7.5 MB/s per core indexing, 4 GB per indexer core, 8 GB per searcher core on S3 | [tantivy releases](https://github.com/quickwit-oss/tantivy/releases); [Quickwit sizing](https://quickwit.io/docs/deployment/cluster-sizing) |
| Tempo, Loki, Husky, OpenObserve, Parseable, Honeycomb, turbopuffer | block and index shapes, Lambda fan-out | [../lake/DESIGN.md](../lake/DESIGN.md) §3 (checked 2026-09-25) |
| HyperDX 2.39.1 | fast paths on MergeTree text indexes, `mergeTreeIndex` picker, kv rollup by name | [../hyperdx/README.md](../hyperdx/README.md) |

## 11. What a prototype would prove (small, cheap first)

**Q1. Sealer + as-of reads (2–3 days).**

- **Build:** a Rust binary reusing `coord.rs`. It seals a local lane set
  every 10 s into Iceberg metadata (manifests written directly or with
  iceberg-rust's writer), with absolute paths to raw slots, and
  create-only `v{N}.metadata.json`.
- **Read:** ClickHouse `icebergS3` latest and with
  `iceberg_timestamp_ms`, and DuckDB.
- **Measure:**
  - planning GETs and latency per poll at Δ = 10 and 30 s;
  - metadata growth with and without snapshot expiry and manifest
    merging;
  - that ClickHouse reads absolute paths outside the table location;
  - exactly-once against the consumer soak's audit, with the sealer's
    Quint mutants (§5.1).
- **Stop if** ClickHouse can't plan a 30 s-cadence table in under 1 s
  warm.

**Q2. HyperDX on a stateless reader (1–2 days).**

- **Build:** a `clickhouse-server` with no MergeTree, `icebergS3` tables
  named like ClickStack over Q1's table, and the kv rollup published as a
  table.
- **Run:** `hyperdx/scripts/hdx_ui.js` through the nine scenarios.
- **Record:** per statement, works / slow / fails, including
  `hasAllTokens` without an index and `mergeTreeTextIndex` on a
  non-MergeTree, with server time against central.
- **Stop if** search pages need a HyperDX patch rather than a proxy.

**Q3. Term LSM on realistic data (3–4 days).**

- **Build:** L0 segments (FST + roaring) per 30 s from hdxgen objects, L1
  and L2 merges, and a lookup returning (file, row group).
- **Measure:**
  - bytes per row (against §6.3's 3.5 B/row and the 2–3× caveat);
  - build µs/row in Rust;
  - needle latency over 1 h and 24 h cold and warm, with GET counts;
  - result equality with `hasAllTokens` on central.
- **Stop if** the index is above 15% of log bytes without trace ids, or a
  needle over 24 h needs more than 1k GETs.

**Q4. Alerts on snapshots (1 day, after Q1).**

- `complete_through` computed per snapshot.
- HyperDX's alert SQL replayed on successive snapshots: same answers,
  lag distribution.
- Replay after a simulated 4-day outage counted as late (D19's scenario).

**Q5. Federation (1 day).** Two stateless readers on two buckets, a front
reader with `Distributed`, one reader killed mid-query: partial results
and latency.

## 12. Open questions

1. **HyperDX on `icebergS3`:** does 26.x evaluate `hasAllTokens` without a
   text index? What does HyperDX do when `mergeTreeTextIndex` fails on a
   non-MergeTree source? (Q2.)
2. **Absolute data-file paths** outside the Iceberg table location, in
   ClickHouse and DuckDB; if refused, a Delta log or copy-on-seal is the
   fallback. (Q1.)
3. **Real term cardinality:** production bodies carry ids, URLs and
   stack traces; the synthetic 3.5 B/row may be 2–3× low.
4. **Reader-tier sizing:** the query load on lake files against MergeTree
   (the 1.5–2.5× assumption); how much the filesystem cache recovers for
   repeated dashboards.
5. **Does the owner accept 15–40 s "near-tail"** instead of live tail, and
   5-minute-lag alerts when the aggregates central is down?
6. **Is cross-site snapshot consistency (§8.1) ever needed,** or is
   timestamp-aligned per-site "as of T" enough?
7. **Node → lane topology:** is a node-level pre-filter worth a routing
   change, or does the entity catalog's cluster pruning suffice?
8. **Nutanix:** conditional writes (DECISIONS risk 1) now also gate the
   sealer, SlateDB and every create-only snapshot. Without them, a
   Keeper/etcd pointer is the fallback. It is small, but it is central
   again.
9. **Edge metadata:** adding min/max `Timestamp` (and `ServiceName` when
   routed) to `x-amz-meta-oscope-*` lets the sealer write per-file stats
   without a footer GET. The edge change is small (both edges, D1).
