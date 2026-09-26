# lake: search, trace lookup and dashboards over the edge Parquet, with less central infrastructure

A design note. Nothing here was built or benchmarked. It builds on:

- [../awss3/README.md](../awss3/README.md): the create-only, per-epoch slot log;
- [../otap-rs/README.md](../otap-rs/README.md), section "Consumer": the leased,
  CAS'd consumer and its GC;
- [../metrics-layout/README.md](../metrics-layout/README.md): layout B and
  what HyperDX does through views;
- [../bench/clean/calc.py](../bench/clean/calc.py): the mid scenario.

Labels:

- **[D]**: from a cited source, including this spike's own READMEs;
- **[E]**: my estimate;
- **[M]**: measured. The only [M] numbers here are quoted from the spike's
  READMEs. Nothing was run for this note.

## 1. Summary and recommendation

**The problem, in numbers** (mid scenario, `calc.py` defaults) [D: calc.py; E: derived]:

- **Rows.** 600k spans/s, 200k logs/s and 1.27 M metric points/s, from 20
  clusters, 60 publishers (3 per cluster), in 10k-row batches.
- **Objects.** That is about **90 edge objects/s, 7.8 M a day**:
  - 60/s traces, 20/s logs, 10/s metrics;
  - traces alone are 216k objects an hour.
- **Bytes.** At the calculator's 50 B/row Parquet (24 B per layout-B point),
  a trace object is about **500 KB**, and the edges write about
  **6.1 TB/day**: 2.6 TB traces, 0.86 TB logs, 2.6 TB metrics.
- **Central ClickHouse today.** About 115 vCPU in 4 nodes of 32 (2 shards ×
  2 replicas):
  - 5.9 TB/day compressed, one copy;
  - 90-day storage of about 990 TB with the default replicated local cold
    tier, or about 530 TB with one cold copy on S3.

**What that means:**

1. **Edge objects are too small and too many to query directly.**
   - A one-hour search for one service touches about 1/20 of the fleet's
     objects: 10k objects, and 20k GETs, per hour searched [E].
   - A per-object sidecar index (idea 1 as stated) can't fix that. For the
     service's own objects every index says "maybe".
   - Search over older data needs the data **rewritten** into large,
     service-clustered objects. That is compaction in the Husky / Quickwit
     sense, not index merging alone.
2. **Trace lookup and dashboards don't need the data rewritten.**
   - Trace lookup needs a merged trace_id → object map (a maplet).
   - Dashboards need pre-aggregates.
   - These can be cheap and small.
3. **At the mid scenario, central ClickHouse isn't expensive.**
   - Against ClickHouse with a one-copy S3 cold tier, a Parquet lake saves
     roughly 30% of retained bytes, mostly on spans [E, §2.5].
   - It removes most central RAM and disk.
   - It opens the data to other engines.
   - It doesn't remove a large compute bill, because there isn't one. The
     case for the lake is operational simplicity and openness at least as
     much as cost.

**The options, briefly:**

| # | Option | Verdict |
|---|---|---|
| 1 | Edge sidecar indexes (trace filters, dictionaries + roaring, token blooms, zone maps), merged by stateless compactors | **Partly.** Zone maps are already free in the Parquet footer. Trace-id indexing belongs in the compactor. Roaring postings and token blooms at the edge don't reduce fan-out enough to pay. Loki abandoned free-text blooms for the same reason (§3.2). |
| 2 | Edge cubes with mergeable sketches | **Yes, cheapest dashboard path, as OTel metrics:** spanmetrics-style exponential histograms and counts through the existing metrics lane. That is about +0.4% metric points [E], and HyperDX-compatible. Theta/HLL/top-k cubes are optional extras that HyperDX can't chart. |
| 3 | A table format over the objects | **Yes, over compacted objects, as Iceberg v2** with create-only `vN.metadata.json` commits (no catalog server). This is what gives ClickHouse (and so HyperDX) time and bucket pruning on cold data. Delta also fits the commit protocol. DuckLake needs a SQL catalog and has no ClickHouse reader. |
| 4 | Search-on-S3 engines | **Borrow, don't adopt.** Tempo's block layout, Husky's locality compaction, Quickwit/tantivy only if cold free-text search becomes a requirement. None of them reads our Parquet as-is. |
| 5 | Hybrid: ClickHouse hot 1–7 days, lake for older data | **Recommended target.** HyperDX keeps working through a hot ∪ cold view, with known degradations. |

**Recommendation: prototype 5 + 3 + 2 first (P1), then the trace-id maplet (P2).**

- **P1** is a stateless **lake compactor** that:
  - follows the existing lanes with the consumer's lease and checkpoint code;
  - deduplicates by content key;
  - rewrites each signal-hour into large Parquet files, sorted the way
    ClickStack sorts, in two locality-compaction levels;
  - commits them as an Iceberg table with create-only metadata files.

  ClickHouse reads it with `icebergS3`, and HyperDX gets a hot ∪ cold view.
  Dashboards on old data come from spanmetrics-style metrics that central
  keeps for the whole retention.
- **P2** adds an hourly and daily **trace_id → file maplet** built by the
  compactor. A cold trace lookup over 30 days then costs a few dozen GETs
  instead of thousands.

Section 5 has the plan and success criteria.

## 2. The options in our architecture

### 2.0 What is common: lanes, the compactor, GC

**Lanes today** [D: ../awss3, ../otap-rs]:

- Keys are `{root}/{producer}/{signal}/{epoch}/{seq:020d}.parquet`.
- Each slot is committed by one create-only PUT.
- The consumer:
  - leases lanes;
  - keeps a CAS'd checkpoint per lane;
  - tombstones dead epochs;
  - deletes behind the checkpoints after `--delay` (in-flight PUTs);
  - deletes closed epochs after `--zombie`.

**New pieces, all of them one-writer-per-key and create-only or CAS'd:**

| Piece | Key | Writer | Commit |
|---|---|---|---|
| Edge cube lane (option 2, if not sent as metrics) | `{root}/{producer}/cubes/{epoch}/{seq}.parquet` | edge, same appender | create-only slot; an ordinary lane |
| Edge sidecar (option 1) | inside the data object (recommended), or `…/{seq}.idx` | edge | same PUT as the data, or create-only after the data commit |
| Lake data files | `{lake}/{signal}/data/hour=…/L{level}-{compactor-epoch}-{n}.parquet` | compactor | unreferenced until the table commit |
| Lake indexes: maplets, zone-map rollups | `{lake}/{signal}/index/{hour or day}/{gen}.mpl` | compactor | unreferenced until the table commit |
| Table commit | `{lake}/{signal}/metadata/v{N:020d}.metadata.json` (Iceberg) | compactor holding the table lease | `PUT If-None-Match: *` on N+1: the s3Inline pattern with one writer |
| Compactor checkpoint | `{ctl}/lake/ckpt/{producer}/{signal}.json` | compactor | CAS (`If-Match`), as the consumer's |

**The compactor** is the consumer's twin, not a new kind of service:

- The same `coord.rs` lease, checkpoint and fencing. It runs as a fleet of
  stateless workers.
- **Input:** committed slots, followed by `LIST StartAfter` as the consumer
  does.
- **Output:** files. It never writes to ClickHouse.
- **Deduplication by content key** (F4 and cross-epoch copies, which the
  consumer removes with its projection check [D: ../otap-rs "Consumer"]):
  - it keeps a content-key set per signal for the dedup window, as a CAS'd
    object sharded by hour;
  - alternatively, it reads the consumer's ingest decisions.
  - The simpler option is its own set: about 32 B per object, so 7.8 M
    objects/day is about 250 MB/day [E].

**GC rules**, extending `consume gc`:

1. **Raw slots:** delete only below
   `min(consumer checkpoint, compactor checkpoint)`, with the existing
   `--delay` and `--zombie` rules. The tombstone rule doesn't change
   (the `gcTombs` mutant still applies).
2. **Lake files and indexes:** delete only when no retained snapshot
   references them, **plus a reader grace period** longer than the longest
   query, since ClickHouse may have planned against an old snapshot.
   Iceberg's expire-snapshots → orphan-delete is exactly this.
3. **Level-1 files** replaced by a level-2 merge follow rule 2.
4. **Retention** drops whole hour partitions in one commit, then applies
   rule 2.
5. **Minute cubes** are deleted after the hourly rollup commits, plus the
   grace period. Hourly cubes after daily.
6. **Never delete or rewrite `vN.metadata.json` below the current N**
   inside the grace window. ClickHouse picks the lexicographically last
   metadata file when no catalog is given [D: ClickHouse iceberg docs /
   search result], so names must be zero-padded and create-only.

### 2.1 Option 1: edge sidecar indexes

**How it would work.**

- The edge computes the index while encoding.
- **Recommended: put the index inside the same Parquet object.**
  - The index bytes go between the last column chunk and the footer.
  - Footer key-value metadata records their offset and length
    (`oscope.idx.*`). The spike already rewrites the footer to add KV
    metadata [D: ../awss3 `inline/`].
  - Parquet readers ignore unreferenced bytes. One tail range GET reads the
    footer and the index together.
  - There is **no new lane and no new commit**: the index is committed with
    the data, and GC deletes it with the data.
- **A separate `.idx` object** would need:
  - a PUT after the data commit;
  - the content hash inside it, since a slot can hold "another batch" after
    HEAD resolution [D: ../awss3 Writer step 3];
  - readers treating a missing sidecar as "scan".
- **Compactors** merge the per-object indexes hourly and daily into lake
  index objects that map to (object ordinal, row group).

**Per index type, at the mid scenario:**

| Index | Edge CPU [E] | Size [E] | What it buys |
|---|---|---|---|
| Zone maps (min/max per row group and page) | 0: Parquet statistics and page index already exist. ClickHouse reads row-group min/max through `ParquetMetadata` [M: ../awss3]. Sorting each object by (ServiceName, Timestamp) at the edge makes them selective: about 1 ms per 10k rows | 0 | Row-group skipping inside an object. Nothing across objects. |
| Trace-id filter per object: binary fuse 8 (≈9 bits/key, FP ≈0.4%) [D: Graf & Lemire] | ≈50 ns/key → <1 ms per 10k spans; fleet +0.03 vCPU | 2–5k distinct trace ids per object → 2–6 KB; about 20 GB/day. For comparison, the Parquet split-block bloom on TraceId costs 16 KB per 10k spans (131 against 115 KB) [M: ../otap-rs] | Only a raw-tier trace lookup that probes every object in the window: 216k probes an hour. Useless without a merged index, and the merged index is built better by the compactor from the TraceId column. |
| Attribute dictionaries + roaring postings (service, status, k8s.*, http.*) | ≈0.4 µs/row for ~10 columns; fleet +0.3 vCPU | a few KB per object | The objects that contain value v. A service is present in almost every object of its cluster's 3 publishers, so the posting list barely prunes (§2.1 fan-out). It pays only on large objects, so build it in the compactor. |
| Token blooms over log bodies (words, not n-grams) | ≈1.5 µs/log (tokenize ~150 B, ~20 inserts); fleet +0.3 vCPU. Trigrams: ~150 inserts/log, about 10× that | 20–50k distinct tokens per 10k logs × 10 bits → 25–60 KB, **5–12% of the object**; 40–100 GB/day | Negative lookups on rare tokens. Hourly merge can't OR blooms of this many keys without saturating them, so it must concatenate: 72k blooms an hour, about 3.6 GB to read per hour searched. |

**Query paths and fan-out, raw tier, no data rewrite:**

- **Trace lookup, 1 day.** A merged hourly map gives about 24 range GETs;
  then the matching raw objects (≈5–20: a trace crosses services and so
  publishers), about 2 GETs each (tail, row group).
  - Total **≈60 GETs, 0.3–1 s** [E].
- **Search `service=X AND status=error`, 1 hour.**
  - 10.8k candidate objects (1/20 of 216k).
  - The merged zone map or postings can't exclude them, since X is in
    nearly all of them.
  - **≈20k GETs per hour searched** [E]. Not interactive.
- **Log token search, 1 hour.** 3.6 GB of blooms, then the hits.
  Not interactive.

**HyperDX compatibility.** None, directly. HyperDX speaks SQL to ClickHouse,
and ClickHouse doesn't know these indexes. It would need either a trace
lookup API or a ClickHouse UDF, which HyperDX wouldn't call.

**Risks:**

- **Edge format lock-in:** every index change is an edge rollout.
- Index bytes on the wire are **5–12% for logs**.
- The raw objects' lifetime under a compacting design is about an hour,
  so edge indexes serve only the window that ClickHouse's hot tier already
  serves (option 5).

**Verdict.**

- Keep zone maps, and sort within the object at the edge.
- Consider **dropping** the edge's Parquet bloom on TraceId (−12% trace
  bytes [M: 131 → 115 KB]) once the compactor builds the trace index.
- Build everything else in the compactor.

### 2.2 Option 2: edge-side materialized views (cubes)

**Two forms.**

**(a) As OTel metrics** (recommended):

- The edge derives span metrics: calls, errors and duration as an
  **exponential histogram**, keyed by (service, span name, kind, status
  code) plus a few chosen attributes.
- It derives log counts keyed by (service, severity).
- They go through the **existing metrics lane**, layout B, into the
  ClickHouse metrics tables, which central keeps for the full retention.
- This is what the contrib **spanmetrics connector** does. It supports delta
  or cumulative temporality and exponential histograms, and its cardinality
  is controlled by `dimensions` and the resource key attributes
  [D: spanmetricsconnector README].
- Exponential histograms merge exactly when their scales are aligned, the
  same kind of guarantee as DDSketch's full mergeability [D: DDSketch
  paper; OTel data model].
- On a Go edge it is config only. On the Rust edge it is new code:
  otap-dataflow has no spanmetrics node that I found [E].

**(b) As cube objects:**

- Per-minute Parquet with sketch columns:
  - DDSketch or KLL for quantiles;
  - HLL or Theta for distinct counts (users, trace ids);
  - space-saving for top-k (routes, error messages).
- On an ordinary `cubes` lane, merged hourly and daily by the compactor.
- Apache DataSketches has an official Rust crate (HLL, Theta, KLL,
  frequent items, cross-language serialization tested by the TCK)
  [D: apache/datasketches-rust, crates.io `datasketches`] and Java/C++/Go
  ports.
- **t-digest isn't a fit:** it has no worst-case error bound, and merge
  results depend on order [D: "Theory meets practice at the median",
  arXiv 2102.09299].
- **KLL** gives rank error.
- **DDSketch** gives relative error, which is what latency percentiles need
  [D: DDSketch, VLDB 2019].

**Edge cost** [E]: one hash of the dimension tuple plus one histogram insert
per span, about 0.1–0.2 µs, so **fleet +0.1 vCPU**. That is next to the
edge's 5.9 vCPU fleet-wide [D: calc.py].

**Size** [E]:

- Assume 3,000 services fleet-wide (150 per cluster) × about 30
  (span name, kind, status) groups = 90k groups.
- Each group is seen by 3 publishers per minute, so 270k group-minutes a
  minute.
- **As metrics:** 4.5k exponential-histogram points/s (+0.4% on 1.27 M/s).
  At layout B's 38.1 B per exponential-histogram point [M: metrics-layout],
  that is **≈15 GB/day** in ClickHouse, about 1.3 TB for 90 days.
- **As cube objects:** about 150 B per group-minute, ≈58 GB/day at minute
  resolution before merging. Merged across publishers and rolled up to an
  hour, about 0.4 GB/day.
- **HLL** (lgK 11, ~1.5 KB) per service-minute, 3,000 services: ≈6 GB/day
  at minute resolution, 0.1 GB/day hourly.

**Commit lanes and GC.**

- **(a)** uses the metrics lane unchanged, with the metrics-layout rule
  (announce a series only after its series object commits).
- **(b)** is an ordinary lane. Minute cubes are deleted after the hourly
  commit, plus the grace period (rule 5).

**Queries.**

- **(a)** is a ClickHouse metrics query. HyperDX charts it natively
  (histogram percentiles, rates). A 30-day chart reads 90k series × 720
  hourly points at most, milliseconds to a second [E].
- **(b)** hourly cubes, if they are Parquet sorted by (service, span name,
  hour): a 7-day dashboard for one service is 168 hour objects × 1 range GET
  (footers cached), **≈0.3–1 s cold** [E]. Daily cubes cut that 24×.
  ClickHouse can't merge DataSketches blobs natively [E], so (b) needs
  DuckDB with a DataSketches extension or our own service.

**HyperDX compatibility.**

- **(a):** yes, as metrics charts.
- The **"events → chart" path**, where HyperDX charts count/p95 straight
  from the spans table, doesn't use them. On cold data it scans. HyperDX's
  rollup acceleration looks for `MaterializedView` engines on the source
  table [D: metrics-layout, `metadata.ts`], so it won't find edge cubes.
  Dashboards on old data must be built as metric charts.
- **(b):** not chartable in HyperDX.

**Risks:**

- **Cardinality at the edge.** A careless dimension (a user id) multiplies
  the metrics. spanmetrics has caches and limits that must be set.
- **Two answers to one question.** A chart from cubes and a chart from raw
  spans differ by the sampling policy and by histogram error (≤ about 2%
  relative at scale 5) [E].
- **Layout B's per-object fixed cost** grows by one object per flush per
  publisher: about 2 objects/s [E].

### 2.3 Option 3: a table format over the objects

**Over raw edge objects: no.**

- 7.8 M data files a day into manifests is a metadata problem.
- Iceberg manifests list every data file.
- Raw objects can hold cross-epoch duplicates, and a table format has no
  content-key deduplication.
- Tables over raw slots would also need their own GC coupling.

**Over compacted files: yes.**

- **Iceberg v2**, no deletion vectors needed. The partition spec is
  `hour(Timestamp)`, plus `bucket(N, TraceId)` only if P1 measures that
  better than the maplet.
- Manifests carry per-file column min/max; the files carry row-group stats,
  the page index and a TraceId bloom.
- **Engine support:**
  - **ClickHouse** `icebergS3` / `DataLakeCatalog` [D: ClickHouse iceberg
    docs]:
    - partition pruning with `use_iceberg_partition_pruning`, for time
      transforms (PR #72044) and for bucket and truncate (PR #79262);
    - min/max pruning from manifests;
    - an in-memory metadata cache (`use_iceberg_metadata_files_cache`, on
      by default);
    - `_path` / `_file` virtual columns;
    - writes since 25.7 behind `allow_insert_into_iceberg`;
    - v3 deletion vectors are read-only.
  - **DuckDB** 1.5.3: v3, deletion vectors, MERGE, REST catalogs
    [D: duckdb.org 2026-05-29].
  - **Trino and Spark:** read, plus Puffin statistics
    [D: Trino Iceberg connector docs].
- **Puffin** defines exactly two blob types:
  - `apache-datasketches-theta-v1` (NDV);
  - `deletion-vector-v1` (v3).

  There is **no bloom-filter blob** [D: puffin-spec.md]. Engines use Puffin
  for planning statistics, not for skipping. So a custom Puffin blob, such as
  our maplet, would be read only by our own code.

**Catalog without a server.**

- An Iceberg catalog only has to swap a pointer atomically.
- The Hadoop catalog's `vN.metadata.json` scheme was unsafe on S3 only
  because it relied on rename [D: Hadoop catalog guidance].
- With `If-None-Match: *` on `v{N+1}.metadata.json` it is the s3Inline
  protocol with one writer, which we already model and test.
- ClickHouse without a catalog picks the lexicographically last
  `*metadata.json` [D: ClickHouse docs, via search]. Boring Catalog does the
  same with one CAS'd JSON file [D: boringdata/boring-catalog].
- **ClickHouse's own `version-hint.text` path has open data-loss bugs**
  (#114194, #120164, #120165) [D]. Don't let ClickHouse write or optimize
  the lake. Read only.

**Delta** fits the commit protocol even more directly:

- `_delta_log/{version:020d}.json` is put-if-absent.
- delta-rs now uses S3 conditional puts by default and has removed the
  DynamoDB log store [D: delta-rs discussion #4482].
- ClickHouse reads Delta through delta-kernel-rs, including deletion vectors
  [D: delta.io blog 2026-05-18].
- Iceberg is preferred here only because ClickHouse's hour and bucket
  **transform pruning** is documented, while pruning on Delta partition
  columns derived from Timestamp is not something I found [E].

**DuckLake** 1.0 (April 2026) keeps all metadata in a SQL database:
PostgreSQL, SQLite or DuckDB [D: ducklake.select]. That is central
infrastructure, and ClickHouse has no DuckLake reader [E]. Not for this.

**Edge cost:** none. **Index size:** manifests are about 1 KB per file
[E], so 1k files an hour is about 25 MB/day.

**Query latency** [E]: planning reads the metadata JSON, the manifest list
and the partition's manifests, about 3–5 GETs, cached in memory after that.

**HyperDX:** as for option 5.

**Risks:**

- ClickHouse's Iceberg code is young. The issue tracker has silent-wrong-
  result bugs in exactly the pruning we'd rely on:
  - hour-transform pruning evaluated in the server time zone (#119173);
  - UUID and FLBA statistics and bloom mismatches (#118371, #120986).

  Pin the version, run the server in UTC, store TraceId as `String`
  (hex) rather than FLBA, and keep a result-equality test against the hot
  tier.

### 2.4 Option 4: search-on-S3 engines

Two ways to use them:

- **Run them.** Tempo queriers over blocks we write in vParquet5 format;
  Quickwit over its own splits.
- **Borrow** their layouts and code.

Running Tempo means writing **its** schema:

- One row per **trace**, with nested ResourceSpans > ScopeSpans > Spans
  and dedicated columns [D: Tempo schema docs].
- That needs trace assembly across edges, which the compactor could do per
  hour (Tempo's own compactor combines partial traces).
- It gives TraceQL and Grafana, **not HyperDX**. HyperDX expects
  span-per-row ClickStack tables.
- So reusing the schema means a second read path and a second UI.
  **Borrow the layout, not the schema** (§3).

**Quickwit:**

- Can't index external Parquet.
- Its splits are tantivy indexes with a hotcache (<0.1% of the split,
  opened in <60 ms) [D: quickwit-101, architecture docs].
- It is healthy again:
  - licensed **Apache-2.0** since 0.9.0 (July 2026; 0.8.x was AGPL-3.0);
  - 0.9.1 on 2026-09-22;
  - commits on main through 2026-09-24 [D: git tags and history].
- If cold **free-text** log search becomes a requirement, the right piece
  is tantivy splits per (hour, service range) built by the compactor.
  That is **the tantivy crate, not Quickwit**, whose metastore (PostgreSQL,
  or a single-writer file on S3) and ingest we'd have to adopt.

### 2.5 Option 5: the hybrid

**How it works.**

- **Hot tier.** The consumer keeps ingesting into ClickHouse. Its TTL drops
  to D days (1–7).
- **Lake.** The lake compactor follows the same lanes independently and
  writes the Iceberg lake for everything.
- **Views.** ClickHouse defines, per signal:

  ```sql
  CREATE VIEW otel_traces_all AS
    SELECT * FROM otel_traces            WHERE Timestamp >= now() - INTERVAL D DAY
    UNION ALL
    SELECT * FROM lake.otel_traces_cold  WHERE Timestamp <  now() - INTERVAL D DAY
  ```

  `lake.otel_traces_cold` is an `icebergS3` table whose column names and
  types match ClickStack's.
- **HyperDX** sources point at the `*_all` views. Metrics sources keep
  pointing at the layout-B views; metrics are cheap enough to keep in
  ClickHouse for the whole retention.

**The compacted layout** (traces; logs likewise), borrowed from Husky's
locality compaction [D: Husky compaction post]:

1. **L1 (every 5 minutes).**
   - Merge the 5 minutes of raw slots: about 18k trace objects, 9 GB.
   - Sort by ClickStack's key `(ServiceName, SpanName, Timestamp)`.
   - Cut at about 256 MB: **about 35 files**, each covering a narrow
     service range.
2. **L2 (each hour, after the hour's zombie bound).**
   - k-way merge the 12 sorted L1 runs into about 420 files of 256 MB.
   - Row groups of about 1 M spans, the page index, zstd, and a bloom on
     TraceId.
   - A service's hour now sits in 1–3 files.
3. **Late data.** Slots for an already-compacted hour become a small
   additional file in that partition (an Iceberg append), merged at the next
   daily pass.

**Compactor cost** [E, from the spike's per-row numbers]:

- Parquet decode + sort + encode at about 5–6 µs per span or log (the Rust
  edge spends 4.5 µs/span encoding from OTLP [M]), twice (L1, L2).
- Metrics at about 2 µs per point.
- **≈15–20 vCPU continuous**, stateless, spot-friendly.
- **S3 requests:** about 7.8 M raw GETs/day (~$3) plus about 25k PUTs/day
  (L1 + L2).
- Reads about 6 TB/day and writes about 10 TB/day (two levels; L1 is
  deleted after L2).

**Storage.**

- Compacted and sorted Parquet at about 50 B/span and 50 B/log
  (calculator constant; sorted zstd should do better) → about 3.5 TB/day.
- Metrics:
  - compacted by series at an estimated 8–10 B/point (layout B stores
    6.3 in ClickHouse [M]) → about 1 TB/day;
  - or downsampled after `rawDays`.
- Total **≈4.5 TB/day** against ClickHouse's 5.9 TB/day (spans 80 B
  there [D: calc.py]).
- For 90 days: **about 400 TB on S3**, against about 530 TB with
  ClickHouse cold on S3 (one copy) or about 990 TB replicated locally
  [E: calc.py].
- Central shrinks to the hot tier:
  - D = 1: 15 TB hot;
  - D = 7: 106 TB hot [E: calc.py].
  - Hot-tier vCPU doesn't change much: ingest and merge dominate, not
    retention.

**Query paths, cold tier** [E]:

| Query | Path | GETs | Latency |
|---|---|---|---|
| Trace by id, window 1 h | Prune to the hour's ~420 files. Without a trace index, every file's TraceId bloom per row group: about 2k range GETs (footers cached by the Parquet metadata cache, `use_parquet_metadata_cache`, 26.3+ [D: PR #98140]) | ≈2,000 | 2–5 s |
| Trace by id, 7–30 days (pasted id) | as above × hours | 300k+ | not interactive → **P2 maplet** |
| Trace by id with the P2 maplet | 1 range GET per day or hour maplet, then 1–5 files × 2 range GETs | 30 (30 days) + ≈10 | **0.3–1 s** |
| `service=X AND StatusCode='Error'`, 24 h | hour pruning; manifest min/max on ServiceName → 1–3 files per hour; page-index pruning inside | 24 × (2–3 files × 2–4) ≈ 150–300 | 1–4 s |
| Log `hasToken(Body,'foo')` with service filter, 24 h | same, then scan Body of the matching pages | ≈200 + data | 2–10 s |
| Log token search with no service filter, 24 h | scan about 0.5 TB of Body | – | minutes; out of scope unless tantivy splits are added |
| Dashboard, 30 days | metrics in ClickHouse (option 2a) | 0 | <1 s |

**Commit lanes and GC:** §2.0. The hot tier's TTL and the lake's
compaction watermark are independent. The view's `now() - D` boundary
must be later than the compaction watermark plus late-data slack, or a
narrow band is invisible. **Check this, don't assume it:** P1 alarms when
`max(lake watermark) < now() - D + slack`.

**HyperDX compatibility** [M for views over MergeTree, from
metrics-layout; E for icebergS3]:

- Search, charts and the trace waterfall work through a view. The view
  must return exactly the ClickStack columns.
- **Degraded:**
  - autocomplete and value pickers use `mergeTreeIndex` and refuse views,
    so they fall back to scans. On the cold side that is an S3 scan.
    Mitigation: a small MergeTree of (ServiceName, SpanName, attribute keys)
    per hour, maintained by the compactor or by an MV on the hot tier and
    kept for the full retention.
  - rollup acceleration (MaterializedView detection) doesn't apply.
  - skip indexes don't exist on the cold side.
- **Unknown:**
  - whether HyperDX's generated SQL pushes the Timestamp predicate through
    `UNION ALL` into `icebergS3` pruning. It should: predicate pushdown
    through a view is standard in ClickHouse [E]. P1 measures it.
  - how HyperDX's trace panel chooses its time window.

**Risks:**

- **Cross-tier consistency:** a hot/cold overlap or gap at the boundary
  (above).
- **Deduplication in two places:** the consumer's projection check and the
  compactor's content-key set. Both must agree. The soak's audit can check
  both.
- **ClickHouse Iceberg bugs** (option 3).
- **Cold latency for unindexed queries** is seconds to minutes. Users will
  notice when a HyperDX query crosses the boundary.

## 3. Prior art

### 3.1 What each system does on object storage, and what it still runs centrally

| System | Indexes or aggregates on object storage | Central infrastructure it still needs | Borrow directly |
|---|---|---|---|
| **Grafana Tempo 3.x** (vParquet4 default in 3.0; vParquet5 default from 3.1, rc on GitHub) | A block is `meta.json`, **sharded bloom files** `bloom-N` (bits-and-blooms; defaults FP 0.01, shard 100 KiB), an **index** of the max trace id per row group (traces sorted by id), and `data.parquet` with 100 MB row groups. One row per trace, nested. Trace by id: shard key → one bloom shard GET per block (cached) → index → row group. TraceQL: the query frontend shards blocks into jobs (100–200 MB per job recommended); bloom, footer and page caches hit about 90%. Compaction and retention run in the backend scheduler and workers. [D: Tempo source `tempodb/encoding/common/config.go`, `vparquet5/block_findtracebyid.go`, `vparquet5/index.go`; Tempo docs "schema", "backend_search", "architecture"; release v3.1.0-rc.1] | Kafka (microservices mode), block-builders, live-stores, queriers, frontends, scheduler, caches (memcached). A **blocklist** polled from object storage, with no metadata DB. | The **"sorted by trace id + max-id-per-row-group index"** trick, and **sharded, individually cacheable filters keyed by a trace-id prefix**. The maplet in P2 is the same idea with values and merging. Dedicated columns is the same idea as ClickStack's materialized columns. |
| **Grafana Loki 3.x** (3.7.8 latest) | **Blooms:** added in 3.0 for free text; **pivoted in 3.3 to structured metadata only**, with a breaking block format V3; the bloom compactor was replaced by a planner and builders. Still **experimental in main** (Sept 2026): "intended for users who are ingesting more than 75TB of logs a month", no SLA, not in single-binary mode. **New architecture** (GA with Grafana 13, April 2026): Kafka write path at RF-1, columnar **DataObjects** ("THOR" container, sections), separate **index objects** with per-column blooms over structured-metadata columns, label **postings** bitmaps, stream stats, and a TOC/metastore. "20× less data scanned, 10× faster" on aggregate queries. [D: Loki 3.3 blog and notes; `docs/sources/operations/bloom-filters.md`; `pkg/dataobj/README.md`, `pkg/dataobj/index/*`; InfoQ 2026-04-23] | Kafka, ingesters and consumers, index builders, query scheduler, caches. | The **lesson**: free-text blooms at the edge don't pay; blooms on key=value metadata do. The **shape**: an index object separate from data objects, holding per-section postings + blooms + a TOC. That is what our compactor's hourly index objects should look like. |
| **Quickwit** 0.9 (Datadog since Jan 2025; Apache-2.0 since 0.9.0) | **Splits**: a tantivy inverted index + columnar fast fields + doc store + **hotcache** (<0.1% of the split, about 10 MB for a 10 M-doc split; opens a split in <60 ms). One hotcache GET, then warmup range GETs. Merge policy: 10 splits up to 10 M docs. [D: quickwit.io architecture, quickwit-101, metastore config; git tags] | A **metastore**: PostgreSQL for clusters, or a single-writer file on S3 (polled). Indexers and control plane. | The **hotcache pattern**: everything needed to plan reads of a big file in one small, cacheable tail. Tantivy, if cold free-text search is needed. |
| **Datadog Husky** | Small sorted columnar **fragments** in S3. Metadata (zone maps, value lists, **pruning regexes from automata**) in **FoundationDB**. **Size-tiered + locality compaction** within time buckets, with an atomic swap in FDB; ~1 M rows per output fragment; one GET per input fragment, streamed. Query side: fragment result caches (~80% hits) and predicate/posting caches; "3.4% of queries scan real data, 0.4% hit blob storage". [D: Datadog blog posts on compaction and on the query path (2025-10-01)] | FoundationDB, writers, compactors, readers, caches. | **Locality compaction** (§2.5 L1/L2) and **per-file pruning regexes/value lists** in the table metadata (Iceberg manifests' min/max is the weak form). |
| **turbopuffer** | Namespace = a prefix; WAL entries (≤1 per second per namespace), asynchronous indexers build ANN/BM25 indexes laid out for **few round trips**; cold p50 874 ms for 1 M docs (3–4 round trips), warm p50 14 ms; **no metadata DB**, compare-and-swap on the commit point. [D: turbopuffer docs "architecture"] | Query and indexer nodes with NVMe caches only. | **Design every index for a bounded number of dependent round trips**, not for bytes. The maplet is one round trip after a cached header. And **state only on S3 with CAS**, as we do. |
| **Honeycomb Retriever** | Per-column files per segment (rolled at 1 M events, 1 GB or 12 h); old segments go to S3 and are queried by **Lambda** fan-out returning partial aggregates. [D: Honeycomb talks/notes] | Kafka, Retriever nodes for recent data. | The **hot local / cold S3 + serverless fan-out** split is our option 5, with ClickHouse as the hot tier. |
| **VictoriaLogs** | Parts on **local disk**: per-block column files, **word blooms** sharded with values, and a two-level index. S3 only for backups. [D: VictoriaMetrics blog, FAQ] | Stateful storage nodes. | Word blooms per block work when blocks are **local and cheap to probe**. On S3 they don't (§2.1). |
| **OpenObserve** | Parquet on S3 plus a **tantivy `.ttv` index per Parquet file**, built at compaction, and blooms for high-cardinality exact-match fields; DataFusion scans. [D: OpenObserve docs; PR #4733] | A metadata DB and a coordinator for clusters, ingesters with a WAL [E]. | **Index built at compaction, stored beside the file.** That is our P1/P2 placement. |
| **Parseable** | Parquet on S3 from stage-and-forward ingest; DataFusion; **on-demand indexing** of chosen chunks; NVMe and memory caches. [D: parseable.com docs] | Ingest and query nodes; metadata in object storage. | **Index lazily**: build the cold log text index for the hours someone actually searches. |
| **Axiom** | Proprietary columnar **immutable blocks** in S3; **ephemeral serverless query workers**; separate MetricsDB. [D: axiom.co docs] | The managed service. | Confirms that stateless, per-query workers over immutable blocks are viable for cold data. |
| **ClickHouse on Parquet** (no MergeTree) | `s3()` / `icebergS3`: row-group min/max, **page-index**, **bloom** and **dictionary** pushdown (`input_format_parquet_{filter,page_filter,bloom_filter,dictionary_filter}_push_down`, all on). Bloom pushdown default since May 2025 (PR #80058). Native v3 reader; **Parquet metadata cache** keyed by path+ETag (`use_parquet_metadata_cache`, 26.3). **Filesystem cache** for S3 reads keyed by path+ETag (`filesystem_cache_name`, `enable_filesystem_cache`). Writes Parquet blooms (`output_format_parquet_write_bloom_filter`, ~2 B/row/column) and a page index. [D: ClickHouse format settings docs, S3 engine docs, PRs #80058, #98140, #71681; parquet blog 2025-05-07] | The ClickHouse server itself. | The **query engine for the cold tier**, unchanged. `s3('…/{k1,k2}')` with full keys does no LIST [M: ../otap-rs], so an external index can hand ClickHouse an exact file list. |

### 3.2 Loki's bloom history, in short

1. **3.0:** blooms over log lines for free-text filters, built by a bloom
   compactor, served by a bloom gateway.
2. **3.3:** the builder became planner + builders. Blooms now index
   **structured metadata** key and key=value hashes, also combined with
   the chunk id. The block format changed to V3, and old blocks are
   obsolete. [D: Loki 3.3 release notes and blog]
3. **Main, September 2026:** the bloom docs still say experimental, >75
   TB/month, and "bloom filters make up <1% of the raw structured metadata
   size". The dataobj index path builds per-column blooms and bitmap
   postings for structured-metadata columns inside **index objects**.
   [D: Loki repository]

"Kept, but only for metadata and only at very large scale; the future is
columnar data objects with separate index objects." I found no statement
that blooms are deprecated or removed [D: docs checked 2026-09].

### 3.3 Filters and maplets for immutable per-object and merged indexes

| Structure | Space | Build | Merge | Values | Deletes | Fit |
|---|---|---|---|---|---|---|
| Split-block Bloom (Parquet native) | ~10 bits/key at 1% | streaming | OR (same size) | no | no | Already written, and read by ClickHouse. Per row group only. |
| **Binary fuse** 8 / 16 | ≈9 / 18 bits/key; FP 0.39% / 0.0015%; within 13% of the lower bound [D: Graf & Lemire 2022] | static; needs all keys; >2× faster than xor | **no**: rebuild from keys | no; a fuse/xor **retrieval** variant stores values but returns garbage for absent keys [D: arXiv 2312.13541] | no | Per immutable object, if a raw-tier filter is wanted. Crates: `xorf` (Rust) [E]; Go FastFilter/xorfilter [D]. |
| **Ribbon** | about 7 bits/key at 1% (RocksDB `NewRibbonFilterPolicy(9.9)`) [D: RocksDB blog; Dillinger & Walzer 2021] | static, slower build | no | retrieval variant (bumped ribbon) | no | Smallest static filter; no merge. |
| **Quotient filter** (RSQF / CQF) | r + ~2.1 bits per slot at high load (r = remainder bits) [E: from the RSQF design] | incremental | **yes**, a linear merge in hash order | extra remainder bits or counters → **maplet** | yes | Dynamic, mergeable. Rust `qfilter` 0.2.5: insert, remove, **merge**, resize, serde [D: docs.rs]. C++ CQF (`splatlab/cqf`) [E]. |
| **Maplet** (abstraction) | O(log 1/ε + v) bits per key [D: arXiv 2510.05518] | insert, delete (if values form a group), query, **merge**, resize | yes, inherited from perfect-hashing filters (QF, cuckoo, Morton) | yes: `m[k] = M[k] ⊕ (a few others)`, one-sided error, with P(ℓ ≥ L) ≤ ε^L | yes | **The right abstraction for trace_id → {file, row group}.** Bender, Conway, Farach-Colton, Johnson, Pandey, "Time To Replace Your Filter: How Maplets Simplify System Design", arXiv 2510.05518, Oct 2025. Rust: `mappy-core` 0.3.2 (Aug 2026, MIT, pre-1.0) [D: docs.rs]. |

**For our case** (built once, merged hourly → daily, never updated,
dropped by whole time ranges), the simplest maplet is **static**:

- **Layout.**
  - 2^p buckets by the top p bits of hash(trace_id).
  - Each bucket holds sorted (remainder r bits, value v bits) pairs.
  - A bucket-offset table sits in a small header.
  - It is a one-level static quotient filter.
- **Lookup:** GET the header (cached) plus **one range GET** of a few KB.
- **Merge:** a streaming merge per bucket, the QF merge.
- **Deletes:** none needed.
- **False matches:** about n / 2^(p+r) per lookup. With p + r = log2(n) +
  12, that is about 1/4096 per maplet, so about 2% of 90-day lookups read
  one extra file.
- Use `qfilter` or `mappy-core` as references and for tests, not as the
  on-disk format: neither is designed for range-GET addressing [E].

## 4. Index and cube sizes at the mid scenario, together [E]

| Item | Per day | 90 days | Relative |
|---|---|---|---|
| Compacted lake data (spans, logs, metrics) | ≈4.5 TB | ≈400 TB | – |
| Iceberg metadata (manifests) | ≈25 MB | 2 GB | – |
| Trace maplet (P2): 108 M traces/h (assumes 20 spans per trace), ≈3 files per trace → 324 M entries/h × ≈27 bits | ≈27 GB | ≈2.4 TB | ≈1% of span bytes |
| Parquet TraceId bloom in lake files | ≈12% of trace bytes [M: edge ratio] | – | Drop once P2 works |
| Span metrics as exponential-histogram points (option 2a) | ≈15 GB in ClickHouse | ≈1.3 TB | +0.4% of metric points |
| Autocomplete helper table (service, span name, attribute keys per hour) | <1 GB | <100 GB | – |
| Content-key dedup set (compactor) | ≈250 MB | kept only for the dedup window | – |

The assumption most worth measuring is **spans per (trace, file)**. It
sets the maplet's size linearly.

## 5. Prototype plan

The box is shared with CPU-heavy benchmarks, so the prototypes should run
on reduced data: one hour at 1/100 scale, and 24 hours at 1/1000.
**Scale the results; don't time them against a loaded machine.**

### P1: hybrid cold tier (options 5 + 3 + 2a)

**Build:**

1. **`lakecompact`**, a binary in the otap-rs workspace.
   - It reuses `src/consumer/coord.rs` (lease, CAS'd checkpoint, fencing)
     and the lane discovery.
   - For each (signal, 5-minute window):
     - read committed slots;
     - drop content-key duplicates (CAS'd hourly key set);
     - sort by ClickStack's key;
     - write L1 files.
   - Per hour, after the zombie bound: k-way merge into L2 files with the
     page index, a TraceId bloom and zstd. Late slots become append files.
   - **Iceberg v2 metadata:**
     - the partition spec is `hour(Timestamp)`;
     - variant B adds `bucket(16, TraceId)`;
     - manifests carry column min/max;
     - commit with `PUT If-None-Match` of
       `metadata/v{N:020d}.metadata.json`;
     - use iceberg-rust for manifest encoding if it allows writing without a
       catalog, or write the Avro manifests directly [E].
   - GC rules 1–6 (§2.0) go into `consume gc`.
2. **ClickHouse objects.**
   - An `icebergS3` table per signal with ClickStack's exact columns.
   - The `*_all` views (§2.5).
   - A filesystem cache and `use_parquet_metadata_cache`.
3. **Span metrics.**
   - On the Go edge, the spanmetrics connector with exponential histograms,
     delta temporality, and dimensions (service, span name, kind, status)
     into layout B.
   - On the Rust edge, the same aggregation inside the exporter, as
     metrics objects on the metrics lane.
   - The Go connector alone is enough to validate the dashboards.
4. **HyperDX:** sources repointed at the `*_all` views. Rebuild the stock
   service dashboards as metric charts on span metrics.

**Measure:**

- **Compactor.**
  - CPU per row per level, bytes in and out;
  - compression against raw edge objects and against ClickHouse's stored
    bytes;
  - files per hour, and service-range width per file;
  - S3 GET and PUT counts.
- **Exactly-once into the lake.** Re-run the consumer soak's chaos (edge
  and worker kills, ambiguous PUTs) with the compactor running, and extend
  `consumer_soak_check.py` to audit the lake. The target is 0 missing,
  0 duplicated and 0 uncommitted, with GC running.
- **Cold queries**, through ClickHouse directly and through HyperDX:
  - trace by id (1 h, 24 h windows);
  - service + status search (1 h, 24 h);
  - log search with a service filter;
  - a HyperDX events chart over 7 days on the cold side.

  For each: latency cold and warm (with and without the filesystem and
  metadata caches), S3 GETs (proxy log), and files and row groups read
  against files pruned.
- **Variant A against variant B** (with or without `bucket(16, TraceId)`)
  for trace lookup and service search. This decides whether P2 is needed.
- **Correctness:**
  - the same queries on the hot and the cold side over one overlap day
    return identical rows;
  - the UTC and time-zone pruning bug (#119173) is checked explicitly.
- **HyperDX feature matrix** through the views:
  - search, the trace waterfall, the pattern view, charts, alerts,
    autocomplete;
  - for each, "works / slow / broken", and the SQL it sent.

**Success criteria (P1):**

| Criterion | Target |
|---|---|
| Lake exactly-once under the chaos soak | 0 missing, 0 duplicate, 0 uncommitted |
| Compactor CPU, scaled to the mid scenario | ≤ 25 vCPU, stateless, restartable at any point |
| Lake bytes for 90 days | ≤ ClickHouse one-copy S3 cold bytes for the same retention |
| Service + status search, 24 h cold | p50 ≤ 4 s cold cache, ≤ 500 GETs |
| Trace by id, 1 h window cold | p50 ≤ 3 s (variant A or B) |
| 30-day service dashboard from span metrics | ≤ 1 s, in HyperDX |
| Hot ∪ cold equality on the overlap day | identical row sets |
| HyperDX | search, trace view and charts work through the views; the degraded features are listed with their SQL |

**Stop or rethink if:**

- a 24-hour cold search needs >5k GETs after locality compaction;
- ClickHouse doesn't push `Timestamp` through the view into Iceberg
  pruning;
- or the compactor can't keep deduplication exact without reading
  ClickHouse.

### P2: trace_id → file maplet (option 1, placed in the compactor)

**Build:**

- The static maplet format (§3.3) with values = (file ordinal in the hour,
  row group ordinal).
- Built by `lakecompact` at L2; merged daily; referenced from Iceberg
  snapshot properties, or from a sibling create-only index log, so GC rule 2
  covers it.
- A lookup path:
  - `lake-trace <id> [--days N]` returns the exact file keys and row groups;
  - an HTTP endpoint for a HyperDX patch or a Grafana data link;
  - ClickHouse then reads `s3('…/{k1,k2}')` with `WHERE TraceId = …`,
    which does no LIST [M: ../otap-rs].

**Measure:**

- entries per hour (spans per trace per file, the key unknown);
- bits per entry;
- build and merge CPU;
- false-match rate against the theory;
- lookup GETs and latency for 1, 7 and 30 days;
- the same against Tempo-style per-file bloom shards, as a baseline.

**Success criteria (P2):**

| Criterion | Target |
|---|---|
| 30-day cold trace lookup | p50 ≤ 1 s, ≤ 50 GETs |
| Maplet size | ≤ 2% of the trace data it indexes |
| False matches | within 2× of n / 2^(p+r) |
| Build + daily merge CPU | ≤ 10% of the compactor's |

**Not in either prototype, deliberately:**

- edge token blooms and edge roaring postings (§2.1);
- n-grams;
- DataSketches cubes (option 2b);
- tantivy splits for cold free text;
- DuckLake.

Each should be revisited only with a concrete query that P1 can't serve.

## Sources

Spike documents: [../awss3/README.md](../awss3/README.md),
[../otap-rs/README.md](../otap-rs/README.md),
[../metrics-layout/README.md](../metrics-layout/README.md),
[../bench/clean/calc.py](../bench/clean/calc.py).

**Grafana Tempo**

- Parquet block format: <https://grafana.com/docs/tempo/latest/configuration/parquet/>
- Parquet schema: <https://grafana.com/docs/tempo/latest/operations/schema/>
- Architecture: <https://grafana.com/docs/tempo/latest/introduction/architecture/>
- Tune search performance: <https://grafana.com/docs/tempo/latest/operations/backend_search/>
- Caching: <https://grafana.com/docs/tempo/latest/operations/caching/>
- v3.1.0-rc.1 release: <https://github.com/grafana/tempo/releases/tag/v3.1.0-rc.1>
- 2.9 release notes (vParquet5 introduced): <https://grafana.com/docs/tempo/latest/release-notes/version-2/v2-9/>
- Bloom shards discussion: <https://github.com/grafana/tempo/discussions/3059>
- Source, read from `main` on 2026-09-25: `tempodb/encoding/common/config.go`,
  `tempodb/encoding/vparquet5/{block_findtracebyid.go,index.go}`

**Grafana Loki**

- 3.3 release notes: <https://grafana.com/docs/loki/latest/release-notes/v3-3/>
- 3.3 blog (blooms for structured metadata): <https://grafana.com/blog/grafana-loki-3-3-release-faster-query-results-via-blooms-for-structured-metadata/>
- Bloom filters (experimental): <https://grafana.com/docs/loki/latest/operations/bloom-filters/>
- Structured metadata: <https://grafana.com/docs/loki/latest/get-started/labels/structured-metadata/>
- InfoQ on the new architecture (2026-04-23): <https://www.infoq.com/news/2026/04/grafana-loki-ai-agents/>
- Removal of the bloom compactor: <https://github.com/grafana/loki/commit/b75eacc288c52737e41ba9932c06409c643e2e5c>
- Source, read from `main` on 2026-09-25: `pkg/dataobj/README.md`, `pkg/dataobj/index/`

**Quickwit**

- Joins Datadog: <https://quickwit.io/blog/quickwit-joins-datadog>
- Datadog acquires Quickwit: <https://www.datadoghq.com/blog/datadog-acquires-quickwit/>
- Architecture: <https://quickwit.io/docs/overview/architecture>
- Quickwit 101: <https://quickwit.io/blog/quickwit-101>
- Metastore configuration: <https://quickwit.io/docs/configuration/metastore-config>
- Git tags v0.8.2 (AGPL-3.0), v0.9.0 (2026-07-25, Apache-2.0) and v0.9.1
  (2026-09-22), from <https://github.com/quickwit-oss/quickwit>

**Iceberg, Delta, DuckLake**

- Puffin spec: <https://iceberg.apache.org/puffin-spec/> and <https://github.com/apache/iceberg/blob/main/format/puffin-spec.md>
- Trino Iceberg connector: <https://trino.io/docs/current/connector/iceberg.html>
- DuckDB-Iceberg in v1.5.3: <https://duckdb.org/2026/05/29/new-iceberg-features>
- Boring Catalog: <https://github.com/boringdata/boring-catalog>
- delta-rs removal of the DynamoDB log store: <https://github.com/delta-io/delta-rs/discussions/4482>
- Rust Delta Kernel in ClickHouse: <https://delta.io/blog/2026-05-18-integrating-the-rust-delta-kernel-into-clickhouse/>
- DuckLake v1.0: <https://ducklake.select/2026/04/13/ducklake-10/>
- DuckLake FAQ: <https://ducklake.select/faq>

**ClickHouse**

- Iceberg table function: <https://clickhouse.com/docs/reference/functions/table-functions/iceberg>
- Iceberg time-transform pruning: <https://github.com/ClickHouse/ClickHouse/pull/72044>
- Iceberg bucket-transform pruning: <https://github.com/ClickHouse/ClickHouse/pull/79262>
- Time-zone pruning bug: <https://github.com/ClickHouse/ClickHouse/issues/119173>
- Version-hint data loss: <https://github.com/ClickHouse/ClickHouse/issues/114194>
- OPTIMIZE bugs: <https://github.com/ClickHouse/ClickHouse/issues/120164>, <https://github.com/ClickHouse/ClickHouse/issues/120165>
- Format settings: <https://clickhouse.com/docs/operations/settings/formats>
- S3 table engine: <https://clickhouse.com/docs/engines/table-engines/integrations/s3>
- Bloom filters on read by default: <https://github.com/ClickHouse/ClickHouse/pull/80058>
- Parquet metadata cache v2: <https://github.com/ClickHouse/ClickHouse/pull/98140>
- Writing Parquet bloom filters: <https://github.com/ClickHouse/ClickHouse/pull/71681>
- UUID statistics bug: <https://github.com/ClickHouse/ClickHouse/issues/118371>
- FLBA bloom bug: <https://github.com/ClickHouse/ClickHouse/issues/120986>
- ClickHouse and Parquet (2025-05-07): <https://clickhouse.com/blog/clickhouse-and-parquet-a-foundation-for-fast-lakehouse-analytics>

**HyperDX / ClickStack**

- <https://github.com/ClickHouse/clickstack>
- The spike's own HyperDX findings in ../metrics-layout.

**Filters and maplets**

- Maplets paper: <https://arxiv.org/abs/2510.05518>
- mappy-core: <https://docs.rs/mappy-core/latest/mappy_core/>
- qfilter: <https://github.com/arthurprs/qfilter>
- Binary fuse filters: <https://arxiv.org/abs/2201.01174>
- Ribbon filter paper: <https://arxiv.org/abs/2103.02515>
- Ribbon filter in RocksDB: <https://rocksdb.org/blog/2021/12/29/ribbon-filter.html>
- Fuse XORier lookup tables: <https://arxiv.org/pdf/2312.13541>

**Sketches**

- DDSketch: <https://arxiv.org/abs/1908.10693>
- Worst-case comparison of relative-error quantile algorithms: <https://arxiv.org/pdf/2102.09299>
- Apache DataSketches for Rust: <https://github.com/apache/datasketches-rust>
- spanmetrics connector: <https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/connector/spanmetricsconnector/README.md>
- OTel metrics data model: <https://opentelemetry.io/docs/specs/otel/metrics/data-model/>

**Other prior art**

- Husky compaction: <https://www.datadoghq.com/blog/engineering/husky-storage-compaction/>
- Husky query architecture: <https://www.datadoghq.com/blog/engineering/husky-query-architecture/>
- Introducing Husky: <https://www.datadoghq.com/blog/engineering/introducing-husky/>
- turbopuffer architecture: <https://turbopuffer.com/docs/architecture>
- Why Honeycomb built its own column store: <https://www.honeycomb.io/resources/why-we-built-our-own-distributed-column-store>
- Notes on Honeycomb's Retriever: <https://imfeld.dev/notes/honeycomb_retriever_database>
- VictoriaLogs columnar storage: <https://victoriametrics.com/blog/victorialogs-internals-columnar-storage-on-disk/>
- VictoriaLogs FAQ: <https://docs.victoriametrics.com/victorialogs/faq/>
- OpenObserve architecture: <https://openobserve.ai/docs/architecture/>
- OpenObserve tantivy index: <https://openobserve.ai/docs/user-guide/performance/tantivy-index/>
- Parseable architecture: <https://www.parseable.com/docs/architecture>
- Axiom architecture: <https://axiom.co/docs/platform-overview/architecture>
