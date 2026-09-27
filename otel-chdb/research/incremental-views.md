# research: incremental views (DBSP / Feldera) and distributed-protocol frameworks (Hydro) for central

A research note. The question from the owner: could something like **Feldera
(DBSP)** and **Hydro** replace **Keeper and ClickHouse** for maintaining the
central views, in a scale-out way, with the same schema or a better one?
Nothing was built or benchmarked for this note. It builds on:

- [../DECISIONS.md](../DECISIONS.md): D2/D3 (transfer format, commit
  protocol), D7 (layout B), D8–D13 (the consumer, replicated central), D14–D15
  (tiers, downsampling), D17 (lake), §3 (sizing);
- [../lake/DESIGN.md](../lake/DESIGN.md): the object-store-native alternative;
- [../hyperdx/README.md](../hyperdx/README.md): what HyperDX sends and needs;
- [../metrics-layout/README.md](../metrics-layout/README.md) and
  [../otap-rs/README.md](../otap-rs/README.md) §Consumer.

Labels, as in lake/DESIGN.md: **[D]** from a cited source (dated where it
matters; web sources read 2026-09-27), **[E]** my estimate, **[M]** measured,
quoted from the spike's READMEs. The hands-on probe was **not run** (§8).

## 1. Summary and recommendation

**Where central's money goes** (mid scenario, calculator [D: DECISIONS §3]):
91 vCPU = per replica 5.7 insert + 13.3 merge + 14.2 headroom, plus 24.8
query; 992 TB. **Derived views are a rounding error in that**: the D15
rollup costs 0.41–1.71 µs per raw point [M], about **0.5–1.5 vCPU** at
1.27 M points/s; the key-value rollups and the picker helper are
insert-time MVs of the same kind [E: ≤ 1 vCPU]. Together **≈ 2–3% of
central** [E]. The other 97% is storing, merging and searching raw spans,
logs and points for 90 days, which is what HyperDX queries, ad hoc, in
ClickHouse SQL.

**What that means:**

1. **An IVM engine can't replace ClickHouse here.** Feldera and its peers
   maintain *declared* queries; HyperDX's search, filters, waterfall and
   pickers are ad-hoc SQL over raw rows. Feldera's own ad-hoc path is
   DataFusion in batch mode over *materialized* views held in the
   pipeline's storage [D: Feldera ad-hoc docs] — a second columnar store,
   not a search engine, and not ClickHouse SQL.
2. **Our views are the easy case for ClickHouse.** Every view on the list
   (5-minute rollups, RED metrics, key-value rollups, the metric-name
   helper, trace summaries) is an **append-only, time-bucketed, mergeable
   aggregate**. ClickHouse maintains those incrementally at insert time with
   no operator state (MV → Aggregating/SummingMergeTree), and a partial
   aggregate per insert merges exactly. DBSP's strengths — joins, distinct,
   retractions, recursion, with provably equal-to-batch results — are
   mostly not needed. The one view that *is* a real incremental join, series
   ⋈ points, **must not be materialized**: materializing it is layout A,
   263 vCPU against 91 [D: D7].
3. **The real gap is exactly-once of derived rows, not incrementality.**
   An MV's target part is not committed atomically with its source part
   (D11: a failing ledger MV left 8,000 rows in the target and none in the
   ledger [M]). That is cheap to close with the tools we have (option G).
4. **Keeper is replaceable, but not by Hydro.** What Keeper does for us is
   ReplicatedMergeTree's replication log, part catalog and insert-dedup
   hashes, plus `ON CLUSTER` DDL. Hydro is an alpha framework for *writing*
   protocols (hydro_lang 0.17.0-alpha.5, 2026-09-21; stable 0.16.0 [D:
   crates.io]); it doesn't give ClickHouse a replication log. Our own
   control plane already runs on S3 CAS. The cheap Keeper-less shape uses
   what we already built: **the lanes are a durable, ordered, content-keyed
   log, so each replica can consume them itself** (option E), as Honeycomb's
   Retriever replicas each consume Kafka [D: Honeycomb].

**The options, briefly:**

| # | Option | Verdict |
|---|---|---|
| A | Feldera for derived views beside ClickHouse | **Not now.** Works technically, but replaces ~2 vCPU of MVs with a 16–32 vCPU stateful service [E]; fault tolerance and checkpoints are **Enterprise-only**; no ClickHouse sink; its S3 source doesn't know slots, epochs, tombstones or content keys. Keep as the answer if non-mergeable views (SLO joins, sessionization, retractions) become a requirement. |
| B | Feldera as ingest + view layer writing Iceberg/Delta, separate query engine | **No.** Raw pass-through buys nothing (no sort/locality compaction, so the lake compactor is still needed); Iceberg output is experimental; ClickHouse hot tier still needed for HyperDX. Collapses into D17 + A. |
| C | Hydro-built coordination/consumer replacing Keeper-dependent parts | **No as infrastructure; watch as a technique.** Alpha, no durable-state facility found, and the only Keeper-dependent part is inside ClickHouse. Our coordinator is already S3-CAS and Quint-checked. |
| D | Keeper-less ClickHouse family | **Only ClickHouse Cloud** (SharedMergeTree is cloud-only [D]); other shared-data engines swap Keeper for their own consensus and break HyperDX. Out of scope for Nutanix. |
| E | **Source-replicated central**: N non-replicated ClickHouse servers, each fed by its own consumer group from the lanes | **Recommended to prototype.** Removes Keeper, the 20 s Keeper slack (D9), `--sync-replica` and quorum questions; about cost-neutral [E]; reuses the consumer, content keys and count check unchanged. |
| F | Embedded `dbsp` crate in otap-rs (owner's addition) | **Best DBSP route if one is ever needed, not needed now.** The open crate (MIT OR Apache-2.0) has spill-to-disk storage, a checkpoint API and multi-worker/multi-host exchange; the S3 checkpoint sync is a trait we'd implement ourselves. But per-worker circuits only help mergeable views (which don't need DBSP), and keyed global state conflicts with lease-based lane rebalancing. |
| G | Closed-window audit and recompute of derived tables in the consumer | **Recommended, small.** Makes MV-derived rows exactly-once the way D11 makes raw rows exactly-once: count-check each closed window against its source and recompute the window idempotently when they differ. |

**Recommendation.**

1. **Keep ClickHouse as the store and query engine, and keep derived views
   as insert-time MVs.** Add **G** (window audit + idempotent recompute) when
   D15 is built.
2. **Prototype E** as the Keeper-less central: two non-replicated servers,
   two consumer groups, GC at the minimum of both groups' positions. It is
   the cheapest path to "no Keeper", and it removes the replicated-central
   failure modes the soaks found (D9, D13). Long term, D17's lake remains the
   object-store-native shape for cold data; E and D17 compose.
3. **Don't adopt Hydro or Feldera now.** Revisit **F** (embedded dbsp), or A
   if the owner is willing to buy Enterprise, when a view appears that is not
   a mergeable aggregate over a time bucket. §5 lists what a prototype of each
   would prove.

## 2. What central does today, and what Keeper does

| Function | Who does it now | Evidence |
|---|---|---|
| Ingest exactly-once from the lanes | consumer: leases, CAS'd checkpoints, time bound + server fence, count check and repair by content key | D8–D11; soaks exactly-once [M] |
| Replication | ReplicatedMergeTree, 2 replicas, **Keeper** (replication log, part catalog, fetches) | D13 |
| Dedup | consumer's projection check (`by_content`); replicated block hashes in **Keeper** only cover exact retries | D10, D11 |
| Read-your-writes for the check | `SYSTEM SYNC REPLICA … LIGHTWEIGHT` (4 **Keeper** transactions, p50 24 ms) | D13 [M] |
| Views: layout-B compatibility | plain views, `ANY LEFT JOIN` at query time | D7 |
| Rollups (D15), key-value rollups, metric-name helper | insert-time MVs into Aggregating/Summing/ReplacingMergeTree | D15; `otap-rs/sql/otel_*.sql`; hyperdx §Picker |
| Search, filters, waterfall, dashboards, alerts | ClickHouse, HyperDX's generated SQL | hyperdx/README |
| Retention and tiering | TTL MOVE and drop-only TTL by day | D14 |
| Coordination of consumers, GC | S3 CAS objects (lease, ckpt, gc.json) — **not Keeper** | D8, D12 |
| DDL on both replicas | `--no-ddl` + manual / `ON CLUSTER` (**Keeper**'s distributed DDL queue) | D13 |

**What Keeper costs us beyond the three nodes** [D: D9, D13]: a commit can
land up to the session timeout after the statement started (19.0 s past a
10 s limit [M]), which forced margin and slack to 20 s and TTL to 75 s; error
answers (`TIMEOUT_EXCEEDED`, `KEEPER_EXCEPTION`, `TABLE_IS_READ_ONLY`) must be
waited out; a Keeper quorum loss stops inserts on both replicas; a replica
lost for good stalls its tables' checks until dropped from Keeper.

## 3. The views, classified

State and output at the mid scenario (38 M active series, 1.27 M points/s,
600k spans/s, 200k logs/s), all [E]. "Mergeable" means a partial result per
insert combines exactly (sum, count, min, max, last-by-time, HLL/sketch
states), so ClickHouse needs no operator state.

| View | Shape | Mergeable? | DBSP state if maintained there | Output rate | Today | DBSP fit |
|---|---|---|---|---|---|---|
| Layout-B compat views (series ⋈ points) | join, late series | no (join) | series maps uncompressed, 38 M × 1–2 KB ≈ **40–75 GB** | 1.27 M **wide** rows/s | query-time view | **Bad**: materializing it is layout A (263 vCPU, 1,117 TB [D: D7]) |
| D15 5-min rollup per series (min/max/sum/count/last) | aggregate, time bucket | **yes** | open windows ≤ 3 × 38 M × ~64 B ≈ **7 GB** with 10-min lateness | 38 M / 300 s ≈ 127k rows/s | MV → AggregatingMergeTree, 0.41–1.71 µs/pt [M] | fits, not needed |
| RED / spanmetrics per (service, span, kind, status, minute) | aggregate + histogram | **yes** (exp. histograms merge at aligned scale) | ~10⁴–10⁵ keys, **< 1 GB** | ~10³ rows/s | proposed at the edge (lake option 2a) or an MV | fits, not needed |
| Trace summary (trace_id → services, start, end, error, span count) | group by trace_id across lanes | **yes** (min, max, `groupUniqArray`, `countIf`) | ~30k traces/s (20 spans/trace) × 300 s lateness × ~150 B ≈ **1.4 GB** | ~30k rows/s after `emit_final` | not built; MV → AggregatingMergeTree by trace_id would do it | fits, not needed |
| HyperDX key/value rollups (`*_kv_rollup_15m`) | distinct (column, key, value) per 15 min, with counts | **yes** (sum) | 10⁶–10⁷ keys × ~100 B ≈ **0.1–1 GB** | small | MV → SummingMergeTree (consumer DDL) | fits, not needed |
| Metric-name helper (type, hour, metric, service) | distinct | **yes** | ~10⁵ rows | tiny | MV → ReplacingMergeTree, 16 ms [M] | fits, not needed (HyperDX can't use it anyway [M]) |
| Retention-windowed aggregates ("active series last hour", SLO burn over 30 days) | sliding window over buckets | **yes** over per-bucket states; exact distinct over a long window is not | 10⁷–10⁸ keys for exact distinct | small | query-time over bucket states | fits where exact and long-lived |
| Search, filters, waterfall, pickers, dashboards over raw rows | ad-hoc | – | – | – | ClickHouse | **None**: not a declared view |

**Where DBSP would earn its keep** [E]: views with **retractions or late
corrections that must be exact** (e.g. a span metric recomputed when a late
parent changes a root's attribution), **joins against slowly changing
dimensions** (service ownership, deploy markers, SLO definitions),
**sessionization**, and **exact distincts over long windows**. None of these is
on the current list.

## 4. The options

### A. Feldera for derived views beside ClickHouse

**How.** A Feldera pipeline (SQL: `CREATE TABLE points (…) WITH
('append_only')`, `LATENESS` on `TimeUnix`, views as `GROUP BY` over
time buckets) reads the edge objects and writes the derived tables
to ClickHouse; raw ingest stays with the consumer.

**Facts that decide it** [D]:

- **License split.** The repo and crates are MIT (Feldera) [D: GitHub];
  "Checkpoints & fault tolerance features are only available in Feldera
  Enterprise Edition" [D: fault-tolerance docs]; S3 checkpoint sync and
  S3-backed state ("100+ TB") are Enterprise [D: blog 2025-02-04,
  checkpoint-sync docs]; multi-pod pipelines are an Enterprise **preview**
  [D: enterprise docs]. The open-source edition is single-node without crash
  recovery.
- **Release cadence:** v0.356.0 on 2026-09-26, v0.350.0 on 2026-09-17 —
  several 0.x releases a week [D: GitHub releases, crates.io].
- **Input:** the S3 connector reads a `key` or a `prefix` "in alphabetical
  order, the order returned by S3's ListObjectsV2"; it supports all three
  fault-tolerance modes [D: S3 connector docs, FT docs]. I found no
  documented continuous-follow mode, and nothing that could express our
  lanes: per-lane epochs, tombstones, "a slot may hold another batch",
  content-key copies across epochs [D: docs 2026-09-27].
- **Exactly-once** is checkpoint (default every 60 s) + an input journal +
  replay, and replay output already sent "is discarded"; non-FT outputs may
  get duplicates [D: FT docs].
- **Output:** sinks are HTTP, Delta, Kafka, Confluent JDBC, and
  experimental Redis, PostgreSQL, DynamoDB, Snowflake, Iceberg; **no
  ClickHouse sink** [D: sinks docs]. Getting rows into ClickHouse means
  Kafka (ClickHouse Kafka engine / Connect) or Delta read by ClickHouse, or
  HTTP into our own writer.
- **Types:** MAP, ARRAY, ROW, VARIANT, BIGINT UNSIGNED, UUID; `TIMESTAMP`
  is microseconds [D: types, datetime docs]; the Parquet page maps
  TIMESTAMP to Arrow milliseconds [D: Parquet format docs] — our
  `TimeUnix` is nanoseconds, so it needs a cast at the edge of the pipeline
  and loses precision (fine for rollups, not for raw). No LowCardinality
  (dictionary encoding is internal).
- **State GC:** needs `LATENESS`, `append_only` or `NOW()` filters, and the
  compiler's unbounded-state analysis is "best effort, and can err in both
  directions" [D: streaming docs].
- **Performance:** Nexmark 2.2× Flink geometric mean on 16 workers, memory
  0.24× Flink's (post 2024-09-10) [D]; no absolute numbers for our shapes.

**What it replaces:** the MVs (~2 vCPU). **What it can't:** raw storage,
search, HyperDX serving, replication, Keeper.

**Cost [E]:** decode + operators ≈ 2–5 µs/row over the rows the views read
(points and spans: 1.9 M/s) → **4–10 vCPU busy**, one 16–32 vCPU host with
NVMe for ~10–15 GB of state (§3 without the join), ×2 for a standby
(Enterprise) → **32–64 vCPU provisioned** to save ~2. Recovery: pull 10–15 GB
of checkpoint (15–30 s) plus replay of ≤ 60 s of input → **1–2 min** of view
lag, no loss (Enterprise).

**Risks.** Enterprise dependency for the property we care about most; no
ClickHouse sink; the lane protocol has to be flattened (a feeder that turns
committed, deduplicated objects into a Kafka topic or an ordered prefix) —
which is our consumer again; 0.x API churn.

**A prototype would prove:** the dialect over our Parquet (maps as key/value
arrays, u64 series ids, ns timestamps), state per view against §3, and
checkpoint/replay behaviour on an OSS build (which has none) versus the
Enterprise trial.

### B. Feldera as ingest + view layer, writing Iceberg/Delta

**How.** Feldera reads the lanes, deduplicates by content key (SQL `DISTINCT`
over `content_key` with `LATENESS` on `received_at` to bound it), writes raw
rows and views to Delta/Iceberg; ClickHouse/DuckDB/DataFusion read the lake.

**Why not** [D, E]:

- Raw pass-through is 2.07 M wide rows/s through an engine whose value is
  incremental state; it adds encode cost [E: 6–12 vCPU] and none of the
  **sort / locality compaction** that makes the lake searchable (lake/DESIGN
  §2.5 L1/L2). The lake compactor is still needed.
- Delta is the mature sink; **Iceberg output is experimental**, and the
  Iceberg *source* is at-least-once, snapshot-based [D: sinks docs, PR #7195
  merged 2026-09-26].
- HyperDX needs ClickHouse for the hot tier anyway (lake/DESIGN: cold
  queries take seconds).
- The content-key dedup then lives in Feldera state and in the consumer:
  lake/DESIGN's "dedup in two places" risk, with a third engine.

**Verdict:** B = D17's lake + A, at more cost. No.

### C. Hydro-built coordination / consumer

**What Hydro is** [D]: a Rust framework ("location-oriented" programs that
span machines, compiled to DFIR dataflow, deployed by Hydro Deploy),
co-led by Berkeley and AWS; Apache-2.0; hydro_lang **0.17.0-alpha.5**
(2026-09-21), last stable 0.16.0; deploy targets in the release notes are
Docker, ECS and Maelstrom; a Paxos lives in `hydro_test`; the simulator
explores non-determinism exhaustively, and Verus-checked commutativity proofs
are landing [D: hydro.run, crates.io, release notes]. Research: rule-driven
**decoupling and partitioning** rewrites raised 2PC throughput 5× and Paxos
3× (SIGMOD 2024), Flo (POPL 2025) for streaming semantics [D].

**Against our needs:**

- **Replacing Keeper** means giving ClickHouse a replication log. ClickHouse
  speaks ZooKeeper's protocol to Keeper; a Hydro Paxos is not that, and
  writing a ZooKeeper-compatible server in an alpha framework is a research
  project, not a swap.
- **Replacing our coordinator**: it is already coordination-light (one
  owner per lane, fencing by CAS epoch), Quint-modelled with 6 mutants and
  model-based tests, and soaked under chaos [D: DECISIONS §1.4]. Hydro would
  re-express it on a framework with no durable-state facility I could find
  [D: docs search 2026-09-27] — S3 CAS would still be the durable store.
- **Where Hydro's ideas do apply:** the SIGMOD paper's decoupling and
  partitioning is what the consumer already does by lane; its simulator
  would complement quint-connect for the worker's balancing logic (the
  liveness bug the model missed, D8).

**Verdict:** no. Revisit only if a coordinator needs more than S3 CAS can give
(sub-second failover with many writers per key).

### D. Keeper-less ClickHouse-family alternatives

| Candidate | Keeper-less? | HyperDX works? | Verdict |
|---|---|---|---|
| **SharedMergeTree** | still uses Keeper for metadata; data on shared S3 [D: ClickHouse docs] | yes | **ClickHouse Cloud only** [D]; our consumer is untested on it (dedup storage differs [D: otap-rs §What the consumer leaves open]) |
| Alibaba ApsaraDB SharedMergeTree | managed | yes | cloud-specific |
| Refreshable MVs, incl. `REFRESH … APPEND INCREMENTAL` (cursor over new rows) | coordinated through Keeper on replicas | yes | incremental refresh is **experimental** (PR #114152; issue #120081 2026-09-14: a `LIMIT` duplicates rows) [D] |
| `s3_plain_rewritable` single writer + read-only readers | no Keeper | yes | rejected as transfer in D2 (readers broke after merges [M]); no replication |
| StarRocks/Doris shared-data, Databend, ByConity | their own consensus / metadata DB instead | **no** (HyperDX speaks ClickHouse SQL) | no |

**Verdict:** the self-hosted ClickHouse family has no Keeper-less replicated
engine. The Keeper-less paths are E (replicate from the source) and D17
(cold data as Iceberg with create-only metadata on S3, no catalog server).

### E. Source-replicated central (recommended to prototype)

**How.**

- R ClickHouse servers with **plain MergeTree** (no Keeper), each with its
  own `{ctl}/group-r/` lease, checkpoint and heartbeat prefix: **R consumer
  groups** following the same lanes independently.
- Each group's worker runs today's protocol unchanged against **its own**
  server: time bound, server fence, count check and repair. No
  `--sync-replica`: the check reads the server that was written.
- **GC** deletes a slot only below `min` over all groups' positions (the
  rule lake/DESIGN §2.0 already gives the compactor); the tombstone and
  zombie rules are unchanged.
- **Shards** (the calculator's 2): shard s's groups take the lanes with
  `hash(producer) mod S = s`; a `Distributed` table over the shards needs no
  Keeper.
- **Rebuilding a lost replica:** restore its last `BACKUP … TO S3`, then
  run its group from the oldest retained slot; the pre-check skips every
  object already present. Raw slots must therefore be retained for
  backup interval + restore time.
- The lanes are the replication log; the content key is the idempotency
  key; exactly-once per replica is what the soaks already showed on a
  single server.

**What it removes:** Keeper (3 nodes), the D9 Keeper slack (the non-replicated
commit has no Keeper retry loop, so slack returns to the single-server bound,
to be measured), `TIMEOUT_EXCEEDED`-with-commit, `--sync-replica` and its
815 deferred checks, the `insert_quorum` durability question (an acked
request is durable on S3 until every group has passed it), the "replica lost
for good stalls checks" failure, and correlated failure: a Keeper quorum loss
today stops inserts on both replicas.

**What it can't / what it costs:**

- Each replica **inserts** every row instead of fetching parts: +5.7 vCPU
  insert on the second replica, minus fetches and the +9% replication cost
  per statement [M: D13] → **≈ +3–5 vCPU**, against removing 3 Keeper nodes
  → **about neutral** [E].
- S3: one more GET per edge object per group (7.8 M/day ≈ **$95/month**)
  and a second set of lease renewals and LISTs; slot retention for rebuilds
  at 6.1 TB/day ≈ **$140/month per retained day** [E, list prices].
- **Replicas agree on content, not on parts or envelope:** copies of a
  request in *different* lanes (a gateway re-route, U20) can be ingested in
  opposite orders, so replicas keep different envelopes (`batch_id`,
  epoch) for the same telemetry [E]. Copies within one lane are ordered
  identically in every group.
- **Freshness skew** between replicas (each group's lag); HyperDX needs a
  sticky connection (proxy) so a page doesn't mix replicas.
- **DDL, deletes and mutations** go to every server by our tooling, not
  `ON CLUSTER` (which uses Keeper's DDL queue).

**Correctness.** The per-group protocol is today's model instance. New:
GC's floor is the minimum over groups (a mutant "GC at one group's position"
must break `neverSkipsCommitted` for the other), and a `replicasAgree`
invariant over the multiset of content keys per replica. The horizon audit
runs per replica and compares them.

**A prototype would prove:** exactly-once on both servers under the D13 soak
(minus Keeper faults, plus server kills and a rebuild from backup), the
measured commit slack of a non-replicated server (can margin go back below
20 s?), insert CPU of two ingesting servers against one plus fetches, and GC
lag when one group stops.

### F. Embedded `dbsp` in otap-rs (owner's addition)

**What the crate is** [D: crates.io, crate source 0.356.0 read 2026-09-27]:

- Published standalone (`dbsp` 0.356.0, 2026-09-26; `feldera-types`,
  `feldera-storage` alongside), **MIT OR Apache-2.0**. Docs: rustdoc
  (docs.rs/dbsp), a tutorial module, two small examples (`degrees`,
  `orgchart`), benchmarks; versions track Feldera's (several a week), 0.x,
  so APIs can change at any release — pin exactly.
- **In the open crate:** the operator library (joins, aggregates, distinct,
  top-k, time-series operators with *waterlines* for state GC, recursion),
  multi-worker runtime, **multi-host exchange** (`Layout::new_multihost`),
  **spill storage** (LSM-style layer files, LZ4/Zstd since v0.352; backends
  in-memory and POSIX files), and a **checkpoint API**:
  `DBSPHandle::checkpoint()` → `prepare` / `commit` / `publish`,
  `list_checkpoints`, `gc_checkpoint`, restore at start.
- **Not in the open crate:** pushing checkpoints to S3. `feldera-storage`
  defines a `CheckpointSynchronizer` **trait** (`push`, `pull`, owner
  fencing by `owner.json`); the implementation (rclone-based per the docs)
  ships with Enterprise. Its ownership fence is a soft `owner.json`
  overwrite, not a CAS [D: PR #7261, 2026-09-26].
- **No SQL**: the SQL compiler (Java/Calcite) generates Rust against this
  crate; embedding means writing circuits by hand in Rust.

**How it would fit the consumer.**

- **Input:** today the consumer never reads object bodies — ClickHouse
  does, via `s3()`. F adds a GET + Parquet decode per object for the tables
  the views read, then Arrow → Rust tuples → Z-set batches (weight +1),
  one `step()` per statement's worth of objects [E: 1–2 µs/row decode →
  2–4 vCPU for points and spans, plus operator cost].
- **Atomicity with our checkpoint** [E, a design]: dbsp checkpoints to a
  local directory under a UUID. The worker zips it to a **create-only** key
  `{ctl}/state/{group}/{uuid}.zip`, then CASes the lane checkpoints to
  `{positions, state: uuid}`. The CAS is the single commit point; an
  uploaded but unreferenced state blob is garbage for GC after the zombie
  bound (the Iceberg pattern lake/DESIGN uses). Restart: read the CAS'd
  checkpoint, pull the state named there, restore, re-ingest slots after
  the positions. Output rows are keyed by (view key, window) into a
  ReplacingMergeTree, so a replayed step's re-emitted rows collapse.
- **The catch: state is per worker, lanes move.** A circuit holds keyed
  state for the lanes its worker owns, but load balancing moves a lane
  between workers every few minutes (D8), and one checkpoint covers many
  lanes. So either (a) state must be **lane-local** — only views keyed by
  producer, which are useless — or (b) views must be **mergeable partials**
  that each worker flushes when it closes a window or releases a lane, and
  ClickHouse merges — which is what an MV already does without DBSP; or (c)
  a separate **keyed tier** (shuffle by series id / trace id / service)
  with a fixed layout of hosts, its own input log and checkpoints — a
  mini-Feldera we would build and operate.
- **Sharding:** dbsp's own exchange repartitions by key inside a circuit,
  across threads or across a *static* host layout. It doesn't do elastic
  membership; our leases do. Cross-partition views combine by a final
  `GROUP BY` in ClickHouse over partial states.
- **Quint:** a new model `s3InlineConsumerState.qnt`: invariants
  `stateMatchesPositions` (restored state = fold of exactly the slots at or
  below the positions), `viewEqualsBatch` (the integrated output = the view
  over the ingested set), `noOutputLost`, and output idempotence; mutants
  `casBeforeUpload`, `stateGcTooEarly`, `staleStateNewPositions`
  (double count), `rebalanceWithoutFlush`.

**Rust integration** [D: crate manifest vs `otap-rs/Cargo.lock`]:

| | dbsp 0.356.0 | otap-rs today |
|---|---|---|
| toolchain | rust-version 1.96.1, edition 2024 | 1.98.1 pinned |
| arrow / parquet | **none** (conversion is ours) | 58.4.0 (otel-arrow `5db8358`) |
| object_store | 0.14.1 (via feldera-storage) | 0.13.2 → two copies |
| tokio | 1.50 | 1.53.1 |
| other | rkyv 0.7, 67 direct deps, 555 packages in its lock (incl. dev) | 395 crates, 11-min clean release build |

Expect a noticeably longer build (dbsp is generic-heavy) [E].

**F against A:** A gives SQL, a compiler, connectors, a console and profiling,
but crash recovery only with Enterprise and no knowledge of our lanes; F keeps
our exactly-once protocol and Quint models as the source of truth and costs
nothing in licences, but every operator, the Parquet → Z-set bridge, the S3
checkpoint sync, the output sink, the shuffle (if needed) and the tests are
ours. **Verdict:** the right DBSP route for this codebase if a non-mergeable
view appears; not needed for today's list.

**A prototype would prove:** series ⋈ points → 5-minute rollup in a tiny
binary with checkpoint/restore around a crash, state bytes per series against
§3, decode cost per row, and that a restore + replay reproduces the batch
result exactly.

### G. Closed-window audit and recompute of derived tables

**How.** Treat each derived table like raw data under D11:

1. Keep the insert-time MVs (cheap, fresh).
2. After a window closes (window end + lateness, e.g. 5 min + 10 min for
   D15; 15 min + lateness for kv rollups), a job in `consume gc`'s process
   compares per (partition, window) a cheap invariant of the derived table
   against the source (e.g. `sum(count)` in the rollup = `count()` of
   points in the window; from projections where possible).
3. On a mismatch, recompute that window from the source and swap it in
   idempotently (rollup keyed by (series, window) in a ReplacingMergeTree
   with a version, or `REPLACE PARTITION` for per-window partitions).
4. Late data after close (known from `received_at` against `TimeUnix`)
   re-opens a window the same way.

**Cost [E]:** the audit reads only projections / counts per window
(seconds of CPU per hour); recomputes are rare. **Risks:** a window whose
source is still being repaired by the consumer (run after the consumer's
position passes the window's `received_at` + horizon). **Model:** extend the
consumer model with a derived table and the mutant "MV lost on failure"
(already observed [M: D11]).

## 5. Who does what, by option

| Function | Today | A | B | C | D (Cloud) | **E** | F | **G** |
|---|---|---|---|---|---|---|---|---|
| Ingest exactly-once | consumer + count check | consumer (Feldera reads a feed) | Feldera (replay + content-key `DISTINCT`) | consumer on Hydro | consumer (untested on SMT) | **consumer, one group per replica** | consumer (+ state in ckpt) | consumer |
| Replication | RMT + Keeper | same | lake (S3) | same | SharedMergeTree | **the lanes** | same as base | same as base |
| Dedup | content-key check | same + Feldera state | Feldera state | same | same | **per replica, same check** | same | same |
| series ⋈ points | query-time view | same | same | same | same | same | same | same |
| Rollups, RED, kv rollups, helper | MVs | Feldera | Feldera | MVs | MVs | MVs | dbsp partials / MVs | **MVs + window audit** |
| Trace summaries | – | Feldera | Feldera | – | MV | MV | dbsp (keyed tier) | MV + audit |
| Search, HyperDX serving | ClickHouse | ClickHouse | ClickHouse hot + lake | ClickHouse | ClickHouse Cloud | ClickHouse (sticky proxy) | ClickHouse | ClickHouse |
| Retention, tiering | TTL | same | lake expiry | same | Cloud | TTL per server | same | same |
| Coordination | S3 CAS | S3 CAS + Feldera | Feldera ckpt + S3 | Hydro + S3 | S3 CAS | **S3 CAS, per group** | S3 CAS + state blobs | S3 CAS |
| Keeper | yes | yes | no (lake) | yes | inside Cloud | **no** | base's | base's |

## 6. Correctness: how the Quint guarantees map

| Guarantee today | A / B (Feldera) | E | F | G |
|---|---|---|---|---|
| `atMostOnce`, `onlyCommittedIngested`, `neverSkipsCommitted` (consumer) | hold for raw; for views they become Feldera's "exactly-once" (Enterprise) over a feed we must produce in commit order without copies — a new model of the feeder | hold per group, unchanged model; add GC-at-min | hold; add `stateMatchesPositions` | hold |
| `noCommitAfterClose`, tombstones, GC zombie bound | unaffected | GC floor = min over groups | state blobs join GC | unaffected |
| D9 slack assumption | unaffected | **weakens** (no Keeper); re-measure | unaffected | unaffected |
| Derived rows exactly-once | Feldera replay discards re-sent output; duplicates on non-FT sinks | MV gap remains → G | upsert-keyed output | **new**: `derivedMatchesSource` per closed window |

The assumptions would change most under A/B: a third system's exactly-once is
trusted rather than modelled, and its checkpoint fence (`owner.json`) is not
a CAS.

## 7. Prior art

| System | What it is (checked 2026-09-27) | State / coordination | Relevance |
|---|---|---|---|
| **Feldera / DBSP** | IVM for full SQL; DBSP paper VLDB 2023, extended in VLDB Journal 2025 [D] | local or S3-backed state (Enterprise), checkpoints + input journal | A, B, F |
| **Materialize** | Timely/Differential Dataflow; self-managed Community Edition under the Materialize BSL, 24 GB memory / 48 GB disk [D] | Postgres metadata store + S3 "persist" [D] | same class as A; BSL; needs Postgres |
| **RisingWave** | Apache-2.0 streaming DB, state on S3 (Hummock) [D] | meta store | S3 file source (Parquet) re-lists the prefix every 60 s, unordered, per-file state; a 2026-09-04 issue shows completed files re-fetched each tick (closed, not planned) [D: #26944]. **Only its Iceberg sink is exactly-once; ClickHouse is at-least-once** [D: delivery docs] |
| **Arroyo** | Rust/DataFusion streaming SQL; joined Cloudflare 2025-04-10, stays Apache-licensed and self-hostable [D] | checkpoints to object store | windowed aggregates, not IVM |
| **Pathway** | Python API over a Differential Dataflow Rust engine; BSL 1.1 → Apache after 4 years [D] | persistence to S3-compatible stores [D] | BSL; Python-first |
| **Epsio** | commercial IVM for PostgreSQL, MySQL, MSSQL [D] | – | no ClickHouse |
| **Timely / Differential Dataflow** | the Rust libraries under Materialize and Pathway [D] | no built-in persistence [E] | alternative to F with less storage built in |
| **ClickHouse MVs** | insert-time, per block; refreshable MVs; `APPEND INCREMENTAL` experimental [D] | none (MV) / Keeper (refresh coordination) | what we use; G closes its gap |
| **SharedMergeTree** | ClickHouse Cloud engine, shared S3 data, Keeper metadata [D] | Keeper | D |
| **Honeycomb Retriever** | each Kafka partition consumed by two Retriever nodes in different AZs, each tracking its own offset [D] | Kafka as the log | **E's prior art** |
| **Hydro** | Rust framework for distributed programs, alpha [D] | – | C |
| **DuckLake** | lakehouse format with a SQL catalog; no ClickHouse reader [D: lake/DESIGN] | SQL database | rejected in D17 |

## 8. The probe: not run

The optional hands-on probe (the embedded dbsp crate preferred, per the
owner) needs a Rust build of dbsp and its ~500 packages. The disk had
**4.4 GB free at the start and 3.5 GB later** (other agents' runs), below the
4 GB floor even before a target directory, so it was skipped. What was done
instead: the `dbsp` 0.356.0 and `feldera-storage` 0.356.0 crate sources
(4.8 MB) were downloaded from crates.io and read for §4F (checkpoint API,
storage backends, the synchronizer trait, dependencies), then deleted.
Nothing about the SQL dialect or state sizes was measured.

## 9. Open questions

1. **Is any view on the roadmap non-mergeable?** (SLO burn joined with SLO
   definitions, sessionization, exact long-window distincts, span metrics
   corrected by late parents.) If none, A and F stay shelved.
2. **E:** the commit slack of a non-replicated server under host faults
   (can margin return below 20 s?); backup/restore time for a replica at
   the mid scenario (sets raw-slot retention); how HyperDX behaves behind a
   sticky proxy over two replicas with different lag.
3. **E vs D13's measured cost:** two ingesting servers against one ingesting
   plus one fetching, on an idle box.
4. **G:** the cheapest per-window invariant for each derived table that the
   projections can answer without scanning.
5. **Feldera Enterprise:** price, and whether its checkpoint sync can be
   made to use CAS (`If-Match`) rather than `owner.json`.
6. **dbsp crate:** does restore require the same worker count / layout
   (it blocks moving a circuit's state to a bigger host)?
7. **Hydro:** does its simulator find the D8 balancing liveness bug if the
   worker loop were expressed in it? (A cheap way to judge its value to us.)

## Sources

Spike documents: [../DECISIONS.md](../DECISIONS.md),
[../lake/DESIGN.md](../lake/DESIGN.md),
[../hyperdx/README.md](../hyperdx/README.md),
[../metrics-layout/README.md](../metrics-layout/README.md),
[../otap-rs/README.md](../otap-rs/README.md) §Consumer,
[../central-replicated/README.md](../central-replicated/README.md),
`../otap-rs/src/central.rs`, `../otap-rs/sql/otel_logs.sql`,
`../otap-rs/Cargo.lock`.

**Feldera / DBSP**

- Releases (v0.348.0–v0.356.0, 2026-09-11 to 2026-09-26): <https://github.com/feldera/feldera/releases>
- Repository (MIT): <https://github.com/feldera/feldera>
- Checkpoints & fault tolerance: <https://docs.feldera.com/pipelines/fault-tolerance/>
- Enterprise edition: <https://docs.feldera.com/architecture/enterprise/>
- Checkpoint sync to object store: <https://docs.feldera.com/pipelines/checkpoint-sync/>
- S3-backed pipelines (2025-02-04): <https://www.feldera.com/blog/s3-backed-pipelines>
- `take_bucket_ownership` (merged 2026-09-26): <https://github.com/feldera/feldera/pull/7261>
- Iceberg source fault tolerance (merged 2026-09-26): <https://github.com/feldera/feldera/pull/7195>
- S3 input connector: <https://docs.feldera.com/connectors/sources/s3>
- Sources: <https://docs.feldera.com/connectors/sources/>; sinks: <https://docs.feldera.com/connectors/sinks/>
- SQL types: <https://docs.feldera.com/sql/types/>; date/time: <https://docs.feldera.com/sql/datetime/>; Parquet format: <https://docs.feldera.com/formats/parquet>
- Streaming (lateness, append-only, state GC): <https://docs.feldera.com/sql/streaming/>
- Ad-hoc queries (DataFusion): <https://docs.feldera.com/sql/ad-hoc/>; materialized: <https://docs.feldera.com/sql/materialized/>
- Multihost issue: <https://github.com/feldera/feldera/issues/7151>; distributed design: <https://github.com/feldera/dist-design>
- Nexmark vs Flink (2024-09-10): <https://www.feldera.com/blog/nexmark-vs-flink>
- DBSP, VLDB 2023: <https://www.vldb.org/pvldb/vol16/p1601-budiu.pdf>; VLDB Journal 2025: <https://link.springer.com/article/10.1007/s00778-025-00922-y>
- crates.io: `dbsp`, `feldera-storage`, `feldera-types` 0.356.0 (source read: `src/circuit/checkpointer.rs`, `src/circuit/dbsp_handle.rs`, `src/operator/communication/exchange.rs`, `feldera-storage/src/{lib.rs,checkpoint_synchronizer.rs}`, `Cargo.toml`)

**Hydro**

- Site: <https://hydro.run/>; research list: <https://hydro.run/research/>
- Repository (Apache-2.0): <https://github.com/hydro-project/hydro>
- hydro_lang v0.17.0-alpha.5: <https://github.com/hydro-project/hydro/releases/tag/hydro_lang-v0.17.0-alpha.5>; crates.io `hydro_lang`, `dfir_rs`, `hydro_deploy`
- Optimizing Distributed Protocols with Query Rewrites (SIGMOD 2024): <https://arxiv.org/abs/2404.01593>
- Paxos in hydro_test: <https://github.com/hydro-project/hydro/blob/main/hydro_test/src/cluster/paxos.rs>

**Adjacent systems**

- RisingWave delivery guarantees: <https://docs.risingwave.com/delivery/overview>; S3 source: <https://docs.risingwave.com/integrations/sources/s3>; issue #26944: <https://github.com/risingwavelabs/risingwave/issues/26944>
- Materialize self-managed / Community Edition: <https://materialize.com/blog/materialize-for-everyone/>, <https://materialize.com/docs/license/>
- Arroyo joins Cloudflare: <https://www.arroyo.dev/blog/arroyo-is-joining-cloudflare/>
- Pathway: <https://pypi.org/project/pathway>
- Epsio: <https://www.epsio.io/incremental-material-views>
- ClickHouse refreshable MVs: <https://clickhouse.com/docs/materialized-view/refreshable-materialized-view>; incremental refresh bug: <https://github.com/ClickHouse/ClickHouse/issues/120081>
- SharedMergeTree: <https://clickhouse.com/docs/products/cloud/features/infrastructure/shared-merge-tree>
- Honeycomb, Kafka and Retriever: <https://www.honeycomb.io/blog/transforming-how-we-run-kafka-honeycomb>, <https://www.honeycomb.io/blog/scaling-kafka-observability-pipelines>
