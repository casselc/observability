# entities: a fleet entity catalog instead of per-row resource attributes

A spike. The idea: a multicluster controller already knows every cluster,
node, namespace, workload, pod and container in the fleet, with their labels.
If that knowledge is kept as a versioned catalog in central ClickHouse, a
span or log row doesn't need to carry its ~30 Kubernetes resource attributes.
It carries a `resource_id` and a small **residual** map, and ClickHouse puts
the attributes back at query time from a dictionary. This directory builds
that on a synthetic 20-cluster fleet and measures it against the consumer's
ClickStack 2.39.1 tables.

It builds on:

- [`../DECISIONS.md`](../DECISIONS.md) D7, layout B for metrics, which already
  moves attribute maps out of the rows, into a series table that compatibility
  views join back;
- [`../metrics-layout/README.md`](../metrics-layout/README.md): what HyperDX
  does through views;
- [`../hyperdx/README.md`](../hyperdx/README.md) §Schema. Aligning traces and
  logs with ClickStack's DDL made inserts 4× dearer per row, and the text
  indexes are most of that cost. The same section records HyperDX's fast
  paths and the SQL HyperDX actually sends.

Labels:

- **[M]**: measured here. The box was the shared 4-vCPU dev box, loaded by
  other agents' CI and soaks; the load average is given with each table.
  Software: ClickHouse 26.10.1.618 and SeaweedFS on localhost. Queries were
  capped at `max_threads = 2` and 3 GB.
- **[D]**: read in a source, mostly HyperDX at `hyperdx@885d30c`
  (`CODE_VERSION=2.39.1`).
- **[E]**: my estimate.

## 1. Summary and verdict

**The storage and insert case is strong.** The views are exact. But
**unmodified HyperDX makes resource-attribute filters 10–170× slower**. The
catalog pays off only if those filters are rewritten into catalog lookups,
through a HyperDX change or a small SQL-rewriting proxy. With the rewrite, the 15
query shapes take 766 ms of server time against the table's 1,340.
- Most shapes are within 1.2× of the table.
- Two are 2.8–12.6× faster: those where the table must read the map.
- Three are slower: a group-by on namespace (59 against 17 ms), value
  discovery (59 against 25 ms) and full text (71 against 61 ms).

| | (a) ClickStack 2.39.1, consumer DDL at HEAD | (b2)/(c) `resource_id` + residual |
|---|---:|---:|
| stored B/span, merged [M] | 150.1 | **84.6** (−44%) |
| stored B/log, merged [M] | 197.9 | **85.3** (−57%) |
| of which resource attributes, span / log [M] | 72.9 / 111.8 (map + items index) | 4.9 / 11.6 (id + residual) |
| insert µs/span / log, from Parquet, loaded box [M] | 24.6 / 27.7 | **8.7 / 8.7** |
| merge µs/span / log [M] | 13.3 / 16.6 | **7.0 / 6.2** |
| exact rows through the view [M] | – | **yes**: count, `sum(cityHash64)`, `EXCEPT ALL` both ways, 975k rows |
| HyperDX, 15 query shapes, total server ms [M] | 1,340 | view 19,475, ALIAS column 18,201, **with the rewrite 766** |
| dictionary memory, 90 days of the fleet [M] | – | **2.15 GB** normalized (flat maps: 32 GB) |
| new data visible after the controller writes it [M] | – | 18–52 s (dictionary `LIFETIME(MIN 30 MAX 60)`) |

**Verdict: worth building, but only as a package. Don't ship half of it.**

1. **Rows carry `resource_id` + `ResourceResidual`; `ResourceAttributes` is
   an `ALIAS` column on the MergeTree itself** (variant **c**), not a view.
   HyperDX then still sees a ClickStack table: its text-index paths for
   `Body`, `TraceId` and `LogAttributes` keep working, and the row panel
   reads the alias explicitly. The per-row reconstruction costs ~4 µs, so it
   is fine for result pages and waterfalls.
2. **Resource-attribute filters and group keys are rewritten to the
   catalog.** `ResourceAttributes['k'] = 'v'` becomes `resource_id IN
   (SELECT resource_id FROM resource_kv WHERE Key = 'k' AND Value = 'v')`.
   Without the rewrite this design loses to (a) on exactly the queries it is
   meant to speed up. Neither a view nor an alias lets ClickHouse invert a
   map built from a dictionary (§3.4).
3. **`resource_id` is a content hash of the covered attribute set, not an
   entity id.** An entity id needs time-range (SCD2) lookups. ClickHouse
   can't invert those, and they get telemetry near version boundaries wrong
   (§3.1).
4. **Correctness must not depend on the controller.** A grace window at the
   edge makes new resources exact before the catalog knows them. A resource
   the catalog never learns about degrades to residual-only rows, and it
   heals at query time once the catalog learns it (§3.6). The remaining hole,
   a pod the controller never saw, is closed by an **edge announcement lane**
   (D7's series objects, for resources). That makes the controller an
   optimization, not a dependency. Variant **ann** tests this lane's row
   shape. The lane itself was not built.
5. **Discovery (HyperDX's filter keys and values) is served from the
   catalog.** It goes into the key-value rollup HyperDX already reads,
   written by a periodic job rather than by a materialized view on every
   insert. HyperDX 2.39.1 can use it with no HyperDX change, under one
   condition on how the materialized views are named (§3.5).

What it would take, in order: the rewrite (a HyperDX feature request, or a
~200-line proxy); `resource_id` in both edges; the catalog controller; the
announcement lane; incremental dictionary refresh. §7 lists the open items.
Two things stay unmeasured: **HyperDX was not run live**, because the disk
had 3.2–6 GB free while other agents' CI ran (§5); and **the data is
synthetic**, so absolute bytes/row depend on its density (§4.1).

## 2. What was built

| step | script | what |
|---|---|---|
| fleet + catalog | `scripts/fleet.py` | 20 clusters (14 prod, 3 staging, 3 dev; 4 regions) × 200 node slots over **90 days of SCD2 history**: nodes replaced every 5–25 days; 4 daemonsets; 520 deployments per cluster in 37 team namespaces (1–12 replicas, rolled out every 1–10 days, 20% HPA-scaled +50% from 08:00 to 20:00, evictions, 3% of pods relabelled mid-life, 25% with an istio sidecar); 40 statefulsets; 30 cronjobs (every 5 min to daily). **4.69 M pods, 4.72 M pod versions, 193 k workload versions, 29 k nodes; 59,779 pods live at any time; 5.10 M resources (pod version × container) over 90 days** [M]. 74 s to generate. |
| catalog DDL | `sql/catalog.sql` | one SCD2 table per level (`clusters`, `nodes`, `namespaces`, `workloads`, `pods`: `valid_from` / `valid_to`), `resources` (the flat covered set per `resource_id`), `resources_current` view |
| `resource_id` | `sql/resources.sql` | the flattening join and the hash (§3.1) |
| dictionaries | `sql/dictionaries.sql`, `scripts/dicts.py` | normalized: `d_res` (id → pod version, container index), `d_pod`, `d_wl`, `d_node`, `d_ns`, `d_cluster` |
| telemetry | `scripts/telemetry.py` | spans and logs in hdxgen's shapes (HTTP server spans, DB and downstream client spans, exceptions on errors; access, app, agent, sidecar and job logs), generated in ClickHouse, weighted by workload traffic over every resource alive in a 2-hour window. **37 resource keys per row** on average: 32 covered, plus 5 residual (`telemetry.sdk.*`, `process.runtime.name`, `service.instance.id`), and `git.commit.sha` for 15% of workloads. Rows go to 3 publishers per cluster, cut into 10,000-row objects. |
| schemas | `scripts/schemas.py` → `sql/generated/` | derived by text transformation from `../otap-rs/sql/otel_{traces,logs}.sql` **as committed at HEAD (`587d4b2`)**. That DDL is already "option 2": ClickStack 2.39.1 without `idx_*_attr_key` (`e784242`). The full ClickStack DDL (`../hyperdx/sql/clickstack_full_*.sql`) is measured as `full`. |
| loads | `scripts/load.py`, `scripts/objects.py` | query databases: one object per INSERT, in the consumer's statement shape, from a staging table. Insert benchmark: 30 Parquet objects per signal per shape, written by ClickHouse's Parquet writer to SeaweedFS and read with `s3()`. **Not the real edge → S3 → consumer pipeline, and not the edges' Parquet writers.** |
| checks | `scripts/conformance.py`, `scripts/race.py`, `scripts/refresh_lag.py` | view = table; the race cases |
| measurements | `scripts/bench.py`, `scripts/merge.py`, `scripts/queries.py`, `scripts/discovery.py`, `scripts/hdx_strategy.py` | §4 |

The schemas compared:

| | rows carry | sort key (traces / logs) | HyperDX reads |
|---|---|---|---|
| **a** | `ResourceAttributes` + `idx_res_attr_items` (text), logs: 8 `__hdx_materialized_k8s.*` columns | `(ServiceName, SpanName, toDateTime(Timestamp))` / `(toStartOfFiveMinutes(Timestamp), ServiceName, Timestamp)` | the table |
| full | a + `idx_*_attr_key` (insert only) | same | the table |
| b1 | `resource_id UInt64`, `ResourceResidual Map`, bloom filter on `resource_id`; no items index, no materialized k8s columns, 6-branch logs rollup view instead of 14 | as a | a view |
| **b2** | as b1, no bloom filter | `(ServiceName, resource_id, SpanName, toDateTime(Timestamp))` / `(toStartOfFiveMinutes(Timestamp), ServiceName, resource_id, Timestamp)` | a view |
| **c** | b2's table | b2's | the table, `ResourceAttributes` an `ALIAS` column |
| ann | `resource_id` = hash of the **whole** map, no residual | b2's | a view over an announced-resources dictionary |

## 3. Mechanism

### 3.1 `resource_id`

```
covered  = the resource attributes the catalog can reproduce, i.e. the keys the edge's
           k8sattributes / resourcedetection processors add (cluster, cloud, node, host,
           namespace, workload, pod, labels, container, image), with a non-empty value
residual = every other resource attribute (SDK-set: telemetry.sdk.*, service.instance.id,
           process.*, custom ones)

resource_id = xxh3_64( "res.v1\0" ‖ for (k, v) in covered sorted by k (bytewise): k ‖ "\0" ‖ v ‖ "\0" )
```

- The separator is safe for the covered set. Every covered value is a
  Kubernetes name, label or uid, an image reference, a cloud id or an
  RFC 3339 time, and none of those can contain NUL. SDK-set values can, but
  they are in the residual and are never hashed.
- In ClickHouse the hash is `xxh3(concat('res.v1\0', arrayStringConcat(arrayMap(x ->
  concat(x.1, '\0', x.2, '\0'), arraySort(...)))))`. In Rust it is
  `xxhash_rust::xxh3::xxh3_64`; in Go, `zeebo/xxh3.Hash`. All three are
  XXH3-64 with seed 0, as D7's series id is.
- It is a **content address** of one version of one resource.
  - A pod's container has one id for its life.
  - A relabel, a rollout (new pod), a reschedule (new pod) or an image change
    gives a new id.
  - The row `Timestamp` plays no part in the lookup.
- **Why not an entity id** (a hash of cluster uid + pod uid + container
  name), which is what the idea started from:
  - its attributes change over the entity's life, so the lookup must be by
    (id, `Timestamp`): a `RANGE_HASHED` dictionary;
  - `optimize_inverse_dictionary_lookup` does not invert range dictionaries
    [M], so no filter could become a primary-key condition;
  - rows near a version boundary resolve to the version the controller
    recorded, not the one the edge saw. The two disagree by the difference in
    informer lag. On late rows the lookup misses outright: 72 rows here were
    stamped after their pod version ended (§4.5);
  - `RANGE_HASHED` also costs 1,953 B per version against 1,612 flat [M].
- **Collisions.** 64 bits over ~5 M resource versions per 90 days: the
  chance of any collision is ~7·10⁻⁷ [E]. A collision shows one resource's
  attributes on the other's rows, as D7's series ids would.

### 3.2 The catalog (the controller's model)

- SCD2 per level, and every level stores only the attributes it contributes:
  - a cluster: `k8s.cluster.name`, `.uid`, `cloud.*`,
    `deployment.environment.name`;
  - a node: `k8s.node.*`, `host.*`, `cloud.availability.zone`;
  - a namespace;
  - a workload **version** (one per rollout): `k8s.<kind>.name`,
    `service.name`, `service.version`, the template's `k8s.pod.label.*`,
    `k8s.replicaset.name`, the container templates;
  - a pod **version** (one per label change): name, uid, `start_time`, job
    name, pod-specific labels.
- A resource is (pod version, container). Its covered set is the `mapConcat`
  of its chain.
- `resources` holds the flattened set per `resource_id`: the dictionary's
  source, and what an edge announcement would write (§6.3).
- `resources_current` is the live state.

### 3.3 Dictionaries: normalized, or they don't fit

Memory of one dictionary after `SYSTEM RELOAD`, measured as the change in the
server's `MemoryTracking` in steady state
(`results/dictionary-layouts.tsv`, `dictionary.jsonl`) [M]:

| layout | entries | per entry | 90 days of the fleet |
|---|---:|---:|---:|
| flat: `resource_id → Map(String, String)` (`HASHED`) | 257,584 (1 cluster, 90 days) | **6,266 B** | 32 GB [E, ×5.10 M] |
| flat, the map as a `String` | same | 1,612 B | 8.2 GB [E] |
| flat, values only (fixed key order) | same | 1,091 B | 5.6 GB [E] |
| SCD2 entity id + range (`RANGE_HASHED`, `String`) | same | 1,953 B | 10 GB [E] |
| **normalized** (`sql/dictionaries.sql`): `d_res` `HASHED_ARRAY` | 5,102,520 | 67 B | 344 MB [M] |
| `d_pod` (name `String`, uid `UUID`, start `DateTime`, extra JSON, 4 keys), `HASHED_ARRAY` | 4,699,830 | 350 B | 1,644 MB [M] |
| `d_wl` (JSON attrs + containers) | 193,409 | 825 B | 159 MB [M] |
| `d_node`, `d_ns`, `d_cluster` | 29,853 | | 10 MB [M] |
| **normalized total** | | | **2.15 GB** per replica |

- `system.dictionaries.bytes_allocated` under-reports `Map` attributes about
  100×: it gives 16 MB for the flat 1.6 GB.
- `HASHED` (one hash table per attribute) costs 771 B per pod against
  `HASHED_ARRAY`'s 428; `SPARSE_HASHED` 537 B, and loads 4× slower.
- Load times at 90 days: `d_pod` 7.2–7.4 s, `d_res` 1.5 s [M]. A full reload
  every 30–60 s is ~15% of a core per replica [E]. At fleet scale, use
  `update_field` (incremental, from the controller's `updated_at`) [E, not
  measured].
- Pods dominate, and cronjob pods dominate the pods: 1,655 runs a day per
  cluster. A hot dictionary over the last 7 days is ~1/13 of this, about
  0.2 GB [E]. Older rows could fall back to a `CACHE` or `DIRECT` layout over
  the same tables [E, not measured]. HyperDX queries are mostly recent.
- Levels are stored as JSON strings and parsed with `JSONExtract`. A `Map`
  attribute on `d_wl` was bigger (2.9 KB per entry) and slower
  (2.3 µs/row against 0.65 µs) [M].
- The normalized reconstruction equals the flattened covered set for all
  75,605 resources of the window [M].

### 3.4 Views, the ALIAS column, and why filters need a rewrite

(b) is a view returning (a)'s ordinary columns in (a)'s order, and (c) is an
`ALIAS` on the table. Both use the same expression:

```
ResourceAttributes = CAST(mapSort(mapUpdate(
    if(dictHas(d_res, resource_id),
       mapConcat(cluster, node, namespace, workload, pod, container),  -- chained dictGets + JSONExtract
       map()),
    ResourceResidual)), 'Map(LowCardinality(String), String)')
```

- The expression costs **~4.3 µs per row** [M: 300k rows, 2 threads].
  That's nothing for a 200-row page or a waterfall, and a lot for a filter or
  group-by over millions of rows.
- **ClickHouse 26.10 can invert a dictionary, but not here** [M].
  - `optimize_inverse_dictionary_lookup` (on by default) turns `dictGet(d,
    'attr', id) = 'x'`, `LIKE` and `ILIKE` into `id IN (reverse lookup)`, a
    primary-key condition: 2 of 1,221 granules in a test.
  - It does not apply to `IN (...)`, `!=`, `RANGE_HASHED` dictionaries,
    `Map` attributes, or `map(...)['k']`: the analyzer doesn't fold a
    constant key out of a constructed map.
  - HyperDX renders `ResourceAttributes['k'] = 'v'`, so the whole map is
    built for every row in the time range.
- **Exactness.** The generated (a) rows keep their map keys sorted, so the
  view's `mapSort` gives byte-equal maps. An edge that writes keys in OTLP
  order would compare equal only under `mapSort` on both sides. HyperDX
  doesn't depend on key order.
- **The rewrite** (`scripts/queries.py`, mode `rw`) is what a proxy or a
  HyperDX change would send:
  - `ResourceAttributes['k'] = 'v'` / `ILIKE` on a covered key →
    `resource_id IN (SELECT resource_id FROM ent_cat.resource_kv WHERE Key =
    'k' AND Value = 'v')`. `resource_kv` is sorted by `(Key, Value)`: 2.42 M
    rows and 23.7 MB for 76 k resources, ≈ 1.6 GB for 90 days of the fleet
    [E];
  - on a residual key → `ResourceResidual['k'] …`;
  - as a group key → the one level dictionary that holds the key.

### 3.5 Discovery from the catalog, and what HyperDX 2.39.1 reads

Read in `common-utils/src/core/metadata.ts` [D]:

- **Map keys (`getMapKeys`).**
  - With a text index on the map, HyperDX reads `mergeTreeTextIndex`. That is
    (a)'s path.
  - Otherwise, if the source has `metadataMaterializedViews` (auto-detected:
    a table named `<source table>_kv_rollup_15m` exists, granularity from its
    `as_select`, else 15 minutes), it reads `SELECT Key FROM <rollup> WHERE
    ColumnIdentifier = '<column>' AND Timestamp …`, with **no further
    check**.
  - Otherwise it samples up to 3 M rows (`sampledKeys`).
- **Values (`getAllKeyValues`).** Per column: a key-value text index, then
  the rollup, then a raw scan. The rollup is used only if
  `doMetadataMVsAggregateColumn` passes. That check takes the **first**
  `MaterializedView` in `SELECT * FROM system.tables WHERE database = …`
  whose create query starts `CREATE MATERIALIZED VIEW db.x TO
  db.<rollup>`, and **returns on it**. It passes if the column name appears
  in that view's `as_select` (a regex, and quoted literals count).
- **Consequence.** A second materialized view into the same rollup (one for
  the catalog's rows) silently switches off the rollup path for whichever
  columns the first one doesn't name. The DDL here names the catalog's
  materialized view `<rollup>_0_catalog_mv`, so it sorts first. It reads a
  `Null` table, `resource_kv_in_<signal>`, and its `WHERE ColumnIdentifier
  IN ('ResourceAttributes', 'ServiceName', …)` lists every column the rollup
  serves. `scripts/hdx_strategy.py` re-implements the check. On `ent_b2` it
  picks the rollup for `ResourceAttributes` and every native column, and
  not for `LogAttributes` / `SpanAttributes`, which were never in the rollup
  [M, `results/hdx_strategy.json`].
- **The rows are written by a job, not by a materialized view on every
  insert:**
  - one row per 15-minute bucket × key × value of every resource alive in the
    bucket, `count` = live resources;
  - fleet-wide that is **~205 k rows per bucket, 40 keys, ≈ 19.7 M rows a
    day**; 3 hours took 7–11 s of loaded-box time per signal [M];
  - stored ≈ 20 B per row unmerged, ≈ 0.4 GB a day per signal, against
    5.9 TB/day of telemetry [E];
  - it replaces the 8 `__hdx_materialized_k8s.*` branches of (a)'s logs
    rollup view, which run on every insert.
- **Residual keys are not in the catalog's rollup**, so they don't appear as
  facets. An edge announcement (§6.3), or a small materialized view on
  `ResourceResidual`'s keys, would add them [E].
- **Through a view** HyperDX also loses the text-index paths for
  `LogAttributes` / `SpanAttributes` key discovery (it samples the table
  instead) and `hasAllTokens` for full text. Variant c keeps both.

### 3.6 Races

The edge rule is: **inside the grace window G after the publisher first
sees a `resource_id`, send the whole covered set in `ResourceResidual`**
(G = 120 s here; the view's `mapUpdate` makes the residual win). Everything
else follows from content addressing (`scripts/race.py`,
`results/race.jsonl`) [M]:

| case | rows (spans / logs, of 497,218 / 478,320) | what the view shows |
|---|---:|---|
| inside the grace window (new pods, mostly cronjob runs) | 126 / 171 (0.03%) | **exact**, with or without the catalog |
| the resource is unknown to the catalog (the controller missed 1 pod in 200 in this test) and the row is outside grace | 1,206 / 1,195 (0.24%; 103 / 150 resources) | residual-only `ResourceAttributes` until the catalog learns it; then **exact on the next query**, with no rewrite of data |
| stamped after the pod version ended (a terminating pod's last logs) | 43 / 29 | **exact**: the id stays in the dictionary; an SCD2 (entity, time) lookup would miss all of them |
| after catch-up | all | **exact**: `EXCEPT ALL` 0 / 0 both ways, hashes equal, for b2 and c (`results/conformance.jsonl`) |

- **Visibility lag.** A resource the controller has just written reaches
  the views in **17.7–52.0 s (median 30, 12 samples)**, through the
  dictionaries' own `LIFETIME(MIN 30 MAX 60)` and no manual reload
  (`results/refresh_lag.jsonl`) [M]. G must cover the controller's write
  delay plus this lag.
- The "before" row counts include 7 log rows (2 resources) that the one-shot
  index build had missed. The cause is not identified: `d_pod` has 4,699,830
  entries for 4,719,226 pod rows, so the generator emits some duplicate pod
  versions. Those rows were re-indexed like the withheld ones.
- **What is not covered:**
  - a pod the controller never sees at all: a controller outage longer than
    the pod's life, since informers relist live objects only. Its rows stay
    residual-only for good. This is why §6.3's announcement lane exists;
  - informer lag on label changes, which the content id turns into "a new
    id, briefly unknown". The generator changes labels exactly at version
    boundaries, so that lag was not simulated.

## 4. Results

### 4.1 Insert, merge, stored bytes

`scripts/bench.py` (insert CPU: the statement's own
`OSCPUVirtualTimeMicroseconds` from `clickhouse client
--print-profile-events`, 30 objects per signal, one per statement, the
consumer's shape, merges stopped; bytes after `OPTIMIZE FINAL` to one part)
and `scripts/merge.py` (merge CPU: `clickhouse local` rusage, 30 parts → 1).
Median [range] over reps 1–5 (rep 0 is a warm-up), variants rotated per rep.
**Loaded box** [M]:

| variant | reps | insert µs/span | insert µs/log | merge µs/span | merge µs/log | B/span | B/log | of which indexes span / log | load (1-min) |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| a | 5 | 24.58 [18.73–28.70] | 27.68 [23.38–31.27] | 13.27 [13.16–13.68] | 16.58 [15.49–17.34] | 158.6 | 201.8 | 62.3 / 73.8 | 2.5–4.2 |
| full | 5 | 28.97 [20.67–32.11] | 26.86 [22.96–32.07] | 14.57 [13.50–15.28] | 17.23 [14.28–18.61] | 159.5 | 203.5 | 63.2 / 75.3 | 2.5–4.3 |
| b1 | 5 | 8.76 [8.09–11.38] | 8.14 [7.09–9.80] | 6.96 [6.68–7.14] | 6.50 [5.92–7.18] | 90.9 | 93.3 | 30.5 / 35.1 | 2.5–3.9 |
| b2 | 5 | 8.67 [7.55–10.83] | 8.69 [7.29–8.86] | 7.01 [6.75–7.15] | 6.19 [5.86–6.31] | 91.5 | 90.3 | 33.0 / 34.5 | 2.8–3.7 |
| ann | 5 | 8.47 [6.92–10.73] | 8.44 [7.80–9.47] | 5.33 [5.25–5.82] | 4.47 [4.47–5.79] | 86.9 | 81.0 | 33.0 / 34.6 | 2.5–4.0 |

- **Insert: 2.8× cheaper per span and 3.2× per log** (8.7 µs against 24.6
  and 27.7). Merges are 1.9× / 2.7× cheaper. Stored bytes fall 42% / 55%.
- The resource side of (a) is the map column plus its items text index
  (62 / 74 B of indexes per row, against 33 / 35 in b), and, for logs, the 8
  materialized columns and 8 rollup branches.
- `full` (with `idx_*_attr_key`) costs little more than (a) on these rows.
- b1 (ClickStack's sort key + a bloom filter) and b2 (`resource_id` in the
  key) cost the same to insert. b2 is the one that prunes rewritten filters.
- ann saves another ~5–9 B/row and 1.7–1.9 µs of merge CPU. Its
  announcements are not counted: ~1 per resource per hour, negligible [E].
- (a) is dearer here than in `../hyperdx/README.md` (13.6 / 7.0 µs, table
  only) because these rows carry 37 resource keys against hdxgen's 10, and
  the box was loaded (2.5–4.3).

- The tables include each variant's rollup materialized view, as shipped.
  The catalog's discovery job and the dictionary refreshes are not in the
  insert path (§3.3, §3.5).
- The objects are 1/300 of the mid scenario's rate. A resource's rows are
  therefore further apart in time than in production, and (a)'s maps
  compress worse than they would at full density [E]. The query databases,
  merged to one part (500k rows per signal), give a similar ratio: 150.1 /
  197.9 B against 84.6 / 85.3 (§1).
- On the wire (ClickHouse's Parquet writer, 10k rows per object), a span is
  95 B in (a), 80 B in b and 75 B in ann; a log is 76 / 59 / 54 B [M].

### 4.2 HyperDX query shapes

`scripts/queries.py` (`results/queries.md`, `.jsonl`) runs 15 scenarios.
They are the statements HyperDX 2.39.1 sent in `../hyperdx`'s captures, with
this data's values, over a 2-hour window: 497,218 spans and 478,320 logs.
Each scenario is rendered for (a), for the view, for c, and with the rewrite.
Server ms is the median of 5 interleaved rounds after a warm-up, with
`use_query_condition_cache = 0`. Loaded box, load 2.6–4.6 [M]:

| scenario | stmts | a | view | c (ALIAS) | c + rewrite |
|---|---:|---:|---:|---:|---:|
| logs: all (histogram + page) | 2 | 35.5 | 42.0 | 35.4 | 36.2 |
| logs: `k8s.pod.name =` + ERROR | 2 | 31.5 | 1,311.8 | 323.6 | 43.6 |
| logs: `k8s.namespace.name =` | 2 | 31.4 | 2,806.0 | 2,855.4 | 37.2 |
| logs: `ServiceName =` | 2 | 27.2 | 37.5 | 30.5 | 29.7 |
| logs: full text | 2 | 61.1 | 67.1 | 72.1 | 71.2 |
| logs: group by namespace | 1 | 17.2 | 2,254.1 | 2,118.9 | 59.2 |
| logs: row detail panel | 1 | 22.4 | 23.4 | 18.7 | 18.4 |
| traces: all | 2 | 39.9 | 50.2 | 43.5 | 41.9 |
| traces: `k8s.pod.name =` | 2 | 29.4 | 5,046.7 | 4,822.5 | 34.8 |
| traces: residual key `ILIKE` + span attribute | 2 | 569.4 | 5,120.5 | 5,171.9 | **205.2** |
| traces: p95 by deployment (errors) | 1 | 295.4 | 140.1 | 144.5 | **23.4** |
| trace waterfall by TraceId (3 statements) | 3 | 111.7 | 78.2 | 66.6 | 66.0 |
| discovery: resource keys | 2 | 43.2 | 43.3 | 44.3 | 40.4 |
| discovery: values of 10 resource keys | 2 | 25.1 | 59.7 | 58.4 | 59.2 |
| discovery: values by scanning (no rollup) | 1 | – | 2,394.5 | 2,394.8 | – |
| **sum** | | **1,340** | **19,475** | **18,201** | **766** |

- **Resource filters and group-bys, unmodified: 10–170× (a).** The map is
  rebuilt for every row in range. (a) prunes through `idx_res_attr_items`
  (417–437k rows read, against 0.85–1.0 M), and uses the materialized
  `__hdx_materialized_k8s.namespace.name` for logs.
- **With the rewrite: close to (a), faster where (a) must read maps, and
  slower on a group-by that (a) answers from a materialized column.**
  `resource_id IN (…)` prunes
  on b2's sort key. Group-bys read one level dictionary per distinct row
  instead of (a)'s map. Where (a) must read the map (`ILIKE` on a key, p95
  by deployment), the rewrite is 2.8–12.6× faster.
- **Everything else is unchanged**: service filters, full text through c's
  `hasAllTokens`, trace lookups, row pages.
  - Through the view, HyperDX falls back to `hasToken` + `LIKE`; that cost
    +10% here.
  - The views read ~1.7× the rows on logs pages, because HyperDX can't see
    the sort key and adds no `toStartOfFiveMinutes` bound. The primary key
    still prunes through the monotonic function.
- **Discovery from the catalog's rollup is as fast** for keys and 2.4× (a)
  for values. It reads the whole rollup (1.3–3.7 M rows): ~34 ms more.
  - (a) reads per-part text-index tokens. Those grow with the parts in the
    window [E].
  - The rollup grows with the buckets in the window: ~20 M rows a day.
    HyperDX caps its catalog window at 1–3 days (`clampCatalogDateRange`),
    so that stays bounded [E].
  - Had the materialized-view check failed (§3.5), HyperDX would scan
    instead: 2.4 s here.

### 4.3 Conformance

`results/conformance.jsonl`, on (a)'s ordinary columns in order, with
`ResourceAttributes` rebuilt [M]:

| presentation | signal | rows | `sum(cityHash64(all))` | `EXCEPT ALL` a→b / b→a |
|---|---|---:|---|---:|
| b2 view, after catch-up | traces | 497,218 | equal | 0 / 0 |
| b2 view, after catch-up | logs | 478,320 | equal | 0 / 0 |
| c `ALIAS` column, after catch-up | traces | 497,218 | equal | 0 / 0 |
| c `ALIAS` column, after catch-up | logs | 478,320 | equal | 0 / 0 |
| b2 view, before catch-up | traces / logs | | differ | 1,206 / 1,206 and 1,195 / 1,195: exactly the rows the catalog could not rebuild (§3.6) |
| ann view (3 objects per signal) | traces / logs | 26,996 / 28,399 | equal | 0 / 0 |

### 4.4 Catalog cost

- Dictionaries: §3.3.
- Visibility lag: §3.6.
- Discovery rows: §3.5.
- `resource_kv`: §3.4.
- The flattening join that computes `resource_id` for every pod version of
  90 days (5.10 M rows) took 17.7 s on 2 threads [M]. The controller would
  compute ids incrementally, per informer event.

## 5. HyperDX

**It was not run live.**

- The images take 1.4 GB (`../hyperdx/README.md` §Disk). While this spike
  ran, other agents' CI took free disk from 6.1 GB down to 3.2 GB, below
  the 4 GB floor.
- Instead, the statements HyperDX sends come from `../hyperdx`'s captures,
  and the view and alias renderings from its source [D]. The discovery
  strategy was re-implemented and checked against the live `system.tables`
  (§3.5).

Compatibility as read [D] and replayed [M]:

| HyperDX feature | (b) view | (c) `ALIAS` on the table |
|---|---|---|
| search, histograms, row panel (`SELECT *, ResourceAttributes AS __hdx_resource_attributes`) | works; the view returns (a)'s columns | works; the alias is selected explicitly |
| resource filters / group-bys | work, 10–170× slower (§4.2) | same |
| full text, `LogAttributes` / `SpanAttributes` items filters and key discovery | fall back to `hasToken` / map filters / sampling | **fast paths kept** |
| resource key / value discovery | the catalog's rollup (auto-detected by name) | the rollup (configure `kvRollupTable`: auto-detection looks for `<table>_kv_rollup_15m`) |
| materialized-column rewrite (logs `k8s.*`) | none | none (the columns are dropped) |
| writes | none (HyperDX never writes) | none |

**The rewrite needs a HyperDX change.** HyperDX already has the pieces:

- `metadataMaterializedViews` for discovery;
- materialized-column substitution in `renderChartConfig` / `queryParser`,
  which maps an expression to a column name when that column is
  `MATERIALIZED` or `DEFAULT`;
- a feature-flagged `seriesTable` for metrics.

A "resource lookup table" on the source would do the same for resource maps.
Map a `ResourceAttributes['k'] op v` filter to `resource_id IN (SELECT
resource_id FROM <table> WHERE Key = 'k' AND Value op v)`, and a group key to
a join or a dictionary. Until then, a rewriting proxy between HyperDX and
ClickHouse (like `../hyperdx/scripts/chproxy.py`) can do it with a few
regular expressions over HyperDX's fixed rendering. **Proxy mode is what
`rw` measures.**

## 6. What it needs outside central

### 6.1 The edge computes `resource_id`

- The agents (`../deploy`, DaemonSet, the Go collector) need
  `k8sattributes` and `resourcedetection`, which they don't run today [D:
  `deploy/base/*/agent-*.yaml`].
  - Filter the agent's informer to its own node
    (`filter.node_from_env_var`).
  - Associate pods by `k8s.pod.ip` / `connection`.
  - Extract a fixed key list: metadata, `k8s.pod.label.*` for a fixed label
    list, `service.name` / `.version` from `app.kubernetes.io/*`.
- The publishers (Rust `otap-s3pq`, Go `parquetgo`) build the rows. The
  change there is:
  - split each resource's attributes by a configured **covered key list**
    (the keys above, and `k8s.pod.label.<l>` for configured labels);
  - hash the covered part (§3.1);
  - write `resource_id` and the residual map instead of `ResourceAttributes`;
  - keep a small LRU of ids with their first-seen time for the grace window.
    After a restart, everything is "new" for G seconds: a brief byte cost,
    not a correctness issue.
- Cost [E]: one xxh3 over ~1.4 KB per resource per request, since resources
  are per-`ResourceSpans`, not per row. That is less than writing the map,
  which the publisher no longer does. Rust and Go must agree id for id; add
  it to `../conformance` as D7's series id is.
- **No dependence on the controller:** the edge never reads the catalog.
  If the controller's covered set ever differs from the edge's (a config
  drift in label extraction), no id matches and every row degrades to
  residual-only. A monitoring query must catch that: the share of rows whose
  `resource_id` is not in `d_res`, per cluster. This is also why the
  announcement lane (§6.3) should exist.

### 6.2 The controller

- One process per region, with multicluster-runtime and shared informers on
  Nodes, Namespaces, Pods and the workload kinds (Deployments via
  ReplicaSets, StatefulSets, DaemonSets, Jobs, CronJobs) of every cluster.
- For each event it writes SCD2 rows: close the old version, open the new
  one. It also writes the flattened `resources` / `res_index` rows, with the
  same covered key list and hash as the edges, and periodically the
  discovery rows (§3.5). All writes are small batched inserts into
  ReplacingMergeTree tables, idempotent by key, so a restart can replay.
- **Failure modes:**
  - **Down.** New resources degrade to residual-only after G, and heal once
    it catches up. Pods that live and die inside the outage never heal,
    unless the announcement lane exists.
  - **Informer relist after a watch gap.** Deletions inside the gap are not
    seen, so `valid_to` stays open. The effect is cosmetic: the discovery
    rollup overstates live resources. Close versions not seen in a relist.
    **Built 2026-09-27 (AMBIGUITY.md X1):** each informer relist writes a
    `gap` record to the lane (`controller/internal/ctrl`: the resource, from
    that informer's last observed event to the relist's completion, when its
    resource version has moved and the work queue has drained;
    `relist_incomplete` if that takes over 5 min). The aggregator marks the
    cluster's versions whose `valid_from` or `closed_at` falls in the window
    `uncertain = 1` (the `versions_final`, `pods` and `resources` views;
    windows in the `gaps` view), leaving the merge itself unchanged. Pods
    born and dead inside the gap are still missing; the window says where.
    A controller restart is not yet a gap record (its lane is new; the gap
    is between two incarnations' lanes).
  - **Lagging.** The same as down, bounded by the lag.
  - **Covered-key drift against the edges.** Silent mass degradation; alert
    on the unknown-id share.
  - **Multiple controllers.** Idempotent rows, so two are safe; leader
    election is only for the discovery job.
  - **Kubernetes API load.** One watch per kind per cluster, the same as any
    multicluster operator.

### 6.3 The announcement lane (recommended, not built)

- D7's `metrics_series` objects, for resources. A publisher writes
  (`resource_id`, covered set) once per id per hour, **committed before**
  the rows that use the id, and the consumer inserts it into `resources`.
- This makes the catalog complete for everything the edges saw, whatever the
  controller did. The controller then adds lifecycle, topology and the
  normalized levels.
- **Note (2026-09-27, `../model/entityCatalog.qnt`):** a *separate*
  announcement lane closes only the permanent gap (no row stays
  residual-only for good, `noPermanentOrphan`). The consumer ingests
  lanes in any order, so rows can be ingested before their announcement
  and read inexact meanwhile, however long the announcement lane lags
  (`exactAfterLag` fails on the design as written). Writing the
  announcement as an object **in the data lane, ahead of the rows** that
  use it, lets the consumer's in-order ingest take it first and bounds the
  transient gap to the dictionary lag (18–52 s measured; `sameLane`:
  `exactAfterLag` holds). The announcement must be marked sent only once
  its object has committed (`announceEarly` leaves a resource
  unannounced after a lost PUT).
- The flat announced sets can be normalized centrally by key family: the
  cluster, node, workload, pod and container parts each hashed, which
  deduplicates them into the same level dictionaries [E].
- Variant **ann** (`resource_id` over the whole map, no residual column) is
  the extreme of this: rows carry only the id. Its bytes are in §4.1.
  - Its dictionary is flat: 71 k entries for the window. At 90 days the flat
    form doesn't fit (§3.3), so ann needs that central normalization.
  - Rows can't use a grace window, because there is no residual to carry the
    map. A row whose announcement hasn't landed shows an empty map until it
    does, as D7's late series do.

## 7. Next steps

1. **Decide on the rewrite path**: a HyperDX feature request (a resource
   lookup table on log and trace sources), or a proxy. Measure the proxy's
   cost per statement.
2. **Measure on real data**: one real cluster's resource maps and rates,
   stored bytes after merges at full density. The ratio in §4.1 is from
   synthetic, time-sparse rows.
3. **`resource_id` in both edges**, with a conformance test (Rust = Go, id
   for id). The agents gain `k8sattributes`.
4. **The controller**, with SCD2 writes, `res_index`, the discovery job, and
   the unknown-id alert.
5. **The announcement lane** (§6.3).
6. **Dictionary refresh at fleet scale**: `update_field`, a hot/cold split
   (7 days hashed, older via `CACHE` / `DIRECT`), and memory per replica
   under the replicated central (`../central-replicated`).
7. **Run HyperDX live** against variant c with the proxy, when the disk
   allows.

Follow-ups this enables, not designed here:

- **A lake entity → object index.** Edge objects could list the
  `resource_id`s they contain (a few hundred per object). With the catalog,
  a query for "workload X over the last 30 days" could touch only the lake
  objects that hold X's resources (`../lake/DESIGN.md`).
- **Topology-aware routing.** The catalog knows which pods share nodes,
  workloads and namespaces, so it could drive D16's service-affine routing
  and dashboards grouped by topology.

## 8. Other domains, briefly [E]

The same split works wherever a small set of entities emits many rows and a
control plane knows the entities:

- **Serverless.** Function version × region × deployment. The provider's
  API (Lambda `ListVersions`, Cloud Run revisions) is the controller, and
  `faas.instance` goes in the residual.
- **CI.** Pipeline × job × runner. The CI system's API is the controller;
  each job run is a resource version, with short lives like cronjob pods.
- **LLM traces.** Model × deployment × prompt-template version × tool set.
  The gateway's registry is the controller, and `gen_ai.request.model`, the
  template id and hash, and tool schemas become covered attributes. The
  content id keys large, repeated system prompts too.

In each case the rules are the same: content-hash the covered set at the
producer, grace plus announcement for correctness, the rewrite for filters.

## 9. Reproduce

```sh
export CH_CLIENT=<clickhouse binary> AWS_ACCESS_KEY_ID=otel AWS_SECRET_ACCESS_KEY=otelsecret
cd scripts
python3 fleet.py --db ent_cat --create          # 74 s, ~360 MB
python3 telemetry.py                            # ent_src staging, 1 M spans + 1 M logs
python3 dicts.py --build                        # res_index for 90 days + dictionary memory (needs ~4 GB free RAM)
#  shrink ent_cat.pods / res_index to the window (see ent-PROGRESS notes), create sql/dictionaries.sql's dictionaries
curl -X PUT --aws-sigv4 aws:amz:us-east-1:s3 --user otel:otelsecret http://localhost:18333/ent-objects
python3 objects.py                              # 30 Parquet objects per signal per shape
python3 load.py a,b2 --objects 60               # query databases; c = ALTER ... ADD COLUMN ResourceAttributes ALIAS on ent_b2.*_rid
python3 conformance.py b2 ent_b2 ; python3 race.py ; python3 refresh_lag.py
python3 discovery.py ; python3 hdx_strategy.py ent_b2
python3 queries.py --rounds 5
python3 bench.py 6 ; python3 merge.py 4
python3 summarize.py bench|queries|conformance
```

Everything lives in databases `ent_*` and the bucket `ent-objects`; all were
dropped at the end.

## Files

- `sql/`:
  - `catalog.sql`: the SCD2 catalog;
  - `resources.sql`: the flattening and `resource_id`;
  - `dictionaries.sql`: the normalized dictionaries;
  - `generated/{b1,b2,c,ann}_{traces,logs}.sql`: the derived DDL and views.
- `scripts/`: as §2; `recon.py` holds the reconstruction expression, and
  `chlib.py` the helpers.
- `results/`:
  - `bench.jsonl`, `merge.jsonl`, `bench.md`: insert, merge, bytes;
  - `queries.jsonl`, `queries.md`;
  - `conformance.jsonl`;
  - `race.jsonl`, `refresh_lag.jsonl`;
  - `dictionary.jsonl`, `dictionary-layouts.tsv`;
  - `discovery.jsonl`;
  - `hdx_strategy.json`.
