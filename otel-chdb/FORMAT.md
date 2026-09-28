# The on-disk format, version 2

The single reference for what the edges write to the bucket, what the
consumer writes beside it, and how a reader tells versions apart. The
decisions behind it are in [DECISIONS.md](DECISIONS.md) (D3 commit protocol,
D8–D12 consumer, D18 credentials and write-side ABAC, D19 custody and
`complete_through`, D21 resource ids and announcements); the proofs
obligations are in [`model/completeness.qnt`](model/completeness.qnt),
[`model/s3Inline.qnt`](model/s3Inline.qnt) and
[`model/entityCatalog.qnt`](model/entityCatalog.qnt).

Version 2 (2026-09-27) changes three things from version 1: the cluster
comes first in every key (so write access can be scoped by prefix, STPA
R-S7), every object carries `oscope-low` (the edge's custody floor), and
edges commit heartbeat slots (so an idle lane still advances). Together the
last two let a reader publish `complete_through` (R-S1, R-S3).

Envelope schema 2 of traces and logs (2026-09-28, still format 2: no new
object kind, §5) adds the resource columns: every row's `resource_id`, and
the **resource announcements** in the data object itself (§2.1).

## 1. Keys

```
{root}/{cluster}/{producer}/{signal}/{epoch}/{seq:020d}.parquet   lane slots: data, heartbeats, tombstones
{ctl}/format.json                                                  the format marker (§5)
{ctl}/lease/{cluster}/{producer}/{signal}.json                     consumer leases (D8)
{ctl}/ckpt/{cluster}/{producer}/{signal}.json                      consumer checkpoints (D8, D12, §4)
{ctl}/workers/{worker}.json                                        consumer worker heartbeats (D8)
{ctl}/gc.json                                                      GC marks, deletions, retired epochs (D12)
{ctl}/audit/{db}.json                                              horizon-audit state (D11)
{ctl}/watermark.json                                               complete_through (§4)
{entities}/{cluster}/{epochMs}-{instance}/{seq:012d}.delta.ndjson.gz          entity lanes
{entities}/{cluster}/{epochMs}-{instance}/{seq:012d}.sync.{syncAtMs}.ndjson.gz
```

- **`{root}`** is the data root inside the bucket (the path of the edges'
  `s3.url` and of the consumer's `--s3`). **`{ctl}`** defaults to
  `{root}/_consumer`; **`{entities}`** is the entity controller's and
  aggregator's `-prefix` (default `lanes`; any prefix outside `{root}`).
- **A lane** is `{cluster}/{producer}/{signal}`: one publisher's one
  signal. The consumer's lease, checkpoint and watermark are per lane.
  Inside a lane, each writer incarnation (and each of `lanes: N` writer
  lanes of one process) appends to its own epoch.
- **Names.** `cluster` and `producer` match
  `[a-z0-9]([a-z0-9._-]{0,61}[a-z0-9])?`: never empty, never `_`-prefixed
  (keys starting with `_` under `{root}` are control objects, never lanes),
  never containing `/`. Both edges refuse to start otherwise. `producer`
  must be unique in the cluster and stable per durable buffer (the
  StatefulSet pod name; D19). `signal` is one of the namespaces below.
- **Epoch** `YYYYMMDDTHHMMSS.mmmZ-xxxxxxxx`: the UTC wall clock at the
  epoch's first write plus 32 random bits, so epochs sort by start time
  (checkpoint compaction relies on it, D12).
- **Seq** is zero-padded to 20 digits so LIST order is slot order. Every
  slot is written with `PUT If-None-Match: *` and never overwritten.

Signals (namespaces), and which a publisher registers (§3), by metrics layout:

| layout | lanes |
|---|---|
| `series_table` (default), `merge_number_points: true` (default) | `traces`, `logs`, `metrics_number_points`, `metrics_histogram_points`, `metrics_exponential_histogram_points`, `metrics_summary_points`, `metrics_series` |
| `series_table`, `merge_number_points: false` | as above with `metrics_gauge_points` and `metrics_sum_points` in place of `metrics_number_points` |
| `clickstack_tables` | `traces`, `logs`, `metrics_gauge`, `metrics_sum`, `metrics_histogram`, `metrics_exponential_histogram`, `metrics_summary` |

## 2. Slot metadata

Every slot's description is S3 user metadata (`x-amz-meta-<key>`; a HEAD
reads it, LIST does not). Data objects repeat it in their Parquet footer's
key-value metadata, except `oscope-format`, `oscope-cluster` and
`oscope-low`, which say where and when the object was published and are S3
metadata only.

| key | on | value |
|---|---|---|
| `oscope-format` | data, beat | `2` |
| `oscope-kind` | all | `data`, `beat` (heartbeat) or `tomb` (tombstone, written by the consumer) |
| `oscope-cluster` | data, beat | the key's `{cluster}` |
| `oscope-producer` | data, beat | the key's `{producer}` |
| `oscope-epoch`, `oscope-seq` | data, beat | the slot |
| `oscope-content` | data, beat | the content key: BLAKE3-128 hex of `"{signal}\0" + request bytes` (layout-B series objects: of their rows); a heartbeat's is `beat-` + 16 random hex digits |
| `oscope-signal`, `oscope-schema`, `oscope-rows`, `oscope-min-time`, `oscope-max-time` | data | the namespace, envelope schema version (traces and logs `2`, metrics `1`), row count and the rows' event-time range (ns) |
| `oscope-announce` | data (traces, logs) | how many resources the object announces in `resource_announce` (§2.1); the consumer reads announcements only from objects where it is nonzero |
| `oscope-received` | data | `received_at` (ns since the Unix epoch): when the request entered the edge's durable custody, kept across retries and replays (D19) |
| `oscope-low` | data, beat | the custody floor (ns), below |

**`oscope-low`** ([`model/completeness.qnt`](model/completeness.qnt)):

```
low(k) = min( the time slot k's PUT is first sent,
              received_at of every other request in the edge's custody at that moment )
```

computed when the object is encoded for its slot and cached with its bytes,
so a resend into the same slot carries the same value. Its promise:
**every request of this publisher with `received_at < low(k)` was committed
before `k` was sent** (it left custody, which happens only once its slot is
seen committed). Custody is, per edge:

- **Rust, `durable_buffer`** (`custody: durable_buffer`): Quiver's un-acked
  bundles, finalized segments and the open one; the buffer publishes their
  oldest ingestion time on its thread (`patches/0006`), and until it has, an
  object's low is 0.
- **Go, `file_storage` sending queue:** the requests the exporter stamped
  and has not finished, plus those it is replaying; until it has seen its own
  probe come out of each queue (everything persisted before the restart is
  ahead of it) an object's low is 0.
- **Without a buffer:** the requests the exporter holds (custody passes at
  the commit).

A low of 0 is always sound; it only holds the watermark where it is.

**Heartbeats** (`oscope-kind: beat`) are zero-byte slots in the lane's
current epoch, committed like data (same create-only slot protocol,
resolve-by-HEAD, halt on a tombstone), with `oscope-low` and no rows:

- a **birth** heartbeat per registered lane (§1's table) when the exporter
  starts, before it commits any data;
- then one per lane that has committed nothing for `heartbeat.interval`
  (default 30 s).

Residual: "before it takes custody" holds for the Go edge (exporters start
before receivers, and `Start` waits for the births up to `birth_timeout`)
and for the Rust edge without a buffer (requests wait in its inbox), not for
a Rust edge with Quiver, whose receiver and WAL run beside the exporter. It
matters only for a producer whose lanes have never existed (a restart's
lanes are registered already) and whose births cannot commit at its first
start (the store unreachable): until they do, the consumer's minimum does
not include it. A pre-start registration step (an init container) would
close it; not built.

The consumer ingests nothing for a heartbeat; it only moves the
checkpoint and the lane's watermark. GC deletes heartbeats like data slots.

### 2.1 Resources: `resource_id` and announcements (traces and logs)

Two columns sit between the ClickStack columns and the envelope of every
trace and log object (schema 2; Rust `src/schema.rs`, Go `schema.go` /
`pgo.go`):

| column | type | value |
|---|---|---|
| `resource_id` | UINT64 | the content address of the row's resource: `xxh3_64("res.v1\0" ‖ for (k, v) in covered, sorted by k bytewise: k ‖ "\0" ‖ v ‖ "\0")`, seed 0 ([`entities/README.md`](entities/README.md) §3.1) |
| `resource_announce` | MAP(STRING, STRING) | the resource's covered set on the first row of each resource the object **announces**; an empty map on every other row |

- **Covered** is the resource's attributes that the entity catalog can
  reproduce: the FIRST occurrence of each key, kept when the key is one of
  27 fixed names (`k8s.*`, `host.*`, `cloud.*`, `container.image.*`,
  `deployment.environment.name`, `service.name`) or `k8s.pod.label.` plus a
  non-empty rest, and the value is a non-empty string without NUL. Anything
  else is the residual and is never hashed or announced. A resource without
  a covered attribute has the id of the empty set and is never announced.
  The definition is the entity controller's `internal/rid` (`Split`, `ID`);
  [`entities/testdata/resource_id_vectors.json`](entities/testdata/resource_id_vectors.json)
  is the vector file the controller, both edges and ClickHouse's own
  expression agree on (hostile cases included).
- **When a resource is announced.** Each writer lane (one per `{signal,
  lane}`, so per epoch) keeps a cache of the resources it has announced in
  its **current epoch**. An object announces every resource it uses that the
  cache does not hold for the request's window (`resources.window`, 1 h, of
  `received_at`), decided when the object is encoded for its slot. The cache
  marks them only once that object has **committed** (resolved as ours, or
  found committed with this encoding); an object whose outcome is unknown
  marks nothing, so the next object of the lane announces again. A new
  epoch starts with an empty cache. The cache is bounded
  (`resources.cache_size`, 65,536 per lane); what it evicts is announced
  again. Re-announcing is always safe, so every doubt resolves that way.
- **Why in the data object** ([`model/entityCatalog.qnt`](model/entityCatalog.qnt),
  `sameObject`): an announcement then commits exactly when the rows that
  need it do, in the data's own lane and epoch, with no extra PUT, and the
  consumer, which ingests a lane in slot order, inserts an object's
  announcements before its rows. So a row never reaches central before the
  announcement of its resource, and it is exact once the dictionaries have
  loaded (`exactAfterLag`), whatever the entity controller saw. A separate
  announcement lane would have allowed rows ahead of their announcement
  (`catalogDesign`: `exactAfterLag` fails).
- **What the consumer does** (`src/consumer/worker.rs` `announce_first`):
  for the trace and log objects of a round with `oscope-announce` > 0, one
  statement per group inserts `(resource_id, resource_announce, signal,
  producer_id, producer_epoch, batch_id, received_at)` into
  `{db}.otel_resources` (a ReplacingMergeTree: an announcement re-inserted is
  one row; `sql/otel_resources.sql`, with the per-resource view
  `otel_resources_announced`), before the round's row inserts. A lane whose
  announcement statement did not surely land (an error, no answer, an answer
  past its fence) inserts nothing that round; an unanswered one is waited
  out like any statement (D9). The entity aggregator merges the view with
  the controller's catalog ([`entities/controller/sql/announced.sql`](entities/controller/sql/announced.sql)).

## 3. What the consumer promises: `complete_through`

Per lane, the checkpoint (`{ctl}/ckpt/{lane}.json`) carries, besides the
per-epoch positions:

- `max_low_ns`: the highest `oscope-low` over the slots the checkpoint has
  passed (ingested and verified, or heartbeats);
- `wm_ns`, `wm_wall_ms`: the lane's watermark and when it last moved
  (a watermark that did not move is not written again).

The lane's holder computes the watermark at each full listing of the lane
(every `--full-list`, 30 s): with `M` = `max_low_ns` as it stood before the
LIST, and `U` = the lowest `oscope-received` over the data slots that LIST
shows and the checkpoint has not passed,

```
wm = min(M, U)      (U = +inf when nothing is pending; not computed when a
                     pending slot's metadata is unknown: past a gap or past
                     the HEAD budget)
```

Every request of the lane with `received_at < wm` is ingested: it is either
still in custody (then `received_at >= M`, by `oscope-low`), or committed
before the slot that set `M` was sent, so visible to the LIST, so ingested
or counted in `U`. This is completeness.qnt's ingested-prefix rule without
an order between epochs, which several writer lanes of one process need.

`consume gc` then publishes, every run, `{ctl}/watermark.json` (CAS by
ETag):

```
complete_through_ns = max(previous, min(t_list - skew, min over lanes of wm_ns))
```

over every lane directory a LIST of `{root}` shows at `t_list` (a lane
without a checkpoint or watermark counts as 0); `skew` (`--wm-skew`,
default 5 s) covers the edges' clocks against the consumer's, for a lane
born after the LIST. The value is a **running max**: the recomputed minimum
dips when a zombie PUT lands late (completeness.qnt `wRegress`), and the
published one does not follow it. The document also names the lanes holding
it back (`holding`), and those whose watermark is older than `--wm-stale`
(default 5 min) (`stale`), and the same is exported as metrics
(`consumer_complete_through_seconds`, `consumer_watermark_stale_lanes`,
`consumer_lane_watermark_lag_seconds{lane}` for the stale ones).

A reader that gates on it (an alert evaluator, R-S3) evaluates a window
only once its end is at or below the serving source's `complete_through`,
pages on a stalled watermark, and labels every result with its source and
that source's value (completeness.qnt `evalWithinComplete`,
`resultLabeled`). The lake's sealer publishes its own, by the same rule.

## 4. Control documents

| key | writer | shape |
|---|---|---|
| `format.json` | consumer (create-only, at start) | `{"format": 2, "layout": "{cluster}/{producer}/{signal}/{epoch}/{seq:020d}.parquet"}` |
| `lease/…` | consumer workers (CAS) | `{lane, owner, epoch, beat, ttl_ms, wall_ms}` (D8) |
| `ckpt/…` | the lane's lease holder (CAS) | `{lane, lease_epoch, version, floor, epochs: {epoch: {next, closed}}, max_low_ns, wm_ns, wm_wall_ms}` |
| `workers/…` | each worker (plain PUT) | `{worker, beat, wall_ms, load, lanes}` |
| `gc.json` | `consume gc` (CAS) | `{version, marks, deleted_below, retired}` (D12) |
| `audit/{db}.json` | `consume horizon-audit` | reported copies (D11) |
| `watermark.json` | `consume gc` (CAS) | `{format, version, complete_through_ns, computed_ns, wall_ms, list_cap_ns, lanes, holding: [{lane, wm_ns, lag_s}], stale: [...], stale_after_s}` |

## 5. Versioning and compatibility

**Version 2 is a clean break; version 1 is not read.** No version-1 data
exists outside test buckets, and a transition reader would have to keep two
lane namings, two discovery walks and two sets of ABAC policies alive for
nothing. What guards the break:

- `{ctl}/format.json`: a consumer or GC creates it (`If-None-Match: *`)
  when absent and refuses to start when it names another format;
- every data and heartbeat object carries `oscope-format: 2`;
- a version-1 key (`{root}/{producer}/{signal}/{epoch}/{seq}.parquet`) seen
  through the version-2 walk puts an epoch name where a signal is expected
  and is ignored (the consumer only takes lanes whose third segment is a
  known signal); a version-1 edge cannot be pointed at a version-2 bucket by
  accident, because version-2 configurations name the cluster and the
  producer separately from the URL.

A version-1 bucket is drained with a version-1 consumer, or purged
(`consume purge`), before version 2 is deployed on it. A future version 3
bumps `oscope-format` and `format.json`, and adds a reader for both only if
real data then exists.

**The resource columns (2026-09-28) are not a format change.** No object
kind, key, lane or control document is new: announcements ride in data
objects, `oscope-announce` is one more metadata key, and the object's own
`oscope-schema` says which columns it has (traces and logs `2`). Both
directions read: a schema-1 object gives `resource_id` 0 and no
announcements (ClickHouse fills a column the Parquet file lacks with its
default, `input_format_parquet_allow_missing_columns`, on in 26.10 [M]), and
a consumer from before reads schema-2 objects and ignores the two columns
(its `s3()` structure names the columns it takes). Central's `otel_traces`
and `otel_logs` gain `resource_id` (`ALTER TABLE … ADD COLUMN resource_id
UInt64` on a table created before, or recreate it: no real data exists).

## 6. Who may write what (D18, ABAC)

| role | may | may not |
|---|---|---|
| edge publisher (cluster C) | `PutObject` under `{root}/C/*`; `GetObject` and `ListBucket` there (resolve-by-HEAD) | delete anything; touch `{ctl}`, `{entities}` or another cluster |
| entity controller (cluster C) | `PutObject` under `{entities}/C/*` | the same |
| consumer, GC, sealer | read `{root}` and `{entities}`; write and delete `{ctl}`; delete lane slots (GC only); write tombstones into lanes | — |

Announcements need no policy of their own: they are in the data objects, so
an edge can announce resources only in its own cluster's prefix, and the
consumer inserts them with the rows' credentials. That a resource's
`k8s.cluster.uid` is the publishing cluster's is not checked (as for
`ResourceAttributes` today).

The cluster comes from the credentials (`${aws:PrincipalTag/cluster}` on
AWS; per-cluster identities elsewhere). Policies and the SeaweedFS
demonstration are in [`deploy/iam/`](deploy/iam/).
