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
{ctl}/watermark.json                                               complete_through (§3, §4)
{ctl}/watermark/{cluster}.json                                     one cluster's complete_through, per signal and per lane (§3, D29)
{entities}/{cluster}/{epochMs}-{instance}/{seq:012d}.delta.ndjson.gz          entity lanes
{entities}/{cluster}/{epochMs}-{instance}/{seq:012d}.sync.{syncAtMs}.ndjson.gz
{root}/{cluster}/_index/v1/{signal}/{hour}/L{level}-{h}.osix             lake index segments (§7)
{root}/{cluster}/_index/v1/{signal}.progress.json                        the indexer's progress (§7.4)
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
| `oscope-part`, `oscope-late-after` | data (traces, logs), split requests only | `bulk` or `late`, and the split's bound in ns (§2.2); absent on an object that holds its whole request |

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

### 2.2 Late rows in their own object (traces and logs; D31)

An object's `oscope-min-time`/`oscope-max-time` span all its rows, so one
old row (a late batch, a replay, a skewed clock) would stretch its range
over every window in between. Both edges therefore split a traces or logs
request whose rows reach back more than a bound B (`late_split_after`,
default 15 min, 0 turns it off) before its **newest** row:

- `M` = the highest event time over the request's rows (a span's start, a
  log record's time or, when 0, its observed time; compared as unsigned
  64-bit ns), `cut = M − B`;
- no row below `cut` (or `M < B`): one object, as always;
- otherwise two data objects in the request's lane, appended in order: the
  **bulk** (rows with event time ≥ `cut`) and then the **late** part (the
  rest). Each is an ordinary data slot: its own content key, rows, time
  range, `row_ordinal` 0..n−1, announcements and `oscope-low`; both carry
  the request's `received_at`, `oscope-part` and `oscope-late-after`. The
  request is acknowledged only once both have committed, like a metrics
  request's objects.
- Content keys: BLAKE3-128 of `"{signal}/{part}/{B}\0"` + the request's
  bytes (B in ns; an OTAP request without canonical bytes: of the part's
  rows under that name). A signal holds no `/`, so a part never shares a key
  with a whole request.

The cut is relative to the request's own newest row, not to `received_at`,
so it depends only on the request's bytes and B: a retry or a replay (with
or without a persistent queue) splits the same rows the same way and finds
its committed parts by their keys. Changing B while requests wait in a
queue gives the replays other keys: at worst duplicates, never a loss. Nothing a reader relies on changes: every object's metadata
stays honest for its own rows, the consumer ingests the parts as two
slots, and `oscope-low` and `complete_through` are per slot as before.
Metrics requests are not split.

Central keeps the parts apart too ([D34](DECISIONS.md#d34-centrals-partition-key-todatereceived_at-late_part-late-parts-in-partitions-of-their-own)):
the consumer writes `late_part` = 1 for an object whose `oscope-part` is
`late` (0 for anything else) into traces and logs tables partitioned by
`(toDate(received_at), late_part)`, so late rows never merge into the
bulk's parts, and never in a statement with bulk objects.

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

**Per cluster, per signal, per lane ([D29](DECISIONS.md#d29-complete_through-per-cluster-per-signal-and-per-lane)).**
The same minimum over a subset of the lanes is sound for the requests of
that subset: a lane's watermark speaks only for its own requests, and a lane
of the subset born after the LIST is above `t_list − skew` like any other.
So each run also publishes, each a running max like the fleet value and
floored by the coarser one (a coarser value is sound for every subset):

```
clusters[c]          = max(previous, fleet, min(t_list - skew, min over c's lanes))
signals[s]           = max(previous, fleet, min(t_list - skew, min over the lanes of signal s))
unlisted_signals_ns  = max(previous, fleet, t_list - skew)      a signal no listed lane carries
```

in `watermark.json`, and one document per listed cluster,
`{ctl}/watermark/{cluster}.json` (CAS by ETag, written after the fleet one),
with that cluster's value, its per-signal values (the minimum over the
cluster's lanes of that signal), its unlisted-signal value, and every lane's
published value `lane_wm["{producer}/{signal}"]` (floored by its signal's).
A cluster or signal first listed takes the previous fleet (or unlisted)
value as its floor. A per-cluster document that fails to write is reported
(`consumer_cluster_watermark_errors_total`) and keeps its previous values,
which stay sound. `consume … --no-cluster-watermarks` writes only the fleet
document. Metrics: `consumer_cluster_complete_through_seconds{cluster}`,
`consumer_cluster_complete_through_lag_seconds{cluster}`.

A reader scoped to some clusters and signals takes, per cluster, the highest
value published for a superset of its lanes (the fleet's, the cluster's,
the cluster's per signal) and the minimum over its clusters; a cluster
without a document falls back to the fleet value, never above it. One
cluster's stalled lane then holds its own cluster, the fleet value and the
signals it carries, and nothing else. The query service does this
(`query/internal/completeness` `Reader.For`).

**Reading at a basis (D30).** Because every value above is sound for its
lanes and never goes back, a reader can name one (per cluster, `C`) and read
only rows with `received_at < C`: that set never grows. The bound is
**strict**: the promise is for `received_at < wm`, and a pending object may
carry `received_at == wm` exactly. The query service does this for
`/v1/query` (a per-row filter on the tables' `received_at`) and `/v1/plan`
(an object's `oscope-received`, from its HEAD or, for an object it could
not HEAD, from the Parquet footer by the reader, §2). A row with
`received_at < C` was ingested before the document that allowed `C` was
written, so its slot's `LastModified` precedes that write; the planner uses
this (with its clock skew bound) to leave newer objects out without a HEAD.

A reader that gates on it (an alert evaluator, R-S3) evaluates a window
only once its end is at or below the serving source's `complete_through`
for the window's scope, pages on a stalled watermark, and labels every
result with its source and that source's value (completeness.qnt
`evalWithinComplete`, `resultLabeled`, `clusterSound`). The lake's sealer
publishes its own, by the same rule.

### 3.1 Retiring a lane (designed, not built: [D35](DECISIONS.md#d35-dead-lane-retirement-a-proof-of-empty-custody-then-quarantine-below-the-bound-designed))

A lane that stops advancing holds its cluster's `complete_through` (and the
fleet's) at its watermark for good, and is paged as `stale`: from S3 an idle
lane, a slow one and a dead publisher with a full buffer look the same.
Retiring a lane takes it out of the minimum. It needs a **proof that the
lane's custody is empty**, and a **bound** on what may still come from it.
Model: [`model/retirement.qnt`](model/retirement.qnt)
(`model/retirement_model.sh`).

**Two proofs, nothing else.**

1. **An orderly close (the publisher's).** At shutdown the publisher stops
   its receivers (not ready; for a StatefulSet, before the scale-down),
   drains its buffer (every request committed and seen committed: Quiver
   empty, `patches/0006`'s custody floor at "none"; the Go queue empty),
   and only then commits, in each of its lanes' current epochs, one last
   slot with `oscope-kind: close` and `oscope-low` = its custody floor,
   which is the close time since custody is empty. A close says: nothing is
   in custody, nothing will enter it (the process is exiting), no PUT of
   this incarnation is in flight (all were acked). A close committed with
   custody left is the mutant `closeUndrained` (unsound). Once the lane's
   holder has passed the close (it and every slot before it ingested, and
   nothing after it listed), the lane is retired at **R = the close's
   low**, recorded in its checkpoint (`retired_ns`, `retired_epoch`).
2. **An operator's retirement, with evidence** (a publisher that died
   without a close: a crash, an eviction, a lost node). `consume
   retire-lane --lane {cluster}/{producer}/{signal} --evidence "…"`, taken
   only when (a) the publisher's volume is **deleted** (its custody lost:
   an acknowledged loss, reported with the count the last heartbeat or
   metrics showed), (b) the process has been gone longer than a request
   lifetime (GC's `zombie_ms`: no PUT of it can still land), after which the
   consumer tombstones the lane's newest epoch's head as it does for a
   superseded one, and (c) the lane's holder has passed every slot the lane
   shows (a zombie PUT that landed before the retirement is ingested, not
   quarantined: the model's first finding). The lane is retired at **R =
   the retirement's time**, which (b) puts strictly after the death, so
   above every `received_at` the dead process held (retiring at the
   death's own instant let a replay received at exactly R through: the
   model's third finding). Without (a) the design refuses it: an operator
   who retires a lane whose volume is kept is the model's `opMistake`
   (below).

A lane that merely stopped (stale for however long) is never retired: that
is the mutant `retireStale`, and the custody it held lands, or not, below
a value already published.

**While retired**, the lane counts as +inf in every minimum of §3 (fleet,
its cluster, its signal): its cluster's value then follows its other lanes
and the cap `t_list − skew`. Its `lane_wm` entry says `retired` with R.

**Coming back.** A lane is retired only up to its retired epoch: when an
object of a later epoch is listed (a new incarnation's birth: StatefulSet
ordinal reuse, the same producer id), it counts again, from that birth (the
mutant `staysRetired` leaves it out, and its new requests pass unread).
That is sound for the same reason a new lane is (§3's "born after the
LIST"): the birth is committed before the new incarnation takes custody, so
its new requests are received after it, above anything published from a
LIST that did not show it.

**The bound: quarantine, never ingest below R.** An object of a retired
lane (any epoch) whose `oscope-received` is below R can only be custody the
retirement said did not exist: a volume that was kept after all and adopted
by a new pod, which replays it with its original `received_at` (D19). Such
an object is **quarantined**: not ingested into central, recorded in
`{ctl}/quarantine/{lane}.json` (slot, content key, rows, received_at) and
paged (`consumer_quarantined_objects_total`); its slot counts as passed for
the checkpoint and the watermark. The alternatives, for the record:
*ingest it* is the hazard (`ingestBelow`: rows appear below a
`complete_through` already published; answers at an earlier basis (D30)
change and alert windows already evaluated OK were wrong, silently);
*refuse it* (leave it unread in the lane) blocks the lane's checkpoint for
good. Admitting a quarantined object is an operator's decision, taken
knowing it re-opens those windows (`consume admit` would insert it and
report the bases and windows it touches: not designed further). With the
evidence the design requires, the quarantine stays empty (the model checks
`quarantineOnlyOnMistake`); an operator's mistake is safe in the sense
that matters for readers (`noLateBelow`: nothing is ever ingested below a
published value), and loses completeness only for the requests it
wrongly declared gone, until they are replayed into the quarantine.

**Also found by the model, without any retirement:** a volume must not be
deleted while a PUT of its dead process can still land. A lost request's
PUT that lands after `complete_through` passed it would be ingested below
it. The scale-down runbook's "delete the PVC only after the drain" gains:
and only once the pod has been gone longer than a request lifetime.

Nothing of this is built yet; building it needs: the close slot in both
edges (the Rust exporter after `durable_buffer` reports an empty custody
at shutdown, the Go exporter after its sending queue drains in
`Shutdown`), with a shutdown budget longer than the drain (else no close,
and the lane stays stale: safe), and the conformance run comparing them;
the consumer's `Found::Close`, the retirement in the checkpoint, the
+inf in `watermark.rs`, the un-retirement on a later epoch, the quarantine
path in the worker and its document and metric; `consume retire-lane`
with the tombstone and the three checks; a DST mutant per model mutant.

## 4. Control documents

| key | writer | shape |
|---|---|---|
| `format.json` | consumer (create-only, at start) | `{"format": 2, "layout": "{cluster}/{producer}/{signal}/{epoch}/{seq:020d}.parquet"}` |
| `lease/…` | consumer workers (CAS) | `{lane, owner, epoch, beat, ttl_ms, wall_ms}` (D8) |
| `ckpt/…` | the lane's lease holder (CAS) | `{lane, lease_epoch, version, floor, epochs: {epoch: {next, closed}}, max_low_ns, wm_ns, wm_wall_ms}` |
| `workers/…` | each worker (plain PUT) | `{worker, beat, wall_ms, load, lanes}` |
| `gc.json` | `consume gc` (CAS) | `{version, marks, deleted_below, retired}` (D12) |
| `audit/{db}.json` | `consume horizon-audit` | reported copies (D11) |
| `watermark.json` | `consume gc` (CAS) | `{format, version, complete_through_ns, computed_ns, wall_ms, list_cap_ns, lanes, holding: [{lane, wm_ns, lag_s}], stale: [...], stale_after_s, clusters: {cluster: ns}, signals: {signal: ns}, unlisted_signals_ns}` (the last three since D29; a reader treats them as absent in older documents) |
| `watermark/{cluster}.json` | `consume gc` (CAS) | `{format, version, cluster, complete_through_ns, computed_ns, wall_ms, list_cap_ns, lanes, signals: {signal: ns}, unlisted_signals_ns, lane_wm: {"{producer}/{signal}": ns}, holding, stale, stale_after_s}` (D29) |

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

**The late split (D31, 2026-09-28) is not a format change** either: its
objects are ordinary data slots, and `oscope-part` / `oscope-late-after`
are two more metadata keys that no reader needs.

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
| lake indexer (cluster C) | `GetObject`, `ListBucket` under `{root}/C/*`; `PutObject` under `{root}/C/_index/*` ([`deploy/iam/indexer.json`](deploy/iam/indexer.json)) | delete anything; write lanes, `{ctl}` or another cluster; an edge may not write `{root}/C/_*` (`edge-publisher.json`, 2026-09-28) |

Announcements need no policy of their own: they are in the data objects, so
an edge can announce resources only in its own cluster's prefix, and the
consumer inserts them with the rows' credentials. That a resource's
`k8s.cluster.uid` is the publishing cluster's is not checked (as for
`ResourceAttributes` today).

The cluster comes from the credentials (`${aws:PrincipalTag/cluster}` on
AWS; per-cluster identities elsewhere). Policies and the SeaweedFS
demonstration are in [`deploy/iam/`](deploy/iam/).

## 7. Index objects (D27)

The lake indexer (`query/cmd/lakeindex`, package `query/internal/lakeidx`)
writes, per cluster, create-only **segments** that map trace ids and log
body terms to (object, row group). They live under the cluster's own
prefix, so reading or writing a cluster's index needs that cluster's grant
and nothing else (§6), and no lane walk takes them for a lane (`_` prefix,
§1).

### 7.1 Keys

```
{root}/{cluster}/_index/v1/{signal}/{hour}/L{level}-{h}.osix     segments
{root}/{cluster}/_index/v1/{signal}.progress.json                the indexer's progress (a hint)
```

- **`{signal}`** is `traces` (the `TraceId` column) or `logs` (`TraceId` and
  `Body`).
- **`{hour}`** (`YYYYMMDDTHH`, UTC) is the hour of the covered objects'
  LIST `LastModified`: the commit time, which a planner knows for every
  candidate object without a HEAD, and which a late object cannot move.
- **`{level}`** is 0 for a segment built from source objects and 1 for an
  hour's merge. **`{h}`** is the first 128 bits of SHA-256 of the segment's
  bytes (hex), so two indexers that build the same bytes meet at one key
  and the second `If-None-Match: *` PUT is a 412 that means "already there".
  A segment whose bytes fail verification is rebuilt beside it as
  `…-r{n}.osix`.

### 7.2 A segment

```
"OSIX" 01 00 00 00                   magic, segment format 1
trace blocks                         (fp32 delta, row group) varints, sorted by fp; ~4 KiB each
term blocks                          front-coded terms, each with its posting inline; ~32 KiB each
header                               JSON
u32le len(header) | u32le crc32c(header) | "OSIX"
```

The header names what is indexed and where the blocks are:

| field | meaning |
|---|---|
| `cluster`, `signal`, `bucket`, `level` | must equal the key's; a reader refuses a segment that names another place |
| `objects` | every covered object: `key`, `size`, `etag`, `lm_ms`, `rg_rows` (rows per row group, file order) and `rg_base`, the global ordinal of its first row group |
| `sources_hash` | SHA-256 over `key\0size\0etag\0` of `objects`, in order |
| `trace` | `column`, `entries`, `fp_bits` (32) and the fence table `blocks: [{first, off, len, crc, n}]` |
| `terms` | `column`, `terms`, `tokenizer` (`jslower-ascii-word-v1`), `max_term` (64), `freq_cut` (0.5), `freq_min_rgs` (8), `frequent`, and `blocks: [{first, off, len, crc, n}]` |

- **Trace entries** are `(FP(id), global row group)`, deduplicated, where
  `FP` is the top 32 bits of FNV-1a 64 over the id's ASCII-lowercased text.
  One fingerprint never spans two blocks, so a lookup is one binary search
  in the (cached) header and **one range GET** of one block. A false match
  (another id with the same fingerprint in the segment) costs one row group
  read; its probability per lookup is about `entries / 2^32`.
- **Terms** are the tokens of the column as the lake UI's search sees text
  (`lakeidx/fold.go`): JavaScript `toLowerCase`, then every character that
  is not an ASCII letter, digit or `_` separates tokens. Only U+0130 and
  U+212A lowercase to anything ASCII, so the indexer folds without Unicode
  tables; `lakeui/test/fold.test.js` asserts that against the browser engine
  and writes the shared vector file
  (`query/internal/lakeidx/testdata/fold_vectors.json`). No normalization;
  invalid UTF-8 is U+FFFD (a separator) on both sides. Each entry is
  `varint shared | varint suffix_len | suffix | varint count+1 | delta
  varint row groups`; count+1 = 0 means **every row group** (a term in more
  than `freq_cut` of the segment's row groups, when it has at least
  `freq_min_rgs`: it cannot narrow anything). The first entry is the empty
  term: the row groups holding a token longer than `max_term`, which are
  not in the dictionary.
- **Every block** carries a CRC-32C in the header; the header has its own in
  the trailer. A reader that finds a mismatch treats the whole segment as
  absent: its objects are scanned.

### 7.3 What a reader may conclude

For each candidate object, the reader LISTs the object's hour, reads the
headers (cached by key and size), and picks the segment that covers the
object (most objects first, then higher level, then key). The object is
covered only if the segment lists its key with the same size (and ETag when
both sides know one). Then, and only then:

- a trace id whose fingerprint has no entry for the object's row groups, or
  a term filter some constraint of which (below) no dictionary entry
  satisfies for them, means **no row of the object matches**;
- otherwise the matching row groups are the union of the entries' row groups.

A text term becomes constraints: the folded text is `[s] r0 s r1 … s rn
[s]`; a middle run must equal a token (**full**), `r0` must end a token
(**suffix**) unless the text starts with a separator, `rn` must start one
(**prefix**) unless it ends with one, and a single run without separators
may lie inside a token (**infix**). Every body containing the text holds all
of them (`TestConstraintsHoldOnEveryMatchingBody`). Full and prefix lookups
read the fenced blocks; suffix and infix read the whole dictionary (one
range GET, cached). The long-token entry answers every partial constraint.

Everything else (no covering segment, a size mismatch, a segment that does
not verify or cannot be read, the reader's byte budget spent) means
**scan**: read the object as if there were no index. An index never
removes a match.

### 7.4 Progress

`{signal}.progress.json` (CAS by ETag, `If-None-Match: *` when absent):
`{format, cluster, signal, lanes: {producer: {epoch: next}}, indexed_through_ms, wall_ms, unindexable}`.
Every slot of an epoch below `next` is covered, empty (a heartbeat or a
tombstone) or unindexable (not decodable; listed, scanned by readers). A
pass skips those slots; everything at or after `next` is checked against the
hour's segments before it is read. Two indexers merge (per-epoch max) on a
CAS conflict. **Readers never use it**: what is indexed is exactly what the
segments' headers list. `indexed_through_ms` (LIST time − 60 s, or the
oldest uncovered object's `LastModified`) is the lag metric's input only.

### 7.5 Merges and retention

An hour's segments are merged into one `L1` segment `merge_after_s` (600 s)
after the hour ends, if no single segment already covers the hour; a
straggler L0 written later makes a new L1 at the next pass. The L0s stay (a
reader prefers the segment covering more). Nothing deletes segments yet:
index GC follows the lanes' GC by hour, not built (AMBIGUITY X14).
