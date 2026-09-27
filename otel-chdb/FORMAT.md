# The on-disk format, version 2

The single reference for what the edges write to the bucket, what the
consumer writes beside it, and how a reader tells versions apart. The
decisions behind it are in [DECISIONS.md](DECISIONS.md) (D3 commit protocol,
D8–D12 consumer, D18 credentials and write-side ABAC, D19 custody and
`complete_through`); the proofs obligations are in
[`model/completeness.qnt`](model/completeness.qnt) and
[`model/s3Inline.qnt`](model/s3Inline.qnt).

Version 2 (2026-09-27) changes three things from version 1: the cluster
comes first in every key (so write access can be scoped by prefix, STPA
R-S7), every object carries `oscope-low` (the edge's custody floor), and
edges commit heartbeat slots (so an idle lane still advances). Together the
last two let a reader publish `complete_through` (R-S1, R-S3).

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
| `oscope-signal`, `oscope-schema`, `oscope-rows`, `oscope-min-time`, `oscope-max-time` | data | the namespace, envelope schema version, row count and the rows' event-time range (ns) |
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

The consumer ingests nothing for a heartbeat; it only moves the
checkpoint and the lane's watermark. GC deletes heartbeats like data slots.

## 3. What the consumer promises: `complete_through`

Per lane, the checkpoint (`{ctl}/ckpt/{lane}.json`) carries, besides the
per-epoch positions:

- `max_low_ns`: the highest `oscope-low` over the slots the checkpoint has
  passed (ingested and verified, or heartbeats);
- `wm_ns`, `wm_wall_ms`: the lane's watermark and when it was computed.

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

## 6. Who may write what (D18, ABAC)

| role | may | may not |
|---|---|---|
| edge publisher (cluster C) | `PutObject` under `{root}/C/*`; `GetObject` and `ListBucket` there (resolve-by-HEAD) | delete anything; touch `{ctl}`, `{entities}` or another cluster |
| entity controller (cluster C) | `PutObject` under `{entities}/C/*` | the same |
| consumer, GC, sealer | read `{root}` and `{entities}`; write and delete `{ctl}`; delete lane slots (GC only); write tombstones into lanes | — |

The cluster comes from the credentials (`${aws:PrincipalTag/cluster}` on
AWS; per-cluster identities elsewhere). Policies and the SeaweedFS
demonstration are in [`deploy/iam/`](deploy/iam/).
