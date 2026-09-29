# Publisher scale-down with a durable buffer

A proposal and a runbook, not a controller (AMBIGUITY.md, row "Kubernetes
StatefulSet scale-down with a buffer volume"). Alert rules:
[`orphaned-buffer.rules.yaml`](orphaned-buffer.rules.yaml).

## What happens today

- A StatefulSet removes its highest ordinal. The pod stops with a 45 s
  grace; whatever its buffer had not committed stays on the PVC, which is
  kept (`persistentVolumeClaimRetentionPolicy.whenScaled: Retain`).
- Those requests were **acked** to the agents when they entered the buffer
  (the Quiver WAL write, the Go persistent queue's enqueue). The agents have
  forgotten them. They are committed only when a publisher mounts the
  volume again: the same ordinal after a scale-up replays its WAL or queue
  and commits them in a new epoch, `received_at` kept (D19), and the
  consumer's check skips any that had already committed.
- **If the ordinal never returns, that data is never committed**, and
  nothing reports it: the publishers' buffer metrics disappear with the pod,
  and kubelet volume statistics exist only for mounted volumes. It is an
  unknown outcome with no settle bound — acked, not visible, not lost yet.
- The longer it waits, the older its custody age: past the hot tier it
  lands cold, past the TTL it is inserted and dropped at the next TTL merge
  (DECISIONS.md D19, R-S6).

## Rule 1: drain before scale-down (proposed procedure)

The ring and the agents follow the pods, so a publisher cannot be drained by
the StatefulSet itself. Until an operator-side drain exists, scale down by
hand, one ordinal at a time, only while S3 is healthy:

1. **Stop new traffic to the top ordinal.** With `components/routing` the
   gateways follow ready EndpointSlice endpoints; without it the agents
   re-resolve on connection failure. Make the pod unready (proposed:
   a `drain` file that `edgeprobe` treats as not ready, e.g.
   `kubectl exec otap-publisher-N -- touch /var/lib/otap-s3pq/buffer/.drain`;
   `edgeprobe` has no such flag yet — the readiness work owns it).
2. **Wait for the buffer to empty.** Rust: `storage_bytes_used_bytes`
   (`otel_scope_name="processor.durable_buffer"`) back to its floor and
   no `flush_failures_total` increase; Go: `otelcol_exporter_queue_size{exporter="s3pq"}` = 0.
   These are observations with freshness: read them twice, a scrape
   interval apart, both at the floor.
3. **Scale down** (`kubectl -n otel-edge scale sts/otap-publisher --replicas=N`).
4. **Only then delete the PVC**, and only if step 2 held. If in doubt keep
   it: a kept PVC costs disk, a deleted non-empty one loses acked data.
   And only once the pod has been gone longer than a request lifetime (the
   consumer's GC `zombie_ms`): a PUT the dead process still had in flight
   can land after the consumer's `complete_through` passed its request,
   which would put its rows below a published value (found by
   `model/retirement.qnt`; FORMAT.md §3.1).

## The orderly close (D35, built)

Since 2026-09-29 a publisher stopped in order ends each of its lanes with a
**close** (`oscope-kind: close`, [FORMAT.md](../../FORMAT.md) §3.1), and the
consumer then **retires** those lanes: they leave their cluster's (and the
fleet's) `complete_through` minimum, which moves on with the publishers
left, instead of holding at the removed one for good (paged as `stale`).
The close is committed only when the publisher's custody is empty at
shutdown: Rust, its Quiver buffer drained and every request committed and
acknowledged back; Go, its sending queue drained and nothing left in its
custody ledger. So Rule 1 still applies, and the close is its proof:

- The pod reports not ready as soon as its shutdown starts (the Rust
  engine's `ShutdownRequested`, the collector's pipeline-not-ready), before
  its receivers stop, then drains. The drain and the close must fit in
  `terminationGracePeriodSeconds` (45 s in `base/`; the Rust engine's own
  drain deadline is 60 s): after Rule 1 step 2 the buffer is empty and the
  close takes one PUT per lane. If the kubelet kills the pod first, there is
  no close and the lane stays stale: safe, and handled as below.
- **Check the retirement** after the scale-down: the consumer logs `lane
  {cluster}/{producer}/{signal} retired by its publisher's close`, counts it
  (`consumer_lane_retirements_total{event="closed"}`), and
  `{ctl}/watermark/{cluster}.json` names the producer's lanes under
  `retired` with R. `consumer_watermark_retired_lanes` counts them.
- **No close** (the edge logs `close: none (…)`: custody not empty, the
  buffer's drain unfinished, a NACK it did not see; or no log line at all:
  killed): scale that ordinal back up (§Orphaned buffer), let it drain, and
  scale down again. For a node and volume that are gone for good, see
  §A lost node.
- A later pod with the same ordinal (the same producer id) starts a new
  epoch; its birth puts the lane back in the minimum (`reborn`), and
  whatever its volume replays that was committed before the close is passed
  as a copy. Anything below R that central does not hold is **quarantined**
  (see §Quarantine), never ingested.

## A lost node: `consume retire-lane`

A publisher that died without a close (a crash, an eviction, a lost node)
holds its lanes stale for good (`consumer_watermark_stale_lanes`, the
lanes in `holding`), and its cluster's alert windows with them. If its pod
comes back with its volume, nothing to do: it replays and the lanes move
again. If the **volume is gone for good**, or you decide to give up its
custody, retire each of its lanes (every registered signal of the
producer: `holding`/`stale` list them):

1. **Delete the volume first** (or confirm it is gone: the node and its
   local disk lost, the PVC and PV deleted). Its un-committed requests are
   lost; that is the acknowledged loss the retirement records. Confirm the
   pod is gone and has been for longer than a request lifetime (GC's
   `--zombie`, 10 min by default).
2. Make sure the consumer is running and has caught up with the lane (its
   checkpoint passed every slot the lane shows).
3. For each lane:
   ```
   consume retire-lane --s3 … --lane {cluster}/{producer}/{signal} \
     --volume-deleted --evidence "PVC buffer-otap-publisher-3 deleted 14:02Z, node ip-10-… terminated (INC-1234)" \
     [--zombie 10m] [--dry-run]
   ```
   It refuses, with the reason, unless: (a) `--volume-deleted` and
   `--evidence` are given (it cannot check Kubernetes; your attestation is
   recorded in the lane's checkpoint and in
   `{ctl}/retired/{lane}/{wall_ms}.json`); (b) the lane wrote nothing for
   `--zombie`; (c) the consumer has passed every slot of the lane. Then it
   tombstones the lane's open epochs and records R = now. `--dry-run` runs
   the checks and writes nothing. The record says from which `oscope-low`
   on the dead publisher's requests may have been lost.
4. Check: the next watermark run lists the lanes under `retired` in
   `{ctl}/watermark/{cluster}.json` and the cluster's `complete_through`
   moves. The lane's holder drops the lane once (its checkpoint changed
   under it) and takes it again after a lease TTL.
5. If the volume turns out to have been kept (another pod mounts it and
   replays), whatever it replays below R is **quarantined**, not ingested
   (§Quarantine): the retirement's mistake is safe for readers.

## Quarantine

`ConsumerQuarantinedObjects` (page, `../alerts/consumer-retirement.rules.yaml`):
an object of a retired lane received below its R that central did not
hold. The worker did not ingest it; it is listed in
`{ctl}/quarantine/{cluster}/{producer}/{signal}.json` (slot, content key,
rows, `received_at`). Its request was acknowledged by a publisher whose
custody the retirement said was empty: a close committed with custody left
(a bug: report it with the edge's log), or an operator's retirement of a
lane whose volume was kept after all. Nothing a reader relies on has
changed (it is below published values and not in the main tables). GC
keeps the object.

**Admitting it** is a separate decision. Its rows go to a **recovered
table**, never the main one, and what they would have changed is reported:

```
consume admit --s3 … --ch URL --db DB --lane {cluster}/{producer}/{signal} --dry-run   # the report alone
consume admit --s3 … --ch URL --db DB --lane {cluster}/{producer}/{signal}
```

It inserts each quarantined object not admitted yet into
`{table}_recovered` (same columns, plus `recovered_at`, `retired_lane`,
`quarantine_ref`), idempotently, marks it admitted in the quarantine
document, and prints per cluster and signal the rows' event-time range and
hours, their `received_at`, and the published `complete_through` values
above them now (no history is kept: every basis at or above their
`received_at` issued since reads without them). Then:

- re-check by hand the alert windows over those event times (nothing is
  re-evaluated automatically);
- a reader who wants the rows asks the query service for them explicitly
  (`"recovered": true`, `central.recovered: true` in its configuration,
  `SELECT` granted on the recovered tables to its read-only user); its
  answers say `source: "recovered"` and are never mixed with the main
  tables.

## Rule 2: an unmounted buffer volume is an alert

`EdgeBufferVolumeUnmounted` (30 min, warn) and `EdgeBufferVolumeOrphaned`
(6 h, page): a `buffer-otap-publisher-N` / `queue-otelcol-publisher-N` claim
that no pod mounts. Lower the 6 h if retention or the hot tier is cut to days.

## Orphaned buffer

When the alert fires for ordinal N:

1. `kubectl -n otel-edge scale sts/otap-publisher --replicas=N+1` (Go:
   `sts/otelcol-publisher`). The pod mounts its own PVC (same producer id
   `{CLUSTER}/otap-publisher-N`), replays it, and commits the backlog. The
   ordinals between the current size and N come back too; they replay
   their own volumes, or start empty.
2. Watch it drain (Rule 1, step 2), then scale back down with Rule 1.
3. Do not run a second copy of the same producer id against the same
   volume. A one-off drain pod is possible (same image and config, the PVC
   mounted, `PRODUCER` set to that ordinal's id), but it must be the only
   writer of the volume; a scale-up is simpler and needs no new manifest.

Replays are safe to repeat: every copy keeps its content key and
`received_at`, and the consumer's count check skips what central already
holds (D11). What is not safe is deleting the claim before the drain.
