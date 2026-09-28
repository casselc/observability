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
