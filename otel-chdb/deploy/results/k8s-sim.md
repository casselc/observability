# Kubernetes simulation: KWOK fleet + entity controller, and the deploy/ manifests

2026-09-27, one shared 4-vCPU / 16 GB box, loaded by other agents' jobs (load
average 3–49 during the runs). Software: KWOK v0.8.0 (`kwokctl`, binary
runtime) running Kubernetes v1.36.1 control planes, SeaweedFS 4.47 as S3 on
:18333, ClickHouse 26.10 on :18123. Labels: **[M]** measured here, **[E]**
estimate.

Two parts were asked for:

1. **KWOK fleet → prototype entity controller → S3 entity lanes → aggregator →
   ClickHouse catalog.** Done. The code is `../../entities/controller/`
   (README there).
2. **kind: one real cluster running deploy/.** Done later the same day,
   once the owner freed the box (§8). Before that, the control-plane half ran
   on KWOK (§7), which found one procedure bug. On kind, 17 datasets went
   end to end through real pods. The run found three bugs in `deploy/`, and
   **one of them lost acknowledged data** (the agent's batch step, now
   fixed). With the fix, delivery was exactly once through every fault
   except a gateway SIGKILL, which duplicates as documented.

## 1. What ran

| Phase | Clusters | Nodes / pods / containers | Churn | Wall time |
|---|---|---|---|---|
| P1 | 3 (`prod-use1-00`, `prod-usw2-01`, `prod-euw1-02`) | 200 / ~3,000 / ~3,500 each: 600 nodes, ~9,100 pods, ~10,500 containers at once | real time (speed 1) | 05:05–05:36 |
| P2 | 2 (the third was stopped at 05:44: another job's 8.3 GB RSS left 1.7 GB free) | as P1 | speed 24 (1 simulated hour per 2.5 min), 45 min ≈ 18 simulated hours; controller outage, aggregator outage, unplanned S3 hang | 05:48–06:35 |
| Scale | 1 (`prod-use1-00` grown in place) | 1,005 / 14,560 / 17,227 | steady 7 min, then speed 24 for 12 min (4.8 simulated hours) | 06:38–07:11 |
| Manifests | the 1,005-node cluster | + deploy/ (1,005 agent pods, 3–4 publishers, 3 gateways) | scale, rollout restart, routing switch | 07:12–07:25 |

**The fleet** (`cmd/fleetsim`) is the entities spike's cluster shape
(`entities/scripts/fleet.py`): 4 DaemonSets, 520 Deployments in 37 team
namespaces, 40 StatefulSets, 30 CronJobs (run by the real
kube-controller-manager; a KWOK stage completes their pods), EKS-style node
labels. Churn per simulated day, per cluster [M]: ~50 deployment rollouts,
~250 HPA-style scale steps, ~60 evictions, ~12 relabels of live pods, ~7 node
replacements, plus the CronJobs. The scale run used 2,600 Deployments on 1,005
nodes.

**Measurement.** A sampler read each process's `/proc/<pid>/stat` and
`VmRSS` every 10 s. The aggregator logs every object in
`ingest_log` (PUT time, ingest time, records, bytes). Every version carries
`first_event` (the Kubernetes event time: creation, deletion, label change)
and `first_ingested`. `ridcheck` (§3) supplied the agreement check, a
freshness probe, and an independent pod watch to count what the catalog
missed.

## 2. Freshness [M]

Lag = `first_ingested - first_event` for every version that opened in the
window. The controller flushes a delta object every 5 s and the aggregator
polls every 5 s, so ~5 s median is the configuration, not a limit.

| Window | Level | Versions | p50 s | p95 s | p99 s | max s |
|---|---|---|---|---|---|---|
| P1, 3×200 nodes, speed 1 | pod | 136 | 5.3 | 7.5 | 86.3 | 86.7 |
| | resource | 139 | 5.3 | 7.4 | 86.3 | 86.7 |
| P2, 2×200 nodes, speed 24, outside outages | pod | 950 | 5.8 | 8.8 | 9.7 | 724¹ |
| | resource | 1,166 | 5.7 | 8.8 | 9.7 | 724¹ |
| | workload | 62 | 6.1 | 8.6 | 9.3 | 9.4 |
| Scale, 1,005 nodes / 14.6k pods, speed 24 | pod | 1,385 | 5.9 | 8.7 | 9.2 | 10.2 |
| | resource | 1,708 | 6.0 | 8.7 | 9.2 | 10.2 |
| During the 10-min controller outage (P2) | resource | 61 | 435 | 566 | 566 | 566 |
| During the 8-min aggregator outage + S3 hang (P2) | resource | 153 | 118 | 422 | 475 | 475 |

¹ One pod created 2 min before the controller stop and first seen at its
restart. The P1 p99 is the cluster start-up.

**The probe** (`ridcheck --mode probe`) creates a pod, relabels it and deletes
it, and times each change until `resources_current` shows it:

| When | n | create s (median) | relabel s | delete s |
|---|---|---|---|---|
| P1 | 16 | 5.3 | 5.4 | 4.9 |
| P2, after the aggregator came back | 3 | 5.0 | 5.6 | 5.0 |
| Scale, 14.6k pods, speed 24 | 5 | 5.0 (3.8–5.2) | 5.9 (5.8–6.4) | 4.3 (3.7–4.6) |

**Freshness does not depend on cluster size at these sizes:** p99 9.2–9.7 s
at both 3k and 14.6k pods.

## 3. resource_id agreement with the agent [M]

`ridcheck agree` lists every running pod from the API server and computes each
container's `resource_id` the way an **agent** does. It follows
k8sattributes' own rules (from `processor/k8sattributesprocessor/internal/kube/client.go`):
owner references plus name heuristics, with no ReplicaSet or Job informer. It
adds what resourcedetection's ec2 detector reads. It then compares the result
with the ids the controller wrote. The two share only the hash function
(`rid.ID`), not the derivation. `hash_check` recomputes every catalog id in
ClickHouse with the spike's SQL expression and compares that with Go's.

| When | Cluster | Containers | Match | Mismatch | Missing | Catalog extra | Hash Go ≠ CH |
|---|---|---|---|---|---|---|---|
| after P1 | prod-use1-00 | 3,603 | 3,600 | 3 (deliberate) | 0 | 2 | 0 of 3,705 |
| after P1 | prod-usw2-01 | 3,470 | 3,470 | 0 | 0 | 0 | 0 of 3,521 |
| after P2 (churn + both outages) | prod-use1-00 | 3,603 | 3,600 | 3 (deliberate) | 0 | 0 | 0 of 4,406 |
| after P2 | prod-usw2-01 | 3,470 | 3,470 | 0 | 0 | 0 | 0 of 4,210 |
| scale, after churn | prod-use1-00 | 17,227 | 17,224 | 3 (deliberate) | 0 | 0 | 0 of 18,052 |

(The third cluster's P1 check timed out against a ClickHouse the other jobs
had stalled. That cluster was stopped before P2.)

**The three mismatches are the edge cases `fleetsim --edge-cases` plants on
purpose, and they are real disagreements:**

- A Job created by hand from a CronJob (`kubectl create job --from=cronjob/…`,
  named `ads-report-24-manual-1`). The controller follows the Job's owner
  reference and sets `k8s.cronjob.name`. The agent recognises a CronJob only
  by the `-<8 digits>` suffix of the Job name, so it sets nothing.
- Two pods of a bare ReplicaSet named like a Deployment's (`legacy-api-7d9f8c6b5`).
  The agent's name heuristic invents `k8s.deployment.name=legacy-api` (and
  `service.name` follows it). The controller reads the ReplicaSet, sees no
  Deployment owner, and sets `service.name=legacy-api-7d9f8c6b5`.

**Consequence:** telemetry from such pods carries a `resource_id` the catalog
does not have, so the join finds nothing. **The fix is on the controller
side:** for the covered attributes it must reproduce the agent's heuristics
exactly, even where they are wrong. It can keep the true owner as an
uncovered catalog attribute. Everything else agreed exactly, including after
churn, a controller outage and 14.6k pods.

**Fixed after the run** (`internal/ctrl` `workload()`, test
`workload_test.go`): the covered attributes now follow the agent's two
rules.

- `k8s.deployment.name` is the ReplicaSet name minus `-<pod-template-hash>`.
- `k8s.cronjob.name` is set only for a Job named `-<8 digits>` within a day
  of the pod's creation.

The version's kind and name still follow the owner chain. The test covers
both edge cases, a normal Deployment and a scheduled CronJob run. The KWOK
agreement check was not rerun after the fix (the KWOK clusters were
already torn down).

## 4. Controller and aggregator cost [M]

CPU in cores, averaged over the window. RSS is the resident set.

| Window | Process | Cores | RSS avg MB | RSS max MB |
|---|---|---|---|---|
| P1, 200 nodes / 3k pods, speed 1 | entityctl (each of 3) | 0.003–0.004 | 75–79 | 91–97 |
| | aggregator (all 3 clusters) | 0.005 | 34 | 72 |
| P2, 200 nodes, speed 24 | entityctl (each of 2) | 0.016–0.019 | 80 | 94–104 |
| | aggregator | 0.001 | 33 | 75 |
| Scale, 1,005 nodes / 14.6k pods, steady | entityctl | 0.002 | 234 | 285 |
| Scale, speed 24 | entityctl | 0.009 | 269 | 307 |
| | aggregator | 0.002 | 53 | 156 |
| Scale, cold start | entityctl | full list + first delta + first sync in **5 s** | | 300 (peak) |

For scale, the KWOK control plane of the 1,005-node cluster used 1.26–1.29 GB
(apiserver) and 0.11–0.22 cores. **The controller is ~2% of the apiserver's
CPU and ~20% of its memory.**

- **Memory:** ~0.016 MB per pod, or 13 KB of Go heap per pod [M]
  (`heap_inuse` 43 MB at 3.3k pods, 177 MB at 14.6k).
- **Per-cluster sizing:** ≤ 0.05 core and ~350 MB for a 15k-pod cluster at
  24× normal churn [E]. That is a small Deployment beside the agents.
- **Aggregator:** its own CPU is negligible. The work is ClickHouse's
  (inserts and the sync-close `INSERT … SELECT`), which was not separated from
  the shared server here.

## 5. Record volume [M, extrapolated per day E]

A lane holds two kinds of object. A **delta** (every 5 s, only when
something changed) carries the versions that opened or closed. A **sync**
(every 10 min) carries every open version, so the aggregator can close what
the controller no longer sees. Objects are gzip NDJSON.

| Cluster | Deltas measured | Per day at normal churn [E] | One sync | Syncs per day at 10 min [E] |
|---|---|---|---|---|
| 200 nodes / 3k pods | 2,046 records, 0.41 MB in ~18 simulated h | ~2,800 records, ~0.55 MB | 7.5–7.9k records, 1.75 MB | 1.1 M records, **250 MB** |
| 1,005 nodes / 14.6k pods | 3,947 records, 0.73 MB in 4.8 simulated h | ~20k records, ~3.7 MB | 37.8k records, 8.9 MB | 5.4 M records, **1.28 GB** |

- A controller restart writes the full state twice (first delta + first
  sync): 71k records, 15 MB at 14.6k pods.
- The whole experiment wrote 1,005 objects, 469k records, 101 MB. In
  ClickHouse that was 40 MB of `records` and 27 MB of `versions`.

**The syncs are >99% of the volume.** The deltas, the actual catalog changes,
are ~0.5 MB/day for a 3k-pod cluster. A sync every hour instead of every 10
minutes cuts a 3k-pod cluster to ~42 MB/day. That makes a 20-cluster fleet
~0.85 GB/day of lane objects instead of 5 GB [E].

What a sync buys is repair after a missed deletion, and those arise only
around controller restarts. A sync written at start-up plus hourly is enough.
A cheaper design is a sync that lists only version keys (8 bytes each, not
the attributes): ~60 KB instead of 1.75 MB [E]. Not implemented.

## 6. Downtime and recovery [M]

**Controller down 10 min** (`prod-usw2-01`, 05:49:44–05:59:44, speed 24).

- **Restart:** the new incarnation opens a new lane
  (`{cluster}/{epochMs}-{instance}/`), lists everything, and wrote its first
  delta and sync (7,696 versions each) 2 s after start. The aggregator
  ingested them 4–5 s later.
- **Deletions missed while down:** the first sync closed the 139 versions
  that disappeared during the outage. Their `closed_at` is the sync time, so
  each one's validity is overstated by up to the outage. That is harmless for
  joins: a deleted pod sends no telemetry.
- **Versions created while down** appeared at the restart: lag p50 435 s,
  max 566 s.
- **Pods that lived and died entirely inside the outage never reach the
  catalog.** The independent watch saw 4 such pods and all 4 are missing.
  Their telemetry would carry `resource_id`s with no catalog row. This is the
  one real loss, and it is structural: an informer relist sees only what
  exists. Short outages and a PodDisruptionBudget'd 2-replica controller
  (records merge commutatively, so two writers per cluster are safe) keep it
  small. The agent's residual map can carry the pod name for such orphans.
- After recovery, agreement was 3,470/3,470 with no extra or missing
  versions.

**Aggregator down 8 min** (06:07:44–06:15:44), overlapped by an **unplanned
S3 hang**: SeaweedFS stopped answering from ~06:08:44 to ~06:14 while the box's
load average hit 49.

- **During the hang,** each controller's in-flight create-only PUT simply
  hung: `retries` stayed 0, because the lane had no per-attempt timeout
  (added since: `--put-timeout`, 20 s). Its object count stood still for ~5.5 min while new records
  queued in memory (`pending` up to 71). When S3 answered again the PUT
  completed and the queue drained to 0 within 30 s. Nothing was lost or
  written twice.
- **On restart,** the aggregator read its progress from `lane_progress`
  and ingested the 24 waiting objects in 5.7 s. The two syncs among them
  closed 21 and 22 versions.
- **Lag inside the window:** p50 118 s, max 475 s (bounded by the outage).
- **Loss:** none. The independent watch found 3,728 of 3,728 pods in the
  catalog.

**Controller cold start at 14.6k pods:** first objects 5 s after start. The
35.5k-version delta was ingested 2.3 s after its PUT and the sync 8.5 s
after.

**Gap found:** a controller that stops for good (the third cluster here)
leaves its versions open indefinitely. Nothing tells central that the
cluster is stale. That needs a lane heartbeat (the sync object serves if its
age is watched) and a rule that versions of a cluster whose lane is silent
beyond N syncs are "unknown", not "open".

## 7. Manifests on KWOK (before kind) [M]

KWOK runs a real apiserver, controller-manager and scheduler with fake
kubelets. That exercises everything in `deploy/` except running containers:
admission, validation, scheduling, the StatefulSet, DaemonSet, Deployment and
PDB controllers, PVC binding (6 static 50 Gi PVs and a default StorageClass),
and EndpointSlices.

- **Every build is valid against a live 1.36.1 apiserver** (strict server-side
  dry-run): 11 of 11 (the 9 overlays, `kind/edge`, `kind/routing`).
- **`kind/edge` applied:** 1,005 agent pods (`system-node-critical` is
  admitted outside kube-system), 3 publishers with bound PVCs, PDB
  `maxUnavailable: 1`.
- **Scale 3 → 4 → 3 → 4:** the fourth ordinal's PVC is kept on scale-down
  (`whenScaled: Retain`) and the same claim and PV are reused on scale-up.
  That is the property the README's scaling section relies on (a scaled-down
  buffer is replayed when its ordinal returns).
- **`kubectl rollout restart`:** one ordinal at a time despite
  `podManagementPolicy: Parallel` (that only governs create and delete).
- **`kind/routing` on top:** the gateway Deployment (3), the ring Service
  with hostnames in its EndpointSlice, and 4 publishers.

**Bug (fixed): the documented switch to routing leaves the ring without
hostnames.** `components/routing` changes the StatefulSet's immutable
`serviceName` to `otap-publisher-ring`, and its comment said to switch an
existing install by `kubectl delete sts --cascade=orphan` and re-applying.

- **What happens:** the new StatefulSet adopts the running pods *without
  replacing them*, because the pod template is identical. An adopted pod keeps
  `spec.subdomain: otap-publisher`.
- **The effect:** the EndpointSlice controller sets an endpoint's `hostname`
  only when the pod's subdomain names the Service, so the ring's EndpointSlice
  showed `hostname=` empty for ordinals 0–2. Only the newly created ordinal 3
  had its hostname. The gateway's resolver runs with `return_hostnames: true`
  and keys the ring on those hostnames, so it would build a ring missing three
  of four publishers.
- **The fix:** `kubectl rollout restart sts/otap-publisher` after the switch
  recreates the pods under the new subdomain, and all four hostnames appeared.
  The step is now in `components/routing/kustomization.yaml`, and the README
  validation table lists the KWOK checks.

**Not covered without kind:** probes, image and securityContext behaviour,
env expansion, and the S3 path. kind covered them (§8).

## 8. kind: deploy/ on one real cluster [M]

11:00–12:15, once the owner had freed the box.

**Setup:**

- **Cluster:** kind v0.31, Kubernetes 1.35.0, one node. dockerd ran inside
  the dev container with its data root in the scratchpad.
- **Images:** built with `kind/prebuilt.Dockerfile` around host builds: the
  otap-s3pq release binary, and ocb v0.161.0 `otelcol-deploy`, stock for the
  agent and patched (`patches/0001`) for the gateway. `otlpsend` served as
  the sender.
- **Outside the cluster:** SeaweedFS as S3, reached through `kind/relay.py`
  on the kind gateway (172.18.0.1), which can also cut S3 or slow it. The
  consumer (`consume`, release build) ran on the host into ClickHouse
  database `k8s_edge`.
- **Traffic:** each dataset is `mixgen` output (seed 100+D, its own
  one-hour timestamp window): 32 traces and 32 logs requests of 10,000 rows
  each, 120 services. A Job sends it to the `otel-agent` Service with
  `otlpsend`, which resends on any error, as an SDK with retries does.
- **The check:** per dataset window in ClickHouse: rows, distinct rows
  (`TraceId, SpanId` for spans; the whole row for logs), duplicates, and
  missing rows out of 320,000.
- **Harness:** `kind/kind_test.sh`, with `kind/kind-config.yaml` and
  `kind/runc-oom-clamp.sh`.

### Bugs found in deploy/

1. **The Rust agents lost acknowledged data on SIGKILL (fixed).**
   `agent-rust.yaml` and `agent-routing.yaml` ran `batch` before the
   persistent queue. The batch processor is asynchronous: the OTLP receiver
   acks a request as soon as the batcher takes it, before it is written to
   the queue.
   - **Evidence:** a SIGKILLed agent lost 1 to 5 acknowledged requests per
     kill (datasets 2, 4 and 5 below). The queue's own indexes prove where:
     all 128 trace requests of datasets 1–4 reached the queue, and one
     request that was being dispatched at a kill was moved back to the queue
     on restart as designed. The lost requests had been acked to the sender
     without ever reaching the queue.
   - **The fix:** drop the batch step from the Rust-edge agents. It is
     redundant there, because `edge-publisher.yaml` batches in front of the
     WAL (~3 MiB objects, oversized requests split at 8 MiB). The queue is
     now sized in items (2.5 M, about the old 250 × 10k).
   - **Verified:** the same kills lost nothing (datasets 7 and 8).
   - **Not fixed here:** the Go edge's agent (`agent-go.yaml`) had the same
     window, and its publisher did not batch. Fixed later the same day by
     moving the batch step into the Go publisher, in front of its queue
     (§9, `go-batch.txt`).
2. **`kind/prebuilt.Dockerfile` used `debian:bookworm-slim` (glibc 2.36)
   (fixed).** The host builds need glibc 2.39, so every publisher
   crash-looped with "GLIBC_2.39 not found". The base is now `ubuntu:24.04`,
   set by `ARG BASE`. `images/otap-s3pq.Dockerfile` builds inside bookworm
   and is not affected.
3. **The kind routing overlay could not roll its gateways (fixed).** Their
   500m CPU requests left no room for the surge pod on a 4-CPU node
   (FailedScheduling), so `kind/routing` now requests 100m. The base keeps
   500m: on a real cluster this is only a reminder that `maxSurge` needs
   headroom.

**Environment quirk, not a deploy/ bug:** no pod sandbox started at first.
The dev container lacks CAP_SYS_RESOURCE, so runc cannot set kubelet's
negative `oom_score_adj` and fails with "can't get final child's PID from
pipe". `kind/runc-oom-clamp.sh` clamps the value, wired in through
`containerdConfigPatches`.

### Results

| # | Fault while the dataset was in flight | Spans: rows / dup / missing | Logs: rows / dup / missing |
|---|---|---|---|
| 1 | none (steady, 3 publishers) | 320,000 / 0 / 0 | 320,000 / 0 / 0 |
| 2 | SIGKILL publisher-1, force-delete publisher-0, SIGKILL and force-delete the agent | 310,000 / 0 / **10,000** | 320,000 / 0 / 0 |
| 3 | SIGKILL publisher-0 and publisher-2 | 320,000 / 0 / 0 | 320,000 / 0 / 0 |
| 4 | SIGKILL the agent twice (old config) | 270,000 / 0 / **50,000** | 270,000 / 0 / **50,000** |
| 5 | SIGKILL the agent once (old config) | 280,000 / 0 / **40,000** | 290,000 / 0 / **30,000** |
| 6 | agent rollout restart (SIGTERM) | 320,000 / 0 / 0 | 320,000 / 0 / 0 |
| 7 | SIGKILL the agent twice, **agent without batch** | 320,000 / 0 / 0 | 320,000 / 0 / 0 |
| 8 | fixed config: SIGKILL agent, SIGKILL publisher-1, SIGKILL agent | 320,000 / 0 / 0 | 320,000 / 0 / 0 |
| 9 | publisher rollout restart | 320,000 / 0 / 0 | 320,000 / 0 / 0 |
| 10 | scale 3→4 at 2 s, 4→3 at 6 s | 320,000 / 0 / 0 | 320,000 / 0 / 0 |
| 11 | 4 publishers, S3 slowed, 4→3 right after the last ack | 320,000 / 0 / 0 | 320,000 / 0 / 0 |
| 12 | same, after an agent restart so that ordinal 3 got traffic; then 3→4 again | 320,000 / 0 / 0 (70,000 waited in ordinal 3's retained PVC until it came back) | same |
| 13 | S3 down, ordinal 3 on a 40 MiB volume that filled | 320,000 / 0 / 0 (after growing the volume and a restart) | 320,000 / 0 / 0 |
| 14 | routing on (4 publishers, 3 gateways), steady | 320,000 / 0 / 0 | 320,000 / 0 / 0 |
| 15 | routing: SIGKILL two gateways, force-delete the third | 322,858 / **2,858** / 0 | 341,278 / **21,278** / 0 |
| 16 | routing: gateway rollout restart (stalled at 1 of 3, bug 3) | 320,000 / 0 / 0 | 320,000 / 0 / 0 |
| 17 | routing: gateway rollout restart; then S3 out of disk for ~4 min (below) | S3 holds 320,000 distinct, 0 duplicates² | 320,000 / 0 / 0 |

- **Rows 2, 4 and 5 are bug 1.** Row 3 isolates the publishers: SIGKILLs
  lose nothing, because their WAL replays. Rows 4 and 5 isolate the agent.
- **Row 12, scale-down:** the removed ordinal's PVC is retained
  (`whenScaled: Retain`). Its 70k rows per signal were committed 30 s after
  the ordinal came back, as the README says: late, not lost.
- **Scale-up gets no traffic:** in rows 10 and 11 the fourth publisher got
  none. The agents' `dns:///` round robin re-resolves only when a connection
  fails, so a new publisher is invisible to running agents. Row 12 needed
  `kubectl rollout restart ds/otel-agent`, which is safe after the fix
  (row 6). Now in the deploy README §Runbook.
- **Row 13, full volume:** the publisher answered `Unavailable` ("wal io
  error: No space left on device"). The agents moved those requests to the
  other publishers, and the pod stayed Ready.
  - After S3 returned, the full volume could not release its committed
    segments, because the progress file needs space. 10,000 spans stayed
    stuck.
  - Growing the volume (a tmpfs remount standing in for PVC expansion) freed
    the segments. The last batch, whose segment flush had failed on ENOSPC,
    came back only when a pod restart replayed the WAL.
  - Now in the deploy README §Durable buffer.
- **Row 15 is the documented batched-publisher duplicate** (README
  §Duplicates, `route-gwkill-batched.txt`: 70,320 and 61,727 with 8
  senders). Pieces resent through a restarted gateway are re-batched, so
  their bytes differ. Nothing was missing.
- **Routing affinity (row 14):** 120 of 120 services on exactly one
  publisher for both signals. The skew was 155k / 75k / 58k / 32k spans
  across the 4 publishers (Zipf services, as D16 measured).
- **The `serviceName` switch to routing** used the procedure fixed in §7
  (orphan, apply, rollout restart). All four ring endpoints had their
  hostnames.
- **The consumer's horizon audit** (`consume horizon-audit --check-horizon
  all`) found 0 late copies. Its scope was only the data left after the
  truncations below.
- ² **Row 17's central count is not usable, and that is my error.** At about
  12:08 the host's free disk fell to 1.85 GB, below the 3 GB floor. The
  growth came from ClickHouse parts, SeaweedFS and the kind node.
  - SeaweedFS refused writes ("failed to find writable volumes") for about
    4 minutes. The publishers held dataset 17 in their buffers and committed
    it once space was back.
  - I then truncated `k8s_edge` to free space while the consumer was
    ingesting that backlog, which wiped 4 just-ingested objects (13,788
    spans) from central.
  - Counted straight from the S3 objects instead, dataset 17 is complete:
    320,000 distinct spans and no duplicate across all objects.
  - Datasets 1–16 had been verified before each truncation. After the first
    truncation, no row reappeared in windows 1–9, so there were no late
    copies.

**Not covered:** EKS and its webhooks, real PVC expansion, multi-node
placement, and metrics (only traces and logs were sent).

## 9. What is left

1. ~~**The Go edge's agent** keeps its batch step.~~ **Done (2026-09-27):**
   the Go publisher batches before its queue (s3pq `batch`, 10k items / 1 s,
   each request answered once its merged request is enqueued) and
   `agent-go.yaml` has no batch step. Locally, two agent SIGKILLs lost 10 of
   32 10k-row requests per signal with the old configs and none with the
   new; objects stay 10k rows at 10k-row requests and double at 300-row
   requests from 4 agents (`results/go-batch.txt`). D4 now says it: agents
   never batch in front of a persistent queue.
2. **New publishers get no traffic** until the agents reconnect (§8). The fix
   is a server-side max connection age on the Rust OTLP receiver (upstream);
   until then the deploy README §Runbook restarts the agents after a
   scale-up.
3. ~~**A full buffer volume needs an operator** … the pod stays Ready.~~
   **Readiness done (2026-09-27):** `edgeprobe` fails the probe on a buffer
   volume under 1 GiB free or a buffer at 95% of its cap, for both edges,
   and `alerts/edge-buffer.rules.yaml` pages on it (`results/wedge.txt`). A
   full volume still needs an operator (grow, then restart). The Go edge
   **loses** acknowledged requests on a full volume (U22), so its queue cap
   is now in bytes, far below the volume.
4. **Rerun the KWOK agreement check** with the controller's agent-rule fix
   (§3).
5. **Sync cadence and shape** (§5): at start-up plus hourly, or key-only.
6. **Lane liveness** (§6): mark a silent cluster's versions unknown.
7. **The controller-outage loss** (§6): two replicas per cluster, measured.
8. **ClickHouse-side cost** of the aggregator (inserts and sync-close), on a
   quiet server.

Done since the first write-up: the kind run (§8), the agent-rule
`resource_id` fix (§3), and a per-attempt lane PUT timeout (`--put-timeout`,
default 20 s, test `internal/lane/s3_test.go`).
