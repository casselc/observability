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
2. **kind: one real cluster running deploy/.** **Not run.** The box never had
   the room: free disk stayed at 3.4–4.8 GB against the 6 GB a kind node image
   plus our images needs (and a 3 GB floor kept for everyone else), memory was
   often under 2 GB, there is no running docker daemon, and the otap-s3pq
   release binary was not built (its cargo target is several GB). The
   control-plane half of the list was run on KWOK instead (§Manifests), which
   found one procedure bug. **Exactly-once end to end through real pods is
   still unverified.** `deploy/kind/` holds the kind setup, ready to run.

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
uncovered catalog attribute. Not done in the prototype. Everything else
agreed exactly, including after churn, a controller outage and 14.6k pods.

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
  hung: `retries` stayed 0, because the lane has no per-attempt timeout (a
  prototype gap). Its object count stood still for ~5.5 min while new records
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

## 7. Manifests on KWOK (instead of kind) [M]

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

**Not covered without kind:** probes, image and securityContext behaviour
(UID 10001 against the hostPath and the PVC), env expansion
(`$(CLUSTER)-$(POD_NAME)`), and the S3 path: pod kills with data in flight,
a full buffer volume, and exactly-once through the consumer.

## 8. What is left

1. **kind** once the box has ≥ 8 GB free memory and ≥ 6 GB free disk. The
   steps are in `deploy/kind/`:
   - build the otap-s3pq release binary and the ocb collector;
   - build the runtime images with `prebuilt.Dockerfile` and `kind load` them;
   - run `relay.py` on the kind gateway, so the pods reach SeaweedFS and the
     relay can inject S3 outages;
   - apply `kind/edge`, then `kind/routing`;
   - drive traffic with telemetrygen, then run the scenarios: pod kills,
     rollout restart, scale 3→4→3, a publisher PVC filled to its cap, and the
     consumer outside the cluster counting every request exactly once.
2. **The controller's covered attributes must follow the agent's heuristics**
   (§3), with a test on the two edge cases.
3. **Sync cadence and shape** (§5): at start-up plus hourly, or key-only.
4. **Lane liveness** (§6): mark a silent cluster's versions unknown.
5. **The controller-outage loss** (§6): two replicas per cluster, measured.
6. **ClickHouse-side cost** of the aggregator (inserts and sync-close), on a
   quiet server.
7. **A per-attempt timeout on lane PUTs** (§6). Without one, a hung S3 stalls
   the lane until the TCP connection breaks.
