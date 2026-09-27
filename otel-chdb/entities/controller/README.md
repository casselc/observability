# entities/controller: a prototype per-cluster entity controller

The writer side of the entity catalog in [`../README.md`](../README.md). One
controller runs in each cluster. It keeps the cluster's versioned entities
(cluster, node, namespace, workload, pod, and one **resource** per container,
keyed by `resource_id`) and writes them to its own **S3 entity lane**. A small
central **aggregator** reads the lanes into ClickHouse tables shaped like
[`../sql/catalog.sql`](../sql/catalog.sql).

The prototype was measured on a KWOK fleet: freshness, cost, volume,
agreement with the agents, and outages. Those results are in
[`../../deploy/results/k8s-sim.md`](../../deploy/results/k8s-sim.md).

**In one paragraph:**

- **Freshness:** ~5–6 s median, p99 < 10 s, from a Kubernetes change to a
  catalog row. That is the 5 s flush plus the 5 s poll.
- **Cost:** under 0.02 core and ~100 MB for a 3k-pod cluster, and ~0.01 core
  and ~270 MB at 14.6k pods.
- **Agreement:** on KWOK, `resource_id` matched what the agent computes for
  every container except two deliberate edge cases. The controller now
  applies the agent's naming rules in those cases too (unit-tested; the
  KWOK check was not rerun).
- **Outages:** nothing is lost across aggregator and S3 outages. Pods that
  live and die entirely inside a controller outage are lost.
- **Volume:** >99% is the periodic full-state sync, which should be made rarer
  or smaller.

## Layout

```
cmd/entityctl    the controller: informers -> versions -> lane
cmd/aggregator   lanes -> ClickHouse (records, versions, catalog views)
cmd/fleetsim     builds and churns the spike's fleet shape in a KWOK cluster
cmd/ridcheck     agree (resource_id vs the agent's derivation), probe (freshness), watchlog (ground truth)
internal/ctrl    informers, derivation of the six levels, version open/close
internal/lane    the record type and the create-only S3 lane writer
internal/rid     resource_id = xxh3_64("res.v1\0" ‖ sorted covered k\0v\0), as in ../sql/resources.sql
internal/ch      a minimal ClickHouse HTTP client
sql/aggregator.sql  records, versions (AggregatingMergeTree), versions_final, catalog views, lane_progress, ingest_log
```

## Design

**Informers.** The controller runs shared informers on Pods, Nodes and
Namespaces, with event handlers. ReplicaSets and Jobs are informers too, used
only as lookups to find the Deployment or CronJob above a pod. A transform
strips cached objects to the fields the controller reads (pod: node,
container names and images, phase, the PodScheduled time).

- **Pods** go through a rate-limited workqueue with 2 workers.
- **Initial sync** waits for every handler registration's `HasSynced`, not
  just the informers' caches. Without that, the first full-state sync ran
  before the handlers had processed the initial list, and would have closed
  everything.

**Versions.** Every entity has a *key* per version: for a resource the key is
its `resource_id`; for node, workload, pod and cluster it is a hash of the
identity plus the covered attributes; a namespace has one version per name. A change to a covered attribute (a relabel, a node
replacement, a rollout's new pod-template hash) closes the old version and
opens a new one. A record carries:

- `valid_from`, `closed_at` (0 while open) and `observed_at`;
- `event_at`: the Kubernetes time of the change;
- `writer`: the lane incarnation.

**Lane** (`internal/lane`). There is one lane per controller incarnation:

```
{prefix}/{cluster}/{epochMs}-{instance}/{seq:012d}.delta.ndjson.gz
{prefix}/{cluster}/{epochMs}-{instance}/{seq:012d}.sync.{syncAtMs}.ndjson.gz
```

- **Writes are create-only** (`If-None-Match: *`) and sequential. On a 412 the
  writer HEADs the key: if the ETag is the MD5 of its own body, an earlier
  attempt landed, otherwise the slot is skipped.
- **A delta** (every `--flush`, 5 s, only if something changed) carries what
  opened or closed.
- **A sync** (at start and every `--resync`, 10 min) carries every open
  version. It is how central learns about deletions the controller missed
  while it was down.

**Aggregator.** It lists lanes past its `lane_progress` and inserts each
object into `records`, using the object key as `insert_deduplication_token`.
A materialized view folds `records` into `versions` (AggregatingMergeTree).
The merge rule is commutative, so lanes can be replayed or read out of order,
and two controllers can write the same cluster:

- `valid_from` = min;
- open/closed = the **latest observation** (on a tie the close wins), so a
  wrongly closed version reopens when the controller sees it again;
- the rest is content-addressed, so any value will do.

For a sync object, the aggregator also closes the cluster's versions that
are open but were last observed before `syncAt - margin` (60 s). It does that
with an `INSERT … SELECT` of close records.

## Running it

```sh
go build -o bin/ ./cmd/...
export AWS_ACCESS_KEY_ID=… AWS_SECRET_ACCESS_KEY=… S3_ENDPOINT=http://localhost:18333   # empty: AWS
bin/aggregator --create sql/aggregator.sql --db k8s_cat --bucket k8s-entities
bin/entityctl --kubeconfig k.yaml --cluster prod-use1-00 --bucket k8s-entities \
  --static cloud.provider=aws,cloud.platform=aws_eks,cloud.region=us-east-1,cloud.account.id=…,deployment.environment.name=prod \
  --metrics 127.0.0.1:19900            # /stats; also logged every 30 s
bin/ridcheck --kubeconfig k.yaml --cluster prod-use1-00 --static …   # agree (default) | --mode probe | --mode watchlog
bin/fleetsim --kubeconfig k.yaml --index 0 --setup [--edge-cases] --churn 45m --speed 24
```

In a cluster, `entityctl` needs `list`/`watch` on pods, nodes, namespaces,
replicasets and jobs, and `s3:PutObject`, `s3:GetObject` and `s3:ListBucket`
on its lane prefix. With `--kubeconfig` empty it uses the in-cluster config.
No manifest is written yet.

## Measured (details in deploy/results/k8s-sim.md)

| | 200 nodes / 3k pods | 1,005 nodes / 14.6k pods |
|---|---|---|
| change → catalog, p50 / p99 | 5.7 s / 9.7 s | 6.0 s / 9.2 s |
| controller CPU, normal churn / 24× churn | 0.003 / 0.017 cores | 0.002 / 0.009 cores |
| controller RSS avg / max | 80 / 104 MB | 269 / 307 MB |
| cold start to first sync | 2 s | 5 s |
| resource_id agreement with the agent | 3,600 of 3,603 and 3,470 of 3,470 | 17,224 of 17,227 |
| deltas per day [E] | ~0.55 MB | ~3.7 MB |
| one sync / per day at 10 min [E] | 1.75 MB / 250 MB | 8.9 MB / 1.28 GB |

**Covered attributes follow the agent's rules, not the truth.** The two
cases the KWOK run caught disagreeing:

- A Job made by hand from a CronJob: the agent recognises a CronJob run only
  by the `-<8 digits>` suffix within a day of the pod's creation.
- A bare ReplicaSet named like a Deployment's: the agent's name heuristic
  (the ReplicaSet name minus `-<pod-template-hash>`) invents a Deployment.

`workload()` now derives `k8s.deployment.name`, `k8s.cronjob.name`,
`k8s.job.name` and `service.name` exactly as k8sattributes does
(`internal/ctrl/workload_test.go`). The version's `kind` and `name` still
record the true owner.

## Known gaps

1. **Agreement after the rule fix is unit-tested only.** Rerun `ridcheck
   agree` on a KWOK cluster with `--edge-cases`.
2. **Controller outages lose short-lived pods.** A pod that lives and dies
   entirely inside the outage never reaches the catalog (4 of 4 in a 10-min
   outage at 24× churn). The mitigation is two replicas per cluster; the
   merge rule already allows two writers.
3. **Syncs dominate volume.** Sync at start plus hourly, or write key-only
   syncs.
4. **No liveness.** A controller that stops for good leaves its cluster's
   versions open. Watch the age of the last sync per lane and treat a silent
   cluster's versions as unknown.
5. **Lane PUTs** now have a per-attempt timeout (`--put-timeout`, 20 s;
   `internal/lane/s3_test.go`). Before it, a hung S3 stalled the lane until
   the connection broke.
6. **Close time after an outage.** A version closed by a sync gets the sync's
   time as `closed_at`, so its validity is overstated by up to the outage.
   That is harmless for joins.
7. **Coverage:** the owner walk knows ReplicaSet → Deployment, StatefulSet,
   DaemonSet, Job → CronJob, and bare pods. Other owners (Argo Rollouts, KEDA
   ScaledJobs, …) were not exercised.
