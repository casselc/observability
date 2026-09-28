# EKS on AWS: validation runbook

Everything the spike measured ran on one shared 4-vCPU box against SeaweedFS on
localhost, with kind as the only real cluster. This runbook takes the same
edge, consumer and checks to a small EKS cluster and a real S3 bucket, and
settles what localhost could not: real S3 semantics and latency, real STS
and webhooks, ABAC with real session tags, per-prefix request limits, and
the kind fault scenarios on real nodes and EBS volumes.

Every command is in `eks/`. The scripts take their parameters from the
environment, are idempotent, record everything they create in a ledger
(`$STATE/ledger.tsv`), and `eks/down.sh` removes exactly that. **No script
deletes a bucket**, and objects are only ever deleted under the run's prefix
`validation/$RUN/`. Nothing here has been run against AWS yet: the scripts
were checked locally (syntax, shellcheck, kustomize builds, the load tool
against a fake store; README §Local checks).

## What this settles

| Q | Question | Tied to | Step |
|---|---|---|---|
| EKS-1 | Does real S3 enforce `If-None-Match: *` and `If-Match` atomically, and resolve ambiguous writes? | DECISIONS §1.4 `ATOMIC_COND`; AMBIGUITY S1, S3; risk 3 | §3 |
| EKS-2 | How often does S3 answer 409 `ConditionalRequestConflict` under contention, and does the edge absorb it (`resolved_own`)? | risk 3; AMBIGUITY S1/S3 (409 → HEAD) | §3, §8 |
| EKS-3 | LIST after write, `StartAfter`, list-race: any acked key missing from a later LIST? | AMBIGUITY S6 (AWS: [D] only) | §3 |
| EKS-4 | Read-after-write on GET/HEAD; HEAD of a free slot is 404 with `s3:ListBucket`, 403 without, **what with a prefix-scoped `ListBucket`** (the old `deploy/iam/` grant: expected 403), and **404 with the amended grant** (prefix + `StringLikeIfExists`, D18 amendment 2026-09-28); and whether a LIST with no prefix is 403 or 200 under it (the accepted residual) | AMBIGUITY S5; acceptance RUNBOOK §5 warns a HEAD carries no `s3:prefix`; the S3 `HeadObject` and IAM condition-operator docs (D18) | §3, §4 |
| EKS-5 | Where does S3 throttle create-only PUTs, and does the cluster-first layout spread them (3,500 PUT/s per prefix)? | risk 3 (per-prefix limits) | §5 |
| EKS-6 | Write-side ABAC with real session tags: cross-cluster writes and reads denied, no delete, no control write, create-only by bucket policy; the consumer and GC still work | STPA R-S7; D18; `iam/seaweedfs_abac.sh` (the local proof) | §4 |
| EKS-7 | IRSA and Pod Identity end to end in both edges (the webhook fires with `automountServiceAccountToken: false`; no fallback to the node role; refresh across a session) | §1.1 "real EKS/STS never used"; D18 | §3, §6, §10 |
| EKS-8 | Commit latency and lane throughput with real PUT latency (locally ~20 batches/s per lane = 1/(encode + PUT)) | risk 3 | §7 |
| EKS-9 | The kind fault scenarios on EKS: exactly once through publisher and agent kills, rollouts, scaling, a full buffer on a **real EBS volume and a real PVC expansion**, a node drain, S3 unavailable, gateway kills | k8s-sim.md §8 "not covered: EKS, real PVC expansion, multi-node"; AMBIGUITY E1–E5; STPA LS-9 | §8 |
| EKS-10 | Exactly once end to end on real S3, and the horizon audit silent | D3, D11; `model/s3Inline.qnt` | §9 |
| EKS-11 | What the pipeline costs in requests and bytes per lane and per object, billed by S3 (LIST per idle lane per 30 s, renewal per 15 s, measured locally) | §3 calculator prices; D8 | §11 |
| EKS-12 | The consumer, GC and `complete_through` over a day: GC lag, checkpoint size, watermark lag | AMBIGUITY S4, S7, S11; risk 6 | §8 (E15) |
| EKS-13 | The query service's basis tokens signed with a KMS HMAC key under IRSA: two processes verify each other's tokens, the role cannot mint with a verify-only key, a non-HMAC key fails startup, KMS latency | DECISIONS D30 amendment 2026-09-28; AMBIGUITY X17; `deploy/iam/query-basis-kms*.json` | §10a |

## 0. Before you start

**What the owner provides:**

- An AWS account (or a sandbox account in the organisation) where you may create
  an EKS cluster, IAM roles and one IAM user, ECR repositories, a VPC endpoint
  and one S3 bucket. The caller needs `sts:AssumeRole` and `sts:TagSession`
  on roles in the account (the ABAC session-tag phase).
- **Quotas:** 16 On-Demand standard vCPUs (3 × m6i.xlarge + 1 for the clock
  node + headroom), 1 Elastic IP (the NAT gateway), 1 new VPC. The defaults of
  a fresh account are enough; an organisation SCP that denies `iam:CreateUser`
  or `iam:CreateAccessKey` blocks §2's consumer user (see the gap below).
- A build host: Linux x86-64, docker with buildx, Go 1.26, ~15 GB free (the
  Rust stages build inside docker), aws CLI v2.22+ (for `--if-none-match`),
  eksctl 0.200+, kubectl, kustomize 5.7, python3.
- A bucket name. Use a new, dedicated bucket: the bucket-policy rows of §4
  need a bucket this tooling created (a bucket policy replaces the whole
  policy, so it is never applied to an existing bucket).

**Two gaps this runbook was written around, both closed on 2026-09-28
(DECISIONS D18 amendment):**

1. ~~**`consume` takes static keys only.**~~ **Closed.** Without
   `--key`/`--secret`, `consume` takes the AWS chain (environment keys,
   `--profile`/`AWS_PROFILE`, IRSA, Pod Identity, IMDS, `--role-arn` on
   top), with session tokens and refresh before expiry. ClickHouse's `s3()`
   gets the same temporary credential, resolved before every statement
   (`--ch-s3-auth pass`, the default), or none and the server's own
   (`--ch-s3-auth server`: give the ClickHouse pod its own IRSA role; the
   consumer sets `s3_allow_server_credentials_in_user_queries`). `eks/iam.sh`
   still makes the IAM **user** for the consumer, so the manifests are
   unchanged; an IRSA role with `iam/consumer.json` + `iam/gc.json` on the
   consumer's service account now works as well (drop `--key`/`--secret`).
2. ~~**`consume` signs for `us-east-1`.**~~ **Closed.** `--region`, else
   `AWS_REGION`, `AWS_DEFAULT_REGION`, the profile's `region`, else
   `us-east-1`, for signing and for the `s3://` endpoint (hence the `s3()`
   URLs). `REGION` still defaults to us-east-1 here only for the prices;
   another region needs `AWS_REGION` in the consumer's environment (or
   `--region`).

**Environment for every step** (one shell; `RUN` names the run everywhere):

```sh
cd otel-chdb/deploy/validation
export RUN=v1 REGION=us-east-1 BUCKET=<new-bucket-name> CLUSTER=otel-val-v1
export KUSTOMIZE=/path/to/kustomize          # 5.7; the overlays need --load-restrictor LoadRestrictionsNone
# $STATE defaults to ~/.otel-validation/$RUN: results.tsv, the ledger, every report
```

**Time and cost** (us-east-1 list prices; README has the totals):

| Step | Wall time | Cost |
|---|---|---|
| §1 images + cluster | 1.5–2 h (Rust builds dominate) | cluster ~$0.80/h while it exists |
| §2 bucket, IAM | 5 min | – |
| §3 acceptance suite | 1 h (+65 min for the credential hold) | < $1 of requests |
| §4 ABAC | 20 min | – |
| §5 per-prefix load | 45 min | ~$30 (6 M PUTs at $0.005/1k) |
| §6–§9 deploy, latency, 17 fault scenarios | 4–5 h | < $5 of requests |
| E15 soak | 24 h | ~$20 cluster + < $5 requests |
| **Campaign** | **2 working days + 1 night** | **~$80–120** |

## 1. Images and cluster

```sh
eks/images.sh                  # ECR repos otel-validate-$RUN/*, builds and pushes: otap-s3pq, otelcol-deploy,
                               # otelcol-deploy-lbpatched, otelcol-s3pq, toolbox -> $STATE/images.env
eks/up.sh                      # eksctl create cluster -f $STATE/cluster.yaml (eks/cluster.yaml.tmpl):
                               # 3 x m6i.xlarge in ng-main, ng-clock at 0 nodes, OIDC (IRSA),
                               # eks-pod-identity-agent, EBS CSI; a default gp3 StorageClass;
                               # an S3 gateway endpoint in the VPC -> $STATE/kubeconfig
export KUBECONFIG=$HOME/.otel-validation/$RUN/kubeconfig
kubectl get nodes -L topology.kubernetes.io/zone     # record the AZs: §8 E11 depends on them
```

The node groups set `disablePodIMDS: true` (hop limit 1): a pod whose
webhook did not fire gets no credentials instead of the node's role
(deploy/README.md §EKS calls a fallback a finding in itself).

## 2. Bucket and IAM

```sh
eks/bucket.sh                  # creates $BUCKET if missing: tagged, public access blocked, a lifecycle rule
                               # expiring validation/ after 7 days; records versioning/encryption/policy
eks/iam.sh                     # roles and the consumer user -> $STATE/iam.env, Secret otel-validate/consumer-s3
```

What `iam.sh` makes, all from `deploy/iam/*.json` rendered for
`ROOT=validation/$RUN/edge` and `ENTITIES=validation/$RUN/entities`:

| Role / user | Trust | Policy | Used by |
|---|---|---|---|
| `$NAME-edge` (tag `cluster=$CLUSTER`) | IRSA: `otel-edge:otap-publisher`, `otel-edge:otelcol-publisher`, `otel-validate:abac-self` | `edge-publisher.json` | the publishers (IRSA variant), §4 pods |
| `$NAME-edge-other` (tag `cluster=$CLUSTER-other`) | IRSA `otel-validate:abac-other` | `edge-publisher.json` | §4: a second cluster |
| `$NAME-edge-untagged` (no tag) | IRSA `otel-validate:abac-untagged` | `edge-publisher.json` | §4: `NoSessionWithoutAClusterTag` |
| `$NAME-edge-podid` | `pods.eks.amazonaws.com` | `edge-publisher.json` with `aws:PrincipalTag/eks-cluster-name` | Pod Identity variant |
| `$NAME-entityctl` (tag `cluster=$CLUSTER`) | IRSA `otel-validate:entityctl` | `entity-controller.json` | real-cluster-telemetry.md §5 |
| `$NAME-abac-sts` | the account, `sts:TagSession` | `edge-publisher.json` | §4 session tags from the shell |
| `$NAME-consumer-sts` | the account | `consumer.json` + `gc.json` | §4 positive rows |
| `$NAME-accept`, `-accept-nolist`, `-accept-prefixlist`, `-accept-ifexists` | IRSA `otel-validate:s3accept*` | the run prefix; `ListBucket` full / none / `s3:prefix`-scoped / scoped + `StringLikeIfExists` (the amended `deploy/iam/` shape) | §3 |
| user `$NAME-consumer` | – | `consumer.json` + `gc.json` + the run prefix | the consumer, GC, the toolbox |

## 3. The acceptance suite against real S3 (EKS-1, -2, -3, -4, -7)

The kit and its interpretation are `acceptance/RUNBOOK.md`; this runs it
from pods under IRSA, on the cluster's nodes and network path.

```sh
eks/accept.sh                  # dry creds run1 run2 race conflicts list perf headmissing
eks/accept.sh hold             # optional: 65 min across a real IRSA refresh (run in a second shell)
```

| Step | What it runs | Pass |
|---|---|---|
| `creds` | `s3accept creds --modes auto` | `irsa` and `chain` PASS; `identity` names `$NAME-accept`, not the node role |
| `run1`, `run2` | the whole suite, twice | verdicts **control-plane ACCEPTED, inline-consumer ACCEPTED, exporter-data ACCEPTED**; every required check PASS in both runs |
| `race` | `create-race` 64 × 50, `cas-race` 32 writers, `ambiguous-*` × 50 | exactly one winner per round; no lost CAS update; every ambiguous write resolved exactly once |
| `conflicts` | `create-race` 128 × 200 | record the 409 count (`data.conflicts_409`) out of 25,600 attempts; any error other than 409/412 fails |
| `list` | `list`, `list-race` (32 lanes), `read-after-write`, 500 keys | 0 acked keys missing from a later LIST, 0 holes; LIST lag 0 (AWS documents strong LIST-after-write) |
| `perf` | PUT/GET/HEAD/LIST at 200 B, 1 MB, 3 MiB, 8 MiB; concurrency 1 and 8 | recorded (§7 uses PUT p50/p99 at 3 MiB) |
| `headmissing` | `head-missing` under full, no, prefix-scoped, and prefix + `IfExists` `ListBucket` | full: 404 PASS; none: 403 (expected FAIL: that is why the policies grant it); prefix-scoped: **expected 403** (the pre-amendment grant; record it, it confirms the D18 amendment); **ifexists: 404 PASS, the answer to EKS-4** |

The prefix-scoped row is what `deploy/iam/` granted until 2026-09-28. S3
answers 404 for a missing key only to a caller with `s3:ListBucket`, and a
HEAD carries no `s3:prefix`, which IAM evaluates as a non-match (DECISIONS
D18 amendment cites both documents). Under that grant every free slot, and
every missing lease, checkpoint and `format.json`, reads as 403, which
AMBIGUITY S5 rightly keeps unknown: the publishers leave a lane unresolved
after any ambiguous PUT, and the consumer does not start. The policies now
add the same prefixes under `StringLikeIfExists`.

**If `ifexists` shows 403**, the amendment does not hold on AWS: stop before
§6 and grant `s3:ListBucket` on the bucket without a condition in
`iam/edge-publisher.json`, `entity-controller.json`, `consumer.json` and
`gc.json` (every role then lists every prefix's key names, not their
contents; DECISIONS D18 rejected it only as the wider grant). **If
`prefixlist` shows 404**, AWS puts the key into `s3:prefix` for this check;
the amended policy is still right (it covers both), note it in D18.

## 4. Write-side ABAC with real session tags (EKS-6)

```sh
eks/abac.sh                    # sts, then pods
```

- **sts phase**, from your shell: the edge policy is assumed three times,
  with the session tag `cluster=$CLUSTER-sts1`, `cluster=$CLUSTER-sts2`, and
  no tag; then the consumer/GC role. The matrix is `eks/abac_matrix.sh`, the
  AWS translation of `iam/seaweedfs_abac.sh`: its own slot 200, again 412,
  its own HEAD 200, the free slot **404** (a checked row since the D18
  amendment; EKS-4), a LIST with no prefix (INFO: 403, or 200 = the
  residual D18 accepts: key names only), the other
  cluster's prefix 403 (PUT, HEAD, LIST), the lease and watermark 403, an
  `_x` pseudo-cluster 403, every DELETE 403, a plain PUT of a slot 403 when
  the bucket policy is on; the untagged session: 403 everywhere. The
  consumer: HEAD, tombstone, lease create-only 200, plain lease PUT 403
  (bucket policy: CAS), gc.json 200, the GC delete 200.
- **pods phase**: the same matrix from Jobs under the real credential paths:
  IRSA with role tags `cluster=$CLUSTER` (`abac-self`) and
  `cluster=$CLUSTER-other` (`abac-other`), IRSA untagged (`abac-untagged`,
  all 403), and **Pod Identity** (`abac-podid`, where the tag is the
  automatic `eks-cluster-name` session tag).

**Pass:** 0 FAIL rows in `$STATE/abac-*.txt`; `results.tsv`
`abac.free_slot_head` all 404 (403 is a FAIL row now: the policy, not
ABAC; see §3 "If `ifexists` shows 403"). The untagged session wants 403
everywhere, the free slot included.

## 5. Per-prefix request limits (EKS-5)

```sh
eks/load.sh                    # PLAN="cluster-first:2000:300 cluster-first:6000:600 single-lane:6000:300" PODS=4
```

`s3accept load` (`acceptance/s3accept/load.go`) sends create-only PUTs at a
target rate, open loop, from 4 pods sharing one run id, so every key shares
the prefix a fleet shares. Keys are the edges' own
`{root}/cNN/edge-P/{signal}/{epoch}/{seq}.parquet` (20 clusters × 3
producers × 7 signals = 420 lanes); `single-lane` puts every PUT in one lane
for contrast. SDK retries are off: every 503 SlowDown is counted as sent.
Each pod deletes what it wrote.

| Phase | Why |
|---|---|
| 2,000/s for 5 min | under the documented 3,500: expect 0 SlowDown. The fleet's steady state is ~90 PUT/s (7.8 M objects/day, §3 of DECISIONS), so this is 20× it |
| 6,000/s for 10 min, cluster-first | above one partition: a drain after a regional S3 outage (60 publishers × several lanes × up to ~20 PUT/s each) looks like this. Does S3 split the prefix, and how fast? |
| 6,000/s for 5 min, single-lane | the worst layout, for contrast |

Read `$STATE/load/<phase>.tsv` (per second, summed over pods) and the
per-minute summary lines. **Pass:** phase 1 has no 503; in phase 2 the 503
share falls to < 1% within the phase (S3 split the prefix) and `missed` is 0
(else the client was the limit: raise `PODS`). **Record** the first second
with a 503, the achieved ok/s per minute, and the time to < 1% 503. A 503
is not a correctness problem (the edge retries it as an ambiguous PUT, and
the consumer never skips a gap), but it bounds the drain rate.

## 6. Deploy the edge and the validation central (EKS-7)

```sh
eks/deploy.sh                  # EDGE=rust VARIANT=irsa: the overlay deploy/overlays/rust-eks-irsa with this run's
                               # cluster, bucket, prefix, images and role, via $STATE/overlay-rust-eks-irsa/;
                               # then ClickHouse (StatefulSet, 100 Gi), consume (2 workers), consume gc (60 s, audit 1 h),
                               # the toolbox pod
cat $STATE/results.tsv | grep deploy.            # the publisher's AWS_* env: AWS_ROLE_ARN and the token file, not keys
kubectl -n otel-edge logs otap-publisher-0 | grep -i birth     # births committed: "births: 7 of 7"
```

**Pass:** the publishers are Ready; each has committed its births (7 lanes
per publisher with layout B); `kubectl -n otel-validate logs deploy/consume`
shows the lanes discovered; no `AccessDenied` anywhere. **If births stay
unresolved** with 403 on HEAD, go back to §3's EKS-4 finding.

## 7. Commit latency and lane throughput (EKS-8)

Locally a lane committed ~20 batches/s: 1 / (encode + PUT) with a PUT of a
few ms. On S3 the PUT dominates.

```sh
# a) PUT latency at the edge's object sizes (one lane = concurrency 1): from §3
grep -A12 'perf' $STATE/accept/perf1.txt
# b) saturate the lanes and count objects per lane from S3 LastModified, GC paused so objects stay
eks/fleet_test.sh gc off
for d in 21 22 23 24; do eks/fleet_test.sh send $d; done; for d in 21 22 23 24; do eks/fleet_test.sh waitsend $d; done
eks/fleet_test.sh lanes        # objects/s and MB/object per lane
eks/fleet_test.sh gc on
# c) end to end: received_at -> visible in ClickHouse
eks/fleet_test.sh metrics latency; grep consumer_visible_seconds $STATE/fleet/metrics-latency.txt
```

**Record:** PUT p50/p99 at 3 MiB and 8 MiB; objects/s per lane at
saturation; `consumer_visible_seconds` quantiles. **Pass:** a lane's
throughput × a publisher's lanes covers the mid scenario's per-publisher
object rate (~1 object/s per signal at 10k rows, DECISIONS §1.2) with ≥ 5×
headroom, so a 1-hour backlog drains in < 15 min; visibility p99 < 30 s (the
owner's near-tail of 15–40 s). If not, the drain time after an outage is
backlog / (lanes × throughput): report it.

## 8. Fault scenarios on EKS (EKS-9, EKS-12)

The kind run's scenarios (`results/k8s-sim.md` §8), each while a dataset
(32 traces + 32 logs requests of 10k rows) is in flight, each checked for
exactly once. `D` increments per scenario; `check` waits for 320,000 rows
per signal and prints rows / duplicates / missing.

```sh
F=eks/fleet_test.sh
$F metrics before
```

| # | Scenario | Commands (after `$F send D`; then `$F waitsend D; $F check D`) | Expect |
|---|---|---|---|
| E1 | steady, 3 publishers | – | 320,000 / 0 dup / 0 missing per signal |
| E2 | SIGKILL publisher-1, force-delete publisher-0, SIGKILL the agent on a node | `$F sigkill otap-publisher-1; $F forcedelete otap-publisher-0; $F sigkill <otel-agent-pod> agent` | exactly once (kind row 8) |
| E3 | SIGKILL publisher-0 and -2 | `$F sigkill otap-publisher-0; $F sigkill otap-publisher-2` | exactly once (WAL replay) |
| E4 | agent rollout restart | `$F restart agents` | exactly once |
| E5 | SIGKILL an agent twice | `$F sigkill <agent-pod> agent; sleep 5; $F sigkill <agent-pod> agent` | exactly once (the no-batch agent, kind row 7) |
| E6 | publisher rollout restart | `$F restart publishers` | exactly once |
| E7 | scale 3→4 mid-send, 4→3 | `$F scale 4; sleep 20; $F scale 3` | exactly once; ordinal 3 gets traffic within ~5.5 min (GOAWAY after `OTLP_MAX_CONN_AGE`, not seen on kind) |
| E8 | scale-down with data in ordinal 3's retained PVC, then back | `$F scale 4; $F restart agents; $F s3cut on; $F send D; $F waitsend D; $F scale 3; $F s3cut off`, check (dataset D short by ordinal 3's share), later `$F scale 4` and check again | late, not lost: ordinal 3's rows arrive when it returns (kind row 12); `EdgeBufferVolumeOrphaned` would fire meanwhile (runbooks/scale-down.md). Then `$F scale 3` |
| E9 | **full buffer on a real EBS volume, real PVC expansion** (a fresh ordinal: 3's PVC exists after E8) | `$F smallpvc 4 2Gi; $F scale 5; $F restart agents; $F s3cut on;` send until otap-publisher-4 is not ready ("under 1 GiB free"); `$F s3cut off; $F growpvc 4 10Gi; $F forcedelete otap-publisher-4`; check; `$F scale 3` | not ready within seconds (edgeprobe); the online expansion completes (record how long: `growpvc` logs it); after the restart every acked row once |
| E10 | S3 unavailable 10 min for the whole fleet (an IAM Deny: 403, which the edge must keep unresolved) | `$F s3cut on; $F send D; sleep 600; $F s3cut off` | 0 lost, 0 duplicates; `s3pq_commit_outcomes_total{outcome="unresolved"}` rises and stops; buffers grow then drain; record drain time |
| E11 | node drain (multi-node; EBS is per-AZ) | `$F drain` | exactly once. **Watch:** a publisher's EBS PVC can only follow it to a node in the same AZ; with one node per AZ it stays Pending until `uncordon`. Record it: a production node group needs ≥ 2 nodes per AZ or topology spread for the publishers |
| E12 | routing on (`OVERLAY=rust-eks-irsa-routing eks/deploy.sh edge`), steady | – | exactly once, 120 services on one publisher each |
| E13 | routing: SIGKILL two gateways | `$F sigkill <otel-gateway-pod> gateway` × 2 | duplicates as documented (re-batched pieces, kind row 15), 0 missing; `consumer_audit_duplicate_rows` counts them |
| E14 | routing: gateway rollout restart | `$F restart gateways` | exactly once |
| E15 | **soak 24 h**: 1 dataset every 10 min (`for d in $(seq 100 243); do $F send $d; sleep 600; done`), consumer, GC and watermark running | then `$F total; $F audit; $F metrics soak` | exactly once over 144 datasets; GC lag bounded; `consumer_complete_through_lag_seconds` < 5 min; checkpoint and gc.json size stable (S3 `ls` of `_consumer/`) |
| E16 | Pod Identity variant: `VARIANT=pod-identity eks/deploy.sh edge`, then E1 and E3 | as E1, E3 | exactly once; the publisher's env shows `AWS_CONTAINER_CREDENTIALS_FULL_URI`, no `AWS_ROLE_ARN` |
| E17 | Go edge: `EDGE=go eks/deploy.sh edge` (IRSA), then E1, E3, E6 | as E1 | exactly once. (A full Go volume loses acked data, U22; do not repeat E9 on the Go edge except to confirm U22 on EBS, and label it) |

After each group: `$F metrics <name>`; the snapshot has the publishers'
`s3pq_commit_outcomes_total{outcome}` (`resolved_own` counts the 409s and
lost answers the protocol absorbed), buffer fill, and the consumer's
counters. `$STATE/fleet/events.log` has every fault with its time.

## 9. Exactly-once verification (EKS-10)

Per dataset, `check` (above). Across the run:

```sh
eks/fleet_test.sh total        # traces and logs: rows, distinct, duplicates over everything ingested
eks/fleet_test.sh audit        # consume horizon-audit --check-horizon all: late copies and re-cut duplicates
```

**Pass:** every dataset 320,000 / 0 / 0 except E13 (documented duplicates,
counted by the audit); `total` duplicates = E13's; the audit reports no late
copy; no `inconsistent` commit outcome anywhere (that would mean S3 broke
read-after-write).

## 10. Credentials across time (EKS-7)

`eks/accept.sh hold` keeps one request a minute for 65 min under IRSA,
across the 1-hour session: 0 errors, ≥ 1 refresh. The E15 soak does the same
for the publishers (IRSA) and E16 for Pod Identity; a refresh failure shows
as `unresolved` commits and `AccessDenied`/`ExpiredToken` in the publisher
log.

## 10a. Basis signing with KMS (EKS-13)

Not scripted in `eks/` yet (no query service is deployed by this runbook);
run by hand, and record everything you create in the ledger for
`down.sh` (`created kms-key …` is not handled by `down.sh`: schedule the
keys' deletion yourself, 7 days, at teardown).

```sh
# two HMAC keys (current, previous) and a symmetric one for the negative check
for k in cur prev; do
  aws kms create-key --region "$REGION" --key-spec HMAC_256 --key-usage GENERATE_VERIFY_MAC \
    --description "otel-chdb validation $RUN basis $k" --tags TagKey=run,TagValue="$RUN" \
    --query KeyMetadata.Arn --output text
done   # -> CUR_ARN, PREV_ARN
# the key policy: deploy/iam/query-basis-kms-key-policy.json with ACCOUNT and
# QUERY_SERVICE_ROLE filled in (aws kms put-key-policy --policy-name default)
# the role: an IRSA role (eks/iam.sh irsa_trust for otel-validate:query-basis)
# with deploy/iam/query-basis-kms.json, REGION/ACCOUNT/BASIS_KEY_CURRENT/
# BASIS_KEY_PREVIOUS filled in; then ci/iam-lint.sh on the rendered copy
(cd otel-chdb/query && GOOS=linux go test -c -o "$STATE/app.test" ./internal/app)
kubectl -n otel-validate create sa query-basis   # annotated with the role ARN
kubectl -n otel-validate run kms-basis --restart=Never --overrides='{"spec":{"serviceAccountName":"query-basis"}}' \
  --image=public.ecr.aws/amazonlinux/amazonlinux:2023 -- sleep 3600   # has CA certificates
kubectl -n otel-validate wait --for=condition=Ready pod/kms-basis --timeout=120s
kubectl -n otel-validate cp "$STATE/app.test" kms-basis:/tmp/app.test
kubectl -n otel-validate exec kms-basis -- env QS_TEST_KMS_KEYS="$CUR_ARN,$PREV_ARN" AWS_REGION="$REGION" \
  /tmp/app.test -test.run TestKMSEmulator -test.v | tee "$STATE/eks-13.txt"
kubectl -n otel-validate delete pod kms-basis
```

Pass: `PASS`, the log line with mint / uncached / cached latency (record
p50 and p95), and under the role: every token minted by one `Bases`
verifies on the other, tampered tokens are `basis_invalid`, minting with the
previous key is refused (`AccessDeniedException` → `basis_signer_unavailable`:
the IAM policy's verify-only grant holds). Then, by hand: start `queryd`
(or the same test) with `QS_BASIS_KMS_KEYS` naming a symmetric key: startup
fails with `key spec SYMMETRIC_DEFAULT, want HMAC_256`; with a key the role
may not `DescribeKey`: startup fails with `AccessDeniedException`; the
CloudTrail events show only the two configured key ARNs.

## 11. Cost capture (EKS-11)

```sh
eks/cost.sh start              # before §3: an S3 request-metrics filter on validation/$RUN/ (CloudWatch)
eks/cost.sh report             # after each section (15 min lag): requests per operation, bytes, $ at list price
```

Split by time window (`cost.sh report FROM TO`) for: §3 (the kit), §5 (the
load), E15 (the soak: the steady-state cost). From E15 compute and record:

- PUT per object (should be 1 + the heartbeats: one per idle lane per 30 s);
- LIST per lane per hour (the consumer's discovery: locally one per idle lane
  per 30 s) and GET/HEAD per object (the consumer's HEAD, ClickHouse's GET);
- the consumer's own control writes (lease renewal per 15 s per held lane,
  checkpoints, gc.json);
- $ per million rows, to replace the calculator's "S3 PUTs ≈ $1.2k/month"
  estimate with a measured per-object cost.

## 12. Teardown

```sh
cp -r ~/.otel-validation/$RUN <somewhere safe>     # results, reports, rendered manifests
eks/down.sh                    # removes the ledger's entries newest first: k8s objects, Pod Identity associations,
                               # the VPC endpoint, IAM roles and the user (and its keys), ECR repos, the bucket
                               # metrics filter, the bucket policy (only if unchanged), objects under validation/$RUN/,
                               # the EKS cluster. The bucket is kept (its lifecycle rule expires validation/).
```

## 13. Pass/fail summary

| Q | Pass | Fail means |
|---|---|---|
| EKS-1 | control-plane and inline-consumer ACCEPTED twice; races exact | the design cannot use this store: the Keeper coordinator fallback (DECISIONS risk 1) |
| EKS-2 | 409s only in races, resolved; `resolved_own` > 0 is fine | a 409 that ends as `unresolved` for long: a bug in the resolution path |
| EKS-3 | 0 missing, 0 holes | a LIST that lags: the consumer is still correct (never skips a gap) but the visibility budget grows |
| EKS-4 | 404 under the amended grant (`ifexists`, and the §4 free-slot rows); prefixlist 403 recorded; the prefix-less LIST recorded | 403 under `ifexists`: grant `ListBucket` without a condition in all four role policies (§3) |
| EKS-5 | no 503 at 2,000/s; < 1% within 10 min at 6,000/s | throttling below the drain rate: spread prefixes further (a hashed first segment) or cap drain concurrency |
| EKS-6 | 0 FAIL rows | the boundary R-S7 relies on is not what the policy says |
| EKS-7 | both variants commit; refresh crosses; no node-role fallback | a credential path that does not work on EKS |
| EKS-8 | ≥ 5× headroom per lane; visibility p99 < 30 s | size lanes (`lanes: N`) or objects up; record the drain time |
| EKS-9/10 | every scenario exactly once (E13 as documented) | a new loss or duplicate path: stop, keep the evidence, open a DECISIONS risk |
| EKS-11 | measured $ per object and per lane within 2× of the calculator's | update the calculator's prices/rates |
| EKS-12 | watermark lag < 5 min, GC keeps up | AMBIGUITY S4/S7 stay *partly*; risk 6 |
| EKS-13 | cross-process tokens verify; tampering and a previous-key mint refused; mint and uncached verify p95 < 50 ms | an IRSA path KMS does not accept, or an IAM grant wider than the policy: fix the policy before enabling `basis.signer: kms`; a p95 above 50 ms: raise `mint_reuse_s` |

## 14. Rows to update with the results

Fill `results/TEMPLATE.md` (section EKS) first; then:

- [ ] **DECISIONS §1.1** EKS row: "real EKS/STS never used" → what ran (IRSA, Pod Identity, both edges).
- [ ] **DECISIONS §1.4** `ATOMIC_COND` bullet: add real S3 [M] (EKS-1).
- [ ] **DECISIONS §4 risk 3** (real AWS behaviour): 409 rate (EKS-2), per-prefix result (EKS-5), commit latency and lane throughput (EKS-8), LIST/renewal cost billed (EKS-11); retire or re-rank.
- [ ] **DECISIONS §4 risk 6** (GC): GC lag and checkpoint size over the soak (EKS-12).
- [ ] **DECISIONS D8** (idle-lane LIST backoff): billed LIST count per lane.
- [ ] **DECISIONS D18**: IRSA and Pod Identity measured; the consumer's chain and region (closed 2026-09-28, §0) measured if the consumer ran under IRSA or outside us-east-1; the `ListBucket` amendment confirmed or replaced (EKS-4).
- [ ] **DECISIONS §3** calculator: the S3 PUT/GET/LIST $ per object (EKS-11) replacing the list-price estimate.
- [ ] **AMBIGUITY S1, S3**: AWS measured (409 → HEAD; 412 for our own write) → status *handled* for AWS.
- [ ] **AMBIGUITY S5**: AWS 404/403 with each `ListBucket` grant (EKS-4) → *handled* for AWS (the amended policy [D] → [M]), or the unconditioned grant; the prefix-less LIST result (the residual).
- [ ] **AMBIGUITY S6**: AWS [D] → [M] (`list-race` on S3).
- [ ] **AMBIGUITY S4, S7, S11**: GC and watermark over the soak.
- [ ] **AMBIGUITY E1, E3b, E4, E5**: the EKS rows (real PVC expansion, readiness, drain).
- [ ] **STPA R-S7**: "proposed, not built" → measured on AWS with session tags (do not edit STPA.md from this runbook's session; hand the row to its owner).
- [ ] **deploy/README.md §Validation**: an EKS row; "Not exercised: EKS and its webhooks, real PVC expansion, multi-node" → what ran.
- [ ] **deploy/results/**: copy `$STATE/fleet/checks.txt`, `events.log`, `abac-*.txt`, `load/*.tsv` as `eks-*.txt`.
- [ ] **acceptance/RUNBOOK.md §9**: an AWS results subsection (the kit's own "not tested at all here" list).
- [ ] **DECISIONS D30** amendment (KMS signer): "Not run against AWS KMS" → EKS-13's result; **AMBIGUITY X17**: the AWS latency, and the account's symmetric-crypto quota headroom (Service Quotas) → *handled* or a `mint_reuse_s` / quota increase.
