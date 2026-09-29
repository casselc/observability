# Validation runbooks: what one 4-vCPU box could not settle

Everything the spike measured ran on one shared 4-vCPU box: SeaweedFS on
localhost, a local ClickHouse, a 3-node Keeper on loopback, kind as the only
real cluster, synthetic telemetry. These runbooks are ready to execute as soon
as a real environment exists (a small EKS cluster on AWS, a Nutanix site, an
idle dedicated machine), and each one answers named questions tied to a
DECISIONS.md risk, an AMBIGUITY.md row, a calculator constant or a claim.

| Runbook | Environment | Settles | Wall time | Cost (on-demand, list) |
|---|---|---|---|---|
| [eks-aws.md](eks-aws.md) | **EKS** (new account or sandbox), us-east-1 | real S3 semantics, 409s, LIST, per-prefix limits, IRSA and Pod Identity, ABAC with session tags, commit latency and lane throughput, the kind fault scenarios on real nodes and EBS, exactly once, request costs | 2 days + a 24 h soak | ~$80–120 |
| [nutanix.md](nutanix.md) | **Nutanix Objects** (+ a site cluster and a VM for §5) | risk 1: conditional writes across gateways, LIST/HEAD consistency, metadata, checksums, ETag, static keys + CA, ABAC without STS | 1 day | no cloud cost |
| [real-cluster-telemetry.md](real-cluster-telemetry.md) | **a real cluster with real workloads** (a read-only production mirror if the owner allows it) + the idle machine as central | real rates and bytes/row, attribute cardinalities, `resource_id` distribution, **the entity-schema savings on real rows (the switch decision's gate)**, the entity controller at real churn | 1–7 days observed + ~3 h | ~$35/day (bench machine) + storage |
| [central-idle-bench.md](central-idle-bench.md) | **an idle dedicated machine** (32 vCPU) + 2 replicas and **3 Keeper hosts** | `usRow`, `mergeRow`, `fixedMs` at production statement sizes, option 2's ratio idle, replicated cost, **the Keeper overrun bound** on real hosts, C7 | 1 day + a 24 h merge run | ~$60–80 |
| [clocks-and-skew.md](clocks-and-skew.md) | **any cluster** for the probe (production is fine); the EKS cluster's `ng-clock` node for the steps | AMBIGUITY E6: node skew and drift, worker vs ClickHouse, edge vs S3; `--wm-skew`, the fence margin, the zombie bound, SigV4; a clock stepped on purpose | 24 h–7 days unattended + 2 h | < $1 |
| [entra-ingress.md](entra-ingress.md) | **an Entra tenant** (app registrations, a CA policy scoped to the ingress API), one Intune-managed Windows 11 and one macOS 14+ device, the EKS cluster | D37: real tokens, the refusal matrix, WAM and the macOS SSO plug-in, the three Langfuse integrations through the forwarder, buffer fates, retries as copies, the drain's close, pair grants | ~1 day + an overnight offline test | < $5 |

Order: eks-aws.md §1–§4 first (the store's semantics gate everything),
nutanix.md as soon as a site exists (risk 1 is the top risk), then
central-idle-bench.md §0 (its machine is the central for
real-cluster-telemetry.md), clocks-and-skew.md §2 can run from day one
beside anything.

## The questions

Each question has an id used in its runbook, in `results/TEMPLATE.md`, and
in the checklist of rows to update at the end of each runbook.

| Q | Question | Tied to | Runbook | Pass |
|---|---|---|---|---|
| EKS-1 | Atomic `If-None-Match: *` / `If-Match`, ambiguous writes resolved, on real S3 | §1.4 `ATOMIC_COND`; AMB S1, S3; risk 3 | eks-aws §3 | kit verdicts ACCEPTED twice; races exact |
| EKS-2 | 409 `ConditionalRequestConflict` rate; absorbed as `resolved_own` | risk 3; AMB S1, S3 | eks-aws §3, §8 | only 409/412 in races; none left unresolved |
| EKS-3 | LIST after write, StartAfter, list-race | AMB S6 (AWS [D]) | eks-aws §3 | 0 missing, 0 holes |
| EKS-4 | Free slot 404 vs 403 with full / no / prefix-scoped / prefix + `IfExists` `s3:ListBucket`; a prefix-less LIST under the last | AMB S5; `iam/*.json`; D18 amendment (2026-09-28: the prefix-scoped grant answers 403 by AWS's documentation, now fixed [D]) | eks-aws §3, §4 | 404 under the amended grant (else grant `ListBucket` unconditionally); prefix-scoped 403 recorded |
| EKS-5 | Throttling of create-only PUTs in the cluster-first layout | risk 3 (3,500 PUT/s per prefix) | eks-aws §5 | 0 × 503 at 2,000/s; < 1% within 10 min at 6,000/s |
| EKS-6 | ABAC with session tags: cross-cluster, delete, control, create-only | STPA R-S7; D18 | eks-aws §4 | 0 FAIL rows |
| EKS-7 | IRSA and Pod Identity end to end, refresh, no node-role fallback | §1.1; D18 | eks-aws §3, §6, §10 | both commit; refresh crosses; no fallback |
| EKS-8 | Commit latency, lane throughput with real latency | risk 3 | eks-aws §7 | ≥ 5× headroom per lane; visibility p99 < 30 s |
| EKS-9 | kind fault scenarios on EKS (real EBS, PVC expansion, drain, S3 outage, gateways) | k8s-sim §8; AMB E1–E5; STPA LS-9 | eks-aws §8 | exactly once (gateway SIGKILL: documented duplicates) |
| EKS-10 | Exactly once end to end, audit silent | D3, D11 | eks-aws §9 | 0 missing, 0 unexplained duplicates |
| EKS-11 | Requests and bytes per object and per lane, billed | §3 prices; D8 | eks-aws §11 | within 2× of the calculator |
| EKS-12 | GC and `complete_through` over a day | AMB S4, S7, S11; risk 6 | eks-aws §8 E15 | watermark lag < 5 min; GC keeps up |
| NX-1…3 | Conditional writes enforced atomically across gateways; ambiguous writes resolvable | **risk 1**; AMB S1–S3, S10 | nutanix §2 | ACCEPTED with `--race-endpoints` |
| NX-4, 5 | Read-after-write, 404 for missing, LIST consistency | AMB S5, S6 (Nutanix unverified) | nutanix §2 | PASS; LIST lag recorded |
| NX-6, 7 | `x-amz-meta-*`, CRC32 trailer | §1.1 Nutanix row | nutanix §2 | PASS (or `when_required` kept) |
| NX-8 | ETag = MD5 | AMB X2 | nutanix §2 | recorded |
| NX-9 | Latency, throttling | risk 3 (other target) | nutanix §2 | recorded |
| NX-10 | Static keys, CA, path-style in both edges and the consumer; ClickHouse's `s3()` | §1.1; D18 | nutanix §3, §5 | births commit; 497 or not recorded |
| NX-11 | ABAC without STS: per-cluster keys + bucket policy | STPA R-S7; D18 | nutanix §4 | 0 FAIL rows with the policy, or the fallback recorded |
| NX-12 | The edge at the site: exactly once through kills and an Objects outage | AMB E1–E5 | nutanix §5 | exactly once |
| NX-13 | Versioning / WORM | risk 6 | nutanix §2 | recorded |
| TEL-1…7 | Real rates, bytes/row (merged), Parquet bytes, attribute shapes, `resource_id` distribution, rows per object | §1.2 [E]; §3 `bSpan`, `bLog`, `bPq`; **risk 2** | real-cluster-telemetry §1, §3 | each replaces an estimate; > 2× off is flagged |
| TEL-8, 9 | **Entity schema on real rows** vs option 2: insert, merge, bytes; exact views | **D21 switch**; risk 7b (d); entities §1 | real-cluster-telemetry §4 | the gate: ≥ 2.0× insert, ≤ −30% bytes, exact |
| TEL-10, 11 | Entity controller at real churn; `resource_id` agreement | controller README; k8s-sim §2–5; STPA LS-5 | real-cluster-telemetry §5 | freshness p99 < 30 s; 100% agreement or explained |
| CEN-1…4 | `usRow`, option-2 ratio, `mergeRow`, `fixedMs` on an idle machine at production sizes | §3 constants; **risks 7, 7b** | central-idle-bench §2–§4 | ±15% over reps; replaces the loaded-box numbers |
| CEN-5 | Replicated insert cost, idle | risk 9; D13 | central-idle-bench §5 | recorded; into the calculator |
| CEN-6 | **Keeper overrun bound** on 3 real Keeper hosts | risk 5c; AMB C1, C5; STPA LS-2 | central-idle-bench §6 | every commit < session timeout after its start |
| CEN-7 | HTTP 200 then an exception | AMB C7 | central-idle-bench §7 | recorded; `wait_end_of_query` if needed |
| CLK-1…5 | Node skew, drift, worker vs ClickHouse, edge vs S3; the constants' headroom; wrong-day rows | **AMB E6**; risk 5 | clocks-and-skew §2, §3 | every constant ≥ 10× headroom |
| CLK-6 | Stepped clocks: ±30 s, midnight, +16 min, −15 min | AMB E6 fault injection; risk 5b, 5d | clocks-and-skew §4 | exactly once; 403 stalls, never guesses |

**Not covered here** (other owners or later): HyperDX live against the views
(risk 4), IAM Roles Anywhere with a real `aws_signing_helper` (§1.1's third
target: acceptance/RUNBOOK.md §3.1 already has the commands), the lake /
sealer, a power-cut test of the edge buffer (risk 14), and any change to the
code gaps these runbooks find.

**Gaps these runbooks found, closed since** (DECISIONS D18 amendments,
2026-09-28; the runbooks' text says what changed):

| Gap | Found by | Closed by |
|---|---|---|
| `consume` took static keys only, and passed them into `s3()` | eks-aws §0 (1) | the AWS chain (IRSA, Pod Identity, profiles, IMDS, session tokens, refresh); `s3()` gets the temporary credential per statement (`--ch-s3-auth pass`) or the server's own (`server`); secrets redacted from every logged error |
| `consume` signed for us-east-1 | eks-aws §0 (2) | `--region` / `AWS_REGION` / `AWS_DEFAULT_REGION` / the profile's |
| the prefix-scoped `s3:ListBucket` in `deploy/iam/` makes a missing key 403 (every role; SeaweedFS could not show it) | eks-aws EKS-4, acceptance RUNBOOK §5 | `StringLikeIfExists` grant in all four role policies [D]; EKS-4 now measures it |

## What the owner must provide

| For | What |
|---|---|
| eks-aws | an AWS account or sandbox allowing EKS, IAM roles **and one IAM user with an access key** (the runbook's consumer uses it; since 2026-09-28 `consume` also takes IRSA / Pod Identity, so an organisation that forbids IAM users can drop it), ECR, a VPC, one new S3 bucket; 16 on-demand vCPUs; `sts:TagSession`; a build host with docker, Go 1.26, aws CLI 2.22+, eksctl, kustomize 5.7 |
| nutanix | the Objects FQDN, **every client-facing IP**, the version, the private CA, a bucket, 4 Objects users/keys (kit, two "clusters", consumer), how a policy names a user; for §5 a site cluster, a registry, a VM |
| real-cluster-telemetry | **a decision on mirroring production** (a second exporter in the production collectors, non-blocking); Prometheus read access; where real telemetry may be stored and for how long |
| central-idle-bench | a dedicated 32-vCPU machine (not burstable, not shared); 2 replica hosts and 3 Keeper hosts with SSH + passwordless sudo; the production Keeper session timeout to test |
| clocks-and-skew | read access to a cluster (production is fine for the probe); UDP/123 egress to the reference |

## Deploy order

**Consumers before edges** when upgrading a deployment to schema 3 (D36 payload offloading, on by default): a consumer from before reads the new objects but ignores the payload columns, so it would ingest offloaded rows **without their content, silently** (the content stays in the S3 object only until GC). Upgrade every consumer, check `consumer_payload_dangling_total` is exported, then the edges. A fresh deployment has no old consumer.

## How the scripts behave

- **Parameterised by the environment** (each script's header lists its
  variables); `RUN` names a run everywhere; `STATE`
  (`~/.otel-validation/$RUN`) holds the ledger, `results.tsv`, every report
  and every rendered manifest.
- **Idempotent**: a rerun skips what exists and reuses it.
- **Clean up only what they create**: every created resource is appended to
  `$STATE/ledger.tsv`; `eks/down.sh` deletes ledger entries only, newest
  first. Objects are only ever written and deleted under
  `validation/$RUN/` (`lib.sh` refuses any other prefix). **No script
  deletes a bucket**, and a bucket policy is applied only to a bucket this
  tooling created that had none, and removed only if unchanged.
- **Anything that costs money or breaks something on purpose asks first**
  (`YES=1` to skip): the cluster, the load, Keeper faults, clock steps
  (`CONFIRM_CLOCK_STEP=yes`).

```
validation/
  README.md                 this file
  lib.sh                    ledger, render, awss3, delete_run_objects, result
  eks-aws.md                eks/: cluster.yaml.tmpl up.sh images.sh bucket.sh iam.sh accept.sh load.sh abac.sh
                            abac_matrix.sh deploy.sh fleet_test.sh cost.sh down.sh; manifests/ (ClickHouse, consumer,
                            toolbox); images/toolbox.Dockerfile
  nutanix.md                nutanix/: accept.sh abac.sh bucket-policy.tmpl.json (+ eks/deploy.sh TARGET=nutanix)
  real-cluster-telemetry.md telemetry/: measure.sql measure.sh stage_real.sql controller.sh entityctl.yaml.tmpl
  central-idle-bench.md     central/: bench_insert.sh keeper_faults.sh
  clocks-and-skew.md        clocks/: sntp_probe.py skew_report.py run.sh skew-daemonset.yaml.tmpl s3_skew.sql clock_step.sh
  results/TEMPLATE.md       where results go, row by row, and which DECISIONS/AMBIGUITY/calculator rows they update
```

New in the acceptance kit for these runbooks: **`s3accept load`**
(`acceptance/s3accept/load.go`): create-only PUTs at a target rate, open
loop, in the format-v2 layout (`cluster-first`, `single-lane`, `hashed`),
per-second counts of ok / 503 SlowDown / 409 / 412 / 403 / transport errors
and latency, cleanup of what it wrote; `--run-id` lets several processes
share one prefix.

## Recording results

1. Each script appends its headline numbers to `$STATE/results.tsv`
   (`time`, `key`, `value`) and writes its full reports under `$STATE`.
2. Copy [`results/TEMPLATE.md`](results/TEMPLATE.md) to
   `results/<runbook>-<date>.md`, fill each question's row (value, pass/fail,
   evidence file), and commit it with the evidence files that matter
   (`deploy/results/` holds the run outputs, as for the local runs).
3. Then work through the runbook's **"Rows to update"** checklist: each
   answered question changes a named row of DECISIONS.md (a risk retired or
   re-ranked, an [E] made [M], a §1.1 "unknown" answered), AMBIGUITY.md (a
   status: *unverified* → *handled*, or the finding), or a calculator
   constant (then recompute §3). STPA.md rows are handed to its owner, not
   edited from a validation session.

## Local checks (2026-09-28)

Nothing here has touched AWS, Nutanix or a real cluster. What was checked
on the spike's box:

| Check | Result |
|---|---|
| every script: `bash -n`; shellcheck 0.11 `-x -S warning` | clean (info-level notes only: intended `A && B || C` and word splitting of `NAME=VALUE` credential lists) |
| every YAML template and JSON policy parses (placeholders substituted); `deploy/iam/*.json` render for a run (placeholders gone, valid JSON) | OK |
| the generated overlays (`eks/deploy.sh edge` with a stub kubectl): `rust-eks-irsa`, `rust-eks-pod-identity`, `go-eks-irsa`, `go-eks-pod-identity`, `rust-eks-irsa-routing`, `rust-nutanix` build with kustomize 5.7.1 `--load-restrictor LoadRestrictionsNone` | 0 `CHANGE-ME` left; the run's CLUSTER, bucket, `S3_BASE`, images, role ARN and CA in place |
| `eks/abac_matrix.sh` against a stub `aws` | the classification of ok / 403 / 404 / 412 and the row logic work |
| `s3accept load`: `go vet`, `go test` (a fake S3 answering every 4th PUT with 503 SlowDown) | pacing, classification, v2 keys and the report are right |
| `clocks/sntp_probe.py` against a stub NTP server 5 s ahead; `skew_report.py` on synthetic samples | offset −5,000 ms (sign right); report and verdicts right |
| `telemetry/measure.sql` (every block), `telemetry/stage_real.sql`, `clocks/s3_skew.sql`, `controller.sh`'s `joinrate` and `freshness` | ran against ClickHouse 26.10.1.618 on a scratch database with the consumer's DDL (`consume --print-ddl`), the series tables, the aggregator's catalog and 100 synthetic rows per signal (s3_skew against one Parquet object on SeaweedFS), then dropped. Found and fixed: an alias shadowing `rows` in `bytes_per_row`; `resource_ids` counting resources as rows |
| the query_log / part_log queries (`central/bench_insert.sh`, central-idle-bench.md §4) | parse (`clickhouse format`); not run: the local server has neither log |
| the Jobs, Pods and PVC the scripts generate (`abac.sh pods`, `fleet_test.sh send`, `smallpvc`, `load.sh`, `accept.sh`), captured through a stub kubectl | valid YAML; every container has a name and an image; no API-server validation (no kubeconform or KWOK here) |
| eksctl and Terraform | not installed here: `eks/cluster.yaml.tmpl` is checked as YAML only; `eksctl create cluster --dry-run -f` is the first thing to run on the build host |
