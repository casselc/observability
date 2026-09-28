# Nutanix Objects: validation runbook

Nutanix Objects is DECISIONS.md's **risk 1**: the commit protocol, the
consumer's leases and checkpoints and `gc.json` all need atomic
`If-None-Match: *` and `If-Match` on single-part PUT, and read-after-write
HEAD and LIST. Nutanix's API documentation is behind a support login and
nothing public says whether Objects decides a conditional write atomically.
A store that accepts the header and ignores it (old MinIO, Garage) looks
correct until two writers race. This runbook finds out, and then tries the
credential and ABAC model Objects allows (no STS: per-cluster keys and a
bucket policy).

The scripts are in `nutanix/` (and reuse `eks/abac_matrix.sh`,
`eks/deploy.sh TARGET=nutanix`, `eks/fleet_test.sh`). They create objects
only under `validation/$RUN/` in the bucket you are given, delete only
those, and never touch the bucket or its policy unless told to
(`ALLOW_BUCKET_POLICY=1`, and only when the bucket has no policy).

## What this settles

| Q | Question | Tied to | Step |
|---|---|---|---|
| NX-1 | Is `If-None-Match: *` enforced, and **atomically**, including across gateways? | risk 1; AMBIGUITY S1, S2, S10; §1.4 `ATOMIC_COND` ("unknown for Nutanix") | §2 `run1`, `race` |
| NX-2 | Is `If-Match` CAS enforced atomically (no lost update)? | risk 1; AMBIGUITY S3, S4, S11 | §2 `race` |
| NX-3 | Can an ambiguous (cancelled) create or CAS be resolved by an identical retry and a read-back? | AMBIGUITY S1, S3 (the timeout rule) | §2 `race` |
| NX-4 | Read-after-write on GET and HEAD; HEAD of a missing key 404 (not 403) | AMBIGUITY S5 ("Nutanix unverified") | §2 `list` |
| NX-5 | LIST after write, `StartAfter` order, list-race under concurrency: any acked key missing? | AMBIGUITY S6 ("Nutanix Objects: unverified") | §2 `list` |
| NX-6 | User metadata (`x-amz-meta-oscope-*`) round-trips on HEAD and GET | FORMAT.md §2 (the consumer HEADs for kind, content key, rows); §1.1 "x-amz-meta-*" | §2 `run1` |
| NX-7 | The SDKs' default CRC32 checksum (header, and the aws-chunked trailer over https): accepted, or does it need `when_required`? | §1.1 "CRC32 trailer handling"; `components/nutanix` sets `when_required` | §2 `run1` (`checksum`) |
| NX-8 | Is the ETag the MD5 of the body for single-part PUTs? | AMBIGUITY X2 (the entity lane resolves a 412 by ETag = MD5) | §2 `etag` |
| NX-9 | Latency and throughput at the edge's object sizes; where does it throttle? | risk 3's latency, for the other target | §2 `perf`, `load` |
| NX-10 | Static keys + private CA + path-style in both edges and the consumer; does ClickHouse read keyless or via a named collection? | §1.1 Nutanix row; D18; acceptance RUNBOOK §7 | §3, §5 |
| NX-11 | Write-side ABAC without STS: can a bucket policy scope one key per cluster to its prefix, deny delete and the control prefix, and does Objects evaluate `s3:if-none-match`? | STPA R-S7; D18; `deploy/README.md` §Write-side ABAC ("on Nutanix Objects it is unverified") | §4 |
| NX-12 | The edge on a cluster at the site: exactly once through kills and an Objects outage | AMBIGUITY E1–E5 on the second target | §5 |
| NX-13 | Versioning / WORM on the bucket (a delete that keeps bytes breaks GC's storage bound) | risk 6; acceptance RUNBOOK §5 | §2 `run1` (`bucket-config`) |

## 0. What the owner provides

- **The Objects endpoint** (FQDN), **every client-facing IP** of the object
  store (Prism → Objects → the store's network settings, or the admin), and
  the **Objects version** (support may change between releases; record it).
- **The private CA** that signed the endpoint's certificate (PEM).
- **A bucket** for the validation (a new one is best: §4 wants to set a
  bucket policy, and a policy replaces the whole policy).
- **Objects users and keys:** one for the kit (Read + Write on the bucket);
  for §4, two more users standing for two clusters and one for the consumer;
  whether the Objects version supports bucket policies with prefix resources,
  and how its policy `Principal` names a user (ask the admin: the AWS form is
  `arn:aws:iam::<account>:user/<name>`).
- **For §5:** a Kubernetes cluster at the site (NKE or any) with a default
  StorageClass that can expand volumes, a registry it can pull from, and a
  Linux VM (8 vCPU, 32 GB, 200 GB) for ClickHouse and the consumer.
- **The Nutanix "Supported S3 APIs" page** for that version, if anyone has
  portal access: what it says about conditional requests goes in the results
  next to what was measured.

Time: §2 about 1.5 h (the hour of acceptance/RUNBOOK.md plus the
cross-gateway races and the load); §4 30 min; §5 half a day. No cloud cost;
the load phases write ~0.75 M small objects (deleted after).

```sh
cd otel-chdb/deploy/validation
export RUN=v1 ENDPOINT=https://objects.site.example BUCKET=<bucket> CA_BUNDLE=$PWD/nutanix-ca.pem
export AWS_ACCESS_KEY_ID=<kit key> AWS_SECRET_ACCESS_KEY=<kit secret> NUTANIX_IPS=10.0.0.11,10.0.0.12,10.0.0.13
export OBJECTS_VERSION=<x.y>
(cd ../../acceptance && ./build.sh)          # bin/s3accept (+ static linux builds to copy to a host at the site)
```

Run from a host at the site (the kit's latency numbers are only meaningful
on the production network path). `acceptance/RUNBOOK.md` §3.2 explains how to
read every Nutanix outcome; the table in §2 below is its summary.

## 1. Dry run

```sh
nutanix/accept.sh dry creds
```

`dry` prints the addressing (path-style), the CA and the credential mode;
`creds` checks the static key end to end (create, HEAD, GET, DELETE) and
that `--ca-bundle` is needed (it fails without it).

## 2. The acceptance suite (NX-1 … NX-9, NX-13)

```sh
nutanix/accept.sh run1 run2 race conflicts list perf etag
nutanix/accept.sh load        # LOAD_PLAN="cluster-first:500:300 cluster-first:2000:300"
```

| Step | Runs | Pass |
|---|---|---|
| `run1`, `run2` | the whole suite (`--store nutanix --ca-bundle`) | control-plane, inline-consumer, exporter-data ACCEPTED in both |
| `race` | `create-only`, `if-match`, `create-race` 32 × 50, `cas-race` 16 writers, `ambiguous-*` × 50, **spread over every client IP** (`--race-endpoints URL=IP`) | one winner per round across gateways; no lost CAS update; every ambiguous write resolved once |
| `conflicts` | `create-race` 64 × 200 across gateways | record 409s and any other error (only 409/412 allowed) |
| `list` | `list`, `list-race`, `read-after-write`, `head-missing`, `metadata`, 500 keys, lag cap 60 s, across gateways | 0 acked keys missing, 0 holes, LIST lag recorded; 404 for a missing key; metadata intact |
| `perf` | 200 B … 8 MiB, concurrency 1 and 8 | recorded |
| `etag` | single-part PUTs of 1 KiB, 1 MiB, 8 MiB: ETag on PUT and HEAD against `md5sum` | recorded (NX-8): 3 of 3 = MD5, or not |
| `load` | create-only PUT load in the cluster-first layout | no 503 at 500/s; record the rate where 503 (or latency) starts |

What each outcome means (acceptance RUNBOOK §3.2, condensed):

| You see | Meaning | Decision |
|---|---|---|
| `create-only` FAIL, second create 200 | the header is ignored | **Keeper-backed Coordinator** for the control plane (`model/S3NATIVE.md` §9 (b)); data can stay on Objects |
| `create-only` FAIL with 501/400 | conditional writes not implemented | the same fallback |
| `create-only` PASS, `create-race` FAIL | the condition is checked outside the write | the same fallback; report to Nutanix with the JSON |
| `create-race` PASS on one endpoint, FAIL across `--race-endpoints` | enforced per gateway | the same fallback |
| `if-match`/`cas-race` FAIL only | slots safe, leases/checkpoints not | Keeper for leases, checkpoints and `gc.json` only |
| `ambiguous-*` FAIL | a cancelled write landed wrongly or was lost | the timeout rule is unsound here: the fallback |
| `checksum` WARN (CRC32 rejected) | a common gap | keep `when_required` (components/nutanix sets it for the Go SDK; object_store sends none) |
| `list` WARN (lag) | LIST eventually consistent | still correct (the consumer never skips a gap); add the lag to the visibility budget |
| `list` FAIL (StartAfter wrong) | discovery by LIST broken | HEAD-probe consecutive slots, or the fallback |
| `read-after-write` FAIL | not strongly consistent | treat the conditional results as suspect too: the fallback |
| `metadata` FAIL | metadata lost/truncated | read the envelope from the Parquet footer (one ranged GET per object) |
| `etag` ETag ≠ MD5 | X2's resolution misreads our own write as another's | safe by idempotence (records written twice); change the entity lane to compare `oscope`-style metadata |
| `bucket-config` versioning on / WORM | GC deletes keep bytes, or fail | a lifecycle rule for noncurrent versions, or no WORM on this bucket |

## 3. ClickHouse reading Objects (NX-10)

On the VM of §5 (or any host with ClickHouse 26.10):

```sh
../../acceptance/bin/s3accept creds --url $ENDPOINT/$BUCKET/validation/$RUN/accept --store nutanix --ca-bundle $CA_BUNDLE --modes static --leave-sample
CH_URL=http://127.0.0.1:8123 CH_USER=default ../../acceptance/credcheck/clickhouse-check.sh <printed URL>
```

ClickHouse ignores `AWS_CA_BUNDLE`: start it with `SSL_CERT_FILE` pointing
at a bundle of the system roots **plus** the Nutanix CA (it replaces the
defaults), or `<openSSL><client><caConfig>`. The consumer passes its keys in
`s3()`, so the keyless path matters only for ad-hoc reads: record whether
code 497 applies and whether a named collection works.

## 4. Write-side ABAC without STS (NX-11)

```sh
export C1_KEY=... C1_SECRET=... C2_KEY=... C2_SECRET=... CONS_KEY=... CONS_SECRET=...
nutanix/abac.sh                                       # 1. the bucket's existing access model, no policy change
P1=<principal of user 1> P2=<user 2> PCONS=<consumer user> ALLOW_BUCKET_POLICY=1 nutanix/abac.sh   # 2. with the policy
```

The policy (`nutanix/bucket-policy.tmpl.json`) is the AWS design's edge
policy expressed per user: each cluster's user may create and read only
under `validation/$RUN/edge/nx-cN/`, may list, may never delete or write
`_*` (control) keys, and a plain (non-create-only) PUT of a slot is denied
if Objects evaluates `s3:if-none-match`. The matrix is the same
`eks/abac_matrix.sh` as on AWS, once per key.

| Run | Expect | Record |
|---|---|---|
| 1, no policy (bucket shares) | FAIL rows: with plain shares every user can write every prefix | that the default model gives no cluster boundary |
| 2, with the policy | 0 FAIL rows; `free-slot-head=404` | whether `put-bucket-policy` was accepted at all; which statements Objects enforced (prefix resources, Deny, the `s3:if-none-match` condition) |

**If Objects cannot express it** (policy refused, or prefix resources not
enforced): the fallbacks are a bucket per cluster (the consumer reads one
root today: it would need a root per cluster) or accepting keys that can
write any cluster's prefix (R-S7 stays open for Nutanix, with the edge keys
kept to the cluster's own namespace Secret). Record which, for the owner.

Key rotation has no refresh path on Objects: rotating a key is a Secret
update and a rolling restart of the publishers. Time it in §5 (E6-style
rollout) and record whether any commit was left `unresolved`.

## 5. The edge at the site (NX-12)

Images: build and push to the site's registry
(`REGISTRY=registry.site.example/otel eks/images.sh`, after `docker login`).
Central: on the VM, ClickHouse 26.10 (with `SSL_CERT_FILE` as in §3) and

```sh
AWS_CA_BUNDLE=$CA_BUNDLE consume --s3 $ENDPOINT/$BUCKET/validation/$RUN/edge --key $CONS_KEY --secret $CONS_SECRET \
  --ch http://127.0.0.1:8123 --db nx_edge --metrics-addr 0.0.0.0:9464 &
AWS_CA_BUNDLE=$CA_BUNDLE consume gc --s3 $ENDPOINT/$BUCKET/validation/$RUN/edge --key $CONS_KEY --secret $CONS_SECRET \
  --ch http://127.0.0.1:8123 --db nx_edge --every 60s --metrics-addr 0.0.0.0:9465 &
```

The edge (the rust-nutanix overlay with the run's endpoint, bucket, CA and
the kit key as the publishers' Secret):

```sh
export KUBECONFIG=<site cluster> NX_ENDPOINT=$ENDPOINT NX_KEY=$C1_KEY NX_SECRET=$C1_SECRET CLUSTER=nx-c1
TARGET=nutanix eks/deploy.sh edge
kubectl -n otel-edge logs otap-publisher-0 | grep -i birth                      # 7 of 7
```

(`CLUSTER=nx-c1` with user 1's key keeps §4's policy consistent. The toolbox
image must be pullable at the site: `send` uses it.) Then, with
`CH_URL=http://<vm>:8123` so `check` queries the VM, the fault rows of
eks-aws.md §8 that do not depend on AWS:

```sh
F="env CH_URL=http://<vm>:8123 eks/fleet_test.sh"
# E1 steady; E3 SIGKILL two publishers; E4 agent rollout; E6 publisher rollout; E7 scale 3->4->3; E9 full volume + PVC expansion
```

and an **Objects outage** instead of E10's IAM deny: have the admin disable
the kit user's access (or block the client IPs at the site firewall) for 10
minutes while a dataset is in flight, then restore. **Pass:** every dataset
320,000 / 0 / 0; after the outage `resolved_own`/`resent` absorb the lost
answers, no `inconsistent` outcome (that would be a read-after-write
failure), the drain time recorded.

## 6. Rows to update with the results

Fill `results/TEMPLATE.md` (section NX) first; then:

- [ ] **DECISIONS §1.1** Nutanix row, "What is unknown": each item answered (If-None-Match atomicity, HEAD/LIST consistency, `x-amz-meta-*`, CRC32 trailer), with the Objects version.
- [ ] **DECISIONS §1.4** `ATOMIC_COND`: "It is unknown for Nutanix" → the measured result.
- [ ] **DECISIONS §4 risk 1**: retired (ACCEPTED across gateways) or the fallback chosen (Keeper Coordinator for all or part of the control plane); re-rank.
- [ ] **DECISIONS D18**: the Nutanix credential model as measured (static keys, CA, rotation), and the ABAC result (NX-11).
- [ ] **AMBIGUITY S1, S3, S5, S6**: "Nutanix unverified" → *handled* (or the finding); S6's evidence gets the `list-race` result.
- [ ] **AMBIGUITY X2**: ETag = MD5 on Objects (NX-8).
- [ ] **AMBIGUITY "Open and unverified rows" item 5** (S6/S1/S3 on Nutanix): closed or restated.
- [ ] **deploy/README.md** §Nutanix Objects and §Write-side ABAC ("on Nutanix Objects it is unverified"); §Validation: a Nutanix row.
- [ ] **components/nutanix**: keep or drop `when_required` (NX-7).
- [ ] **acceptance/RUNBOOK.md §9**: a Nutanix results subsection.
- [ ] **STPA R-S7** for Nutanix: hand to the STPA owner (not edited from this runbook).
