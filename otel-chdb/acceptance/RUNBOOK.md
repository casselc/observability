# Acceptance runbook: the first hour on a real store

The first session on real infrastructure has one job: find out whether the
store and the cluster hold the assumptions the design makes. The targets are
AWS S3 from EKS, and Nutanix Objects. This kit should make that session take
about an hour and end in a decision, not a to-do list.

| Tool | What it answers |
|---|---|
| `s3accept` (Go, one static binary) | Does the store do what the design needs: atomic create-only and If-Match writes, ambiguous outcomes that can be resolved, metadata on HEAD, consistent reads and LIST, 404 for free slots, the SDKs' default checksums? And what latency does it give at our object sizes? |
| `s3accept creds` | Which credential mode is live? Does each configured mode work end to end, and does refresh work? |
| `credcheck/clickhouse-central.sql` | Can the central ClickHouse read the edge's objects keyless with its own credentials, and does the 26.10 restriction apply? |
| `rates/` | Real spans, logs and series per pod and per node, and the scrape interval, turned into the sizing calculator's inputs. |

Everything here was run locally against SeaweedFS 4.47, stand-ins for each
credential mode, ClickHouse 26.10, and a fake Prometheus and kubectl. The
results are at the end of this file and in `results/`. None of it has touched
AWS or Nutanix yet.

---

## 1. What the design assumes, and which check tests it

A **required** check that fails rejects a capability. A **recommended** check
that fails costs a workaround. An **info** check is measured and never gates.

| Assumption | Check | Used by | Level |
|---|---|---|---|
| `If-None-Match: *` is enforced: 412 for an existing key, including an identical retry | `create-only` | log slots, fence, tombstones (`model/S3NATIVE.md` §3; `otap-rs` slots) | required |
| …and enforced **atomically**: of N concurrent creates, exactly one wins | `create-race` (16 writers × 20 rounds) | the same; a racy check-then-act store passes `create-only` and fails here | required |
| `If-Match` CAS is enforced: current 200, stale 412, missing key 404 (412 tolerated) | `if-match` | leases, checkpoints, `gc.json` | required |
| …atomically: no lost updates under contention | `cas-race` (8 writers × 25 increments) | the same | required |
| A timed-out create or CAS can be resolved by an identical retry and a read-back | `ambiguous-create`, `ambiguous-cas` (the request is cancelled the moment it has been written) | the writer's timeout rule; the consumer's CAS resolution | required |
| User metadata survives on HEAD and GET, and a losing create doesn't change it | `metadata` | the consumer HEADs slots for kind, content key and rows | required |
| Strong read-after-write on GET and HEAD (create, overwrite, delete) | `read-after-write` | read-after-412, CAS resolution | required |
| LIST sees new keys; `StartAfter` works in key order, across pages, with the `{epoch}/0` form; the delimiter lists epochs | `list` | consumer discovery | required (lag only WARNs: the consumer never skips a gap) |
| HEAD or GET of a missing key answers **404**, not 403 | `head-missing` | free-slot detection, timeout resolution | required (a 403 means IAM, not the store) |
| Plain PUT is allowed (no bucket policy forcing conditional writes) | `plain-put` | chDB `s3()`, ClickHouse writes, heartbeats | recommended |
| The SDKs' default CRC32 checksum (a trailer over https), `DeleteObjects` | `checksum` | every Go and Rust client | recommended |
| Conditional DELETE | `cond-delete`, `cas-vs-delete` | **not needed**: GC deletes unconditionally below a CAS'd horizon | recommended / info |
| Conditional multipart completion | `mpu-race` | **not used**; expected to FAIL on SeaweedFS | info |
| Versioning, lifecycle, bucket-policy condition keys | `bucket-config` | GC and storage reclaim | info |
| Store clock within SigV4's 15 minutes; within the lease margin | `clock` | SigV4; the consumer's server-side insert fence | info |
| Latency and throughput of PUT, GET, HEAD and LIST at 100 KB and 1 MB, 200-byte slot PUTs, CAS, HEAD-404 | `perf` | exporter timeout and queue depth; consumer visibility budget | info |

The report ends with a verdict per capability:

- **control-plane:** the S3-native / manifest-less commit;
- **inline-consumer:** the otap-rs consumer as built;
- **exporter-data:** plain data publishing.

Each verdict is ACCEPTED, REJECTED (with the fallback to use) or UNPROVEN (a
needed check was skipped or not run). The exit code is 1 if control-plane is
REJECTED, 2 on a setup error, and 0 otherwise.

---

## 2. Before the session (about 10 minutes, the day before)

**Build** (on any machine with Go 1.24+):

```sh
cd spike/otel-chdb/acceptance && ./build.sh
# bin/s3accept (this machine), bin/s3accept-linux-{amd64,arm64} (static), bin/credstubs
```

**Get:**

- **AWS:**
  - the bucket and region;
  - the collector's namespace and ServiceAccount, with its IRSA annotation or
    Pod Identity association;
  - a second role for the central ClickHouse, if it differs;
  - read access to Prometheus (a `port-forward` is enough);
  - `kubectl` rights for `pods/log`, `nodes/proxy` and `pods/proxy` for the
    fallback sampler.
- **Nutanix:**
  - the Objects endpoint URL (FQDN) and **every client-facing IP** of the
    object store: Prism, Objects, the store's network settings, or the admin;
  - an access key and secret for a user granted Read and Write on the bucket;
  - the private CA PEM;
  - the Objects **version**. Nutanix's API pages are behind the support
    portal. If someone has access, ask for the "Supported S3 APIs" page for
    that version and note what it says about conditional requests.
- **IAM** (AWS): attach `iam/s3-policy.json` with `BUCKET` replaced. See
  section 5.

**Place a pod that runs as the collector.** Credentials are injected per
ServiceAccount, so the probe must run under the same one. It must also run on
the same node pool, so the network path and the IMDS hop limit are the
production ones.

```sh
NS=otel SA=otel-collector
kubectl -n $NS run s3accept --image=alpine:3.20 --restart=Never \
  --overrides="{\"spec\":{\"serviceAccountName\":\"$SA\"}}" --command -- sleep 7200
kubectl -n $NS wait --for=condition=Ready pod/s3accept
kubectl -n $NS cp bin/s3accept-linux-amd64 s3accept:/tmp/s3accept        # arm64 nodes: the arm64 build
kubectl -n $NS exec s3accept -- sh -c 'env | grep -E "^AWS_" | sed "s/=.*SECRET.*/=***/"'
```

For Nutanix, also copy the CA:
`kubectl cp nutanix-ca.pem $NS/s3accept:/tmp/ca.pem`. Alpine is used because
it has CA roots and `tar` (which `kubectl cp` needs). Distroless has neither.

---

## 3. The hour

| Time | Step | Command | Record |
|---|---|---|---|
| 0:00 | Dry run: confirm the target, addressing, CA and credential environment. Nothing is sent. | `/tmp/s3accept --url s3://BUCKET/otel-accept --region R --dry-run` | the credential env it lists |
| 0:03 | Credentials: every configured mode, with forced refresh; start a hold across a real refresh in the background | `/tmp/s3accept creds --url … -v` then `nohup /tmp/s3accept creds --url … --modes chain --hold 65m --every 60s --out /tmp/hold.json > /tmp/hold.txt 2>&1 &` | table + `s3accept-creds-*.json` |
| 0:08 | Full acceptance run | `/tmp/s3accept --url … -v --out /tmp/run1.json \| tee /tmp/run1.txt` (about 2–5 min on AWS) | verdicts, every FAIL/WARN, perf table |
| 0:15 | Second run, for variance (Nutanix: across gateways, see 3.2) | `… --out /tmp/run2.json` | differences from run 1 |
| 0:22 | Stress the conditional writes harder | `… --only create-race,cas-race --race-writers 64 --race-rounds 50 --cas-writers 32 --out /tmp/race.json` | winners histogram, 409s, errors |
| 0:28 | Central ClickHouse keyless read | `/tmp/s3accept creds --url … --modes chain --leave-sample` then `CH_URL=… CH_USER=ingest CH_PASSWORD=… credcheck/clickhouse-check.sh <printed URL>` | 497 or not; which fix |
| 0:38 | Rates from Prometheus | `rates/collect.py --prom http://127.0.0.1:9090 --out rates.json` (with a `port-forward`), then `rates/to_calculator.py rates.json` | the table, notes, and the console snippet |
| 0:48 | Fallback sampling, if there are no shipper metrics | `rates/kubectl_sample.py --pods 30 --nodes 3` then `to_calculator.py rates.json --sample sample.json` | |
| 0:53 | Collect and clean up | `kubectl cp $NS/s3accept:/tmp/ ./results-$(date +%F)/`, `/tmp/s3accept cleanup --url …`, `kubectl delete pod s3accept` (after the hold finishes, or accept an unfinished hold) | |

Copy every JSON file off the pod. The JSON is the evidence. The tables are
for reading in the session.

### 3.1 AWS (EKS)

```sh
# IRSA or Pod Identity: no keys; the pod's env carries everything
/tmp/s3accept --url s3://BUCKET/otel-accept --region eu-west-1 -v
# IAM Roles Anywhere from outside AWS: a profile with credential_process ...
AWS_CONFIG_FILE=/etc/ra/config /tmp/s3accept --url s3://BUCKET/otel-accept --profile otel -v
# ... or aws_signing_helper serve beside it (what chDB and ClickHouse need)
AWS_EC2_METADATA_SERVICE_ENDPOINT=http://127.0.0.1:9911 /tmp/s3accept --url s3://BUCKET/otel-accept --region R -v
# a role on top, as parquetgo Config.RoleARN / otap-rs role_arn
/tmp/s3accept --url s3://BUCKET/otel-accept --region R --role-arn arn:aws:iam::ACCOUNT:role/otel-writer
```

Things to watch on AWS:

- `identity` should name IRSA or Pod Identity and the expected role ARN. If
  it says **IMDS (the node's instance role)**, the ServiceAccount isn't wired,
  and pods can reach IMDS. Fix that before trusting any IAM result.
- `create-race` may show some **409 ConditionalRequestConflict**. That's
  allowed: AWS documents it, and the writer resolves it like a timeout. Other
  errors in a race are not allowed.
- On an SSE-KMS bucket the ETag isn't the MD5 (`create-only` WARNs). The
  design never relies on the MD5. Check that the role has
  `kms:GenerateDataKey` and `kms:Decrypt`.
- **perf on a new prefix:** S3 partitions a new prefix under load. If run 1
  shows 503 SlowDown, run 2 usually doesn't.
- If STS is refused in a VPC without an STS endpoint, `identity` WARNs. IRSA
  itself then fails too, and the pod needs an STS VPC endpoint or egress.

### 3.2 Nutanix Objects

Nutanix's support for conditional writes is the biggest unknown. Here the
race checks matter more than anything else.

```sh
/tmp/s3accept --url https://objects.example.com/BUCKET/otel-accept --store nutanix \
  --ca-bundle /tmp/ca.pem --access-key-id "$NUTANIX_ACCESS_KEY" --secret-access-key "$NUTANIX_SECRET_KEY" -v
# the same, with writers spread over every gateway: URL=IP keeps the FQDN for TLS and signing
/tmp/s3accept --url https://objects.example.com/BUCKET/otel-accept --store nutanix --ca-bundle /tmp/ca.pem \
  --race-endpoints "https://objects.example.com=10.0.0.11,https://objects.example.com=10.0.0.12,https://objects.example.com=10.0.0.13" \
  --only create-race,cas-race,create-only,if-match --race-writers 32 --race-rounds 50 -v
```

Why `--race-endpoints`: Go dials the first address that answers, so with
round-robin DNS every writer can land on the same gateway. A store that
decides the condition per gateway, rather than in its metadata service, would
pass a single-gateway race and fail a cross-gateway one.

How to read the Nutanix outcomes:

| What you see | What it means | Decision |
|---|---|---|
| `create-only` FAIL "If-None-Match:* was ignored" (the second create returned 200) | The header is accepted and ignored, as with old MinIO and Garage. | **Keeper-backed Coordinator** for the control plane (`model/S3NATIVE.md` §9 (b)). Data stays on Nutanix. |
| `create-only` FAIL with 501 or 400 | Conditional writes aren't implemented. | The same fallback. |
| `create-only` PASS but `create-race` FAIL | The condition is checked outside the write: the worst case, because it looks supported. | The same fallback. Report it to Nutanix with the JSON. |
| `create-race` PASS on one endpoint, FAIL with `--race-endpoints` | Enforced per gateway, not globally. | The same fallback. |
| `if-match` or `cas-race` FAIL, create-only fine | Log slots are safe; leases and checkpoints aren't. | Keeper for leases, checkpoint and `gc.json` only. |
| `ambiguous-*` FAIL | A request the client gave up on landed wrongly or was lost. | The fallback. The timeout rule is unsound here. |
| `checksum` WARN "the SDK's default CRC32 is rejected" | A common gap on S3-compatible stores: newer SDKs send CRC32 by default, as an aws-chunked trailer over https. | Set `AWS_REQUEST_CHECKSUM_CALCULATION=when_required` and `AWS_RESPONSE_CHECKSUM_VALIDATION=when_required` for every Go and Rust client (parquetgo, otap-rs edge and consumer). ClickHouse is unaffected. |
| `list` WARN (LIST lag) | LIST is eventually consistent. | The consumer is still correct: it never skips a gap. Add the worst observed lag to the visibility budget. |
| `list` FAIL (wrong StartAfter results) | Discovery by LIST is broken. | HEAD-probe consecutive slots (no LIST), or use the fallback. |
| `read-after-write` FAIL | Not strongly consistent. | Treat the conditional results as suspect too, and use the fallback. |
| `metadata` FAIL | Metadata isn't returned or is truncated. | Read the envelope from the Parquet footer instead: one ranged GET per object. |
| `head-missing` 403 | Unusual on Nutanix (the user usually owns the bucket). | Check the bucket share: Read as well as Write. |

Record the Objects version with the result: support may change between
releases.

---

## 4. What to record

Save all JSON files. Then fill this in, in whatever tracks the decision:

| Field | AWS | Nutanix |
|---|---|---|
| date, operator | | |
| store and version (Objects version / AWS region) | | |
| endpoint, addressing (path / virtual-hosted), TLS CA | | |
| credential mode per `identity` / `creds`, role ARN | | |
| refresh: forced refresh OK? hold crossed a refresh with 0 errors? | | |
| create-only / create-race / if-match / cas-race / ambiguous-* | | |
| 409s seen in races (count) | | |
| metadata / read-after-write / list (lag) / head-missing | | |
| checksum: default OK? trailer used? `when_required` needed? | | |
| mpu-race (information only) | | |
| versioning, lifecycle rules, bucket-policy condition keys | | |
| perf p50/p99: PUT 100 KB, PUT 1 MB, slot PUT, CAS, HEAD-404, LIST | | |
| verdicts: control-plane / inline-consumer / exporter-data | | |
| ClickHouse: 497 on keyless? fix used (profile / role_arn / named collection) | | |
| calculator inputs from `to_calculator.py` (basis) | | |

---

## 5. IAM and access

**AWS: the role the collector and the probe run as.** `iam/s3-policy.json`
grants:

- `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` and
  `s3:AbortMultipartUpload` on `BUCKET/edge/*` and `BUCKET/otel-accept/*`;
- `s3:ListBucket` and `s3:ListBucketMultipartUploads` on the bucket;
- optionally, for the probe's `bucket-config` check only,
  `s3:GetBucketVersioning`, `s3:GetLifecycleConfiguration` and
  `s3:GetBucketPolicy` (a 403 there is reported, not failed).

Notes:

- **Conditional writes need no extra action.** `If-None-Match` needs only
  `s3:PutObject`. `If-Match` also needs `s3:GetObject` (AWS documents this),
  which the policy has.
- **`s3:ListBucket` decides 404 against 403** for a missing key. Without it,
  a free slot answers 403, and the writer and the consumer can't tell "free"
  from "denied". Grant it on the bucket without conditions. If you scope it
  with `s3:prefix`, check that `head-missing` still passes: a HEAD carries no
  `s3:prefix`.
- `s3:DeleteObject` is for GC and this probe's cleanup. An edge that never
  runs GC can do without it.
- For SSE-KMS, add `kms:GenerateDataKey` and `kms:Decrypt` on the key.
- **Trust policies:**
  - IRSA: `iam/trust-irsa.example.json`, with the OIDC provider and
    `system:serviceaccount:NS:SA` as `sub`;
  - Pod Identity: `iam/trust-pod-identity.example.json` (principal
    `pods.eks.amazonaws.com`, `sts:AssumeRole` and `sts:TagSession`), plus
    `aws eks create-pod-identity-association`.
- **Central ClickHouse:** `s3:GetObject` and `s3:ListBucket` for reads, plus
  the ingest profile setting in section 7.
- **Bucket policies that enforce conditional writes.** AWS has had condition
  keys for this since 2024-11: `s3:if-none-match`, `s3:if-match`,
  `s3:ObjectCreationOperation`. `iam/bucket-policy-require-conditional.example.json`
  denies non-create-only PUTs to log slots and non-CAS PUTs to checkpoints and
  leases. That is defence in depth against a buggy writer. **Scope it to
  control prefixes only.** chDB `s3()`, ClickHouse `INSERT INTO FUNCTION
  s3()`, `s3_plain_rewritable` disks, worker heartbeats and CopyObject send no
  conditional headers, and would get 403. A CopyObject into a covered prefix
  fails with 403, or 501 with the header. With the otap-rs inline layout,
  slots live at `{root}/{producer}/{signal}/{epoch}/*.parquet` and control
  objects under `{root}/_consumer/`. If such a policy already covers the
  bucket, `plain-put` FAILs and `bucket-config` WARNs.

**Nutanix Objects.** Access is per bucket, granted to an Objects user's
access key: share the bucket with that user, with Read and Write. What
isn't known:

- whether Objects' bucket policies support prefix conditions or the
  conditional-write condition keys (`bucket-config` shows what
  `GetBucketPolicy` returns);
- whether versioning or WORM is on (`bucket-config` shows versioning). WORM
  would make GC's deletes fail.

The private CA:

- Go clients: `--ca-bundle` / `AWS_CA_BUNDLE` (appended to the system
  roots);
- chDB and ClickHouse: `SSL_CERT_FILE` (it *replaces* the defaults, so
  bundle the system roots with it) or `<openSSL><client><caConfig>`.
  `AWS_CA_BUNDLE` is ignored by ClickHouse.

---

## 6. `s3accept creds`: interpreting the credential modes

`creds` runs each configured mode on its own. It hides the other modes'
environment and the shared files, so the SDK's default chain can only land
on that mode. For each mode it:

1. resolves credentials;
2. calls STS GetCallerIdentity where there is an STS;
3. does a create-only PUT, then HEAD, GET and DELETE;
4. forces a refresh (`Invalidate` + `Retrieve`, which goes back to STS, the
   Pod Identity agent, IMDS or the helper process);
5. HEADs again with the new credentials.

The `chain` row is what the exporters get.

| Row | PASS means | FAIL: first thing to check |
|---|---|---|
| `irsa` | web identity → STS → S3 works, and refresh re-runs AssumeRoleWithWebIdentity | the token file is mounted; the trust policy's `sub`/`aud`; STS reachable |
| `pod-identity` | agent → S3 works; the token file is re-read per fetch | the agent DaemonSet runs on the node; the association exists; the token file has **no trailing newline** (found here: aws-sdk-go-v2 rejects an Authorization token containing one) |
| `imds` | IMDS or `aws_signing_helper serve` answers | for serve: the port and `AWS_EC2_METADATA_SERVICE_ENDPOINT`. A pod reaching the *node's* IMDS is itself a finding (hop limit) |
| `profile` | the profile resolves (`credential_process` ran) | run the helper by hand with the same arguments |
| `static` | keys work | Nutanix: the key's bucket share |
| `chain` WARN "several modes configured" | the chain picked one and shadows the others | aws-sdk-go-v2's order: a profile passed in code, env keys, IRSA env, the shared profile (AWS_PROFILE or `[default]`), container (Pod Identity), IMDS. A leftover `AWS_ACCESS_KEY_ID` shadows IRSA and Pod Identity; a mounted `~/.aws` with a `[default]` shadows Pod Identity. |
| `hold` | N requests across the hold, 0 errors, ≥ 1 refresh crossed | an error near the expiry time means refresh fails under load. INFO "no refresh happened" means the hold was shorter than the credential lifetime minus the SDK's window. IRSA's default session is 1 h; Pod Identity's credentials were refetched on every request here once they were within 5 minutes of expiry (the SDK's window for the container provider). |

---

## 7. Central ClickHouse: keyless `s3()`

Run `credcheck/clickhouse-check.sh` as the **ingest** user. The restriction is
per user profile.

- **(2) Code 497** `S3 access from user queries is not allowed to use the
  server's own credentials` means the 26.10 default,
  `s3_allow_server_credentials_in_user_queries = 0`. **(3)** with the setting
  lifted per query tells you whether the server's own credentials work.
  Fixes, least privilege first:
  - put `<s3_allow_server_credentials_in_user_queries>1</…>` in the ingest
    user's profile only (keep it `0` and `readonly` for everyone else); or
  - use `extra_credentials(role_arn = '…')` in `s3()`, which the restriction
    allows; or
  - on Nutanix, a named collection with the keys, which is never
    restricted.
- **(2) or (3) AccessDenied** means the server's role lacks `s3:GetObject`.
- **(4) glob fails but (3) works** means `s3:ListBucket` is missing. The
  consumer's `s3('…/{k1,k2}')` does no LIST, but GC tooling and ad-hoc reads
  do.
- Which source the server used is in the server log (section 7 of the SQL
  file). ClickHouse doesn't run `credential_process`: for Roles Anywhere, use
  `aws_signing_helper serve` and `AWS_EC2_METADATA_SERVICE_ENDPOINT`. Its
  IRSA STS endpoint is hard-coded to `sts.<region>.amazonaws.com` (region
  from `AWS_DEFAULT_REGION`).

---

## 8. Rates: feeding the sizing calculator

`rates/queries.promql` is the single source of the PromQL. Each block has
alternatives for Fluent Bit, Vector, Promtail, OTel filelog and Loki (logs),
Tempo, Jaeger and OTel collectors (spans), and scrape-sample accounting for
series.

```sh
kubectl -n monitoring port-forward svc/prometheus-operated 9090 &
rates/collect.py --prom http://127.0.0.1:9090 [--selector 'cluster="prod-1"'] [--heavy] --out rates.json
rates/to_calculator.py rates.json            # --basis avg (default) | peak | now; --spans-source tempo|jaeger|otelcol
```

`to_calculator.py` prints:

- each input with how it was derived;
- notes on double counting and bytes per line or span;
- the JSON;
- a one-line snippet to paste into the browser console on the calculator
  page. It sets the fields in the page's own saved state (`localStorage`
  `central-sizing-v9`) and reloads.

How the inputs are derived:

- **series per pod** = (all scraped series − node-exporter − kubelet −
  kube-state-metrics − control plane), i.e. the app series, plus cAdvisor,
  divided by pods;
- **series per node** = node-exporter + kubelet + kube-state-metrics +
  control plane, divided by nodes. This matches the calculator's labels.
  `--heavy` adds a direct `count by (namespace, pod)` median and p90 as a
  cross-check (it touches every series).
- **logs per pod / node** = container-log inputs ÷ pods, and journal inputs ÷
  nodes, from the first shipper found.
- **spans per pod** = the backend's received rate ÷ pods. Collector receiver
  counts include every tier (agent → gateway), so the script warns you when
  it had to fall back to them. `spans_otelcol_by_job` shows the tiers.
- **interval** = the median scrape interval.
- **avg** is over `--range` (1d). **peak** is the maximum over 1d of a 5m
  rate, per *current* pod. The calculator already applies its own 1.75×
  headroom, so feed it avg unless the daily peak is far above it.

Pitfalls:

- An HA Prometheus pair doubles `prometheus_tsdb_head_series`, so query one
  replica.
- A Thanos or Mimir instance spanning clusters needs `--selector` per
  cluster, or it reports `clusters` from the `cluster` label.
- Rates are after sampling only if measured after the sampler: the backend
  is, an agent's receiver is not.

**Attribute counts and sizes (cheap proxies):**

- `to_calculator.py` prints Tempo's wire bytes per span and the shipper's
  bytes per log line when those metrics exist.
- If an existing ClickHouse holds contrib-exporter tables:

  ```sql
  SELECT avg(length(SpanAttributes)), avg(length(ResourceAttributes)),
         avg(byteSize(SpanAttributes)), avg(byteSize(ResourceAttributes))
  FROM otel_traces WHERE Timestamp > now() - INTERVAL 1 HOUR
  ```

- The calculator's stored bytes per span and per log are *after*
  compression. Change them only with a central measurement.

**No shipper metrics?** Run `rates/kubectl_sample.py`. It samples:

- `kubectl logs --since=10m` line and byte counts on 30 random pods;
- cAdvisor and kubelet series on 3 nodes, through `nodes/proxy`;
- app series of `prometheus.io/scrape` pods, through `pods/proxy`.

Pass its output with `--sample`. Node journal rates need a privileged debug
pod: the command is in the script's help.

---

## 9. Local verification (what was run here, 2026-09-26)

Everything below is in `results/`.

**`s3accept` against SeaweedFS 4.47** (`s3accept-seaweedfs.txt` and `.json`,
http, `--perf-ops 50`). All three capabilities were ACCEPTED, and:

- **required checks:** all PASS. `if-match` WARNs because a PUT `If-Match` on
  a missing key answers 412 where AWS documents 404; the design treats both
  as "not applied".
- **races:**
  - create race: 20 rounds × 16 writers, exactly one winner every round;
  - CAS race: 8 × 25, final 200, none lost, about 1,000 refused.
- **ambiguous writes:** 20 of 20 cancelled creates and 20 of 20 cancelled
  CASes had landed, and each was resolved exactly once. All 20 creates looked
  **free on a HEAD right after the cancel and landed afterwards**, which is
  the late-landing hazard the design's resend rule covers.
- **`mpu-race`: FAIL, as expected.** Winners per round were 4, 3, 4 (want 1),
  which reproduces the known non-atomic conditional multipart completion.
- **`checksum`:** PASS. Over http the SDK sends a CRC32 header. Through the
  TLS proxy (`s3accept-seaweedfs-tls.json`) the default PUT is aws-chunked
  with a trailing CRC32 (`STREAMING-UNSIGNED-PAYLOAD-TRAILER`), and SeaweedFS
  accepts it.
- **perf (localhost, concurrency 8):**

  | op | size | op/s | p50 ms | p99 ms |
  |---|---|---|---|---|
  | PUT If-None-Match | 100 KB | 955 | 7.3 | 19.7 |
  | PUT If-None-Match | 1 MB | 280 | 25.6 | 52.9 |
  | GET | 100 KB | 2,374 | 2.7 | 6.5 |
  | GET | 1 MB | 742 | 8.8 | 22.3 |
  | HEAD | — | 4,563 | 1.2 | 3.6 |
  | LIST (StartAfter, 100 keys) | — | 1,665 | 3.2 | 5.5 |
  | slot PUT (200 B, create-only) | 200 B | 1,762 | 3.8 | 10.6 |
  | CAS (200 B, If-Match) | 200 B | 1,544 | 4.5 | 11.4 |
  | HEAD of a free slot (404) | — | 4,916 | 1.4 | 2.7 |

  These are localhost numbers. They say the tool works, not what AWS or
  Nutanix will do.

**Across endpoints** (`s3accept-seaweedfs-2endpoints.txt`): the create and
CAS races were spread over http, the TLS proxy, and the TLS proxy addressed
as `URL=IP`. Both passed.

**The detector catches broken stores** (`go-test.txt`, `s3accept/fake_test.go`).
An in-memory S3 runs in three modes:

| Mode | Results |
|---|---|
| honest | all four conditional checks PASS |
| ignores conditional headers | `create-only`, `if-match`, `create-race` and `cas-race` all FAIL (for example "80 successful CASes but the counter reads 10") |
| racy (checks, then writes without a lock) | `create-only` and `if-match` **PASS**, `create-race` and `cas-race` **FAIL** |

The racy row is exactly why the race checks exist.

**`creds` against the stand-ins** (`credcheck-local.txt`,
`credcheck/local-standins.sh`):

- static keys, IRSA (stub STS via `AWS_ENDPOINT_URL_STS`: 6
  AssumeRoleWithWebIdentity), Pod Identity (stub agent), Roles Anywhere by
  `aws_signing_helper serve` (IMDSv2 stub: token, role, credentials) and by
  `credential_process` (a stand-in helper, run 4 times): all PASS, including
  S3 and forced refresh;
- static keys over https with a private CA: PASS with `--ca-bundle`, and FAIL
  without it (`x509: certificate signed by unknown authority`);
- everything configured at once: `chain` WARNs that the env keys shadow IRSA
  and Pod Identity;
- a 40 s hold with 3-minute Pod Identity credentials crossed 4 refreshes with
  0 errors;
- **found:** a token file with a trailing newline makes aws-sdk-go-v2 refuse
  the Pod Identity token. The kubelet-projected file has none, so this only
  matters for hand-made test setups.
- The stand-ins hand out an empty session token, because SeaweedFS rejects
  tokens it didn't issue. The header path with a real token wasn't tested
  here; the SDK signs it as usual.

**ClickHouse 26.10.1** (`clickhouse-check-local.txt`): run against the
shared local server as `default`:

- `getSetting(...)` is `false`;
- the keyless `s3()` failed with **Code 497**;
- with the setting lifted per query, the read returned 3 rows (the server
  has environment keys);
- the glob returned 3 rows.

Separately, `clickhouse local` read the same sample keyless through the Pod
Identity stand-in.

**Rates** (`rates-test.txt`, `rates/test.sh`):

- `collect.py` ran all 32 blocks (72 queries) against a fake Prometheus,
  including the `--heavy` blocks and a `--selector`;
- `to_calculator.py` produced the expected inputs. 16 assertions pass: pods,
  nodes, spans, logs, series per pod and node, interval, the avg and peak
  bases, the otelcol source, the kubectl-sample path, and selector injection
  into empty and non-empty braces.
- Every PromQL line in `queries.promql`, with and without a selector, and
  wrapped in the collector's avg and peak subqueries (258 expressions), parses
  with the Prometheus v0.303.0 parser. That check was a throwaway program
  outside this kit.
- **Not tested here:** real Prometheus, Thanos or Mimir semantics (label
  names like `metrics_path` and job names vary by install, which is why
  every block has alternatives), and `kubectl` against a real cluster.

**Not tested at all here:** AWS, EKS, real STS, Nutanix Objects, 409
ConditionalRequestConflict (SeaweedFS never sends it), SSE-KMS, versioned
buckets, and bucket policies with conditional-write keys (SeaweedFS has no
such keys). This kit exists to test exactly those.

---

## 10. Files

```
acceptance/
  RUNBOOK.md                       this file
  build.sh                         builds bin/ (git-ignored): s3accept (+ linux amd64/arm64), credstubs
  s3accept/                        the Go tool (own go.mod; aws-sdk-go-v2)
    main.go                        flags, check registry, dry run, run loop
    client.go                      options, transport (CA bundle, recorder, URL=IP dialing), classification
    ops.go                         PUT/GET/HEAD/DELETE/LIST helpers
    checks_cond.go                 create-only, plain-put, if-match, cond-delete, races, ambiguity, multipart
    checks_misc.go                 identity, metadata, read-after-write, list, head-missing, checksum, bucket-config, clock, cleanup
    perf.go                        latency and throughput
    creds.go                       `creds` mode: per-mode isolation, refresh, hold
    report.go                      results, capabilities and verdicts, table, JSON
    fake_test.go                   an in-memory S3 (honest / ignoring / racy) the checks must judge correctly
  credcheck/
    local-standins.sh              every credential mode against parquetgo's credstubs + SeaweedFS
    clickhouse-central.sql         the central server's keyless s3() checks, with the 26.10 restriction notes
    clickhouse-check.sh            runs the SQL over HTTP with the sample URL substituted
  iam/
    s3-policy.json                 the role's S3 actions
    bucket-policy-require-conditional.example.json
    trust-irsa.example.json, trust-pod-identity.example.json
  rates/
    queries.promql                 the PromQL (single source; readable by hand)
    collect.py                     runs it against Prometheus/Thanos/Mimir → rates.json
    kubectl_sample.py              fallback sampling with kubectl only → sample.json
    to_calculator.py               → the calculator's inputs, JSON and a console snippet
    test.sh, testdata/             fake Prometheus + fixtures, fake kubectl
  results/                         the local runs quoted in section 9
```
