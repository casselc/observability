# Edge deployments: the decisions as Kubernetes manifests

Reference deployments of the edge publishers for the three targets in
[DECISIONS.md §1.1](../DECISIONS.md#11-deployment-targets): EKS against
AWS S3, Nutanix Objects, and IAM Roles Anywhere from on-premises to AWS S3.
Each target exists for the Rust edge (primary, D1) and the Go edge
(secondary, kept by D1). Service-affine routing (D16) is a separate
component that can be switched on. Everything here was built and checked
locally (§Validation); nothing has run on a real cluster, EKS, STS, Nutanix
or a real `aws_signing_helper`.

Labels as in DECISIONS.md: **[M]** measured here, **[D]** from docs or
source, **[E]** estimate.

## Layout

```
deploy/
  base/common/        namespace, node agent DaemonSet (+ Service, internalTrafficPolicy: Local),
                      NetworkPolicy, the edge-target ConfigMap (per-target settings)
  base/rust/          otap-s3pq publishers (StatefulSet + PVC) running
                      ../otap-rs/configs/edge-publisher.yaml; agent config agent-rust.yaml
  base/go/            otelcol-awss3 publishers (StatefulSet + PVC, awss3inline); publisher-config.yaml;
                      agent config agent-go.yaml (traces and logs only)
  components/         eks-irsa, eks-pod-identity, nutanix, roles-anywhere: one per target;
                      routing: the D16 gateway tier (Rust edge)
  overlays/           {rust,go}-{eks-irsa,eks-pod-identity,nutanix,roles-anywhere}, rust-eks-irsa-routing
  collector/          ocb build of the agent/gateway collector; patches/0001 for the gateway
  images/             Dockerfiles: otap-s3pq, any ocb collector, aws_signing_helper
  scripts/            the local tests below
  results/            their outputs
```

Build an overlay with the load restrictor off, because base/rust reads the
publisher configuration from `../otap-rs/configs/` rather than keeping a copy
that could drift:

```sh
kustomize build --load-restrictor LoadRestrictionsNone overlays/rust-eks-irsa | kubectl apply -f -
```

Every value an operator sets is marked `CHANGE-ME` in the overlay's
`kustomization.yaml`: the cluster name (producer ids are
`CLUSTER-POD_NAME`, so it must be unique in the fleet), the bucket, the
region, and the target's credentials wiring. Put overlay-specific values in
the overlay, never in a component: a component's ConfigMap generator merges
after the overlay's, so a value set in a component cannot be overridden.

## Topology

```
 app pods ──OTLP──► otel-agent (DaemonSet, one per node)
                     memory_limiter → batch (≤10k items) → persistent queue (hostPath)
                     retry forever
                        │ OTLP/gRPC, round robin over the headless Service
                        ▼
                    otap-publisher-N (StatefulSet, 3 per cluster; 8 routed)  ┌─ with components/routing:
                     receiver → batch (3 MiB / 1 s) → durable buffer (PVC)  │  agents → otel-gateway (Deployment)
                     → exporter:s3pq ── 1 create-only PUT per object ──► S3  │  load_balancing, routing_key: service
                                                                             │  → the publisher that owns the service
```

The agents are the custodians of anything not yet acknowledged: their queue
is on the node's disk and they never give up. A publisher acknowledges after
its WAL write, and its buffer holds the data until the create-only commit.
Central (the consumer) is unchanged and is not deployed here.

## What each decision became

| Decision | Where | Setting |
|---|---|---|
| D1 edge publisher | base/rust (primary), base/go (secondary) | the Go edge is `awss3inline` (manifest-less, traces and logs); its agents drop metrics, which have no Go lane yet |
| D3 commit protocol | both publishers | one create-only PUT per object at `{bucket}/edge/{CLUSTER-pod}/{signal}/{epoch}/{seq}`; the IAM policy grants `s3:ListBucket` so a free slot is 404 |
| D4 / risk #10: batch before any queue, never after | agent-rust.yaml, agent-go.yaml, edge-publisher.yaml | agents: `batch` processor, then `sending_queue` on `file_storage`, no `sending_queue.batch`; Rust publisher: `processor:batch` in front of the WAL, nothing between the WAL and the exporter; Go publisher: no batching (its queue is right behind the receiver) |
| D4: retries never give up | agents, Go publisher, Rust buffer | `retry_on_failure.max_elapsed_time: 0`; Quiver retries NACKs with backoff 1–30 s and no deadline (`max_age` unset) |
| D7 layout B | edge-publisher.yaml | `metrics_layout: series_table` (per-type points lanes + the series lane) |
| D16 sorting off | all otap-rs configs | `parquet.sort.by: none` |
| D16 routing | components/routing | gateway with `routing_key: service`, 8 publishers, ring Service with `publishNotReadyAddresses` (§Routing) |
| D18 credentials | components/* | no keys in any file; the default chain in both clients (§Targets) |
| D19 durable buffer | base/rust | on, 40 GiB cap on a 50 Gi PVC, `size_cap_policy: backpressure` (§Durable buffer) |
| risk #11: 10k-row objects | agents; edge-publisher.yaml | `send_batch_max_size: 10000` splits big requests deterministically; the publisher's `max_size: 8 MiB` bounds a merged batch |
| risk #14 disk full | edge-durable.yaml / edge-publisher.yaml | backpressure, cap well below the volume, measured (§Durable buffer) |
| memory limits | everywhere | Rust: `policies.resources.memory_limiter` (enforce, 1 GiB / 1.5 GiB of a 2 GiB container); Go: `memory_limiter` first in every pipeline, `GOMEMLIMIT` |

### Changes to the existing edge configurations

`otap-rs/configs/` (all three are exercised by `scripts/configs_e2e.sh`):

1. **Static keys removed** from `edge.yaml`, `edge-durable.yaml` and
   `edge-otap.yaml` (they defaulted to `otel` / `otelsecret` through
   `S3_KEY` / `S3_SECRET`). A key field in the config shadows the whole
   credential chain (store.rs checks it first), so IRSA, Pod Identity and
   IMDS could never apply. Credentials now come only from the environment:
   `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` for SeaweedFS or Nutanix,
   and the chain otherwise. **The crate's scripts that start these configs
   (bench.sh, faults.sh, durable.sh, …) now need
   `export AWS_ACCESS_KEY_ID=otel AWS_SECRET_ACCESS_KEY=otelsecret`.**
2. **Memory limiter added** (none before): `policies.resources.memory_limiter`
   in enforce mode, sampling the process's RSS against explicit limits
   (`MEM_SOFT_LIMIT` / `MEM_HARD_LIMIT`, 768 MiB / 1 GiB by default, 1 / 1.5
   GiB in the StatefulSet). Above the hard limit the receivers answer 503 /
   RESOURCE_EXHAUSTED (retryable) and readiness fails. Not `source: auto`:
   it samples the cgroup's working set, which in a shared cgroup (this box)
   is everybody's memory [M: every request got 503 "memory pressure"], and
   it refuses to start without explicit limits when there is no
   `memory.max`.
3. **Sorting pinned off**: `parquet.sort.by: none` explicitly (it was the
   code default; D16 makes it a decision).
4. **Admin endpoint from the environment** (`ADMIN_HTTP`): the fixed
   127.0.0.1:8080 made two publishers on one host collide and left the
   probes (`/api/v1/readyz`, `/api/v1/livez`) unreachable in a pod.
5. **Headers** state the delivery contract, the credential chain and the
   disk-full behaviour.
6. **New `edge-publisher.yaml`**: `edge-durable.yaml` plus `processor:batch`
   in front of the buffer; the deployed Rust publisher. `edge-durable.yaml`
   keeps one object per request, which `scripts/durable.sh` relies on.

Checked and left alone: the durable buffer's policy (`backpressure`, now
measured), its retry settings, the OTLP receivers' `wait_for_result`, the
exporter's timeouts, layout B.

`otelcol/config.edge.yaml` (the chDB edge, D4's config): the `batch`
processor before the persistent queue, `max_elapsed_time: 0` and
`seal_optimize: false` were right. Two defects fixed: **no memory limiter in
any pipeline** (the build carried it, nothing used it), and **batches of
5,000–20,000 rows** where the design and the consumer's limits assume
10,000 (`send_batch_size` / `send_batch_max_size: 10000`). It passes
`otelcol-chdb validate`.

`awss3/collector/builder-config.yaml`: `memorylimiterprocessor` and
`healthcheckextension` added, for the Go publisher's config and probes.

## Targets

All three publishers read the same `edge-target` ConfigMap (`envFrom`), and
build `S3_URL=$(S3_BASE)/$(PRODUCER)` (Rust) or `s3_prefix:
edge/${PRODUCER}` (Go), so the bucket layout is the same everywhere and the
consumer reads both with `--depth 2`.

### EKS: IRSA or Pod Identity (`components/eks-irsa`, `components/eks-pod-identity`)

- **IAM policy**: [`../acceptance/iam/s3-policy.json`](../acceptance/iam/s3-policy.json)
  with `BUCKET` replaced. It grants `s3:PutObject`/`GetObject` on
  `BUCKET/edge/*` (where `S3_BASE` puts every lane) and `s3:ListBucket` on
  the bucket. Drop its `otel-accept/*` resource and the bucket-config
  statement once acceptance is done.
- **IRSA**: the overlay annotates the publisher's ServiceAccount with
  `eks.amazonaws.com/role-arn`; the role's trust policy is
  [`trust-irsa.example.json`](../acceptance/iam/trust-irsa.example.json) with
  `sub = system:serviceaccount:otel-edge:otap-publisher` (or
  `otelcol-publisher`). otap-s3pq uses object_store's web-identity provider,
  which talks https to the regional STS (D18).
- **Pod Identity**: no annotation; `aws eks create-pod-identity-association`
  for the same ServiceAccount, trust policy
  [`trust-pod-identity.example.json`](../acceptance/iam/trust-pod-identity.example.json).
  The agent injects `AWS_CONTAINER_CREDENTIALS_FULL_URI` and the token file.
- Both set `AWS_EC2_METADATA_DISABLED=true`: if the webhook did not fire, the
  Go SDK must fail rather than fall back to the node's instance role
  (RUNBOOK §3.1 calls that a finding in itself). object_store ignores the
  variable [D]; block pod access to IMDS with the node's hop limit as well.
- The agents and the gateway get no AWS permissions at all.

### Nutanix Objects (`components/nutanix`)

- **Keys** from the Secret `nutanix-objects-credentials`
  (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`), created out of band and
  loaded with `envFrom`. Both clients take them from the environment.
- **Private CA**: the overlay generates the ConfigMap `nutanix-objects-ca`
  from `nutanix-ca.pem`, mounted at `/etc/ssl/nutanix/ca.pem`, with
  `AWS_CA_BUNDLE` pointing at it. otap-s3pq adds it to the roots of its S3
  and STS clients (src/store.rs); the Go SDK appends it to the system
  roots. `SSL_CERT_FILE` is not used: it would replace the system roots.
- **Path-style**: otap-s3pq is path-style for any custom endpoint
  (`S3_BASE=https://objects.example/BUCKET/edge`); the Go publisher gets
  `s3_force_path_style: true`.
- **Checksums**: `AWS_REQUEST_CHECKSUM_CALCULATION=when_required` and
  `AWS_RESPONSE_CHECKSUM_VALIDATION=when_required`, which the acceptance
  kit's `checksum` check calls for when the store rejects the SDKs' default
  CRC32 trailer (RUNBOOK §3.2). They matter for the Go SDK; object_store
  sends no checksum unless configured [D], so they are inert for otap-s3pq.
- **Before any of this**: run the acceptance kit against the bucket. Whether
  Objects decides `If-None-Match: *` atomically is risk #1, and nothing here
  can compensate if it does not.

### IAM Roles Anywhere, on-premises to AWS S3 (`components/roles-anywhere`)

- `aws_signing_helper serve` runs as a **native sidecar** (an init container
  with `restartPolicy: Always`, Kubernetes ≥ 1.29) on 127.0.0.1:9911. It
  starts before the publisher, stays up with it, and serves IMDSv2 with the
  role's credentials, refreshed from the certificate.
- Both publishers find it through `AWS_EC2_METADATA_SERVICE_ENDPOINT`
  (otap-s3pq maps the variable onto object_store's metadata endpoint).
  `serve` rather than `credential-process`: the same pattern then works for
  chDB and ClickHouse, which have no process provider (D18, U13).
- The certificate is the Secret `rolesanywhere-cert` (`tls.crt`, `tls.key`),
  issued by the trust anchor's CA; cert-manager can issue and rotate it. The
  trust anchor, profile and role ARNs are set in the overlay. The role's
  permissions are the same `s3-policy.json`; its trust policy is the Roles
  Anywhere service principal with the certificate's subject as a condition.
- The image: `images/aws-signing-helper.Dockerfile` downloads a pinned release
  and checks its SHA-256. Never run for real (D18's open risk).

## Durable buffer: size and disk-full policy (D19, risk #14)

- **Default on** for the deployed Rust publisher. The agents' persistent queue
  makes custody durable one hop earlier as well; the publisher buffer is what
  lets an S3 outage be absorbed per publisher without pushing back on 200
  nodes' queues, and makes a publisher restart cost nothing.
- **Size**: `BUFFER_CAP=40 GiB` on a 50 Gi PVC. At the mid scenario a
  publisher (3 per cluster) takes about 10k spans, 3.3k logs and 21k points
  a second; at ~315 B of segment per span (half of the 630 B written) the cap
  holds roughly an hour of S3 outage [E]. The engine refuses a cap under
  192 MiB per core (WAL 128 MiB + two 32 MiB segments) [M].
- **Disk-full policy: `backpressure`** [M, `results/durable-diskfull.txt`]:
  - at the cap the receiver answers 503 "storage at capacity" (retryable),
    the agents keep the data, and every acked request is committed later:
    0 lost;
  - `drop_oldest` keeps acknowledging and deletes the oldest segments: 47 of
    64 acked requests never reached S3. Never use it;
  - if the **filesystem** fills before the cap, ingest also fails
    retryably, but a restart cannot replay the WAL (it needs room for a
    segment) and the process exits after 5 attempts: a crash loop until the
    volume is grown. After growing it every acked request was committed.
    Stuck, not lost. Hence the cap at 80% of the volume.
- **Alert** on `storage_bytes_used_bytes` (the publisher's
  `/api/v1/metrics`) against the cap, and on the pod's volume usage.
- A host crash loses at most the last 25 ms of acknowledged requests (the
  WAL's fsync interval; D19).

## Duplicates and request identity (risk #10)

The content key hashes a request's bytes. Central drops a second copy only if
its bytes are identical. Every hop that can resend was checked [M,
`results/`]:

| Resend | Same bytes? | Result |
|---|---|---|
| agent resends from its queue (publisher down, timeout) | yes: the queue item is the batch | the publisher recognises its own content key, or central does; `agent-queue.txt`: 32 requests acked while the publisher was down, agent SIGKILLed, all 160,000 spans and 160,000 logs once, 10,000 rows per object |
| Quiver replays after a publisher restart | yes: the WAL holds the batch | `durable-diskfull.txt`, `route-pubkill-patched.txt`: 0 duplicates |
| the gateway's per-publisher retry | yes: the same piece | `route-pubkill-stock.txt`: 0 |
| the agent resends through a restarted gateway, traces | yes: the split is deterministic for traces | `route-gwkill-stock.txt`: traces 0 |
| … logs and metrics, stock exporter | **no**: pieces are merged in Go map order | `route-gwkill-stock.txt`: **18,277 duplicate log rows** in 320,000 |
| … logs, with `collector/patches/0001` | yes | `route-gwkill-patched.txt`: 0 |
| … after the ring changed (a publisher added or removed) | no: the pieces are cut differently | `route-reroute-patched.txt`: 3,348 trace and 6,412 log rows duplicated |
| a request that reached a publisher's WAL is resent (publisher crash between the WAL write and the ack, a sender timeout), with the publisher's batch step | no: it lands in another batch | a window of milliseconds; not hit in `route-pubkill-batched-direct.txt` (publisher SIGKILLed under 8 senders: 0 duplicates, 4 byte-identical replays dropped) |
| the agents resend through a killed gateway to publishers **with** the batch step | no: the resent pieces are re-batched | `route-gwkill-batched.txt`: **70,320 trace and 61,727 log rows** duplicated by two SIGKILLs (8 concurrent senders) |
| … the gateway stopped with SIGTERM (a rolling update) | nothing is resent | `route-gwterm-batched.txt`: 0 (the gateway drained its in-flight requests in ~6 s) |
| … publishers **without** the batch step, patched gateway | yes | `route-gwkill-unbatched.txt`: 0, with 8 concurrent senders |

Nothing was ever missing: in every scenario central had all 320,000 rows of
each signal.

## Routing (D16): `components/routing`

**When**: only where raw Parquet is read by service (the lake option, D17).
With central ingest alone the objects are deleted after ingest, and routing
buys nothing but skew.

**What it deploys**: `otel-gateway`, a stateless Deployment (3 replicas) of
the ocb build with the contrib `load_balancing` exporter, `routing_key:
service`, the k8s resolver on a headless ring Service; 8 publishers (with
the batch step, below); and an agent config that sends
traces and logs through the gateway and **metrics straight to the
publishers**: routing by service drops any metric resource without
`service.name` (the exporter logs it and moves on [D]), and layout B gains
nothing from affinity.

Gateway settings and why:

- **No queue at the load_balancing level, and no batching.** The agent's
  request is answered only when every piece is acknowledged, so a gateway
  restart loses nothing and the agent resends.
- **`timeout: 60s` at the load_balancing level.** Its default (5 s) cut the
  pieces' own retries short while a publisher restarted, which turned a
  publisher restart into an agent-level resend and 7,754 duplicate log rows
  [M, `route-pubkill-stock-lbtimeout5s.txt`]; with 60 s, 0
  (`route-pubkill-stock.txt`). The agents' exporter timeout (90 s) is above it.
- **Per-piece retry up to 45 s**: the same bytes to the same publisher.
- **The ring Service publishes not-ready addresses.** A publisher that fails
  readiness for a moment (memory limiter at its hard limit) must not leave
  the ring: every ring change moves ~1/N of the services and re-cuts every
  in-flight request (the reroute row above).
- **The patched exporter** (`collector/patches/0001`, 10 lines): iterate the
  routing keys in sorted order when merging pieces per publisher, so a resent
  logs or metrics request produces byte-identical pieces. Upstream candidate
  (proposed as U20). It is what makes the unbatched variant exact; with
  batching publishers it changes nothing, and costs nothing.

**Measured** (N = 8, 120 services with Zipf 1.1 shares, 32 distinct
10k-row requests per signal, 120 services in each):

| | traces | logs | D16 (generated routing) |
|---|---|---|---|
| services on exactly one publisher | 120 of 120 | 120 of 120 | – |
| busiest publisher | 29.0% of rows (2.3× mean) | 36.8% (2.9× mean) | 36% (2.9×) |
| quietest publisher | 4.2% | 3.1% | 0.3% |
| rows per object, one piece per object | 1,250 mean (420–2,898 by publisher) | 1,250 (306–3,683) | 3,750 / 1,250 at T = 1 s |
| objects per 10k-row request | 8 | 8 | – |
| with edge-publisher.yaml's batch step, 8 concurrent senders | 146 objects for 256 pieces | 127 for 256 | – |

The test's requests are 10,000 rows each, as a gateway would see from
well-filled agents; real agent requests are much smaller (below).

The skew matches D16. **The object count depends on where the batching
is**, and that is the design choice this component makes:

- **With the publishers' batch step** (the default here): each publisher
  merges the pieces it receives for up to 1 s or 3 MiB, which is D16's
  model (objects of the owner's rate × 1 s; 320 objects/s fleet-wide at
  N = 8). The test above shows the merge (256 pieces → 146 and 127 objects
  with only 8 senders); with a cluster's 200 agents the size threshold is
  reached first. The cost: a gateway that dies with requests in flight
  (SIGKILL, OOM, node loss) makes the agents resend them, the publishers
  re-batch the resent pieces, and central keeps both copies: two SIGKILLs
  duplicated 70,320 of 320,000 trace rows and 61,727 of 320,000 log rows
  (`route-gwkill-batched.txt`, 16 senders each holding a request in flight
  at the kill). A graceful stop duplicates nothing (`route-gwterm-batched.txt`),
  so rolling updates are clean; the exposure is per crash, about the
  gateway's in-flight data. The gateway exports a request's pieces to the
  publishers one after another, so a request is held for up to N × the
  batch window (8 s at N = 8 in the one-sender test), which is also the
  size of that exposure; a gateway that fanned out concurrently (a second
  small patch) would cut both by N.
- **Without it** (`edge-durable.yaml`; the snippet is in the component):
  every piece is its own object and a resent piece is byte-identical, so a
  gateway crash duplicates nothing (`route-gwkill-unbatched.txt`, with the
  patched exporter). But every agent request becomes up to N objects, and
  real agent requests are small: a mid-scenario node makes ~150 spans/s, so
  a 2 s agent batch of ~300 spans becomes ~8 objects of ~40 rows, about
  800 trace objects/s per cluster and ~30k objects/s fleet-wide with logs,
  roughly 100× D16's count [E]. At $0.005 per 1,000 PUTs that is on the
  order of $10k a day, before central's per-object cost. Only worth it with
  far larger agent batches.

Exactness across gateway crashes with batching needs piece-level identity:
the publisher remembering the content keys of the requests it received
(not only of the batches it wrote) and acking a resent piece it already
holds. That is edge-side Rust work, proposed, not built.

**What a gateway restart does**: SIGTERM (a rolling update): the gateway
finishes its in-flight requests (~6 s here) and exits; nothing is resent.
SIGKILL, OOM or node loss: the agents resend what was in flight, which
duplicates as above with batching publishers, and nothing with unbatched
ones and the patched exporter. The stock exporter also duplicates logs and
metrics on the unbatched path (its merge order is random).

**What scaling does**:

- **Scale up** (N → N+1): ~1/(N+1) of the services move to the new
  publisher (15 of 120 trace services, 8 of 120 log services in the test).
  Their rows for the hour of the change are in two publishers' lanes: a
  reader that knows the ring must know its history. Requests in flight
  during the change are re-cut and duplicated in central.
- **Scale down**: the same moves in reverse, plus the removed publisher's
  PVC keeps whatever its buffer had not committed (`whenScaled: Retain`).
  That data is committed when the same ordinal comes back; it is not lost,
  but it is late. A StatefulSet always removes the highest ordinal, and the
  ring and the agents follow the pods, so a publisher cannot be drained
  while it still exists as a pod. Scale down only when S3 is healthy (the
  buffers are then near their floor: watch `storage_bytes_used_bytes`), and
  keep the PVC until that ordinal has been back once or its directory is
  empty.
- **Producer ids are stable** per ordinal (`CLUSTER-otap-publisher-N`), so a
  restarted or rescheduled publisher continues its lanes, and a lane's
  consumer lease and checkpoint are reused.
- The object count per cluster grows with N either way (D16): keep the
  consumer at 32 objects per statement.

## Security notes

- OTLP inside the cluster is plaintext (`tls.insecure: true`); for mTLS set
  the `tls` blocks of the agents' and gateway's exporters and the receivers'
  TLS settings. The publishers take OTLP only from their namespace
  (NetworkPolicy).
- The Rust engine's admin port (8080) also accepts configuration and
  shutdown POSTs. It listens on the pod IP for the kubelet's probes; the
  NetworkPolicy opens it only to a namespace labelled
  `otel-edge/monitoring=true`.
- Containers run as UID 10001 with a read-only root filesystem and no
  capabilities. The agent's init container runs as root only to chown its
  hostPath queue directory.

## Validation

What ran here, 2026-09-26, against SeaweedFS 4.47 on :18333 (bucket
`deploy-edge`) and ClickHouse 26.10 on :18123, with the otap-s3pq release
build of this branch and ocb v0.161.0 builds:

| Check | Script | Result |
|---|---|---|
| every overlay builds | `kustomize build --load-restrictor LoadRestrictionsNone` (v5.7.1) | 9 of 9 |
| every rendered object against the API schemas | `kubeconform -strict`, Kubernetes 1.31 | 118 of 118 valid |
| every collector config | `otelcol-* validate` (agent ×3, gateway, Go publisher, chDB edge) | valid |
| every otap-rs config, rows | `scripts/configs_e2e.sh` → the crate's `correctness.py` (against parquetgo) and `metrics_correctness.py` (layout B views against contrib) | 237 PASS, 0 FAIL (`results/configs-correctness.txt`): traces/logs 28 of 28 per config, metrics 51 per config |
| agent contract | `scripts/agent_test.sh` | `results/agent-queue.txt` |
| durable buffer full | `scripts/diskfull.sh` | `results/durable-diskfull.txt` |
| routing, restarts, scaling | `scripts/route_test.sh` (+ `gateway-local.yaml`, `publishers.sh`, `route_check.sh`) | `results/route-*.txt` |
| Go publisher | `scripts/go_edge_test.sh` | objects checked directly (below) |

Notes:

- **The Go publisher** (`base/go/publisher-config.yaml` on `otelcol-awss3`):
  16 + 16 requests acked, SIGKILL 4 s in, restart on the same queue: all
  160,000 spans and 160,000 logs in S3 once, 16 objects each, one epoch (the
  queue replayed everything). The consumer could not be used for it: the
  working-tree consumer the other work in this branch was building at the
  time failed every statement with a quoting error (a `throwIf` message
  containing apostrophes), so rows were counted from the objects instead.
- `route_check.sh` has a `CHECK=s3` mode that counts straight from the
  objects and drops an object whose rows, in order, equal another's (what
  the consumer's content check skips). On `gwkill-stock` it gives the
  consumer's numbers exactly (18,277 duplicate log rows).
- Not exercised: a real cluster (probes, the webhooks, the k8s resolver,
  EndpointSlice churn, PVC expansion), mTLS, OTLP compression between the
  tiers (left off: not measured against the Rust receiver), metrics through
  the routing tier (sent around it by design).

## Reproduce

```sh
S=scratch; B=$S/otap-rs-target/release; T=otap-rs tools (otlpsend, otlpgen, metricsref)
collector/build.sh $S/otelcol-deploy; PATCHED=1 collector/build.sh $S/otelcol-patched
(cd ../bench/sorting/gen && go build -o $S/mixgen .)
for s in traces logs; do $S/mixgen -signal $s -out $S/mix -batches 32 -publishers 1 -route none -seed 7; done
export AWS_ACCESS_KEY_ID=otel AWS_SECRET_ACCESS_KEY=otelsecret     # SeaweedFS; bucket deploy-edge
BIN=$B/otap-s3pq T=$T W=$S/run RUN=cfg1 scripts/configs_e2e.sh
GW=$S/otelcol-deploy/otelcol-deploy BIN=$B/otap-s3pq SEND=$T/otlpsend MIX=$S/mix W=$S/run RUN=agent1 scripts/agent_test.sh
BIN=$B/otap-s3pq SEND=$T/otlpsend MIX=$S/mix W=$S/run TMPFS=/mnt/160m-tmpfs scripts/diskfull.sh
GW=... BIN=... CONSUME=$B/consume SEND=... MIX=... W=$S/run RUN=gwkill-patched SCEN=gwkill N=8 scripts/route_test.sh
CFG=$PWD/../otap-rs/configs/edge-publisher.yaml SENDERS=8 CHECK=s3 ... RUN=gwkill-batched ... scripts/route_test.sh
GOCOL=otelcol-awss3 CONSUME=... SEND=... MIX=... W=$S/run RUN=go1 scripts/go_edge_test.sh
```
