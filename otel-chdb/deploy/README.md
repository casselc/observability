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
  base/go/            otelcol-s3pq publishers (StatefulSet + PVC, the s3pq exporter); publisher-config.yaml;
                      agent config agent-go.yaml (every signal)
  components/         eks-irsa, eks-pod-identity, nutanix, roles-anywhere: one per target;
                      routing: the D16 gateway tier (Rust edge)
  overlays/           {rust,go}-{eks-irsa,eks-pod-identity,nutanix,roles-anywhere}, rust-eks-irsa-routing
  collector/          ocb build of the agent/gateway collector; patches/0001 for the gateway
  images/             Dockerfiles: otap-s3pq, any ocb collector, aws_signing_helper
  edgeprobe/          the publishers' readiness probe (a wedged buffer is not ready; §Durable buffer)
  alerts/             Prometheus rules for the buffers, the probe and the commit outcomes
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
                     memory_limiter → persistent queue (hostPath); no batch step
                     retry forever
                        │ OTLP/gRPC, round robin over the headless Service
                        ▼
                    otap-publisher-N (StatefulSet, 3 per cluster; 8 routed)  ┌─ with components/routing:
                     receiver → batch (3 MiB / 1 s) → durable buffer (PVC)  │  agents → otel-gateway (Deployment)
                     (Go: receiver → s3pq batch (10k items / 1 s) → queue)  │
                     → exporter:s3pq ── 1 create-only PUT per object ──► S3  │  load_balancing, routing_key: service
                                                                             │  → the publisher that owns the service
```

The agents are the custodians of anything not yet acknowledged: their queue
is on the node's disk and they never give up. A publisher acknowledges after
its WAL write (Go: its queue write), and its buffer holds the data until the
create-only commit. **Nothing batches in front of a persistent queue and
acknowledges before the batch is written**: the agents do not batch at all,
and each publisher batches before its buffer but answers a request only
once the batch holding it is on disk (D4, 2026-09-27).
Central (the consumer) is unchanged and is not deployed here.

## What each decision became

| Decision | Where | Setting |
|---|---|---|
| D1 edge publisher | base/rust (primary), base/go (secondary) | the Go edge is the `s3pq` exporter (`../parquetgo/s3pqexporter`, 2026-09-26): manifest-less for traces, logs and metrics layout B, the Rust edge's objects (`../conformance`); it replaced `awss3inline`, which had no metrics lanes |
| D3 commit protocol | both publishers | one create-only PUT per object at `{bucket}/edge/{CLUSTER-pod}/{signal}/{epoch}/{seq}`; the IAM policy grants `s3:ListBucket` so a free slot is 404 |
| D4 / risk #10: agents never batch in front of a persistent queue; publishers batch before theirs and ack after it | agent-rust.yaml, agent-go.yaml, edge-publisher.yaml, Go publisher-config.yaml | Agents, both edges (2026-09-27): **no batch processor** (it acks before the queue write: acked requests were lost on SIGKILL on kind, `results/k8s-sim.md` §kind, and locally 10 of 32 per signal on the Go edge, `results/go-batch.txt`), `sending_queue` on `file_storage` sized in items, no `sending_queue.batch`; Rust publisher: `processor:batch` in front of the WAL (acks propagate after the WAL write), nothing between the WAL and the exporter; Go publisher: s3pq's `batch` in front of its queue (10k items / 1 s; each caller answered once the merged request is enqueued), no `sending_queue.batch` (rejected by s3pq) |
| D4: retries never give up | agents, Go publisher, Rust buffer | `retry_on_failure.max_elapsed_time: 0`; Quiver retries NACKs with backoff 1–30 s and no deadline (`max_age` unset) |
| D7 layout B | edge-publisher.yaml, Go publisher-config.yaml | `metrics_layout: series_table` (per-type points lanes + the series lane) |
| D16 sorting off | all otap-rs configs | `parquet.sort.by: none` |
| D16 routing | components/routing | gateway with `routing_key: service`, 8 publishers, ring Service with `publishNotReadyAddresses` (§Routing) |
| D18 credentials | components/* | no keys in any file; the default chain in both clients (§Targets) |
| D19 durable buffer | base/rust, base/go | Rust: on, 40 GiB cap on a 50 Gi PVC, `size_cap_policy: backpressure`; Go: the `file_storage` queue, 16 GiB of OTLP (`sizer: bytes`) on a 50 Gi PVC; both: not ready when the buffer can't take writes (§Durable buffer) |
| risk #11: 10k-row objects | edge-publisher.yaml; Go publisher-config.yaml | the Rust publisher's `max_size: 8 MiB` splits big requests deterministically and bounds a merged batch; the Go publisher's `batch.max_size: 10000` (items) does the same (the batch processor's split) |
| risk #14 disk full | edge-publisher.yaml, Go publisher-config.yaml, both publisher.yaml | backpressure, cap well below the volume, measured; readiness fails on a full volume or a buffer at 95% of its cap (`edgeprobe`), alerts in `alerts/` (§Durable buffer) |
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

`otelcol/config.edge.yaml` (the chDB edge, D4's config): `max_elapsed_time:
0` and `seal_optimize: false` were right; the `batch` processor before the
persistent queue was thought right then, but it acknowledges before the
queue write (`results/k8s-sim.md` §8, 2026-09-27). The chDB edge is not deployed (D1), so
it is left as is and noted in D4. Two defects fixed: **no memory limiter in
any pipeline** (the build carried it, nothing used it), and **batches of
5,000–20,000 rows** where the design and the consumer's limits assume
10,000 (`send_batch_size` / `send_batch_max_size: 10000`). It passes
`otelcol-chdb validate`.

`awss3/collector/builder-config.yaml`: `memorylimiterprocessor` and
`healthcheckextension` added, for the Go publisher's config and probes;
since 2026-09-26 it builds `otelcol-s3pq` with the `s3pq` exporter (the
image `base/go` runs), the awss3 prototypes kept for their demo.

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
  (`S3_BASE=https://objects.example/BUCKET/edge`), and so is the Go
  publisher's `s3pq` (parquetgo's client: path-style for an http(s)://
  URL; `s3.path_style` overrides). `S3_FORCE_PATH_STYLE` is no longer read.
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
  - on kind [M, `results/k8s-sim.md` §kind] (ordinal 3 on a 40 MiB volume,
    S3 down): the publisher answered `Unavailable` ("wal io error: No space
    left on device") and the agents sent those requests to the other
    publishers; it stayed **Ready** throughout. After S3 came back it could
    not release committed segments either (its progress file needs room).
    Growing the volume freed the segments, but one batch whose segment
    flush had failed was committed only after a pod restart replayed the
    WAL. End result: every row once. Operationally: grow the PVC, then
    restart the pod.
- **Go publisher** (`base/go`): the `file_storage` queue is the buffer,
  capped at **16 GiB of OTLP** (`sizer: bytes`; ~1.6× that in bbolt, and
  compaction copies the live data) on a 50 Gi PVC. The cap must stay well
  below the volume, more strictly than for Rust: **a full volume loses
  acknowledged requests** [M, `results/wedge.txt`: 1 of 5 in each of 4
  runs]. The exporterhelper queue (v0.161.0) advances its read index before
  the storage write that marks an item dispatched; when that write fails
  with ENOSPC it abandons the item, and the index is saved at the next write
  that succeeds (U22). Queue full at its cap (not the volume): refused
  retryably, nothing lost.
- **Not ready when the buffer can't take writes** (2026-09-27, `edgeprobe/`,
  `results/wedge.txt`). Both publishers' readiness probe is `edgeprobe`, a
  6 MB static binary in each image, run by the kubelet as an exec probe. It
  wraps the old check (`-ready`: the engine's `/api/v1/readyz`, which fails
  at the memory hard limit; the collector's `health_check`) and adds two:
  - the buffer volume has **under 1 GiB free** (statfs of the buffer
    directory): the kind wedge;
  - the buffer is at **95% of its cap**: Rust `storage_bytes_used_bytes /
    storage_bytes_cap_bytes` from `/api/v1/metrics`, Go
    `otelcol_exporter_queue_size / otelcol_exporter_queue_capacity`
    (`exporter="s3pq"`) from `:8888/metrics`.

  Both are states, not events, so the pod returns by itself once S3 drains
  the buffer or the volume grows (`successThreshold: 3`, 15 s). Why a probe
  binary: neither collector can report it. The engine's readyz knows memory
  pressure and the pipeline phase only, and a node has no hook into it
  without patching otap-dataflow. The Go `health_check` (v1) reports the
  pipeline up; its component-status mode (a feature gate) would need the
  exporter to watch its own queue and disk anyway. A sidecar would add a
  container and an image per pod to do the same reads.

  | Local test (`scripts/wedge_test.sh`, 48 MiB tmpfs or a small cap, S3 cut) | Not ready after | Ready again | Acknowledged → on S3 |
  |---|---|---|---|
  | Rust, volume full (cap 1 GiB) | 1 s ("0 bytes available") | only after the volume grew (S3 back 90 s: still full) | 40,000 → 40,000 |
  | Rust, cap 192 MiB reached | 2 s (fill 0.951) | 6 s after S3 came back | 210,000 → 210,000 |
  | Go, volume full (cap 16 GiB) | 1 s | only after the volume grew | 50,000 → **40,000** (U22) |
  | Go, cap 150 MB reached | 5 s (fill 0.988) | 2 s after S3 came back | 240,000 → 240,000 |

  **What not ready does, and what it does not.** It removes the pod from
  the Service's endpoints and DNS, so new connections (a restarted agent)
  go elsewhere, and `EdgePublisherNotReady` fires
  (`alerts/edge-buffer.rules.yaml`, on kube-state-metrics). It does not
  close the running agents' connections: their `dns:///` round robin
  re-resolves only when a connection fails (§Runbook). What moves those
  agents' requests to the other publishers is the refusal itself (503 /
  `Unavailable`, retried by the agent through the next connection), as on
  kind.
- **Alerts** (`alerts/edge-buffer.rules.yaml`): the probe failing for 5 min
  (page); the Rust buffer above 50% of its cap or the Go queue above 50%
  (warn); writes refused (`processor.durable_buffer.ingest` failures, the
  Go queue's `enqueue_failed_*`); a Rust segment flush failed (restart the
  pod once the volume has room); the PVC under 10% free.
- A host crash loses at most the last 25 ms of acknowledged requests (the
  WAL's fsync interval; D19).

## Runbook

- **Scale publishers up** (N → N+1): `kubectl -n otel-edge scale
  sts/otap-publisher --replicas=N+1` (Go: `sts/otelcol-publisher`). **Rust
  publishers: nothing else to do.** Their receiver sends every agent
  connection a GOAWAY after ~5 min (`OTLP_MAX_CONN_AGE`, ±10% per
  connection; otap-rs `patches/0004`), the agents re-resolve the headless
  Service, and the new pod gets its share within ~5.5 min, with no agent
  restart [M, `../otap-rs/results/goaway/`: the third publisher took an
  equal share one age after it joined the DNS answer; exactly once]. To
  spread load sooner, restart the agents: `kubectl -n otel-edge rollout
  restart ds/otel-agent` (a graceful agent restart loses and duplicates
  nothing, `results/k8s-sim.md` §8 row 6). Without a max connection age a
  running agent never resolves the new pod (its `dns:///` round robin
  re-resolves only when a connection closes; on kind the fourth publisher
  got no traffic for as long as we watched, §8). The Go publisher gets the
  same behaviour from grpc-go's native
  `keepalive.server_parameters.max_connection_age` (5m, grace 45s in
  `base/go/publisher-config.yaml`, 2026-09-27; configured, not yet tested
  end to end). With `components/routing` the gateways watch EndpointSlices; the
  agents' metrics go straight to the publishers and are covered by the
  connection age.
- **Scale down** only while S3 is healthy, and keep the removed ordinal's
  PVC until it has been back once or its buffer is empty (§Routing, What
  scaling does).
- **A publisher not ready, "buffer volume full"**: S3 was unreachable long
  enough to fill the volume. Restore S3, **grow the PVC** (the buffer can't
  release committed segments without room), then **restart the pod**: a
  Rust segment whose flush failed on ENOSPC is committed only when a restart
  replays the WAL (kind). On the Go edge this should not happen with the
  cap below the volume; if it did, some acknowledged requests may be lost
  (U22), and nothing at the publisher tells which.
- **A publisher not ready, "buffer at its cap"**: S3 unreachable or slower
  than the traffic. Nothing to do at the publisher; the agents hold the
  rest in their queues, and the pod returns once S3 drains it.

## Commit outcomes (AMBIGUITY.md S1, E1)

Both publishers export **`s3pq_commit_outcomes_total{outcome}`**, one counter
with the same name and label values on each edge (Rust: the engine's
`/api/v1/metrics`, `otel_scope_name="exporter.s3pq"`,
`../otap-rs/src/commit_metrics.rs`; Go: `:8888/metrics`,
`../parquetgo/s3pqexporter/telemetry.go`). Each value is one event of the
create-only commit (`PUT If-None-Match: *`, then a HEAD when the answer is a
412 or missing):

| `outcome` | Event | Ends the append? |
|---|---|---|
| `committed` | 200 on the PUT | yes: ours |
| `resolved_own` | 412 or no answer, then the HEAD found our content key and epoch (includes a 5xx or 409 answered after the write applied) | yes: ours |
| `known` | the lane already found this content committed: the retry of a request whose answer was lost | yes: ours |
| `learned_other` | the HEAD found another batch in the slot; ours moves on | no |
| `resent` | no answer, then the HEAD found the slot free; the PUT is sent again to the same slot | no |
| `tombstoned` | the HEAD found a tombstone: the consumer closed the log, a new epoch | no |
| `unresolved` | the HEAD failed or timed out: the slot is kept, the request is NACKed and retried | yes: unknown |
| `inconsistent` | 412, then the HEAD found nothing (store not read-after-write): kept, NACKed | yes: unknown |

The terminal outcomes sum to the appends. `resolved_own`, `known` and
`resent` are the lost answers the protocol absorbed; none of them makes a
duplicate. What they cannot see is a copy with a **new content key** (a
gateway SIGKILL re-cut, a sender's resend into a new batch: §Duplicates);
they are neither prevented nor counted here. The Rust engine drops a series
while it is 0, so an outcome appears at its first event.

Alerts (`alerts/edge-commit.rules.yaml`): unresolved commits for 15 min
(warn: S3 or its permissions; the buffer fills behind it); unresolved with
nothing settling for 20 min (page); any `inconsistent` (page: the store
broke read-after-write).

## Duplicates and request identity (risk #10)

The content key hashes a request's bytes. Central drops a second copy only if
its bytes are identical. Every hop that can resend was checked [M,
`results/`]:

| Resend | Same bytes? | Result |
|---|---|---|
| agent resends from its queue (publisher down, timeout) | yes: the queue item is the request | the publisher recognises its own content key, or central does; `agent-queue.txt`: 32 requests acked while the publisher was down, agent SIGKILLed, all 160,000 spans and 160,000 logs once, 10,000 rows per object |
| Quiver replays after a publisher restart | yes: the WAL holds the batch | `durable-diskfull.txt`, `route-pubkill-patched.txt`: 0 duplicates |
| the Go publisher's queue replays a merged request after a crash mid-PUT | yes: the queue item is the merged request, with its received_at | `go-batch.txt` killput: 4-request batches, 3 replays per run landed twice on S3 with the same rows and received_at, the consumer skipped them: 0 duplicates in 2 runs |
| the gateway's per-publisher retry | yes: the same piece | `route-pubkill-stock.txt`: 0 |
| the agent resends through a restarted gateway, traces | yes: the split is deterministic for traces | `route-gwkill-stock.txt`: traces 0 |
| … logs and metrics, stock exporter | **no**: pieces are merged in Go map order | `route-gwkill-stock.txt`: **18,277 duplicate log rows** in 320,000 |
| … logs, with `collector/patches/0001` | yes | `route-gwkill-patched.txt`: 0 |
| … after the ring changed (a publisher added or removed) | no: the pieces are cut differently | `route-reroute-patched.txt`: 3,348 trace and 6,412 log rows duplicated |
| a request that reached a publisher's WAL (Go: its queue) is resent (publisher crash between the write and the ack, a sender timeout), with the publisher's batch step | no: it lands in another batch | a window of milliseconds; not hit in `route-pubkill-batched-direct.txt` (publisher SIGKILLed under 8 senders: 0 duplicates, 4 byte-identical replays dropped), nor in `go-batch.txt` (killpub, killput: 0 duplicates) |
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

### The hash ring

**What the exporter does** [D: `loadbalancingexporter@v0.161.0/consistent_hashing.go`]:
- It uses Karger-style consistent hashing with virtual nodes, as Dynamo
  and Cassandra do: 200 points per endpoint on a ring of 131,071 positions,
  placed by CRC32 of the endpoint string and the point's index.
- A routing key (here `service.name`) goes to the next point clockwise.
- The ring is a pure function of the endpoint set. Every gateway replica
  that sees the same publishers routes a service to the same publisher,
  with no coordination between them.
- Adding or removing one publisher moves only ~1/N of the services, which
  the scale-up row above measured.

**Keyed on pod hostnames, not IPs.** The resolver sets
`return_hostnames: true`, and the component makes the ring Service the
StatefulSet's `serviceName`, so the ring's endpoints are
`otap-publisher-N.otap-publisher-ring…`.
- Pod IPs change on every restart. Keyed on IPs, each publisher restart
  would move that publisher's services, even with not-ready addresses kept
  in the ring.
- With hostnames, only a change of N moves anything.
- This was fixed after the measurements above. Those used a static
  resolver, whose endpoints never changed.

**The ring is not the source of the skew** [E]:
- With 200 virtual nodes, a publisher's share of the ring deviates from
  1/N by about 1/√200 ≈ 7%.
- The measured 2.3–2.9× is key granularity: service sizes are heavy-tailed
  (Zipf), and one service is never split. No placement function spreads a
  service larger than 1/N of the traffic.

**The alternatives:**

| Scheme | Balance of the key space | Moved on a membership change | Fit here |
|---|---|---|---|
| Ring with virtual nodes (current) | ±~7% at 200 vnodes | ~1/N (minimal, in expectation) | fine at N = 8–16 |
| Rendezvous / HRW (Thaler & Ravishankar 1998) | exact in expectation, no vnodes | exactly the departing node's keys | a little cleaner; O(N) per lookup is free at this N; needs an upstream patch |
| Jump consistent hash (Lamping & Veach 2014) | near-perfect | minimal, but buckets are numbered 0…N−1 and only the last can go | matches StatefulSet ordinals and scaling from the end; needs a patch and a resolver that yields ordinals |
| Maglev (Eisenbud et al. 2016) | near-perfect via a lookup table | slightly more than minimal | built for packet load balancers; no gain over HRW at this N |
| Bounded-load consistent hashing (Mirrokni, Thorup & Zadimoghaddam 2018; in HAProxy and Envoy) | caps each node at (1+ε) × mean by spilling to the next node | minimal, plus spill churn as load moves | attacks the real problem (skew), but spilling breaks affinity for exactly the hot services, and each gateway replica only sees its own load |

**Recommendation.** Keep the ring: switching the placement function
doesn't touch the skew.

If the skew matters once real service sizes are known (the lake option,
D17), the fix is to **split hot services**:
- Route by `service` for ordinary services.
- For services above ~1/N of the traffic, route by `(service, hash(trace_id) mod k)`.
- Traces stay whole, and a hot service lands in k publishers' lanes, so a
  query for it reads k lanes instead of 1.

This could be an OTTL transform that adds a routing attribute for a listed
set of hot services, with `routing_key: attributes`. It is not built, and
is to be sized against real rates.

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
- **The agents find a new publisher within one connection age** [M,
  locally]: their `dns:///` round robin re-resolves the headless Service
  only when a connection closes. On kind, before the fix, the fourth
  publisher got no traffic for as long as we watched (2.5 min) until
  `kubectl rollout restart ds/otel-agent`. The Rust publishers now close
  each agent connection gracefully after ~5 min (`OTLP_MAX_CONN_AGE` /
  `OTLP_MAX_CONN_AGE_GRACE` in `base/rust/publisher.yaml`, otap-rs
  `patches/0004`): requests in flight finish (the 45 s grace is above the
  30 s receiver timeout, so none is cut and resent), and the agent
  reconnects through a fresh resolution. Scale-down needs nothing: the
  removed publisher's connections fail and the agents re-resolve. The
  routing gateway watches EndpointSlices and is not affected.
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
| scale-up without an agent restart (2026-09-27) | `../otap-rs/scripts/goaway_e2e.sh` (the agent config above, a local DNS name, 2 → 3 publishers, 20 s connection age) | `../otap-rs/results/goaway/`: the third publisher took an equal share one age after joining, never without the age; exactly once (0 missing, 0 duplicates) with a grace, 1,900 duplicate rows with none |
| Go publisher | `scripts/go_edge_test.sh` | objects checked directly (below) |
| Go edge batching before the publisher's queue (2026-09-27) | `scripts/go_batch_test.sh` (+ `../parquetgo/s3pqexporter` unit tests) | `results/go-batch.txt`: agent SIGKILL ×2: old configs lost 100,000 of 320,000 rows per signal, new 0; publisher SIGKILL ×2 and a SIGKILL mid-PUT with merged batches: 0 lost, 0 duplicates; rows per object 10,000 = 10,000 (10k-row requests) and 600 → 1,297 (300-row requests, 4 agents) |
| readiness on a wedged buffer (2026-09-27) | `scripts/wedge_test.sh` (+ `edgeprobe` unit tests) | `results/wedge.txt`: both edges not ready within 1–4 s of a full volume or a buffer at 95% of its cap, ready again when S3 drains it or the volume grows; the Go edge lost 1 of 5 acked requests on a full volume (U22) |
| every build against a live API server (2026-09-27) | `kubectl apply --dry-run=server --validate=strict`, Kubernetes 1.36.1 on KWOK | 11 of 11 (the 9 overlays, `kind/edge`, `kind/routing`) |
| one real cluster, end to end (2026-09-27) | kind v0.31 / Kubernetes 1.35, `kind/edge` then `kind/routing`, SeaweedFS and the consumer on the host | `results/k8s-sim.md` §kind: 17 datasets of 320k spans + 320k logs through agents → publishers → S3 → consumer; exactly once through publisher SIGKILLs, force deletes, rollout restarts, scale 3→4→3 with a retained buffer, a full buffer volume, graceful gateway restarts; duplicates only on gateway SIGKILL (as `route-gwkill-batched`); the agent SIGKILL loss found and fixed (`agent-rust.yaml`) |
| control-plane behaviour, no containers | `kind/edge` and `kind/routing` applied to a 1,000-node KWOK cluster | `results/k8s-sim.md` §Manifests: PVCs bind and are kept on scale-down and reused on scale-up (3→4→3), rollout restart one ordinal at a time, `system-node-critical` admitted outside kube-system, and the routing switch needs a rollout restart (fixed in `components/routing`) |

Notes:

- **The Go publisher** (`base/go/publisher-config.yaml` on `otelcol-s3pq`,
  2026-09-26, `results/go-edge-s3pq.txt`): 16 traces, 16 logs and 16
  metrics requests (10k items each), SIGKILL 4 s in, restart on the same
  queue, then the Rust consumer: 160,000 spans, 160,000 logs, 64,000
  number points and 32,000 points of each other type in central, each
  request once (16 content keys per table; 8 cross-epoch copies skipped by
  the consumer's content check), 8,000 series rows, no point without its
  series row. (The earlier `awss3inline` run, `results/go-edge.txt`, had
  traces and logs only, counted from the objects: the consumer of that
  hour was broken.)
- `route_check.sh` has a `CHECK=s3` mode that counts straight from the
  objects and drops an object whose rows, in order, equal another's (what
  the consumer's content check skips). On `gwkill-stock` it gives the
  consumer's numbers exactly (18,277 duplicate log rows).
- Not exercised: EKS and its webhooks, real PVC expansion (a tmpfs
  remount stood in for it on kind), multi-node scheduling, mTLS, OTLP compression between the
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
GOCOL=otelcol-s3pq CONSUME=... SEND=... MIX=... W=$S/run RUN=go1 scripts/go_edge_test.sh   # MIX: + metrics-b00NN.pb
$S/mixgen -signal traces -out $S/mixs -batches 160 -rows 300 -publishers 1 -route none -seed 11   # and logs; -rows 1000 for killput
GOCOL=... GW=... CONSUME=... SEND=... MIX=$S/mixs W=$S/run RUN=sizes-after SCEN=sizes ROWS=300 scripts/go_batch_test.sh
AGENT_CFG=<agent-go.yaml of 30e5320> PUB_CFG=<publisher-config.yaml of 30e5320> ... RUN=sizes-before ... scripts/go_batch_test.sh
FAULTPROXY=$T/faultproxy2 ... MIX=$S/mixm SCEN=killput ROWS=1000 scripts/go_batch_test.sh   # also SCEN=bulk, killagent, killpub
(cd edgeprobe && CGO_ENABLED=0 go build -o $S/edgeprobe .)
EDGE=rust CASE=volume BIN=$B/otap-s3pq PROBE=$S/edgeprobe SEND=... MIX=$S/mixw W=$S/run RUN=w1 scripts/wedge_test.sh   # root: mounts a tmpfs; EDGE=go GOCOL=..., CASE=cap
```
