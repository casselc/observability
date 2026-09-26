# Decisions: edge → S3 → ClickHouse telemetry pipeline (otel-chdb spike)

The architecture decision record for this spike. It gathers findings that are
spread over about twenty READMEs, several of which changed direction more than
once. Where a README and this file disagree, the README is the evidence and
this file is the summary. Section 5 lists the places where the READMEs
disagreed with each other, and how each was resolved.

- **Branch:** `claude/brave-pascal-0fecgh`, head `7125a72` (2026-09-26).
- **Scope:** the spike commits from `8156caa` (chdb-go vendored) to `7125a72`
  (edge sorting). Earlier commits on the branch belong to oscope itself.
- **Labels,** as in the READMEs: **[M]** measured in the spike, **[E]**
  estimate, **[D]** from docs or source, **[Q]** Quint model. Nearly every [M]
  was taken on one shared 4-vCPU box against SeaweedFS on localhost. The
  exceptions are the idle-box runs in `bench/clean` and `bench/sorting`.
- **Commit hashes** point at the commit that added or last changed the
  evidence.

## The pipeline in one picture

```
 k8s cluster (×20 per region)                         S3 (AWS) or Nutanix Objects
 ┌──────────────────────────────────────┐            ┌──────────────────────────────────────────┐
 │ pods → gateway collectors (3/cluster)│  1 create- │ {root}/{producer}/{signal}/{epoch}/       │
 │   Rust otap-dataflow + s3pq exporter │  only PUT  │     {seq:020d}.parquet   (data = commit)  │
 │   (or Go collector + parquetgo)      │ ─────────► │ {ctl}/lease/…  {ctl}/ckpt/…  (CAS'd)      │
 │   ack upstream only after the commit │  per batch │ {ctl}/gc.json               (CAS'd)       │
 └──────────────────────────────────────┘            └──────────────────┬───────────────────────┘
                                                                          │ LIST StartAfter, HEAD
                                                                          ▼
                                      consume workers (leased lanes, ≤32 objects per statement)
                                      INSERT … SELECT FROM s3('{k1,…,k32}') → verify by projection
                                                                          │
                                                                          ▼
                                      central ClickHouse 26.10: ReplicatedMergeTree, 2 replicas,
                                      hot fast disk 1–7 days → cold tier, 90 days total
                                      traces/logs: ClickStack tables; metrics: series layout (B)
```

## Decision index

| # | Decision | Status |
|---|---|---|
| [D1](#d1-edge-publisher-rust-otap-dataflow-exporter-go-parquetgo-not-chdb) | Edge publisher: Rust otap-dataflow exporter where it can run, Go `parquetgo` for Go collectors, not chDB | accepted; **Go path kept** (2026-09-26), its gaps are follow-up work |
| [D2](#d2-transfer-format-parquet-read-with-s3-not-native-parts) | Transfer format: Parquet read with `s3()`, not native parts on `s3_plain_rewritable` | accepted |
| [D3](#d3-commit-protocol-manifest-less-create-only-slots) | Commit protocol: manifest-less create-only slots | accepted; manifests and the S3-native log superseded |
| [D4](#d4-awss3exporter-stock-rejected-patched-prototyped-own-exporter-preferred) | awss3exporter: stock rejected, patched version prototyped, own exporter preferred | stock rejected; Go choice open |
| [D5](#d5-otap-variants-otap-only-as-an-input-transport) | OTAP: only as an input transport; never stored | accepted |
| [D6](#d6-no-edge-to-central-fast-path) | No edge-to-central fast path | accepted |
| [D7](#d7-metrics-series-table-layout-b-not-the-clickstack-tables) | Metrics: series-table layout B; wire-size ordinal rejected | accepted |
| [D8](#d8-consumer-leases-and-checkpoints-on-s3) | Consumer: leases and checkpoints on S3 | accepted |
| [D9](#d9-consumer-time-bound-on-inserts-plus-a-server-side-deadline) | Consumer: time bound plus a server-side deadline | accepted |
| [D10](#d10-consumer-multi-object-statements-squashed-to-one-block) | Consumer: multi-object statements squashed to one block | accepted |
| [D11](#d11-consumer-count-check-and-repair-not-dedup-tokens) | Consumer: count check and repair, not dedup tokens | accepted |
| [D12](#d12-consumer-gc-and-checkpoint-compaction) | Consumer: GC and checkpoint compaction | accepted |
| [D13](#d13-replicated-central-plain-replicatedmergetree-no-zero-copy) | Replicated central: plain ReplicatedMergeTree, zero-copy rejected, sync before checks | accepted |
| [D14](#d14-storage-tiers) | Storage tiers: hot 1–7 days, then cold | accepted; cold medium open |
| [D15](#d15-metrics-downsampling) | Metrics downsampling: 5-minute rollups | proposed; not built |
| [D16](#d16-edge-sorting-off-service-affine-routing-on-at-n--8) | Edge sorting off; service-affine routing at N ≥ 8 | accepted (routing not deployed) |
| [D17](#d17-lake--hybrid-cold-tier) | Lake / hybrid cold tier | exploratory |
| [D18](#d18-s3-client-and-credentials) | S3 client and credentials | accepted |
| [D19](#d19-durable-buffer-at-the-edge) | Durable buffer at the edge | Go accepted; Rust default open |
| [D20](#d20-pbt-defect-fixes-in-chdbexporter) | PBT defect fixes in chdbexporter | 1–3 fixed; 4–7 open |

---

## 1. Context and requirements

### 1.1 Deployment targets

Every S3 client in the pipeline must work in all three. The evidence for each
client is in [D18](#d18-s3-client-and-credentials).

| Target | Store | Credentials | TLS | What is unknown |
|---|---|---|---|---|
| EKS | AWS S3 | IRSA (`AWS_ROLE_ARN` + web-identity token) or EKS Pod Identity (container credentials + token file) | public CAs | real EKS/STS never used: all [M] is against local stand-ins |
| Nutanix | Nutanix Objects, custom https endpoint, path-style | static keys | **private CA** | **whether `If-None-Match: *` is supported and atomic**; HEAD/LIST consistency; `x-amz-meta-*` and CRC32 trailer handling ([`awss3/README.md`](awss3/README.md) §Deployment, [`model/S3NATIVE.md`](model/S3NATIVE.md) §9) |
| Outside AWS | AWS S3 | IAM Roles Anywhere: `aws_signing_helper` as `credential_process`, or `serve` (IMDSv2 emulation) | public CAs | a real `aws_signing_helper` was never run |

### 1.2 Fleet and rates, per region

The fleet is 15–20 Kubernetes clusters of about 200 nodes and about 3,000 pods.
The sizing uses the calculator's mid scenario, which takes the top of that
range. Only the fleet shape comes from requirements; the per-pod rates are
estimates ([risk 2](#4-open-risks-and-unknowns-ranked)).

| Input | Mid scenario | Provenance |
|---|---|---|
| clusters × nodes × pods | 20 × 200 × 3,000 (4,000 nodes, 60,000 pods) | requirement (upper end of 15–20) |
| spans / s per pod (after sampling) | 10 | [E] calculator default |
| logs / s per pod, per node | 3, 5 | [E] |
| active series per pod, per node | 500, 2,000 (38 M series) | [E] |
| export interval | 30 s | [E] |
| resulting rows / s | 600k spans, 200k logs, 1.27 M points: **2.07 M** | derived |
| gateway publishers per cluster | 3 (60 per region) | [E] |
| edge batch | 10,000 rows per object | design choice |

### 1.3 Retention

| Tier | Requirement | Calculator default |
|---|---|---|
| total | 90 days | 90 |
| fast disk (hot) | 1–7 days | 1 |
| raw metrics | not stated | 14 days, then 5-minute rollups ([D15](#d15-metrics-downsampling)) |
| series table | as long as any point or rollup refers to it | TTL on `LastSeen` ([`metrics-layout/README.md`](metrics-layout/README.md) §Downsampling) |

### 1.4 Correctness requirements (Quint models)

These invariants are the correctness contract. "Checked in code" means a
quint-connect or quintgo test replays model traces through the implementation.

| Model | Invariants the design must keep | Checked | Checked in code |
|---|---|---|---|
| `model/edgePublish.qnt` (manifests; now superseded) | `commitImpliesData`, `onlyCommittedIngested`, `batchIngestedAtMostOnce`, `payloadIngestedAtMostOnce`, `sealMatchesManifests` | 5,000 × 40-step simulation; Apalache ≤ 8–10 steps ([`model/README.md`](model/README.md), `1307816`) | quintgo conformance and model-seeded PBT ([`PBT.md`](PBT.md), `38c641e`) |
| `model/partLifetime.qnt` (native parts; now moot) | `noReadOfDeleted`, `noLeakAfterExit`, `noDoubleCount`; the rule **`old_parts_lifetime` > max query + refresh interval** | Apalache ≤ 10–12 steps | server test `TestOldPartsLifetimeProtectsServerQueries` (`2f1f3c2`) |
| `model/s3Inline.qnt` (**the commit protocol in use**) | `payloadIngestedAtMostOnce`, `epochNoDuplicatePayload`, `onlyCommittedIngested`, `noCommitLost`, `ackedImpliesCommitted`, `noPayloadLost`, `gapNeverTakenForLoss`, `consumerNeverSkipsCommitted`, `noCommitAfterClose` | 3,000 × 80 steps, two seeds; Apalache ≤ 6 steps (8 partial); 6 mutations caught ([`awss3/README.md`](awss3/README.md), `0855334`) | `tests/mbt_s3inline.rs`: 300 traces, 16,981 steps; 3 code mutants caught ([`otap-rs/README.md`](otap-rs/README.md), `060e963`) |
| `model/s3InlineMetrics.qnt` | `reqAckedImpliesAllCommitted`, `noObjectLost` (a request is acked only when all its objects commit) | 3,000 × 60; mutant `ackOnAny` caught | `tests/mbt_s3inline_metrics.rs`: 300 traces, 23,968 steps |
| `model/s3InlineConsumer.qnt` | `atMostOnce`, `onlyCommittedIngested`, `neverSkipsCommitted`, `noCommitAfterClose`, `announcedOnlyAfterCommit` | 5,000 × 60; mutants `noTimeBound`, `noVerify`, `gcTombs`, `announceEarly` | `tests/mbt_s3inline_consumer.rs`; code mutants `no_time_bound`, `no_verify` (`1b5ce6c`) |
| `model/s3InlineConsumerCompact.qnt` | the above plus `neverSkipsCommittedCompact`, `noCommitBelowFloor`, `floorSound`, `viewFloorSound`, `bounded` | 5,000 × 60; `compactBound` 20,000 × 150; mutants `earlyCompact`, `floorOnly` | code mutant `early_compact` (`9f2c75d`) |
| `model/fastPath.qnt` | `onlyCommittedIngested`, `batchIngestedAtMostOnce`, `noLostBehindCheckpoint` | 1,500 × 40, 23 scenarios; Apalache ≤ 10 steps ([`model/FASTPATH.md`](model/FASTPATH.md), `bd1ae88`) | not applicable: not built ([D6](#d6-no-edge-to-central-fast-path)) |
| `model/s3Native.qnt` (superseded) | 13 invariants, including `noWriteFromFencedWriter`, `gcKeepsLiveData`, `nsSingleWriter` | 3,000 × 120; Apalache ≤ 6 steps ([`model/S3NATIVE.md`](model/S3NATIVE.md), `30210a5`) | `s3cas` protocol tests only |

Assumptions every model makes, and which the code must therefore guarantee:

- **Atomic create-only PUT and CAS on the store** (`ATOMIC_COND`). This holds
  for single-part PUT on SeaweedFS 4.47 [M]. It fails for multipart completion
  on SeaweedFS [M]. It is unknown for Nutanix.
- **Read-your-writes on central** for the count check. On a replicated central
  this needs `SYSTEM SYNC REPLICA … LIGHTWEIGHT` ([D13](#d13-replicated-central-plain-replicatedmergetree-no-zero-copy)).
- **A bounded zombie lifetime and a bounded PUT lifetime,** which GC relies on
  ([D12](#d12-consumer-gc-and-checkpoint-compaction)).
- **Worker insert time bound** (`ZOMBIE_INSERT_BOUNDED`). This is enforced by
  the worker's own clock and by the server-side fence ([D9](#d9-consumer-time-bound-on-inserts-plus-a-server-side-deadline)).
- **Small domains and bounded depth.** A ✓ in simulation is not a proof.
  Apalache goes to 6–12 steps only.

---

## 2. Decisions

### D1. Edge publisher: Rust otap-dataflow exporter, Go parquetgo, not chDB

**Status:** accepted: chDB rejected for publishing; Rust where the Rust engine
can run; Go `parquetgo` where the edge must stay a Go collector. **The Go path
is kept for now** (decided 2026-09-26 by the project owner: not frozen, not
deprecated; see below).

**Decision.** Publish ClickStack-shaped Parquet from a native writer at the
edge. The Rust exporter (`otap-rs`, `urn:otel:exporter:s3pq`) walks the OTLP
protobuf bytes directly through otap-dataflow's zero-copy views. `parquetgo`
does the same job for Go collectors. chDB stays an option only for edges that
must answer SQL locally.

**Alternatives.** chDB in the collector (`chdbexporter`, with a chdb-go fork
for binary-safe inserts); arrow-go as the Go engine; the Go and Rust OTAP
variants ([D5](#d5-otap-variants-otap-only-as-an-input-transport)).

**Evidence** (per 10k-row batch, publishing to S3):

| | chDB exporter | Go `parquetgo` | Rust `otap-s3pq` | Source |
|---|---|---|---|---|
| edge CPU, traces / logs (loaded box) | 105 / 79 ms | 74 / 52 ms | 45 / 33 ms | [`parquetgo/README.md`](parquetgo/README.md) (`de81419`); [`otap-rs/README.md`](otap-rs/README.md) (`060e963`) |
| edge CPU, traces / logs (idle box) | – | 68 / 48 ms | 40 / 29 ms | [`bench/clean/README.md`](bench/clean/README.md) block 1 (`c9376ac`) |
| peak RSS | 390 / 353 MB | 108 / 142 MB | 38 / 32 MB in process; 58 / 51 MB whole engine | same |
| binary | 9.2 MB + 566 MB `libchdb.so` (glibc) | 15.7 MB static | 44.1 MB (glibc, thin LTO) | same |
| object size, traces / logs | 308 / 181 KB (local) | 265 / 155 KB | 131 / 76 KB (bloom on TraceId only) | same |
| S3 requests per batch | 2 PUTs (object + manifest) | 2 PUTs | 1 create-only PUT | same |
| arrow-go engine, for reference | – | 145–149 ms, 628k allocations per batch | – | [`parquetgo/README.md`](parquetgo/README.md) |

- **Rows are identical** across chDB, both Go engines and Rust: checksum and
  `EXCEPT` in both directions, on testgen and on hostile data. The Rust path
  passes 28 of 28 checks ([`otap-rs/README.md`](otap-rs/README.md) §Correctness).
- **Central ingest is at parity** within about ±10% (318 against 265 ms of
  server CPU for 10 traces batches; the 9-run re-run gave 287 against 259). The
  Rust objects read half the bytes.
- **The Rust gain comes from reading OTLP bytes directly, not from OTAP.**
  Going through OTAP record batches first costs 71 / 47 ms.

**Consequences.**

- There are two edge implementations that must stay row-identical. Keep the
  server-side conformance tests (`parquetgo/compare`,
  `otap-rs/scripts/correctness.py`) in CI. The arrow-go page split
  ([UPSTREAM_ISSUES.md](UPSTREAM_ISSUES.md) U5) shows that
  writer/reader drift is a real risk.
- The Rust build pins upstream otel-arrow at `5db8358` plus two local patches.
  It needs Rust 1.98.1 (577 MB of toolchain), 395 crates, and an 11-minute
  clean release build ([`otap-rs/README.md`](otap-rs/README.md) §Build).
- The saving is not where the money is. 28 ms saved per traces batch is about
  1.4 cores fleet-wide at 500 producers [E]. Central dominates cost.

**Is the Go path frozen? Decided 2026-09-26: no. The Go path is kept.** The
project owner decided to keep Go `parquetgo` as a supported edge for
collectors that must stay Go, neither frozen nor deprecated. That commits the
project to:

- keeping the Go edge row-identical to the Rust one, with the server-side
  conformance tests (`parquetgo/compare`, `otap-rs/scripts/correctness.py`)
  in CI (see Consequences);
- keeping the Go collector config on the D4 settings (persistent queue,
  `max_elapsed_time: 0`, batching before the queue);
- treating the Go gaps below as known follow-up work, **not done now**.

The Go gaps, as they stand:

- `parquetgo` was last changed in `84afd7e`. It still commits with manifests.
- The manifest-less commit exists in Go only as the `awss3inline` prototype,
  for traces and logs ([D4](#d4-awss3exporter-stock-rejected-patched-prototyped-own-exporter-preferred)).
  Metrics lanes are "not wired" ([`parquetgo/README.md`](parquetgo/README.md) §Gaps).
- Layout B exists in Go only as the `metrics-layout/seriesenc` prototype.
- The production consumer, the durable buffer, sorting, the wire encodings and
  checkpoint compaction were all built in Rust only (`4cd7692` → `7125a72`).

So a Go edge today cannot produce what the consumer and the default metrics
layout expect. Until the follow-up lands, a Go edge can commit manifest-less
only through the `awss3inline` prototype, for traces and logs. The follow-up
is: manifest-less commits for every signal in Go, with per-type metrics lanes
behind a request-level ack (through the patched awss3exporter or through
`parquetgo` plus the appender, which is D4's open choice); and a Go layout-B
lane built from `seriesenc`, with the wire encodings. The consumer is
Rust-only and serves both edges. A Go edge's durable buffer is the
collector's persistent queue ([D19](#d19-durable-buffer-at-the-edge)), and
sorting is off everywhere ([D16](#d16-edge-sorting-off-service-affine-routing-on-at-n--8)).

**Open risks.** otap-dataflow is pre-1.0. Its OTAP receiver closes the whole
stream on one undecodable batch (U10). Edges that must be `otelcol-contrib`
builds can't use Rust.

---

### D2. Transfer format: Parquet read with `s3()`, not native parts

**Status:** accepted (`7a1ea50`).

**Decision.** The edge ships one Parquet object per batch. Central ingests it
with `INSERT … SELECT FROM s3()`. Native MergeTree parts on
`s3_plain_rewritable` disks, attached read-only at central, are rejected as the
transfer path.

**Alternatives.** chDB writing `s3_plain_rewritable` tables that central
attaches and reads (built and tested: `452131c`, `2f1f3c2`); both at once.

**Evidence** ([`bench/central/REPORT.md`](bench/central/REPORT.md), `7a1ea50`; [`README.md`](README.md) §Publishing):

| | native parts | Parquet |
|---|---|---|
| central `INSERT … SELECT`, 50 batches: wall / query CPU | 1.49 s / 1.78 s | 1.72 s / 2.12 s (+10% CPU) |
| read for 1 batch | 7.49 MB, 19 GETs (merged part) | 0.26 MB, 1 GET + 1 HEAD |
| edge S3 writes per batch | 51 (compact parts), 75 (wide) | 4.7 by chDB's counter; **2 PUTs** by proxy count ([`parquetgo/README.md`](parquetgo/README.md)) |
| standing cost | 3 LIST + 1 GET per part per table per refresh; +5.7 ms CPU/s per table | none |
| retry after the writer merged the source | **all 80k rows re-inserted** | not applicable: objects are immutable |
| reader with `refresh_parts_interval = 0` | broke about 14 min after a merge | not applicable |
| objects leaked on writer exit | 1,814 objects, 36.5 MB for a 7.2 MB part | none |
| stored size | 1× | 2.2× (while in transit) |
| estimated edge PUT cost, 500 producers | $33k / month | $3k / month [E] |

**Consequences.**

- The partLifetime rules (`old_parts_lifetime` > query + refresh, reader
  leases) no longer bind, because nothing reads live native parts.
- Central pays about 0.4 µs/row for type conversion.
- The format is ClickHouse-version independent, and a lakehouse can read it.
  Spark needs `nanosAsLong` for `TIMESTAMP(NANOS)` ([`parquetgo/README.md`](parquetgo/README.md) §Correctness).

**Open risks.** Parquet needs a writer/reader conformance test (U5).

---

### D3. Commit protocol: manifest-less create-only slots

**Status:** accepted (`bd1ae88`, `060e963`). Per-batch manifests
(`452131c`) and the S3-native shared log with a fence entry (`30210a5`) are
**superseded**. The S3-native log is kept on paper for native tables, which D2
rejects.

**Decision.** Each batch is its own commit record. It is written at
`{prefix}/{signal}/{epoch}/{seq:020d}.parquet` with `PUT If-None-Match: *`,
and its description goes in `x-amz-meta-oscope-*` and the Parquet footer.

- On a 412 or no answer, the writer HEADs the slot:
  - ours → done;
  - free → resend identical bytes;
  - another batch → learn it, next slot;
  - tombstone → halt and start a new epoch.
- The consumer closes a superseded, quiet epoch by racing a zero-byte
  create-only tombstone into its first free slot.
- Epochs are one per lane per incarnation. A lane never abandons a slot, so a
  live epoch has no gaps.

**Alternatives.**

| Alternative | Why not |
|---|---|
| Table → Parquet → manifest JSON per batch, `_sealed.json` per generation (`chdbexporter/publish.go`) | The model found F1 (an ambiguous manifest PUT plus the queue's retry ingests the request twice), F2 (orphan rows for live readers), F3 (the seal undercounts) and F4 (dedup-window eviction at the consumer) ([`model/README.md`](model/README.md), `1307816`). Two PUTs per batch. |
| Content-derived batch ids plus listing seals (the model's first fix) | Closes F1 within a generation only; cross-epoch copies and F4 still need the consumer (`b3b76f1`, `a6d7daa`) |
| S3-native: one shared log per producer, lease CAS, fence entry, replay on start | Closes F1–F3 and PBT 1–3 [Q], but costs a data PUT plus a log PUT, a startup replay, and leaves orphans to sweep. It is the only option for native tables. |
| Keeper/etcd `Coordinator` for the control plane | Fallback if a store lacks atomic conditional writes ([`model/S3NATIVE.md`](model/S3NATIVE.md) §9) |

**Evidence.**

- **Store probes** ([`model/S3NATIVE.md`](model/S3NATIVE.md) §2, `s3cas/`, `30210a5`): 16 goroutines × 20 rounds of `If-None-Match` on one key gave exactly 1 winner every round. 20 of 20 cancelled PUTs had landed and resolved by read-back. Conditional **multipart** completion was **not** atomic: 4–8 winners out of 8 (U4).
- **Model:** 9 invariants hold. The mutations `plainPut`, `nonAtomicCond`, `retryNewKey`, `noHalt`, `skipGaps` and `noCheckCentral` each break something ([`awss3/README.md`](awss3/README.md), `0855334`).
- **Collector demo, SIGKILL plus a fault proxy:** stock awss3exporter stored 29 objects, 1,450 rows for 1,000 spans. The inline design stored 21 objects in 3 epochs, and central got exactly 1,000 ([`awss3/README.md`](awss3/README.md) §Test results).
- **Rust faults** (ambiguous, slow and dropped PUTs, crash, zombie): 60,000 rows and 6 contents in central in every scenario ([`otap-rs/README.md`](otap-rs/README.md) §Fault tests, `060e963`).

**Consequences.**

- **One PUT per batch.** Discovery is by `LIST StartAfter`. There is one HEAD
  per object for stats: LIST returns no metadata, and `ParquetMetadata` does
  not expose footer key-value pairs (U19).
- **Store requirements:** atomic single-part `If-None-Match`, read-after-write
  HEAD and LIST, and user metadata. IAM must grant `s3:PutObject`,
  `s3:GetObject` and **`s3:ListBucket`**; without it a HEAD of a missing key
  answers 403 and the writer stalls.
- **No multipart.** Keep objects under the 16 MiB transfermanager threshold,
  or call PutObject directly, as both appenders do.
- **Duplicates are expected, and bounded:** a request in flight at a crash is
  committed again in the next epoch. Central removes it
  ([D11](#d11-consumer-count-check-and-repair-not-dedup-tokens)). Any reader
  that bypasses central must deduplicate by `oscope-content`.
- **Deleted slots reopen** (`If-None-Match` succeeds on a deleted key). GC
  must respect the zombie bound ([D12](#d12-consumer-gc-and-checkpoint-compaction)).

**Open risks.**

- Nutanix atomicity is unknown ([risk 1](#4-open-risks-and-unknowns-ranked)).
- The fallback without conditional writes (close epochs by time) is not
  modelled [E].
- The content key hashes the request bytes. A client that re-batches after a
  restart (`sending_queue.batch` after the queue) defeats it, and central then
  ingests both copies ([`awss3/README.md`](awss3/README.md) §Requirements,
  [`otap-rs/README.md`](otap-rs/README.md) §Remaining gaps).

---

### D4. awss3exporter: stock rejected, patched prototyped, own exporter preferred

**Status:** stock awss3exporter **rejected**; the "commit protocol as a
marshaler" idea **rejected**; patched `awss3inline` is a **working prototype**.
For Go edges the choice between carrying that patch and owning an exporter
(`parquetgo` plus the appender) is **open**, with the README leaning to owning
one ([`awss3/README.md`](awss3/README.md) §Recommendation). For Rust, our own
exporter is built.

**Evidence** ([`awss3/README.md`](awss3/README.md), v0.161.0 source [S] and demos [M], `bd1ae88`):

| Property of stock awss3exporter | Effect |
|---|---|
| key = `…_{randInt()}` or uuidv7, new on **every attempt** | an exporterhelper retry after an ambiguous PUT always leaves a duplicate: 2 objects per slow PUT; 9 of 29 in the demo [M] |
| default key space of about 9×10⁸ | about 290 silent overwrites a year at 1,000 objects/min in one partition [E]; use `uuidv7` |
| partition from `now` at each `Upload` | a retry can land in another time partition |
| marshaler interface `Marshal(pdata) ([]byte, error)` | no context, key, retry flag or upload result; can't set metadata or conditional headers; a separate manifest would be written *before* the data |
| `sending_queue.batch` runs after the queue | batch content, and its hash, changes across a restart |
| `retry_on_failure.max_elapsed_time` default 300 s | the item is **deleted** from the persistent queue after the last failure |

What works: the `parquetencoding/` extension writes ClickStack Parquet with the
manifest fields in the footer, and stock awss3exporter can use it. The
`key_mode: sequence` patch is +110/−2 lines plus a 285-line appender
(`inline/log.go`).

**Consequences.**

- **Go collector config for any of these:**
  - `sending_queue` on `file_storage`;
  - `retry_on_failure.max_elapsed_time: 0`;
  - `timeout` of at least the p99 PUT latency;
  - batch **before** the queue, not with `sending_queue.batch`.
- **Upstreamable, in increasing ambition:**
  1. Content-Type and metadata from encoding extensions;
  2. `if_none_match` plus a content-derived key;
  3. `key_mode: sequence` (U15).

**Open risks.** None in config: `otelcol/config.edge.yaml` now batches
before the queue with the `batch` processor and sets `max_elapsed_time: 0`
and `seal_optimize: false` ([§5](#5-contradictions-and-stale-statements),
item 16).

---

### D5. OTAP variants: OTAP only as an input transport

**Status:** accepted (`2ab9d09`, `4cd7692`).

**Decision.** Store only flat ClickStack Parquet. OTAP is accepted as an input
protocol at a Rust edge. Its tables are never stored for central to join.

**Evidence** ([`otap/README.md`](otap/README.md), Go otel-arrow v0.57.0, per 10k spans; [`otap-rs/README.md`](otap-rs/README.md) §Inputs):

| Option | Edge CPU (traces) | Bytes on S3 | Central CPU vs flat | Verdict |
|---|---|---|---|---|
| a. OTAP star tables as Parquet, joined at central | 144 ms (Go) | 475 KB, 8 PUTs | **3.1–3.6×** | rejected |
| b. flatten OTAP at the edge → ClickStack Parquet | 213 ms (Go, pqarrow); **49 ms (Rust, OTAP input)** | 354 KB (Go) / 131 KB (Rust) | ≈1× | accepted, Rust only |
| c. same rows as Arrow IPC | 121 ms | 1,456 KB (5.5×) | 1.6× | rejected |
| d. raw OTAP IPC payloads, decoded in SQL | 7 ms | 109 KB | **5.2×**, single-threaded only; loses CBOR map bodies (750 of 3,000 logs) | rejected |
| Go: pdata → OTAP conversion alone | +188 ms | – | – | – |
| Rust: OTLP → OTAP, then walk | 71 ms (vs 45 direct) | – | – | rejected: the direct walk is cheaper |

- **Rust inputs** (idle-ish box, 10k items): traces cost 31.3 ms over
  OTLP/HTTP, 33.3 ms over OTLP/gRPC and 42.3 ms over OTAP/gRPC. OTAP costs the
  edge **14–55% more** than OTLP.
- **Correctness:** OTAP input gives the same rows except for order. The
  producer sorts rows and map entries. The hostile datasets **close the whole
  OTAP stream** (U10).

**Consequences.**

- OTAP's gain is on the wire into the edge, not at a ClickStack edge.
- Records from the OTAP receiver need `decode_transport_optimized_ids()`. The
  exporter missed this once, and one metrics batch drove the edge to 13.5 GB
  and the OOM killer.
- Upstream's parquet exporter has no acks and holds batches for minutes. Its
  ClickHouse exporter renders values differently from contrib. Neither is used.

**Open risks.** A poison batch on an OTAP stream stalls a resending client
(U10). The Go OTAP decoder silently drops data (U3); a Go edge must not decode
OTAP through the library unguarded.

---

### D6. No edge-to-central fast path

**Status:** accepted (`bd1ae88`; confirmed by `060e963` and `1b5ce6c`).

**Decision.** Edges talk only to S3. Visibility comes from a short consumer
poll. Edges never insert into ClickHouse.

**Alternative.** After the S3 commit, the exporter also sends the same bytes
once to central with `async_insert`. The importer then waits a grace
`G > D + R + B` and checks the target before inserting.

**Evidence** ([`model/FASTPATH.md`](model/FASTPATH.md)):

- **It is safe in exactly one shape** [Q]: fire after the commit, one shot, a
  strict grace, and a full count check with repair. `fireBeforeCommit`,
  `fastPathRetries`, `noGrace`, `graceNoMargin`, `checkAnyRow`, `tokenOnly`,
  `ledgerCheck`, `markerWait` and `claimsNoCheck` each fail.
- **It saves no central work** (the same insert, moved), and it **doubles edge
  upstream bytes**.
- It needs per-edge ClickHouse credentials, row validation, a pinned
  `async_insert` dedup behaviour, and `G` of about 30–60 s on the slow path.
- **Measured visibility without it:**

| Consumer poll | Visible p50 / p90 | Server CPU per object | Objects per statement | Source |
|---|---|---|---|---|
| 200 ms | 211 / 320 ms (clean: 221 p50) | 22.4 ms | 1.01 | [`otap-rs/README.md`](otap-rs/README.md) §Steady state; [`bench/clean/README.md`](bench/clean/README.md) block 5 |
| 1 s | 663 / 1,061 ms (clean: 666 p50) | 8.9 ms | 3.71 | same |

**Consequences.** Visibility is traded against batching. A 200 ms poll gives
sub-second visibility but one object per statement, at about 9× the server CPU
per object of a 32-object statement.

**Worth adopting regardless, and adopted in [D10](#d10-consumer-multi-object-statements-squashed-to-one-block) and [D11](#d11-consumer-count-check-and-repair-not-dedup-tokens):**

- the projection check;
- the Parquet reader's single-block limits;
- the batch-constant partition key `toDate(received_at)`;
- a pinned `deduplicate_insert = enable`.

**When to revisit.** A sub-second SLO that must hold while the consumer
batches. Even then, an S3-event-driven importer comes first.

---

### D7. Metrics: series-table layout B, not the ClickStack tables

**Status:** accepted as the default (`a8d94ea`, `4cd7692`). The ClickStack
tables (A) remain selectable (`metrics_layout: clickstack_tables`).
**Per-epoch series ordinal: rejected** (`ad77824`). BYTE_STREAM_SPLIT, no
statistics, and gauge+sum merged into one points table: accepted.

**Decision.** The edge computes a 64-bit series id (xxh3 over a canonical
encoding) and sends:

- narrow per-type points objects (`metrics_number_points` for gauge and sum,
  plus histogram, exponential-histogram and summary points);
- a `metrics_series` object for series not yet announced in the current hour.

A series counts as announced **only after its series object commits**. Central
keeps the maps once per series in an `AggregatingMergeTree`. Compatibility
views return exactly contrib's `otel_metrics_*` rows.

**Evidence:**

| | A: ClickStack tables | **B: series table** | Source |
|---|---|---|---|
| central insert µs/point (idle box) | 4.47 | **1.18** | [`bench/clean/README.md`](bench/clean/README.md) (`c9376ac`) |
| central insert µs/point (loaded, same data) | 7.85 + 18 ms/object | 1.15 + 16 ms/object | [`metrics-layout/README.md`](metrics-layout/README.md) (`a8d94ea`) |
| merge µs/point at 10⁴ parts (idle) | 19.4 | **4.1** | `bench/clean` block 3 |
| stored B/point | 26.4 | **6.7** (+38.4 B per series row) | `bench/clean` block 4 |
| Rust edge µs/point, fleet data | 5.04 | **1.76** | [`otap-rs/README.md`](otap-rs/README.md) (`4cd7692`) |
| Parquet B/point on the wire, Rust writer | 19.8 | **18.1** after the wire encodings (−8%) | `otap-rs` §Wire size (`ad77824`) |
| objects per request | 5 | 4, plus a series object on 0.8% of requests | same |
| mid scenario, central vCPU | 263 (5 shards × 2) | **91** (2 × 2) | calculator ([§3](#3-current-sizing-summary)) |

- **Rejected alternatives** ([`metrics-layout/README.md`](metrics-layout/README.md)):
  - B-central, with the id computed in ClickHouse: 24 µs/point, worse than A.
  - ClickHouse TimeSeries (experimental): 60 µs/point, and floats only.
  - Prometheus TSDB: 4.2 µs and 7.2 B native, but no HyperDX.
- **Wire-size ordinal:** a per-epoch u32 in place of the 8-byte id saves about
  40% of wire bytes (−8.0 B/point on the fleet). It costs **+43% central
  statement CPU** at 1 object per statement (+11.6 ms) and +26% at 32. It also
  makes points lanes depend on the series lane: a lost mapping stalls a lane
  for good. Wire bytes into S3 cost nothing per byte, and central CPU is what B
  exists to save, so it was rejected.
- **Correctness:** Rust = Go prototype, id for id, on 1.32 M fleet rows.
  Views = contrib rows on testgen, hostile and duplicate-key data (230 PASS).
  The consumer soak had 0 points without a series row.

**Consequences.**

- **HyperDX degrades through the views** ([risk 4](#4-open-risks-and-unknowns-ranked)):
  - its chart SQL is 12–18% cheaper;
  - resource-attribute filters cost 1.6–2.1× A;
  - the metric-name picker scans: 1.26 s against 0.08 s;
  - rollup acceleration does not apply;
  - nothing can write through the views, so every metric must arrive on the
    edge path.
- **Late series:** a point shows with empty maps for seconds (`ANY LEFT JOIN`).
- **The 64-bit id:** about a 3% chance of any collision among 10⁹ series ever
  seen [E]. A collision merges two series' attributes.
- **Long retention** needs a coarse time bucket in B's sort key [E, not
  measured].

---

### D8. Consumer: leases and checkpoints on S3

**Status:** accepted (`1b5ce6c`).

**Decision.** A lane is one producer's signal namespace. Workers share lanes
through S3 objects written with conditional requests:

- `lease/{producer}/{signal}.json`: CAS'd `{owner, epoch, beat, ttl_ms}`. The
  epoch is a fencing counter, +1 per change of owner.
- `ckpt/{producer}/{signal}.json`: CAS'd `{lease_epoch, version, floor, epochs}`.
- `workers/{w}.json`: a heartbeat, for the fair share.

Taking a lane rewrites its checkpoint first, so every later CAS by the old
holder fails. Expiry is judged on the observer's own monotonic clock. Load is
balanced as ⌈lanes / live workers⌉.

**Alternatives.** A catalog database; Keeper/etcd; a single consumer. The
S3-native design already had a CAS'd lease and checkpoint; this generalises it
to a fleet of workers.

**Evidence** ([`otap-rs/README.md`](otap-rs/README.md) §Consumer):

- **30-minute chaos soak:** 3 edges behind fault proxies and 3 workers on 21
  lanes. There were 29 worker SIGKILLs, 35 pauses past the lease and 30 edge
  SIGKILLs. 77,284 objects in 1,089 epochs. **Missing 0, partial 0, duplicated
  0, uncommitted 0.** 34 checkpoint CASes were lost to a new holder.
- **The first soak failed on liveness:** 43 committed batches were orphaned.
  A paused worker skipped its own expired leases while the others sat at their
  fair share. It is fixed, with a regression test. **The model missed it**,
  because quint-connect drives `coord`, not `Worker::balance`.

**Consequences.** Consumer workers are stateless and use S3 as the only
coordination store. S3 cost: one LIST per held lane per poll, plus one
checkpoint CAS per lane that advanced.

**Open risks.**

- **LIST cost at fleet scale:** 2.6 M LISTs per lane per month at a 1 s poll,
  about $13 per lane [E]. Thousands of lanes need S3 event notifications, which
  are not built.
- **Fairness** counts lanes, not load.
- The consumer's own S3 faults were injected in in-memory tests only.

---

### D9. Consumer: time bound on inserts plus a server-side deadline

**Status:** accepted (`1b5ce6c`).

**Decision.** A statement starts only if `now + budget ≤ safe_until` for every
lane in it, and runs with `max_execution_time = budget`. The holder's window
counts from when it *sent* the lease write: `sent + ttl − margin`, so it closes
2 × margin before anyone may take over. The statement also carries a fence
evaluated on ClickHouse's clock:

```sql
WHERE now64(3) <= fromUnixTimestamp64Milli(sent_wall + ttl − margin − budget)
```

A statement that arrives late, for example after a GC pause or a SIGSTOP, is a
no-op and opens no objects.

**Why.** S3 CAS cannot fence a side effect in ClickHouse. Without the bound,
the model's `noTimeBound` mutant lets a paused worker's checked statement land
after the new holder inserted the same batch (`atMostOnce` ✗)
([`model/S3NATIVE.md`](model/S3NATIVE.md) §3; `otap-rs` §Model).

**Evidence.** The soak had 210 lanes lapse on their own clock and 0
duplicates. The code mutant `no_time_bound` is caught at trace 3, step 35.

**Consequences.**

- The server-side half needs the worker's and ClickHouse's **wall clocks
  within the margin**.
- **On a replicated central, one Keeper request can outlive the time limit by
  `operation_timeout_ms` (10 s).** Production settings are therefore a TTL of
  30 s and a margin of at least 10 s ([`central-replicated/README.md`](central-replicated/README.md) §A time-bound caveat).
- The soak used a TTL of 6 s and a margin of 1 s.

---

### D10. Consumer: multi-object statements squashed to one block

**Status:** accepted (`1b5ce6c`). This supersedes FASTPATH's "one object per
insert, token as backstop" importer shape.

**Decision.** Per table, across a worker's lanes, the consumer issues:

```sql
INSERT … SELECT …, transform(_path, …) FROM s3('…/{k1,…,k32}')
```

- A statement holds up to **32 objects, 16 MB and 200k rows**. An object over
  100k rows or 8 MB goes alone.
- Squashing is on, so a statement is **one part per partition** and atomic.
- `insert_deduplication_token` = a hash of the ordered key list, with
  `deduplicate_insert = enable` and `deduplicate_insert_select = force_enable`
  pinned.
- The table is partitioned by `toDate(received_at)`, which is constant per
  batch.

**Evidence** ([`otap-rs/README.md`](otap-rs/README.md) §Batching; [`bench/clean/README.md`](bench/clean/README.md) block 2):

| Objects per statement | Server CPU per small object (loaded) | Fixed ms per object (idle box) |
|---|---|---|
| 1 | 19.6 ms | 12–20 (`fixedMs` = 15.3) |
| 8, squashed | 4.3 ms | – |
| 32, squashed | **2.4 ms** | **about 2** |
| 32, one part per object | 9.0 ms | – |

- A full-key glob `s3('…/{k1,k2}')` issues **no LIST**.
- **Big objects:** a 150,000-point object (158 MB decoded) split into two
  blocks at a varying row, even with the single-block settings. A retry then
  re-inserted the tail, 3,477 rows in one run. Nothing of 100k points or fewer
  split in 60+ inserts ([`parquetgo/README.md`](parquetgo/README.md) §Correctness; U12).

**Consequences.**

- The token only covers an exact retry of the same statement. Exactly-once
  comes from D11.
- Batching needs several objects pending per table per poll. At a 200 ms poll
  with 7 lanes, statements averaged 1.01 objects. Get batching with more lanes
  per worker, a longer poll, or a linger, which is not built.

---

### D11. Consumer: count check and repair, not dedup tokens

**Status:** accepted (`bd1ae88` for the design, `1b5ce6c` for the
implementation).

**Decision.**

1. **Before inserting,** one projection query per table, `content_key IN (…)`
   against an aggregating projection `by_content: content_key → count()`.
   Present objects are skipped: cross-epoch copies and retries.
2. **After the statement,** including after `KILL QUERY … SYNC` if the answer
   was lost, the same check verifies each object:
   - complete → done;
   - missing → re-insert one by one;
   - partial → row repair with `row_ordinal NOT IN …`;
   - more rows than committed → report `over_count`, never fix silently.
3. The checkpoint moves only past the verified prefix.

**Why not tokens.**

- The dedup window is a count (non-replicated: `non_replicated_deduplication_window`,
  default 0, **no** `_seconds` variant). Crashes and evictions defeat it (F4)
  [Q, M].
- In 26.10 `INSERT … SELECT` is **not deduplicated without a token**, and
  block ids depend on how a statement groups objects [M].
- Token dedup breaks when blocks form differently. Default Parquet chunking
  duplicated a 64,000-row batch (128,000 rows) [M] ([`model/FASTPATH.md`](model/FASTPATH.md) §4).

**Alternatives rejected** [Q, M] ([`model/FASTPATH.md`](model/FASTPATH.md) §2):

- a ledger MV: it fires on deduplicated attempts too, and it lags when the MV
  fails (8,000 rows in the target, none in the ledger);
- S3 ack markers;
- claims;
- tokens only;
- "any row present": it loses partial batches.

**Evidence.**

| Check | Cost | Source |
|---|---|---|
| projection, one batch, 10M-span table | 12.0 ms, 220 rows, 6.9 KB read | [`model/FASTPATH.md`](model/FASTPATH.md) §4 |
| projection, 100 batches in one query | 0.5 ms per batch | same |
| raw envelope scan, 100 batches | 544 ms, 99 MB | same |
| consumer check, 1 or 32 keys | under 1 ms | [`otap-rs/README.md`](otap-rs/README.md) §Batching |
| consumer check on replicated tables whose parts moved to S3 | 11 ms CPU and 5 S3 GETs | [`central-replicated/README.md`](central-replicated/README.md) |

The soak skipped 357 copies by the check, with `over_count` 0.

**Consequences.**

- Projections make lightweight `DELETE`/`UPDATE` throw unless
  `lightweight_mutation_projection_mode` is set. They are rejected on
  `ReplacingMergeTree` by default.
- The series lane has no check. Re-inserting into the `AggregatingMergeTree`
  is idempotent.
- **The check reads every part's projection, cold parts included.** It needs a
  partition predicate: `toDate(received_at) >= oldest pending − 1`. **This is
  not implemented** ([`central-replicated/README.md`](central-replicated/README.md) §Two further findings).

---

### D12. Consumer: GC and checkpoint compaction

**Status:** accepted (GC `1b5ce6c`; compaction `9f2c75d`).

**Decision.**

- **GC** (`consume gc`) runs separately and is safe to run concurrently.
  - Each run appends every lane's checkpoint position, with a timestamp, as a
    mark to the CAS'd `gc.json`.
  - It deletes data slots below the newest mark that is at least `--delay`
    old. The delay is lease TTL + margin + the longest a PUT can be in flight.
  - A closed epoch, **tombstone included**, is deleted only after `--zombie`,
    a bound on how long a fenced writer can live, and is then recorded as
    *retired*.
- **Checkpoint compaction.** An epoch leaves a lane's checkpoint once it is
  both closed there and retired by GC. A per-lane *floor* bounds discovery.
  Edges now name a lane's epoch at its **first write**, so an idle lane can't
  write under a name below the floor.

**Evidence** ([`otap-rs/README.md`](otap-rs/README.md) §Checkpoint compaction):

| 15-minute soak, an edge restart every 2–5 s | before | after |
|---|---|---|
| epochs made | 2,471 | 2,542 |
| checkpoint entries per lane (max) | 30 → 185, growing linearly | **13–31, flat** |
| largest checkpoint | 10,634 B, growing | 1,864 B |
| `gc.json` | 1.83 MB, growing | 197–228 KB, flat |
| exactly-once | PASS | PASS |

- Model mutants:
  - `gcTombs`: deleting a tombstone early lets a zombie re-create the slot;
  - `earlyCompact`: loses a late batch;
  - `floorOnly`: a plain low watermark is pinned by a gap and unbounded.
- The 30-minute soak ran 344 GC runs with 0 CAS conflicts.

**Consequences.** The bound in production is about 1–2 entries per lane at a
30 s quiet time, a 10-minute zombie bound and one restart an hour.

**Open risks** ([risk 6](#4-open-risks-and-unknowns-ranked)):

- **Compaction waits for GC.** If `consume gc` stops, checkpoints grow again.
- `gc.json` is one object of size marks × lanes × entries; thousands of lanes
  want it sharded.
- A producer clock that steps back by more than about the zombie bound, or a
  writer that outlives the zombie bound, can leave a batch **uningested,
  never duplicated**. Nothing watches for this.

---

### D13. Replicated central: plain ReplicatedMergeTree, no zero-copy

**Status:** accepted (`1a88da1`). **Zero-copy is rejected.** That commit's
title, "replicated central with zero-copy replication on S3", names the
experiment, not the decision; the verdict in
[`central-replicated/README.md`](central-replicated/README.md) (which now
says so under its title) rejects it.

**Decision.**

- **Replication:** ReplicatedMergeTree with 2 replicas, a 3-node Keeper, and
  **each replica keeps its own S3 copy** of the cold tier (`tiered_own`).
- **`allow_remote_fs_zero_copy_replication`: rejected.**
- **Consumer flags:**
  - `--sync-replica`: before a check that may follow statements committed on
    another replica, run `SYSTEM SYNC REPLICA <t> LIGHTWEIGHT`;
  - `--no-ddl`, so a missing replicated table never silently becomes a local
    one;
  - `--ch r1,r2` for failover.

**Evidence** ([`central-replicated/README.md`](central-replicated/README.md)):

| | Zero-copy | Plain, a copy per replica |
|---|---|---|
| tier status in 26.10 | **Experimental**: "not ready for production" | GA |
| orphaned blobs after faults | 4,120 (20 MB, 23% of live) in a 15-min chaos soak; 26% garbage at the end | 0 |
| other leaks | 137 Keeper lock nodes; 626 `ignored_` detached parts, one pointing at 24 deleted blobs | none |
| copies of cold data | 1 | 2 |
| lifecycle of 10 parts: PUT / GET | 377 / 1,449 | 754 / 2,100 |
| Keeper transactions, same lifecycle | 1,165 | 545 |
| merge CPU on S3 | 2.6 s on one replica | 2.5 s + 2.5 s |

**Check consistency** [M]:

| Setting | Effect on the post-insert check |
|---|---|
| **with** `--sync-replica`, 15-min soak (13 replica kills, 6 Keeper node kills, 3 quorum losses) | both replicas identical, exactly once; 815 syncs failed and deferred their check |
| **without** it, 5-min control | **7 batches duplicated**: they arrived by replication after the check |
| `select_sequential_consistency = 1` without quorum inserts | does nothing: r2 still answered 0 |
| `insert_quorum_parallel = 0` | makes it work, but serialises writers, and is refused with `async_insert = 1` |
| `SYSTEM SYNC REPLICA … LIGHTWEIGHT` | p50 24 ms, 1 ms CPU, 4 Keeper transactions |

- Replicated dedup works across replicas: the hashes live in Keeper.
- Replicated insert CPU measured **58.6–66.7 µs/row** under a load average of
  26–35, at 7.9 objects per statement. Treat that as an upper bound; the
  single-node figure is about 12 µs/row.

**Consequences.**

- Cold S3 bytes and PUTs double, and each replica merges its own S3 parts.
- The lease margin must be at least the Keeper operation timeout
  ([D9](#d9-consumer-time-bound-on-inserts-plus-a-server-side-deadline)).
- `insert_quorum` is a **durability** choice, not an exactly-once one. With 2
  replicas, an acked batch sits on one replica's local disk until the other
  fetches it. **Open:** use quorum 2 of 3 replicas, or accept that window.

---

### D14. Storage tiers

**Status:** hot then cold, `toDate(received_at)` partitions and drop-only TTL
are **accepted**. The **cold medium is open**: local HDD (the calculator's
default) or S3 with a copy per replica (the replicated recommendation).

**Decision and evidence:**

| Aspect | Choice | Evidence |
|---|---|---|
| hot | fast disk, 1–7 days (calculator default 1) | requirement |
| move to cold | TTL MOVE, day-granular | about 1.3 ms CPU/MB to local disk, about 15 ms/MB to S3 [E by difference]: 0.01× and about 0.1× insert CPU ([`bench/merges/README.md`](bench/merges/README.md) §TTL costs, `a4cec67`) |
| `move_factor` | **0** on tiered policies | the default 0.1 on a disk over 90% full moved **every new part** to the cold volume, and merges there cost +19% |
| expiry | `ttl_only_drop_parts = 1`, day-granular | ≈ 0 merge CPU; a row-level TTL costs 0.24× insert |
| partition key | `toDate(received_at)` | makes every batch one part and every insert atomic ([`model/FASTPATH.md`](model/FASTPATH.md) §6) |
| hourly partitions | not adopted | −17–20% merge CPU at 1,100 parts, for 24× the partitions |
| cold copies | a copy per replica ([D13](#d13-replicated-central-plain-replicatedmergetree-no-zero-copy)) | one copy needs zero-copy, which is rejected |

**Cost of the cold medium at the mid scenario** (calculator, 2 replicas; list
prices [E]):

| Cold tier | Storage, all copies | Storage $/month |
|---|---|---|
| local HDD (default) | 992 TB | $45.2k |
| S3, a copy per replica | 992 TB | **$23.7k** |
| S3, one copy (zero-copy; rejected) | 504 TB | $12.5k |

**Open risks.**

- At the calculator's own prices, the default (local HDD) costs about 1.9× S3
  per replica. The default deserves a decision.
- With cold on S3, the consumer's check reads cold projections
  ([D11](#d11-consumer-count-check-and-repair-not-dedup-tokens)).

---

### D15. Metrics downsampling

**Status:** **proposed**. It is modelled in the calculator (on by default:
14 raw days, then 5-minute rollups) and prototyped as SQL. No materialized view
is wired into the consumer's DDL or `otap-rs/sql/`.

**Decision (proposed).** An `AggregatingMergeTree` rollup per series and
5-minute window (min, max, sum, count, last), fed by a materialized view and
kept for the whole retention. Raw points are kept 14 days.

**Evidence** ([`metrics-layout/README.md`](metrics-layout/README.md) §Downsampling, loaded box):

| Rollup | Build µs per raw point | Stored B per raw point |
|---|---|---|
| B, number | 0.41 | 0.94 |
| B, histogram (`argMaxState`, cumulative) | 1.71 | 2.7 |
| A, number | 8.8 | 4.2 |
| A, histogram | 11.9 | 6.2 |

The calculator charges 1 µs and 18 B per series window for B.

- **At the mid scenario:** 91 vCPU and 992 TB with rollups, against 85 vCPU and
  1,068 TB keeping all raw data.

**Consequences.**

- HyperDX queries the raw tables. Its rollup acceleration looks for
  MaterializedView engines on the source table and does not apply to views
  [D]. **Dashboards over older data must be rebuilt on the rollups.**
- The series table must outlive the points: TTL on `LastSeen`.

**Open risks.** Not built, and not re-measured on an idle box. Which
dashboards are acceptable at 5-minute resolution is undecided.

---

### D16. Edge sorting off; service-affine routing at N ≥ 8

**Status:** accepted (`7125a72`). Sorting code is in `otap-rs`, off by default
(`parquet.sort: {by: none}`). Routing is a recommendation for the gateway's
loadbalancing exporter. It was measured on generated routed objects, **not
deployed**.

**Evidence** ([`bench/sorting/README.md`](bench/sorting/README.md), idle box, 5 reps):

| Option | Edge CPU | Object size | Central insert | Read saving, 1 service × 1 hour |
|---|---|---|---|---|
| sort, 1 row group | +20–22% | −4 to −6% | 0% (traces) to −13% (logs) | **none** on ClickHouse's default `s3()` read path, which fetches ~1 MB objects whole |
| sort, 4 range row groups | +23–26% | −6 to −7.5% | – | 2× with ranged IO forced; 4–6× once footers are cached |
| sort, 16 row groups | +50–59% | +4 to +8% | +10–32% | only with cached footers |
| **route by service,** unsorted | 0 | 0 | 0 | **3–20×** fewer bytes: 1.94 → 0.65 GB/h at N = 3; 5.01 → 0.31 GB/h at N = 16 |

- **Sorting at the mid scenario:** about **+1.2 vCPU** at the edge for about
  0.1–0.3 vCPU saved at central. It is not the "about 1 ms" lake/DESIGN.md first
  assumed.
- **Routing's cost is load skew.** The busiest publisher carries 36% of a
  cluster at N = 8 (2.9× the mean) and 26% at N = 16 (4.1×).
- The fixed central cost grows with N either way: 1.2 → 9.6 vCPU at one object
  per statement, and 0.17 → 1.34 vCPU at 32.

**Consequences.**

- Routing only matters if raw Parquet is queried, that is, in the lake option
  ([D17](#d17-lake--hybrid-cold-tier)). With central ingest alone, raw objects
  are deleted after ingest.
- Keep 32 objects per statement, and don't raise N without them.

**Step 0 of the same work** (`0a04d57`): the 18–33% edge slowdown reported by
bench/clean block 1 was **a harness artefact, not a regression**.

---

### D17. Lake / hybrid cold tier

**Status:** **exploratory.** Research only; nothing built or measured
([`lake/DESIGN.md`](lake/DESIGN.md), `1a72daf`, updated in `7125a72`).

**Proposal.**

- ClickHouse keeps 1–7 days hot.
- A stateless **lake compactor**, the consumer's twin with the same lease and
  checkpoint code, rewrites each signal-hour into large Parquet files sorted by
  ClickStack's key, in two levels.
- The files are committed as **Iceberg v2** with create-only
  `vN.metadata.json`, so no catalog server is needed.
- HyperDX reads a hot ∪ cold view.
- Dashboards over old data come from edge-derived span metrics.
- Later (P2), an hourly and daily trace_id → file maplet.

**Estimates (all [E]):**

- compactor 15–20 vCPU;
- about 400 TB for 90 days, against 504 TB (one-copy cold) or 992 TB (the
  default);
- a 24-hour service search in 1–4 s;
- a 30-day trace lookup in 0.3–1 s with the maplet.

**Rejected within the note:**

- per-object edge sidecar indexes: every object holds most services;
- token blooms at the edge;
- a table format over raw slots: 7.8 M files a day;
- Tempo's schema, which is not HyperDX's;
- Quickwit, whose metastore we would have to adopt;
- DuckLake, which needs a SQL catalog.

**Risks named.** ClickHouse Iceberg pruning bugs (time zone #119173;
UUID/FLBA #118371, #120986); `version-hint` write bugs; predicate pushdown
through the `UNION ALL` view is unverified; content-key dedup would live in two
places.

**Note.** The calculator's lake mode modelled something different: raw edge
objects kept, served "through sidecar indexes and edge-built cubes", with no
compactor cost. This was fixed in the calculator, v10
([§5](#5-contradictions-and-stale-statements), item 19).

---

### D18. S3 client and credentials

**Status:** accepted (`a34a48f`, `1a927f6`, `4cd7692`).

**Decision.**

- **Go:** aws-sdk-go-v2 everywhere, replacing minio-go. It provides typed
  `IfNoneMatch`/`IfMatch` and a typed 412, the full credential chain, and the
  same stack contrib links. The switch costs +28 KB of binary; the credential
  chain costs +1.7 MB.
- **Rust:** object_store 0.13.2, plus this crate's `creds.rs`:
  - `credential_process`;
  - shared profiles;
  - AssumeRole chaining;
  - `AWS_CA_BUNDLE`;
  - SigV4.
- **chDB and ClickHouse:** their own AWS chain with empty keys.

**Evidence, against local stand-ins for STS, the Pod Identity agent, IMDS and
a private-CA TLS proxy** [M] ([`parquetgo/README.md`](parquetgo/README.md) §Credentials; [`otap-rs/README.md`](otap-rs/README.md) §Credentials):

| Mode | Go (SDK) | Rust (object_store + `creds.rs`) | chDB / ClickHouse 26.10 `s3()` |
|---|---|---|---|
| EKS IRSA | ✓ | ✓; STS must be https | ✓; the STS endpoint is hard-coded and `AWS_ENDPOINT_URL_STS` is ignored |
| EKS Pod Identity | ✓, refresh 5 min before expiry | ✓ | ✓ |
| Nutanix: static keys, path-style, private CA | ✓ `CABundle` / `AWS_CA_BUNDLE` | ✓ `ca_bundle` / `SSL_CERT_FILE` / `AWS_CA_BUNDLE` | ✓ keys; CA via `SSL_CERT_FILE` or `<openSSL><client><caConfig>`; **`AWS_CA_BUNDLE` ignored** |
| Roles Anywhere, `credential_process` | ✓ | ✓ (`creds.rs`; object_store has none) | **✗**: ClickHouse removed the process provider |
| Roles Anywhere, `aws_signing_helper serve` | ✓ | ✓ (maps `AWS_EC2_METADATA_SERVICE_ENDPOINT`) | ✓ |
| SSO profiles | supported by the SDK chain [D], not tested | refused, with a message | – |

**Consequences.**

- The central server needs `s3_allow_server_credentials_in_user_queries = 1`
  for the ingest user before keyless `s3()` works (26.10 default 0, Code 497).
  The alternative is `extra_credentials(role_arn = …)`.
- `SSL_CERT_FILE` *replaces* the default bundle. Point it at system roots plus
  the private CA.
- For Nutanix, consider `AWS_REQUEST_CHECKSUM_CALCULATION=when_required`: some
  S3-compatible stores reject the SDK's default CRC32 [D].
- Under an expired credential a writer gets 403 and must **keep the append
  unresolved**; a 403 says nothing about an earlier timed-out attempt
  ([`model/S3NATIVE.md`](model/S3NATIVE.md) §9).
- SigV4 needs a wall clock within 15 minutes.

**Open risks.** No real AWS, EKS, STS or `aws_signing_helper` was used. The
stand-ins issue empty session tokens, because SeaweedFS rejects foreign ones,
so the token header was never exercised against a store.

---

### D19. Durable buffer at the edge

**Status:**

- **Go:** accepted. The collector's `sending_queue` on `file_storage` plus
  `retry_on_failure.max_elapsed_time: 0` gives at-least-once delivery across
  restarts ([`awss3/README.md`](awss3/README.md) §2).
- **Rust:** ack-after-commit is the baseline. Upstream's durable buffer
  (Quiver) is **built and tested but opt-in** (`configs/edge-durable.yaml`).
  Which is the default is **open**.

**Evidence** ([`otap-rs/README.md`](otap-rs/README.md) §Edge durability, `4cd7692`):

| | `edge.yaml` (ack after commit) | `edge-durable.yaml` (Quiver) |
|---|---|---|
| client ack | after the S3 commit, 36 ms | after the WAL write, **19 ms** |
| edge CPU per 10k-span request | 31.7 ms | **42.7 ms (+35%)** |
| local disk written per request | 0 | 6.3 MB (the 3.2 MB request, twice) |
| S3 down: acked before SIGKILL → committed after restart | nothing acked; the client keeps the data | **12 of 12** |
| exposure | none at the edge | a host crash loses the last **≤ 25 ms** of acked requests (the WAL fsync interval, not configurable) |

**Consequences.** Without the buffer, an S3 outage pushes back to the clients
(503) and custody stays with them. With it, custody moves to the edge's disk:
size `retention_size_cap` for the outage to ride out, at about 630 B per span.

**Open risks.** Disk-full behaviour and a real power cut are untested.

---

### D20. PBT defect fixes in chdbexporter

**Status:** findings 1–3 **fixed** in `chdbexporter/publish.go` (`00be8f2`);
findings 4–7 **open** ([`PBT.md`](PBT.md)). The chDB exporter is no longer
the chosen publisher ([D1](#d1-edge-publisher-rust-otap-dataflow-exporter-go-parquetgo-not-chdb)),
so 4–7 matter only if chDB is kept for local-SQL edges.

hegel-go v0.9.8 state-machine tests drive the real publisher over a
fault-injecting fake session, in `go.pbt.mod` so the purego alpha stays out of
production builds (`38c641e`). They rediscovered the model's F1–F3 in real
code, and found:

| # | Defect | Status |
|---|---|---|
| 1 | a restart within a generation reused the previous epoch's tables (`CREATE … IF NOT EXISTS`, names without the epoch) | fixed: table names carry the epoch. Still open: a predecessor's tables stay attached |
| 2 | the generation came from a clock read taken before the lock; a clock step back, or a stale read, reopened a sealed generation | fixed: the clock is read under the lock and generations only move forward |
| 3 | a failed seal or DETACH was never retried | fixed: an `unsealed` list retried by the sweep and by close. Still open: a crashed epoch's generations stay unsealed |
| 4 | `insert_format: json` rejects timestamps before 1973-03-03 or after 2262 (whole batch, `CANNOT_PARSE_DATETIME`) | open; use RowBinary |
| 5 | the manifest's `min_event_time` treats 0 as unset; times ≥ 2⁶³ ns wrap to 1677 | open |
| 6 | a TTL under 1 s passes `Validate` and renders `toIntervalSecond(0)` | open |
| 7 | `generation: 1ns` passes `Validate` in Parquet-only mode | open |

The same work checked real publisher runs against `edgePublish.qnt` through
quintgo, both ways: publisher traces are validated against the model, and
model traces drive the publisher. 20 of 20 end states agree, and the
manifest-before-Parquet mutant is caught.

---

## 3. Current sizing summary

The mid scenario, as the calculator
(`scratchpad/central-sizing.html`, read-only; its `compute()` evaluated
unchanged) computes it with its current constants.

**Inputs:**

- fleet and rates as in [§1.2](#12-fleet-and-rates-per-region);
- metrics in layout B;
- 90 days retained, 1 day hot, cold on local HDD;
- rollups on: 14 raw days, 300 s windows;
- 2 replicas of 32-vCPU nodes, 4 GB RAM per vCPU;
- headroom 1.75×, query load 75% of ingest.

**Result: 91 vCPU in 2 shards × 2 replicas (4 nodes of 32 vCPU), and about
992 TB over 90 days.**

| Output | Value |
|---|---|
| rows / s | 2.07 M (600k spans, 200k logs, 1.27 M points) |
| insert vCPU per replica | 5.7 (of which fixed per-object 0.15) |
| merge vCPU per replica | 13.3 |
| headroom vCPU per replica | 14.2 |
| query vCPU (spread over replicas) | 24.8 |
| **vCPU per replica / total** | **45.6 / 91.1**: 2 shards of 32, about 71% of the provisioned 64 |
| compressed TB/day, one copy | 5.92 |
| hot tier, all copies (+25% merge room) | 15.3 TB |
| cold tier, all copies | 977 TB |
| **storage, all copies** | **992 TB** |
| storage $/month (local HDD / S3 per replica) | $45.2k / $23.7k |
| edge objects per day | 7.8 M (S3 PUTs ≈ $1.2k/month) |
| edge CPU, region (Rust / Go) | 5.3 / 7.3 vCPU |
| with every constant at the low / high end of its spread | 78–95 vCPU ([`bench/clean/README.md`](bench/clean/README.md)) |
| same scenario, metrics layout A | 263 vCPU (5 × 2), 1,117 TB |
| same scenario, no downsampling | 85 vCPU, 1,068 TB |
| before the idle-box re-measurement | 115 vCPU, 991 TB |

**Constants and their provenance:**

| Constant | Value | Provenance |
|---|---|---|
| `usRow`: insert µs per span or log | 3.43 [2.87–3.96] | **idle box**, `bench/clean` block 2: all-in CPU of one-object, 10k-row statements (earlier, loaded box: 5) |
| `usPointB` / `usPointA`: insert µs per metric point | 1.18 / 4.47 | **idle box**, block 2. The A value is partly an encoder change: Rust objects, where the earlier value came from Go objects |
| `fixedMs`: fixed ms per inserted object | 15.3 [11.6–18.6] | **idle box**, block 2: one-object statements |
| `mergeRow`, `mergePointB`, `mergePointA`: merge µs | 10.1 / 4.1 / 19.4 | **idle box**, block 3, **projected** to 10⁴ parts per daily partition from runs that reached 161–2,100 parts. The projection is 5–60× beyond the parts measured |
| `bPointB`, `bPointA`, `bSeries`: stored bytes | 6.7 / 26.4 / 38.4 | **idle box**, block 4, `OPTIMIZE FINAL`, no-replay pool. The data is synthetic (±50% for a real fleet [E]) |
| `edgeGoSpan`, `edgeGoLog`, `edgeRsSpan`, `edgeRsLog`: edge µs | 6.81 / 4.76 / 4.0 / 2.94 | **idle box**, block 1 |
| `edgePointB`, `edgeRsPointA`: edge µs/point | 1.82 / 5.05 | **idle box**, the `bench/sorting` bisect at head (18.2 and 50.5 ms per 10k). bench/clean's block 1 values of 2.06 and 6.20 were a harness artefact |
| `edgeGoPointA` | 7.47 | **idle box**, block 1 |
| `rollUsB`, `rollUsA`, `rollWinB`, `rollWinA`: rollups | 1 µs / 10.5 µs / 18 B / 50 B | **loaded box**, metrics-layout. Not re-measured |
| `bPqPointB`, `bPqPointA`: Parquet B/point | 24 / 38 | **loaded box**, parquet-go-era objects. The Rust objects are 18.1 / 19.8 |
| `bSpan`, `bLog`: stored bytes | 80 / 60 | **[E]**. Synthetic data stored 9.6–39 B per span and 19 per log; not used |
| `bPq`: Parquet bytes per span or log | 50 | [E] |
| headroom, query load | 1.75×, 75% | [E] |
| hot-disk merge room (`HOT_SLACK`) | 1.25 | [E], a code constant |
| prices: PUT, GET, disk, S3 | $0.005 and $0.0004 per 1k; $0.08, $0.045, $0.023 per GB-month | [E] list prices |
| fleet and rates | [§1.2](#12-fleet-and-rates-per-region) | [E], apart from the fleet shape |

**What moves the answer most.** Merges are 29% of per-replica CPU and rest on
an extrapolation. Headroom and query load are 58% of the total and are pure
estimates. The storage total rests on `bSpan` and `bLog`: spans and logs are
about 88% of the daily bytes in layout B, and both constants are estimates.

---

## 4. Open risks and unknowns, ranked

Ranked by how much of the design fails or changes if the risk lands, then by
how likely it is.

| Rank | Risk | Why it matters | What would retire it |
|---|---|---|---|
| 1 | **Nutanix Objects conditional writes** | The commit protocol ([D3](#d3-commit-protocol-manifest-less-create-only-slots)) and the consumer's leases, checkpoints and `gc.json` ([D8](#d8-consumer-leases-and-checkpoints-on-s3), [D12](#d12-consumer-gc-and-checkpoint-compaction)) all need atomic `If-None-Match: *` and `If-Match` on single-part PUT, plus read-after-write HEAD/LIST. The docs are behind a login and nothing public says. A store that ignores the header looks correct until two writers race; MinIO before 2024-09-13 and Garage are examples. The fallbacks are a Keeper/etcd `Coordinator` for the control plane (designed, not built) or closing epochs by time (not modelled). | Run `awss3/probe` and `s3cas` (`TestCreateRace`, `TestCASRace`, the ambiguous-resolution tests) against a real bucket, with the private CA and static keys. Add a startup self-test: two create-only PUTs of a scratch key, and refuse to run unless the second gets 412. |
| 2 | **Real per-pod rates and data shape** | Every rate in §1.2 is an estimate. `bSpan` and `bLog` (80 / 60 B) drive about 88% of stored bytes. Synthetic compression is ±50%. Real fleets carry 20+ resource attributes; the synthetic data carries 12–21. Series churn is untested. | Sample a real cluster's spans/s, logs/s and series per pod, and the stored bytes per row after merges. |
| 3 | **Real AWS behaviour** | Everything ran on SeaweedFS on localhost: no latency, and no 409 `ConditionalRequestConflict`, which object_store retries. Per-prefix request limits (3,500 PUT/s per prefix), LIST pricing at thousands of lanes, real STS and session tokens were not exercised. Lane throughput is 1 / (encode + PUT): about 20 batches/s locally, lower with real latency. | A soak on a real bucket under IRSA and Pod Identity: commit latency, 409 rates, LIST cost per lane. |
| 4 | **HyperDX compatibility with the series layout** | HyperDX was never run live. Its SQL came from its own test snapshots at `hyperdx@885d30c`. Known degradations: the metric picker scans (1.26 s against 0.08 s); map filters cost 1.6–2.1× A; no rollup acceleration; no writes through the views. HyperDX changes can break the views silently. | Run HyperDX against the views; add the proposed `(MetricName, ServiceName)` helper MergeTree; pin the HyperDX version. |
| 5 | **Clock assumptions** | (a) The server-side fence needs worker and ClickHouse wall clocks within the lease margin. (b) Checkpoint compaction assumes no producer clock steps back by more than about the zombie bound, since epochs are named by wall-clock ms. (c) Replicated central: a Keeper operation can run 10 s past `max_execution_time`, so margin ≥ 10 s. (d) SigV4 fails beyond 15 min of skew (a stall, not corruption). Lease expiry itself uses monotonic clocks and is safe. | NTP monitoring with alerts tighter than the margin; an occasional unbounded listing to detect an epoch below a floor (not built). |
| 6 | **GC dependence** | GC is what bounds S3 storage, checkpoint size and `gc.json`. If it stops, compaction stops and checkpoints grow. Its safety rests on two bounds: the PUT lifetime (`--delay`) and the zombie lifetime (`--zombie`). A writer that outlives the zombie bound can re-create a deleted slot; such a batch is never ingested (not duplicated). `gc.json` is one object sized marks × lanes × entries. | An alert on GC lag; shard `gc.json` per lane; enforce the zombie bound (pod termination grace plus kill). |
| 7 | **Merge CPU extrapolation** | Merges are 29% of central CPU per replica, projected to 10⁴ parts from runs of 161–2,100 parts. The fits are within −2 to +18% when fitted on ≥ 300 parts, and off by ±27% on 100–130. Random-id traces borrow another run's slope. | A day-long run at production statement sizes. |
| 8 | **Consumer check reads the cold tier** | The projection check touches cold parts on S3: 11 ms CPU and 5 GETs per check in the replicated soak. At 90 days that is every cold partition. | Add the partition predicate to the check (not implemented). |
| 9 | **Replicated insert cost** | 58.6–66.7 µs/row measured on replicas (loaded box, 7.9 objects per statement), against about 12 on one node. If even part of that is real, the calculator is low. | Re-measure replicated inserts on an idle box at 32 objects per statement. |
| 10 | **Content key against re-batching** | The content key hashes the request. A collector that re-batches after a restart produces new keys, and central ingests both copies. | Batch before the queue; never use `sending_queue.batch` in front of these exporters. (`otelcol/config.edge.yaml` fixed 2026-09-26.) |
| 11 | **Large objects and single-block inserts** | Above about 100k points (158 MB decoded) ClickHouse split objects nondeterministically. The consumer caps statements at 200k rows and 16 MB and sends big objects alone, so the verify-and-repair path is what keeps them exact. | Keep edge batches at 10k rows; report U12. |
| 12 | **Pinned ClickHouse behaviour** | Dedup defaults changed across versions: `deduplicate_insert`, `async_insert_deduplicate`, `deduplicate_insert_select`. Parquet reader chunking changes block formation. Everything was measured on 26.10.1.618 only. | Pin the settings in the consumer (done for two of them) and re-run the correctness and fault suites on every upgrade. |
| 13 | **Rust upstream maturity** | otap-dataflow is pre-1.0, pinned at `5db8358` plus 2 patches. The OTAP receiver closes a whole stream on a poison batch. The build needs a pinned 577 MB toolchain. | Upstream the patches (U2, U11); track releases. |
| 14 | **Edge durability window** | With Quiver, a host crash can lose ≤ 25 ms of acknowledged requests. Disk-full behaviour is untested. | Decide the Rust default ([D19](#d19-durable-buffer-at-the-edge)); test `backpressure` and `drop_oldest`. |
| 15 | **Series id collisions** | 64-bit: about 3% chance of any collision among 10⁹ series ever seen [E]. A collision merges two series' attributes. | Accept, or move to 128 bits: +0.03 B/point stored, +8 B/point of Parquet [E]. |

---

## 5. Contradictions and stale statements

These were the places where the READMEs were stale or contradicted each other
at `7125a72`. **All were resolved on 2026-09-26** in the source files, by a
corrected sentence or a short dated "superseded" note where the historical
text still has value; recorded results and logs were not edited. Paths are
relative to `otel-chdb/`. The move from oscope (`ce95da6`) rewrote every
commit hash; the hashes in this file and in UPSTREAM_ISSUES.md have been
mapped to this repository's history. Hashes in other READMEs, commit
messages and recorded results still name the original oscope commits.

| # | Was stale or conflicting | Status (files changed) |
|---|---|---|
| 1 | README.md §Publishing: manifests announce batches; "consumer and catalog aren't built yet" | fixed: superseded note (create-only slots, consumer built) (`README.md`) |
| 2 | README.md, bench/central/REPORT.md: Parquet costs "4.7" S3 writes per batch, native "11×" | fixed: 4.7 labelled as chDB's counter; the proxy saw 2 PUTs, Rust makes 1 (`README.md`, `bench/central/REPORT.md`) |
| 3 | model/README.md: `non_replicated_deduplication_window` "or `_seconds`" | fixed: no `_seconds` variant (`model/README.md`) |
| 4 | replicated check needs `select_sequential_consistency` / `insert_quorum`; replicated central untested | fixed: needs `SYSTEM SYNC REPLICA … LIGHTWEIGHT`; tested (`model/README.md`, `model/FASTPATH.md` A5 and §7, `model/S3NATIVE.md`, `otap-rs/README.md`, `bench/central/REPORT.md`) |
| 5 | model/README.md: Go code "isn't linked to the model" | fixed (`model/README.md`); quintgo's stand-in statement was true, a pointer to PBT.md added (`../quintgo/README.md`) |
| 6 | model/S3NATIVE.md §8: adopt the log and fence, drop manifests | fixed: superseded note, log kept for native tables only (`model/S3NATIVE.md`) |
| 7 | otap/README.md: building otap-dataflow "not practical" | fixed: dated note, built in 11 min (`otap/README.md`) |
| 8 | otap/README.md: for Rust, adapt upstream's ClickHouse exporter plus a manifest | fixed: superseded notes in the short answer and the recommendation (`otap/README.md`) |
| 9 | otap-rs/README.md: METRICS_SCHEMA.md "still says" pdata order, `dt` slips | fixed: marked as corrected there since (`otap-rs/README.md`) |
| 10 | otap-rs/README.md summary: layout B "no smaller on the wire" | fixed: 8% smaller, 18.1 against 19.8 B/point (`otap-rs/README.md`) |
| 11 | metrics-layout/README.md: B cuts insert CPU "7×", wire "1.6×" | fixed: dated note, 3.8× idle, 8% against the Rust writer (`metrics-layout/README.md`) |
| 12 | metrics-layout/README.md, model/FASTPATH.md: one exact-key object per insert | fixed: superseded notes, 32-object squashed statements and `content_key` check (`metrics-layout/README.md`, `model/FASTPATH.md` §1 step 4 and §6) |
| 13 | metrics-layout/README.md: announce invariant "[E, not written]" | fixed: written, `announcedOnlyAfterCommit` / `announceEarly` (`metrics-layout/README.md`) |
| 14 | metrics-layout/README.md: gauge+sum merge, exemplar `FilteredAttributes` not built | fixed: built in Rust, on by default (`metrics-layout/README.md`) |
| 15 | parquetgo/README.md, otap-rs/README.md: calculator's 2.5 µs / 10 B / 2 µs per point | fixed: notes give the current per-layout constants (`parquetgo/README.md`, `otap-rs/README.md`) |
| 16 | otelcol/config.edge.yaml: `seal_optimize: true`, post-queue `sending_queue.batch`, default `max_elapsed_time` | fixed: `seal_optimize: false`, `batch` processor before the queue, `retry_on_failure.max_elapsed_time: 0`; `batchprocessor` added to the ocb build; passes `otelcol-chdb validate` (`otelcol/config.edge.yaml`, `otelcol/builder-config.yaml`, `README.md`) |
| 17 | bench/clean/README.md: Rust metrics edge 18–33% slower, "merits a look" | fixed: harness artefact, calculator uses 1.82 / 5.05 (`bench/clean/README.md`); commit `c9376ac`'s message can't change |
| 18 | `central-sizing.html`: series edge constant "Go encoder; Rust not built" | fixed outside the repo: fixed in the calculator, v10 |
| 19 | `central-sizing.html`: lake mode via "sidecar indexes and edge-built cubes" | fixed outside the repo: fixed in the calculator, v10 |
| 20 | bench/merges/README.md: calculator models merges as 1.5× insert | fixed: dated note, per-row constants (`bench/merges/README.md`) |
| 21 | lake/DESIGN.md: 115 vCPU, 5.9 vCPU edge, 4.5 µs/span, 530 TB one-copy baseline | fixed: dated notes, 91 vCPU, 5.3, 4.0, compare with 992 TB (`lake/DESIGN.md`) |
| 22 | bench/sorting/README.md: sorting code "(uncommitted)" | fixed (`bench/sorting/README.md`) |
| 23 | awss3/README.md: `aws_signing_helper serve` "[E]" | fixed: measured against an IMDS stand-in (`awss3/README.md`) |
| 24 | parquetgo/README.md, METRICS_SCHEMA.md: metrics commit object then manifest; inline lanes "not wired" | true of `parquetgo`, and now a recorded gap: D1 keeps the Go path, with this as follow-up work (note in `parquetgo/README.md`; METRICS_SCHEMA.md already describes both designs, unchanged) |
| 25 | PBT.md: "New, outside what the model can express:" before "Findings 1–3 are fixed" | fixed: heading moved to the list it introduces (`PBT.md`) |
| 26 | commit `1a88da1` titled "replicated central with zero-copy replication on S3" | cannot change history; documented (`central-replicated/README.md` note under the title; D13 status) |

---

## 6. Upstream bugs found

Short drafts, with repro, expected and actual behaviour, and versions, are in
[UPSTREAM_ISSUES.md](UPSTREAM_ISSUES.md). **None has been reported yet.**

| # | Project | Issue | Severity for us | Local mitigation |
|---|---|---|---|---|
| U1 | parquet-go v0.32.0 | `Writer.Reset` empties every column chunk's `path_in_schema` after the first file | medium: retries weren't byte-identical | `restoreColumnPaths` via reflection; guarded by a test |
| U2 | otel-arrow (Rust) `5db8358` | OTAP views return no attributes for span events and links (`Dictionary(UInt16, UInt32)` parent ids) | high for OTAP input | patch 0002 |
| U3 | otel-arrow (Go) v0.57.0 + contrib `otelarrowreceiver` v0.161.0 | the consumer ignores the error from `RelatedDataFrom`; invalid UTF-8 in a map attribute silently drops the batch, and the receiver acks | high: silent data loss | don't decode OTAP through the Go library on the edge path |
| U4 | SeaweedFS 4.47 (master `635f69a`) | conditional `CompleteMultipartUpload` checks outside the lock: several winners | high only if multipart is used | single-part PUT only |
| U5 | arrow-go v18.7.0 | V1 data pages split a repeated column mid-row while a page index is written; ClickHouse rejects the file | medium | V2 pages; use parquet-go |
| U6 | otel-arrow (Go) v0.57.0 | the producer writes nulls into a non-nullable IPC field (empty event names) | low (option d rejected) | – |
| U7 | otel-arrow (Go) v0.57.0 | the producer drops attributes whose value is Empty | low | – |
| U8 | otel-arrow (Go) v0.57.0 | the consumer returns nested map keys in random order | low | order-preserving CBOR decoder in `otap/cbor.go` |
| U9 | otap-dataflow parquet exporter | resource/scope id offsets skip the root's struct fields (from source reading; needs a repro) | none (not used) | – |
| U10 | otap-dataflow OTAP receiver | one undecodable batch closes the whole stream instead of a NACK | medium for OTAP input | – |
| U11 | otap-dataflow `pdata` | depends on all of DataFusion for two types | low (build size) | patch 0001 |
| U12 | ClickHouse 26.10.1 | large Parquet objects split into 2 blocks at a varying row despite single-block settings | medium | ≤ 100k-row objects; verify and repair |
| U13 | ClickHouse 26.10 / chDB 26.7 | no `credential_process`; IRSA STS ignores `AWS_ENDPOINT_URL_STS`; `AWS_CA_BUNDLE` ignored | medium for Roles Anywhere | `aws_signing_helper serve`; `SSL_CERT_FILE` |
| U14 | object_store 0.13.2 | `with_client_options` resets `allow_http`; no `credential_process`; IMDS variable name; https-only STS | low | `creds.rs` |
| U15 | contrib awss3exporter v0.161.0 | retries use a new random key; no conditional write; encodings can't set Content-Type or metadata | high for stock use | own exporter or patch |
| U16 | chdb-go `9f8e35a` | `Session.Query` truncates at NUL, so binary inserts are impossible; the CLI ignores `-path` | low now | fork; `patches/0001` |
| U17 | SeaweedFS 4.47 | `If-Match` on a missing key → 412 (AWS: 404); `StartAfter` naming a "directory" returns nothing | low | slot-0 key `{epoch}/0` |
| U18 | Quint Rust evaluator v0.6.0 | about 40% of `--mbt` traces label state 0 `step`, not `init` | low | driver maps `step` → `init` |
| U19 | ClickHouse 26.10.1 | `ParquetMetadata` omits footer key-value metadata | low | HEAD for `x-amz-meta-*` |
