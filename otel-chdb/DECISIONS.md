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
 │ pods → gateway collectors (3/cluster)│  1 create- │ {root}/{cluster}/{producer}/{signal}/     │
 │   Rust otap-dataflow + s3pq exporter │  only PUT  │  {epoch}/{seq:020d}.parquet (FORMAT.md)   │
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
| [D1](#d1-edge-publisher-rust-otap-dataflow-exporter-go-parquetgo-not-chdb) | Edge publisher: Rust otap-dataflow exporter where it can run, Go `s3pq` (parquetgo) for Go collectors, not chDB | accepted; **Go path kept** (2026-09-26); its gaps closed the same day (s3pq: manifest-less for every signal, layout B, row-identical with Rust through the consumer) |
| [D2](#d2-transfer-format-parquet-read-with-s3-not-native-parts) | Transfer format: Parquet read with `s3()`, not native parts on `s3_plain_rewritable` | accepted; traces/logs land in ClickStack 2.39.1's DDL minus the four mapKeys indexes (option 2, 2026-09-27): insert 2.4–2.6×, merges 2.0× the old tables |
| [D3](#d3-commit-protocol-manifest-less-create-only-slots) | Commit protocol: manifest-less create-only slots | accepted; manifests and the S3-native log superseded |
| [D4](#d4-awss3exporter-stock-rejected-patched-prototyped-own-exporter-preferred) | awss3exporter: stock rejected, patched version prototyped, own exporter built | stock rejected; **own exporter `s3pq` built and deployed (2026-09-26)**; awss3inline superseded |
| [D5](#d5-otap-variants-otap-only-as-an-input-transport) | OTAP: only as an input transport; never stored | accepted |
| [D6](#d6-no-edge-to-central-fast-path) | No edge-to-central fast path | accepted |
| [D7](#d7-metrics-series-table-layout-b-not-the-clickstack-tables) | Metrics: series-table layout B; wire-size ordinal rejected | accepted; built in both edges (Go: 2026-09-26) |
| [D8](#d8-consumer-leases-and-checkpoints-on-s3) | Consumer: leases and checkpoints on S3 | accepted; idle-lane LIST backoff, load-based balancing (2026-09-26); event notifications designed, not built |
| [D9](#d9-consumer-time-bound-on-inserts-plus-a-server-side-deadline) | Consumer: time bound plus a server-side deadline | accepted; margin ≥ 20 s (TTL 75 s) enforced, unanswered statements and error answers that may still commit waited out (2026-09-26, after the replicated run measured commits 19 s past the time limit) |
| [D10](#d10-consumer-multi-object-statements-squashed-to-one-block) | Consumer: multi-object statements squashed to one block | accepted; linger built, off by default (2026-09-26) |
| [D11](#d11-consumer-count-check-and-repair-not-dedup-tokens) | Consumer: count check and repair, not dedup tokens | accepted; the check reads the batch's partitions ± a copy horizon (2026-09-26), 3 days, with a horizon audit reporting late copies; edge replays keep `received_at` (2026-09-27), so the horizon now covers sender resends only |
| [D12](#d12-consumer-gc-and-checkpoint-compaction) | Consumer: GC and checkpoint compaction | accepted; GC keeps the slot below the position (2026-09-26) |
| [D13](#d13-replicated-central-plain-replicatedmergetree-no-zero-copy) | Replicated central: plain ReplicatedMergeTree, zero-copy rejected, sync before checks | accepted; the fleet-scale consumer run on it (2026-09-26/27): exactly once, margin raised to 20 s, audit syncs; replicated DDL now ClickStack's with the rollups, derived from the consumer's (2026-09-27); insert CPU paid once per shard, +9% per statement (2026-09-27) |
| [D14](#d14-storage-tiers) | Storage tiers: hot 1–7 days, then cold | accepted; cold medium open |
| [D15](#d15-metrics-downsampling) | Metrics downsampling: 5-minute rollups | proposed; not built |
| [D16](#d16-edge-sorting-off-service-affine-routing-on-at-n--8) | Edge sorting off; service-affine routing at N ≥ 8 | accepted; routing built as `deploy/components/routing`, measured locally, not deployed |
| [D17](#d17-lake--hybrid-cold-tier) | Lake / hybrid cold tier | exploratory |
| [D18](#d18-s3-client-and-credentials) | S3 client and credentials | accepted |
| [D19](#d19-durable-buffer-at-the-edge) | Durable buffer at the edge | accepted: Go persistent queue; Rust Quiver on in the deployed publisher (`backpressure`); `received_at` = entry into the buffer, kept across replays (2026-09-27); **retention bounds custody age, not cap ÷ rate** (model, 2026-09-27) |
| [D20](#d20-pbt-defect-fixes-in-chdbexporter) | PBT defect fixes in chdbexporter | 1–3 fixed; 4–7 open |
| [D21](#d21-entity-catalog-resource_id-at-the-edges-announcements-in-the-data-object) | Entity catalog: `resource_id` at both edges, announcements in the data object | **built** (2026-09-28): `resource_id` on every trace/log row and the announcement lane; central keeps `ResourceAttributes` (the schema switch and the rewrite proxy not decided) |
| [D22](#d22-query-service-sql-rebuilt-from-the-tree-scope-as-table-filters-labels-on-every-result) | Query service: OIDC + audit, SQL rebuilt from the tree, scope as `additional_table_filters`, `complete_through` on every result, a presigned lake plan | **first slice built** (2026-09-28, [`query/`](query/README.md)): central queries and lake plans for both UIs; the UIs and the alert evaluator are not wired yet |

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

**CI (2026-09-27)** runs these checks, not only the author's box
([`../ci/README.md`](../ci/README.md)): `ci.yml` on every push (`go vet`
and `go test -race` in the fast Go modules, clippy, the Rust unit tests
with the consumer's ClickHouse/S3 tests, and the Rust integration tests
`determinism`, `otap_view`, `metrics`, `series`); `nightly.yml` daily
(conformance for both layouts and `go_faults.sh`, `faults.sh` and a
consumer soak, the three quint-connect MBT suites at seed 0x5eed, the
model checks through `ci/model-check.sh` and `parquetgo/modelcheck`, and
the chDB tests). A CI run is on SeaweedFS and a single ClickHouse in
Docker, like the [M] here: it guards against regressions, it does not
retire the risks of §4.

| Model | Invariants the design must keep | Checked | Checked in code |
|---|---|---|---|
| `model/edgePublish.qnt` (manifests; now superseded) | `commitImpliesData`, `onlyCommittedIngested`, `batchIngestedAtMostOnce`, `payloadIngestedAtMostOnce`, `sealMatchesManifests` | 5,000 × 40-step simulation; Apalache ≤ 8–10 steps ([`model/README.md`](model/README.md), `1307816`) | quintgo conformance and model-seeded PBT ([`PBT.md`](PBT.md), `38c641e`) |
| `model/partLifetime.qnt` (native parts; now moot) | `noReadOfDeleted`, `noLeakAfterExit`, `noDoubleCount`; the rule **`old_parts_lifetime` > max query + refresh interval** | Apalache ≤ 10–12 steps | server test `TestOldPartsLifetimeProtectsServerQueries` (`2f1f3c2`) |
| `model/s3Inline.qnt` (**the commit protocol in use**) | `payloadIngestedAtMostOnce`, `epochNoDuplicatePayload`, `onlyCommittedIngested`, `noCommitLost`, `ackedImpliesCommitted`, `noPayloadLost`, `gapNeverTakenForLoss`, `consumerNeverSkipsCommitted`, `noCommitAfterClose`; since 2026-09-27 module `s3InlineReceived`: `replayKeepsReceivedAt` (every copy carries its custody day) and at-most-once under a check limited to HORIZON days | 3,000 × 80 steps, two seeds; Apalache ≤ 6 steps (8 partial); 6 mutations caught ([`awss3/README.md`](awss3/README.md), `0855334`); `s3InlineReceived` 3,000 × 80 at HORIZON 1 and 0, mutant `restampOnReplay` caught (`484f22f`) | `tests/mbt_s3inline.rs`: 300 traces, 16,981 steps; 3 code mutants caught ([`otap-rs/README.md`](otap-rs/README.md), `060e963`) |
| `model/s3InlineMetrics.qnt` | `reqAckedImpliesAllCommitted`, `noObjectLost` (a request is acked only when all its objects commit) | 3,000 × 60; mutant `ackOnAny` caught | `tests/mbt_s3inline_metrics.rs`: 300 traces, 23,968 steps |
| `model/s3InlineConsumer.qnt` | `atMostOnce`, `onlyCommittedIngested`, `neverSkipsCommitted`, `noCommitAfterClose`, `announcedOnlyAfterCommit`; since 2026-09-26 over release, the commit slack, daily partitions and the check's range; the horizon audit's `auditSilent` (designs) and `dupAudited` (`noHorizon`) | 5,000 × 60 on five design instances; mutants by simulation `noTimeBound`, `noVerify`, `gcTombs`, `announceEarly`, `releaseInFlight`, `errorSettles`; by scripted counterexample runs `releaseInFlight`, `keeperOverrun`, `noHorizon`, `wallRange`, `gcReopens`, `errorSettles` ([`otap-rs/README.md`](otap-rs/README.md) §Consumer at fleet scale). **2026-09-27, custody day:** an object's received day is its request's custody day (`pDay`, stamped once per incarnation); new instance `noHorizonReplays` (HORIZON 0, edge replays only) passes safety and `auditSilent`; `noHorizon` now needs sender resends; new mutant `restamp` (the edge before `8efc34f`) breaks `atMostOnce` by a scripted run; all verdicts as expected except that random simulation (20,000 × 60, seed 0x5eed) found none of the six mutants that need about a dozen specific steps (`releaseInFlight`, `keeperOverrun`, `errorSettles`, `noHorizon`, `restamp`, `gcReopens`), each caught by its scripted run (`otap-rs/results/consumer/custody/model.txt`); quint-connect on all five instances passes after the change (`custody/mbt.txt`) | `tests/mbt_s3inline_consumer.rs`; code mutants `no_time_bound`, `no_verify` (`1b5ce6c`), `release_in_flight`, `wall_range` |
| `model/s3InlineConsumerCompact.qnt` | the above plus `neverSkipsCommittedCompact`, `noCommitBelowFloor`, `floorSound`, `viewFloorSound`, `bounded` | 5,000 × 60; `compactBound` 20,000 × 150; mutants `earlyCompact`, `floorOnly` | code mutant `early_compact` (`9f2c75d`) |
| `model/fastPath.qnt` | `onlyCommittedIngested`, `batchIngestedAtMostOnce`, `noLostBehindCheckpoint` | 1,500 × 40, 23 scenarios; Apalache ≤ 10 steps ([`model/FASTPATH.md`](model/FASTPATH.md), `bd1ae88`) | not applicable: not built ([D6](#d6-no-edge-to-central-fast-path)) |
| `model/s3Native.qnt` (superseded) | 13 invariants, including `noWriteFromFencedWriter`, `gcKeepsLiveData`, `nsSingleWriter` | 3,000 × 120; Apalache ≤ 6 steps ([`model/S3NATIVE.md`](model/S3NATIVE.md), `30210a5`) | `s3cas` protocol tests only |
| `model/completeness.qnt` (open scenario: `complete_through` and the alert evaluator; [research §5.4](research/central-optional.md#54-complete_through-a-watermark-for-deterministic-alerts)) | `completeSound` (every request with `received_at` below the published watermark is ingested), `evalWithinComplete`, `okMeansNoErrors`, `noSilentOk`, `resultLabeled`, `wmBounded` | 20,000 × 80; 11 witnesses reached; mutants `lastReceived`, `listTimeIdle`, `maxNotPrefix`, `noBirth`, `evalPastComplete`, `noDataOk`, `unlabeledFallback` caught by simulation and by scripted runs; `noHeartbeat` safe but stalls every window (scripted) (`cadd7a1`). **Finding:** §5.4's rules are unsound (an open requirement, recorded under [D19](#d19-durable-buffer-at-the-edge)) | not built |
| `model/entityCatalog.qnt` (open scenario: [`entities/`](entities/README.md), STPA LS-5) | `noPermanentOrphan`, `announcedAfterCommit`; `exactAtQuery` (controller up, G ≥ D + LAG); `exactAfterLag` (announcements in the data lane) | 20,000 × 60 on four instances (`sameObject`, as built, since 2026-09-28); mutants `noAnnounce`, `announceEarly`, `sameObjectEarly`, `shortGrace`. **Finding:** a separate announcement lane closes only the permanent gap: rows can land before their announcement (`exactAfterLag` fails); announcements ahead of their rows in the data lane bound the gap to the dictionary lag | **built** ([D21](#d21-entity-catalog-resource_id-at-the-edges-announcements-in-the-data-object)): `tests/dst/fleet.rs` checks `sameLane` (no row lands before its resource's announcement) and announcements exactly once under the fault menu (`hegel_dst` rule `announce`); `entities/scripts/announce_e2e.py` (a pod the controller never saw exact 34.7–59.1 s after its rows, with `LIFETIME(MIN 30 MAX 60)`) |
| `model/retention.qnt` (open scenario: STPA LS-10, R-S6) | `acceptedVisible` (an acked request is never inserted into an expired partition), `noEdgeDrop`, `custodyAgeBounded` | 20,000 × 50; 6 witnesses; mutants `retentionShort`, `cutDuringOutage`, `capSized` (retention sized as cap ÷ rate) caught; `dropOldest` keeps `acceptedVisible` and breaks `noEdgeDrop`. **Finding:** D19's retention rule was wrong ([D19](#d19-durable-buffer-at-the-edge)) | not applicable (a sizing rule) |
| `model/sealer.qnt` (open scenario: the lake's snapshot log, [research §5.1](research/central-optional.md#51-the-sealer-is-a-consumer-group-with-a-table-log-sink)) | `monotone`, `repeatableAsOf`, `atMostOncePerSnapshot`, `neverSkipsSealed`, `wmSound` | 20,000 × 40; 5 witnesses; mutants `blindCommit`, `noRebase`, `wmFromList`, `noDedup` (`abe3e97`) | not built |

The four open-scenario models run from `model/open_models.sh` (quint 0.32,
Rust evaluator), a row per check with its expected verdict: every row as
expected at seed 0x5eed (`abe3e97`; the script has 82 rows, the commit
message counts 86) [Q]. 2026-09-28: the `entityCatalog` rows rerun with the
built design (`sameObject`) and its mutant (`sameObjectEarly`): 26 rows,
all as expected; the script now has 89 rows. They are sampled simulations
and scripted runs of small instances, not proofs; only `entityCatalog`'s
ingest rule is mirrored in code (`tests/dst/fleet.rs` `sameLane`), and CI's
`ci/model-check.sh` runs `consumer_model.sh`, not this script.

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
- **A commit lands by fence + budget + slack, and the margin covers the
  slack** (`SLACK ≤ MARGIN`; the worker refuses to start otherwise, [D9](#d9-consumer-time-bound-on-inserts-plus-a-server-side-deadline)).
- **A worker acts on a lane only once its statements are gone:** verify,
  retry and release wait out an unanswered statement, and (2026-09-26) an
  error answer that may come with a commit still resolving: "answered" is
  not "gone" on a replicated central ([D9](#d9-consumer-time-bound-on-inserts-plus-a-server-side-deadline)).
- **The slack is real:** on a replicated central a commit landed 19.0 s
  past a 10 s time limit [M], so slack and margin are 20 s (the Keeper
  session timeout minus the budget; [D9](#d9-consumer-time-bound-on-inserts-plus-a-server-side-deadline)).
- **A copy of a request is received within the check's horizon of its
  original** (3 days; `HORIZON ≥ MAX_DAY` in the model, [D11](#d11-consumer-count-check-and-repair-not-dedup-tokens)).
  **Edge-buffer replays meet it by construction since 2026-09-27:**
  `received_at` is the time the request entered the edge's durable
  custody and a replay keeps it ([D19](#d19-durable-buffer-at-the-edge)),
  so the replay is in its original's partition whatever the outage
  (`replay_received.sh`, a 4-day outage: 3/3 replays skipped; the old
  binary: 3 copies ingested twice, 3 audit WARNs) [M]; in the consumer
  model `noHorizon` (HORIZON 0) holds for edge replays and fails only for
  sender resends [Q]. **The residual:** new custody of the same bytes (a
  sender that resends after its own long outage, or to another
  publisher). The consumer can't enforce it, so it watches for it: the
  horizon audit reports every copy ingested twice because of it
  (`auditLate` in the model; `consumer_late_copies_total`).
- **A bounded custody age** (2026-09-27, `model/retention.qnt`): the TTL
  by `received_at` exceeds the longest time a request stays in an edge's
  custody (backlog + outage with its flaps + drain), not the buffer's cap
  ÷ its rate ([D19](#d19-durable-buffer-at-the-edge)). The edge can't
  enforce it; it is a sizing rule plus an alert on the oldest request in
  custody.
- **Small domains and bounded depth.** A ✓ in simulation is not a proof.
  Apalache goes to 6–12 steps only.

---

## 2. Decisions

### D1. Edge publisher: Rust otap-dataflow exporter, Go parquetgo, not chDB

**Status:** accepted: chDB rejected for publishing; Rust where the Rust engine
can run; Go where the edge must stay a Go collector. **The Go path is kept**
(decided 2026-09-26 by the project owner), and since the same day the Go edge
is the `s3pq` exporter (`parquetgo/s3pqexporter` on `parquetgo/edge`), which
publishes what the Rust edge publishes.

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
- **Go `s3pq` (2026-09-26), loaded box:** 43 / 29 ms per 10k spans / logs
  (same run: manifest publisher 40 / 28); 39 ms per 10k metric points in
  layout B against 64 ms for the ClickStack tables with manifests; one
  create-only PUT per object (metrics: 4 PUTs against 10); objects 26–28%
  larger than Rust's, same rows (`parquetgo/compare/results/edge-bench.md`).
- **Go `s3pq` = Rust through the consumer** (`conformance/`, 2026-09-26):
  layout B 228 checks, ClickStack tables 187, 0 failures, on testgen, nasty,
  duplicate-key and a new hostile set (unicode, 1 MiB values, 1,000-entry
  maps, out-of-range enums, extreme histograms, exemplars); same content
  keys, metadata, footers and schema. One known difference, from the Rust
  side: span kinds outside the enum (Rust `Unspecified` via otap-dataflow's
  view; Go and contrib `''`).

**Consequences.**

- There are two edge implementations that must stay row-identical. Keep the
  server-side conformance tests in CI: `conformance/run.sh` (both edges
  through the Rust consumer, both layouts), `conformance/go_faults.sh`,
  `parquetgo/modelcheck` (Go runs against `s3Inline.qnt` /
  `s3InlineMetrics.qnt`), `otap-rs/scripts/correctness.py`. The arrow-go page
  split ([UPSTREAM_ISSUES.md](UPSTREAM_ISSUES.md) U5) and parquet-go's
  untruncated statistics (a 1 MiB value put 2 MiB into the footer, fixed by
  cutting them to 64 bytes) show that writer drift is a real risk.
- The Rust build pins upstream otel-arrow at `5db8358` plus two local patches.
  It needs Rust 1.98.1 (577 MB of toolchain), 395 crates, and an 11-minute
  clean release build ([`otap-rs/README.md`](otap-rs/README.md) §Build).
- The saving is not where the money is. 28 ms saved per traces batch is about
  1.4 cores fleet-wide at 500 producers [E]. Central dominates cost.

**Is the Go path frozen? Decided 2026-09-26: no. The Go path is kept, and
its gaps are closed** (same day). The project owner decided to keep Go as a
supported edge for collectors that must stay Go. The Go edge is now the
`s3pq` exporter (built into `otelcol-s3pq`, `awss3/collector/builder-config.yaml`;
deployed by `deploy/base/go`):

- **Manifest-less for every signal:** `parquetgo/commit` ports the Rust lane
  (`PUT If-None-Match: *` at `{root}/{cluster}/{producer}/{namespace}/{epoch}/{seq:020d}.parquet` (format v2),
  HEAD on a 412 or no answer, epochs named at the first write, tombstone
  halts) with the Rust keys, epoch names, `x-amz-meta-oscope-*` metadata and
  BLAKE3 content keys; a metrics request is acked only when every object has
  committed.
- **Layout B:** `parquetgo/series.go` promotes `metrics-layout/seriesenc`
  (identical to it in prototype mode) with the Rust defaults (merged number
  points, exemplar attributes, BYTE_STREAM_SPLIT, no statistics); series
  announced only after the series object commits.
- **Checked:** row-identical with Rust through the consumer
  (`conformance/`); the Rust fault scenarios pass through the Go edge
  (`conformance/go_faults.sh`); real Go runs conform to `s3Inline.qnt` (30
  runs, 1,558 steps, lane state checked per step) and `s3InlineMetrics.qnt`
  (30 runs, 748 steps), and the mutants RetryNewKey, NoHalt and AckOnAny are
  rejected (`parquetgo/modelcheck`); the deployed config survives a SIGKILL
  with every request once (`deploy/results/go-edge-s3pq.txt`).
- **What stays different:** Go objects are 26–28% larger (parquet-go's zstd
  and V2 pages); the ClickStack layout walks the request once per type
  (87 ms per 10k points, slower than the old 64 ms; layout B is the
  default); a request not in Go's canonical protobuf encoding gets a
  different content key than in Rust. The consumer is Rust-only and serves
  both edges. A Go edge's durable buffer is the collector's persistent queue
  ([D19](#d19-durable-buffer-at-the-edge)), and sorting is off everywhere
  ([D16](#d16-edge-sorting-off-service-affine-routing-on-at-n--8)).

**Open risks.** otap-dataflow is pre-1.0. Its OTAP receiver closes the whole
stream on one undecodable batch (U10). Edges that must be `otelcol-contrib`
builds can't use Rust. The Go edge links parquet-go v0.32.0, whose writer
`Reset` needs a workaround (`TestReusedWriterIdentical`) and which writes a
column and offset index where the Rust layout-B objects have none.

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
- **The central tables (2026-09-27, option 2; `e784242`, `3c5985e`):**
  traces and logs are ClickStack 2.39.1's DDL minus the four text indexes
  on `mapKeys()` (`idx_res_attr_key`, `idx_span_attr_key`,
  `idx_scope_attr_key`, `idx_log_attr_key`), with its key-value rollup.
  HyperDX 2.39.1 live finds map keys through the `*_attr_items` indexes
  instead: all 46 key-discovery statements rewritten that way, the same
  keys, the rest of its SQL identical; the nine scenarios cost 0.12× the
  old tables' server time, the same as the full DDL [M]. The price
  against the pre-alignment tables: insert 2.4–2.6× (full DDL 2.9–3.5×),
  merges 2.0×; stored bytes spans −20 to −25%, logs ±3% [M, loaded box]
  ([`hyperdx/README.md`](hyperdx/README.md) §Schema, §Option 2;
  `hyperdx/results/schema3-*`). Sizing in [§3](#3-current-sizing-summary),
  the insert cost as a risk in [§4](#4-open-risks-and-unknowns-ranked).
  HyperDX's filter panel lists the envelope columns (`producer_id`,
  `content_key`, …) as facets; hiding them (ALIAS or naming) is open.
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
`{root}/{cluster}/{producer}/{signal}/{epoch}/{seq:020d}.parquet` with
`PUT If-None-Match: *`, and its description goes in `x-amz-meta-oscope-*`
and the Parquet footer. The keys, metadata and control objects are
specified once, in [FORMAT.md](FORMAT.md) (format v2).

- **Cluster first (format v2, 2026-09-27).** The key starts with the
  cluster, so write access can be scoped by prefix
  ([D18](#d18-s3-client-and-credentials), STPA R-S7); a lane is
  `{cluster}/{producer}/{signal}`. Version 1
  (`{root}/{producer}/{signal}/…`) is a clean break, not read: no real data
  existed, and a transition reader would keep two namings, two discovery
  walks and two policy sets alive for nothing. `{ctl}/format.json` and
  `oscope-format: 2` on every object guard it (FORMAT.md §5).

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
- **A copy keeps its `received_at` (2026-09-27, `8efc34f`, `1fa3651`).**
  The envelope's `received_at`, which partitions central by day and bounds
  the consumer's check, is when the request entered the edge's durable
  custody: the Quiver WAL write (Rust; patch 0003 keeps it in the segment
  manifest, the exporter reads the pdata's `ingestion_time`) or the Go
  persistent queue's enqueue (`s3pq` stamps client metadata
  `x-s3pq-received-at` before the sending queue; `file_storage` persists
  it). Retries and replays carry it, so a copy committed in a later epoch
  lands in its original's `toDate(received_at)` partition. Without a
  buffer, custody passes at the commit, so it is the exporter's receive
  time. Content keys and rows are unchanged
  ([D19](#d19-durable-buffer-at-the-edge)).
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
marshaler" idea **rejected**; patched `awss3inline` was the working
prototype and is **superseded** (2026-09-26) by our own Go exporter, `s3pq`
(`parquetgo/s3pqexporter`), the README's recommendation
([`awss3/README.md`](awss3/README.md) §Recommendation). A marshaler returns
one byte slice per request, while a metrics request is up to five objects in
five lanes with one acknowledgement, plus a series object whose announcement
waits for its commit. For Rust, our own exporter is built.

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
  - batch **before** the queue, not with `sending_queue.batch`;
  - **and acknowledge only after the batch is persisted (2026-09-27).**
    The `batch` processor is asynchronous: the receiver acks a request as
    soon as the batcher holds it, before the queue write, so a SIGKILL loses
    acknowledged data (kind: 1–5 of 32 requests per kill; locally 10 of 32
    per signal on the Go edge, [`deploy/results/go-batch.txt`](deploy/results/go-batch.txt)).
    So **agents never batch in front of a persistent queue** (no `batch`
    processor, no `sending_queue.batch`; a queue item is the sender's
    request), and **publishers batch after the sender's hop and before their
    own durable buffer, answering each request only once the batch holding
    it is written**: Rust, `processor:batch` in front of Quiver (acks
    propagate after the WAL write); Go, `s3pq`'s own `batch` (10,000 items
    or 1 s, a larger request split deterministically at 10,000; each caller
    returns once the merged request is in the `file_storage` queue).
  - A batch persisted as one queue item replays byte-identical: same
    content key, same `received_at` (the time the merged request was
    enqueued, [D19](#d19-durable-buffer-at-the-edge)). The window it keeps:
    a sender resending a request whose batch was written but whose answer
    was lost puts it in a new batch (milliseconds; 0 duplicates in the
    local kill tests).
- **Upstreamable, in increasing ambition:**
  1. Content-Type and metadata from encoding extensions;
  2. `if_none_match` plus a content-derived key;
  3. `key_mode: sequence` (U15).
- `s3pq` enforces the Go config rules in its validation: it refuses
  `retry_on_failure.max_elapsed_time` ≠ 0 and any `sending_queue.batch`;
  its `batch` block is the batch step before the queue.

**Open risks.** `otelcol/config.edge.yaml` (the chDB edge, retired as a
publisher: D1; **marked deprecated** in its header, 2026-09-27, with what a
revival must change) still batches with the `batch` processor before its
queue, so it has the ack-before-persist window above; `max_elapsed_time: 0` and
`seal_optimize: false` are right
([§5](#5-contradictions-and-stale-statements), item 16).

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
  Go (2026-09-26): the promoted encoder (`parquetgo/series.go`) writes the
  prototype's objects value for value in prototype mode, and the Rust edge's
  rows, id for id, with its defaults, through the consumer (`conformance/`,
  `otel_metrics_series` 11,539 rows and every points table equal).

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

**Status:** accepted (`1b5ce6c`); idle-lane backoff and load-based
balancing added (`f923fa8`, `11b479d`, 2026-09-26).

**Decision.** A lane is one producer's signal namespace. Workers share lanes
through S3 objects written with conditional requests:

- `lease/{producer}/{signal}.json`: CAS'd `{owner, epoch, beat, ttl_ms}`. The
  epoch is a fencing counter, +1 per change of owner.
- `ckpt/{producer}/{signal}.json`: CAS'd `{lease_epoch, version, floor, epochs}`.
- `workers/{w}.json`: a heartbeat, for the fair share.

Taking a lane rewrites its checkpoint first, so every later CAS by the old
holder fails. Expiry is judged on the observer's own monotonic clock.

**Fleet scale (2026-09-26):**

- **Load is balanced by weight,** not lane count: a base per lane plus its
  recent rows/s, exchanged in the heartbeats; the target is a water level
  (a lane heavier than an even split gets a worker to itself), with a
  ±20% band, one release per round, a 30 s minimum hold. `--balance count`
  keeps ⌈lanes / live workers⌉. A release is allowed only once none of the
  lane's statements can still land.
- **Idle lanes back off their LIST:** after 10 s without work, waits of
  1 s doubling to 30 s, jittered; work found puts a lane back on every
  poll. Lane directories are listed every 30 s instead of every 2 s.
- **Event notifications** (S3 → SQS/EventBridge) are designed as hints
  that wake a lane early, with LIST kept as the truth and the 30 s cap as
  the reconciliation; the seam (`discovery::Hints`) and the event parsing
  are built, the SQS client is not.

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
- **Fleet-scale LIST cost** ([`otap-rs/README.md`](otap-rs/README.md) §Consumer at fleet scale,
  `scripts/consumer_scale.sh`): 42 lanes (7 busy, 35 idle), one worker, 1 s
  poll, the previous binary (180 s) against this one (600 s): **2.87 M →
  0.65 M LISTs per lane per month** over all the worker's LISTs [M]; an
  idle lane's own LISTs 2.6 M → 156 k (the 10 s grace and the doubling
  included) and 86 k at the 30 s cap [E from M]; a busy lane's unchanged
  (one per poll); discovery per worker 11.4 M → 2.7 M.
- **Balancing:** a unit test with one 4,000 rows/s lane and eight of 25 on
  three workers settles with the heavy lane alone and the light ones 3–5 per worker,
  and stops moving; the randomized fleet (12 seeds) passes with load
  balancing, backoff and linger on. The model's `releaseInFlight` mutant
  (release with a statement in flight) breaks `atMostOnce`.

**Consequences.** Consumer workers are stateless and use S3 as the only
coordination store. S3 cost: one LIST per busy lane per poll and one per
idle lane per 30 s; per worker, two LISTs per `--discover` and one per
producer every 30 s; one checkpoint CAS per lane that advanced; **and one
lease renewal (CAS) per lane every TTL/3, now the largest cost of an idle
lane: 104 k PUTs a month at the 75 s TTL, about $0.52 (173 k, $0.86, at the
45 s TTL before the replicated run)** [E] (the LIST at the cap is about
$0.43).

**Open risks.**

- LIST pricing was measured locally (counts), not billed on AWS
  ([risk 3](#4-open-risks-and-unknowns-ranked)). Event notifications, which
  would take an idle lane's LIST to near zero, are not built; whether
  Nutanix Objects has them is unknown.
- Balancing uses rows/s as the load; a lane's cost also depends on its
  objects/s (fixed cost per statement). The weights were tested in unit
  tests and the soak, not on a real fleet.
- The consumer's own S3 faults were injected in in-memory tests only.

---

### D9. Consumer: time bound on inserts plus a server-side deadline

**Status:** accepted (`1b5ce6c`); margin ≥ 10 s enforced, unanswered
statements waited out (`f923fa8`, 2026-09-26); **margin ≥ 20 s and error
answers that may still commit waited out** after the replicated run
measured commits 19 s past the time limit (2026-09-26).

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
- **On a replicated central a commit can outlive the time limit by far
  more than one Keeper request** ([`central-replicated/README.md`](central-replicated/README.md)
  §Keeper overrun). The assumption until 2026-09-26 was
  `operation_timeout_ms` (10 s). Measured [M]: when the Keeper node a
  replica's session is on freezes or is partitioned, or the quorum is lost,
  the server's retry loop around the commit runs up to the session timeout
  (30 s) after the statement started, and the part lands when Keeper
  answers: **19.0 s past a 10 s `max_execution_time`** in a directed run,
  11.6 s past a 2 s one (14 of 2,363 statements) in a random mix of Keeper
  faults; every statement had answered within 29.0 s of its start. **Such
  a statement answers `TIMEOUT_EXCEEDED`** and lands all the same.
- **Enforced:** the worker refuses to start with a margin below 20 s (it was
  10 s), or below `--keeper-slack` (20 s), and on a replicated central with a
  slack below the replicas' Keeper session timeout minus the budget (read
  from `system.zookeeper_connection`), naming the reason and the fix;
  `--allow-short-margin` is for tests. A statement lands by
  `sent + ttl + slack` on the worker's clock (fence, the server clock up to
  a margin behind, budget, slack), and nobody takes over before
  `sent + ttl + margin`, hence `margin ≥ slack`. The model's commit slack
  (`cApply` by fence + BUDGET + SLACK) and its mutant `keeperOverrun`
  (SLACK > MARGIN breaks `atMostOnce`) record it.
- **Defaults now: TTL 75 s, margin 20 s, budget 10 s, slack 20 s**
  (budget + 2 × margin + TTL/3 ≤ TTL), `consume gc --delay` 115 s. A crashed
  worker's lanes are taken over after 95 s. History: TTL 30 s / margin 2 s
  (the code), "TTL 30 s and margin ≥ 10 s" (this section, which never fit a
  10 s budget), TTL 45 s / margin 10 s (the fleet-scale round). A shorter
  Keeper session timeout on the replicas would allow a shorter slack.
- **Unanswered statements (a gap, fixed 2026-09-26):** after an insert with
  no answer the worker killed it and verified at once, re-inserting what
  was missing. The KILL can reach the server before the statement, and a
  replicated commit can outlive the worker's HTTP timeout, so the first
  statement could land after the retry. Now its lanes are left alone
  (no check, verify, retry or release) until `sent + ttl + slack`. The
  model always required this (a worker verifies only once its statement is
  gone); the code didn't. Tested with statements that land as late as
  fence + budget + slack (`MemCentral`), and by the code mutant
  `release_in_flight`.
- **Error answers (a gap found on the replicated central, fixed
  2026-09-26):** a server's error answer settled a statement at once, but
  `TIMEOUT_EXCEEDED` (and `KEEPER_EXCEPTION`, `TABLE_IS_READ_ONLY` from the
  commit's retry loop) can come with a part that lands anyway, so the
  verify re-inserted it. Now only errors raised before anything is written
  settle at once (parse, analysis, access, admission, `TOO_MANY_PARTS`, the
  range assertion); every other error waits like an unanswered statement.
  Tested (`MemCentral::late_error_every`; the code mutant `ErrorSettles`
  duplicates) and modelled (the mutant `errorSettles` breaks `atMostOnce`
  by simulation and by a scripted run).
- The soak used a TTL of 6 s and a margin of 1 s (`--allow-short-margin`).

---

### D10. Consumer: multi-object statements squashed to one block

**Status:** accepted (`1b5ce6c`). This supersedes FASTPATH's "one object per
insert, token as backstop" importer shape. Linger added, off by default
(`f923fa8`, 2026-09-26).

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
  per worker, a longer poll, or a linger.
- **Linger (built 2026-09-26, `--linger`, off by default):** a table whose
  pending objects don't fill a statement waits up to the linger from its
  oldest object's HEAD, never past a lane's window; HEADs are cached, so
  waiting costs no second HEAD. At 2 objects/s into one table [M]
  ([`otap-rs/README.md`](otap-rs/README.md) §Consumer at fleet scale):

  | linger | objects per statement | server CPU per object | visible p50 / max |
  |---|---|---|---|
  | 0 | 4.0 | 14.8 ms | 538 / 1,142 ms |
  | 1 s | 7.1 | 9.2 ms | 1,956 / 2,907 ms |
  | 3 s | 10.2 | 7.2 ms | 2,382 / 4,707 ms |

  (Server CPU is server-wide over the run, loaded box.) At fleet rates a
  worker's lanes of a table fill statements without it; it is for sparse
  tables.

---

### D11. Consumer: count check and repair, not dedup tokens

**Status:** accepted (`bd1ae88` for the design, `1b5ce6c` for the
implementation); the check's partition range (`f923fa8`, 2026-09-26); the
horizon 3 days and the horizon audit (`9f1d958`, `eed5d9c`, 2026-09-26);
edge replays keep `received_at`, so the horizon's assumption is down to
sender resends (2026-09-27, [D19](#d19-durable-buffer-at-the-edge)).

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
- **The check reads only the partitions the batch's rows can be in
  (2026-09-26).** Not a predicate on `received_at`: that makes 26.10 read
  the table instead of the projection [M]; `_partition_value.1 BETWEEN
  toDate(lo) AND toDate(hi)` keeps the projection [M]. The range comes from
  the data, not the clock: each object's `received_at` is constant and in
  its metadata, and **the insert asserts it on every row** (`throwIf`,
  before the object's block is written), so rows an insert wrote are
  provably in it. The pre-check reads the batch's days ± a **copy
  horizon** (3 days; it was 1 day), because a copy of a request (resent into a new epoch)
  has the same content key and may have a later `received_at` (since
  2026-09-27 only a copy in new custody: an edge replay keeps the original's); the verify reads the
  statement's own days and recounts over the horizon before re-inserting
  (a narrower count is only ever lower). No metadata, a failed assertion, a
  table partitioned otherwise, or `--check-horizon all`: every partition.
  On 90 daily partitions on S3 (180 parts) a check went from **2,320 S3
  GETs and 546 ms CPU to 56 and 12.8 ms** at 1 day (cold; warm 1,240 /
  275 → 32 / 7.3), and **116 and 24.9 ms at 3 days** [M] ([`otap-rs/README.md`](otap-rs/README.md) §The count check's partition range).
  Tested: a batch spanning midnight, an object received five days before
  the worker's clock with an earlier attempt already in central, a copy 20
  hours later, rows that don't match their metadata; the model's `noHorizon`
  and `wallRange` mutants break `atMostOnce`.
- **The horizon is an assumption, now watched (2026-09-26):** a copy
  received more than the horizon after its original is ingested twice.
  The default went from 1 day to **3 days** (an edge that committed,
  crashed before learning so, and replays its durable buffer after a long
  outage stamps a new `received_at`), and a **horizon audit** reports each
  such copy: keys of recent partitions also in another partition, from the
  content projection; for those only, repeated `row_ordinal`s and the
  ingestions per day and epoch from the table; a copy beyond the check's
  reach in either order is **late** (`consumer_late_copies_total`, a WARN
  with lane, epoch, key and ages), one within it **unexplained** (a bug).
  It runs beside GC or alone, never in a worker, once a day by default;
  failures are counted, never fatal. **Cost per run: about one unranged
  check** (90 days on S3: 1,716 GETs, 426 ms CPU cold; 18 M keys: 3.8 s
  CPU, 0.78 s at a 1/16 key sample) [M]; at fleet scale a full run is
  ~150 s of central CPU and ~20 GB of projection read [E], hence daily, or
  sampled hourly. Tested end to end (edge objects on S3, the worker, the
  audit: the copy 2 days late skipped, the one 5 days late ingested twice
  and reported, nothing else) and in the model (`auditSilent` on the
  designs, `dupAudited` under `noHorizon`). Not built: an edge-side
  warning; the real fix is stamping `received_at` before the durable
  buffer, so a replay keeps its original partition
  ([`otap-rs/README.md`](otap-rs/README.md) §The horizon audit).
- **The fix at the source is built (2026-09-27):** `received_at` is the
  edge's custody time and a replay keeps it
  ([D19](#d19-durable-buffer-at-the-edge)). Edge replays no longer depend
  on the horizon: a 4-day outage replayed 3/3 requests into their
  original's partition, the check skipped all three and the audit was
  silent; the old binary ingested the 3 twice and the audit reported
  each (96.0 h late) [M] (`otap-rs/results/replay-received/`). The
  horizon and the audit now cover new custody of the same bytes: a
  sender's resend after its own outage, or a resend to another
  publisher. The consumer model says the same [Q]: an object's received
  day is its request's custody day (`pDay`, stamped once per
  incarnation); with `RESTAMP` (the old edge) `noHorizon`'s failure comes
  back without any resend (mutant `restamp`), without it HORIZON 0 holds
  for edge replays (`noHorizonReplays`) and fails only with sender
  resends (`noHorizon`) ([`otap-rs/README.md`](otap-rs/README.md) §Model
  and model-based test).
- **The rollup follows the table (2026-09-27).** For traces and logs the
  consumer's `ensure()` creates ClickStack's key-value rollup
  (`<table>_kv_rollup_15m`) and its materialized view after the table
  (`central::create_rollups`). The rollup table has the same dedup
  window, so the view's block of an exact retry (same token) is
  deduplicated too: an insert through the consumer and its exact retry
  count each row once [M] (single node; and on the replicas, by the
  replicated window). A regrouped retry never happens: the check keeps
  it from inserting anything present.

---

### D12. Consumer: GC and checkpoint compaction

**Status:** accepted (GC `1b5ce6c`; compaction `9f2c75d`; the slot below
the position kept, `11b479d`, 2026-09-26).

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

**GC keeps the slot below the position (a gap, fixed 2026-09-26).** The
consumer model at TTL 3 with writer faults broke `neverSkipsCommitted`,
before this round's changes too: a writer whose PUT's answer was lost is
`Unresolved` at that slot, and its next batch goes to the same slot,
create-only. If the first PUT landed late, was ingested, and GC deleted it
before the writer's next batch, that PUT succeeded on the deleted key:
committed and acked below the checkpoint, never ingested. `--delay` covers
a PUT's lifetime, not how long a lane may stay idle and unresolved. GC now
keeps each epoch's slot just below the position until the epoch retires
(one object per open epoch); the writer then gets 412, sees another batch,
and moves on. Mutant `gcReopens` (the old rule) breaks the invariant by a
scripted run; the design instance at TTL 3 with faults passes 5,000 × 60
[Q] ([`otap-rs/README.md`](otap-rs/README.md) §GC keeps the slot below the checkpoint).

**Open risks** ([risk 6](#4-open-risks-and-unknowns-ranked)):

- **Compaction waits for GC.** If `consume gc` stops, checkpoints grow again.
- `gc.json` is one object of size marks × lanes × entries; thousands of lanes
  want it sharded.
- A producer clock that steps back by more than about the zombie bound, or a
  writer that outlives the zombie bound, can leave a batch **uningested,
  never duplicated**. Nothing watches for this.

---

### D13. Replicated central: plain ReplicatedMergeTree, no zero-copy

**Status:** accepted (`1a88da1`); the fleet-scale consumer validated on
it on 2026-09-26/27 (below). **Zero-copy is rejected.** That commit's
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
  single-node figure is about 12 µs/row. **Re-measured 2026-09-27 at 31.3
  objects per statement (loaded box, load 2–6): replication adds 9% to an
  insert statement** (39.3 against 36.1 µs/row on the same server, all
  tables; traces 17.1 against 16.4, logs 15.9 against 14.4) and 2.1 Keeper
  transactions; +20% counting both replicas' whole CPU (checks, fetches,
  merges).

**Second run, the fleet-scale consumer (2026-09-26/27)** [M]
([`central-replicated/README.md`](central-replicated/README.md) §4): plain
replication (`tiered_own`), every fleet-scale feature on (the check's
partition range, load balancing, backoff, linger, the horizon audit beside
GC, metrics), production lease timing.

| | Result |
|---|---|
| two 15-min soaks: 4 replica kills, 13 replica network partitions, 13 Keeper leader stops, 6 Keeper node partitions, 3 node kills, 4 quorum losses, 3 worker pauses past the lease, worker and edge kills | **exactly once on both replicas**: 55,440 committed objects, 4.2 M rows; missing, partial, duplicated, uncommitted 0; every acked request once |
| the check's partition range on a replica | same counts on r1 and r2, answered from `by_content`; the sync waits for the whole table, so it covers every partition the check reads |
| the horizon audit on a replicated table | the same copies on both replicas when synced; **a lagging replica missed a late copy** → the audit now syncs (`--sync-replica`) and fails over between `--ch` replicas |
| Keeper overrun | **commits up to 19.0 s past a 10 s `max_execution_time`** (11.6 s past 2 s in a random fault mix), under Keeper faults, with the statement answering `TIMEOUT_EXCEEDED`; 3 such commits in the soaks. The 10 s margin was short by 9 s → **margin 20 s, TTL 75 s**, error answers that may come with a commit waited out ([D9](#d9-consumer-time-bound-on-inserts-plus-a-server-side-deadline)) |
| metrics | late and unexplained copies 0; lanes lapsed exactly for the paused workers; unsettled statements at the Keeper faults and partitions |

**Consequences.**

- Cold S3 bytes and PUTs double, and each replica merges its own S3 parts.
- **Insert CPU is paid once per shard** (2026-09-27): a part is parsed,
  indexed and run through the materialized views on the replica that
  inserts it, and the other replicas fetch it; every replica still merges
  its own copy. Measured overhead of replication on that one insert: +9%
  per statement, and about +20% counting both replicas' whole CPU
  (checks, fetches, merges) [M] (`094f9cb`,
  `central-replicated/results/bench-insert/`). The alternative,
  independent servers with a consumer group each on the same lanes, pays
  the insert on every server and multiplies the S3 GETs, with no Keeper.
  The calculator (v12) models both: the mid scenario is 139 vCPU with
  ReplicatedMergeTree against 151 independent ([§3](#3-current-sizing-summary)).
- The lease margin must cover how late a replicated commit can land: up to
  the Keeper session timeout after the statement started (19.0 s past a
  10 s budget measured), not the operation timeout as first assumed; the
  worker checks its slack against the replicas' session timeout at start
  ([D9](#d9-consumer-time-bound-on-inserts-plus-a-server-side-deadline)).
- On a replicated central an error answer to an insert does not end it:
  only errors raised before anything is written settle a statement.
- The horizon audit runs with `--sync-replica` (and `--ch` listing the
  replicas) on a replicated central.
- **The replicated DDL is derived, not copied (2026-09-27):**
  `central-replicated/scripts/ddl.py` rewrites the consumer's own
  statements (`consume --print-ddl`, `--print-rollups`): ClickStack
  2.39.1's traces/logs tables as ReplicatedMergeTree with the tiered TTL
  and storage settings, and their key-value rollups as
  ReplicatedSummingMergeTree (TTL on the 15-minute `Timestamp`, default
  policy) with the materialized views: three statements per traces/logs
  signal. The consumer runs with `--no-ddl` there and warns if a rollup is
  missing. Checked [M]: it parses (`clickhouse format -n`), applies on both
  replicas, and an exact retry counts once in the replicated rollup. The
  soaks above ran on the earlier, rollup-less DDL.
- A replica lost for good with parts nobody fetched stalls those tables'
  checks until the operator drops it (`SYSTEM DROP REPLICA`).
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
(`parquet.sort: {by: none}`). Routing is built as `deploy/components/routing` (loadbalancing gateway,
`routing_key: service`, 8 publishers) and measured locally
(`deploy/results/route-*.txt`); not deployed on a cluster.

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
- **With the real exporter** (v0.161.0, N = 8, 120 services): every service
  on exactly one publisher; busiest publisher 29% of traces (2.3×) and
  36.8% of logs (2.9×) [M: `deploy/results/`].
- **The skew is key granularity, not the ring.** The exporter's
  consistent-hash ring (CRC32, 200 virtual nodes per endpoint) is within
  ~7% of even [E]; a service is never split. The fix, if needed, is splitting
  hot services by trace id, not another placement function
  ([`deploy/README.md`](deploy/README.md) §The hash ring).

**Consequences.**

- Routing only matters if raw Parquet is queried, that is, in the lake option
  ([D17](#d17-lake--hybrid-cold-tier)). With central ingest alone, raw objects
  are deleted after ingest.
- Keep 32 objects per statement, and don't raise N without them.
- **Where the batching happens decides the object count and the
  duplicates.** Behind the gateway each agent request becomes up to N
  pieces. Publishers that batch them (`configs/edge-publisher.yaml`) give
  this section's object counts, but a gateway that dies with requests in
  flight makes the agents resend them, the publishers re-batch them, and
  central keeps both copies (two SIGKILLs: ~20% of that test's rows);
  graceful restarts duplicate nothing. Unbatched publishers are exact but
  write ~100× the objects at real agent batch sizes [E]. Exactness with
  batching needs piece-level identity at the publisher (not built).
- The loadbalancing exporter merges a publisher's pieces of a logs/metrics
  request in map order (U20); `deploy/collector/patches/0001` sorts them.
  Metrics without `service.name` are dropped under `routing_key: service`,
  so metrics bypass the gateway. Every ring change moves ~1/N of the
  services and duplicates in-flight requests; the ring Service publishes
  not-ready addresses, and the ring is keyed on StatefulSet pod hostnames so
  a publisher restart is not a ring change.

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

**Write-side ABAC: built with format v2 (2026-09-27; STPA R-S7, SEC-1..3).**
The layout is cluster-first ([D3](#d3-commit-protocol-manifest-less-create-only-slots),
[FORMAT.md](FORMAT.md)); the policies are in [`deploy/iam/`](deploy/iam/):
`edge-publisher.json` and `entity-controller.json` (create under
`${aws:PrincipalTag/cluster}` only, never delete, no `{ctl}`, deny a
session without the tag), `consumer.json` (read everything, write
`{ctl}` and tombstones), `gc.json` (delete slots, write its own control
objects), and `bucket-policy.json` (slots create-only, leases, checkpoints
and the watermark CAS). The entity aggregator drops a record whose cluster
key is not the one its lane's own cluster record binds (`clusterFilter`,
`TestClusterFilterRejectsForeignRecords`, and the gap test with a forged
record) [M].

- **SeaweedFS 4.47, demonstrated** [M] (`deploy/iam/seaweedfs_abac.sh`,
  `deploy/results/abac-seaweedfs.txt`, 16 of 16): static identities with an
  attached policy (`s3.json` `policies` + `policyNames`) and
  `${aws:username}` in the resource, one identity per cluster named after
  it. An edge of cluster c1 creates, re-reads (HEAD 200, free slot 404) and
  gets 412 on its own slots; it gets 403 writing or reading c2's prefix,
  writing a lease or `watermark.json`, and deleting anything; the
  consumer identity writes control objects and deletes. The Rust publisher
  with c1's key registers 7 of 7 lanes as c1 and 0 of 7 claiming c2.
  Plain `actions` (`Write:bucket/prefix/*`) scope by prefix but include
  DELETE, so they are not enough. SeaweedFS's STS with `${jwt:…}`
  variables was not needed and not tested.
- **AWS:** policies written, **not run on AWS** [D]. With EKS Pod Identity
  the session carries `eks-cluster-name` as a principal tag
  automatically: substitute it for `cluster` (then `CLUSTER` must be the
  EKS cluster name); IRSA has no session tags from the service account, so
  use a role per cluster or `https://aws.amazon.com/tags` in the token.
- **Nutanix Objects: unverified** [D]. It has no STS; the fallback is one
  access key per cluster with a prefix-scoped bucket policy, still to be
  tried on a cluster.

*The proposal as written before it was built:*
Today every edge in a bucket can write, and delete, any key its credentials
reach, so a compromised node can forge or delete another cluster's data or the
consumer's control objects. The fix is attribute-based access on session
credentials, with the key layout carrying the boundary:

- **Layout.** Put the cluster first: `{root}/{cluster}/{producer}/{signal}/{epoch}/{seq:020d}.parquet`
  for lanes, `{ctl}/…` for leases, checkpoints, tombstones and GC marks, and
  `{entities}/{cluster}/…` for entity lanes. Today's layout has no cluster
  segment (`{root}/{producer}/…`), so this is a layout change for the
  consumer's listing and GC.
- **Edge publishers:** `s3:PutObject` only, under `{root}/${aws:PrincipalTag/cluster}/*`;
  no `DeleteObject`, no access to `{ctl}`. Create-only is already enforced by
  `If-None-Match: *`; the policy adds that a publisher cannot write outside its
  cluster.
- **Entity controllers:** `PutObject` only under `{entities}/${aws:PrincipalTag/cluster}/*`.
  The aggregator rejects a record whose cluster differs from its key's.
- **Consumer, GC and sealer roles:** read all lanes; write and delete only under
  `{ctl}`; delete lanes only as GC. No edge role can touch `{ctl}`.
- **Where the tag comes from:** the `cluster` principal tag is set when the
  credentials are issued: an IRSA or Pod Identity role per cluster, or a session
  tag on `AssumeRoleWithWebIdentity` / Roles Anywhere. One policy per role
  serves all clusters through `${aws:PrincipalTag/cluster}`.
- **Portability.** Policy variables and session tags are AWS features. On
  SeaweedFS and Nutanix Objects, whether they are supported is unverified
  [D]; there the fallback is a separate credential per cluster scoped to its
  prefix, and the same separation of edge and consumer roles.
- **Read side** (viewers, the lake UI) uses the same attributes; see
  `research/lake-ui.md` when it lands.
- **Not covered:** isolation inside an object, and revoking a credential before
  it expires (keep sessions short).

---

### D19. Durable buffer at the edge

**Status:**

- **Go:** accepted. The collector's `sending_queue` on `file_storage` plus
  `retry_on_failure.max_elapsed_time: 0` gives at-least-once delivery across
  restarts ([`awss3/README.md`](awss3/README.md) §2).
- **Rust:** Quiver **on** in the deployed publisher
  (`configs/edge-publisher.yaml`, `deploy/base/rust`: 40 GiB cap on 50 Gi,
  `size_cap_policy: backpressure`); `configs/edge.yaml` stays the
  ack-after-commit baseline.
- **`received_at` is the time the request entered the buffer, kept across
  retries and replays (2026-09-27, `8efc34f`, `1fa3651`, tests `33c1800`,
  model `484f22f`).**

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
Retention is sized for custody age, not for the cap (below).

**received_at across replays (2026-09-27)** [M]
([`otap-rs/README.md`](otap-rs/README.md) §received_at is the custody time):

- **Rust:** the Quiver WAL write. `patches/0003` keeps each bundle's
  ingestion time in the segment manifest and hands it to the exporter as
  the pdata's `ingestion_time`; `s3pq` writes it as `received_at`.
- **Go:** the enqueue. `s3pq` stamps client metadata `x-s3pq-received-at`
  before the sending queue; `file_storage` persists client metadata with
  the request, so a replay publishes the original value.
- **A merged request (Go `batch`, 2026-09-27)** carries **the time it was
  handed to the queue**, stamped after the merge in a context of its own.
  Not the min or the max of its members' arrival times: none of them is in
  custody until the merged request is written (none was acknowledged), and
  a stamp older than that start breaks both users of the value. The
  consumer's partition and count check need a replay to keep its value
  (it does: the stamp is persisted with the merged item), and
  `complete_through` (below; `model/completeness.qnt`) claims that a request with `received_at` below
  an object's `oscope-low` is already committed. A request still being
  merged is in no one's custody and so in no object's low; a stamp from its
  arrival could fall below a later object's low while it is not yet
  committed. The Rust batch step does the same: Quiver stamps the batch at
  its WAL write. Test: `TestBatchReceivedIsTheEnqueueTime`; replays after a
  SIGKILL mid-PUT kept it (`deploy/results/go-batch.txt`, killput).
- **Without a buffer** custody passes at the commit: the exporter's clock.
- Request bytes, content keys and rows are unchanged.

| crash/replay test (SIGKILL after the commits, before the ACKs) | replays keeping `received_at` | central rows (expected 40,000) | horizon-audit WARNs |
|---|---|---|---|
| Rust, restart with the clock 4 days ahead (`otap-rs/scripts/replay_received.sh`) | **3/3** | **40,000** | 0 |
| control: the Rust binary before the change | 0/3 (96 h later) | 70,000 (3 requests twice) | 3 |
| Go, `file_storage` queue (`conformance/go_replay.sh`) | **2/2** | **40,000** | 0 |

Cost: Rust within noise (25–30 against 26–28 ms CPU per 10k-span request;
disk unchanged; +8 B per bundle in the segment manifest); Go 1.5 µs,
624 B and 10 allocations per request, +45 B per request in the
`file_storage` record.

**TTL against custody age (2026-09-27)** [E, Q]. Partitions are dropped
by `received_at`, and a replay now keeps the time it entered the buffer:
a replay older than the TTL is inserted and then dropped at the next TTL
merge, acked and never visible, and one older than the hot tier goes to
cold at once (latency, not loss).

- ~~The TTL by `received_at` must exceed the longest outage the edge
  buffer rides out (`retention_size_cap` at the edge's rate).~~ **Wrong
  under `backpressure`** (`model/retention.qnt`, `cadd7a1`): a full buffer
  refuses new requests but keeps its oldest, so the oldest request's age
  at the replay is not bounded by cap ÷ rate (mutant `capSized` breaks
  `acceptedVisible`).
- **The rule (R-S6): the TTL by `received_at` must exceed the longest
  custody age**, the time the oldest request stays in an edge's buffer:
  the backlog before the outage + the outage with its flaps (a link back
  for a moment, not drained, down again, resets an outage clock while
  the oldest request keeps aging) + the drain after it. It is independent
  of the cap. A retention cut at runtime must stay above the same bound
  (`cutDuringOutage` breaks it).
- **Only `drop_oldest` bounds custody age by the cap, and it does so by
  losing acked data at the edge** (`dropOldest`: `acceptedVisible` holds,
  `noEdgeDrop` fails; 47 of 64 lost, measured below).
- At 90 days the rule does not bind for any plausible outage; it binds
  when retention (or the hot tier, for latency) is cut to days. What it
  needs is a measure: an edge metric and alert on the oldest
  `received_at` still in custody; neither `s3pq` exporter exports one
  today (Quiver's and `file_storage`'s own metrics not checked).
- **The lake sealer's dedup TTL** (research
  [§5.1](research/central-optional.md#51-the-sealer-is-a-consumer-group-with-a-table-log-sink))
  must likewise be at least the longest custody age, not the 3-day copy
  horizon: a replay older than it is sealed a second time.

**`complete_through`: implemented (format v2, 2026-09-27)** [M, Q].
What the model requires below is built; [FORMAT.md](FORMAT.md) §2–§3 is the
reference:

- **`x-amz-meta-oscope-low`** on every data object, both edges: the lowest
  of the send time, the `received_at` of the requests the exporter holds
  (its own included: lower, still sound) and the buffer's floor. Rust:
  Quiver publishes the oldest ingestion time over its unresolved segments
  and the open one on the pipeline thread (`patches/0006`,
  `otel_arrow_dfe_otap::custody`); `custody: durable_buffer` makes every
  low 0 until it has. Go: a custody ledger keyed by a custody id in the
  client metadata (persisted by `file_storage` with the request), and a
  probe per queue at start: until the probe comes out of the FIFO queue,
  requests persisted by the previous incarnation may be in it and every
  low is 0. Computed at encode time and cached with the bytes, so a resend
  carries the same value.
- **Heartbeat slots** (`oscope-kind: beat`, zero bytes, same slot
  protocol): a **birth** per registered lane (the layout's namespaces)
  before the exporter takes requests (Go: in `Start`, which runs before the
  receivers; Rust: before the exporter's loop, up to `birth_timeout`), then
  one per lane idle for `heartbeat.interval` (30 s). Residual: a brand-new
  Rust publisher with Quiver takes requests into its WAL while its births
  are still pending, so if the store is unreachable at its very first
  start its lanes are unregistered until they commit (FORMAT.md §2). Cost at 30 s: about
  86 k PUTs per idle lane-month (≈$0.43), seven lanes per publisher.
- **The lane watermark** is computed by the lane's holder at each full
  listing: `min(max low passed before the LIST, min received_at of the data
  slots that LIST shows above the checkpoint)`. That is the model's prefix
  rule without an order between epochs, which several writer lanes of one
  process need; the model has it as `completenessImpl` (`PENDING`).
  It is kept in the checkpoint (`max_low_ns`, `wm_ns`).
- **`complete_through`** is published by `consume gc` (or `consume
  watermark`) to `{ctl}/watermark.json` by CAS: `max(previous, min(t_list −
  skew, min over listed lanes))`, a lane without a watermark counting as 0.
  The document names the lanes holding it back and the stale ones
  (`--wm-stale`, 5 min); the same goes to metrics. Chosen over a
  ClickHouse table because it must outlive central (the lake's sealer and
  the evaluator's fallback read it with one GET) and CAS gives the running
  max across publishers; a query service caches it.
- **Custody age (R-S6)** is now observable: a lane's lag
  (`now − wm`) includes the age of the oldest request in its publisher's
  custody, so the TTL rule above has its measure (`consumer_lane_watermark_lag_seconds`).

Evidence: `consumer::tests::complete_through_is_sound_and_advances` (20
randomized runs: two clusters, two writer lanes per signal sharing a
custody, sends in any order, crashes leaving zombie PUTs, heartbeats,
partial and lost statements; after every publication every request
received before it is in central; the recomputed value dipped and the
published one held) and `complete_through_mutants_break_soundness` (own
`received_at` as the low, no births, and the lane rule without the pending
cap each break it); s3pqexporter `TestOscopeLowFollowsCustody`,
`TestCustodyLedger`; conformance with births 254 PASS.

**What the model asked for (2026-09-27, kept for the record)** [Q]
(`model/completeness.qnt`, `cadd7a1`). A reader (the consumer, or the
lake's sealer) can publish "every request received before W is readable"
for deterministic alerts (research
[§5.4](research/central-optional.md#54-complete_through-a-watermark-for-deterministic-alerts))
only if the edge tells it what is still in custody. §5.4's rules as
written are unsound: the lane's last `received_at` seen passes an older
request still queued (`lastReceived`), and counting an idle lane as
caught up to the LIST time can't tell it from an offline edge with a
full buffer (`listTimeIdle`). What the model needs, none of it built:

- **a new object metadata field,** `x-amz-meta-oscope-low`: the minimum
  of the object's PUT time and the oldest `received_at` still in the
  edge's custody when it was first sent (other than its own); a resend
  carries the same value;
- **heartbeat slots:** an edge with nothing in custody commits an empty
  object whose low is its PUT time, or it holds the watermark forever
  (`noHeartbeat`: safe, but every window stalls until it pages);
- **lanes registered before custody:** a lane's first object (a birth
  heartbeat) is committed before its edge accepts data, so the minimum
  is over every lane, not the ones seen so far (`noBirth`);
- **per lane, the highest low over an ingested prefix** (`maxNotPrefix`),
  the minimum over lanes, **published as a running max** (a zombie PUT
  landing late makes the recomputed value dip);
- the alert evaluator runs a window only once its end is ≤ the serving
  source's watermark, pages on no source and on a stalled watermark, and
  every result carries its source and that source's watermark
  (`evalPastComplete`, `noDataOk`, `unlabeledFallback`).

**Disk full, measured** (`deploy/results/durable-diskfull.txt`):
`backpressure` answers 503 at the cap and every acked request is committed
later; `drop_oldest` acked and lost 47 of 64; a filesystem full before the
cap blocks the restart (WAL replay, U21) until the volume grows, then
nothing is lost; minimum cap 192 MiB/core.

**Open risks.** A real power cut; a full volume is a crash loop, so the cap
stays well below it. `received_at` is the edge's wall clock at the WAL
write or enqueue: as trusted as the exporter's clock was, and never
re-read, so a clock step after it changes nothing.

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

### D21. Entity catalog: `resource_id` at the edges, announcements in the data object

**Status:** built (2026-09-28). Both edges write `resource_id` on every
trace and log row and announce new resources in the data object; the
consumer inserts announcements into `otel_resources` before the rows; the
entity aggregator merges them with the controller's catalog. Central's
tables still carry the full `ResourceAttributes` with `resource_id` beside
it: switching them to `resource_id` + residual (the `ALIAS` column, the
rewrite proxy, [`entities/README.md`](entities/README.md) §1) is not
decided here.

**Context.** The entity catalog ([`entities/README.md`](entities/README.md))
puts resource attributes back at query time from a dictionary keyed by a
content hash of the covered attributes. Its correctness must not depend on
the controller: a pod born and dead inside a controller outage (or an
informer gap, AMBIGUITY.md X1) never reaches the catalog (STPA LS-5), and
its rows would stay residual-only. §6.3 proposed an edge announcement lane;
[`model/entityCatalog.qnt`](model/entityCatalog.qnt) then showed that a
*separate* lane closes the permanent gap but not the transient one, because
the consumer ingests lanes in any order.

**Decision.**

1. **`resource_id` is the controller's `rid.ID(rid.Split(attributes))`**
   at both edges (`otap-rs/src/resource.rs`, `parquetgo/resource.go`): the
   covered keys are a fixed list plus `k8s.pod.label.*`; the first
   occurrence of a key decides; only non-empty string values without NUL
   are covered. `entities/testdata/resource_id_vectors.json` (17 vectors,
   hostile: unicode, invalid UTF-8, duplicate keys, 300 labels, NUL,
   non-string values, near-miss keys) is generated by the controller and
   checked by the controller, both edges and ClickHouse's own expression
   (17 of 17 [M]); Hegel checks the Rust side against a transcription of the
   controller for arbitrary attribute lists and orders, through the OTLP and
   the OTAP walk.
2. **Announcements are columns of the data object** (`resource_announce`:
   the covered set on the first row of each resource the object announces;
   `oscope-announce` counts them), not a lane of their own and not a
   sidecar. Weighed against the model and the slot protocol
   ([`FORMAT.md`](FORMAT.md) §2.1):

   | | separate `resources` lane | sidecar object in the same slot | **columns of the data object** |
   |---|---|---|---|
   | gap bound (`exactAfterLag`) | no: lanes are ingested in any order, whatever the edge's commit order | only if the consumer reads the sidecar before the slot, and a sidecar key is outside the slot protocol (a second PUT, its own ambiguity) | **yes**: the announcement commits with the rows and is inserted before them |
   | "announced only after commit" | a second commit to track per request, a request acked only when both commit | a second create-only PUT, not atomic with the slot | **one commit**: the cache is marked when the data object commits |
   | PUTs | +1 per announcing request, serial (the announcement before the data) | +1 per announcing request | **none** |
   | format | a new namespace and registered lanes (births, heartbeats, ABAC entries) | a new key shape in every lane | **none**: two columns, one metadata key, `oscope-schema` 2 |
   | bytes | an object per announcement | the same | **~400 B per announced resource** (16 covered keys, zstd) [M]; nothing on the other rows but an empty map |

   The cache is per writer lane and epoch (D7's rule for series), windowed
   (a resource is announced again every hour, so central keeps first- and
   last-seen evidence), bounded (65,536 per lane, the least recently
   announced eighth evicted and re-announced), and marked only when the
   object committed as encoded (entityCatalog.qnt `sameObjectEarly` is the
   mutant that marks it at encode time).
3. **The consumer** inserts a round's announcements before its rows
   (`announce_first`) into `otel_resources` (ReplacingMergeTree keyed by
   resource and announcing object: idempotent, read once with FINAL), and a
   lane whose announcement statement did not surely land inserts nothing
   that round. An unanswered announcement statement is waited out like a data
   statement (D9); the first version did not, and `dst_consumer` found it
   landing after the lane changed hands.
4. **The aggregator** (`entities/controller`, `-announced db.otel_resources`)
   merges: the controller is the authority for a resource's attributes and
   history; an announcement is evidence that the resource existed. The
   catalog's `resources` view takes the controller's row where there is one,
   else the announcement's, `uncertain = 1` with a lifetime from first seen
   to last seen + 2 windows (`sql/announced.sql`); `resource_evidence` says
   which ids the controller lacks. X1's gap handling is unchanged.
5. **No format version bump**: no new object kind ([`FORMAT.md`](FORMAT.md) §5).

**Evidence** [M]: conformance Go = Rust, 260 PASS 0 FAIL (the objects'
schema, metadata and `oscope-announce`; `otel_traces`/`otel_logs` with
`resource_id`; `otel_resources` and its view). `dst_consumer` (300 seeds)
and `hegel_dst` with the rule `announce`: no row lands before its
resource's announcement (`sameLane`), every committed announcement lands and
nothing else (a planted consumer that ingests rows without waiting for the
announcement is caught by both). End to end
(`entities/scripts/announce_e2e.py`, real edge, SeaweedFS, consumer,
ClickHouse, the aggregator's view and a flat dictionary): a pod the
controller never saw is exact 34.7 s and 59.1 s (two runs) after its rows
became visible with `LIFETIME(MIN 30 MAX 60)`, 4.6 s with 5–10 s; a pod the
controller knew is exact at once, and a pod behind an edge with
announcements off stays residual-only.

**What it does not do.** Metrics carry no `resource_id` yet (layout B's
series rows are the place: next). The residual still travels in full in
`ResourceAttributes`; the grace window of §3.6 matters only after the
schema switch. An edge's covered keys that drift from the controller's
(another label list) now show up as announced-only resources the
controller lacks (`resource_evidence.in_controller = 0` for live pods), the
unknown-id alert of §6.1.

---

### Owner decisions, 2026-09-27

The owner accepted a **near-tail of 15–40 s** in place of live tail, and
**alerts about 5 minutes behind** when the aggregates central is down
(open question 5 of [research/central-optional.md](research/central-optional.md)).
The UI strategy is **(1) a HyperDX fork first, and (2) a dedicated lake-first
UI as well**, which may query in the browser with DuckDB or chDB in
WebAssembly. The feasibility spike for (2) is
[research/lake-ui.md](research/lake-ui.md): a hybrid design where a share
endpoint plans and presigns and the browser reads the lake, with the query
service, entity catalog API and `complete_through` banner shared with the fork.

---

### D22. Query service: SQL rebuilt from the tree, scope as table filters, labels on every result

**Status:** first slice built (2026-09-28): [`query/`](query/README.md). The
HyperDX fork, the lake UI and the alert evaluator do not call it yet.

**Context.** Both UIs (owner decisions, 2026-09-27) need one place that
authenticates viewers, scopes what they read by cluster and namespace
(STPA R-S8, SEC-4), builds SQL only from a parsed tree with per-user limits
(R-S9, SEC-5, SEC-7), labels every result with its source and
`complete_through` (R-S1, R-S2) and hands the lake UI presigned objects
(research/lake-ui.md §5.3).

**Decision.**

1. **Go, with rwproxy's parser** (AfterShip `clickhouse-sql-parser` v0.5.6:
   799 of 799 HyperDX statements, rwproxy README §2). Statements are parsed,
   checked by a reflective walk over the whole tree (allow-listed tables and
   visible CTEs only; no table functions, `SETTINGS`, `FORMAT`, object-reading
   functions, `IN table`), **rebuilt by the parser's formatter** with every
   table qualified, and parsed and checked again (a fixed point, the same
   tables). Nothing is spliced.
2. **Scope as `additional_table_filters`, not as a `WHERE`.** The per-table
   predicate (cluster and namespace expressions, or `resource_id IN` the
   entity catalog's ids, and the request's window) is built as a tree and
   passed as a setting. Measured on ClickHouse 26.10 [M]: it applies to every
   read of the table (subqueries, CTEs, both sides of a self-join,
   `IN (SELECT …)`, views, projections), as a `PREWHERE` on the primary key,
   and a `SELECT` alias cannot shadow it, which defeats a `WHERE` predicate;
   it does not apply to `merge()`, which the allow-list refuses. Grants on a
   `readonly = 2` user are the second fence.
3. **Every overflow mode pinned to `throw`**, `wait_end_of_query=1`: a limit
   is an error (X7, C2, C7).
4. **Labels.** `{ctl}/watermark.json` is read through S3 with a short cache
   and its own freshness; a missing, unreadable or stale document makes a
   result's completeness `unknown`, never `complete`. The plan reads it
   before it lists, and marks a start GC may have truncated.
5. **The lake plan lists v2 lanes directly** until the sealer exists: per
   cluster prefix (cluster-first keys make the scope a prefix), HEAD for the
   time range, presigned GETs (300 s, 60–900), `replan_after`, X8's rules in
   every plan. A namespace-restricted caller is refused: a raw object holds
   every namespace of its cluster.
6. **Audit before action.** A decision is written before a statement runs or
   a URL leaves; if it cannot be written, the request is refused.

**Evidence** [M]: unit and property tests (20,000 generated statements: accepted
exactly when no forbidden piece is used; accepted output is a fixed point with
a filter per table and literals preserved); the integration test with the Go
edge, SeaweedFS, the Rust consumer and ClickHouse (two clusters: a cluster's
token sees its 5 of 12 rows, 0 of the other's, 5 with an alias shadowing the
scope column; the plan returns only its cluster's objects; a cross-cluster plan
is 403). Rewrite cost ~99 µs per statement.

**Found while building it (fixed).** The entity aggregator spliced S3 key
names into its `ingest_log` and `lane_progress` INSERTs, so one cluster's
controller could forge another cluster's catalog-lag row and stop the
aggregator's pass for every cluster after it (the lag R-S5 reports comes from
that table). Keys now travel as JSONEachRow data, gap-record times are parsed,
and a failing lane no longer stops the others
(`cmd/aggregator/inject_test.go`).

**What it does not do.** No UI calls it yet; no Iceberg-REST `loadTable`; no
per-cluster watermark (a restricted caller's label uses the fleet minimum); no
count of rows without an entity match (R-S5's second half); no rate limit
beyond per-caller concurrency; no STS session-tag signing on AWS.

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
- ReplicatedMergeTree: a part is inserted on one replica and fetched by
  the other ([D13](#d13-replicated-central-plain-replicatedmergetree-no-zero-copy)),
  plus 6 vCPU of Keeper [E];
- headroom 1.75×, query load 75% of ingest;
- traces and logs in ClickStack 2.39.1's DDL minus the mapKeys indexes
  (option 2, [D2](#d2-transfer-format-parquet-read-with-s3-not-native-parts)).

**Result (calculator v12, 2026-09-27): 139 vCPU in 3 shards × 2 replicas
(6 nodes of 32 vCPU), and about 992 TB over 90 days.** With independent
servers instead of ReplicatedMergeTree (each server's own consumer group
inserts everything, no Keeper): 151 vCPU, same layout. Before option 2
(the pre-alignment tables, v11): 91 vCPU in 2 × 2.

| Output | Value |
|---|---|
| rows / s | 2.07 M (600k spans, 200k logs, 1.27 M points) |
| insert vCPU, whole region / per replica | 10.1 (of which fixed per-object 0.15) / 5.1 (independent: 10.1) |
| merge vCPU per replica | 21.3 |
| headroom vCPU per replica | 19.7 (independent: 23.5) |
| query vCPU (spread over replicas) | 41.2 |
| Keeper vCPU | 6 [E] (independent: 0) |
| **vCPU per replica / total** | **66.7 / 139.4** (independent: 75.5 / 151.1): 3 shards of 32, about 69% of the provisioned 96 per replica |
| compressed TB/day, one copy | 5.92 |
| hot tier, all copies (+25% merge room) | 15.3 TB |
| cold tier, all copies | 977 TB |
| **storage, all copies** | **992 TB** |
| storage $/month (local HDD / S3 per replica) | $45.2k / $23.7k |
| edge objects per day | 7.8 M (S3 PUTs ≈ $1.2k/month) |
| edge CPU, region (Rust / Go) | 5.3 / 7.3 vCPU |
| with every constant at the low / high end of its spread | 78–95 vCPU at the pre-alignment constants ([`bench/clean/README.md`](bench/clean/README.md)); not recomputed for option 2 |
| same scenario, metrics layout A | 282 vCPU (5 × 2), 1,117 TB |
| same scenario, no downsampling | 135 vCPU (3 × 2), 1,068 TB |
| with option 2's measured byte ratios (`bSpan` 64, `bLog` 61) | 846 TB ([`hyperdx/README.md`](hyperdx/README.md) §Option 2) |
| before the idle-box re-measurement | 115 vCPU, 991 TB |

**Constants and their provenance:**

| Constant | Value | Provenance |
|---|---|---|
| `usRow`: insert µs per span or log | **9.0** (table only 6.2; full ClickStack DDL 12.0; pre-alignment 3.43 [2.87–3.96]) | option 2 with its key-value rollup view (2026-09-27): the ratio to the pre-alignment tables measured on a **loaded box** (×2.40–2.62, one-object statements, 0.75 spans + 0.25 logs; `hyperdx/results/schema3-insert.md`) applied to the **idle-box** baseline 3.43 (`bench/clean` block 2) |
| `usPointB` / `usPointA`: insert µs per metric point | 1.18 / 4.47 | **idle box**, block 2. The A value is partly an encoder change: Rust objects, where the earlier value came from Go objects |
| `fixedMs`: fixed ms per inserted object | 15.3 [11.6–18.6] | **idle box**, block 2: one-object statements |
| `mergeRow`, `mergePointB`, `mergePointA`: merge µs | **20.1** / 4.1 / 19.4 | **idle box**, block 3, **projected** to 10⁴ parts per daily partition from runs that reached 161–2,100 parts. The projection is 5–60× beyond the parts measured. `mergeRow` (2026-09-27): the pre-alignment 10.1 × option 2's measured ratio 1.95–2.02 (full DDL 21.6; `schema3-insert.md`, clickhouse-local on 20–21 parts, loaded box) |
| `bPointB`, `bPointA`, `bSeries`: stored bytes | 6.7 / 26.4 / 38.4 | **idle box**, block 4, `OPTIMIZE FINAL`, no-replay pool. The data is synthetic (±50% for a real fleet [E]) |
| `edgeGoSpan`, `edgeGoLog`, `edgeRsSpan`, `edgeRsLog`: edge µs | 6.81 / 4.76 / 4.0 / 2.94 | **idle box**, block 1 |
| `edgePointB`, `edgeRsPointA`: edge µs/point | 1.82 / 5.05 | **idle box**, the `bench/sorting` bisect at head (18.2 and 50.5 ms per 10k). bench/clean's block 1 values of 2.06 and 6.20 were a harness artefact |
| `edgeGoPointA` | 7.47 | **idle box**, block 1 |
| `rollUsB`, `rollUsA`, `rollWinB`, `rollWinA`: rollups | 1 µs / 10.5 µs / 18 B / 50 B | **loaded box**, metrics-layout. Not re-measured |
| `bPqPointB`, `bPqPointA`: Parquet B/point | 24 / 38 | **loaded box**, parquet-go-era objects. The Rust objects are 18.1 / 19.8 |
| `bSpan`, `bLog`: stored bytes | 80 / 60 | **[E]**, left as they were for option 2. Synthetic data stored 9.6–39 B per span and 19 per log; not used. Option 2 against the pre-alignment tables, measured: spans ×0.75–0.80, logs ×0.97–1.01 (`schema3-insert.md`) |
| `bPq`: Parquet bytes per span or log | 50 | [E] |
| headroom, query load | 1.75×, 75% | [E] |
| hot-disk merge room (`HOT_SLACK`) | 1.25 | [E], a code constant |
| prices: PUT, GET, disk, S3 | $0.005 and $0.0004 per 1k; $0.08, $0.045, $0.023 per GB-month | [E] list prices |
| fleet and rates | [§1.2](#12-fleet-and-rates-per-region) | [E], apart from the fleet shape |

**What moves the answer most.** Merges are 32% of per-replica CPU and rest on
an extrapolation, now times a ratio taken on a loaded box. Headroom and
query load are 58% of the total and are pure estimates. The insert and merge
constants of option 2 are loaded-box ratios on an idle-box base
([risk 7b](#4-open-risks-and-unknowns-ranked)). The storage total rests on `bSpan` and `bLog`: spans and logs are
about 88% of the daily bytes in layout B, and both constants are estimates.

---

## 4. Open risks and unknowns, ranked

Ranked by how much of the design fails or changes if the risk lands, then by
how likely it is. Every boundary call's unknown outcomes, with their settle
bounds and caller rules, are in [AMBIGUITY.md](AMBIGUITY.md); its open rows
feed this list.

| Rank | Risk | Why it matters | What would retire it |
|---|---|---|---|
| 1 | **Nutanix Objects conditional writes** | The commit protocol ([D3](#d3-commit-protocol-manifest-less-create-only-slots)) and the consumer's leases, checkpoints and `gc.json` ([D8](#d8-consumer-leases-and-checkpoints-on-s3), [D12](#d12-consumer-gc-and-checkpoint-compaction)) all need atomic `If-None-Match: *` and `If-Match` on single-part PUT, plus read-after-write HEAD/LIST. The docs are behind a login and nothing public says. A store that ignores the header looks correct until two writers race; MinIO before 2024-09-13 and Garage are examples. The fallbacks are a Keeper/etcd `Coordinator` for the control plane (designed, not built) or closing epochs by time (not modelled). | Run `awss3/probe` and `s3cas` (`TestCreateRace`, `TestCASRace`, the ambiguous-resolution tests) against a real bucket, with the private CA and static keys. Add a startup self-test: two create-only PUTs of a scratch key, and refuse to run unless the second gets 412. |
| 2 | **Real per-pod rates and data shape** | Every rate in §1.2 is an estimate. `bSpan` and `bLog` (80 / 60 B) drive about 88% of stored bytes. Synthetic compression is ±50%. Real fleets carry 20+ resource attributes; the synthetic data carries 12–21. Series churn is untested. | Sample a real cluster's spans/s, logs/s and series per pod, and the stored bytes per row after merges. |
| 3 | **Real AWS behaviour** | Everything ran on SeaweedFS on localhost: no latency, and no 409 `ConditionalRequestConflict`, which object_store retries. Per-prefix request limits (3,500 PUT/s per prefix), real STS and session tokens were not exercised. LIST and renewal *counts* at fleet scale were measured locally (an idle lane: one LIST per 30 s, one renewal per 15 s; [D8](#d8-consumer-leases-and-checkpoints-on-s3)), not billed. Lane throughput is 1 / (encode + PUT): about 20 batches/s locally, lower with real latency. | A soak on a real bucket under IRSA and Pod Identity: commit latency, 409 rates, LIST cost per lane. |
| 4 | **HyperDX compatibility with the series layout** | HyperDX was never run live. Its SQL came from its own test snapshots at `hyperdx@885d30c`. Known degradations: the metric picker scans (1.26 s against 0.08 s); map filters cost 1.6–2.1× A; no rollup acceleration; no writes through the views. HyperDX changes can break the views silently. | Run HyperDX against the views; add the proposed `(MetricName, ServiceName)` helper MergeTree; pin the HyperDX version. |
| 5 | **Clock assumptions** | (a) The server-side fence needs worker and ClickHouse wall clocks within the lease margin. (b) Checkpoint compaction assumes no producer clock steps back by more than about the zombie bound, since epochs are named by wall-clock ms. (c) Replicated central: a commit can land after `max_execution_time`. Assumed 10 s (one Keeper operation) and "retired" on 2026-09-26 with a 10 s margin; **the replicated run the same day measured 19.0 s** (a commit resolved by the server's retry loop when Keeper came back, up to the session timeout after the start, answering `TIMEOUT_EXCEEDED`), so the 10 s margin did not hold. **Retired again (2026-09-26/27):** margin and slack 20 s (TTL 75 s), a start-up check against the replicas' Keeper session timeout, and error answers that may come with a commit waited out ([D9](#d9-consumer-time-bound-on-inserts-plus-a-server-side-deadline)); the bound is measured (≤ 29.0 s after the start, under the 30 s session timeout), not proven. (d) SigV4 fails beyond 15 min of skew (a stall, not corruption). Lease expiry itself uses monotonic clocks and is safe. | NTP monitoring with alerts tighter than the margin; an occasional unbounded listing to detect an epoch below a floor (not built). |
| 6 | **GC dependence** | GC is what bounds S3 storage, checkpoint size and `gc.json`. If it stops, compaction stops and checkpoints grow. Its safety rests on two bounds: the PUT lifetime (`--delay`) and the zombie lifetime (`--zombie`). A writer that outlives the zombie bound can re-create a deleted slot; such a batch is never ingested (not duplicated). (A live, unresolved writer could do the same below the checkpoint; fixed 2026-09-26: GC keeps the slot below the position, [D12](#d12-consumer-gc-and-checkpoint-compaction).) `gc.json` is one object sized marks × lanes × entries. | An alert on GC lag; shard `gc.json` per lane; enforce the zombie bound (pod termination grace plus kill). |
| 7 | **Merge CPU extrapolation** | Merges are 29% of central CPU per replica, projected to 10⁴ parts from runs of 161–2,100 parts. The fits are within −2 to +18% when fitted on ≥ 300 parts, and off by ±27% on 100–130. Random-id traces borrow another run's slope. | A day-long run at production statement sizes. |
| 7b | **Insert and merge cost of the ClickStack DDL** (new 2026-09-27) | Aligning traces and logs with ClickStack 2.39.1 (option 2) multiplies insert CPU 2.4–2.6× and merges 2.0× against the pre-alignment tables; it takes the mid scenario from 91 to 139 vCPU ([§3](#3-current-sizing-summary)). The ratios were measured on a loaded box (1-min load 5.6–20; full-DDL spans ranged 8.4–15.3 µs) and applied to idle-box baselines. Most of the insert is the text indexes and the rollup view; most of the merge is ZSTD and the items indexes ([`hyperdx/README.md`](hyperdx/README.md) §Option 2). **Levers, cheapest first:** (a) larger objects: the rollup view's cost is mostly per statement (logs +2.1 µs/row at 10k rows per object, +8.6 at 3.2k), so it shrinks as objects grow; (b) drop `idx_trace_id` (trace-id lookups lose their index; not measured); (c) drop the rollup (HyperDX's filter panel falls back to scans for native-column values: 9.7 s of scans against 0.9 s with the rollup, 3 M rows); (d) the entity catalog ([`entities/README.md`](entities/README.md)): rows carry `resource_id` + a residual map, about 2.8× less insert CPU per span and 3.2× per log (8.7 against 24.6 / 27.7 µs, loaded box) and −44 to −57% bytes, but only with HyperDX's resource-attribute SQL rewritten onto the catalog, e.g. by the rewrite proxy ([`entities/rwproxy/README.md`](entities/rwproxy/README.md): **exact** mode equal to the ALIAS column on 1,452 statements, **catalog** mode exact once the catalog is complete) and an edge change. | Re-measure option 2 on an idle box at production statement sizes (≈32 objects per statement); measure (a)–(c) against HyperDX live. |
| 8 | **Consumer check's copy horizon** (was: the check reads the cold tier, **retired 2026-09-26**; lowered 2026-09-27 to the residual case) | The check reads the batch's own days ± 3 days (1 day until the audit round): 116 GETs and 25 ms CPU cold against 2,320 and 516 ms over 90 days on S3 [M] ([D11](#d11-consumer-count-check-and-repair-not-dedup-tokens)). What remains is its assumption: a copy of a request received more than 3 days after its original is ingested twice. **Since 2026-09-27 an edge buffer's replay is not such a copy:** it keeps its `received_at` and lands in its original's partition (4-day outage: 3/3 skipped, audit silent [M]; [D19](#d19-durable-buffer-at-the-edge)). The residual case is new custody of the same bytes: a sender that resends after its own outage of more than 3 days, or resends to another publisher. **It is no longer silent:** the horizon audit counts every such copy (`consumer_late_copies_total`, a WARN per copy) after the fact, daily by default; it doesn't prevent the duplicate, and a same-day duplicate is outside what it sees. On a replicated central it must run with `--sync-replica`: unsynced, a lagging replica missed a late copy [M] (fixed 2026-09-26, `central-replicated/README.md` §4). | Measure the resend delay of real senders, and alert on the counter; ~~stamp `received_at` before the durable buffer~~ (built 2026-09-27); a deletion tool for reported copies (not built). |
| 9 | **Replicated insert cost** (mostly retired 2026-09-27) | 58.6–66.7 µs/row was measured on replicas at 7.9 objects per statement under a load average of 26–35. **Re-measured at 31.3 objects per statement on the same server (load 2–6): replication adds 9% to an insert statement** (39.3 against 36.1 µs/row, all tables; traces 17.1 against 16.4) and 2.1 Keeper transactions, **+20% counting both replicas' whole CPU** (checks, the other replica's fetches, merges) [M] ([D13](#d13-replicated-central-plain-replicatedmergetree-no-zero-copy)). The earlier figure was the small statements and the box. What remains: the plain baseline here (14–16 µs/row for traces and logs) is above the idle single-node 12, so the calculator's constants should be checked on an idle box. | Re-measure both on an idle box. The calculator (v12, 2026-09-27) now pays insert once per shard under ReplicatedMergeTree and on every server when independent; the +9% per statement is not in it yet. |
| 10 | **Content key against re-batching** | The content key hashes the request. A collector that re-batches after a restart produces new keys, and central ingests both copies. The loadbalancing exporter (U20) and any batch step behind a fan-out also re-cut requests. | Batch before the queue; never use `sending_queue.batch` in front of these exporters; ack only after the batch is persisted ([D4](#d4-awss3exporter-stock-rejected-patched-prototyped-own-exporter-preferred)). (`otelcol/config.edge.yaml` fixed 2026-09-26; `deploy/` agents and publishers checked 2026-09-26; 2026-09-27: agents no longer batch, both publishers batch before their buffer and replay merged batches byte-identical; with routing: the ordering patch, graceful gateway restarts, piece-level identity (not built).) |
| 11 | **Large objects and single-block inserts** | Above about 100k points (158 MB decoded) ClickHouse split objects nondeterministically. The consumer caps statements at 200k rows and 16 MB and sends big objects alone, so the verify-and-repair path is what keeps them exact. | Keep edge batches at 10k rows; report U12. (`deploy/`: the Go publisher's batch caps objects at 10,000 items; the Rust publisher's at 8 MiB.) |
| 12 | **Pinned ClickHouse behaviour** | Dedup defaults changed across versions: `deduplicate_insert`, `async_insert_deduplicate`, `deduplicate_insert_select`. Parquet reader chunking changes block formation. Everything was measured on 26.10.1.618 only. | Pin the settings in the consumer (done for two of them) and re-run the correctness and fault suites on every upgrade. |
| 12b | **Two edges drift** | Rust and Go must write the same rows; the Go and Rust Parquet writers and otap-dataflow's views differ in edge cases (found so far: span kinds outside the enum; parquet-go's untruncated statistics). | Run `conformance/run.sh` (both layouts), `conformance/go_faults.sh` and `parquetgo/modelcheck` in CI on every writer, otap-dataflow or collector version bump. (All three run nightly since 2026-09-27, `nightly.yml`; a bump is caught the next night, not on its push.) |
| 13 | **Rust upstream maturity** | otap-dataflow is pre-1.0, pinned at `5db8358` plus 2 patches. The OTAP receiver closes a whole stream on a poison batch. The build needs a pinned 577 MB toolchain. | Upstream the patches (U2, U11); track releases. |
| 14 | **Edge durability window** | With Quiver, a host crash can lose ≤ 25 ms of acknowledged requests (Go: `file_storage` does not fsync by default, so a host crash can lose what the page cache held). A filesystem full before the cap stalls the Rust publisher until the volume grows (measured; nothing lost), and **loses acknowledged requests on the Go publisher** (1 of 5 in each of 4 runs, U22). | Keep the cap below the volume (Go: 16 GiB of OTLP, `sizer: bytes`, on 50 Gi) and alert on it; since 2026-09-27 a publisher whose buffer volume has under 1 GiB free or whose buffer is at 95% of its cap is not ready and pages (`deploy/edgeprobe`, `deploy/alerts`); a power-cut test. |
| 14b | **Retention against custody age** (new 2026-09-27) | Partitions are dropped by `received_at`, which a replay keeps. An acked request whose custody age at the replay exceeds the TTL is inserted and dropped at the next TTL merge: acked and never visible. Under `backpressure` custody age is backlog + outage with its flaps + drain, **not bounded by the buffer cap** ([D19](#d19-durable-buffer-at-the-edge); `model/retention.qnt` [Q]); `drop_oldest` bounds it only by losing acked data. At 90 days it does not bind; a retention cut to days, or a cut during a site's outage, does. The same bound applies to the lake sealer's dedup TTL. | An edge metric and alert on the oldest `received_at` in custody; allow retention cuts only above the longest custody age planned for. |
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
| 24 | parquetgo/README.md, METRICS_SCHEMA.md: metrics commit object then manifest; inline lanes "not wired" | resolved 2026-09-26: the Go edge no longer commits with manifests; `parquetgo/edge` (the `s3pq` exporter) commits every signal manifest-less with a request-level acknowledgement (`parquetgo/README.md`; METRICS_SCHEMA.md revision 4); `publish.go` keeps manifests for the benchmarks and the chDB comparison only |
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
| U20 | contrib loadbalancingexporter v0.161.0 | logs/metrics pieces merged per backend in map order, so a retry isn't byte-identical; metrics without `service.name` logged and dropped | medium | `deploy/collector/patches/0001`; metrics bypass the gateway |
| U21 | otap-dataflow Quiver `5db8358` | with its filesystem full, a restart fails WAL replay (ENOSPC writing a segment) and the pipeline exits after 5 attempts | medium | cap well below the volume |
| U22 | collector exporterhelper v0.161.0 persistent queue | with its filesystem full, reading the next item fails on the metadata write; the queue has already advanced its read index, drops the item, and saves the index at the next write that succeeds: an acknowledged request is lost (logged at debug) | high for a full volume | a byte cap well below the volume; readiness and alerts on free space (`deploy/`) |
