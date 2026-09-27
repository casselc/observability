# Ambiguity register

Every call this pipeline makes across a process boundary, what its outcomes
are, and what the caller does when it cannot tell which one happened.

## Why this exists

The CAST pass over the defects found so far ([STPA.md](STPA.md) §CAST)
found one control flaw behind most of them: **a controller treated an
ambiguous outcome as a definite one.**

- An INSERT answered `TIMEOUT_EXCEEDED` and committed 19 s later
  ([D9](DECISIONS.md#d9-consumer-time-bound-on-inserts-plus-a-server-side-deadline),
  [D13](DECISIONS.md#d13-replicated-central-plain-replicatedmergetree-no-zero-copy);
  model mutant `errorSettles`).
- A worker verified, and retried, while its unanswered statement could still
  land (`releaseInFlight`).
- GC deleted a slot that a writer with a lost answer later retried into
  (`gcReopens`, [D12](DECISIONS.md#d12-consumer-gc-and-checkpoint-compaction)).
- The lease margin assumed a commit settles within one Keeper request
  (`keeperOverrun`).
- The horizon audit read a lagging replica as current.
- The load balancer re-cut a retried request into different bytes (U20).
- A replay restamped `received_at`, so a copy looked like a new request
  (`restamp`, [D19](DECISIONS.md#d19-durable-buffer-at-the-edge)).

Each call has three outcome classes:

| Class | Meaning | Example |
|---|---|---|
| **done** | the effect happened, and it cannot be undone by anything still in flight | a 200 to `PUT If-None-Match: *`; a HEAD showing our content key in our slot |
| **not done** | the effect did not happen, and nothing in flight can still make it happen | a parse error from ClickHouse; a 403 before evaluation for *this* request |
| **unknown** | anything else: no answer, a timeout, a reset, most 5xx, an error that may come with a commit, a 412 that may be our own write, a restart, a stale reader | `TIMEOUT_EXCEEDED` on a replicated INSERT |

"Unknown" is the default. An answer moves a call to done or not done only
where this register says it may.

## Rules

1. **Every unknown has three things, or it is an open row:**
   - a **settle bound**: the time after which nothing in flight can still
     change the outcome;
   - an **observation that resolves it**: a read of the effect itself (HEAD
     the slot, GET the lease, count the rows), taken after the settle bound;
   - a **caller rule**, one of:
     - **wait**: do nothing that depends on the outcome until the settle bound;
     - **verify**: after the bound, observe and act on what is there;
     - **fence**: make a late effect harmless (a server-side deadline, a
       lease epoch, a create-only key);
     - **idempotent retry**: resend something whose second application is a
       no-op (same key and bytes; a dedup token; a content key the reader
       skips).
2. **Feedback carries its freshness.** An observation says as of when, and
   from where: a replica synced before the read; a probe's period; a LIST
   that may be behind. A reader that cannot state its freshness is a lagging
   replica.
3. **Model template.** Every external action in a Quint model gets
   lost-request, late-effect, lost-answer and duplicate-delivery actions,
   and the caller's rule is a `const`, so the unsafe choice is a named
   mutant beside the design. See [model/TEMPLATE.md](model/TEMPLATE.md) and
   [model/ambiguousCall.qnt](model/ambiguousCall.qnt), with the coverage of
   the existing models.
4. **Fault injection per row.** Each row names the tool that makes its
   unknown happen on purpose (the table under
   [Fault injection and metrics](#fault-injection-and-metrics)). A row with
   none is unverified.
5. **Production metrics per unknown class.** An unknown that resolves
   silently is invisible in production. Each class needs a counter: how
   often it happened, and how it resolved. A row without one is at most
   *partly*.

Labels as in DECISIONS.md: **[M]** measured, **[D]** docs or source,
**[Q]** Quint, **[E]** estimate.

**Status:**

- *handled*: all three of rule 1, and evidence;
- *partly*: safe, but a bound, observation, metric or test is missing;
- *open*: an unknown with no rule, or no code yet (design rows);
- *unverified*: depends on behaviour nobody has observed on the target
  (Nutanix, real AWS).

## The register

### S3 and the commit protocol

| # | Interface | Outcomes: done / not done / unknown | Settle bound | How it is resolved | Caller rule | Evidence | Status |
|---|---|---|---|---|---|---|---|
| S1 | **Data slot `PUT If-None-Match: *`**. Rust `store.rs` `put_create` via `runner::append`; Go `parquetgo/commit` `Lane.Append`; `s3cas` `Writer.Append` for the S3-native prototype | **done:** 200; or 412, then a HEAD finds our content key and epoch. This includes the **5xx or 409 answered after the write applied**: the client library's own retry then gets **412 for our own write**. **not done:** 412, then a HEAD finds another batch (learn it, next slot) or a tombstone (halt, new epoch). **unknown:** no answer, timeout, reset, 5xx without a retry, a 409 (Go: `Classify` → `PutUnknown`; object_store maps 409 on create to `AlreadyExists` → HEAD), a HEAD error, 412 then HEAD free (`Inconsistent`) | `put_timeout` 10 s (Rust `S3Config`; object_store `max_retries` 2 inside `retry_timeout` = `put_timeout`); a PUT still in flight after that is bounded only by the **assumed PUT lifetime** that GC's `--delay` covers | HEAD the same slot; compare `oscope-content` and `oscope-epoch`, not the ETag. **Measured today:** with a proxy that applies the PUT and then answers 500, 503 or 409, Rust and Go both commit at slot 0, `resolved_own` 1, `learned_other` 0, one object | **verify**, and **idempotent retry** into the same slot (create-only: at most one copy lands). An unresolved slot is kept: the request is NACKed, and the next append resolves the slot first | model `s3Inline` (`timeout`/`resolve`/`apply`/`lose`; mutants `retryNewKey`, `plainPut`, `nonAtomicCond`) [Q]; `tests/mbt_s3inline.rs`; `scripts/faults.sh`, `conformance/go_faults.sh` [M]; **`otap-rs/tests/ambig_s3.rs`**, **`otap-rs/tools/ambig`** (500/503/409 after the write, 2026-09-27, `otap-rs/results/ambig/`) [M]; `s3accept ambiguous-create` [M] | **handled**; Nutanix unverified (risk 1) |
| S2 | **Tombstone `PUT If-None-Match: *`** (the consumer closes a quiet epoch; `worker.rs` `tombstone`) | done: 200, or a HEAD finds a tombstone after a lost answer. not done: a HEAD finds data (ingest it). unknown: no answer; 412 then HEAD free | as S1 | HEAD; a free slot is resent, up to 3 times | verify, idempotent retry | model `s3Inline` `cTombTimeout`/`cTombResolve` [Q]; unit tests | **handled** |
| S3 | **Lease and checkpoint `PUT If-Match`** (`worker.rs` `write_lease`, `write_ckpt`) | done: 200 with the new ETag; or a read-back equal to our doc. not done: the read-back shows another doc. **unknown:** no answer; **a 412 after a 5xx or 409**: the write applied, and object_store's retry carried the old ETag into our new one (**measured today** for 500, 503 and 409: `Err(Precondition)`, object holds our body) | the lease window: `sent + ttl − margin` on the holder's own clock; the checkpoint has no bound of its own, because the lease fences it | GET, and compare the whole doc (worker, lease epoch, beat, wall time; checkpoint lease epoch and version) | verify. **Fixed 2026-09-27:** a 412 used to mean "taken over", so the worker dropped the lane and counted `lanes_lost_cas`. That was safe, but the lane stalled until our own lease expired (TTL + margin, 95 s), and it was misreported. A 412 is now read back like no answer | `tests/ambig_s3.rs` (the 412) [M]; consumer test `a_412_for_our_own_lease_or_checkpoint_write_keeps_the_lane` (fails before the fix: nothing ingested) [M]; `s3accept ambiguous-cas` [M]; **model: atomic** (`wAcquire`/`wRenew`/`wAdvance`, no lost answer; TEMPLATE.md) | **handled** in code; model partly |
| S4 | **`gc.json` `PUT If-Match`** (`gc.rs`) | done: 200. unknown: 412 or no answer (`cas_conflict`) | the next GC run | the next run re-reads `gc.json`. Deletes already done are idempotent, and marks are re-appended; a lost `retired` record delays compaction | idempotent retry (a whole run) | 344 GC runs, 0 conflicts in the 30-min soak [M]; no test with a lost answer; `cas_conflict` is in the report, not a metric | **partly** |
| S5 | **GET / HEAD** (slot reads, lease and checkpoint reads, consumer stats HEAD) | done: 200 with metadata. **not done** (a free slot): 404, but only with `s3:ListBucket`. Without it, a missing key is **403**: unknown, and the writer stalls, correctly. unknown: timeout, 5xx; after a 412, a 404 means `Inconsistent` (store not read-after-write) | `head_timeout` 2 s | retried by the next append or poll | wait (the slot stays unresolved; never guessed free) | `s3accept read-after-write`, `head-missing` on SeaweedFS [M]; D3 IAM note [D] | **handled** (SeaweedFS); Nutanix, AWS unverified |
| S6 | **LIST** (`StartAfter`, discovery, GC, audit) | LIST is an **observation, never "not done"**: a key missing from a LIST may be committed. AWS: strong LIST-after-write [D: AWS S3 consistency page]. **SeaweedFS 4.47: consistent** in sequence (30/30, `s3accept list`) and **under concurrency** (16 lanes × 500 create-only slots, 4 listers, 173 LISTs: 0 acked keys missing, 0 holes; `s3accept list-race`, new 2026-09-27) [M]. **Nutanix Objects: unverified**; no public document states its LIST consistency [D: searched 2026-09-27; the Nutanix Bible's Objects chapter describes the metadata service, not consistency] | none needed | the consumer never skips a gap. A slot missing from a LIST is not yet visible; closing an epoch is a create-only tombstone, never "LIST shows nothing" | wait | `acceptance/results/s3accept-seaweedfs-listrace.txt` [M]; `gaps_are_never_skipped_nor_tombstoned` [M] | **handled** (AWS [D], SeaweedFS [M]); **Nutanix unverified**: run `s3accept --only list,list-race` there |
| S7 | **DELETE** (GC, `DeleteObjects`) | done: gone. unknown: no answer, a partial batch error | the next run | the next run lists and deletes again (idempotent) | idempotent retry. The hazard is not the DELETE but what it **reopens**: a deleted slot accepts `If-None-Match` again, so a slot is deleted only below `--delay` (PUT lifetime) and `--zombie` (writer lifetime), and the slot below the position is kept until the epoch retires | mutants `gcTombs`, `gcReopens`, `earlyCompact` [Q]; 15-min soak [M] | **partly**: the zombie bound is assumed, not enforced (risk 6); no metric for GC lag |
| S8 | **Worker heartbeat plain PUT** (`workers/{w}.json`) | done / unknown | the next heartbeat | none: advisory (fair share only) | idempotent retry | fleet tests [M] | **handled** (no safety role) |
| S9 | **S3 event notifications** (designed hints, D8) | a hint may be late, lost or duplicated | the 30 s LIST cap | LIST stays the truth | wait (a hint only wakes a lane early) | seam `discovery::Hints` tested; the SQS client is not built | **open** (not built; Nutanix support unknown) |

### ClickHouse (central)

| # | Interface | Outcomes: done / not done / unknown | Settle bound | How it is resolved | Caller rule | Evidence | Status |
|---|---|---|---|---|---|---|---|
| C1 | **Consumer INSERT … SELECT FROM s3()** (`sql.rs` `run_insert`) | **done:** only after the count check, never by the 200 alone. **not done:** error codes raised before anything is written: `SETTLING_CODES` (16, 36, 43, 47, 60, 62, 81, 115, 202, 252 `TOO_MANY_PARTS`, 395 the range assertion, 497, 516). **unknown:** every other code, including **159 `TIMEOUT_EXCEEDED`, 999 `KEEPER_EXCEPTION`, 242 `TABLE_IS_READ_ONLY`** (the commit's Keeper retry loop), no answer, and a transport error | **`sent + ttl + slack`** on the worker's clock: the fence (`now64(3) <= sent_wall + ttl − margin − budget`, ClickHouse's clock) + budget (`max_execution_time` 10 s) + slack 20 s. Checked at start against the replicas' Keeper session timeout (`system.zookeeper_connection`). Measured: commits up to **19.0 s** past a 10 s limit; every statement answered within 29.0 s of its start [M] | the count check by projection (C2) after the bound, after `SYSTEM SYNC REPLICA` (C3) | **fence + wait + verify**, then repair (missing: re-insert; partial: `row_ordinal NOT IN`; over: report, never fix) | mutants `noTimeBound`, `errorSettles`, `releaseInFlight`, `keeperOverrun` [Q]; code mutants `no_time_bound`, `release_in_flight`, `ErrorSettles`; `MemCentral::late_error_every`; replicated soaks with Keeper faults: exactly once [M] | **handled** (the bound is measured, not proven) |
| C2 | **Count check `SELECT content_key, count() … GROUP BY`** (`sql.rs` `counts`; also the repair's `NOT IN` subquery and the audit's queries) | done: HTTP 200 with the counts. unknown: error, timeout. **Hazard H-2, found 2026-09-27: a partial answer with HTTP 200 and no error.** A server or user profile with `read_overflow_mode = 'break'` and `max_rows_to_read`, `timeout_overflow_mode = 'break'`, `group_by_overflow_mode = 'any'`, `set_overflow_mode = 'break'`, … returns short counts. **Demonstrated on 26.10** with the consumer's own query and settings through such a profile: 3,932 instead of 8,000 per key, HTTP 200, when the projection can't serve the query (parts written before `ADD PROJECTION`). A short count makes the worker "repair" rows central holds (duplicates, L-3). `group_by_overflow_mode = 'any'` dropped a key entirely in a direct test | – | **fixed:** every consumer and audit query pins the eleven `*_overflow_mode` settings to `throw` (`sql.rs` `NO_PARTIAL_RESULTS`), so a limit is an error, which the worker already retries | verify (an error is not an answer) | `a_profile_with_break_modes_cannot_shorten_the_count_check` (unpinned: `k1 3932, k2 3932` HTTP 200; pinned: Code 158) and `every_query_pins_the_overflow_modes_to_throw` [M] | **handled** |
| C3 | **`SYSTEM SYNC REPLICA <t> LIGHTWEIGHT`** (`--sync-replica`, before a check that does not follow our own statement on this replica; the horizon audit) | done: the replica holds everything committed anywhere before the call. unknown: `receive_timeout`, error | `sync_timeout_ms` 5 s | none: the check is deferred and retried | wait (no check on an unsynced replica) | 15-min soak: 815 syncs failed and deferred their check; exactly once. Without the sync: 7 duplicates. The audit missed a late copy on a lagging replica, and now syncs [M] | **handled** |
| C4 | **DDL** (`ensure()`: `CREATE TABLE IF NOT EXISTS`, the rollups) | done / not done / unknown (a lost answer; on a replicated central, DDL is the operator's, `--no-ddl`) | none | re-run (idempotent `IF NOT EXISTS`); `--no-ddl` refuses to create a local table where a replicated one is missing | idempotent retry | `statements_parse_on_clickhouse`, `ensure_creates_the_rollup…` [M] | **partly**: the operator's replicated DDL (`ON CLUSTER`, `distributed_ddl_task_timeout`) has no runbook row |
| C5 | **Keeper** (under ReplicatedMergeTree commits) | session alive / expired (the replica goes read-only) / unknown (partition, frozen node) | the **session timeout** (30 s) after the statement started [M] | the server's own retry loop; the consumer sees C1's unknown class | wait (via C1's slack) | `central-replicated/scripts/keeper_overrun.sh`, `keeper_long_outage.sh`, `overrun.py` [M]; mutant `keeperOverrun` [Q] | **handled** (measured bound) |
| C6 | **Horizon audit** (`audit.rs`) | a report of late or unexplained copies. unknown: a failed run; a replica not synced | daily | runs are counted (`consumer_audit_runs_total`, `…last_success_timestamp_seconds`); it syncs and fails over | verify; failures are counted, never fatal | end-to-end audit test; replicated run [M]; `auditSilent`, `dupAudited` [Q] | **handled** |
| C7 | **HTTP answer vs body** (any `ClickHouse::query`) | the client treats non-2xx as an error and 2xx as success. ClickHouse can send `200` and then an exception in the body once it has streamed part of a result (`send_progress_in_http_headers`, large results) | – | the consumer's queries return a few lines or nothing, so the whole answer fits the server's buffer before the status is sent [D, not tested]; `counts` rejects a line it can't parse | verify | none | **unverified**: add `wait_end_of_query=1` or check `X-ClickHouse-Exception-Code` if results grow |

### The edge: OTLP, buffers, gateways

| # | Interface | Outcomes: done / not done / unknown | Settle bound | How it is resolved | Caller rule | Evidence | Status |
|---|---|---|---|---|---|---|---|
| E1 | **OTLP export agent → publisher → ack** (Rust `exporter.rs`; Go `s3pqexporter`) | **done:** the ack. It is sent once the request is in the publisher's durable buffer (E3), after the publisher's batch step, which merges requests before the buffer (D4: Rust `processor:batch` before Quiver; Go `s3pq` `batch`, 10,000 items or 1 s, a larger request split deterministically). Without a buffer, the ack comes once every object is committed or found committed (`request_verdict`). **not done:** a permanent NACK (undecodable). **unknown:** a retryable NACK (some parts unresolved), no answer, a closed stream | the agent's `retry_on_failure` (`max_elapsed_time: 0`: forever) | past the buffer, a batch persisted as one queue item replays **byte-identical** (same content key, same `received_at`), and a publisher lane resolves its slot first. **Before the buffer, a sender that resends after a lost answer** (its request's batch was written, the answer was lost) **is merged into a new batch**: a new content key, so central ingests both copies. The window is milliseconds; the local kill-mid-PUT tests saw 0 duplicates (D4, `deploy/results/go-batch.txt`) | idempotent retry past the buffer; **none** for the sender's resend inside the window | mutant `ackOnAny` [Q]; `faults.sh`, `metrics_faults.sh`, kind rows [M]; agent SIGKILL loss found and fixed (`k8s-sim.md` §9.1) [M]; Go byte-identical replay, 0 duplicates in kill tests (`5f59bfd`, `go-batch.txt`) [M] | **partly**: the resend-into-a-new-batch window is accepted and not counted (the horizon audit sees it only across days) |
| E2 | **Gateway (loadbalancing exporter, `components/routing`)** | the gateway acks the agent after its own export; a gateway SIGKILL re-sends pieces **re-batched**, so their bytes, and content keys, differ | – | none: the consumer cannot recognise a re-cut copy | idempotent retry **only** when the bytes are identical (patch 0001 sorts pieces, U20); graceful restarts are clean | `route-gwkill-batched.txt`: 70,320 and 61,727 duplicate rows with 8 senders; graceful restarts 0 [M] | **open**: piece-level identity not built (risk 10); duplicates are neither prevented nor counted |
| E3 | **Durable buffer write / ack** (Rust Quiver WAL; Go `file_storage` persistent queue) | done: the WAL write or the enqueue; the client is acked. **unknown after the ack:** a host crash within the WAL fsync interval (≤ 25 ms, not configurable); a flush that failed on ENOSPC (the segment returns only when a restart replays the WAL); the exporter's ack back into the buffer lost in a crash, so the request is replayed | the replay at the next start | replay; `received_at` kept, so a copy lands in its original's partition and the check skips it | idempotent retry (content key + kept `received_at`) | `replay_received.sh` 3/3, `go_replay.sh` 2/2; mutant `restamp` [Q, M]; `durable-diskfull.txt` [M] | **partly**: the ≤ 25 ms window is accepted; no metric for the oldest `received_at` in custody (D19) |
| E3b | **Durable buffer write under ENOSPC** (the buffer volume fills before the cap) | **Rust (Quiver):** writes are refused (503) and the probe goes not ready. Every acked request is committed once the volume grows and the pod restarts: 40,000 of 40,000 acked (`wedge.txt` `rust-volume`) [M]. **Go (exporterhelper v0.161.0 persistent queue):** the queue advances its read index before persisting it; when that write fails with ENOSPC the item is **dropped**, logged only at debug level, so **acked data is lost**: 1 of 5 acked requests, in 4 of 4 runs (**U22**, `wedge.txt`) [M] | none (Go: the loss is silent) | Rust: WAL replay after the volume grows. Go: none | avoid the state: a byte cap far below the volume (Go `QUEUE_BYTES` 16 GiB on a 50 Gi PVC), readiness (`edgeprobe`: volume free, fill ≤ 95%), alerts (`deploy/alerts/edge-buffer.rules.yaml`) | `deploy/scripts/wedge_test.sh`, `deploy/results/wedge.txt` (`0d226c2`) [M]; UPSTREAM_ISSUES.md U22; DECISIONS.md risk 14 | Rust **handled**; Go **open** (upstream bug U22, mitigated only by sizing) |
| E4 | **Kubernetes pod delete / StatefulSet scale-down with a buffer volume** | a graceful stop commits or keeps. **Scale-down leaves acked, uncommitted requests on a retained PVC**: unknown, with **no settle bound** if the ordinal never returns, and nothing reports it (the buffer metrics go with the pod; kubelet volume stats need a mount) | none today | **proposed 2026-09-27:** drain before scale-down (make the top ordinal unready, wait for the buffer floor, then scale) and alert on an unmounted buffer PVC | wait (keep the PVC) + **alert** | kind 3→4→3 with a retained buffer: late, not lost [M]; [`deploy/runbooks/scale-down.md`](deploy/runbooks/scale-down.md), [`orphaned-buffer.rules.yaml`](deploy/runbooks/orphaned-buffer.rules.yaml) (not deployed) | **partly** (a runbook and alert, no drain mechanism) |
| E5 | **Readiness and liveness probes** | readiness is an **observation with a period** (5 s × 3 successes); liveness restarts the process | the probe period | Rust `livez` is live in every phase but `Deleted` ([D] `otap-dataflow` `config/src/health.rs`: `Failed` counts as live). Go `health_check` (legacy) answers 200 while the collector runs. **No liveness probe depends on the buffer**, so a full buffer never restarts a pod. Readiness is the exec probe `edgeprobe` (volume ≥ 1 GiB free, fill ≤ 95% of the cap, the engine's own readiness; `successThreshold` 3, `0d226c2`). Checked at `4cbd07d`: the liveness probes (Rust `httpGet /api/v1/livez`, Go `httpGet /` on `health_check`) do not call it, so **a wedged publisher (full buffer or volume) is taken out of rotation and never killed**; restarting it would not help (Go loses data at ENOSPC, E3b; Rust replays only once the volume has room) | readiness gates traffic; liveness only on a hang | `deploy/results/wedge.txt`: not ready at the full volume and at 95% of the cap, ready again after the fix, no restart [M]; this audit [D] | **handled**; note a `Failed` Rust pipeline is never restarted by liveness, only paged (`EdgePublisherNotReady`) |
| E6 | **Clocks: `received_at`, SigV4, epoch names, the fence** | `received_at` is the edge wall clock at custody, never re-read: a skewed edge puts rows in the wrong day (the insert asserts the metadata, so rows and metadata agree). SigV4 beyond 15 min of skew: 403 `RequestTimeTooSkewed`, and a 403 says nothing about an earlier attempt (keep it unresolved). The fence needs the worker and ClickHouse clocks within the margin. Epoch names are wall-clock ms: a step back beyond the zombie bound hides an epoch below the floor | the margin (20 s) for the fence | nothing measures skew | wait (unresolved on 403); fence | D18 403 rule [D]; risk 5 | **open**: no NTP or skew metric; the "occasional unbounded listing" is not built |

### Entities, lake, alerts, UI

| # | Interface | Outcomes: done / not done / unknown | Settle bound | How it is resolved | Caller rule | Evidence | Status |
|---|---|---|---|---|---|---|---|
| X1 | **Entity controller informers: watch / relist** (`entities/controller/internal/ctrl`) | an event observed / not. **A relist is a gap:** a 410 Gone, a broken watch or a restart. Pods born and dead inside it never reach the catalog; deletions seen only by the relist (`DeletedFinalStateUnknown`) are closed at the relist time, not the deletion time. **client-go 0.37 relists after a 410 without calling the watch error handler** ([D] `reflector.go`: `isExpiredError` → `return nil`), so a handler cannot see it | none: what happened in the gap is lost | **added 2026-09-27:** every informer LIST is counted (a LIST, or a watch with `SendInitialEvents`); `relists` = LISTs beyond one per informer, and `deleted_unknown`, in `/stats`, with a log line per relist | the periodic sync (10 min) closes what vanished. **Open:** a gap record in the lane so the aggregator can mark closes in the gap as uncertain | `relist_test.go` [M]; KWOK controller outage: 4 of 4 pods born and dead in the gap missing (`k8s-sim.md`) [M]; model `entityCatalog` `noPermanentOrphan` [Q] | **partly** |
| X2 | **Entity lane `PUT If-None-Match: *`** (`internal/lane/s3.go`) | done: 200, or 412 and a HEAD ETag equal to our body's MD5. not done: another ETag, so the slot is skipped. unknown: timeout, 5xx (retried, 20 s per attempt) | 20 s `PutTimeout` per attempt | HEAD **ETag = MD5**. That holds for single-part, non-KMS PUTs on AWS and SeaweedFS; with SSE-KMS, or on a store whose ETag is not the MD5 (Nutanix unverified), our own write reads as another writer's, and the records are written again at the next slot. A HEAD error is treated the same way | verify; a wrong verdict writes the records twice | `internal/lane/s3_test.go` [M]; aggregator records are idempotent (token = object key) | **partly** (safe by idempotence; compare `oscope`-style metadata, not the ETag) |
| X3 | **Aggregator** (`cmd/aggregator`: lanes → catalog) | insert done / unknown | – | `insert_deduplication_token` = the object key; records idempotent; `lane_progress` is an optimisation | idempotent retry | fleetsim runs [M] | **partly**: the dedup window is a count; a sync's "absent means closed" uses a 60 s margin, not a settle bound on the controller's lane |
| X4 | **Sealer snapshot commit** (lake, designed: `research/central-optional.md` §5.1) | done: CAS won. not done: 412 from another sealer (re-plan: `REBASE`). **unknown:** a lost answer, or a 412 for its own commit behind an error answer (S3's shape) | – | re-read the latest snapshot; if it is ours, done | verify | `model/sealer.qnt` models the 412 from another sealer, not its own (atomic `commit`) [Q] | **open** (design; add `applyAnswerLost`) |
| X5 | **Alert evaluation** (HyperDX `checkAlerts`) | a query failure is recorded and leaves alert state alone ([D] `tasks/checkAlerts/index.ts`). **Unknown:** a window evaluated before its data is complete (late data, replays, lagging replicas); a partial result through a source's `querySettings` or the user profile (C2's hazard) | `complete_through` (designed, unsound as first written; `model/completeness.qnt`) | none built | should be **wait** (evaluate only windows ≤ the watermark), and page on a failed evaluation (R-S3) | `completeness.qnt`: 7 mutants [Q] | **open** |
| X6 | **Paging / notification delivery** (no code) | done: the pager acknowledged the event. not done: a 4xx for a bad payload. unknown: no answer, 5xx, timeout | the pager's dedup window | *Required rules:* retry with the **same dedup key** (alert id + window + state) until a 2xx; a notifier that cannot deliver for N minutes pages through a second channel; a watchdog alert that always fires and pages when it stops arriving (dead man's switch) | idempotent retry + watchdog | none | **open** (design) |
| X7 | **Query service / UI queries** (HyperDX; the rewrite proxy `entities/rwproxy`; the lake UI) | done: a complete result. unknown: timeout, error. **Partial with HTTP 200:** HyperDX sets `timeout_overflow_mode: 'break'` and `read_overflow_mode: 'break'` on its metadata queries (filter values, key values, the metric catalog: `common-utils/src/core/metadata.ts`, `useMetricCatalog.ts`, `useFetchMetricAttributeValues.tsx`), `group_by_overflow_mode: 'any'` on value distributions, and `result_overflow_mode: 'break'` on search pages (`useOffsetPaginatedQuery.tsx`, deliberate paging). A missing filter value reads as "no such value". The proxy passes settings through; our scripts set none | – | none: nothing marks a result partial | *Required:* every result carries source, freshness and completeness (R-S1, R-S2); a partial result is labelled | verify, visibly | grep at `hyperdx@885d30c` [D]; C2's demonstration [M] | **open** (H-2 in the UI) |
| X8 | **Presigned URL expiry in the browser** (lake UI, `research/lake-ui.md`; prototype signs for 300 s) | done: 200/206. **unknown mid-query:** 403 on an expired URL, which an engine reports as an error or, worse, as a missing object | the URL's `expires` | *Required rules:* the plan carries each URL's expiry; re-plan (or refresh) any URL within 60 s of expiry before reading; a 403 on a planned object means re-plan, never "no data"; a query that could not read every planned object renders as incomplete, with the objects it lacks; results name the snapshot they read | verify (re-plan) | lake-ui prototype (expiry not exercised) | **open** (design) |

## Fault injection and metrics

What makes each unknown happen on purpose, and what reports it in
production.

| Row | Fault injection | Production metric |
|---|---|---|
| S1 | `faultproxy2 -mode answer-late / apply-late / drop / commit-error -status 500\|503\|409` (commit-error new 2026-09-27); `awss3/cmd/faultproxy`; `commit.MemStore` faults | **missing**: `resolved_own`, `resent`, `learned_other` are logged at exporter stop (Rust) or by zap (Go), not exported. Proposed: `s3pq_commit_outcomes_total{outcome=ok\|resolved_own\|resent\|learned_other\|unresolved\|inconsistent}` |
| S2 | `MemBucket` faults | `consumer_epochs_closed_total`; no unresolved-tombstone counter |
| S3 | `faultproxy2 -mode commit-error` (`tests/ambig_s3.rs`); `MemFaults.own_conflict_every` (new) | `consumer_lane_changes_total{event="lost_cas"}` (now only real losses); the own-write case is a log line, no counter |
| S4, S7 | none for lost answers | GC report `cas_conflict`; **missing**: GC lag alert (risk 6) |
| S5, S6 | `s3accept` (`ambiguous-*`, `read-after-write`, `list`, `list-race`) against the target store | `consumer_gaps_seen_total`, `consumer_lane_lists_total` |
| C1 | `MemCentral` `late_error_every`, `lost_answer_every`, `late_by_ms`; Keeper faults in `central-replicated/scripts/soak_replicated.sh`, `keeper_overrun.sh` | `consumer_unsettled_statements_total`, `consumer_insert_errors_total` |
| C2 | a user profile with `*_overflow_mode = 'break'` (`a_profile_with_break_modes…`) | `consumer_checks_total`, `consumer_errors_total` (a limit is now an error) |
| C3, C6 | replica kills and partitions in the replicated soaks | `consumer_audit_*`; sync failures are logged, **no counter** |
| E1, E3, E3b | `wedge_test.sh`; `faults.sh`, `go_faults.sh`, `replay_received.sh`, `go_replay.sh`, SIGKILLs in `k8s-sim` | the buffers' own (`storage_bytes_used_bytes`, `otelcol_exporter_queue_size`, `failures_total`, `flush_failures_total`: `deploy/alerts/`); **missing**: oldest `received_at` in custody |
| E2 | gateway SIGKILL (`route_test.sh`) | **missing** (duplicates are invisible) |
| E4 | kind 3→4→3 | `EdgeBufferVolumeUnmounted`, `EdgeBufferVolumeOrphaned` (new, `deploy/runbooks/`) |
| E6 | a clock step (`replay_received.sh` restarts 4 days ahead) | **missing** |
| X1 | KWOK controller outage (`k8s-sim.md`) | `/stats` `relists`, `deleted_unknown` (new) |
| X2–X8 | none | none |

## Open and unverified rows, in order

1. **E3b (Go)**: a full queue volume silently drops acked data (U22).
   Mitigated by sizing, readiness and alerts; the fix is upstream.
2. **E2**: gateway SIGKILL duplicates re-cut requests. Nothing prevents or
   counts them.
3. **X5, X6, X7, X8**: alerts, paging, UI completeness, URL expiry. The
   rules are stated above; no code exists.
4. **E6**: no skew measurement for the clocks the fence and the partitions
   rely on.
5. **S6 / S1 / S3 on Nutanix Objects**: run `s3accept` (all checks,
   including `list-race`) with `--race-endpoints` for every client IP.
6. **S9**: event hints, not built.
7. **X4**: the sealer's own lost answer, in the model and in any build.
8. **C7**: HTTP 200 followed by an exception in the body. Not a hazard at
   today's result sizes; untested.
9. *partly*: E1 (a resend inside the batch window), S4, S7 (GC lag, zombie bound), C4 (operator DDL), E3 (U22,
   the custody-age metric), E4 (no drain mechanism), X1 (no gap record),
   X2 (ETag = MD5), X3 (count window), and **edge commit-outcome metrics**
   (S1).
