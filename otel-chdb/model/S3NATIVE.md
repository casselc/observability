# An S3-native variant of edge publishing

This note asks whether the edge publisher (`../chdbexporter/publish.go`) uses
S3's newer conditional-write operations, and designs a variant that does. It
has five parts:

- research on the operations and prior art;
- tests of what the local S3 stack supports;
- the design;
- a Quint model with checked properties (`s3Native.qnt`, `s3Native_test.qnt`);
- a Go prototype of the control plane (`../s3cas/`).

**Short answer.** The implementation uses none of these operations. Every S3
write goes through chDB's `s3()` table function or an `s3_plain_rewritable`
disk as a plain PUT, and neither sends conditional headers. SeaweedFS 4.47,
the local S3, does support them for single-part PUT, copy and delete,
atomically under concurrency. Conditional multipart completion is racy (see
below). So a control plane written with a Go S3 client can use them today.
The design below closes F1 and F3, closes PBT findings 1–3, and makes GC and
reader leases work without a catalog database. F4 is closed with
check-before-insert and a lease time bound, not by CAS alone. F2 is closed
for readers that go through the log, but not for live native-table readers.

## 1. Research

### AWS S3 conditional operations

| Operation | Header | Since | Notes |
| --- | --- | --- | --- |
| PutObject, CompleteMultipartUpload: create-only | `If-None-Match: *` | 2024-08-20 [a1] | 412 if the key exists (for a versioned bucket: if the current version exists and isn't a delete marker). |
| PutObject, CompleteMultipartUpload: compare-and-swap | `If-Match: <etag>` | 2024-11-25 [a2] | 412 on an ETag mismatch; 404 if the key is missing. Needs `s3:GetObject` as well as `s3:PutObject`. |
| Bucket policies that require conditional writes | `s3:if-none-match`, `s3:if-match`, `s3:ObjectCreationOperation` | 2024-11 [a3] | With such a policy, CopyObject into the prefix fails (403 without the header, 501 with it) [a4]. |
| Directory buckets (S3 Express One Zone): conditional deletes | `If-Match` on DeleteObject | 2024-11 [a5] | |
| General-purpose buckets: conditional deletes | `If-Match: <etag>` or `*`, on DeleteObject and DeleteObjects (ETag per key in the XML body) | 2025-09-16 [a6] | Enforceable by policy with `s3:if-match` [a7]. The SDK also has `IfMatchLastModifiedTime` and `IfMatchSize`, documented for directory buckets. |
| CopyObject: conditional destination | `If-None-Match`, `If-Match` | 2025-10 [a8] | |
| S3 Express One Zone: append | `x-amz-write-offset-bytes` | 2024-11-21 [a9] | Directory buckets only. The offset must equal the current size, which makes it a create-or-append CAS on the length. |

**Semantics** [a4, a10]:

- "If multiple conditional writes occur for the same object name, the first
  write operation to finish succeeds", and later ones get 412.
- A **409 ConditionalRequestConflict** means a concurrent operation (for
  example a delete) interfered. For PutObject, "on a 409 failure, retry the
  upload"; for If-Match, "fetch the object's ETag and retry". For
  CompleteMultipartUpload, the whole upload has to be restarted.
- In-progress multipart uploads are invisible to conditions until completed.
- Conditions apply to the **current version** only.
- Requests must use SigV4.
- There is no extra charge; failed requests are charged at the normal rate [a10].

S3 has been strongly read-after-write consistent, LIST included, since
December 2020 [a11]. That is what makes read-after-412 and log probing sound.
The request-rate guidance is 3,500 PUT/COPY/POST/DELETE per second per
partitioned prefix [a12].

**Ambiguous outcomes.** AWS's documentation says nothing about timeouts. A
request that timed out may have been applied, may still be applied later
(it's in flight), or never will be. The protocol has to resolve this itself:

- **Create-only:** retry the identical request. A 200 means it's committed
  now. A 412 means *something* is there: read it, and it's yours if the body
  (or an embedded writer token) matches. The ETag is the body's MD5 only for
  single-part uploads without SSE-KMS or SSE-C, so compare content, or a
  unique token in it, rather than relying on the ETag.
- **CAS:** retry with the old ETag. A 412 followed by a read showing your
  body, plus a unique token (epoch and sequence), means your CAS won. Without
  the unique token you get an ABA problem.
- A late duplicate of a create-only request can never apply twice. A late
  CAS can't either, because the ETag it names is gone.

AWS's own guidance [a13] covers 409 and 412 but not timeouts.

### Other stores

| Store | Create-only | CAS | Conditional delete | Source |
| --- | --- | --- | --- | --- |
| GCS | `ifGenerationMatch=0` / `x-goog-if-generation-match: 0` | generation / metageneration match | generation match | [s1]. XML API `If-Match` / `If-None-Match` are for reads. Delta, SlateDB and object_store use generation preconditions. |
| Azure Blob | `If-None-Match: *` | `If-Match: <etag>` (native, for years) | `If-Match` | [s2] |
| Cloudflare R2 | yes | yes (also ETag lists, weak ETags) | — | [s3]. Community questions about atomicity under concurrency were unanswered at the time of writing. |
| MinIO | from RELEASE.2024-09-13 (earlier releases **silently accepted** the second write) | yes | — | [s4] |
| Ceph RGW | yes | yes; reported to answer 412 to a *quoted* `If-Match` on some builds | — | [s5] |
| Tigris | yes | yes | — | [s6] |
| Nutanix Objects | **unknown** | **unknown** | **unknown** | Nutanix's API and consistency docs (the "Supported APIs" pages for Objects 4.x/5.x) are behind the support portal's JavaScript and login and could not be read here; public material (Nutanix Bible, product pages) doesn't mention conditional headers. The metadata store is a Paxos-based Cassandra derivative, which suggests strong consistency is *possible*; that is not a documented guarantee. See Section 9. |
| Garage | **no, by design**: no consensus, so it "cannot implement a safe and consistent way to reject" a concurrent overwrite | no | no | [s7] |
| SeaweedFS 4.47 | **yes, atomic (tested)** | **yes, atomic (tested)** | **yes (tested)** | Section 2. Conditional multipart completion is **not** atomic. No bucket-policy condition keys for conditional writes. |

### Prior art, and the patterns worth reusing

- **turbopuffer** [p1–p3]:
  - Each namespace's WAL is on object storage (`{ns}/wal/01.bin, 02.bin, …`),
    and a write returns only once committed there.
  - Commits go through a CAS, one WAL entry per second per namespace, with
    group commit. Their queue post (2026-02-12) runs a whole job queue as a
    single `queue.json`, CAS'd by a stateless broker with group commit.
  - Workers heartbeat into the object, and a timed-out job is taken over. An
    old broker "discovers it's no longer the broker when it gets a CAS
    failure".
  - The pattern: **one CAS'd object per coordination domain, plus group
    commit** to keep the rate of CAS writes low.
- **SlateDB** [p4, p5]:
  - Manifests are **create-only sequential objects**
    (`manifest/00…03.manifest`) or CAS'd.
  - A writer bumps `writer_epoch` in the manifest, then **fences by writing an
    empty WAL SST at the next WAL id**. A zombie that finds a newer epoch in
    its slot halts.
  - Readers keep **checkpoints with expiry** in the manifest, and GC deletes
    only what no active manifest or checkpoint references, and only after a
    minimum age.
  - RFC 0026 adds **boundary files**, so GC never lets an old sequence number
    be re-created.
- **Delta Lake**:
  - Commits are `_delta_log/NNN.json` written **create-only**.
  - delta-rs removed its DynamoDB log store and uses S3 conditional put by
    default from 1.0 (discussion dated 2026-05-26) [p6].
  - Delta Spark still uses DynamoDB for multi-cluster writes (an open issue
    asks for conditional writes) [p7].
- **Iceberg**: the commit is a CAS on the catalog's metadata pointer.
  ClickHouse's own Iceberg writer writes `vN.metadata.json` with
  `If-None-Match: *` and `version-hint` with `If-Match` [p8].
- **WarpStream, Bufstream, AutoMQ**: data goes to object storage, but
  ordering and commit live in a separate metadata store:
  - WarpStream's control plane;
  - Bufstream on etcd, Postgres or Spanner;
  - AutoMQ on KRaft.

  They are a counter-example: they chose *not* to use S3 CAS for the hot
  path [p9, p10].
- **Neon safekeepers** use Paxos across nodes, not S3 CAS, so they aren't
  relevant here.
- **S3 Express append** could turn a log into one appendable object, but it's
  single-AZ and directory buckets only, so it isn't used.

**The reusable patterns:**

1. **Content-addressed, immutable data objects.** A retry rewrites the same
   key.
2. **Create-only commit records**, at sequential keys, so that commits
   collide and are totally ordered.
3. **Epoch fencing through the collision**: a new writer's first entry
   occupies the next slot, and anyone who hits it halts.
4. **Checkpoint objects**, CAS'd, for consumer progress.
5. **Leases** as CAS'd objects with an epoch.
6. **GC by mark-and-sweep**, coordinated with reader leases through **one**
   CAS'd object, deleting only what is provably unreachable (a closed
   namespace, or below a horizon).
7. **Group commit**, to keep CAS and PUT rates low.

### What chDB and ClickHouse can do

- ClickHouse has `WriteSettings::object_storage_write_if_none_match` and
  `…_if_match`, honoured by `WriteBufferFromS3` and the Azure write buffers.
  Only two callers set them [c1]:
  - the Iceberg writer (`Iceberg/Utils.cpp`, BETA, behind
    `allow_insert_into_iceberg`): metadata create-only and `version-hint` CAS;
  - backups (`BackupWriterS3::writeFileIfNotExists`).
- **No user-visible setting exposes them.** A search of `system.settings`
  and `system.merge_tree_settings` for match or conditional settings in
  ClickHouse 26.10.1 found nothing relevant. The `s3()` table function and
  `s3_plain_rewritable` metadata write unconditionally.
- A ClickHouse issue (#112077, 26.8) shows that `LocalObjectStorage` ignores
  the setting, which silently turns the Iceberg commit CAS into an overwrite.
  So even the internal path depends on the backend.
- **Consequence:** the S3-native control plane (log slots, leases, the
  checkpoint, the GC state) has to use a Go S3 client (aws-sdk-go-v2 has
  `IfNoneMatch` and `IfMatch` on PutObject, CompleteMultipartUpload and
  CopyObject, and `IfMatch`, `IfMatchLastModifiedTime` and `IfMatchSize` on
  DeleteObject). Data objects can still go through chDB's `s3()` with plain
  PUTs, because their keys are content-addressed inside a namespace that one
  epoch owns. They could also go through a Go Parquet writer (evaluated in
  parallel) with `If-None-Match`.
- **Dedup windows, verified in 26.10.1:** non-replicated MergeTree has only
  the **count** window `non_replicated_deduplication_window` (default 0).
  There is **no `_seconds` variant** for non-replicated tables; `model/README.md`
  implies one. Replicated tables have `replicated_deduplication_window`
  (10,000) and `…_seconds` (3,600).

## 2. What the local S3 supports (tested)

`../s3cas/probe_test.go` and `protocol_test.go`, aws-sdk-go-v2 against
SeaweedFS 4.47 at `127.0.0.1:18333`. The client uses the default credential
chain:

```
S3CAS_BUCKET=otel S3CAS_ENDPOINT=http://127.0.0.1:18333 \
  AWS_ACCESS_KEY_ID=otel AWS_SECRET_ACCESS_KEY=otelsecret go test -v -count=1 .
```

Raw output is in the scratch directory; the results:

| Probe | Result |
| --- | --- |
| PUT `If-None-Match: *`, then a second PUT, then an identical retry | 200, 412, 412; the body is unchanged; the ETag is the body's MD5 |
| PUT `If-Match`: current, stale, wrong, unquoted | 200, 412, 412, 200 |
| PUT `If-Match` on a missing key | **412** (AWS documents 404) |
| CompleteMultipartUpload `If-None-Match: *` on a new or existing key; `If-Match` stale or current | 200, 412, 412, 200 (sequential) |
| **8 concurrent CompleteMultipartUpload `If-None-Match: *`, one key, 5 rounds** | **4, 6, 6, 8, 8 winners per round: not atomic** |
| **6 concurrent CompleteMultipartUpload `If-Match` on one ETag, 3 rounds** | **4, 5, 4 winners: not atomic** |
| CopyObject `If-None-Match: *` onto an existing or new key | 412, 200 |
| DELETE `If-Match` stale, then current; `*` on a missing key, then an existing one | 412, 204; 412, 204 |
| DeleteObjects with per-key ETags (one right, one wrong) | the right one is deleted, the wrong one reports `PreconditionFailed` |
| 16 goroutines × 20 rounds of `If-None-Match` on one key | exactly 1 winner in every round |
| 8 writers × 25 read-CAS increments | 200 won, 938 refused, final value 200, no value installed twice |
| CAS racing a conditional DELETE on one ETag, 50 rounds | never both succeeded |
| PUT cancelled as soon as the request was written (a real ambiguous outcome), 20 times | all 20 had landed; the retry got 412 and the ETag matched our MD5 |
| The same for If-Match CAS, 20 times | all 20 had applied; resolved by ETag |
| GET `If-None-Match` current ETag | 304 |
| Prototype log (`protocol_test.go`): fence, replay, zombie halts; a zombie racing its successor, 20 rounds | the zombie was fenced in 8 rounds; every log was epoch-monotone with 12 distinct commits |

**Why single PUTs are atomic and multipart completion isn't** (SeaweedFS
source, `weed/s3api`):

- PUT re-checks the precondition either under a distributed per-object lock
  (`withObjectWriteLock`), or by routing the create, with a
  `filer_pb.WriteCondition`, to the object's owner filer, which evaluates it
  inside its transaction (`s3api_object_routed_write.go`).
- `completeMultipartUpload` (`filer_multipart.go`) takes a different path
  when an owner filer exists: it calls `checkConditionalHeaders` at the
  gateway, *outside* any lock, and then `routedMkFile` writes the object with
  no condition. That is a check-then-act race.
- The code is unchanged on master as of 2026-09-25 (`635f69a`).
- A likely one-line fix: take the lock path whenever the request carries
  conditional headers. Neither the fix nor a report upstream has been made.

**Not tested:**

- bucket-policy enforcement (SeaweedFS's policy engine has no `s3:if-*`
  condition keys);
- 409 ConditionalRequestConflict (SeaweedFS has no such error code);
- versioned buckets;
- SSE-KMS ETags;
- multi-filer or multi-gateway SeaweedFS;
- AWS itself;
- MinIO, Ceph and Garage (no binaries here; releases can't be downloaded,
  and a source build wasn't attempted).

The design uses only single-part conditional PUT and DELETE, which are the
operations that proved atomic here.

## 3. The design

### Layout

```
{root}/{region}/{signal}/v{schema}/{producer}/
    lease.json                      CAS'd: {epoch}; a new incarnation takes epoch+1 (If-Match)
    log/{slot:020d}.json            create-only (If-None-Match: *). One entry per slot:
                                      {kind: commit, epoch, gen, content, rows, …envelope…}
                                      {kind: seal,   epoch, gen}
                                      {kind: fence,  epoch}
    head.json                       optional hint of the first free slot (plain PUT; never trusted)
    ns/e{epoch}/g{gen}/             one namespace per (epoch, generation): chDB tables' disk and
        {content}.parquet           Parquet, keyed by content hash inside it
        _owner.json                 create-only claim {epoch} (defence in depth for PBT 1)
    consumer/lease.json             CAS'd: {owner, epoch}
    consumer/checkpoint.json        CAS'd: {next_slot}
    gc.json                         CAS'd: {horizon, leases: [{reader, slot, expires}]}
```

### Writer

1. **Start.** Take `lease.json`, CAS'ing epoch to epoch+1: a unique epoch per
   incarnation. Append a **fence** entry at the first free slot. The zombie
   predecessor cannot write past it, and it seals every generation of every
   older epoch. Then **replay** the log below the fence to learn every
   committed content hash. The fence makes the replay complete: nothing older
   can land after it.
2. **Push** of payload P, content hash H:
   - If H is known committed, acknowledge the queue. This covers a retry
     after a crash or an ambiguous commit.
   - Otherwise write the data into `ns/e{epoch}/g{gen}/` (a retry targets the
     same key), then **append** `commit{epoch, gen, H}`.
3. **Append** (the only way anything is committed or sealed). Send a PUT with
   `If-None-Match: *` to slot n.
   - 200: done.
   - 412: read slot n.
     - The entry is ours: done (an earlier attempt landed).
     - A newer epoch: **halt, fenced**.
     - A commit of the same H: already committed, so acknowledge.
     - Anything else: learn it and try n+1.
   - Timeout: read slot n. If it's still free, resend the identical request.
     The in-flight copy and the resend can't both land.

   An append is never abandoned. It's retried until resolved, fenced, or the
   process dies.
4. **Generations.**
   - The generation number is never read from the clock. It moves only
     forward, by a **seal** entry appended under the push lock: gen+1.
   - The clock only *triggers* rotation, once it has passed the current
     generation. A clock step back therefore can't reopen a generation.
   - A seal is an ordinary append, so it's retried.
   - "What does generation g contain" is exactly "the commits for g before
     its seal", or before the next epoch's fence.
   - Local retirement (DETACH, dropping views) is driven from the log by a
     sweeper, idempotently: "retire every attached generation that is closed
     in the log". A failed DETACH is retried on the next sweep.
5. **Namespaces.**
   - Table names and endpoints include the epoch
     (`otel_logs_e{epoch}_g{gen}` on `ns/e{epoch}/g{gen}/`), so a restart
     never reuses its predecessor's table.
   - An optional `_owner.json`, create-only, makes a collision fail loudly.
     Its content check covers only the stale-local-table case; a zombie can
     still write *data* into its own namespace after being fenced. Those
     objects are orphans: no commit names them, and GC removes them.

### Consumer

- One worker at a time holds `consumer/lease.json` for a producer's log (a
  CAS with epoch). The lease has a TTL. A worker **stops issuing inserts
  before the lease can expire**: this time bound is the only way to fence a
  side effect S3 can't see.
- Per slot:
  1. read `checkpoint.json` and its ETag, then the slot;
  2. for a commit, `SELECT count() FROM central WHERE content_key = H`;
  3. if absent, `INSERT … SELECT … FROM s3(ns/…/H.parquet)` with
     `insert_deduplication_token = H` and the single-block settings from
     `model/README.md`;
  4. advance the checkpoint by PUT `If-Match` with the ETag from step 1.
- Seals and fences just advance the checkpoint.
- Several producers are spread over workers by taking their leases, as with
  Kafka partitions.
- **What S3 CAS can't make atomic.** The central INSERT and the checkpoint
  PUT are two systems. A crash between them makes the next owner re-insert.
  A worker that has lost its lease can still run an INSERT. S3 cannot fence
  either one.
- **The cover:**
  - the check before insert, which is correct only while one worker inserts
    at a time, hence the lease time bound;
  - the content token, which catches a zombie insert while H is still in the
    dedup window.

  The model shows both halves: without the check, F4 comes back; without the
  time bound, correctness rests on the window.

### Readers and GC

- A reader leases a slot range by CAS on `gc.json`, only at or above
  `horizon`.
- GC reads `gc.json` (and its ETag) and `checkpoint.json`, and computes
  `h = min(checkpoint, lowest lease)`. It writes the horizon back with
  `If-Match`. A lease taken in between makes the CAS fail, and GC retries.
- GC then deletes the data of commits below the horizon, and **orphans**:
  data in a namespace whose generation is closed (sealed or fenced) that no
  commit names. Closed means nothing can ever commit it, so no grace period
  is needed for orphans.
- Leases carry an expiry so a dead reader doesn't pin data forever. Expiry is
  outside the model.
- `old_parts_lifetime` > query bound + refresh interval
  (`partLifetime.qnt`) is still needed for native tables that the writer
  merges.

### Log truncation (not modelled)

- Slots can't simply be deleted. `If-None-Match` succeeds on a deleted key,
  so a zombie whose next slot was truncated could create it, and its commit
  would never be consumed.
- Options:
  - (a) never delete slots below the lowest slot a live or zombie writer
    could target. In practice that means deleting only after a bound on
    zombie lifetime (pods are killed within minutes), plus a lifecycle rule;
  - (b) overwrite truncated slots, with `If-Match`, by a `tombstone{epoch:
    current}` entry that a zombie reads as "fenced";
  - (c) SlateDB's boundary object.
- Slots are about 200 bytes. At one batch a second, that is roughly 30
  million objects and about 6 GB per producer per year, so (a) with 7–30 days
  is cheap.

### Costs

- **Per batch:**
  - the data PUT, as today;
  - one log PUT, which replaces today's manifest PUT;
  - a checkpoint PUT at the consumer (or one per N slots).
- **Per restart:** one lease CAS, one fence PUT, and a replay that GETs each
  slot since the last checkpoint. Bound it with a consumer-checkpoint hint,
  and have the checkpoint carry the set of recent hashes.
- **Contention:** conditional requests cost nothing extra on AWS. The log
  uses a new key per append, so it isn't subject to GCS's 1 write/s
  per-object limit and doesn't contend. A single CAS'd head, turbopuffer
  style, would need group commit.

## 4. The model

`s3Native.qnt` follows edgePublish's conventions: design choices are `const`
flags, and each configuration is an instance module.

**S3 as shared state:**

- the log map;
- a soup of in-flight append requests: S3 decides `If-None-Match` *when it
  applies* a request, and a request can apply late or never, while the
  writer times out independently;
- data objects per namespace;
- the CAS'd lease, checkpoint and GC-state objects.

**Actors:**

- writer incarnations, which can be zombies;
- the persistent queue;
- consumer workers, which crash or outlive their lease;
- readers;
- GC;
- a wall clock that can step back.

**Domains:** 2 payloads, 2 writer epochs, 6 log slots, 3 generations, 2
workers with 3 lease takeovers, 1 reader, and a dedup window of 2.

**Properties** (all together form `safety`):

- `payloadIngestedAtMostOnce`: end to end.
- `logNoDuplicatePayload`: edgePublish's `committedNoDuplicatePayload`.
- `sealMatchesManifests`: nothing is committed into a generation after its
  seal, or after a later epoch's fence.
- `sealOnce`.
- `commitImpliesData`, down to the GC horizon.
- `onlyCommittedIngested`.
- `noWriteFromFencedWriter`: epochs never decrease along the log.
- `noCommitLost`.
- `nsSingleWriter`: each namespace is written by exactly one epoch (PBT 1).
- `gcKeepsLiveData`: nothing unacknowledged or leased is deleted.
- `noReadOfDeleted`.
- `ackedImpliesIngested`.
- `generationsSealedOrPending`: a live writer never leaves an opened
  generation unsealed except for the seal it is still appending (PBT 3).

**Witnesses:**

- commits, seals, fences;
- a zombie halted, an ambiguous append resolved as ours, a resend, a known
  payload skipped;
- ingest, a dedup hit, a lost checkpoint CAS;
- GC deletes and orphan sweeps, a reader read;
- a commit after a clock step back, a commit after a restart;
- `wAllCommitted`, `wAllIngested`, `wAllClosed`: every payload committed and
  ingested, and every generation that holds a commit closed. These are the
  liveness-flavoured ones.

Results are in Section 5.

## 5. Model results

`quint run --backend typescript`, 120 steps, seed `0x5eed`. **✓** means no
violation was found in that sampling, which is not a proof. **✗** means a
counterexample was found; the number is how many traces it took. Commands
and raw output are in the scratch directory (`runall.sh`, `targeted.sh`,
`which.sh`).

**The design** (`s3NativeDesign`, hostile environment):

- The environment: ambiguous, late and lost appends; zombie writers; a clock
  that steps back; worker crashes and lease takeovers; token eviction.
- All 13 invariants hold together over **3,000 traces** (7 min), and again
  over a second 3,000 with seed `0x1`. That second run was on an earlier
  revision without the `SHARED_LOG` flag, which is inert when it is true.
- Every witness is reached. Per 3,000 traces:

  | Witness | Traces |
  | --- | --- |
  | committed | 1,800 |
  | sealed | 1,951 |
  | fenced | 2,913 |
  | zombie halted | 185 |
  | ambiguous append resolved as ours | 2,548 |
  | resend | 2,664 |
  | known payload skipped after a restart | 5 |
  | ingested | 1,401 |
  | dedup hit | 4 |
  | checkpoint CAS lost | 17 |
  | GC delete | 1,926 |
  | orphan swept | 1,323 |
  | reader read | 833 |
  | commit after a clock step back | 1,800 |
  | commit after a restart | 1,569 |
  | **every payload committed** | **201** |
  | **every payload ingested** | **85** |
  | **every generation closed** | **35** |

- Apalache: APALACHE_PLACEHOLDER

**Mutations.** Each flips one design choice:

| Instance | What changes | Violated (traces to find it) |
| --- | --- | --- |
| `nonConditionalLog` | Plain PUT for log slots | `noWriteFromFencedWriter` (3), `noCommitLost` (39), `logNoDuplicatePayload` (188) |
| `leaseWithoutFencing` | Per-epoch logs; a CAS'd lease is the only exclusion | `noWriteFromFencedWriter` (4), `logNoDuplicatePayload` (19) |
| `noFenceEntry` | Shared log, but no fence entry | none: ✓ all 13 over 3,000. See below. |
| `restartReusesTable` (PBT 1) | Namespace without the epoch; local tables persist | `nsSingleWriter` (7), `commitImpliesData` (39) |
| `genFromClock` (PBT 2) | The generation is read from the clock at push | `generationsSealedOrPending` (3), `sealMatchesManifests` (8), `sealOnce` (48) |
| `noSealRetry` (PBT 3) | A timed-out seal is abandoned | `generationsSealedOrPending` (4) |
| `blindCheckpoint` | Checkpoint without If-Match | not found by simulation: ✓ over 3,000 × 120 steps and 5,000 × 150 (seed `0x2`), because the interleaving is rare. The scenario `blindCheckpointTest.zombieCheckpointRegressesTest` shows `gcKeepsLiveData` violated: a zombie moves the checkpoint back below data GC has deleted. |
| `gcBlindWrite` | GC state written back blindly | `gcKeepsLiveData` (220), `noReadOfDeleted` (273) |
| `gcIgnoresLeases` | GC horizon = the checkpoint | `gcKeepsLiveData` (180), `noReadOfDeleted` (206) |
| `noCheckCentral` | No check before insert | `payloadIngestedAtMostOnce` (789): F4 returns |
| `unboundedZombieInsert` | A worker inserts after losing its lease; the window is evicted | `payloadIngestedAtMostOnce` (9), `noReadOfDeleted` (8: the zombie reads data GC already removed) |
| `unboundedZombieTokenHolds` | The same, with no other traffic | `payloadIngestedAtMostOnce` ✓ over 3,000: the content token covers it; `noReadOfDeleted` (29) |

**Two model findings:**

1. **Fencing comes from the collision, not the fence entry.** A zombie's
   next append targets a slot at or below its successor's first entry, and
   reads what is there. A newer epoch halts it. A writer that collides with
   a commit of the same content acknowledges it instead of committing again.
   So `noFenceEntry` keeps every safety property. The fence entry buys:
   - a complete replay at startup, so no append-and-collide is needed to
     learn old commits;
   - the zombie halting on its next append;
   - closing the dead epoch's generations. Without it they stay open, and
     their orphans are never swept. That is liveness, which a safety run
     can't see (`noFenceEntryTest.collisionStillFencesTest` shows the open
     generation).

   `leaseWithoutFencing` isolates the classic failure: when epochs don't
   share keys, a lease alone doesn't stop a zombie.
2. **The consumer's time bound protects GC too.** A worker that inserts
   after losing its lease can read data GC has already deleted
   (`noReadOfDeleted` in both unbounded-zombie instances). That's harmless
   for the data, because the INSERT fails, but GC's horizon should trail the
   checkpoint by at least one lease TTL.

**Deterministic scenarios** (`quint test s3Native_test.qnt --main <module>
--backend typescript`): 24 tests, all passing.

- `designTest` (14): F1 ambiguous and late appends; F1 across a crash; the
  zombie fenced; a late zombie request losing; PBT 1–3; the fence closing
  abandoned generations; F4 with the check; a zombie checkpoint losing its
  CAS; a reader lease stopping GC; an orphan swept; every payload committed
  and ingested.
- One or two per mutation, each showing its counterexample:
  - `restartReusesTableTest`
  - `genFromClockTest`
  - `noSealRetryTest`
  - `nonConditionalLogTest`
  - `leaseWithoutFencingTest`
  - `noFenceEntryTest`
  - `blindCheckpointTest`
  - `gcBlindWriteTest`
  - `unboundedZombieInsertTest` (two, including the within-window control)
  - `noCheckCentralTest`

**Caveats:**

- Small domains and bounded runs, as with edgePublish.
- One push at a time per writer, so no group commit.
- Log truncation, lease expiry, reader lease expiry and DETACH are not
  modelled.
- The central check-before-insert assumes read-your-writes (on a replicated
  central, `select_sequential_consistency`).
- The consumer's time bound is an assumption, encoded as
  `ZOMBIE_INSERT_BOUNDED`.
- SeaweedFS's multipart race is outside the model, which assumes atomic
  single PUTs, as tested.


## 6. What it closes

| Finding | Closed by | Checked by |
| --- | --- | --- |
| F1: an ambiguous manifest write plus the queue's retry commits twice | Create-only log slots with read-on-412 and read-after-timeout, which resolve the ambiguity to "ours"; a fence plus replay across incarnations; the content hash in the commit | `logNoDuplicatePayload`, `payloadIngestedAtMostOnce`; tests `ambiguousCommitResolvedOnceTest`, `lateRequestLandsOnceTest`, `crashAfterCommitReplaySkipsTest`; Go `TestAmbiguousCreateResolution`, `TestFencingAndReplay` |
| F2: orphan rows | Readers and the consumer go through the log. Orphans are unique per (namespace, H) and are swept once their generation is closed. **Live readers of native tables still see orphan rows**: chDB's part writes can't be made conditional (keep the edge dedup token, or publish Parquet only). | `orphanSweptAfterSealTest`; the table side is not modelled |
| F3: the seal undercounts | A seal is a log *position*, not a count; the next epoch's fence seals a crashed epoch | `sealMatchesManifests`, `sealOnce`; `fenceClosesAbandonedGenerationsTest` |
| F4: dedup-window eviction at the consumer | **Not by CAS**: insert and checkpoint span two systems. Check-before-insert under an exclusive lease with a time bound, plus the content token for the zombie case. | `payloadIngestedAtMostOnce` in the design; mutations `noCheckCentral`, `unboundedZombieInsert`, `unboundedZombieTokenHolds` |
| GC under readers, catalog | `gc.json`: one CAS'd object holds the horizon and the reader leases | `gcKeepsLiveData`, `noReadOfDeleted`; mutations `gcBlindWrite`, `gcIgnoresLeases` |
| Leak when the writer exits | GC by horizon plus the orphan sweep of closed namespaces | `wGcDeleted`, `wOrphanSwept` (witnesses) |
| Consumer checkpoint regression | CAS'd checkpoint | mutation `blindCheckpoint` |
| PBT 1: a restart reuses the previous epoch's table | Namespaces and table names per (epoch, generation), plus the create-only `_owner.json` | `nsSingleWriter`, `commitImpliesData`; `restartGetsOwnNamespaceTest`; mutation `restartReusesTable` |
| PBT 2: the generation comes from a clock read before the lock | The generation advances only by a seal appended under the lock; the clock only triggers rotation | `sealMatchesManifests`, `sealOnce`, `generationsSealedOrPending` under `CLOCK_SKEW`; `clockBackDoesNotReopenTest`; mutation `genFromClock` |
| PBT 3: a failed seal or DETACH is never retried | The seal is an append, retried until resolved; the fence closes a dead epoch's generations; DETACH is driven from the log, idempotently (not modelled) | `generationsSealedOrPending`, `wAllClosed`; `failedSealIsRetriedTest`; mutation `noSealRetry` |

**Smaller PBT defects.** These are outside the protocol, so they aren't modelled:

- `insert_format: json` rejects times before 1973 or after 2262: use
  RowBinary, or clamp and flag.
- `min_event_time` treats 0 as unset: track "set" separately.
- A TTL under 1 s renders as 0: reject sub-second TTLs in `Validate`.
- A Parquet-only config accepts `generation: 1ns`: apply the same minimum
  without object storage.

## 7. Compared with the current design and prior art

| | publish.go today | S3-native (this) | turbopuffer | SlateDB | Delta (delta-rs 1.0) |
| --- | --- | --- | --- | --- | --- |
| Commit record | Manifest per batch, plain PUT, under an epoch prefix | Create-only log slot per batch (or group) | CAS'd WAL entry per namespace, about 1/s with group commit | Create-only WAL SST ids; manifest create-only or CAS | Create-only `_delta_log/N.json` |
| Retry idempotency | None (new batch id): F1 | Content hash plus read-on-412, and a fence plus replay | Group commit; the client retries | WAL id per flush | OCC: rebase and retry at N+1 |
| Fencing | None (new epoch prefix) | The fence entry collides with the zombie's next slot | CAS failure on the object | Epoch in the manifest, plus an empty fence SST | None needed (every write is a CAS) |
| Seal / snapshot | `_sealed.json` from counters: F3 | Seal entry = log position; fence seals a dead epoch | n/a | Manifest | Checkpoint files |
| Consumer progress | Planned external catalog | CAS'd checkpoint plus a CAS'd lease | Queue in one CAS'd JSON with heartbeats | Checkpoints in the manifest | Readers are stateless |
| GC and readers | Planned catalog | One CAS'd `gc.json` (horizon plus leases); orphans in closed namespaces | Indexer-driven | Checkpoints with expiry; min-age GC; boundary files | VACUUM with a retention period (time-based) |

## 8. Recommendation and next steps

1. **Adopt the log and fence for the control plane**, in Go (aws-sdk-go-v2).
   Keep data writes in chDB (plain PUT into the epoch's namespace), or use a
   Go Parquet writer with `If-None-Match`.
   - The prototype `../s3cas` is the core: about 150 lines for the append
     loop, fence and replay.
   - Drop per-batch manifests and `_sealed.json`; the log replaces both.
2. **Fix the three PBT defects the same way even if the log is deferred:**
   - put the epoch in table names;
   - decide the generation under the lock and never let it go backwards;
   - retry seals from the retention loop.
3. **Consumer:**
   - a content-key column in central;
   - check-before-insert;
   - `non_replicated_deduplication_window` large enough (it's count-based);
     use a replicated central if a seconds window is wanted;
   - a lease TTL with a stop-inserting margin.
4. **Prefer Parquet-only publishing.** It removes F2 for live readers and
   about 90% of the edge's PUTs (bench `REPORT.md`).
5. **Before production:**
   - run `../s3cas` against real S3 (under IRSA, Pod Identity and Roles
     Anywhere) and, above all, **against Nutanix Objects**. Its conditional
     support is unknown; if it fails, use the Keeper-backed `Coordinator`
     (Section 9);
   - report the SeaweedFS multipart race upstream, and avoid conditional
     multipart until it's fixed. The design doesn't need it; log entries are
     small.
6. **Model next:**
   - log truncation;
   - lease expiry for readers;
   - group commit (several payloads per entry);
   - generations with concurrent pushes (the model serialises one push per
     writer, which is what the lock plus the sequential appender give).

## 9. Deployment targets: Nutanix Objects and refreshing credentials

Production runs in three ways:

1. EKS with IRSA or EKS Pod Identity, against AWS S3;
2. Nutanix, with static keys against Nutanix Objects;
3. IAM Roles Anywhere (`credential_process` with `aws_signing_helper`), for
   AWS S3 from outside AWS.

### Nutanix Objects: support unknown, so make it a checked requirement

**What is unknown.** Whether Nutanix Objects supports any of the following:

- `If-None-Match: *` on PUT;
- `If-Match` on PUT or DELETE;
- conditional CompleteMultipartUpload;
- strong read-after-write and LIST consistency.

The Objects "Supported APIs" and release-notes pages (4.3–5.2) need a portal
login that wasn't available here, and nothing public says either way. Assume
nothing: older MinIO answered 200 to a second `If-None-Match: *` PUT, and
Garage ignores the headers. A store that ignores the header *looks* like one
that supports it until two writers race.

**Requirement.** The S3-native mode needs:

- atomic `If-None-Match: *` on single-part PUT (log slots, the fence);
- atomic `If-Match` on single-part PUT (leases, checkpoint, `gc.json`);
- strong read-after-write on GET.

It doesn't need conditional multipart, conditional DELETE (GC can delete
unconditionally below a CAS'd horizon), or consistent LIST (slots are probed
by GET, not listed).

**Check it, don't assume it.**

- Run `../s3cas` against the Nutanix bucket as the acceptance test (static
  keys through `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, or `S3CAS_KEY`).
  `TestPutIfNoneMatch`, `TestCreateRace`, `TestPutIfMatch`, `TestCASRace` and
  the two `TestAmbiguous…` tests must pass. The race tests catch a store that
  checks the condition outside the write.
- The writer does the same at startup: two `If-None-Match: *` PUTs of a
  scratch key, and the second must be 412. Refuse the S3-native mode
  otherwise. This is cheap, and it catches a gateway or proxy that drops the
  header.

**Fallback if Nutanix lacks it.** Put the control plane (log slots, leases,
checkpoint, GC state) behind a small `Coordinator` interface: `CreateIfAbsent`,
`CompareAndSwap`, `Get`. Implement it on:

- (a) S3 conditional writes (AWS, SeaweedFS, and Nutanix if it passes);
- (b) **ClickHouse Keeper or ZooKeeper**, which a replicated central already
  runs. `create` fails if the node exists, and `set(path, data, version)` is
  CAS on the node version. These are exactly the two primitives, with
  linearizable writes; add a `sync` before a read that must see the latest;
- (c) etcd (`Txn` with `CreateRevision == 0`, or `ModRevision` compare).

Data objects stay in Nutanix Objects. Their keys are content-addressed inside
a namespace one epoch owns, so they need only plain PUTs and read-after-write
for new keys. The model is unchanged: it assumes an atomic create-only
register and CAS, not S3 in particular. This is the Bufstream and WarpStream
shape: object storage for data, a small consistent store for metadata.

The last resort, with no consistent store at all, is today's design plus the
PBT fixes (epoch in table names, generation under the lock, retried seals),
content-hash commit records, and consumer-side dedup by content key. It loses
zombie fencing and exact seals across crashes (F3 via a crashed epoch).

### Refreshing credentials: IRSA, Pod Identity and Roles Anywhere

The client is built with `config.LoadDefaultConfig`. It picks up:

- IRSA (`AWS_ROLE_ARN` + `AWS_WEB_IDENTITY_TOKEN_FILE`);
- Pod Identity (`AWS_CONTAINER_CREDENTIALS_FULL_URI` +
  `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`);
- Roles Anywhere (a profile with `credential_process = aws_signing_helper
  credential-process …`);
- static keys (environment or shared file);
- `AWS_ENDPOINT_URL_S3`.

Static keys are only an override, for Nutanix. SDK retries are off in the
probes, so every outcome is visible. The SDK refreshes expiring credentials
before they expire.

**How expiry interacts with the protocol.**

- **Safety doesn't depend on a writer keeping its credentials.** An expired
  or revoked writer gets 403 (`ExpiredToken`, `AccessDenied`) on every
  request. S3 rejects those before evaluating the condition, so nothing is
  applied. A zombie whose credentials have expired is simply a zombie that
  can't write, which is the harmless case. Writer fencing is by collision in
  the log, never by the writer's lease timing out, so a writer that loses its
  credentials is never assumed dead.
- **The one hazard is conflating 403 with "not committed".** A 403 says the
  *current* request didn't apply. It says nothing about an earlier attempt at
  the same slot that timed out. The writer has to keep the append unresolved
  and retry the *same* entry once credentials are back. It must never
  acknowledge or drop the payload, and never move to another slot.
  Resolution needs a GET, which also needs credentials. So an outage of the
  credential source stalls the writer, and the persistent queue absorbs it.
  `s3cas.Classify` maps 403 to `Unauthorized`, and `Writer.Append` returns
  without touching `Next`.
- **The consumer is where expiry bites.** A worker whose S3 credentials
  expire can't renew its consumer lease (a CAS on `consumer/lease.json`).
  But its **ClickHouse** connection is separate and may still work, so it is
  exactly the `unboundedZombieInsert` case. The rule has to be time-based and
  local: stop issuing inserts at `lease_acquired_at + TTL − margin`, by the
  worker's own monotonic clock, whether or not the renewal failed. It must
  not be "until a CAS fails". The margin covers the longest INSERT plus clock
  drift, and GC's horizon should trail by at least one TTL (model finding 2).
- **SigV4 needs a roughly correct wall clock**: requests more than 15
  minutes off fail with `RequestTimeTooSkewed`. The design already tolerates
  clock steps for generations (PBT 2), but a badly skewed edge node will
  stall on S3, not corrupt it.
- **Long multipart uploads** can outlive a credential's validity between
  parts. The control plane never uses multipart. Data through a Go writer
  should use single PUTs for batch-sized objects.
- **chDB's own `s3()` calls**: the exporter passes static keys in SQL today,
  which can't follow rotating credentials. ClickHouse's S3 client has its own
  credential chain (environment, web identity, instance metadata). Whether it
  covers Pod Identity's container endpoint and Roles Anywhere's
  `credential_process` in chDB 26.7 is **unverified**. If it doesn't, data
  PUTs have to move to the Go client too, the Go Parquet writer route. That
  also puts data and control plane on one credential source.

## Sources

- [a1] AWS What's New, 2024-08-20, "Amazon S3 now supports conditional writes": https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes
- [a2] 2024-11-25, "Amazon S3 adds new functionality for conditional writes": https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-s3-functionality-conditional-writes
- [a3] 2024-11, "enforcement of conditional write operations": https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-s3-enforcement-conditional-write-operations-general-purpose-buckets
- [a4] User guide, conditional writes: https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html and enforcement: https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes-enforce.html
- [a5] 2024-11, S3 Express One Zone conditional deletes: https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-s3-express-one-zone-conditional-deletes
- [a6] 2025-09-16, conditional deletes in general-purpose buckets: https://aws.amazon.com/about-aws/whats-new/2025/09/amazon-s3-conditional-deletes-s3-general-purpose-buckets
- [a7] https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-deletes.html
- [a8] 2025-10, conditional writes for copy: https://aws.amazon.com/about-aws/whats-new/2025/10/amazon-s3-conditional-write-functionality-copy-operations
- [a9] 2024-11-21, S3 Express One Zone append: https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-s3-express-one-zone-append-data-object
- [a10] https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-requests.html ; PutObject API (409 ConditionalRequestConflict and retry): https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html
- [a11] https://aws.amazon.com/s3/consistency/
- [a12] https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html
- [a13] AWS Storage Blog, 2025-06-17, "Building multi-writer applications on Amazon S3 using native controls": https://aws.amazon.com/blogs/storage/building-multi-writer-applications-on-amazon-s3-using-native-controls/
- [s1] https://docs.cloud.google.com/storage/docs/request-preconditions
- [s2] https://learn.microsoft.com/en-us/rest/api/storageservices/specifying-conditional-headers-for-blob-service-operations
- [s3] https://developers.cloudflare.com/r2/api/s3/extensions/ ; changelog https://developers.cloudflare.com/r2/reference/changelog
- [s4] https://blog.min.io/leading-the-way-minios-conditional-write-feature-for-modern-data-workloads/ ; https://github.com/minio/minio/issues/20346
- [s5] https://github.com/ceph/s3-tests/issues/583 ; https://github.com/CulverLab/sparcd-exploration/issues/378
- [s6] https://www.tigrisdata.com/docs/objects/conditionals/
- [s7] https://garagehq.deuxfleurs.fr/documentation/reference-manual/known-issues/
- [p1] https://turbopuffer.com/docs/architecture ; [p2] https://turbopuffer.com/docs/guarantees ; [p3] https://turbopuffer.com/blog/object-storage-queue (2026-02-12)
- [p4] https://github.com/slatedb/slatedb/blob/main/rfcs/0001-manifest.md ; [p5] https://slatedb.io/docs/design/checkpoints/ , https://slatedb.io/rfcs/0026-garbage-collector-boundary/
- [p6] https://github.com/delta-io/delta-rs/discussions/4482 ; [p7] https://github.com/delta-io/delta/issues/3596
- [p8] ClickHouse `src/Storages/ObjectStorage/DataLakes/Iceberg/Utils.cpp`, `src/IO/WriteSettings.h` (master); https://github.com/ClickHouse/ClickHouse/issues/112077
- [p9] https://docs.warpstream.com/warpstream/overview/architecture ; [p10] https://buf.build/docs/bufstream/architecture/kafka-flow/
- [n1] Nutanix Objects docs (login-gated): https://portal.nutanix.com/docs/Objects-v5_2:top-supported-apis-r.html ; Nutanix Bible, Objects: https://www.nutanixbible.com/11c-book-of-storage-services-objects.html
- [c1] ClickHouse master: `WriteSettings.h` (`object_storage_write_if_none_match`, `object_storage_write_if_match`), `WriteBufferFromS3.cpp`, `BackupIO_S3.cpp`, Iceberg `Utils.cpp`.
