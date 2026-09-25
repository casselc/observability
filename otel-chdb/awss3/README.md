# awss3: manifests, the contrib awss3exporter, and publishing without manifests

This directory answers three questions about the edge publisher
(`../chdbexporter/publish.go`, `../parquetgo/publish.go`) and the contrib
[awss3exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/awss3exporter).
It checks the answers against the Quint models in `../model/`, against
SeaweedFS 4.47 and ClickHouse 26.10.1 on this machine, and against a collector
built with `ocb`.

Labels:

- **[S]**: read in the source: awss3exporter, exporterhelper and
  encoding-extension v0.161.0, aws-sdk-go-v2 (config v1.33.3, service/s3,
  feature/s3/transfermanager v0.4.3).
- **[M]**: measured here.
- **[Mo]**: shown by the Quint model `../model/s3Inline.qnt`, by simulation,
  Apalache or scenario tests.
- **[E]**: estimate, or from documentation, and not tried.

## The three questions, answered

**1. Could our manifests be built into a custom encoder or marshaler for awss3exporter?**

The manifest's *content* can go into the object, and does here. The
manifest's *job*, being the commit record, can't be done by a marshaler.

- **What works** [M]:
  - `parquetencoding/` is an encoding extension that writes ClickStack-schema
    Parquet through `../parquetgo`.
  - The fields a manifest holds go into the Parquet footer as key-value
    metadata:
    - producer, epoch and sequence;
    - rows, min and max event time, received time;
    - schema version, signal, and the request's content hash.
  - Stock awss3exporter can use it (`encoding: parquet_encoding`).
  - pyarrow reads the footer metadata back, and ClickHouse reads the objects.
- **What doesn't** [S]:
  - A marshaler is `MarshalTraces(ptrace.Traces) ([]byte, error)` and gets no
    context.
  - It runs before the upload, and never learns:
    - the object key;
    - whether this is a retry;
    - whether the upload succeeded.
  - It can't set Content-Type, S3 user metadata or conditional headers.
  - Anything it writes as a separate manifest would therefore be written
    **before** the data. That inverts "a manifest implies its data".
- **Duplicates** [M]:
  - Stock awss3exporter gives every attempt a new random key.
  - An exporterhelper retry after an ambiguous PUT therefore always leaves a
    duplicate object, whatever the marshaler does.
- **A hack that works but shouldn't be used:**
  - Stock `Upload` returns nil without writing anything when the marshaled
    bytes are empty.
  - So a marshaler could do the whole publish itself and return `[]byte{}`.
  - It would then be an exporter hidden inside a marshaler, with no context
    and so no exporter timeout.

**2. Does that path, plus the collector's pipeline config, give the edge everything our model requires?**

No. The persistent queue, `retry_on_failure` with `max_elapsed_time: 0` and
`timeout` give **at-least-once delivery**:

- a request stays in the file-backed queue until an export attempt returns
  nil;
- it is redelivered after a crash.

Everything that turns that into exactly-once commits is missing:

- **No idempotent key.** A retry writes a new random key. That is F1 as
  duplicate objects: [M] 9 duplicates among 29 objects in the collector demo.
- **No conditional write** (`If-None-Match`).
- **No sequence, epoch or seal**, so a consumer can't tell "not written yet"
  from "lost", or know that a prefix is complete.
- **No request identity.**
- **A post-queue `sending_queue.batch` re-batches redelivered requests
  differently after a restart.** Even a content hash of the batch is then
  unstable.

The table under "Requirements" maps each one.

**3. Can CAS or object naming reduce or remove the separate manifests?**

Yes, for Parquet publishing: the data object can be its own commit record.

- **Keys.** Each batch is written at
  `{prefix}/{epoch}/{seq:020d}.parquet` with `PUT If-None-Match: *`. The
  batch description goes into S3 user metadata (`x-amz-meta-oscope-*`) and
  the Parquet footer.
- **Commit.** One PUT is data plus commit, so a commit can't exist without
  its data, and there are no orphans (F2 for Parquet).
- **Retries and timeouts.** On a timeout or a 412 the writer HEADs the slot:
  - it is ours: done;
  - it is free: resend the same request;
  - it holds another batch: that batch is committed, so move to the next slot;
  - it holds a tombstone: halt.

  So retries commit once (F1 within an epoch) and never leave gaps.
- **Closing a dead epoch.** The consumer races a zero-byte create-only
  **tombstone** into the dead epoch's first free slot. That replaces the
  seal (F3) and fences zombies, and it can never be mistaken for data loss:
  whichever PUT lands first decides.
- **Reading.** The consumer follows each epoch with `LIST StartAfter` its
  checkpoint.
- **Evidence:**
  - modelled [Mo];
  - prototyped as a ~110-line patch to awss3exporter plus a 285-line
    appender;
  - run end to end in an `ocb` collector with SIGKILL [M]: 1,000 spans sent,
    1,000 ingested; stock awss3exporter stored 1,450.
- **What it does not remove:**
  - the consumer's content-key check before insert: a request re-sent after
    a crash is committed again in the next epoch, and F4;
  - a manifest for **native chDB tables**: a part is many objects, so it
    needs `../model/S3NATIVE.md`'s separate log entry;
  - the dependence on the store deciding `If-None-Match` atomically. That is
    unknown for Nutanix Objects; see "Deployment".

## What awss3exporter v0.161.0 actually does [S]

Source: `exporter.go`, `s3_writer.go`, `internal/upload/{partition,writer}.go`,
`marshaler.go`, `factory.go`, and exporterhelper's `internal/{base_exporter,
retry_sender,timeout_sender,queue_sender}.go`, `internal/queue/persistent_queue.go`
and `internal/queuebatch/*`.

| Question | Answer |
| --- | --- |
| Object key | `path.Join(s3_base_prefix, prefix, strftime(now, s3_partition_format), file_prefix + signal + "_" + unique + "." + ext + compressionExt)`. `prefix` is `s3_prefix`, or the value of the resource attribute named by `resource_attrs_to_s3.s3_prefix` on the **first** ResourceSpans/Logs, if present. `now` is `clock.Now(ctx)` **at each `Upload` call**, so a retry can land in another partition. `ext` is `encoding_file_extension` (or the built-in marshaler's). |
| Unique part | The default is `randInt()`, a uniform integer in [100000000, 999999999): about 9×10⁸ values, **new for every call**. `unique_key_func_name: uuidv7` is the only alternative. There is no content-derived or sequence option. At 1,000 objects a minute in one partition the default collides about 5.6×10⁻⁴ times a minute, **about 290 silent overwrites a year** [E, birthday bound]. Use `uuidv7`. |
| Can the marshaler influence the key, Content-Type or metadata? | No. The interface is `MarshalTraces/Logs/Metrics(pdata) ([]byte, error)`. The extension is looked up by `encoding: <id>`, and `encoding_file_extension` is static config. `UploadObjectInput` gets only Bucket, Key, Body, StorageClass, ACL and ContentEncoding: no ContentType, no Metadata. |
| One Consume call, one object? | Yes, one `UploadObject` per call. There are exceptions. Empty marshaled bytes write nothing and return nil. With `resource_attrs_to_s3.s3_prefix`, `batchperresourceattr` splits the request **before** exporterhelper, so each part becomes its own queue item and object. Above 16 MiB, transfermanager switches to multipart: still one object, but completed by `CompleteMultipartUpload`. |
| Conditional headers? | Never sent. transfermanager's `UploadObjectInput` does carry `IfNoneMatch` and `IfMatch`, and passes them to PutObject and CompleteMultipartUpload, so a patch is a few lines. |
| What a retry re-sends | Three layers retry. (1) The SDK retryer (`retry_mode: standard`, 3 attempts) retries one HTTP request **with the same key**. (2) exporterhelper `retry_on_failure` calls `ConsumeTraces` again with the **same pdata**. That marshals again and builds a **new key**, so an ambiguous first attempt leaves a duplicate object [M]. (3) After a crash, the persistent queue re-enqueues every dispatched item that wasn't finished, at the back of the queue, and a new process exports it under new keys. |
| Request identity | None reaches the exporter. The queue's item index is internal, and the restored context carries only span context and client metadata. The marshaler sees less still: no context, no attempt number, no key. |
| Retry end | With `max_elapsed_time` > 0 (default 300 s) the item is **deleted** from the persistent queue after the last failure ("Dropping data"). On shutdown a `ShutdownErr` keeps it for the next start. `max_elapsed_time: 0` retries forever. |
| Timeout | `timeout` (default 5 s) is a context deadline around each attempt. An S3 PUT cut off by it may still land: the ambiguous outcome [M: proxy shows landed PUTs whose client gave up]. |
| Batching | `sending_queue.batch` runs **after** the queue. The persistent queue holds pre-batch requests; the batcher merges and splits them and acknowledges the items when the merged batch is done. After a crash, redelivered items can be merged with different neighbours and split at different points. Batch content, and a hash of it, is **not stable across a restart**. With batching and no partitioner, `num_consumers` is forced to 1. |
| Compression | `compression: gzip/zstd` compresses the whole marshaled body. For encoding extensions `IsCompressed=false`, so the object also gets `Content-Encoding`. Parquet compresses internally, so use none. |
| Credentials, endpoint, TLS | See "Deployment". |

## Requirements: what the stock path gives, and what it would take

"Stock" means awss3exporter plus an encoding extension plus config:
`sending_queue` on `file_storage`, `retry_on_failure.max_elapsed_time: 0`,
`timeout`, and `unique_key_func_name: uuidv7`. "Patch" means the
`key_mode: sequence` patch here, which is `awss3inline.patch`, +110/−2 lines,
plus `inline/log.go`. "Consumer" means the central side.

| Requirement (model) | Stock | A marshaler can add | Needs the patch (or a custom exporter) | Consumer must |
| --- | --- | --- | --- | --- |
| Nothing acked is lost; nothing leaves the queue uncommitted (`noPayloadLost`, `ackedImpliesCommitted`) | Yes, with `max_elapsed_time: 0` and a persistent queue: an item is deleted only after a nil export. With the default 300 s, data is dropped. | – | – | – |
| Commit implies data (`commitImpliesData`) | Trivially, as there is no separate commit. But there is also no commit: a consumer can't tell a finished object from an abandoned attempt, apart from multipart, which is invisible until complete. | – | The object *is* the commit (create-only slot). | – |
| Only committed rows are ingested (`onlyCommittedIngested`) | "Committed" is undefined: every object that exists is ingested, duplicates included. | – | Every slot object is committed by definition. | Read only slots in order. |
| Batch committed at most once per incarnation (F1, `epochNoDuplicatePayload`) | **No**: a retry gets a new key. [M]: 2 objects per slow PUT; 9 of 29 objects duplicated in the demo. | Content hash in the footer (done), so the consumer can deduplicate, but S3 still holds copies. | HEAD on a timeout or 412, and resend the same slot [M][Mo]. | – |
| Request ingested at most once end to end (`payloadIngestedAtMostOnce`, F1 across restarts, F4) | No. | The content hash makes it possible. | The content hash goes into `x-amz-meta-oscope-content`. | Check before insert on the content key under an exclusive lease, plus `insert_deduplication_token` [Mo: `noCheckCentral` fails]. A copy from a restart is in another epoch [M: 1 copy skipped in the demo]. |
| Stable request identity across restarts | **No, with `sending_queue.batch`** (re-batched). With batching before the queue (`batch` processor, losing its in-memory window on a crash) or no batching, the queue item is the request, and its protobuf hash survives the queue [M: `TestContentHashStableAcrossProtoRoundTrip`]. | Hash computation (10–12 ms per 10k spans [M], about 15% of Parquet encoding). | – | With post-queue batching, deduplicate by row identity (for example a row hash) rather than by batch. |
| Seal matches manifests (F3, `sealMatchesManifests`) | No seal. Completeness is unknowable; time partitions can gain objects at any time (a retry after a restart writes into the partition of *now*). | Can't: it doesn't know what was committed. | No writer seal. The epoch closes at the consumer's tombstone, and "the epoch holds slots 0..t−1" is exact [Mo: `noCommitAfterClose`, `consumerNeverSkipsCommitted`]. | Tombstone superseded, quiet epochs, and retry the tombstone until resolved. |
| Unique epoch per incarnation (PBT 1) | No epoch. | Can stamp its own process epoch in rows (done). | Epoch per lane per process, `{ms timestamp}-{32 random bits}`. A collision isn't unsafe, because slots are create-only; it only merges two logs. | – |
| Generation monotonic under a lock (PBT 2) | No generations; the partition comes from the clock at each attempt. | – | No generations and no clock in keys. The slot counter advances under the lane mutex, only after the slot is seen occupied. | – |
| Seal / retirement retried (PBT 3) | – | – | Nothing to seal at the writer. | The tombstone is retried each poll until it wins or loses to data [M: `Tombstone` returns `TombOpen` on an unknown outcome]. |
| Orphan rows (F2) | No tables; a failed PUT leaves nothing, or a whole object that is a duplicate. | – | Nothing can be orphaned: the only write is the commit. | – |
| Dedup window eviction (F4) | – | – | – | Check before insert under a lease with a time bound (`../model/S3NATIVE.md`), unchanged. |
| Zombie writer fenced (`noWriteFromFencedWriter`) | No notion of one. | – | The zombie's next PUT hits the tombstone (412), HEAD shows it, and it halts [M: `TestTombstoneFencesWriter`][Mo: `noHalt` fails]. | Write the tombstone. |
| No gap mistaken for loss (`gapNeverTakenForLoss`) | No sequence. | – | A live lane never leaves a gap; only a dead epoch's head can be free. | Never skip a free slot: race a tombstone into it [Mo: `skipGaps` fails]. |
| Consumer checkpoint by key | Impossible: random keys in time partitions, with late writes into old partitions. | – | `LIST StartAfter={epoch}/{ckpt−1}` returns exactly the new slots in order [M]. | A CAS'd checkpoint per producer (`If-Match`), as in S3NATIVE.md. |
| Per-batch stats without GET | – | In the footer [M: pyarrow]. ClickHouse's `ParquetMetadata` does **not** expose footer key-value metadata, but it does give `num_rows` and per-column min/max [M]. | The same fields go into `x-amz-meta-*`, and HEAD returns them [M]. | HEAD, or `ParquetMetadata` over a glob. |
| Credentials: EKS IRSA / Pod Identity, static keys, Roles Anywhere | Yes, through the SDK's default chain (below). | – | The patch reuses the exporter's client, so the same. | – |

## The manifest-less design (`inline/`, `awss3inline/`, `../model/s3Inline.qnt`)

### Layout

```
{s3_base_prefix}/{s3_prefix}/{signal}/{epoch}/{seq:020d}.parquet
    data slot:  the batch; x-amz-meta-oscope-{kind=data,epoch,seq,content,producer,signal,
                schema,rows,min-time,max-time,received}; the same in the Parquet footer
    tombstone:  0 bytes; x-amz-meta-oscope-kind=tomb (written by the consumer)
```

`epoch` is one log: one writer lane of one process incarnation. The
patched exporter runs `lanes` of them (default 1) and routes a request to
`lane = hash(content) mod lanes`, so a retry meets the lane, and the
unresolved slot, of its first attempt.

### Writer: one lane, one batch at a time (`inline/log.go`, `Log.Append`)

1. If the content hash was committed recently by this lane (a bounded map),
   return success. This is the queue's retry of a request whose earlier
   attempt was resolved as ours.
2. Encode for `(epoch, next)`, then `PUT If-None-Match: *` with metadata.
   - 200: committed; `next++`.
3. On a 412 or an unknown outcome (timeout, reset), HEAD the slot, on a
   detached 2 s deadline, since the exporter's deadline has usually passed:
   - our epoch and content hash: committed (resolved as ours);
   - free: resend the identical PUT. The in-flight copy and the resend can't
     both land;
   - another batch: it is committed (remember its hash); `next++` and
     re-encode for the next slot;
   - a tombstone: this epoch was closed by the consumer. Start a new epoch
     at slot 0 and append there;
   - 412, then HEAD 404: return an error. That means the store isn't
     read-after-write consistent, or the slot was deleted. Never guess.
4. An error return leaves the slot unresolved. The next call on the lane,
   whether a retry or another request, goes to the same slot, and step 3
   sorts it out. The model's `switchPayload` covers this.

The lane never abandons a slot, so a live epoch has no gaps. Nothing is
read at startup: a new epoch starts at slot 0 and no other writer uses its
keys.

### Consumer (`cmd/inlineconsume`, a prototype)

- `LIST {prefix}/` with a delimiter returns the epochs. Per epoch,
  `LIST StartAfter` the checkpoint returns the new slots, in order.
- For each consecutive slot:
  1. HEAD: the kind and the content hash.
  2. `SELECT count() … WHERE content_key = H`.
  3. If absent: `INSERT … SELECT *, H, _path FROM s3('<slot>')` with
     `insert_deduplication_token = H`.
  4. Advance the checkpoint.
- **A superseded epoch whose head slot is free and has been quiet for T:**
  `PUT If-None-Match: *` a tombstone there.
  - It won: the epoch is closed at that slot.
  - A late batch won: ingest it and try the next slot.
- T only affects liveness. A premature tombstone makes a live lane move to
  a new epoch; nothing is lost or duplicated.
- The checkpoint is in memory in the prototype. S3NATIVE.md's CAS'd
  checkpoint object, consumer lease and GC state apply unchanged, keyed by
  (epoch, slot).
- `s3_skip_empty_files = 1`, the default in 26.10, lets `s3()` globs over an
  epoch skip the zero-byte tombstones. With it off, a glob fails on them
  ("Parquet file too short: 0 bytes") [M].

### What replaces what

| Manifest-era piece | Here |
| --- | --- |
| Batch manifest (commit record, stats) | The slot object itself, with stats in `x-amz-meta-*` and the footer. |
| `_sealed.json` from counters | The consumer's tombstone. The epoch is exactly slots `0..t−1`, and counts come from LIST or `ParquetMetadata`. |
| Generations (rotation, the lock, clock reads) | Not needed for Parquet-only publishing: the slot order is the log. Retention and GC work by epoch and slot. |
| Batch id | `(epoch, seq)`; the envelope's `batch_id` column holds `seq`, and `producer_epoch` the epoch [M]. |
| Epoch per process | Epoch per lane per process, plus a new epoch after a halt. |
| Content-derived batch ids with an edge dedup token (model README's fix) | The content hash in metadata and the resolution by HEAD. |

### Costs [M] and [E]

- **Edge PUTs** [M]: one per batch, against two (object plus manifest) in
  `../parquetgo`. The demo made 21 inline PUTs for 20 requests, and the 21st
  was the redelivery after SIGKILL.
- **Extra requests on ambiguity**: one HEAD, sometimes a resend, and one 412.
- **Consumer** [E]: per object, one HEAD plus one count query plus the
  insert, where a manifest design has one GET plus the insert. Per poll,
  one LIST per open epoch. Tombstones cost one PUT per dead epoch.
- **Saving** [E]: at one batch per second per producer, dropping the
  manifest saves about 2.6 M PUTs a month, about $13 at AWS's
  $0.005/1,000.
- **Hashing** [M]: 10–12 ms per 10k spans (protobuf plus SHA-256), on a box
  with load 16.
- **Writer concurrency**: a lane appends serially, so throughput per lane
  is 1/PUT latency. At 10k-span batches of about 300 KB, about 50 ms per PUT
  on S3 [E] means about 200k spans/s per lane. Add lanes for more.

### What's lost, and the limits

- **Discovery is by LIST**, not by reading one manifest prefix. LIST returns
  no user metadata [E: S3 API], so stats cost a HEAD per object or one
  `ParquetMetadata` query over a glob. S3 event notifications can speed up
  discovery but aren't needed for correctness [E].
- **Only single-part PUT is used.** The appender calls `PutObject` directly
  (limit 5 GiB), not transfermanager, whose threshold is 16 MiB. Conditional
  `CompleteMultipartUpload` is **not atomic on SeaweedFS 4.47**
  (S3NATIVE.md, section 2). Modelled as `nonAtomicCond`, it lets a late batch
  overwrite a consumer tombstone, which then **skips a committed batch**
  [Mo]. Keep batches well under the 16 MiB threshold, or keep the direct
  PutObject, as here.
- **Deleting slots re-opens them.** `If-None-Match` succeeds on a deleted
  key. GC should delete a closed epoch's slots only after a bound on request
  lifetime, as in S3NATIVE.md's log-truncation discussion; that isn't
  modelled here. A late request re-creating a slot behind the checkpoint is
  garbage that the next GC LIST finds.
- **HEAD needs `s3:ListBucket`.** Without it, S3 answers HEAD on a missing
  key with 403 instead of 404 [E: AWS docs]. The writer then can't tell
  "free" from "error" and keeps retrying. Grant `s3:PutObject`,
  `s3:GetObject` and `s3:ListBucket` on the prefix.
- **A zombie can still commit until the tombstone.** Its batches before the
  tombstone are legitimately committed and ingested. Copies of a request in
  two epochs are deduplicated by content at the consumer, as in s3Native's
  per-epoch mutation. Readers that bypass central, such as a lakehouse
  reading the Parquet, must deduplicate by `oscope-content` or the envelope
  themselves.
- **Native tables** (`s3_plain_rewritable`) can't be inline. They keep
  S3NATIVE.md's log entry.

### Against the s3Native log design

| | s3Native (`../model/s3Native.qnt`) | inline (this) |
| --- | --- | --- |
| Commit | Data PUT, then a create-only log slot | One create-only data PUT |
| Orphans (F2) | Data without a log entry; swept after close | None |
| Log | One shared log per producer | One log per epoch and lane |
| Writer startup | Lease CAS, fence entry at the first free slot, and a replay of the log | Nothing: slot 0 of a new epoch |
| Fencing | The collision with the successor's fence entry | The consumer's tombstone in the dead epoch's head slot |
| Cross-restart duplicates | Removed at the edge by the replay | Committed once per epoch; removed by the consumer's content check, which both designs need anyway (F4) |
| Gaps | None: an append is never abandoned | None in a live epoch; the dead head is resolved by the tombstone race |
| Cost of a collision | A 200-byte log PUT | A full batch upload that gets 412. Rare: only on retries or tombstones, and only the loser pays |
| Native tables | Supported | Not supported |

## Deployment: credentials, endpoints and CA with awss3exporter [S]

`s3_writer.go` builds its client with `config.LoadDefaultConfig(ctx,
WithRegion(region), retry options)`. There are no credential fields in
the exporter's config, and no `tls` block. Everything else comes from the
SDK's default chain and environment:

| Environment | How | Notes |
| --- | --- | --- |
| **EKS, IRSA** | Nothing in the exporter config. The pod gets `AWS_ROLE_ARN` and `AWS_WEB_IDENTITY_TOKEN_FILE`, and the default chain calls AssumeRoleWithWebIdentity. | Set `region` (the exporter defaults to `us-east-1`). `role_arn` optionally chains one more AssumeRole (`stscreds`, an STS client from the same config). |
| **EKS Pod Identity** | Nothing in config. The agent injects `AWS_CONTAINER_CREDENTIALS_FULL_URI` and `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`, and the chain's container provider uses them. | As above. |
| **IAM Roles Anywhere** | A shared-config profile with `credential_process = aws_signing_helper credential-process --certificate … --private-key … --trust-anchor-arn … --profile-arn … --role-arn …`, selected with `AWS_PROFILE` (and `AWS_CONFIG_FILE`). The chain supports `credential_process`. Alternatively, `aws_signing_helper serve` provides an IMDS-style endpoint (`AWS_EC2_METADATA_SERVICE_ENDPOINT`) [E]. | Needs the helper binary and the certificate in the image. `role_arn` can chain from it. |
| **Nutanix Objects** (static keys, custom endpoint, private CA) | `endpoint: https://objects.example`, `s3_force_path_style: true`. Keys in `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, or a credentials file (`AWS_SHARED_CREDENTIALS_FILE`); there are no key fields. For a private CA, `AWS_CA_BUNDLE=/path/ca.pem` or `ca_bundle` in the profile. `region`: whatever the store signs with (often `us-east-1`). | Leave `disable_ssl` false: it forces `http://` through a middleware even when `endpoint` says https. The SDK sends CRC32 checksums by default, which some S3-compatible stores reject. `AWS_REQUEST_CHECKSUM_CALCULATION=when_required` and `AWS_RESPONSE_CHECKSUM_VALIDATION=when_required` turn that off. transfermanager is explicitly made to honour the client's setting. |

The patched exporter reuses exactly this client (`newS3Client`), so all four
work the same for `key_mode: sequence`.

**What the manifest-less design needs from the store:**

1. `If-None-Match: *` on single-part PutObject, decided **atomically**:
   under concurrency, exactly one of several creates of a key wins. Two
   failure modes are fatal:
   - a store that ignores the header, as MinIO did before
     RELEASE.2024-09-13, degrades to the `plainPut` mutation: overwrites and
     lost batches [Mo];
   - one that checks on arrival but writes later degrades to
     `nonAtomicCond`.
2. Read-after-write consistency for HEAD after 412, and for LIST.
   Otherwise the writer returns errors and retries (safe but stuck), and the
   consumer waits.
3. User metadata on PUT, returned by HEAD. With no metadata, the ETag
   against the MD5 of the body sent works for the writer's own-object check
   on single-part PUTs without SSE-KMS; the content hash would then have to
   go into the key or the footer.

- **AWS S3**, for EKS and Roles Anywhere: 1–3 are documented
  (conditional writes since 2024-08; strong consistency since 2020) [E].
- **SeaweedFS 4.47**: 1–3 are measured for single-part PUT, here and in
  `../s3cas`.
- **Nutanix Objects: unknown.** Whether it supports `If-None-Match: *` on
  PutObject at all, whether it is atomic under concurrent creates, whether
  HEAD and LIST are read-after-write, and how it treats `x-amz-meta-*` and
  CRC32 trailers. The S3-native work is checking conditional-write support.
  `probe/` is the acceptance test to run against it, with
  `INLINE_S3=https://…` and the CA and keys in the environment. It needs one
  more test from `../s3cas` (16 goroutines racing `If-None-Match` on one
  key) before trusting atomicity.
- **Fallback without conditional writes:** keep per-epoch sequence keys, but
  never put different content in a slot (a lane resends only the same
  batch), and close epochs by time, after a bound on request lifetime and
  zombie lifetime, instead of by tombstone. Safety then rests on that time
  bound [E, not modelled].

## Model results (`../model/s3Inline.qnt`, `s3Inline_test.qnt`)

The model has:

- epochs as separate logs, and S3 as a soup of in-flight requests that
  apply late, never, or after the client timed out;
- the writer's HEAD-based resolution, including `switchPayload` (the next
  request goes into an unresolved slot);
- zombies, restarts with queue redelivery, and consumer crashes;
- the consumer's check-before-insert and its tombstone race.

Domains: 2 payloads, 3 epochs, 4 slots per epoch.

Invariants:

- `payloadIngestedAtMostOnce`
- `epochNoDuplicatePayload`
- `onlyCommittedIngested`
- `noCommitLost` (create-only; this is also "commit implies data")
- `ackedImpliesCommitted` and `noPayloadLost` (success implies a commit, and
  no data loss)
- `gapNeverTakenForLoss`
- `consumerNeverSkipsCommitted` (with the tombstone, this is also "the seal
  matches")
- `noCommitAfterClose`

**Design, hostile environment** [Mo]:

- all nine hold together (`safety`) in `quint run` over 3,000 traces × 80
  steps with seed `0x5eed`, and again with seed `0x1`;
- `idealEnv` passes 1,000 traces;
- Apalache: `safety` holds for every execution up to 6 steps (149 s).
  A 10-step run (started 02:08) had checked every state up to 8 steps with
  no violation at 02:47 and was still on step 9 when this was written. Its
  output goes to the scratch `apalache10.txt`.

Every witness is reached:

- a commit; resolved as ours; a resend; a switched payload; learned another
  batch;
- a writer halted by a tombstone; an epoch closed; a tombstone lost to late
  data;
- an insert; a dedup hit; a cross-epoch copy;
- everything ingested; every superseded epoch closed.

**Mutations**, 500 traces × 60 steps per invariant (✗ = counterexample found):

| Instance | What it changes | ✗ (violated) | ✓ (held) |
| --- | --- | --- | --- |
| `plainPut` | Slots written with a plain PUT, as stock awss3exporter does | `noCommitLost`, `ackedImpliesCommitted`, `noPayloadLost`, `consumerNeverSkipsCommitted`, `noCommitAfterClose`, `payloadIngestedAtMostOnce`, `epochNoDuplicatePayload` | `onlyCommittedIngested`, `gapNeverTakenForLoss` |
| `nonAtomicCond` | The condition is checked on arrival and the write happens later (SeaweedFS multipart completion) | `noCommitLost`, `ackedImpliesCommitted`, `noPayloadLost`, `consumerNeverSkipsCommitted`, `noCommitAfterClose` | the other 4 |
| `retryNewKey` | Timeout or 412 means retry at a new slot (stock exporterhelper retry) | `epochNoDuplicatePayload`, `consumerNeverSkipsCommitted`, `noCommitAfterClose` (the gap breaks the tombstone close) | the other 6, including end to end, thanks to the consumer check |
| `noHalt` | The writer steps over a tombstone | `consumerNeverSkipsCommitted`, `noCommitAfterClose` | the other 7 |
| `skipGaps` | The consumer treats a dead epoch's free slot as lost | `gapNeverTakenForLoss`, `consumerNeverSkipsCommitted` | the other 7 |
| `noCheckCentral` | No content check before insert | `payloadIngestedAtMostOnce` (F4 and cross-epoch copies) | the other 8 |

Each ✗ was found within 0.1–28 s of sampling; each ✓ held over 500 traces.
Every mutation breaks something that the design keeps.

**Scenario tests**: `quint test s3Inline_test.qnt --main <module> --backend
typescript`, 16 tests, all passing.

- `inlineDesignTest` (9):
  - ambiguous PUT resolved as ours;
  - both orders of a switched slot;
  - crash redelivery deduplicated by the consumer;
  - zombie fenced by a tombstone;
  - tombstone lost to late data;
  - the consumer's own tombstone timing out;
  - a consumer crash between insert and checkpoint;
  - everything ingested and closed.
- One or two per mutation, each showing its counterexample:
  - `retryNewKeyTest`: a late copy committed twice, and a gap that breaks
    the tombstone close;
  - `skipGapsTest`
  - `noHaltTest`
  - `nonAtomicCondTest`
  - `plainPutTest`
  - `noCheckCentralTest`

**Faults covered**, per fault, with the scenario that shows it:

- ambiguous PUT (landed, answer lost): `ambiguousLandedResolvedOwnTest`;
- late PUT (lands after the client gave up): the switched tests and
  `tombLosesToLateDataTest`;
- lost PUT: `gapBreaksTombstoneCloseTest` under the stock retry;
- crash with redelivery: `crashRedeliveryDedupedByConsumerTest`;
- zombie writer: `zombieFencedByTombstoneTest`;
- consumer crash: `crashBetweenInsertAndCheckpointTest`;
- a retry of the same or another request into an unresolved slot: the
  switch tests.
- A clock step isn't a fault here: no key or decision depends on the clock.

**Caveats**:

- small domains, and the bounded runs aren't a proof;
- one lane per epoch, and lanes are independent logs;
- GC, deletion and log truncation aren't modelled;
- the consumer lease and CAS'd checkpoint are left to s3Native (one
  consumer here);
- consumer read-your-writes on central is assumed;
- the store's atomicity is an assumption (`ATOMIC_COND`), and the test on
  SeaweedFS covers single-part only.

## Test results [M]

Commands are in "Running it". Output is in `results/tests.txt`,
`results/demo.txt` and `results/demo-proxy.log`.

**S3 and ClickHouse assumptions** (`probe/`, SeaweedFS 4.47, ClickHouse 26.10.1):

| Check | Result |
| --- | --- |
| `PUT If-None-Match: *` with `x-amz-meta-*` and Content-Type, then a second create with other metadata | 200, then 412. HEAD returns the **first** object's metadata and `application/vnd.apache.parquet`. |
| Tombstone, then a data PUT into the same slot; data, then a tombstone | 412 for the data; the tombstone reports `DataWon`. |
| `LIST StartAfter` on zero-padded slots with gaps (0,1,2,5,9,10,11,100) from slot 3; delimiter LIST | `[5 9 10 11 100]` in order; epochs `[e1 e2]`. |
| PUT lands, response dropped (fault transport) | `Append` resolves it as ours with one HEAD: 1 object. A retry of the same request returns the same slot with no request (known). |
| PUT held past the deadline, retried, then the late copy lands | 1 object: the retry's resend won, and the late copy got 412. The next batch goes to slot 1. |
| Tombstone at slot 2, then the writer appends | The writer halts and commits at slot 0 of a new epoch. |
| ClickHouse `s3()` glob over an epoch with a tombstone | 100/200/300 rows, with `producer_epoch` and `batch_id` = slot; the tombstone is skipped (`s3_skip_empty_files=1`). With 0: `Parquet file too short: 0 bytes`. |
| `ParquetMetadata` format | Columns `num_columns num_rows num_row_groups format_version metadata_size total_*_size columns row_groups`: **no key-value metadata**. `num_rows` and the Timestamp min/max come from row-group statistics. |
| Footer key-value metadata after the footer rewrite | Read back by Go and by pyarrow 25.0.1 (`oscope-seq`, `oscope-rows`, …); pyarrow reads all 200 rows. |

**Stock against patched exporter, through the factory with exporterhelper
timeout (1 s) and retry** (`retrytest/`). A proxy holds the first PUT's
answer for 3 s and applies it at 0.5 s or 1.5 s:

| Exporter | The slow PUT lands… | Objects | What happened |
| --- | --- | --- | --- |
| stock | after the retry | **2** | Two random keys (`traces_525918618`, `traces_317672089`) with the same rows. |
| stock | before the retry | **2** | The same. |
| patched | after the retry | 1 | The retry's PUT to slot 0 won; the late copy got **412**. |
| patched | before the retry | 1 | The detached HEAD found our batch; no second PUT. |
| patched, two incarnations, same request | – | 2, in two epochs | The same `oscope-content`, so the consumer's check removes the second. |

**End to end in an ocb collector** (`collector/run-demo.sh`, `results/demo.txt`):

- Setup: OTLP/HTTP to a collector with both exporters, `parquet_encoding`,
  a `file_storage` persistent queue, `timeout: 5s` and
  `max_elapsed_time: 0`.
- The fault proxy answers every third PUT only after 8 s; those PUTs had
  landed by then.
- Sent 20 requests × 50 spans. SIGKILL after 3 s, restart on the same queue,
  then a third run with the faults off to drain.
- 50 PUTs in all, and 16 of their answers never reached the client.

| | Objects | Rows | Distinct spans |
| --- | --- | --- | --- |
| stock awss3exporter | 29 | 1,450 | 1,000 |
| awss3inline (`key_mode: sequence`, 2 lanes) | 21 in 3 epochs | 1,050 | 1,000 |
| central after `inlineconsume` | 20 inserted, 1 copy skipped, 2 epochs closed by tombstone | **1,000** | 1,000 |

The 21st inline object is the request in flight at the SIGKILL, committed
again by the next incarnation. The consumer's content check skipped it.

**Not tested**:

- real AWS S3;
- Nutanix Objects;
- MinIO or Ceph;
- versioned buckets;
- SSE-KMS;
- S3 event notifications;
- consumer GC;
- more than one consumer;
- throughput of the patched exporter beyond these small runs.

## Recommendation

1. **Don't build the commit protocol as a marshaler.**
   - Use an encoding extension only for the format, with the manifest's
     fields in the footer (`parquetencoding/`). That part works with stock
     awss3exporter today.
   - A marshaler can't commit, can't choose a key, and can't stop the
     duplicate objects that exporterhelper's retry creates.
2. **For the manifest-less edge, use a small awss3exporter patch, and treat
   it as a custom exporter until upstream wants it.**
   - The patch (`awss3inline.patch`) is +110/−2 lines: config, validation,
     `newS3Client`, and `ConsumeX` calling an appender.
   - The appender (`inline/log.go`, 285 lines with the lane pool) is
     self-contained.
   - Upstreamable pieces, in increasing order of ambition:
     - Content-Type and user metadata from the encoding extension (an
       optional method);
     - `if_none_match` and a content-derived unique key, which would make
       the stock retry idempotent within a partition;
     - `key_mode: sequence`, which is a protocol and less likely to be
       accepted.
   - Without upstream interest it is simpler to own one exporter,
     `parquetgo` plus the appender, than to carry a fork of awss3exporter.
3. **Collector config** for any of these:
   - `sending_queue` on `file_storage`;
   - `retry_on_failure.max_elapsed_time: 0`;
   - `timeout` at least the p99 PUT latency;
   - with the stock exporter, `unique_key_func_name: uuidv7`.
   - Avoid `sending_queue.batch` if the consumer deduplicates by request
     content. Batch before the queue and accept its in-memory window on a
     crash, or deduplicate by row identity.
4. **Consumer:**
   - follow epochs by `LIST StartAfter`;
   - check before insert on `oscope-content`, under S3NATIVE.md's lease and
     CAS'd checkpoint;
   - close superseded, quiet epochs with tombstones;
   - never skip a free slot.
5. **Before production:**
   - run `probe/` and a concurrent create-only race against **Nutanix
     Objects** and AWS;
   - if Nutanix lacks atomic `If-None-Match`, use the time-bounded fallback
     there or keep manifests;
   - keep batches under the multipart threshold, or keep direct PutObject;
   - grant `s3:ListBucket` for HEAD.
6. **Keep S3NATIVE.md's log for native chDB tables.** Inline commits are for
   single-object Parquet batches.

## Files

| Path | What |
| --- | --- |
| `inline/` | Format: slot keys, metadata names, content hash, and a Parquet footer rewrite that adds key-value metadata. `Log`/`Lanes` is the create-only appender with HEAD resolution and halt on a tombstone. There are also consumer primitives (`Epochs`, `After`, `Tombstone`) and unit tests plus a hash benchmark. |
| `parquetencoding/` | The encoding extension (`parquet_encoding`): the ptrace/plog Marshaler for stock awss3exporter, and `Marshal{Traces,Logs}Slot` plus `ObjectContentType` for the patch. |
| `awss3inline/`, `awss3inline.patch` | awss3exporter v0.161.0 (Apache-2.0) with `key_mode: sequence` and `lanes`, component type `awss3inline`. The patch is against the upstream files (import paths rewritten). |
| `probe/` | S3 and ClickHouse assumption tests. |
| `retrytest/` | Stock against patched exporter through factories, exporterhelper and a fault proxy. |
| `cmd/faultproxy/` | A proxy that applies PUTs at once and answers late. |
| `cmd/inlineconsume/` | The consumer prototype (ClickHouse HTTP). |
| `collector/` | `builder-config.yaml` (ocb v0.161.0), `config.yaml`, `run-demo.sh`. |
| `results/` | Raw test and demo output. |
| `../model/s3Inline.qnt`, `../model/s3Inline_test.qnt` | The model and its scenarios. |

## Running it

```sh
cd awss3
go test ./inline                                   # unit tests; -bench ContentHash for the hash cost
INLINE_S3=http://127.0.0.1:18333 INLINE_CH=http://127.0.0.1:18123 go test -v -count=1 ./probe ./retrytest
# collector: build with ocb into a scratch dir, plus the proxy and consumer
B=/tmp/awss3col; mkdir -p $B
sed "s#@AWSS3@#$PWD#; s#@OUT@#$B#" collector/builder-config.yaml > $B/builder.yaml
go run go.opentelemetry.io/collector/cmd/builder@v0.161.0 --config $B/builder.yaml
go build -o $B/faultproxy ./cmd/faultproxy && go build -o $B/inlineconsume ./cmd/inlineconsume
B=$B collector/run-demo.sh
# model
cd ../model
quint test s3Inline_test.qnt --main inlineDesignTest --backend typescript
quint run s3Inline.qnt --main s3InlineDesign --invariant safety --max-steps 80 --max-samples 3000 --backend typescript
java -jar ~/.quint/apalache-dist-*/apalache/lib/apalache.jar server --port=8831 &   # quint verify hung waiting for its own server here
quint verify s3Inline.qnt --main s3InlineDesign --invariant safety --max-steps 6 --server-endpoint localhost:8831
```

Everything writes under bucket `otel`, prefix `s3inline/`. The SeaweedFS
here has no free volume slots for new buckets.
