# otap-rs: ClickStack Parquet on S3 from the Rust otap-dataflow engine

A lean otap-dataflow pipeline: upstream's OTLP receiver feeding this crate's
exporter, `urn:otel:exporter:s3pq`. The exporter writes one ClickStack-shaped
Parquet object per request. It commits with the manifest-less protocol of
[../awss3](../awss3/README.md) and [../model/s3Inline.qnt](../model/s3Inline.qnt),
and acknowledges upstream only once the commit is resolved. The same
directory holds a Rust port of the central consumer
([../awss3/cmd/inlineconsume](../awss3/cmd/inlineconsume)), and it is
measured against [../parquetgo](../parquetgo/README.md) and the Go OTAP
variants of [../otap](../otap/README.md).

## Recommendation

**Use this Rust path where the edge can run the Rust engine and doesn't
need Go collector components. It beats parquetgo on edge CPU, memory,
bytes and S3 requests, and matches it row for row. Its binary is about 3×
larger. Keep parquetgo where the edge is, or must stay, a Go collector.**

- **It wins on the edge [M]:**
  - **CPU:** 45 ms per 10k-span batch against 73 ms for parquetgo (−38%),
    and 33 ms per 10k-log batch against 52 ms (−37%). Both are in-process,
    publishing to S3.
  - **Whole process:** the full engine process (OTLP/HTTP receive, flatten,
    encode, commit) costs 47 / 34 ms. That is still below parquetgo's
    in-process cost, which excludes receiving.
  - **Memory:** 38 MB peak RSS in-process against 108 MB, and 58 MB for the
    whole engine process.
  - **Bytes and requests:** half the bytes per batch (131 KB against 265 KB
    for traces), and one S3 PUT per batch against two.
- **Central is unchanged [M].**
  - The central ingest is the same `INSERT … SELECT FROM s3()`, at the
    same server CPU within about ±10%. That is inside the run-to-run ranges
    of this shared server. It reads half the bytes.
  - The rows are identical to parquetgo's on testgen and on the hostile
    dataset: the same checksums, `EXCEPT` empty in both directions, the same
    inferred schema.
- **The win comes from reading OTLP bytes directly, not from OTAP.**
  - The exporter walks the OTLP protobuf through otap-dataflow's zero-copy
    views, with no pdata or OTAP decoding.
  - Converting to OTAP record batches first, with upstream's encoder, costs
    about 26 ms more per traces batch (71 ms in total). It also rejects a
    whole batch when a map or slice value holds invalid UTF-8.
  - An edge that already receives OTAP walks the records directly, at
    49 / 35 ms (the "OTAP input" column). That is 4× cheaper than the Go
    OTAP flattener's 213 / 144 ms.
- **Keep parquetgo when:**
  - the edge is an `otelcol-contrib` build, or needs processors, receivers
    or extensions that only the Go collector has. The Rust engine is a
    different binary with its own config and operations;
  - (metrics are no longer a reason: both now publish the contrib
    clickhouseexporter's five metrics tables, row for row; see
    [Metrics](#metrics));
  - build and supply-chain weight matter more than edge CPU. See
    [Build and footprint](#build-and-footprint).
  - maturity matters: otap-dataflow is pre-1.0, and this exporter is a
    spike-quality component on top of it. parquetgo has had more exposure
    (pyarrow, Spark, chDB).
- **Added since, each with its section below [M]:**
  - **Metrics default to layout B** of ../metrics-layout (series table +
    narrow points, the series id computed at the edge): the Go prototype's
    rows and ids exactly, the views equal to contrib's rows, **1.8 µs of edge
    CPU per point against 5.0** for the ClickStack tables, 4 objects per
    request instead of 5 (gauge and sum merged). No smaller on the wire than
    this crate's ClickStack objects: its gain is central, as the spike found.
    See [Metrics layout B](#metrics-layout-b-the-series-table-the-default).
  - **Edge durability:** upstream's durable buffer works in this pipeline;
    requests acked before a SIGKILL are all committed after the restart,
    for +35% edge CPU and 2× the request bytes written to local disk.
  - **Credentials:** `AWS_CA_BUNDLE`, `HTTPS_PROXY` / `NO_PROXY` (already
    honoured, now tested), `AWS_PROFILE` and the shared files, AssumeRole
    chaining (`role_arn`), SigV4 signed here.
  - **Inputs:** upstream's OTAP receiver works end to end with the Go
    otelarrow producer, after decoding its transport-optimized ids (a bug
    here that blew up memory); OTLP/gRPC costs the same as HTTP; OTAP input
    costs the edge 14–55% more than OTLP.
- **The saving isn't where the money is.** Central ingest is the larger
  cost ([../bench/central/REPORT.md](../bench/central/REPORT.md)), and both
  paths leave it where it is. At 500 producers × one batch per 10 s, 28 ms
  saved per traces batch is about 1.4 cores fleet-wide [E].

Labels: **[M]** measured here · **[D]** from docs or source, not executed ·
**[E]** estimate.

The environment:

- one 4-vCPU box, shared; load average 0.7–1.2 during the edge benchmarks;
- SeaweedFS on :18333, bucket `otel`, prefix `otap-rs/`;
- ClickHouse server 26.10.1.618 on :18123, with private databases dropped after each run;
- Rust 1.98.1;
- upstream otel-arrow `main` at 5db8358 (2026-09-24), which is v0.57.0 plus fixes;
- arrow-rs/parquet 58.4;
- object_store 0.13.2;
- quint 0.32.0.

## Comparison

Medians of 3 processes, each doing 3 warm-up and 30 timed batches of
10,000 testgen rows, the accounting of parquetgo's `pubbench`
(`results/bench.md`, `results/bench.jsonl`). parquetgo was re-measured in the
same interleaved runs, and reproduced ../otap's baseline (73 / 52 ms
against 72 / 52 ms there). So the "Go OTAP" column, copied from
[../otap/README.md](../otap/README.md), is comparable, although it was
measured on a busier day.

| | **parquetgo** (baseline) | **Rust, OTLP direct** (this, default) | Rust, via OTAP (upstream OTLP→OTAP, then walk) | Rust, OTAP input (records already built) | Go OTAP option b (from ../otap) |
|---|---|---|---|---|---|
| Edge CPU per batch, traces / logs, to S3 [M] | 73 / 52 ms | **45 / 33 ms** | 71 / 47 ms | 49 / 35 ms | 213 / 144 ms from OTAP; +188 / 176 ms for OTLP→OTAP |
| Whole edge process (OTLP/HTTP receive + export), traces / logs [M] | – (not measured as a collector) | **47 / 34 ms** | 75 / 47 ms | – | – |
| Of which: flatten / encode / commit, traces [M] | – | 18 / 26 / 6 ms | 44 / 27 / 6 ms | 23 / 25 / 6 ms | 29 decode + 44 flatten + ~140 write |
| Peak RSS [M] | 108 / 151 MB | **38 / 32 MB** in-process; 58 / 51 MB whole process | 44 / 35 MB; 62 / 54 MB | – | 183 MB |
| Binary | 15.7 MB static (parquet-go + AWS SDK, credential chain) [D: ../parquetgo] | 44 MB (thin LTO, stripped; links glibc) | same binary | same | 50 MB (Go, everything) |
| Bytes per 10k spans / logs [M] | 265 / 155 KB (all blooms); 174 / 97 KB without | **131 / 76 KB** (bloom on TraceId); 115 / 68 KB without; 196 / 121 KB with all blooms | same as direct | same | 354 / 212 KB |
| S3 requests per batch [M] | 2 PUTs (object + manifest) | **1 create-only PUT**; +1 HEAD only on a 412 or no answer | 1 PUT | 1 PUT | 2 PUTs |
| Central CPU, 10 batches, traces / logs, default settings [M] | 265 / 197 ms (300 / 206 in ../otap) | 318 / 172 ms. A 9-run re-run of traces gave 287 against 259. Single-threaded: 291 / 193 against 294 / 211. **Parity within about ±10%** | same objects | same | 370 / 194 ms |
| Central bytes read, 10 batches, traces / logs [M] | 2.71 / 1.59 MB | 1.34 / 0.78 MB | same | same | – |
| Rows vs parquetgo, testgen + hostile [M] | reference | **identical**: checksum, EXCEPT both ways, schema, central insert | testgen identical (after patch 0002); **hostile batch rejected** by the OTLP→OTAP conversion | as via OTAP | equal after sorting; OTAP losses |
| Ack / commit | manifest after object | ack after the create-only commit resolves (HEAD on 412 / no answer; tombstone halts) | same | same | – |

Arrow IPC instead of Parquet, the same rows (local) [M]: 28 / 14 ms CPU but
1,021 / 661 KB per batch, 8× the Parquet bytes. As in ../otap, it isn't
worth it for S3 transfer.

## What was built

```
otap-s3pq (one binary)
  receiver:otlp (upstream core-nodes; gRPC + HTTP, wait_for_result)
  receiver:otap (upstream; OTel Arrow gRPC streams; configs/edge-otap.yaml)
    [-> processor:durable_buffer (upstream Quiver WAL + segments; configs/edge-durable.yaml)]
    -> exporter:s3pq (this crate)
         metrics       layout B by default (series.rs): points objects + a series object for
                       series new this hour, announced only once it commits; or the ClickStack tables
         content key   BLAKE3("{signal}\0" + the OTLP request bytes), 128 bits hex
         flatten       otap-dataflow's view traits: RawTraceData / RawLogsData (OTLP bytes,
                       zero-copy) or OtapTracesView / OtapLogsView (OTAP records), one walker
         encode        arrow-rs ArrowWriter -> Parquet: one row group, zstd 3, dictionary
                       (not on the near-unique columns), page statistics + page index, bloom
                       filter on TraceId sized for the batch, footer key-value = the batch
                       description; no ARROW:schema
         commit        lane: PUT If-None-Match: * at {prefix}/{signal}/{epoch}/{seq:020d}.parquet,
                       x-amz-meta-oscope-*; HEAD on 412 / no answer; resend / learn / halt
         ack           upstream ACK once the commit resolves; NACK (retryable) while unresolved;
                       NACK permanent (400) for undecodable input
consume (a second binary): the central consumer (port of inlineconsume + FASTPATH rules)
encbench: the in-process edge benchmark (pubbench's accounting)
```

- **Row shape and rendering** (`src/flatten.rs`, `src/render.rs`, `src/schema.rs`).
  - The spec is parquetgo's `walk.go` and `schema.go`: ClickStack
    `otel_traces` / `otel_logs` columns with plain types, plus the envelope
    (`producer_id`, `producer_epoch`, `batch_id` = the slot's seq,
    `row_ordinal`, `received_at`, `schema_version`).
  - Values render as `pcommon.Value.AsString` does:
    - `Server` / `Ok`, not `SPAN_KIND_SERVER` / `STATUS_CODE_OK`;
    - `5`, not `5.0`; `1e+21` and `1e-7`; `NaN` / `Infinity`;
    - base64 for bytes;
    - Go `encoding/json` for maps and slices: keys sorted bytewise,
      duplicates last-wins, `\ufffd` for invalid bytes, `\u2028` escaped,
      no HTML escaping, and **an empty string for a map or slice holding
      NaN or ±Inf**, as Go's `json.Encoder` fails;
    - hex ids, with zero ids empty; durations wrap as uint64.
  - Strings stay bytes, invalid UTF-8 included. The arrays are Arrow
    `Binary`, and the writer gets the published Parquet schema with STRING
    annotations (`ArrowWriterOptions::with_parquet_schema`). So no invalid
    `&str` ever exists, and the file is typed like parquetgo's.
- **Commit protocol** (`src/proto.rs`, sans-IO; `src/runner.rs`, the I/O loop).
  - `Lane` is `../awss3/inline/log.go`'s `Log.Append` as a state machine:
    Idle → Ready → Waiting → (Committed | Head → own / free: resend /
    another batch: learn, next slot / tombstone: halt).
  - Epochs are `YYYYMMDDTHHMMSS.mmmZ-<8 hex>`, one per lane per incarnation.
    A new one is taken after a halt.
  - The encoded object is kept for the slot, so a resend, or a retry of the
    same request into the same slot, is byte-identical. The content key is
    stable across processes, because it hashes the request.
  - A bounded map of content keys found committed (own or others') acks a
    retried request without any request to S3.
- **Exporter node** (`src/exporter.rs`).
  - A local (thread-per-core) otap-dataflow exporter with N lanes per
    signal. A request goes to lane `hash(content) mod N`, so a retry meets
    its own unresolved slot.
  - Up to 2N commits are in flight.
  - Shutdown drains in-flight commits until the deadline.
- **S3** (`src/store.rs`): object_store 0.13 `AmazonS3`.
  - `PutMode::Create` sends `If-None-Match: *`.
  - `Attribute::Metadata` is sent as `x-amz-meta-*`.
  - HEAD is `get_opts(head: true)`, which returns the user metadata.
  - A custom endpoint means path-style addressing and `allow_http`.
  - `ca_bundle` adds extra roots.
  - Credentials: see [Credentials](#credentials-and-deployment).
- **Consumer** (`src/bin/consume.rs`, `src/consumer/`, `src/central.rs`):
  a fleet of workers sharing the lanes through a lease and a checkpoint per
  lane, CAS'd on S3; several committed objects per `INSERT … SELECT FROM
  s3()`, verified against an aggregating projection `content_key →
  count()`; dead epochs closed by tombstones; a separate GC step. See
  [Consumer](#consumer). (The prototype it replaces followed one epoch at a
  time with one statement per object and a local state file; its flags
  still work.)

## Upstream: what was changed, what was found

Upstream is used at a pinned commit (`UPSTREAM`), prepared by
`scripts/fetch-upstream.sh`: a shallow clone, plus `patches/*.patch`, linked
at `.upstream`. The crate's `Cargo.lock` started as upstream's, so shared
dependencies resolve to what upstream tests.

| Patch | Why |
|---|---|
| `0001-pdata-depend-on-datafusion-leaf-crates.patch` | `pdata` depends on the whole `datafusion` 53 crate for two types, `ScalarValue` and `ColumnarValue`, and there is no feature to turn it off. The patch depends on `datafusion-common` and `datafusion-expr-common` instead. This build's graph shrinks from 424 to 395 crates, and datafusion from 25 crates to 2. Behaviour is unchanged. |
| `0002-otap-views-u32-parent-id-dictionary16.patch` | **Bug:** `views/otap/common.rs` `build_attribute_index_u32` accepts `UInt32` and `Dictionary(UInt8, UInt32)` parent ids, but upstream's own OTLP→OTAP encoder writes `Dictionary(UInt16, UInt32)` for span event and link attributes. `OtapTracesView` then returns **no attributes for any event or link**. It showed as 750 of 3,000 testgen spans differing, in `Events.Attributes` and `Links.Attributes` [M]. The patch uses `MaybeDictArrayAccessor`, as the u16 variant does. `tests/otap_view.rs` prints the encodings. |

Found upstream, not patched here:

- **The OTLP→OTAP conversion rejects a batch with invalid UTF-8 inside a map
  or slice value** ("error serializing value as CBOR: Invalid UTF-8") [M].
  It fails loudly, unlike the Go library, which drops the batch silently
  (../otap finding 1). It still means an OTAP-first edge can't carry the
  hostile dataset, where the direct path stores it byte for byte.
- **The behaviours listed for upstream's exporters are avoided by not using
  them** [D, verified by the row comparison]:
  - the parquet exporter (no ack or nack, batches held for minutes, the
    resource/scope id offsets);
  - the ClickHouse exporter's rendering (`SPAN_KIND_SERVER`,
    `STATUS_CODE_OK`, `5.0`).

  This exporter acks per batch after the commit, has no id offsets (it
  writes flat rows), and renders as contrib does.
- **object_store has no `credential_process`.** This crate adds a
  provider for it (below).
  - object_store reads IMDS's endpoint from `AWS_METADATA_ENDPOINT`, not the
    SDKs' `AWS_EC2_METADATA_SERVICE_ENDPOINT`; this crate maps one onto the
    other.
  - A gotcha: `with_client_options` replaces `allow_http` set earlier
    through `with_allow_http`. The first build failed with "URL scheme is not
    allowed".
- **The OTAP receiver hands on records with transport-optimized ids**
  (delta-encoded parent ids), and the views don't decode them: a consumer
  must call `decode_transport_optimized_ids()` first, as the parquet
  exporter does. This crate didn't, which went unnoticed until a real OTAP
  sender was used ([Inputs](#inputs-otap-end-to-end-otlpgrpc-m)).
- **The OTAP receiver closes the whole stream on a batch it can't decode**
  (invalid UTF-8 in an Arrow `Utf8` column), instead of NACKing that batch.
- **A NACK's status matters.** The OTLP receiver maps a permanent NACK
  without `NackCause::Refused` to HTTP 500 / INTERNAL. This exporter marks
  undecodable input `Refused` (400 / INVALID_ARGUMENT), so a client doesn't
  retry it. An unresolved commit is a plain NACK (503 / UNAVAILABLE): retry.
- **Quint's Rust evaluator (v0.6.0)** labels the initial state of about 40%
  of `quint run --mbt` traces `step` instead of `init`. It only ever does this
  for state 0; the TypeScript backend labels them all `init`. The model-based
  test's driver maps `step` to `init`.

## Correctness [M]

`scripts/correctness.py` does this on the ClickHouse server:

- the same datasets go through the pipeline as OTLP/HTTP (`otlpsend`) and
  through parquetgo, the reference (`otlpgen -ref`);
- the datasets are 3,000 testgen spans and logs, and 700 spans and logs of
  `../parquetgo/compare/nasty.go`: invalid UTF-8, NULs, NaN/±Inf in maps,
  100 KB strings, extreme ints and timestamps, every value type, empty
  values, a non-string `service.name`, zero ids, negative durations;
- the checks are those of `../parquetgo/compare/correctness_test.go`.

The envelope columns that name the run (`producer_id`, `producer_epoch`,
`batch_id`, `received_at`) differ by construction. They are checked
separately: producer, the epoch in the key, `batch_id` = seq, and a recent
`received_at`. `row_ordinal` and `schema_version` are compared with the
content.

Every check passed on the direct path: 28 of 28. The via-OTAP path
passed all 14 checks on testgen, after patch 0002. Before the patch, its
traces differed in 750 rows (`Events.Attributes` and `Links.Attributes`
empty). It can't carry the hostile batches: upstream's conversion rejects
them, and the exporter answers 400 (`results/correctness.txt`).

| Check, per signal × dataset | OTLP direct: traces testgen, traces hostile, logs testgen, logs hostile | Via OTAP |
|---|---|---|
| count + sum(cityHash64(content, row_ordinal, schema_version)), explicit structure | equal ×4 (for example traces testgen `3000 2144364149278328350` on both sides) | equal ×2 (testgen); hostile: rejected |
| the same, with schema inference | equal ×4 | equal ×2 |
| `DESCRIBE s3(…)` (inferred schema, envelope included) | identical ×4 | identical ×2 |
| rows `EXCEPT` in both directions | 0 ×8 | 0 ×4 |
| envelope | producer, epoch = the key's, batch_id = the slot, ordinals 0…n−1, schema 1 | same |
| `INSERT … SELECT` into the central-typed tables (LowCardinality, `Map(LowCardinality(String), String)`, Nested), then checksums | equal ×4 | equal ×2 |

`tests/determinism.rs` [M] covers re-encoding:

- the same request, re-encoded for the same slot, gives byte-identical
  objects and the same content key (testgen and hostile);
- reusing the encoder's buffers changes nothing;
- the OTLP and OTAP input paths give identical columns for testgen.

## Fault tests [M]

`scripts/faults.sh` runs the whole chain:

- the sender resends until it gets a 2xx, as a collector exporter with
  `retry_on_failure` does;
- `otap-s3pq` (OTLP/HTTP, `wait_for_result`, `put_timeout: 1s`);
- `tools/cmd/faultproxy2`;
- SeaweedFS;
- the Rust consumer, into a private database.

Six distinct 10k-span requests per scenario (`results/faults/`):

| Scenario | What the proxy / harness does | Edge counters | S3 objects | Central after the consumer |
|---|---|---|---|---|
| Ambiguous PUT | every 2nd PUT applied, answer held 3 s (> put_timeout) | 3 committed, **3 resolved as ours by HEAD**, 0 resent | 6 | **60,000 rows, 6 contents** |
| Slow PUT, then retry; the late copy lands after | every 2nd PUT held 2.5 s before it reaches S3 | 6 committed, **5 resent after HEAD found the slot free**; the late copies got 412 | 6 | **60,000 / 6** |
| Dropped PUT | every 3rd PUT answered 503 after 200 ms, never applied | object_store's own retry (same key, same bytes, still create-only) succeeded; 0 HEADs | 6 | **60,000 / 6** |
| Crash and restart | every 2nd PUT applied with its answer held 8 s; the edge is SIGKILLed after 3 s and restarted; the sender resends what it had no 2xx for | new epoch; the resent request commits again there | 7 (6 + 1 copy in the new epoch), 1 tombstone | **60,000 / 6**: 1 cross-epoch copy skipped by the content check; the dead epoch closed by a tombstone at its first free slot |
| Zombie writer | edge A keeps running after edge B starts (same producer); the consumer uses `--quiet 1s` | A's next PUT hits the tombstone the consumer put at the head of A's old epoch: **halted=1**, then A continues in a new epoch | 6 in 3 epochs, 2 tombstones. The second closed B's epoch once A's new one superseded it: premature, but safe, as B would move to a new epoch on its next PUT | **60,000 / 6** |

No batch was lost or duplicated in central in any scenario. The only
duplicate objects are the ones the design expects: the same request
committed once in each of two incarnations.

## Model-based testing (quint-connect) [M]

`tests/mbt_s3inline.rs` checks `proto::Lane` and `proto::Consumer` against
[../model/s3Inline.qnt](../model/s3Inline.qnt) (instance `s3InlineDesign`)
with [quint-connect](https://crates.io/crates/quint-connect) 0.1.2.

- The environment is hostile: ambiguous, late and lost S3 writes, zombie
  writers, and consumer crashes.
- quint-connect runs `quint run --mbt` on the model, which records each
  step's action and its nondeterministic picks. It then replays every trace
  against a driver.
- **The driver** maps every model action to the implementation, injecting
  the outcome the model chose:

  | Model action | Implementation call |
  |---|---|
  | `startPush` / `switchPayload` | `Lane::start` |
  | `send` | `Lane::sent`, request in flight |
  | `apply` | S3 applies a create-only PUT |
  | `lose` | the request vanishes |
  | `receive` | `Lane::on_put(Ok \| Exists)`, then the HEAD on a 412 |
  | `timeout` | `on_put(Unknown)` |
  | `resolve` | `on_head` |
  | `newIncarnation(zombie)` | a new lane; the old one is killed or kept |
  | `cCheck`, `cInsert`, `cAdvance` | `Consumer::check` with central's count, `insert`, `advance` |
  | `cSeeTomb`, `cTomb`, `cTombReceive`, `cTombTimeout`, `cTombResolve` | the tombstone race |
  | `cCrash` | `Consumer::crash` |

  S3 is an in-memory bucket with atomic create-only puts. It holds the same
  user metadata the runner writes, read back through `Slot::from_meta`.
- **After every step** the implementation's state is projected onto the
  model's variables and compared. The projection covers:
  - what each slot of each epoch holds;
  - each writer's alive flag, phase, next slot and batch;
  - the queue and its acks, and the lease;
  - the requests and answers in flight;
  - the consumer's checkpoints, closed epochs, phase, slot and
    `sawPresent`;
  - central's count per payload.

  The history variables `everLog` and `events` are the only ones left out.
- **No wrapper module was needed.** Every action in `step` is named, and
  the instance's constants are fixed. `s3Inline.qnt` isn't modified.
- **Result** (seed `0x5eed`, `results/mbt/mutants.txt`):
  - 300 traces of up to 60 steps, 16,981 steps in all, **pass**;
  - every design event is reached: resolved-own, resend, switched slot,
    learned-other, halted, tombstone closed, tombstone lost to data,
    dedup hit, consumer crash and zombie;
  - 37 s.
- **Mutants** (`OTAPRS_MUTANT`; the same code with one protocol rule
  broken, `proto::Mutation`), each caught at the first step where it
  diverges:

  | Mutant | Rust change | Fails at | Model says / implementation says |
  |---|---|---|---|
  | `retry_new_key` | a 412 or timeout moves to the next slot without reading it (stock awss3exporter + exporterhelper) | trace 1, step 36, `timeout` | writer `WUnresolved`, slot 1 / `WReady`, slot 2 |
  | `no_halt` | a tombstone in our slot is skipped | trace 4, step 58, `receive` of a 412 on a tombstone | `WHalted`, slot 1 / `WReady`, slot 2 |
  | `no_check_central` | the consumer doesn't check central before inserting | trace 2, step 39, `cCheck` | `sawPresent: true` / `false` |

  These are the model's own mutation instances (`retryNewKey`, `noHalt`,
  `noCheckCentral`) applied to the code. The model shows each one breaking
  an invariant.
- **What it covers, and what it doesn't:**
  - It tests the protocol cores, where every decision is made. The async
    runner around them is thin:
    - `put → on_put → head → on_head` with timeouts;
    - it is covered by `runner::tests`, with an in-memory store that
      applies-and-loses, drops and holds requests;
    - and by the SeaweedFS fault tests above.
  - The consumer's ClickHouse statements are covered by the fault tests and
    the correctness run, not by the model.

**The Quint Rust evaluator.**

- `quint run` defaults to the Rust backend, and quint 0.32 downloads
  evaluator v0.6.0 from GitHub releases. The network policy here returns 403
  for that.
- It was **built from source instead**:
  - `git clone --branch evaluator/v0.6.0 https://github.com/informalsystems/quint`
    (commit 513910b);
  - `cargo build --release` in `evaluator/`, in 1 min;
  - the binary copied to `~/.quint/rust-evaluator-v0.6.0/quint_evaluator`,
    where quint looks before downloading.
- With it, quint-connect works unmodified, since it calls `quint run`
  without `--backend`.
- Where neither the download nor a build is possible, put a `quint` shim
  first on PATH that adds `--backend typescript`. That backend was checked
  to produce the same `--mbt` traces, with every initial state labelled
  `init`.
- If github.com becomes reachable, the download works as normal.

```sh
cargo test --release --test mbt_s3inline -- --nocapture                    # quint on PATH
QUINT_SEED=0x5eed QUINT_VERBOSE=1 cargo test --release --test mbt_s3inline -- --nocapture
OTAPRS_MUTANT=retry_new_key QUINT_SEED=0x5eed cargo test --release --test mbt_s3inline  # fails; also no_halt, no_check_central
```

## Credentials and deployment

`tests/creds.rs` checks each mode against the stand-ins parquetgo used
(`../parquetgo/compare/cmd/credstubs`), with no keys in config
(`results/creds.txt`). Each check is:

- a create-only PUT with user metadata;
- a second create that must get 412;
- a HEAD that must return the metadata;
- the stand-in's log, showing the credential exchange.

| Mode | How, in this exporter | Result [M] |
|---|---|---|
| **EKS IRSA** | `AWS_ROLE_ARN` + `AWS_WEB_IDENTITY_TOKEN_FILE`, as the EKS webhook injects them → object_store's web-identity provider → STS `AssumeRoleWithWebIdentity` | ✓. The stand-in STS got `AssumeRoleWithWebIdentity` with the role ARN. object_store **only talks https to STS**: it refuses an `http://` `AWS_ENDPOINT_URL_STS`. So the test reached the default `https://sts.us-east-1.amazonaws.com` through the stand-in's CONNECT proxy, which has a private CA (`AWS_PROXY_URL` + `ca_bundle`). |
| **EKS Pod Identity** | `AWS_CONTAINER_CREDENTIALS_FULL_URI` + `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE` | ✓. The token file's content arrived as `Authorization`. |
| **Nutanix Objects**: static keys, custom endpoint, path-style, private CA | `s3.url: https://objects.example/bucket/prefix`, `access_key_id` / `secret_access_key`; the CA in `ca_bundle`, or `SSL_CERT_FILE` (read by rustls-native-certs) | ✓ through a TLS proxy with a private CA, by either route. Without the CA the PUT fails (`UnknownIssuer`). **Whether Nutanix decides `If-None-Match: *` atomically is unknown:** run `../awss3/probe` against it first. |
| **IAM Roles Anywhere, `aws_signing_helper serve`** | `AWS_EC2_METADATA_SERVICE_ENDPOINT=http://127.0.0.1:9911`, the SDKs' name for the variable, which this crate maps onto object_store's `AWS_METADATA_ENDPOINT` → IMDSv2 | ✓: token PUT, role list, then credentials |
| **IAM Roles Anywhere, `credential_process`** | `s3.credential_process: "aws_signing_helper credential-process --certificate … --role-arn …"` → `store::ProcessCredentials`, which is this crate's (object_store has none). It is cached until 5 minutes before `Expiration`. | ✓. The helper ran once for 3 requests. |
| static keys, plain http (SeaweedFS, MinIO) | `access_key_id` / `secret_access_key` | ✓ |

**What `src/creds.rs` adds** (the same test, `results/creds.txt`: 19 modes
pass, plus the signer check) [M]:

| Mode | How | Result [M] |
|---|---|---|
| `AWS_CA_BUNDLE` | read when `ca_bundle` isn't set (then the profile's `ca_bundle`); added to the roots of the S3 and STS clients | ✓ through the private-CA TLS proxy |
| `HTTPS_PROXY` / `NO_PROXY` | **already honoured**: reqwest's system proxy applies whenever no `proxy_url` / `AWS_PROXY_URL` is set, with the SDKs' contract (`HTTPS_PROXY` for https, `HTTP_PROXY` for http, `NO_PROXY`). The old "ignores HTTPS_PROXY" line here was wrong. Now tested | ✓ S3 requests tunneled (a CONNECT proxy in the test logs `CONNECT 127.0.0.1:18903`); with `NO_PROXY=127.0.0.1` nothing reaches the proxy; STS through `HTTPS_PROXY` too (last row) |
| `AWS_PROFILE`, shared files | `AWS_CONFIG_FILE` / `AWS_SHARED_CREDENTIALS_FILE` (default `~/.aws/…`), or `s3.profile`: static keys, `credential_process`, `role_arn` + `source_profile` (chained, any depth) or `credential_source`, `role_arn` + `web_identity_token_file`, `external_id`, `role_session_name`, `region` (for STS), `ca_bundle`. SSO profiles are refused with a message. Order as aws-sdk-go-v2: a named profile, then env keys, env web identity, the `default` profile, then container / IMDS | ✓ `AWS_PROFILE=edge` (credentials file); ✓ the default profile without `AWS_PROFILE`; ✓ `credential_process` in the config file (the helper ran once); ✓ `sso` refused; ✓ `chained` → STS `AssumeRole` with the role ARN, session name and `ExternalId` |
| **AssumeRole chaining** | `s3.role_arn` (+ `role_session_name`, `external_id`, `sts_endpoint`) on top of whatever the base is: config keys, `credential_process`, a profile, or object_store's own chain. STS `AssumeRole` is signed here (SigV4), cached until 5 min before `Expiration`. An `http://` STS is allowed when named (`sts_endpoint`, `AWS_ENDPOINT_URL_STS`) | ✓ on `credential_process` (helper once, one AssumeRole for 3 requests); ✓ on IMDS (`aws_signing_helper serve`: token, role, credentials, then AssumeRole); ✓ via the default `https://sts.us-east-1.amazonaws.com` reached through `HTTPS_PROXY`, CA from `AWS_CA_BUNDLE` |
| SigV4 signer | `creds::sign` | ✓ AWS's documented example (IAM ListUsers, signature `5d672d79…`) in `creds::tests`; ✓ SeaweedFS, which checks signatures, answers a signed ListObjectsV2 200 and the same with a wrong secret 403 (the STS stand-in doesn't check signatures) |

**Gaps and differences from the Go publisher (aws-sdk-go-v2) that remain:**

- SSO profiles (`sso_session`, `sso_start_url`): refused; use
  `credential_process` (`aws configure export-credentials --format process`).
- The IRSA path is still object_store's: it only talks https to STS (an
  `http://` `AWS_ENDPOINT_URL_STS` is refused there, not in `creds.rs`).
- The stand-ins hand out an empty session token (SeaweedFS rejects tokens
  its own STS didn't issue), so the token header wasn't exercised against
  the store [E: object_store signs it as the SDKs do]. STS is a stand-in
  that answers any action; no real AWS, EKS or STS was used.

Configuration, per deployment (`configs/edge.yaml` substitutes these from
the environment):

```yaml
# EKS, IRSA or Pod Identity: no keys; the pod's env carries the credentials
s3: { url: "s3://otel-telemetry/edge", region: "eu-west-1" }
# Nutanix Objects, private CA
s3: { url: "https://objects.nutanix.example/otel/edge", region: "us-east-1",
      access_key_id: "${env:NUTANIX_ACCESS_KEY}", secret_access_key: "${env:NUTANIX_SECRET_KEY}",
      ca_bundle: "/etc/ssl/nutanix/ca.pem" }
# IAM Roles Anywhere, from outside AWS
s3: { url: "s3://otel-telemetry/edge", region: "eu-west-1",
      credential_process: "aws_signing_helper credential-process --certificate /etc/ra/cert.pem --private-key /etc/ra/key.pem --trust-anchor-arn arn:… --profile-arn arn:… --role-arn arn:…" }
# … or, with `aws_signing_helper serve` running beside it: no s3 credential
# fields, and AWS_EC2_METADATA_SERVICE_ENDPOINT=http://127.0.0.1:9911 in the env
# A shared-config profile (AWS_PROFILE=edge works too), and a role on top of it
s3: { url: "s3://otel-telemetry/edge", region: "eu-west-1", profile: "edge",
      role_arn: "arn:aws:iam::111122223333:role/otel-writer", external_id: "…" }
```

The exporter needs `s3:PutObject` and `s3:GetObject` on the prefix, and
`s3:ListBucket` so that a HEAD of a missing key answers 404, not 403
(../awss3). The consumer also needs `s3:ListBucket` for its LIST.

## Measurements in detail

### Edge [M]

`results/bench.md` (from `results/bench.jsonl`):

- "flatten" includes the content hash and, for via-OTAP, upstream's
  conversion;
- "commit" is the PUT round trip to SeaweedFS on localhost;
- "pipeline" rows are the whole `otap-s3pq` process, measured from
  `/proc/<pid>` around 30 distinct OTLP/HTTP requests.

| dest | signal | impl | n | CPU ms/batch | ms/batch | k rows/s | peak RSS MB | object KB | flatten / encode / commit ms | S3 req/batch |
|---|---|---|---|---|---|---|---|---|---|---|
| s3 | logs | rust-pipeline-direct | 3 | 34 [32–35] | 37.5 [36.6–38.3] | 240 [235–250] | 51 [51–51] | – | – | – |
| s3 | logs | rust-pipeline-via_otap | 3 | 47 [45–49] | 51.7 [50.4–54.4] | 185 [174–187] | 54 [53–54] | – | – | – |
| s3 | logs | rust-direct | 3 | 33 [31–34] | 37.2 [35.3–37.8] | 267 [254–274] | 32 [31–32] | 76 | 10.8 / 21.4 / 5.2 | HEAD 0.00, PUT 1.00 |
| s3 | logs | rust-direct-allbloom | 3 | 35 [35–38] | 40.5 [39.9–42.0] | 245 [228–249] | 31 [31–32] | 121 | 10.3 / 24.6 / 5.9 | HEAD 0.00, PUT 1.00 |
| s3 | logs | rust-direct-nobloom | 3 | 33 [33–33] | 36.6 [36.2–37.6] | 267 [260–268] | 31 [31–32] | 68 | 10.9 / 21.4 / 5.1 | HEAD 0.00, PUT 1.00 |
| s3 | logs | rust-otap-input | 3 | 35 [33–38] | 39.4 [37.0–40.5] | 251 [233–266] | 35 [35–35] | 76 | 13.2 / 21.4 / 5.2 | HEAD 0.00, PUT 1.00 |
| s3 | logs | rust-via-otap | 3 | 47 [47–48] | 51.3 [51.1–51.8] | 190 [187–192] | 35 [35–35] | 76 | 25.3 / 22.0 / 5.2 | HEAD 0.00, PUT 1.00 |
| s3 | logs | parquet-go | 3 | 52 [52–54] | 57.4 [56.8–58.2] | 171 [169–173] | 151 [134–176] | – | – | – |
| s3 | logs | parquet-go-nobloom | 3 | 50 [49–50] | 55.0 [52.8–55.4] | 182 [181–186] | 137 [129–154] | – | – | – |
| s3 | traces | rust-pipeline-direct | 3 | 47 [46–49] | 52.3 [50.3–52.8] | 176 [172–182] | 58 [58–59] | – | – | – |
| s3 | traces | rust-pipeline-via_otap | 3 | 75 [73–75] | 77.9 [77.3–78.8] | 119 [117–121] | 62 [61–62] | – | – | – |
| s3 | traces | rust-direct | 3 | 45 [44–45] | 49.3 [49.2–49.8] | 200 [198–200] | 38 [38–38] | 131 | 18.3 / 25.9 / 5.8 | HEAD 0.00, PUT 1.00 |
| s3 | traces | rust-direct-allbloom | 3 | 51 [48–51] | 54.4 [52.9–55.2] | 176 [175–186] | 39 [38–39] | 196 | 19.1 / 30.9 / 6.6 | HEAD 0.00, PUT 1.00 |
| s3 | traces | rust-direct-nobloom | 3 | 45 [44–45] | 49.0 [48.0–50.3] | 197 [196–202] | 38 [38–38] | 115 | 18.8 / 25.8 / 5.6 | HEAD 0.00, PUT 1.00 |
| s3 | traces | rust-otap-input | 3 | 49 [48–53] | 53.5 [53.0–55.2] | 184 [172–186] | 43 [43–43] | 131 | 23.3 / 25.4 / 5.8 | HEAD 0.00, PUT 1.00 |
| s3 | traces | rust-via-otap | 3 | 71 [70–73] | 74.3 [74.1–75.8] | 130 [127–131] | 44 [43–44] | 131 | 44.0 / 26.8 / 5.7 | HEAD 0.00, PUT 1.00 |
| s3 | traces | parquet-go | 3 | 73 [72–73] | 80.4 [78.5–81.5] | 124 [123–127] | 108 [107–113] | – | – | – |
| s3 | traces | parquet-go-nobloom | 3 | 70 [69–71] | 74.5 [74.2–78.0] | 133 [127–133] | 109 [106–124] | – | – | – |
| local | logs | rust-direct | 3 | 30 [29–33] | 30.0 [29.6–32.0] | 334 [295–336] | 28 [28–28] | 76 | 9.3 / 20.7 / 0.0 | – |
| local | logs | rust-direct-arrow | 3 | 14 [14–14] | 14.0 [13.6–14.0] | 710 [708–720] | 25 [24–25] | 661 | 8.4 / 5.5 / 0.0 | – |
| local | logs | rust-direct-zstd1 | 3 | 29 [29–29] | 29.0 [28.8–29.4] | 340 [339–340] | 28 [27–28] | 74 | 9.6 / 19.9 / 0.0 | – |
| local | logs | rust-otap-input | 3 | 33 [32–36] | 32.7 [31.1–33.5] | 304 [277–315] | 31 [31–31] | 76 | 12.1 / 20.8 / 0.0 | – |
| local | logs | parquet-go | 3 | 50 [47–52] | 47.6 [45.2–48.1] | 203 [198–217] | 170 [136–170] | 155 | – | – |
| local | traces | rust-direct | 3 | 44 [44–45] | 43.1 [42.4–43.5] | 222 [219–227] | 35 [35–35] | 131 | 18.6 / 26.3 / 0.0 | – |
| local | traces | rust-direct-arrow | 3 | 28 [25–28] | 26.5 [26.1–27.5] | 357 [353–402] | 30 [30–31] | 1021 | 18.4 / 9.5 / 0.0 | – |
| local | traces | rust-direct-zstd1 | 3 | 43 [41–43] | 43.5 [41.0–44.2] | 231 [227–244] | 34 [34–35] | 133 | 18.3 / 24.9 / 0.0 | – |
| local | traces | rust-otap-input | 3 | 48 [48–51] | 47.4 [47.2–49.2] | 206 [195–209] | 40 [40–40] | 131 | 22.8 / 25.7 / 0.0 | – |
| local | traces | parquet-go | 3 | 68 [68–71] | 66.7 [66.0–69.2] | 148 [141–148] | 106 [100–118] | 265 | – | – |

- **zstd level 1 against 3** saves 1 ms per batch or less, and changes the
  size by about ±2 KB. Level 3 stays.
- **Bloom filters:**
  - a filter on TraceId only costs under 1 ms and 16 KB per traces batch;
  - on every column, as parquetgo and ClickHouse write by default, it costs
    2–6 ms and 45–65 KB;
  - central ingest doesn't read them.
- **Where the time goes:**
  - A callgrind profile of the direct traces path puts about 40% of the
    instructions in parquet-rs's column writer: dictionary interning (hash
    and memcmp), levels and RLE. The repeated resource and scope strings are
    interned per row.
  - About 20–25% is the OTLP view walk and rendering, and about 6% is zstd.
  - An Arrow-dictionary input for those columns is the next thing to try
    [E].

### Central [M]

`scripts/central_bench.py`, following `../otap/central_bench_test.go`:

- 10 objects of the testgen batch per layout;
- `INSERT … SELECT FROM s3()` of 1 or 10 objects into central-typed tables;
- server-wide `system.events` CPU deltas;
- median [min–max] of 3.

Full table: `results/central.md`.

| signal | layout | objects | settings | wall ms | server CPU ms | peak mem MB | S3 GET | S3 HEAD | MB read |
|---|---|---|---|---|---|---|---|---|---|
| traces | rust (TraceId bloom) | 10 | default | 231 [207–233] | 318 [303–320] | 216 [216–216] | 10 [10–10] | 0 [0–0] | 1.34 |
| traces | rust (TraceId bloom) | 10 | single-thread | 307 [284–308] | 291 [286–303] | 38 [38–38] | 10 [10–10] | 0 [0–0] | 1.34 |
| traces | rust, no bloom | 10 | default | 212 [208–233] | 277 [268–298] | 216 [216–216] | 10 [10–10] | 0 [0–0] | 1.34 |
| traces | rust, no bloom | 10 | single-thread | 297 [282–302] | 305 [283–309] | 38 [38–38] | 10 [10–10] | 0 [0–0] | 1.34 |
| traces | parquetgo (all blooms) | 10 | default | 198 [195–205] | 265 [253–278] | 216 [216–216] | 10 [10–10] | 0 [0–0] | 2.71 |
| traces | parquetgo (all blooms) | 10 | single-thread | 295 [288–318] | 294 [288–318] | 38 [38–38] | 10 [10–10] | 0 [0–0] | 2.71 |
| traces | parquetgo, no bloom | 10 | default | 219 [195–237] | 278 [271–318] | 216 [216–216] | 10 [10–10] | 0 [0–0] | 1.77 |
| traces | parquetgo, no bloom | 10 | single-thread | 323 [300–324] | 298 [298–320] | 38 [38–38] | 10 [10–10] | 0 [0–0] | 1.77 |
| logs | rust (TraceId bloom) | 10 | default | 132 [122–152] | 172 [165–190] | 152 [152–155] | 10 [10–10] | 0 [0–0] | 0.78 |
| logs | rust (TraceId bloom) | 10 | single-thread | 193 [178–202] | 193 [185–197] | 24 [24–24] | 10 [10–10] | 0 [0–0] | 0.78 |
| logs | rust, no bloom | 10 | default | 136 [133–159] | 181 [172–204] | 155 [151–156] | 10 [10–10] | 0 [0–0] | 0.78 |
| logs | rust, no bloom | 10 | single-thread | 196 [193–199] | 197 [190–199] | 24 [24–24] | 10 [10–10] | 0 [0–0] | 0.78 |
| logs | parquetgo (all blooms) | 10 | default | 155 [144–171] | 197 [180–206] | 152 [150–152] | 10 [10–10] | 0 [0–0] | 1.59 |
| logs | parquetgo (all blooms) | 10 | single-thread | 220 [194–225] | 211 [192–222] | 24 [24–24] | 10 [10–10] | 0 [0–0] | 1.59 |
| logs | parquetgo, no bloom | 10 | default | 145 [135–176] | 184 [183–212] | 152 [151–153] | 10 [10–10] | 0 [0–0] | 0.99 |
| logs | parquetgo, no bloom | 10 | single-thread | 205 [187–209] | 190 [185–198] | 23 [23–24] | 10 [10–10] | 0 [0–0] | 0.99 |

**The Rust objects cost about the same to ingest as parquetgo's.**

- Traces with default settings: 318 against 265 ms. A 9-run re-run
  (`results/central-traces-9reps.md`) gave 287 [254–356] against 259
  [251–608].
- Traces single-threaded: 291 against 294 ms (re-run: 284 against 273).
- Logs: 172 against 197 ms.
- So the difference is about ±10%, inside the run-to-run ranges of this
  shared server (the counters are server-wide).
- The Rust objects are read with half the bytes, because only TraceId
  carries a bloom filter.
- The 1-object rows are dominated by other load: see `results/central.md`.

### End-to-end latency to query visibility [M]

`scripts/latency.sh` runs one box, 30 requests of 10k spans, one per
second, with the consumer polling at P (`results/latency/`):

- "ack" is the OTLP response, which comes after the S3 commit;
- "visible" runs from the edge receiving the request (`received_at`, in the
  object) to the consumer's `INSERT` returning.

| Consumer poll P | Edge ack, median [min–max] | Visible in central, p50 / p90 / max |
|---|---|---|
| 200 ms | 55 [50–94] ms | **218 / 292 / 329 ms** |
| 1 s | 54 [49–65] ms | 600 / 671 / 680 ms. The 1 req/s sender is phase-locked with the poll here; expect up to P + ~0.3 s |

Per batch, the path to visibility is:

- the commit, about 50 ms;
- up to one poll;
- a LIST, a HEAD, the count check, and a single-block insert: about
  60–150 ms locally.

This is FASTPATH's "importer at a short poll" in practice. Sub-second
visibility needs no edge-to-central path.

### Build and footprint [M]

| | Rust `otap-s3pq` (engine + OTLP receiver + exporter) | parquetgo |
|---|---|---|
| Binary | **44.1 MB**: thin LTO, 1 codegen unit, stripped; links glibc (libc, libm, libgcc_s) dynamically. 69.6 MB for a plain release build (50 MB stripped) | 15.7 MB static (a library; a Go collector with it is larger) |
| Start to OTLP port ready (warm cache) | 29 ms | 32–38 ms (publisher only) [D: ../parquetgo] |
| RSS after start | 30 MB | – |
| Dependency graph | 395 crates (424 without patch 0001) | Go modules |
| Toolchain | Rust 1.98.1 (577 MB). Upstream pins it, and stable 1.94 can't build it (sysinfo 0.39 needs 1.95) | Go 1.26 |
| Clean build, 3 jobs on 4 vCPUs | release: 11 min wall, 29 min CPU; dist: 10.4 min; target dir 2.0 GB (release) + 1.5 GB (dist) | seconds |

The engine is most of the binary, not the exporter. The in-process
benchmark, which links pdata, object_store and parquet but not the
controller, admin server or receivers, is 22 MB stripped [M]. A static musl
build wasn't tried.

## Metrics

(This section is the `metrics_layout: clickstack_tables` layout, the contrib
exporter's five tables. The default is now layout B: see
[Metrics layout B](#metrics-layout-b-the-series-table-the-default).)

**Metrics are supported, OTLP direct by default, OTAP too. The rows are
identical to the contrib clickhouseexporter v0.161.0's own rows, including
on hostile input. Edge CPU is 3.8–6.8 µs per data point by type, not the
calculator's 2 µs, and about half of parquetgo's. Central is at parity.**

### Upstream's metrics support [D, then M]

- **OTLP bytes views** (`views/otlp/bytes/metrics.rs`, `RawMetricsData`):
  complete for all five types, exemplars and bucket ranges included. They
  are zero-copy, like the traces and logs views.
- **OTAP views** (`views/otap/metrics.rs`, `OtapMetricsView`): complete as
  well. Upstream's OTLP→OTAP encoder followed by this view gives
  **column-identical** rows to the direct walk on testgen, the mixed batch
  and the duplicate-key cases (`tests/metrics.rs`). No patch was needed.
- **Limits found in the views** [D, not exercised end to end]:
  - `aggregation_temporality()` returns an enum that maps every unknown
    value to `Unspecified` (0). Contrib stores the raw `int32`, so a
    request with, say, temporality 7 would differ.
  - A point that carries both `as_double` and `as_int` on the wire reads as
    the double. Go keeps the last one.
  - An exemplar id of the wrong length reads as absent, so it renders as
    zeros. Go's pdata rejects the whole request.
  - `SumView::data_points` reads `GAUGE_DATA_POINTS`. Both are field 1, so
    this is harmless.
- **The OTLP→OTAP conversion rejects invalid UTF-8** inside map values
  (CBOR), as it does for traces. The hostile metrics batch is answered 400
  on the via-OTAP path.

### What was built

- **Spec:** `../parquetgo/METRICS_SCHEMA.md` is followed for the layout and
  the rendering.
  - One object per non-empty metric type per request. Each object goes under
    its own signal namespace (`metrics_gauge`, `metrics_sum`,
    `metrics_histogram`, `metrics_exponential_histogram`,
    `metrics_summary`), with its own lane, epoch, slots and content key
    (`BLAKE3("{signal}\0" + request)`).
  - `DateTime` columns are TIMESTAMP(MILLIS) of `uint32(floor(int64(ns)/1e9))`
    (`metrics::dt_ms`), as clickhouse-go stores them. ClickHouse's own
    DateTime64→DateTime conversion wraps the same way, which was checked.
  - Exemplar ids are always hex, so a zero id is zeros. Unset
    sum/min/max/value are 0.
  - A metric of type Empty rejects the whole request, as permanent 400.
  - A request with no points is ACKed with no objects.
- **Code:**
  - `src/metrics.rs`: one generic walk over the view traits that fills all
    five types' buffers in one pass. Resource and scope maps are rendered
    and sorted once per scope.
  - `src/schema.rs`: `metrics(signal)`.
  - `src/batch.rs`: `flatten_all`, with `Input::OtlpMetrics` and
    `Input::OtapMetrics`.
  - `src/central.rs`: s3() structures and the consumer's metrics tables,
    which use contrib's columns and types plus the envelope, `content_key`
    and the projection.
  - `consume --signal metrics_<type>`.
- **Ack:** `exporter.rs` commits a request's objects concurrently, each in
  its type's lane. It ACKs only when every object has committed
  (`proto::request_verdict`). Otherwise it NACKs the whole request, and on
  the retry the committed parts are found in their lanes' known set (no
  request to S3).
- **Map order: `src/gosort.rs`.**
  - Contrib builds every metrics map with clickhouse-go's
    `orderedmap.CollectN`, which **sorts keys with Go's unstable
    `slices.SortFunc`** and keeps duplicates.
  - Up to 12 entries that sort is a stable insertion sort. Above 12, the
    order of duplicate keys is whatever pdqsort leaves.
  - `gosort.rs` ports it and is unit-tested against Go's output. It is only
    used when a map has duplicate keys, which wire-decoded pdata can hold.

### Disagreements with METRICS_SCHEMA.md

- **Duplicate keys in maps of more than 12 entries.**
  - parquetgo sorts with `SortStableFunc`, so it differs from contrib there.
    Rust matches contrib.
  - `metrics-extra.pb` (wire-built duplicate keys, all five types) shows it:
    the Rust rows equal contrib's, while parquetgo's differ from both, and
    only in the order of equal keys.
  - The spec's text still says maps are "in pdata order"; its code sorts.
- **Two worked `dt` examples have arithmetic slips.**
  - `MaxInt64` gives `633_437_444_000`, and `1<<63` gives
    `3_661_529_851_000`.
  - The formula and parquetgo's `dtMillis` agree with this crate.
- **Not part of the contract:**
  - V1 data pages, where parquetgo writes V2.
  - No bloom filters on metrics: there is no TraceId column.
  - No dictionary on `Value`, `Sum` and the exemplar leaves.

### Correctness [M]

`scripts/metrics_e2e.sh` sends each dataset as OTLP/HTTP to `otap-s3pq`
(direct and via OTAP). Then `scripts/metrics_correctness.py` compares, per
type and dataset:

- against **contrib's own rows**: `tools/cmd/metricsref`, the exporter from
  its factory with `create_schema`, into ClickHouse. The object is
  `INSERT … SELECT`ed into a table `AS` the exporter's, then checked by count
  + `sum(cityHash64(all columns))` and `EXCEPT` both ways;
- against **parquetgo's objects** (`otlpgen -metrics -ref`): count + hash
  with structure and inferred, `DESCRIBE`, `EXCEPT` both ways, plus the
  envelope.

The datasets are:

- `compare.Metrics(3000)`: 3,000 points per type;
- `compare.NastyMetrics(700)`: NaN/±Inf/−0, int extremes, unset values,
  exemplars (none, 30, zero ids, zero time), empty and 60-entry maps with
  100 KB values, every value type, invalid UTF-8 everywhere, extreme
  timestamps, exponential histograms with negative buckets, `MinInt32`
  offsets and extreme scales, mismatched bucket lengths;
- `metrics-extra.pb`: duplicate keys, and `service.name` twice.

Results are in `results/metrics/correctness.txt`: 230 PASS.

| | testgen | nasty | extra (duplicate keys) |
|---|---|---|---|
| Rust direct vs contrib rows, ×5 types | **equal** (hash, EXCEPT 0/0) | **equal** | **equal** |
| Rust direct vs parquetgo objects | identical (hash both ways, schema, EXCEPT) | identical | differ in duplicate-key order only |
| parquetgo vs contrib rows | equal | equal | **differ** |
| Rust via OTAP vs contrib | equal | request rejected (400, CBOR UTF-8) | equal |

### Faults: no half-acked request [M]

`scripts/metrics_faults.sh` runs 6 distinct mixed requests. Each request is
2,000 points of each type, so 5 objects. Every scenario ends with one Rust
consumer per type (`results/metrics/faults/summary.txt`). **All pass**: in
every table, 12,000 rows and 6 distinct content keys; every request 2xx
exactly once.

| Scenario | Fault | Edge / sender | Objects per type | Central |
|---|---|---|---|---|
| ambiguous | every 2nd PUT, any type, applied, answer held 3 s | 15 resolved own by HEAD | 6 | 6/6 per type |
| **partial** | histogram PUTs time out *and* their HEADs time out | **3 NACKs with 4 of 5 objects committed**; the retries: 12 parts skipped as known, the histogram slot resolved by 412→HEAD | 6 | 6/6 |
| dropped | every 3rd PUT, 503, never lands | object_store's retry | 6 | 6/6 |
| crash | histogram PUT and HEAD answers held 8 s; SIGKILL at 3 s with the first request 4/5 committed; restart | resent to new epochs | 7 (+1 copy), 1 tombstone | 6/6: the copy of each type skipped by content key, dead epochs tombstoned |
| crashall | every 2nd PUT of any type held 8 s, SIGKILL, restart | | 7, 1 tombstone | 6/6 |

`tools/cmd/faultproxy2` gained `-head-hold`/`-head-limit`, to make a part
stay unresolved.

### Model [M]

**Why a new model:**

- The per-type logs are unchanged: each is `s3Inline.qnt`.
- The request level is new:
  - ACK only when every object has committed;
  - an edge crash restarts every lane at once;
  - the sender resends to every lane.
- So `../model/s3InlineMetrics.qnt` wraps **two instances** of `s3Inline`
  (G and S), unmodified, and adds `ackRequest`, `crash` and `reqAcked`.

**Invariants:**

- `reqAckedImpliesAllCommitted`;
- `noObjectLost`;
- both instances' full `safety`.

**Results** (`results/mbt/metrics.txt`):

- **Simulation:**
  - the design: 3,000 traces × 60 steps, no violation;
  - the mutant `ackOnAny` (2xx once any object commits) breaks
    `reqAckedImpliesAllCommitted` in 0.3 s. After a crash it also breaks
    `noObjectLost`: the other object is lost;
  - every witness is reached: half-committed, cross-epoch copy, all acked.
- **quint-connect** (`tests/mbt_s3inline_metrics.rs`):
  - It drives two lanes, two consumers and the real `request_verdict`. The
    driver is shared with `mbt_s3inline.rs` via `tests/common/s3inline.rs`.
  - After every step it also compares **`ackable`**: the requests the
    implementation would ACK now, against those the model enables
    `ackRequest` for.
  - The design passes: 300 traces, 23,968 steps, 103 s. The traces reach:
    - 600 crashes and 198 acks;
    - a half-committed state in 296 traces;
    - a cross-epoch dedup in 178 traces.
  - `OTAPRS_MUTANT=ack_on_any` fails at trace 1, step 23. The gauge object
    commits while the sum object hasn't; the model has `ackable {}`, the
    implementation `{2}`.
- Quint 0.32 quirk: in the wrapper, a state variable read on the right of
  `G::x' = …` resolves in G's namespace. `crash` therefore takes the
  pending set as a parameter.

### Measurements [M]

The environment and accounting are as for traces and logs: 3 processes ×
(3 warm-up + 30 timed) batches of **10,000 data points**, interleaved with
parquetgo's `pubbench` (built from `../parquetgo/compare`, same
`MetricsBatch` data). The box was busier than for traces: load average
1.4–3.6 (`results/metrics/bench.md`, `.jsonl`).

**Edge, to S3, CPU per 10k points** (median [min–max]). "mixed" is one
request with 2,000 points of each type, so 5 objects.

| type | Rust OTLP direct | µs/point | Rust OTAP input | Rust via OTAP | parquetgo | peak RSS Rust / parquetgo | object KB Rust / parquetgo (all blooms, no bloom) | S3 requests per request |
|---|---|---|---|---|---|---|---|---|
| gauge | **38** [38–40] | 3.8 | 45 | 58 | 91 | 41 / 133 MB | **87** / 211, 183 | 1 PUT (parquetgo: 2) |
| sum | **40** [39–42] | 4.0 | 44 | 58 | 92 | 40 / 139 | **35** / 126, 111 | 1 (2) |
| histogram | **67** [66–67] | 6.7 | 73 | 94 | 134 | 58 / 253 | **117** / 281, 252 | 1 (2) |
| exp. histogram | **68** [66–70] | 6.8 | 69 | 92 | 142 | 60 / 179 | **261** / 428, 395 | 1 (2) |
| summary | **50** [47–50] | 5.0 | 50 | 68 | 87 | 44 / 139 | **131** / 247, 225 | 1 (2) |
| mixed (5 objects) | **68** [64–70] | 6.8 | 68 | 86 | 140 | 39 / 251 | **179** / 328, 291 | **5 PUTs, 0 HEAD** (10: object + manifest per type) |

- **The whole `otap-s3pq` process** was fed 30 distinct single-type 10k-point
  requests over OTLP/HTTP. It used 53 [51–54] ms CPU per request and 82 MB
  peak RSS (29 MB after start).
- **Where the time goes:**
  - For direct gauge: flatten 12, encode 26, commit 5 ms.
  - Encode is Parquet column writing, as for traces.
  - Flatten includes rendering and Go-order sorting of every point's
    attribute map.
- **Commit:** 5–7.5 ms per object on localhost. The mixed row's 20.6 ms is
  the five commits one after another in `encbench`; the exporter runs them
  concurrently.
- **The calculator's 2 µs/point is too low:**
  - the Rust edge measures 3.8 µs/point (gauge, sum) to 6.8 µs/point
    (histograms, mixed traffic);
  - parquetgo measures 8.7–14 µs/point;
  - use ~4 µs for gauge/sum-heavy traffic and ~7 µs for histogram-heavy or
    mixed traffic.

**Central.** `scripts/metrics_central_bench.py` inserts 10 objects of
10,000 points each into the consumer's central table for the type, measured
as server CPU (`results/metrics/central.md`).

| type | Rust µs/point, default / 1 thread | parquetgo, default / 1 thread | MB read, 10 objects, Rust / parquetgo |
|---|---|---|---|
| gauge | 4.1 / 3.6 | 3.8 / 3.3 | 0.89 / 2.16 |
| sum | 3.9 / 3.6 | 4.0 / 4.7 | 0.36 / 1.29 |
| histogram | 4.9 / 4.5 | 5.0 / 5.2 | 1.20 / 2.87 |
| exp. histogram | 5.7 / 4.7 | 5.6 / 6.3 | 2.67 / 4.39 |
| summary | 4.1 / 4.1 | 4.2 / 5.1 | 1.34 / 2.53 |

- **Central `INSERT … SELECT` costs about 3.5–5.7 µs of server CPU per
  point**, for both producers. That is within the ±15% noise of this shared
  server.
- It is about as much as the Rust edge spends, and less than half of what
  parquetgo's edge spends.
- The Rust objects are read with 40–70% fewer bytes.

### Metrics gaps

- **The raw temporality enum and the double-plus-int oneof** differ from
  contrib, as noted above. The OTLP views don't expose raw values, and
  fixing that would take an upstream patch.
- ~~No OTAP receiver was run.~~ Done: metrics over upstream's OTAP receiver
  from the Go otelarrow producer equal contrib's rows on testgen, in both
  layouts (see [Inputs](#inputs-otap-end-to-end-otlpgrpc-m)); hostile input
  closes the stream, duplicate keys are dropped by the producer.
- **One lane per type was measured.** A mixed request holds five lanes, one
  per type, at once.

## Metrics layout B: the series table (the default)

**The edge now writes metrics as layout B of
[../metrics-layout](../metrics-layout/README.md) by default
(`metrics_layout: series_table`; `clickstack_tables` keeps the five contrib
tables): narrow points objects keyed by a series id computed at the edge,
plus a series object for series not announced yet this hour. It is the Go
prototype's layout exactly, row for row and id for id, the compatibility
views return exactly the contrib exporter's rows, and edge CPU per point
drops by 50–65%. On the wire it is no smaller than this crate's ClickStack
objects.**

### What was built

- `src/series.rs`: one walk over the view traits (OTLP bytes or OTAP
  records), a port of `../metrics-layout/seriesenc/seriesenc.go`:
  - **series id v1**, xxh3-64 over the prototype's length-prefixed
    canonical encoding, two-level (resource+scope hashed once per scope);
  - **points objects** per type; with `series.merge_number_points` (default
    on) gauge and sum share `metrics_number_points`, with a `MetricType`
    column after `series_id`;
  - **the series object** (`metrics_series`): every non-point field of the
    contrib row, maps as key/value arrays in contrib's order;
  - `Exemplars.FilteredAttributes` in the points objects
    (`series.exemplar_attributes`, default on), which the prototype drops
    and contrib has;
  - `SeriesOptions::prototype()` turns both additions off: the objects are
    then the prototype's, column for column.
- **The series cache** (per exporter, window `series.window`, default 1 h
  of the point's `TimeUnix`): a series is announced once per window, and
  within a request once.
  - **A series counts as announced only after its series object has
    committed** (`exporter.rs` `commit_one` → `Encoder::series_announced`,
    with the lane's epoch). Until then every request re-announces it, so a
    NACKed or crashed request's retry carries the series again.
  - A commit in an epoch the cache hasn't seen empties it (a new epoch
    re-announces everything). Entries older than the previous window are
    pruned, so the cache doesn't grow with series churn; a point more than a
    window late may be re-announced (harmless: the table is idempotent).
- **Lanes:** the points objects are keyed like the ClickStack ones,
  `BLAKE3("{namespace}\0" + request)`. The series object is keyed by **its
  own content hash** (`content_hash_cols`): what a request announces depends
  on the cache, so a retry may carry a different series object, or none. It
  goes on its own `metrics_series` lane(s). The request is ACKed when every
  object, the series object included, has committed.
- **Central** (`sql/series_tables.sql`, `sql/series_views.sql`): the spike's
  DDL plus `otel_metrics_number_points` and the exemplar attributes; the
  views read gauge and sum from the merged table, split on the point's
  `MetricType`. `series::structure` / `series::insert_select` are the
  importer's `s3()` structure and statement per namespace (the series
  object: `mapFromArrays`, `LastSeen = FirstSeen`, no envelope stored).
- **Gauge + sum merge: kept, compatible with the views.** The views need to
  know a point's type without the series row (which may land later, and the
  views keep such a point with empty maps rather than hiding it), so the
  merged points carry `MetricType`: one UInt8 per point, constant per
  series, ≈0 B stored and <0.1 B/point in Parquet. It saves one object
  (and one ~16 ms central insert) per request.
- Encoding: dictionaries on, none on the near-unique numbers, and
  `row_ordinal` DELTA_BINARY_PACKED (as the prototype's `delta` tag): plain
  it was 2.3 B/point of every object.

### Rust = Go [M] (`tests/series.rs`, `tools/cmd/seriesref`)

`seriesref` runs the prototype over the same OTLP requests with the same
envelope, one encoder and `Announced()` after each request; the test does
the same with `SeriesOptions::prototype()` and compares every object: the
set of objects per request, the Parquet schema (root name, every leaf's
path, physical and logical type, levels) and every leaf column's values and
levels (doubles by bits, strings as bytes).

| Input | Result |
|---|---|
| the spike's fleet (`seriesref -fleet`): 130 consecutive 10k-point batches of 20 pods, crossing an hourly window | **identical**: 652 objects, 1,320,000 rows, 20,000 series rows (the initial announce and the hourly re-announce), so every series id |
| testgen, nasty (NaN, invalid UTF-8, extreme timestamps, exemplars…), extra (duplicate keys) | **identical**: 18 objects, 27,034 rows, 9,919 series rows, except 40 series-row maps of `metrics-extra.pb` that differ only in the order of duplicate keys: contrib's (unstable) order here, the prototype's stable order there. The entries are the same; the ids are equal |

**A bug in the Go prototype, found and fixed:** it rendered the scope
attributes under the resource's once-per-resource check, so for every scope
after a resource's first the series row had **empty scope attributes** (the
id was right). The fleet data has no scope attributes, so the spike's
numbers are unaffected; `seriesenc.go` is fixed (3 lines).

### Central correctness [M]

`scripts/metrics_e2e.sh` with `PATHS="series:direct series:via_otap"` runs
the pipeline with `metrics_layout: series_table`;
`scripts/metrics_correctness.py` imports every object with the importer's
statements into layout B's tables and compares **the views** with the
contrib exporter's own rows (`tools/cmd/metricsref`) exactly as the
ClickStack objects are compared: count + `sum(cityHash64(every column))` and
`EXCEPT` both ways through a table created `AS` contrib's, plus "every point
has its series row" (`results/series/correctness.txt`).

| | testgen | nasty | extra (duplicate keys) |
|---|---|---|---|
| OTLP direct, views vs contrib, ×5 types | **equal** | **equal** | **equal** |
| via OTAP | equal | rejected (CBOR UTF-8, as before) | equal |
| OTAP input from the Go otelarrow producer (below) | equal | rejected (Arrow UTF-8) | differs: the producer drops duplicate keys |

### Edge cost [M] (`scripts/series_bench.sh`, `results/series/bench.md`)

3 processes each, in-process (encbench, to SeaweedFS through the real
lanes) and the whole `otap-s3pq` process (OTLP/HTTP); load 1.5–2.0.

| | layout B (series table) | ClickStack tables | |
|---|---|---|---|
| **edge CPU per point**, fleet | **1.76 µs** (17.6 ms / 10k) | 5.04 µs | −65% |
| edge CPU per point, testgen (mostly unique series) | 2.45 µs | 4.87 µs | −50% |
| whole process, fleet over OTLP/HTTP | **1.67 µs** | 4.65 µs | −64% |
| flatten / encode / commit ms per 10k, fleet | 6.1 / 10.5 / 12.0 | 9.8 / 39.5 / 14.5 | encode is 4× cheaper |
| **Parquet bytes per point**, fleet | 20.3 B | 19.8 B | +2.5% |
| Parquet bytes per point, testgen | 20.3 B | 18.4 B | +10% |
| **objects (PUTs) per request** | **4** + a series object on 0.8% of requests (the hourly re-announce) | 5 | −20% |

- **The wire saving the spike measured is gone** against this crate's
  writer. The spike's 1.6× was against parquet-go's ClickStack objects
  (38 B/point); the Rust ClickStack writer already dictionary-encodes the
  repeated maps (the fleet's 21 resource attributes cost ~1 KB per 2,800
  rows). B's points carry an 8-byte random `series_id` per point, which
  zstd can't shrink: 64 KB of the 89 KB number object per 10k points. The
  prototype's own gap (a dense per-epoch ordinal in the points, the id in the
  series object) is the fix [E].
- **Central is where B pays** (the spike's measurements, not re-run here):
  4.1× fewer stored bytes and 7× less insert CPU per point; the per-object
  fixed cost (~16 ms) now dominates, and the gauge+sum merge removes one of
  five objects per request.
- The series object is 97 KB for 10k new series (the first request and
  each hourly re-announce): ~10 B/series, 0.1 B/point amortized.
- Peak RSS 242 MB in the fleet encbench rows is the 130 input files held in
  memory; the whole process peaks at 72 MB.

## Edge durability: upstream's durable buffer (Quiver) [M]

**It builds and works in this pipeline, and requests acknowledged to the
client survive a SIGKILL of the edge. It costs about a third more edge CPU
and writes twice the OTLP bytes to local disk.** `configs/edge-durable.yaml`
is the OTLP receiver → `processor:durable_buffer` → `exporter:s3pq`; the
feature (`durable-buffer`, on by default in this crate) and the processor
compile with no patch (+Quiver; the release binary with it and the OTAP
receiver is 53.8 MB stripped, against 50 MB before).

How it changes the contract:

- the client's 2xx now follows the **WAL write**, not the S3 commit. The
  buffer forwards finalized segments (`max_segment_open_duration`, 1 s) to
  the exporter and deletes a bundle only when the exporter ACKs it, i.e.
  after the create-only commit; a retryable NACK is retried with backoff,
  forever (bounded by `retention_size_cap` + `size_cap_policy`), a permanent
  one (undecodable input) is dropped;
- Quiver acknowledges after `write()`, and fsyncs the WAL every 25 ms
  (`WalConfig::flush_interval`, not exposed by the processor's config): a
  process kill loses nothing acknowledged, **a host crash or power loss can
  lose the last ≤25 ms** of acknowledged requests;
- a replayed request is the same OTLP bytes, so it has the same content key:
  if the first incarnation's commit landed, the new epoch's copy is dropped
  by the consumer's content check, as in the non-durable crash scenario.

**Crash test** (`scripts/durable.sh`, `results/durable/summary.txt`): 12
distinct 10k-span requests; each object's rows are fingerprinted and matched
to the request that produced it through a baseline run.

| Scenario | Acked to the client before the SIGKILL | In S3 after the restart |
|---|---|---|
| S3 unreachable (closed port) while sending: every PUT fails, so nothing can commit; SIGKILL; restart with S3 up | 12 of 12 (from the WAL; 75 MB of buffer) | **all 12**, 12 objects |
| control: the same, but the restart gets an empty buffer directory | 12 of 12 | **0 of 12**: the check does catch loss |
| mid-flight: every PUT's answer held 5 s by faultproxy2 (the objects land, the exporter doesn't learn it), SIGKILL 0.8 s into a stream of 12, restart, the sender resends what it had no answer for | 7 of 12 | **all 12**; 13 objects (one request committed by both incarnations: the consumer skips it) |

Without the buffer the S3-down scenario acks nothing: the exporter NACKs
(503) and the client keeps the data, which is the design above; the buffer
moves that custody to the edge's disk.

**Cost** (30 distinct 10k-span requests over OTLP/HTTP, 3 processes each,
`results/durable/cost.md`):

| | edge CPU per request | disk written per request | client ack, median | 30 requests sent → all 30 committed |
|---|---|---|---|---|
| `edge.yaml` | 31.7 ms | 0 | 36 ms (after the commit) | 1.27 s |
| `edge-durable.yaml` | **42.7 ms (+35%)** | **6.3 MB** (the 3.2 MB request, twice: WAL + segment) | **19 ms** (after the WAL write) | 1.67 s: the segment's 1 s open window, then the commit |

- At the calculator's traces rate that is +1.1 µs of edge CPU per span and
  ~630 B of local writes per span; size the buffer volume for the outage it
  should ride out (`retention_size_cap`, default here 1 GiB).
- Visibility gets up to `max_segment_open_duration` + `poll_interval`
  (1.1 s by default) later; lower values cost more I/O.
- Not measured: a real power cut (the fsync window is from the source),
  disk-full behaviour (`backpressure` vs `drop_oldest`), and metrics through
  the buffer (the payload is opaque OTLP bytes, so no difference is
  expected).

## Inputs: OTAP end to end, OTLP/gRPC [M]

**Upstream's OTAP receiver works end to end with the Go otelarrow producer,
after one fix in this exporter; OTLP/gRPC costs the same as OTLP/HTTP at the
edge; OTAP input costs the edge more, not less.**

- `configs/edge-otap.yaml` adds `receiver:otap` (OTel Arrow gRPC streams,
  `wait_for_result`) next to the OTLP receiver. The sender is
  `tools/cmd/otapsend`: otel-arrow's Go `arrow_record.Producer` (what the
  collector's otelarrowexporter uses), one stream per signal kept for the
  whole run (so later batches carry dictionary deltas), each BatchStatus
  awaited.
- **Found and fixed:** records from the OTAP receiver keep the
  *transport-optimized* id encoding (delta-encoded parent ids), which
  otap-dataflow's views don't undo. Walked as they came, every span matched
  ~9,000 attribute rows (3,000 spans took 2.7 s, and one metrics batch drove
  the edge to 13.5 GB and the OOM killer). `exporter.rs` now calls
  `decode_transport_optimized_ids()` on OTAP input (a shallow clone; the
  parquet exporter upstream does the same). The in-process "OTAP input"
  numbers above were unaffected: upstream's Rust encoder hands out plain ids.
- **Correctness** (`scripts/otap_e2e.sh`, `results/otap/`):
  - traces and logs, testgen: **the same rows as OTLP input, except order**:
    the producer sorts rows and each map's entries (by type and key), so the
    row-level checks against parquetgo fail on `row_ordinal` and map order.
    With every map sorted and rows compared as a multiset, OTAP = OTLP in
    both directions (`order-insensitive.txt`). Contrib's traces and logs keep
    map order as received, so this is a real difference in the stored maps'
    order, not in their content;
  - metrics, testgen: **equal to the contrib exporter's rows** for both
    layouts (contrib sorts metrics maps itself, and row order doesn't enter
    the check);
  - `metrics-extra.pb`: differs: the producer drops duplicate keys;
  - the hostile datasets: **the receiver closes the whole stream**
    ("Invalid UTF8 sequence", Arrow's IPC reader validates `Utf8`), so a
    client that reconnects and resends would be stuck on that batch. The same
    limit as the via-OTAP path's CBOR error, but worse in effect.
- **Cost** (`scripts/input_bench.sh`, `results/inputs/bench.md`: the same
  process and exporter, 30 distinct 10k-item requests, 3 processes each,
  load 0.5–0.9; metrics in layout B):

  | 10k items per request | OTLP/HTTP | OTLP/gRPC | OTAP/gRPC (Go producer) |
  |---|---|---|---|
  | traces: edge CPU / client ack | **31.3 ms** / 36 ms | 33.3 ms / 42 ms | 42.3 ms / 47 ms, +79 ms to encode at the sender |
  | logs | 26.0 / 31 | 26.7 / 32 | 29.7 / 33, +51 |
  | metrics | 15.3 / 14 | 15.7 / 17 | 23.7 / 24, +56 |

  - OTLP/gRPC is within 2–6% of HTTP at the edge: both hand the exporter the
    same protobuf bytes.
  - OTAP costs the edge 14–55% more than OTLP: IPC decode, the id decoding,
    and the column-hash content key replace a walk over bytes that were
    already there. Its gain is on the wire (compression, dictionaries), not
    at a ClickStack edge.

## Consumer

**`consume` is now a production-shaped importer: a fleet of workers
shares the producer lanes through a lease and a checkpoint per lane,
CAS'd on S3; it ingests up to 32 committed objects per `INSERT … SELECT
FROM s3()`, verifies every statement against the content projection, and
closes dead epochs with tombstones; a separate GC step deletes behind the
checkpoints. Under chaos (ambiguous, slow and dropped PUTs, edge SIGKILLs,
worker SIGKILLs and pauses past their lease), a 30-minute soak of 77k
committed objects put every committed batch into central exactly once and
nothing uncommitted [M]. The run before it found a liveness bug (lanes
orphaned after a worker's pause), now fixed and tested. Batching cuts server
CPU per small object 8× (19.6 → 2.4 ms at 32 objects per statement), but
only when a poll finds several objects per table. Visibility is 211 ms p50 /
320 ms p90 at a 200 ms poll (about 1 object per statement there) and 663 /
1,061 ms at 1 s (3.7). What remains costly is LIST: one per lane per
poll.**

Labels as above. Code: `src/consumer/` (mounted by `src/bin/consume.rs`),
`src/central.rs`; tests: `src/consumer/tests.rs`,
`tests/mbt_s3inline_consumer.rs`, `../model/s3InlineConsumer.qnt`;
scripts: `scripts/consumer_*.sh`.

### What it does

```
{root}/{producer}/{signal}/{epoch}/{seq:020d}.parquet      the edges' slots (unchanged)
{ctl}/lease/{producer}/{signal}.json    CAS'd {owner, epoch (fencing), beat, ttl_ms}
{ctl}/ckpt/{producer}/{signal}.json     CAS'd {lease_epoch, version, epochs: {E: {next, closed}}}
{ctl}/workers/{worker}.json             heartbeat (plain PUT), for the fair share
{ctl}/gc.json                           CAS'd GC marks: {wall_ms, every lane's positions}
```

`{ctl}` defaults to `{root}/_consumer`. A lane is one producer's one signal
namespace; `--depth 1` treats `{root}/{signal}` as the lanes (the
prototype's layout, and its flags `--signal S --table db.t` still work:
`scripts/faults.sh` passes all five scenarios unchanged,
`results/consumer/faults-compat.txt`).

- **Leases** (`coord.rs`, sans-IO). A lease has a fencing epoch (+1 per
  change of owner) and a TTL. Expiry is judged on the observer's own
  monotonic clock: a lease whose ETag it has seen unchanged for
  TTL + margin is expired. The holder counts from the moment it *sent* the
  write that installed its version, so its window (`sent + ttl − margin`)
  always closes 2 × margin before anyone may take over, whatever the clocks'
  offsets. Renewal is a CAS every TTL/3; a failed renewal or a lapsed window
  (own clock) drops the lane at once.
- **The time bound on inserts.** A statement starts only if
  `now + budget ≤ safe_until` for every lane it contains, and runs with
  `max_execution_time = budget`. For a process paused between that check
  and the send (GC pause, SIGSTOP), the statement also carries a server-side
  fence, `WHERE now64(3) <= fromUnixTimestamp64Milli(sent_wall + ttl −
  margin − budget)`: on ClickHouse's clock, a late statement is a no-op,
  and doesn't even open its objects [M]. That half needs the holder's and
  the server's wall clocks within the margin.
- **Checkpoint fencing.** Taking a lane rewrites its checkpoint under the
  new lease epoch before anything else, so every later CAS by the previous
  holder fails on the ETag. Ambiguous CAS answers are resolved by reading
  the object back.
- **Several workers** (`worker.rs`). Each discovery round (every
  `--discover`) lists the lanes (one LIST per producer), beats the
  worker's heartbeat, lists heartbeats and leases (their ETags), and evens
  the load: fair share = ⌈lanes / live workers⌉; above it a worker releases
  one lane per round (a released lease can be taken at once), below it
  takes free, released or expired lanes.
- **Discovery.** Per held lane and open epoch, `LIST StartAfter` the
  checkpoint's key. The newest epoch's LIST is lane-wide, so it also
  returns epochs created since; `--full-list` periodically lists the
  lane's epochs, catching one that sorts earlier (a producer whose clock
  stepped back).
  - **SeaweedFS quirk [M]:** a StartAfter naming a "directory"
    (`{lane}/{epoch}/`) is taken as "after that whole directory" and
    returns nothing. The consumer uses `{lane}/{epoch}/0` for slot 0, which
    sorts before every slot key on AWS too.
- **Slots.** HEAD each consecutive slot from the checkpoint: data (content
  key, committed rows) or a tombstone (the epoch is closed there). The scan
  stops at the first free slot. **A gap** (a free slot with a later one
  listed) is never skipped and never tombstoned: a live writer can't leave
  one, so it is LIST lag or a deleted slot, and it is reported
  (`gaps_seen`). A superseded epoch whose head stays free for `--quiet` gets
  a create-only tombstone race, as in ../awss3.
- **Ingest** (`sql.rs`), per table, across all the worker's lanes:
  1. one projection check (`content_key IN (…)`) for every pending object:
     present ones are skipped (a copy of the request in another epoch, or a
     retry), one object per content key;
  2. the absent ones in statements of up to `--max-batch` objects (32),
     16 MB and 200k rows (and an object over 100k rows or 8 MB alone):
     `INSERT INTO t (…, content_key) SELECT …, transform(_path, [paths],
     [keys], '') FROM s3('http://…/bucket/{k1,k2,…}', …)`;
  3. the single-block settings with squashing on (`min_insert_block_size_*`
     above the statement), so a statement is **one part per partition**,
     atomic; `--no-squash` gives one part per object;
  4. `insert_deduplication_token` = a hash of the ordered key list, with
     `deduplicate_insert = enable` and `deduplicate_insert_select =
     force_enable` pinned;
  5. after it (after `KILL QUERY … SYNC` if the answer was lost), the same
     check verifies: complete → done; missing → inserted again one by one;
     partial → row repair (`row_ordinal NOT IN …`); more rows than committed
     → reported (`over_count`), never fixed silently;
  6. per lane, the checkpoint moves past the done prefix only
     (`plan::advance_to`), and a tombstone at the new position closes the
     epoch; one CAS per lane per step.
- **Every lane:** traces, logs, the contrib metrics tables
  (`clickstack_tables`), and layout B's points lanes
  (`metrics_number_points`, `metrics_{gauge,sum}_points` when not merged,
  `metrics_{histogram,exponential_histogram,summary}_points`) with the
  edge's own `series::structure` / `series::insert_select` and
  `sql/series_tables.sql` plus `content_key` and the projection. The series
  lane (`metrics_series`) has no count check and no verify: re-inserting is
  harmless (AggregatingMergeTree).
- **GC** (`gc.rs`, `consume gc`, separate and safe to run from anywhere):
  each run appends every lane's checkpoint, with the time, as a mark to
  `gc.json` (CAS), then deletes the data slots below the newest mark that
  is at least `--delay` old (lease TTL + margin + the longest a PUT can be
  in flight): the horizon trails the checkpoint by a lease length, and a
  slot is deleted only once no late copy of its PUT can still land. A
  position is the first slot *not* consumed, so the head of a live epoch
  and a closed epoch's tombstone are never deleted by the horizon; a closed
  epoch is removed entirely only after `--zombie` (a bound on how long a
  fenced writer process can live). Two GCs racing is harmless.

### ClickHouse facts found [M] (26.10.1)

- **`s3('…/{k1,k2}')` with full keys in the braces does no LIST**: it HEADs
  and GETs each key.
- **`INSERT … SELECT` is not deduplicated without a token** in 26.10:
  `deduplicate_insert_select = enable_when_possible` (the default) only
  deduplicates a "stable" select (`ORDER BY ALL`, one stream). With
  `enable_even_for_bad_queries`, block ids are position-dependent: the same
  object in a differently grouped statement was inserted again. So a token
  protects only an exact retry of the same statement; the projection check
  is what makes ingest exactly-once.
- **Squashing is where most of the batching win is** (below): a statement
  of 32 small objects costs 5.1–5.9 ms per object with one part per object,
  1.8–2.2 ms squashed into one part.

### Batching: one object per statement against many [M]

`scripts/consumer_fixedcost.sh`: the statement's own CPU (ClickHouse
per-query profile events), median of 3, on edge objects of 200 spans or
20 points per type (`results/consumer/fixedcost.jsonl`):

| server CPU per object | 1 per statement | 8, part per object | 8, squashed | 32, part per object | 32, squashed |
|---|---|---|---|---|---|
| traces (200 spans, 13.5 KB) | 14.7 ms | 6.8 | 3.3 | 5.9 | **2.2** |
| number points (40 points, 5.5 KB) | 11.7 ms | 6.0 | 2.8 | 5.1 | **1.8** |

The check query costs under 1 ms for 1 or 32 keys.

`scripts/consumer_bench.sh`: the whole consumer (`--once`, one worker) on
2,800 committed objects (400 per namespace: traces, logs, and layout B's
five), a fresh database and checkpoint per run, median of 3; server CPU from
`system.events` (server-wide) (`results/consumer/bench.jsonl`):

| objects per statement | statements | server CPU per object | consumer CPU per object | wall | S3 per object (HEAD / LIST / CAS / GET) |
|---|---|---|---|---|---|
| 1 | 2,800 | 19.6 ms | 0.57 ms | 57 s | 1.00 / 0.017 / 0.018 / 0.003 |
| 8, squashed | 350 | 4.3 ms | 0.23 ms | 14.2 s | 1.00 / 0.014 / 0.013 / 0.003 |
| **32, squashed** (default) | 91 | **2.4 ms** | **0.19 ms** | **9.9 s** | 1.00 / 0.014 / 0.013 / 0.003 |
| 32, part per object | 91 | 9.0 ms | 0.19 ms | 21.9 s | same |

- **8× less server CPU per object** at 32 objects per statement, 4.6× at 8.
  At the metrics-layout calculator's 667 objects/s that is 13 cores of
  fixed cost down to about 1.6 [E].
- A statement batches across the worker's lanes of one table, so the win
  needs several objects pending per table per poll: many lanes per worker,
  or a longer poll. At a 200 ms poll with a few objects per second per table
  (below) statements averaged 1.01 objects.
- The first run of this benchmark exposed a real issue, now fixed: a long
  step starved the lease renewals, so objects were deferred and HEADed
  twice; renewals now run inside the ingest loop.

### Steady state: latency, CPU and S3 requests [M]

`scripts/consumer_latency.sh`: one edge, one worker, no faults, 3.7
requests/s per signal (traces and logs of 200 rows, metrics of 20 points per
type: 7 objects per metrics request), 90 s per poll period
(`results/consumer/latency.jsonl`). "Visible" runs from the edge receiving
the request (`received_at` in the object's metadata) to the worker's INSERT
returning.

| poll | visible p50 / p90 / p99 / max | objects per statement | server CPU per object | consumer CPU per object / per row | S3 per object: HEAD / LIST / CAS / GET | checks per object |
|---|---|---|---|---|---|---|
| 200 ms | **211 / 320 / 441 / 1,333 ms** | 1.01 | 22.4 ms | 1.29 ms / 18 µs | 1.00 / 1.47 / 1.02 / 0.003 | 1.69 |
| 1 s | **663 / 1,061 / 1,211 / 1,329 ms** | 3.71 | 8.9 ms | 0.51 ms / 7.1 µs | 1.00 / 0.38 / 0.30 / 0.003 | 0.46 |

- The edge ack (the commit) was 17 ms p50 in both.
- **Latency and batching pull against each other.** At 200 ms, with 7
  lanes and a few objects per second per table, a poll finds about one
  object per table: statements hold 1.01 objects, and the per-statement
  cost comes back (22 ms server CPU per object against 2.4 ms in the batch
  benchmark). At 1 s they hold 3.7, and server CPU per object is 2.5×
  lower. A worker holding many lanes of one table batches at any poll; a
  deployment that wants 200 ms visibility *and* the batching win needs
  enough lanes per worker, or a short linger (not built).
- **S3 requests per object are dominated by polling, not by objects:**
  one HEAD per object, but at 200 ms 1.5 LISTs and one checkpoint CAS per
  object, because every poll LISTs every held lane and writes each lane's
  checkpoint it advanced. At 1 s these drop to 0.38 and 0.30.
- Server CPU here is `system.events` (server-wide) over the run, so it
  includes merges.

### Soak under chaos [M]

`scripts/consumer_soak.sh` + `scripts/consumer_soak_check.py`
(`results/consumer/soak/summary.txt`, `chaos.log`):

- 3 edges (`otap-s3pq`, 7 lanes each; one with 2 exporter lanes per
  signal) behind 3 fault proxies: every 7th PUT ambiguous (applied, answer
  held 3 s against a 1 s `put_timeout`), every 5th slow (held 2.5 s before
  it reaches S3, so the retry lands first and the late copy gets 412), every
  6th dropped (503);
- senders (`tools/cmd/soaksend`) with distinct requests, resent until 2xx;
- 3 workers on 21 lanes at a 200 ms poll, lease TTL 6 s, margin 1 s, budget
  2 s, quiet 3 s; GC every 5 s (delay 10 s, zombie bound 60 s); an audit
  recording every committed object before GC removes it;
- chaos every 8–20 s: SIGKILL a worker (restarted 1–3 s later as a new
  incarnation), SIGSTOP a worker for 7–12 s (past its lease: it resumes as
  a zombie), or SIGKILL an edge (restarted at once; its senders resend into
  new epochs).

**Result (30 minutes, after one fix): PASS.** Every committed batch is in
central exactly once, and nothing uncommitted is
(`results/consumer/soak/summary.txt`):

| | |
|---|---|
| chaos events | 29 worker SIGKILLs, 35 worker pauses past the lease, 30 edge SIGKILLs |
| requests acked to the senders | 38,428 (38,620 attempts; none dropped) |
| committed objects (audit) | 77,284 in 1,089 epochs: traces 14,388, logs 14,395, and 9,697–9,702 per layout-B namespace; 156 cross-epoch copies |
| tombstones | 1,068 (1,067 won by the workers; 5 races lost to a late batch, which was then ingested) |
| central, per table | traces 14,373 batches / 2,874,600 rows; logs 14,380 / 2,876,000; number points 9,675 / 387,000; histogram, exp. histogram and summary points 9,675 / 193,500 each: **missing 0, partial 0, duplicated 0, uncommitted 0** |
| per request | every acked traces and logs request's rows exactly once (by `soak.req`); every metrics request's points exactly once through its series; **0 points without a series row** |
| workers (32 incarnations) | 47,755 statements; 357 copies skipped by the check; 735 lanes taken, 267 released, 210 lapsed on their own clock, 34 checkpoint CASes lost to a new holder; `over_count` 0, gaps 0 |
| worker CPU | 117 s over 5,430 s of worker time: 1.5 ms per object, 17 µs per row (at a 200 ms poll, with 21 lanes busy polling) |
| GC | 344 runs; 77,284 data objects and 1,044 tombstones (retired epochs) deleted; 0 CAS conflicts |

The faults the edges saw: 3,520 answers held past `put_timeout` and 4,865
PUTs delayed 2.5 s (proxy logs), plus dropped PUTs; the edges resolved
them by HEAD and resend.

**The first 30-minute run failed, and found a real bug**
(`results/consumer/soak-30min-before-fix/`): 43 committed batches (20
traces, 17 number points, 6 summary points, in 5 epochs) were never
ingested. No duplicate, nothing uncommitted. All sat in lanes whose lease
still named worker `w1`:

- `w1` had been paused past its leases and dropped them on resume.
- The other workers were at their fair share, since `w1` was alive again,
  so they didn't take the lanes.
- `w1` itself skipped any lease whose owner was itself when looking for
  lanes to take. So the lanes stayed orphaned for as long as `w1` lived.

It was a liveness bug, not a safety one. The fix: a worker takes back an
expired lease that names it, like anyone else's. The regression test
`a_worker_retakes_lanes_it_let_lapse` fails on the old code. The model
missed it because its `wAcquire` has no such exclusion, and quint-connect
drives `coord`, not `Worker::balance`. Two earlier 5- and 6-minute runs
passed (`results/consumer/soak-6min/`, `soak-5min-b/`); both were stopped
early by the disk guard, before `old_parts_lifetime` was shortened on the
test tables.

"Hours-equivalent": the lease timing is compressed 5× against a 30 s
production TTL, and the chaos rate (one fault every 14 s on average) is
far above production's; counted in lease lifetimes it is 30 min / 6 s = 300 TTLs, about 2.5 hours
of a 30 s-TTL deployment, with a restart or pause every ~20 s instead of
every few hours.

### Model and model-based test [M]

**Model** (`../model/s3InlineConsumer.qnt`, a new wrapper;
`s3Inline.qnt` unchanged): one `s3Inline` instance supplies the writers,
S3 and the network; the wrapper replaces its single consumer with two
workers on a discrete clock sharing a CAS'd lease and checkpoint,
multi-object statements that are checked, then in flight, land only by
their fence and possibly in part, the verify-then-advance, tombstones, GC,
and the series lane's announce rule at the edge
(`results/consumer/model.txt`).

- **Invariants:** `atMostOnce`, `onlyCommittedIngested`,
  `neverSkipsCommitted`, `noCommitAfterClose`, the writer's
  `ackedImpliesCommitted` / `noPayloadLost` / `epochNoDuplicatePayload`
  (GC-aware), and the series rule **`announcedOnlyAfterCommit`**: a series
  is marked announced only after its series object committed.
- **The design** passes 5,000 traces × 60 steps, in the full hostile
  environment and in the mutants' narrower one.
- **Mutants** (each a design flag), each breaking its invariant:

  | Mutant | What changes | Breaks | Found in |
  |---|---|---|---|
  | `noTimeBound` | workers ignore their window; no fence | `atMostOnce`: a paused worker's checked statement lands after the new holder checked and inserted the same batch | 8.2 s |
  | `noVerify` | the checkpoint moves past a statement without verifying it | `neverSkipsCommitted` (a partial statement) | 0.5 s |
  | `gcTombs` | GC also deletes a closed epoch's tombstone | `noCommitAfterClose`, `neverSkipsCommitted`: a zombie writer re-creates the slot with a batch it is told is committed | 0.4 s |
  | `announceEarly` | the edge marks a series announced when it *sends* the series object | `announcedOnlyAfterCommit`; after a lost PUT, later points reference a series row that never lands | 0.2 s |

- The mutants' instances switch off what their counterexample doesn't
  need (writer faults, the series lane); in the full environment
  `noTimeBound` needs about 16 specific steps among ~35 kinds of action,
  and 20,000 random traces didn't find it.
- **Witnesses** (each reached): a takeover, a lapse, a fenced statement, a partial statement, a lost checkpoint CAS, a tombstone, GC, everything ingested after a takeover, a series announced.

**quint-connect** (`tests/mbt_s3inline_consumer.rs`): replays
`s3InlineConsumerDesign` traces through the consumer's own decision code:
`Observer::may_take`, `coord::take` / `renew`, `Held::may_start` /
`lapsed` / `fence_wall_ms`, the checkpoint fence and CAS, `plan::verdict`,
`plan::advance_to`, `plan::found` and `gc::doomed`; the log part uses the
shared s3Inline driver. After every step it compares the log, the lease,
each worker's lease and checkpoint view, phase and objects, the checkpoint,
central and the statements in flight, **and what each worker's clock
allows it now** (`takeable`, `mayStart`, `lapsed`), which the model derives
from its formulas and the code from `coord.rs`. The series actions aren't
driven (the edge's cache is `series.rs`, tested by `tests/series.rs`).

- **The design passes** (`results/consumer/mbt.txt`):
  - `s3InlineConsumerDesign`: 300 traces of up to 60 steps, 18,231 steps;
  - `designQuiet` (no writer faults, no series lane, so the steps go to
    the workers): 1,000 traces of up to 80 steps, 45,165 steps. They include
    3,161 takeovers, 2,688 renewals, 260 lapses, 486 checks and 193
    statements (62 landed whole or in part, 129 dropped or fenced), 66
    verifies, 478 tombstones and 17 GC runs.
- **Mutants of the code** (`OTAPRS_CONSUMER_MUTANT`, `coord::Mutation`),
  each caught on both instances:
  - `no_time_bound` at trace 3, step 35 (`wSend`): the model's statement
    carries the fence 19; the code's carries none;
  - `no_verify` at trace 3, step 41 (`wAdvance`): nothing landed, so the
    model keeps epoch 2 at slot 0; the code moves to slot 1.
- A worker restart loses its observations, so the model's workers carry a
  `seen` time, reset on a crash. The model is then no more permissive than
  the code about takeover.
- **What it doesn't cover:** the driver calls `coord`, `plan` and `gc`
  directly, not `Worker::balance`. So it could not see the liveness bug the
  soak found (below). The model's `wAcquire` has no "not my own lease"
  exclusion; the code's `balance` had one.

**Unit tests** (`cargo test --release --bin consume`, 23): the lease
windows (they never overlap), the observer, the checkpoint, keys, the scan
(runs, gaps), grouping, tokens, the statements, GC (horizon, retirement),
and the whole worker on an in-memory bucket and central with a fake clock:
ordering and cross-epoch copies, dead epochs, gaps, takeover and the old
holder's lapse, the server fence, partial statements and lost answers, the
series lane, a worker re-taking lanes it let lapse (the soak's bug; it
fails on the old code), and a randomized fleet (12 seeds × 1,500 events: 3 producers
× 3 signals, edge restarts with copies, 3 workers crashing and pausing past
their lease, ambiguous and dropped lease/checkpoint writes, half-landed
statements, GC throughout), each ending exactly once.

### What the consumer leaves open

- **LIST cost at short polls.** One LIST per held lane per poll, whether
  or not anything arrived: at 1 s that is 2.6 M LISTs per lane per month,
  about $13 at $0.005 per 1,000 [E]; at 5 s $2.6. A fleet of thousands of
  lanes needs S3 event notifications (SQS/EventBridge) as the discovery
  path, with LIST as the periodic reconciliation, or an idle-lane backoff
  (not built).
- **Fairness is coarse:** fair share by lane count, not by load; a lane
  moves only when a worker is above its share.
- **The server fence needs synchronized wall clocks** (within the margin)
  between worker and ClickHouse; the client-side bound doesn't.
- **Replicated or SharedMergeTree central** wasn't tested: the check would
  need `select_sequential_consistency`, and dedup storage differs [D].
- **The checkpoint keeps every closed epoch** (a few bytes each, forever);
  GC retires the objects, not the entries.
- **Not tested against AWS S3:** SeaweedFS's single-part `If-Match` and
  `If-None-Match` were shown atomic (../model/S3NATIVE.md); AWS may answer a
  concurrent `If-Match` with 409, which object_store retries.
- The fault injection covered the edges' PUTs; the consumer's own S3
  requests were faulted in the in-memory tests only.

## Remaining gaps

Closed in this round (each has its section above): metrics layout B at the
edge, `AWS_CA_BUNDLE` / `HTTPS_PROXY` / `NO_PROXY` / `AWS_PROFILE` and
AssumeRole chaining, a persistent queue at the edge (Quiver), the OTAP
receiver end to end, OTLP/gRPC input measured, and the consumer (a CAS'd
lease and checkpoint on S3, GC, several objects per statement, every lane
including layout B's; its own open items are in
[What the consumer leaves open](#what-the-consumer-leaves-open)).

- **Not tested against:**
  - real AWS S3, EKS or STS (STS is a stand-in that answers any action and
    checks no signature; the signer was checked against AWS's documented
    example and SeaweedFS);
  - Nutanix Objects, whose conditional-write support is still unknown
    (../awss3, "Deployment");
  - a real `aws_signing_helper`.
- **Credentials:** SSO profiles are refused; IRSA's STS must still be https
  (object_store's path); a non-empty session token wasn't exercised against
  the store.
- **Layout B:**
  - **no wire saving** against this crate's ClickStack objects (the 8-byte
    `series_id` per point); a per-epoch dense ordinal is the untried fix;
  - the announce rule is in the model now (`../model/s3InlineConsumer.qnt`:
    `announcedOnlyAfterCommit`, broken by the `announceEarly` mutant), but
    by simulation only: quint-connect doesn't drive the edge's cache, which
    `tests/series.rs` covers; the eventual form ("every ingested point's
    series row is ingested") is checked on the soak's data, not in the
    model;
  - central CPU and stored bytes were not re-measured on the Rust objects
    (the spike's are for the prototype's objects, which are the same rows);
  - the views filter merged gauge/sum on the point's `MetricType`; HyperDX's
    fast paths still refuse views (the spike's findings stand).
- **Durable buffer:** a host crash can lose the last ≤25 ms of acknowledged
  requests (WAL fsync interval, not configurable through the processor);
  disk-full behaviour and a real power cut weren't tested; +35% edge CPU and
  2× the request bytes written locally.
- **OTAP input:** invalid UTF-8 in any string closes the whole stream (a
  poison batch for a resending client); map entry and row order are the
  producer's, not the client's; the edge spends 14–55% more CPU than on OTLP.
- **Lanes:** one lane per signal was measured. Throughput per lane is
  1 / (encode + PUT), about 20 batches/s locally. More lanes run concurrently
  on the pipeline's one thread and share its CPU.
- **Content key:** the key hashes the request bytes, as ../awss3 does
  (SHA-256 there, BLAKE3 here). A client that re-batches differently after a
  restart produces new keys, and the consumer ingests both copies: its
  check keys on content, and its dedup token on the statement. This is
  ../awss3's documented caveat for post-queue batching. (OTAP input and layout B's series objects are
  keyed by a hash of their columns instead.)

## Reproduce

```sh
S=/path/to/scratch RUN=c$(date +%s)
scripts/fetch-upstream.sh $S/otel-arrow            # pinned upstream + patches -> .upstream
export CARGO_TARGET_DIR=$S/target CARGO_BUILD_JOBS=3
cargo build --release                                # otap-s3pq, consume, encbench
cargo test --release --lib                           # render, protocol cores, runner with faults
(cd tools && go build -o $S/bin/ ./cmd/... && cd ../../parquetgo/compare && go build -o $S/bin/ ./cmd/pubbench ./cmd/credstubs)
$S/bin/otlpgen -out $S/data -variants 33 -ref http://127.0.0.1:18333/otel/otap-rs/corr/$RUN/ref -epoch $RUN
OTAPRS_DATA=$S/data cargo test --release --test determinism --test otap_view -- --nocapture
# correctness: run the pipeline per path, send the datasets, compare on the server
for p in direct via_otap; do S3_URL=http://127.0.0.1:18333/otel/otap-rs/corr/$RUN/$p OTLP_PATH=$p $CARGO_TARGET_DIR/release/otap-s3pq -c configs/edge.yaml & sleep 2
  for s in traces logs; do $S/bin/otlpsend -url http://127.0.0.1:14318 -signal $s -file $S/data/$s-testgen-3000.pb,$S/data/$s-nasty-700.pb -n 2; done
  kill -INT %1; wait; done
python3 scripts/correctness.py $RUN direct via_otap
B=$CARGO_TARGET_DIR/release T=$S/bin D=$S/data OUT=results/faults scripts/faults.sh
cargo test --release --test mbt_s3inline -- --nocapture
(cd $S/creds && $S/bin/credstubs) & OTAPRS_CREDSTUBS=$S/creds cargo test --release --test creds -- --nocapture --test-threads 1
B=$CARGO_TARGET_DIR/release T=$S/bin D=$S/data OUT=results/bench.jsonl scripts/bench.sh && python3 scripts/summarize.py results/bench.jsonl
python3 scripts/central_bench.py $CARGO_TARGET_DIR/release/encbench $S/bin/pubbench $S/data results/central.md
B=$CARGO_TARGET_DIR/release T=$S/bin D=$S/data OUT=results/latency POLL=200ms scripts/latency.sh
# metrics
$S/bin/otlpgen -metrics -out $S/mdata -variants 12
OTAPRS_DATA=$S/mdata cargo test --release --test metrics -- --nocapture
B=$CARGO_TARGET_DIR/release T=$S/bin D=$S/mdata RUN=mc$(date +%s) PATHS="direct via_otap" OUT=results/metrics scripts/metrics_e2e.sh
B=$CARGO_TARGET_DIR/release T=$S/bin D=$S/mdata OUT=results/metrics/faults scripts/metrics_faults.sh
cargo test --release --test mbt_s3inline_metrics -- --nocapture       # OTAPRS_MUTANT=ack_on_any: fails
B=$CARGO_TARGET_DIR/release T=$S/bin D=$S/mdata OUT=results/metrics/bench.jsonl scripts/metrics_bench.sh
python3 scripts/metrics_central_bench.py $CARGO_TARGET_DIR/release $S/bin $S/mdata results/metrics/central.md
cargo build --profile dist --bin otap-s3pq           # thin LTO, stripped: the size number
# metrics layout B (series table): Rust = Go, views = contrib, edge cost
$S/bin/seriesref -out $S/series/go/corr $S/mdata/metrics-{testgen-3000,nasty-700,extra}.pb
$S/bin/seriesref -fleet $S/series/fleet -services 2 -rounds 130 -pods-per-batch 20 && $S/bin/seriesref -out $S/series/go/fleet $S/series/fleet/*.pb
OTAPRS_DATA=$S/mdata OTAPRS_SERIES_GO=$S/series/go OTAPRS_SERIES_FLEET=$S/series/fleet cargo test --release --test series -- --nocapture
B=$CARGO_TARGET_DIR/release T=$S/bin D=$S/mdata RUN=ms$(date +%s) PREFIX=otap-rs-edge PATHS="series:direct series:via_otap" OUT=results/series scripts/metrics_e2e.sh
B=$CARGO_TARGET_DIR/release T=$S/bin FLEET=$S/series/fleet D=$S/mdata OUT=results/series/bench.jsonl scripts/series_bench.sh && python3 scripts/series_summarize.py results/series/bench.jsonl
# durable buffer: crash test and cost
B=$CARGO_TARGET_DIR/release T=$S/bin D=$S/data OUT=results/durable scripts/durable.sh && python3 scripts/input_summarize.py --durable results/durable/cost.jsonl
# OTAP input end to end (Go otelarrow producer), OTLP/gRPC and OTAP benchmarks
B=$CARGO_TARGET_DIR/release T=$S/bin D=$S/data RUN=o$(date +%s) PREFIX=otap-rs-edge OUT=results/otap scripts/otap_e2e.sh
B=$CARGO_TARGET_DIR/release T=$S/bin D=$S/mdata RUN=mo$(date +%s) PREFIX=otap-rs-edge PATHS="otapgrpc series:otapgrpc" OUT=results/otap scripts/metrics_e2e.sh
B=$CARGO_TARGET_DIR/release T=$S/bin D=$S/data MD=$S/mdata OUT=results/inputs/bench.jsonl scripts/input_bench.sh && python3 scripts/input_summarize.py results/inputs/bench.jsonl
$S/bin/seriesref -rm otap-rs-edge/                   # clean up the run's objects
# the consumer: unit tests (with an in-memory fleet), the model, the model-based test
cargo test --release --bin consume
(cd ../model && quint run s3InlineConsumer.qnt --main s3InlineConsumerDesign --invariant safety --max-samples 5000 --max-steps 60)
(cd ../model && quint run s3InlineConsumer.qnt --main noTimeBound --invariant atMostOnce --max-samples 20000 --max-steps 60)  # fails; noVerify, gcTombs, announceEarly
QUINT_SEED=0x5eed cargo test --release --test mbt_s3inline_consumer -- --nocapture   # OTAPRS_CONSUMER_MUTANT=no_time_bound|no_verify: fails
(cd tools && go build -o $S/bin/ ./cmd/soaksend ./cmd/faultproxy2)
cp $CARGO_TARGET_DIR/release/{otap-s3pq,consume} $S/bin/
# batching: 400 small requests per signal into one edge, then the consumer at 1, 8 and 32 objects per statement
R=cb$(date +%s); ADMIN_HTTP=127.0.0.1:28080 S3_URL=http://127.0.0.1:18333/otel/otap-rs-consumer/$R/edges/edge-1 $S/bin/otap-s3pq -c scripts/consumer_soak_edge.yaml &
sleep 2; $S/bin/soaksend -url http://127.0.0.1:14318 -rate 40 -n 400 -rows 200 -points 20 -out /dev/null; kill -TERM %1
B=$S/bin RUN=$R BATCHES="1 8 32" REPS=3 OUT=results/consumer/bench.jsonl scripts/consumer_bench.sh
CHC="clickhouse client --port 19000" RUN=$R SIGNAL=traces scripts/consumer_fixedcost.sh   # after one consume --db otaprs_consumer_fc run
B=$S/bin OUT=results/consumer/latency.jsonl POLLS="200ms 1s" scripts/consumer_latency.sh
B=$S/bin OUT=$S/soak DURATION=1800 scripts/consumer_soak.sh                              # verdict: $S/soak/summary.txt
$S/bin/consume purge --s3 http://127.0.0.1:18333/otel/otap-rs-consumer/$R                # clean up
```

Everything writes under `s3://otel/otap-rs/` (metrics: `s3://otel/metrics-rs/`; this round's runs:
`s3://otel/otap-rs-edge/` and, for the consumer, `s3://otel/otap-rs-consumer/`, deleted afterwards) on the local SeaweedFS. Every
ClickHouse database is private and dropped afterwards.

## Files

| Path | What |
|---|---|
| `Cargo.toml`, `Cargo.lock`, `rust-toolchain.toml` | the crate (path dependencies on `.upstream`), pinned to upstream's toolchain and lock |
| `UPSTREAM`, `patches/`, `scripts/fetch-upstream.sh` | the pinned upstream commit and the two patches |
| `src/main.rs` | the `otap-s3pq` engine binary: upstream controller + OTLP and OTAP receivers + the durable buffer + this exporter |
| `src/exporter.rs` | `urn:otel:exporter:s3pq`: config (`metrics_layout`, `series`), lanes, ack/nack, the announce-after-commit rule, OTAP id decoding |
| `src/series.rs` | metrics layout B: series id, points and series objects, the series cache, schemas, the importer's structure and statements |
| `src/creds.rs` | shared config / credentials profiles, AssumeRole and web identity via STS, SigV4 |
| `src/metrics.rs`, `src/gosort.rs` | the metrics walker (five types, one pass); Go's `slices.SortFunc` for contrib's map order |
| `src/flatten.rs`, `src/render.rs`, `src/schema.rs`, `src/columns.rs` | the view walker, contrib-compatible rendering, the published schema, zero-copy column buffers |
| `src/batch.rs`, `src/encode.rs` | content key, flatten + encode per slot; Parquet and Arrow IPC writers |
| `src/proto.rs` | the commit protocol: `Lane`, `Consumer`, keys, metadata, mutations |
| `src/runner.rs`, `src/store.rs` | the protocol's I/O loop; object_store S3, credential order, CA bundle, credential_process, in-memory store with faults |
| `src/bin/consume.rs`, `src/consumer/`, `src/central.rs` | the central consumer: `coord` (lease, checkpoint), `plan` (scan, verdicts, grouping), `bucket` (S3 with request counts; in memory), `sql` (lane kinds, statements; in-memory central), `worker`, `gc`, `tests` (the in-memory fleet); `consume gc`, `audit`, `purge` |
| `src/bin/encbench.rs` | the in-process edge benchmark |
| `tests/mbt_s3inline.rs`, `tests/mbt_s3inline_metrics.rs`, `tests/mbt_s3inline_consumer.rs`, `tests/common/` | quint-connect model-based tests against `../model/s3Inline.qnt`, `../model/s3InlineMetrics.qnt` and `../model/s3InlineConsumer.qnt`; the shared log driver |
| `tests/metrics.rs` | metrics: determinism, OTLP vs OTAP input, DateTime rendering, Empty-type rejection |
| `tests/creds.rs`, `tests/determinism.rs`, `tests/otap_view.rs` | credential modes (19, plus the signer against SeaweedFS), deterministic encoding, the upstream view bug |
| `tests/series.rs` | layout B against the Go prototype (schema, rows, ids, through the cache); the cache rules |
| `configs/edge.yaml`, `configs/edge-durable.yaml`, `configs/edge-otap.yaml` | the pipeline (env-substituted); with the durable buffer; with the OTAP receiver |
| `sql/series_tables.sql`, `sql/series_views.sql` | layout B's central tables and the contrib-compatible views |
| `tools/` (Go) | `otlpgen` (datasets as OTLP, `-metrics` too; parquetgo reference), `otlpsend` (the retrying sender), `faultproxy2` (answer-late / apply-late / drop, held HEADs), `metricsref` (the contrib exporter's rows), `seriesref` (the Go prototype's objects; fleet batches; S3 cleanup), `otapsend` (OTAP sender, otel-arrow's Go producer); `otlpsend -grpc`; `soaksend` (endless distinct requests, tagged per request, resent until 2xx) |
| `scripts/` | correctness, faults, bench, central bench, latency, summaries; `series_bench.sh`, `durable.sh`, `otap_e2e.sh`, `otap_diff.py`, `input_bench.sh` and their summarizers; the consumer's `consumer_bench.sh`, `consumer_fixedcost.sh`, `consumer_latency.sh`, `consumer_soak.sh` (+ `consumer_soak_edge.yaml`, `consumer_soak_check.py`) |
| `results/` | `metrics/` (correctness, faults, bench, central), `mbt/metrics.txt`, `bench.jsonl`/`.md`, `central.md`, `correctness.txt`, `faults/`, `mbt/`, `latency/`, `creds.txt`; `series/` (correctness, bench), `durable/` (crash test, cost), `otap/` (OTAP correctness), `inputs/` (transport bench); `consumer/` (bench, fixed cost, latency, soak, model, mbt, faults compat) |
