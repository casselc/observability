# OTAP to S3 for central ClickStack: a comparison with parquetgo

**Short answer.** It can be done, and this spike built and measured four ways
of doing it. But OTAP adds nothing for this job unless the edge already runs
an OTAP-native pipeline.

- **A Go collector with OTLP input should keep [parquetgo](../parquetgo/README.md).**
  - Converting pdata to OTAP costs 188 ms of CPU per 10k-span batch in the Go
    otel-arrow library. parquetgo's whole publish costs 69 ms.
  - Every OTAP-based variant then needs further work to reach the ClickStack
    row shape.
  - The Go OTAP decoder also loses data: a whole batch vanishes, with no error,
    when a map or slice attribute holds invalid UTF-8.
- **An edge that already runs the Rust otap-dataflow engine should denormalise
  at the edge (option b).** Its ClickHouse exporter already builds
  ClickStack-shaped Arrow batches. Change it to write one Parquet object plus
  a manifest per batch, instead of an HTTP insert.
  - Central then ingests exactly what it ingests from parquetgo, at the same
    cost.
  - The exporter's value rendering needs fixing first: `SPAN_KIND_SERVER`
    instead of `Server`, and doubles through `ryu` (`5.0` instead of `5`).
- **Storing OTAP's own tables is feasible but costs more at central.**
  - **As Parquet star tables (option a):** 3–4× the central CPU of flat Parquet.
  - **As the raw OTAP Arrow IPC payloads (option d, `raw`):** 5× the central
    CPU. It also needs single-threaded window functions to decode the ids.
  - Neither can render map or slice values, which OTAP stores as CBOR, unless
    the edge converts them.
  - The stock Rust parquet exporter meets none of our reliability requirements
    (see [Reliability](#reliability-against-the-model)).
- **Arrow IPC instead of Parquet (option c)** reads fine in ClickHouse but is
  5.5× the bytes of Parquet (1.4 MB against 264 KB per 10k spans). It saves
  nothing at central.
- **ClickHouse cannot ingest OTAP directly.**
  - 26.10 has `Arrow`, `ArrowStream` and `arrowFlight`, but no OTAP input, and
    no issue or roadmap item for one was found.
  - ClickStack Cloud's managed endpoint takes OTLP.
  - ClickHouse does read OTAP's per-table IPC payloads byte for byte with
    `ArrowStream`, and option d builds on that.

Labels: **[M]** measured here · **[D]** from docs or source, not executed ·
**[E]** estimate. Everything measured used the Go otel-arrow library
(`github.com/open-telemetry/otel-arrow/go` v0.57.0, 2026-09-21), not Rust.
Building otap-dataflow was not practical:

- `rust-toolchain.toml` pins Rust 1.98.1 (a 645 MB toolchain);
- the workspace resolves to 519 crates;
- its `pdata` crate depends on DataFusion 53;
- about 4.7 GB of disk was free, shared with other work.

`cargo tree` alone installed the toolchain and 300 MB of crates, and both
were removed straight away. The Rust facts below come from reading the source
at `main` 5db8358 (2026-09-24) **[D]**.

The environment:

- one 4-vCPU box, heavily shared: load average 9–20 during the runs;
- ClickHouse server 26.10.1;
- SeaweedFS 4.47 on localhost;
- Go 1.26, arrow-go v18.7.0, parquet-go v0.32.0;
- `testgen` batches of 10,000 spans or records, as parquetgo's `pubbench` uses.

parquetgo was re-measured here (`ref`) in the same runs. It reproduces its
README: 68.7 ms of CPU per traces batch here against 68 ms there.

## Comparison

"Edge CPU" is user+sys CPU per 10k-row batch, the median of 3 processes, each
running 3 warm-up and 30 timed batches (`cmd/otapbench`). The OTAP variants
start from an already-built OTAP batch, because an OTAP edge receives OTAP. An
edge that receives OTLP adds the conversion: 188 ms for traces and 176 ms for
logs [M]. "Central" is the server-wide CPU for `INSERT … SELECT` of 10 batches
(100k rows) into `otel_traces` / `otel_logs` types, median of 3 [M].

| | **parquetgo** (baseline) | **a. OTAP star tables → Parquet**, JOINs at central | **b. OTAP → ClickStack rows at edge → Parquet** | **c. same rows → Arrow IPC file** | **d. raw OTAP IPC payloads**, decoded at central |
|---|---|---|---|---|---|
| What's on S3 per batch | 1 Parquet + manifest | 7 Parquet (traces) / 4 (logs) + manifest | 1 Parquet + manifest | 1 `.arrow` + manifest | 7 / 4 `.arrows` (bytes as received) + manifest |
| Edge CPU, traces / logs, S3 [M] | **72 / 52 ms** | 144 / 112 ms (Go) | 213 / 144 ms (Go, arrow-go writer); ~110 / 80 ms with a parquet-go-class writer [E]; Rust faster [E] | 121 / 77 ms (Go) | **7 / 5 ms** |
| Of which (traces): decode the OTAP IPC / transform [M] | – | 29 / 8 ms | 29 / 44 ms | 29 / 44 ms | none |
| Edge peak RSS [M] | 140 MB | 196 MB | 183 MB | 152 MB | 83 MB |
| Edge binary | 14 MB (parquet-go only) [M] | 50 MB for this spike's Go binary with everything (arrow-go + otel-arrow + parquet-go + AWS SDK) [M]; the Rust engine wasn't built | same | same | same, or no Arrow code at all if it only relays bytes [E] |
| Bytes on S3 per 10k spans / logs [M] | 264 / 155 KB (173 / 97 KB without blooms) | 475 / 383 KB (220 / 212 KB without blooms) | 354 / 212 KB (199 / 91 KB without blooms) | **1,456 / 979 KB** | **109 / 120 KB** |
| S3 PUTs per batch [M] | 2 | 8 / 5 | 2 | 2 | 8 / 5 |
| Central CPU, 10 batches, traces / logs [M] | 300 / 206 ms | 1,089 / 638 ms (**3.6× / 3.1×**) | 370 / 194 ms (≈ baseline) | 490 / 289 ms | 1,548 / 744 ms (**5.2× / 3.6×**), single-threaded only |
| Central CPU, 1 batch (one INSERT per dedup token) [M] | 42 ms | 164 ms | 39–106 ms | 98 ms | 217 ms |
| Central S3 GET+HEAD, 10 batches [M] | 38 | 176 | 38 | 86 | 219 |
| Central query | `SELECT … FROM s3(list)` | 4 attribute aggregations, 2 child aggregations, 4 hash JOINs (`central.go`) | same as parquetgo | same, format `Arrow` | as for a, plus window functions to decode delta and quasi-delta ids (`raw.go`) |
| Correctness vs the clickhouse exporter's rows (testgen) [M] | identical (parquetgo's own test) | equal after sorting Map keys and rows¹ | equal after sorting¹ | equal after sorting¹ | traces equal after sorting¹; **logs: 750 of 3,000 map bodies are ''** (CBOR) |
| Correctness, hostile data [M] | identical | OTAP losses²; ±Inf needs care in SQL | OTAP losses² | OTAP losses² | **fails**: ClickHouse rejects the Go producer's IPC for the traces batch; logs lose columns (schema inferred from the first object) |
| Retry idempotency (token, 2 attempts) [M] | 1 copy | 1 copy, default or single-thread | 1 copy | 1 copy | 1 copy (single-thread) |
| Deterministic content on re-run [M] | yes | yes at this size; `groupArray` order is not guaranteed multi-threaded [D] | yes | yes | yes (single-thread required) |
| Schema evolution | fixed ClickStack schema | OTAP columns are optional and change per batch; needs an explicit `s3()` structure with every column [M] | fixed ClickStack schema | fixed | as for a, and **type inference from the first object silently drops later objects' extra columns** [M] |
| Credentials: IRSA, Pod Identity, static keys to Nutanix, Roles Anywhere³ | aws-sdk-go-v2: all four: parquetgo loads the default chain (tested with stand-ins) [M] | Go: as parquetgo. Rust (`object_store` 0.13.2): IRSA ✓, Pod Identity ✓, static + endpoint + path-style ✓, **Roles Anywhere ✗ natively** [D] | same as a | same as a | same as a |
| Maturity / risk | built, tested against the server, pyarrow and Spark | Rust parquet exporter is Experimental, with no acks and no per-batch commit; Go star writer is this spike's | Rust ClickHouse exporter is Experimental and renders differently from contrib; Go flattener is this spike's | as b | depends on Go producer IPC quirks and ClickHouse's Arrow reader; the most fragile |

¹ OTAP sorts attribute rows by (type, key, value) and span events and links by
name and trace id, so Map key order and the order of events and links within a
span differ from pdata order. Values and multisets are equal. So is every
`SpanAttributes['k']` lookup, but the exact row hash is not. Rows come out in
OTAP order, so `row_ordinal` differs as well.

² What OTAP itself loses, checked on the 700-span / 700-log hostile dataset
(`parquetgo/compare/nasty.go`):

- keys with an Empty value are dropped (577 of 700 spans had one);
- start timestamps above 2^63 ns (after 2262) saturate the duration (42 spans);
- the order of events and links within a span changes (195 and 233 spans).

Once those three are normalised away, 0 rows differ in either direction.

³ Details in [Deployment and credentials](#deployment-and-credentials).

## What OTAP is, and what exists [D]

- **Data model.** A signal is a set of Arrow record batches, one per payload
  type.
  - Traces have SPANS, SPAN_ATTRS, SPAN_EVENTS, SPAN_EVENT_ATTRS, SPAN_LINKS,
    SPAN_LINK_ATTRS, RESOURCE_ATTRS and SCOPE_ATTRS.
  - Logs have LOGS, LOG_ATTRS, RESOURCE_ATTRS and SCOPE_ATTRS.
  - Children point at parents: `parent_id` references `id`, or `resource.id` /
    `scope.id`, which are struct fields of the root table.
  - Ids are u16 or u32 and **unique only within one batch** (spec §6.1). A
    batch therefore holds at most 65,536 spans.
  - Attributes are rows of `(parent_id, key, type, str|int|double|bool|bytes|ser)`.
    Maps and slices are CBOR in `ser`.
- **Transport encodings** (spec §6.4):
  - `id` columns are delta-encoded;
  - `parent_id` columns are "quasi-delta": a delta while the equality columns
    repeat, absolute otherwise. The equality columns are type, key and value
    for attributes, the name for events and the trace id for links;
  - tables are sorted by those columns;
  - strings, ints and ids may be `Dictionary(u8|u16)`, and a producer switches
    types freely ("adaptive schemas").
  - In the Go producer's output for 10k testgen spans [M]:
    - spans are sorted by name;
    - `duration` is a dictionary of durations;
    - the attribute tables carry `sorting_columns: type,key,value,parent_id`.
- **Wire protocol.**
  - gRPC bidirectional streams of `BatchArrowRecords`: one Arrow IPC stream per
    payload type, zstd-compressed buffers, and schemas and dictionaries sent
    once per stream.
  - `BatchStatus` acks per `batch_id`, and `batch_id` is unique only within
    one gRPC stream.
  - For testgen, a self-contained batch is **109 KB per 10k spans and 120 KB
    per 10k logs** [M], less than any Parquet here.
- **Go** (`otel-arrow/go`, used by contrib `otelarrowexporter` / `otelarrowreceiver`):
  pdata ↔ Arrow conversion only. The receiver converts every batch back to
  pdata, so in a Go collector OTAP is purely a transport.
- **Rust otap-dataflow.** An Arrow-native engine: receivers, processors and
  exporters that pass Arrow batches around.
  - Its `durable_buffer` processor (Quiver) is a WAL plus Arrow IPC segments,
    with ack/nack and retry.
  - Two exporters matter here, the parquet exporter and the ClickHouse
    exporter; both are covered below.
- **The Rust parquet exporter** (`core-nodes/src/exporters/parquet_exporter`,
  Experimental). It writes each payload type as its own Parquet table.
  - **Ids:** it decodes the transport ids, then makes them unique by adding a
    per-process running offset (`PartitionSequenceIdGenerator`). The files go
    under a `_part_id=<uuid>` directory that is new for each process.
  - **Possible bug (source reading, not executed):** the offset is added to
    the top-level `id` and `parent_id` columns only.
    - `RESOURCE_ATTRS.parent_id` and `SCOPE_ATTRS.parent_id` are shifted.
    - The root table's `resource.id` and `scope.id` are struct fields, so they
      are not shifted.
    - From the second batch in a file onwards, resource and scope attributes
      would then join to the wrong rows, or to none. The upstream query example
      joins only `log_attrs`.
  - **Files:** `{base}/{payload_type}/_part_id=…/part-<millis>-<uuid>.parquet`.
    - A file accumulates batches until `target_rows_per_file` (100M by
      default) or `flush_when_older_than`, and is uploaded through
      object_store multipart.
    - Child tables are flushed before parents, so a parent row never becomes
      visible before its children.
    - It uses parquet-rs `WriterProperties::default()`, which means
      uncompressed.
  - **Acks: none.** The pdata context is dropped ("support for
    acknowledgements and nack messages" is on its TODO list), and a write
    error ends the node.
  - **Signals:** logs, traces and metrics.
- **The Rust ClickHouse exporter** (`contrib-nodes/src/exporters/clickhouse_exporter`,
  Experimental) is option b in all but the destination.
  - It reshapes OTAP into ClickStack-shaped Arrow: attribute rows grouped by
    parent into `Map(LowCardinality(String), String)`, events and links
    grouped into the Nested arrays, `ServiceName` pulled out, CBOR turned into
    JSON.
  - It inserts with `FORMAT ArrowStream` over HTTP. It acks on success, nacks
    on failure, and sends no dedup token.
  - **It differs from the contrib Go exporter (v0.161.0):**
    - SpanKind `SPAN_KIND_SERVER` against `Server`;
    - StatusCode `STATUS_CODE_OK` against `Ok`;
    - doubles through `ryu` (`5.0` against Go's `5`).

    The rows would not match the clickhouse exporter's rows until those are
    aligned.

## The options in detail

### a. OTAP star tables as Parquet, joined at central

`star.go` writes each OTAP table with:

- decoded ids;
- a `batch_id` column in every table, instead of the Rust exporter's offsets
  and `_part_id`, so that any set of batches joins on `(batch_id, id)`;
- flattened structs;
- the envelope on the root table.

**The one modification central can't do without:** `ser` (CBOR) becomes the
JSON `pcommon.Value.AsString` would give (`ser_json`), because ClickHouse has
no CBOR decoder.

`central.go` `StarTracesSelect` / `StarLogsSelect` rebuild the rows:

- per attribute table, `mapFromArrays(groupArray(key), groupArray(render(value)))`
  grouped by `(batch_id, parent_id)`;
- events and links grouped per span, with their attribute Maps LEFT JOINed on;
- then four hash LEFT JOINs onto the spans.

`render` is `AsString` in SQL:

- ints with `toString`;
- bools as `true`/`false`;
- bytes as `base64Encode`;
- doubles as `NaN` / `Infinity` / `-Infinity`, and with an `e+NN` exponent
  outside [1e-6, 1e21).

The first version wrote `+Inf`, and the hostile test caught it.

- **Correct [M]:** the output equals option b's byte for byte (same exact
  hash), and equals the reference as described in note ¹.
- **Deterministic [M]:**
  - Two independent runs gave identical content under both default and
    single-threaded settings.
  - A retry with the same `insert_deduplication_token` left one copy, even
    when the first attempt used `max_threads = 1` and the retry the defaults.
  - ClickHouse doesn't document `groupArray` order under parallel
    aggregation, so run these inserts with `max_threads = 1` if the rows
    themselves must be identical across attempts. With a token, dedup keys on
    the token and the block index, not on the content [D, REPORT].
- **Cost [M]:** 3.1–3.6× the central CPU of flat Parquet, 1.3–1.8× the peak
  memory, and 4.6× the S3 GET+HEAD requests.
  - It is also 4× per single-batch insert: 164 ms against 42 ms. That matters
    because the model's consumer inserts one batch per token.
  - Per row that is about 11 µs for traces, against about 3 µs.
  - bench/central measured the whole insert at about 5 µs/row. So OTAP star
    tables make central the more expensive side, where today it is the write
    that dominates.
- **Bytes [M]:** larger than flat Parquet with bloom filters (475 KB against
  264 KB for parquetgo), and comparable without them (220 KB against 173 KB).
  Most of it is the spans table, with binary ids and many small columns.

### b. Denormalise at the edge

`flat.go` `Flatten` works on the Arrow tables directly:

1. bucket each attribute table by decoded parent id (a counting sort);
2. emit Map entries, and the events and links arrays;
3. fill parquetgo's exact Arrow schema (`parquetgo.TracesSchema`);
4. write Parquet with the same options as parquetgo's arrow engine.

- **Correct [M]:** it equals the reference after sorting (note ¹). On the
  hostile data it matches the star tables byte for byte.
- **CPU [M]:** 29 ms to decode the IPC, 44 ms to flatten traces, and about
  140 ms to write Parquet with arrow-go's pqarrow.
  - pqarrow is the engine parquetgo already rejected: 634k allocations per
    batch, from its dictionary encoder.
  - With a writer as fast as parquet-go (parquetgo spends about 50–60 ms per
    10k spans on encoding [E]), b would cost about 110 ms for traces and 80 ms
    for logs from OTAP [E].
  - That is still 1.5× parquetgo from pdata, because an OTAP batch has to be
    decoded and pivoted before it can be written row-wise.
- **Rust [E]:** the Rust ClickHouse exporter's transform does the same work,
  natively on arrow-rs. The otap-dataflow project reports 10–20× the
  throughput for OTAP-in/OTAP-out against OTLP, but no number was measured
  here for this transform.
- **Via pdata [M]** (`via-pdata`: Go library decoder, then parquetgo) costs
  161 ms for traces and 96 ms for logs. It also inherits the library's
  silent-drop bug (below).

### c. Arrow IPC files instead of Parquet

- The same record as (b), written by `ipc.NewFileWriter` with zstd, is read
  by `s3(…, 'Arrow', structure)` and gives the same rows [M].
- It is cheaper to write than pqarrow Parquet (121 ms against 213 ms), but
  still more than parquetgo.
- It is 5.5× the bytes, because the Arrow buffers are plain and zstd works
  per buffer.
- Central read 30 MB instead of 3 MB for 10 batches, and spent 1.6× the CPU
  on traces.
- Dictionary-encoded string columns would shrink it, and ClickHouse maps them
  to LowCardinality [E, not tried].
- For S3 transfer, Parquet is the better container. Arrow IPC wins only for
  in-memory hand-off, such as the Rust exporter's `ArrowStream` insert.

### d. Raw OTAP payloads (the zero-work edge)

- **Edge.** Each payload's IPC bytes, exactly as they sit in the
  `BatchArrowRecords`, become their own object (`raw.go`).
  - The edge does no Arrow work: 7 ms of CPU per batch against S3, 2 ms
    locally [M].
  - It stores the fewest bytes: 109 KB per 10k spans.
- **Central.** ClickHouse's `ArrowStream` reads them directly, dictionaries
  and zstd included [M]. The id decoding moves into SQL:
  - `rowNumberInAllBlocks()` for row order, since `_row_number` is NULL for
    ArrowStream [M];
  - the batch from `_path`;
  - running sums for delta ids;
  - quasi-delta ids as a run number, `sum(break) OVER w`, with a running sum
    of `parent_id` inside each run;
  - then the same JOINs as (a).
- **What it costs and where it fails:**
  - It needs `max_threads = 1`.
  - Central CPU is 5.2× the baseline for traces, and peak memory 458 MB for
    10 batches.
  - It can't render CBOR, so testgen loses the map bodies of 750 of its 3,000
    logs [M].
  - Adaptive schemas bite: `DESCRIBE` over a glob infers from the first
    object, so later objects' extra columns (`bytes`, `bool`) were silently
    dropped [M]. This needs one DESCRIBE, or an explicit structure, per object.
  - For the hostile traces, the Go producer writes an IPC field declared
    non-nullable that holds 505 nulls (empty event names). ClickHouse rejects
    the file: `Arrow IPC field 'name' is declared non-nullable but its FieldNode
    reports 505 nulls` [M].
  - Payloads are self-contained only at the start of an IPC stream. A relay
    that receives a long-lived gRPC stream must keep the schema and dictionary
    messages and prepend them to every object [D].
- **Verdict:** good as a cheap archive or replay format. It is not an ingest
  format for ClickHouse SQL. A central otap-dataflow instance that reads these
  objects and runs its ClickHouse exporter would be the natural consumer [E],
  but no S3 receiver exists [D].

(`bar`, the whole `BatchArrowRecords` protobuf as one object, is the same
bytes in one PUT: 1 ms per batch. ClickHouse can't read it.)

## Correctness findings worth reporting upstream [M]

1. **The Go consumer drops whole batches silently.** `Consumer.TracesFrom` and
   `LogsFrom` ignore the error from `RelatedDataFrom`.
   - When a map or slice attribute contains invalid UTF-8, the CBOR decoder
     rejects it (`cbor: invalid UTF-8 string`). The call then returns zero
     batches and a nil error.
   - contrib `otelarrowreceiver` v0.161.0 then counts 0 items and
     acknowledges the request.
   - This lost all 700 hostile spans and all 700 hostile logs in `via-pdata`
     (`ErrLibraryDroppedBatch`).
2. **The Go consumer returns nested map keys in random order.** It decodes
   CBOR into a Go map: 8 different key orders in 50 decodes of one body. The
   ClickHouse row doesn't change, because `AsString` sorts keys. Anything
   that hashes or re-exports the decoded pdata does see the difference.
   `cbor.go` is an order-preserving decoder.
3. **The Go producer drops attributes whose value is Empty.** The key
   disappears; the clickhouse exporter stores it as `''`.
4. **The Go producer writes IPC that ClickHouse rejects:** nulls in a
   non-nullable field (option d).
5. **The Rust parquet exporter's resource and scope ids** (source reading; see
   above).
6. **The Rust ClickHouse exporter's SpanKind, StatusCode and double rendering**
   differ from contrib's.

Encoding is deterministic [M]: the same pdata encoded twice gives
byte-identical payloads, for testgen and for the hostile logs. So a content
key over the OTAP payload (`ContentKey`) is stable across retries that
re-encode.

## Reliability against the model

The model's requirements (`../model/README.md`, `../model/S3NATIVE.md`) are
about the commit record and the consumer, not the data format, so OTAP
changes less than it seems.

- **Commit implies data.**
  - Unchanged: write every data object, then one manifest or log-slot entry
    that names them. A batch is committed if and only if that entry exists.
  - Several objects per batch (7 star tables, or 7 raw payloads) do **not**
    make atomic publication harder. The data objects were never the commit;
    the manifest is.
  - They do cost more: 8 PUTs instead of 2, and 7 possible orphans per
    failed batch for GC to sweep. Every one of them lives in the
    epoch/generation namespace, so the closed-namespace orphan sweep in
    S3NATIVE covers them unchanged.
  - One Parquet file can't hold several schemas. Arrow IPC can't either (one
    schema per stream), so a single object would need a custom container of
    concatenated streams with an index, which ClickHouse can't read.
- **A unique commit per batch, and retry idempotency.**
  - `ContentKey` hashes the OTAP payloads, and encoding is deterministic [M].
    So a content-derived batch id and content-addressed object keys
    (`ns/e{epoch}/g{gen}/{content}/{table}.parquet`) work as S3NATIVE
    designs them.
  - The commit is a create-only log slot (`If-None-Match: *`; `Sink.Put` has
    the flag).
  - On an OTAP-native edge the durable buffer keeps the Arrow IPC segments, so
    a retry replays the same bytes. Nothing needs re-encoding.
- **At-most-once central ingest.**
  - The same consumer protocol applies: check before insert, a content token,
    and a lease with a time bound.
  - Token dedup was measured for every layout [M]. The 10-batch insert,
    retried with the same token, left exactly 100,000 rows each time.
  - Star and raw JOINs are more complex plans, but the token dedup still held.
  - Raw must run single-threaded to be correct at all.
- **Seal and completeness.**
  - Unchanged: the seal is a log position, not a count.
  - One thing to watch: the manifest must list the payload types present.
    OTAP omits empty tables, and the central query must read an absent table
    as empty (`null('structure')`), not as a missing file.
- **What the stock Rust parquet exporter would need** before any of this
  applies:
  1. ack or nack after the commit, where today nothing is acknowledged;
  2. one set of files per batch, instead of multi-batch files that live
     minutes in multipart uploads and are lost on a crash;
  3. content-derived object names, instead of `part-<millis>-<uuid>`;
  4. a manifest or log commit;
  5. per-batch ids, or a fix for the resource and scope offsets.

  That is a rewrite of its writer. For option b, adapting the ClickHouse
  exporter, which already acks and nacks, is the smaller change.

## Deployment and credentials

The three production setups:

1. EKS with IRSA or EKS Pod Identity, against AWS S3.
2. Nutanix Objects with static keys: a custom endpoint, path-style addressing,
   possibly a private CA.
3. IAM Roles Anywhere (`credential_process` through `aws_signing_helper`),
   against AWS S3 from outside AWS.

| Client | 1. IRSA | 1. Pod Identity | 2. Nutanix Objects | 3. Roles Anywhere |
|---|---|---|---|---|
| **Rust otap-dataflow** (`object_store` 0.13.2, used by the parquet exporter and by any S3 writer added to it) [D] | ✓ `auth: {type: web_identity}`, or the `AWS_ROLE_ARN` / `AWS_WEB_IDENTITY_TOKEN_FILE` that EKS injects (`from_env`) | ✓ from env: `AWS_CONTAINER_CREDENTIALS_FULL_URI` plus `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE` (`EKSPodCredentialProvider`) | ✓ `static_credentials`, `endpoint`, `virtual_hosted_style_request: false`, `allow_http`. **Private CA:** reqwest with rustls native roots reads the system trust store (or `SSL_CERT_FILE` [E]). `ClientOptions::with_root_certificate` exists, but the exporter config doesn't expose it | **✗ natively.** No `credential_process` and no shared config or credentials files (the `default` auth's doc comment says otherwise; the code doesn't read them). Workarounds [E, untested]: `aws_signing_helper serve` (a local IMDSv2 endpoint) with `AWS_METADATA_ENDPOINT` pointed at it, or a local container-credentials endpoint |
| **Go** (aws-sdk-go-v2: parquetgo, and this spike's `Sink`) [D] | ✓ via `config.LoadDefaultConfig` | ✓ (container provider with the token file) | ✓ `BaseEndpoint`, `UsePathStyle`, a custom CA through `AWS_CA_BUNDLE` or the HTTP client | ✓ `credential_process` in the shared config profile |

parquetgo now uses the default credential chain when no keys are given, and
each of these modes is tested locally with stand-ins (`../parquetgo/README.md`,
"Credentials and deployment targets") [M]. This spike's `Sink` still takes
static keys or anonymous access only; the same change applies [E]. The Go collector's contrib
components already link this SDK.

Nutanix Objects' support for the conditional writes S3NATIVE relies on
(`If-None-Match` on PUT) is unknown [E]. Check it with `../s3cas`'s probe
before relying on it there. `object_store` can send conditional PUTs
(`PutMode::Create`), but the parquet exporter doesn't use them [D].

## Recommendation, and when it flips

- **Keep parquetgo for Go edges.**
  - Among Go-built variants it is the cheapest edge; only the no-transform
    raw and bar variants use less CPU.
  - Its central ingest is the cheapest.
  - It already matches the clickhouse exporter's rows exactly.
- **Use OTAP as the transport into the edge only where it is already
  deployed**, and denormalise to the flat ClickStack Parquet before S3
  (option b):
  - Rust: adapt the ClickHouse exporter's transform, then write Parquet per
    batch plus a manifest, and ack after the commit.
  - Go: decode to pdata and use parquetgo, once the silent-drop bug is fixed
    or guarded against, as `via-pdata` does.
- **Don't make central join OTAP's tables** (a, d). It moves 3–5× the CPU to
  the place that already dominates cost. It adds single-threaded window
  decoding (d), and it can't handle CBOR values.

It flips toward storing OTAP tables (d, or a) when:

- **Edge CPU and egress are the binding constraint and central is not.**
  Raw payloads cost 7 ms per batch and 41% of parquetgo's bytes (109 against
  264 KB).
- **The consumer is an OTAP pipeline**, not ClickHouse SQL: a central
  otap-dataflow instance reading S3 and writing ClickHouse, or an archive
  replayed later.
- **Map and slice values are rare or unimportant**, or the edge converts CBOR
  to JSON, which gives up most of d's zero-work advantage.

## Measured vs estimated

- **Measured [M]:**
  - every edge number (`results/otapbench.md` and `.jsonl`);
  - central cost, determinism and dedup (`results/central.md` and
    `central-determinism.md`);
  - correctness (`results/correctness.txt`);
  - the library bugs;
  - ClickHouse reading raw payloads;
  - Rust toolchain and dependency sizes.
- **From source [D]:** everything about the Rust exporters, `object_store`
  credentials, and contrib's receiver ack behaviour.
- **Estimated [E]:**
  - option b with a parquet-go-class writer;
  - any Rust performance;
  - dictionary-encoded IPC sizes;
  - Roles Anywhere workarounds;
  - Nutanix conditional writes;
  - `SSL_CERT_FILE` with object_store.
- **Not tested:** a Rust build, real AWS, metrics, multi-node central, and
  concurrent publishing.
- **Noise:** the load average was 9–20 on 4 vCPUs. Latency spreads are wide,
  so compare CPU per batch, which is steadier. The ClickHouse CPU figures are
  server-wide counter deltas (the server's `query_log` is off), so other load
  can leak in; medians of 3.
- **An incident during the runs:** SeaweedFS turned every volume read-only
  when free disk fell under its 1% minimum (about 2.5 GB). Deleting Go
  build-cache entries unused for more than 6 hours freed 4 GB, and writes
  resumed. The first benchmark attempt was discarded.

## Files and running

| Path | What |
|---|---|
| `decode.go` | pdata → OTAP (Go producer, one self-contained batch); OTAP IPC → Arrow tables with ids decoded (delta and quasi-delta, as the Go consumer does) |
| `flat.go` | Option b/c: Arrow-native denormalisation into parquetgo's schema |
| `star.go` | Option a: OTAP tables with decoded ids, `batch_id`, flattened structs, CBOR → JSON |
| `raw.go` | Option d: store payloads as-is; central SQL that decodes ids with window functions |
| `central.go` | Central SQL: flat structures, star JOIN queries, `AsString` rendering in SQL |
| `cbor.go` | Order-preserving CBOR → pcommon decoder |
| `write.go`, `publish.go`, `variants.go` | Parquet and IPC writers, S3/local sink (optional `If-None-Match`), manifests, the publishing variants |
| `correctness_test.go` | Every variant against parquetgo on the server: testgen and hostile data, exact and normalised, with per-column explanations; library-bug tests |
| `central_bench_test.go` | Central INSERT cost, determinism, token dedup |
| `rawipc_test.go`, `emptyattr_test.go`, `smoke_test.go` | ClickHouse reading raw payloads; Empty-value loss; a local smoke test |
| `cmd/otapbench`, `results/runbench.sh`, `results/summarize.py` | The edge benchmark (same accounting as parquetgo's `pubbench`) |

```sh
export OTAP_TEST_S3=http://127.0.0.1:18333/otel/otap OTAP_TEST_S3_KEY=otel OTAP_TEST_S3_SECRET=otelsecret \
       OTAP_TEST_CLICKHOUSE=http://127.0.0.1:18123
go test -count=1 -v .                                  # correctness + library-bug tests (skips without the env)
OTAP_TEST_EXPLAIN=1 go test -run TestCentralMatchesReference -v .
OTAP_CENTRAL_BENCH=1 go test -timeout 30m -run TestCentralIngestCost -v .
go build -o $S/otapbench ./cmd/otapbench && S=$S results/runbench.sh && python3 results/summarize.py $S/bench.jsonl
```

Objects were written under `s3://otel/otap/` on the local SeaweedFS. No
buckets were created.

## Sources

- OTAP specification: `docs/otap-spec.md` in
  [open-telemetry/otel-arrow](https://github.com/open-telemetry/otel-arrow) (main, 5db8358).
- Go library `otel-arrow/go` v0.57.0:
  - `pkg/otel/arrow_record/consumer.go` (`TracesFrom`, `LogsFrom`);
  - `pkg/otel/common/cbor.go`;
  - `pkg/otel/common/otlp/attributes.go`.
- Rust `rust/otap-dataflow`:
  - `crates/core-nodes/src/exporters/parquet_exporter/{mod,idgen,writer,schema}.rs`;
  - `crates/contrib-nodes/src/exporters/clickhouse_exporter/`;
  - `crates/otap/src/{object_store.rs,cloud_auth/aws.rs}`;
  - `crates/core-nodes/src/processors/durable_buffer_processor/README.md`.
- `object_store` 0.13.2 (crates.io): `src/aws/builder.rs` (credential chain,
  `from_env`), `src/client/mod.rs` (`with_root_certificate`), `Cargo.toml`
  (`rustls-tls-native-roots`).
- contrib `receiver/otelarrowreceiver` v0.161.0, `internal/arrow/arrow.go`;
  contrib `exporter/clickhouseexporter` v0.161.0, `exporter_traces.go`.
- [OTel-Arrow Phase 2 (OpenTelemetry blog, 2026)](https://opentelemetry.io/blog/2026/otel-arrow-phase-2/);
  [ClickStack: ingesting with OpenTelemetry](https://clickhouse.com/docs/use-cases/observability/clickstack/ingesting-data/opentelemetry).
