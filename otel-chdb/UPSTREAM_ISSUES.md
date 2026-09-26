# Upstream issues found by the otel-chdb spike: drafts

Short issue drafts for the upstream bugs and gaps the spike ran into. **None
has been filed yet.** Each draft names its source in this repository, so the
evidence is there to attach. The summary table is in
[DECISIONS.md §6](DECISIONS.md#6-upstream-bugs-found).

Before filing any of these:

- Re-check each one against the project's current release; several were read
  at `main` on 2026-09-24/25.
- Search for existing issues.
- Attach the minimal repro, not this spike.
- U9 is from source reading only. Reproduce it before filing.

---

## U1. parquet-go: `Writer.Reset` empties `path_in_schema` in every later file

- **Project / version:** `github.com/parquet-go/parquet-go` v0.32.0.
- **Source here:** [`parquetgo/README.md`](parquetgo/README.md) §"parquet-go's
  `Writer.Reset` erased the column paths" (`fa3af89`); workaround
  `parquetgo/pgo.go` `restoreColumnPaths`; test `TestReusedWriterIdentical`.

**Title:** `Writer.Reset` clears `ColumnMetaData.PathInSchema` of the next file,
because the slice aliases `ColumnWriter.columnPath`

**Repro.**

1. Create a `parquet.Writer` for any schema with at least one column.
2. Write rows and `Close`: this is file 1.
3. Call `Reset(w2)` with a new `io.Writer`.
4. Write rows and `Close`: this is file 2.
5. Read file 2's footer (for example with `parquet.OpenFile(...).Metadata()`,
   or pyarrow's `metadata.row_group(0).column(i).path_in_schema`).

**Expected:** every column chunk's `path_in_schema` names the column, as in
file 1.

**Actual:** every element of every column chunk's `path_in_schema` in file 2
(and in later files) is `""`.

- `Reset` runs `format.ColumnMetaData.Reset`, which calls
  `clear(PathInSchema)`, and that slice is the writer's own `columnPath`.
- Readers that match chunks by position still work: ClickHouse 26.10 and
  pyarrow do. Readers that match by path may not.
- A file written by a reused writer is not byte-identical to one from a fresh
  writer, which breaks content-addressed retries.

**Suggested fix:** copy `columnPath` when building the `ColumnChunk` metadata,
instead of aliasing it.

---

## U2. otel-arrow (Rust): OTAP views return no attributes for span events and links

- **Project / version:** `open-telemetry/otel-arrow`, `rust/otap-dataflow`,
  `main` at `5db8358` (2026-09-24).
- **Source here:** [`otap-rs/README.md`](otap-rs/README.md) §Upstream;
  `otap-rs/patches/0002-otap-views-u32-parent-id-dictionary16.patch`; test
  `otap-rs/tests/otap_view.rs` (`fb9527c`).

**Title:** `build_attribute_index_u32` rejects `Dictionary(UInt16, UInt32)`
parent ids, so `OtapTracesView` drops all event and link attributes

**Repro.**

1. Encode OTLP traces whose spans have events and links with attributes,
   using upstream's own OTLP→OTAP encoder. testgen data works: 3,000 spans.
2. Walk the result with `OtapTracesView`, and read each event's and link's
   attributes.

**Expected:** the attributes that were encoded.

**Actual:** none, for every event and link.

- `views/otap/common.rs` `build_attribute_index_u32` accepts `UInt32` and
  `Dictionary(UInt8, UInt32)` parent ids.
- The encoder writes `Dictionary(UInt16, UInt32)` for `SPAN_EVENT_ATTRS` and
  `SPAN_LINK_ATTRS`.
- On testgen, 750 of 3,000 spans differ, all in `Events.Attributes` and
  `Links.Attributes`.

**Fix (tested):** use `MaybeDictArrayAccessor`, as the u16 variant already
does. Patch 0002 is ready to submit.

---

## U3. otel-arrow (Go): the consumer silently drops a batch on invalid UTF-8, and the receiver acks it

- **Project / version:** `github.com/open-telemetry/otel-arrow/go` v0.57.0
  (2026-09-21), `pkg/otel/arrow_record/consumer.go`; contrib
  `receiver/otelarrowreceiver` v0.161.0.
- **Source here:** [`otap/README.md`](otap/README.md) §"Correctness findings
  worth reporting upstream", item 1; `otap/correctness_test.go`
  (`ErrLibraryDroppedBatch`) (`a14c6f8`).

**Title:** `Consumer.TracesFrom` / `LogsFrom` ignore the error from
`RelatedDataFrom`: a batch with invalid UTF-8 in a map or slice attribute
vanishes with a nil error

**Repro.**

1. Build a `ptrace.Traces` with one span whose attribute is a map (or slice)
   holding a string value with invalid UTF-8 bytes (e.g. `"\xff"`).
2. Encode it with `arrow_record.Producer`.
3. Decode it with `arrow_record.Consumer.TracesFrom`.

**Expected:** an error. The CBOR decoder fails with `cbor: invalid UTF-8
string`. The receiver should NACK the request.

**Actual:**

- `TracesFrom` returns zero batches and `nil`.
- `otelarrowreceiver` counts 0 items and **acknowledges** the request.
- All 700 hostile spans and all 700 hostile logs were lost this way.

The same applies to `LogsFrom`.

**Suggested fix:** propagate the error from `RelatedDataFrom`. Separately,
decide whether invalid UTF-8 inside CBOR should be accepted, since pdata
itself accepts it.

---

## U4. SeaweedFS: conditional `CompleteMultipartUpload` is not atomic under concurrency

- **Project / version:** `seaweedfs/seaweedfs` 4.47 (built from source); the
  code is unchanged on `master` at `635f69a` (2026-09-25).
- **Source here:** [`model/S3NATIVE.md`](model/S3NATIVE.md) §2;
  `s3cas/probe_test.go` `TestMultipartCreateRace`, `TestMultipartCASRace`
  (`922ea7d`).

**Title:** `CompleteMultipartUpload` with `If-None-Match: *` or `If-Match`
checks the condition outside the object lock, so several concurrent completions
succeed

**Repro** (single gateway, single filer, aws-sdk-go-v2):

1. Start 8 multipart uploads to the same new key.
2. Complete all 8 concurrently with `If-None-Match: *`.
3. Repeat for 5 rounds.

**Expected:** exactly one completion returns 200 per round; the others get
412. Single-part `PutObject` with the same header behaves this way: exactly 1
winner in each of 20 rounds of 16 goroutines.

**Actual:** 4, 6, 6, 8 and 8 winners in the 5 rounds. The same test with
`If-Match` on one ETag (6 concurrent, 3 rounds) gave 4, 5 and 4 winners.

**Cause** (`weed/s3api`):

- `completeMultipartUpload` (`filer_multipart.go`) calls
  `checkConditionalHeaders` at the gateway, outside any lock, when an owner
  filer exists.
- It then writes with `routedMkFile` and no `WriteCondition`: check-then-act.
- Single-part PUT re-checks the condition under `withObjectWriteLock`, or
  routes a `filer_pb.WriteCondition` to the owner filer
  (`s3api_object_routed_write.go`).

**Suggested fix:** take the locked or conditional-routed path whenever the
request carries conditional headers.

---

## U5. arrow-go: V1 data pages can split a repeated column's row while a page index is written

- **Project / version:** `github.com/apache/arrow-go/v18` v18.7.0, `pqarrow`.
- **Source here:** [`parquetgo/README.md`](parquetgo/README.md) §"arrow-go can
  write Parquet that ClickHouse rejects"; `parquetgo/compare/pagesplit_test.go`,
  `compare/cmd/pqpages` (`d55c1a7`).

**Title:** with V1 data pages, a page of a repeated column can start at
repetition level 1 even though the file carries an offset index

**Repro.**

1. Write a table with a `MAP<string,string>` column (or any `LIST`), with
   some values around 100 KB. Use V1 data pages and the default page index.
2. Inspect the pages: `SpanAttributes.value` pages start at repetition
   level 1.
3. Read the file with ClickHouse 26.10.

**Expected:** a writer that emits a page index starts every page at a row
boundary; the Parquet spec requires this for the offset index.

**Actual:** ClickHouse fails with `Invalid array of tuples ... different array
lengths`.

| Variant | ClickHouse reads it |
|---|---|
| V1 pages, or V1 without dictionary | no |
| V1 pages without a page index | yes |
| V2 pages | yes |
| parquet-go | yes |

---

## U6. otel-arrow (Go): the producer writes nulls into a non-nullable IPC field

- **Project / version:** `otel-arrow/go` v0.57.0.
- **Source here:** [`otap/README.md`](otap/README.md) §d and findings item 4
  (`a14c6f8`).

**Title:** the producer declares span-event `name` non-nullable but writes
nulls for empty names

**Repro.** Encode traces with span events whose name is `""`: the hostile
700-span dataset has 505. Read the SPAN_EVENTS IPC payload with a strict
reader, for example ClickHouse `FORMAT ArrowStream`.

**Expected:** either a nullable field, or empty strings.

**Actual:** ClickHouse rejects the payload: `Arrow IPC field 'name' is declared
non-nullable but its FieldNode reports 505 nulls`.

---

## U7. otel-arrow (Go): attributes whose value is Empty are dropped

- **Project / version:** `otel-arrow/go` v0.57.0, `pkg/otel/common/otlp/attributes.go`.
- **Source here:** [`otap/README.md`](otap/README.md) findings item 3;
  `otap/emptyattr_test.go` (`a14c6f8`).

**Title:** the producer drops attribute keys whose value is
`pcommon.ValueTypeEmpty`; the round trip loses the key

**Repro:** a span with attribute `k` set to an Empty value → producer →
consumer.

**Expected:** `k` present with an Empty value. The contrib clickhouse
exporter stores it as `''`.

**Actual:** `k` is missing. 577 of 700 hostile spans lost at least one key.

---

## U8. otel-arrow (Go): nested map keys come back in random order

- **Project / version:** `otel-arrow/go` v0.57.0, `pkg/otel/common/cbor.go`.
- **Source here:** [`otap/README.md`](otap/README.md) findings item 2;
  order-preserving decoder `otap/cbor.go` (`a14c6f8`).

**Title:** CBOR-encoded map attributes decode through a Go map, so key order
is nondeterministic

**Repro:** decode the same map-valued attribute 50 times.

**Expected:** a stable order, ideally the order that was encoded.

**Actual:** 8 different key orders in 50 decodes. `AsString` sorts, so rows
rendered through it don't change. Anything that hashes or re-exports the
decoded pdata does see the difference.

---

## U9. otap-dataflow parquet exporter: resource and scope ids are not offset (unverified)

- **Project / version:** `rust/otap-dataflow`, `main` `5db8358`,
  `crates/core-nodes/src/exporters/parquet_exporter/{idgen,writer}.rs`.
- **Source here:** [`otap/README.md`](otap/README.md) §"What OTAP is, and what
  exists", "Possible bug" (`a14c6f8`). **Found by reading the source; not
  executed.** Reproduce before filing.

**Title:** `PartitionSequenceIdGenerator` offsets top-level `id` and
`parent_id` columns but not the root table's `resource.id` / `scope.id`
struct fields

**Suspected repro:**

1. Export two or more batches into one file with resource and scope
   attributes.
2. Join `RESOURCE_ATTRS` / `SCOPE_ATTRS` to the root table by id.

**Expected:** each row joins to its own resource and scope attributes.

**Suspected actual:** from the second batch in a file on, resource and scope
attributes join to the wrong rows, or to none. `RESOURCE_ATTRS.parent_id` is
shifted and the struct field isn't. Upstream's query example joins only
`log_attrs`, which may be why this hasn't shown.

**Related, feature gaps of the same exporter** (Experimental):

- no ack or nack ("support for acknowledgements and nack messages" is on its
  TODO list);
- a write error ends the node;
- `WriterProperties::default()` means uncompressed output.

---

## U10. otap-dataflow OTAP receiver: one undecodable batch closes the whole stream

- **Project / version:** `rust/otap-dataflow` `main` `5db8358`, OTAP receiver.
- **Source here:** [`otap-rs/README.md`](otap-rs/README.md) §Upstream and
  §Inputs (`8cf80ad`).

**Title:** a batch with invalid UTF-8 in an Arrow `Utf8` column closes the
gRPC stream instead of NACKing that batch

**Repro:**

1. Run the OTAP receiver with `wait_for_result`.
2. Send batches from the Go `arrow_record.Producer` on one long-lived stream,
   including one with invalid UTF-8 in a string attribute.

**Expected:** a `BatchStatus` NACK for that `batch_id`, with a permanent
status. The stream stays open.

**Actual:** "Invalid UTF8 sequence" from Arrow's IPC reader, and the whole
stream closes. A client that reconnects and resends is stuck on that batch.

**Related:** the receiver hands on records with transport-optimized
(delta-encoded) ids, and the views don't decode them. A consumer must call
`decode_transport_optimized_ids()` first. That deserves a doc note, or a
debug assertion in the views.

---

## U11. otap-dataflow `pdata`: depends on the whole DataFusion crate for two types

- **Project / version:** `rust/otap-dataflow` `main` `5db8358`, crate `pdata`.
- **Source here:** `otap-rs/patches/0001-pdata-depend-on-datafusion-leaf-crates.patch`
  (`fb9527c`).

**Title (enhancement):** depend on `datafusion-common` and
`datafusion-expr-common` instead of `datafusion`

**Detail:** `pdata` uses only `ScalarValue` and `ColumnarValue`, and no feature
turns the dependency off. With the patch, the graph shrinks from 424 to 395
crates, and DataFusion from 25 crates to 2. Behaviour is unchanged. The patch
is ready.

---

## U12. ClickHouse: a large Parquet object splits into two blocks nondeterministically under single-block settings

- **Project / version:** ClickHouse 26.10.1.618 (the `master` build).
- **Source here:** [`parquetgo/README.md`](parquetgo/README.md) §Metrics,
  Correctness, "Central ingest under the FASTPATH single-block settings"
  (`fa3af89`).

**Title:** `INSERT … SELECT FROM s3(<one Parquet object>)` with the
single-block settings sometimes produces 2 blocks, at a row that varies between
runs, which breaks `insert_deduplication_token` on retry

**Repro:**

1. One Parquet object with one row group of 150,000 rows (a metrics gauge
   table, about 158 MB decoded).
2. Insert it with:
   - `max_threads = 1`, `max_insert_threads = 1`;
   - `max_block_size` and `max_insert_block_size` ≥ rows;
   - `min_insert_block_size_rows = 0`, `min_insert_block_size_bytes = 0`;
   - `input_format_parquet_max_block_size` ≥ rows;
   - `input_format_parquet_prefer_block_bytes` above the decoded size;
   - and a token.
3. Repeat the insert on fresh tables.

**Expected:** one block and one part every time, so a retry with the same
token is fully deduplicated.

**Actual:**

- In most runs the insert split into two blocks at a varying row: 139,135 +
  10,865, or 97,720 + 52,280.
- A retry that split differently re-inserted the tail: 150,000 + 3,477 rows.
- `input_format_parquet_preserve_order = 1` and
  `input_format_parquet_use_offset_index = 0` each looked like a fix once,
  then failed.
- Turning squashing on always gave one part, but a retry once inserted a full
  second copy (302,180 rows).
- Nothing of 100,000 rows or fewer split, in more than 60 inserts.

**Related** ([`otap-rs/README.md`](otap-rs/README.md) §ClickHouse facts): in
26.10, `INSERT … SELECT` without a token isn't deduplicated under the default
`deduplicate_insert_select = enable_when_possible`. With
`enable_even_for_bad_queries`, block ids depend on how a statement groups its
objects. That may be by design, but it deserves documentation.

---

## U13. ClickHouse / chDB AWS credential chain: gaps for Roles Anywhere, custom STS and private CAs

- **Project / version:** ClickHouse 26.10.1; chDB 26.7.3 (libchdb);
  `src/IO/S3/Credentials.cpp`.
- **Source here:** [`parquetgo/README.md`](parquetgo/README.md) §"Credentials
  and deployment targets" (`bfe5d85`).

**Title (feature / docs):** support `credential_process`, honour
`AWS_ENDPOINT_URL_STS` for web identity, and document `AWS_CA_BUNDLE` as
unsupported

**Findings:**

1. **`credential_process` is not supported.** With `AWS_PROFILE` naming such
   a profile, the helper never runs and the read is anonymous
   (`AccessDenied`). The source comment says the process provider was removed
   as "useless in our case". IAM Roles Anywhere relies on it. The workaround
   is `aws_signing_helper serve` with `AWS_EC2_METADATA_SERVICE_ENDPOINT`.
2. **The web-identity STS endpoint is hard-coded** as
   `https://sts.<region>.amazonaws.com`. `AWS_ENDPOINT_URL_STS` and
   `AWS_ENDPOINT_URL` are ignored, which matters for VPC endpoints and
   testing. The STS region comes from `AWS_DEFAULT_REGION`, not `AWS_REGION`.
3. **`AWS_CA_BUNDLE` is ignored**; the string isn't in the binary.
   `SSL_CERT_FILE` or `<openSSL><client><caConfig>` works. For chDB, which has
   no config file in an embedding process, `SSL_CERT_FILE` is the only route.

**Expected:** parity with the AWS SDKs' default chain, or documented
limitations.

---

## U14. object_store (Rust): credential and client-option gaps

- **Project / version:** `object_store` 0.13.2 (`src/aws/builder.rs`,
  `src/client/mod.rs`).
- **Source here:** [`otap-rs/README.md`](otap-rs/README.md) §Upstream and
  §Credentials; workarounds in `otap-rs/src/creds.rs`, `src/store.rs`
  (`fb9527c`, `8cf80ad`).

**Title (several small issues):**

1. **`AmazonS3Builder::with_client_options` silently resets `allow_http`**
   set earlier through `with_allow_http`. The build then fails with "URL
   scheme is not allowed". Expected: options merge, or the order is
   documented.
2. **No `credential_process` provider**, and no shared config or credentials
   files. The `default` auth's doc comment suggests otherwise; the code
   doesn't read them.
3. **IMDS endpoint variable:** object_store reads `AWS_METADATA_ENDPOINT`, not
   the SDKs' `AWS_EC2_METADATA_SERVICE_ENDPOINT`.
4. **Web identity refuses an `http://` `AWS_ENDPOINT_URL_STS`.** It is
   https-only, which blocks local testing.
5. The exporter-level config has no way to pass a root certificate, although
   `ClientOptions::with_root_certificate` exists. This one is for the
   otap-dataflow exporters.

---

## U15. contrib awss3exporter: retries are not idempotent, and encodings can't set metadata

- **Project / version:** `opentelemetry-collector-contrib/exporter/awss3exporter`
  v0.161.0.
- **Source here:** [`awss3/README.md`](awss3/README.md) (source reading and
  `retrytest/`, `collector/` demos); the patch `awss3/awss3inline.patch`
  (`ebf5376`).

**Title (feature):** idempotent object keys and conditional writes; let
encoding extensions set Content-Type and user metadata

**Repro of the duplicate:**

1. Run exporterhelper with `timeout: 1s` and `retry_on_failure`.
2. Put a proxy in front that holds the first PUT's answer for 3 s but applies
   it.

**Expected:** one object per request.

**Actual:**

- Two objects with different random keys (`traces_525918618`,
  `traces_317672089`) and the same rows.
- In a collector demo with SIGKILL, 29 objects and 1,450 rows were stored for
  1,000 spans.
- The key's random part (`randInt()`, about 9×10⁸ values) is new on every
  attempt. The time partition is read at each attempt, so a retry can land in
  another partition.

**Proposals, in increasing scope:**

1. an optional method on encoding extensions returning Content-Type and
   metadata;
2. `if_none_match: true` plus a content-derived unique key, which makes the
   stock retry idempotent within a partition;
3. a `key_mode: sequence` per-epoch create-only log (our patch: +110/−2 lines
   plus a 285-line appender).

**Doc note:** the default `retry_on_failure.max_elapsed_time` (300 s)
**deletes** items from the persistent queue after the last failure. Since
`sending_queue.batch` runs after the queue, redelivered requests re-batch
differently after a restart.

---

## U16. chdb-go: binary-safe inserts, and the CLI ignores `-path`

- **Project / version:** `chdb-io/chdb-go` at `9f8e35a` (v2.2.0 era), with
  libchdb 26.7.3.
- **Source here:** [`README.md`](README.md) §"The fork";
  `patches/0001-chdb-go-binary-safe-streaming-insert.patch` (`d9a5e3c`).

**Title (1):** `Session.Query` passes SQL as a NUL-terminated C string, so
RowBinary, Native, Parquet and Arrow inserts are impossible

- **Repro:** `sess.Query("SELECT length('a\x00b')")` sees `SELECT length('a`.
  Any binary insert payload is cut at its first zero byte.
- **Expected:** a length-delimited query and insert API.
- **Proposal (patch ready, about 310 lines plus 130 of tests):** bind
  `chdb_query_n` and the `chdb_stream_insert_n` family as a second interface,
  `ChdbInsertConn`, and add `Session.Insert`, `InsertStream` and `QueryBytes`.
  Symbols are probed, and an older libchdb returns `ErrInsertABIUnavailable`.
  The fork is 2.6× the throughput of the JSON path at 10k-span batches.

**Title (2):** `go run ./chdb-go -path p "SQL"` ignores `-path` for single
queries and runs them in a throwaway session.

**Related, chDB itself:**

- a process with MergeTree tables can't shut the engine down cleanly on
  26.7.3;
- `INSERT … SELECT FROM input()` is refused by the streaming insert.

---

## U17. SeaweedFS: two S3 compatibility differences

- **Project / version:** SeaweedFS 4.47.
- **Source here:** [`model/S3NATIVE.md`](model/S3NATIVE.md) §2;
  [`otap-rs/README.md`](otap-rs/README.md) §Consumer "SeaweedFS quirk".

**Title (1): `PUT` with `If-Match` on a missing key returns 412; AWS
documents 404**

- **Repro:** `PutObject` with `If-Match: "<any etag>"` on a key that doesn't
  exist.
- **Expected (AWS):** 404 `NoSuchKey`.
- **Actual:** 412.

**Title (2): `ListObjectsV2` with `StartAfter` naming a "directory" returns
nothing**

- **Repro:** objects `lane/E1/00…00.parquet`, `lane/E1/00…01.parquet`;
  `ListObjectsV2(Prefix="lane/", StartAfter="lane/E1/")`.
- **Expected (AWS, lexicographic):** both objects, since they sort after
  `lane/E1/`.
- **Actual:** nothing: the key is treated as "after that whole directory".
  The workaround is `StartAfter="lane/E1/0"`.

**Also noted:**

- SeaweedFS rejects any `X-Amz-Security-Token` its own STS didn't issue
  (`InvalidAccessKeyId`).
- An emptied "directory" still appears in delimiter listings for some time.
- It stops accepting writes below 1% free disk, and doesn't reclaim deleted
  space until a vacuum.

These are probably by design; document them rather than file bugs.

---

## U18. Quint Rust evaluator: `--mbt` traces label the initial state `step`

- **Project / version:** `informalsystems/quint` evaluator v0.6.0 (commit
  `513910b`, built from source), with quint 0.32.0.
- **Source here:** [`otap-rs/README.md`](otap-rs/README.md) §Upstream and
  §"The Quint Rust evaluator" (`fb9527c`).

**Title:** about 40% of `quint run --mbt` traces record `mbt::actionTaken =
"step"` for state 0 instead of `"init"`

**Repro:** `quint run --mbt --out-itf …` on `model/s3Inline.qnt`
(`--main s3InlineDesign`) with the Rust backend. Inspect
`mbt::actionTaken` of each trace's first state.

**Expected:** `init`, which the TypeScript backend gives for every trace.

**Actual:** `step` in about 40% of traces. Only ever for state 0.

**Also:**

- quint 0.32 downloads the evaluator from GitHub releases at runtime, with no
  documented offline path. Building it and copying it to
  `~/.quint/rust-evaluator-v0.6.0/` works.
- In a module wrapping an instance, a state variable read on the right of
  `G::x' = …` resolves in `G`'s namespace.
- Nested instances' names are not re-exported.

---

## U19. ClickHouse: `ParquetMetadata` omits footer key-value metadata

- **Project / version:** ClickHouse 26.10.1.
- **Source here:** [`awss3/README.md`](awss3/README.md) §"Test results",
  `probe/` (`ebf5376`).

**Title (feature):** expose the Parquet footer's `key_value_metadata` in
`FORMAT ParquetMetadata`

**Repro:** write a Parquet file with footer key-value pairs (e.g.
`oscope-seq`), then `SELECT * FROM s3('…', ParquetMetadata)`.

**Expected:** a column with the key-value pairs. pyarrow and parquet-go read
them.

**Actual:** the columns are `num_columns, num_rows, num_row_groups,
format_version, metadata_size, total_*_size, columns, row_groups`, with no
key-value metadata. Batch descriptions must therefore be fetched with a HEAD
per object (`x-amz-meta-*`).

**Also noted:** for `FORMAT ArrowStream`, the `_row_number` virtual column is
NULL. Row order had to come from `rowNumberInAllBlocks()`
([`otap/README.md`](otap/README.md) §d).
