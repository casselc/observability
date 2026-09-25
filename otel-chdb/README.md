# otel-chdb: a chDB exporter for the OpenTelemetry Collector

`chdbexporter` is a collector exporter, like the contrib
[clickhouse exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/clickhouseexporter),
but its "ClickHouse" is chDB running inside the collector process. It writes
the same `otel_traces` / `otel_logs` schema, so anything that reads that
exporter's tables reads these.

| Directory | What it is |
| --- | --- |
| [`chdb-go/`](chdb-go/) | [chdb-go](https://github.com/chdb-io/chdb-go) at upstream `9f8e35a`, plus one patch: a binary-safe streaming insert. The patch is also in [`patches/`](patches/) for sending upstream. |
| [`chdbexporter/`](chdbexporter/) | The exporter: config, factory, RowBinary and JSONEachRow encoders, tests, and in-package benchmarks. |
| [`bench/`](bench/) | A head-to-head against the contrib clickhouse exporter writing to a real ClickHouse server. [`bench/results/`](bench/results/) holds the raw output. |
| [`model/`](model/README.md) | Quint models of the publishing protocol and of object lifetime, with scenario tests; what they found, and which ClickHouse settings mitigate it. |
| [`otelcol/`](otelcol/) | An `ocb` distribution (OTLP receiver, memory limiter, chdb exporter, file_storage), configs for local storage and for edge-buffer publishing, and end-to-end demos of both. |

## Can it be done easily with chdb-go? Mostly

chdb-go loads libchdb with purego, so the collector still builds with
`CGO_ENABLED=0`: `otelcol/` builds with the stock builder, and `go version -m`
reports `CGO_ENABLED=0`. chdb-go also turns off chDB's signal handlers, which
would otherwise break the Go runtime, and it already shares one data path
between sessions.

What it lacks is a way to insert binary data. `Session.Query` passes its SQL to
libchdb as a C string, which ends at the first NUL byte. RowBinary, Native,
Parquet and Arrow payloads all contain NULs, so every insert through stock
chdb-go has to be text. The exporter supports three insert paths:

| `insert_format` | Needs | How |
| --- | --- | --- |
| `rowbinary` (default) | the fork | RowBinary through `Session.Insert`, which calls `chdb_stream_insert_n` |
| `file` | stock chdb-go | RowBinary written to a temp file, then `INSERT ... SELECT FROM file()` |
| `json` | stock chdb-go | JSONEachRow appended to the `INSERT` statement and sent through `Session.Query` |

`go.stock.mod` builds against stock chdb-go v2.2.0 without the fork:

```
go test -tags stockchdb -modfile=go.stock.mod ./...
```

The same tests pass on both builds.

### The fork

[`patches/0001-chdb-go-binary-safe-streaming-insert.patch`](patches/0001-chdb-go-binary-safe-streaming-insert.patch)
(about 310 lines, plus 130 of tests) binds `chdb_query_n` and the
`chdb_stream_insert_n` family. It exposes them as a second interface,
`ChdbInsertConn`, the same way chdb-go already exposes its admin API, so
existing implementations of `ChdbConn` keep compiling. It also adds three
`Session` methods:

```go
sess.Insert("INSERT INTO db.t (a, b)", "RowBinary", data)  // one chunk
st, _ := sess.InsertStream("INSERT INTO db.t (a, b)", "RowBinary")  // many chunks
st.Append(chunk); st.Done()                                // or st.Cancel()
sess.QueryBytes([]byte("SELECT length('a\x00b')"), "CSV")   // SQL containing NUL bytes
```

The symbols are probed, not assumed, so an older libchdb returns
`ErrInsertABIUnavailable` instead of panicking. New tests cover NUL bytes, 50
chunks through one reused buffer, truncated data, cancel, a bad target table,
and NULs in SQL. chdb-go's own test suite still passes. GitHub didn't let this
session fork `chdb-io/chdb-go`, so the fork lives here as a vendored copy used
through a `replace` directive.

## What the exporter does

- **Schema.** The DDL is copied from the clickhouse exporter v0.161.0 (Apache-2.0),
  including the trace-id → time lookup table and its materialized view.
  Cluster and engine templating are removed. Options: `database`, table names,
  `ttl`, `create_schema`.
- **Encoding.** One pass over pdata in column order, written to a pooled buffer.
  A span costs about 0.5 µs to encode as RowBinary and 1.7 µs as JSON, with no
  allocations per span. Attribute values are rendered exactly as
  `pcommon.Value.AsString` renders them, which is what the clickhouse exporter
  stores; a test checks that the fast paths agree with it.
- **Staging tables (`staging_tables: true`, the default).** Parsing RowBinary
  straight into `LowCardinality(String)` and `Map(LowCardinality(String), String)`
  columns costs about twice as much as parsing into `String` and converting
  afterwards. For a 10k-span batch into a Null table, parsing takes 60 ms with
  the table's own types and 23 ms with plain ones. The usual fix,
  `INSERT ... SELECT FROM input()`, is refused by chDB's streaming insert.
  Instead, inserts go to a Null-engine `<table>_in` with plain types, and a
  materialized view converts each block into the real table. With 10k-span
  batches that takes throughput from 80k to 117k spans/s.
- **`connections`.** A pool of native connections. A connection runs one
  statement at a time, and the sending queue calls the exporter from several
  goroutines. 4 connections give about 2.3× the throughput of 1 at 1000-span
  batches.
- **`buffer_seconds`.** Optional Buffer tables in front of MergeTree, flushed on
  shutdown. Rows still in a buffer are lost if the process dies, so pair this
  with the persistent queue only if you can accept that window.
- **Durability.** This comes from the collector: `sending_queue.storage:
  file_storage` persists batches until an insert succeeds. That plays the role
  the WAL plays in oscope-core.
- **Tests.** Traces and logs are written through all three formats, each with
  staging on and off, and the resulting tables must be byte-identical. Spot
  checks cover nanosecond timestamps, hex ids, empty parent ids, attribute
  rendering, NUL bytes, control characters, Nested columns, structured log
  bodies and the trace-id materialized view. Other tests cover Buffer flush on
  shutdown, concurrent pushes over the pool, the full factory and
  exporterhelper path, and config validation.

## Benchmarks

The machine: 4 vCPU (Xeon, 2.8 GHz), chDB 26.7.3, ClickHouse 26.10.1 (the
`master` build) on the same machine, Go 1.26. Each cell is one 3-second run, so
treat differences under about 20% as noise, especially at 100-span batches.

`bench/` drives both exporters through their collector factories, with
`ConsumeTraces`/`ConsumeLogs` and the queue off, so each call is one
synchronous insert of one batch. **CPU/span** is user plus system CPU per
span. For ClickHouse it is this process plus the server, since the machine
pays for both. The chdb cases run with no server up, then the clickhouse cases
run against it.

### Traces: throughput in spans/s, and CPU per span

| batch | chdb rowbinary (fork) | chdb file (stock) | chdb json (stock) | chdb rowbinary + Buffer | clickhouse sync | clickhouse async_insert |
| --- | --- | --- | --- | --- | --- | --- |
| 100 | 4,004 · 451 µs | 3,400 · 492 µs | 3,580 · 467 µs | 14,102 · 78 µs | 6,331 · 322 µs | 1,348 · 353 µs |
| 1,000 | 32,728 · 79 µs | 26,923 · 88 µs | 23,480 · 88 µs | 69,294 · 22 µs | 30,435 · 77 µs | 11,503 · 76 µs |
| 10,000 | **113,073 · 21.6 µs** | 103,478 · 24.0 µs | 42,828 · 40.1 µs | 122,310 · 11.8 µs | 72,140 · 27.1 µs | 52,455 · 28.7 µs |

### Logs

| batch | chdb rowbinary (fork) | chdb file (stock) | chdb json (stock) | chdb rowbinary + Buffer | clickhouse sync | clickhouse async_insert |
| --- | --- | --- | --- | --- | --- | --- |
| 100 | 7,332 · 325 µs | 5,148 · 480 µs | 6,391 · 392 µs | 12,803 · 151 µs | 4,522 · 386 µs | 1,215 · 439 µs |
| 1,000 | 47,089 · 64 µs | 34,259 · 91 µs | 29,333 · 97 µs | 77,150 · 29 µs | 30,739 · 75 µs | 11,038 · 73 µs |
| 10,000 | **133,070 · 24.7 µs** | 123,657 · 27.5 µs | 60,103 · 54.0 µs | 112,340 · 29.7 µs | 93,221 · 23.5 µs | 58,659 · 23.6 µs |

Go heap per 10k-span batch is about 0.5 MB and 40 allocations for the chdb
exporter, against about 48 MB and 465k allocations for the clickhouse exporter,
most of it clickhouse-go building column buffers.

### What the numbers say

- **At collector-sized batches, in-process chDB beats a local ClickHouse server
  on throughput and roughly matches it on CPU.** With 10k-span batches it does
  1.4–1.6× the spans/s, at about 20% less CPU per span for traces and the same
  for logs. It avoids the network hop, the native-protocol encoding, and
  clickhouse-go's allocations.
- **The fork is worth 2.6× over stock chdb-go's obvious path (JSON), but only
  about 10% over the stock `file()` workaround.** The workaround writes RowBinary
  to a temp file and has chDB read it back, so without the fork you still get
  most of the benefit. The fork's advantages are that it touches no temp files,
  it streams, and it is the path chDB means you to use.
- **Small batches are bound by statement overhead, not by the format.** Every
  chDB statement costs about 1–1.5 ms of CPU, even `SELECT 1`, and no setting
  changes that. A MergeTree insert into this schema, with its indexes and
  materialized view, costs about 40–60 ms of CPU per part plus merges, and the
  server pays nearly the same: ClickHouse's own `ProfileEvents` show 58 ms per
  100-span insert in chDB against 40 ms in the server. Either batch big
  (`sending_queue.batch.min_size` in the thousands; `otelcol/config.yaml` uses
  5,000–20,000) or put a Buffer table in front, which gives 3–4× at 100-span
  batches.
- **`async_insert` isn't a throughput win in this setup.** The clickhouse
  exporter waits for each asynchronous insert to be flushed, and this benchmark
  inserts serially, so every insert waits for the flush. Its purpose is
  combining many concurrent clients, which this benchmark doesn't do.

In-package benchmarks (`go test -bench . ./chdbexporter`, raw output in
`bench/results/insert.txt`) also cover direct vs staged inserts and 4
connections.

## Publishing: object storage and Parquet

The exporter can also run as a short-lived **edge buffer**. Each batch is
published somewhere a central consumer can read it after this process is
gone, and a manifest announces it once it's durable. The embedded database
keeps hours of state, never more than 72; everything older lives only in
object storage until the consumer has taken it. This covers the chDB side
only. The central consumer and catalog aren't built yet; `cmd/chdbattach`
shows the core of what they would do.

```yaml
chdb:
  producer: {id: edge-1, region: eu-west-1, schema_version: 1}   # epoch: new per process
  object_storage:                    # MergeTree tables on s3_plain_rewritable disks
    endpoint: https://bucket.s3.eu-west-1.amazonaws.com/otel
    access_key_id: ...
    secret_access_key: ...
    generation: 1h                   # rotate tables
    local_retention: 6h              # then DETACH locally; objects stay
    seal_optimize: true              # OPTIMIZE FINAL when sealing
    compact_parts: true              # never wide parts: ~20 objects per part, not ~150
    old_parts_lifetime: 10m          # keep merged-away parts for running readers
  parquet:                           # and/or one Parquet object per batch
    url: https://bucket.s3.../otel-parquet     # or file:///dir
    compression: zstd
  store_tables: true                 # false + parquet = pure Parquet publisher
```

[`otelcol/config.edge.yaml`](otelcol/config.edge.yaml) is a complete config.

### Layout and commit protocol

```
{root}/{region}/{signal}/v{schema}/{producer}/{epoch}/
    {generation}/{table}/...               s3_plain_rewritable data, one table per prefix
    {generation}/{batch_id}.parquet        (under parquet.url)
    manifests/{generation}/{batch_id}.json written last: the batch's commit record
    manifests/{generation}/_sealed.json    written when the generation takes no more batches
```

- **The envelope.** Every row carries `producer_id`, `producer_epoch`,
  `batch_id`, `row_ordinal`, `received_at` and `schema_version`, in the table,
  the Parquet and the manifest. A consumer checkpoints on
  (producer, epoch, batch), never on event time or part names, which merges
  change.
- **Commit.** A batch goes into the table, then Parquet, then its manifest. A
  batch without a manifest isn't committed: that covers an insert that
  succeeded before a crash, and the first attempt of a retry, which comes back
  as a new batch id. A consumer that selects `WHERE batch_id IN (manifested
  ids)` never sees those orphans. **But it can still double-count**, as the
  Quint model in [`model/`](model/README.md) showed. A manifest PUT that lands
  while the exporter sees an error (a timeout, or a crash before the queue
  acks) commits the batch, and the retry commits it again under a new id. The
  seal, built from in-memory counters, also misses such a batch. The
  model-checked fixes are:
  - content-derived batch ids with an edge dedup token;
  - seals from an S3 listing;
  - a content key, a content lease and a check before insert at the consumer.

  [`model/README.md`](model/README.md) also maps each finding to the
  ClickHouse settings that help. None of these fixes is implemented in
  `publish.go` yet.
- **One writer per table, with no fencing.** The epoch is new for every process
  incarnation, so a restarted pod writes to new tables and can never overlap
  its predecessor. One exporter instance per signal per process is enforced at
  start.
- **Generations.** A push in a new period creates the next set of tables
  (staging, view and trace-id table included). The old set then drains:
  rotation takes the lock that in-flight pushes hold shared. After that it's
  sealed:
  1. flush the Buffer, if any;
  2. drop the process-local views and staging table;
  3. `OPTIMIZE ... FINAL`;
  4. write `_sealed.json` with the batch count, row count and batch id range.

  After `local_retention` it's DETACHed, never dropped, since DROP would delete
  objects the consumer may not have read. Shutdown seals the open generation.
  A crash leaves one unsealed; its batch manifests are still valid, and the
  catalog should treat an epoch that has gone quiet as abandoned.
- **Reading.** `ReaderDDL(cfg, db, signal, generation, manifest.tables, ...)`
  returns the writer's table definitions on the same endpoints with
  `disk(readonly = 1, ...)` and `refresh_parts_interval`. `cmd/chdbattach`
  reads a manifest from S3, attaches the generation read-only, and runs
  queries against it.

Here is a real batch manifest from the tests, with `tables` shortened (the
trace-id table is listed too):

```json
{"producer_id":"os-follow","producer_epoch":"20260924T191120Z-47366a","region":"test","signal":"traces",
 "schema_version":1,"generation":"g20260924T190000","batch_id":1,"rows":300,"rowbinary_bytes":166768,
 "received_at":"2026-09-24T19:11:20.215047562Z",
 "min_event_time":"2026-09-24T12:00:00.123456789Z","max_event_time":"2026-09-24T12:00:00.422456789Z",
 "tables":[{"table":"os_follow.otel_traces_g20260924T190000",
            "endpoint":"http://127.0.0.1:18333/otel/test/traces/v1/os-follow/20260924T191120Z-47366a/g20260924T190000/otel_traces/"}, ...],
 "parquet":"http://127.0.0.1:18333/otel/parquet/test/traces/v1/os-follow/20260924T191120Z-47366a/g20260924T190000/00000000000000000001.parquet"}
```

### What was verified

Tested against [SeaweedFS](https://github.com/seaweedfs/seaweedfs) 4.47 (built
from source) as the S3 server, with chDB 26.7.3 on both the writing and the
reading side:

- **chDB can be the single writer of an `s3_plain_rewritable` table**, with an
  inline `disk(...)` and `table_disk = 1`, which needs no server config.
- **A reader in a separate process sees each new batch about 1 s after the
  writer's insert returns** (`TestObjectStorageReaderFollowsWriter`: 1.01 s and
  0.97 s in two runs), attached read-only with `refresh_parts_interval = 1`
  while the writer keeps running.
- **Sealed and detached generations stay fully readable.** After rotation,
  seal, `OPTIMIZE FINAL` and detach, another process attaches the generation
  from its `_sealed.json` and reads every row
  (`TestGenerationsRotateSealAndDetach`). Merged parts cover the parts they
  replaced, so the retained old parts don't double-count.
- **Parquet streams straight from the batch.** The fork's streaming insert
  accepts `INSERT INTO FUNCTION s3(..., 'Parquet', structure)` with RowBinary
  input, so the batch is encoded once and chDB writes the Parquet. The Parquet
  columns match, row for row, what the table exporter stores for the same
  input (`TestParquetToLocalDirectory`).
- **Concurrent pushes across rotations lose and duplicate nothing.** Four
  goroutines push 40 batches while 1 s generations rotate under them. Batch
  ids run 1–40, every generation's seal agrees with its batch manifests, and
  the Parquet holds all 2,000 rows (`TestConcurrentPushesAcrossRotations`,
  also under `-race`).
- **The collector does it end to end.** `otelcol/run-edge-demo.sh` sent
  65,510 spans and 34,566 logs with telemetrygen. The seal manifests, batch
  manifests, Parquet, and the tables a separate process attached from the seal
  manifest all hold exactly those counts, with batch ids 1–14 and 1–7 and no
  gaps.

### Reading with ClickHouse server

The central fleet will be ClickHouse, so the same tables were attached with
the same `ReaderDDL` on a ClickHouse **26.10.1** server (the `master` build).
The writer was chDB **26.7.3**. `chdbattach -print-ddl` prints the statements
for a manifest.

- **Sealed data.** The edge demo's sealed traces generation attached and
  returned 65,510 spans, 14 batches and 13,102 traces.
  `sum(cityHash64(*))` over every column of both tables is identical on the
  server and in chDB, so the parts written by 26.7.3 read back byte for byte on
  26.10.1. The server also reads the Parquet directly with `s3()`, and it
  agrees with the tables.
- **Live data.** The server follows the writer. It saw a new batch 0.99–1.0 s
  after the writer's insert returned (`refresh_parts_interval = 1`, 4 runs;
  `TestClickHouseServerReaderFollowsWriter`).
- **Merges.** The server stays consistent while the writer merges. Through 20
  small pushes and 4 merges, about 90 polls per run each saw whole batches
  only: never a partial batch, and never a merged part counted beside the
  parts it replaced.
- **Dropping is safe.** `DROP TABLE` on the server's read-only table leaves
  the writer's objects alone: the writer still has every row, and a
  re-attach sees them all.
- **Deletion, demonstrated.** With chDB's default `old_parts_lifetime = 0`, a
  slow server query fails when the writer merges the parts it's reading:
  `File 20260924_6_6_0/data.bin does not exist`, 3 runs out of 3 once the
  writer's cleanup runs. With the exporter's default of 10 minutes, the same
  query returns all 12,000 rows (`TestOldPartsLifetimeProtectsServerQueries`).
  So the setting is required, not optional: it bounds the longest query that
  is safe on an active generation. Sealed generations aren't merged after
  they're sealed.
- **Clean log.** The server logged no warnings or errors about the disks or
  the parts.

### What it costs

10k-span batches, SeaweedFS on the same 4-vCPU machine (so there is no
network latency; real S3 will be slower on the table path). S3 writes are
chDB's own `S3WriteRequestsCount`, including the merges the inserts trigger.
Raw output is in `bench/results/publish.txt`.

| Variant | spans/s | ms/batch | S3 writes/batch |
| --- | --- | --- | --- |
| local tables (no publishing) | 110k | 91 | 0 |
| Parquet only, local directory | 174k | 57 | 0 |
| Parquet only, S3 | 123k | 82 | 4.7 |
| local tables + local Parquet | 69k | 145 | 0 |
| `s3_plain_rewritable` tables, compact parts | 68k | 148 | 51 |
| `s3_plain_rewritable` tables, wide parts | 55k | 183 | 75 |
| S3 tables + S3 Parquet | 45k | 222 | 52 |

- **Parquet is the cheap boundary:** under 5 S3 writes per batch, and faster
  than local MergeTree, because it skips sorting, indexes and merges.
- **Native parts cost about 24 objects per inserted part** (data, marks,
  checksums, primary index, one file per skip index — this schema has six
  bloom filters — and a `plain_rewritable` prefix record). The traces signal
  writes two parts per batch. On real S3, those PUTs and their latency are
  what to budget for. Fewer, bigger batches, a Buffer table in front, or fewer
  skip indexes on the edge tables all cut them.
- **`compact_parts` cuts S3 writes by a third.** Past 10 MB, MergeTree makes
  merged parts wide, with a file per column, about 150 objects for this
  schema.

### Open questions for the next step

- **Real S3 latency.** Part writes on `plain_rewritable` weren't measured
  against AWS; SeaweedFS on localhost hides per-request latency.
- **Deletion by the consumer.** `old_parts_lifetime` protects reader queries
  against the writer's merges, but nothing protects them against the
  consumer's own garbage collection. That belongs in the catalog: delete a
  generation only after every reader has released it.
- **Other version pairs.** Only one pair was tested: chDB 26.7.3 writing and
  ClickHouse 26.10.1 reading. Pin the pair used in production, and test an
  upgrade of either side.
- **Stock chdb-go can't publish.** Publishing streams RowBinary and needs the
  fork. The publishing tests skip on the stock build.

## Running it

```sh
export CHDB_LIB_PATH=/path/to/libchdb.so   # chDB 26.7.x

# exporter tests, on the fork and on stock chdb-go
cd chdbexporter && go test ./... && go test -tags stockchdb -modfile=go.stock.mod ./...

# the comparison (starts and stops its own ClickHouse server)
CLICKHOUSE_BIN=/path/to/clickhouse OUT=results/compare.txt bench/run-compare.sh 3s

# a real collector: build with ocb, send OTLP, stop, read back
TELEMETRYGEN=$(go env GOPATH)/bin/telemetrygen otelcol/run-demo.sh 10

# publishing: an S3 server with a bucket (SeaweedFS here: weed server -s3 -s3.config s3.json,
# then `s3.bucket.create -name otel` in weed shell)
export CHDB_TEST_S3=http://127.0.0.1:8333/otel CHDB_TEST_S3_KEY=... CHDB_TEST_S3_SECRET=...
export CHDB_TEST_CLICKHOUSE=http://127.0.0.1:8123   # optional: ClickHouse server reader tests
cd chdbexporter && go test -race ./... && go test -run '^$' -bench Publish -benchtime 40x .
S3_ENDPOINT=$CHDB_TEST_S3 S3_ACCESS_KEY_ID=$CHDB_TEST_S3_KEY S3_SECRET_ACCESS_KEY=$CHDB_TEST_S3_SECRET \
  TELEMETRYGEN=... otelcol/run-edge-demo.sh 10
```

`run-demo.sh` sends one hand-written OTLP/HTTP JSON trace and log, then 10
seconds of telemetrygen traces and logs over gRPC. It stops the collector
(SIGTERM drains the queue) and reads the data back with `chdbq`. The last run
stored exactly what telemetrygen reported sending: 67,445 spans and 35,150 logs,
plus the hand-written pair, which joins trace to log on trace and span id.

## Deploying it

- **Getting into contrib is unlikely.** A 566 MB native library doesn't belong
  in `otelcol-contrib`, and a component needs a sponsor and code owners. The
  realistic route is a standalone module that people add to their own `ocb`
  build, which is exactly what `otelcol/builder-config.yaml` does.
- **One data path per process.** Every chdb exporter in a collector must name
  the same `path`. chdb-go shares it between sessions and refuses a second path.
- **No other process can open the local data while the collector runs.** Only
  the process holding the path can open it, so neither `chdbq` nor oscope's
  server can read a live collector's local tables. Queries have to go through
  the collector: an extension sharing the session could serve oscope's `/api/*`
  and web UI, or a subset of ClickHouse's HTTP interface for HyperDX and
  Grafana. Published tables are the exception: any number of processes can
  attach those read-only.
- **Shutdown.** With chDB 26.7.3, a process with MergeTree tables can't shut the
  engine down cleanly, so the exporter closes its sessions and leaves the
  engine running until the process exits.
- **Credentials.** Leave `access_key_id` / `secret_access_key` empty and chDB
  uses its own AWS provider chain, with no code in the exporter. That chain
  covers env keys, EKS IRSA (web identity), EKS Pod Identity (container
  credentials plus the token file), IMDS, and static keys in a
  shared-credentials profile. It was measured with libchdb against local
  stand-ins, for `s3()` writes and reads and for `plain_rewritable` disks.
  - **Not supported: `credential_process`**, and so IAM Roles Anywhere's usual
    setup: ClickHouse's chain has no process provider. Run
    `aws_signing_helper serve` and set `AWS_EC2_METADATA_SERVICE_ENDPOINT`
    instead.
  - **Private CA** (e.g. Nutanix Objects): set `SSL_CERT_FILE` in the pod to a
    bundle that includes it. `AWS_CA_BUNDLE` is ignored.
  - **Central ClickHouse 26.10:** needs
    `s3_allow_server_credentials_in_user_queries = 1` for the ingest user
    before keyless `s3()` may use the server's credentials.

  Details, the evidence, and config examples for each deployment are in
  [`parquetgo/README.md`](parquetgo/README.md#credentials-and-deployment-targets).
- **chdb-go's CLI** (`go run ./chdb-go -path p "SQL"`) ignores `-path` for single
  queries and runs them in a throwaway session. Use `chdbq` instead.
