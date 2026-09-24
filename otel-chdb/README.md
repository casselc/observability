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
| [`otelcol/`](otelcol/) | An `ocb` distribution (OTLP receiver, memory limiter, chdb exporter, file_storage), a config with a persistent queue, and an end-to-end demo. |

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

## Running it

```sh
export CHDB_LIB_PATH=/path/to/libchdb.so   # chDB 26.7.x

# exporter tests, on the fork and on stock chdb-go
cd chdbexporter && go test ./... && go test -tags stockchdb -modfile=go.stock.mod ./...

# the comparison (starts and stops its own ClickHouse server)
CLICKHOUSE_BIN=/path/to/clickhouse OUT=results/compare.txt bench/run-compare.sh 3s

# a real collector: build with ocb, send OTLP, stop, read back
TELEMETRYGEN=$(go env GOPATH)/bin/telemetrygen otelcol/run-demo.sh 10
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
- **Nothing else can read the data while the collector runs.** Only the process
  holding the path can open it, so neither `chdbq` nor oscope's server can read
  a live collector's data. Queries have to go through the collector: the next
  piece would be an extension that shares the session and serves oscope's
  `/api/*` and web UI (or a subset of ClickHouse's HTTP interface for
  HyperDX/Grafana).
- **Shutdown.** With chDB 26.7.3, a process with MergeTree tables can't shut the
  engine down cleanly, so the exporter closes its sessions and leaves the
  engine running until the process exits.
- **chdb-go's CLI** (`go run ./chdb-go -path p "SQL"`) ignores `-path` for single
  queries and runs them in a throwaway session. Use `chdbq` instead.
