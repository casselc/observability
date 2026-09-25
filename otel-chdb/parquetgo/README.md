# parquetgo: Parquet publishing without chDB

**Verdict:** the edge doesn't need chDB just to publish Parquet. A Go-native
writer ([parquet-go/parquet-go](https://github.com/parquet-go/parquet-go))
plus an S3 client produces the same rows as the chDB path, byte for byte as
the ClickHouse server reads them. It uses 25–35% less CPU per batch, about a
third of the memory, and a 16 MB static binary instead of a 566 MB native
library. Keep chDB only where the edge needs SQL: local queries, SQL
transforms or pre-aggregation, or the native-table option.

Labels: **[M]** measured here · **[R]** from `../README.md` or
`../bench/central/REPORT.md` · **[E]** estimate.

The machine had 4 vCPUs, shared with other agents' work (load average 3.5–5.5
during the runs). The software was chDB 26.7.3 (its footer says 26.7.2),
ClickHouse server 26.10.1, SeaweedFS on localhost, Go 1.26, arrow-go v18.7.0,
parquet-go v0.32.0 and aws-sdk-go-v2 (service/s3 v1.113.4). The first
measurements used minio-go v7.0.91; the publisher was switched to the AWS SDK
afterwards, to share one S3 client with the conditional-write protocol in
`../s3cas` (see "S3 client" below).

## What's here

| Path | What it is |
| --- | --- |
| `walk.go` | The pdata walkers, envelope and `valueString`, copied from `chdbexporter/encode.go` so both producers visit pdata in the same order and render values identically. |
| `walk_metrics.go`, `pgo_metrics.go` | **Metrics** (parquet-go engine only): one walk writes up to five files, one per metric type, in the contrib `clickhouseexporter` v0.161.0's `otel_metrics_*` schema. `METRICS_SCHEMA.md` is the column-by-column contract. See "Metrics". |
| `pgo.go` | **parquet-go engine (recommended).** The walker writes level-annotated `parquet.Value`s straight into one buffer per leaf column, with no intermediate arrays. Each column goes to parquet-go's column writer. Setting `Parallelism > 1` encodes and compresses columns on several goroutines. |
| `encode.go`, `schema.go`, `alloc.go` | **arrow-go engine.** The same walker fills Arrow builders, then `pqarrow` writes them. Build with `-tags noarrow` to leave it out, which saves about 27 MB of binary. |
| `options.go` | Parquet tuning. The defaults mirror ClickHouse's `output_format_parquet_*` settings. |
| `publish.go` | Publisher: the same object layout, batch ids, generations and manifest JSON as `chdbexporter/publish.go` (Parquet-only, `store_tables: false`). Uploads with aws-sdk-go-v2, or writes local files and renames them into place. |
| `s3client.go` | The S3 client: `s3://bucket/prefix` (AWS, virtual-hosted) or `http(s)://host/bucket/prefix` (custom endpoint, path-style); static keys, or the AWS default credential chain (IRSA, EKS Pod Identity, `credential_process`, IMDS, ...), optional assume-role, and a private CA bundle. See "Credentials and deployment targets". |
| `compare/` | Its own module, so `parquetgo` never links chdb-go. For metrics it also links the contrib `clickhouseexporter` as the reference (`refexporter.go`, `metrics_test.go`), and holds the metrics generators (`metricsgen.go`) and `cmd/metricscentral`. It drives the chdb exporter through its factory, and holds the correctness tests, `cmd/pubbench`, `cmd/pqpages` (a page-alignment inspector), `cmd/credstubs` (stand-ins for STS, the Pod Identity agent and IMDS, for the ClickHouse/chDB credential checks), size probes and raw results. |

`chdbexporter` wasn't modified.

## What chDB does on this path, and what replaces it

I inspected ClickHouse's output with pyarrow and with ClickHouse's
`output_format_parquet_*` settings [M].

| Concern | chDB (ClickHouse writer) | Go-native |
| --- | --- | --- |
| Types | `DateTime64(9)` becomes `TIMESTAMP(NANOS, UTC)`; `UInt8/16/32/64` become unsigned `INT`; `Map(String,String)` becomes `MAP` with required `key_value.key/value`; each Nested sub-column becomes its own required 3-level `LIST`; every column is required. | The same. pyarrow reports equal schemas for all three writers. |
| Column order | Structure order | Structure order. parquet-go sorts group fields by name, so `pgo.go` wraps the root in an ordered group. |
| Compression | zstd (`output_format_parquet_compression_method`) | zstd (klauspost), configurable. |
| Dictionary | Tried on every column; falls back to PLAIN past 1 MiB | RLE_DICTIONARY on every column except the near-unique ones (`HighCardinality`: timestamps, ids, `row_ordinal`), which are PLAIN or DELTA_LENGTH_BYTE_ARRAY. |
| Statistics, page index | Chunk and page stats, column and offset index | The same. |
| Bloom filters | Every column, about 10.5 bits per distinct value | Every column, 11 bits per distinct value (parquet-go). The arrow engine uses adaptive sizing at FPP 0.005. |
| Row groups, pages | 1M rows per row group, so one per batch; 1 MiB pages; V1 data pages | One row group per batch; 1 MiB pages; V2 data pages. |
| Parallelism | `output_format_parquet_parallel_encoding=1`: about 1.3–1.4 cores busy per batch [M] | Serial by default; `Parallelism: 4` is optional. |
| S3 upload | One PUT for the object and one for the manifest [M, counting proxy]. `S3WriteRequestsCount` said 4.7/batch [R]; the proxy saw 2 HTTP PUTs. | One PUT each (aws-sdk-go-v2 `PutObject`, standard retryer: 3 attempts with backoff; no multipart). |
| Manifest | `rowbinary_bytes` = RowBinary size | Same JSON keys and values, except that `rowbinary_bytes` holds the Parquet size, since no RowBinary exists. |

## Correctness [M]

`compare/correctness_test.go` publishes the same batches through chDB, the
arrow engine, the parquet-go engine, and parquet-go with 4-way parallelism.
Each goes to its own S3 prefix. There are two datasets:

- 3,000 `testgen` spans plus 3,000 `testgen` logs.
- 700 spans and 700 logs of hostile data (`nasty.go`):
  - NULs in keys and values;
  - control characters, emoji, RTL marks and combining characters, a BOM, and **invalid UTF-8**;
  - 100 KB strings;
  - doubles: NaN, ±Inf, −0, 5e−324, 1e21 and MaxFloat64;
  - ints at their minimum and maximum;
  - bytes, slice, map and empty values;
  - a non-string `service.name`;
  - zero trace and span ids;
  - timestamps of 0, 1 ns and MaxInt64, and negative durations;
  - 0/1/2/50 events and 0/1/3 links, with empty and non-empty attributes;
  - every log body type.

The Go publishers stamp the same `received_at` as the chDB run, so every
column can be compared. The ClickHouse **server** reads every object with
`s3()`, and the test runs `-race` clean. The results:

- `count(), sum(cityHash64(all 28 / 22 columns))` is **identical** for all
  four writers, both with the exporter's explicit structure and with schema
  inference.
- Row-level `EXCEPT` in both directions, per batch: **0 rows** differ.
- `DESCRIBE s3(...)` (the inferred schema) is identical.
- **Central ingest:** `INSERT INTO` a table with the `otel_traces` /
  `otel_logs` types (`LowCardinality`, `Map(LowCardinality(String), String)`,
  `Nested`) gives identical checksums, both from the structured `s3()` and
  from the inferred one.
- Batch manifests are identical apart from the object URL prefix and the
  `rowbinary_bytes` value. Seal manifests are identical apart from `sealed_at`.
- **pyarrow 25.0.1:** the schemas are equal and the tables are equal once strings
  are cast to binary, because the invalid UTF-8 survives, identically, in
  every writer.
- **Spark 4.0.1, local:** all writers fail the same way by default,
  `Illegal Parquet type: INT64 (TIMESTAMP(NANOS,true))`. That includes chDB,
  so it comes from the schema, not the writer. With
  `spark.sql.legacy.parquet.nanosAsLong=true`:
  - all writers read, with equal schemas and 0 differing rows;
  - `Timestamp` reads as `bigint`, `UInt64` columns as `decimal(20,0)`, and
    maps and arrays natively.

  Databricks presumably behaves the same, since it is Spark [E]. For a
  lakehouse, add a `TIMESTAMP(MICROS)` column or change the unit. That is a
  one-line schema change in the Go writer.

### Found on the way: arrow-go can write Parquet that ClickHouse rejects [M]

arrow-go v18.7.0 with **V1** data pages can end a page in the middle of a row
of a repeated column. `cmd/pqpages` shows `SpanAttributes.value` pages
starting at repetition level 1. It happens once values are large, here with
100 KB attribute values, and big SQL statements or stack traces will do the
same in production.

The file also carries a page index, which requires pages to start at row
boundaries. ClickHouse 26.10 therefore fails with `Invalid array of tuples ...
different array lengths`. `compare/pagesplit_test.go` pins down what does and
doesn't work:

| Variant | Server reads it |
| --- | --- |
| V1 pages | no |
| V1 pages, no dictionary | no |
| V1 pages, no page index | yes |
| V2 pages | yes |
| parquet-go | yes |

The arrow engine now defaults to V2 pages. This is exactly the kind of risk a
Go-native path takes on that chDB doesn't: the reader and the writer come from
different projects.

### Found on the way: parquet-go's `Writer.Reset` erased the column paths [M]

The encoder reuses one `parquet.Writer` per signal and calls `Reset` before
each file. In parquet-go v0.32.0, `Reset` clears the previous footer's
metadata in place. `format.ColumnMetaData.Reset` runs
`clear(PathInSchema)`, and that slice aliases the writer's own
`ColumnWriter.columnPath`. So every file after an encoder's first had an
empty string for every element of every column chunk's `path_in_schema`.

- The files were still readable. The ClickHouse server read every such
  object in the traces/logs correctness test, which is why it never showed,
  and pyarrow opens them. Readers that match column chunks by
  `path_in_schema`, perhaps parquet-mr, may not cope [E].
- A retry was also not byte-identical: the pool could hand the first
  attempt a fresh encoder and the retry a used one. The metrics
  determinism test (`TestMetricsDeterministic`) found it.
- **Fix** (`pgo.go`, `restoreColumnPaths`): after `Reset`, the column path
  strings are rewritten from the schema through reflection on the
  unexported field. If the field ever isn't there, the encoder falls back to
  a new writer per file. `TestReusedWriterIdentical` fails if the paths are
  not restored.
- **Cost:** none. Every file is about 0.6 KB bigger, because it now carries
  its paths (traces: 270,575 → 271,168 bytes). Creating a new writer per
  file instead costs 15–30% more CPU and doubles RSS [M]: traces
  81 ms / 229 MB against 72 ms / 113 MB (`compare/results/pubbench-after-resetfix.md`).
- Worth reporting upstream; the one-line fix is to copy `columnPath` when
  building the `ColumnChunk`.

## Benchmarks

### End to end: one process per implementation, per batch [M]

`cmd/pubbench` runs each implementation in its own process. Each run is 3
warm-up batches plus 30 timed ones, one batch at a time, repeated in **3
separate processes**. The numbers are medians across the processes with
[min–max].

- `ms/batch` is the median batch latency.
- `CPU` is `getrusage` user+sys for the whole process, so it includes chDB's
  threads.
- `Go allocs` is Go heap only; chDB's own allocations are invisible to it.
- `RSS` is peak `ru_maxrss`.
- A batch is 10,000 spans or records from `testgen`.

Raw data is in `compare/results/pubbench.jsonl` and `pubbench.md`.

| dest | signal | impl | ms/batch | k rows/s | CPU ms/batch | Go allocs/batch | peak RSS MB | object KB |
|---|---|---|---|---|---|---|---|---|
| local | traces | chdb | 62.7 [53.5–75.9] | 159 [125–184] | 91 [87–92] | 102 | 395 [380–401] | 308 |
| local | traces | parquet-go | 68.6 [66.2–69.3] | 141 [139–150] | **68** [67–71] | 602 | **99** [97–101] | 265 |
| local | traces | parquet-go ×4 | **53.9** [48.5–57.9] | 182 [167–200] | 73 [71–75] | 607 | 107 [104–124] | 265 |
| local | traces | arrow | 138.8 [125.9–151.3] | 70 [66–78] | 145 [142–155] | 627,935 | 201 [191–217] | 314 |
| local | logs | chdb | 43.2 [42.3–55.9] | 221 [176–235] | 65 [64–68] | 27,606 | 340 [326–363] | 181 |
| local | logs | parquet-go | 46.8 [46.8–46.9] | 207 [199–210] | **47** [47–48] | 27,954 | **125** [120–136] | 155 |
| local | logs | parquet-go ×4 | **38.4** [35.5–45.2] | 252 [215–272] | 52 [52–53] | 27,966 | 127 [124–130] | 155 |
| local | logs | arrow | 100.9 [95.3–101.5] | 98 [94–104] | 107 [103–112] | 484,698 | 172 [163–192] | 201 |
| s3 | traces | chdb | 82.7 [75.5–103.8] | 117 [91–125] | 105 [104–107] | 147 | 390 [390–391] | – |
| s3 | traces | parquet-go | 84.0 [83.3–84.1] | 118 [116–118] | **74** [70–75] | 1,326 | **108** [108–114] | – |
| s3 | traces | parquet-go ×4 | **64.6** [59.3–77.7] | 152 [131–166] | 78 [75–80] | 1,323 | 116 [113–119] | – |
| s3 | traces | arrow | 149.5 [141.1–155.8] | 67 [63–70] | 147 [146–154] | 628,661 | 213 [205–223] | – |
| s3 | logs | chdb | 66.6 [60.8–79.4] | 148 [121–162] | 79 [77–79] | 27,653 | 353 [341–359] | – |
| s3 | logs | parquet-go | 58.5 [57.2–64.6] | 167 [153–171] | **52** [50–54] | 28,586 | **142** [129–154] | – |
| s3 | logs | parquet-go ×4 | **46.6** [43.9–51.4] | 209 [198–224] | 55 [53–56] | 28,584 | 137 [126–141] | – |
| s3 | logs | arrow | 114.1 [108.5–115.3] | 88 [86–89] | 112 [108–118] | 485,414 | 188 [164–205] | – |

The chDB `local traces` row reproduces the README's 57 ms/batch [R] within
the noise of a loaded box. The ~27k allocs/batch on logs, in every
implementation, are `Body.AsString()` for map bodies in the walker, which both
paths share.

**S3 requests** [M], counted by a reverse proxy in front of SeaweedFS over 10
batches: every implementation made **2 PUTs per batch** (object + manifest),
with no HEAD, LIST or multipart.

**The same objects read by the central server** [M]: `sum(cityHash64(*))` over
10 traces batches (100k rows), with the query cache off, 3 runs each.

| writer | ms | MB read |
| --- | --- | --- |
| chDB | 151 [139–213] | 2.01 |
| arrow | 125 [96–138] | 1.57 |
| parquet-go | 95 [88–123] | 1.71 |

Go-written files are at least as cheap to ingest.

### Encode only, in memory [M]

`go test -bench GoEncode ./compare`, 3×20 iterations; raw output in
`compare/results/encode.txt`. Values are ms/op, with object sizes.

| | traces, blooms | traces, no blooms | logs, blooms | logs, no blooms |
|---|---|---|---|---|
| parquet-go | 70 [68–79] · 271 KB | 71 [70–75] · 177 KB | 50 [46–51] · 158 KB | 50 [46–52] · 99 KB |
| arrow | 156 [151–159] · 322 KB | 122 [120–130] · 163 KB | 112 [112–122] · 205 KB | 89 [89–92] · 82 KB |

On the shared box, in-process `par4` was no faster in this run (74–78 ms);
in `pubbench` it was 20–25% faster.

- **Bloom filters are 35–60% of every file, chDB's included.**
  - parquet-go without blooms gives 177 KB against 271 KB for traces, and
    99 KB against 158 KB for logs. chDB writes 308 KB and 181 KB.
  - Central ingest scans whole batches, so blooms only help lakehouse point
    lookups.
  - Turning them off, or keeping them only for `TraceId`, cuts object size
    and PUT bytes by a third to a half. With chDB this is one setting
    (`output_format_parquet_write_bloom_filter`); in Go it's one option.
- **arrow-go is the wrong engine for this.**
  - Its dictionary encoder boxes every byte-array value into an `interface{}`
    (`BinaryMemoTable.GetOrInsert`): about 600k allocations per 10k spans.
  - Its builders and zstd encoders churn 50–70 MB of heap per batch; a
    pooling allocator (`alloc.go`) only trims this.
  - It's 2× the CPU of parquet-go, it has the page-split bug above, and it
    adds 27 MB of binary.
  - Arrow would be the right tool only if the pipeline were already Arrow,
    as in OTAP.

### S3 client

The publisher uses aws-sdk-go-v2: path-style for custom endpoints (so
SeaweedFS, Garage, MinIO and Nutanix Objects need no virtual-host DNS),
virtual-hosted for `s3://` URLs; static credentials when a key is given, and
the AWS default credential chain otherwise (see "Credentials and deployment
targets"). It was first written with minio-go; it moved to the AWS SDK
because:

- the conditional-write protocol (`../s3cas`) is built on it: `IfNoneMatch` /
  `IfMatch` are `PutObjectInput` fields and a 412 is a typed error, where
  minio-go only sets them as custom headers documented as a MinIO extension;
- contrib collectors (`awss3exporter` and others) already link it, so one S3
  stack instead of two;
- the full AWS credential chain (now used) and checksum behaviour.

After the switch the correctness test (below) passes again, `-race` clean,
and per-batch cost is unchanged within noise: 71.6–73.7 ms CPU against
72.4–74.3 ms with minio-go, the same 2 PUTs per batch, about 240 more Go
allocations and 4 MB more resident at start (`compare/results/s3client.md`).
Per PUT the SDK makes about 500 allocations regardless of settings, and over
https it already sends `UNSIGNED-PAYLOAD`, so no body SHA-256 is computed;
`compare/s3put_bench_test.go` measures both clients and a bare SigV4 floor.

### Size, startup, portability [M]

Static builds with `CGO_ENABLED=0` and `-trimpath -ldflags="-s -w"`:

| binary | amd64 | arm64 |
| --- | --- | --- |
| chdb exporter via factory (`cmd/chdbpq`) | 9.2 MB **+ 566 MB `libchdb.so`** | 8.7 MB + a `libchdb.so` for aarch64 [E] |
| Go publisher, parquet-go engine only (`-tags noarrow`) | **15.7 MB**, nothing else (13.9 MB before the credential chain) | 14.5 MB (12.9) |
| Go publisher, both engines | 48.8 MB (46.3) | 42.3 MB (40.2) |
| parquet-go alone / arrow-go pqarrow alone / aws-sdk-go-v2 S3 alone | 10.5 / 32.3 / 8.0 MB | – |

With minio-go instead of the AWS SDK these were 13.9 / 12.8 MB (noarrow),
41.0 / 35.6 MB (both engines) and 6.5 MB (client alone): the switch costs
+28 KB on the recommended build, because the SDK shares most of its weight
(net/http, crypto, XML) with what the binary already links.

**The AWS credential chain costs +1.7 MB** on the recommended build
(13,934,754 → 15,679,650 bytes, `cmd/sizes/full`, amd64, `-tags noarrow`;
+2.4 MB with both engines). `config.LoadDefaultConfig` links `service/sts`,
`sso`, `ssooidc`, `signin`, `feature/ec2/imds` and the credential providers;
by symbol size (unstripped `go tool nm`) that is sts 176 KB, config 138 KB,
ssooidc 122 KB, signin 118 KB, sso 98 KB, imds 27 KB and ~60 KB of providers,
and the rest is their type metadata, pclntab and endpoint-rule tables. It
could be trimmed by wiring only the providers a deployment uses, at the price
of losing SSO/`credential_process`/profile handling; 1.7 MB in a collector
that already links the SDK (contrib does) is not worth that.

- **Ready time**, from `main` to publisher started, with the page cache warm:
  - chDB 157–228 ms median, 145–235 ms range: the dlopen of 566 MB plus
    engine init.
  - Go 32–38 ms; most of that is building the test data.
- **Cold page cache** [E]: libchdb has to be read from disk. At 200 MB/s that
  is about 3 s; it wasn't measured, because dropping caches would disturb
  other work on the box.
- **Portability:**
  - `libchdb.so` needs glibc (`NEEDED libc.so.6, ld-linux-x86-64.so.2, ...`),
    so it won't run on musl/Alpine without a glibc shim.
  - The Go binary is fully static and cross-compiles to arm64 with one env
    var.
  - chDB does publish aarch64 Linux builds, but those weren't tested here [E].

## Credentials and deployment targets

There are three real deployments:

1. **EKS on AWS S3**, with IRSA (web identity) or EKS Pod Identity (container
   credentials).
2. **Nutanix** with Nutanix Objects: static keys, a custom https endpoint,
   path-style addressing, and quite possibly a private CA.
3. **IAM Roles Anywhere** to AWS S3 from outside AWS: an X.509 certificate is
   exchanged for temporary credentials by `aws_signing_helper`.

Three things read or write the objects: this publisher, the chDB exporter
(libchdb in the collector pod), and the central ClickHouse reading with
`s3()`. Labels: **[M]** measured here against local stand-ins, **[D]** from
source or documentation.

| mode | parquetgo | chDB exporter | central ClickHouse `s3()` |
| --- | --- | --- | --- |
| **EKS IRSA** (`AWS_ROLE_ARN` + `AWS_WEB_IDENTITY_TOKEN_FILE`) | **yes**: leave the keys empty; SDK web-identity provider [M `TestCredsIRSA`] | **yes**: leave `access_key_id` empty; chDB's own chain [M] | **yes**: `s3(url, 'Parquet')` with no keys [M, server and local], but see the 26.10 restriction below |
| **EKS Pod Identity** (`AWS_CONTAINER_CREDENTIALS_FULL_URI` + `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`) | **yes**, including refresh [M `TestCredsPodIdentity`] | **yes** [M] | **yes**, token file included [M, server and local] |
| **Nutanix Objects**: static keys, `https://host/bucket/prefix`, path-style, private CA | **yes**: keys + `CABundle` or `AWS_CA_BUNDLE` [M `TestCredsPrivateCA`] | **yes**: keys as today; CA through `SSL_CERT_FILE` [M]. `AWS_CA_BUNDLE` is ignored [M] | **yes**: keys (or a named collection) + `<openSSL><client><caConfig>` [M] or `SSL_CERT_FILE` [M]. `AWS_CA_BUNDLE` is ignored [M] |
| **Roles Anywhere, `credential_process`** in a shared-config profile | **yes**: `AWS_PROFILE` or `Config.Profile` [M `TestCredsRolesAnywhere`] | **no**: the helper is never run [M]; ClickHouse removed the process provider [D] | **no**, the same code [M, D] |
| **Roles Anywhere, `aws_signing_helper serve`** (IMDSv2 emulation on localhost) | **yes**, through `AWS_EC2_METADATA_SERVICE_ENDPOINT` [M `TestCredsRolesAnywhereServe`] | **yes** [M] | **yes** [M, server and local] |
| assume a role on top | `Config.RoleARN` (STS AssumeRole) [M `TestCredsRoleARN`] | not wired | `extra_credentials(role_arn = '...')` [D] |

The chDB column needed no code change. The exporter already builds
`s3(url, '', '', 'Parquet', ...)` and `disk(..., access_key_id = '', ...)`
when no keys are configured, and chDB treats empty keys as "use the
provider chain". That was checked for `s3()` reads and writes and for a
`plain_rewritable` disk, with the Pod Identity stand-in [M].

### What was tested, and how

Everything ran against SeaweedFS on localhost (bucket `otel`, prefixes
under `otel/creds/`). The AWS side was played by stand-ins:

- **Go publisher:** `creds_test.go` (gated on `CHDB_TEST_S3*`, env set only
  with `t.Setenv`). Every test publishes real batches, reads the Parquet
  back and checks the row count.
  - **Pod Identity.** A local HTTP server acts as the Pod Identity agent.
    Each request carried the token file's content as `Authorization` [M].
    - Credentials with a 1 h expiry are fetched once per publisher [M].
    - Credentials expiring in 1 min are refetched for every request, because
      the SDK's container provider refreshes 5 minutes before `Expiration`
      [M].
  - **IRSA.** A stub STS is reached through `AWS_ENDPOINT_URL_STS`. It got
    exactly one `AssumeRoleWithWebIdentity` for two batches, carrying the
    role ARN and the token [M].
  - **Roles Anywhere.** A shell script named `aws_signing_helper` prints the
    `credential_process` JSON, and it is invoked once per publisher [M].
    `aws_signing_helper` itself isn't available here; the SDK side is
    identical.
  - **Private CA.** An `httptest` TLS reverse proxy fronts SeaweedFS, and its
    self-signed certificate is written to a PEM file:
    - without the bundle: `x509: certificate signed by unknown authority`;
    - with `CABundle` or `AWS_CA_BUNDLE`: success, with requests still passing
      through `WrapTransport` [M].

    SigV4 signs the Host header. `httputil.ReverseProxy` forwards the
    client's Host (the proxy's `127.0.0.1:port`) unchanged, and SeaweedFS
    verifies against that, so no special handling was needed.
  - **Addressing.** `TestS3URLAddressing` checks the endpoint and addressing
    (no network):
    - `s3://b/p` goes to `https://b.s3.<region>.amazonaws.com/p/...`;
    - `PathStyle` overrides that;
    - `https://host/b/p` stays path-style;
    - `s3://` without a region is an error.
- **Session tokens.** Real temporary credentials always carry one, but
  SeaweedFS answers `InvalidAccessKeyId` to any `X-Amz-Security-Token` that
  its own STS did not issue. The header is signed, so a proxy can't strip it.
  So the stand-ins hand out an empty token for the SeaweedFS runs. Each Go
  test repeats its flow with a token against a fake S3 and asserts that
  every request carries it. For ClickHouse and chDB, a chain-provided token
  and one passed as `s3(url, key, secret, token, ...)` both arrived at a
  logging proxy [M].
- **ClickHouse 26.10.1 and chDB (libchdb 26.7):** `clickhouse local`, a
  separate throwaway `clickhouse server` (its own ports and data dir, not the
  shared one), and libchdb through chdb-go. Each read a parquetgo object with
  `s3(url, 'Parquet')` and no keys, with a clean environment
  (`env -i`, `AWS_EC2_METADATA_DISABLED=true` unless testing IMDS).
  `compare/cmd/credstubs` provided:
  - `:18901`, the Pod Identity agent and a plain STS;
  - `:18902`, a CONNECT proxy that terminates TLS for any host with a
    certificate from its private CA;
  - `:18903`, a TLS reverse proxy to SeaweedFS;
  - `:18904`, IMDSv2 as `aws_signing_helper serve` provides it.

  Findings:
  - **Pod Identity** works. The token file's content arrives as
    `Authorization` [M].
    - The C++ SDK only accepts a loopback address, 169.254.170.2 or
      169.254.170.23 for an http `FULL_URI`, which EKS satisfies [D].
    - A new fetch happens per query (per S3 client) [M].
  - **IRSA** works, but the STS endpoint is hard-coded as
    `https://sts.<region>.amazonaws.com`. `AWS_ENDPOINT_URL_STS` and
    `AWS_ENDPOINT_URL` are ignored [M]. It does honour `https_proxy` and
    `no_proxy`, which is how it was tested: the CONNECT stand-in, with the
    stand-in CA trusted via `caConfig` or `SSL_CERT_FILE`. Without the CA
    there is no STS call and the read fails [M].
    - The provider is cached process-wide: 1 STS call for 2 queries with 1 h
      credentials [M].
    - It refreshes inside `expiration_window_seconds` (default 120 s): 13 STS
      calls for 2 queries with 60 s credentials [M].
    - The STS region comes from `AWS_DEFAULT_REGION` or the profile, not
      `AWS_REGION` [D]. The EKS webhook sets both.
  - **`credential_process`** is not supported. With `AWS_PROFILE` naming such
    a profile, the helper was never executed and the read was anonymous
    (`AccessDenied`) [M]. `src/IO/S3/Credentials.cpp` builds the chain as web
    identity, env, SSO, ECS/container, IMDS, then
    `ProfileConfigFileAWSCredentialsProvider`, which reads only keys. The
    comment says: "we removed process provider because it's useless in our
    case" [D]. Static keys in a profile of the *credentials* file do work
    [M].
  - **IMDS through `AWS_EC2_METADATA_SERVICE_ENDPOINT`** works in local,
    server and chDB, with the IMDSv2 token flow [M].
  - **Custom CA:**
    - `<openSSL><client><caConfig>` in the server (or `clickhouse local
      --config-file`) config works, and so does `SSL_CERT_FILE`, which is
      read by the bundled OpenSSL's default verify paths [M];
    - without either, the read fails with `certificate verify failed` [M];
    - `AWS_CA_BUNDLE` has no effect (the string isn't even in the binary)
      [M];
    - chDB has no config file in the exporter's session, so `SSL_CERT_FILE`
      is the way there [M]. It *replaces* the default bundle, so point it at
      system roots + the private CA if the pod also talks to public
      endpoints. Go honours `SSL_CERT_FILE` the same way.
  - **The 26.10 server restriction.** On the **server**, a user query may
    not use the server's own credentials by default: the setting
    `s3_allow_server_credentials_in_user_queries` is `0`. `s3(url,
    'Parquet')` without keys then fails with
    `Code: 497 ... S3 access from user queries is not allowed to use the
    server's own credentials (environment variables, instance metadata,
    IRSA, ...)` [M].
    - Setting it to 1 in the ingest user's profile (or per query) makes IRSA,
      Pod Identity and IMDS work [M].
    - `extra_credentials(role_arn = ...)` stays allowed under the restriction
      [D].
    - `clickhouse local` and chDB don't enforce it [M].

### Workaround where a mode is unsupported: `credential_process` in chDB and ClickHouse

- **Preferred:** run `aws_signing_helper serve` (the Roles Anywhere helper's
  IMDSv2 emulator; default `127.0.0.1:9911`) next to the process, and set
  `AWS_EC2_METADATA_SERVICE_ENDPOINT=http://127.0.0.1:9911`. Leave
  `AWS_EC2_METADATA_DISABLED` unset. chDB, ClickHouse and this publisher then
  all read the same refreshed credentials, with no code [M against an IMDS
  stand-in, D for the helper].
- **Alternative (described, not implemented):** the Go side resolves
  credentials with `config.LoadDefaultConfig` (which runs
  `credential_process`) and `aws.CredentialsCache`. It calls `Retrieve` per
  batch and writes `s3(url, key, secret, session_token, 'Parquet',
  structure)`. ClickHouse accepts the session-token argument and sends it
  [M]. This covers per-statement `s3()` (Parquet and manifests). It does not
  cover the `object_storage` disks, whose keys are fixed in the table's DDL
  when it's created, so the tables would have to be recreated on every
  refresh. It would also add the SDK (+1.7 MB) to the chDB exporter. Serve
  mode covers the same case without either cost, so it wasn't built.

### Configuration examples

The publisher is a library here (no collector component yet), so its
settings are shown as `parquetgo.Config` fields. The chDB exporter's are its
collector YAML.

**EKS with IRSA.** The ServiceAccount is annotated
`eks.amazonaws.com/role-arn: arn:aws:iam::111122223333:role/otel-edge`, and
the webhook injects `AWS_ROLE_ARN`, `AWS_WEB_IDENTITY_TOKEN_FILE`,
`AWS_REGION` and `AWS_DEFAULT_REGION`.

```go
parquetgo.Config{URL: "s3://otel-telemetry/edge", S3Region: "eu-west-1"} // no keys: the chain
```
```yaml
exporters:
  chdb:
    parquet:
      url: https://otel-telemetry.s3.eu-west-1.amazonaws.com/edge   # no access_key_id: chDB's chain
```
```xml
<!-- central ClickHouse (pod with its own IRSA role): allow the ingest user the server's credentials -->
<profiles><ingest>
  <s3_allow_server_credentials_in_user_queries>1</s3_allow_server_credentials_in_user_queries>
</ingest></profiles>
```
```sql
INSERT INTO otel_traces SELECT ... FROM s3(
  'https://otel-telemetry.s3.eu-west-1.amazonaws.com/edge/eu-west-1/traces/v1/**/*.parquet', 'Parquet');
-- or, without relaxing the restriction: s3(url, 'Parquet', extra_credentials(role_arn = 'arn:aws:iam::111122223333:role/otel-central-read'))
```

**EKS Pod Identity.** Create the association with `aws eks
create-pod-identity-association --namespace otel --service-account
otel-collector --role-arn ...`. The agent injects
`AWS_CONTAINER_CREDENTIALS_FULL_URI=http://169.254.170.23/v1/credentials` and
`AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE=/var/run/secrets/pods.eks.amazonaws.com/serviceaccount/eks-pod-identity-token`.
The collector and central configs are the same as for IRSA: no keys
anywhere. Only `AWS_REGION` (or `S3Region`) is needed for `s3://` URLs.

**Nutanix Objects with a private CA.**

```go
parquetgo.Config{
	URL:         "https://objects.nutanix.example/otel/edge", // custom endpoint: path-style by default
	AccessKeyID: os.Getenv("NUTANIX_ACCESS_KEY"), SecretAccessKey: os.Getenv("NUTANIX_SECRET_KEY"),
	CABundle:    "/etc/ssl/nutanix/ca.pem", // or AWS_CA_BUNDLE; appended to the system roots
}
```
```yaml
exporters:
  chdb:
    parquet:
      url: https://objects.nutanix.example/otel/edge
      access_key_id: ${env:NUTANIX_ACCESS_KEY}
      secret_access_key: ${env:NUTANIX_SECRET_KEY}
# collector pod env: SSL_CERT_FILE=/etc/ssl/nutanix/bundle.pem   (system roots + the Nutanix CA)
```
```xml
<!-- central ClickHouse: trust the CA; keys in a named collection or in s3() -->
<openSSL><client>
  <loadDefaultCAFile>true</loadDefaultCAFile>
  <caConfig>/etc/clickhouse-server/nutanix-ca.pem</caConfig>
  <verificationMode>strict</verificationMode>
  <invalidCertificateHandler><name>RejectCertificateHandler</name></invalidCertificateHandler>
</client></openSSL>
<named_collections><nutanix_otel>
  <url>https://objects.nutanix.example/otel/edge/</url>
  <access_key_id>...</access_key_id><secret_access_key>...</secret_access_key>
</nutanix_otel></named_collections>
```

**IAM Roles Anywhere, from outside AWS.**

```ini
# ~/.aws/config (AWS_CONFIG_FILE) for the Go publisher
[profile otel]
region = eu-west-1
credential_process = aws_signing_helper credential-process --certificate /etc/ra/cert.pem --private-key /etc/ra/key.pem --trust-anchor-arn arn:aws:rolesanywhere:eu-west-1:111122223333:trust-anchor/TA --profile-arn arn:aws:rolesanywhere:eu-west-1:111122223333:profile/PR --role-arn arn:aws:iam::111122223333:role/otel-edge
```
```go
parquetgo.Config{URL: "s3://otel-telemetry/edge", Profile: "otel"} // or AWS_PROFILE=otel
```
For the chDB exporter and a central ClickHouse outside AWS, use serve mode
instead:
```sh
aws_signing_helper serve --certificate /etc/ra/cert.pem --private-key /etc/ra/key.pem \
  --trust-anchor-arn ... --profile-arn ... --role-arn ...        # listens on 127.0.0.1:9911
export AWS_EC2_METADATA_SERVICE_ENDPOINT=http://127.0.0.1:9911 AWS_REGION=eu-west-1
```
The exporter then has no keys (as for IRSA). The central server needs
`s3_allow_server_credentials_in_user_queries = 1` for the ingest user. The
Go publisher can use serve mode too, without the profile.

## Metrics

parquetgo publishes OTel metrics as Parquet in the contrib
`clickhouseexporter` **v0.161.0** metrics schema (ClickStack):
`otel_metrics_gauge`, `_sum`, `_histogram`, `_exponential_histogram` and
`_summary`, each with the envelope columns traces and logs carry. Central
ingests each object with `INSERT INTO otel_metrics_<type> SELECT … FROM
s3()`.

Only the parquet-go engine does this. **The arrow engine stays traces and
logs only**: `PushMetrics` on an arrow publisher returns an error.

Labels: **[M]** measured here · **[E]** estimate.

### Layout: one object per metric type per batch

One OTLP request can hold all five metric types. `PushMetrics` walks it once
and writes **one Parquet object per metric type that has data points**. Each
type is its own signal namespace: `metrics_gauge`, `metrics_sum`,
`metrics_histogram`, `metrics_exponential_histogram` and `metrics_summary`.
The table is `otel_` plus the signal.

```
{url}/{region}/metrics_gauge/v{schema}/{producer}/{epoch}/{generation}/{batch_id}.parquet
                                                        manifests/{generation}/{batch_id}.json
{url}/{region}/metrics_sum/v{schema}/...                (its own batch_id sequence, manifests, seal)
```

Why this layout:

- **One object is one table's insert.** The importer's `INSERT … SELECT
  FROM s3('<exact key>')` targets one table and forms one block
  (FASTPATH.md). A combined object would have to be read five times, each
  time filtering on a type column. It would also need a sparse union schema
  in which most columns are empty for most rows. Parquet row groups per type
  wouldn't help either: `s3()` can't address a row group.
- **Each object commits on its own.** In the manifest-less design
  (`../awss3`), each namespace is its own log: epoch plus sequential slot,
  with one create-only PUT per object. Every object is then exactly one slot
  commit, like a traces batch, and needs no cross-object transaction.
- **The ack waits for all of them.** `PushMetrics` commits the non-empty
  objects concurrently and returns nil only once all have committed.
  - In manifest mode: object then manifest, per type.
  - If one type fails, the collector retries the request. That republishes
    every type, the same at-least-once behaviour as a traces retry, and
    central's content check removes the copies.
  - In the inline design, each type's lane should key its content hash on
    (request hash, signal). A retried request then resolves the types that
    already committed as "ours" (step 1 of `Log.Append`) and appends only
    the missing ones [E: not wired, `../awss3` is outside this directory].
- **A retry is byte-identical.** The same pdata and the same envelope give
  the same bytes: fresh or reused encoder, serial or `Parallelism: 4`, and
  whether one type is encoded alone (`PGEncoder.MetricsOf`, the call an
  inline lane makes when it re-encodes for another slot) or all five in one
  walk (`PGEncoder.Metrics`) [M: `TestMetricsDeterministic`]. That test
  found the parquet-go `Reset` bug above.
- All objects of one request share `received_at`. `row_ordinal` counts
  within each object.
- The same generation, manifest and seal machinery as traces/logs applies
  per namespace. `Close` seals every open metrics generation too.

### Schema mapping

`METRICS_SCHEMA.md` has every table's column list: name, ClickHouse type,
Parquet type and rendering rule. It is the reference for `../otap-rs`. In
short:

| ClickHouse type (contrib DDL) | Parquet | Rendering, as clickhouse-go stores it |
| --- | --- | --- |
| `DateTime` (`StartTimeUnix`, `TimeUnix`, `Exemplars.TimeUnix`) | `INT64 TIMESTAMP(MILLIS, UTC)`, ClickHouse's own writer's mapping for DateTime | `uint32(floor(int64(ns) / 1e9)) * 1000`: signed, floor-divided, wrapped mod 2³², so 0 stays 0, MaxInt64 wraps, and sub-second precision is lost as in the exporter's table |
| `Map(LowCardinality(String), String)` | `MAP<string,string>` | **keys sorted in byte order** (clickhouse-go's `orderedmap.CollectN` sorts), values `AsString()` |
| `Float64` (`Value`, `Sum`, `Min`, `Max`, bounds, quantiles) | `DOUBLE` | raw bits. An int point becomes `float64(int)`; an unset value, sum, min or max is 0 |
| `UInt32` / `UInt64` / `Int32` / `Bool` | `INT32 UINT_32` / `INT64 UINT_64` / `INT32` / `BOOLEAN` | `Flags` raw; `AggregationTemporality` as the enum's int; `IsMonotonic` |
| `Array(...)` and `Nested` (`Exemplars.*`, `ValueAtQuantiles.*`) | one required 3-level `LIST` per sub-column | Exemplar trace and span ids are **always hex, zero ids too** (`"000…0"`, not `""` as in traces) |

Two rules were learned by failing, not by reading:

- **Map keys are sorted.** Written in pdata order, 3,080 of 3,140 rows
  differed from the exporter's, because ClickHouse compares a Map entry by
  entry. The exporter sorts every attribute map it sends. The contrib
  *traces and logs* exporters do too (`internal.AttributesToMap`). The chDB
  exporter and parquetgo's traces/logs keep pdata order, so their rows
  differ from what the contrib exporter would have stored (see Gaps).
- **DateTime is seconds**, wrapped as ch-go does it, not the nanoseconds the
  traces/logs tables keep.

With schema inference (no structure in `s3()`), ClickHouse reads the
DateTime columns as `DateTime64(3, 'UTC')`. Inserting them into `DateTime`
gives the same values.

### Correctness [M]

`compare/metrics_test.go`, `TestMetricsSameRowsAsExporter`:

- **The reference is the contrib exporter itself.** It is built through its
  factory (`compare/refexporter.go`) with `create_schema` on, synchronous
  inserts, and no queue or retries, and it writes to the ClickHouse 26.10
  server over the native port.
- **The same pmetric data goes to parquetgo**, serial and `Parallelism: 4`,
  and to S3 under `otel/metrics-go/…`. Each object is `INSERT … SELECT`ed
  into a table created from the exporter's own `SHOW CREATE TABLE`, once
  with the explicit `s3()` structure and once with schema inference.
- **Comparison, per table:** `count()`, `sum(cityHash64(<all contrib
  columns>))`, and `EXCEPT` in both directions. The envelope is checked too:
  one batch per request, `row_ordinal` 0..n−1, one `received_at`, producer,
  epoch and schema version.
- **Data**, 3,420 points per type (17,100 rows):
  - `Metrics(3000)`: the realistic generator, 3,000 points of each type.
  - `NastyMetrics(700)`: 420 points of each type, with:
    - int, double and unset values, including NaN, ±Inf, −0, 5e−324,
      ±MaxFloat64, MinInt64/MaxInt64 and 2⁵³;
    - exemplars: 0, 1, 2 or 30 per point; int, double and empty values; NaN
      and Inf; zero ids; zero and extreme times;
    - attribute maps that are empty or hold 60 entries with 100 KB values,
      with every value type (bytes, slice, map, empty) and NUL keys;
    - start time 0, and timestamps of 1 ns, 2³² s (the DateTime wrap),
      MaxInt64, 2⁶³ and MaxUint64;
    - exponential histograms with only a zero count, all-zero buckets,
      negative buckets with negative offsets, `MinInt32`/`MaxInt32` offsets
      and scales, and 160 buckets;
    - explicit histograms that are empty, whose bucket and bound counts
      don't match, that have NaN/Inf bounds, `MaxUint64` counts, or 200
      bounds, and whose sum, min or max is unset or NaN/Inf;
    - summaries with 0, 1, 2 or 5 quantiles, including NaN;
    - invalid UTF-8, NULs and 100 KB strings in names, descriptions, units,
      scopes, schema URLs and service names (including a double
      `service.name`);
    - every flags value, every temporality, and both monotonicities.
- **Result: identical.** Count and checksum are equal for all five tables,
  both engines, and both read modes, and `EXCEPT` returns 0 rows either way.
  The test passes `-race` clean. `TestSameRowsAsChdb` still passes.
- **Local unit tests (`metrics_test.go`)** cover:
  - determinism;
  - the Parquet schema read back with parquet-go (column order, required,
    `TIMESTAMP(MILLIS)`, `BOOLEAN`);
  - `dtMillis` against `pcommon.Timestamp.AsTime().Unix()`;
  - one object and manifest per type under `file://`;
  - the rejection of an Empty-typed metric;
  - an arrow publisher refusing metrics.

**Central ingest under the FASTPATH single-block settings**
(`TestMetricsCentralSingleBlock`):

- The target is the exporter's table plus the envelope, `PARTITION BY
  toDate(received_at)` (constant per batch) and
  `non_replicated_deduplication_window = 1000` (`compare.CentralDDL`).
- For 10,000-point objects and the hostile ones: each object became
  **exactly one part**, and two retries with the same
  `insert_deduplication_token` **added nothing** [M].
- **For a much bigger object it did not hold** [M]. The object was 150,000
  gauge points, 158 MB decoded.
  - With the FASTPATH settings, ClickHouse 26.10 split it into two blocks
    at a row that varied from insert to insert (for example 139,135 +
    10,865, or 97,720 + 52,280). This happened in most runs of the test.
  - A retry with the same token that split differently re-inserted the
    tail: 150,000 + 3,477 rows in one run.
  - `input_format_parquet_preserve_order = 1` and
    `input_format_parquet_use_offset_index = 0` each looked like a fix in
    one probe, then failed in the next.
  - Turning squashing on (`min_insert_block_size_rows/bytes` above the
    batch) always gave one part. But the dedup id of a squashed block
    depends on the chunks it came from, and a retry once inserted a full
    second copy (302,180 rows).
  - The splitting came and went. Some probe series of fresh 150,000-point
    objects split in most inserts; others of the same size, minutes later,
    never did. No object of 100,000 points or fewer split, in more than 60
    inserts across 10k–140k-point sizes (in fact nothing up to 149,000 did).
  - So: keep objects well below about 100 MB decoded (10k points is about
    5–9 MB), and treat the token as the backstop FASTPATH says it is. The
    count check against `row_ordinal`, with repair, is what guarantees
    exactly-once.

### Measurements

**Edge [M]** (`cmd/pubbench -signal metrics_<type>|metrics`,
`results/runmetrics.sh`):

- Setup: 3 processes × 30 batches after 3 warm-up batches, 10,000 points of
  one type per batch (`compare.MetricsBatch`), parquet-go engine, default
  options (bloom filters on).
- The box was 4 vCPUs at load 1.0–1.5. Values are medians [min–max].
- `metrics` is one request holding 10,000 points of *each* type (50,000
  points, 5 objects), so its k rows/s column counts 10,000 per batch.
- Raw data: `results/metrics-pubbench.{jsonl,md}`.

| type | CPU µs/point, local | CPU µs/point, S3 | ms/batch S3 (serial / ×4) | object B/point | Go allocs/batch (S3) | peak RSS MB |
| --- | --- | --- | --- | --- | --- | --- |
| gauge | 6.9 [6.7–7.7] | 7.3 [7.2–7.5] | 78 / 59 | 21.6 | 1,458 | 111 |
| sum | 7.1 [6.7–7.2] | 7.1 [7.1–7.3] | 77 / 58 | 12.9 | 1,489 | 111 |
| histogram | 10.2 [10.0–10.4] | 10.6 [10.5–10.6] | 111 / 85 | 28.8 | 1,584 | 134 |
| exponential histogram | 11.1 [11.0–11.1] | 11.2 [11.2–11.7] | 121 / 97 | 43.8 | 1,646 | 146 |
| summary | 6.4 [6.3–7.2] | 6.6 [6.5–7.0] | 70 / 46 | 25.3 | 1,437 | 114 |
| all five in one request | 8.4 | 8.6 | 431 / 316 | 26.5 | 7,702 | 332 |

- **Most of the cost is the attribute maps.** Every row repeats 12 resource
  attributes and 4 point attributes (32 strings), and sorts the point map;
  the resource and scope maps are sorted once per scope.
- **Where the time goes.** In a gauge profile, 37% is the walk
  (`Value.Level` appends into the per-leaf buffers dominate). 45% is
  parquet-go's column writing, dictionary and page statistics, and zstd.
- **Options barely change it.** Without bloom filters it is the same CPU
  and 13% smaller. Without statistics and the page index it is 8% faster.
  Without dictionaries it is 19% slower and 4.6× bigger.
- **Traces are similar per row:** 7 µs per span.
- `Parallelism: 4` cuts latency 25–35% for about 5% more CPU.

**Central [M]** (`cmd/metricscentral`; raw data in
`results/metrics-central.jsonl`):

- **Data:** per type, 100 contiguous batches of 10,000 points, published by
  parquetgo to SeaweedFS. That is 1 M points per type: 2,000 series × 500
  rounds of 10 s, or 83 minutes.
- **Ingest:** one `INSERT … SELECT FROM s3('<object>')` per object, with the
  single-block settings and a token, into the exporter's table plus the
  envelope, merges stopped.
- **Server CPU:** the `system.events` `OSCPUVirtualTimeMicroseconds` delta
  over the 100 inserts. It counts only when no other query ran, which the
  `Query` counter confirmed (`OtherQueries: 0`).
- **Wall time:** the per-insert `elapsed_ns` from `X-ClickHouse-Summary`.
- **Stored bytes:** after `OPTIMIZE FINAL`, from `system.parts`
  (`data_compressed_bytes`; `bytes_on_disk` is within 0.3 B), for the
  exporter's table alone and with the envelope.
- **Runs:** two full runs, plus one run with 100,000-point objects. The
  server's load average was 1.9–3.2.

| type | server CPU µs/point (2 runs) | insert wall µs/point, median of the runs' medians [min–max over both] | stored B/point, exporter table | + envelope | uncompressed B/point | biggest columns, B/point |
| --- | --- | --- | --- | --- | --- | --- |
| gauge | 7.6 / 8.2 | 7.9 [6.3–17.2] | **17.7** | 18.2 | 531 | ResourceAttributes 8.9, Value 5.3, Attributes 2.1 |
| sum | 7.5 / 7.8 | 7.8 [6.3–12.6] | **15.3** | 15.9 | 546 | ResourceAttributes 8.9, Value 2.9, Attributes 2.1 |
| histogram | 9.5 / 9.5 | 9.7 [8.2–14.6] | **48.2** | 48.8 | 839 | BucketCounts 24.1, ResourceAttributes 8.9, Sum 4.0, Max 2.2 |
| exponential histogram | 10.4 / 11.0 | 10.6 [8.6–24.5] | **38.2** | 38.8 | 890 | PositiveBucketCounts 12.2, ResourceAttributes 8.9, Sum 7.5 |
| summary | 7.7 / 7.9 | 8.0 [6.9–11.8] | **31.8** | 32.5 | 613 | ValueAtQuantiles.Value 15.1, ResourceAttributes 8.9, Sum 2.8 |

- **Per-insert overhead is real.** With 100,000-point objects the server
  CPU per point drops to 5.7 (gauge) and 7.2 (histogram) µs, so each insert
  costs about 20 ms of fixed work. That is the S3 GET, and a part with its
  marks, primary index and the DDL's six map bloom-filter skip indexes.
  Batching larger objects, or grouping several small objects into one
  insert, amortizes it; FASTPATH's one-object-per-insert rule for dedup
  pays it.
- **S3 bytes per point:** gauge 21.6, sum 13.4, histogram 30.1,
  exponential histogram 43.6, summary 25.9. That is Parquet with bloom
  filters; without them, gauge is 18.7.
- **Stored bytes hardly depend on how much data a part holds.** After 50k
  points gauge was 18.1 B/point, after 1 M 17.7. The largest single term,
  `ResourceAttributes` at about 8.9 B/point, is the same for every type.

**How realistic the data is** (`compare/metricsgen.go`, `MetricsBatch`):

- **Shape:**
  - 20 resources (5 services × 4 pods), each with 12 k8s/host/SDK resource
    attributes, including per-pod `service.instance.id`, `k8s.pod.name`
    and `host.name`;
  - one scope, 10 metric names per type with descriptions and units;
  - 10 attribute sets of 4 HTTP attributes per metric;
  - 2,000 series per type, a 10 s interval, and contiguous batches.
- **Values:**
  - gauges: half ints near 1e6 with noise, half doubles random-walking with
    noise in the low bits;
  - sums: cumulative, smooth, two-thirds ints;
  - explicit histograms: the SDK default 15 bounds, a cumulative
    bell-shaped distribution filling most buckets;
  - exponential histograms: scale 3, 24–48 random buckets, delta
    temporality;
  - summaries: 5 noisy quantiles;
  - exemplars on 10% of histogram points.
- **What it gets right:** the structural cost. Maps repeated per row, the
  exporter's sort key (`ServiceName, MetricName, hour, cityHash64(Attributes),
  TimeUnix`), which interleaves a service's 4 pods row by row, and the
  exporter's codecs and indexes are all real.
- **What it can't know:** the value entropy and label shape of a real fleet.
  - Real gauges are often integers with long runs of equal values, so
    `Value` compresses better. Real histograms are often sparse, so bucket
    arrays of zeros compress far better than the 24 B/point here.
  - Real fleets have more resource attributes (cloud and k8s labels, often
    20+), and more pods per service. `ResourceAttributes` scales with both.
  - Series churn (pods coming and going) worsens compression.
- **So:** treat stored bytes as ±50% for a real fleet, sensitive mostly to
  resource-attribute count and histogram density. Treat CPU per point as
  ±30%, scaling with attribute entries per point.

### The sizing calculator's three numbers

| calculator input | assumed | measured replacement | notes |
| --- | --- | --- | --- |
| central CPU per point | 2.5 µs | **≈ 8 µs** (gauge, sum, summary 7.5–8.2; histograms 9.5–11) at 10k-point objects; 5.7–7.2 at 100k-point objects | [M] server CPU for `INSERT … SELECT FROM s3()` into the exporter's DDL (with its 6 skip indexes) plus envelope, single-block settings. Merges (and `OPTIMIZE`) excluded. |
| stored bytes per point | 10 B | **≈ 16–18 B** for gauge and sum; **32 B** summary; **38–48 B** histograms (+0.5–0.7 B with the envelope) | [M] compressed, after `OPTIMIZE FINAL`, 1 M points per type, synthetic data (±50% [E], see above) |
| edge CPU per point | 2 µs | **≈ 7 µs** (gauge 6.9–7.3, sum 7.1, summary 6.4–6.6); **10–11 µs** histograms; 8.4–8.6 µs for all five in one request | [M] pubbench, encode + PUT, bloom filters on, 12 resource and 4 point attributes |
| (blend, for one number) | | central **8.2 µs**, stored **25 B**, edge **7.8 µs** | [E] weights 40% sum, 30% gauge, 20% histogram, 5% exponential histogram, 5% summary points |

### Gaps

- **arrow engine:** no metrics (traces/logs only).
- **Map key order for traces/logs.** The contrib exporter sorts attribute
  map keys for *all* its tables. parquetgo's traces/logs (and the chDB
  exporter, their reference) keep pdata order.
  - Lookups by key (`m['k']`) and `mapContains` give the same results.
  - Hashes, `EXCEPT`, `toString(map)` and `mapKeys` order don't.
  - Fixing it means sorting in `walk.go`/`pgo.go` and, to keep the chDB
    comparison, in the chDB exporter too. Not done here.
- **Duplicate attribute keys** (possible only in wire-decoded pdata) keep
  pdata order here. The exporter's unstable sort leaves them unspecified,
  so such rows can differ.
- **A metric of type Empty rejects the whole request,** as the exporter
  does (`ErrMetricTypeUnset`). A collector wrapper should make that a
  permanent error.
- **Manifest mode republishes every type on a retry** after a partial
  failure. The consumer's content check removes the copies. The inline
  per-type lanes are not wired (`../awss3` is outside this directory);
  `MetricsOf` is the hook.
- **Big objects and single-block inserts:** see "Correctness" above.
- **The `Reset` workaround** relies on parquet-go v0.32.0's unexported
  field name. `TestReusedWriterIdentical` fails loudly if a version bump
  breaks it, and the encoder then falls back to new writers.
- **Lakehouse readers get seconds, not nanoseconds,** in the metric time
  columns, because that is the exporter's table type. Adding a ns column
  would be a schema change for both producers.
- **Not tested:** real AWS S3; Spark reading metrics objects; exemplar-heavy
  or high-churn workloads at scale; the central cost with concurrent
  inserts or merges running; metrics through the chDB exporter (it has
  none).

## Existing art

- **contrib `awss3exporter`:** marshalers `otlp_json`, `otlp_proto`, `sumo_ic`
  and `body`, with gzip or zstd compression and strftime partitioning. It has
  **no Parquet** and no envelope, batch ids or manifests. An encoding
  extension could plug in a Parquet marshaler, but the manifest/commit
  protocol would still have to live somewhere.
- **contrib `parquetexporter`:** a skeleton that never wrote data; deprecated
  and removed in 2023 (#27284).
- **contrib `fileexporter`:** OTLP JSON or proto, with rotation. No Parquet.
- **otel-arrow / OTAP:** the Go `otelarrowexporter`/`receiver` are a wire
  protocol, not files. The Rust OTAP dataflow engine has a Parquet exporter,
  but it writes OTAP's normalised star schema: attributes in separate tables
  joined by parent ids. That isn't ClickHouse's flat `otel_traces` schema
  with `Map` columns, so central couldn't `INSERT … SELECT` it without joins.
  It also isn't a Go collector component.
- **Conclusion:** nothing off the shelf writes this schema. The Go path here
  is about 1,100 lines on top of an existing library (walker, column buffers,
  publisher), and it could become a contrib-style exporter or an
  `awss3exporter` encoding extension plus a small manifest writer.

## Pros and cons at the edge

**chDB (RowBinary → `INSERT INTO FUNCTION s3(... 'Parquet')`)**

- **Pros:**
  - ClickHouse writes the Parquet ClickHouse reads: one vendor at both ends,
    and a well-tested writer.
  - Parallel encoding by default, so batch latency is slightly lower on an
    idle multi-core box.
  - SQL is right there:
    - local queries on the edge's tables;
    - staging and Buffer tables;
    - transforms, filtering and pre-aggregation in SQL before publishing;
    - the native-table and `s3_plain_rewritable` options;
    - the `file()` workaround for stock chdb-go.
  - Changing the published shape is a `SELECT` or a structure string, not
    code.
- **Cons:**
  - 566 MB glibc-only native library.
  - About 350–400 MB RSS against 100–140 MB.
  - 1.3–1.4× the CPU per batch.
  - Around 200 ms warm start, and seconds cold [E].
  - One data path per process.
  - The shutdown quirk with MergeTree.
  - Publishing needs the chdb-go fork (stock chdb-go can't stream RowBinary).
  - About 1 ms per statement [R].
  - The edge's chDB version must be pinned against central [R].
  - Can't go into `otelcol-contrib` [R].

**Go-native (parquet-go + aws-sdk-go-v2)**

- **Pros:**
  - Identical rows and schema: verified on the server, pyarrow and Spark.
  - 25–35% less CPU per batch.
  - About 3–4× less RSS.
  - A 16 MB static binary: arm64, musl and distroless all work, with no CGO,
    purego or dlopen.
  - Instant start.
  - Ordinary Go errors and retries (the AWS SDK's standard retryer).
  - Every Parquet knob in code: row groups, blooms per column, timestamp
    unit, sort order, key-value metadata. A Spark-friendly
    `TIMESTAMP(MICROS)` or an Iceberg-compatible layout is a small change.
  - Could plausibly live in contrib.
- **Cons:**
  - No SQL at the edge: no local queries, staging, Buffer tables, SQL
    transforms or pre-aggregation, and no native or `s3_plain_rewritable`
    tables. Those need Go code (collector processors) or chDB.
  - The writer and reader are different projects, so format edge cases are
    our risk. The arrow-go page split shows this is real, so keep a
    server-side conformance test (`compare/`) in CI and pin library
    versions.
  - Serial encoding is slightly slower per batch than chDB on an idle box.
    `Parallelism` closes that, at +5–7% CPU.
  - The walker is duplicated from `chdbexporter` (copied, not shared) until
    it moves to a common package.
  - `rowbinary_bytes` in the manifest means something different.

## Recommendation, and when it flips

**Publish Parquet with the Go-native parquet-go writer, and make chDB optional
for local queries.** Concretely:

1. Move `walk.go` into a package both exporters import.
2. Make the Parquet-only publisher a pure-Go exporter, built without chdb-go.
3. Keep `chdbexporter` for deployments that want local SQL. There it can
   either keep its own Parquet path or share the Go writer and feed chDB only
   the local tables.

The hybrid gets both, and the central side can't tell the producers apart.

Turn bloom filters off by default, or keep one on `TraceId`, and add a
microsecond timestamp if a lakehouse will read the files.

It flips back to chDB-only when:

- **The edge must answer queries.** Local search or the oscope UI against the
  last hours of data needs a query engine, and chDB is then already in the
  process. Using it for Parquet too costs about 20–40 ms of CPU per 10k-row
  batch over Go, which is minor next to the MergeTree insert it's doing
  anyway (about 90 ms [R]).
- **The published data is computed in SQL:** pre-aggregation, sampling,
  joins or enrichment that you want in SQL rather than in processors.
- **Native tables or `s3_plain_rewritable` publishing come back into scope.**
  The central report already argues against it for transfer [R].
- **Writer/reader symmetry is worth more than footprint.** For example, if you
  can't run a conformance test in CI, or if central moves ahead to a
  ClickHouse version that tightens the reader again.

It does **not** flip on throughput. On this box the Go path matches or beats
chDB's rows/s per batch with less CPU, and a collector's sending queue can run
several batches in parallel.

## Measured vs estimated

- **Measured [M]:**
  - everything in the Correctness and Benchmarks sections;
  - the page-split failure and its fixes;
  - binary sizes;
  - warm ready times;
  - PUT counts;
  - Spark 4.0.1 and pyarrow reads;
  - library inventories;
  - server read cost.
- **Measured for metrics [M]:** everything in "Metrics" not marked [E]:
  - row identity with the contrib exporter;
  - the single-block behaviour;
  - edge and central cost per point, and stored bytes on synthetic data.
- **Estimated [E]:**
  - cold-start time for libchdb;
  - arm64 chDB availability and behaviour;
  - Databricks behaviour, inferred from Spark;
  - real-S3 latency, where both paths do 2 PUTs per batch so the difference
    should be unchanged;
  - the Go exporter's size once linked into a full collector (roughly +8 MB
    on top of the collector for parquet-go + the AWS SDK, which contrib collectors already link, against +566 MB of
    library for chDB).
  - the credential modes against local stand-ins (see "Credentials and
    deployment targets" for what each test covers).
- **Not tested:**
  - real AWS S3, real STS, a real EKS cluster, Nutanix Objects and
    `aws_signing_helper` itself (each was replaced by a local stand-in);
  - concurrent pushes through the Go publisher beyond the `-race` test's
    serial use;
  - batches large enough to need multipart (none do: one PUT allows 5 GiB);
  - arrow-go versions other than v18.7.0.
- **Noise:** the box was shared, with a load average of 3.5–5.5 against 4
  vCPUs. Latency ranges run up to ±20%; CPU/batch and allocation counts are
  much steadier.

## Running it

```sh
cd parquetgo && go test ./... && go test -tags noarrow ./...
cd compare
export CHDB_LIB_PATH=/path/to/libchdb.so CHDB_TEST_S3=http://127.0.0.1:18333/otel \
  CHDB_TEST_S3_KEY=otel CHDB_TEST_S3_SECRET=otelsecret CHDB_TEST_CLICKHOUSE=http://127.0.0.1:18123
go test -race -v -run 'TestSameRowsAsChdb|TestArrowPageSplitVariants' .
(cd .. && go test -v -run 'TestCreds|TestS3URLAddressing' .)   # credential modes, needs CHDB_TEST_S3*
go test -run '^$' -bench GoEncode -benchtime 20x -count 3 .
go build -o $S/pubbench ./cmd/pubbench && S=$S results/runbench.sh && python3 results/summarize.py $S/bench.jsonl
# metrics: correctness against the contrib clickhouseexporter (native port: CHDB_TEST_CLICKHOUSE_NATIVE, default host:19000)
go test -race -v -run 'TestMetrics' .
S=$S results/runmetrics.sh && python3 results/summarize.py $S/metrics.jsonl       # edge, per type
go build -o $S/metricscentral ./cmd/metricscentral && $S/metricscentral -n 10000 -batches 100   # central
```
