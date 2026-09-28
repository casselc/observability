# conformance: the Go edge and the Rust edge, row for row, after the consumer

DECISIONS.md D1 keeps two edge implementations, and requires that they stay
row-identical. This directory checks it end to end, on the ClickHouse server:
the same OTLP requests go through the Rust edge (`otap-s3pq`,
`../otap-rs/configs/edge.yaml`) and the Go edge (`otelcol-s3pq` with the `s3pq`
exporter, `go-edge.yaml`), each into its own S3 root; the one Rust consumer
(`consume`, built from `../otap-rs`) ingests each root into its own database;
`compare.py` then compares the databases table by table, and the objects
slot by slot.

Labels: **[M]** measured here, on the shared 4-vCPU box, SeaweedFS and
ClickHouse 26.10 on localhost.

## Result [M] (2026-09-26)

| | layout B (`series_table`, the default) | ClickStack metrics tables |
|---|---|---|
| checks | **228 PASS, 0 FAIL**, 1 known difference | **187 PASS, 0 FAIL** |
| tables | `otel_traces` 4,180 rows, `otel_logs` 4,180, `otel_metrics_number_points` 11,926, `_histogram_points` 5,663, `_exponential_histogram_points` 5,603, `_summary_points` 5,543, `otel_metrics_series` 11,539: **count + hash equal, EXCEPT ALL empty both ways** | the five contrib tables, 5,543–6,143 rows each: equal |
| content keys | the same keys, the same rows per key, in every counted table | the same |
| objects | the same namespaces, object counts, S3 metadata, footer key-values, Parquet schema, BYTE_STREAM_SPLIT / DELTA columns and statistics policy (layout B: none) | the same; dictionary choices align |

`results/series_table.txt` and `results/clickstack_tables.txt` are the runs.

**With the late split (D31, 2026-09-28)** [M]: both edges at their default
`late_split_after: 15m`; the `nasty-700` traces and logs, whose timestamps
reach from 1 ns to `i64::MAX`, are each split into a bulk and a late object
by both edges alike (the same slots, keys, rows, metadata and footers).
Layout B: **272 PASS, 0 FAIL** (`results/series_table.txt`, replaced); the
tables' rows are unchanged (traces and logs 4,180, now under 4 content keys
each). The first run failed on two findings, both fixed: the Go edge
committed the two parts concurrently, so their slot order differed from the
Rust edge's (both now append bulk, then late), and a reused parquet-go
writer kept the split's footer keys on the next unsplit object.

**The one difference, span kinds outside the enum.** A span with kind 6
(invalid on the wire) is stored as `''` by the Go edge, which is what
`pdata.SpanKind.String()` and so the contrib exporter store, and as
`'Unspecified'` by the Rust edge: otap-dataflow's OTLP span view reads the
kind as `SpanKind::try_from(v).unwrap_or(Unspecified)`
(`.upstream/.../views/otlp/bytes/traces.rs`), before this crate's renderer
(which maps unknown kinds to `''`, like Go) sees it. `compare.py` normalises
it (`KNOWN`) and reports the rows it changes (60 of 4,180). Out-of-range
status codes (9) and severity numbers (300) are stored alike by both edges.

Also measured, not a row difference: the Go objects are **28% larger** than
the Rust objects on these datasets (1.32 MB against 1.03 MB; traces and logs
+26%), with the same encodings per column: parquet-go's zstd and V2 pages
against parquet-rs's. Wire bytes are not billed by S3, and central reads the
same rows; see `../parquetgo/README.md` for the cost measurements.

## Faults [M] (`go_faults.sh`, `results/go-faults.txt`)

The Rust edge's fault scenarios (`../otap-rs/scripts/faults.sh`), with the Go
edge (`otelcol-s3pq`, `go-edge.yaml`, `put_timeout: 1s`) behind
`faultproxy2`, a sender that resends until it gets a 2xx, and the Rust
consumer. Six distinct 10k-span requests per scenario. **All six pass:
60,000 rows and 6 content keys in central in every scenario.**

| Scenario | Fault | Go edge | S3 | Central |
|---|---|---|---|---|
| ambiguous | every 2nd PUT applied, answer held 3 s | 3 committed, **3 resolved as ours by HEAD** | 6 objects | 60,000 / 6 |
| applylate | every 2nd PUT held 2.5 s before it reaches S3 | **5 resent** after HEAD found the slot free; the late copies got 412 | 6 | 60,000 / 6 |
| dropped | every 3rd PUT answered 503, never applied | the SDK's retry or a resend after HEAD (2) | 6 | 60,000 / 6 |
| unresolved | PUT answers and the first 2 HEADs held 3 s | requests answered 503 while unresolved; the retries resolved their slots first (3 own) | 6 | 60,000 / 6 |
| crash | answers held 8 s, SIGKILL at 3 s, restart | the resent request committed again in the new epoch | 7 (1 copy) | 60,000 / 6: the copy skipped by the content check |
| zombie | edge A keeps running after B starts | A met the consumer's tombstone: **halted**, new epoch | 6, 2 tombstones | 60,000 / 6 |

## What is compared

- **Datasets** (`$D`): `otap-rs/tools/cmd/otlpgen -out` and `-metrics -out`
  (testgen 3,000 spans / logs / points per type; `nasty-700`: invalid UTF-8,
  NULs, NaN/±Inf in maps, 100 KB strings, extreme ints and timestamps, zero
  ids, negative durations, exemplars, extreme exponential histograms;
  `metrics-extra.pb`: wire-built duplicate map keys above pdqsort's
  12-entry cutoff; `metrics-mixed-10000`: 2,000 points of each type), plus
  `gen` (this directory): unicode everywhere (emoji, RTL, combining marks,
  CJK, U+2028, a BOM, NUL, invalid UTF-8), an empty key, empty and
  1,000-entry maps, 1 MiB attribute values and bodies, nested maps and
  slices, `service.name` missing or not a string, out-of-range enums,
  histograms with 0–500 buckets and `MaxUint64` counts, exponential
  histograms at scales −10 and 20 with 1,000 buckets and extreme offsets,
  summaries with 100 quantiles, exemplars with filtered attributes, series
  repeated across resources and scopes.
- **Central** (both databases, as the consumer created them): the same
  tables; per table count + `sum(cityHash64(every column))` and `EXCEPT ALL`
  both ways over every column but the run's identity (`producer_id`,
  `producer_epoch`, `received_at`, `content_key`); the same content keys
  with the same row counts (so the Go edge's BLAKE3 of its re-marshalled
  request equals the Rust edge's of the bytes it received); the envelope
  (one `received_at` per object, `row_ordinal` 0..n−1, the producer).
- **Objects**: the same namespaces and counts; per slot the S3 user
  metadata (`x-amz-meta-oscope-*`, all but producer, epoch and received),
  the Parquet footer key-values (= the metadata), the Parquet schema (every
  leaf's physical and logical type and levels; a signed INT32/INT64 with or
  without its `Int(signed)` annotation counts as the same type), the
  encodings that are part of the contract (BYTE_STREAM_SPLIT,
  DELTA_BINARY_PACKED) and the statistics policy. Dictionary choices are
  reported (none differ now).

## Run it

```sh
S=scratch; B=$S/bin    # otap-s3pq, consume (../otap-rs, cargo --release), otlpsend (../otap-rs/tools),
                       # otelcol-s3pq (../awss3/collector/builder-config.yaml with ocb v0.161.0)
(cd ../otap-rs/tools && go build -o $B ./cmd/otlpgen ./cmd/otlpsend)
$B/otlpgen -out $S/data && $B/otlpgen -metrics -out $S/data
go run ./gen -out $S/data
export AWS_ACCESS_KEY_ID=otel AWS_SECRET_ACCESS_KEY=otelsecret    # SeaweedFS; bucket goedge-conf
B=$B D=$S/data RUN=c1 ./run.sh                                    # layout B
B=$B D=$S/data RUN=c2 LAYOUT=clickstack_tables DATASETS="metrics-testgen-3000 metrics-nasty-700 \
  metrics-extra metrics-hostile metrics-mixed-10000" ./run.sh
```

`KEEP=1` keeps the two databases (`goedge_{run}_rust`, `goedge_{run}_go`);
`python3 compare.py RUN S3_ROOT` re-runs the comparison alone.
