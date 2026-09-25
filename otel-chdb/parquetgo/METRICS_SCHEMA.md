# Metrics Parquet schema (parquetgo)

The Parquet layout parquetgo publishes for OTel metrics. It targets the contrib
`clickhouseexporter` **v0.161.0** metrics tables exactly
(`internal/sqltemplates/metrics_*_table.sql`, filled by
`internal/metrics/*_metrics.go`), plus the edge envelope columns that the
traces/logs objects carry. Central ingests an object with
`INSERT INTO otel_metrics_<type> SELECT <cols> FROM s3(<object>, 'Parquet')`.

This file is the reference for other producers, such as `../otap-rs`. The
correctness test (`compare/metrics_test.go`) checks every rule below against
a real clickhouseexporter v0.161.0 writing to ClickHouse 26.10.

**Revisions since the first draft**, which other producers may have copied:

1. Map columns are **sorted by key**, not written in pdata order. The first
   draft said pdata order, and the correctness test failed on almost every
   row until the order was fixed.
2. The DateTime wrap examples are corrected: `633_437_444_000` and
   `3_661_529_851_000`.
3. `i32` carries the `INT(32, signed)` annotation.

## Objects

- One Parquet object per metric type per batch. An OTLP request with gauges
  and sums becomes two objects. Types with no data points write nothing.
- Signal namespaces: `metrics_gauge`, `metrics_sum`, `metrics_histogram`,
  `metrics_exponential_histogram` and `metrics_summary`. The table is
  `otel_` + the signal. Each namespace has its own batch-id sequence and its
  own manifests, and in the manifest-less design its own log (epoch, slot).
- One row group, every column `required`, and columns in the order below.
  Nested columns are flattened, as with `flatten_nested = 1`: one
  3-level `LIST` per sub-column (`<name>` / `list` / `element`).
- Rows appear in pdata order: ResourceMetrics, then ScopeMetrics, then
  Metrics (only those of this type), then data points.
- A request that holds a metric of type Empty is rejected as a whole, as the
  exporter rejects it (`metrics type is unset`).

## Parquet physical and logical types

| Tag | ClickHouse type | Parquet |
| --- | --- | --- |
| `str` | String / LowCardinality(String) | `BYTE_ARRAY` + `STRING` (bytes as-is, invalid UTF-8 kept) |
| `map` | Map(LowCardinality(String), String) | `MAP` { repeated group `key_value` { required `key` str; required `value` str } } |
| `dt` | DateTime | `INT64` + `TIMESTAMP(MILLIS, isAdjustedToUTC=true)`: what ClickHouse's own Parquet writer produces for `DateTime` |
| `ts9` | DateTime64(9) | `INT64` + `TIMESTAMP(NANOS, isAdjustedToUTC=true)` |
| `f64` | Float64 | `DOUBLE` (raw IEEE bits: NaN, ±Inf and −0 pass through) |
| `i32` | Int32 | `INT32` + `INT(32, signed)` (ClickHouse's own writer omits the annotation; both read back as Int32) |
| `u16` / `u32` | UInt16 / UInt32 | `INT32` + `INT(16/32, unsigned)` |
| `u64` | UInt64 | `INT64` + `INT(64, unsigned)` |
| `bool` | Bool | `BOOLEAN` |
| `list<T>` | Array(T) | `LIST` { repeated group `list` { required `element` T } } |

An empty list or map is written as the list/map being present and empty
(definition level 0 for the leaf), never as null.

## Rendering rules

These are what clickhouse-go stores when the exporter appends the row.

- **`dt` (all DateTime columns, including `Exemplars.TimeUnix`)**: the
  exporter passes `pcommon.Timestamp.AsTime()` and clickhouse-go/ch-go stores
  `uint32(t.Unix())`. So the stored second is
  `s = uint32(floor(int64(ns) / 1e9))`: the nanoseconds are reinterpreted as
  a **signed** int64, floor-divided (not truncated toward zero), and wrapped
  mod 2³². The Parquet value is `s * 1000` (milliseconds). Examples:
  `0 → 0`; `1_700_000_000_999_999_999 → 1_700_000_000_000`;
  `math.MaxInt64 → uint32(9_223_372_036) * 1000 = 633_437_444_000`;
  `ns = 1<<63` (int64 min) `→ uint32(-9_223_372_037) * 1000 = 3_661_529_851_000`.
  Sub-second precision is lost, as it is in the exporter's table.
- **`map` attributes** (`ResourceAttributes`, `ScopeAttributes`,
  `Attributes`, `Exemplars.FilteredAttributes`): one entry per pdata map
  entry, **sorted by key in byte order** (not pdata order), value =
  `pcommon.Value.AsString()`. The exporter builds each map with
  clickhouse-go's `orderedmap.CollectN`, which sorts the keys with
  `slices.SortFunc(..., cmp.Compare)`. ClickHouse compares Map values in
  order, so an unsorted map does not hash or `EXCEPT` equal. Duplicate keys,
  which only wire-decoded pdata can hold, stay in pdata order here; the
  exporter's unstable sort leaves them in an unspecified order. The contrib
  traces and logs tables sort the same way (`internal.AttributesToMap`),
  but the chDB exporter and parquetgo's traces/logs keep pdata order (see
  README, Metrics, "Gaps"). So Str is
  as-is; Int is decimal; Bool is `true`/`false`; Double is Go
  `strconv.FormatFloat(f, 'f', -1, 64)` for 0 and 1e-6 ≤ |f| < 1e21, and
  otherwise `AsString()`'s ES6-style rendering (`1e+21`, `1e-7`, `5e-324`,
  `NaN`, `Infinity`, `-Infinity`); Bytes is standard base64; Slice and Map
  are JSON of `AsRaw()` without HTML escaping; Empty is `""`.
- **`ServiceName`**: resource attribute `service.name` rendered with
  `AsString()`, or `""` when absent.
- **`ResourceSchemaUrl`**: `ResourceMetrics.SchemaUrl`. **`ScopeSchemaUrl`**:
  `ScopeMetrics.SchemaUrl`. **`ScopeName` / `ScopeVersion` /
  `ScopeDroppedAttrCount`**: from the InstrumentationScope.
- **`MetricName` / `MetricDescription` / `MetricUnit`**: `Metric.Name()`,
  `Description()` and `Unit()`.
- **Number values** (gauge and sum `Value`, `Exemplars.Value`): Double as-is;
  Int converted with `float64(int64)` (round to nearest even, so
  2⁵³+1 → 2⁵³); Empty (unset) → `0.0`.
- **`Flags`**: `uint32(dp.Flags())`, the raw bits.
- **`Sum`, `Min`, `Max`** (histograms and summary): `dp.Sum()`, `dp.Min()`
  and `dp.Max()`, which are `0` when the optional field is unset. There is
  no NULL.
- **`AggregationTemporality`**: `int32` of the enum: 0 Unspecified,
  1 Delta, 2 Cumulative. **`IsMonotonic`**: bool.
- **Exemplar ids**: `Exemplars.TraceId` / `Exemplars.SpanId` are **always**
  lowercase hex, 32 and 16 characters. An all-zero id is
  `"00000000000000000000000000000000"`, **not** `""`. That differs from
  the traces/logs tables, where a zero id is `""`.
- **Buckets**: `BucketCounts`, `ExplicitBounds`, `PositiveBucketCounts` and
  `NegativeBucketCounts` are copied element by element. They are not
  validated, so an explicit-bounds histogram with len(counts) ≠
  len(bounds) + 1 is stored as given. `Scale`, `ZeroCount`, `PositiveOffset`
  and `NegativeOffset` are copied.
- **Quantiles**: `ValueAtQuantiles.Quantile` / `.Value`, in pdata order.
- **Envelope**, the same as traces/logs: `producer_id`, `producer_epoch`
  (the log epoch in the manifest-less design), `batch_id` (the per-signal
  sequence, or the slot), `row_ordinal` (0-based row index **within this
  object**), `received_at` (ns, one value for every object of a request) and
  `schema_version`. The manifest's min/max event time come from the data
  points' `TimeUnix` in nanoseconds, with 0 ignored as in traces/logs.

## Column lists

`C` is the 14 columns every table starts with, `X` is the five exemplar
columns, and `E` is the envelope.

**C** (common)

| # | column | CH type | tag |
| --- | --- | --- | --- |
| 1 | ResourceAttributes | Map(LowCardinality(String), String) | map |
| 2 | ResourceSchemaUrl | String | str |
| 3 | ScopeName | String | str |
| 4 | ScopeVersion | String | str |
| 5 | ScopeAttributes | Map(LowCardinality(String), String) | map |
| 6 | ScopeDroppedAttrCount | UInt32 | u32 |
| 7 | ScopeSchemaUrl | String | str |
| 8 | ServiceName | LowCardinality(String) | str |
| 9 | MetricName | LowCardinality(String) | str |
| 10 | MetricDescription | String | str |
| 11 | MetricUnit | String | str |
| 12 | Attributes | Map(LowCardinality(String), String) | map |
| 13 | StartTimeUnix | DateTime | dt |
| 14 | TimeUnix | DateTime | dt |

**X** (exemplars: `Exemplars Nested(...)`)

| column | CH type | tag |
| --- | --- | --- |
| Exemplars.FilteredAttributes | Array(Map(LowCardinality(String), String)) | list<map> |
| Exemplars.TimeUnix | Array(DateTime) | list<dt> |
| Exemplars.Value | Array(Float64) | list<f64> |
| Exemplars.SpanId | Array(String) | list<str> |
| Exemplars.TraceId | Array(String) | list<str> |

**E** (envelope, appended to every table)

| column | CH type (in s3() structure) | tag |
| --- | --- | --- |
| producer_id | String | str |
| producer_epoch | String | str |
| batch_id | UInt64 | u64 |
| row_ordinal | UInt32 | u32 |
| received_at | DateTime64(9) | ts9 |
| schema_version | UInt16 | u16 |

**otel_metrics_gauge** (`metrics_gauge`): C, then

| column | CH type | tag |
| --- | --- | --- |
| Value | Float64 | f64 |
| Flags | UInt32 | u32 |

then X, then E. 26 columns.

**otel_metrics_sum** (`metrics_sum`): C, `Value` f64, `Flags` u32, X, then

| column | CH type | tag |
| --- | --- | --- |
| AggregationTemporality | Int32 | i32 |
| IsMonotonic | Bool | bool |

then E. 28 columns.

**otel_metrics_histogram** (`metrics_histogram`): C, then

| column | CH type | tag |
| --- | --- | --- |
| Count | UInt64 | u64 |
| Sum | Float64 | f64 |
| BucketCounts | Array(UInt64) | list<u64> |
| ExplicitBounds | Array(Float64) | list<f64> |
| *X (5 columns)* | | |
| Flags | UInt32 | u32 |
| Min | Float64 | f64 |
| Max | Float64 | f64 |
| AggregationTemporality | Int32 | i32 |

then E. 33 columns.

**otel_metrics_exponential_histogram** (`metrics_exponential_histogram`): C, then

| column | CH type | tag |
| --- | --- | --- |
| Count | UInt64 | u64 |
| Sum | Float64 | f64 |
| Scale | Int32 | i32 |
| ZeroCount | UInt64 | u64 |
| PositiveOffset | Int32 | i32 |
| PositiveBucketCounts | Array(UInt64) | list<u64> |
| NegativeOffset | Int32 | i32 |
| NegativeBucketCounts | Array(UInt64) | list<u64> |
| *X (5 columns)* | | |
| Flags | UInt32 | u32 |
| Min | Float64 | f64 |
| Max | Float64 | f64 |
| AggregationTemporality | Int32 | i32 |

then E. 37 columns.

**otel_metrics_summary** (`metrics_summary`): C, then

| column | CH type | tag |
| --- | --- | --- |
| Count | UInt64 | u64 |
| Sum | Float64 | f64 |
| ValueAtQuantiles.Quantile | Array(Float64) | list<f64> |
| ValueAtQuantiles.Value | Array(Float64) | list<f64> |
| Flags | UInt32 | u32 |

then E. 25 columns (no exemplars).

## `s3()` structure strings

The same strings are in `compare/metrics_test.go` (`metricStructures`), and
the central `INSERT … SELECT` names the same columns:

```
common    = ResourceAttributes Map(String, String), ResourceSchemaUrl String, ScopeName String, ScopeVersion String,
            ScopeAttributes Map(String, String), ScopeDroppedAttrCount UInt32, ScopeSchemaUrl String, ServiceName String,
            MetricName String, MetricDescription String, MetricUnit String, Attributes Map(String, String),
            StartTimeUnix DateTime, TimeUnix DateTime
exemplars = `Exemplars.FilteredAttributes` Array(Map(String, String)), `Exemplars.TimeUnix` Array(DateTime),
            `Exemplars.Value` Array(Float64), `Exemplars.SpanId` Array(String), `Exemplars.TraceId` Array(String)
envelope  = producer_id String, producer_epoch String, batch_id UInt64, row_ordinal UInt32,
            received_at DateTime64(9), schema_version UInt16
```

With schema inference, ClickHouse infers `dt` columns as
`DateTime64(3, 'UTC')`. Inserting them into `DateTime` gives the same
seconds, because every value is a whole second in [0, 2³²).

## Encoding (not part of the contract, but what parquetgo does)

- zstd; dictionary on every column except `row_ordinal`; statistics and a
  page index; 1 MiB pages; V2 data pages. Bloom filters follow
  `Options.BloomFilters` (on by default, as for traces/logs).
- The output is deterministic: the same pdata and the same envelope give the
  same bytes (`TestMetricsDeterministic`).
