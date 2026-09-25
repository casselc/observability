# Layout B wire encodings: before / after

load average at start: 3.84

## Parquet bytes per point

| data | encoding | total | number_points | histogram_points | exponential_histogram_points | summary_points | series |
|---|---|---|---|---|---|---|---|
| fleet | before | **20.15** | 9.10 | 8.23 | 1.81 | 0.94 | 0.08 |
| fleet | after | **18.13** | 8.59 | 7.22 | 1.43 | 0.82 | 0.07 |
| testgen | before | **20.77** | 4.26 | 3.61 | 7.66 | 4.63 | 0.62 |
| testgen | after | **19.01** | 4.15 | 3.17 | 6.61 | 4.47 | 0.60 |

## Edge flatten + encode CPU (A/B in one process)

```
fleet: before: PLAIN, page statistics: flatten+encode 2.026 µs/point (median of 7, [1.970–2.135]); 20.15 B/point (metrics_exponential_histogram_points 1.81, metrics_histogram_points 8.23, metrics_number_points 9.10, metrics_series 0.08, metrics_summary_points 0.94)
fleet: after: BYTE_STREAM_SPLIT, no statistics: flatten+encode 1.905 µs/point (median of 7, [1.802–2.009]); 18.13 B/point (metrics_exponential_histogram_points 1.43, metrics_histogram_points 7.22, metrics_number_points 8.59, metrics_series 0.07, metrics_summary_points 0.82)
testgen: before: PLAIN, page statistics: flatten+encode 3.204 µs/point (median of 15, [3.125–3.426]); 20.77 B/point (metrics_exponential_histogram_points 7.66, metrics_histogram_points 3.61, metrics_number_points 4.26, metrics_series 0.62, metrics_summary_points 4.63)
testgen: after: BYTE_STREAM_SPLIT, no statistics: flatten+encode 3.040 µs/point (median of 15, [2.918–3.517]); 19.01 B/point (metrics_exponential_histogram_points 6.61, metrics_histogram_points 3.17, metrics_number_points 4.15, metrics_series 0.60, metrics_summary_points 4.47)
```

## Central INSERT ... SELECT, the statement's own CPU (median of 11; [min])

| type | objects / statement | before µs/point | after µs/point | contents |
|---|---|---|---|---|
| number_points | 1 | 2.68 [2.48] | 2.69 [2.47] | equal |
| number_points | 32 | 1.20 [1.02] | 1.08 [0.97] | equal |
| histogram_points | 1 | 11.83 [10.33] | 11.35 [10.86] | equal |
| histogram_points | 32 | 3.64 [3.15] | 3.19 [3.04] | equal |
| exponential_histogram_points | 1 | 91.73 [81.94] | 88.10 [77.40] | equal |
| exponential_histogram_points | 32 | 15.94 [13.87] | 14.45 [13.36] | equal |
| summary_points | 1 | 82.05 [71.78] | 73.98 [69.97] | equal |
| summary_points | 32 | 11.60 [10.74] | 11.61 [10.61] | equal |

load average at end: 5.84

## The options that were measured and not taken

Per-object shape: on the fleet every series appears **exactly once** per
points object (distinct series_id / rows = 1.000; testgen 0.800), as it does
for any edge that flushes once per scrape interval. `series_id` is 8.0 B of
the fleet's 20.15 B/point.

### Option 1, no schema change (pyarrow re-encode of the Rust objects; its baseline 19.92 / 19.99 B/point)

| variant | fleet B/point | testgen B/point |
|---|---|---|
| baseline (pyarrow) | 19.92 | 19.99 |
| series_id DELTA_BINARY_PACKED, input order | 19.98 | – |
| sort by (series_id, TimeUnix), series_id DELTA, row_ordinal renumbered | 20.68 | 23.86 |
| sort by the table key (MetricName, ServiceName, series_id, TimeUnix), series_id DELTA | 20.10 | 23.49 |
| sort by (series_id, TimeUnix), series_id dictionary | 25.13 | – |

Sorting shrinks series_id (8.07 → 6.76 B/row: sorted random ids are ~51
bits apart) but scatters MetricName (0.07 → 0.67), ServiceName, MetricType
and the timestamps, which are dictionary runs in input order. A dictionary
can't help ids that don't repeat. Central, statement CPU, fleet number
points, table-key order: 1.32 → 1.24 µs/point at 32 objects per statement,
1 object and histograms within noise.

### Option 2, per-epoch ordinal (prototype)

Points carry `series_ord UInt32` (DELTA) + `series_epoch` instead of the id;
central resolves `(producer_id, series_epoch, series_ord)` against a
`series_ords` ReplacingMergeTree (ORDER BY producer_id, series_epoch,
series_ord; 10M rows = 200 other producers) inside the INSERT … SELECT:
`LEFT ANY JOIN (SELECT … FROM series_ords WHERE producer_id IN (…))`,
`if(series_ord = 0, series_id, m.series_id + throwIf(m.series_id = 0, …))`.
Resolved rows equal the current insert's (count + hash).

| | wire B/point, fleet / testgen | central statement CPU, 1 object (8k points) | 32 objects, µs/point |
|---|---|---|---|
| current (8-byte id) | 19.92 / 19.99 | 26.7 ms | 1.27 |
| ordinal + JOIN | **11.92 / 13.53** (no id at all: 11.84 / 13.43) | 38.3 ms (**+43%**) | 1.60 (**+26%**) |

A narrower right side (`(producer_id, series_epoch) IN (SELECT DISTINCT … FROM s3(…))`)
reads each object twice and cost more (+0.56 against +0.19 µs/point, SELECT only).

### Option 3, per encoding (Rust, fleet / testgen B/point; before 20.15 / 20.77)

| variant | fleet | testgen |
|---|---|---|
| BYTE_STREAM_SPLIT Value, Sum, Min, Max | 19.58 | 20.54 |
| BYTE_STREAM_SPLIT bucket-count lists | 19.52 | 20.12 |
| BYTE_STREAM_SPLIT Count, ZeroCount | 20.06 | 20.68 |
| BYTE_STREAM_SPLIT quantile and exemplar values | 20.14 | 22.86 |
| BYTE_STREAM_SPLIT StartTimeUnix, TimeUnix | 20.16 | 20.76 |
| DELTA_BINARY_PACKED bucket-count lists | 20.63 | 20.90 |
| DELTA_BINARY_PACKED Count, ZeroCount | 20.14 | 20.72 |
| statistics chunk (no page index) | 19.84 | 20.44 |
| statistics none | 19.58 | 20.16 |
| V2 data pages | 21.21 | 23.74 |
| zstd 1 / 6 / 9 / 12 (3 is the default) | 19.94 / 20.18 / 20.07 / 19.81 | 25.12 / 21.31 / 21.09 / 20.73 |
| **chosen: BSS on the first three rows + statistics none + no offset index** | **18.13** | **19.01** |

ClickHouse 26.10 reads the BYTE_STREAM_SPLIT INT64, DOUBLE and list-leaf
columns, without statistics or page index, identically with both Parquet
readers (`input_format_parquet_use_native_reader_v3` 1 and 0: count and
hash equal to the old objects, every type, fleet and testgen); the
metrics correctness run (views against contrib's rows, nasty data included)
is unchanged: 85 PASS, and the same expected rejection on via-OTAP nasty.
