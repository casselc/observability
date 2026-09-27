# Schema comparison, three ways: insert CPU, stored bytes, merge cost (scripts/schema_bench.py, scripts/schema_merge.py)

old = the pre-alignment DDL; full = ClickStack 2.39.1's (../sql/clickstack_full_*.sql); new = the consumer's, option 2 (ClickStack's without the idx_*_attr_key indexes); new_main = option 2's table without the rollup view. `full` and `new` include the key-value rollup view.

Shared ClickHouse 26.10.1.618 on :18123 (no query_log: CPU is the INSERT's own ProfileEvents from `clickhouse client --print-profile-events`), max_memory_usage 2 GB; loaded box (4 vCPU, other agents' builds and soaks; 1-min load averages below). One object per statement in the consumer's shape; 6 reps interleaved, the first dropped. Merge CPU: clickhouse-local on the same 20–21 parts per table (merges stopped while loading), OPTIMIZE FINAL's process CPU minus a baseline open, 4 reps.

## bench-v objects: 20 x 10k spans, 20 x 10k logs (4 resource + 5 span attributes; 2 resource + 3 log attributes)

```
variant reps | traces cpu us/row med [min-max] | logs cpu us/row | traces B/row (after OPTIMIZE) data+idx | logs B/row 
old 5 | 3.37 [3.31–3.90] | 3.06 [2.50–3.45] | 23.73 (idx 0.00) | 14.96 (idx 0.00) 
full 5 | 12.81 [8.35–15.29] | 7.48 [6.79–8.84] | 19.20 (idx 7.54) | 15.16 (idx 7.18) 
new 5 | 9.11 [6.92–10.21] | 7.12 [6.71–8.82] | 18.94 (idx 7.54) | 15.16 (idx 7.18) 
new_main 5 | 6.22 [5.79–7.78] | 4.98 [3.89–5.71] | 18.94 (idx 7.54) | 15.16 (idx 7.18) 
```
Insert runs at 1-min load 5.6–19.9.

| variant | reps | merge µs/span | merge µs/log | parts merged | load (1-min) |
|---|---:|---:|---:|---:|---|
| old | 4 | 2.40 [1.72–2.72] | 2.84 [2.23–3.44] | 20 / 20 | 5.7–6.6 |
| full_main | 4 | 5.40 [5.07–6.12] | 4.74 [4.04–5.21] | 20 / 20 | 6.0–6.5 |
| new_main | 4 | 4.85 [4.61–5.49] | 5.00 [4.31–5.51] | 20 / 20 | 5.6–7.4 |

## hdxgen objects: 21 x ~9.6k spans, 21 x ~3.2k logs (10 resource attributes, ~2.9 span / 1.5 log attributes)

```
variant reps | traces cpu us/row med [min-max] | logs cpu us/row | traces B/row (after OPTIMIZE) data+idx | logs B/row 
old 5 | 3.89 [3.28–4.59] | 7.20 [5.73–9.61] | 82.13 (idx 0.00) | 68.82 (idx 0.00) 
full 5 | 9.96 [8.02–13.46] | 25.20 [21.55–27.05] | 61.94 (idx 10.55) | 66.43 (idx 22.56) 
new 5 | 8.07 [7.05–9.64] | 21.06 [15.63–22.98] | 61.81 (idx 10.46) | 66.41 (idx 22.54) 
new_main 5 | 5.47 [5.02–7.61] | 12.45 [9.31–13.98] | 61.81 (idx 10.46) | 66.41 (idx 22.54) 
```
Insert runs at 1-min load 5.9–9.3.

| variant | reps | merge µs/span | merge µs/log | parts merged | load (1-min) |
|---|---:|---:|---:|---:|---|
| old | 4 | 2.10 [1.54–2.43] | 4.72 [2.46–5.43] | 21 / 21 | 4.7–6.6 |
| full_main | 4 | 5.14 [4.86–5.45] | 9.14 [8.73–10.16] | 21 / 21 | 4.5–6.6 |
| new_main | 4 | 4.77 [3.92–5.28] | 8.01 [6.67–9.35] | 21 / 21 | 4.2–6.0 |

An earlier pass of the same insert runs at a busier moment (load 6–50, `opt2-*`) gave bench-v old 3.62 / 3.35, full 9.89 / 8.71, new 7.69 / 6.55, new_main 6.13 / 5.20 µs per span / log; hdxgen old 3.87 / 7.62, full 11.78 / 23.39, new 7.84 / 23.19, new_main 6.39 / 11.71.

Stored bytes on the HyperDX databases (hdxgen 3 h + 3.0 M haystack rows per signal, after OPTIMIZE FINAL): spans old / full / new 28.4 / 16.7 / 16.6 B, logs 17.7 / 14.8 / 14.8 B. The mapKeys indexes are small (low-cardinality keys): dropping them saves 0.0–0.3 B/row.
