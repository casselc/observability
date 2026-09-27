# Schema comparison: insert CPU, stored bytes, merge cost (scripts/schema_bench.py)

Loaded box (4 vCPU shared with other agents' builds and a replicated-central soak; load averages per run below), private ClickHouse 26.10.1.618 with query_log/part_log, 20 + 20 objects of 10k spans / 10k logs (otlpgen bench-v00..19: 4 resource and 5 span attributes per span, 2 resource and 3 log attributes per log), one object per statement in the consumer's statement shape. CPU is the INSERT's OSCPUVirtualTimeMicroseconds / rows; bytes are bytes_on_disk / rows after OPTIMIZE FINAL (idx: the skip indexes' share); merge is the OPTIMIZE FINAL's part_log CPU / rows (20 parts -> 1). The first rep of each variant is dropped as warm-up.

## old vs new (7 reps, interleaved)

```
variant reps | traces cpu us/row med [min-max] | logs cpu us/row | traces B/row (after OPTIMIZE) data+idx | logs B/row | merge cpu us/row traces / logs (20->1) | load
old 7 | 3.52 [3.05–4.06] | 3.10 [2.97–3.34] | 23.73 (idx 0.00) | 14.96 (idx 0.00) | 2.61 [2.30–2.94] / 3.20 [2.98–3.40] merges [2, 2, 2, 2, 2, 2, 2] | 1.66-2.22 | rollup bytes {}
new_main 7 | 13.62 [12.88–15.00] | 6.96 [6.48–7.07] | 19.20 (idx 7.54) | 15.16 (idx 7.18) | 5.38 [5.00–5.74] / 5.32 [4.93–5.65] merges [2, 2, 2, 2, 2, 2, 2] | 1.61-2.23 | rollup bytes {}
new 7 | 15.17 [13.89–16.88] | 9.47 [9.05–10.13] | 19.20 (idx 7.54) | 15.16 (idx 7.18) | 4.89 [4.36–5.05] / 4.79 [4.54–5.10] merges [2, 2, 2, 2, 2, 2, 2] | 1.72-2.26 | rollup bytes {'otel_traces_kv_rollup_15m': 2371, 'otel_logs_kv_rollup_15m': 2529}
new_stockpart 7 | 15.92 [14.99–16.72] | 9.78 [8.86–10.29] | 19.20 (idx 7.54) | 15.16 (idx 7.18) | 5.10 [4.70–5.28] / 4.74 [4.60–5.00] merges [2, 2, 2, 2, 2, 2, 2] | 1.72-2.18 | rollup bytes {'otel_traces_kv_rollup_15m': 2371, 'otel_logs_kv_rollup_15m': 2529}
old {}
new_main {'otel_logs': {'idx_trace_id': 1.78, 'idx_res_attr_key': 0.0, 'idx_res_attr_items': 0.0, 'idx_scope_attr_key': 0.0, 'idx_scope_attr_items': 0.0, 'idx_log_attr_key': 0.0, 'idx_log_attr_items': 1.75, 'idx_lower_body': 3.65}, 'otel_traces': {'idx_trace_id': 3.49, 'idx_rum_session_id': 0.0, 'idx_res_attr_key': 0.0, 'idx_res_attr_items': 0.0, 'idx_span_attr_key': 0.0, 'idx_span_attr_items': 4.04, 'idx_duration': 0.0, 'idx_lower_span_name': 0.0}}
new {'otel_logs': {'idx_trace_id': 1.78, 'idx_res_attr_key': 0.0, 'idx_res_attr_items': 0.0, 'idx_scope_attr_key': 0.0, 'idx_scope_attr_items': 0.0, 'idx_log_attr_key': 0.0, 'idx_log_attr_items': 1.75, 'idx_lower_body': 3.65}, 'otel_traces': {'idx_trace_id': 3.49, 'idx_rum_session_id': 0.0, 'idx_res_attr_key': 0.0, 'idx_res_attr_items': 0.0, 'idx_span_attr_key': 0.0, 'idx_span_attr_items': 4.04, 'idx_duration': 0.0, 'idx_lower_span_name': 0.0}}
new_stockpart {'otel_logs': {'idx_trace_id': 1.78, 'idx_res_attr_key': 0.0, 'idx_res_attr_items': 0.0, 'idx_scope_attr_key': 0.0, 'idx_scope_attr_items': 0.0, 'idx_log_attr_key': 0.0, 'idx_log_attr_items': 1.75, 'idx_lower_body': 3.65}, 'otel_traces': {'idx_trace_id': 3.49, 'idx_rum_session_id': 0.0, 'idx_res_attr_key': 0.0, 'idx_res_attr_items': 0.0, 'idx_span_attr_key': 0.0, 'idx_span_attr_items': 4.04, 'idx_duration': 0.0, 'idx_lower_span_name': 0.0}}
```

## ablation: new_main without some text indexes / codecs (3 reps)

```
variant reps | traces cpu us/row med [min-max] | logs cpu us/row | traces B/row (after OPTIMIZE) data+idx | logs B/row | merge cpu us/row traces / logs (20->1) | load
new_main 3 | 13.56 [9.83–13.74] | 6.10 [4.00–6.21] | 19.20 (idx 7.54) | 15.16 (idx 7.18) | 5.32 [4.22–5.46] / 5.37 [4.57–5.42] merges [2, 2, 2] | 1.38-2.35 | rollup bytes {}
abl_noidx 3 | 3.96 [3.37–4.15] | 3.63 [2.62–3.72] | 11.40 (idx 0.00) | 7.98 (idx 0.00) | 4.03 [3.71–4.25] / 4.64 [4.40–5.10] merges [2, 2, 2] | 1.26-2.32 | rollup bytes {}
abl_notraceid 3 | 13.15 [7.69–13.33] | 6.19 [4.57–6.42] | 15.71 (idx 4.05) | 13.39 (idx 5.41) | 5.01 [4.43–5.19] / 5.08 [4.74–5.80] merges [2, 2, 2] | 1.24-2.61 | rollup bytes {}
abl_noitems 3 | 6.64 [6.52–6.67] | 5.42 [5.20–5.49] | 14.90 (idx 3.50) | 13.41 (idx 5.43) | 4.94 [4.44–5.09] / 5.30 [4.88–5.36] merges [2, 2, 2] | 1.22-2.31 | rollup bytes {}
abl_nokeys 3 | 7.04 [6.76–7.04] | 5.56 [5.53–5.77] | 18.94 (idx 7.54) | 15.16 (idx 7.18) | 5.34 [4.85–5.39] / 5.42 [5.17–5.80] merges [2, 2, 2] | 1.20-2.36 | rollup bytes {}
abl_nobody 3 | 13.58 [13.23–14.88] | 6.19 [6.11–6.25] | 19.19 (idx 7.54) | 11.51 (idx 3.53) | 5.18 [4.96–5.44] / 5.19 [4.22–5.29] merges [2, 2, 2] | 1.17-2.25 | rollup bytes {}
abl_noidx_nocodec 3 | 3.65 [3.35–4.14] | 3.27 [3.25–3.33] | 23.77 (idx 0.00) | 15.11 (idx 0.00) | 2.58 [2.44–2.87] / 3.65 [3.26–4.10] merges [2, 2, 2] | 1.16-2.29 | rollup bytes {}
old 3 | 3.33 [3.27–3.60] | 2.99 [2.20–3.22] | 23.73 (idx 0.00) | 14.96 (idx 0.00) | 2.62 [2.50–2.78] / 3.05 [2.87–3.06] merges [2, 2, 2] | 1.16-2.35 | rollup bytes {}
new_main {'otel_logs': {'idx_trace_id': 1.78, 'idx_res_attr_key': 0.0, 'idx_res_attr_items': 0.0, 'idx_scope_attr_key': 0.0, 'idx_scope_attr_items': 0.0, 'idx_log_attr_key': 0.0, 'idx_log_attr_items': 1.75, 'idx_lower_body': 3.65}, 'otel_traces': {'idx_trace_id': 3.49, 'idx_rum_session_id': 0.0, 'idx_res_attr_key': 0.0, 'idx_res_attr_items': 0.0, 'idx_span_attr_key': 0.0, 'idx_span_attr_items': 4.04, 'idx_duration': 0.0, 'idx_lower_span_name': 0.0}}
abl_noidx {'otel_traces': {'idx_duration': 0.0}}
abl_notraceid {'otel_logs': {'idx_res_attr_key': 0.0, 'idx_res_attr_items': 0.0, 'idx_scope_attr_key': 0.0, 'idx_scope_attr_items': 0.0, 'idx_log_attr_key': 0.0, 'idx_log_attr_items': 1.75, 'idx_lower_body': 3.65}, 'otel_traces': {'idx_rum_session_id': 0.0, 'idx_res_attr_key': 0.0, 'idx_res_attr_items': 0.0, 'idx_span_attr_key': 0.0, 'idx_span_attr_items': 4.04, 'idx_duration': 0.0, 'idx_lower_span_name': 0.0}}
abl_noitems {'otel_logs': {'idx_trace_id': 1.78, 'idx_res_attr_key': 0.0, 'idx_scope_attr_key': 0.0, 'idx_log_attr_key': 0.0, 'idx_lower_body': 3.65}, 'otel_traces': {'idx_trace_id': 3.49, 'idx_rum_session_id': 0.0, 'idx_res_attr_key': 0.0, 'idx_span_attr_key': 0.0, 'idx_duration': 0.0, 'idx_lower_span_name': 0.0}}
abl_nokeys {'otel_logs': {'idx_trace_id': 1.78, 'idx_res_attr_items': 0.0, 'idx_scope_attr_items': 0.0, 'idx_log_attr_items': 1.75, 'idx_lower_body': 3.65}, 'otel_traces': {'idx_trace_id': 3.49, 'idx_rum_session_id': 0.0, 'idx_res_attr_items': 0.0, 'idx_span_attr_items': 4.04, 'idx_duration': 0.0, 'idx_lower_span_name': 0.0}}
abl_nobody {'otel_logs': {'idx_trace_id': 1.78, 'idx_res_attr_key': 0.0, 'idx_res_attr_items': 0.0, 'idx_scope_attr_key': 0.0, 'idx_scope_attr_items': 0.0, 'idx_log_attr_key': 0.0, 'idx_log_attr_items': 1.75}, 'otel_traces': {'idx_trace_id': 3.49, 'idx_res_attr_key': 0.0, 'idx_res_attr_items': 0.0, 'idx_span_attr_key': 0.0, 'idx_span_attr_items': 4.04, 'idx_duration': 0.0}}
abl_noidx_nocodec {'otel_traces': {'idx_duration': 0.0}}
old {}
```

## one text index at a time (2 reps)

```
variant reps | traces cpu us/row med [min-max] | logs cpu us/row | traces B/row (after OPTIMIZE) data+idx | logs B/row | merge cpu us/row traces / logs (20->1) | load
only_idx_trace_id 2 | 5.50 [5.42–5.58] | 4.14 [4.11–4.17] | 14.89 (idx 3.49) | 9.75 (idx 1.78) | 4.59 [4.31–4.86] / 4.84 [4.45–5.24] merges [2, 2] | 2.24-2.80 | rollup bytes {}
only_idx_res_attr_key 2 | 4.86 [4.85–4.86] | 4.03 [3.95–4.11] | 11.40 (idx 0.00) | 7.98 (idx 0.00) | 4.35 [4.02–4.69] / 4.65 [4.65–4.66] merges [2, 2] | 2.24-3.47 | rollup bytes {}
only_idx_res_attr_items 2 | 4.76 [4.67–4.85] | 3.87 [3.81–3.93] | 11.40 (idx 0.00) | 7.98 (idx 0.00) | 4.17 [4.15–4.18] / 4.91 [4.88–4.94] merges [2, 2] | 2.80-3.47 | rollup bytes {}
only_idx_span_attr_key 2 | 4.89 [4.55–5.23] | 3.53 [3.51–3.55] | 11.40 (idx 0.00) | 7.98 (idx 0.00) | 4.38 [4.33–4.43] / 4.75 [4.65–4.86] merges [2, 2] | 2.66-3.43 | rollup bytes {}
only_idx_span_attr_items 2 | 5.32 [5.30–5.34] | 3.57 [3.56–3.58] | 15.44 (idx 4.04) | 7.98 (idx 0.00) | 4.37 [4.19–4.55] / 4.51 [4.22–4.81] merges [2, 2] | 2.69-3.23 | rollup bytes {}
only_idx_lower_span_name 2 | 4.77 [4.75–4.79] | 3.64 [3.52–3.76] | 11.40 (idx 0.00) | 7.98 (idx 0.00) | 3.99 [3.82–4.15] / 4.61 [4.55–4.67] merges [2, 2] | 2.55-3.05 | rollup bytes {}
only_idx_log_attr_key 2 | 3.81 [3.28–4.34] | 4.21 [4.10–4.32] | 11.40 (idx 0.00) | 7.98 (idx 0.00) | 3.66 [3.50–3.81] / 5.16 [5.10–5.22] merges [2, 2] | 2.55-3.05 | rollup bytes {}
only_idx_log_attr_items 2 | 4.51 [4.43–4.59] | 4.08 [3.93–4.23] | 11.40 (idx 0.00) | 9.72 (idx 1.75) | 3.78 [3.74–3.82] / 4.83 [4.82–4.84] merges [2, 2] | 2.51-3.05 | rollup bytes {}
only_idx_lower_body 2 | 4.32 [4.12–4.52] | 4.26 [4.24–4.27] | 11.40 (idx 0.00) | 11.63 (idx 3.65) | 3.84 [3.80–3.89] / 4.43 [4.28–4.58] merges [2, 2] | 2.47-3.05 | rollup bytes {}
abl_noidx 2 | 4.01 [3.97–4.05] | 3.75 [3.70–3.80] | 11.40 (idx 0.00) | 7.98 (idx 0.00) | 4.26 [4.09–4.43] / 4.73 [4.65–4.81] merges [2, 2] | 2.35-2.96 | rollup bytes {}
only_idx_trace_id {'otel_logs': {'idx_trace_id': 1.78}, 'otel_traces': {'idx_trace_id': 3.49, 'idx_duration': 0.0}}
only_idx_res_attr_key {'otel_logs': {'idx_res_attr_key': 0.0}, 'otel_traces': {'idx_res_attr_key': 0.0, 'idx_duration': 0.0}}
only_idx_res_attr_items {'otel_logs': {'idx_res_attr_items': 0.0}, 'otel_traces': {'idx_res_attr_items': 0.0, 'idx_duration': 0.0}}
only_idx_span_attr_key {'otel_traces': {'idx_span_attr_key': 0.0, 'idx_duration': 0.0}}
only_idx_span_attr_items {'otel_traces': {'idx_span_attr_items': 4.04, 'idx_duration': 0.0}}
only_idx_lower_span_name {'otel_traces': {'idx_duration': 0.0, 'idx_lower_span_name': 0.0}}
only_idx_log_attr_key {'otel_logs': {'idx_log_attr_key': 0.0}, 'otel_traces': {'idx_duration': 0.0}}
only_idx_log_attr_items {'otel_logs': {'idx_log_attr_items': 1.75}, 'otel_traces': {'idx_duration': 0.0}}
only_idx_lower_body {'otel_logs': {'idx_lower_body': 3.65}, 'otel_traces': {'idx_duration': 0.0}}
abl_noidx {'otel_traces': {'idx_duration': 0.0}}
```

## consumer statements on hdxgen data (the HyperDX run's two consumers, same objects, 12 statements of ~45 objects each)

| side | signal | statements | rows | CPU ms per statement |
|---|---|---:|---:|---:|
| old | traces | 12 | 17,216 | 77.6 |
| new | traces | 12 | 17,216 | 109.5 |
| old | logs | 12 | 5,811 | 69.6 |
| new | logs | 12 | 5,811 | 105.6 |

hdxgen objects hold ~48 spans / ~16 logs each, so per-object costs dominate; the marginal difference is +22 us/span and +6 us/log (10 resource attributes per row). Stored after OPTIMIZE FINAL: traces 79.5 -> 57.9 B/span, logs 79.6 -> 80.0 B/log (text indexes 14.1 B/span, 29.6 B/log; 17k spans / 6k logs, so per-part overhead is included).
