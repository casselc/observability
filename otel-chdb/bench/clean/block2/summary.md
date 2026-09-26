| table | marginal µs/row, clean | earlier µs/row | fixed ms per 1-object statement (F_stmt + F_obj), clean | earlier ms | fixed ms per object at 32 per statement (F_stmt/32 + F_obj) |
|---|---|---|---|---|---|
| otel_logs | 1.93 [1.48–1.97] | – | 14.2 [10.8–15.8] | – | 2.02 [1.81–2.33] |
| otel_metrics_exponential_histogram | 7.87 [5.28–8.98] | 7.85 (blend) | 17.4 [15.0–19.0] | 18.3 (A) | 2.30 [1.93–2.44] |
| otel_metrics_exponential_histogram_points | 3.26 [2.39–4.76] | 2.7 | 17.7 [15.6–19.9] | 16.3 (B) | 1.95 [1.79–2.21] |
| otel_metrics_gauge | 4.00 [3.08–4.30] | 7.85 (blend) | 19.9 [9.2–47.8] | 18.3 (A) | 2.25 [1.68–2.41] |
| otel_metrics_histogram | 5.12 [4.14–8.56] | 7.85 (blend) | 23.8 [-14.0–28.8] | 18.3 (A) | 2.25 [1.93–2.46] |
| otel_metrics_histogram_points | 1.54 [1.25–1.57] | 1.8 | 15.4 [13.6–18.5] | 16.3 (B) | 1.97 [1.69–2.00] |
| otel_metrics_number_points | 0.79 [0.69–0.94] | 0.72 sum / 0.64 gauge | 12.9 [9.8–17.4] | 16.3 (B) | 1.95 [1.73–2.15] |
| otel_metrics_series | 11.09 [9.70–15.48] | 13.2 (0.11 per point at 1/120) | 14.6 [10.0–16.4] | 16.3 (B) | 1.59 [1.31–1.72] |
| otel_metrics_sum | 4.25 [3.21–4.43] | 7.85 (blend) | 3.3 [-2.7–6.0] | 18.3 (A) | 2.28 [2.01–2.41] |
| otel_metrics_summary | 3.93 [3.00–5.40] | 7.85 (blend) | 14.1 [12.7–17.1] | 18.3 (A) | 2.17 [1.85–2.28] |
| otel_metrics_summary_points | 1.14 [0.81–1.40] | 1.3 | 14.1 [11.4–15.3] | 16.3 (B) | 1.87 [1.55–1.91] |
| otel_traces | 2.29 [2.02–2.53] | ~5 all-in | 14.3 [10.9–16.9] | – | 2.33 [1.76–2.39] |
