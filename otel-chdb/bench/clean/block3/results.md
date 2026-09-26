| table | rows/stmt | parts reached | insert µs/row | merge µs/ins. row at end | rewrites/row at end | slope, rewrites per decade (from) | µs per rewritten row, upper levels | merge µs/row at N=1e3 | **N=1e4** [other estimate] | N=1e5 | ×insert at 1e4 | earlier (loaded box) µs/row at 1e4, ×insert | Δ at 1e4 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| traces, random ids | 100,000 | 161 | 3.62 | 5.52 | 2.53 | 1.37 (slope of traces-100k-tg) | 2.10 | 7.8 | **10.7** [7.8] | 13.6 | 3.0× | 11.7 (2.2×) | -9% |
| traces, testgen ids | 100,000 | 1,066 | 3.30 | 6.03 | 3.27 | 1.37 (own) | 1.79 | 6.0 | **8.4** [8.3] | 10.9 | 2.5× | 12.2 (2.6×) | -31% |
| logs | 100,000 | 536 | 2.52 | 5.79 | 3.51 | 1.26 (own) | 1.49 | 6.3 | **8.2** [7.2] | 10.1 | 3.2× | 9.7 (2.6×) | -16% |
| series layout: number points (gauge+sum) | 80,000 | 2,100 | 0.83 | 3.03 | 5.21 | 1.68 (own) | 0.54 | 3.0 | **3.6** [3.6] | 4.6 | 4.4× | 5.9 (5.0×) | -38% |
| series layout: histogram points | 96,000 | 1,496 | 1.61 | 4.10 | 4.09 | 1.26 (own) | 0.91 | 4.1 | **5.0** [4.4] | 6.2 | 3.1× | 7.4 (3.1×) | -32% |
| series layout: exp. histogram points | 100,000 | 458 | 3.52 | 4.88 | 2.48 | 1.27 (own) | 1.84 | 5.7 | **8.0** [8.6] | 10.3 | 2.3× | 13.5 (2.6×) | -41% |
| series layout: summary points | 100,000 | 1,800 | 1.61 | 2.86 | 3.92 | 1.37 (own) | 0.68 | 2.9 | **3.5** [3.6] | 4.5 | 2.2× | 8.1 (3.2×) | -56% |
| series layout: series table | 100,000 | 600 | 8.24 | 1.95 | 1.20 | 0.06 (own) | 1.62 | 2.0 | **2.1** [2.1] | 2.2 | 0.2× | – (not projected) |  |
| ClickStack otel_metrics_sum | 104,000 | 491 | 5.83 | 11.09 | 2.58 | 1.26 (own) | 4.22 | 12.7 | **18.1** [18.2] | 23.4 | 3.1× | – (not projected) |  |
| ClickStack otel_metrics_histogram | 96,000 | 314 | 7.75 | 12.53 | 2.50 | 1.40 (own) | 4.84 | 15.9 | **22.7** [23.1] | 29.5 | 2.9× | 29.3 (2.7×) | -23% |

| calculator constant | earlier | clean, N=1e4 [range: the two estimates] | Δ |
|---|---|---|---|
| mergeRow | 11.0 | 10.1 [7.7–10.1] | -9% |
| mergePointB | 6.5 | 4.1 [4.0–4.2] | -36% |
| mergePointA | 30.0 | 19.4 [19.4–19.7] | -35% |
