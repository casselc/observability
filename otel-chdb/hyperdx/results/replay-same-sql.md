Same SQL on both: every statement HyperDX sent for a scenario (either source)
replayed with the database swapped, 7 rounds interleaved, median server ms,
summed per scenario (`scripts/replay.py`, 6.04 M points, range 16:58–22:58).
"errors views" are HyperDX's `mergeTreeIndex()` reads from the stock run,
which a view cannot serve (HyperDX never sends them to a view). "results
differ" counts statements whose rows differ; all are explained in the README
(HyperDX's `last_value`, the alert queries' live edge). The alert task's
statements run under whatever tag is current; its untagged ones are the last row.

| scenario | statements | views ms (sum) | stock ms (sum) | ratio | slowest views / stock ms | results differ | errors views / stock |
|---|---:|---:|---:|---:|---:|---:|---:|
| big-picker | 26 | 1022.5 | 420.6 | 2.43 | 300.6 / 101.5 | 6 | 4 / 0 |
| big-picker-search | 8 | 220.7 | 87.8 | 2.51 | 57.5 / 14.6 | 0 | 0 / 0 |
| big-picker-catalog | 12 | 1081.2 | 489.3 | 2.21 | 406.4 / 156.4 | 0 | 0 / 0 |
| big-m-gauge-avg | 30 | 912.3 | 549.8 | 1.66 | 155.6 / 100.0 | 6 | 4 / 0 |
| big-m-gauge-groupby-attr | 26 | 1356.4 | 507.5 | 2.67 | 232.3 / 148.5 | 6 | 4 / 0 |
| big-m-gauge-groupby-res | 30 | 1083.3 | 745.3 | 1.45 | 165.4 / 154.6 | 6 | 4 / 0 |
| big-m-gauge-where-res | 26 | 766.9 | 238.9 | 3.21 | 158.8 / 28.3 | 6 | 4 / 0 |
| big-m-gauge-where-res-sql | 26 | 838.7 | 511.1 | 1.64 | 164.0 / 174.5 | 6 | 4 / 0 |
| big-m-sum-cumulative | 26 | 2234.3 | 703.1 | 3.18 | 466.8 / 232.2 | 4 | 4 / 0 |
| big-m-sum-cumulative-increase | 26 | 2827.2 | 939.0 | 3.01 | 796.3 / 362.0 | 4 | 4 / 0 |
| big-m-sum-delta | 26 | 736.7 | 322.8 | 2.28 | 154.0 / 68.5 | 4 | 4 / 0 |
| big-m-sum-updown | 26 | 696.4 | 299.5 | 2.33 | 142.2 / 58.2 | 6 | 4 / 0 |
| big-m-hist-p95 | 26 | 1275.8 | 750.0 | 1.70 | 351.6 / 256.6 | 6 | 4 / 0 |
| big-m-hist-count | 26 | 1048.4 | 498.8 | 2.10 | 225.9 / 125.3 | 4 | 4 / 0 |
| big-m-gauge-where-cluster | 26 | 819.5 | 564.8 | 1.45 | 153.8 / 107.7 | 6 | 4 / 0 |
| big-m-exphist-p50 | 26 | 946.7 | 574.2 | 1.65 | 197.6 / 194.3 | 6 | 4 / 0 |
| big-m-summary | 24 | 456.4 | 162.4 | 2.81 | 170.0 / 22.7 | 4 | 4 / 0 |
| (alert task, between scenarios) | 20 | 737.3 | 701.0 | 1.05 | 62.4 / 63.7 | 0 | 0 / 0 |
