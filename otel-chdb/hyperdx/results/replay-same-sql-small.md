As replay-same-sql.md, on the small set only (60 k points, 3 h), 3 rounds,
before the big load; the two sides not interleaved.

| scenario | statements | views ms (sum) | stock ms (sum) | ratio | slowest views / stock ms | results differ | errors views / stock |
|---|---:|---:|---:|---:|---:|---:|---:|
| kubernetes | 7 | 240.8 | 125.1 | 1.92 | 122.4 / 65.9 | 0 | 0 / 0 |
| picker | 30 | 384.5 | 207.9 | 1.85 | 25.1 / 18.7 | 1 | 0 / 0 |
| picker-search | 21 | 251.7 | 133.2 | 1.89 | 22.5 / 15.7 | 1 | 0 / 0 |
| picker-catalog | 12 | 141.5 | 73.6 | 1.92 | 14.8 / 9.8 | 0 | 0 / 0 |
| m-gauge-avg | 22 | 275.6 | 143.3 | 1.92 | 21.5 / 17.1 | 1 | 0 / 0 |
| m-gauge-groupby-attr | 22 | 325.3 | 162.8 | 2.00 | 31.5 / 21.0 | 2 | 0 / 0 |
| m-gauge-groupby-res | 22 | 288.1 | 154.8 | 1.86 | 27.9 / 22.1 | 2 | 0 / 0 |
| m-gauge-where-res | 22 | 288.2 | 146.4 | 1.97 | 25.9 / 16.2 | 1 | 0 / 0 |
| m-gauge-where-res-sql | 22 | 285.4 | 157.6 | 1.81 | 30.4 / 26.0 | 2 | 0 / 0 |
| m-sum-cumulative | 22 | 394.3 | 183.2 | 2.15 | 53.0 / 36.1 | 0 | 0 / 0 |
| m-sum-cumulative-increase | 22 | 504.7 | 209.2 | 2.41 | 118.3 / 49.7 | 0 | 0 / 0 |
| m-sum-delta | 22 | 285.3 | 154.8 | 1.84 | 30.3 / 22.8 | 0 | 0 / 0 |
| m-sum-updown | 22 | 295.1 | 137.9 | 2.14 | 29.7 / 19.0 | 2 | 0 / 0 |
| m-hist-p95 | 22 | 338.9 | 176.5 | 1.92 | 62.1 / 42.5 | 0 | 0 / 0 |
| m-hist-count | 22 | 291.7 | 127.0 | 2.30 | 26.5 / 19.0 | 1 | 0 / 0 |
| m-exphist-p50 | 22 | 448.9 | 258.3 | 1.74 | 123.6 / 85.6 | 2 | 0 / 0 |
| m-summary | 20 | 194.3 | 88.9 | 2.19 | 13.2 / 5.7 | 0 | 0 / 0 |
