CPU ms per request of 10k points (control: 10k spans), median [min–max] over repetitions (n per cell in brackets after).

| config | pre (8cf80ad) | head (d4bb951 + later) | head, no durable-buffer feature | head vs pre | earlier otap-rs (loaded box) | bench/clean block 1 |
|---|---|---|---|---|---|---|
| series-fleet | 19.5 [18.0–19.9] (n=5) | 18.2 [16.7–18.7] (n=5) | 18.4 [16.8–19.1] (n=5) | -6.4% | 17.6 | 20.8 |
| series-testgen | 25.1 [24.4–27.7] (n=5) | 25.1 [24.4–25.5] (n=5) | 25.5 [24.0–26.8] (n=5) | +0.0% | 24.5 | 29.9 |
| clickstack-fleet | 53.1 [51.1–57.4] (n=5) | 53.9 [52.1–54.7] (n=5) | 52.7 [51.3–54.9] (n=5) | +1.4% | 50.4 | 64.3 |
| clickstack-testgen | 51.3 [50.1–55.9] (n=5) | 50.7 [48.8–52.4] (n=5) | 52.0 [48.7–54.6] (n=5) | -1.1% | 48.7 | 59.9 |
| rust-pipeline-series_table | 18.7 [18.3–19.9] (n=5) | 18.2 [17.4–19.4] (n=5) | 17.5 [16.9–18.3] (n=5) | -2.5% | 16.7 | 20.6 |
| rust-pipeline-clickstack_tables | 50.5 [48.3–52.5] (n=5) | 50.5 [49.8–51.9] (n=5) | 50.3 [49.0–50.6] (n=5) | +0.0% | 46.5 | 62.0 |
| control-traces | 32.7 [30.8–34.8] (n=5) | 31.8 [31.6–32.2] (n=5) | 31.4 [31.0–32.4] (n=5) | -2.5% | 44.7 | 40.0 |

Phases (encbench), ms per request, median: flatten / encode / commit; bytes per request.

| config | variant | flatten | encode | commit | KB/request |
|---|---|---|---|---|---|
| series-fleet | pre | 6.2 | 11.8 | 11.1 | 202.6 |
| series-fleet | head | 6.3 | 10.5 | 11.6 | 181.7 |
| series-fleet | head-nodur | 6.5 | 10.5 | 10.8 | 181.7 |
| series-testgen | pre | 9.5 | 14.4 | 11.4 | 202.7 |
| series-testgen | head | 10.0 | 13.7 | 11.0 | 184.5 |
| series-testgen | head-nodur | 10.2 | 13.9 | 11.1 | 184.5 |
| clickstack-fleet | pre | 10.4 | 40.9 | 15.0 | 197.6 |
| clickstack-fleet | head | 10.3 | 41.7 | 15.9 | 197.6 |
| clickstack-fleet | head-nodur | 10.1 | 40.8 | 15.1 | 197.6 |
| clickstack-testgen | pre | 11.7 | 37.9 | 14.2 | 183.7 |
| clickstack-testgen | head | 11.5 | 38.0 | 14.6 | 183.7 |
| clickstack-testgen | head-nodur | 12.0 | 38.3 | 13.5 | 183.7 |
| control-traces | pre | 11.1 | 20.8 | 4.6 | 134.2 |
| control-traces | head | 11.0 | 20.3 | 4.4 | 134.2 |
| control-traces | head-nodur | 10.8 | 20.2 | 4.8 | 134.2 |

Environment: 15 gated runs; load at start max 0.45; steal per run median 0.18% / max 0.42%; flagged 0.
