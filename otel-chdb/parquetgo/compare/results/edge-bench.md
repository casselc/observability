# The Go edge per batch, old path (parquetgo.Publisher: object + manifest) against new (parquetgo/edge: manifest-less), 2026-09-26

results/runedge.sh, REPS=3, 30 batches after 3 warm-up, to SeaweedFS on localhost through a counting proxy. **Loaded box** (load average 5.3-5.9 on 4 vCPUs, other agents' work): CPU and allocation counts are steady, latencies are not.
Batches: 10,000 testgen spans or logs; metrics 2,000 points of each type (10,000 points; compare.Metrics). Each batch differs from the previous in one timestamp, for every impl.

load avg during runs: median 5.5, range 5.3–5.9
| dest | signal | impl | runs | ms/batch (median of per-run medians) | k rows/s | CPU ms/batch | Go allocs/batch | Go MB alloc/batch | peak RSS MB | ready ms | object KB |
|---|---|---|---|---|---|---|---|---|---|---|---|
| s3 | logs | edge | 3 | 32.6 [32.2–34.7] | 292 [275–293] | 29 [28–30] | 28672 [28664–28674] | 6.2 [5.4–6.2] | 144 [131–159] | 49 [47–50] | – |
| s3 | logs | parquet-go | 3 | 33.8 [33.2–33.8] | 280 [265–296] | 28 [26–30] | 29141 [29136–29143] | 4.7 [4.1–4.7] | 124 [116–124] | 42 [36–59] | – |
| s3 | metrics | edge-clickstack_tables | 3 | 63.1 [58.4–68.8] | 29 [27–32] | 87 [84–100] | 7776 [7738–7905] | 22.6 [22.4–24.4] | 512 [457–513] | 44 [40–47] | – |
| s3 | metrics | edge-series_table | 3 | 39.7 [37.9–40.7] | 49 [48–51] | 39 [37–39] | 5913 [5899–5932] | 9.5 [7.4–10.2] | 234 [232–254] | 47 [44–49] | – |
| s3 | metrics | parquet-go | 3 | 75.0 [74.8–75.6] | 26 [25–26] | 64 [62–65] | 9025 [9024–9029] | 2.1 [2.0–2.1] | 148 [145–149] | 44 [43–45] | – |
| s3 | traces | edge | 3 | 48.9 [44.7–51.0] | 199 [192–215] | 43 [39–44] | 1333 [1319–1337] | 6.5 [5.8–7.9] | 158 [158–166] | 44 [41–57] | – |
| s3 | traces | parquet-go | 3 | 48.0 [46.9–49.0] | 205 [201–208] | 40 [38–40] | 1780 [1779–1785] | 2.3 [1.7–2.4] | 90 [86–91] | 43 [41–60] | – |

S3 requests per batch (counting proxy): traces/logs 1 PUT (edge) against 2 (object + manifest); metrics 4 PUTs (edge, layout B: number, histogram, exponential histogram, summary points; the series object only when a series is new) or 5 (edge, ClickStack tables) against 10.
(The metrics rows' k rows/s count 2,000 rows per batch: multiply by 5 for points.)
