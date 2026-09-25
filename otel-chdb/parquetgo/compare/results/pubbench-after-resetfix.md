# pubbench traces/logs, parquet-go, after the Writer.Reset path fix (restoreColumnPaths)
load avg during runs: median 1.2, range 1.2–1.3
| dest | signal | impl | runs | ms/batch (median of per-run medians) | k rows/s | CPU ms/batch | Go allocs/batch | Go MB alloc/batch | peak RSS MB | ready ms | object KB |
|---|---|---|---|---|---|---|---|---|---|---|---|
| local | logs | parquet-go | 3 | 48.0 [47.5–51.7] | 201 [189–207] | 51 [48–52] | 27968 [27946–27985] | 4.5 [1.8–6.7] | 146 [130–181] | 33 [33–45] | 155 [155–155] |
| local | logs | parquet-go-par4 | 3 | 36.8 [36.6–37.4] | 268 [267–271] | 53 [53–54] | 27960 [27960–27961] | 2.4 [2.4–2.4] | 133 [130–136] | 40 [29–44] | 155 [155–155] |
| local | traces | parquet-go | 3 | 68.4 [66.5–69.4] | 140 [140–147] | 72 [68–72] | 610 [601–621] | 2.2 [0.8–3.6] | 113 [109–128] | 38 [35–41] | 265 [265–265] |
| local | traces | parquet-go-par4 | 3 | 51.1 [47.2–51.9] | 190 [185–210] | 78 [74–79] | 605 [605–610] | 0.2 [0.2–0.7] | 119 [116–121] | 32 [32–32] | 265 [265–265] |
