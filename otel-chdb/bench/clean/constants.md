| constant | current (calculator) | earlier like-for-like (loaded box) | clean, median [min–max] | n | Δ vs current | >15%? | source |
|---|---|---|---|---|---|---|---|
| Insert µs per span or log (`usRow`) | 5 | 5.00 | **3.43** [2.87–3.96] | 5 | -31% | **yes** | block 2: all-in CPU/row of 1-object 10k-row statements, traces·0.75 + logs·0.25 |
| Insert µs per point, series layout (`usPointB`) | 1.15 | 1.15 | **1.18** [1.01–1.36] | 5 | +2% | no | block 2: u_row, B points mix + series/120 |
| Insert µs per point, ClickStack tables (`usPointA`) | 7.85 | 7.85 | **4.47** [3.65–4.80] | 5 | -43% | **yes** | block 2: u_row, ClickStack tables mix |
| Fixed ms per inserted object (`fixedMs`) | 17 | 17.00 | **15.30** [11.62–18.56] | 5 | -10% | no | block 2: 1-object statements of 20 points, CPU − rows·u_row |
| Merge µs per span or log (`mergeRow`) | 11 | 11.00 | **10.06** [7.66–10.06] | 1 | -9% | no | block 3: traces·0.75 + logs·0.25 at 1e4 parts (range: the two projection estimates) |
| Merge µs per point, series layout (`mergePointB`) | 6.5 | 6.50 | **4.14** [4.01–4.17] | 1 | -36% | **yes** | block 3: B points mix at 1e4 parts (range: the two projection estimates) |
| Merge µs per point, ClickStack tables (`mergePointA`) | 30 | 30.00 | **19.45** [19.45–19.70] | 1 | -35% | **yes** | block 3: sum·0.7 + histogram·0.3 at 1e4 parts (range: the two projection estimates) |
| Stored B per point, series layout (`bPointB`) | 6.3 | 6.30 | **6.70** | 1 | +6% | no | block 4: B mix, no-replay pool, OPTIMIZE FINAL |
| Stored B per point, ClickStack tables (`bPointA`) | 26 | 26.00 | **26.37** | 1 | +1% | no | block 4: ClickStack mix, no-replay pool, OPTIMIZE FINAL |
| Stored B per series row (`bSeries`) | 40 | 39.60 | **38.44** | 1 | -4% | no | block 4: series table, 5 cycles |
| Edge µs/span, Go (`edgeGoSpan`) | 7.3 | 7.29 | **6.81** [6.33–6.89] | 5 | -7% | no | block 1: parquet-go, traces, s3 |
| Edge µs/log, Go (`edgeGoLog`) | 5.2 | 5.24 | **4.76** [4.66–4.97] | 5 | -8% | no | block 1: parquet-go, logs, s3 |
| Edge µs/span, Rust (`edgeRsSpan`) | 4.5 | 4.47 | **4.00** [3.96–4.07] | 5 | -11% | no | block 1: rust-direct, traces, s3 |
| Edge µs/log, Rust (`edgeRsLog`) | 3.3 | 3.26 | **2.94** [2.92–3.06] | 5 | -11% | no | block 1: rust-direct, logs, s3 |
| Edge µs/point, series layout (`edgePointB`) | 2 | 1.67 | **2.06** [1.98–2.15] | 5 | +3% | no | block 1: rust-pipeline-series_table, metrics fleet, s3 (the calculator's 2.0 is the Go prototype) |
| Edge µs/point, ClickStack, Go (`edgeGoPointA`) | 7.8 | 7.82 | **7.47** [7.21–7.77] | 5 | -4% | no | block 1: parquet-go per type, local, mix blend (earlier: the parquetgo results it was built from) |
| Edge µs/point, ClickStack, Rust (`edgeRsPointA`) | 5.5 | 4.65 | **6.20** [6.16–6.26] | 5 | +13% | no | block 1: rust-pipeline-clickstack_tables, metrics fleet, s3 |
| (block 2 fit: marginal µs/row, traces·0.75 + logs·0.25 (no fixed cost)) | | | 2.21 [1.88–2.38] | 5 | | | |
| (block 2 fit: F_stmt + F_obj, median over metrics tables) | | | 14.96 [13.15–17.39] | 5 | | | |
| (block 2 fit: F_stmt/32 + F_obj per object at 32 objects/statement) | | | 2.12 [1.80–2.24] | 5 | | | |
