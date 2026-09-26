| dest | signal | impl | n | clean CPU ms/10k, median [min–max] | earlier, otap-rs results | Δ | earlier, parquetgo results | Δ | object KB (clean) |
|---|---|---|---|---|---|---|---|---|---|
| local | logs | parquet-go | 5 | 44.2 [41.2–46.8] | 49.6 (n=3) | -11% | 47.4 (n=3) | -7% | 159 |
| local | logs | parquet-go-nobloom | 5 | 43.8 [42.9–44.3] | 46.9 (n=3) | -7% | – |  | 99 |
| local | logs | rust-direct | 5 | 28.7 [28.0–30.2] | 29.5 (n=3) | -3% | – |  | 78 |
| local | logs | rust-direct-allbloom | 5 | 31.1 [30.2–32.4] | 34.2 (n=3) | -9% | – |  | 124 |
| local | logs | rust-direct-arrow | 5 | 12.1 [11.4–12.9] | 14.1 (n=3) | -14% | – |  | 677 |
| local | logs | rust-direct-nobloom | 5 | 27.9 [27.4–29.9] | 28.8 (n=3) | -3% | – |  | 70 |
| local | logs | rust-direct-zstd1 | 5 | 25.8 [25.3–27.6] | 29.4 (n=3) | -12% | – |  | 76 |
| local | logs | rust-via-otap | 5 | 42.3 [39.2–44.4] | 44.6 (n=3) | -5% | – |  | 78 |
| local | metrics | parquet-go | 5 | 88.6 [86.0–91.3] | 134.2 (n=3) | -34% | – |  | 336 |
| local | metrics | parquet-go-nobloom | 5 | 87.3 [85.1–89.4] | 131.9 (n=3) | -34% | – |  | 298 |
| local | metrics | rust-direct | 5 | 57.2 [56.1–58.9] | 59.6 (n=3) | -4% | – |  | 183 |
| local | metrics | rust-otap-input | 5 | 58.3 [56.3–61.1] | 62.2 (n=3) | -6% | – |  | 183 |
| local | metrics | rust-via-otap | 5 | 76.0 [73.5–78.9] | 83.1 (n=3) | -9% | – |  | 183 |
| local | metrics_exponential_histogram | parquet-go | 5 | 109.6 [105.4–115.0] | 140.5 (n=3) | -22% | 111.2 (n=3) | -1% | 439 |
| local | metrics_exponential_histogram | parquet-go-nobloom | 5 | 114.4 [101.0–121.5] | 140.6 (n=3) | -19% | – |  | 404 |
| local | metrics_exponential_histogram | rust-direct | 5 | 61.1 [59.3–62.5] | 63.9 (n=3) | -4% | – |  | 267 |
| local | metrics_exponential_histogram | rust-otap-input | 5 | 64.1 [62.8–67.9] | 67.4 (n=3) | -5% | – |  | 267 |
| local | metrics_exponential_histogram | rust-via-otap | 5 | 83.0 [81.1–85.8] | 92.3 (n=3) | -10% | – |  | 267 |
| local | metrics_gauge | parquet-go | 5 | 66.2 [65.5–69.3] | 87.9 (n=3) | -25% | 69.5 (n=3) | -5% | 216 |
| local | metrics_gauge | parquet-go-nobloom | 5 | 65.2 [61.9–66.4] | 89.6 (n=3) | -27% | – |  | 187 |
| local | metrics_gauge | rust-direct | 5 | 35.0 [33.6–38.1] | 37.2 (n=3) | -6% | – |  | 89 |
| local | metrics_gauge | rust-otap-input | 5 | 40.9 [39.9–42.7] | 43.3 (n=3) | -6% | – |  | 89 |
| local | metrics_gauge | rust-via-otap | 5 | 52.2 [49.6–54.2] | 55.1 (n=3) | -5% | – |  | 89 |
| local | metrics_histogram | parquet-go | 5 | 97.6 [94.9–102.2] | 125.1 (n=3) | -22% | 101.6 (n=3) | -4% | 287 |
| local | metrics_histogram | parquet-go-nobloom | 5 | 99.9 [96.2–101.8] | 131.9 (n=3) | -24% | – |  | 258 |
| local | metrics_histogram | rust-direct | 5 | 57.5 [54.6–60.6] | 63.0 (n=3) | -9% | – |  | 120 |
| local | metrics_histogram | rust-otap-input | 5 | 63.4 [62.5–70.8] | 69.1 (n=3) | -8% | – |  | 120 |
| local | metrics_histogram | rust-via-otap | 5 | 86.4 [80.7–88.3] | 91.5 (n=3) | -6% | – |  | 120 |
| local | metrics_sum | parquet-go | 5 | 67.3 [63.4–69.1] | 87.4 (n=3) | -23% | 70.7 (n=3) | -5% | 129 |
| local | metrics_sum | parquet-go-nobloom | 5 | 65.3 [64.1–69.5] | 89.7 (n=3) | -27% | – |  | 114 |
| local | metrics_sum | rust-direct | 5 | 35.6 [34.6–36.4] | 37.6 (n=3) | -5% | – |  | 36 |
| local | metrics_sum | rust-otap-input | 5 | 41.5 [40.0–41.8] | 44.6 (n=3) | -7% | – |  | 36 |
| local | metrics_sum | rust-via-otap | 5 | 53.3 [51.6–54.8] | 55.4 (n=3) | -4% | – |  | 36 |
| local | metrics_summary | parquet-go | 5 | 58.0 [56.7–60.8] | 81.8 (n=3) | -29% | 64.4 (n=3) | -10% | 253 |
| local | metrics_summary | parquet-go-nobloom | 5 | 57.4 [56.7–62.6] | 84.1 (n=3) | -32% | – |  | 230 |
| local | metrics_summary | rust-direct | 5 | 43.3 [40.5–44.3] | 47.0 (n=3) | -8% | – |  | 134 |
| local | metrics_summary | rust-otap-input | 5 | 45.1 [42.4–47.0] | 48.7 (n=3) | -7% | – |  | 134 |
| local | metrics_summary | rust-via-otap | 5 | 61.3 [60.2–65.9] | 67.3 (n=3) | -9% | – |  | 134 |
| local | traces | parquet-go | 5 | 63.6 [61.3–65.1] | 67.7 (n=3) | -6% | 68.2 (n=3) | -7% | 271 |
| local | traces | parquet-go-nobloom | 5 | 62.4 [61.2–64.2] | 66.7 (n=3) | -7% | – |  | 178 |
| local | traces | rust-direct | 5 | 40.1 [39.1–40.6] | 43.9 (n=3) | -8% | – |  | 134 |
| local | traces | rust-direct-allbloom | 5 | 43.2 [41.4–43.7] | 47.4 (n=3) | -9% | – |  | 201 |
| local | traces | rust-direct-arrow | 5 | 21.5 [20.7–25.3] | 28.0 (n=3) | -23% | – |  | 1045 |
| local | traces | rust-direct-nobloom | 5 | 39.0 [37.4–39.4] | 43.5 (n=3) | -10% | – |  | 118 |
| local | traces | rust-direct-zstd1 | 5 | 38.5 [36.0–39.7] | 43.3 (n=3) | -11% | – |  | 136 |
| local | traces | rust-via-otap | 5 | 64.3 [61.5–65.3] | 68.5 (n=3) | -6% | – |  | 134 |
| s3 | logs | parquet-go | 5 | 47.6 [46.6–49.7] | 52.4 (n=3) | -9% | 52.5 (n=3) | -9% | – |
| s3 | logs | parquet-go-nobloom | 5 | 45.8 [44.6–47.3] | 49.8 (n=3) | -8% | – |  | – |
| s3 | logs | rust-direct | 5 | 29.4 [29.2–30.6] | 32.6 (n=3) | -10% | – |  | 78 |
| s3 | logs | rust-direct-allbloom | 5 | 32.7 [32.2–33.4] | 35.5 (n=3) | -8% | – |  | 124 |
| s3 | logs | rust-direct-nobloom | 5 | 29.0 [28.7–29.7] | 32.8 (n=3) | -12% | – |  | 70 |
| s3 | logs | rust-pipeline-direct | 5 | 29.7 [29.3–30.7] | 33.7 (n=3) | -12% | – |  | – |
| s3 | logs | rust-pipeline-via_otap | 5 | 44.0 [42.7–46.3] | 47.0 (n=3) | -6% | – |  | – |
| s3 | logs | rust-via-otap | 5 | 43.3 [42.3–44.2] | 46.8 (n=3) | -8% | – |  | 78 |
| s3 | metrics | clickstack-fleet | 5 | 64.3 [63.0–70.2] | 50.4 (n=3) | +28% | – |  | 203 |
| s3 | metrics | clickstack-testgen | 5 | 59.9 [58.3–60.2] | 48.7 (n=3) | +23% | – |  | 183 |
| s3 | metrics | parquet-go | 5 | 97.1 [92.5–99.2] | 139.7 (n=3) | -30% | – |  | – |
| s3 | metrics | parquet-go-nobloom | 5 | 92.6 [91.9–94.0] | 139.5 (n=3) | -34% | – |  | – |
| s3 | metrics | rust-direct | 5 | 60.1 [57.0–62.6] | 67.6 (n=3) | -11% | – |  | 183 |
| s3 | metrics | rust-otap-input | 5 | 61.1 [59.3–65.4] | 67.7 (n=3) | -10% | – |  | 183 |
| s3 | metrics | rust-via-otap | 5 | 79.4 [77.0–84.0] | 85.5 (n=3) | -7% | – |  | 183 |
| s3 | metrics | series-fleet | 5 | 20.8 [19.8–23.6] | 17.6 (n=3) | +18% | – |  | 185 |
| s3 | metrics | series-testgen | 5 | 29.9 [29.3–31.2] | 24.5 (n=3) | +22% | – |  | 184 |
| s3 | metrics (30 single-type requests) | rust-pipeline-direct | 5 | 51.0 [50.3–55.3] | 53.0 (n=3) | -4% | – |  | – |
| s3 | metrics fleet | rust-pipeline-clickstack_tables | 5 | 62.0 [61.6–62.6] | 46.5 (n=3) | +33% | – |  | – |
| s3 | metrics fleet | rust-pipeline-series_table | 5 | 20.6 [19.8–21.5] | 16.7 (n=3) | +23% | – |  | – |
| s3 | metrics_exponential_histogram | parquet-go | 5 | 109.0 [106.2–112.1] | 142.0 (n=3) | -23% | 111.6 (n=3) | -2% | – |
| s3 | metrics_exponential_histogram | parquet-go-nobloom | 5 | 111.4 [106.9–112.7] | 142.8 (n=3) | -22% | – |  | – |
| s3 | metrics_exponential_histogram | rust-direct | 5 | 60.8 [60.0–62.2] | 67.7 (n=3) | -10% | – |  | 267 |
| s3 | metrics_exponential_histogram | rust-otap-input | 5 | 67.1 [64.1–68.5] | 68.6 (n=3) | -2% | – |  | 267 |
| s3 | metrics_exponential_histogram | rust-via-otap | 5 | 85.9 [83.3–88.4] | 92.3 (n=3) | -7% | – |  | 267 |
| s3 | metrics_gauge | parquet-go | 5 | 68.5 [65.6–71.5] | 91.1 (n=3) | -25% | 72.9 (n=3) | -6% | – |
| s3 | metrics_gauge | parquet-go-nobloom | 5 | 67.9 [65.7–69.4] | 92.1 (n=3) | -26% | – |  | – |
| s3 | metrics_gauge | rust-direct | 5 | 35.5 [33.8–36.4] | 38.5 (n=3) | -8% | – |  | 89 |
| s3 | metrics_gauge | rust-otap-input | 5 | 41.8 [40.8–42.9] | 45.5 (n=3) | -8% | – |  | 89 |
| s3 | metrics_gauge | rust-via-otap | 5 | 54.0 [51.7–54.6] | 57.6 (n=3) | -6% | – |  | 89 |
| s3 | metrics_histogram | parquet-go | 5 | 101.6 [100.0–103.0] | 134.4 (n=3) | -24% | 105.7 (n=3) | -4% | – |
| s3 | metrics_histogram | parquet-go-nobloom | 5 | 103.6 [99.5–104.6] | 131.7 (n=3) | -21% | – |  | – |
| s3 | metrics_histogram | rust-direct | 5 | 59.7 [58.6–62.6] | 66.7 (n=3) | -11% | – |  | 120 |
| s3 | metrics_histogram | rust-otap-input | 5 | 67.9 [63.7–72.3] | 72.6 (n=3) | -7% | – |  | 120 |
| s3 | metrics_histogram | rust-via-otap | 5 | 86.7 [81.4–89.0] | 94.2 (n=3) | -8% | – |  | 120 |
| s3 | metrics_sum | parquet-go | 5 | 69.5 [64.9–70.1] | 92.1 (n=3) | -25% | 71.2 (n=3) | -2% | – |
| s3 | metrics_sum | parquet-go-nobloom | 5 | 68.9 [68.3–72.3] | 89.7 (n=3) | -23% | – |  | – |
| s3 | metrics_sum | rust-direct | 5 | 36.7 [34.8–37.3] | 39.7 (n=3) | -7% | – |  | 36 |
| s3 | metrics_sum | rust-otap-input | 5 | 39.6 [39.4–43.0] | 43.5 (n=3) | -9% | – |  | 36 |
| s3 | metrics_sum | rust-via-otap | 5 | 51.8 [51.1–55.5] | 58.0 (n=3) | -11% | – |  | 36 |
| s3 | metrics_summary | parquet-go | 5 | 61.4 [59.6–62.3] | 87.4 (n=3) | -30% | 66.2 (n=3) | -7% | – |
| s3 | metrics_summary | parquet-go-nobloom | 5 | 59.7 [59.2–61.6] | 87.3 (n=3) | -32% | – |  | – |
| s3 | metrics_summary | rust-direct | 5 | 45.4 [44.5–47.0] | 50.0 (n=3) | -9% | – |  | 134 |
| s3 | metrics_summary | rust-otap-input | 5 | 46.4 [45.5–47.4] | 50.1 (n=3) | -7% | – |  | 134 |
| s3 | metrics_summary | rust-via-otap | 5 | 63.5 [63.2–66.3] | 67.8 (n=3) | -6% | – |  | 134 |
| s3 | traces | parquet-go | 5 | 68.1 [63.3–68.9] | 72.9 (n=3) | -7% | 73.7 (n=3) | -8% | – |
| s3 | traces | parquet-go-nobloom | 5 | 63.3 [62.4–65.7] | 69.8 (n=3) | -9% | – |  | – |
| s3 | traces | rust-direct | 5 | 40.0 [39.6–40.7] | 44.7 (n=3) | -10% | – |  | 134 |
| s3 | traces | rust-direct-allbloom | 5 | 44.0 [43.4–46.0] | 50.9 (n=3) | -14% | – |  | 201 |
| s3 | traces | rust-direct-nobloom | 5 | 39.7 [39.2–40.4] | 44.9 (n=3) | -12% | – |  | 118 |
| s3 | traces | rust-pipeline-direct | 5 | 40.7 [39.7–43.7] | 46.7 (n=3) | -13% | – |  | – |
| s3 | traces | rust-pipeline-via_otap | 5 | 67.0 [64.0–69.7] | 74.7 (n=3) | -10% | – |  | – |
| s3 | traces | rust-via-otap | 5 | 64.4 [63.7–66.6] | 70.7 (n=3) | -9% | – |  | 134 |
