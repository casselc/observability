statements: 25630; objects-per-statement vs S3HeadObject mismatches: 815

| table | root | max-batch | statements | objects/stmt | rows/stmt | CPU ms/stmt, median [min–max] | CPU ms per object | µs per row |
|---|---|---|---|---|---|---|---|---|
| otel_logs | large | 1 | 100 | 1 | 10,000 | 32.35 [22.26–53.36] | 32.35 | 3.23 |
| otel_logs | large | 32 | 5 | 20 | 200,000 | 435.03 [335.97–440.67] | 21.75 | 2.18 |
| otel_logs | small | 1 | 2000 | 1 | 200 | 15.35 [8.84–45.92] | 15.35 | 76.73 |
| otel_logs | small | 32 | 65 | 32 | 6,400 | 75.94 [36.81–142.39] | 2.42 | 12.11 |
| otel_metrics_exponential_histogram | large | 1 | 60 | 1 | 2,000 | 33.42 [22.71–48.80] | 33.42 | 16.71 |
| otel_metrics_exponential_histogram | large | 32 | 5 | 12 | 24,000 | 220.55 [162.00–254.77] | 18.38 | 9.19 |
| otel_metrics_exponential_histogram | small | 1 | 2000 | 1 | 20 | 17.42 [10.51–33.27] | 17.42 | 870.92 |
| otel_metrics_exponential_histogram | small | 32 | 65 | 32 | 640 | 75.61 [39.59–96.35] | 2.41 | 120.44 |
| otel_metrics_exponential_histogram_points | large | 1 | 60 | 1 | 2,000 | 27.02 [19.20–34.37] | 27.02 | 13.51 |
| otel_metrics_exponential_histogram_points | large | 32 | 5 | 12 | 24,000 | 114.41 [89.60–147.67] | 9.53 | 4.77 |
| otel_metrics_exponential_histogram_points | small | 1 | 2000 | 1 | 20 | 16.22 [9.88–50.15] | 16.22 | 811.15 |
| otel_metrics_exponential_histogram_points | small | 32 | 65 | 32 | 640 | 65.59 [37.42–86.08] | 2.10 | 104.95 |
| otel_metrics_gauge | large | 1 | 60 | 1 | 28,000 | 129.95 [74.56–277.33] | 129.95 | 4.64 |
| otel_metrics_gauge | large | 32 | 10 | 6 | 168,000 | 597.63 [464.48–898.57] | 103.93 | 3.71 |
| otel_metrics_gauge | small | 1 | 2000 | 1 | 20 | 16.54 [10.21–53.25] | 16.54 | 826.80 |
| otel_metrics_gauge | small | 32 | 65 | 32 | 640 | 70.06 [35.93–84.42] | 2.23 | 111.69 |
| otel_metrics_histogram | large | 1 | 60 | 1 | 16,000 | 110.93 [70.49–157.10] | 110.93 | 6.93 |
| otel_metrics_histogram | large | 32 | 5 | 12 | 192,000 | 1026.84 [833.10–1662.38] | 85.57 | 5.35 |
| otel_metrics_histogram | small | 1 | 2000 | 1 | 20 | 16.42 [10.61–31.36] | 16.42 | 820.98 |
| otel_metrics_histogram | small | 32 | 65 | 32 | 640 | 74.24 [38.75–92.35] | 2.35 | 117.41 |
| otel_metrics_histogram_points | large | 1 | 60 | 1 | 16,000 | 37.73 [30.61–58.77] | 37.73 | 2.36 |
| otel_metrics_histogram_points | large | 32 | 5 | 12 | 192,000 | 328.20 [269.93–333.27] | 27.35 | 1.71 |
| otel_metrics_histogram_points | small | 1 | 2000 | 1 | 20 | 14.13 [9.41–32.66] | 14.13 | 706.40 |
| otel_metrics_histogram_points | small | 32 | 65 | 32 | 640 | 59.16 [31.24–72.53] | 1.90 | 94.95 |
| otel_metrics_number_points | large | 1 | 60 | 1 | 80,000 | 71.73 [60.00–104.10] | 71.73 | 0.90 |
| otel_metrics_number_points | large | 32 | 30 | 2 | 160,000 | 149.61 [119.89–176.20] | 74.80 | 0.94 |
| otel_metrics_number_points | small | 1 | 2000 | 1 | 40 | 14.18 [9.04–88.92] | 14.18 | 354.45 |
| otel_metrics_number_points | small | 32 | 65 | 32 | 1,280 | 61.95 [32.82–75.41] | 1.96 | 48.93 |
| otel_metrics_series | large | 1 | 5 | 1 | 100,000 | 1153.63 [1133.16–1999.43] | 1153.63 | 11.54 |
| otel_metrics_series | large | 32 | 5 | 1 | 100,000 | 1105.37 [779.93–1124.95] | 1105.37 | 11.05 |
| otel_metrics_series | small | 1 | 2000 | 1 | 36 | 14.43 [8.41–37.70] | 14.43 | 400.74 |
| otel_metrics_series | small | 32 | 65 | 32 | 1,152 | 63.84 [33.68–77.75] | 2.02 | 56.10 |
| otel_metrics_sum | large | 1 | 60 | 1 | 52,000 | 183.82 [125.43–269.39] | 183.82 | 3.54 |
| otel_metrics_sum | large | 32 | 20 | 3 | 156,000 | 669.62 [490.83–750.55] | 223.21 | 4.29 |
| otel_metrics_sum | small | 1 | 2000 | 1 | 20 | 16.39 [7.41–39.05] | 16.39 | 819.67 |
| otel_metrics_sum | small | 32 | 65 | 32 | 640 | 70.33 [38.00–88.57] | 2.22 | 111.12 |
| otel_metrics_summary | large | 1 | 60 | 1 | 2,000 | 20.69 [15.31–34.85] | 20.69 | 10.35 |
| otel_metrics_summary | large | 32 | 5 | 12 | 24,000 | 129.78 [104.04–164.52] | 10.82 | 5.41 |
| otel_metrics_summary | small | 1 | 2000 | 1 | 20 | 15.02 [9.42–32.40] | 15.02 | 751.23 |
| otel_metrics_summary | small | 32 | 65 | 32 | 640 | 69.55 [34.64–87.71] | 2.22 | 111.01 |
| otel_metrics_summary_points | large | 1 | 60 | 1 | 2,000 | 15.83 [10.72–20.52] | 15.83 | 7.91 |
| otel_metrics_summary_points | large | 32 | 5 | 12 | 24,000 | 57.99 [48.32–65.08] | 4.83 | 2.42 |
| otel_metrics_summary_points | small | 1 | 2000 | 1 | 20 | 12.87 [8.33–28.77] | 12.87 | 643.50 |
| otel_metrics_summary_points | small | 32 | 65 | 32 | 640 | 58.26 [29.36–71.86] | 1.83 | 91.75 |
| otel_traces | large | 1 | 100 | 1 | 10,000 | 36.62 [27.80–64.58] | 36.62 | 3.66 |
| otel_traces | large | 32 | 5 | 20 | 200,000 | 509.46 [443.33–560.03] | 25.47 | 2.55 |
| otel_traces | small | 1 | 2000 | 1 | 200 | 16.05 [9.74–53.94] | 16.05 | 80.25 |
| otel_traces | small | 32 | 65 | 32 | 6,400 | 88.26 [40.57–116.97] | 2.78 | 13.90 |

| table | per statement, ms | per object, ms | per row, µs | per-rep µs/row [min–max] | fit points |
|---|---|---|---|---|---|
| otel_logs | 12.23 | 1.70 | 1.804 | 1.48–1.97 | 25 |
| otel_metrics_exponential_histogram | 15.60 | 1.76 | 7.669 | 5.28–8.98 | 25 |
| otel_metrics_exponential_histogram_points | 16.23 | 1.49 | 3.468 | 2.39–4.76 | 25 |
| otel_metrics_gauge | 20.23 | 1.45 | 3.707 | 3.08–4.30 | 30 |
| otel_metrics_histogram | 14.13 | 1.79 | 5.663 | 4.14–8.56 | 25 |
| otel_metrics_histogram_points | 14.22 | 1.43 | 1.447 | 1.25–1.57 | 25 |
| otel_metrics_number_points | 11.78 | 1.60 | 0.815 | 0.69–0.94 | 25 |
| otel_metrics_series | 12.82 | 1.17 | 11.665 | 9.70–15.48 | 25 |
| otel_metrics_sum | -0.11 | 2.23 | 3.927 | 3.21–4.43 | 25 |
| otel_metrics_summary | 12.97 | 1.69 | 4.063 | 3.00–5.40 | 25 |
| otel_metrics_summary_points | 12.45 | 1.40 | 1.125 | 0.81–1.40 | 25 |
| otel_traces | 12.30 | 1.82 | 2.305 | 2.02–2.53 | 25 |
