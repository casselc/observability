
## clickstack-histogram-100k (100k rows/insert, 522.4 s, stop: free disk, errors 0)
| table | stmts | rows/stmt | stmt/s | insert µs/row | merges | merge µs/row in | merge µs/ins row | merge÷insert (ingest win) | rewrites/row | bytes written/ins byte | B/row stored | final parts |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| A_histogram | 314 | 96000 | 0.6 | 7.754 | 59 | 5.01 | 12.526 | 1.615 (1.615) | 2.5 | 1.76 | 28.5 | 9 |
crosscheck: {'merge_threads_proc_cpu_s': 387.22, 'part_log_merge_cpu_s': 377.59, 'part_log_over_proc': 0.975, 'move_threads_cpu_s': 0.0}

## clickstack-sum-100k (100k rows/insert, 817.9 s, stop: db size, errors 0)
| table | stmts | rows/stmt | stmt/s | insert µs/row | merges | merge µs/row in | merge µs/ins row | merge÷insert (ingest win) | rewrites/row | bytes written/ins byte | B/row stored | final parts |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| A_sum | 491 | 104000 | 0.6 | 5.833 | 93 | 4.304 | 11.088 | 1.901 (1.869) | 2.576 | 2.081 | 19.62 | 7 |
crosscheck: {'merge_threads_proc_cpu_s': 574.41, 'part_log_merge_cpu_s': 566.2, 'part_log_over_proc': 0.986, 'move_threads_cpu_s': 0.0}

## exphist-100k (100k rows/insert, 381.5 s, stop: free disk, errors 0)
| table | stmts | rows/stmt | stmt/s | insert µs/row | merges | merge µs/row in | merge µs/ins row | merge÷insert (ingest win) | rewrites/row | bytes written/ins byte | B/row stored | final parts |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| exponential_histogram | 458 | 100000 | 1.2 | 3.515 | 86 | 1.963 | 4.878 | 1.388 (1.388) | 2.485 | 1.125 | 16.57 | 13 |
crosscheck: {'merge_threads_proc_cpu_s': 278.48, 'part_log_merge_cpu_s': 223.41, 'part_log_over_proc': 0.802, 'move_threads_cpu_s': 0.0}

## histogram-100k (100k rows/insert, 747.9 s, stop: free disk, errors 0)
| table | stmts | rows/stmt | stmt/s | insert µs/row | merges | merge µs/row in | merge µs/ins row | merge÷insert (ingest win) | rewrites/row | bytes written/ins byte | B/row stored | final parts |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| histogram | 1496 | 96000 | 2.0 | 1.612 | 292 | 1.001 | 4.098 | 2.543 (2.014) | 4.093 | 1.604 | 7.58 | 6 |
crosscheck: {'merge_threads_proc_cpu_s': 600.22, 'part_log_merge_cpu_s': 588.5, 'part_log_over_proc': 0.98, 'move_threads_cpu_s': 0.0}

## logs-100k (100k rows/insert, 446.6 s, stop: db size, errors 0)
| table | stmts | rows/stmt | stmt/s | insert µs/row | merges | merge µs/row in | merge µs/ins row | merge÷insert (ingest win) | rewrites/row | bytes written/ins byte | B/row stored | final parts |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| logs | 536 | 100000 | 1.2 | 2.522 | 107 | 1.649 | 5.793 | 2.297 (1.765) | 3.513 | 3.505 | 18.53 | 6 |
crosscheck: {'merge_threads_proc_cpu_s': 317.27, 'part_log_merge_cpu_s': 310.51, 'part_log_over_proc': 0.979, 'move_threads_cpu_s': 0.0}

## number-100k (100k rows/insert, 702.5 s, stop: duration, errors 0)
| table | stmts | rows/stmt | stmt/s | insert µs/row | merges | merge µs/row in | merge µs/ins row | merge÷insert (ingest win) | rewrites/row | bytes written/ins byte | B/row stored | final parts |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| number | 2100 | 80000 | 2.99 | 0.83 | 390 | 0.583 | 3.034 | 3.655 (3.655) | 5.206 | 0.61 | 1.24 | 10 |
crosscheck: {'merge_threads_proc_cpu_s': 522.96, 'part_log_merge_cpu_s': 509.67, 'part_log_over_proc': 0.975, 'move_threads_cpu_s': 0.0}

## series-100k (100k rows/insert, 602.1 s, stop: duration, errors 0)
| table | stmts | rows/stmt | stmt/s | insert µs/row | merges | merge µs/row in | merge µs/ins row | merge÷insert (ingest win) | rewrites/row | bytes written/ins byte | B/row stored | final parts |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| series | 600 | 100000 | 1.0 | 8.245 | 120 | 1.624 | 1.946 | 0.236 (0.234) | 1.198 | 0.2 | 38.86 | 1 |
crosscheck: {'merge_threads_proc_cpu_s': 123.54, 'part_log_merge_cpu_s': 116.73, 'part_log_over_proc': 0.945, 'move_threads_cpu_s': 0.0}

## summary-100k (100k rows/insert, 903.3 s, stop: duration, errors 0)
| table | stmts | rows/stmt | stmt/s | insert µs/row | merges | merge µs/row in | merge µs/ins row | merge÷insert (ingest win) | rewrites/row | bytes written/ins byte | B/row stored | final parts |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| summary | 1800 | 100000 | 1.99 | 1.613 | 338 | 0.728 | 2.856 | 1.771 (1.771) | 3.921 | 0.989 | 2.76 | 12 |
crosscheck: {'merge_threads_proc_cpu_s': 529.09, 'part_log_merge_cpu_s': 514.15, 'part_log_over_proc': 0.972, 'move_threads_cpu_s': 0.0}

## traces-100k (100k rows/insert, 160.6 s, stop: free disk, errors 0)
| table | stmts | rows/stmt | stmt/s | insert µs/row | merges | merge µs/row in | merge µs/ins row | merge÷insert (ingest win) | rewrites/row | bytes written/ins byte | B/row stored | final parts |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| traces | 161 | 100000 | 1.0 | 3.62 | 30 | 2.178 | 5.521 | 1.525 (1.401) | 2.534 | 2.488 | 40.78 | 3 |
crosscheck: {'merge_threads_proc_cpu_s': 90.79, 'part_log_merge_cpu_s': 88.88, 'part_log_over_proc': 0.979, 'move_threads_cpu_s': 0.0}

## traces-100k-tg (100k rows/insert, 888.3 s, stop: db size, errors 0)
| table | stmts | rows/stmt | stmt/s | insert µs/row | merges | merge µs/row in | merge µs/ins row | merge÷insert (ingest win) | rewrites/row | bytes written/ins byte | B/row stored | final parts |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| traces | 1066 | 100000 | 1.2 | 3.303 | 206 | 1.844 | 6.027 | 1.824 (1.824) | 3.267 | 3.023 | 9.4 | 10 |
crosscheck: {'merge_threads_proc_cpu_s': 651.5, 'part_log_merge_cpu_s': 642.43, 'part_log_over_proc': 0.986, 'move_threads_cpu_s': 0.0}
