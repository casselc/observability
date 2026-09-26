
## traces-10k (10k rows/insert, 532.7 s, stop: db size, errors 0)
| table | stmts | rows/stmt | stmt/s | insert µs/row | merges | merge µs/row in | merge µs/ins row | merge÷insert (ingest win) | rewrites/row | bytes written/ins byte | B/row stored | final parts |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| traces | 2131 | 10000 | 4.0 | 6.504 | 412 | 4.961 | 17.233 | 2.65 (2.637) | 3.473 | 2.913 | 42.26 | 11 |
crosscheck: {'merge_threads_proc_cpu_s': 385.4, 'part_log_merge_cpu_s': 367.24, 'part_log_over_proc': 0.953, 'move_threads_cpu_s': 0.0}

## traces-100k (100k rows/insert, 132.0 s, stop: free disk, errors 0)
| table | stmts | rows/stmt | stmt/s | insert µs/row | merges | merge µs/row in | merge µs/ins row | merge÷insert (ingest win) | rewrites/row | bytes written/ins byte | B/row stored | final parts |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| traces | 132 | 100000 | 1.0 | 5.318 | 23 | 3.125 | 5.279 | 0.993 (0.993) | 1.689 | 1.663 | 40.89 | 11 |
crosscheck: {'merge_threads_proc_cpu_s': 83.21, 'part_log_merge_cpu_s': 69.68, 'part_log_over_proc': 0.837, 'move_threads_cpu_s': 0.0}

## traces-10k-tg (10k rows/insert, 603.3 s, stop: duration, errors 0)
| table | stmts | rows/stmt | stmt/s | insert µs/row | merges | merge µs/row in | merge µs/ins row | merge÷insert (ingest win) | rewrites/row | bytes written/ins byte | B/row stored | final parts |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| traces | 2400 | 10000 | 3.98 | 6.01 | 472 | 3.688 | 17.143 | 2.852 (2.852) | 4.648 | 2.19 | 11.5 | 8 |
crosscheck: {'merge_threads_proc_cpu_s': 425.51, 'part_log_merge_cpu_s': 411.43, 'part_log_over_proc': 0.967, 'move_threads_cpu_s': 0.0}

## traces-100k-tg (100k rows/insert, 518.6 s, stop: db size, errors 0)
| table | stmts | rows/stmt | stmt/s | insert µs/row | merges | merge µs/row in | merge µs/ins row | merge÷insert (ingest win) | rewrites/row | bytes written/ins byte | B/row stored | final parts |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| traces | 519 | 100000 | 1.0 | 4.748 | 99 | 2.415 | 8.028 | 1.691 (1.208) | 3.324 | 3.094 | 9.36 | 4 |
crosscheck: {'merge_threads_proc_cpu_s': 422.81, 'part_log_merge_cpu_s': 416.66, 'part_log_over_proc': 0.985, 'move_threads_cpu_s': 0.0}

## logs-10k (10k rows/insert, 603.6 s, stop: duration, errors 0)
| table | stmts | rows/stmt | stmt/s | insert µs/row | merges | merge µs/row in | merge µs/ins row | merge÷insert (ingest win) | rewrites/row | bytes written/ins byte | B/row stored | final parts |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| logs | 2400 | 10000 | 3.98 | 4.954 | 478 | 4.083 | 17.108 | 3.454 (3.454) | 4.19 | 3.748 | 22.1 | 11 |
crosscheck: {'merge_threads_proc_cpu_s': 424.02, 'part_log_merge_cpu_s': 410.59, 'part_log_over_proc': 0.968, 'move_threads_cpu_s': 0.0}

## logs-100k (100k rows/insert, 422.5 s, stop: duration, errors 0)
| table | stmts | rows/stmt | stmt/s | insert µs/row | merges | merge µs/row in | merge µs/ins row | merge÷insert (ingest win) | rewrites/row | bytes written/ins byte | B/row stored | final parts |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| logs | 420 | 100000 | 0.99 | 3.653 | 82 | 2.299 | 5.808 | 1.59 (1.59) | 2.526 | 2.571 | 18.91 | 8 |
crosscheck: {'merge_threads_proc_cpu_s': 251.66, 'part_log_merge_cpu_s': 243.95, 'part_log_over_proc': 0.969, 'move_threads_cpu_s': 0.0}

## metrics-10k (10k rows/insert, 901.9 s, stop: duration, errors 0)
| table | stmts | rows/stmt | stmt/s | insert µs/row | merges | merge µs/row in | merge µs/ins row | merge÷insert (ingest win) | rewrites/row | bytes written/ins byte | B/row stored | final parts |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| number | 3600 | 10000 | 3.99 | 2.996 | 643 | 1.245 | 9.405 | 3.139 (3.139) | 7.554 | 2.579 | 2.33 | 9 |
| histogram | 2700 | 10000 | 2.99 | 4.882 | 521 | 1.982 | 8.614 | 1.765 (1.765) | 4.347 | 1.499 | 9.54 | 10 |
| exponential_histogram | 900 | 10000 | 1.0 | 6.402 | 169 | 3.063 | 11.495 | 1.796 (1.796) | 3.752 | 2.172 | 19.08 | 8 |
| summary | 1800 | 10000 | 2.0 | 3.939 | 324 | 1.152 | 6.685 | 1.697 (1.697) | 5.802 | 2.251 | 4.22 | 7 |
| series | 450 | 12500 | 0.5 | 14.12 | 84 | 5.475 | 12.215 | 0.865 (0.865) | 2.231 | 1.347 | 38.94 | 1 |
crosscheck: {'merge_threads_proc_cpu_s': 900.3, 'part_log_merge_cpu_s': 863.64, 'part_log_over_proc': 0.959, 'move_threads_cpu_s': 0.0}

## metrics-100k (100k rows/insert, 587.1 s, stop: db size, errors 0)
| table | stmts | rows/stmt | stmt/s | insert µs/row | merges | merge µs/row in | merge µs/ins row | merge÷insert (ingest win) | rewrites/row | bytes written/ins byte | B/row stored | final parts |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| number | 588 | 80000 | 1.0 | 1.185 | 108 | 0.891 | 3.886 | 3.279 (3.279) | 4.361 | 0.551 | 1.07 | 7 |
| histogram | 294 | 96000 | 0.5 | 2.392 | 53 | 1.397 | 4.713 | 1.97 (1.5) | 3.374 | 1.324 | 7.29 | 4 |
| exponential_histogram | 118 | 100000 | 0.2 | 5.186 | 21 | 2.632 | 6.536 | 1.26 (1.26) | 2.483 | 1.122 | 17.0 | 5 |
| summary | 177 | 100000 | 0.3 | 2.541 | 31 | 1.109 | 3.696 | 1.455 (1.455) | 3.333 | 1.356 | 9.2 | 3 |
| series | 59 | 100000 | 0.1 | 13.952 | 11 | 2.479 | 2.773 | 0.199 (0.199) | 1.119 | 0.186 | 38.87 | 4 |
crosscheck: {'merge_threads_proc_cpu_s': 488.83, 'part_log_merge_cpu_s': 474.74, 'part_log_over_proc': 0.971, 'move_threads_cpu_s': 0.0}

## clickstack-10k (10k rows/insert, 422.3 s, stop: duration, errors 0)
| table | stmts | rows/stmt | stmt/s | insert µs/row | merges | merge µs/row in | merge µs/ins row | merge÷insert (ingest win) | rewrites/row | bytes written/ins byte | B/row stored | final parts |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| A_histogram | 840 | 16000 | 1.99 | 11.539 | 158 | 7.904 | 25.631 | 2.221 (2.221) | 3.243 | 1.93 | 29.67 | 7 |
crosscheck: {'merge_threads_proc_cpu_s': 354.53, 'part_log_merge_cpu_s': 344.48, 'part_log_over_proc': 0.972, 'move_threads_cpu_s': 0.0}

## clickstack-100k (100k rows/insert, 191.4 s, stop: db size, errors 0)
| table | stmts | rows/stmt | stmt/s | insert µs/row | merges | merge µs/row in | merge µs/ins row | merge÷insert (ingest win) | rewrites/row | bytes written/ins byte | B/row stored | final parts |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| A_sum | 192 | 52000 | 1.0 | 8.007 | 34 | 5.899 | 14.779 | 1.846 (1.519) | 2.505 | 1.978 | 20.2 | 8 |
| A_histogram | 96 | 96000 | 0.5 | 10.983 | 17 | 6.635 | 11.403 | 1.038 (0.998) | 1.719 | 1.275 | 30.89 | 7 |
crosscheck: {'merge_threads_proc_cpu_s': 263.3, 'part_log_merge_cpu_s': 252.65, 'part_log_over_proc': 0.96, 'move_threads_cpu_s': 0.0}

## set-wide (10k rows/insert, 296.7 s, stop: db size, errors 0)
| table | stmts | rows/stmt | stmt/s | insert µs/row | merges | merge µs/row in | merge µs/ins row | merge÷insert (ingest win) | rewrites/row | bytes written/ins byte | B/row stored | final parts |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| traces | 1187 | 10000 | 4.0 | 8.266 | 224 | 4.85 | 15.912 | 1.925 (1.925) | 3.281 | 2.738 | 42.27 | 13 |
crosscheck: {'merge_threads_proc_cpu_s': 196.43, 'part_log_merge_cpu_s': 188.88, 'part_log_over_proc': 0.962, 'move_threads_cpu_s': 0.0}

## set-stochastic (10k rows/insert, 296.7 s, stop: db size, errors 0)
| table | stmts | rows/stmt | stmt/s | insert µs/row | merges | merge µs/row in | merge µs/ins row | merge÷insert (ingest win) | rewrites/row | bytes written/ins byte | B/row stored | final parts |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| traces | 1187 | 10000 | 4.0 | 6.179 | 225 | 4.699 | 15.46 | 2.502 (2.502) | 3.29 | 2.757 | 42.26 | 12 |
crosscheck: {'merge_threads_proc_cpu_s': 188.28, 'part_log_merge_cpu_s': 183.51, 'part_log_over_proc': 0.975, 'move_threads_cpu_s': 0.0}

## set-novertical (10k rows/insert, 296.7 s, stop: db size, errors 0)
| table | stmts | rows/stmt | stmt/s | insert µs/row | merges | merge µs/row in | merge µs/ins row | merge÷insert (ingest win) | rewrites/row | bytes written/ins byte | B/row stored | final parts |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| traces | 1187 | 10000 | 4.0 | 6.306 | 222 | 6.839 | 24.514 | 3.887 (3.872) | 3.585 | 3.0 | 42.15 | 10 |
crosscheck: {'merge_threads_proc_cpu_s': 297.64, 'part_log_merge_cpu_s': 290.98, 'part_log_over_proc': 0.978, 'move_threads_cpu_s': 0.0}

## set-hourly (10k rows/insert, 301.6 s, stop: db size, errors 0)
| table | stmts | rows/stmt | stmt/s | insert µs/row | merges | merge µs/row in | merge µs/ins row | merge÷insert (ingest win) | rewrites/row | bytes written/ins byte | B/row stored | final parts |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| traces | 1200 | 10000 | 3.98 | 6.001 | 213 | 4.829 | 12.95 | 2.158 (2.158) | 2.682 | 2.236 | 42.13 | 31 |
crosscheck: {'merge_threads_proc_cpu_s': 160.11, 'part_log_merge_cpu_s': 155.4, 'part_log_over_proc': 0.971, 'move_threads_cpu_s': 0.0}

## ttl-drop (10k rows/insert, 723.8 s, stop: duration, errors 0)
| table | stmts | rows/stmt | stmt/s | insert µs/row | merges | merge µs/row in | merge µs/ins row | merge÷insert (ingest win) | rewrites/row | bytes written/ins byte | B/row stored | final parts |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| number | 2880 | 10000 | 3.98 | 2.974 | 511 | 1.258 | 8.54 | 2.872 (2.872) | 6.79 | 2.467 | 7.78 | 5 |
crosscheck: {'merge_threads_proc_cpu_s': 260.02, 'part_log_merge_cpu_s': 245.94, 'part_log_over_proc': 0.946, 'move_threads_cpu_s': 0.0}

## ttl-rows (10k rows/insert, 723.7 s, stop: duration, errors 0)
| table | stmts | rows/stmt | stmt/s | insert µs/row | merges | merge µs/row in | merge µs/ins row | merge÷insert (ingest win) | rewrites/row | bytes written/ins byte | B/row stored | final parts |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| number | 2880 | 10000 | 3.98 | 2.988 | 531 | 1.07 | 9.072 | 3.036 (3.012) | 8.477 | 2.665 | 3.61 | 7 |
crosscheck: {'merge_threads_proc_cpu_s': 274.82, 'part_log_merge_cpu_s': 261.27, 'part_log_over_proc': 0.951, 'move_threads_cpu_s': 0.0}

## ttl-move-local (10k rows/insert, 592.8 s, stop: db size, errors 0)
| table | stmts | rows/stmt | stmt/s | insert µs/row | merges | merge µs/row in | merge µs/ins row | merge÷insert (ingest win) | rewrites/row | bytes written/ins byte | B/row stored | final parts |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| traces | 1186 | 10000 | 2.0 | 6.277 | 210 | 4.933 | 13.173 | 2.099 (2.093) | 2.67 | 2.238 | 42.43 | 34 |
crosscheck: {'merge_threads_proc_cpu_s': 164.85, 'part_log_merge_cpu_s': 156.23, 'part_log_over_proc': 0.948, 'move_threads_cpu_s': 0.66}

## ttl-move-s3 (10k rows/insert, 593.0 s, stop: db size, errors 0)
| table | stmts | rows/stmt | stmt/s | insert µs/row | merges | merge µs/row in | merge µs/ins row | merge÷insert (ingest win) | rewrites/row | bytes written/ins byte | B/row stored | final parts |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| traces | 1186 | 10000 | 2.0 | 6.289 | 211 | 4.9 | 13.649 | 2.17 (2.165) | 2.786 | 2.334 | 42.34 | 22 |
crosscheck: {'merge_threads_proc_cpu_s': 171.09, 'part_log_merge_cpu_s': 161.88, 'part_log_over_proc': 0.946, 'move_threads_cpu_s': 0.54}

## ttl-move-local (10k rows/insert, 593.1 s, stop: db size, errors 0)
| table | stmts | rows/stmt | stmt/s | insert µs/row | merges | merge µs/row in | merge µs/ins row | merge÷insert (ingest win) | rewrites/row | bytes written/ins byte | B/row stored | final parts |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| traces | 1187 | 10000 | 2.0 | 6.959 | 210 | 5.045 | 13.489 | 1.938 (1.927) | 2.674 | 2.242 | 42.44 | 33 |
crosscheck: {'merge_threads_proc_cpu_s': 171.38, 'part_log_merge_cpu_s': 160.12, 'part_log_over_proc': 0.934, 'move_threads_cpu_s': 4.28}

## ttl-move-s3 (10k rows/insert, 593.8 s, stop: db size, errors 0)
| table | stmts | rows/stmt | stmt/s | insert µs/row | merges | merge µs/row in | merge µs/ins row | merge÷insert (ingest win) | rewrites/row | bytes written/ins byte | B/row stored | final parts |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| traces | 1188 | 10000 | 2.0 | 7.311 | 211 | 5.861 | 16.301 | 2.23 (2.23) | 2.781 | 2.331 | 42.36 | 24 |
crosscheck: {'merge_threads_proc_cpu_s': 205.97, 'part_log_merge_cpu_s': 193.66, 'part_log_over_proc': 0.94, 'move_threads_cpu_s': 13.07}
