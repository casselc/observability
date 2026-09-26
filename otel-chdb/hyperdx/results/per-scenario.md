What a user of each source pays: the statements HyperDX sent for the views
source timed on the views, against those it sent for the stock source timed
on the stock tables (`scripts/summarize.py --exclude 'INTERVAL 1 minute'`,
which drops the alert task's statements). Median server ms, 7 rounds.

| scenario | chart query views / stock ms | ratio | metric-name queries views / stock ms | metadata queries views / stock ms | all statements views / stock ms | ratio |
|---|---:|---:|---:|---:|---:|---:|
| big-picker | 113 / 102 | 1.12 | 394 / 20 | 236 / 67 | 744 / 189 | 3.93 |
| big-picker-search | 0 / 0 | – | 115 / 44 | 0 / 0 | 115 / 44 | 2.60 |
| big-picker-catalog | 0 / 0 | – | 0 / 0 | 649 / 208 | 649 / 208 | 3.12 |
| big-m-gauge-avg | 92 / 100 | 0.92 | 245 / 19 | 159 / 58 | 496 / 177 | 2.80 |
| big-m-gauge-groupby-attr | 232 / 148 | 1.57 | 244 / 20 | 371 / 83 | 847 / 251 | 3.37 |
| big-m-gauge-groupby-res | 129 / 152 | 0.85 | 250 / 19 | 188 / 64 | 567 / 234 | 2.42 |
| big-m-gauge-where-res | 92 / 28 | 3.25 | 233 / 19 | 191 / 58 | 516 / 105 | 4.92 |
| big-m-gauge-where-res-sql | 93 / 174 | 0.54 | 269 / 19 | 190 / 59 | 552 / 253 | 2.19 |
| big-m-sum-cumulative | 427 / 232 | 1.84 | 277 / 21 | 530 / 91 | 1235 / 344 | 3.59 |
| big-m-sum-cumulative-increase | 796 / 338 | 2.36 | 253 / 19 | 495 / 89 | 1544 / 445 | 3.47 |
| big-m-sum-delta | 104 / 69 | 1.51 | 237 / 19 | 153 / 63 | 494 / 151 | 3.27 |
| big-m-sum-updown | 89 / 58 | 1.52 | 225 / 22 | 155 / 66 | 469 / 146 | 3.21 |
| big-m-hist-p95 | 352 / 257 | 1.37 | 232 / 20 | 183 / 89 | 767 / 366 | 2.09 |
| big-m-hist-count | 226 / 125 | 1.80 | 252 / 19 | 185 / 92 | 663 / 236 | 2.81 |
| big-m-gauge-where-cluster | 102 / 108 | 0.95 | 240 / 21 | 189 / 147 | 532 / 276 | 1.93 |
| big-m-exphist-p50 | 196 / 194 | 1.01 | 256 / 21 | 145 / 68 | 596 / 283 | 2.11 |
| big-m-summary | 0 / 0 | – | 257 / 18 | 98 / 50 | 354 / 68 | 5.18 |
| **total** | 3044 / 2084 | 1.46 | 3978 / 341 | 4118 / 1352 | 11140 / 3778 | 2.95 |
