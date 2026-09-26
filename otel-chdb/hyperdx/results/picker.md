Metric-name picker, sum over the four kinds it lists, 24 h window (HyperDX
widens a sub-day range to a day), 6.04 M points, median of 9 (`scripts/picker_bench.py`).

| path | ms (sum of 4 kinds, median) | rows read | bytes read | names returned |
|---|---:|---:|---:|---:|
| views-exhaustive | 263.5 | 11,727,780 | 101,711,670 | 38 |
| views-search | 110.6 | 11,710,908 | 23,893,897 | 2 |
| stock-index | 16.9 | 777 | 21,722 | 38 |
| stock-exhaustive | 45.7 | 5,986,900 | 29,934,535 | 38 |
| stock-search | 36.9 | 5,986,900 | 9,429,415 | 2 |
| points-direct | 60.4 | 6,394,736 | 37,669,296 | 38 |
| helper | 15.9 | 5,160 | 35,572 | 38 |
