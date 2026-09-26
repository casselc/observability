| table | rows/insert | parts reached | insert µs/row [M] | merge µs/ins. row at end [M] | merge ÷ insert at end [M] | merge ÷ insert, N=1e3 | N=1e4 | N=1e5 [E] | merge µs/row at N=1e4 [E] |
|---|---|---|---|---|---|---|---|---|---|
| traces, random ids | 10,000 | 2,131 | 6.50 | 17.23 | 2.65× | 2.6× | 3.0× | 3.6× | 19.6 |
| traces, random ids | 100,000 | 132 | 5.32 | 5.28 | 0.99× | 1.6× | 2.2× | 2.8× | 11.7 |
| traces, testgen ids | 10,000 | 2,400 | 6.01 | 17.14 | 2.85× | 2.9× | 3.2× | 3.8× | 19.4 |
| traces, testgen ids | 100,000 | 519 | 4.75 | 8.03 | 1.69× | 1.9× | 2.6× | 3.2× | 12.2 |
| logs | 10,000 | 2,400 | 4.95 | 17.11 | 3.45× | 3.5× | 4.0× | 4.9× | 19.8 |
| logs | 100,000 | 420 | 3.65 | 5.81 | 1.59× | 1.9× | 2.6× | 3.4× | 9.7 |
| metrics number points (gauge+sum) | 10,000 | 3,600 | 3.00 | 9.40 | 3.14× | 3.1× | 3.5× | 4.3× | 10.5 |
| metrics number points (gauge+sum) | 80,000 | 588 | 1.19 | 3.89 | 3.28× | 3.6× | 5.0× | 6.4× | 5.9 |
| metrics histogram points | 10,000 | 2,700 | 4.88 | 8.61 | 1.76× | 1.8× | 2.0× | 2.5× | 10.0 |
| metrics histogram points | 96,000 | 294 | 2.39 | 4.71 | 1.97× | 2.4× | 3.1× | 3.8× | 7.4 |
| metrics exp. histogram points | 10,000 | 900 | 6.40 | 11.49 | 1.80× | 1.8× | 2.4× | 3.1× | 15.7 |
| metrics exp. histogram points | 100,000 | 118 | 5.19 | 6.54 | 1.26× | 1.9× | 2.6× | 3.3× | 13.5 |
| metrics summary points | 10,000 | 1,800 | 3.94 | 6.68 | 1.70× | 1.7× | 2.2× | 2.8× | 8.6 |
| metrics summary points | 100,000 | 177 | 2.54 | 3.70 | 1.45× | 2.2× | 3.2× | 4.2× | 8.1 |
| metrics series table | 12,500 | 450 | 14.12 | 12.21 | 0.87× | – | – | – | – |
| metrics series table | 100,000 | 59 | 13.95 | 2.77 | 0.20× | – | – | – | – |
| ClickStack otel_metrics_sum | 52,000 | 192 | 8.01 | 14.78 | 1.85× | – | – | – | – |
| ClickStack otel_metrics_histogram | 16,000 | 840 | 11.54 | 25.63 | 2.22× | 2.3× | 3.1× | 3.9× | 35.4 |
| ClickStack otel_metrics_histogram | 96,000 | 96 | 10.98 | 11.40 | 1.04× | 1.9× | 2.7× | 3.5× | 29.3 |
| metrics blend (calculator weights) | 10k | – | 3.59 | 9.22 | 2.57× | 2.6× | 2.9× | 3.6× | 10.5 |
| metrics blend (calculator weights) | 100k | – | 1.69 | 4.17 | 2.46× | 2.9× | 3.9× | 5.0× | 6.7 |

| table | run | rewrites per inserted row at end [M] | compressed bytes written by merges per inserted byte [M] | stored B/row [M] | slope: rewrites per decade of parts [M] | µs per rewritten row, upper levels [M] |
|---|---|---|---|---|---|---|
| traces, random ids | traces-10k | 3.47 | 2.91 | 42.26 | 1.13 | 3.20 |
| traces, random ids | traces-100k | 1.69 | 1.66 | 40.89 | 1.13 | 3.03 |
| traces, testgen ids | traces-10k-tg | 4.65 | 2.19 | 11.5 | 1.38 | 2.61 |
| traces, testgen ids | traces-100k-tg | 3.32 | 3.09 | 9.36 | 1.38 | 2.33 |
| logs | logs-10k | 4.19 | 3.75 | 22.1 | 1.38 | 3.12 |
| logs | logs-100k | 2.53 | 2.57 | 18.91 | 1.38 | 2.02 |
| metrics number points (gauge+sum) | metrics-10k | 7.55 | 2.58 | 2.33 | 1.97 | 1.21 |
| metrics number points (gauge+sum) | metrics-100k | 4.36 | 0.55 | 1.07 | 1.97 | 0.83 |
| metrics histogram points | metrics-10k | 4.35 | 1.50 | 9.54 | 1.39 | 1.69 |
| metrics histogram points | metrics-100k | 3.37 | 1.32 | 7.29 | 1.39 | 1.25 |
| metrics exp. histogram points | metrics-10k | 3.75 | 2.17 | 19.08 | 1.48 | 2.70 |
| metrics exp. histogram points | metrics-100k | 2.48 | 1.12 | 17.0 | 1.48 | 2.45 |
| metrics summary points | metrics-10k | 5.80 | 2.25 | 4.22 | 2.44 | 1.05 |
| metrics summary points | metrics-100k | 3.33 | 1.36 | 9.2 | 2.44 | 1.04 |
| metrics series table | metrics-10k | 2.23 | 1.35 | 38.94 | – | – |
| metrics series table | metrics-100k | 1.12 | 0.19 | 38.87 | – | – |
| ClickStack otel_metrics_sum | clickstack-100k | 2.50 | 1.98 | 20.2 | – | – |
| ClickStack otel_metrics_histogram | clickstack-10k | 3.24 | 1.93 | 29.67 | 1.37 | 6.64 |
| ClickStack otel_metrics_histogram | clickstack-100k | 1.72 | 1.27 | 30.89 | 1.37 | 6.48 |
