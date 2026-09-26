### a. Steady state: one edge, one worker, 3.7 requests/s per signal, 90 s per run

| poll | n | visible p50 ms | visible p99 ms | objects/stmt | consumer CPU ms/object | consumer CPU µs/row | server CPU ms/object |
|---|---|---|---|---|---|---|---|
| 200ms (clean) | 5 | 221 [217–224] | 596 [450–1235] | 1.01 [1.01–1.02] | 1.080 [1.055–1.251] | 15.12 [14.77–17.52] | 24.6 [24.2–26.4] |
| 200ms (earlier, loaded box, n=1) | 1 | 211 | 441 | 1.01 | 1.287 | 18.01 | 22.4 |
| 1s (clean) | 5 | 666 [653–668] | 1222 [1194–1228] | 3.71 [3.71–3.71] | 0.438 [0.411–0.444] | 6.13 [5.75–6.22] | 9.1 [8.1–9.2] |
| 1s (earlier, loaded box, n=1) | 1 | 663 | 1211 | 3.71 | 0.51 | 7.14 | 8.93 |

### b. Throughput: one worker draining a backlog (block 2's roots), --max-batch 32 (default)

| root | poll | n | objects | rows | statements | rows/s per worker | objects/s | consumer CPU ms/object | consumer CPU µs/row | server CPU ms/object | errors |
|---|---|---|---|---|---|---|---|---|---|---|---|
| small | 200ms | 5 | 4800 | 240,000 | 156 | 14,828 [14,293–15,694] | 297 [286–314] | 0.162 [0.151–0.169] | 3.233 [3.016–3.386] | 2.5 [2.4–2.6] | 0 |
| small | 1s | 5 | 4800 | 240,000 | 156 | 14,970 [13,975–15,360] | 299 [280–307] | 0.161 [0.150–0.178] | 3.215 [3.002–3.569] | 2.5 [2.4–2.7] | 0 |
| large | 200ms | 5 | 149 | 2,800,000 | 21 | 358,561 [355,827–378,174] | 19 [19–20] | 0.522 [0.508–0.631] | 0.028 [0.027–0.034] | 57.3 [56.1–58.4] | 0 |
| large | 1s | 5 | 149 | 2,800,000 | 21 | 377,206 [370,321–382,723] | 20 [20–20] | 0.399 [0.375–0.471] | 0.021 [0.020–0.025] | 56.8 [55.4–58.2] | 0 |

Earlier (loaded box, `consumer_bench.sh`, 2,800 small objects, 32 per statement, n=3): rows/s per worker 20,185 [15,904–21,667], consumer CPU 0.189 [0.188–0.192] ms/object (2.65 [2.63–2.69] µs/row), server CPU 2.4 [2.3–2.5] ms/object.
