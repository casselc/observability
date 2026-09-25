run cb1790311444, 9 reps; median [min–max]

| signal | layout | objects | settings | wall ms | server CPU ms | peak mem MB | S3 GET | S3 HEAD | MB read |
|---|---|---|---|---|---|---|---|---|---|
| traces | rust (TraceId bloom) | 10 | default | 210 [190–252] | 287 [254–356] | 216 [215–217] | 10 [10–10] | 0 [0–0] | 1.34 |
| traces | rust (TraceId bloom) | 10 | single-thread | 279 [264–324] | 284 [268–331] | 38 [38–38] | 10 [10–10] | 0 [0–0] | 1.34 |
| traces | rust, no bloom | 10 | default | 199 [188–229] | 279 [252–553] | 216 [216–218] | 10 [10–10] | 0 [0–0] | 1.34 |
| traces | rust, no bloom | 10 | single-thread | 279 [270–296] | 285 [276–303] | 38 [38–38] | 10 [10–10] | 0 [0–0] | 1.34 |
| traces | parquetgo (all blooms) | 10 | default | 195 [179–238] | 259 [251–608] | 216 [215–217] | 10 [10–10] | 0 [0–0] | 2.71 |
| traces | parquetgo (all blooms) | 10 | single-thread | 282 [269–294] | 273 [259–288] | 38 [38–38] | 10 [10–10] | 0 [0–0] | 2.71 |
| traces | parquetgo, no bloom | 10 | default | 201 [194–240] | 261 [252–701] | 216 [215–217] | 10 [10–10] | 0 [0–0] | 1.77 |
| traces | parquetgo, no bloom | 10 | single-thread | 278 [266–315] | 278 [261–315] | 38 [38–38] | 10 [10–10] | 0 [0–0] | 1.77 |
