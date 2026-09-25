run cb1790310281, 3 reps; median [min–max]

| signal | layout | objects | settings | wall ms | server CPU ms | peak mem MB | S3 GET | S3 HEAD | MB read |
|---|---|---|---|---|---|---|---|---|---|
| traces | rust (TraceId bloom) | 1 | default | 54 [54–64] | 47 [45–58] | 24 [24–24] | 1 [1–1] | 0 [0–0] | 0.13 |
| traces | rust (TraceId bloom) | 1 | single-thread | 48 [47–50] | 41 [40–42] | 24 [24–24] | 1 [1–1] | 0 [0–0] | 0.13 |
| traces | rust (TraceId bloom) | 10 | default | 231 [207–233] | 318 [303–320] | 216 [216–216] | 10 [10–10] | 0 [0–0] | 1.34 |
| traces | rust (TraceId bloom) | 10 | single-thread | 307 [284–308] | 291 [286–303] | 38 [38–38] | 10 [10–10] | 0 [0–0] | 1.34 |
| traces | rust, no bloom | 1 | default | 49 [47–49] | 137 [40–163] | 24 [24–24] | 1 [1–1] | 0 [0–0] | 0.13 |
| traces | rust, no bloom | 1 | single-thread | 51 [43–51] | 46 [45–53] | 24 [24–24] | 1 [1–1] | 0 [0–0] | 0.13 |
| traces | rust, no bloom | 10 | default | 212 [208–233] | 277 [268–298] | 216 [216–216] | 10 [10–10] | 0 [0–0] | 1.34 |
| traces | rust, no bloom | 10 | single-thread | 297 [282–302] | 305 [283–309] | 38 [38–38] | 10 [10–10] | 0 [0–0] | 1.34 |
| traces | parquetgo (all blooms) | 1 | default | 51 [45–56] | 95 [42–96] | 24 [24–25] | 1 [1–1] | 0 [0–0] | 0.27 |
| traces | parquetgo (all blooms) | 1 | single-thread | 47 [46–50] | 115 [41–125] | 25 [25–25] | 1 [1–1] | 0 [0–0] | 0.27 |
| traces | parquetgo (all blooms) | 10 | default | 198 [195–205] | 265 [253–278] | 216 [216–216] | 10 [10–10] | 0 [0–0] | 2.71 |
| traces | parquetgo (all blooms) | 10 | single-thread | 295 [288–318] | 294 [288–318] | 38 [38–38] | 10 [10–10] | 0 [0–0] | 2.71 |
| traces | parquetgo, no bloom | 1 | default | 52 [48–72] | 133 [103–263] | 25 [24–25] | 1 [1–1] | 0 [0–0] | 0.18 |
| traces | parquetgo, no bloom | 1 | single-thread | 48 [45–49] | 40 [37–144] | 25 [25–25] | 1 [1–1] | 0 [0–0] | 0.18 |
| traces | parquetgo, no bloom | 10 | default | 219 [195–237] | 278 [271–318] | 216 [216–216] | 10 [10–10] | 0 [0–0] | 1.77 |
| traces | parquetgo, no bloom | 10 | single-thread | 323 [300–324] | 298 [298–320] | 38 [38–38] | 10 [10–10] | 0 [0–0] | 1.77 |
| logs | rust (TraceId bloom) | 1 | default | 43 [36–85] | 146 [79–237] | 19 [16–19] | 1 [1–1] | 0 [0–0] | 0.08 |
| logs | rust (TraceId bloom) | 1 | single-thread | 35 [33–38] | 46 [28–105] | 16 [16–16] | 1 [1–1] | 0 [0–0] | 0.08 |
| logs | rust (TraceId bloom) | 10 | default | 132 [122–152] | 172 [165–190] | 152 [152–155] | 10 [10–10] | 0 [0–0] | 0.78 |
| logs | rust (TraceId bloom) | 10 | single-thread | 193 [178–202] | 193 [185–197] | 24 [24–24] | 10 [10–10] | 0 [0–0] | 0.78 |
| logs | rust, no bloom | 1 | default | 48 [46–49] | 119 [113–142] | 16 [16–19] | 1 [1–1] | 0 [0–0] | 0.08 |
| logs | rust, no bloom | 1 | single-thread | 44 [34–50] | 36 [29–102] | 16 [16–16] | 1 [1–1] | 0 [0–0] | 0.08 |
| logs | rust, no bloom | 10 | default | 136 [133–159] | 181 [172–204] | 155 [151–156] | 10 [10–10] | 0 [0–0] | 0.78 |
| logs | rust, no bloom | 10 | single-thread | 196 [193–199] | 197 [190–199] | 24 [24–24] | 10 [10–10] | 0 [0–0] | 0.78 |
| logs | parquetgo (all blooms) | 1 | default | 44 [42–44] | 109 [107–138] | 16 [16–19] | 1 [1–1] | 0 [0–0] | 0.16 |
| logs | parquetgo (all blooms) | 1 | single-thread | 37 [37–43] | 34 [28–73] | 16 [16–16] | 1 [1–1] | 0 [0–0] | 0.16 |
| logs | parquetgo (all blooms) | 10 | default | 155 [144–171] | 197 [180–206] | 152 [150–152] | 10 [10–10] | 0 [0–0] | 1.59 |
| logs | parquetgo (all blooms) | 10 | single-thread | 220 [194–225] | 211 [192–222] | 24 [24–24] | 10 [10–10] | 0 [0–0] | 1.59 |
| logs | parquetgo, no bloom | 1 | default | 48 [43–48] | 115 [31–168] | 16 [16–19] | 1 [1–1] | 0 [0–0] | 0.10 |
| logs | parquetgo, no bloom | 1 | single-thread | 48 [46–49] | 38 [34–181] | 16 [16–16] | 1 [1–1] | 0 [0–0] | 0.10 |
| logs | parquetgo, no bloom | 10 | default | 145 [135–176] | 184 [183–212] | 152 [151–153] | 10 [10–10] | 0 [0–0] | 0.99 |
| logs | parquetgo, no bloom | 10 | single-thread | 205 [187–209] | 190 [185–198] | 23 [23–24] | 10 [10–10] | 0 [0–0] | 0.99 |
