run mcb1790316073, 3 reps; median [min–max]; server-wide CPU (the server is shared)

| type | layout | objects | settings | wall ms | server CPU ms | CPU µs/point | peak mem MB | S3 GET | MB read |
|---|---|---|---|---|---|---|---|---|---|
| gauge | rust | 1 | default | 68 [68–73] | 56 [50–63] | 5.58 | 25 [25–25] | 1 [1–1] | 0.09 |
| gauge | rust | 1 | single-thread | 53 [50–62] | 45 [44–55] | 4.53 | 26 [26–26] | 1 [1–1] | 0.09 |
| gauge | rust | 10 | default | 356 [325–368] | 411 [408–416] | 4.11 | 248 [248–248] | 10 [10–10] | 0.89 |
| gauge | rust | 10 | single-thread | 389 [352–392] | 355 [324–366] | 3.55 | 43 [43–43] | 10 [10–10] | 0.89 |
| gauge | parquetgo | 1 | default | 69 [59–84] | 50 [47–63] | 4.96 | 28 [28–29] | 1 [1–1] | 0.22 |
| gauge | parquetgo | 1 | single-thread | 59 [56–64] | 44 [44–45] | 4.44 | 28 [28–28] | 1 [1–1] | 0.22 |
| gauge | parquetgo | 10 | default | 308 [305–329] | 376 [365–381] | 3.76 | 248 [248–248] | 10 [10–10] | 2.16 |
| gauge | parquetgo | 10 | single-thread | 333 [333–352] | 325 [303–453] | 3.25 | 48 [48–48] | 10 [10–10] | 2.16 |
| sum | rust | 1 | default | 58 [56–59] | 47 [46–48] | 4.69 | 28 [27–28] | 1 [1–1] | 0.04 |
| sum | rust | 1 | single-thread | 53 [53–55] | 47 [46–49] | 4.71 | 27 [27–27] | 1 [1–1] | 0.04 |
| sum | rust | 10 | default | 331 [318–362] | 387 [386–416] | 3.87 | 251 [251–251] | 10 [10–10] | 0.36 |
| sum | rust | 10 | single-thread | 366 [346–372] | 356 [342–365] | 3.56 | 44 [44–44] | 10 [10–10] | 0.36 |
| sum | parquetgo | 1 | default | 55 [51–60] | 46 [45–48] | 4.56 | 29 [29–30] | 1 [1–1] | 0.13 |
| sum | parquetgo | 1 | single-thread | 57 [50–60] | 46 [43–48] | 4.61 | 29 [29–29] | 1 [1–1] | 0.13 |
| sum | parquetgo | 10 | default | 344 [316–364] | 397 [350–431] | 3.97 | 251 [251–251] | 10 [10–10] | 1.29 |
| sum | parquetgo | 10 | single-thread | 388 [371–426] | 474 [395–490] | 4.74 | 49 [49–49] | 10 [10–10] | 1.29 |
| histogram | rust | 1 | default | 76 [72–83] | 67 [64–73] | 6.71 | 31 [31–33] | 1 [1–1] | 0.12 |
| histogram | rust | 1 | single-thread | 63 [63–64] | 57 [55–59] | 5.67 | 32 [32–32] | 1 [1–1] | 0.12 |
| histogram | rust | 10 | default | 404 [376–458] | 488 [460–567] | 4.88 | 293 [293–293] | 10 [10–10] | 1.20 |
| histogram | rust | 10 | single-thread | 466 [420–490] | 452 [418–468] | 4.52 | 52 [52–52] | 10 [10–10] | 1.20 |
| histogram | parquetgo | 1 | default | 67 [65–70] | 56 [44–57] | 5.56 | 34 [34–34] | 1 [1–1] | 0.29 |
| histogram | parquetgo | 1 | single-thread | 64 [61–72] | 52 [27–53] | 5.22 | 34 [34–34] | 1 [1–1] | 0.29 |
| histogram | parquetgo | 10 | default | 411 [397–427] | 498 [464–510] | 4.98 | 294 [294–294] | 10 [10–10] | 2.87 |
| histogram | parquetgo | 10 | single-thread | 462 [438–469] | 522 [438–555] | 5.22 | 57 [57–57] | 10 [10–10] | 2.87 |
| exponential_histogram | rust | 1 | default | 81 [73–88] | 73 [66–74] | 7.35 | 33 [33–33] | 1 [1–1] | 0.27 |
| exponential_histogram | rust | 1 | single-thread | 81 [75–90] | 75 [67–80] | 7.49 | 33 [33–33] | 1 [1–1] | 0.27 |
| exponential_histogram | rust | 10 | default | 472 [449–504] | 572 [527–595] | 5.72 | 333 [333–333] | 10 [10–10] | 2.67 |
| exponential_histogram | rust | 10 | single-thread | 510 [503–521] | 474 [449–487] | 4.74 | 54 [54–54] | 10 [10–10] | 2.67 |
| exponential_histogram | parquetgo | 1 | default | 68 [68–72] | 59 [58–60] | 5.94 | 35 [35–35] | 1 [1–1] | 0.44 |
| exponential_histogram | parquetgo | 1 | single-thread | 63 [62–65] | 57 [56–57] | 5.70 | 36 [36–36] | 1 [1–1] | 0.44 |
| exponential_histogram | parquetgo | 10 | default | 474 [460–497] | 556 [550–582] | 5.56 | 333 [333–333] | 10 [10–10] | 4.39 |
| exponential_histogram | parquetgo | 10 | single-thread | 507 [488–541] | 631 [480–671] | 6.31 | 59 [59–59] | 10 [10–10] | 4.39 |
| summary | rust | 1 | default | 60 [57–62] | 52 [50–54] | 5.22 | 27 [27–28] | 1 [1–1] | 0.13 |
| summary | rust | 1 | single-thread | 60 [59–60] | 45 [19–49] | 4.51 | 28 [28–28] | 1 [1–1] | 0.13 |
| summary | rust | 10 | default | 355 [333–412] | 408 [382–461] | 4.08 | 234 [234–234] | 10 [10–10] | 1.34 |
| summary | rust | 10 | single-thread | 421 [397–473] | 413 [390–431] | 4.13 | 45 [45–45] | 10 [10–10] | 1.34 |
| summary | parquetgo | 1 | default | 63 [59–74] | 53 [51–56] | 5.25 | 31 [30–31] | 1 [1–1] | 0.25 |
| summary | parquetgo | 1 | single-thread | 67 [54–69] | 56 [46–59] | 5.65 | 30 [30–30] | 1 [1–1] | 0.25 |
| summary | parquetgo | 10 | default | 342 [339–365] | 419 [404–432] | 4.19 | 234 [234–234] | 10 [10–10] | 2.53 |
| summary | parquetgo | 10 | single-thread | 423 [399–434] | 506 [429–531] | 5.06 | 50 [50–50] | 10 [10–10] | 2.53 |
