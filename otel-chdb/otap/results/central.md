| signal | layout | batches | settings | wall ms | query CPU ms | peak mem MB | S3 GET | S3 HEAD | MB read |
| traces | ref | 1 | default | 54 [52–91] | 42 [41–50] | 24 [24–24] | 1 [1–19] | 1 [1–1] | 0 [0–0] |
| traces | ref | 1 | single-thread | 40 [38–41] | 34 [34–37] | 25 [25–25] | 1 [1–1] | 1 [1–1] | 0 [0–0] |
| traces | ref | 10 | default | 487 [452–523] | 300 [291–322] | 216 [215–218] | 28 [28–28] | 10 [10–10] | 3 [3–3] |
| traces | ref | 10 | single-thread | 450 [426–649] | 290 [282–291] | 38 [38–40] | 28 [28–28] | 10 [10–10] | 3 [3–3] |
| traces | star | 1 | default | 203 [200–660] | 164 [163–871] | 44 [44–54] | 25 [25–25] | 7 [7–7] | 0 [0–0] |
| traces | star | 1 | single-thread | 179 [159–192] | 142 [131–143] | 40 [40–40] | 7 [7–7] | 7 [7–7] | 0 [0–0] |
| traces | star | 10 | default | 1021 [915–1182] | 1089 [1077–1141] | 288 [286–291] | 106 [88–112] | 70 [70–70] | 5 [5–5] |
| traces | star | 10 | single-thread | 1589 [1291–1827] | 1166 [1068–1263] | 134 [134–134] | 124 [106–124] | 70 [70–70] | 5 [5–5] |
| traces | flat-parquet | 1 | default | 82 [61–407] | 106 [39–478] | 24 [24–24] | 1 [1–37] | 1 [1–1] | 0 [0–0] |
| traces | flat-parquet | 1 | single-thread | 44 [42–46] | 39 [39–97] | 25 [25–25] | 1 [1–1] | 1 [1–1] | 0 [0–0] |
| traces | flat-parquet | 10 | default | 572 [393–830] | 370 [286–422] | 215 [215–215] | 28 [28–46] | 10 [10–10] | 4 [4–4] |
| traces | flat-parquet | 10 | single-thread | 663 [286–754] | 289 [278–305] | 39 [39–39] | 28 [10–28] | 10 [10–10] | 4 [4–4] |
| traces | flat-arrow | 1 | default | 76 [58–115] | 98 [48–185] | 26 [26–26] | 4 [4–4] | 1 [1–1] | 3 [3–3] |
| traces | flat-arrow | 1 | single-thread | 287 [184–541] | 203 [138–225] | 35 [35–35] | 22 [4–40] | 1 [1–1] | 3 [3–3] |
| traces | flat-arrow | 10 | default | 823 [540–1651] | 490 [361–594] | 218 [218–218] | 76 [40–94] | 10 [10–10] | 30 [30–30] |
| traces | flat-arrow | 10 | single-thread | 610 [503–681] | 492 [470–522] | 51 [51–51] | 76 [40–76] | 10 [10–10] | 30 [30–30] |
| traces | raw | 1 | single-thread | 302 [250–443] | 217 [211–341] | 56 [56–64] | 25 [7–25] | 14 [14–14] | 0 [0–0] |
| traces | raw | 10 | single-thread | 2218 [1711–3218] | 1548 [1432–1751] | 458 [458–460] | 142 [142–192] | 77 [77–78] | 1 [1–1] |
| logs | ref | 1 | default | 29 [29–34] | 28 [27–31] | 15 [15–19] | 1 [1–1] | 1 [1–1] | 0 [0–0] |
| logs | ref | 1 | single-thread | 29 [28–30] | 26 [26–28] | 16 [16–16] | 1 [1–1] | 1 [1–1] | 0 [0–0] |
| logs | ref | 10 | default | 239 [223–410] | 206 [172–207] | 152 [149–152] | 10 [10–28] | 10 [10–10] | 2 [2–2] |
| logs | ref | 10 | single-thread | 292 [212–322] | 200 [187–201] | 24 [24–24] | 28 [10–28] | 10 [10–10] | 2 [2–2] |
| logs | star | 1 | default | 234 [166–253] | 231 [214–361] | 29 [22–40] | 4 [4–22] | 4 [4–4] | 0 [0–0] |
| logs | star | 1 | single-thread | 110 [100–151] | 93 [93–119] | 25 [25–25] | 4 [4–22] | 4 [4–4] | 0 [0–0] |
| logs | star | 10 | default | 742 [538–764] | 638 [631–658] | 181 [179–187] | 58 [58–76] | 40 [40–40] | 4 [4–4] |
| logs | star | 10 | single-thread | 935 [892–959] | 842 [823–993] | 64 [64–64] | 76 [58–76] | 40 [40–40] | 4 [4–4] |
| logs | flat-parquet | 1 | default | 29 [28–37] | 79 [26–105] | 19 [16–19] | 1 [1–1] | 1 [1–1] | 0 [0–0] |
| logs | flat-parquet | 1 | single-thread | 46 [37–57] | 67 [32–73] | 16 [16–16] | 1 [1–1] | 1 [1–1] | 0 [0–0] |
| logs | flat-parquet | 10 | default | 218 [161–348] | 194 [186–233] | 151 [150–151] | 28 [10–28] | 10 [10–10] | 2 [2–2] |
| logs | flat-parquet | 10 | single-thread | 265 [261–267] | 209 [208–236] | 26 [24–26] | 10 [10–28] | 10 [10–10] | 2 [2–2] |
| logs | flat-arrow | 1 | default | 90 [55–128] | 31 [29–72] | 16 [16–16] | 1 [1–1] | 1 [1–1] | 1 [1–1] |
| logs | flat-arrow | 1 | single-thread | 53 [33–59] | 31 [30–82] | 22 [22–22] | 1 [1–19] | 1 [1–1] | 1 [1–1] |
| logs | flat-arrow | 10 | default | 240 [214–354] | 289 [191–310] | 149 [149–149] | 10 [10–28] | 10 [10–10] | 10 [10–10] |
| logs | flat-arrow | 10 | single-thread | 285 [218–572] | 201 [194–266] | 31 [31–31] | 28 [10–46] | 10 [10–10] | 10 [10–10] |
| logs | raw | 1 | single-thread | 168 [143–351] | 232 [126–446] | 33 [33–36] | 22 [4–22] | 8 [8–8] | 0 [0–0] |
| logs | raw | 10 | single-thread | 877 [797–879] | 744 [736–819] | 270 [270–273] | 58 [58–76] | 44 [44–44] | 1 [1–1] |
| signal | layout | retry | rows after 2 attempts (want 100000) |
| traces | ref | default settings both times | 100000 |
| traces | ref | single-thread both times | 100000 |
| traces | ref | max_threads 1 then default | 100000 |
| traces | star | default settings both times | 100000 |
| traces | star | single-thread both times | 100000 |
| traces | star | max_threads 1 then default | 100000 |
| traces | flat-parquet | default settings both times | 100000 |
| traces | flat-parquet | single-thread both times | 100000 |
| traces | flat-parquet | max_threads 1 then default | 100000 |
| traces | flat-arrow | default settings both times | 100000 |
| traces | flat-arrow | single-thread both times | 100000 |
| traces | flat-arrow | max_threads 1 then default | 100000 |
| traces | raw | single-thread both times | 100000 |
| logs | ref | default settings both times | 100000 |
| logs | ref | single-thread both times | 100000 |
| logs | ref | max_threads 1 then default | 100000 |
| logs | star | default settings both times | 100000 |
| logs | star | single-thread both times | 100000 |
| logs | star | max_threads 1 then default | 100000 |
| logs | flat-parquet | default settings both times | 100000 |
| logs | flat-parquet | single-thread both times | 100000 |
| logs | flat-parquet | max_threads 1 then default | 100000 |
| logs | flat-arrow | default settings both times | 100000 |
| logs | flat-arrow | single-thread both times | 100000 |
| logs | flat-arrow | max_threads 1 then default | 100000 |
| logs | raw | single-thread both times | 100000 |
--- PASS: TestCentralIngestCost (100.61s)
PASS
ok  	github.com/chucklehead-dev/oscope/spike/otel-chdb/otap	100.650s
