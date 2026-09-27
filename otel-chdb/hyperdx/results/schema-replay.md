# Schema comparison: HyperDX 2.39.1 on the pre-alignment vs the ClickStack DDL (scripts/schema_replay.py)

hdx_old / hdx_new on a private ClickHouse 26.10.1.618 (:18723): the same hdxgen objects (3 h, 17,216 spans, 5,811 logs) through two real consumers, plus a haystack of 3.0 M spans and 3.0 M logs (the bench objects shifted into the same 3 h, the same rows in both). HyperDX's statements were captured (chproxy) from hdx_ui.js runs on the small data, one source pair per schema; each side's statements are replayed on its own database, 5 rounds after a warm-up, sides interleaved, server time = median elapsed. Loaded box.

## Replay on 3 M rows per signal (run 2, load 10.8 -> 18.9)

| scenario | statements old / new | server ms old / new (sum of medians) | ratio | rows read old / new | search result same | errors |
|---|---:|---:|---:|---:|---|---:|
| logs-all | 13 / 17 | 1364 / 314 | 0.23 | 7,119,479 / 6,467,227 | 2 of 2 (201 rows) | 0 / 0 |
| logs-fulltext | 16 / 21 | 1666 / 242 | 0.15 | 12,896,821 / 3,192,962 | 5 of 5 (35 rows) | 0 / 0 |
| logs-attr | 14 / 19 | 1502 / 225 | 0.15 | 9,849,564 / 3,263,546 | 2 of 2 (84 rows) | 0 / 0 |
| logs-res-attr | 16 / 21 | 1429 / 278 | 0.19 | 13,277,837 / 3,397,088 | 4 of 4 (51 rows) | 0 / 0 |
| logs-sql | 16 / 21 | 1486 / 251 | 0.17 | 13,599,383 / 3,192,962 | 5 of 5 (35 rows) | 0 / 0 |
| traces-all | 13 / 16 | 2636 / 217 | 0.08 | 6,976,327 / 6,689,062 | 2 of 2 (201 rows) | 0 / 0 |
| traces-errors | 17 / 20 | 2256 / 252 | 0.11 | 15,644,273 / 12,551,187 | 5 of 5 (801 rows) | 0 / 0 |
| traces-res-attr | 14 / 17 | 2575 / 210 | 0.08 | 9,930,787 / 3,081,270 | 2 of 2 (149 rows) | 0 / 0 |
| trace-waterfall | 22 / 26 | 2128 / 360 | 0.17 | 10,382,784 / 6,794,213 | 2 of 2 (201 rows) | 0 / 0 |
| **all** | 141 / 178 | **17043 / 2349** | 0.14 | 99,677,255 / 48,629,517 | | |

| statement kind | old: n, ms, rows read | new: n, ms, rows read |
|---|---:|---:|
| text-index key discovery (mergeTreeTextIndex) | 0, 0, 0 | 34, 259, 3,249 |
| parts overlap (system.parts, for the above) | 0, 0, 0 | 34, 259, 3,249 |
| kv rollup | 0, 0, 0 | 16, 100, 6,351 |
| items filter (has(*AttributeItems)) | 0, 0, 0 | 23, 234, 832,745 |
| full text: hasAllTokens | 0, 0, 0 | 9, 86, 186,262 |
| full text: hasToken (bloom-filter era) | 8, 375, 9,181,827 | 0, 0, 0 |
| key discovery by sampling the map (sampledKeys) | 23, 326, 4,682,574 | 0, 0, 0 |
| value discovery by sampling (groupUniqArray) | 13, 14474, 39,046,084 | 31, 1075, 27,081,085 |

Run 1 (load 9.5 -> 11.7): all 9 scenarios 11,915 / 2,093 ms (0.18); logs 0.17-0.30, traces 0.11-0.21.

Result lists: every result-list statement both sides sent for the same window and page (29 pairs) returns the same rows on the columns both select.

## Index use (EXPLAIN indexes = 1, 3 h window, 3 M logs)

| statement | old granules | new granules |
|---|---:|---:|
| full text `hasAllTokens(lower(Body), 'card declined')` (old: `hasToken` x2) | 380 / 380 (no index) | 6 / 383 (`idx_lower_body`, text) |
| `has(ResourceAttributeItems, 'k8s.pod.name=…')` (old: `ResourceAttributes['k8s.pod.name'] = …`) | 380 / 380 | 16 / 383 (`idx_res_attr_items`) |

## The UI on the small data only (23 k rows, proxy wall ms, 2 rounds each)

| scenario | statements old / new | ms old / new |
|---|---:|---:|
| logs-all | 38 / 49 | 405 / 544 |
| logs-fulltext | 46 / 56 | 468 / 818 |
| logs-attr | 42 / 52 | 494 / 587 |
| logs-res-attr | 46 / 55 | 677 / 801 |
| logs-sql | 44 / 54 | 469 / 681 |
| traces-all | 37 / 44 | 409 / 540 |
| traces-errors | 48 / 53 | 488 / 768 |
| traces-res-attr | 42 / 48 | 751 / 889 |
| trace-waterfall | 59 / 64 | 897 / 1268 |
| all | 402 / 475 | 5057 / 6897 |
