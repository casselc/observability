# Schema comparison, three ways: HyperDX 2.39.1 live on old / full ClickStack / option 2 (scripts/schema_replay.py)

hdx_old (pre-alignment), hdx_full (ClickStack 2.39.1's full DDL), hdx_new (the consumer's, option 2: no idx_*_attr_key) on the shared ClickHouse 26.10.1.618 (:18123): the same hdxgen objects (3 h, 17,216 spans, 5,811 logs) and a haystack of 3.0 M spans and 3.0 M logs (bench objects shifted into the 3 h), loaded in the consumer's statement shape (scripts/schema_load.py). Same rows in all three: count and sum(cityHash64(every written column)) equal (traces 3,017,216 / 14810277793986098504, logs 3,005,811 / 10069122824569677808).

HyperDX 2.39.1 ran live (Docker), one Logs + Traces source pair per schema (scripts/hdx_setup.py --schema old=…,full=…,new=…), nine scenarios driven by hdx_ui.js twice per schema (old1 full1 new1 new2 full2 old2). HyperDX picked its paths at runtime from each table's indexes. **Map key discovery on option 2 took the items path** (metadata.ts getMapKeys: no key index, so the kv one): every one of the 46 `SELECT token AS key FROM mergeTreeTextIndex(…, 'idx_*_attr_key')` statements HyperDX sent for the full DDL was sent for option 2 as `SELECT splitByString('=', token)[1] AS key FROM mergeTreeTextIndex(…, 'idx_*_attr_items')` (res 18, log 10, scope 10, span 8), and the discovered keys are the same sets for every map column. Everything else HyperDX sent is the same SQL for both.

The first round's statements of each side, replayed on its own database, 5 rounds after a warm-up, sides rotating, max_threads 2, max_memory_usage 2 GB:

| scenario | statements old / full / new | server ms old / full / new (sum of medians) | rows read old / full / new | result lists full = old | result lists new = old | errors |
|---|---:|---:|---:|---|---|---:|
| logs-all | 13 / 17 / 17 | 1255 / 265 / 252 | 10,997,602 / 6,467,241 / 6,458,237 | 2 of 2 (200 rows) | 2 of 2 (200 rows) | 0 / 0 / 0 |
| logs-fulltext | 16 / 20 / 20 | 1409 / 251 / 249 | 16,282,898 / 3,462,578 / 3,414,113 | 5 of 5 (35 rows) | 5 of 5 (35 rows) | 0 / 0 / 0 |
| logs-attr | 14 / 18 / 18 | 1419 / 206 / 198 | 14,266,161 / 3,547,301 / 3,500,602 | 2 of 2 (84 rows) | 2 of 2 (84 rows) | 0 / 0 / 0 |
| logs-res-attr | 16 / 20 / 20 | 1477 / 250 / 227 | 17,080,439 / 4,185,317 / 4,095,250 | 4 of 4 (51 rows) | 4 of 4 (51 rows) | 0 / 0 / 0 |
| logs-sql | 16 / 20 / 20 | 1498 / 253 / 266 | 17,624,870 / 3,462,578 / 3,415,280 | 5 of 5 (35 rows) | 5 of 5 (35 rows) | 0 / 0 / 0 |
| traces-all | 13 / 16 / 16 | 2544 / 217 / 207 | 7,359,313 / 6,644,628 / 6,651,378 | 2 of 2 (200 rows) | 2 of 2 (200 rows) | 0 / 0 / 0 |
| traces-errors | 14 / 17 / 17 | 2477 / 176 / 168 | 10,484,882 / 6,610,409 / 6,630,833 | 2 of 2 (200 rows) | 2 of 2 (200 rows) | 0 / 0 / 0 |
| traces-res-attr | 14 / 16 / 17 | 2585 / 188 / 413 | 10,276,302 / 3,082,909 / 6,627,242 | 2 of 2 (142 rows) | 2 of 2 (142 rows) | 0 / 0 / 0 |
| trace-waterfall | 23 / 26 / 26 | 2369 / 313 / 313 | 10,004,271 / 6,766,732 / 6,755,230 | 2 of 2 (200 rows) | 2 of 2 (200 rows) | 0 / 0 / 0 |
| **all** | 139 / 170 / 171 | **17034 / 2118 / 2293** | 114,376,738 / 44,229,693 / 47,548,165 | | | |

| statement kind | old: n, ms, rows read | full: n, ms, rows read | new: n, ms, rows read |
|---|---:|---:|---:|
| map key discovery: mergeTreeTextIndex on a mapKeys index | 0, 0, 0 | 23, 128, 334 | 0, 0, 0 |
| map key discovery: mergeTreeTextIndex on an items index (split at '=') | 0, 0, 0 | 0, 0, 0 | 23, 146, 55,703 |
| map values: mergeTreeTextIndex on an items index | 0, 0, 0 | 11, 175, 56,175 | 11, 178, 42,395 |
| parts overlap (system.parts, for the above) | 0, 0, 0 | 34, 303, 56,509 | 34, 324, 98,098 |
| kv rollup | 0, 0, 0 | 11, 79, 6,013 | 11, 80, 6,013 |
| items filter (has(*AttributeItems)) | 0, 0, 0 | 23, 268, 2,174,479 | 23, 256, 1,990,177 |
| full text: hasAllTokens | 0, 0, 0 | 9, 111, 455,910 | 9, 110, 408,598 |
| full text: hasToken (no text index) | 8, 263, 9,180,605 | 0, 0, 0 | 0, 0, 0 |
| map key discovery by sampling the map (sampledKeys) | 23, 648, 26,198,442 | 0, 0, 0 | 0, 0, 0 |
| value discovery by sampling (groupUniqArray over rows) | 13, 14643, 39,059,561 | 9, 631, 27,053,379 | 9, 606, 27,095,389 |

Box load before: 9.93 7.15 25.10 4/1070 4372 after: 6.06 6.64 22.31 4/1081 5598

traces-res-attr, option 2: the UI sent one more statement in that run than for the full DDL (a StatusCode histogram, 175 ms, 2.7 M rows: page timing, not the schema); without it the scenario is 238 ms. Over the nine scenarios option 2 and the full DDL are the same (2,118 ms each without that statement), 0.12× the pre-alignment tables.
