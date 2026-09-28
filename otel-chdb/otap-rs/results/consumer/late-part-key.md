# Central's partition key `(toDate(received_at), late_part)` through the real consumer (D34)

2026-09-28, ClickHouse 26.10.1.618, SeaweedFS, one box (4 vCPU, shared with
other agents' work). Dataset: `parquetgo/edge` `TestLatePartitionDataset`
through the real Go edge (late_split_after 15 min): 72 h of trace batches
every 10 s, 20 nodes × 10 spans, 1% of batches with 20 spans 15 min–24 h
old; 25,920 requests → 26,194 objects (274 late parts), 5,189,480 spans,
5,480 of them late. The real consumer (`consume run`, defaults: 32 objects a
statement) ingested it into each table (916 statements both times).
Windows: 24 five-minute windows over day 2 (one per hour, +1234 s),
`EXPLAIN indexes = 1 SELECT count() … WHERE Timestamp in the window [AND
ServiceName = 'svc-3']`, averaged.

## Parts read and granules per window

At the end of the ingest (background merges only):

```
== old-key (PARTITION BY toDate(received_at)): 14 parts, 5189480 rows, 147.01 MiB
   per partition: 2026-09-01:4, 2026-09-02:4, 2026-09-03:6
   5-min windows, day 2, Timestamp only:          3.37 parts, 353.8 granules
   5-min windows, day 2, and ServiceName = svc-3: 3.37 parts, 55.5 granules
== new-key (PARTITION BY (toDate(received_at), late_part)): 24 parts, 5189480 rows, 145.97 MiB
   per partition: ('2026-09-01',0):5 ('2026-09-01',1):2 ('2026-09-02',0):7 ('2026-09-02',1):2 ('2026-09-03',0):5 ('2026-09-03',1):3
   5-min windows, day 2, Timestamp only:          2.75 parts, 62.8 granules
   5-min windows, day 2, and ServiceName = svc-3: 1.00 parts, 9.9 granules
== migrated (the old-key table after migrate_late_part.py): 14 parts, 5189480 rows, 155.72 MiB
   5-min windows, day 2, Timestamp only:          3.58 parts, 192.2 granules
   5-min windows, day 2, and ServiceName = svc-3: 1.83 parts, 28.5 granules
```

After `OPTIMIZE TABLE … FINAL` (one part per partition):

```
== old-key:  3 parts, 172.20 MiB   1.91 parts, 431.2 granules   (svc-3: 1.91 parts, 65.1 granules)
== new-key:  6 parts, 173.50 MiB   2.75 parts, 229.7 granules   (svc-3: 1.00 parts, 34.0 granules)
== migrated: 6 parts, 172.01 MiB   2.75 parts, 213.7 granules   (svc-3: 1.00 parts, 33.0 granules)
```

## Insert and merge cost (server counters around each ingest; server-wide)

```
== new key: 240.7 s wall
  InsertQuery 1057, InsertedRows 5252703, InsertQueryTimeMicroseconds 112760374
  MergeTreeDataWriterBlocks 2017, Merge 549, MergedRows 16733525, MergeTotalMilliseconds 41459
== old key: 261.4 s wall
  InsertQuery 1057, InsertedRows 5252908, InsertQueryTimeMicroseconds 122451544
  MergeTreeDataWriterBlocks 2019, Merge 562, MergedRows 20114991, MergeTotalMilliseconds 50374
```

(InsertQuery and InsertedRows include the announcement statements and the
key/value rollup's view.)

## Migration (`scripts/migrate_late_part.py`)

The 60-hour old-key table (4,324,580 rows), the consumer ingesting 6 more
hours into it during `copy`:

```
copy:  17.7 s  {"days": 3, "copied": [2026-09-01 1729640 rows 8.53 s, 2026-09-02 1729980 rows 5.34 s, 2026-09-03 1037080 rows 3.59 s]}
       old 128.53 MiB, copy 135.91 MiB
final (consumer stopped): 3.9 s  {"copied": [2026-09-03 1297380 rows 3.56 s (whole day)], "skipped": 2, "exchange_s": 0.002, "rows": 4757000}
then the consumer on the new table: 6 more hours, late_part from metadata
```

Against a direct ingest of the same objects into a new-key table: rows
5,189,480 = 5,189,480; content keys 26,194 = 26,194; (content key, day,
late_part) groups differing: 0; late_part per key: 26,194 agree; the
rollup's ServiceName counts = rows (5,189,480); duplicates 0.

The delta path (`final` copies only the keys a copied day lacks), a
separate run: an old-key table of day 1 and part of day 3 (3,315,340 rows),
`copy` 5.8 s, `copy` again 0.15 s (both days skipped), 728 new objects
(144,160 rows) ingested into day 3 by the consumer, `final` 1.3 s
(`"delta": true`, 1.02 s) instead of the day's 1.73M rows; `final` again
after the exchange only verifies (0.26 s).
