#!/usr/bin/env python3
"""Resource-attribute discovery served by the catalog, not by the insert path:

1. resource_kv: (Key, Value, resource_id) for every resource the catalog
   knows, sorted by (Key, Value): what a rewritten resource filter looks up
   (`resource_id IN (SELECT resource_id FROM resource_kv WHERE Key = k AND
   Value = v)`), and what a value search reads.
2. The key-value rollup rows HyperDX reads (ColumnIdentifier =
   'ResourceAttributes', one row per 15-minute bucket, key and value, count =
   live resources), written into each variant's `resource_kv_in_<signal>`
   (Null) and from there by the catalog materialized view into the rollup:
   a periodic job over the catalog (here: once for the window), where the
   ClickStack DDL would run a materialized view on every insert.

Records rows and time per bucket (results/discovery.jsonl) to size it for
the fleet.

  discovery.py [--dbs ent_b2] [--hours 3] [--end '2026-09-27 00:00:00']
"""
import argparse, json, time
import chlib as c

ap = argparse.ArgumentParser()
ap.add_argument("--cat", default="ent_cat")
ap.add_argument("--dbs", default="ent_b2")
ap.add_argument("--hours", type=int, default=3)
ap.add_argument("--end", default="2026-09-27 00:00:00")
ap.add_argument("--out", default=f"{c.HERE}/../results/discovery.jsonl")
a = ap.parse_args()
W1 = f"toDateTime('{a.end}', 'UTC')"
W0 = f"({W1} - toIntervalHour({a.hours}))"
CAT = a.cat
# the flat covered sets of every resource the catalog knows (controller rows + the caught-up ones)
FLAT = (f"(SELECT resource_id, attrs, valid_from, valid_to FROM {CAT}.resources WHERE resource_id IN (SELECT resource_id FROM {CAT}.res_index) "
        f"UNION ALL SELECT resource_id, attrs, valid_from, valid_to FROM ent_src.res_all WHERE resource_id IN (SELECT resource_id FROM {CAT}.res_index))")
rec = dict(ts=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), load=c.load())
c.q(f"CREATE TABLE IF NOT EXISTS {CAT}.resource_kv (Key LowCardinality(String), Value String, resource_id UInt64) "
    f"ENGINE = ReplacingMergeTree ORDER BY (Key, Value, resource_id)")
c.q(f"TRUNCATE TABLE {CAT}.resource_kv")
t = time.time()
c.q(f"INSERT INTO {CAT}.resource_kv SELECT kv.1, kv.2, resource_id FROM {FLAT} ARRAY JOIN CAST(attrs, 'Array(Tuple(String, String))') AS kv")
c.q(f"OPTIMIZE TABLE {CAT}.resource_kv FINAL")
rec["resource_kv"] = dict(s=round(time.time() - t, 2), **c.rows(
    f"SELECT sum(rows) AS rows, sum(bytes_on_disk) AS bytes FROM system.parts WHERE database = '{CAT}' AND table = 'resource_kv' AND active")[0],
    resources=int(c.one(f"SELECT uniqExact(resource_id) FROM {CAT}.resource_kv")))
BUCKETS = (f"SELECT toStartOfFifteenMinutes(b) AS Timestamp, 'ResourceAttributes' AS ColumnIdentifier, kv.1 AS Key, kv.2 AS Value, count() AS count "
           f"FROM {FLAT} ARRAY JOIN arrayMap(i -> toDateTime({W0}) + toIntervalMinute(15 * i), range(toUInt32({a.hours} * 4))) AS b "
           f"ARRAY JOIN CAST(attrs, 'Array(Tuple(String, String))') AS kv "
           f"WHERE valid_from < b + toIntervalMinute(15) AND valid_to > b GROUP BY Timestamp, Key, Value")
for db in a.dbs.split(","):
    for sig in ("traces", "logs"):
        t = time.time()
        c.q(f"INSERT INTO {db}.resource_kv_in_{sig} {BUCKETS}", max_bytes_before_external_group_by=1_000_000_000)
        rec[f"{db}.{sig}"] = dict(s=round(time.time() - t, 2), rollup_rows=int(c.one(
            f"SELECT count() FROM {db}.otel_{sig}_kv_rollup_15m WHERE ColumnIdentifier = 'ResourceAttributes'")))
rec["per_bucket"] = c.rows(f"SELECT Timestamp, count() AS rows, uniqExact(Key) AS keys FROM {a.dbs.split(',')[0]}.otel_logs_kv_rollup_15m "
                           f"WHERE ColumnIdentifier = 'ResourceAttributes' GROUP BY Timestamp ORDER BY Timestamp")
print(json.dumps(rec, indent=1))
with open(a.out, "a") as f:
    f.write(json.dumps(rec) + "\n")
