#!/usr/bin/env python3
"""ddl.py: the consumer's central tables as ReplicatedMergeTree, tiered to S3.

Takes the consumer's own DDL (`consume --print-ddl SIGNAL`, i.e. src/central.rs
and sql/series_tables.sql plus content_key and the by_content projection) and
rewrites, per table:

  ENGINE = MergeTree / AggregatingMergeTree
    -> ReplicatedMergeTree / ReplicatedAggregatingMergeTree(
         '/clickhouse/tables/{shard}/<db>/<table>', '{replica}')
  + TTL <t> + INTERVAL <move> TO VOLUME 'cold', <t> + INTERVAL <delete> DELETE
  + SETTINGS storage_policy = <policy>, allow_remote_fs_zero_copy_replication = <0|1>, ...

<t> is toDateTime(received_at) (the envelope's ingest time, also the partition
key's source), or LastSeen for the series table.

  ddl.py --db central --policy tiered_zc --zero-copy 1 --move '3 MINUTE' --delete '1 DAY' [--extra 'k = v, ...']

prints one statement per line-group, separated by ';\n'. apply with
scripts/apply_ddl.sh on every replica (not ON CLUSTER: the replicas are
addressed one by one so a down replica is visible).
"""
import argparse
import re
import subprocess

SIGNALS = ["traces", "logs", "metrics_series", "metrics_number_points", "metrics_histogram_points",
           "metrics_exponential_histogram_points", "metrics_summary_points"]
CONSUME = "/tmp/claude-0/-home-user/db86342c-d57d-54b7-95b1-90f220828b73/scratchpad/otap-rs-target/release/consume"

ap = argparse.ArgumentParser()
ap.add_argument("--db", required=True)
ap.add_argument("--policy", default="tiered_zc")
ap.add_argument("--zero-copy", default="1")
ap.add_argument("--move", default="3 MINUTE")
ap.add_argument("--delete", default="1 DAY")
ap.add_argument("--extra", default="")
ap.add_argument("--signals", default=",".join(SIGNALS))
ap.add_argument("--suffix", default="", help="appended to every table name")
a = ap.parse_args()

out = [f"CREATE DATABASE IF NOT EXISTS {a.db}"]
for sig in a.signals.split(","):
    ddl = subprocess.run([CONSUME, "--print-ddl", sig], capture_output=True, text=True, check=True).stdout.strip()
    m = re.match(r"CREATE TABLE IF NOT EXISTS db\.(\w+)", ddl)
    table = m.group(1) + a.suffix
    ddl = ddl.replace(f"db.{m.group(1)}", f"{a.db}.{table}", 1)
    zk = f"'/clickhouse/tables/{{shard}}/{a.db}/{table}', '{{replica}}'"
    ddl = re.sub(r"ENGINE = (Aggregating)?MergeTree", lambda mm: f"ENGINE = Replicated{mm.group(1) or ''}MergeTree({zk})", ddl, count=1)
    t = "LastSeen" if sig == "metrics_series" else "toDateTime(received_at)"
    ttl = f"TTL {t} + INTERVAL {a.move} TO VOLUME 'cold', {t} + INTERVAL {a.delete} DELETE"
    settings = [f"storage_policy = '{a.policy}'", f"allow_remote_fs_zero_copy_replication = {a.zero_copy}"]
    if a.extra:
        settings.append(a.extra)
    # The non-replicated dedup window means nothing on a Replicated table; the
    # replicated one (replicated_deduplication_window) is left at its default.
    ddl = re.sub(r"non_replicated_deduplication_window = 1000,? ?", "", ddl)
    ddl = re.sub(r"\n-- .*", "", ddl)  # the series table's comment sits before SETTINGS
    i = ddl.rfind("SETTINGS ")
    rest = ddl[i + len("SETTINGS "):].strip().rstrip(",")
    ddl = ddl[:i] + ttl + "\nSETTINGS " + ", ".join(settings + ([rest] if rest else []))
    out.append(ddl)
print(";\n".join(out) + ";")
