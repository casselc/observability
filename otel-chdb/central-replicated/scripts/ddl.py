#!/usr/bin/env python3
"""ddl.py: the consumer's central tables as ReplicatedMergeTree, tiered to S3.

Takes the consumer's own DDL (`consume --print-ddl SIGNAL`, i.e. src/central.rs:
ClickStack 2.39.1's traces/logs tables from otap-rs/sql/otel_{traces,logs}.sql,
the metrics tables, sql/series_tables.sql, plus content_key and the by_content
projection) and rewrites, per table:

  ENGINE = MergeTree / AggregatingMergeTree
    -> ReplicatedMergeTree / ReplicatedAggregatingMergeTree(
         '/clickhouse/tables/{shard}/<db>/<table>', '{replica}')
  + TTL <t> + INTERVAL <move> TO VOLUME 'cold', <t> + INTERVAL <delete> DELETE
  + SETTINGS storage_policy = <policy>, allow_remote_fs_zero_copy_replication = <0|1>, ...

<t> is toDateTime(received_at) (the envelope's ingest time, also the partition
key's source), or LastSeen for the series table.

Then, for traces and logs, what the consumer's ensure() runs after the table
(`consume --print-rollups SIGNAL`: ClickStack's key-value rollup table
<table>_kv_rollup_15m and the materialized view that fills it), with

  ENGINE = SummingMergeTree
    -> ReplicatedSummingMergeTree('/clickhouse/tables/{shard}/<db>/<rollup>', '{replica}')
  + TTL Timestamp + INTERVAL <delete> DELETE    (the rollup is partitioned by
                                                 toDate(Timestamp), the 15-min bucket)

on the default storage policy (the rollup is small: one row per 15 minutes,
column and value), and the view's table names rewritten. Nothing is hand
copied, so a change to the consumer's DDL (an index dropped, a column added)
reaches the replicated DDL on the next run. --no-rollups leaves them out (the
consumer's --no-ddl then warns that the rollup is not maintained).

  ddl.py --db central --policy tiered_zc --zero-copy 1 --move '3 MINUTE' --delete '1 DAY' [--extra 'k = v, ...']

prints the statements, each ending in ';' (three per traces/logs signal, one
per metrics table). Apply with `clickhouse client --multiquery` on every
replica (not ON CLUSTER: the replicas are addressed one by one so a down
replica is visible; common.sh apply_sql). --extra goes to the main tables only.
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
ap.add_argument("--no-rollups", action="store_true", help="leave out the traces/logs key-value rollups")
a = ap.parse_args()


def consume(flag, sig):
    return subprocess.run([CONSUME, flag, sig], capture_output=True, text=True, check=True).stdout.strip()


def replicated(ddl, engine, path):
    """ENGINE = <engine> -> Replicated<engine>(path, '{replica}') (the first one)."""
    zk = f"'/clickhouse/tables/{{shard}}/{path}', '{{replica}}'"
    out, n = re.subn(rf"ENGINE = {engine}\b", f"ENGINE = Replicated{engine}({zk})", ddl, count=1)
    assert n == 1, f"no ENGINE = {engine} in {ddl[:200]}"
    return out


def with_ttl_settings(ddl, ttl, settings):
    """TTL before the final SETTINGS, and `settings` first in it. The
    non-replicated dedup window means nothing on a Replicated table; the
    replicated one (replicated_deduplication_window, 10000 by default) is left
    at its default. It also deduplicates the view's block of an exact retry
    on the rollup (deduplicate_blocks_in_dependent_materialized_views)."""
    ddl = re.sub(r"non_replicated_deduplication_window = 1000,? ?", "", ddl)
    ddl = re.sub(r"\n-- .*", "", ddl)  # the series table's comment sits before SETTINGS
    i = ddl.rfind("SETTINGS ")
    rest = ddl[i + len("SETTINGS "):].strip().rstrip(",")
    return ddl[:i] + ttl + "\nSETTINGS " + ", ".join(settings + ([rest] if rest else []))


out = [f"CREATE DATABASE IF NOT EXISTS {a.db}"]
for sig in a.signals.split(","):
    ddl = consume("--print-ddl", sig)
    m = re.match(r"CREATE TABLE IF NOT EXISTS db\.(\w+)", ddl)
    base = m.group(1)
    table = base + a.suffix
    # db.<base> and every name built on it (<base>_kv_rollup_15m, its view).
    rename = lambda sql: re.sub(rf"\bdb\.{base}(\w*)", lambda mm: f"{a.db}.{table}{mm.group(1)}", sql)
    ddl = rename(ddl)
    engine = "AggregatingMergeTree" if re.search(r"ENGINE = AggregatingMergeTree\b", ddl) else "MergeTree"
    ddl = replicated(ddl, engine, f"{a.db}/{table}")
    t = "LastSeen" if sig == "metrics_series" else "toDateTime(received_at)"
    ttl = f"TTL {t} + INTERVAL {a.move} TO VOLUME 'cold', {t} + INTERVAL {a.delete} DELETE"
    settings = [f"storage_policy = '{a.policy}'", f"allow_remote_fs_zero_copy_replication = {a.zero_copy}"]
    if a.extra:
        settings.append(a.extra)
    out.append(with_ttl_settings(ddl, ttl, settings))
    if a.no_rollups:
        continue
    for st in [x.strip() for x in consume("--print-rollups", sig).split(";\n") if x.strip()]:
        st = rename(st.rstrip(";"))
        if st.startswith("CREATE TABLE "):
            name = re.match(rf"CREATE TABLE IF NOT EXISTS {a.db}\.(\w+)", st).group(1)
            st = replicated(st, "SummingMergeTree", f"{a.db}/{name}")
            st = with_ttl_settings(st, f"TTL Timestamp + INTERVAL {a.delete} DELETE", [])
        out.append(st)
print(";\n\n".join(out) + ";")
