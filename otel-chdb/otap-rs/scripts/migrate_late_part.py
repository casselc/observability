#!/usr/bin/env python3
"""migrate_late_part.py: move a central traces or logs table from
PARTITION BY toDate(received_at) to PARTITION BY (toDate(received_at),
late_part) (DECISIONS.md D34), restartably and without breaking the
consumer's exactly-once ingest.

ClickHouse cannot change a table's partition key in place, and ATTACH
PARTITION FROM needs the same key on both tables, so the data is copied
into a new table, day by day, and the two tables are swapped atomically
(EXCHANGE TABLES, an Atomic database). The phases, each safe to run again
(a crash anywhere is recovered by running the same phase again):

  copy    ONLINE, consumers running. Creates <table>_pk2 from the table's own
          SHOW CREATE TABLE (so an operator's TTL, storage policy and
          settings carry over), with the `late_part` column and the new key
          (a Replicated engine gets <path>_pk2 as its Keeper path), then
          copies every day whose per-content-key counts differ between the
          two tables (DROP the day's partitions in the copy, INSERT SELECT
          the day). A day that already matches is skipped, which is what
          makes the phase restartable.
  final   Consumers PAUSED (scaled to zero, and every statement they may
          have in flight settled: wait the lease TTL plus the insert
          budget plus the Keeper slack after the last worker stopped, or
          check system.processes). Copies again what differs: in a day the
          consumers wrote to during `copy` (today, and any day a replay
          landed in) only the content keys the copy lacks (their new
          objects: an object is one part, so it is all there or absent),
          or the whole day if a key's count differs (a duplicate the
          consumers inserted later); drops days the old table no longer has
          (TTL), checks every day matches, then EXCHANGE TABLES.
          Restartable: after the exchange it only re-verifies.
  verify  After the exchange: every day's per-content-key counts are the
          same in <table> (new key) and <table>_pk2 (the old table).
  drop-old  DROP TABLE <table>_pk2, once `verify` passed and the operator
          no longer wants the old data.

Why the pause (and why only for `final`): the consumer's exactly-once
rests on its count check (content key -> rows in the objects' days) seeing
every row it has ever inserted. Rows inserted into the old table after a
day was copied and before the exchange would vanish from the live table;
if the consumer had already verified them and moved its checkpoint past
them (in S3, not in ClickHouse), nothing would ever insert them again: a
silent loss. So the last copy and the exchange happen with no statement
able to land, and the consumer's own state needs no change: its
checkpoints, leases and fences are in S3 and name slots, not tables, and
after the exchange every (content key, day) count it can ask for is the
one it would have read before. The dedup window (a table setting) starts
empty on the new table: harmless, since the consumer checks before every
insert and the dedup token is only its backstop. The rollup's
materialized view follows the table's name, so the copy does not feed it
twice and the new table feeds it after the exchange [M, 26.10].

Which rows are late: a table from before D34 does not know which object a
row came from. A content key is a late part iff every one of its rows'
event times is more than --late-after (the edges' late_split_after, 15
min by default) before its received_at: `max(Timestamp) < min(received_at)
- late_after`, per content key, so a key is never split across the two
partitions. Objects written before the edges split (D31) have late and
bulk rows mixed and stay bulk, which is what they are. The column only
steers partitioning: a misclassified key costs pruning, never a row.

  migrate_late_part.py --ch http://127.0.0.1:8123 --db otel --table otel_traces copy
  migrate_late_part.py --ch ... --db otel --table otel_traces final --consumers-paused
  migrate_late_part.py --ch ... --db otel --table otel_traces verify
  migrate_late_part.py --ch ... --db otel --table otel_traces drop-old --yes

Timing and progress go to stderr; `--json` prints the phase's report.
"""
import argparse
import json
import re
import sys
import time
import urllib.parse
import urllib.request

OLD_KEY = "toDate(received_at)"
NEW_KEY = "(toDate(received_at), late_part)"
SUFFIX = "_pk2"
LATE_COLUMN = "`late_part` UInt8 CODEC(ZSTD(1))"
# `final` copies at most this many new content keys of a day by name (a
# statement of ~40 bytes a key); a day with more is copied whole.
DELTA_MAX_KEYS = 20000


class CH:
    def __init__(self, url, user=None, password=None):
        self.url = url.rstrip("/") + "/"
        self.user, self.password = user, password

    def q(self, sql, **settings):
        params = {k: str(v) for k, v in settings.items()}
        req = urllib.request.Request(self.url + "?" + urllib.parse.urlencode(params), data=sql.encode(), method="POST")
        if self.user:
            req.add_header("X-ClickHouse-User", self.user)
            req.add_header("X-ClickHouse-Key", self.password or "")
        try:
            with urllib.request.urlopen(req, timeout=3600) as r:
                return r.read().decode()
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"clickhouse {e.code}: {e.read().decode()[:2000]}\n  in: {sql[:400]}") from None

    def rows(self, sql, **settings):
        out = self.q(sql + " FORMAT TSV", **settings)
        return [l.split("\t") for l in out.splitlines() if l]


def sq(s):
    return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"


def log(msg):
    print(f"{time.strftime('%H:%M:%S')} {msg}", file=sys.stderr, flush=True)


def partition_key(ch, db, t):
    r = ch.rows(f"SELECT partition_key FROM system.tables WHERE database = {sq(db)} AND name = {sq(t)}")
    return r[0][0] if r else None


def norm(k):
    return re.sub(r"\s+", "", k or "")


def new_ddl(show_create, db, table):
    """The copy's DDL from the old table's SHOW CREATE TABLE: the new name,
    the late_part column after content_key, the new key, and a Replicated
    engine's Keeper path suffixed. Refuses anything it does not recognise
    exactly once (a silent half-rewrite would be worse than none)."""
    ddl = show_create.replace("\\n", "\n").replace("\\'", "'")
    fq, nfq = f"{db}.{table}", f"{db}.{table}{SUFFIX}"
    for pat in (f"CREATE TABLE {fq}\n", f"CREATE TABLE {fq} "):
        if ddl.startswith(pat):
            ddl = f"CREATE TABLE IF NOT EXISTS {nfq}" + ddl[len(pat) - 1:]
            break
    else:
        raise SystemExit(f"unexpected SHOW CREATE TABLE head: {ddl[:120]!r}")
    m = re.findall(r"\n(\s*)`content_key` LowCardinality\(String\)[^\n]*,\n", ddl)
    if len(m) != 1:
        raise SystemExit("no single `content_key` column line: not a consumer table")
    ddl = re.sub(r"(\n(\s*)`content_key` LowCardinality\(String\)[^\n]*,\n)", lambda x: x.group(1) + x.group(2) + LATE_COLUMN + ",\n", ddl, count=1)
    if ddl.count(f"PARTITION BY {OLD_KEY}\n") != 1:
        raise SystemExit(f"no single PARTITION BY {OLD_KEY}")
    ddl = ddl.replace(f"PARTITION BY {OLD_KEY}\n", f"PARTITION BY {NEW_KEY}\n")
    rep = re.findall(r"ENGINE = Replicated\w*MergeTree\('([^']+)'", ddl)
    if len(rep) > 1:
        raise SystemExit("more than one Replicated engine clause")
    if rep:
        ddl = ddl.replace(f"('{rep[0]}'", f"('{rep[0]}{SUFFIX}'", 1)
    return ddl


def days(ch, fq):
    """The days (Date strings) that have rows. Read from `_partition_value`,
    never by parsing `system.parts.partition` (whose text differs between
    the two keys: a parse is where a day would be silently skipped)."""
    return sorted(r[0] for r in ch.rows(f"SELECT toString(_partition_value.1) AS d FROM {fq} GROUP BY d"))


def counts(ch, fq, day):
    """content key -> rows on one day, from the by_content projection."""
    rows = ch.rows(f"SELECT content_key, count() FROM {fq} WHERE _partition_value.1 = toDate({sq(day)}) GROUP BY content_key",
                   optimize_use_projections=1)
    return {k: int(n) for k, n in rows}


def insert_columns(ch, db, t):
    """The columns an INSERT can name (not MATERIALIZED, ALIAS or EPHEMERAL), in table order."""
    return [r[0] for r in ch.rows(
        f"SELECT name FROM system.columns WHERE database = {sq(db)} AND table = {sq(t)} "
        f"AND default_kind NOT IN ('MATERIALIZED', 'ALIAS', 'EPHEMERAL') ORDER BY position")]


def copy_day(ch, db, table, day, late_after_s, threads, keys=None):
    """Copy one day: whole (its two partitions in the copy dropped first), or
    only `keys`, content keys the copy does not hold at all (the rows the
    consumers inserted after the day was copied: an object's rows are one
    part, so a key is either all there or absent)."""
    old, new = f"{db}.{table}", f"{db}.{table}{SUFFIX}"
    t0 = time.time()
    only = ""
    if keys is None:
        for lp in (0, 1):
            ch.q(f"ALTER TABLE {new} DROP PARTITION ({sq(day)}, {lp})")
    else:
        only = f" AND content_key IN ({', '.join(sq(k) for k in sorted(keys))})"
    cols = [c for c in insert_columns(ch, db, table) if c != "late_part"]
    names = ", ".join(f"`{c}`" for c in cols)
    ts = "Timestamp"
    late_keys = (f"SELECT content_key FROM {old} WHERE _partition_value.1 = toDate({sq(day)}){only} GROUP BY content_key "
                 f"HAVING max({ts}) < min(received_at) - toIntervalSecond({int(late_after_s)})")
    ch.q(f"INSERT INTO {new} ({names}, late_part) SELECT {names}, toUInt8(content_key IN ({late_keys})) FROM {old} "
         f"WHERE _partition_value.1 = toDate({sq(day)}){only}",
         insert_deduplicate=0, max_insert_threads=threads, max_threads=threads, max_partitions_per_insert_block=10,
         max_query_size=4 << 20)
    return time.time() - t0


def compare(ch, db, table, day):
    a = counts(ch, f"{db}.{table}", day)
    b = counts(ch, f"{db}.{table}{SUFFIX}", day)
    return a == b, a, b


def copy_phase(ch, a, final):
    db, table = a.db, a.table
    old, new = f"{db}.{table}", f"{db}.{table}{SUFFIX}"
    k_old, k_new = norm(partition_key(ch, db, table)), norm(partition_key(ch, db, table + SUFFIX))
    rep = {"table": old, "phase": "final" if final else "copy", "days": 0, "copied": [], "skipped": 0, "copy_s": 0.0}
    if k_old == norm(NEW_KEY) and k_new == norm(OLD_KEY):
        log(f"{old} already has the new key and {new} the old one: exchanged; verifying")
        rep["exchanged"] = True
        rep.update(verify(ch, a))
        return rep
    if k_old != norm(OLD_KEY):
        raise SystemExit(f"{old}: partition key {k_old!r}, want {OLD_KEY}")
    if k_new is None or k_new == "":
        show = ch.q(f"SHOW CREATE TABLE {old} FORMAT TSVRaw")
        ddl = new_ddl(show.rstrip("\n"), db, table)
        log(f"creating {new}")
        ch.q(ddl)
    elif k_new != norm(NEW_KEY):
        raise SystemExit(f"{new} exists with key {k_new!r}")
    engine = ch.rows(f"SELECT engine FROM system.databases WHERE name = {sq(db)}")[0][0]
    if engine not in ("Atomic", "Replicated"):
        raise SystemExit(f"database {db} is {engine}: EXCHANGE TABLES needs Atomic")
    t0 = time.time()
    src_days = days(ch, old)
    for d in src_days:
        same, x, y = compare(ch, db, table, d)
        if same:
            rep["skipped"] += 1
            continue
        # Only keys the copy lacks entirely (the consumers' new objects):
        # copy those. Anything else (a count that differs: a duplicate the
        # consumers inserted later, rows gone by TTL): the whole day again.
        # (A day never copied, or mostly new, is copied whole.)
        missing = {k for k in x if k not in y}
        delta = (bool(missing) and bool(y) and len(missing) <= DELTA_MAX_KEYS
                 and set(y) <= set(x) and all(y[k] == x[k] for k in y))
        s = copy_day(ch, db, table, d, a.late_after, a.threads, keys=missing if delta else None)
        rows = sum(x[k] for k in missing) if delta else sum(x.values())
        nk = len(missing) if delta else len(x)
        rep["copied"].append({"day": d, "keys": nk, "rows": rows, "delta": delta, "s": round(s, 2)})
        log(f"{d}: {rows} rows, {nk} keys copied ({'the keys the copy lacked' if delta else 'the whole day'}) in {s:.1f} s")
        if final:
            same, x, y = compare(ch, db, table, d)
            if not same:
                raise SystemExit(f"{d}: still differs after the copy with consumers paused: are they?")
    rep["days"] = len(src_days)
    if final:
        # days the old table no longer has (dropped by TTL meanwhile)
        for d in sorted(set(days(ch, new)) - set(src_days)):
            for lp in (0, 1):
                ch.q(f"ALTER TABLE {new} DROP PARTITION ({sq(d)}, {lp})")
            log(f"{d}: gone from {old}, dropped from {new}")
        bad = [d for d in src_days if not compare(ch, db, table, d)[0]]
        if bad:
            raise SystemExit(f"days still differ: {bad}")
        tot = [ch.rows(f"SELECT count() FROM {t}")[0][0] for t in (old, new)]
        if tot[0] != tot[1]:
            raise SystemExit(f"row counts differ: {tot}")
        t1 = time.time()
        ch.q(f"EXCHANGE TABLES {old} AND {new}")
        rep["exchange_s"] = round(time.time() - t1, 3)
        rep["rows"] = int(tot[0])
        log(f"exchanged {old} and {new} ({tot[0]} rows): restart the consumers")
    rep["copy_s"] = round(time.time() - t0, 2)
    return rep


def verify(ch, a):
    db, table = a.db, a.table
    live, old = f"{db}.{table}", f"{db}.{table}{SUFFIX}"
    if norm(partition_key(ch, db, table)) != norm(NEW_KEY):
        raise SystemExit(f"{live} does not have the new key yet")
    bad = []
    ds = sorted(set(days(ch, live)) | set(days(ch, old)))
    for d in ds:
        if counts(ch, live, d) != counts(ch, old, d):
            bad.append(d)
    split = ch.rows(f"SELECT count() FROM (SELECT content_key FROM {live} GROUP BY content_key HAVING uniqExact(late_part) > 1)")[0][0]
    late = ch.rows(f"SELECT countIf(late_part = 1), count(), uniqExactIf(content_key, late_part = 1) FROM {live}")[0]
    rep = {"verified_days": len(ds), "differing_days": bad, "keys_in_both_parts": int(split),
           "late_rows": int(late[0]), "rows": int(late[1]), "late_keys": int(late[2])}
    log(f"verify: {rep}")
    if bad or int(split):
        raise SystemExit(f"verify failed: {rep} (days that differ may be the consumer's new rows since the exchange)")
    return rep


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--ch", default="http://127.0.0.1:8123")
    p.add_argument("--user")
    p.add_argument("--password")
    p.add_argument("--db", required=True)
    p.add_argument("--table", required=True, choices=["otel_traces", "otel_logs"], help="the consumer's traces or logs table")
    p.add_argument("--late-after", type=float, default=900, help="the edges' late_split_after, seconds (default 900)")
    p.add_argument("--threads", type=int, default=2)
    p.add_argument("--json", action="store_true")
    p.add_argument("--consumers-paused", action="store_true", help="final: required; the consumers are stopped and their statements settled")
    p.add_argument("--yes", action="store_true", help="drop-old: required")
    p.add_argument("phase", choices=["copy", "final", "verify", "drop-old"])
    a = p.parse_args()
    ch = CH(a.ch, a.user, a.password)
    if a.phase == "copy":
        rep = copy_phase(ch, a, final=False)
    elif a.phase == "final":
        if not a.consumers_paused:
            raise SystemExit("final needs the consumers paused and settled: --consumers-paused")
        rep = copy_phase(ch, a, final=True)
    elif a.phase == "verify":
        rep = verify(ch, a)
    else:
        if not a.yes:
            raise SystemExit("drop-old drops the old table: --yes")
        verify(ch, a)
        ch.q(f"DROP TABLE {a.db}.{a.table}{SUFFIX} SYNC")
        rep = {"dropped": f"{a.db}.{a.table}{SUFFIX}"}
    if a.json:
        print(json.dumps(rep))


if __name__ == "__main__":
    main()
