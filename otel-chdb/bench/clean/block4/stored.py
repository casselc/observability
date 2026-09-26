#!/usr/bin/env python3
"""Block 4: stored bytes per row after OPTIMIZE FINAL, per table, on the
private ClickHouse, with the central DDL of block 3 (../block3/merges.py
ddl(): the consumer's tables with content_key and the by_content projection)
and the same replay pool.

Per table: insert N statements sequentially (merges.py stmt(), the
consumer's statement shape), OPTIMIZE TABLE … FINAL, then read system.parts
(data_compressed_bytes, bytes_on_disk, rows) and system.columns (bytes per
column), drop the table. One table at a time, so disk stays small.

Layout B per type: gauge and sum go into two copies of the number-points
table so each type's bytes are known (the production table holds both).
Layout A (ClickStack tables): all five types from mgen's A objects, columns
matched by name against the s3() inferred schema.

  stored.py [LABEL,...]     appends one line per table to results.jsonl as
                            it finishes, and skips labels already there
                            (restart-safe)
"""
import json, os, re, sys, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "block3"))
import merges  # noqa: E402
from merges import ch, stmt, ddl, SETTINGS, S3, KEY, SECRET, POOL, envelope, FENCE  # noqa: E402

DB = "clean_b4"
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results.jsonl")
BASE_NS = int(merges.BASE.timestamp() * 1e9)


class A:
    partition, ttl, setting, db, ids, run = "toDate(received_at)", "", [], DB, "random", "b4"


def one_type_number(t, seq):
    """merges.stmt('number') with only the gauge or only the sum object."""
    sql, _ = stmt("number", seq, "100k", BASE_NS + seq * 10**9, A)
    other = "sum" if t == "gauge" else "gauge"
    sql = re.sub(r"s3\('[^']*'", f"s3('{S3}/{POOL}/big/B/{t}/{seq % 24:04d}-00.parquet'", sql, count=1)
    assert f"/{other}/" not in sql.split("FROM s3(")[1].split("'")[1]
    return sql


LONG = f"{POOL}/long"  # mgen -rounds 120 -pods-per-batch 200 -a: each round once, no replay
LONG_ROUNDS = 120
# rounds per statement for the no-replay jobs (about 100k rows each)
PER = {"sum": 2, "gauge": 4, "histogram": 6, "exponential_histogram": 40, "summary": 40}


def long_b(name, t, seq):
    """merges.stmt(name) for statement seq, reading rounds seq*k .. seq*k+k-1 of type t from the long pool,
    with no time shift (every round is distinct)."""
    sql, _ = stmt(name, seq, "100k", BASE_NS + seq * 10**9, A)
    k = PER[t]
    keys = [f"{LONG}/B/{t}/{r:04d}-00.parquet" for r in range(seq * k, seq * k + k)]
    sql = re.sub(r"s3\('[^']*'", "s3('" + merges.url(keys) + "'", sql, count=1)
    return re.sub(r"TimeUnix \+ \d+", "TimeUnix", sql)


def a_cols(url):
    d = ch(f"DESCRIBE s3('{url}', '{KEY}', '{SECRET}', 'Parquet') FORMAT JSONCompact")
    return [r[0] for r in json.loads(d)["data"]]


def a_stmt(t, seq, tcols, long=False):
    n_obj = PER[t] if long else {"sum": 2, "gauge": 2, "histogram": 6, "exponential_histogram": 6, "summary": 6}[t]
    if long:
        keys = [f"{LONG}/A/{t}/{seq * n_obj + i:04d}-00.parquet" for i in range(n_obj)]
        cyc = 0
    else:
        keys = [f"{POOL}/big/A/{t}/{(seq * n_obj + i) % 24:04d}-00.parquet" for i in range(n_obj)]
        cyc = (seq * n_obj) // 24
    src = merges.s3src(keys, "").replace(", ''", "")  # inferred schema
    pcols = a_cols(f"{S3}/{keys[0]}")
    env = ["producer_id", "producer_epoch", "batch_id", "row_ordinal", "received_at", "schema_version"]
    cols = [c for c in pcols if c in tcols and c not in env and c != "content_key"]
    sel = [f"TimeUnix + {cyc * 720}" if c == "TimeUnix" else f"`{c}`" for c in cols]
    return (f"INSERT INTO {DB}.otel_metrics_{t} ({', '.join('`%s`' % c for c in cols + env)}, content_key) "
            f"SELECT {', '.join(sel)}, {envelope(seq, BASE_NS + seq * 10**9, 'b4')} FROM {src} {FENCE}")


def measure(label, table, stmts, create):
    ch(f"DROP TABLE IF EXISTS {DB}.{table} SYNC")
    ch(create)
    t0 = time.time()
    for i, s in enumerate(stmts):
        ch(s, dict(SETTINGS, insert_deduplication_token=f"b4-{label}-{i}-{time.time()}"))
    ch(f"OPTIMIZE TABLE {DB}.{table} FINAL", timeout=3600)
    p = json.loads(ch(f"""SELECT count() AS parts, sum(rows) AS rows, sum(data_compressed_bytes) AS dcb, sum(bytes_on_disk) AS bod,
        sum(data_uncompressed_bytes) AS dub FROM system.parts WHERE database = '{DB}' AND table = '{table}' AND active FORMAT JSONEachRow"""))
    cols = json.loads(ch(f"""SELECT groupArray((name, data_compressed_bytes)) AS c FROM (SELECT name, data_compressed_bytes FROM system.columns
        WHERE database = '{DB}' AND table = '{table}' ORDER BY data_compressed_bytes DESC) FORMAT JSONEachRow"""))["c"]
    rows = int(p["rows"])
    out = {"label": label, "table": table, "statements": len(stmts), "rows": rows, "parts": int(p["parts"]),
           "data_compressed_B_per_row": round(int(p["dcb"]) / rows, 3), "bytes_on_disk_B_per_row": round(int(p["bod"]) / rows, 3),
           "uncompressed_B_per_row": round(int(p["dub"]) / rows, 1), "secs": round(time.time() - t0, 1),
           "columns_B_per_row": {c: round(int(b) / rows, 3) for c, b in cols[:12]}}
    print(json.dumps(out), flush=True)
    with open(OUT, "a") as f:
        f.write(json.dumps(out) + "\n")
    ch(f"DROP TABLE {DB}.{table} SYNC")


def main():
    only = sys.argv[1].split(",") if len(sys.argv) > 1 else None
    ch(f"CREATE DATABASE IF NOT EXISTS {DB}")
    jobs = []
    for ids in ("random", "testgen"):
        A.ids = ids
        for sig in ("traces", "logs"):
            if sig == "logs" and ids == "testgen":
                continue
            jobs.append((f"{sig}-{ids}-ids", f"otel_{sig}", [stmt(sig, s, "100k", BASE_NS + s * 10**9, A)[0] for s in range(50)], ddl(sig, A)))
    A.ids = "random"
    for t in ("gauge", "sum"):
        jobs.append((f"B-{t}", "otel_metrics_number_points", [one_type_number(t, s) for s in range(120)], ddl("number", A)))
    jobs.append(("B-number (gauge+sum)", "otel_metrics_number_points", [stmt("number", s, "100k", BASE_NS + s * 10**9, A)[0] for s in range(120)], ddl("number", A)))
    jobs.append(("B-histogram", "otel_metrics_histogram_points", [stmt("histogram", s, "100k", BASE_NS + s * 10**9, A)[0] for s in range(20)], ddl("histogram", A)))
    for t in ("exponential_histogram", "summary"):
        jobs.append((f"B-{t}", merges.TABLE_OF[t], [stmt(t, s, "100k", BASE_NS + s * 10**9, A)[0] for s in range(3)], ddl(t, A)))
    jobs.append(("B-series (5 cycles)", "otel_metrics_series", [stmt("series", s, "100k", BASE_NS + s * 10**9, A)[0] for s in range(5)], ddl("series", A)))
    for t in ("sum", "gauge", "histogram", "exponential_histogram", "summary"):
        create = merges.a_ddl(t).replace("{db}", DB)
        create = re.sub(r"(SETTINGS [^\n]*)$", lambda m: m.group(1) + ", old_parts_lifetime = 30", create)
        jobs.append((f"A-{t}", f"otel_metrics_{t}", None, create))
    done = {json.loads(l)["label"] for l in open(OUT) if l.strip()} if os.path.exists(OUT) else set()
    # no replay: the long pool, each round once (the jobs above replay a 24- or 50-round pool 3-5 times)
    if os.environ.get("B4_LONG"):
        for t in ("gauge", "sum"):
            jobs.append((f"B-{t} no replay", "otel_metrics_number_points", [long_b("number", t, s) for s in range(LONG_ROUNDS // PER[t])], ddl("number", A)))
        for t in ("histogram", "exponential_histogram", "summary"):
            jobs.append((f"B-{t} no replay", merges.TABLE_OF[t], [long_b(t, t, s) for s in range(LONG_ROUNDS // PER[t])], ddl(t, A)))
        for t in ("sum", "gauge", "histogram", "exponential_histogram", "summary"):
            create = merges.a_ddl(t).replace("{db}", DB)
            create = re.sub(r"(SETTINGS [^\n]*)$", lambda m: m.group(1) + ", old_parts_lifetime = 30", create)
            jobs.append((f"A-{t} no replay", f"otel_metrics_{t}", None, create))
    for label, table, stmts, create in jobs:
        if only and not any(o in label for o in only):
            continue
        if label in done:
            continue
        if stmts is None:  # layout A: needs the table's columns first
            ch(f"DROP TABLE IF EXISTS {DB}.{table} SYNC")
            ch(create)
            tcols = [r[0] for r in json.loads(ch(f"SELECT name FROM system.columns WHERE database = '{DB}' AND table = '{table}' FORMAT JSONCompact"))["data"]]
            t = table.replace("otel_metrics_", "")
            long = label.endswith("no replay")
            n = LONG_ROUNDS // PER[t] if long else {"sum": 60, "gauge": 60, "histogram": 20, "exponential_histogram": 20, "summary": 20}[t]  # 120 rounds = 5 pool cycles
            stmts = [a_stmt(t, s, tcols, long) for s in range(n)]
        measure(label, table, stmts, create)
    ch(f"DROP DATABASE IF EXISTS {DB} SYNC")


if __name__ == "__main__":
    main()
