#!/usr/bin/env python3
"""Load the staged telemetry (telemetry.py) into one database per schema
(schemas.py), one 10k-row object per INSERT in the consumer's statement shape
(ONE_BLOCK settings), then OPTIMIZE FINAL: the query databases. Inserts read
the staging MergeTree directly (not Parquet; bench.py measures inserts from
Parquet objects).

  load.py VARIANT[,VARIANT...] [--prefix ent_] [--src ent_src] [--objects N]
"""
import argparse, time
import chlib as c
import schemas

ONE_BLOCK = dict(max_threads=1, max_insert_threads=1, max_block_size=1048576, max_insert_block_size=1048576,
                 min_insert_block_size_rows=0, min_insert_block_size_bytes=0)


def announced(src):
    """The edge-announced resources (variant ann) and their dictionary: every distinct whole resource map."""
    c.q(f"CREATE TABLE IF NOT EXISTS {schemas.CAT}.announced (resource_id UInt64, attrs String) ENGINE = ReplacingMergeTree ORDER BY resource_id")
    c.q(f"TRUNCATE TABLE {schemas.CAT}.announced")
    for sig in schemas.SIGS:
        c.q(f"INSERT INTO {schemas.CAT}.announced SELECT resource_id_full, any(toJSONString(ResourceAttributes)) FROM {src}.{sig} GROUP BY resource_id_full")
    c.q(f"CREATE DICTIONARY IF NOT EXISTS {schemas.CAT}.d_ann (resource_id UInt64, attrs String DEFAULT '{{}}') PRIMARY KEY resource_id "
        f"SOURCE(CLICKHOUSE(QUERY 'SELECT resource_id, attrs FROM {schemas.CAT}.announced FINAL')) LAYOUT(HASHED_ARRAY()) LIFETIME(MIN 30 MAX 60)")
    c.q(f"SYSTEM RELOAD DICTIONARY {schemas.CAT}.d_ann")


def insert_select(variant, sig, db, src, cols):
    exprs = []
    for col in cols:
        if col == "resource_id" and variant == "ann":
            exprs.append("resource_id_full")
        else:
            exprs.append(f"`{col}`")
    return f"INSERT INTO {db}.{'otel_' + sig if variant in ('a', 'full', 'c') else 'otel_' + sig + '_rid'} ({', '.join('`' + x + '`' for x in cols)}) SELECT {', '.join(exprs)} FROM {src}.{sig}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("variants")
    ap.add_argument("--prefix", default="ent_")
    ap.add_argument("--src", default="ent_src")
    ap.add_argument("--objects", type=int, default=0, help="0 = all")
    a = ap.parse_args()
    for v in a.variants.split(","):
        if v == "ann":
            announced(a.src)
        db = f"{a.prefix}{v}"
        c.q(f"DROP DATABASE IF EXISTS {db} SYNC")
        schemas.create(v, db)
        t0 = time.time()
        for sig in schemas.SIGS:
            base = f"otel_{sig}" if v in ("a", "full", "c") else f"otel_{sig}_rid"
            cols = schemas.ordinary_columns(db, base)
            n = int(c.one(f"SELECT max(obj) + 1 FROM {a.src}.{sig}"))
            if a.objects:
                n = min(n, a.objects)
            ins = insert_select(v, sig, db, a.src, cols)
            for i in range(n):
                c.q(f"{ins} WHERE obj = {i}", **ONE_BLOCK)
            c.q(f"OPTIMIZE TABLE {db}.{base} FINAL", max_threads=2)
            print(v, sig, n, "objects", f"{time.time() - t0:.0f}s", flush=True)
        print(c.one(f"SELECT table, sum(rows), round(sum(bytes_on_disk) / sum(rows), 1) FROM system.parts WHERE database = '{db}' AND active GROUP BY table ORDER BY table"))


if __name__ == "__main__":
    main()
