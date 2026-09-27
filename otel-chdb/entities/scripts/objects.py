#!/usr/bin/env python3
"""Parquet objects for the insert benchmark (bench.py): the first N staged
objects of each signal, written by ClickHouse's Parquet writer to SeaweedFS
(bucket ent-objects), in three shapes: a (ResourceAttributes), b (resource_id
+ ResourceResidual), ann (resource_id = the whole-map hash). Columns are each
schema's insert columns.

  objects.py [--n 30] [--src ent_src]
"""
import argparse
import chlib as c
import schemas

S3 = "http://127.0.0.1:18333/ent-objects"
ap = argparse.ArgumentParser()
ap.add_argument("--n", type=int, default=30)
ap.add_argument("--src", default="ent_src")
a = ap.parse_args()
for shape in ("a", "b", "ann"):
    for sig in schemas.SIGS:
        cols = [x for x in schemas.a_columns(sig)]
        if shape != "a":
            i = cols.index("ResourceAttributes")
            cols[i:i + 1] = ["resource_id"] + (["ResourceResidual"] if shape == "b" else [])
        exprs = ", ".join("resource_id_full AS resource_id" if (x == "resource_id" and shape == "ann") else f"`{x}`" for x in cols)
        for o in range(a.n):
            c.q(f"INSERT INTO FUNCTION s3('{S3}/{shape}/{sig}/{o:04d}.parquet', 'otel', 'otelsecret', 'Parquet') "
                f"SELECT {exprs} FROM {a.src}.{sig} WHERE obj = {o} ORDER BY row_ordinal", s3_truncate_on_insert=1, max_threads=1)
        print(shape, sig, c.one(f"SELECT count(), sum(_size), round(sum(_size) / count(), 1) FROM (SELECT DISTINCT _path, _size FROM s3('{S3}/{shape}/{sig}/*.parquet', 'otel', 'otelsecret', 'One'))"),
              c.one(f"SELECT count() FROM s3('{S3}/{shape}/{sig}/*.parquet', 'otel', 'otelsecret', 'Parquet')"), flush=True)
