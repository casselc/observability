#!/usr/bin/env python3
"""View conformance (as ../../metrics-layout's): every (b)/(c) presentation
returns exactly (a)'s rows. For each signal, on (a)'s ordinary columns in
(a)'s order (ResourceAttributes included, rebuilt): count, sum(cityHash64(all
columns)), and EXCEPT ALL both ways, chunked by object (content_key) to bound
memory. Rows the catalog cannot rebuild (resource unknown to the dictionary
and outside the grace window) are counted separately: they are the race the
README describes, not a view defect.

  conformance.py VARIANT DB [--a ent_a] [--chunks 8] [--out ../results/conformance.jsonl]
"""
import argparse, json, time
import chlib as c
import schemas

ap = argparse.ArgumentParser()
ap.add_argument("variant")
ap.add_argument("db")
ap.add_argument("--a", default="ent_a")
ap.add_argument("--chunks", type=int, default=8)
ap.add_argument("--label", default="")
ap.add_argument("--out", default=f"{c.HERE}/../results/conformance.jsonl")
a = ap.parse_args()

for sig in schemas.SIGS:
    cols = schemas.a_columns(sig)
    sel = ", ".join(f"`{x}`" for x in cols)
    tup = f"cityHash64({sel})"
    A = f"{a.a}.otel_{sig}"
    B = f"{a.db}.otel_{sig}_rid" if a.variant == "c" else f"{a.db}.otel_{sig}"
    t = time.time()
    ca, ha = c.one(f"SELECT count(), sum({tup}) FROM {A}").split("\t")
    cb, hb = c.one(f"SELECT count(), sum({tup}) FROM {B}").split("\t")
    ab = ba = 0
    for k in range(a.chunks):
        w = f"WHERE cityHash64(content_key) % {a.chunks} = {k}"
        ab += int(c.one(f"SELECT count() FROM (SELECT {sel} FROM {A} {w} EXCEPT ALL SELECT {sel} FROM {B} {w})"))
        ba += int(c.one(f"SELECT count() FROM (SELECT {sel} FROM {B} {w} EXCEPT ALL SELECT {sel} FROM {A} {w})"))
    unknown = None
    if a.variant != "ann":
        base = B if a.variant == "c" else f"{B}_rid"  # c: ResourceAttributes is an ALIAS on the table itself
        unknown = int(c.one(f"SELECT count() FROM {base} WHERE NOT dictHas('{schemas.CAT}.d_res', resource_id) "
                            f"AND NOT mapContains(ResourceResidual, 'k8s.pod.uid')"))
    rec = dict(variant=a.variant, db=a.db, label=a.label, signal=sig, count_a=int(ca), count_b=int(cb), hash_a=ha, hash_b=hb,
               hash_equal=ha == hb, a_except_b=ab, b_except_a=ba, rows_unknown_to_catalog=unknown, s=round(time.time() - t, 1),
               ts=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    print(json.dumps(rec), flush=True)
    with open(a.out, "a") as f:
        f.write(json.dumps(rec) + "\n")
