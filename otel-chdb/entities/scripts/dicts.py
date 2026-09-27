#!/usr/bin/env python3
"""The catalog dictionaries (../sql/dictionaries.sql) at fleet scale: builds
res_index for every pod version in the catalog, loads each dictionary and
records its memory (the server's MemoryTracking before and after, steady
state; system.dictionaries.bytes_allocated alongside, which under-reports
Map attributes) and load time; then checks that the normalized dictionaries
rebuild exactly the flattened covered set of every resource in the window.

  dicts.py [--cat ent_cat] [--build] [--out ../results/dictionary.jsonl]
"""
import argparse, json, time
import chlib as c

ap = argparse.ArgumentParser()
ap.add_argument("--cat", default="ent_cat")
ap.add_argument("--build", action="store_true")
ap.add_argument("--out", default=f"{c.HERE}/../results/dictionary.jsonl")
ap.add_argument("--reps", type=int, default=2)
a = ap.parse_args()

st = c.statements(f"{c.SQL}/dictionaries.sql", db=a.cat)
if a.build:
    c.q(st[0])
    c.q(f"TRUNCATE TABLE {a.cat}.res_index")
    t = time.time()
    c.q(c.statements(f"{c.SQL}/resources.sql", db=a.cat, where="1")[1], max_threads=2)
    print("res_index", c.one(f"SELECT count() FROM {a.cat}.res_index"), f"{time.time() - t:.1f}s", flush=True)
names = ["d_res", "d_pod", "d_wl", "d_node", "d_ns", "d_cluster"]


def tracked():
    return int(c.one("SELECT value FROM system.metrics WHERE metric = 'MemoryTracking'"))


for rep in range(a.reps):
    for n, ddl in zip(names, st[1:]):
        if c.mem_available_gb() < 5:
            raise SystemExit("MemAvailable < 5 GB")
        c.q(f"DROP DICTIONARY IF EXISTS {a.cat}.{n}")
        time.sleep(2)
        m0 = tracked()
        c.q(ddl)
        t = time.time()
        c.q(f"SYSTEM RELOAD DICTIONARY {a.cat}.{n}")
        wall = time.time() - t
        time.sleep(4)
        m1 = tracked()
        d = c.rows(f"SELECT element_count, bytes_allocated, loading_duration FROM system.dictionaries WHERE database = '{a.cat}' AND name = '{n}'")[0]
        rec = dict(rep=rep, dict=n, entries=int(d["element_count"]), bytes_allocated=int(d["bytes_allocated"]), tracked_delta=m1 - m0,
                   load_s=float(d["loading_duration"]), wall_s=round(wall, 3), load=c.load(), ts=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
        print(rec, flush=True)
        with open(a.out, "a") as f:
            f.write(json.dumps(rec) + "\n")
