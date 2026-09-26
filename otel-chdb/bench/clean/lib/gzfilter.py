#!/usr/bin/env python3
"""gzfilter.py FILE.jsonl.gz ROOT MAX_BATCH REP: rewrite FILE without the rows
of that (root, max_batch, rep), tolerating a truncated last gzip member (a run
killed mid-append). Used before a block 2 run is redone."""
import gzip, json, os, sys, zlib
f, root, mb, rep = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
if not os.path.exists(f):
    sys.exit(0)
keep, drop = [], 0
try:
    for l in gzip.open(f, "rt"):
        if not l.strip():
            continue
        try:
            r = json.loads(l)
        except ValueError:
            drop += 1
            continue
        if (r["root"], int(r["max_batch"]), int(r["rep"])) == (root, mb, rep):
            drop += 1
        else:
            keep.append(l if l.endswith("\n") else l + "\n")
except (EOFError, OSError, zlib.error):
    drop += 1
if drop:
    with gzip.open(f + ".tmp", "wt") as o:
        o.writelines(keep)
    os.replace(f + ".tmp", f)
    print(f"gzfilter: dropped {drop} rows of {root} m{mb} r{rep}", file=sys.stderr)
