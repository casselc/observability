#!/usr/bin/env python3
"""blobcheck.py [DISK PREFIX]: the S3 objects of a disk against what the replicas reference.

Lists every object under PREFIX (default zc/, the zero-copy disk s3_zc) and
every blob each replica's metadata references (system.remote_data_paths,
which walks the disk's local metadata: active, outdated, detached and
temporary parts alike), then prints:

  listed          objects / bytes in the bucket under PREFIX
  ref r1, ref r2  blobs each replica references
  shared          referenced by both (one copy serving two replicas)
  orphans         listed, referenced by nobody (leaked: storage paid for nothing)
  missing         referenced, not listed (DATA LOSS: a part points at a deleted blob)

Run it when nothing is moving or merging (a blob being written is listed
before its metadata commits). A replica that is down is reported and skipped.
"""
import json
import sys
import urllib.request

sys.path.insert(0, __file__.rsplit("/", 1)[0])
import s3du  # noqa: E402

DISK = sys.argv[1] if len(sys.argv) > 1 else "s3_zc"
PREFIX = sys.argv[2] if len(sys.argv) > 2 else "zc"
REPLICAS = {"r1": "http://127.0.0.1:28123", "r2": "http://127.0.0.1:38123"}


def q(url, sql):
    r = urllib.request.urlopen(urllib.request.Request(url + "/", data=sql.encode()), timeout=120)
    return r.read().decode()


# PREFIX may name a per-replica prefix, e.g. 'own-{r}' for the s3_own disks.
listed = {}
for pfx in sorted({PREFIX.replace("{r}", n) for n in REPLICAS}):
    listed.update({k.lstrip("/"): v for k, v in s3du.objects(pfx).items()})
refs = {}
by_table = {}
for name, url in REPLICAS.items():
    try:
        rows = q(url, f"SELECT remote_path, local_path, size FROM system.remote_data_paths WHERE disk_name = '{DISK}' FORMAT JSONCompactEachRow")
    except Exception as e:  # noqa: BLE001
        print(f"{name}: unreachable ({e}); skipped")
        continue
    refs[name] = {}
    for line in rows.splitlines():
        rp, lp, size = json.loads(line)
        refs[name][rp] = (lp, size)
        t = "/".join(lp.split("/")[:3])
        by_table.setdefault(t, set()).add(rp)

# Classify every referenced blob: which table, and whether the part holding it
# is active, outdated (waiting for removal), detached, or something else.
cls = {}
kinds = {}
dbof = {}
for name, url in REPLICAS.items():
    if name not in refs:
        continue
    uu = {}
    for line in q(url, "SELECT toString(uuid), database || '.' || name FROM system.tables WHERE uuid != toUUIDOrZero('') FORMAT TSV").splitlines():
        u, t = line.split("\t"); uu[u] = t
    states = {}
    for line in q(url, "SELECT toString(t.uuid), p.name, p.active FROM system.parts AS p INNER JOIN system.tables AS t ON p.database = t.database AND p.table = t.name WHERE p.disk_name = '" + DISK + "' FORMAT TSV").splitlines():
        u, pn, a = line.split("\t"); states[(u, pn)] = "active" if a == "1" else "outdated"
    for rp, (lp, size) in refs[name].items():
        parts = lp.split("/")
        u = parts[2] if len(parts) > 3 else "?"
        if "/detached/" in lp:
            kind = "detached"
        elif "/moving/" in lp or "/tmp" in lp:
            kind = "tmp_or_moving"
        else:
            kind = states.get((u, parts[3]), "unknown")
        key = (uu.get(u, u).split(".")[0], kind)
        kinds.setdefault(name, {})[rp] = kind
        dbof[rp] = key[0]
        c = cls.setdefault(name, {}).setdefault(key, [0, 0])
        c[0] += 1; c[1] += size

allref = set().union(*[set(r) for r in refs.values()]) if refs else set()
orphans = set(listed) - allref
missing = allref - set(listed)
out = {
    "disk": DISK,
    "prefix": PREFIX,
    "listed_objects": len(listed),
    "listed_bytes": sum(listed.values()),
    **{f"ref_{n}": len(r) for n, r in refs.items()},
    **{f"ref_{n}_bytes": sum(s for _, s in r.values()) for n, r in refs.items()},
    "shared": len(set(refs.get("r1", {})) & set(refs.get("r2", {}))),
    "only_r1": len(set(refs.get("r1", {})) - set(refs.get("r2", {}))),
    "only_r2": len(set(refs.get("r2", {})) - set(refs.get("r1", {}))),
    "orphans": len(orphans),
    "orphan_bytes": sum(listed[k] for k in orphans),
    "missing": len(missing),
    "missing_examples": [(k, [refs[n][k][0] for n in refs if k in refs[n]]) for k in sorted(missing)[:5]],
}
act = {n: {rp for rp, k in kinds.get(n, {}).items() if k == "active"} for n in refs}
if len(act) == 2:
    a1, a2 = act.values()
    out["active_blobs_r1_r2_shared"] = [len(a1), len(a2), len(a1 & a2)]
    out["active_bytes_one_copy"] = sum(listed.get(k, 0) for k in a1 | a2)
# Per database: blobs of active parts on r1 / r2 / both, and their bytes counted once.
if len(act) == 2:
    per = {}
    for rp in a1 | a2:
        e = per.setdefault(dbof.get(rp, "?"), [0, 0, 0, 0])
        e[0] += rp in a1; e[1] += rp in a2; e[2] += (rp in a1 and rp in a2); e[3] += listed.get(rp, 0)
    out["active_by_db_r1_r2_shared_bytes"] = per
out["by_db_kind"] = {n: {f"{db}/{k}": v for (db, k), v in sorted(c.items())} for n, c in cls.items()}
print(json.dumps(out))
