#!/usr/bin/env python3
"""s3du.py [PREFIX ...]: objects and bytes under each prefix of bucket central-zc
(through the SeaweedFS filer's listing, which is the S3 namespace)."""
import json, sys, urllib.parse, urllib.request
F = "http://127.0.0.1:18888/buckets/central-zc"
def ls(path):
    last = ""
    while True:
        u = f"{F}{path}?limit=1000&lastFileName={urllib.parse.quote(last)}"
        try:
            d = json.load(urllib.request.urlopen(urllib.request.Request(u, headers={"Accept": "application/json"})))
        except urllib.error.HTTPError:
            return
        ents = d.get("Entries") or []
        yield from ents
        if not d.get("ShouldDisplayLoadMore") or not ents:
            return
        last = d["LastFileName"]
def walk(path, keys):
    for e in ls(path):
        p = e["FullPath"][len("/buckets/central-zc"):]
        if e.get("Mode", 0) & (1 << 31):
            walk(p + "/", keys)
        else:
            keys[p] = e.get("FileSize", 0)
def objects(prefix):
    keys = {}
    walk("/" + prefix.strip("/") + "/", keys)
    return keys
if __name__ == "__main__":
    for p in sys.argv[1:] or ["zc", "own-r1", "own-r2"]:
        k = objects(p)
        print(f"{p}\t{len(k)}\t{sum(k.values())}")
