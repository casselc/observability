#!/usr/bin/env python3
"""Summarize an ITF trace from `quint run --mbt --out-itf`: per step, the action
taken, its nondeterministic picks, and the variables that changed."""
import json, sys

def val(v):
    if isinstance(v, dict):
        if "#bigint" in v: return int(v["#bigint"])
        if "#set" in v: return "{" + ", ".join(sorted(str(val(x)) for x in v["#set"])) + "}"
        if "#map" in v: return "{" + ", ".join(sorted(f"{val(k)}:{val(x)}" for k, x in v["#map"])) + "}"
        if "#tup" in v: return "(" + ", ".join(str(val(x)) for x in v["#tup"]) + ")"
        if "tag" in v and "value" in v:
            inner = val(v["value"])
            return v["tag"] if inner in ("{}", "()") else f"{v['tag']}({inner})"
        return "{" + ", ".join(f"{k}: {val(x)}" for k, x in v.items() if not k.startswith("mbt::")) + "}"
    if isinstance(v, list): return "[" + ", ".join(str(val(x)) for x in v) + "]"
    return str(v).lower() if isinstance(v, bool) else str(v)

t = json.load(open(sys.argv[1]))
hide = set(sys.argv[2].split(",")) if len(sys.argv) > 2 else set()
prev = {}
for i, s in enumerate(t["states"]):
    act = s.get("mbt::actionTaken", "init")
    picks = {k: val(x["value"]) for k, x in s.get("mbt::nondetPicks", {}).items() if isinstance(x, dict) and x.get("tag") == "Some"}
    changed = {k: val(x) for k, x in s.items() if not k.startswith("mbt::") and not k.startswith("#") and k not in hide and prev.get(k) != val(x)}
    print(f"{i:2d} {act}" + (f"  {picks}" if picks else ""))
    for k, x in changed.items():
        print(f"     {k} = {x}")
    prev = {k: val(x) for k, x in s.items()}
