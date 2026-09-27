#!/usr/bin/env python3
"""Correctness: every statement either passes through byte for byte, or is
rewritten and returns exactly what the original returns.

For each statement of the corpora (corpus.py), `rwproxy rewrite` gives the
rewritten text. Each rewritten statement and its original are run against
ClickHouse with the same parameters and settings (statistics off, so JSON
output is deterministic), and their response bodies compared:

  equal        byte-equal bodies
  equal-rows   the same lines in another order (no ORDER BY, or ties)
  both-error   both fail, with the same exception code (a statement shaped
               for another schema side, e.g. has(ResourceAttributeItems, ...)
               on variant c)
  estimate     EXPLAIN ESTIMATE: the rewritten plan reads other granules; not
               a result
  nondet       the original doesn't equal itself run twice
  MISMATCH     anything else

  correct.py --label STATE [--modes exact,catalog] [--sets captured,scenarios,matrix] [--via URL]
             [--out ../results/correctness.jsonl]

The original runs once per statement; each mode's rewrite is compared with
it. --via URL sends the original through a running proxy instead of the
rewritten text (the proxy's own rewrite and transport; one mode).
"""
import argparse, collections, hashlib, json, os, subprocess, sys, time, urllib.error, urllib.parse, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "scripts"))
import chlib as c  # noqa: E402

SCR = os.environ.get("RW_SCRATCH", "/tmp")
ap = argparse.ArgumentParser()
ap.add_argument("--label", required=True)
ap.add_argument("--modes", default="exact,catalog")
ap.add_argument("--sets", default="captured,scenarios,matrix")
ap.add_argument("--corpus", default=os.path.join(SCR, "rwc"))
ap.add_argument("--bin", default=os.path.join(SCR, "rwproxy-target", "rwproxy"))
ap.add_argument("--config", default=os.path.join(SCR, "rwproxy.json"))
ap.add_argument("--via", default="")
ap.add_argument("--out", default=os.path.join(HERE, "..", "results", "correctness.jsonl"))
ap.add_argument("--details", default="")
a = ap.parse_args()

SETTINGS = dict(max_threads="2", max_memory_usage="3000000000", output_format_write_statistics="0",
                use_query_condition_cache="0", max_execution_time="300")


def run(base, q, params, db):
    qs = dict(SETTINGS)
    qs.update({"param_" + k: v for k, v in (params or {}).items()})
    if db:
        qs["database"] = db
    req = urllib.request.Request(base + "/?" + urllib.parse.urlencode(qs), data=q.encode(), method="POST")
    try:
        with urllib.request.urlopen(req, timeout=600) as r:
            return 200, r.read(), r.headers.get("X-ClickHouse-Exception-Code")
    except urllib.error.HTTPError as e:
        return e.code, e.read(), e.headers.get("X-ClickHouse-Exception-Code")


def compare(orig, st, rq):
    s1, b1, e1 = orig
    s2, b2, e2 = (run(a.via, st["query"], st["params"], st["db"]) if a.via else run(c.CH, rq, st["params"], st["db"]))
    if st["query"].lstrip().upper().startswith("EXPLAIN"):
        return "estimate", s1, s2
    if s1 != 200 or s2 != 200:
        if s1 != 200 and s2 != 200 and e1 == e2:
            return "both-error", e1, e2
        return "MISMATCH", (s1, b1[:300].decode(errors="replace")), (s2, b2[:300].decode(errors="replace"))
    if b1 == b2:
        return "equal", hashlib.sha256(b1).hexdigest()[:16], len(b1)
    if sorted(b1.splitlines()) == sorted(b2.splitlines()):
        return "equal-rows", hashlib.sha256(b"\n".join(sorted(b1.splitlines()))).hexdigest()[:16], len(b1)
    _, b3, _ = run(c.CH, st["query"], st["params"], st["db"])
    if b3 != b1:
        return "nondet", len(b1), len(b3)
    return "MISMATCH", b1[:400].decode(errors="replace"), b2[:400].decode(errors="replace")


def main():
    modes = a.modes.split(",")
    summary = dict(label=a.label, modes=modes, via=a.via, ts=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), load=c.load(), sets={})
    details = []
    for name in a.sets.split(","):
        path = os.path.join(a.corpus, f"{name}.jsonl")
        sts = [json.loads(l) for l in open(path)]
        rws = {}
        for m in modes:
            rw = subprocess.run([a.bin, "rewrite", "-config", a.config, "-mode", m], stdin=open(path), capture_output=True, text=True, check=True)
            rws[m] = [json.loads(l) for l in rw.stdout.splitlines()]
        per = {m: dict(cnt=collections.Counter(), reasons=collections.Counter(), examples=[]) for m in modes}
        for i, st in enumerate(sts):
            orig = None
            for m in modes:
                r = rws[m][i]
                per[m]["reasons"][r["reason"]] += 1
                if not r["rewritten"]:
                    continue  # byte-identical: the proxy forwards the original
                if orig is None:
                    orig = run(c.CH, st["query"], st["params"], st["db"])
                res = compare(orig, st, r["sql"])
                per[m]["cnt"][res[0]] += 1
                details.append(dict(label=a.label, mode=m, set=name, id=st["id"], scenario=st.get("scenario"), result=res[0], info=[str(x)[:400] for x in res[1:]]))
                if res[0] in ("MISMATCH", "nondet") and len(per[m]["examples"]) < 5:
                    per[m]["examples"].append(dict(id=st["id"], scenario=st.get("scenario"), result=res[0], info=[str(x)[:600] for x in res[1:]], sql=r["sql"][:1500]))
        summary["sets"][name] = {m: dict(statements=len(sts), reasons=dict(p["reasons"]), rewritten=sum(p["cnt"].values()), results=dict(p["cnt"]),
                                         examples=p["examples"]) for m, p in per.items()}
        for m, p in per.items():
            print(name, m, dict(p["reasons"]), dict(p["cnt"]), flush=True)
            for e in p["examples"]:
                print("  ", json.dumps(e)[:1500], flush=True)
    summary["load_after"] = c.load()
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    with open(a.out, "a") as f:
        f.write(json.dumps(summary) + "\n")
    if a.details:
        with open(a.details, "a") as f:
            for d in details:
                f.write(json.dumps(d) + "\n")


if __name__ == "__main__":
    main()
