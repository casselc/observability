#!/usr/bin/env python3
"""Step 1 tables from the raw results (generated into results.md).

  summarize.py > results.md
"""
import gzip, json, os, statistics as st, subprocess, sys
from collections import defaultdict

H = os.path.dirname(os.path.abspath(__file__))
CFGS = ["a-unsorted", "b-sorted-1rg", "c-hash-4rg", "d-hash-16rg", "e-range-4rg", "f-range-16rg", "g-hash-4rg-bloom", "h-hash-16rg-bloom"]


def lines(p):
    p = os.path.join(H, p)
    if not os.path.exists(p):
        return []
    op = gzip.open if p.endswith(".gz") else open
    with op(p, "rt") as f:
        return [json.loads(l) for l in f if l.strip()]


def mm(xs, f="{:.1f}"):
    xs = list(xs)
    if not xs:
        return "–"
    return (f + " [" + f + "–" + f + "]").format(st.median(xs), min(xs), max(xs))


def envsum(path):
    out = subprocess.run([sys.executable, f"{H}/../clean/lib/envlog.py", f"{H}/{path}", "--json"], capture_output=True, text=True).stdout
    rs = [json.loads(l) for l in out.splitlines()]
    if not rs:
        return "no env log"
    return (f"{len(rs)} measured processes/runs; load at start max {max(r['load_start'] for r in rs):.2f}; steal per run median "
            f"{st.median(r['steal_pct'] for r in rs):.2f}% / max {max(r['steal_pct'] for r in rs):.2f}%; flagged (steal > 2% or load > 0.5) {sum(r['bad'] for r in rs)}")


def edge():
    rows = [d for d in lines("edge.jsonl") if d["pass"] == "cpu"]
    sets = {(d["signal"], d["config"]): d for d in lines("edge.jsonl") if d["pass"] == "set"}
    print("## Edge: CPU per 10k rows and object size\n")
    print("encbench, whole process CPU (flatten + sort + encode + create-only PUT to SeaweedFS) per 10k-row request, "
          "median [min–max] over repetitions; object size from the 32-object set (all distinct requests).\n")
    print("| signal | config | n | CPU ms / 10k rows | Δ vs a | encode ms (incl. sort) | object KB | Δ size vs a | row groups |")
    print("|---|---|---|---|---|---|---|---|---|")
    for sig in ("traces", "logs"):
        base = st.median(d["CPUMSPerBatch"] for d in rows if d["signal"] == sig and d["config"] == "a-unsorted") if rows else None
        bsz = sets.get((sig, "a-unsorted"), {}).get("BytesPerBatch")
        for c in CFGS:
            xs = [d["CPUMSPerBatch"] for d in rows if d["signal"] == sig and d["config"] == c]
            en = [d["EncodeMSPerBatch"] for d in rows if d["signal"] == sig and d["config"] == c]
            sz = sets.get((sig, c), {}).get("BytesPerBatch")
            dcpu = f"{(st.median(xs) / base - 1) * 100:+.1f}%" if xs and base else "–"
            dsz = f"{(sz / bsz - 1) * 100:+.1f}%" if sz and bsz else "–"
            rg = rgs.get((sig, c), "–")
            print(f"| {sig} | {c} | {len(xs)} | {mm(xs)} | {dcpu} | {mm(en)} | {sz / 1000 if sz else 0:.1f} | {dsz} | {rg} |")
    print(f"\nEnvironment (edge-env.jsonl): {envsum('edge-env.jsonl')}.\n")


rgs = {}


def direct():
    d = [x for x in lines("direct.jsonl") if x["set"] == "set"]
    if not d:
        return
    sv = json.load(open(f"{H}/services.json"))
    for x in d:
        rgs[(x["signal"], x["config"])] = f"{st.mean(o['rgs'] for o in x['objects']):.1f}"
    print("## Reads, direct ranged path (modelled exactly from each object's footer)\n")
    print("A one-service, one-hour query over the 32-object set (all objects are in the hour). Per object, mean over the 32: "
          "GETs and KB read. 'whole' = one GET of the object. Footer: one 64 KiB suffix GET (a second if the footer is longer); "
          "column chunks of the picked row groups merged into one GET when within 1 MiB (object_store's default). "
          "In brackets: 16 KiB footer guess / no merging, where it differs.\n")
    for cols in ("narrow", "all"):
        print(f"\n### Columns: {cols} ({'Timestamp, ServiceName, SpanName/SeverityText, Duration/Body' if cols == 'narrow' else 'SELECT *'})\n")
        print("| signal | config | object KB | footer KB | query service | pick | row groups read | GETs/object | KB/object | read / whole |")
        print("|---|---|---|---|---|---|---|---|---|---|")
        for sig in ("traces", "logs"):
            for x in d:
                if x["signal"] != sig:
                    continue
                objs = x["objects"]
                size = st.mean(o["size"] for o in objs)
                foot = st.mean(o["footer"] for o in objs)
                for rank, q in sv.items():
                    for pick in ("minmax", "bucket") if x["config"] != "a-unsorted" else ("whole", "minmax"):
                        k = f"{cols}/fg64k/co1024k"
                        vals = [o["q"][rank][k].get(pick) for o in objs]
                        if not all(vals):
                            continue
                        g = st.mean(v["gets"] for v in vals)
                        b = st.mean(v["bytes"] for v in vals)
                        rr = st.mean(v["rgs"] for v in vals)
                        alt = [o["q"][rank][f"{cols}/fg16k/co1024k"][pick] for o in objs]
                        alt2 = [o["q"][rank][f"{cols}/fg64k/co0k"][pick] for o in objs]
                        ga, ba, g2 = st.mean(v["gets"] for v in alt), st.mean(v["bytes"] for v in alt), st.mean(v["gets"] for v in alt2)
                        extra = f" [{ga:.1f} / {g2:.1f}]" if (abs(ga - g) > 0.05 or abs(g2 - g) > 0.05) else ""
                        extrab = f" [{ba / 1000:.1f}]" if abs(ba - b) > 500 else ""
                        print(f"| {sig} | {x['config']} | {size / 1000:.1f} | {foot / 1000:.1f} | {rank} {q['service']} ({q['share'] * 100:.2f}%) | {pick} | "
                              f"{rr:.1f} of {st.mean(o['rgs'] for o in objs):.1f} | {g:.2f}{extra} | {b / 1000:.1f}{extrab} | {b / size:.2f} |")
    print()


def chreads():
    ev = lines("ch-events.jsonl")
    if not ev:
        return
    sv = json.load(open(f"{H}/services.json"))
    keys = sorted({k for e in ev for k in e.get("pe", {})})
    print("## Reads through ClickHouse s3()\n")
    print(f"ProfileEvents seen: {', '.join(k for k in keys if 'S3' in k or 'Parquet' in k or 'RowGroup' in k)}.\n")
    print("Per query over the 32-object set, median over repetitions. GET = S3GetObject; KB = ReadBufferFromS3Bytes; "
          "'nopushdown' = row-group filtering off (reads whole objects' needed columns), 'minmax' = input_format_parquet_filter_push_down, "
          "'+bloom' = input_format_parquet_bloom_filter_push_down; seek 0 = remote_read_min_bytes_for_seek = 0 (default 4 MiB).\n")
    g = defaultdict(list)
    for e in ev:
        g[(e["signal"], e["config"], e["rank"], e["cols"], e["variant"], e["seek"])].append(e)
    rgk = [k for k in keys if "RowGroup" in k or "Pruned" in k]
    print("| signal | config | service | cols | variant | seek | GETs | KB read | rows read | " + " | ".join(rgk) + " |")
    print("|---|---|---|---|---|---|---|---|---|" + "---|" * len(rgk))
    for (sig, c, rank, cols, v, sk), es in sorted(g.items(), key=lambda kv: (kv[0][0], CFGS.index(kv[0][1]), kv[0][2], kv[0][3], kv[0][4], str(kv[0][5]))):
        med = lambda k: st.median(e["pe"].get(k, 0) for e in es)
        print(f"| {sig} | {c} | {rank} | {cols} | {v} | {sk if sk is not None else 'def'} | {med('S3GetObject'):.0f} | {med('ReadBufferFromS3Bytes') / 1000:.0f} | "
              f"{st.median(e['read_rows'] for e in es):,.0f} | " + " | ".join(f"{med(k):.0f}" for k in rgk) + " |")
    print()


def central():
    sts = lines("raw/statements.jsonl.gz")
    tabs = lines("raw/tables.jsonl")
    if not sts:
        return
    last = {}
    for s in sts:
        k = (s["config"], s["max_batch"], s["rep"])
        last[k] = max(last.get(k, 0), s["attempt"])
    sts = [s for s in sts if s["attempt"] == last[(s["config"], s["max_batch"], s["rep"])] and s.get("exception_code", 0) == 0]
    per = defaultdict(lambda: defaultdict(lambda: [0, 0, 0, 0, 0, 0, 0]))
    for s in sts:
        t = s["tables"][0].split(".")[-1] if s["tables"] else "?"
        sig = "traces" if "traces" in t else "logs" if "logs" in t else t
        a = per[(sig, s["config"], s["max_batch"])][s["rep"]]
        a[0] += s["cpu_us"]; a[1] += s["inserted_rows"] or s["written_rows"]; a[2] += s["sort_us"]
        a[3] += s["blocks"]; a[4] += s["blocks_sorted"]; a[5] += s["part_bytes"]; a[6] += 1
    print("## Central: INSERT … SELECT CPU per row, presorted vs unsorted\n")
    print("The real consumer (`consume --once`), one configuration's 32 × 10k-row objects per signal into a fresh database; "
          "per statement from query_log. CPU = OSCPUVirtualTimeMicroseconds. Sort = MergeTreeDataWriterSortingBlocksMicroseconds; "
          "'already sorted' = MergeTreeDataWriterBlocksAlreadySorted / MergeTreeDataWriterBlocks. Median [min–max] over repetitions.\n")
    print("| signal | objects/statement | config | n | CPU µs/row | Δ vs a | sort µs/row | blocks already sorted | part B/row |")
    print("|---|---|---|---|---|---|---|---|---|")
    for sig in ("traces", "logs"):
        for mb in (1, 32):
            base = None
            for c in ["a-unsorted", "b-sorted-1rg", "d-hash-16rg", "f-range-16rg"]:
                reps = per.get((sig, c, mb))
                if not reps:
                    continue
                cpu = [a[0] / a[1] for a in reps.values() if a[1]]
                srt = [a[2] / a[1] for a in reps.values() if a[1]]
                bs = [a[4] / a[3] for a in reps.values() if a[3]]
                pb = [a[5] / a[1] for a in reps.values() if a[1]]
                if c == "a-unsorted":
                    base = st.median(cpu)
                d = f"{(st.median(cpu) / base - 1) * 100:+.1f}%" if base else "–"
                print(f"| {sig} | {mb} | {c} | {len(cpu)} | {mm(cpu, '{:.2f}')} | {d} | {mm(srt, '{:.3f}')} | {st.median(bs) * 100:.0f}% | {mm(pb, '{:.1f}')} |")
    bad = [t for t in tabs if t.get("rc", 0) != 0]
    print(f"\nRuns: {len({t['db'] for t in tabs})} databases, {len(sts)} statements; consume exit != 0: {len({t['db'] for t in bad})}. "
          f"Environment (central-env.jsonl): {envsum('central-env.jsonl')}.\n")


if __name__ == "__main__":
    print("# bench/sorting: step 1 results (generated by summarize.py)\n")
    direct_out = []
    # direct() fills rgs (row groups per object) for the edge table: run it first, print after.
    import io, contextlib
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        direct()
    edge()
    print(buf.getvalue())
    chreads()
    central()
