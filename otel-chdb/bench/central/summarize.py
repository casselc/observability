"""Median [min-max] tables from results/*.jsonl."""
import json, sys, statistics
from collections import defaultdict


def fmt(xs, f="%.3f"):
    m = statistics.median(xs)
    if min(xs) == max(xs):
        return f % m
    return (f % m) + " [" + (f % min(xs)) + "–" + (f % max(xs)) + "]"


def ingest(path):
    g = defaultdict(list)
    order = []
    for l in open(path):
        r = json.loads(l)
        k = (r["set"], r["source"], r["mode"])
        if k not in g:
            order.append(k)
        g[k].append(r)
    print("| batches | source | mode | n | wall s | rows/s (k) | query CPU s | server CPU s | GET | LIST | HEAD | S3 MB read | parts/marks selected | central parts |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for k in sorted(order, key=lambda k: (k[0], k[2], k[1])):
        rs = g[k]
        wall = [r["wall_s"] for r in rs]
        rps = [r["rows"] / r["wall_s"] / 1000 for r in rs]
        qcpu = [(r["UserTimeMicroseconds"] + r["SystemTimeMicroseconds"]) / 1e6 for r in rs]
        pcpu = [r["proc_cpu_s"] for r in rs]
        ok = all(r["result_rows"] == r["rows"] for r in rs)
        print(f"| {k[0]} | {k[1]} | {k[2]} | {len(rs)} | {fmt(wall)} | {fmt(rps, '%.0f')} | {fmt(qcpu, '%.2f')} | {fmt(pcpu, '%.2f')} | "
              f"{fmt([r['S3GetObject'] for r in rs], '%d')} | {fmt([r['S3ListObjects'] for r in rs], '%d')} | {fmt([r['S3HeadObject'] for r in rs], '%d')} | "
              f"{fmt([r['ReadBufferFromS3Bytes'] / 1e6 for r in rs], '%.2f')} | {rs[0]['SelectedParts']}/{rs[0]['SelectedMarks']} | "
              f"{fmt([r.get('central_parts', 0) for r in rs], '%d') if k[2] == 'insert' else '-'} |" + ("" if ok else " ROWCOUNT MISMATCH"))


if __name__ == "__main__":
    ingest(sys.argv[1])
