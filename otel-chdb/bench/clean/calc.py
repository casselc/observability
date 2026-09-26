#!/usr/bin/env python3
"""The central-sizing calculator's compute() (central-sizing.html), ported
line for line, to evaluate its mid scenario with the current constants and
with the clean ones.

  calc.py [clean.json]     clean.json: {input id: value} overrides
"""
import json, math, sys

DEFAULTS = dict(clusters=20, pods=3000, nodes=200, spansPod=10, logsPod=3, logsNode=5, seriesPod=500, seriesNode=2000, interval=30,
                retention=90, hotDays=1, coldMode="local", dsMode="on", rawDays=14, rollup=300, metricsLayout="b", publishers=3, flush=30,
                replicas=2, vcpu=32, ramRatio=4, usRow=5, usPointB=1.15, usPointA=7.85, fixedMs=17, rollUsB=1, rollUsA=10.5, mergeRow=11,
                mergePointB=6.5, mergePointA=30, headroom=1.75, queryPct=75, bSpan=80, bLog=60, bPointB=6.3, bPointA=26, bSeries=40,
                rollWinB=18, rollWinA=50, bPq=50, bPqPointB=24, bPqPointA=38, batch=10000, edgeGoSpan=7.3, edgeGoLog=5.2, edgeRsSpan=4.5,
                edgeRsLog=3.3, edgePointB=2, edgeGoPointA=7.8, edgeRsPointA=5.5, pPut=0.005, pGet=0.0004, pHot=0.08, pCold=0.045, pS3=0.023)
HOT_SLACK = 1.25


def compute(o):
    pods, nodes = o["clusters"] * o["pods"], o["clusters"] * o["nodes"]
    spans = pods * o["spansPod"]
    logs = pods * o["logsPod"] + nodes * o["logsNode"]
    points = (pods * o["seriesPod"] + nodes * o["seriesNode"]) / max(1, o["interval"])
    ds = o["dsMode"] == "on"
    L = "A" if o["metricsLayout"] == "a" else "B"
    series = pods * o["seriesPod"] + nodes * o["seriesNode"]
    metricObjS = o["clusters"] * max(1, o["publishers"]) * 5 / max(1, o["flush"]) if points > 0 else 0
    fixed = metricObjS * o["fixedMs"] / 1000
    metricCpu = points * o["usPoint" + L] / 1e6 + (points * o["rollUs" + L] / 1e6 if ds else 0)
    insert = (spans + logs) * o["usRow"] / 1e6 + metricCpu + fixed
    merge = ((spans + logs) * o["mergeRow"] + points * o["mergePoint" + L]) / 1e6
    head = (insert + merge) * (o["headroom"] - 1)
    ing = insert + merge + head
    query = ing * o["queryPct"] / 100
    R = max(1, o["replicas"])
    perReplica = ing + query / R
    shards = max(1, math.ceil(perReplica / max(1, o["vcpu"])))
    total = ing * R + query
    day = 86400 / 1e12
    daySpan, dayLog, dayMetric = spans * o["bSpan"] * day, logs * o["bLog"] * day, points * o["bPoint" + L] * day
    hotDays = min(max(0, o["hotDays"]), o["retention"])
    coldDays = max(0, o["retention"] - hotDays)
    dayTL = daySpan + dayLog
    dayRoll = series * (86400 / max(1, o["rollup"])) * o["rollWin" + L] / 1e12 if ds else 0
    seriesTB = series * o["bSeries"] / 1e12 if L == "B" else 0
    rawKeep = min(max(0, o["rawDays"]), o["retention"]) if ds else o["retention"]
    hotRaw, coldRaw = min(hotDays, rawKeep), max(0, rawKeep - hotDays)
    hotCopy = dayTL * hotDays + dayMetric * hotRaw + dayRoll * hotDays + seriesTB
    coldCopy = dayTL * coldDays + dayMetric * coldRaw + dayRoll * coldDays
    hotAll = hotCopy * R * HOT_SLACK
    coldAll = coldCopy * (1 if o["coldMode"] == "s3one" else R)
    edgeRs = (spans * o["edgeRsSpan"] + logs * o["edgeRsLog"] + points * (o["edgeRsPointA"] if L == "A" else o["edgePointB"])) / 1e6
    edgeGo = (spans * o["edgeGoSpan"] + logs * o["edgeGoLog"] + points * (o["edgeGoPointA"] if L == "A" else o["edgePointB"])) / 1e6
    return dict(spans=spans, logs=logs, points=points, rows=spans + logs + points, metricObjS=metricObjS, fixed=fixed, metricCpu=metricCpu,
                insert=insert, merge=merge, head=head, query=query, perReplica=perReplica, shards=shards, nodes_total=shards * R,
                total=total, dayTB=daySpan + dayLog + dayMetric, daySpan=daySpan, dayLog=dayLog, dayMetric=dayMetric,
                hotTB=hotAll, coldTB=coldAll, storTB=hotAll + coldAll, edgeRs=edgeRs, edgeGo=edgeGo)


def main():
    clean = json.load(open(sys.argv[1])) if len(sys.argv) > 1 else {}
    for layout in ("b", "a"):
        old = compute(dict(DEFAULTS, metricsLayout=layout))
        new = compute(dict(DEFAULTS, **clean, metricsLayout=layout))
        print(f"\n### Mid scenario, metrics layout {'B (series table, the default)' if layout == 'b' else 'A (ClickStack tables)'}\n")
        print("| output | current constants | clean constants | Δ |")
        print("|---|---|---|---|")
        for k, lab, f in [("rows", "rows/s", "{:,.0f}"), ("insert", "insert vCPU per replica", "{:.1f}"), ("fixed", "  of it fixed per-object", "{:.2f}"),
                          ("merge", "merge vCPU per replica", "{:.1f}"), ("head", "headroom vCPU per replica", "{:.1f}"),
                          ("query", "query vCPU", "{:.1f}"), ("perReplica", "vCPU per replica", "{:.1f}"), ("shards", "shards (32 vCPU)", "{:d}"),
                          ("nodes_total", "nodes (x2 replicas)", "{:d}"), ("total", "central vCPU, total", "{:.0f}"),
                          ("dayTB", "compressed TB/day, one copy", "{:.2f}"), ("storTB", "storage TB, all copies", "{:.0f}"),
                          ("edgeRs", "edge vCPU, region, Rust", "{:.1f}"), ("edgeGo", "edge vCPU, region, Go", "{:.1f}")]:
            a, b = old[k], new[k]
            d = f"{(b - a) / a * 100:+.0f}%" if a else ""
            print(f"| {lab} | {f.format(a)} | {f.format(b)} | {d} |")


if __name__ == "__main__":
    main()
