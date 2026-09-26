#!/usr/bin/env python3
"""The metric-name picker's cost, per path, summed over the four kinds the
picker lists (gauge, sum, histogram, exponential histogram), median of N runs
(server time, X-ClickHouse-Summary elapsed_ns):

  views-exhaustive   HyperDX 2.39.1 on layout B: the index read is refused (View),
                     so it runs getMetricNames' GROUP BY on the view
  views-search       the same with a name pattern (typing in the picker)
  stock-index        HyperDX on the stock tables: DISTINCT MetricName from
                     mergeTreeIndex() over the parts overlapping the range
  stock-exhaustive   the GROUP BY on the stock tables (what stock pays on search)
  points-direct      the GROUP BY on B's points tables, without the view's join
  helper             the GROUP BY on otel_metrics_names (../sql/metric_picker.sql)

  picker_bench.py --from-ms F --to-ms T [--runs 7] [--b hdx_b] [--stock hdx_stock]
"""
import argparse, json, statistics, urllib.parse, urllib.request

ap = argparse.ArgumentParser()
ap.add_argument("--ch", default="http://127.0.0.1:18123")
ap.add_argument("--b", default="hdx_b")
ap.add_argument("--stock", default="hdx_stock")
ap.add_argument("--from-ms", type=int, required=True)
ap.add_argument("--to-ms", type=int, required=True)
ap.add_argument("--runs", type=int, default=7)
ap.add_argument("--pattern", default="http")
a = ap.parse_args()

KINDS = {"gauge": ("otel_metrics_gauge", "otel_metrics_number_points", "MetricType = 'gauge'", "gauge"),
         "sum": ("otel_metrics_sum", "otel_metrics_number_points", "MetricType = 'sum'", "sum"),
         "histogram": ("otel_metrics_histogram", "otel_metrics_histogram_points", "1", "histogram"),
         "exp": ("otel_metrics_exponential_histogram", "otel_metrics_exponential_histogram_points", "1", "exponential_histogram")}
F, T = f"fromUnixTimestamp64Milli({a.from_ms})", f"fromUnixTimestamp64Milli({a.to_ms})"
TF = f"(TimeUnix >= {F} AND TimeUnix <= {T})"


def exhaustive(db, table, extra="1", pattern=None):
    like = f" AND MetricName ILIKE '%{pattern}%'" if pattern else ""
    order = (f"lower(MetricName) = lower('{pattern}') DESC, positionCaseInsensitive(MetricName, '{pattern}') ASC, MetricName ASC"
             if pattern else "MetricName ASC")
    return (f"SELECT MetricName FROM {db}.{table} WHERE {TF} AND {extra} AND MetricName != ''{like} "
            f"GROUP BY MetricName ORDER BY {order} LIMIT 501")


def index(db, table):
    return (f"SELECT DISTINCT MetricName AS value FROM mergeTreeIndex('{db}', '{table}') WHERE part_name IN ("
            f"SELECT name FROM system.parts WHERE database = '{db}' AND table = '{table}' AND active = 1 AND "
            f"((min_time >= {F} AND min_time <= {T}) OR (max_time <= {T} AND max_time >= {F}) OR (min_time <= {F} AND max_time >= {T}))) "
            f"LIMIT 10000")


def helper(db, mtype):
    return (f"SELECT MetricName FROM {db}.otel_metrics_names WHERE MetricType = '{mtype}' AND Hour >= toStartOfHour({F}) "
            f"AND Hour <= {T} GROUP BY MetricName ORDER BY MetricName ASC LIMIT 501")


def run(q):
    url = a.ch + "/?" + urllib.parse.urlencode({"use_query_cache": "0", "max_rows_to_read": "0"})
    with urllib.request.urlopen(urllib.request.Request(url, data=q.encode()), timeout=300) as r:
        body = r.read().decode()
        s = json.loads(r.headers["X-ClickHouse-Summary"])
        return int(s["elapsed_ns"]) / 1e6, int(s["read_rows"]), int(s["read_bytes"]), len([l for l in body.split("\n") if l])


paths = {
    "views-exhaustive": {k: exhaustive(a.b, v[0]) for k, v in KINDS.items()},
    "views-search": {k: exhaustive(a.b, v[0], pattern=a.pattern) for k, v in KINDS.items()},
    "stock-index": {k: index(a.stock, v[0]) for k, v in KINDS.items()},
    "stock-exhaustive": {k: exhaustive(a.stock, v[0]) for k, v in KINDS.items()},
    "stock-search": {k: exhaustive(a.stock, v[0], pattern=a.pattern) for k, v in KINDS.items()},
    "points-direct": {k: exhaustive(a.b, v[1], v[2]) for k, v in KINDS.items()},
    "helper": {k: helper(a.b, v[3]) for k, v in KINDS.items()},
}
print("| path | ms (sum of 4 kinds, median) | rows read | bytes read | names returned |")
print("|---|---:|---:|---:|---:|")
res = {}
for name, qs in paths.items():
    tot_ms, rows, byts, names = 0.0, 0, 0, 0
    for k, q in qs.items():
        run(q)
        rs = [run(q) for _ in range(a.runs)]
        tot_ms += statistics.median(r[0] for r in rs)
        rows += rs[0][1]
        byts += rs[0][2]
        names += rs[0][3]
    res[name] = tot_ms
    print(f"| {name} | {tot_ms:.1f} | {rows:,} | {byts:,} | {names} |", flush=True)
