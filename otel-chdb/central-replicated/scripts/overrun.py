#!/usr/bin/env python3
"""overrun.py: how far past max_execution_time replicated inserts committed.

  overrun.py OUT BUDGET_S QUERY_ID_LIKE DB [T_START T_END]

Reads system.query_log (the inserts' end: finished, or the exception) and
system.part_log (NewPart: the commit, by query_id) on both replicas (R1, R2
from the environment, default the central-replicated ports) for inserts whose
query_id is LIKE QUERY_ID_LIKE into database DB, and writes OUT/inserts.tsv
(query_id, replica, start ms, duration ms, type, exception code, commit ms,
max_execution_time) and a summary on stdout. overrun = commit - start -
max_execution_time. BUDGET_S is the default max_execution_time when a query's
own setting isn't logged.
"""
import collections
import os
import sys
import urllib.request

out, budget_s, like, db = sys.argv[1], float(sys.argv[2]), sys.argv[3], sys.argv[4]
window = f" AND event_time BETWEEN toDateTime({sys.argv[5]}) AND toDateTime({sys.argv[6]})" if len(sys.argv) > 6 else ""
R = [("r1", os.environ.get("R1", "http://127.0.0.1:28123")), ("r2", os.environ.get("R2", "http://127.0.0.1:38123"))]


def q(url, sql):
    return urllib.request.urlopen(urllib.request.Request(url + "/", data=sql.encode()), timeout=120).read().decode()


rows = []
for name, url in R:
    q(url, "SYSTEM FLUSH LOGS")
    sql = f"""
    SELECT q.query_id, '{name}', toUnixTimestamp64Milli(q.query_start_time_microseconds), q.query_duration_ms, q.type, q.exception_code,
           ifNull(toUnixTimestamp64Milli(p.t), 0), q.met
    FROM (SELECT query_id, query_start_time_microseconds, query_duration_ms, toString(type) AS type, exception_code,
                 toFloat64OrZero(Settings['max_execution_time']) AS met
          FROM system.query_log WHERE query_id LIKE '{like}' AND type != 'QueryStart' AND query_kind = 'Insert'{window}) q
    LEFT JOIN (SELECT query_id, max(event_time_microseconds) AS t FROM system.part_log
               WHERE event_type = 'NewPart' AND database = '{db}' AND query_id LIKE '{like}' GROUP BY query_id) p
    ON q.query_id = p.query_id FORMAT TSV"""
    rows += [l.split("\t") for l in q(url, sql).splitlines() if l.strip()]
with open(f"{out}/inserts.tsv", "w") as f:
    for r in rows:
        f.write("\t".join(r) + "\n")


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p * len(xs)))] if xs else 0.0


met = lambda r: (float(r[7]) or budget_s) * 1000
by = collections.Counter((r[4], r[5]) for r in rows)
committed = [r for r in rows if int(r[6]) > 0]
over = [(int(r[6]) - int(r[2]) - met(r)) / 1000 for r in committed]
end_over = [(int(r[3]) - met(r)) / 1000 for r in rows]
print(f"inserts {len(rows)}: " + ", ".join(f"{t} {'code ' + c if c != '0' else ''}: {n}".replace(" :", ":") for (t, c), n in sorted(by.items())))
print(f"committed {len(committed)}; commit - start - max_execution_time: p50 {pct(over, .5):.2f} s, p90 {pct(over, .9):.2f}, "
      f"p99 {pct(over, .99):.2f}, p99.9 {pct(over, .999):.2f}, max {max(over, default=0):.2f}")
print("  committed more than N s past max_execution_time: " + ", ".join(f">{n} s: {sum(1 for o in over if o > n)}" for n in (0, 1, 2, 5, 8, 10, 12, 15)))
err_commit = [r for r in committed if r[4] != "QueryFinish"]
print(f"committed although the statement ended in an error: {len(err_commit)} {dict(collections.Counter(r[5] for r in err_commit))}")
print(f"statement end - start - max_execution_time: p99 {pct(end_over, .99):.2f} s, max {max(end_over, default=0):.2f} s; "
      f"ended more than 10 s past: {sum(1 for d in end_over if d > 10)}")
for r in sorted(committed, key=lambda r: int(r[6]) - int(r[2]) - met(r))[-5:]:
    print(f"  worst: {r[0]} {r[1]} {r[4]} code {r[5]}: committed {(int(r[6]) - int(r[2]) - met(r)) / 1000:.2f} s past max_execution_time {met(r) / 1000:.0f} s")
