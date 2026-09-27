#!/usr/bin/env python3
"""Databases for the HyperDX evaluation (../README.md), on CH_URL:

  setup_db.py stock DB        the stock ClickStack metrics tables (../sql/stock_metrics.sql)
  setup_db.py views DB [VDB]  the compatibility views (../../otap-rs/sql/series_views.sql)
                              over layout B in DB (VDB defaults to DB), once the
                              consumer has created B's tables
  setup_db.py picker DB       the (MetricName, ServiceName) helper table + MVs (../sql/metric_picker.sql)
  setup_db.py file DB SQL     any {db}-templated SQL file
  setup_db.py consumer DB     the consumer's otel_traces / otel_logs with their key-value rollups
                              (../../otap-rs/sql/otel_*.sql: ClickStack 2.39.1's DDL), before the
                              consumer runs (it keeps existing tables)
"""
import os, sys, time, urllib.request

CH = os.environ.get("CH_URL", "http://127.0.0.1:18123")
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)


def q(sql):
    req = urllib.request.Request(CH + "/", data=sql.encode(), method="POST")
    try:
        with urllib.request.urlopen(req) as r:
            return r.read().decode()
    except urllib.error.HTTPError as e:
        raise SystemExit(f"{e.code}: {e.read().decode()[:2000]}\n--- {sql[:400]}")


def statements(path, **subs):
    text = open(path).read()
    body = "\n".join(l for l in text.split("\n") if not l.strip().startswith("--"))
    for k, v in subs.items():
        body = body.replace("{" + k + "}", v)
    return [s.strip() for s in body.split(";") if s.strip()]


def run(path, **subs):
    for s in statements(path, **subs):
        q(s)


def main():
    cmd, db = sys.argv[1], sys.argv[2]
    q(f"CREATE DATABASE IF NOT EXISTS {db}")
    if cmd == "stock":
        run(os.path.join(ROOT, "sql/stock_metrics.sql"), db=db)
    elif cmd == "views":
        vdb = sys.argv[3] if len(sys.argv) > 3 else db
        need = ["otel_metrics_series", "otel_metrics_number_points", "otel_metrics_histogram_points",
                "otel_metrics_exponential_histogram_points", "otel_metrics_summary_points"]
        for _ in range(120):
            have = set(q(f"SELECT name FROM system.tables WHERE database = '{db}' FORMAT TSV").split())
            if all(t in have for t in need):
                break
            time.sleep(1)
        else:
            raise SystemExit(f"{db}: B's tables not all there: {sorted(have)}")
        q(f"CREATE DATABASE IF NOT EXISTS {vdb}")
        for s in statements(os.path.join(ROOT, "../otap-rs/sql/series_views.sql"), db=db, vdb=vdb):
            name = s.split("CREATE VIEW ")[1].split()[0]
            q(f"DROP VIEW IF EXISTS {name}")
            q(s)
    elif cmd == "picker":
        run(os.path.join(ROOT, "sql/metric_picker.sql"), db=db)
    elif cmd == "file":
        run(sys.argv[3], db=db)
    elif cmd == "consumer":
        for sig in ("traces", "logs"):
            run(os.path.join(ROOT, f"../otap-rs/sql/otel_{sig}.sql"), table=f"{db}.otel_{sig}")
    print(q(f"SELECT name, engine FROM system.tables WHERE database = '{db}' ORDER BY name FORMAT TSV"))


if __name__ == "__main__":
    main()
