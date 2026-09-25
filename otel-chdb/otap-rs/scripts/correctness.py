#!/usr/bin/env python3
"""Row-level comparison of the Rust exporter's objects with the parquetgo
reference, on the ClickHouse server, as ../parquetgo/compare/correctness_test.go
does for chDB vs Go: count + sum(cityHash64(...)) with the explicit structure
and with schema inference, the inferred schema, EXCEPT in both directions,
and INSERT ... SELECT into central-typed otel_traces / otel_logs.

The envelope columns that identify the producer run (producer_id,
producer_epoch, batch_id, received_at) differ by construction and are
checked separately; row_ordinal and schema_version are compared.

  correctness.py RUN [direct|via_otap|otapgrpc ...]

(otapgrpc: the objects of scripts/otap_e2e.sh, sent as OTAP by the Go
otelarrow producer into the upstream OTAP receiver.) S3_PREFIX (default
otap-rs) is the prefix the run's objects are under, in the bucket.
"""
import os, sys, requests

CH = os.environ.get("CH_URL", "http://127.0.0.1:18123")
S3 = os.environ.get("S3_ROOT", "http://127.0.0.1:18333/otel")
KEY, SECRET = os.environ.get("S3_KEY", "otel"), os.environ.get("S3_SECRET", "otelsecret")
PREFIX = os.environ.get("S3_PREFIX", "otap-rs")

ENV_ST = ", producer_id String, producer_epoch String, batch_id UInt64, row_ordinal UInt32, received_at DateTime64(9), schema_version UInt16"
TRACE_ST = ("Timestamp DateTime64(9), TraceId String, SpanId String, ParentSpanId String, TraceState String, SpanName String, "
            "SpanKind String, ServiceName String, ResourceAttributes Map(String, String), ScopeName String, ScopeVersion String, "
            "SpanAttributes Map(String, String), Duration UInt64, StatusCode String, StatusMessage String, "
            "`Events.Timestamp` Array(DateTime64(9)), `Events.Name` Array(String), `Events.Attributes` Array(Map(String, String)), "
            "`Links.TraceId` Array(String), `Links.SpanId` Array(String), `Links.TraceState` Array(String), "
            "`Links.Attributes` Array(Map(String, String))" + ENV_ST)
LOG_ST = ("Timestamp DateTime64(9), TraceId String, SpanId String, TraceFlags UInt8, SeverityText String, SeverityNumber UInt8, "
          "ServiceName String, Body String, ResourceSchemaUrl String, ResourceAttributes Map(String, String), ScopeSchemaUrl String, "
          "ScopeName String, ScopeVersion String, ScopeAttributes Map(String, String), LogAttributes Map(String, String), "
          "EventName String" + ENV_ST)
CENTRAL_ENV = (", producer_id LowCardinality(String), producer_epoch LowCardinality(String), batch_id UInt64, row_ordinal UInt32, "
               "received_at DateTime64(9), schema_version UInt16")
CENTRAL = {
    "traces": """(Timestamp DateTime64(9), TraceId String, SpanId String, ParentSpanId String, TraceState String,
  SpanName LowCardinality(String), SpanKind LowCardinality(String), ServiceName LowCardinality(String),
  ResourceAttributes Map(LowCardinality(String), String), ScopeName String, ScopeVersion String,
  SpanAttributes Map(LowCardinality(String), String), Duration UInt64, StatusCode LowCardinality(String), StatusMessage String,
  Events Nested (Timestamp DateTime64(9), Name LowCardinality(String), Attributes Map(LowCardinality(String), String)),
  Links Nested (TraceId String, SpanId String, TraceState String, Attributes Map(LowCardinality(String), String))"""
    + CENTRAL_ENV + ") ENGINE = MergeTree ORDER BY (ServiceName, SpanName, toDateTime(Timestamp))",
    "logs": """(Timestamp DateTime64(9), TraceId String, SpanId String, TraceFlags UInt8, SeverityText LowCardinality(String),
  SeverityNumber UInt8, ServiceName LowCardinality(String), Body String, ResourceSchemaUrl LowCardinality(String),
  ResourceAttributes Map(LowCardinality(String), String), ScopeSchemaUrl LowCardinality(String), ScopeName String,
  ScopeVersion LowCardinality(String), ScopeAttributes Map(LowCardinality(String), String),
  LogAttributes Map(LowCardinality(String), String), EventName String""" + CENTRAL_ENV + ") ENGINE = MergeTree ORDER BY (ServiceName, Timestamp)",
}
TRACE_COLS = ("Timestamp, TraceId, SpanId, ParentSpanId, TraceState, SpanName, SpanKind, ServiceName, ResourceAttributes, ScopeName, "
              "ScopeVersion, SpanAttributes, Duration, StatusCode, StatusMessage, `Events.Timestamp`, `Events.Name`, `Events.Attributes`, "
              "`Links.TraceId`, `Links.SpanId`, `Links.TraceState`, `Links.Attributes`")
LOG_COLS = ("Timestamp, TraceId, SpanId, TraceFlags, SeverityText, SeverityNumber, ServiceName, Body, ResourceSchemaUrl, "
            "ResourceAttributes, ScopeSchemaUrl, ScopeName, ScopeVersion, ScopeAttributes, LogAttributes, EventName")
ALL_ENV = ", producer_id, producer_epoch, batch_id, row_ordinal, received_at, schema_version"
CMP_ENV = ", row_ordinal, schema_version"


def ch(sql):
    r = requests.post(CH + "/", data=sql.encode(), params={"use_query_condition_cache": 0}, timeout=600)
    if r.status_code != 200:
        raise RuntimeError(f"{r.status_code}: {r.text[:600]}\n{sql[:300]}")
    return r.text.strip()


def ls(prefix):
    """Object keys under a prefix (via ClickHouse: s3 LIST through a glob)."""
    return ch(f"SELECT DISTINCT _path FROM s3('{S3}/{prefix}/**', '{KEY}', '{SECRET}', 'One') ORDER BY _path FORMAT TSV").splitlines()


def main():
    run = sys.argv[1]
    paths = sys.argv[2:] or ["direct"]
    fails = 0
    out = []

    def check(name, ok, detail=""):
        nonlocal fails
        fails += 0 if ok else 1
        line = f"{'PASS' if ok else 'FAIL'} {name}{': ' + detail if detail else ''}"
        print(line, flush=True)
        out.append(line)

    db = f"otaprs_corr_{run}"
    ch(f"CREATE DATABASE IF NOT EXISTS {db}")
    try:
        for path in paths:
            for sig, st, cols in [("traces", TRACE_ST, TRACE_COLS), ("logs", LOG_ST, LOG_COLS)]:
                keys = [k for k in ls(f"{PREFIX}/corr/{run}/{path}/{sig}") if k.endswith(".parquet")]
                for ds, seq in [("testgen-3000", 0), ("nasty-700", 1)]:
                    mine = [k for k in keys if k.endswith(f"/{seq:020d}.parquet")]
                    if not mine:
                        check(f"{path} {sig} {ds}", False, "no object (the request was rejected)")
                        continue
                    key = mine[0].split("/", 1)[1]  # drop the bucket
                    epoch = key.split("/")[-2]

                    def src(which, structured=True):
                        url = (f"{S3}/{key}" if which == "rust" else
                               f"{S3}/{PREFIX}/corr/{run}/ref/cmp/{sig}/v1/{ds}/{run}/*/*.parquet")
                        return f"s3('{url}', '{KEY}', '{SECRET}', 'Parquet'" + (f", '{st}')" if structured else ")")

                    c = cols + CMP_ENV
                    tag = f"{path} {sig} {ds}"
                    sums = {w: ch(f"SELECT count(), sum(cityHash64({c})) FROM {src(w)}") for w in ("rust", "ref")}
                    check(f"{tag} count+hash (structure)", sums["rust"] == sums["ref"], f"rust {sums['rust']} ref {sums['ref']}")
                    inf = {w: ch(f"SELECT count(), sum(cityHash64({c})) FROM {src(w, False)}") for w in ("rust", "ref")}
                    check(f"{tag} count+hash (inferred)", inf["rust"] == inf["ref"], f"rust {inf['rust']} ref {inf['ref']}")
                    d = {w: ch(f"DESCRIBE {src(w, False)}") for w in ("rust", "ref")}
                    check(f"{tag} inferred schema", d["rust"] == d["ref"])
                    for a, b in [("rust", "ref"), ("ref", "rust")]:
                        n = ch(f"SELECT count() FROM (SELECT {c} FROM {src(a)} EXCEPT SELECT {c} FROM {src(b)})")
                        check(f"{tag} rows in {a} not in {b}", n == "0", n)
                    env = ch(f"SELECT uniqExact(producer_id), any(producer_id), uniqExact(producer_epoch), any(producer_epoch), "
                             f"uniqExact(batch_id), any(batch_id), min(received_at) > now64(9) - INTERVAL 1 DAY, "
                             f"min(row_ordinal), max(row_ordinal), any(schema_version) FROM {src('rust')} FORMAT TSV").split("\t")
                    ok = env[0] == "1" and env[2] == "1" and env[3] == epoch and env[4] == "1" and env[5] == str(seq) and env[6] == "1"
                    check(f"{tag} envelope", ok, f"producer={env[1]} epoch={env[3]} batch_id={env[5]} ordinals {env[7]}..{env[8]} schema={env[9]}")
                    # central: INSERT ... SELECT into the ClickStack-typed table
                    h = {}
                    for w in ("rust", "ref"):
                        t = f"{db}.{sig}_{path}_{ds.replace('-', '_')}_{w}"
                        ch(f"DROP TABLE IF EXISTS {t}")
                        ch(f"CREATE TABLE {t} {CENTRAL[sig]}")
                        ch(f"INSERT INTO {t} SELECT {cols}{ALL_ENV} FROM {src(w)}")
                        cc = c.replace("`Events.", "`Events.").replace("`Links.", "`Links.")
                        h[w] = ch(f"SELECT count(), sum(cityHash64({cc})) FROM {t}")
                    check(f"{tag} central insert", h["rust"] == h["ref"], f"rust {h['rust']} ref {h['ref']}")
    finally:
        ch(f"DROP DATABASE IF EXISTS {db}")
    print(f"{fails} failed")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
