#!/usr/bin/env python3
"""Metrics: row-level comparison of the Rust exporter's objects with the two
references, on the ClickHouse server, per metric type and dataset:

  - the contrib clickhouseexporter v0.161.0's own rows (tools/cmd/metricsref:
    the exporter, create_schema on, into a private database);
  - parquetgo's objects for the same pmetric input (otlpgen -metrics -ref),
    written to ../parquetgo/METRICS_SCHEMA.md.

Checks per (type, dataset), as correctness.py does for traces and logs:
  vs parquetgo: count + sum(cityHash64(content, row_ordinal, schema_version))
    with the explicit structure and with schema inference; DESCRIBE of the
    inferred schema; EXCEPT in both directions; the envelope.
  vs contrib: INSERT ... SELECT the object into a table created AS the
    exporter's own table (its DDL: LowCardinality, DateTime, Nested), then
    count + sum(cityHash64(every column)) and EXCEPT in both directions
    against the exporter's rows. The same for parquetgo's objects, so a
    disagreement between the two references shows up too.

  metrics_correctness.py RUN DATADIR [direct|via_otap]

RUN names the S3 prefix metrics-rs/corr/RUN/{path,ref}; the Rust objects must
already be there (scripts/metrics_e2e.sh sends them): one exporter run per
dataset, under metrics-rs/corr/RUN/PATH/DATASET/<type>/<epoch>/, slot 0.
"""
import os, sys, subprocess, requests

CH = os.environ.get("CH_URL", "http://127.0.0.1:18123")
S3 = os.environ.get("S3_ROOT", "http://127.0.0.1:18333/otel")
KEY, SECRET = os.environ.get("S3_KEY", "otel"), os.environ.get("S3_SECRET", "otelsecret")

TYPES = ["metrics_gauge", "metrics_sum", "metrics_histogram", "metrics_exponential_histogram", "metrics_summary"]
DATASETS = [("testgen-3000", 0), ("nasty-700", 1), ("extra", 2)]

C = ("ResourceAttributes Map(String, String), ResourceSchemaUrl String, ScopeName String, ScopeVersion String, "
     "ScopeAttributes Map(String, String), ScopeDroppedAttrCount UInt32, ScopeSchemaUrl String, ServiceName String, "
     "MetricName String, MetricDescription String, MetricUnit String, Attributes Map(String, String), "
     "StartTimeUnix DateTime, TimeUnix DateTime")
X = ("`Exemplars.FilteredAttributes` Array(Map(String, String)), `Exemplars.TimeUnix` Array(DateTime), "
     "`Exemplars.Value` Array(Float64), `Exemplars.SpanId` Array(String), `Exemplars.TraceId` Array(String)")
E = ("producer_id String, producer_epoch String, batch_id UInt64, row_ordinal UInt32, received_at DateTime64(9), "
     "schema_version UInt16")
ST = {
    "metrics_gauge": f"{C}, Value Float64, Flags UInt32, {X}, {E}",
    "metrics_sum": f"{C}, Value Float64, Flags UInt32, {X}, AggregationTemporality Int32, IsMonotonic Bool, {E}",
    "metrics_histogram": f"{C}, Count UInt64, Sum Float64, BucketCounts Array(UInt64), ExplicitBounds Array(Float64), {X}, "
                         f"Flags UInt32, Min Float64, Max Float64, AggregationTemporality Int32, {E}",
    "metrics_exponential_histogram": f"{C}, Count UInt64, Sum Float64, Scale Int32, ZeroCount UInt64, PositiveOffset Int32, "
                                     f"PositiveBucketCounts Array(UInt64), NegativeOffset Int32, NegativeBucketCounts Array(UInt64), {X}, "
                                     f"Flags UInt32, Min Float64, Max Float64, AggregationTemporality Int32, {E}",
    "metrics_summary": f"{C}, Count UInt64, Sum Float64, `ValueAtQuantiles.Quantile` Array(Float64), "
                       f"`ValueAtQuantiles.Value` Array(Float64), Flags UInt32, {E}",
}
ENV_N = 6


def cols_of(t):
    """The content columns (quoted), from the structure."""
    out, depth, cur = [], 0, ""
    for ch_ in ST[t]:
        if ch_ == "," and depth == 0:
            out.append(cur.strip()); cur = ""; continue
        depth += ch_ == "("
        depth -= ch_ == ")"
        cur += ch_
    out.append(cur.strip())
    names = []
    for c in out:
        n = c.split("`")[1] if c.startswith("`") else c.split(" ")[0]
        names.append(f"`{n}`")
    return names[:-ENV_N]


def ch(sql, **params):
    p = {"use_query_condition_cache": 0}
    p.update(params)
    r = requests.post(CH + "/", data=sql.encode(), params=p, timeout=900)
    if r.status_code != 200:
        raise RuntimeError(f"{r.status_code}: {r.text[:800]}\n{sql[:400]}")
    return r.text.strip()


def ls(prefix):
    return ch(f"SELECT DISTINCT _path FROM s3('{S3}/{prefix}/**', '{KEY}', '{SECRET}', 'One') ORDER BY _path FORMAT TSV").splitlines()


def main():
    run, data = sys.argv[1], sys.argv[2]
    paths = sys.argv[3:] or ["direct"]
    tools = os.environ.get("T", "")
    fails, results = 0, []

    def check(name, ok, detail=""):
        nonlocal fails
        fails += 0 if ok else 1
        line = f"{'PASS' if ok else 'FAIL'} {name}{': ' + detail if detail else ''}"
        print(line, flush=True)
        results.append(line)

    refdb = f"otaprs_mref_{run}"
    db = f"otaprs_mcorr_{run}"
    # 1. contrib's rows, one database per dataset
    files = [f"{data}/metrics-{d}.pb" for d, _ in DATASETS]
    out = subprocess.run([f"{tools}/metricsref", "-db", refdb, "-db-per-file", *files], capture_output=True, text=True)
    print(out.stdout + out.stderr, end="")
    if out.returncode != 0:
        sys.exit("metricsref failed")
    ch(f"CREATE DATABASE IF NOT EXISTS {db}")
    try:
        for path in paths:
            for t in TYPES:
                for ds, seq in DATASETS:
                    tag = f"{path} {t} {ds}"
                    keys = [k for k in ls(f"metrics-rs/corr/{run}/{path}/{ds}/{t}") if k.endswith(".parquet")]
                    mine = [k for k in keys if k.endswith(f"/{0:020d}.parquet")]
                    if not mine:
                        check(tag, False, "no object (the request was rejected)")
                        continue
                    key = mine[0].split("/", 1)[1]
                    epoch = key.split("/")[-2]
                    ref_url = f"{S3}/metrics-rs/corr/{run}/ref/cmp/{t}/v1/{ds}/{run}/*/*.parquet"

                    def src(which, structured=True):
                        url = f"{S3}/{key}" if which == "rust" else ref_url
                        return f"s3('{url}', '{KEY}', '{SECRET}', 'Parquet'" + (f", '{ST[t]}')" if structured else ")")

                    cols = cols_of(t)
                    c = ", ".join(cols) + ", row_ordinal, schema_version"
                    # -- vs parquetgo's objects
                    s = {w: ch(f"SELECT count(), sum(cityHash64({c})) FROM {src(w)}") for w in ("rust", "ref")}
                    check(f"{tag} vs parquetgo: count+hash (structure)", s["rust"] == s["ref"], f"rust {s['rust']} parquetgo {s['ref']}")
                    s = {w: ch(f"SELECT count(), sum(cityHash64({c})) FROM {src(w, False)}") for w in ("rust", "ref")}
                    check(f"{tag} vs parquetgo: count+hash (inferred)", s["rust"] == s["ref"], f"rust {s['rust']} parquetgo {s['ref']}")
                    d = {w: ch(f"DESCRIBE {src(w, False)}") for w in ("rust", "ref")}
                    check(f"{tag} vs parquetgo: inferred schema", d["rust"] == d["ref"],
                          "" if d["rust"] == d["ref"] else f"\n rust: {d['rust'][:600]}\n pgo:  {d['ref'][:600]}")
                    for a, b in [("rust", "ref"), ("ref", "rust")]:
                        n = ch(f"SELECT count() FROM (SELECT {c} FROM {src(a)} EXCEPT SELECT {c} FROM {src(b)})")
                        check(f"{tag} vs parquetgo: rows in {a} not in {b}", n == "0", n)
                    env = ch(f"SELECT uniqExact(producer_id), any(producer_id), uniqExact(producer_epoch), any(producer_epoch), "
                             f"uniqExact(batch_id), any(batch_id), min(received_at) > now64(9) - INTERVAL 1 DAY, "
                             f"min(row_ordinal), max(row_ordinal), count(), any(schema_version) FROM {src('rust')} FORMAT TSV").split("\t")
                    ok = (env[0] == "1" and env[2] == "1" and env[3] == epoch and env[4] == "1" and env[5] == "0"
                          and env[6] == "1" and env[7] == "0" and int(env[8]) == int(env[9]) - 1)
                    check(f"{tag} envelope", ok, f"producer={env[1]} epoch={env[3]} batch_id={env[5]} ordinals {env[7]}..{env[8]} rows={env[9]} schema={env[10]}")
                    # -- vs contrib's rows, through a table with the exporter's own DDL
                    reft = f"{refdb}_{seq}.otel_{t}"
                    h = {"contrib": ch(f"SELECT count(), sum(cityHash64({', '.join(cols)})) FROM {reft}")}
                    for w in ("rust", "ref"):
                        tt = f"{db}.{path}_{t}_{ds.replace('-', '_')}_{w}"
                        ch(f"DROP TABLE IF EXISTS {tt}")
                        ch(f"CREATE TABLE {tt} AS {reft}")
                        ch(f"INSERT INTO {tt} ({', '.join(cols)}) SELECT {', '.join(cols)} FROM {src(w)}")
                        h[w] = ch(f"SELECT count(), sum(cityHash64({', '.join(cols)})) FROM {tt}")
                        who = "rust" if w == "rust" else "parquetgo"
                        check(f"{tag} {who} vs contrib rows: count+hash", h[w] == h["contrib"], f"{who} {h[w]} contrib {h['contrib']}")
                        for a, b in [(tt, reft), (reft, tt)]:
                            n = ch(f"SELECT count() FROM (SELECT {', '.join(cols)} FROM {a} EXCEPT SELECT {', '.join(cols)} FROM {b})")
                            check(f"{tag} {who} vs contrib rows: in {'object' if a == tt else 'contrib'} only", n == "0", n)
    finally:
        if not os.environ.get("KEEP"):
            ch(f"DROP DATABASE IF EXISTS {db}")
            for _, seq in DATASETS:
                ch(f"DROP DATABASE IF EXISTS {refdb}_{seq}")
    print(f"{fails} failed")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
