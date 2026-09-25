#!/usr/bin/env python3
"""Layout B's wire encodings, before against after (README "Wire size").

"before" is series.byte_stream_split false + series.statistics page (the
encodings up to this change); "after" is the default (BYTE_STREAM_SPLIT on
the float and count columns, no statistics, no page index). The rows are the
same; only the Parquet encodings differ.

  1. bytes per point, per type and overall: both encodings of the fleet
     (130 consecutive 10k-point requests; the first 3, which announce every
     series, excluded) and testgen's 12 mixed 10k-point requests, dumped by
     `tests/series.rs dump_series`;
  2. edge CPU per point, flatten + encode, both encodings interleaved in one
     process (`tests/series.rs wire_cost`, thread CPU time);
  3. central INSERT ... SELECT CPU per point, per points type: fleet requests
     3..34 of both encodings (uploaded through the SeaweedFS filer), inserted
     1 and 32 objects per statement with the consumer's single-block squashed
     settings into sql/series_tables.sql's tables, interleaved; the query's
     own CPU from its ProfileEvents (OSCPUVirtualTimeMicroseconds, native
     protocol), so other load on the server doesn't count; median of REPS.
     The tables' contents are compared at the end (count + hash).

  FLEET=dir D=metrics-data CLICKHOUSE=path/to/clickhouse WORK=scratch-dir \\
    [CARGO_TARGET_DIR=...] [PREFIX=otap-rs-wire] [REPS=11] scripts/series_wire.py OUT.md
"""
import glob, json, os, re, statistics, subprocess, sys, time
import requests
import pyarrow.parquet as pq

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
CH = os.environ.get("CH_URL", "http://127.0.0.1:18123")
FILER = os.environ.get("FILER", "http://127.0.0.1:18888")
S3 = os.environ.get("S3_ROOT", "http://127.0.0.1:18333/otel")
KEY, SECRET = "otel", "otelsecret"
PREFIX = os.environ.get("PREFIX", "otap-rs-wire")
REPS = int(os.environ.get("REPS", "11"))
CONFIGS = {"before": {"SERIES_BSS": "0", "SERIES_STATS": "page"}, "after": {}}
POINTS = ["metrics_number_points", "metrics_histogram_points", "metrics_exponential_histogram_points", "metrics_summary_points"]
# the consumer's statement settings (central.rs ONE_BLOCK, squashed), single-threaded
ONE = {"max_insert_threads": "1", "max_threads": "1", "min_insert_block_size_rows": "1048576",
       "min_insert_block_size_bytes": "4294967296", "max_insert_block_size": "10000000", "max_block_size": "10000000"}


def ch(sql):
    r = requests.post(CH + "/", data=sql.encode(), params={"use_query_condition_cache": 0}, timeout=900)
    if r.status_code != 200:
        raise RuntimeError(f"{r.status_code}: {r.text[:800]}\n{sql[:300]}")
    return r.text.strip()


def qcpu(sql, settings):
    """The statement's own CPU (ms), from its ProfileEvents."""
    args = [os.environ["CLICKHOUSE"], "client", "--port", os.environ.get("CH_TCP", "19000"), "--print-profile-events",
            "--profile-events-delay-ms=-1"] + [f"--{k}={v}" for k, v in settings.items()]
    p = subprocess.run(args + ["-q", sql], capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(p.stderr[-800:])
    return int(re.search(r"\[ 0 \] OSCPUVirtualTimeMicroseconds: (\d+)", p.stderr).group(1)) / 1000


def cargo_test(name, env):
    e = dict(os.environ, **env)
    p = subprocess.run(["cargo", "test", "-q", "--release", "--test", "series", name, "--", "--ignored", "--nocapture"],
                       cwd=ROOT, env=e, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(p.stdout[-2000:] + p.stderr[-2000:])
    return p.stdout


def datasets():
    return {"fleet": (sorted(glob.glob(f"{os.environ['FLEET']}/*.pb")), 3),
            "testgen": (sorted(glob.glob(f"{os.environ['D']}/metrics-mixed-10000-b*.pb")), 0)}


def sizes(d, skip):
    tot, pts = {}, 0
    for f in glob.glob(f"{d}/*.parquet"):
        b = os.path.basename(f)
        if int(b[:4]) < skip:
            continue
        ns = b.split(".")[1]
        tot[ns] = tot.get(ns, 0) + os.path.getsize(f)
        if ns != "metrics_series":
            pts += pq.ParquetFile(f).metadata.num_rows
    return tot, pts


def ddl(table, fq):
    s = open(f"{ROOT}/sql/series_tables.sql").read()
    body = s[s.index(f"CREATE TABLE {{db}}.{table}\n"):]
    body = body[:body.index(";")].replace(f"{{db}}.{table}", fq)
    # as the consumer creates it (central::series_layout_create_table)
    return body.replace("\n)\nENGINE", ",\n    content_key LowCardinality(String),\n    PROJECTION by_content "
                                        "(SELECT content_key, count() GROUP BY content_key)\n)\nENGINE"), body


def structure(body, names):
    types = {}
    for line in body.splitlines():
        m = re.match(r"\s+`?([A-Za-z_.]+)`? (.+?) CODEC", line)
        if m:
            t = re.sub(r"Enum8\(.*\)", "UInt8", m.group(2)).replace("LowCardinality(String)", "String")
            types[m.group(1)] = t
    return ", ".join(f"`{n}` {types[n]}" for n in names)


def main():
    out = sys.argv[1]
    work = os.environ["WORK"]
    lines = [f"# Layout B wire encodings: before / after\n", f"load average at start: {open('/proc/loadavg').read().split()[0]}\n"]
    # 1. bytes
    lines += ["## Parquet bytes per point\n", "| data | encoding | total | " + " | ".join(n.replace("metrics_", "") for n in POINTS + ["metrics_series"]) + " |",
              "|---|---|---|" + "---|" * (len(POINTS) + 1)]
    for ds, (files, skip) in datasets().items():
        for cfg, env in CONFIGS.items():
            d = f"{work}/{cfg}/{ds}"
            os.makedirs(d, exist_ok=True)
            for f in glob.glob(f"{d}/*.parquet"):
                os.remove(f)
            cargo_test("dump_series", dict(env, FILES=",".join(files), OUT=d))
            tot, pts = sizes(d, skip)
            lines.append(f"| {ds} | {cfg} | **{sum(tot.values()) / pts:.2f}** | " +
                         " | ".join(f"{tot.get(n, 0) / pts:.2f}" for n in POINTS + ["metrics_series"]) + " |")
    # 2. edge CPU
    lines += ["\n## Edge flatten + encode CPU (A/B in one process)\n", "```"]
    for ds, (files, skip) in datasets().items():
        o = cargo_test("wire_cost", {"FILES": ",".join(files), "SKIP": str(skip), "REPS": "7" if ds == "fleet" else "15"})
        lines += [f"{ds}: {l}" for l in o.splitlines() if l.startswith(("before", "after"))]
    lines.append("```")
    # 3. central
    db = f"otaprs_wire_{int(time.time())}"
    ch(f"CREATE DATABASE {db}")
    lines += ["\n## Central INSERT ... SELECT, the statement's own CPU (median of %d; [min])\n" % REPS,
              "| type | objects / statement | before µs/point | after µs/point | contents |", "|---|---|---|---|---|"]
    try:
        keys = [f"{n:04d}" for n in range(3, 35)]
        for ns in POINTS:
            table = "otel_" + ns
            for cfg in CONFIGS:
                for k in keys:
                    with open(f"{work}/{cfg}/fleet/{k}.{ns}.parquet", "rb") as fh:
                        requests.post(f"{FILER}/buckets/otel/{PREFIX}/{cfg}/{ns}/{k}.parquet", files={"file": fh}).raise_for_status()
                ch(ddl(table, f"{db}.{ns}_{cfg}")[0])
            names = [l.split("\t")[0] for l in ch(f"DESCRIBE s3('{S3}/{PREFIX}/after/{ns}/0003.parquet', '{KEY}', '{SECRET}', 'Parquet') FORMAT TSV").splitlines()]
            st = structure(ddl(table, "x")[1], names)
            cols = ", ".join(f"`{n}`" for n in names)
            rows = pq.ParquetFile(f"{work}/after/fleet/0003.{ns}.parquet").metadata.num_rows
            res = {}
            for r in range(REPS):
                for n in (1, 32):
                    ks = keys if n == 32 else [keys[r % len(keys)]]
                    for cfg in CONFIGS:
                        u = f"{S3}/{PREFIX}/{cfg}/{ns}/" + (ks[0] if n == 1 else "{" + ",".join(ks) + "}") + ".parquet"
                        sql = (f"INSERT INTO {db}.{ns}_{cfg} ({cols}, content_key) SELECT {cols}, 'k' "
                               f"FROM s3('{u}', '{KEY}', '{SECRET}', 'Parquet', '{st}')")
                        res.setdefault((n, cfg), []).append(qcpu(sql, ONE) * 1000 / (rows * n))
            h = {cfg: ch(f"SELECT count(), sum(cityHash64(* EXCEPT content_key)) FROM {db}.{ns}_{cfg}") for cfg in CONFIGS}
            same = "equal" if len(set(h.values())) == 1 else f"DIFFER {h}"
            for n in (1, 32):
                f = lambda c: f"{statistics.median(res[(n, c)]):.2f} [{min(res[(n, c)]):.2f}]"
                lines.append(f"| {ns.replace('metrics_', '')} | {n} | {f('before')} | {f('after')} | {same} |")
    finally:
        ch(f"DROP DATABASE IF EXISTS {db}")
        requests.delete(f"{FILER}/buckets/otel/{PREFIX}/", params={"recursive": "true"})
    lines.append(f"\nload average at end: {open('/proc/loadavg').read().split()[0]}")
    open(out, "w").write("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
