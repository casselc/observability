#!/usr/bin/env python3
"""Central INSERT ... SELECT cost of the Rust exporter's objects against
parquetgo's, the same way ../otap/central_bench_test.go measured the Go OTAP
variants: 10 objects of the same testgen 10k-row batch per layout, inserted
1 or 10 at a time into the central-typed otel_traces / otel_logs, with
default settings and single-threaded; server-wide system.events deltas
(User+SystemTimeMicroseconds, S3GetObject, S3HeadObject,
ReadBufferFromS3Bytes) around each statement (the server's query_log is
off), median [min-max] of REPS runs. Also: the consumer's own insert (one
object, ONE_BLOCK settings, dedup token) and its check query.

  central_bench.py ENC_BIN PUBBENCH_BIN DATA_DIR OUT.md
"""
import json, os, statistics, subprocess, sys, time, requests

CH = os.environ.get("CH_URL", "http://127.0.0.1:18123")
S3 = os.environ.get("S3_ROOT", "http://127.0.0.1:18333/otel")
KEY, SECRET = "otel", "otelsecret"
REPS = int(os.environ.get("REPS", "3"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

STRUCT = {}
COLS = {}


def ch(sql, settings=None):
    p = {"use_query_condition_cache": "0"}
    p.update(settings or {})
    r = requests.post(CH + "/", params=p, data=sql.encode(), timeout=600)
    if r.status_code != 200:
        raise RuntimeError(f"{r.status_code}: {r.text[:500]}\n{sql[:300]}")
    return r.text.strip(), json.loads(r.headers.get("X-ClickHouse-Summary", "{}") or "{}")


def events():
    out, _ = ch("SELECT event, value FROM system.events WHERE event IN ('UserTimeMicroseconds','SystemTimeMicroseconds',"
                "'S3GetObject','S3HeadObject','ReadBufferFromS3Bytes') FORMAT TSV")
    d = {k: 0.0 for k in ["UserTimeMicroseconds", "SystemTimeMicroseconds", "S3GetObject", "S3HeadObject", "ReadBufferFromS3Bytes"]}
    for l in out.splitlines():
        k, v = l.split("\t")
        d[k] = float(v)
    return d


def measured(sql, settings):
    a = events()
    t0 = time.perf_counter()
    _, summ = ch(sql, settings)
    wall = (time.perf_counter() - t0) * 1000
    b = events()
    dd = {k: b[k] - a[k] for k in a}
    return {"wall": wall, "cpu": (dd["UserTimeMicroseconds"] + dd["SystemTimeMicroseconds"]) / 1000,
            "mem": float(summ.get("memory_usage", 0)) / 1e6, "gets": dd["S3GetObject"], "heads": dd["S3HeadObject"],
            "mb": dd["ReadBufferFromS3Bytes"] / 1e6}


def fmt(xs, k):
    v = sorted(x[k] for x in xs)
    return f"{v[len(v)//2]:.0f} [{v[0]:.0f}–{v[-1]:.0f}]"


def main():
    enc, pubbench, data, out = sys.argv[1:5]
    from importlib import util
    spec = util.spec_from_file_location("central_consts", os.path.join(os.path.dirname(__file__), "correctness.py"))
    cc = util.module_from_spec(spec)
    spec.loader.exec_module(cc)
    run = f"cb{int(time.time())}"
    base = f"otap-rs/central/{run}"
    env = dict(os.environ, CHDB_TEST_S3=S3, CHDB_TEST_S3_KEY=KEY, CHDB_TEST_S3_SECRET=SECRET)
    layouts = {}
    for sig in os.environ.get("SIGS", "traces,logs").split(","):
        f = f"{data}/{sig}-testgen-10000.pb"
        for name, extra in [("rust (TraceId bloom)", []), ("rust, no bloom", ["--bloom", "none"])]:
            tag = "rust" if not extra else "rust-nobloom"
            subprocess.run([enc, "--file", f, "--signal", sig, "--warmup", "1", "--batches", "9", "--s3", f"{S3}/{base}/{tag}",
                            "--key", KEY, "--secret", SECRET], check=True, stdout=subprocess.DEVNULL)
            layouts[(sig, name)] = lambda n, sig=sig, tag=tag: (
                f"{S3}/{base}/{tag}/{sig}/*/" + ("{" + ",".join(f"{i:020d}" for i in range(n)) + "}" if n > 1 else f"{0:020d}") + ".parquet")
        for name, extra in [("parquetgo (all blooms)", []), ("parquetgo, no bloom", ["-bloom=false"])]:
            tag = "pg" if not extra else "pg-nobloom"
            subprocess.run([pubbench, "-impl", "parquet-go", "-url", f"{S3}/{base}/{tag}", "-signal", sig, "-warmup", "1",
                            "-batches", "9"] + extra, check=True, stdout=subprocess.DEVNULL, env=env)
            layouts[(sig, name)] = lambda n, sig=sig, tag=tag: (
                f"{S3}/{base}/{tag}/cmp/{sig}/v1/*/*/*/" + ("{" + ",".join(f"{i:020d}" for i in range(1, n + 1)) + "}" if n > 1 else f"{1:020d}") + ".parquet")
    db = f"otaprs_{run}"
    ch(f"CREATE DATABASE IF NOT EXISTS {db}")
    default = {}
    single = {"max_threads": "1", "max_insert_threads": "1", "min_insert_block_size_rows": "0", "min_insert_block_size_bytes": "0",
              "max_insert_block_size": "10000000", "max_block_size": "10000000"}
    lines = ["| signal | layout | objects | settings | wall ms | server CPU ms | peak mem MB | S3 GET | S3 HEAD | MB read |",
             "|---|---|---|---|---|---|---|---|---|---|"]
    try:
        for (sig, name), url in layouts.items():
            st = cc.TRACE_ST if sig == "traces" else cc.LOG_ST
            cols = (cc.TRACE_COLS if sig == "traces" else cc.LOG_COLS) + cc.ALL_ENV
            tbl = f"{db}.{sig}_{abs(hash(name)) % 10**8}"
            ch(f"CREATE TABLE {tbl} {cc.CENTRAL[sig]} SETTINGS non_replicated_deduplication_window = 1000")
            n_rows, _ = ch(f"SELECT count() FROM s3('{url(10)}', '{KEY}', '{SECRET}', 'Parquet', '{st}')")
            assert n_rows == "100000", (name, n_rows)
            for n in [int(x) for x in os.environ.get("NS", "1,10").split(",")]:
                for sname, s in [("default", default), ("single-thread", single)]:
                    xs = []
                    for _ in range(REPS):
                        ch(f"TRUNCATE TABLE {tbl}")
                        xs.append(measured(f"INSERT INTO {tbl} SELECT {cols} FROM s3('{url(n)}', '{KEY}', '{SECRET}', 'Parquet', '{st}')", s))
                    lines.append(f"| {sig} | {name} | {n} | {sname} | {fmt(xs,'wall')} | {fmt(xs,'cpu')} | {fmt(xs,'mem')} | "
                                 f"{fmt(xs,'gets')} | {fmt(xs,'heads')} | {sum(x['mb'] for x in xs)/len(xs):.2f} |")
                    print(lines[-1], flush=True)
    finally:
        ch(f"DROP DATABASE IF EXISTS {db}")
    open(out, "w").write(f"run {run}, {REPS} reps; median [min–max]\n\n" + "\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
