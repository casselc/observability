#!/usr/bin/env python3
"""Central INSERT ... SELECT cost of the metrics objects, Rust against
parquetgo, as central_bench.py does for traces and logs: per metric type, 10
objects of the same 10,000-point batch per layout (encbench / pubbench),
inserted 1 or 10 at a time into the consumer's central table for that type
(`consume` creates it: the contrib exporter's columns and types, plus the
envelope and content_key, the by_content projection, partitioned by
toDate(received_at)); default settings and single-threaded; server-wide
system.events deltas around each statement; median [min-max] of REPS runs.
Server CPU per point = CPU ms / (objects x 10,000).

  metrics_central_bench.py B T DATA_DIR OUT.md
"""
import os, subprocess, sys, time
sys.path.insert(0, os.path.dirname(__file__))
from central_bench import ch, measured, fmt  # noqa: E402
from metrics_correctness import ST, cols_of  # noqa: E402

S3 = os.environ.get("S3_ROOT", "http://127.0.0.1:18333/otel")
KEY, SECRET = "otel", "otelsecret"
REPS = int(os.environ.get("REPS", "3"))
TYPES = ["gauge", "sum", "histogram", "exponential_histogram", "summary"]
ENV = "producer_id, producer_epoch, batch_id, row_ordinal, received_at, schema_version"


def main():
    b, t, data, out = sys.argv[1:5]
    run = f"mcb{int(time.time())}"
    base = f"metrics-rs/central/{run}"
    env = dict(os.environ, CHDB_TEST_S3=S3, CHDB_TEST_S3_KEY=KEY, CHDB_TEST_S3_SECRET=SECRET)
    db = f"otaprs_{run}"
    ch(f"CREATE DATABASE IF NOT EXISTS {db}")
    lines = ["| type | layout | objects | settings | wall ms | server CPU ms | CPU µs/point | peak mem MB | S3 GET | MB read |",
             "|---|---|---|---|---|---|---|---|---|---|"]
    single = {"max_threads": "1", "max_insert_threads": "1", "min_insert_block_size_rows": "0", "min_insert_block_size_bytes": "0",
              "max_insert_block_size": "10000000", "max_block_size": "10000000"}
    try:
        for typ in TYPES:
            sig = f"metrics_{typ}"
            f = f"{data}/metrics-{typ}-10000-b00.pb"
            subprocess.run([f"{b}/encbench", "--file", f, "--signal", "metrics", "--warmup", "1", "--batches", "9",
                            "--s3", f"{S3}/{base}/rust", "--key", KEY, "--secret", SECRET], check=True, stdout=subprocess.DEVNULL)
            subprocess.run([f"{t}/pubbench", "-impl", "parquet-go", "-url", f"{S3}/{base}/pg", "-signal", sig, "-n", "10000",
                            "-warmup", "1", "-batches", "9"], check=True, stdout=subprocess.DEVNULL, env=env)
            layouts = {
                "rust": lambda n: f"{S3}/{base}/rust/{sig}/*/" + ("{" + ",".join(f"{i:020d}" for i in range(n)) + "}" if n > 1 else f"{0:020d}") + ".parquet",
                "parquetgo": lambda n: f"{S3}/{base}/pg/cmp/{sig}/v1/*/*/*/" + ("{" + ",".join(f"{i:020d}" for i in range(1, n + 1)) + "}" if n > 1 else f"{1:020d}") + ".parquet",
            }
            tbl = f"{db}.{sig}"
            # the consumer creates its central table (and finds nothing to ingest under an empty prefix)
            subprocess.run([f"{b}/consume", "--s3", f"{S3}/{base}/none", "--signal", sig, "--table", tbl, "--once"],
                           check=True, stdout=subprocess.DEVNULL)
            cols = ", ".join(cols_of(sig))
            for name, url in layouts.items():
                n_rows, _ = ch(f"SELECT count() FROM s3('{url(10)}', '{KEY}', '{SECRET}', 'Parquet', '{ST[sig]}')")
                assert n_rows == "100000", (sig, name, n_rows)
                for n in [int(x) for x in os.environ.get("NS", "1,10").split(",")]:
                    for sname, s in [("default", {}), ("single-thread", single)]:
                        xs = []
                        for _ in range(REPS):
                            ch(f"TRUNCATE TABLE {tbl}")
                            xs.append(measured(f"INSERT INTO {tbl} ({cols}, {ENV}, content_key) SELECT {cols}, {ENV}, 'k' "
                                               f"FROM s3('{url(n)}', '{KEY}', '{SECRET}', 'Parquet', '{ST[sig]}')", s))
                        for x in xs:
                            x["us_pt"] = x["cpu"] * 1000 / (n * 10000)
                        lines.append(f"| {typ} | {name} | {n} | {sname} | {fmt(xs,'wall')} | {fmt(xs,'cpu')} | "
                                     f"{sorted(x['us_pt'] for x in xs)[len(xs)//2]:.2f} | {fmt(xs,'mem')} | {fmt(xs,'gets')} | "
                                     f"{sum(x['mb'] for x in xs)/len(xs):.2f} |")
                        print(lines[-1], flush=True)
    finally:
        ch(f"DROP DATABASE IF EXISTS {db}")
    open(out, "w").write(f"run {run}, {REPS} reps; median [min–max]; server-wide CPU (the server is shared)\n\n" + "\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
