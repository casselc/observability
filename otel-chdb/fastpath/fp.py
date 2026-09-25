"""Shared helpers for the fast-path ClickHouse experiments (see ../model/FASTPATH.md).

Everything lives in database `fastpath_test` on the shared server and under
s3://otel/fastpath/ on the shared SeaweedFS. Nothing here restarts the server
or changes its config; every setting is per query.
"""
import json, os, sys, time, uuid
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "bench", "central"))

CH = os.environ.get("CH_URL", "http://127.0.0.1:18123")
S3 = os.environ.get("S3_ROOT", "http://127.0.0.1:18333/otel")
KEY, SECRET = "otel", "otelsecret"
DB = "fastpath_test"
PREFIX = f"{S3}/fastpath"
# An existing 8,000-span batch written by the bench generator (read only).
SEED = f"{S3}/parquet/ce/traces/v1/big/20260924T211216Z-96e046/g20260924T000000/00000000000000000001.parquet"

RESULTS = os.path.join(os.path.dirname(__file__), "results")
os.makedirs(RESULTS, exist_ok=True)


def ch(sql, settings=None, data=None, check=True, timeout=600):
    """POST a statement. With `data`, the statement goes in ?query= and the
    body is the insert payload (what an exporter would send)."""
    params = dict(settings or {})
    t0 = time.perf_counter()
    if data is None:
        r = requests.post(CH + "/", params=params, data=sql.encode(), timeout=timeout)
    else:
        params["query"] = sql
        r = requests.post(CH + "/", params=params, data=data, timeout=timeout)
    wall = time.perf_counter() - t0
    summ = json.loads(r.headers.get("X-ClickHouse-Summary", "{}") or "{}")
    if check and r.status_code != 200:
        raise RuntimeError(f"ClickHouse {r.status_code}: {r.text[:400]}\n{sql[:300]}")
    return r.text, wall, summ, r.status_code


def q(sql, **kw):
    return ch(sql, **kw)[0].strip()


def count(t, where="1"):
    return int(q(f"SELECT count() FROM {DB}.{t} WHERE {where}"))


def flush():
    q("SYSTEM FLUSH ASYNC INSERT QUEUE")


def token():
    return "tok-" + uuid.uuid4().hex[:12]


class Log:
    def __init__(self, name):
        self.f = open(os.path.join(RESULTS, name), "a")

    def __call__(self, **kw):
        kw["ts"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        self.f.write(json.dumps(kw) + "\n")
        self.f.flush()
        print(json.dumps(kw), flush=True)


# The exact single-block settings for INSERT ... SELECT FROM s3(<one key>):
# one reading thread, one insert thread, and squashing that never splits or
# merges an 8k-row batch.
ONE_BLOCK = {"max_threads": 1, "max_insert_threads": 1, "max_block_size": 1048576,
             "max_insert_block_size": 1048576, "min_insert_block_size_rows": 0,
             "min_insert_block_size_bytes": 0,
             # the Parquet reader's own chunking (65,409 rows or ~16 MB by default)
             "input_format_parquet_max_block_size": 1048576,
             "input_format_parquet_prefer_block_bytes": 1 << 34}
