"""Shared helpers for the central-ingest benchmark: a ClickHouse HTTP client,
server-wide counter snapshots (system.events + /proc CPU), manifests, and the
reader-table attach via chdbattach -print-ddl."""
import json, os, subprocess, tempfile, time, shutil, statistics
import requests

CH = os.environ.get("CH_URL", "http://127.0.0.1:18123")
S3 = os.environ.get("S3_ROOT", "http://127.0.0.1:18333/otel")
KEY = os.environ.get("S3_KEY", "otel")
SECRET = os.environ.get("S3_SECRET", "otelsecret")
CHDBATTACH = os.environ.get("CHDBATTACH")
CHDB_LIB = os.environ.get("CHDB_LIB_PATH")
SCRATCH = os.environ.get("SCRATCH", tempfile.gettempdir())
RESULTS = os.environ.get("RESULTS", "results")

EVENTS = ["S3GetObject", "S3ListObjects", "S3HeadObject", "ReadBufferFromS3Bytes",
          "S3ReadRequestsCount", "UserTimeMicroseconds", "SystemTimeMicroseconds",
          "SelectedParts", "SelectedMarks", "SelectedRows", "SelectedBytes",
          "InsertedRows", "InsertedBytes", "DuplicatedInsertedBlocks",
          "SchemaInferenceCacheHits", "SchemaInferenceCacheMisses"]


def ch(sql, settings=None, fmt=None, check=True, timeout=600):
    params = dict(settings or {})
    t0 = time.perf_counter()
    r = requests.post(CH + "/", params=params, data=sql.encode(), timeout=timeout)
    wall = time.perf_counter() - t0
    summary = json.loads(r.headers.get("X-ClickHouse-Summary", "{}") or "{}")
    if check and r.status_code != 200:
        raise RuntimeError(f"ClickHouse {r.status_code}: {r.text[:500]}\n{sql[:400]}")
    return r.text, wall, summary, r.status_code


def q(sql, **kw):
    return ch(sql, **kw)[0].strip()


def server_pid():
    return int(subprocess.check_output(["pgrep", "-x", "clickhouse"]).split()[0])


def proc_cpu(pid):
    f = open(f"/proc/{pid}/stat").read()
    f = f[f.rindex(")") + 2:].split()
    return (int(f[11]) + int(f[12])) / os.sysconf("SC_CLK_TCK")


def snap(pid):
    rows = q("SELECT event, value FROM system.events WHERE event IN (%s) FORMAT TSV" %
             ",".join("'%s'" % e for e in EVENTS))
    d = {e: 0 for e in EVENTS}
    for line in rows.splitlines():
        k, v = line.split("\t")
        d[k] = int(v)
    d["proc_cpu_s"] = proc_cpu(pid)
    d["t"] = time.perf_counter()
    return d


def delta(a, b):
    return {k: b[k] - a[k] for k in a}


def run_measured(pid, sql, settings=None):
    """Run one statement; return wall time, server-wide deltas and the
    per-query X-ClickHouse-Summary."""
    a = snap(pid)
    _, wall, summary, _ = ch(sql, settings)
    b = snap(pid)
    d = delta(a, b)
    d["wall_s"] = wall
    d["summary"] = summary
    return d


def med(xs):
    return statistics.median(xs)


def attach(db, manifest_url, refresh=0, tmp=None):
    """Attach a published generation read-only on the server, via the
    exporter's own ReaderDDL (chdbattach -print-ddl). Returns the main table."""
    path = tempfile.mkdtemp(prefix="att-", dir=SCRATCH)
    try:
        env = dict(os.environ, CHDB_LIB_PATH=CHDB_LIB)
        out = subprocess.check_output([CHDBATTACH, "-print-ddl", "-path", path, "-key", KEY, "-secret", SECRET,
                                       "-db", db, "-refresh", str(refresh), "-manifest", manifest_url], env=env, text=True)
    finally:
        shutil.rmtree(path, ignore_errors=True)
    stmts = [s for s in out.splitlines() if s.strip()]
    return stmts


def manifests(ns_glob, gen="*"):
    """All batch manifests under a namespace glob (producer/epoch)."""
    rows = q(f"SELECT json FROM s3('{S3}/{ns_glob}/manifests/{gen}/0*.json', '{KEY}', '{SECRET}', 'JSONAsString') FORMAT JSONEachRow")
    ms = [json.loads(json.loads(l)["json"]) for l in rows.splitlines()]
    return sorted(ms, key=lambda m: (m["producer_id"], m["batch_id"]))
