"""Small ClickHouse helpers shared by the entities scripts (HTTP interface, capped)."""
import json, os, re, subprocess, time, urllib.error, urllib.parse, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
SQL = os.path.join(HERE, "..", "sql")
REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
CH = os.environ.get("CH", "http://127.0.0.1:18123")
CHC = os.environ.get("CH_CLIENT", "clickhouse")  # set CH_CLIENT to the clickhouse binary
CHC_PORT = os.environ.get("CH_PORT", "19000")
CAP = dict(max_memory_usage=os.environ.get("MAX_MEMORY", "3000000000"), max_threads=os.environ.get("MAX_THREADS", "2"))


def q(sql, fmt=None, timeout=3600, **settings):
    params = dict(CAP)
    params.update({k: str(v) for k, v in settings.items()})
    if fmt:
        sql = sql.rstrip().rstrip(";") + f" FORMAT {fmt}"
    req = urllib.request.Request(CH + "/?" + urllib.parse.urlencode(params), data=sql.encode(), method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read().decode()
            summary = json.loads(r.headers.get("X-ClickHouse-Summary") or "{}")
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"{e.code}: {e.read().decode()[:1500]}\n--- {sql[:600]}") from None
    q.last_summary = summary
    return body.strip()


def rows(sql, **settings):
    out = q(sql, fmt="JSONEachRow", **settings)
    return [json.loads(l) for l in out.splitlines() if l]


def one(sql, **settings):
    return q(sql, fmt="TSV", **settings)


def statements(path, **subs):
    body = "\n".join(l for l in open(path).read().splitlines() if not l.lstrip().startswith("--"))
    for k, v in subs.items():
        body = body.replace("{" + k + "}", v)
    return [x.strip().rstrip(";").strip() for x in body.split(";\n") if x.strip()]


def head_sql(name):
    """A file of ../../otap-rs/sql as committed at HEAD (other agents edit the working tree)."""
    return subprocess.check_output(["git", "-C", REPO, "show", f"HEAD:otel-chdb/otap-rs/sql/{name}"], text=True)


def head_commit():
    return subprocess.check_output(["git", "-C", REPO, "rev-parse", "--short", "HEAD"], text=True).strip()


def client_events(sql, **settings):
    """Run one statement with clickhouse client; return its own ProfileEvents totals."""
    args = [CHC, "client", "--port", CHC_PORT, "--print-profile-events", "--profile-events-delay-ms=-1", "--query", sql]
    s = dict(CAP)
    s.update(settings)
    args += [f"--{k}={v}" for k, v in s.items()]
    r = subprocess.run(args, capture_output=True, text=True, timeout=3600)
    if r.returncode != 0:
        raise RuntimeError(r.stderr[-1500:])
    ev = {}
    for m in re.finditer(r"\[ 0 \] (\w+): (\d+) \(increment\)", r.stderr):
        ev[m.group(1)] = ev.get(m.group(1), 0) + int(m.group(2))
    return ev


def load():
    try:
        return [float(x) for x in open("/proc/loadavg").read().split()[:3]]
    except OSError:
        return []


def mem_available_gb():
    for l in open("/proc/meminfo"):
        if l.startswith("MemAvailable"):
            return int(l.split()[1]) / 1e6
    return 0.0


def disk_free_gb():
    st = os.statvfs("/")
    return st.f_bavail * st.f_frsize / 1e9
