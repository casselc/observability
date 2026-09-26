#!/usr/bin/env python3
"""Background-merge cost of the central tables under a steady, consumer-shaped
ingest.

Each table gets INSERT … SELECT FROM s3() statements at a fixed rate, shaped as
the consumer (otap-rs/src/consumer/sql.rs) sends them: the edge objects named
in one s3('…/{k1,k2,…}') call, the single-block settings with squashing on (one
part per statement), a dedup token, the fence WHERE, content_key. The objects
come from a replayed pool (see README), so each statement shifts time and sets
the envelope; traces and logs also get fresh random trace/span ids per row
(`--ids testgen` keeps the pool's).

While it runs it samples the tables' part layout and the server's merge-thread
CPU; afterwards it dumps this run's system.part_log and system.query_log rows
(the per-merge and per-insert CPU) into results/<run>/*.jsonl.gz and drops the database.
analyze.py turns those files into the numbers.

  merges.py --run t10k --tables traces --rows 10k --rate 4 --duration 600
"""
import argparse, datetime as dt, gzip, json, os, re, shutil, subprocess, sys, threading, time, uuid
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
SPIKE = os.path.dirname(os.path.dirname(HERE))
CH = os.environ.get("CH_URL", "http://127.0.0.1:18623")
S3 = os.environ.get("S3_ROOT", "http://127.0.0.1:18333/otel")
KEY, SECRET = "otel", "otelsecret"
POOL = "merges/pool"
BASE = dt.datetime(2026, 9, 20, 0, 0, 0, tzinfo=dt.timezone.utc)  # synthetic received_at origin
POOL_T0_NS = int(dt.datetime(2026, 9, 24, 12, 0, 0, tzinfo=dt.timezone.utc).timestamp()) * 10**9 + 123456789  # testgen.Epoch

S = requests.Session()


def ch(sql, params=None, timeout=900, check=True):
    r = S.post(CH + "/", params=params or {}, data=sql.encode(), timeout=timeout)
    if check and r.status_code != 200:
        raise RuntimeError(f"{r.status_code}: {r.text[:600]}\n{sql[:300]}")
    return r.text


# ---------------------------------------------------------------- tables

ENV_ST = ", producer_id String, producer_epoch String, batch_id UInt64, row_ordinal UInt32, received_at DateTime64(9), schema_version UInt16"
TRACES_ST = ("Timestamp DateTime64(9), TraceId String, SpanId String, ParentSpanId String, TraceState String, SpanName String, "
             "SpanKind String, ServiceName String, ResourceAttributes Map(String, String), ScopeName String, ScopeVersion String, "
             "SpanAttributes Map(String, String), Duration UInt64, StatusCode String, StatusMessage String, "
             "`Events.Timestamp` Array(DateTime64(9)), `Events.Name` Array(String), `Events.Attributes` Array(Map(String, String)), "
             "`Links.TraceId` Array(String), `Links.SpanId` Array(String), `Links.TraceState` Array(String), "
             "`Links.Attributes` Array(Map(String, String))" + ENV_ST)
LOGS_ST = ("Timestamp DateTime64(9), TraceId String, SpanId String, TraceFlags UInt8, SeverityText String, SeverityNumber UInt8, "
           "ServiceName String, Body String, ResourceSchemaUrl String, ResourceAttributes Map(String, String), ScopeSchemaUrl String, "
           "ScopeName String, ScopeVersion String, ScopeAttributes Map(String, String), LogAttributes Map(String, String), EventName String"
           + ENV_ST)
# metrics-layout/central BStructure (the Go edge's layout-B objects)
B_HEAD = "MetricName String, ServiceName String, series_id UInt64, StartTimeUnix DateTime, TimeUnix DateTime"
B_EX = "`Exemplars.TimeUnix` Array(DateTime), `Exemplars.Value` Array(Float64), `Exemplars.SpanId` Array(String), `Exemplars.TraceId` Array(String)"
B_ST = {
    "number": B_HEAD + ", Value Float64, Flags UInt32, " + B_EX + ENV_ST,
    "histogram": B_HEAD + ", Count UInt64, Sum Float64, BucketCounts Array(UInt64), Min Float64, Max Float64, Flags UInt32, " + B_EX + ENV_ST,
    "exponential_histogram": B_HEAD + ", Count UInt64, Sum Float64, Scale Int32, ZeroCount UInt64, PositiveOffset Int32, "
    "PositiveBucketCounts Array(UInt64), NegativeOffset Int32, NegativeBucketCounts Array(UInt64), Min Float64, Max Float64, Flags UInt32, "
    + B_EX + ENV_ST,
    "summary": B_HEAD + ", Count UInt64, Sum Float64, `ValueAtQuantiles.Quantile` Array(Float64), `ValueAtQuantiles.Value` Array(Float64), Flags UInt32"
    + ENV_ST,
    "series": "series_id UInt64, MetricType UInt8, MetricName String, MetricDescription String, MetricUnit String, ServiceName String, "
    "ResourceAttributesKeys Array(String), ResourceAttributesValues Array(String), ResourceSchemaUrl String, ScopeName String, ScopeVersion String, "
    "ScopeAttributesKeys Array(String), ScopeAttributesValues Array(String), ScopeDroppedAttrCount UInt32, ScopeSchemaUrl String, "
    "AttributesKeys Array(String), AttributesValues Array(String), AggregationTemporality Int32, IsMonotonic Bool, ExplicitBounds Array(Float64), FirstSeen DateTime"
    + ENV_ST,
}
# metrics-layout/central AStructure (parquetgo's contrib-schema objects), by type
A_COMMON = ("ResourceAttributes Map(String, String), ResourceSchemaUrl String, ScopeName String, ScopeVersion String, ScopeAttributes Map(String, String), "
            "ScopeDroppedAttrCount UInt32, ScopeSchemaUrl String, ServiceName String, MetricName String, MetricDescription String, MetricUnit String, "
            "Attributes Map(String, String), StartTimeUnix DateTime, TimeUnix DateTime")
A_EX = ("`Exemplars.FilteredAttributes` Array(Map(String, String)), `Exemplars.TimeUnix` Array(DateTime), `Exemplars.Value` Array(Float64), "
        "`Exemplars.SpanId` Array(String), `Exemplars.TraceId` Array(String)")
A_ST = {
    "sum": A_COMMON + ", Value Float64, Flags UInt32, " + A_EX + ", AggregationTemporality Int32, IsMonotonic Bool" + ENV_ST,
    "histogram": A_COMMON + ", Count UInt64, Sum Float64, BucketCounts Array(UInt64), ExplicitBounds Array(Float64), " + A_EX
    + ", Flags UInt32, Min Float64, Max Float64, AggregationTemporality Int32" + ENV_ST,
}


def cols_of(structure):
    out, depth, start = [], 0, 0
    for i, c in enumerate(structure + ","):
        if c == "," and depth == 0:
            f = structure[start:i].strip()
            out.append(f[:f.index(" ")].strip("`"))
            start = i + 1
        elif c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
    return out


def series_tables_sql():
    return open(os.path.join(SPIKE, "otap-rs", "sql", "series_tables.sql")).read()


def b_ddl(table, counted):
    """otap-rs central::series_layout_create_table: the table from
    sql/series_tables.sql, plus content_key and the by_content projection on
    a points table."""
    src = series_tables_sql()
    head = f"CREATE TABLE {{db}}.{table}\n"
    body = src[src.index(head) + len(head):]
    body = body[:body.index(";")]
    if counted:
        i = body.index("\n)\nENGINE")
        body = body[:i] + (",\n    content_key LowCardinality(String),\n"
                           "    PROJECTION by_content (SELECT content_key, count() GROUP BY content_key)") + body[i:]
    return f"CREATE TABLE {{db}}.{table}\n{body}"


def a_ddl(t):
    """metrics-layout central.ADDL (contrib DDL + envelope, PARTITION BY
    toDate(received_at), dedup window), plus content_key and the projection."""
    s = open(os.path.join(SPIKE, "metrics-layout", "sql", "contrib", f"otel_metrics_{t}.sql")).read()
    s = s.replace("    INDEX idx_res_attr_key", "    producer_id LowCardinality(String), producer_epoch LowCardinality(String), batch_id UInt64, "
                  "row_ordinal UInt32, received_at DateTime64(9), schema_version UInt16, content_key LowCardinality(String),\n"
                  "    PROJECTION by_content (SELECT content_key, count() GROUP BY content_key),\n    INDEX idx_res_attr_key", 1)
    s = s.replace("PARTITION BY toDate(TimeUnix)", "PARTITION BY toDate(received_at)", 1)
    s = s.replace("SETTINGS index_granularity = 8192", "SETTINGS non_replicated_deduplication_window = 1000, index_granularity = 8192", 1)
    s = s.replace(f"CREATE TABLE IF NOT EXISTS {{db}}.otel_metrics_{t}", f"CREATE TABLE {{db}}.otel_metrics_{t}")
    return s.strip().rstrip(";")


def ddl(name, a):
    part = a.partition
    if name in ("traces", "logs"):
        s = open(os.path.join(HERE, "sql", f"otel_{name}.sql")).read()
        s = s[s.index("CREATE TABLE"):].strip().rstrip(";")
        s = s.replace("{partition}", part).replace("{ttl}", a.ttl or "")
    elif name.startswith("A_"):
        s = a_ddl(name[2:])
        if part != "toDate(received_at)":
            s = s.replace("PARTITION BY toDate(received_at)", f"PARTITION BY {part}")
        if a.ttl:
            s = s.replace("\nSETTINGS", f"\n{a.ttl}\nSETTINGS", 1)
    else:
        table = {"number": "otel_metrics_number_points", "histogram": "otel_metrics_histogram_points",
                 "exponential_histogram": "otel_metrics_exponential_histogram_points", "summary": "otel_metrics_summary_points",
                 "series": "otel_metrics_series"}[name]
        s = b_ddl(table, name != "series").strip()
        if part != "toDate(received_at)" and name != "series":
            s = s.replace("PARTITION BY toDate(received_at)", f"PARTITION BY {part}")
        if a.ttl and name != "series":
            s = s.replace("\nSETTINGS", f"\n{a.ttl}\nSETTINGS", 1)
    extra = ", old_parts_lifetime = 30"  # disk only: merged-away parts go after 30 s, not 8 min
    for kv in a.setting:
        extra += ", " + kv
    if "{settings}" in s:
        s = s.replace("{settings}", extra)
    else:
        s = re.sub(r"(SETTINGS [^\n]*)$", lambda m: m.group(1) + extra, s)
    return s.replace("{db}", a.db)


TABLE_OF = {"traces": "otel_traces", "logs": "otel_logs", "number": "otel_metrics_number_points",
            "histogram": "otel_metrics_histogram_points", "exponential_histogram": "otel_metrics_exponential_histogram_points",
            "summary": "otel_metrics_summary_points", "series": "otel_metrics_series",
            "A_sum": "otel_metrics_sum", "A_histogram": "otel_metrics_histogram"}

# ---------------------------------------------------------------- statements


def url(keys):
    if len(keys) == 1:
        return f"{S3}/{keys[0]}"
    common = os.path.commonpath(keys)
    return f"{S3}/{common}/{{" + ",".join(k[len(common) + 1:] for k in keys) + "}"


def s3src(keys, st):
    return f"s3('{url(keys)}', '{KEY}', '{SECRET}', 'Parquet', '{st}')"


def lit_dt64(ns):
    s, n = divmod(ns, 10**9)
    return f"toDateTime64('{dt.datetime.fromtimestamp(s, dt.timezone.utc):%Y-%m-%d %H:%M:%S}.{n:09d}', 9, 'UTC')"


def envelope(seq, recv_ns, run):
    return (f"concat('edge-', toString(cityHash64(_path) % 1000)), 'e1', {seq} * 1000 + cityHash64(_path) % 1000, row_ordinal, "
            f"{lit_dt64(recv_ns)}, schema_version, concat('{run}-{seq}-', _file)")


FENCE = "WHERE now64(3) <= fromUnixTimestamp64Milli(toInt64(4102444800000))"  # the consumer's fence, far in the future


def stmt(name, seq, size, recv_ns, a):
    """(sql, rows) for statement number seq of table `name` at `size`."""
    db, big = a.db, size == "100k"
    if name in ("traces", "logs"):
        n = 10 if big else 1
        keys = [f"{POOL}/{name}/{(seq * n + i) % 10:02d}.parquet" for i in range(n)]
        shift = recv_ns - POOL_T0_NS - 10 * 10**9
        iv = f"toIntervalNanosecond({shift})"
        rnd = a.ids == "random"
        if name == "traces":
            st = TRACES_ST
            sel = [f"Timestamp + {iv}",
                   "lower(hex(randomString(16)))" if rnd else "TraceId",
                   "lower(hex(randomString(8)))" if rnd else "SpanId",
                   "if(ParentSpanId = '', '', lower(hex(randomString(8))))" if rnd else "ParentSpanId",
                   "TraceState", "SpanName", "SpanKind", "ServiceName", "ResourceAttributes", "ScopeName", "ScopeVersion",
                   "SpanAttributes", "Duration", "StatusCode", "StatusMessage", f"arrayMap(t -> t + {iv}, `Events.Timestamp`)",
                   "`Events.Name`", "`Events.Attributes`", "`Links.TraceId`", "`Links.SpanId`", "`Links.TraceState`", "`Links.Attributes`"]
        else:
            st = LOGS_ST
            sel = [f"Timestamp + {iv}",
                   "if(TraceId = '', '', lower(hex(randomString(16))))" if rnd else "TraceId",
                   "if(SpanId = '', '', lower(hex(randomString(8))))" if rnd else "SpanId",
                   "TraceFlags", "SeverityText", "SeverityNumber", "ServiceName", "Body", "ResourceSchemaUrl", "ResourceAttributes",
                   "ScopeSchemaUrl", "ScopeName", "ScopeVersion", "ScopeAttributes", "LogAttributes", "EventName"]
        cols = [c for c in cols_of(st)] + ["content_key"]
        sql = (f"INSERT INTO {db}.{TABLE_OF[name]} ({', '.join('`%s`' % c for c in cols)}) SELECT {', '.join(sel)}, "
               f"{envelope(seq, recv_ns, a.run)} FROM {s3src(keys, st)} {FENCE}")
        return sql, 10000 * n
    if name.startswith("A_"):
        t = name[2:]
        # big pool only: one round (all 200 pods) per object; sum 52k, histogram 16k rows per object
        n = 1 if t == "sum" else (6 if big else 1)
        if t == "sum" and not big:
            raise SystemExit("A_sum is 52k rows per object; run it at --rows 100k (one object)")
        idx = [(seq * n + i) for i in range(n)]
        keys = [f"{POOL}/big/A/{t}/{i % 24:04d}-00.parquet" for i in idx]
        cyc = (seq * n) // 24
        st = A_ST[t]
        cs = cols_of(st)
        sel = [f"TimeUnix + {cyc * 720}" if c == "TimeUnix" else f"`{c}`" for c in cs[:-6]]
        sql = (f"INSERT INTO {db}.{TABLE_OF[name]} ({', '.join('`%s`' % c for c in cs)}, content_key) SELECT {', '.join(sel)}, "
               f"{envelope(seq, recv_ns, a.run)} FROM {s3src(keys, st)} {FENCE}")
        return sql, (52000 if t == "sum" else 16000) * n
    # layout B
    if name == "number":
        if big:  # one whole-fleet batch: gauge 28k + sum 52k
            k, cyc = seq % 24, seq // 24
            keys = [f"{POOL}/big/B/{t}/{k:04d}-00.parquet" for t in ("gauge", "sum")]
            rows = 80000
        else:  # one 25-pod batch: gauge 3.5k + sum 6.5k
            k, cyc = seq % 192, seq // 192
            keys = [f"{POOL}/small/B/{t}/{k // 8:04d}-{k % 8:02d}.parquet" for t in ("gauge", "sum")]
            rows = 10000
    elif name == "histogram":
        if big:
            n, pool, per = 6, 24, 16000
            keys = [f"{POOL}/big/B/histogram/{((seq * n) % pool + i) % pool:04d}-00.parquet" for i in range(n)]
        else:
            n, pool, per = 5, 190, 2000
            b0 = (seq * n) % pool
            keys = [f"{POOL}/small/B/histogram/{(b0 + i) // 8:04d}-{(b0 + i) % 8:02d}.parquet" for i in range(n)]
        cyc, rows = (seq * n) // pool, n * per
    elif name in ("exponential_histogram", "summary"):
        n = 50 if big else 5
        b0 = (seq * n) % 50
        keys = [f"{POOL}/big50/B/{name}/{(b0 + i) % 50:04d}-00.parquet" for i in range(n)]
        cyc, rows = (seq * n) // 50, n * 2000
    elif name == "series":
        if big:
            keys, cyc, rows = [f"{POOL}/big/B/series/0000-00.parquet"], seq, 100000
        else:
            keys, cyc, rows = [f"{POOL}/small/B/series/0000-{seq % 8:02d}.parquet"], seq // 8, 12500
        st = B_ST["series"]
        sql = (f"INSERT INTO {db}.otel_metrics_series (series_id, MetricType, MetricName, MetricDescription, MetricUnit, ServiceName, "
               "ResourceAttributes, ResourceSchemaUrl, ScopeName, ScopeVersion, ScopeAttributes, ScopeDroppedAttrCount, ScopeSchemaUrl, Attributes, "
               "AggregationTemporality, IsMonotonic, ExplicitBounds, FirstSeen, LastSeen) "
               "SELECT series_id, MetricType, MetricName, MetricDescription, MetricUnit, ServiceName, "
               "mapFromArrays(ResourceAttributesKeys, ResourceAttributesValues), ResourceSchemaUrl, ScopeName, ScopeVersion, "
               "mapFromArrays(ScopeAttributesKeys, ScopeAttributesValues), ScopeDroppedAttrCount, ScopeSchemaUrl, "
               "mapFromArrays(AttributesKeys, AttributesValues), AggregationTemporality, IsMonotonic, ExplicitBounds, "
               f"FirstSeen + {cyc * 720}, FirstSeen + {cyc * 720} FROM {s3src(keys, st)}")
        return sql, rows
    st = B_ST["number" if name == "number" else name]
    cs = cols_of(st)
    content = cs[:-6]
    sel = [f"TimeUnix + {cyc * 720}" if c == "TimeUnix" else f"`{c}`" for c in content]
    tcols = list(content)
    if name == "number":
        tcols.insert(3, "MetricType")
        sel.insert(3, "if(position(_path, '/gauge/') > 0, 'gauge', 'sum')")
    tcols += cs[-6:] + ["content_key"]
    sql = (f"INSERT INTO {db}.{TABLE_OF[name]} ({', '.join('`%s`' % c for c in tcols)}) SELECT {', '.join(sel)}, "
           f"{envelope(seq, recv_ns, a.run)} FROM {s3src(keys, st)} {FENCE}")
    return sql, rows


# the consumer's settings: ONE_BLOCK with squashing on, the dedup token
SETTINGS = {"max_threads": "1", "max_insert_threads": "1", "max_block_size": "1048576", "max_insert_block_size": "1048576",
            "min_insert_block_size_rows": "1048576", "min_insert_block_size_bytes": "4294967296",
            "input_format_parquet_max_block_size": "1048576", "input_format_parquet_prefer_block_bytes": "4294967296",
            "insert_deduplicate": "1", "deduplicate_insert": "enable", "deduplicate_insert_select": "force_enable"}

# ---------------------------------------------------------------- sampling


def ch_pid():
    for l in open(os.path.join(os.environ.get("MERGESRV", "/tmp/claude-0/-home-user/db86342c-d57d-54b7-95b1-90f220828b73/scratchpad/mergesrv"),
                               "data", "status")):
        if l.startswith("PID"):
            return int(l.split()[1])


def thread_cpu(pid):
    """CPU seconds of the server's threads, by thread name (MergeMutate, Move, …)."""
    out, tck = {}, os.sysconf("SC_CLK_TCK")
    for tid in os.listdir(f"/proc/{pid}/task"):
        try:
            st = open(f"/proc/{pid}/task/{tid}/stat").read()
        except OSError:
            continue
        name = st[st.index("(") + 1:st.rindex(")")]
        f = st[st.rindex(")") + 2:].split()
        out[name] = out.get(name, 0) + (int(f[11]) + int(f[12])) / tck
    return out


def free_gb(path="/"):
    s = os.statvfs(path)
    return s.f_bavail * s.f_frsize / 1e9


def sample(a, pid, t0):
    parts = ch(f"""SELECT table, count(), sum(rows), sum(bytes_on_disk), max(rows),
        toString(groupArray(level)), countIf(part_type = 'Compact') FROM system.parts
        WHERE database = '{a.db}' AND active GROUP BY table FORMAT JSONCompact""")
    merges = ch(f"SELECT table, count(), sum(total_size_bytes_compressed) FROM system.merges WHERE database = '{a.db}' GROUP BY table FORMAT JSONCompact")
    levels = {}
    for r in json.loads(parts)["data"]:
        lv = json.loads(r[5])
        levels[r[0]] = {"parts": r[1], "rows": int(r[2]), "bytes": int(r[3]), "max_rows": int(r[4]), "compact": r[6],
                        "levels": {str(l): lv.count(l) for l in sorted(set(lv))}}
    return {"t": round(time.time() - t0, 2), "wall": time.time(), "tables": levels,
            "merging": {r[0]: [r[1], int(r[2])] for r in json.loads(merges)["data"]},
            "threads": thread_cpu(pid), "free_gb": round(free_gb(), 3), "loadavg": os.getloadavg()[0]}


# ---------------------------------------------------------------- run


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--run", required=True)
    p.add_argument("--tables", default="traces")
    p.add_argument("--rows", default="10k", choices=["10k", "100k"])
    p.add_argument("--rate", action="append", default=[], help="statements per second per table (default 4); name=rate overrides, repeatable")
    p.add_argument("--duration", type=float, default=300)
    p.add_argument("--drain", type=float, default=60, help="seconds to keep sampling after the last insert")
    p.add_argument("--workers", type=int, default=3, help="concurrent statements per table (consumer workers)")
    p.add_argument("--ids", default="random", choices=["random", "testgen"])
    p.add_argument("--partition", default="toDate(received_at)")
    p.add_argument("--ttl", default="", help="TTL clause for the tables")
    p.add_argument("--setting", action="append", default=[], help="extra MergeTree setting, k = v")
    p.add_argument("--time-scale", type=float, default=1.0, help="synthetic received_at seconds per wall second")
    p.add_argument("--real-time", action="store_true", help="received_at = the wall clock (TTL is evaluated against it)")
    p.add_argument("--max-db-gb", type=float, default=1.3)
    p.add_argument("--min-free-gb", type=float, default=2.6)
    p.add_argument("--keep", action="store_true", help="don't drop the database")
    p.add_argument("--after", default="", help="SQL to run after the drain (e.g. a TTL move), sampled for --after-wait s")
    p.add_argument("--after-wait", type=float, default=0)
    a = p.parse_args()
    a.db = "mrg_" + re.sub(r"\W", "_", a.run)
    out = os.path.join(HERE, "results", a.run)
    os.makedirs(out, exist_ok=True)
    names = a.tables.split(",")
    rates = {}
    for tok in ",".join(a.rate or ["4"]).split(","):
        if "=" in tok:
            k, v = tok.split("=")
            rates[k] = float(v)
        else:
            rates["*"] = float(tok)
    ch(f"DROP DATABASE IF EXISTS {a.db} SYNC")
    ch(f"CREATE DATABASE {a.db}")
    ddls = []
    for n in names:
        d = ddl(n, a)
        ddls.append(d)
        ch(d)
    open(os.path.join(out, "ddl.sql"), "w").write(";\n\n".join(ddls) + ";\n")
    pid = ch_pid()
    json.dump(vars(a) | {"argv": sys.argv, "pid": pid, "start": time.time()}, open(os.path.join(out, "args.json"), "w"), indent=1)

    stop = threading.Event()
    inserts, errors = [], []
    lock = threading.Lock()
    t0 = time.time()
    wall0 = time.time()

    def table_loop(name):
        rate = rates.get(name, rates.get("*", 4.0))
        seq = [0]
        slock = threading.Lock()

        def worker():
            while not stop.is_set():
                with slock:
                    k = seq[0]
                    seq[0] += 1
                due = t0 + k / rate
                now = time.time()
                if due > now:
                    if stop.wait(due - now):
                        return
                if time.time() - t0 > a.duration:
                    return
                base = wall0 if a.real_time else BASE.timestamp()
                recv_ns = int((base + (time.time() - wall0) * a.time_scale) * 1e9)
                sql, rows = stmt(name, k, a.rows, recv_ns, a)
                qid = f"{a.run}-{name}-{k}-{uuid.uuid4().hex[:6]}"
                prm = dict(SETTINGS, query_id=qid, insert_deduplication_token=qid)
                ts = time.time()
                try:
                    ch(sql, prm)
                    ok = True
                except Exception as e:  # noqa
                    ok = False
                    with lock:
                        errors.append({"q": qid, "e": str(e)[:400]})
                with lock:
                    inserts.append({"table": name, "seq": k, "qid": qid, "rows": rows, "start": ts, "wall": time.time() - ts,
                                    "late": ts - due, "ok": ok})
        ths = [threading.Thread(target=worker, daemon=True) for _ in range(a.workers)]
        for t in ths:
            t.start()
        return ths

    samples = []
    base_thr = thread_cpu(pid)
    threads = [t for n in names for t in table_loop(n)]
    reason = "duration"
    while True:
        time.sleep(5)
        s = sample(a, pid, t0)
        samples.append(s)
        tot = sum(v["bytes"] for v in s["tables"].values())
        mm = s["threads"].get("MergeMutate", 0) - base_thr.get("MergeMutate", 0)
        with lock:
            ni, ne = len(inserts), len(errors)
        print(f"[{a.run}] t={s['t']:.0f}s inserts={ni} err={ne} parts={ {k: v['parts'] for k, v in s['tables'].items()} } "
              f"db={tot / 1e6:.0f}MB free={s['free_gb']:.2f}GB mergeCPU={mm:.1f}s", flush=True)
        if ne and ne > 20:
            reason = "errors"
            break
        if tot / 1e9 > a.max_db_gb:
            reason = "db size"
            break
        if s["free_gb"] < a.min_free_gb:
            reason = "free disk"
            break
        if time.time() - t0 > a.duration:
            break
    stop.set()
    for t in threads:
        t.join(timeout=120)
    t_ins_end = time.time()
    print(f"[{a.run}] inserts stopped ({reason}); draining {a.drain}s", flush=True)
    end = time.time() + a.drain
    while time.time() < end:
        time.sleep(5)
        s = sample(a, pid, t0)
        samples.append(s)
        if s["free_gb"] < a.min_free_gb - 0.3:
            print("free disk low during drain; stopping", flush=True)
            break
    t_after = time.time()
    if a.after:
        for st_ in a.after.split(";;"):
            print(f"[{a.run}] after: {st_[:120]}", flush=True)
            ch(st_.replace("{db}", a.db), timeout=3600)
        end = time.time() + a.after_wait
        while time.time() < end:
            time.sleep(5)
            samples.append(sample(a, pid, t0))
    final = ch(f"""SELECT table, count(), sum(rows), sum(bytes_on_disk), sum(data_uncompressed_bytes), toString(groupArray((level, rows)))
        FROM system.parts WHERE database = '{a.db}' AND active GROUP BY table FORMAT JSONCompact""")
    ch("SYSTEM FLUSH LOGS")
    tend = time.time()
    # raw per-merge and per-insert records for this run
    pl = ch(f"""SELECT event_type, merge_reason, merge_algorithm, event_time_microseconds, duration_ms, table, part_name, part_type, disk_name,
        rows, size_in_bytes, bytes_uncompressed, read_rows, read_bytes, length(merged_from) AS sources, peak_memory_usage,
        ProfileEvents['UserTimeMicroseconds'] AS user_us, ProfileEvents['SystemTimeMicroseconds'] AS sys_us,
        ProfileEvents['OSCPUVirtualTimeMicroseconds'] AS cpu_us, ProfileEvents['RealTimeMicroseconds'] AS real_us,
        ProfileEvents['MergeHorizontalStageExecuteMilliseconds'] AS h_ms, ProfileEvents['MergeVerticalStageExecuteMilliseconds'] AS v_ms,
        ProfileEvents['MergeProjectionStageExecuteMilliseconds'] AS proj_ms, ProfileEvents['MergedRows'] AS merged_rows,
        ProfileEvents['MergedUncompressedBytes'] AS merged_uncompressed, error
        FROM system.part_log WHERE database = '{a.db}' AND event_time >= toDateTime({int(t0) - 5}) ORDER BY event_time_microseconds FORMAT JSONEachRow""", timeout=600)
    gzip.open(os.path.join(out, "part_log.jsonl.gz"), "wt").write(pl)
    ql = ch(f"""SELECT query_id, type, event_time_microseconds, query_duration_ms, written_rows, written_bytes, read_rows, read_bytes,
        ProfileEvents['UserTimeMicroseconds'] AS user_us, ProfileEvents['SystemTimeMicroseconds'] AS sys_us,
        ProfileEvents['OSCPUVirtualTimeMicroseconds'] AS cpu_us, ProfileEvents['S3GetObject'] AS gets, ProfileEvents['S3HeadObject'] AS heads,
        ProfileEvents['InsertedRows'] AS inserted_rows, ProfileEvents['InsertedBytes'] AS inserted_bytes, memory_usage, exception_code
        FROM system.query_log WHERE query_id LIKE '{a.run}-%' AND type != 'QueryStart' AND event_time >= toDateTime({int(t0) - 5})
        ORDER BY event_time_microseconds FORMAT JSONEachRow""", timeout=600)
    gzip.open(os.path.join(out, "query_log.jsonl.gz"), "wt").write(ql)
    with gzip.open(os.path.join(out, "inserts.jsonl.gz"), "wt") as f:
        for r in inserts:
            f.write(json.dumps(r) + "\n")
    with gzip.open(os.path.join(out, "samples.jsonl.gz"), "wt") as f:
        for s in samples:
            f.write(json.dumps(s) + "\n")
    json.dump({"t0": t0, "t_ins_end": t_ins_end, "t_after": t_after, "t_end": tend, "reason": reason, "errors": errors[:20],
               "n_errors": len(errors), "final_parts": json.loads(final)["data"], "base_threads": base_thr},
              open(os.path.join(out, "run.json"), "w"), indent=1)
    if not a.keep:
        ch(f"DROP DATABASE {a.db} SYNC")
    print(f"[{a.run}] done: {len(inserts)} inserts, {len(errors)} errors, results in {out}", flush=True)


if __name__ == "__main__":
    main()
