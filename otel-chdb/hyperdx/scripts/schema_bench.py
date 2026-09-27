#!/usr/bin/env python3
"""The schema comparison's insert benchmark (../README.md §Schema): insert CPU per
row, stored bytes per row and merge cost of the consumer's traces/logs DDL
before the ClickStack 2.39.1 alignment (../sql/pre_alignment_traces_logs.sql)
and after (../../otap-rs/sql/otel_*.sql), same objects, in the consumer's own
statement shape (one object per statement, the ONE_BLOCK settings, a dedup
token), on a ClickHouse with query_log and part_log. Background merges are
stopped while inserting; then one OPTIMIZE FINAL (its part_log CPU is the
merge cost).

Variants: old; new (the consumer's DDL, option 2: ClickStack's without the
idx_*_attr_key indexes; table + rollup + view); new_main (its table only);
full / full_main (ClickStack's full DDL, ../sql/clickstack_full_*.sql);
new_stockpart (ClickStack's PARTITION BY toDate(Timestamp)); abl_* (full_main
without some text indexes or codecs); only_<index> (full_main with that one
text index).

  CH=http://127.0.0.1:18723 S3_PREFIX=http://127.0.0.1:18333/bucket/root CONSUME=.../consume \
    VARIANTS=old,new_main,new,new_stockpart OUT=results.jsonl schema_bench.py REPS
  schema_bench.py --summarize results.jsonl
"""
import json, os, re, statistics, subprocess, sys, time, uuid, collections
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
CH = os.environ.get("CH", "http://127.0.0.1:18723")
S3 = os.environ.get("S3_PREFIX", "http://127.0.0.1:18333/schema-otel/bench/edges/bench")
OUT = os.environ.get("OUT", "results.jsonl")
CONSUME = os.environ.get("CONSUME", "consume")
ONE_BLOCK = dict(max_threads=1, max_insert_threads=1, max_block_size=1048576, max_insert_block_size=1048576,
                 min_insert_block_size_rows=0, min_insert_block_size_bytes=0,
                 input_format_parquet_max_block_size=1048576, input_format_parquet_prefer_block_bytes=4294967296)


def sql_statements(path, **subs):
    body = "\n".join(l for l in open(path).read().splitlines() if not l.lstrip().startswith("--"))
    for k, v in subs.items():
        body = body.replace("{" + k + "}", v)
    return [x.strip().rstrip(";").strip() for x in body.split(";\n") if x.strip()]


def statements(table, sig):
    return sql_statements(os.path.join(HERE, "..", "..", "otap-rs", "sql", f"otel_{sig}.sql"), table=table)


OLD = {sig: next(x for x in sql_statements(os.path.join(HERE, "..", "sql", "pre_alignment_traces_logs.sql"))
                 if x.startswith(f"CREATE TABLE IF NOT EXISTS {{db}}.otel_{sig} ")).replace(f"{{db}}.otel_{sig}", "{table}", 1)
       for sig in ("traces", "logs")}


def full_statements(table, sig):
    """ClickStack 2.39.1's full DDL (with the idx_*_attr_key indexes), as the consumer had it before option 2."""
    return sql_statements(os.path.join(HERE, "..", "sql", f"clickstack_full_{sig}.sql"), table=table)


def ddl(variant, table, sig):
    if variant == "old":
        return [OLD[sig].replace("{table}", table)]
    if variant in ("full", "full_main") or variant.startswith(("only_", "abl_")):
        st = full_statements(table, sig)
    else:
        st = statements(table, sig)  # the consumer's: option 2
    if variant in ("new_main", "full_main"):  # the table only (what ensure() creates today): no rollup
        return st[:1]
    if variant.startswith("only_"):  # new_main with a single text index (by name)
        name = variant[5:]
        t = "\n".join(l for l in st[0].split("\n") if "TYPE text" not in l or f"INDEX {name} " in l)
        return [t]
    if variant.startswith("abl_"):  # new_main minus some indexes/codecs, for the cost of each
        drop = {"abl_noidx": r"TYPE text", "abl_notraceid": r"INDEX idx_trace_id ", "abl_noitems": r"INDEX idx_\w+_attr_items ",
                "abl_nokeys": r"INDEX idx_\w+_attr_key ", "abl_nobody": r"INDEX idx_(lower_body|lower_span_name|rum_session_id) ",
                "abl_noidx_nocodec": r"TYPE text"}[variant]
        t = "\n".join(l for l in st[0].split("\n") if not re.search(drop, l))
        if variant == "abl_noidx_nocodec":
            t = re.sub(r" CODEC\([^()]*(\([^()]*\)[^()]*)*\)", "", t)
        return [t]
    if variant == "new_stockpart":  # ClickStack's own partition key, for the deviation's cost
        return [st[0].replace("PARTITION BY toDate(received_at)", "PARTITION BY toDate(Timestamp)")] + st[1:]
    return st


CPU_SOURCE = os.environ.get("CPU_SOURCE", "query_log")  # or "client": a server without query_log / part_log
CHC = os.environ.get("CHC", "clickhouse")  # the client binary, for CPU_SOURCE=client
CHC_PORT = os.environ.get("CHC_PORT", "19000")
CAP = dict(max_memory_usage=int(os.environ.get("MAX_MEMORY", "2000000000")))


def client_events(sql, **settings):
    """Run one statement with clickhouse client and return its own ProfileEvents (the [ 0 ] totals)."""
    args = [CHC, "client", "--port", CHC_PORT, "--print-profile-events", "--profile-events-delay-ms=-1", "--query", sql]
    args += [f"--{k}={v}" for k, v in settings.items()]
    r = subprocess.run(args, capture_output=True, text=True, timeout=900)
    if r.returncode != 0:
        raise RuntimeError(r.stderr[-800:])
    ev = collections.Counter()
    for m in re.finditer(r"\[ 0 \] (\w+): (\d+) \(increment\)", r.stderr):
        ev[m.group(1)] += int(m.group(2))
    return ev


def q(sql, **settings):
    r = requests.post(CH + "/", data=sql.encode(), params=settings, timeout=900)
    if r.status_code != 200:
        raise RuntimeError(f"{r.status_code}: {r.text[:500]}\n{sql[:200]}")
    return r.text.strip()


def keys(sig):
    ls = requests.post(CH + "/", data=(f"SELECT DISTINCT _path FROM s3('{S3}/{sig}/**', 'otel', 'otelsecret', 'One') "
                                                        "ORDER BY _path FORMAT TSV").encode()).text.split()
    return [p for p in ls if p.endswith(".parquet")]


def load():
    try:
        return open("/proc/loadavg").read().split()[:3]
    except OSError:
        return []


def main():
    reps = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    variants = os.environ.get("VARIANTS", "old,new_main,new,new_stockpart").split(",")
    st = {s: subprocess.check_output([CONSUME, "--print-structure", s], text=True).strip() for s in ("traces", "logs")}
    cols = {s: subprocess.check_output([CONSUME, "--print-cols", s], text=True).strip() for s in ("traces", "logs")}
    objs = {s: keys(s) for s in ("traces", "logs")}
    for rep in range(reps):
        order = variants[rep % len(variants):] + variants[:rep % len(variants)]
        for v in order:
            db = f"schema_b_{v}_r{rep}"
            q(f"DROP DATABASE IF EXISTS {db} SYNC")
            q(f"CREATE DATABASE {db}")
            for sig in ("traces", "logs"):
                for s in ddl(v, f"{db}.otel_{sig}", sig):
                    q(s)
            for sig in ("traces", "logs"):
                q(f"SYSTEM STOP MERGES {db}.otel_{sig}")
            t0 = time.time()
            la0 = load()
            qids = []
            client_cpu = {}
            for sig in ("traces", "logs"):
                for i, k in enumerate(objs[sig]):
                    qid = f"schema-{v}-r{rep}-{sig}-{i}-{uuid.uuid4().hex[:8]}"
                    sql = (f"INSERT INTO {db}.otel_{sig} ({cols[sig]}, content_key) SELECT {cols[sig]}, 'ck{i:04d}' "
                           f"FROM s3('http://127.0.0.1:18333/{k}', 'otel', 'otelsecret', 'Parquet', '{st[sig]}') "
                           f"WHERE now64(3) <= fromUnixTimestamp64Milli(toInt64(4102444800000))")
                    st_ = dict(**ONE_BLOCK, **CAP, insert_deduplication_token=uuid.uuid4().hex, insert_deduplicate=1, deduplicate_insert="enable",
                               deduplicate_insert_select="force_enable", max_execution_time=300, timeout_overflow_mode="throw", query_id=qid)
                    if CPU_SOURCE == "client":
                        ev = client_events(sql, **st_)
                        a = client_cpu.setdefault(sig, dict(n=0, rows=0, cpu=0, us_sys=0, ms=0))
                        a["n"] += 1
                        a["cpu"] += ev["OSCPUVirtualTimeMicroseconds"]
                        a["us_sys"] += ev["UserTimeMicroseconds"] + ev["SystemTimeMicroseconds"]
                    else:
                        q(sql, **st_)
                    qids.append(qid)
            la1 = load()
            if CPU_SOURCE != "client":
                q("SYSTEM FLUSH LOGS")
            rows = "" if CPU_SOURCE == "client" else q(f"""SELECT query_id, written_rows, query_duration_ms, ProfileEvents['OSCPUVirtualTimeMicroseconds'] AS cpu,
                         ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds'] AS us_sys, memory_usage
                  FROM system.query_log WHERE type = 'QueryFinish' AND query_id LIKE 'schema-{v}-r{rep}-%' AND event_time >= toDateTime({int(t0) - 5})
                  FORMAT JSONEachRow""")
            stm = [json.loads(l) for l in rows.splitlines()]
            per = client_cpu
            for r in stm:
                sig = r["query_id"].split("-")[3]
                a = per.setdefault(sig, dict(n=0, rows=0, cpu=0, us_sys=0, ms=0))
                a["n"] += 1
                a["rows"] += int(r["written_rows"]) if False else 0
                a["cpu"] += int(r["cpu"]); a["us_sys"] += int(r["us_sys"]); a["ms"] += int(r["query_duration_ms"])
            for sig in ("traces", "logs"):
                per[sig]["rows"] = int(q(f"SELECT count() FROM {db}.otel_{sig}"))
            # stored bytes before and after merging to one part
            def size():
                out = {}
                for l in q(f"""SELECT table, sum(rows), count(), sum(bytes_on_disk), sum(data_compressed_bytes), sum(secondary_indices_compressed_bytes),
                                sum(data_uncompressed_bytes) FROM system.parts WHERE database = '{db}' AND active GROUP BY table FORMAT TSV""").splitlines():
                    t, *n = l.split("\t")
                    out[t] = dict(zip(["rows", "parts", "bytes_on_disk", "data_compressed", "idx_compressed", "data_uncompressed"], map(int, n)))
                return out
            before = size()
            t1 = time.time()
            opt = {}
            for sig in ("traces", "logs"):
                q(f"SYSTEM START MERGES {db}.otel_{sig}")
                if CPU_SOURCE == "client":  # OPTIMIZE ... FINAL merges in the statement's own thread
                    ev = client_events(f"OPTIMIZE TABLE {db}.otel_{sig} FINAL", **CAP, max_threads=2)
                    opt[f"otel_{sig}"] = dict(merges=1, rows=0, ms=0, cpu=ev["OSCPUVirtualTimeMicroseconds"],
                                              us_sys=ev["UserTimeMicroseconds"] + ev["SystemTimeMicroseconds"])
                else:
                    q(f"OPTIMIZE TABLE {db}.otel_{sig} FINAL", query_id=f"schemaopt-{v}-r{rep}-{sig}-{uuid.uuid4().hex[:6]}")
            if CPU_SOURCE != "client":
                q("SYSTEM FLUSH LOGS")
            merges = "" if CPU_SOURCE == "client" else q(f"""SELECT table, count(), sum(rows), sum(duration_ms), sum(ProfileEvents['OSCPUVirtualTimeMicroseconds']),
                            sum(ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds'])
                     FROM system.part_log WHERE database = '{db}' AND event_type = 'MergeParts' AND event_time >= toDateTime({int(t1) - 2})
                       AND table IN ('otel_traces', 'otel_logs') GROUP BY table FORMAT TSV""")
            mg = opt
            for l in merges.splitlines():
                t, n, r, ms, cpu, us = l.split("\t")
                mg[t] = dict(merges=int(n), rows=int(r), ms=int(ms), cpu=int(cpu), us_sys=int(us))
            after = size()
            cols_bytes = q(f"""SELECT table, column, sum(column_data_compressed_bytes) FROM system.parts_columns
                               WHERE database = '{db}' AND active AND table IN ('otel_traces', 'otel_logs') GROUP BY table, column FORMAT TSV""")
            cb = {}
            for l in cols_bytes.splitlines():
                t, c, b = l.split("\t")
                cb.setdefault(t, {})[c] = int(b)
            idx = {}
            for l in q(f"""SELECT table, name, data_compressed_bytes FROM system.data_skipping_indices
                            WHERE database = '{db}' AND table IN ('otel_traces', 'otel_logs') FORMAT TSV""").splitlines():
                t, n, b = l.split("\t")
                idx.setdefault(t, {})[n] = int(b)
            rec = dict(indexes=idx, variant=v, rep=rep, load_before=la0, load_after=la1, insert=per, before=before, after=after, merge=mg,
                       columns=cb, ts=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
            with open(OUT, "a") as f:
                f.write(json.dumps(rec) + "\n")
            ins = {s: round(per[s]["cpu"] / per[s]["rows"], 3) for s in per}
            byt = {t: round(after[t]["bytes_on_disk"] / after[t]["rows"], 2) for t in after if t in ("otel_traces", "otel_logs")}
            print(v, rep, "cpu us/row", ins, "B/row", byt, "load", la0, flush=True)
            if not os.environ.get("KEEP"):
                q(f"DROP DATABASE {db} SYNC")


def summarize(f):
    st = statistics
    R = [json.loads(l) for l in open(f)]
    by = collections.defaultdict(list)
    for r in R: by[r["variant"]].append(r)
    def med(xs): return st.median(xs) if xs else float('nan')
    def rng(xs): return f"[{min(xs):.2f}–{max(xs):.2f}]"
    print("variant reps | traces cpu us/row med [min-max] | logs cpu us/row | traces B/row (after OPTIMIZE) data+idx | logs B/row | merge cpu us/row traces / logs (20->1) | load")
    for v, rs in by.items():
        rs = [r for r in rs if r["rep"] > 0] or rs  # drop the warm-up rep when there are others
        tc = [r["insert"]["traces"]["cpu"] / r["insert"]["traces"]["rows"] for r in rs]
        lc = [r["insert"]["logs"]["cpu"] / r["insert"]["logs"]["rows"] for r in rs]
        a = rs[-1]["after"]
        tb = a["otel_traces"]["bytes_on_disk"] / a["otel_traces"]["rows"]; lb = a["otel_logs"]["bytes_on_disk"] / a["otel_logs"]["rows"]
        ti = a["otel_traces"]["idx_compressed"] / a["otel_traces"]["rows"]; li = a["otel_logs"]["idx_compressed"] / a["otel_logs"]["rows"]
        mt = [r["merge"]["otel_traces"]["cpu"] / r["insert"]["traces"]["rows"] for r in rs if "otel_traces" in r["merge"]]
        ml = [r["merge"]["otel_logs"]["cpu"] / r["insert"]["logs"]["rows"] for r in rs if "otel_logs" in r["merge"]]
        mn = [r["merge"].get("otel_traces", {}).get("merges") for r in rs]
        loads = [float(r["load_before"][0]) for r in rs]
        roll = {k: x["bytes_on_disk"] for k, x in a.items() if "rollup" in k}
        print(f"{v} {len(rs)} | {med(tc):.2f} {rng(tc)} | {med(lc):.2f} {rng(lc)} | {tb:.2f} (idx {ti:.2f}) | {lb:.2f} (idx {li:.2f}) | {med(mt):.2f} {rng(mt) if mt else ''} / {med(ml):.2f} {rng(ml) if ml else ''} merges {mn} | {min(loads):.2f}-{max(loads):.2f} | rollup bytes {roll}")
    if True:
        for v, rs in by.items():
            print(v, {t: {k: round(b / rs[-1]["after"][t]["rows"], 2) for k, b in d.items()} for t, d in rs[-1].get("indexes", {}).items()})


if __name__ == "__main__":
    summarize(sys.argv[2]) if sys.argv[1:2] == ["--summarize"] else main()
