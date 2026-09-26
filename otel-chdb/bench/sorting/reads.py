#!/usr/bin/env python3
"""Step 1 (and 2), reads: what a one-service, one-hour query reads from a set
of edge objects, per sort configuration.

  reads.py services           the query services (by traffic rank) -> services.json
  reads.py direct SETDIR...   the direct ranged-read path, modelled exactly from
                              each object's footer (pyarrow metadata), for each
                              set under otel/sorting/<SETDIR>/<config>/ -> direct.jsonl
  reads.py ch                 ClickHouse s3() over otel/sorting/set/<config>/, per
                              settings variant, bytes/GETs from query_log -> ch.jsonl

The direct path (a reader of our own, e.g. a compactor or a query service
that knows the layout): GET the last FOOTER_GUESS bytes (one suffix-range GET;
a second GET if the footer is longer), pick row groups, then GET the needed
column chunks of each picked group, ranges closer than COALESCE merged into
one GET (object_store's default is 1 MiB, which merges nearly everything in
objects this small; 0 shows the unmerged count). Picking row groups:
  whole    every row group (reading the whole object: 1 GET of the object)
  minmax   ServiceName min/max statistics of each row group
  bucket   the footer's oscope-sort bucket list (hash split: xxh3 of the
           service mod the bucket count, the exporter's own function)
Columns: 'all' (SELECT *) or the narrow projection of the query below.
"""
import json, os, subprocess, sys, urllib.request, urllib.parse
import pyarrow.parquet as pq
import io

HERE = os.path.dirname(os.path.abspath(__file__))
S = "/tmp/claude-0/-home-user/db86342c-d57d-54b7-95b1-90f220828b73/scratchpad"
FILER = "http://127.0.0.1:18888/buckets/otel/sorting"
CH = os.environ.get("CH", "http://127.0.0.1:18723")  # chpriv.sh (query_log)
NARROW = {"traces": ["Timestamp", "ServiceName", "SpanName", "Duration"], "logs": ["Timestamp", "ServiceName", "SeverityText", "Body"]}
HOUR = ("2026-09-26 10:00:00", "2026-09-26 11:00:00")
RANKS = [1, 10, 60, 120]


def services():
    """Query services by traffic rank, from mixgen's own mix."""
    out = subprocess.run([f"{S}/sorting/bin/mixgen", "-ring", "-publishers", "1"], capture_output=True, text=True, check=True).stdout
    m = json.loads(out)
    ranked = sorted(m.items(), key=lambda kv: -kv[1]["share"])
    sel = {f"rank{r}": {"service": ranked[r - 1][0], "share": ranked[r - 1][1]["share"]} for r in RANKS}
    json.dump(sel, open(f"{HERE}/services.json", "w"), indent=1)
    print(json.dumps(sel))


def listing(prefix):
    """Every .parquet key under a filer prefix (recursive)."""
    keys = []
    def walk(p):
        last = ""
        while True:
            u = f"http://127.0.0.1:18888/buckets/otel/{p}/?limit=1000&lastFileName={urllib.parse.quote(last)}"
            req = urllib.request.Request(u, headers={"Accept": "application/json"})
            d = json.load(urllib.request.urlopen(req))
            ents = d.get("Entries") or []
            for e in ents:
                name = e["FullPath"].rsplit("/", 1)[1]
                if e.get("Mode", 0) & 0o20000000000:
                    walk(f"{p}/{name}")
                elif name.endswith(".parquet"):
                    keys.append(f"{p}/{name}")
            if not d.get("ShouldDisplayLoadMore"):
                break
            last = d.get("LastFileName", "")
    walk(prefix)
    return sorted(keys)


def fetch(key):
    # no local cache: disk is short (SeaweedFS stops writing below 1% free)
    return urllib.request.urlopen(f"http://127.0.0.1:18888/buckets/otel/{key}").read()


def chunk_range(cc):
    start = cc.dictionary_page_offset if cc.has_dictionary_page and cc.dictionary_page_offset else cc.data_page_offset
    return start, start + cc.total_compressed_size


def model(b, svc, bucket_of, cols, footer_guess, coalesce):
    """GETs and bytes for one object and one service, per row-group pick."""
    size = len(b)
    flen = int.from_bytes(b[-8:-4], "little") + 8
    f = pq.ParquetFile(io.BytesIO(b))
    md = f.metadata
    kv = {k.decode(): v.decode() for k, v in (md.metadata or {}).items()}
    sort = kv.get("oscope-sort", "")
    names = [md.schema.column(i).path for i in range(md.num_columns)]
    svc_i = names.index("ServiceName")
    want = [i for i, p in enumerate(names) if cols == "all" or p.split(".")[0] in cols or p in cols]
    buckets = None
    if sort.startswith("service_time;hash:"):
        _, h, bl = sort.split(";")
        n = int(h.split(":")[1])
        buckets = [int(x) for x in bl.split(",")] if bl else [0]
    out = {}
    rg_rows = [md.row_group(g).num_rows for g in range(md.num_row_groups)]
    for pick in ("whole", "minmax", "bucket"):
        if pick == "whole":
            out[pick] = {"gets": 1, "bytes": size, "rgs": md.num_row_groups, "rows": sum(rg_rows)}
            continue
        if pick == "bucket" and buckets is None:
            continue
        sel = []
        for g in range(md.num_row_groups):
            if pick == "minmax":
                st = md.row_group(g).column(svc_i).statistics
                if st is None or not st.has_min_max:
                    sel.append(g)
                    continue
                lo, hi = st.min, st.max
                lo = lo.decode() if isinstance(lo, bytes) else lo
                hi = hi.decode() if isinstance(hi, bytes) else hi
                if lo <= svc <= hi:
                    sel.append(g)
            else:
                if buckets[g] == bucket_of(svc, n):
                    sel.append(g)
        # footer: suffix GET of footer_guess bytes, a second one if short
        gets = 1 if flen <= footer_guess else 2
        got = min(size, max(footer_guess, flen))
        tail_start = size - got
        ranges = []
        for g in sel:
            for i in want:
                s, e = chunk_range(md.row_group(g).column(i))
                if s >= tail_start:
                    continue  # already in the suffix read
                ranges.append((s, min(e, tail_start)))
        ranges.sort()
        merged = []
        for s, e in ranges:
            if merged and s - merged[-1][1] <= coalesce:
                merged[-1][1] = max(merged[-1][1], e)
            else:
                merged.append([s, e])
        gets += len(merged)
        byts = got + sum(e - s for s, e in merged)
        out[pick] = {"gets": gets, "bytes": byts, "rgs": len(sel), "rows": sum(rg_rows[g] for g in sel)}
    return {"size": size, "footer": flen, "rgs": md.num_row_groups, "sort": sort[:40], "picks": out}


def buckets_helper():
    """xxh3_64(name) mod n (encode::service_bucket) for every query service
    and n in (4, 16): ClickHouse's xxh3() is the same XXH3-64, seed 0 (checked
    against the objects: `check_bucket`). Cached in buckets.json."""
    p = f"{HERE}/buckets.json"
    if not os.path.exists(p):
        sv = json.load(open(f"{HERE}/services.json"))
        out = {}
        for s in sv.values():
            n = s["service"]
            out[n] = {str(k): int(ch(f"SELECT xxh3('{n}') % {k}")) for k in (4, 16)}
        json.dump(out, open(p, "w"), indent=1)
    return json.load(open(p))


def check_bucket(b, svc, bucket):
    """The row group the footer names for this bucket holds the service's rows."""
    f = pq.ParquetFile(io.BytesIO(b))
    sort = {k.decode(): v.decode() for k, v in f.metadata.metadata.items()}["oscope-sort"]
    bl = [int(x) for x in sort.split(";")[2].split(",")]
    tot = 0
    for g in range(f.metadata.num_row_groups):
        vals = f.read_row_group(g, columns=["ServiceName"]).column(0).to_pylist()
        c = sum(1 for v in vals if v == svc)
        if c:
            assert bl[g] == bucket, (svc, bl[g], bucket)
        tot += c
    return tot


def direct(setdirs):
    sv = json.load(open(f"{HERE}/services.json"))
    bk = buckets_helper()
    bucket_of = lambda s, n: 0 if n == 1 else bk[s][str(n)]  # one row group: "hash:1;0"
    outp = open(f"{HERE}/direct.jsonl", "a")
    done = set()
    if os.path.exists(f"{HERE}/direct.jsonl"):
        for l in open(f"{HERE}/direct.jsonl"):
            d = json.loads(l)
            done.add((d["set"], d["config"], d["signal"]))
    for setdir in setdirs:
        pre = f"sorting/{setdir}/"
        cfgs = sorted({k[len(pre):].split("/")[0] for k in listing(f"sorting/{setdir}")})
        for cfg in cfgs:
            keys = listing(f"sorting/{setdir}/{cfg}")
            for sig in ("traces", "logs"):
                if (setdir, cfg, sig) in done:
                    continue
                ks = [k for k in keys if f"/{sig}/" in k]
                if not ks:
                    continue
                objs = []
                for j, k in enumerate(ks):
                    b = fetch(k)
                    if j == 0 and "hash" in cfg:
                        n = 16 if "16" in cfg else 4
                        for s in sv.values():
                            check_bucket(b, s["service"], bucket_of(s["service"], n))
                    per = {}
                    for rank, s in sv.items():
                        per[rank] = {}
                        for cols in ("narrow", "all"):
                            for fg, co in ((65536, 1 << 20), (65536, 0), (16384, 1 << 20)):
                                m = model(b, s["service"], bucket_of, NARROW[sig] if cols == "narrow" else "all", fg, co)
                                per[rank][f"{cols}/fg{fg // 1024}k/co{co >> 10}k"] = m["picks"]
                    objs.append({"key": k, "size": m["size"], "footer": m["footer"], "rgs": m["rgs"], "q": per})
                outp.write(json.dumps({"set": setdir, "config": cfg, "signal": sig, "objects": objs}) + "\n")
                outp.flush()
                print(setdir, cfg, sig, len(objs), "objects", file=sys.stderr)


STRUCT = {
    "traces": "Timestamp DateTime64(9), TraceId String, SpanId String, ParentSpanId String, TraceState String, SpanName String, SpanKind String, ServiceName String, ResourceAttributes Map(String, String), ScopeName String, ScopeVersion String, SpanAttributes Map(String, String), Duration UInt64, StatusCode String, StatusMessage String, `Events.Timestamp` Array(DateTime64(9)), `Events.Name` Array(String), `Events.Attributes` Array(Map(String, String)), `Links.TraceId` Array(String), `Links.SpanId` Array(String), `Links.TraceState` Array(String), `Links.Attributes` Array(Map(String, String))",
    "logs": "Timestamp DateTime64(9), TraceId String, SpanId String, TraceFlags UInt8, SeverityText String, SeverityNumber UInt8, ServiceName String, Body String, ResourceSchemaUrl String, ResourceAttributes Map(String, String), ScopeSchemaUrl String, ScopeName String, ScopeVersion String, ScopeAttributes Map(String, String), LogAttributes Map(String, String), EventName String",
}
ENV = ", producer_id String, producer_epoch String, batch_id UInt64, row_ordinal UInt32, received_at DateTime64(9), schema_version UInt16"


def ch(sql, params=None):
    q = urllib.parse.urlencode(params or {})
    r = urllib.request.Request(f"{CH}/?{q}", data=sql.encode())
    return urllib.request.urlopen(r).read().decode()


VARIANTS = {
    # every row group read: the baseline "whole objects"
    "nopushdown": {"input_format_parquet_filter_push_down": 0, "input_format_parquet_bloom_filter_push_down": 0},
    "minmax": {"input_format_parquet_filter_push_down": 1, "input_format_parquet_bloom_filter_push_down": 0},
    "minmax+bloom": {"input_format_parquet_filter_push_down": 1, "input_format_parquet_bloom_filter_push_down": 1},
}


def chq(configs, setdir="set", reps=3):
    sv = json.load(open(f"{HERE}/services.json"))
    have = set(ch("SELECT name FROM system.settings WHERE name LIKE '%parquet%' OR name LIKE 'remote%' FORMAT TSV").split())
    extra = {}
    # footer caching across queries would hide the footer GETs
    for k in ("input_format_parquet_use_metadata_cache", "use_parquet_metadata_cache"):
        if k in have:
            extra[k] = 0
    seek = [None] + ([0] if "remote_read_min_bytes_for_seek" in have else [])
    # The default remote read (threadpool with prefetch) downloads a whole
    # small object whatever the pruning; method 'read' with seek 0 makes
    # ranged GETs. "read0-mdcache" also keeps the footer cache on (rep 1
    # warms it: reps 2-3 are the cached-footer case).
    if os.environ.get("RANGED", "1") == "1":
        seek += ["read0", "read0-mdcache"]
    outp = open(f"{HERE}/ch.jsonl", "a")
    done = set()
    if os.path.exists(f"{HERE}/ch.jsonl"):
        for l in open(f"{HERE}/ch.jsonl"):
            d = json.loads(l)
            done.add((d["set"], d["config"], d["signal"], d["rank"], d["cols"], d["variant"], d["seek"], d["rep"]))
    for cfg in configs:
        for sig in ("traces", "logs"):
            url = f"http://127.0.0.1:18333/otel/sorting/{setdir}/{cfg}/edges/*/{sig}/*/*.parquet"
            for rank, s in sv.items():
                for cols in ("narrow", "all"):
                    sel = ("count() AS c, sum(Duration) AS x" if sig == "traces" else "count() AS c, sum(length(Body)) AS x") if cols == "narrow" else "*"
                    for vname, vset in VARIANTS.items():
                        for sk in seek:
                            for rep in range(1, reps + 1):
                                key = (setdir, cfg, sig, rank, cols, vname, sk, rep)
                                if key in done:
                                    continue
                                settings = dict(vset, **extra)
                                if sk in ("read0", "read0-mdcache"):
                                    settings["remote_read_min_bytes_for_seek"] = 0
                                    settings["remote_filesystem_read_method"] = "'read'"
                                    if sk == "read0-mdcache":
                                        for k in extra:
                                            settings[k] = 1
                                elif sk is not None:
                                    settings["remote_read_min_bytes_for_seek"] = sk
                                qid = f"sort-{setdir}-{cfg}-{sig}-{rank}-{cols}-{vname}-{sk}-{rep}-{os.getpid()}".replace("+", "_")
                                sql = (f"SELECT {sel} FROM s3('{url}', 'otel', 'otelsecret', 'Parquet', '{STRUCT[sig]}{ENV}') "
                                       f"WHERE ServiceName = '{s['service']}' AND Timestamp >= '{HOUR[0]}' AND Timestamp < '{HOUR[1]}' "
                                       f"SETTINGS " + ", ".join(f"{k} = {v}" for k, v in settings.items()) + " FORMAT Null")
                                ch(sql, {"query_id": qid})
                                outp.write(json.dumps({"set": setdir, "config": cfg, "signal": sig, "rank": rank, "service": s["service"],
                                                       "cols": cols, "variant": vname, "seek": sk, "rep": rep, "query_id": qid, "settings": settings}) + "\n")
                                outp.flush()
    ch("SYSTEM FLUSH LOGS")


def chlog():
    """Joins ch.jsonl with query_log's ProfileEvents -> ch-events.jsonl."""
    rows = [json.loads(l) for l in open(f"{HERE}/ch.jsonl")]
    ids = [r["query_id"] for r in rows]
    ev = {}
    for i in range(0, len(ids), 200):
        part = ids[i:i + 200]
        q = ("SELECT query_id, read_rows, read_bytes, query_duration_ms, "
             "mapFilter((k, v) -> k LIKE '%S3%' OR k LIKE '%Parquet%' OR k LIKE 'ReadBuffer%' OR k LIKE '%CPU%' OR k LIKE '%RowGroup%', ProfileEvents) AS pe "
             "FROM system.query_log WHERE type = 'QueryFinish' AND query_id IN (" + ",".join(f"'{x}'" for x in part) + ") FORMAT JSONEachRow")
        for l in ch(q).splitlines():
            d = json.loads(l)
            ev[d["query_id"]] = d
    with open(f"{HERE}/ch-events.jsonl", "w") as f:
        for r in rows:
            if r["query_id"] in ev:
                r.update(ev[r["query_id"]])
                f.write(json.dumps(r) + "\n")


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "services":
        services()
    elif cmd == "direct":
        direct(sys.argv[2:] or ["set"])
    elif cmd == "ch":
        chq(sys.argv[2].split(",") if len(sys.argv) > 2 else [], reps=int(os.environ.get("REPS", "3")))
    elif cmd == "chlog":
        chlog()
