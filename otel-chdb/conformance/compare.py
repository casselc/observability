#!/usr/bin/env python3
"""Go edge = Rust edge, after the Rust consumer: compare.py TAG S3_RUN_ROOT

Two levels, every check PASS/FAIL, exit 1 on any FAIL:

1. Central (ClickHouse, databases goedge_{TAG}_rust and goedge_{TAG}_go,
   filled by `consume` from each edge's root): the same tables; per table,
   count + sum(cityHash64(every column)) and EXCEPT both ways over every
   column except the run's identity (producer_id, producer_epoch,
   received_at, content_key, and otel_resources' times); the same content keys with the same row
   counts (the Go edge hashes its re-marshalled request, the Rust edge the
   bytes it received: equal for a canonically encoded request); the
   envelope (one producer, received_at constant per object, row_ordinal
   0..n-1, batch_id = the slot).
2. Objects (S3, `{root}/{edge}/edge-{edge}/{namespace}/{epoch}/{seq}.parquet`):
   the same namespaces and object counts; per object pair (same slot): the
   same S3 user metadata and Parquet footer key-values (all but the run's
   identity), footer = metadata + kind/epoch/seq/content; the same Parquet
   schema (every leaf's physical and logical type and repetition); the
   same column encodings where they are part of the contract
   (BYTE_STREAM_SPLIT, DELTA_BINARY_PACKED) and the same statistics policy
   (layout B: none). Dictionary choices are reported, not failed.
"""
import io, os, subprocess, sys
import requests
import pyarrow.parquet as pq

CH = os.environ.get("CH", "http://127.0.0.1:18123")
KEY, SECRET = os.environ.get("AWS_ACCESS_KEY_ID", "otel"), os.environ.get("AWS_SECRET_ACCESS_KEY", "otelsecret")
RUN_COLS = {"producer_id", "producer_epoch", "received_at", "content_key",
            # otel_resources (the announcements) and its view: the same identity, as times
            "seen_at", "ingested_at", "first_seen", "last_seen", "first_ingested"}
RUN_META = {"oscope-producer", "oscope-epoch", "oscope-received", "oscope-low"}
fails = 0

# Known differences between the edges, each normalized on both sides before
# the row comparison, and reported (INFO) with the rows each edge holds that
# the normalization changes. (table, column) -> (expression, why).
KNOWN = {
    ("otel_traces", "SpanKind"): (
        "if(`SpanKind` = '', 'Unspecified', `SpanKind`)",
        "a span kind outside 0..5 is '' from the Go edge (pdata SpanKind.String(), as contrib stores it) and "
        "'Unspecified' from the Rust edge (otap-dataflow's OTLP span view: SpanKind::try_from(v).unwrap_or(Unspecified))"),
}


# ClickStack's key-value rollups (SummingMergeTree, fed by a view) are compared
# summed over their key, not row by row: how many rows hold a key depends on
# which parts have merged, not on the edge. The span-kind difference above
# reaches them as a Key = 'SpanKind' row with Value '' (Go) or 'Unspecified'
# (Rust), normalized the same way.
ROLLUP_VALUE = "if(`Key` = 'SpanKind' AND `Value` = '', 'Unspecified', `Value`)"


def rollup_source(table, cols):
    """The rollup summed over every column but `count`: its sort key, which
    includes `cluster` since D33 (a table created before it has none), and any
    later column; so a column added to the rollup is compared, never dropped."""
    keys = [c for c in cols if c != "count"]
    sel = ", ".join(f"{ROLLUP_VALUE} AS `Value`" if c == "Value" else f"`{c}`" for c in keys)
    return f"(SELECT {sel}, sum(`count`) AS `count` FROM {table} GROUP BY {', '.join(f'`{c}`' for c in keys)})"


def check(name, ok, detail=""):
    global fails
    fails += 0 if ok else 1
    print(f"{'PASS' if ok else 'FAIL'} {name}{': ' + detail if detail else ''}", flush=True)


def info(name, detail):
    print(f"INFO {name}: {detail}", flush=True)


def ch(sql):
    r = requests.post(CH + "/", data=sql.encode(), params={"use_query_condition_cache": 0}, timeout=600)
    if r.status_code != 200:
        raise RuntimeError(f"{r.status_code}: {r.text[:500]}\n{sql[:300]}")
    return r.text.strip()


def s3(args, url):
    return subprocess.run(["curl", "-s", "--aws-sigv4", "aws:amz:us-east-1:s3", "--user", f"{KEY}:{SECRET}"] + args + [url],
                          capture_output=True, check=True).stdout


def list_keys(root):
    """Every key under an http://host/bucket/prefix root (ListObjectsV2, paged)."""
    scheme, rest = root.split("://", 1)
    host, bucket, prefix = rest.split("/", 2)
    keys, token = [], None
    while True:
        q = f"{scheme}://{host}/{bucket}?list-type=2&prefix={prefix}/"
        if token:
            q += "&continuation-token=" + requests.utils.quote(token, safe="")
        body = s3([], q).decode()
        keys += [k.split("</Key>")[0] for k in body.split("<Key>")[1:]]
        if "<IsTruncated>true</IsTruncated>" not in body:
            break
        token = body.split("<NextContinuationToken>")[1].split("</NextContinuationToken>")[0]
    return f"{scheme}://{host}/{bucket}", keys


def head_meta(url):
    out = {}
    for line in s3(["-I"], url).decode(errors="replace").splitlines():
        k, _, v = line.partition(":")
        if k.lower().startswith("x-amz-meta-"):
            out[k.lower()[len("x-amz-meta-"):]] = v.strip()
    return out


def leaf_schema(f):
    """Every leaf: path, physical and logical type, levels. A signed INT32/INT64
    with or without its Int(signed) annotation is the same type (the spec's
    default for an unannotated INT32/INT64), so the annotation is dropped."""
    sc = f.schema
    out = []
    for i in range(len(sc)):
        c = sc.column(i)
        lt = str(c.logical_type)
        if (c.physical_type, lt) in (("INT32", "Int(bitWidth=32, isSigned=true)"), ("INT64", "Int(bitWidth=64, isSigned=true)")):
            lt = "None"
        out.append((c.path, c.physical_type, lt, c.max_repetition_level, c.max_definition_level))
    return out


def encodings(f):
    md = f.metadata
    out = {}
    for rg in range(md.num_row_groups):
        for c in range(md.num_columns):
            col = md.row_group(rg).column(c)
            e = set(col.encodings) - {"RLE", "BIT_PACKED"}
            out.setdefault(col.path_in_schema, set()).update(e)
            out.setdefault(col.path_in_schema + "#stats", set()).add(
                "minmax" if col.is_stats_set and col.statistics.has_min_max else "none")
    return out


def central(tag):
    dbs = {e: f"goedge_{tag}_{e}" for e in ("rust", "go")}
    tables = {e: set(ch(f"SELECT name FROM system.tables WHERE database = '{db}' AND NOT startsWith(name, '.') FORMAT TSV").split())
              for e, db in dbs.items()}
    check("central: same tables", tables["rust"] == tables["go"] and len(tables["rust"]) > 0,
          f"rust only {sorted(tables['rust'] - tables['go'])}, go only {sorted(tables['go'] - tables['rust'])}")
    for t in sorted(tables["rust"] & tables["go"]):
        cols = ch(f"SELECT name FROM system.columns WHERE database = '{dbs['rust']}' AND table = '{t}' ORDER BY position FORMAT TSV").split("\n")
        src = {e: f"{db}.{t}" for e, db in dbs.items()}
        if t == "otel_resources":
            # A ReplacingMergeTree: an announcement re-inserted (a retry) is one row.
            src = {e: f"{s} FINAL" for e, s in src.items()}
        if "_kv_rollup_" in t and {"Key", "Value", "count"} <= set(cols):
            n = {e: ch(f"SELECT countIf(`Key` = 'SpanKind' AND `Value` = '') FROM {s}") for e, s in src.items()}
            info(f"{t}: compared summed per key", f"span-kind '' rows normalized to 'Unspecified'; rows changed: rust {n['rust']}, go {n['go']}")
            src = {e: rollup_source(s, cols) for e, s in src.items()}
        exprs = []
        for c in cols:
            if c in RUN_COLS:
                continue
            if (t, c) in KNOWN:
                x, why = KNOWN[(t, c)]
                n = {e: ch(f"SELECT countIf(toString(`{c}`) != toString({x})) FROM {s}") for e, s in src.items()}
                info(f"{t}.{c}: known difference normalized", f"{why}; rows changed: rust {n['rust']}, go {n['go']}")
                exprs.append(f"{x} AS `{c}`")
            else:
                exprs.append(f"`{c}`")
        cmp = ", ".join(exprs)
        h = {e: ch(f"SELECT count(), sum(cityHash64({cmp})) FROM {s} FORMAT TSV") for e, s in src.items()}
        check(f"{t}: count + hash", h["rust"] == h["go"] and h["rust"].split("\t")[0] != "0", f"rust {h['rust']} go {h['go']}")
        for a, b in (("rust", "go"), ("go", "rust")):
            n = ch(f"SELECT count() FROM (SELECT {cmp} FROM {src[a]} EXCEPT ALL SELECT {cmp} FROM {src[b]})")
            check(f"{t}: rows in {a} not in {b}", n == "0", n)
        if h["rust"] != h["go"]:
            # Which columns differ, and a sample of the values only one side has.
            for x in exprs:
                name = x.split(" AS ")[-1]
                hh = {e: ch(f"SELECT sum(cityHash64({x})) FROM {s}") for e, s in src.items()}
                if hh["rust"] != hh["go"]:
                    for a, b in (("rust", "go"), ("go", "rust")):
                        sample = ch(f"SELECT substring(toString(v), 1, 160), count() FROM (SELECT {x.split(' AS ')[0]} AS v FROM {src[a]} "
                                    f"EXCEPT ALL SELECT {x.split(' AS ')[0]} AS v FROM {src[b]}) GROUP BY 1 ORDER BY 2 DESC LIMIT 3 FORMAT TSV")
                        print(f"DIFF {t}.{name}: only in {a}: {sample!r}", flush=True)
        if "content_key" in cols:
            ks = {e: ch(f"SELECT content_key, count() FROM {s} GROUP BY content_key ORDER BY content_key FORMAT TSV") for e, s in src.items()}
            n = len(ks["rust"].splitlines())
            check(f"{t}: same content keys and rows per key ({n})", ks["rust"] == ks["go"],
                  f"rust {len(ks['rust'].splitlines())} keys, go {len(ks['go'].splitlines())}, "
                  f"common {len(set(ks['rust'].splitlines()) & set(ks['go'].splitlines()))}")
        if "received_at" in cols:
            for e, s in src.items():
                bad = ch(f"SELECT countIf(nr != 1) + countIf(mx + 1 != n) + countIf(np != 1) FROM (SELECT producer_epoch, batch_id, "
                         f"uniqExact(received_at) nr, max(row_ordinal) mx, count() n, uniqExact(producer_id) np "
                         f"FROM {s} GROUP BY producer_epoch, batch_id)")
                prod = ch(f"SELECT groupUniqArray(producer_id) FROM {s}")
                check(f"{t}: {e} envelope (one received_at per object, ordinals 0..n-1, producer {prod})", bad == "0" and prod == f"['edge-{e}']", bad)


def objects(root):
    listed = {}
    for e in ("rust", "go"):
        base, keys = list_keys(f"{root}/{e}/conf/edge-{e}")  # {root}/{cluster}/{producer} (FORMAT.md)
        by = {}
        for k in keys:
            parts = k.split("/")
            if len(parts) < 3 or not parts[-1].endswith(".parquet") or parts[-3].startswith("_"):
                continue
            by.setdefault(parts[-3], []).append((parts[-2], parts[-1], k))
        listed[e] = (base, {ns: sorted(v) for ns, v in by.items()})
    nss = {e: set(v[1]) for e, v in listed.items()}
    check("objects: same namespaces", nss["rust"] == nss["go"], f"rust {sorted(nss['rust'])} go {sorted(nss['go'])}")
    dict_diffs = 0
    for ns in sorted(nss["rust"] & nss["go"]):
        r, g = listed["rust"][1][ns], listed["go"][1][ns]
        check(f"{ns}: same object count", len(r) == len(g), f"rust {len(r)} go {len(g)}")
        for (re_, rs, rk), (ge, gs, gk) in zip(r, g):
            tag = f"{ns}/{rs}"
            rm, gm = head_meta(f"{listed['rust'][0]}/{rk}"), head_meta(f"{listed['go'][0]}/{gk}")
            beat = gm.get("oscope-kind") == "beat"  # a birth heartbeat (FORMAT.md §2): random content, no body
            same = lambda m: {k: v for k, v in m.items() if k not in RUN_META and not ((ns == "metrics_series" or beat) and k == "oscope-content")}
            check(f"{tag}: S3 metadata", same(rm) == same(gm) and set(rm) == set(gm)
                  and gm.get("oscope-epoch") == ge and gm.get("oscope-seq") == str(int(gs[:-8])),
                  f"rust {sorted(rm.items())} go {sorted(gm.items())}" if same(rm) != same(gm) or set(rm) != set(gm) else "")
            if beat:
                check(f"{tag}: both heartbeats", rm.get("oscope-kind") == "beat", f"rust {rm.get('oscope-kind')}")
                continue
            rb, gb = s3([], f"{listed['rust'][0]}/{rk}"), s3([], f"{listed['go'][0]}/{gk}")
            rf, gf = pq.ParquetFile(io.BytesIO(rb)), pq.ParquetFile(io.BytesIO(gb))
            kv = {e: {k.decode(): v.decode() for k, v in (f.metadata.metadata or {}).items()} for e, f in (("rust", rf), ("go", gf))}
            # the lane's metadata includes kind, epoch, seq, content and producer; format,
            # cluster and low are S3 metadata only (FORMAT.md §2)
            want = {k: v for k, v in gm.items() if k not in ("oscope-format", "oscope-cluster", "oscope-low")}
            check(f"{tag}: footer = metadata (go)", kv["go"] == want, f"{sorted(set(kv['go'].items()) ^ set(want.items()))}")
            check(f"{tag}: footer keys as rust's", set(kv["go"]) == set(kv["rust"]), f"{sorted(set(kv['go']) ^ set(kv['rust']))}")
            check(f"{tag}: Parquet schema", leaf_schema(rf) == leaf_schema(gf),
                  f"{[x for x in zip(leaf_schema(rf), leaf_schema(gf)) if x[0] != x[1]][:3]}")
            re, ge2 = encodings(rf), encodings(gf)
            contract = lambda e: {k: sorted(v & {"BYTE_STREAM_SPLIT", "DELTA_BINARY_PACKED"}) if not k.endswith("#stats") else sorted(v)
                                  for k, v in e.items()}
            bad = {k: (contract(re).get(k), contract(ge2).get(k)) for k in set(re) | set(ge2) if contract(re).get(k) != contract(ge2).get(k)}
            check(f"{tag}: wire encodings and statistics", not bad, f"{sorted(bad.items())[:6]}")
            dd = {k for k in set(re) | set(ge2) if not k.endswith("#stats")
                  and ("PLAIN_DICTIONARY" in re.get(k, set()) or "RLE_DICTIONARY" in re.get(k, set()))
                  != ("PLAIN_DICTIONARY" in ge2.get(k, set()) or "RLE_DICTIONARY" in ge2.get(k, set()))}
            if dd:
                dict_diffs += 1
                info(f"{tag}: dictionary differs", f"{sorted(dd)[:8]}")
            check(f"{tag}: rows", rf.metadata.num_rows == gf.metadata.num_rows, f"rust {rf.metadata.num_rows} go {gf.metadata.num_rows}")
            info(f"{tag}: bytes", f"rust {len(rb)} (footer {rf.metadata.serialized_size}) go {len(gb)} (footer {gf.metadata.serialized_size})")
    return dict_diffs


def main():
    tag, root = sys.argv[1], sys.argv[2]
    central(tag)
    objects(root)
    print(f"{fails} failed")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
