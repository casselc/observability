#!/usr/bin/env python3
"""End to end: a pod the entity controller never sees becomes exact through
its edge announcement, and the transient gap is the dictionary's lag.

  real edge (otap-s3pq, ../../otap-rs/configs/edge.yaml) -> SeaweedFS -> consume -> ClickHouse
  catalog: ../controller/sql/aggregator.sql + announced.sql over the consumer's otel_resources
  a flat dictionary over the merged {cat}.resources, LIFETIME(MIN 30 MAX 60) as measured in README §3.6

Three pods send logs: `known` (the controller wrote it before any row),
`orphan` (born and gone inside a controller outage: only the edge's
announcement tells the catalog), and `silent` through a second edge with
ANNOUNCE=false (the noAnnounce mutant of ../../model/entityCatalog.qnt: it
must stay residual-only). A row is exact when the covered part of its
ResourceAttributes equals the dictionary's attributes for its resource_id.
Measured: when each pod's rows became visible and exact; the gap for
`orphan` (entityCatalog.qnt exactAfterLag: bounded by the dictionary lag);
objects, PUTs and bytes per lane; the bytes of an announcement.

  B=<dir with otap-s3pq and consume> python3 announce_e2e.py [--lifetime 30:60] [--keep]

Uses bucket $BUCKET (default otel) under announce-e2e/<run>/ only, and
databases ann_e2e_<run>_{c,cat}; removes both at the end unless --keep.
Never deletes a bucket.
"""
import argparse, json, os, re, signal, subprocess, sys, tempfile, time, urllib.parse, urllib.request
import xml.etree.ElementTree as ET

HERE = os.path.dirname(os.path.abspath(__file__))
CHDB = os.path.abspath(os.path.join(HERE, "..", ".."))
CH = os.environ.get("CH", "http://127.0.0.1:18123")
S3 = os.environ.get("S3", "http://127.0.0.1:18333")
BUCKET = os.environ.get("BUCKET", "otel")
KEY, SECRET = os.environ.get("AWS_ACCESS_KEY_ID", "otel"), os.environ.get("AWS_SECRET_ACCESS_KEY", "otelsecret")
COVERED = json.load(open(os.path.join(CHDB, "entities", "testdata", "resource_id_vectors.json")))["covered_keys"]


def q(sql, fmt=None):
    if fmt:
        sql += f" FORMAT {fmt}"
    req = urllib.request.Request(CH + "/", data=sql.encode(), method="POST")
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            return r.read().decode().strip()
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"{e.code}: {e.read().decode()[:800]}\n--- {sql[:400]}") from None


def script(path, **subs):
    body = "\n".join(l for l in open(path).read().splitlines() if not l.lstrip().startswith("--"))
    for k, v in subs.items():
        body = body.replace("{" + k + "}", v)
    for st in body.split(";\n"):
        st = st.strip().rstrip(";").strip()
        if st:
            q(st)


def s3(method, path, query=""):
    url = f"{S3}/{BUCKET}/{path}" + (f"?{query}" if query else "")
    out = subprocess.run(["curl", "-s", "-X", method, "--aws-sigv4", "aws:amz:us-east-1:s3", "--user", f"{KEY}:{SECRET}", url],
                         capture_output=True, check=True)
    return out.stdout.decode()


def s3_list(prefix):
    items, token = [], None
    while True:
        qs = "list-type=2&prefix=" + urllib.parse.quote(prefix) + (f"&continuation-token={urllib.parse.quote(token)}" if token else "")
        root = ET.fromstring(s3("GET", "", qs))
        ns = {"s": root.tag.split("}")[0].strip("{")}
        for c in root.findall("s:Contents", ns):
            items.append((c.find("s:Key", ns).text, int(c.find("s:Size", ns).text)))
        if root.findtext("s:IsTruncated", default="false", namespaces=ns) != "true":
            return items
        token = root.findtext("s:NextContinuationToken", namespaces=ns)


# ---- a minimal OTLP/protobuf logs request ------------------------------------------------------

def varint(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        out.append(b | (0x80 if n else 0))
        if not n:
            return bytes(out)


def ld(field, body):
    return varint(field << 3 | 2) + varint(len(body)) + body


def anystr(s):
    return ld(1, s.encode())


def kv(k, v):
    return ld(1, k.encode()) + ld(2, anystr(v))


def logs_request(attrs, n, tag, t0_ns):
    res = b"".join(ld(1, kv(k, v)) for k, v in attrs.items())
    recs = b""
    for i in range(n):
        rec = varint(1 << 3 | 1) + (t0_ns + i).to_bytes(8, "little") + ld(5, anystr(f"{tag} record {i}"))
        recs += ld(2, rec)
    rl = ld(1, res) + ld(2, recs)
    return ld(1, rl)


def pod(name, cluster_uid="e2e-cluster-uid"):
    return {
        "k8s.cluster.name": "e2e", "k8s.cluster.uid": cluster_uid, "k8s.namespace.name": "shop",
        "k8s.node.name": "node-1", "k8s.deployment.name": "cart", "k8s.replicaset.name": "cart-5f6d",
        "k8s.pod.name": name, "k8s.pod.uid": "uid-" + name, "k8s.pod.start_time": "2026-09-28T10:00:00Z",
        "k8s.pod.label.team": "payments", "k8s.container.name": "app", "container.image.name": "reg/cart",
        "container.image.tag": "1.0", "service.name": "cart",
        # the residual: SDK-set, never hashed, never announced
        "telemetry.sdk.name": "opentelemetry", "service.instance.id": name + "/app",
    }


def post(port, body):
    req = urllib.request.Request(f"http://127.0.0.1:{port}/v1/logs", data=body, method="POST",
                                 headers={"content-type": "application/x-protobuf"})
    with urllib.request.urlopen(req, timeout=60) as r:
        assert r.status == 200, r.status


def wait_up(port):
    for _ in range(300):
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=1)
            return
        except urllib.error.HTTPError:
            return
        except Exception:
            time.sleep(0.2)
    raise SystemExit(f"edge on {port} did not start")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lifetime", default="30:60", help="the dictionary's LIFETIME(MIN:MAX), s")
    ap.add_argument("--keep", action="store_true")
    ap.add_argument("--out", default=os.path.join(HERE, "..", "results", "announce_e2e.json"))
    a = ap.parse_args()
    B = os.environ["B"]
    run = f"{int(time.time())}"
    root = f"announce-e2e/{run}/edges"
    cdb, cat = f"ann_e2e_{run}_c", f"ann_e2e_{run}_cat"
    lo, hi = a.lifetime.split(":")
    work = tempfile.mkdtemp(prefix="announce-e2e-")
    procs = []
    out = {"run": run, "lifetime_s": [int(lo), int(hi)]}
    try:
        # The catalog: the controller knows `known` only (its records are
        # inserted before any row: a controller that was up for it).
        q(f"CREATE DATABASE {cdb}")
        q(f"CREATE DATABASE {cat}")
        script(os.path.join(CHDB, "otap-rs", "sql", "otel_resources.sql"), table=f"{cdb}.otel_resources")
        script(os.path.join(CHDB, "entities", "controller", "sql", "aggregator.sql"), db=cat)
        script(os.path.join(CHDB, "entities", "controller", "sql", "announced.sql"), db=cat, ann=f"{cdb}.otel_resources")
        known = {k: v for k, v in pod("known").items() if k in COVERED or k.startswith("k8s.pod.label.")}
        rid_known = int(q(f"SELECT xxh3(concat('res.v1\\0', arrayStringConcat(arrayMap(x -> concat(x.1, '\\0', x.2, '\\0'), "
                          f"arraySort(CAST(map({', '.join(repr(k) + ', ' + repr(v) for k, v in known.items())}), 'Array(Tuple(String, String))'))))))"))
        rec = {"level": "resource", "key": rid_known, "entity": 1, "cluster_key": 1, "pod_uid": "uid-known", "container": "app",
               "attrs": known, "valid_from": "2026-09-28 10:00:00.000", "closed_at": "1970-01-01 00:00:00.000",
               "observed_at": "2026-09-28 10:00:00.000", "event_at": "2026-09-28 10:00:00.000", "writer": "ctl"}
        q(f"INSERT INTO {cat}.records FORMAT JSONEachRow\n" + json.dumps(rec))
        q(f"CREATE DICTIONARY {cat}.d_res (resource_id UInt64, attrs Map(String, String)) PRIMARY KEY resource_id "
          f"SOURCE(CLICKHOUSE(QUERY 'SELECT resource_id, attrs FROM {cat}.resources')) LAYOUT(HASHED()) LIFETIME(MIN {lo} MAX {hi})")
        q(f"SYSTEM RELOAD DICTIONARY {cat}.d_res")
        out["dict_at_start"] = int(q(f"SELECT element_count FROM system.dictionaries WHERE database = '{cat}' AND name = 'd_res'"))

        # Two edges: announcements on, and off (the noAnnounce control).
        env = dict(os.environ, AWS_ACCESS_KEY_ID=KEY, AWS_SECRET_ACCESS_KEY=SECRET, CLUSTER="e2e", HEARTBEAT="30s")
        ports = {}
        for i, (name, ann) in enumerate([("edge-ann", "true"), ("edge-noann", "false")]):
            p = 14620 + 10 * i
            ports[name] = p
            e = dict(env, PRODUCER=name, ANNOUNCE=ann, OTLP_HTTP=f"127.0.0.1:{p}", OTLP_GRPC=f"127.0.0.1:{p + 1}",
                     ADMIN_HTTP=f"127.0.0.1:{p + 2}", S3_URL=f"{S3}/{BUCKET}/{root}")
            log = open(os.path.join(work, name + ".log"), "w")
            procs.append(subprocess.Popen([os.path.join(B, "otap-s3pq"), "-c", os.path.join(CHDB, "otap-rs", "configs", "edge.yaml")],
                                          env=e, stdout=log, stderr=subprocess.STDOUT))
        for p in ports.values():
            wait_up(p)
        t0 = time.time_ns()
        # The cost probe: the same request to both edges, then again to the announcing one.
        probe = logs_request(pod("probe"), 100, "probe", t0)
        post(ports["edge-ann"], probe)
        post(ports["edge-noann"], probe)
        post(ports["edge-ann"], logs_request(pod("probe"), 100, "probe2", t0))
        # The pods.
        post(ports["edge-ann"], logs_request(pod("known"), 50, "known", t0))
        post(ports["edge-ann"], logs_request(pod("orphan"), 50, "orphan", t0))
        post(ports["edge-ann"], logs_request(pod("orphan"), 50, "orphan-again", t0 + 10**9))
        post(ports["edge-noann"], logs_request(pod("silent"), 50, "silent", t0))
        for pr in procs:
            pr.send_signal(signal.SIGINT)
        for pr in procs:
            pr.wait(timeout=60)
        procs.clear()

        # The consumer, in the background; poll central meanwhile.
        clog = open(os.path.join(work, "consume.log"), "w")
        cons = subprocess.Popen([os.path.join(B, "consume"), "--s3", f"{S3}/{BUCKET}/{root}", "--ch", CH, "--db", cdb,
                                 "--exit-after-idle", "20s", "--poll", "300ms", "--key", KEY, "--secret", SECRET],
                                stdout=clog, stderr=subprocess.STDOUT)
        procs.append(cons)
        covered = ("mapSort(mapFilter((k, v) -> (has([" + ", ".join(repr(k) for k in COVERED) +
                   "], k) OR (startsWith(k, 'k8s.pod.label.') AND length(k) > 14)) AND v != '', CAST(ResourceAttributes, 'Map(String, String)')))")
        exact = f"(mapSort(dictGetOrDefault('{cat}.d_res', 'attrs', resource_id, map())) = {covered})"
        seen, first = {}, {}
        start = time.time()
        deadline = start + int(hi) * 3 + 120
        while time.time() < deadline:
            now = time.time()
            try:
                r = q(f"SELECT ResourceAttributes['k8s.pod.name'] AS p, count(), countIf({exact}) FROM {cdb}.otel_logs GROUP BY p", "TSV")
            except RuntimeError:
                r = ""  # the table is not there yet
            for line in r.splitlines():
                p, n, ex = line.split("\t")
                if int(n) > 0 and p not in seen:
                    seen[p] = now
                if int(n) > 0 and int(ex) == int(n) and p not in first:
                    first[p] = now
            if all(p in first for p in ("known", "orphan", "probe")) and "silent" in seen and now > first.get("orphan", now) + int(hi):
                break
            time.sleep(0.5)
        out["visible_s"] = {p: round(t - start, 1) for p, t in seen.items()}
        out["exact_s"] = {p: round(t - start, 1) for p, t in first.items()}
        out["orphan_gap_s"] = round(first["orphan"] - seen["orphan"], 1) if "orphan" in first else None
        out["silent_exact"] = "silent" in first
        # How the pods' resources stand in the catalog.
        out["catalog"] = [dict(zip(["pod", "source", "uncertain"], l.split("\t"))) for l in q(
            f"SELECT attrs['k8s.pod.name'], source, uncertain FROM {cat}.resources ORDER BY 1", "TSV").splitlines()]
        out["announcements"] = [dict(zip(["pod", "rows_final", "producers"], l.split("\t"))) for l in q(
            f"SELECT ResourceAttributes['k8s.pod.name'], count(), uniqExact(producer_epoch) FROM {cdb}.otel_resources FINAL GROUP BY 1 ORDER BY 1", "TSV").splitlines()]
        out["residual_announced"] = int(q(f"SELECT countIf(mapContains(ResourceAttributes, 'telemetry.sdk.name')) FROM {cdb}.otel_resources"))
        cons.wait(timeout=600)
        procs.clear()
        # Objects, PUTs, bytes, per lane.
        objs = s3_list(root + "/")
        lanes = {}
        for k, size in objs:
            m = re.match(rf"{re.escape(root)}/([^/]+/[^/]+/[^/]+)/", k)
            if m and not k.split("/")[len(root.split("/"))].startswith("_"):
                d = lanes.setdefault(m.group(1), {"objects": 0, "bytes": 0, "sizes": []})
                d["objects"] += 1
                d["bytes"] += size
                d["sizes"].append(size)
        out["lanes"] = lanes
        heads = {}
        for k, size in objs:
            if k.endswith(".parquet") and "/logs/" in k and size > 0:
                h = subprocess.run(["curl", "-sI", "--aws-sigv4", "aws:amz:us-east-1:s3", "--user", f"{KEY}:{SECRET}", f"{S3}/{BUCKET}/{k}"],
                                   capture_output=True).stdout.decode()
                ann = re.search(r"x-amz-meta-oscope-announce: (\d+)", h, re.I)
                heads[k] = {"size": size, "announce": int(ann.group(1)) if ann else None}
        out["log_objects"] = heads
    finally:
        for pr in procs:
            pr.kill()
        if not a.keep:
            for d in (cdb, cat):
                try:
                    q(f"DROP DATABASE IF EXISTS {d} SYNC")
                except Exception as e:
                    print("drop", d, e, file=sys.stderr)
            for k, _ in s3_list(f"announce-e2e/{run}/"):
                s3("DELETE", urllib.parse.quote(k))
    os.makedirs(os.path.dirname(a.out), exist_ok=True)
    json.dump(out, open(a.out, "w"), indent=1)
    print(json.dumps(out, indent=1))
    ok = out.get("orphan_gap_s") is not None and out["orphan_gap_s"] <= int(hi) + 5 and not out["silent_exact"] and "known" in out["exact_s"]
    print("E2E", "PASS" if ok else "FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
