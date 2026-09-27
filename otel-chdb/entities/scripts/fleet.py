#!/usr/bin/env python3
"""Synthetic Kubernetes fleet with SCD2 history, written into the entity
catalog (../sql/catalog.sql): what a multicluster controller with shared
informers would have recorded over DAYS days.

Shape (defaults: the mid scenario of ../../DECISIONS.md §1.2): 20 clusters
(14 prod, 3 staging, 3 dev in 4 regions) x 200 node slots, about 3,000 live
pods per cluster:

  node slots   replaced every 5-25 days (autoscaler / AMI rolls); pods on a
               replaced node are rescheduled (new pod uid)
  daemonsets   4 per cluster, one pod per node, rolled every 14-40 days
  deployments  520 per cluster in 37 team namespaces, 1-20 replicas, rolled
               out every 1-10 days (new ReplicaSet, pod-template-hash,
               version); 20% scale +50% from 08:00 to 20:00 UTC daily (HPA);
               ~1/40 days random eviction per pod; 3% of pods relabelled
               mid-life (a second pod version); 25% with an istio-proxy sidecar
  statefulsets 40 per cluster, 1-5 replicas, rolled every 10-40 days
  cronjobs     30 per cluster: 2 every 5 min, 8 every 15 min, 12 hourly,
               5 every 6 h, 3 daily; each run is a pod living 20 s - 8 min

Deterministic (seeded). Writes straight into ClickHouse through
`clickhouse client` (no intermediate files).

  fleet.py --db ent_cat [--days 90] [--clusters 20] [--end 2026-09-27T00:00:00Z] [--create]
"""
import argparse, bisect, datetime, hashlib, os, random, subprocess, sys, time, uuid

HERE = os.path.dirname(os.path.abspath(__file__))
CH = os.environ.get("CH_CLIENT", "clickhouse")
PORT = os.environ.get("CH_PORT", "19000")

ap = argparse.ArgumentParser()
ap.add_argument("--db", default="ent_cat")
ap.add_argument("--days", type=float, default=90)
ap.add_argument("--clusters", type=int, default=20)
ap.add_argument("--nodes", type=int, default=200)
ap.add_argument("--deployments", type=int, default=520)
ap.add_argument("--end", default="2026-09-27T00:00:00Z")
ap.add_argument("--seed", type=int, default=7)
ap.add_argument("--create", action="store_true", help="create the database and the catalog tables first")
a = ap.parse_args()

T_END = int(datetime.datetime.fromisoformat(a.end.replace("Z", "+00:00")).timestamp() * 1000)
T0 = T_END - int(a.days * 86400_000)
OPEN = 4102444800000  # 2100-01-01: an open version
MIN, HOUR, DAY = 60_000, 3_600_000, 86_400_000
R = random.Random(a.seed)


def key(*parts):
    return int.from_bytes(hashlib.blake2b("\x00".join(map(str, parts)).encode(), digest_size=8).digest(), "little")


def uid(*parts):
    return str(uuid.UUID(bytes=hashlib.blake2b("\x00".join(map(str, parts)).encode(), digest_size=16).digest(), version=4))


def rfc3339(ms):
    return datetime.datetime.fromtimestamp(ms / 1000, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def m(d):  # ClickHouse Map literal inside TSV
    return "{" + ",".join(f"'{k}':'{v}'" for k, v in d.items()) + "}"


ALNUM = "bcdfghjklmnpqrstvwxz2456789"


def suffix(n, *parts):
    h = key(*parts)
    s = []
    for _ in range(n):
        s.append(ALNUM[h % len(ALNUM)])
        h //= len(ALNUM)
    return "".join(s)


class Sink:
    """One `clickhouse client` INSERT per table, fed TSV on stdin."""

    def __init__(self, table, cols, select):
        q = f"INSERT INTO {a.db}.{table} SELECT {select} FROM input('{cols}') FORMAT TSV"
        self.p = subprocess.Popen([CH, "client", "--port", PORT, "--query", q, "--max_memory_usage", "3000000000",
                                   "--max_insert_threads", "1"], stdin=subprocess.PIPE, text=True, bufsize=1 << 20)
        self.n = 0

    def row(self, *vals):
        self.p.stdin.write("\t".join(map(str, vals)) + "\n")
        self.n += 1

    def close(self):
        self.p.stdin.close()
        if self.p.wait() != 0:
            sys.exit("insert failed")


TS = "fromUnixTimestamp64Milli(vf), fromUnixTimestamp64Milli(vt)"
if a.create:
    subprocess.run([CH, "client", "--port", PORT, "--query", f"CREATE DATABASE IF NOT EXISTS {a.db}"], check=True)
    body = "\n".join(l for l in open(os.path.join(HERE, "..", "sql", "catalog.sql")).read().splitlines() if not l.lstrip().startswith("--"))
    for st in [x.strip() for x in body.replace("{db}", a.db).split(";\n") if x.strip()]:
        subprocess.run([CH, "client", "--port", PORT, "--query", st.rstrip(";")], check=True)

clusters = Sink("clusters", "k UInt64, a Map(String,String), vf Int64, vt Int64", f"k, a, {TS}")
nodes = Sink("nodes", "k UInt64, c UInt64, a Map(String,String), vf Int64, vt Int64", f"k, c, a, {TS}")
nss = Sink("namespaces", "k UInt64, c UInt64, a Map(String,String), vf Int64, vt Int64", f"k, c, a, {TS}")
wls = Sink("workloads", "k UInt64, c UInt64, n UInt64, kind String, name String, a Map(String,String), "
           "ct Array(Tuple(String,String,String)), res Map(String,String), w Float32, vf Int64, vt Int64",
           f"k, c, n, kind, name, a, ct, res, w, {TS}")
pods = Sink("pods", "k UInt64, u String, c UInt64, wl UInt64, nd UInt64, ns UInt64, a Map(String,String), vf Int64, vt Int64, ps Int64, pe Int64",
            f"k, u, c, wl, nd, ns, a, {TS}, fromUnixTimestamp64Milli(ps), fromUnixTimestamp64Milli(pe)")

REGIONS = [("us-east-1", "use1"), ("us-west-2", "usw2"), ("eu-west-1", "euw1"), ("ap-southeast-1", "apse1")]
ENVS = ["prod"] * 14 + ["staging"] * 3 + ["dev"] * 3
ACCOUNTS = {"prod": "412345678901", "staging": "512345678902", "dev": "612345678903"}
INSTANCE = ["m6i.4xlarge", "m6i.8xlarge", "c6i.8xlarge", "r6i.4xlarge", "m7g.4xlarge"]
TEAMS = ["payments", "checkout", "catalog", "search", "identity", "ads", "risk", "ledger", "growth", "mobile-api",
         "notifications", "pricing", "inventory", "shipping", "reviews", "recs", "media", "billing", "support",
         "analytics", "platform", "data-eng", "ml-serving", "partner-api", "fraud"]
WORDS = ["api", "worker", "gateway", "sync", "indexer", "scheduler", "cache", "router", "consumer", "exporter",
         "reconciler", "frontend", "backend", "admin", "stream", "batch", "webhook", "auth", "proxy", "store"]
LANGS = [("java", "1.42.1", "opentelemetry-java-instrumentation", "OpenJDK Runtime Environment"),
         ("go", "1.36.0", "opentelemetry", "go"),
         ("nodejs", "1.30.1", "opentelemetry", "nodejs"),
         ("python", "1.33.1", "opentelemetry", "cpython"),
         ("dotnet", "1.12.0", "opentelemetry", ".NET")]
COMPONENTS = ["api", "worker", "frontend", "backend", "cache", "database", "queue"]
CRON = [5] * 2 + [15] * 8 + [60] * 12 + [360] * 5 + [1440] * 3  # minutes

stats = dict(pods=0, pod_versions=0, workload_versions=0, nodes=0)


def versions(start, end, lo_days, hi_days, rnd):
    """Rollout boundaries: [(vstart, vend), ...] covering [start, end)."""
    out, t = [], start
    while t < end:
        nxt = t + int(rnd.uniform(lo_days, hi_days) * DAY)
        out.append((t, min(nxt, OPEN) if nxt < end else OPEN))
        t = nxt
    return out


t_start = time.time()
for c in range(a.clusters):
    rc = random.Random(a.seed * 1000 + c)
    env = ENVS[c % len(ENVS)]
    region, rcode = REGIONS[c % len(REGIONS)]
    cname = f"{env}-{rcode}-{c:02d}"
    cuid = uid("cluster", c)
    ck = key("cluster", cuid)
    clusters.row(ck, m({"k8s.cluster.name": cname, "k8s.cluster.uid": cuid, "cloud.provider": "aws", "cloud.platform": "aws_eks",
                        "cloud.region": region, "cloud.account.id": ACCOUNTS[env], "deployment.environment.name": env}), T0 - 400 * DAY, OPEN)
    # nodes: per slot, generations
    slot_gens = []  # per slot: (starts list, [(start, end, node_key)])
    for s in range(a.nodes):
        gens, t, g = [], T0 - int(rc.uniform(0, 20) * DAY), 0
        while t < T_END:
            life = int(rc.uniform(5, 25) * DAY)
            end = t + life if t + life < T_END else OPEN
            nuid = uid("node", c, s, g)
            nk = key("node", nuid)
            nname = f"ip-10-{c * 8 + s // 32}-{(s % 32) * 8 + g % 8}-{(g * 37 + s) % 250 + 2}.{region}.compute.internal"
            nodes.row(nk, ck, m({"k8s.node.name": nname, "k8s.node.uid": nuid, "host.name": nname, "host.id": "i-0" + uid("iid", c, s, g).replace("-", "")[:16],
                                 "host.type": INSTANCE[s % len(INSTANCE)], "cloud.availability.zone": region + "abc"[s % 3]}), t, end)
            stats["nodes"] += 1
            gens.append((t, end, nk))
            t, g = t + life, g + 1
        slot_gens.append(([x[0] for x in gens], gens))

    def node_at(slot, t):
        starts, gens = slot_gens[slot]
        i = max(0, bisect.bisect_right(starts, t) - 1)
        return gens[i]

    # namespaces
    ns_list = ["kube-system", "observability", "istio-system"] + [f"{TEAMS[i % len(TEAMS)]}-{['core', 'svc', 'jobs'][i // len(TEAMS)]}" for i in range(37)]
    nsk = {}
    for n in ns_list:
        nsk[n] = key("ns", cuid, n)
        nss.row(nsk[n], ck, m({"k8s.namespace.name": n}), T0 - 400 * DAY, OPEN)

    def emit_pod(wl_key, ns, podname, pod_uid, slot, start, end, attrs, relabel_p=0.0, rnd=rc):
        """A pod on `slot` from start to end; cut where its node goes away (the rest is rescheduled by the caller)."""
        gstart, gend, nk = node_at(slot, start)
        pend = min(end, gend)
        # a mid-life relabel: a second pod version
        cuts = [start, pend]
        if relabel_p and rnd.random() < relabel_p and pend - start > 2 * HOUR and pend <= T_END + DAY:
            cuts = [start, start + int((pend - start) * rnd.uniform(0.2, 0.8)), pend]
        elif relabel_p and rnd.random() < relabel_p and pend == OPEN:
            cuts = [start, start + int((T_END - start) * rnd.uniform(0.2, 0.9)), pend]
        base = dict(attrs, **{"k8s.pod.name": podname, "k8s.pod.uid": pod_uid, "k8s.pod.start_time": rfc3339(start)})
        for i in range(len(cuts) - 1):
            at = dict(base)
            if i > 0:
                at["k8s.pod.label.debug"] = "true"
            pods.row(key("podv", pod_uid, i), pod_uid, ck, wl_key, nk, nsk[ns], m(at), cuts[i], cuts[i + 1], start, pend)
            stats["pod_versions"] += 1
        stats["pods"] += 1
        return pend

    def run_replica(wl_key, ns, mkname, start, end, attrs, relabel_p, evict_days, rnd, slot=None):
        """One replica slot of a controller from start to end: successive pods (node loss, evictions)."""
        t, i = start, 0
        while t < end and t < T_END:
            sl = slot if slot is not None else rnd.randrange(a.nodes)
            e = end
            if evict_days:
                ev = t + int(rnd.expovariate(1 / evict_days) * DAY)
                if ev < e:
                    e = ev
            pod_uid = uid("pod", wl_key, start, i, t)
            pend = emit_pod(wl_key, ns, mkname(pod_uid), pod_uid, sl, t, e, attrs, relabel_p, rnd)
            if pend >= end or pend == OPEN:
                break
            t, i = pend + int(rnd.uniform(2, 40) * 1000), i + 1  # rescheduled a few seconds later

    def workload(kind, name, ns, team, lang, versions_list, containers_fn, extra_attrs, weight, residual_extra, per_version):
        """Emit each version of a workload; per_version(wl_key, vstart, vend, attrs, vi) makes its pods."""
        for vi, (vs, ve) in enumerate(versions_list):
            ver = f"{1 + vi // 20}.{vi % 20}.{key(name, vi) % 10}"
            wk = key("wl", cuid, ns, name, vi)
            lg = LANGS[lang]
            attrs = {f"k8s.{kind}.name": name, "service.name": name, "service.version": ver,
                     "k8s.pod.label.app.kubernetes.io/name": name, "k8s.pod.label.app.kubernetes.io/instance": f"{name}-{ns}",
                     "k8s.pod.label.app.kubernetes.io/version": ver, "k8s.pod.label.app.kubernetes.io/component": COMPONENTS[key(name) % len(COMPONENTS)],
                     "k8s.pod.label.app.kubernetes.io/part-of": team, "k8s.pod.label.app.kubernetes.io/managed-by": "Helm",
                     "k8s.pod.label.team": team}
            attrs.update(extra_attrs(vi))
            residual = {"telemetry.sdk.language": lg[0], "telemetry.sdk.name": lg[2], "telemetry.sdk.version": lg[1], "process.runtime.name": lg[3]}
            residual.update(residual_extra(vi))
            wls.row(wk, ck, nsk[ns], kind, name, m(attrs), "[" + ",".join(f"('{a_}','{b_}','{c_}')" for a_, b_, c_ in containers_fn(ver)) + "]",
                    m(residual), weight, vs, ve)
            stats["workload_versions"] += 1
            per_version(wk, vs, ve, attrs, vi)

    # daemonsets: one pod per node generation per version
    for di, (dname, dns) in enumerate([("kube-proxy", "kube-system"), ("aws-node", "kube-system"), ("otel-agent", "observability"), ("fluent-bit", "observability")]):
        vl = versions(T0 - int(rc.uniform(0, 30) * DAY), T_END, 14, 40, rc)

        def ds_pods(wk, vs, ve, attrs, vi, dname=dname, dns=dns):
            for s in range(a.nodes):
                t = max(vs, T0)
                while t < min(ve, T_END):
                    gs, ge, nk = node_at(s, t)
                    e = min(ve, ge)
                    pu = uid("dspod", wk, s, t)
                    emit_pod(wk, dns, f"{dname}-{suffix(5, pu)}", pu, s, t, e, {"k8s.pod.label.controller-revision-hash": suffix(10, wk)})
                    t = e
        workload("daemonset", dname, dns, "platform", 1, vl, lambda ver, dname=dname: [(dname, f"public.ecr.aws/eks/{dname}", "v" + ver)],
                 lambda vi, wk=None: {}, 0.3, lambda vi: {}, ds_pods)

    # deployments
    for d in range(a.deployments):
        ns = ns_list[3 + d % 37]
        team = ns.rsplit("-", 1)[0]
        name = f"{team}-{WORDS[(d // 37) % len(WORDS)]}" + (f"-{d // (37 * len(WORDS))}" if d >= 37 * len(WORDS) else "")
        replicas = rc.choice([1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 5, 6, 8, 12])
        hpa = rc.random() < 0.2
        sidecar = rc.random() < 0.25
        lang = key(name) % len(LANGS)
        vl = versions(T0 - int(rc.uniform(0, 10) * DAY), T_END, 1, 10, rc)
        weight = rc.paretovariate(1.2)
        custom = rc.random() < 0.15

        def cont(ver, name=name, team=team, sidecar=sidecar):
            cs = [(name, f"123456789012.dkr.ecr.us-east-1.amazonaws.com/{team}/{name}", ver)]
            if sidecar:
                cs.append(("istio-proxy", "docker.io/istio/proxyv2", "1.24.2"))
            return cs

        def dep_pods(wk, vs, ve, attrs, vi, name=name, ns=ns, replicas=replicas, hpa=hpa):
            pth = suffix(10, wk)
            rs = f"{name}-{pth}"
            pa = {"k8s.replicaset.name": rs, "k8s.pod.label.pod-template-hash": pth}
            s0 = max(vs, T0)
            for r in range(replicas):
                run_replica(wk, ns, lambda pu, rs=rs: f"{rs}-{suffix(5, pu)}", s0 + r * 15_000, ve + r * 15_000 if ve != OPEN else OPEN,
                            pa, 0.03, 40, rc)
            if hpa:  # extra replicas 08:00-20:00 UTC each day of this version
                extra = max(1, replicas // 2)
                day = (s0 // DAY) * DAY
                while day < min(ve, T_END):
                    us, ue = max(day + 8 * HOUR, s0), min(day + 20 * HOUR, ve)
                    if ue > us:
                        for r in range(extra):
                            run_replica(wk, ns, lambda pu, rs=rs: f"{rs}-{suffix(5, pu)}", us + r * 20_000, ue, pa, 0.0, 0, rc)
                    day += DAY

        workload("deployment", name, ns, team, lang, vl, cont,
                 lambda vi: {}, weight, (lambda vi, name=name: {"git.commit.sha": uid("sha", name, vi).replace("-", "")[:40]}) if custom else (lambda vi: {}), dep_pods)

    # statefulsets
    for s_ in range(40):
        ns = ns_list[3 + (s_ * 7) % 37]
        team = ns.rsplit("-", 1)[0]
        name = f"{team}-{['db', 'kafka', 'redis', 'zk', 'es'][s_ % 5]}" + (f"-{s_ // 5}" if s_ >= 5 else "")
        replicas = rc.choice([1, 3, 3, 5])
        vl = versions(T0 - int(rc.uniform(0, 30) * DAY), T_END, 10, 40, rc)

        def sts_pods(wk, vs, ve, attrs, vi, name=name, ns=ns, replicas=replicas):
            crh = suffix(10, wk)
            for r in range(replicas):
                run_replica(wk, ns, lambda pu, r=r, name=name: f"{name}-{r}", max(vs, T0) + (replicas - r) * 60_000, ve, {
                    "k8s.pod.label.controller-revision-hash": f"{name}-{crh}", "k8s.pod.label.statefulset.kubernetes.io/pod-name": f"{name}-{r}"},
                    0.0, 60, rc, slot=(key(name, r) + vi) % a.nodes)
        workload("statefulset", name, ns, team, 0, vl, lambda ver, name=name, team=team: [(name, f"123456789012.dkr.ecr.us-east-1.amazonaws.com/{team}/{name}", ver)],
                 lambda vi: {}, rc.uniform(0.5, 3), lambda vi: {}, sts_pods)

    # cronjobs
    for j, every in enumerate(CRON):
        ns = ns_list[3 + (j * 11) % 37]
        team = ns.rsplit("-", 1)[0]
        name = f"{team}-{['report', 'cleanup', 'sync', 'backup', 'reindex', 'rollup'][j % 6]}-{j}"
        vl = versions(T0 - int(rc.uniform(0, 14) * DAY), T_END, 7, 30, rc)
        dur = rc.uniform(20, 480) * 1000

        def cron_pods(wk, vs, ve, attrs, vi, name=name, ns=ns, every=every, dur=dur):
            t = ((max(vs, T0) // (every * MIN)) + 1) * every * MIN
            while t < min(ve, T_END):
                job = f"{name}-{t // MIN}"
                pu = uid("job", wk, t)
                d_ = int(dur * rc.uniform(0.5, 1.5))
                emit_pod(wk, ns, f"{job}-{suffix(5, pu)}", pu, rc.randrange(a.nodes), t + 1000, t + 1000 + d_,
                         {"k8s.job.name": job, "k8s.pod.label.batch.kubernetes.io/job-name": job})
                t += every * MIN
        workload("cronjob", name, ns, team, key(name) % len(LANGS), vl,
                 lambda ver, name=name, team=team: [(name, f"123456789012.dkr.ecr.us-east-1.amazonaws.com/{team}/{name}", ver)],
                 lambda vi: {}, 0.2, lambda vi: {}, cron_pods)
    print(f"cluster {c} {cname}: {stats} {time.time() - t_start:.0f}s", flush=True)

for s in (clusters, nodes, nss, wls, pods):
    s.close()
print("done", stats, f"{time.time() - t_start:.0f}s")
