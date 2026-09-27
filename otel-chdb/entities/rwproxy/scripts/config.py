#!/usr/bin/env python3
"""Write the proxy's configuration from the catalog.

For every covered key, the catalog's value for one resource as one
expression over the normalized dictionaries (../../sql/dictionaries.sql):
the level that holds the key (cluster, node, namespace, workload, pod,
container), found from the catalog tables. A key held by more than one level
gets no expression (the proxy then leaves it to the ALIAS column). Each
expression is then checked against the ALIAS column on every row of the
variant-c tables: `if(mapContains(ResourceResidual, k), ResourceResidual[k],
<expression>) = ResourceAttributes[k]` must hold for all rows, or the key is
dropped.

  config.py [--cat rw_cat] [--db rw_c] [--mode exact] [--listen 127.0.0.1:18125] [--out ../rwproxy.json]
"""
import argparse, json, os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "..", "scripts"))
import chlib as c  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--cat", default="rw_cat")
ap.add_argument("--db", default="rw_c")
ap.add_argument("--mode", default="exact")
ap.add_argument("--listen", default="127.0.0.1:18125")
ap.add_argument("--upstream", default=c.CH)
ap.add_argument("--log", default="")
ap.add_argument("--out", default=os.path.join(HERE, "..", "rwproxy.json"))
ap.add_argument("--no-verify", action="store_true")
a = ap.parse_args()
CAT = a.cat

RID = "{rid}"
PK = f"dictGet('{CAT}.d_res', 'pod_key', {RID})"


def pod(attr):
    return f"dictGet('{CAT}.d_pod', '{attr}', {PK})"


def sq(s):
    return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"


def level(d, key_attr, k):
    return f"JSONExtractString(dictGet('{CAT}.{d}', 'attrs', {pod(key_attr)}), {sq(k)})"


def keys(sql):
    return [x for x in c.one(sql).splitlines() if x]


levels = {}  # key -> [expression per level that holds it]
for d, key_attr, table in (("d_cluster", "cluster_key", "clusters"), ("d_node", "node_key", "nodes"),
                           ("d_ns", "ns_key", "namespaces"), ("d_wl", "wl_key", "workloads")):
    for k in keys(f"SELECT DISTINCT arrayJoin(mapKeys(attrs)) FROM {CAT}.{table} ORDER BY 1"):
        levels.setdefault(k, []).append(level(d, key_attr, k))
TYPED = {"k8s.pod.name": pod("name"), "k8s.pod.uid": f"toString({pod('uid')})",
         "k8s.pod.start_time": f"formatDateTime({pod('start')}, '%Y-%m-%dT%H:%i:%SZ', 'UTC')"}
for k in keys(f"SELECT DISTINCT arrayJoin(mapKeys(attrs)) FROM {CAT}.pods ORDER BY 1"):
    levels.setdefault(k, []).append(TYPED.get(k) or f"JSONExtractString({pod('extra')}, {sq(k)})")
for k in ("k8s.container.name", "container.image.name", "container.image.tag"):
    levels.setdefault(k, []).append(
        f"JSONExtract(dictGet('{CAT}.d_wl', 'containers', {pod('wl_key')}), 'Array(Map(String, String))')"
        f"[dictGet('{CAT}.d_res', 'ct', {RID})][{sq(k)}]")

values, ambiguous = {}, []
for k, exprs in sorted(levels.items()):
    if len(exprs) != 1:
        ambiguous.append(k)
        continue
    values[k] = f"if(dictHas('{CAT}.d_res', {RID}), {exprs[0]}, '')"

covered = sorted(set(levels) | set(keys(f"SELECT DISTINCT Key FROM {CAT}.resource_kv ORDER BY 1")))

dropped = {}
if not a.no_verify:
    for sig in ("traces", "logs"):
        ks = sorted(values)
        checks = ", ".join(
            f"countIf(if(mapContains(ResourceResidual, {sq(k)}), ResourceResidual[{sq(k)}], {values[k].replace(RID, 'resource_id')}) "
            f"!= ra[{sq(k)}]) AS `{k}`" for k in ks)
        r = c.rows(f"SELECT count() AS rows, {checks} FROM (SELECT resource_id, ResourceResidual, ResourceAttributes AS ra FROM {a.db}.otel_{sig})",
                   max_execution_time=600)[0]
        print(sig, r.pop("rows"), "rows checked", flush=True)
        for k, n in r.items():
            if int(n):
                dropped[k] = dropped.get(k, 0) + int(n)
    for k in dropped:
        values.pop(k, None)

cfg = dict(listen=a.listen, upstream=a.upstream, mode=a.mode,
           tables=[f"{a.db}.otel_traces", f"{a.db}.otel_logs"],
           column="ResourceAttributes", residual="ResourceResidual", resource_id="resource_id",
           kv=f"{CAT}.resource_kv", values=values, covered=covered,
           refresh_covered_sql=f"SELECT DISTINCT Key FROM {CAT}.resource_kv", refresh_seconds=30,
           materialized={}, fallback=True, log=a.log)
with open(a.out, "w") as f:
    json.dump(cfg, f, indent=1, sort_keys=True)
    f.write("\n")
print(f"{len(values)} value expressions, {len(covered)} covered keys, ambiguous {ambiguous}, dropped {dropped} -> {a.out}")
