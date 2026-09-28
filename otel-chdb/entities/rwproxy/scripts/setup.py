#!/usr/bin/env python3
"""Rebuild the entities spike's data for the rewriting proxy, under databases
rw_* (the entities scripts hard-code ent_*; this reuses them with the catalog
name patched):

  rw_cat   the catalog (fleet.py), res_index, the normalized dictionaries,
           resource_kv (what a rewritten filter looks up)
  rw_src   staging telemetry (telemetry.py), and the withheld pods
  rw_a     variant a: ClickStack 2.39.1 tables (consumer DDL at HEAD)
  rw_c     variant c: resource_id + ResourceResidual, ResourceAttributes an
           ALIAS column on the table; the catalog's discovery rows in its
           key-value rollups

Steps (each idempotent enough to re-run on its own):

  setup.py fleet       fleet.py --db rw_cat --days DAYS --create
  setup.py telemetry   telemetry.py --cat rw_cat --db rw_src
  setup.py index       res_index for every pod version; withhold 1 in 200 pods
                       (the controller never wrote them) into rw_src.*_withheld
  setup.py dicts       sql/dictionaries.sql on rw_cat
  setup.py load        load.py a,c --prefix rw_ --src rw_src --objects 60
  setup.py kv          resource_kv + the rollup's ResourceAttributes rows
                       (discovery.py's job, from what the catalog knows now)
  setup.py race        also take the grace-window resources out of the catalog
                       (the controller hasn't written a brand-new pod yet)
  setup.py catchup     the controller catches up: withheld pods back into the
                       catalog, dictionaries reloaded, kv rebuilt
  setup.py withhold    the reverse of catchup (back to the race state)
  setup.py drop        drop rw_* databases
"""
import os, subprocess, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
ENT = os.path.abspath(os.path.join(HERE, "..", "..", "scripts"))
sys.path.insert(0, ENT)
import chlib as c  # noqa: E402

CAT, SRC = "rw_cat", "rw_src"
DAYS = os.environ.get("DAYS", "2")
WITHHELD = 200


def py(script, *args):
    t = time.time()
    subprocess.run([sys.executable, os.path.join(ENT, script), *args], check=True, cwd=ENT)
    print(f"{script} {' '.join(args)}: {time.time() - t:.0f}s", flush=True)


def index():
    st = c.statements(f"{c.SQL}/dictionaries.sql", db=CAT)
    c.q(st[0])
    c.q(f"TRUNCATE TABLE {CAT}.res_index")
    c.q(c.statements(f"{c.SQL}/resources.sql", db=CAT, where="1")[1], max_threads=2)
    w = f"cityHash64(pod_uid) % {WITHHELD} = 0"
    for t in ("pods_withheld", "res_index_withheld"):
        c.q(f"DROP TABLE IF EXISTS {SRC}.{t} SYNC")
    c.q(f"CREATE TABLE {SRC}.pods_withheld ENGINE = MergeTree ORDER BY pod_key AS SELECT * FROM {CAT}.pods WHERE {w}")
    c.q(f"CREATE TABLE {SRC}.res_index_withheld ENGINE = MergeTree ORDER BY resource_id AS "
        f"SELECT * FROM {CAT}.res_index WHERE pod_key IN (SELECT pod_key FROM {SRC}.pods_withheld)")
    withhold()
    print(c.one(f"SELECT (SELECT count() FROM {CAT}.res_index), (SELECT count() FROM {SRC}.res_index_withheld), "
                f"(SELECT count() FROM {SRC}.pods_withheld)"))


def withhold():
    c.q(f"DELETE FROM {CAT}.pods WHERE pod_key IN (SELECT pod_key FROM {SRC}.pods_withheld)", mutations_sync=2)
    c.q(f"DELETE FROM {CAT}.res_index WHERE resource_id IN (SELECT resource_id FROM {SRC}.res_index_withheld)", mutations_sync=2)
    if exists(SRC, "res_index_grace"):
        c.q(f"DELETE FROM {CAT}.res_index WHERE resource_id IN (SELECT resource_id FROM {SRC}.res_index_grace)", mutations_sync=2)
    reload()


def race():
    """The grace race: resources whose rows carry the covered set in the
    residual (the edge's first sightings) are new, so the controller hasn't
    written them yet either. Take them out of the catalog too (on top of the
    1-in-200 withheld pods), so the race state has rows of both kinds."""
    c.q(f"DROP TABLE IF EXISTS {SRC}.res_index_grace SYNC")
    grace = " UNION ALL ".join(f"SELECT resource_id FROM rw_c.otel_{s} WHERE mapContains(ResourceResidual, 'k8s.pod.uid')" for s in ("traces", "logs"))
    c.q(f"CREATE TABLE {SRC}.res_index_grace ENGINE = MergeTree ORDER BY resource_id AS "
        f"SELECT * FROM {CAT}.res_index WHERE resource_id IN ({grace})")
    print("grace resources", c.one(f"SELECT count() FROM {SRC}.res_index_grace"))
    withhold()


def catchup():
    c.q(f"INSERT INTO {CAT}.pods SELECT * FROM {SRC}.pods_withheld")
    c.q(f"INSERT INTO {CAT}.res_index SELECT * FROM {SRC}.res_index_withheld")
    if exists(SRC, "res_index_grace"):
        c.q(f"INSERT INTO {CAT}.res_index SELECT * FROM {SRC}.res_index_grace")
    reload()


def exists(db, t):
    return c.one(f"SELECT count() FROM system.tables WHERE database = '{db}' AND name = '{t}'") == "1"


def reload():
    for d in ("d_res", "d_pod", "d_wl", "d_node", "d_ns", "d_cluster"):
        if c.one(f"SELECT count() FROM system.dictionaries WHERE database = '{CAT}' AND name = '{d}'") == "1":
            c.q(f"SYSTEM RELOAD DICTIONARY {CAT}.{d}")
    if c.one(f"SELECT count() FROM system.tables WHERE database = '{CAT}' AND name = 'resource_kv'") == "1":
        kv()


def dicts():
    for s in c.statements(f"{c.SQL}/dictionaries.sql", db=CAT):
        c.q(s)
    reload()


def load():
    import schemas  # the catalog name is baked into c's ALIAS expression
    import load as L
    schemas.CAT = CAT
    sys.argv = ["load.py", "a,c", "--prefix", "rw_", "--src", SRC, "--objects", "60"]
    L.main()


def kv():
    """resource_kv and the rollup rows from what the catalog knows (d_res's
    source, res_index), as discovery.py does: the flat covered set of a known
    resource is resources.attrs (controller rows) or res_all (withheld ones)."""
    flat = (f"(SELECT resource_id, attrs, valid_from, valid_to FROM {CAT}.resources WHERE resource_id IN (SELECT resource_id FROM {CAT}.res_index) "
            f"UNION ALL SELECT resource_id, attrs, valid_from, valid_to FROM {SRC}.res_all WHERE resource_id IN (SELECT resource_id FROM {CAT}.res_index) "
            f"AND resource_id NOT IN (SELECT resource_id FROM {CAT}.resources))")
    # cluster and namespace: the query service serves resource_kv scoped by
    # them (DECISIONS.md D33); rwproxy reads only Key, Value, resource_id
    c.q(f"CREATE TABLE IF NOT EXISTS {CAT}.resource_kv (Key LowCardinality(String), Value String, resource_id UInt64, "
        f"cluster LowCardinality(String), namespace LowCardinality(String)) "
        f"ENGINE = ReplacingMergeTree ORDER BY (Key, Value, resource_id)")
    c.q(f"TRUNCATE TABLE {CAT}.resource_kv")
    c.q(f"INSERT INTO {CAT}.resource_kv SELECT kv.1, kv.2, resource_id, attrs['k8s.cluster.name'], attrs['k8s.namespace.name'] "
        f"FROM {flat} ARRAY JOIN CAST(attrs, 'Array(Tuple(String, String))') AS kv")
    c.q(f"OPTIMIZE TABLE {CAT}.resource_kv FINAL")
    w1 = "toDateTime('2026-09-27 00:00:00', 'UTC')"
    w0 = f"({w1} - toIntervalHour(3))"
    buckets = (f"SELECT toStartOfFifteenMinutes(b) AS Timestamp, 'ResourceAttributes' AS ColumnIdentifier, kv.1 AS Key, kv.2 AS Value, count() AS count "
               f"FROM {flat} ARRAY JOIN arrayMap(i -> toDateTime({w0}) + toIntervalMinute(15 * i), range(12)) AS b "
               f"ARRAY JOIN CAST(attrs, 'Array(Tuple(String, String))') AS kv "
               f"WHERE valid_from < b + toIntervalMinute(15) AND valid_to > b GROUP BY Timestamp, Key, Value")
    for sig in ("traces", "logs"):
        if c.one(f"SELECT count() FROM system.tables WHERE database = 'rw_c' AND name = 'resource_kv_in_{sig}'") != "1":
            continue
        c.q(f"DELETE FROM rw_c.otel_{sig}_kv_rollup_15m WHERE ColumnIdentifier = 'ResourceAttributes'", mutations_sync=2)
        c.q(f"INSERT INTO rw_c.resource_kv_in_{sig} {buckets}", max_bytes_before_external_group_by=1_000_000_000)
    print("resource_kv", c.one(f"SELECT count(), uniqExact(resource_id) FROM {CAT}.resource_kv"), flush=True)


def drop():
    for db in ("rw_a", "rw_c", SRC, CAT, "ent_scratch"):
        c.q(f"DROP DATABASE IF EXISTS {db} SYNC")


if __name__ == "__main__":
    for step in sys.argv[1:]:
        t = time.time()
        if step == "fleet":
            py("fleet.py", "--db", CAT, "--days", DAYS, "--create")
        elif step == "telemetry":
            py("telemetry.py", "--cat", CAT, "--db", SRC)
        else:
            globals()[step]()
        print(f"== {step} {time.time() - t:.0f}s load {c.load()} mem {c.mem_available_gb():.1f} GB disk {c.disk_free_gb():.1f} GB", flush=True)
