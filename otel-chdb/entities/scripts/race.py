#!/usr/bin/env python3
"""The race cases, on the loaded (b2) database:

1. grace:    rows the edge sent inside the grace window (the whole covered set
             in ResourceResidual): how many, and that they are exact while
             their resource is still unknown to the catalog;
2. late:     rows stamped after their pod's deletion (a terminating pod's last
             logs): content-addressed ids stay in the dictionary, so they
             resolve; an SCD2 entity lookup by (entity, Timestamp) would miss
             them (counted against res_index validity);
3. withheld: resources the controller never wrote. Before: their rows show
             only the residual (conformance counts them). Catch-up: the
             controller's rows land (pods + res_index), and the dictionaries
             pick them up on their own LIFETIME (MIN 30 MAX 60) without a
             manual reload; the time until the view is exact is measured.

  race.py [--db ent_b2] [--a ent_a] [--out ../results/race.jsonl]
"""
import argparse, json, subprocess, time
import chlib as c

ap = argparse.ArgumentParser()
ap.add_argument("--db", default="ent_b2")
ap.add_argument("--a", default="ent_a")
ap.add_argument("--out", default=f"{c.HERE}/../results/race.jsonl")
a = ap.parse_args()
CAT = "ent_cat"
rec = dict(ts=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), load=c.load())
for sig in ("traces", "logs"):
    t = f"{a.db}.otel_{sig}_rid"
    r = c.rows(f"""SELECT count() AS rows,
        countIf(mapContains(ResourceResidual, 'k8s.pod.uid')) AS grace_rows,
        countIf(mapContains(ResourceResidual, 'k8s.pod.uid') AND NOT dictHas('{CAT}.d_res', resource_id)) AS grace_rows_unknown,
        countIf(NOT dictHas('{CAT}.d_res', resource_id)) AS unknown_rows,
        countIf(NOT dictHas('{CAT}.d_res', resource_id) AND NOT mapContains(ResourceResidual, 'k8s.pod.uid')) AS unknown_rows_degraded,
        uniqExactIf(resource_id, NOT dictHas('{CAT}.d_res', resource_id)) AS unknown_resources
      FROM {t}""")[0]
    # late rows: Timestamp at or after the resource version's valid_to (res_index + the withheld ones)
    late = c.rows(f"""SELECT countIf(t.Timestamp >= v.valid_to) AS after_version_end,
        countIf(t.Timestamp >= v.valid_to AND dictHas('{CAT}.d_res', t.resource_id)) AS after_end_resolved
      FROM {t} AS t INNER JOIN (SELECT resource_id, valid_to FROM {CAT}.res_index UNION ALL SELECT resource_id, valid_to FROM ent_src.res_index_withheld) AS v
      ON v.resource_id = t.resource_id""")[0]
    r.update(late)
    rec[sig] = r
print(json.dumps(rec), flush=True)

# 3. catch-up: the controller writes the withheld pods; no manual reload
t0 = time.time()
c.q(f"INSERT INTO {CAT}.pods SELECT * FROM ent_src.pods_withheld")
c.q(f"INSERT INTO {CAT}.res_index SELECT * FROM ent_src.res_index_withheld")
rec["catchup_written_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
while True:
    unk = int(c.one(f"SELECT count() FROM {a.db}.otel_logs_rid WHERE NOT dictHas('{CAT}.d_res', resource_id) AND NOT mapContains(ResourceResidual, 'k8s.pod.uid')"))
    pod_ok = int(c.one(f"SELECT count() FROM ent_src.pods_withheld WHERE dictHas('{CAT}.d_pod', pod_key)"))
    if unk == 0 and pod_ok == int(c.one("SELECT count() FROM ent_src.pods_withheld")):
        break
    if time.time() - t0 > 300:
        break
    time.sleep(2)
rec["catchup_s"] = round(time.time() - t0, 1)
rec["dict_update"] = c.rows(f"SELECT name, last_successful_update_time, loading_duration FROM system.dictionaries WHERE database = '{CAT}' ORDER BY name")
print(json.dumps(rec), flush=True)
with open(a.out, "a") as f:
    f.write(json.dumps(rec) + "\n")
for v, db in (("b2", a.db), ("c", a.db)):
    subprocess.run(["python3", f"{c.HERE}/conformance.py", v, db, "--a", a.a, "--label", "after catch-up"], check=True)
