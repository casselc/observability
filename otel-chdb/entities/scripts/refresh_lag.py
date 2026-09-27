#!/usr/bin/env python3
"""How long a resource the controller has just written takes to become
visible through the views (dictionary LIFETIME(MIN 30 MAX 60), no manual
reload): write a res_index row for a new id, poll dictHas every 0.5 s.

  refresh_lag.py [--n 5] [--out ../results/refresh_lag.jsonl]
"""
import argparse, json, random, time
import chlib as c

ap = argparse.ArgumentParser()
ap.add_argument("--n", type=int, default=5)
ap.add_argument("--out", default=f"{c.HERE}/../results/refresh_lag.jsonl")
a = ap.parse_args()
pk = c.one("SELECT pod_key FROM ent_cat.res_index LIMIT 1")
for i in range(a.n):
    rid = random.getrandbits(63)
    t0 = time.time()
    c.q(f"INSERT INTO ent_cat.res_index VALUES ({rid}, {pk}, 1, now64(3), now64(3) + 60)")
    while int(c.one(f"SELECT dictHas('ent_cat.d_res', toUInt64({rid}))")) == 0:
        time.sleep(0.5)
    lag = round(time.time() - t0, 1)
    rec = dict(i=i, lag_s=lag, load=c.load(), ts=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    print(rec, flush=True)
    with open(a.out, "a") as f:
        f.write(json.dumps(rec) + "\n")
    c.q(f"DELETE FROM ent_cat.res_index WHERE resource_id = {rid}")
    time.sleep(random.uniform(0, 20))
