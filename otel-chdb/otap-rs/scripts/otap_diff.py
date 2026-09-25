#!/usr/bin/env python3
"""OTAP input vs OTLP input, as row sets with every map sorted: the objects
of scripts/otap_e2e.sh (paths otapgrpc and direct), slot 0 (testgen).
  otap_diff.py RUN traces|logs"""
import requests,sys
CH="http://127.0.0.1:18123/"
def q(s):
    r=requests.post(CH,data=s.encode())
    if r.status_code!=200: raise SystemExit(r.text[:500])
    return r.text.strip()
run,sig=sys.argv[1],sys.argv[2]
import os
base=f"http://127.0.0.1:18333/otel/{os.environ.get('S3_PREFIX', 'otap-rs-edge')}/corr/{run}"
src=lambda p: f"s3('{base}/{p}/{sig}/*/00000000000000000000.parquet','otel','otelsecret','Parquet')"
desc=[l.split('\t')[:2] for l in q(f"DESCRIBE {src('direct')} FORMAT TSV").splitlines()]
env={'producer_id','producer_epoch','batch_id','received_at','row_ordinal','schema_version'}
exprs=[]
for c,t in desc:
    if c in env: continue
    if t.startswith('Map'): exprs.append(f"mapSort(`{c}`)")
    elif t.startswith('Array(Map'): exprs.append(f"arrayMap(m -> mapSort(m), `{c}`)")
    else: exprs.append(f"`{c}`")
e=", ".join(exprs)
for a,b in (("otapgrpc","direct"),("direct","otapgrpc")):
    print(sig, f"rows in {a} not in {b} (maps sorted, row order ignored):", q(f"SELECT count() FROM (SELECT {e} FROM {src(a)} EXCEPT ALL SELECT {e} FROM {src(b)})"))
