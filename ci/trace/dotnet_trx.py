#!/usr/bin/env python3
"""Trace records for the .NET tests (otel-chdb/forwarder), from claims and a TRX file.

xUnit does not give a test its own outcome, so the .NET helper
(forwarder/tests/Oscope.Forwarder.Tests/OscopeTrace.cs) only writes a claim when a tagged
test starts: {ids, technique, class, func, file}. After `dotnet test --logger trx`, this
joins each claim with the TRX results of that class and method and appends one record per
claim to --out, with the fields tracetag (Go), oscope_trace (Rust) and the node reporter
write. A theory's cases are one record: failed if any case failed. A claim with no result
is recorded as failed: an unrun test is not evidence (STPA.md CAST rows 41, 43, 53). The
claims file is consumed, so a second run in the same job does not record them twice.

    dotnet_trx.py --trx unit.trx --claims "$OSCOPE_TRACE_OUT.dotnet-claims.jsonl" --out "$OSCOPE_TRACE_OUT"

Standard library only.
"""
import argparse
import json
import os
import subprocess
import sys
import xml.etree.ElementTree as ET

NS = {'t': 'http://microsoft.com/schemas/VisualStudio/TeamTest/2010'}


def commit():
    if os.environ.get('GITHUB_SHA'):
        return os.environ['GITHUB_SHA']
    try:
        return subprocess.run(['git', 'rev-parse', 'HEAD'], capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return ''


def outcomes(trx):
    """(class, method) -> 'passed' | 'failed' | 'skipped' (any failure wins)."""
    root = ET.parse(trx).getroot()
    by_id = {}
    for ut in root.iterfind('.//t:TestDefinitions/t:UnitTest', NS):
        tm = ut.find('t:TestMethod', NS)
        if tm is not None:
            by_id[ut.get('id')] = (tm.get('className', ''), tm.get('name', ''))
    out = {}
    for r in root.iterfind('.//t:Results/t:UnitTestResult', NS):
        key = by_id.get(r.get('testId'))
        if key is None:
            continue
        o = {'Passed': 'passed', 'NotExecuted': 'skipped'}.get(r.get('outcome', ''), 'failed')
        prev = out.get(key)
        if prev == 'failed' or (prev == 'passed' and o == 'skipped'):
            continue
        out[key] = o
    return out


def records(claims, results, env=os.environ):
    sha = commit()
    recs = []
    for c in claims:
        cls, func = c.get('class', ''), c.get('func', '')
        o = results.get((cls, func))
        if o is None:  # a TRX may name the class without its namespace
            o = next((v for (k, f), v in results.items() if f == func and (k == cls or cls.endswith('.' + k))), 'failed')
        recs.append({'ids': c.get('ids', []), 'technique': c.get('technique', ''), 'test': f'{cls}.{func}', 'func': func,
                     'package': cls.rsplit('.', 1)[0] if '.' in cls else cls, 'file': c.get('file', ''), 'lang': 'dotnet',
                     'commit': sha, 'job': env.get('GITHUB_JOB', ''), 'outcome': o})
    return recs


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument('--trx', required=True)
    ap.add_argument('--claims', required=True)
    ap.add_argument('--out', required=True)
    a = ap.parse_args(argv)
    if not os.path.exists(a.claims):
        print(f'dotnet_trx: no claims at {a.claims} (no tagged test ran)')
        return 0
    taken = f'{a.claims}.{os.getpid()}'
    os.replace(a.claims, taken)
    with open(taken, encoding='utf-8') as f:
        claims = [json.loads(l) for l in f if l.strip()]
    try:
        results = outcomes(a.trx)
    except (OSError, ET.ParseError) as e:
        print(f'::error::dotnet_trx: {a.trx}: {e}; every claim is recorded as failed')
        results = {}
    # One record per (file, func): a test run twice (a theory's cases) claimed once per case.
    seen, recs = set(), []
    for r in records(claims, results):
        k = (r['file'], r['func'])
        if k not in seen:
            seen.add(k)
            recs.append(r)
    os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
    with open(a.out, 'a', encoding='utf-8') as f:
        for r in recs:
            f.write(json.dumps(r, sort_keys=True) + '\n')
    bad = [r for r in recs if r['outcome'] != 'passed']
    print(f'dotnet_trx: {len(recs)} records, {len(bad)} not passed')
    for r in bad:
        print(f'  {r["outcome"]}: {r["test"]} ({r["file"]})')
    return 0


if __name__ == '__main__':
    sys.exit(main())
