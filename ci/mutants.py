#!/usr/bin/env python3
"""Surviving mutants against a baseline of accepted survivors (nightly job `mutants`).

    ci/mutants.py rust  MUTANTS_OUT_DIR  > survivors-rust.txt   # cargo-mutants' mutants.out
    ci/mutants.py go    GREMLINS_JSON MODULE_DIR > survivors-go.txt
    ci/mutants.py check ci/mutants-baseline.txt survivors-*.txt [--summary FILE]

A survivor is a mutant no test caught: missed or timed out (cargo-mutants), lived or not
covered (gremlins). Each is written as a key that does not move when lines do:

    rust <file>: <cargo-mutants' description>          e.g. rust src/consumer/gc.rs: replace < with <= in doomed
    go <module-relative file> <func> <MUTATOR> #n      e.g. go commit/lane.go (*Lane).Append CONDITIONALS_NEGATION #2

(#n numbers the equal keys of a file in line order.) `check` fails when a survivor is not in
the baseline (a regression: a test lost its teeth, or new code has none), and reports
baseline entries that no longer survive (remove them). Every baseline line is
`key | reason`; a line without a reason is an error, as a known-gaps line without an owner is.
"""
import collections
import json
import os
import re
import sys


def keyed(pairs):
    """[(base key, sort position)] -> keys with #n for repeats, in position order."""
    seen = collections.Counter()
    out = []
    for base, _ in sorted(pairs, key=lambda p: p[1]):
        seen[base] += 1
        out.append(f'{base} #{seen[base]}' if seen[base] > 1 or base.startswith('go ') else base)
    return out


def rust(outdir):
    pairs = []
    for name in ('missed.txt', 'timeout.txt'):
        p = os.path.join(outdir, name)
        if not os.path.exists(p):
            continue
        for n, line in enumerate(open(p, encoding='utf-8')):
            line = line.strip()
            m = re.match(r'^(\S+?):(\d+):(\d+): (.*)$', line)
            if m:
                pairs.append((f'rust {m.group(1)}: {m.group(4)}', (m.group(1), int(m.group(2)), int(m.group(3)))))
            elif line:
                pairs.append((f'rust {line}', (line, 0, n)))
    return keyed(pairs)


FUNC = re.compile(r'^func\s+(?:\((?:\w+\s+)?(\*?\w+)(?:\[[^\]]*\])?\)\s*)?(\w+)')


def enclosing(path, line):
    name = '<top>'
    try:
        lines = open(path, encoding='utf-8').read().split('\n')
    except OSError:
        return name
    for l in lines[:line]:
        m = FUNC.match(l)
        if m:
            name = f'({m.group(1)}).{m.group(2)}' if m.group(1) else m.group(2)
    return name


def go(report, moddir):
    d = json.load(open(report, encoding='utf-8'))
    pairs = []
    for f in d.get('files', []):
        fn = f.get('file_name', '')
        # gremlins names files relative to the package it mutated (moddir, e.g. ./commit);
        # the key carries that path from where it ran, never the runner's absolute one
        path = fn if os.path.isabs(fn) else os.path.join(moddir, fn)
        fn = os.path.normpath(os.path.relpath(path) if os.path.isabs(path) else path)
        for m in f.get('mutations', []):
            if m.get('status') not in ('LIVED', 'NOT COVERED'):
                continue
            ln = int(m.get('line', 0))
            func = enclosing(fn, ln)
            pairs.append((f'go {fn} {func} {m.get("type")}', (fn, ln, int(m.get('column', 0)))))
    return keyed(pairs)


def check(baseline, files, summary):
    base, bad = {}, []
    for n, l in enumerate(open(baseline, encoding='utf-8'), 1):
        l = l.rstrip('\n')
        if not l.strip() or l.lstrip().startswith('#'):
            continue
        k, sep, reason = l.partition(' | ')
        if not sep or not reason.strip():
            bad.append(f'{baseline}:{n}: no reason: {l}')
        base[k.strip()] = reason.strip()
    surv = []
    for f in files:
        surv += [l.strip() for l in open(f, encoding='utf-8') if l.strip()]
    new = [s for s in surv if s not in base]
    gone = [k for k in base if k not in set(surv)]
    out = [f'## Mutation testing: {len(surv)} surviving mutants, {len(surv) - len(new)} accepted in ci/mutants-baseline.txt, '
           f'{len(new)} new', '']
    if new:
        out += ['### New survivors (not in the baseline): a test to kill each, or a baseline line with its reason', '']
        out += [f'- `{s}`' for s in new] + ['']
    if gone:
        out += ['### Baseline entries no longer surviving (remove them)', ''] + [f'- `{k}`' for k in gone] + ['']
    if bad:
        out += ['### Baseline lines without a reason', ''] + [f'- {b}' for b in bad] + ['']
    out += ['### Accepted survivors', ''] + [f'- `{s}`: {base[s]}' for s in surv if s in base]
    text = '\n'.join(out) + '\n'
    print(text)
    if summary:
        with open(summary, 'a', encoding='utf-8') as fh:
            fh.write(text)
    for s in new:
        print(f'::error::surviving mutant not in ci/mutants-baseline.txt: {s}')
    for b in bad:
        print(f'::error::{b}')
    for k in gone:
        print(f'::warning::no longer survives, remove from ci/mutants-baseline.txt: {k}')
    return 1 if new or bad else 0


def main(a):
    if a[:1] == ['rust'] and len(a) == 2:
        print('\n'.join(rust(a[1])))
        return 0
    if a[:1] == ['go'] and len(a) == 3:
        print('\n'.join(go(a[1], a[2])))
        return 0
    if a[:1] == ['check'] and len(a) >= 3:
        summary = None
        if '--summary' in a:
            i = a.index('--summary')
            summary = a[i + 1]
            a = a[:i] + a[i + 2:]
        return check(a[1], a[2:], summary)
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
