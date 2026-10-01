#!/usr/bin/env python3
"""Requirements-to-test traceability from runtime evidence (ci/README.md, "Traceability").

A static tag says only that someone meant a test to cover an item. Skipped
or unrun tests are unknowns (STPA.md CAST rows 41, 43, 45, 46, 53), so the
evidence here is a record written by the tagged test itself when it ran, with
its outcome, at this commit. This script:

  catalog   parses every STPA ID (losses, hazards, constraints, UCAs, loss
            scenarios, SEC, TM, requirements, CAST rows) from otel-chdb/STPA.md
            and the STPA sections of research/{langfuse,entra-ingress,grants}.md,
            the technique codes of VERIFICATION.md §1 and its required
            (hazard x technique) cells of §3; fails on an ID defined twice with
            different meanings unless ci/trace/same-meaning.txt says they agree.
  scan      lists every tag in source: tracetag.Covers (Go), oscope_trace::covers
            (Rust), covers(t, ...) (node:test), OscopeTrace.Covers (.NET, whose
            records ci/trace/dotnet_trx.py writes from the TRX), the model mapping
            ci/trace/models.txt, and `trace.py record` steps in the workflows.
  record    appends one record (a CI step that is evidence, such as a lint).
  model     turns a Quint script's report into records, per ci/trace/models.txt.
  check     joins the records of a workflow run with the catalogue and the
            tags, writes the report, and fails when a tag names an unknown ID
            or technique, a record failed or comes from another commit, a tag
            that one of the run's jobs should have run has no passing record,
            or a CAST row or a required cell has no passing record and is not
            in ci/trace/known-gaps.txt.
  snapshot  writes otel-chdb/TRACEABILITY.md from a green run's report.

Standard library only: it runs on a bare runner before anything is installed.
"""
import argparse
import datetime
import fnmatch
import glob
import json
import os
import re
import subprocess
import sys
from collections import defaultdict

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
TRACE_DIR = os.path.join(ROOT, 'ci', 'trace')
STPA = 'otel-chdb/STPA.md'
VERIFICATION = 'otel-chdb/VERIFICATION.md'
RESEARCH = ['otel-chdb/research/langfuse.md', 'otel-chdb/research/entra-ingress.md', 'otel-chdb/research/grants.md']
ENV = 'OSCOPE_TRACE_OUT'
OUTCOMES = ('passed', 'failed', 'skipped')
BEGIN, END = '----- BEGIN TRACEABILITY REPORT -----', '----- END TRACEABILITY REPORT -----'

# An ID as it is written in the tables; the prefix decides its kind.
ID_TOKEN = re.compile(r'(?<![\w-])(?:L|H|SC|UCA|LS|SEC|TM|R)-(?:[A-Z]{1,2})?\d+(?![\w])')
ID_FULL = re.compile(r'^(?:L|H|SC|UCA|LS|SEC|TM|R)-(?:[A-Z]{1,2})?\d+$')
CAST_ID = re.compile(r'^CAST-\d+$')
KINDS = [('CAST-', 'CAST row'), ('UCA-', 'unsafe control action'), ('LS-', 'loss scenario'),
         ('SEC-', 'STPA-Sec'), ('TM-', 'STPA-Teaming'), ('SC-', 'system constraint'),
         ('R-', 'requirement'), ('H-', 'hazard'), ('L-', 'loss')]


def kind(i):
    for p, k in KINDS:
        if i.startswith(p):
            return k
    return '?'


def rel(p):
    return os.path.relpath(p, ROOT).replace(os.sep, '/')


def lines_of(path):
    with open(path, encoding='utf-8') as f:
        return f.read().split('\n')


def read(path):
    with open(os.path.join(ROOT, path), encoding='utf-8') as f:
        return f.read()


def cells(line):
    s = line.strip()
    if not (s.startswith('|') and s.endswith('|')):
        return None
    return [c.strip() for c in re.split(r'(?<!\\)\|', s[1:-1])]


def plain(s):
    s = re.sub(r'\*\*|`', '', s)
    s = re.sub(r'\[([^\]]*)\]\([^)]*\)', r'\1', s)
    return re.sub(r'\s+', ' ', s).strip()


def norm_title(s):
    return re.sub(r'[^a-z0-9]+', ' ', plain(s).lower()).strip()


# ---------------------------------------------------------------- catalogue

def stpa_sections(path, text):
    """(first line number, lines) of the parts of a file that hold STPA tables."""
    lines = text.split('\n')
    if path == STPA:
        return [(1, lines)]
    out, start = [], None
    for n, l in enumerate(lines):
        if re.match(r'^## 1\.? ', l):
            start = n
        elif start is not None and l.startswith('## '):
            out.append((start + 1, lines[start:n]))
            start = None
    if start is not None:
        out.append((start + 1, lines[start:]))
    return out


TITLE_HEADERS = ('loss', 'hazard', 'constraint', 'requirement', 'scenario', 'adversary action', 'teaming issue',
                 'issue', 'unsafe when')


def parse_tables(path, first, lines):
    """Yield (id, title, cited ids, line number, cells) for every table row whose first cell is an ID."""
    header = None
    for n, l in enumerate(lines):
        c = cells(l)
        if c is None:
            header = None
            continue
        if all(re.fullmatch(r':?-+:?', x) for x in c if x):
            continue
        first_cell = plain(c[0])
        if header is None or (first_cell in ('ID', '#') and not ID_FULL.match(first_cell)):
            header = [plain(x).lower() for x in c]
            continue
        lineno = first + n
        if header[0] == '#' and path == STPA:
            if not re.fullmatch(r'\d+', first_cell):
                continue
            i = 'CAST-' + first_cell
            title = plain(c[1]) if len(c) > 1 else ''
        else:
            m = re.match(r'^((?:L|H|SC|UCA|LS|SEC|TM|R)-(?:[A-Z]{1,2})?\d+)\b', first_cell)
            if not m:
                continue
            i = m.group(1)
            col = next((k for k, h in enumerate(header) if k > 0 and any(h.startswith(t) for t in TITLE_HEADERS)), 1)
            if header[1:2] and header[1] in ('controller', 'controller: action', 'actor', 'uca'):
                parts = [plain(x) for x in c[1:col + 1] if plain(x) not in ('', '-', '–')]
                title = ': '.join(parts[:2]) + (' — ' + parts[-1] if len(parts) > 2 else '')
            else:
                title = plain(c[col]) if col < len(c) else ''
        cited = sorted({t for x in c[1:] for t in ID_TOKEN.findall(plain(x))} - {i})
        yield i, title, cited, lineno, c


def markdown_catalog():
    """The catalogue source today: the tables of STPA.md and the research notes' STPA sections."""
    ids, dups, cites_all = {}, [], defaultdict(set)
    sources = [STPA] + RESEARCH
    for path in sources:
        text = read(path)
        for first, lines in stpa_sections(path, text):
            for i, title, cited, lineno, _ in parse_tables(path, first, lines):
                entry = {'id': i, 'kind': kind(i), 'title': title[:300], 'source': f'{path}:{lineno}', 'cites': cited}
                if i in ids:
                    dups.append((ids[i], entry))
                    # keep the first (STPA.md is read first: the adopted text); merge citations
                    ids[i]['cites'] = sorted(set(ids[i]['cites']) | set(cited))
                    ids[i].setdefault('also', []).append(entry['source'])
                else:
                    ids[i] = entry
            for l in lines:
                for t in ID_TOKEN.findall(plain(l)):
                    cites_all[t].add(path)
    # Requirements stated in prose in STPA.md (R-E1..R-E9: "Requirements R-E1..R-E9 (full text there)")
    # are catalogued from their own tables in the research notes above.
    return ids, dups, cites_all


def records_catalog():
    """A later source: the structured STPA records (one Markdown + YAML front-matter file per item
    under otel-chdb/stpa/, our ID as `label`), once they replace the tables. Reads only the flat
    `label`, `title`, `name` and `formerly` keys; each item's citations from any other ID in its
    file. A `formerly` label (UCA-10 and UCA-12 became loss scenarios, D39) stays an ID. Labels
    the records' project still leaves to hand-kept tables (otel-chdb/stpa/project.yaml
    `hand_kept`: the extension analyses' UCA, LS, SEC and TM rows) are read from those tables."""
    ids, dups, cites_all = {}, [], defaultdict(set)

    def add(i, entry):
        if i in ids:
            dups.append((ids[i], entry))
        else:
            ids[i] = entry

    for f in code_files(['otel-chdb/stpa/records/*.md']):
        text = '\n'.join(lines_of(f))
        m = re.match(r'^---\n(.*?)\n---\n', text, re.S)
        if not m:
            continue
        meta = dict(re.findall(r'^(\w+):\s*"?(.*?)"?\s*$', m.group(1), re.M))
        i = meta.get('label', '')
        if not (ID_FULL.match(i) or CAST_ID.match(i)):
            continue
        formerly = ID_TOKEN.findall(meta.get('formerly', ''))
        cited = sorted(set(ID_TOKEN.findall(text)) - {i} - set(formerly))
        title = (meta.get('title') or meta.get('name') or '')[:300]
        add(i, {'id': i, 'kind': kind(i), 'title': title, 'source': rel(f), 'cites': cited})
        for old in formerly:
            add(old, {'id': old, 'kind': kind(old), 'title': f'now {i}: {title}'[:300], 'source': rel(f), 'cites': [i]})
        for t in cited:
            cites_all[t].add(rel(f))
    project = os.path.join(ROOT, 'otel-chdb', 'stpa', 'project.yaml')
    hand = re.findall(r"^\s*-\s*\{doc:\s*([^,}]+),\s*labels:\s*'([^']+)'\}", read(rel(project)), re.M) if os.path.exists(project) else []
    for doc, pat in hand:
        path, want = 'otel-chdb/' + doc.strip(), re.compile(pat)
        for first, lines in stpa_sections(path, read(path)):
            for i, title, cited, lineno, _ in parse_tables(path, first, lines):
                if want.match(i):
                    add(i, {'id': i, 'kind': kind(i), 'title': title[:300], 'source': f'{path}:{lineno}', 'cites': cited})
    return ids, dups, cites_all


# The one interface the rest of this script uses: (ids, duplicates, citations). The source is the
# Markdown tables until the structured records land; OSCOPE_STPA_SOURCE=records switches, and IDs
# stay the same either way.
CATALOG_SOURCES = {'markdown': markdown_catalog, 'records': records_catalog}


def build_catalog():
    src = os.environ.get('OSCOPE_STPA_SOURCE', 'markdown')
    if src not in CATALOG_SOURCES:
        raise SystemExit(f'OSCOPE_STPA_SOURCE={src}: one of {", ".join(CATALOG_SOURCES)}')
    return CATALOG_SOURCES[src]()


def load_same_meaning():
    rules = []
    p = os.path.join(TRACE_DIR, 'same-meaning.txt')
    if os.path.exists(p):
        for n, l in enumerate(lines_of(p), 1):
            l = l.strip()
            if not l or l.startswith('#'):
                continue
            parts = [x.strip() for x in l.split('|')]
            if len(parts) != 4 or not all(parts):
                raise SystemExit(f'ci/trace/same-meaning.txt:{n}: want "ID-glob | file | file | reason"')
            rules.append(parts)
    return rules


def conflicting(dups, rules):
    bad = []
    for a, b in dups:
        if norm_title(a['title']) == norm_title(b['title']):
            continue
        fa, fb = a['source'].rsplit(':', 1)[0], b['source'].rsplit(':', 1)[0]
        ok = any(fnmatch.fnmatchcase(a['id'], g) and {fa, fb} == {x, y} for g, x, y, _ in rules)
        if not ok:
            bad.append((a, b))
    return bad


def techniques():
    """The codes of VERIFICATION.md §1, with their names."""
    out, sec = {}, False
    for l in read(VERIFICATION).split('\n'):
        if l.startswith('## '):
            sec = l.startswith('## 1.')
            continue
        c = cells(l) if sec else None
        if c and re.fullmatch(r'\*\*[A-Z0-9]+\*\*', c[0]):
            out[c[0].strip('*')] = plain(c[1])
    return out


def required_cells(tech):
    """VERIFICATION.md §3: every (hazard, technique) the plan marks required.

    "A + B" is two cells; "A / B" one cell either technique satisfies; a
    parenthetical or "on target" qualifies the technique, and "—" rows (not
    built) name none.
    """
    out, hz, sec = {}, None, False
    for n, l in enumerate(read(VERIFICATION).split('\n'), 1):
        if l.startswith('## '):
            sec = l.startswith('## 3.')
            continue
        if not sec:
            continue
        m = re.match(r'^### (H-\d+) ', l)
        if m:
            hz = m.group(1)
            continue
        c = cells(l)
        if not c or not hz or len(c) < 4 or c[0] in ('Req', '') or re.fullmatch(r'-+', c[0]):
            continue
        req = re.sub(r'\([^)]*\)', '', plain(c[0])).replace('on target', '')
        for part in req.split('+'):
            alts = [a.strip() for a in part.split('/') if a.strip()]
            alts = [a for a in alts if a in tech]
            if not alts:
                continue
            cid = f'{hz}:{"|".join(alts)}'
            e = out.setdefault(cid, {'id': cid, 'hazard': hz, 'techniques': alts, 'rows': [], 'source': f'{VERIFICATION}:{n}'})
            e['rows'].append({'component': plain(c[1]), 'status': plain(c[2]), 'evidence': plain(c[3])[:240]})
    return out


def catalog(args):
    ids, dups, cites_all = build_catalog()
    rules = load_same_meaning()
    bad = conflicting(dups, rules)
    tech = techniques()
    req = required_cells(tech)
    dangling = sorted(t for t in cites_all if t not in ids)
    doc = {'ids': ids, 'techniques': tech, 'required_cells': req,
           'dangling': {t: sorted(cites_all[t]) for t in dangling}}
    if args.json:
        with open(args.json, 'w', encoding='utf-8') as f:
            json.dump(doc, f, indent=1, sort_keys=True)
    by = defaultdict(int)
    for e in ids.values():
        by[e['kind']] += 1
    print(f'{len(ids)} IDs: ' + ', '.join(f'{k} {v}' for k, v in sorted(by.items())))
    print(f'{len(tech)} techniques, {len(req)} required cells; {len(dups)} restated IDs')
    for t in dangling:
        print(f'::warning::{t} is cited ({", ".join(sorted(cites_all[t]))}) but defined in no table')
    for a, b in bad:
        print(f'::error::{a["id"]} has two meanings: {a["source"]} "{a["title"][:80]}" vs {b["source"]} "{b["title"][:80]}" '
              f'(same meaning? say so in ci/trace/same-meaning.txt)')
    return 1 if bad else 0


# ---------------------------------------------------------------- tags in source

GO_TAG = re.compile(r'tracetag\.Covers\(\s*\w+\s*,((?:\s*"[^"]*"\s*,?)+)\)')
RS_TAG = re.compile(r'oscope_trace::covers\(\s*"([^"]*)"\s*,\s*&\[([^\]]*)\]\s*\)')
JS_TAG = re.compile(r"""\bcovers\(\s*t\s*,((?:\s*'[^']*'\s*,?)+)\)""")
CS_TAG = re.compile(r'OscopeTrace\.Covers\(\s*"([^"]*)"\s*,\s*"([^"]*)"\s*\)')
STEP_TAG = re.compile(r'trace\.py record\b(.*)$')
# The helpers' own sources and fixtures are not tags.
NOT_TAGS = {'otel-chdb/testgate/tracetag/tracetag.go', 'otel-chdb/testgate/tracetag/tracetag_test.go',
            'otel-chdb/otap-rs/src/oscope_trace.rs', 'otel-chdb/lakeui/test/trace.js',
            'otel-chdb/lakeui/test/trace.test.js', 'otel-chdb/lakeui/test/fixtures/trace.fixture.js',
            'otel-chdb/forwarder/tests/Oscope.Forwarder.Tests/OscopeTrace.cs'}


def strs(s, q='"'):
    return re.findall(q + r'((?:\\.|[^' + q + r'\\])*)' + q, s)


def enclosing(text, pos, pat):
    last = None
    for m in pat.finditer(text, 0, pos):
        last = m
    return last


def code_files(patterns):
    out = []
    for p in patterns:
        out += glob.glob(os.path.join(ROOT, p), recursive=True)
    return sorted({f for f in out if '/node_modules/' not in f and '/target/' not in f and '/.upstream/' not in f
                   and '/vendor/' not in f})


def load_scope():
    rules = []
    for n, l in enumerate(lines_of(os.path.join(TRACE_DIR, 'scope.txt')), 1):
        l = l.split('#', 1)[0].strip()
        if l:
            parts = l.split()
            if len(parts) < 2:
                raise SystemExit(f'ci/trace/scope.txt:{n}: want "path-glob job..."')
            rules.append((parts[0], parts[1:]))
    return rules


def jobs_for(path, rules):
    for g, jobs in rules:
        if fnmatch.fnmatchcase(path, g):
            return jobs
    return []


def workflow_steps():
    """`trace.py record` steps in the workflows, with the job each is in."""
    out = []
    for wf in sorted(glob.glob(os.path.join(ROOT, '.github', 'workflows', '*.yml'))):
        name = os.path.splitext(os.path.basename(wf))[0]
        job, in_jobs = None, False
        for n, l in enumerate(lines_of(wf), 1):
            if re.match(r'^jobs:\s*$', l):
                in_jobs = True
                continue
            m = re.match(r'^  ([A-Za-z0-9_-]+):\s*$', l)
            if in_jobs and m:
                job = m.group(1)
            m = STEP_TAG.search(l)
            if m and job and not l.lstrip().startswith('#'):
                a = m.group(1)
                test = re.search(r'--test\s+("([^"]*)"|(\S+))', a)
                tech = re.search(r'--technique\s+(\S+)', a)
                ids = re.search(r'--ids\s+(\S+)', a)
                out.append({'kind': 'step', 'file': rel(wf), 'line': n, 'func': (test.group(2) or test.group(3)) if test else '',
                            'technique': tech.group(1) if tech else '', 'ids': ids.group(1).split(',') if ids else [],
                            'jobs': [f'{name}:{job}']})
    return out


def load_models():
    out = []
    p = os.path.join(TRACE_DIR, 'models.txt')
    for n, l in enumerate(lines_of(p), 1):
        if not l.strip() or l.lstrip().startswith('#'):
            continue
        parts = l.rstrip('\n').split('|')
        if len(parts) < 6:
            raise SystemExit(f'ci/trace/models.txt:{n}: want "jobs | script | name | line regex | technique | ids"')
        # (the regex is everything between the third and the second-to-last bar: it may alternate)
        jobs, script, name, tech, ids = [x.strip() for x in parts[:3] + parts[-2:]]
        rx = '|'.join(parts[3:-2]).strip()
        out.append({'kind': 'model', 'file': 'ci/trace/models.txt', 'line': n, 'func': f'{script}: {name}', 'script': script,
                    'regex': rx, 'technique': tech, 'ids': ids.split(), 'jobs': jobs.split()})
    return out


def scan_tags():
    rules = load_scope()
    tags = []
    go_func = re.compile(r'^func (\w+)\(', re.M)
    rs_func = re.compile(r'^\s*(?:pub(?:\([^)]*\))? )?(?:async )?fn (\w+)', re.M)
    js_test = re.compile(r"""\b(?:test|it)\(\s*(['"`])((?:\\.|(?!\1).)*)\1""")
    for f in code_files(['otel-chdb/**/*_test.go', 'quintgo/**/*_test.go']):
        r = rel(f)
        if r in NOT_TAGS:
            continue
        text = '\n'.join(lines_of(f))
        for m in GO_TAG.finditer(text):
            fn = enclosing(text, m.start(), go_func)
            args = strs(m.group(1))
            tags.append({'kind': 'go', 'file': r, 'line': text.count('\n', 0, m.start()) + 1, 'func': fn.group(1) if fn else '',
                         'technique': args[0] if args else '', 'ids': args[1:], 'jobs': jobs_for(r, rules)})
    for f in code_files(['otel-chdb/otap-rs/src/**/*.rs', 'otel-chdb/otap-rs/tests/**/*.rs']):
        r = rel(f)
        if r in NOT_TAGS:
            continue
        text = '\n'.join(lines_of(f))
        for m in RS_TAG.finditer(text):
            ls = text.rfind('\n', 0, m.start()) + 1
            if text[ls:m.start()].lstrip().startswith('//'):
                continue
            fn = enclosing(text, m.start(), rs_func)
            tags.append({'kind': 'rust', 'file': r, 'line': text.count('\n', 0, m.start()) + 1, 'func': fn.group(1) if fn else '',
                         'technique': m.group(1), 'ids': strs(m.group(2)), 'jobs': jobs_for(r, rules)})
    for f in code_files(['otel-chdb/lakeui/**/*.test.js', 'otel-chdb/lakeui/**/*.test.mjs', 'otel-chdb/lakeui/**/*.spec.mjs']):
        r = rel(f)
        if r in NOT_TAGS:
            continue
        text = '\n'.join(lines_of(f))
        for m in JS_TAG.finditer(text):
            t = enclosing(text, m.start(), js_test)
            name = re.sub(r'\\(.)', r'\1', t.group(2)) if t else ''
            args = strs(m.group(1), "'")
            tags.append({'kind': 'node', 'file': r, 'line': text.count('\n', 0, m.start()) + 1, 'func': name,
                         'technique': args[0] if args else '', 'ids': args[1:], 'jobs': jobs_for(r, rules)})
    cs_func = re.compile(r'^\s*public\s+(?:async\s+)?(?:Task|void)\s+(\w+)\s*\(', re.M)
    for f in code_files(['otel-chdb/forwarder/tests/**/*.cs']):
        r = rel(f)
        if r in NOT_TAGS:
            continue
        text = '\n'.join(lines_of(f))
        for m in CS_TAG.finditer(text):
            ls = text.rfind('\n', 0, m.start()) + 1
            if text[ls:m.start()].lstrip().startswith('//'):
                continue
            fn = enclosing(text, m.start(), cs_func)
            tags.append({'kind': 'dotnet', 'file': r, 'line': text.count('\n', 0, m.start()) + 1, 'func': fn.group(1) if fn else '',
                         'technique': m.group(1), 'ids': m.group(2).replace(',', ' ').split(), 'jobs': jobs_for(r, rules)})
    tags += load_models()
    tags += workflow_steps()
    for t in tags:
        t['key'] = f'{t["file"]}::{t["func"]}'
    return tags


def scan(args):
    tags = scan_tags()
    if args.json:
        with open(args.json, 'w', encoding='utf-8') as f:
            json.dump(tags, f, indent=1)
    for t in tags:
        print(f'{t["file"]}:{t["line"]} {t["func"]} [{t["technique"]}] {" ".join(t["ids"])} -> {" ".join(t["jobs"]) or "NO JOB"}')
    print(f'{len(tags)} tags')
    return 0


# ---------------------------------------------------------------- records

def git_commit():
    if os.environ.get('GITHUB_SHA'):
        return os.environ['GITHUB_SHA']
    try:
        return subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=ROOT, capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return ''


def append(out, recs):
    if not out:
        return
    os.makedirs(os.path.dirname(os.path.abspath(out)), exist_ok=True)
    with open(out, 'a', encoding='utf-8') as f:
        for r in recs:
            f.write(json.dumps(r, sort_keys=True) + '\n')


def workflow_file():
    ref = os.environ.get('GITHUB_WORKFLOW_REF', '')  # owner/repo/.github/workflows/ci.yml@refs/heads/x
    m = re.search(r'(\.github/workflows/[^@]+)', ref)
    return m.group(1) if m else 'local'


def record(args):
    r = {'ids': [i for i in args.ids.split(',') if i], 'technique': args.technique, 'test': args.test, 'func': args.test,
         'package': 'ci', 'file': args.file or workflow_file(), 'lang': 'step', 'commit': git_commit(),
         'job': os.environ.get('GITHUB_JOB', ''), 'outcome': args.outcome}
    append(os.environ.get(ENV), [r])
    print(json.dumps(r))
    return 0


def model(args):
    """One record per models.txt entry of this script: the lines its regex matches in the report.

    A `sim` or `verify` line passes when its result is what it expects, a
    `runs` line when it passed some and failed none; a mutant a random
    simulation did not reach (`ok`, expect VIOLATED) and a row with no answer
    in its time (`UNKNOWN`) are `skipped`, an unknown. No matching line, no
    record: the check reports the entry as not run.
    """
    entries = [e for e in load_models() if e['script'] == args.script]
    if not entries:
        print(f'::error::no ci/trace/models.txt entries for {args.script}')
        return 1
    try:
        report = lines_of(args.report)
    except OSError as e:
        print(f'::error::{args.report}: {e}')
        report = []
    recs = []
    for e in entries:
        rx = re.compile(e['regex'])
        lines = [l for l in report if rx.search(l)]
        if not lines:
            print(f'::warning::{e["func"]}: no line matches /{e["regex"]}/ in {args.report}')
            continue
        outs = []
        for l in lines:
            m = re.search(r' (ok|VIOLATED|ERROR|UNKNOWN)\s+\(expect (ok|VIOLATED)\)', l)
            p = re.search(r'passed (\d+) failed (\d+)', l)
            if not m and ' UNKNOWN ' in l:
                outs.append('skipped')
            elif m:
                got, want = m.groups()
                # UNKNOWN: a time limit (Apalache's, or the job's budget) ended it before an answer
                outs.append('passed' if got == want else 'skipped' if (got, want) == ('ok', 'VIOLATED') or got == 'UNKNOWN'
                            else 'failed')
            elif p:
                outs.append('passed' if int(p.group(1)) > 0 and int(p.group(2)) == 0 else 'failed')
            else:
                outs.append('failed')
        o = 'failed' if 'failed' in outs else 'skipped' if 'skipped' in outs else 'passed'
        recs.append({'ids': e['ids'], 'technique': e['technique'], 'test': e['func'], 'func': e['func'], 'package': 'model',
                     'file': e['file'], 'lang': 'model', 'commit': git_commit(), 'job': os.environ.get('GITHUB_JOB', ''),
                     'outcome': o, 'lines': lines[:5]})
    append(os.environ.get(ENV), recs)
    for r in recs:
        print(f'{r["outcome"]:8} {r["test"]}')
    return 0


def load_records(paths):
    recs, bad = [], []
    files = []
    for p in paths:
        if os.path.isdir(p):
            files += sorted(glob.glob(os.path.join(p, '**', '*.jsonl'), recursive=True))
        elif os.path.exists(p):
            files.append(p)
    for f in files:
        for n, l in enumerate(lines_of(f), 1):
            if not l.strip():
                continue
            try:
                r = json.loads(l)
            except json.JSONDecodeError as e:
                bad.append(f'{f}:{n}: {e}')
                continue
            r['_from'] = rel(f) if f.startswith(ROOT) else f
            recs.append(r)
    return recs, bad, files


# ---------------------------------------------------------------- known gaps

def load_gaps():
    gaps, errs = {}, []
    p = os.path.join(TRACE_DIR, 'known-gaps.txt')
    for n, l in enumerate(lines_of(p), 1):
        if not l.strip() or l.lstrip().startswith('#'):
            continue
        parts = [x.strip() for x in l.rstrip('\n').split('|')]
        if len(parts) != 3 or not all(parts):
            errs.append(f'ci/trace/known-gaps.txt:{n}: want "ID | owner | reason"')
            continue
        if parts[0] in gaps:
            errs.append(f'ci/trace/known-gaps.txt:{n}: {parts[0]} listed twice')
        gaps[parts[0]] = {'owner': parts[1], 'reason': parts[2], 'line': n}
    return gaps, errs


# ---------------------------------------------------------------- check

def tech_list(t):
    return [x for x in re.split(r'[,+ ]+', t or '') if x]


def check(args):
    ids, dups, cites_all = build_catalog()
    bad_dups = conflicting(dups, load_same_meaning())
    tech = techniques()
    cells_req = required_cells(tech)
    tags = scan_tags()
    gaps, gap_errs = load_gaps()
    recs, bad_lines, files = load_records(args.records)
    commit = args.commit or git_commit()
    wf = args.workflow
    jobs = {j if ':' in j else f'{wf}:{j}' for j in (args.jobs or '').split(',') if j.strip()}
    fails, warns = [], []

    for a, b in bad_dups:
        fails.append(f'{a["id"]} has two meanings: {a["source"]} vs {b["source"]}')
    fails += gap_errs
    fails += [f'unreadable record {x}' for x in bad_lines]

    known = set(ids)

    def unknown_ids(xs):
        return [x for x in xs if x not in known]

    def unknown_tech(t):
        ts = tech_list(t)
        return [x for x in ts if x not in tech] or ([] if ts else ['(none)'])

    # 1. tags: known IDs and techniques; a job that runs them
    for t in tags:
        where = f'{t["file"]}:{t["line"]} ({t["func"]})'
        for x in unknown_ids(t['ids']):
            fails.append(f'{where}: tag names unknown ID {x}')
        if not t['ids']:
            fails.append(f'{where}: tag names no ID')
        for x in unknown_tech(t['technique']):
            fails.append(f'{where}: tag names unknown technique {x} (VERIFICATION.md §1)')
        if not t['jobs']:
            fails.append(f'{where}: no job runs this file (ci/trace/scope.txt)')
    for g in gaps:
        if not (g in known or g in cells_req or g.startswith('test:')):
            fails.append(f'ci/trace/known-gaps.txt:{gaps[g]["line"]}: unknown ID or cell {g}')

    # 2. records: known, from this commit, not failed; matched to their tag
    by_key = {t['key']: t for t in tags}
    per_tag = defaultdict(list)
    for r in recs:
        who = f'{r.get("test")} ({r.get("file")}, job {r.get("job")})'
        if r.get('outcome') not in OUTCOMES:
            fails.append(f'record {who}: outcome {r.get("outcome")!r}')
        if r.get('outcome') == 'failed':
            fails.append(f'record {who}: FAILED')
        if commit and r.get('commit') != commit:
            fails.append(f'record {who}: commit {r.get("commit")!r}, not this run\'s {commit}')
        for x in unknown_ids(r.get('ids', [])):
            fails.append(f'record {who}: unknown ID {x}')
        for x in unknown_tech(r.get('technique')):
            fails.append(f'record {who}: unknown technique {x}')
        key = f'{r.get("file")}::{r.get("func")}'
        if key in by_key:
            per_tag[key].append(r)
        else:
            warns.append(f'record {who} matches no tag in source')

    # 3. every tag a selected job should run has a passing record from one of them
    expected = [t for t in tags if set(t['jobs']) & jobs]
    for t in expected:
        # A record without a job comes from a local run (no GITHUB_JOB): it stands for any job.
        rs = [r for r in per_tag[t['key']] if not r.get('job') or f'{wf}:{r.get("job")}' in set(t['jobs']) & jobs]
        t['records'] = rs
        if any(r.get('outcome') == 'passed' for r in rs):
            continue
        g = gaps.get(f'test:{t["func"]}')
        if g:
            warns.append(f'{t["key"]}: no passing record (known gap: {g["reason"]})')
            continue
        seen = ', '.join(sorted({r.get('outcome') for r in rs})) or 'no record'
        fails.append(f'{t["file"]}:{t["line"]} {t["func"]}: tagged, and should run in '
                     f'{", ".join(sorted(set(t["jobs"]) & jobs))}, but {seen} (an unrun or skipped test is an unknown)')
    for t in tags:
        t.setdefault('records', [r for r in per_tag[t['key']]])

    # 4. coverage: CAST rows and required cells
    passing = [r for r in recs if r.get('outcome') == 'passed' and (not commit or r.get('commit') == commit)]

    def covering(pred):
        return [r for r in passing if pred(r)]

    def claimed(pred):
        return [t for t in tags if pred(t)]

    items = []
    for i in sorted((x for x in ids if x.startswith('CAST-')), key=lambda s: int(s.split('-')[1])):
        items.append(('cast', i, lambda o, i=i: i in o['ids']))
    for cid, c in sorted(cells_req.items(), key=lambda kv: (int(kv[1]['hazard'].split('-')[1]), kv[0])):
        items.append(('cell', cid, lambda o, c=c: c['hazard'] in o['ids'] and set(tech_list(o['technique'])) & set(c['techniques'])))
    status = {}
    for typ, i, pred in items:
        cov = covering(lambda r: pred({'ids': r.get('ids', []), 'technique': r.get('technique')}))
        cl = claimed(pred)
        cl_here = [t for t in cl if set(t['jobs']) & jobs]
        g = gaps.get(i)
        if cov:
            st = 'covered'
            if g:
                warns.append(f'{i}: covered at this commit but still in ci/trace/known-gaps.txt:{g["line"]} (remove it)')
        elif g:
            st = 'known gap'
        elif cl and not cl_here:
            st = 'deferred'
        else:
            st = 'MISSING'
            fails.append(f'{i}: no passing record' + (f' (tags: {", ".join(t["func"] for t in cl_here)})' if cl_here else
                                                       ' and no tag; tag its test or list it in ci/trace/known-gaps.txt'))
        status[i] = {'type': typ, 'status': st, 'records': cov, 'tags': cl, 'gap': g}

    report = render(ids, tech, cells_req, tags, recs, files, status, gaps, fails, warns, commit, wf, jobs,
                    {t: sorted(cites_all[t]) for t in cites_all if t not in ids})
    if args.report:
        with open(args.report, 'w', encoding='utf-8') as f:
            f.write(report)
    if args.summary:
        with open(args.summary, 'a', encoding='utf-8') as f:
            f.write(report)
    print(BEGIN)
    print(report)
    print(END)
    for w in warns:
        print(f'::warning::{w}')
    for x in fails:
        print(f'::error::{x}')
    print(f'traceability: {len(fails)} failures, {len(warns)} warnings')
    return 1 if fails else 0


def load_exit():
    """ci/trace/exit-criteria.txt: [{phase, criterion, techniques, ids, line}], errors."""
    out, errs = [], []
    p = os.path.join(TRACE_DIR, 'exit-criteria.txt')
    for n, l in enumerate(lines_of(p), 1):
        if not l.strip() or l.lstrip().startswith('#'):
            continue
        parts = [x.strip() for x in l.rstrip('\n').split('|')]
        if len(parts) != 4 or not all(parts):
            errs.append(f'ci/trace/exit-criteria.txt:{n}: want "phase | criterion | techniques | IDs"')
            continue
        out.append({'phase': parts[0], 'criterion': parts[1], 'techniques': tech_list(parts[2]),
                    'ids': [x for x in re.split(r'[, ]+', parts[3]) if x], 'line': n})
    return out, errs


def exit_criteria(args):
    """Judges one phase's exit criteria against records (VERIFICATION.md §4): fails while any is unmet."""
    ids, _, _ = build_catalog()
    tech = techniques()
    crit, errs = load_exit()
    for c in crit:
        errs += [f'ci/trace/exit-criteria.txt:{c["line"]}: unknown ID {x}' for x in c['ids'] if x not in ids]
        errs += [f'ci/trace/exit-criteria.txt:{c["line"]}: unknown technique {x}' for x in c['techniques'] if x not in tech]
    phases = sorted({c['phase'] for c in crit})
    if args.phase not in phases:
        errs.append(f'no exit criteria for phase {args.phase!r} (known: {", ".join(phases)})')
    recs, bad, _ = load_records(args.records or [])
    commit = args.commit or git_commit()
    passing = [r for r in recs if r.get('outcome') == 'passed' and (not commit or r.get('commit') == commit)]
    lines = [f'### Exit criteria of phase {args.phase} at {commit[:12] if commit else "(no commit)"}', '',
             '| criterion | techniques | IDs | evidence |', '|---|---|---|---|']
    unmet = 0
    for c in (c for c in crit if c['phase'] == args.phase):
        ev = sorted({r.get('test', '?') for r in passing
                     if set(r.get('ids', [])) & set(c['ids']) and set(tech_list(r.get('technique'))) & set(c['techniques'])})
        unmet += not ev
        lines.append(f'| {md(c["criterion"])} | {", ".join(c["techniques"])} | {", ".join(c["ids"])} | '
                     f'{md("; ".join(ev)) if ev else "**unmet**"} |')
    lines += ['', f'{unmet} unmet' + (' (the phase is not done)' if unmet else ': every criterion has a passing record')]
    text = '\n'.join(lines) + '\n'
    print(text)
    if args.summary:
        with open(args.summary, 'a', encoding='utf-8') as f:
            f.write(text)
    for e in errs + [f'unreadable record {x}' for x in bad]:
        print(f'::error::{e}')
    return 1 if errs or bad or unmet else 0


def outcome_str(rs):
    if not rs:
        return 'not run'
    c = defaultdict(int)
    for r in rs:
        c[r.get('outcome')] += 1
    return ', '.join(f'{k} ×{v}' if v > 1 else k for k, v in sorted(c.items()))


def md(s):
    return str(s).replace('|', '\\|').replace('\n', ' ')


def render(ids, tech, cells_req, tags, recs, files, status, gaps, fails, warns, commit, wf, jobs, dangling):
    run = ''
    if os.environ.get('GITHUB_RUN_ID'):
        run = f'{os.environ.get("GITHUB_SERVER_URL", "https://github.com")}/{os.environ.get("GITHUB_REPOSITORY")}/actions/runs/{os.environ["GITHUB_RUN_ID"]}'
    by = defaultdict(int)
    for e in ids.values():
        by[e['kind']] += 1
    rc = defaultdict(int)
    for r in recs:
        rc[r.get('outcome')] += 1
    def count(typ, st):
        return sum(1 for s in status.values() if s['type'] == typ and s['status'] == st)
    L = []
    L.append('# Traceability report')
    L.append('')
    L.append(f'Generated by `ci/trace/trace.py check` ({datetime.datetime.now(datetime.timezone.utc):%Y-%m-%d %H:%MZ}). '
             f'Workflow **{wf}**, commit `{commit}`' + (f', run {run}' if run else '') + '.')
    L.append(f'Jobs judged: {", ".join(sorted(jobs)) or "none"}. Record files: {len(files)}.')
    L.append('')
    L.append(f'**Verdict: {"PASS" if not fails else "FAIL"}** ({len(fails)} failures, {len(warns)} warnings).')
    L.append('')
    L.append('Evidence is a record the tagged test wrote when it ran at this commit, with its outcome; '
             'a tag with no passing record is an unknown, not coverage (CAST rows 41, 43, 45, 46, 53).')
    L.append('')
    L.append('## Numbers')
    L.append('')
    L.append('| | Count |')
    L.append('|---|---|')
    L.append(f'| IDs catalogued | {len(ids)} ({", ".join(f"{k} {v}" for k, v in sorted(by.items()))}) |')
    L.append(f'| Techniques (VERIFICATION.md §1) | {len(tech)} |')
    L.append(f'| Tags in source | {len(tags)} ({len({t["key"] for t in tags})} tests or checks) |')
    L.append(f'| Records this run | {len(recs)} ({", ".join(f"{k} {v}" for k, v in sorted(rc.items())) or "none"}) |')
    nc = sum(1 for s in status.values() if s['type'] == 'cast')
    L.append(f'| CAST rows | {nc}: covered {count("cast", "covered")}, known gap {count("cast", "known gap")}, '
             f'deferred to another run {count("cast", "deferred")}, missing {count("cast", "MISSING")} |')
    ncl = sum(1 for s in status.values() if s['type'] == 'cell')
    L.append(f'| Required cells (VERIFICATION.md §3) | {ncl}: covered {count("cell", "covered")}, known gap {count("cell", "known gap")}, '
             f'deferred {count("cell", "deferred")}, missing {count("cell", "MISSING")} |')
    L.append('')
    if fails:
        L.append('## Failures')
        L.append('')
        L += [f'- {md(x)}' for x in fails]
        L.append('')
    if warns:
        L.append('## Warnings')
        L.append('')
        L += [f'- {md(x)}' for x in warns]
        L.append('')
    # hazard -> requirement -> technique -> tests
    L.append('## Hazards → requirements → techniques → tests')
    L.append('')
    graph = {i: set(e['cites']) for i, e in ids.items()}
    def hazards_of(i, depth=4, seen=None):
        seen = seen or set()
        out = set()
        for c in graph.get(i, ()):
            if c in seen:
                continue
            seen.add(c)
            if c.startswith('H-'):
                out.add(c)
            elif depth:
                out |= hazards_of(c, depth - 1, seen)
        return out
    reqs = defaultdict(set)
    for i in ids:
        if i.startswith('R-'):
            for h in hazards_of(i):
                reqs[h].add(i)
    tag_by_id = defaultdict(list)
    for t in tags:
        for x in t['ids']:
            tag_by_id[x].append(t)
    for h in sorted((i for i in ids if i.startswith('H-')), key=lambda s: (re.sub(r'\d+', '', s), int(re.sub(r'\D', '', s) or 0))):
        L.append(f'### {h} {md(ids[h]["title"])}')
        L.append('')
        rq = sorted(reqs.get(h, ()), key=lambda s: (s[:3], int(re.sub(r'\D', '', s) or 0)))
        L.append(f'Requirements: {", ".join(rq) if rq else "none traced"}.')
        L.append('')
        cells_h = [(cid, c) for cid, c in cells_req.items() if c['hazard'] == h]
        rows = []
        for cid, c in sorted(cells_h):
            s = status[cid]
            tests = '; '.join(f'`{md(t["func"])}` ({outcome_str(t.get("records", []))})' for t in s['tags']) or '—'
            note = s['gap']['reason'] if s['gap'] and s['status'] != 'covered' else ''
            rows.append(f'| {"/".join(c["techniques"])} (required) | {s["status"]} | {tests} | {md(note)} |')
        other = defaultdict(list)
        for t in tag_by_id.get(h, []):
            for tt in tech_list(t['technique']):
                if not any(h == c['hazard'] and tt in c['techniques'] for c in cells_req.values()):
                    other[tt].append(t)
        for tt, ts in sorted(other.items()):
            tests = '; '.join(f'`{md(t["func"])}` ({outcome_str(t.get("records", []))})' for t in ts)
            rows.append(f'| {tt} | {"covered" if any(r.get("outcome") == "passed" for t in ts for r in t.get("records", [])) else "no passing record"} | {tests} | |')
        if rows:
            L.append('| Technique | Status | Tests (outcomes this run) | Known gap |')
            L.append('|---|---|---|---|')
            L += rows
        else:
            L.append('No required cell and no tag yet.')
        L.append('')
    L.append('## CAST rows')
    L.append('')
    L.append('| Row | Issue | Status | Tests (outcomes this run) | Known gap (owner: reason) |')
    L.append('|---|---|---|---|---|')
    for i, s in status.items():
        if s['type'] != 'cast':
            continue
        tests = '; '.join(f'`{md(t["func"])}` [{t["technique"]}] ({outcome_str(t.get("records", []))})' for t in s['tags']) or '—'
        g = f'{s["gap"]["owner"]}: {s["gap"]["reason"]}' if s['gap'] else ''
        L.append(f'| {i} | {md(ids[i]["title"][:110])} | {s["status"]} | {tests} | {md(g)} |')
    L.append('')
    L.append('## Tags')
    L.append('')
    L.append('| Test or check | Technique | IDs | Jobs | Outcomes this run |')
    L.append('|---|---|---|---|---|')
    for t in sorted(tags, key=lambda t: t['key']):
        L.append(f'| `{md(t["func"])}` ({t["file"]}) | {t["technique"]} | {" ".join(t["ids"])} | {" ".join(t["jobs"])} | '
                 f'{outcome_str(t.get("records", []))} |')
    L.append('')
    if dangling:
        L.append('## IDs cited but defined in no table')
        L.append('')
        L += [f'- {t}: {", ".join(v)}' for t, v in sorted(dangling.items())]
        L.append('')
    return '\n'.join(L) + '\n'


# ---------------------------------------------------------------- snapshot

def snapshot(args):
    text = '\n'.join(lines_of(args.src))
    if BEGIN in text:
        text = text.split(BEGIN, 1)[1].split(END, 1)[0]
        # job logs prefix every line with a timestamp
        text = '\n'.join(re.sub(r'^\d{4}-\d\d-\d\dT[\d:.]+Z ', '', l) for l in text.split('\n')).strip('\n') + '\n'
    if '**Verdict: PASS**' not in text:
        print('the report is not from a passing run; a snapshot records only green evidence', file=sys.stderr)
        return 1
    head = ('<!-- Generated: python3 ci/trace/trace.py snapshot <report.md or job log>, from the traceability job of a green run.\n'
            '     Do not edit by hand; regenerate. -->\n')
    out = os.path.join(ROOT, 'otel-chdb', 'TRACEABILITY.md')
    with open(out, 'w', encoding='utf-8') as f:
        f.write(head + text.replace('# Traceability report', '# Traceability (snapshot)', 1))
    print(f'wrote {rel(out)}')
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    sub = ap.add_subparsers(dest='cmd', required=True)
    p = sub.add_parser('catalog'); p.add_argument('--json'); p.set_defaults(fn=catalog)
    p = sub.add_parser('scan'); p.add_argument('--json'); p.set_defaults(fn=scan)
    p = sub.add_parser('record')
    p.add_argument('--test', required=True); p.add_argument('--technique', required=True); p.add_argument('--ids', required=True)
    p.add_argument('--outcome', default='passed', choices=OUTCOMES); p.add_argument('--file'); p.set_defaults(fn=record)
    p = sub.add_parser('model'); p.add_argument('--script', required=True); p.add_argument('--report', required=True); p.set_defaults(fn=model)
    p = sub.add_parser('check')
    p.add_argument('--records', nargs='+', required=True); p.add_argument('--workflow', required=True, choices=('ci', 'nightly'))
    p.add_argument('--jobs', required=True, help='comma list of the jobs this run ran (ci: go-test,rust,...)')
    p.add_argument('--commit'); p.add_argument('--report'); p.add_argument('--summary'); p.set_defaults(fn=check)
    p = sub.add_parser('snapshot'); p.add_argument('src'); p.set_defaults(fn=snapshot)
    p = sub.add_parser('exit', help="judge one phase's exit criteria (ci/trace/exit-criteria.txt)")
    p.add_argument('--phase', required=True); p.add_argument('--records', nargs='*')
    p.add_argument('--commit'); p.add_argument('--summary'); p.set_defaults(fn=exit_criteria)
    a = ap.parse_args()
    sys.exit(a.fn(a))


if __name__ == '__main__':
    main()
