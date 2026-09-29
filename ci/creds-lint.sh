#!/bin/bash
# Lints the configurations we ship for static credentials (STPA CAST row 11):
# edge, collector and service configs once carried literal S3 keys "for local
# runs", which shadow IRSA and Pod Identity wherever the config is deployed.
# Credentials come from the environment (or the platform's chain) only.
#
# A credential field (access_key, access_key_id, secret_key,
# secret_access_key, session_token, password, client_secret, and the
# AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN variables in
# a manifest) may be empty or name the environment: ${env:NAME} or ${NAME},
# with no default value (a default is a static key by another name). A
# *_env field (client_secret_env: ALR_CLIENT_SECRET) names a variable and is
# fine.
#
# Checked: every YAML / JSON / TOML under otel-chdb/deploy, otel-chdb/otelcol,
# otel-chdb/otap-rs/configs, and every otel-chdb/**/*.example.{yaml,yml,json}.
# Not checked, each for a reason (ALLOW below): the local-only kind overlay and
# the test scripts' own configs, whose keys are the throwaway SeaweedFS ones.
#
#   ci/creds-lint.sh             # the repository
#   ci/creds-lint.sh --self-test # the lint finds planted keys (a lint that finds nothing proves nothing)
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
python3 - "$root" "${1:-}" <<'PY'
import glob, os, re, sys, tempfile

root, mode = sys.argv[1], sys.argv[2]
KEYS = r'(access_key|access_key_id|secret_key|secret_access_key|session_token|password|client_secret|' \
       r'aws_access_key_id|aws_secret_access_key|aws_session_token)'
FIELD = re.compile(r'^\s*(?:-\s*)?["\']?' + KEYS + r'["\']?\s*[:=]\s*(.*?)\s*,?\s*$', re.I)
ENVVAR = re.compile(r'\b(AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AWS_SESSION_TOKEN)=(\S*)')
JSONFIELD = re.compile(r'"' + KEYS + r'"\s*:\s*("(?:[^"\\]|\\.)*"|[^,}\s]+)', re.I)
NAMEVAL = re.compile(r'^\s*-?\s*name:\s*["\']?(AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AWS_SESSION_TOKEN)["\']?\s*$')
OK = re.compile(r'^(|""|\'\'|null|~|\$\{env:[A-Za-z_][A-Za-z0-9_]*\}|\$\{[A-Za-z_][A-Za-z0-9_]*\}|"\$\{env:[A-Za-z_][A-Za-z0-9_]*\}"|"\$\{[A-Za-z_][A-Za-z0-9_]*\}"|\{\}|\{)$')
# path prefix -> why it is not a shipped configuration
ALLOW = {
    'otel-chdb/deploy/kind/': 'the local kind cluster, against its own in-cluster SeaweedFS and its dev keys',
    'otel-chdb/deploy/scripts/': 'test and measurement scripts, run against the local SeaweedFS',
}


def files(base):
    pats = ['otel-chdb/deploy/**/*.yaml', 'otel-chdb/deploy/**/*.yml', 'otel-chdb/deploy/**/*.json', 'otel-chdb/deploy/**/*.toml',
            'otel-chdb/otelcol/**/*.yaml', 'otel-chdb/otap-rs/configs/**/*.yaml', 'otel-chdb/otap-rs/configs/**/*.toml',
            'otel-chdb/**/*.example.yaml', 'otel-chdb/**/*.example.yml', 'otel-chdb/**/*.example.json']
    out = set()
    for p in pats:
        out.update(glob.glob(os.path.join(base, p), recursive=True))
    return sorted(f for f in out if '/node_modules/' not in f)


def lint(base):
    bad, n = [], 0
    for f in files(base):
        r = os.path.relpath(f, base)
        if any(r.startswith(a) for a in ALLOW):
            continue
        n += 1
        lines = open(f, encoding='utf-8', errors='replace').read().split('\n')
        for i, l in enumerate(lines, 1):
            s = l.split('#', 1)[0] if not f.endswith('.json') else l
            if f.endswith('.json'):
                for m in JSONFIELD.finditer(s):
                    if not OK.match(m.group(2).strip()):
                        bad.append(f'{r}:{i}: {m.group(1)} has a literal value')
            else:
                m = FIELD.match(s)
                if m and not OK.match(m.group(2).strip()):
                    bad.append(f'{r}:{i}: {m.group(1)} has a literal value')
            for m in ENVVAR.finditer(s):
                if m.group(2) and not OK.match(m.group(2)):
                    bad.append(f'{r}:{i}: {m.group(1)} set to a literal')
            if NAMEVAL.match(s) and i < len(lines):
                v = re.match(r'^\s*value:\s*(.*?)\s*$', lines[i].split('#', 1)[0])
                if v and not OK.match(v.group(1)):
                    bad.append(f'{r}:{i + 1}: {NAMEVAL.match(s).group(1)} value is a literal')
    return bad, n


if mode == '--self-test':
    with tempfile.TemporaryDirectory() as d:
        os.makedirs(os.path.join(d, 'otel-chdb/deploy/x'))
        os.makedirs(os.path.join(d, 'otel-chdb/otelcol'))
        os.makedirs(os.path.join(d, 'otel-chdb/deploy/kind'))
        planted = {
            'otel-chdb/deploy/x/a.yaml': 'env:\n  - name: AWS_SECRET_ACCESS_KEY\n    value: otelsecret\n',
            'otel-chdb/otelcol/config.edge.yaml': 'auth:\n  access_key_id: ${env:S3_KEY:-otel}\n  secret_access_key: "${env:S3_SECRET}"\n',
            'otel-chdb/deploy/x/k.yaml': 'literals:\n  - AWS_ACCESS_KEY_ID=otel\n',
            'otel-chdb/svc.example.json': '{"s3": {"secret_key": "abc"}}\n',
            'otel-chdb/deploy/kind/k.yaml': 'literals:\n  - AWS_ACCESS_KEY_ID=otel\n',
        }
        for p, t in planted.items():
            with open(os.path.join(d, p), 'w') as fh:
                fh.write(t)
        bad, _ = lint(d)
        want = {'otel-chdb/deploy/x/a.yaml:3', 'otel-chdb/otelcol/config.edge.yaml:2', 'otel-chdb/deploy/x/k.yaml:2',
                'otel-chdb/svc.example.json:1'}
        got = {b.rsplit(':', 1)[0] for b in bad}
        if got != want:
            print(f'FAIL self-test: found {sorted(got)}, want {sorted(want)}')
            sys.exit(1)
        print(f'self-test: {len(bad)} planted keys found, the allowed overlay and the env reference passed')
    sys.exit(0)

bad, n = lint(root)
for b in bad:
    print('FAIL ' + b)
print(f'{n} shipped configs, {len(bad)} static credentials')
sys.exit(1 if bad else 0)
PY
