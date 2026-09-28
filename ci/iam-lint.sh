#!/bin/bash
# Lints the IAM policies in otel-chdb/deploy/iam/: each is valid JSON, and
# every role that grants s3:ListBucket under an s3:prefix condition also
# grants it, on the same prefixes, under StringLikeIfExists (or with no
# condition). S3 answers a missing key 404 only to a caller with ListBucket,
# and a HEAD or GET carries no s3:prefix, so a prefix-only grant turns every
# free slot, missing lease and missing format marker into 403
# (DECISIONS.md D18 amendment of 2026-09-28, AMBIGUITY.md S5).
#
#   ci/iam-lint.sh [DIR]
set -euo pipefail
dir=${1:-$(dirname "$0")/../otel-chdb/deploy/iam}
python3 - "$dir" <<'PY'
import glob, json, os, sys
fails = 0
for path in sorted(glob.glob(os.path.join(sys.argv[1], "*.json"))):
    name = os.path.basename(path)
    try:
        doc = json.load(open(path))
    except ValueError as e:
        print(f"FAIL {name}: not JSON: {e}")
        fails += 1
        continue
    sts = doc.get("Statement", [])
    sts = sts if isinstance(sts, list) else [sts]
    def lists(st):
        a = st.get("Action", [])
        return st.get("Effect") == "Allow" and "Principal" not in st and "s3:ListBucket" in (a if isinstance(a, list) else [a])
    def prefixes(st, op):
        v = st.get("Condition", {}).get(op, {}).get("s3:prefix")
        return set(v if isinstance(v, list) else [v]) if v is not None else None
    scoped = [prefixes(st, "StringLike") for st in sts if lists(st) and prefixes(st, "StringLike") is not None]
    if not scoped:
        print(f"ok   {name}: no prefix-scoped ListBucket")
        continue
    unconditioned = any(lists(st) and not st.get("Condition") for st in sts)
    ifexists = set().union(*[prefixes(st, "StringLikeIfExists") or set() for st in sts if lists(st)])
    missing = set().union(*scoped) - ifexists
    if unconditioned or not missing:
        print(f"ok   {name}: a HEAD of a missing key is covered by ListBucket")
    else:
        print(f"FAIL {name}: s3:ListBucket is scoped by s3:prefix {sorted(missing)} with no StringLikeIfExists grant on it: "
              "a HEAD carries no prefix, so a missing key answers 403")
        fails += 1
sys.exit(1 if fails else 0)
PY
