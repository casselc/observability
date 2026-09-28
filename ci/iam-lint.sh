#!/bin/bash
# Lints the IAM policies in otel-chdb/deploy/iam/: each is valid JSON, and
# every role that grants s3:ListBucket under an s3:prefix condition also
# grants it, on the same prefixes, under StringLikeIfExists (or with no
# condition). S3 answers a missing key 404 only to a caller with ListBucket,
# and a HEAD or GET carries no s3:prefix, so a prefix-only grant turns every
# free slot, missing lease and missing format marker into 403
# (DECISIONS.md D18 amendment of 2026-09-28, AMBIGUITY.md S5).
# KMS grants (the query service's basis key, D30 amendment of 2026-09-28)
# name one key ARN per resource with named actions, and a key policy never
# lets "*" or the account root call GenerateMac/VerifyMac/CreateGrant.
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
    # KMS (the query service's basis key, D30 amendment 2026-09-28): an
    # identity policy names the configured keys by key ARN, never "*", a
    # wildcard or an alias (KMS authorizes on the key, so an alias resource
    # grants nothing and a wildcard grants every key); actions are named,
    # never kms:* or kms:Generate*. A key policy never lets "*" or the
    # account root use the key (the root statement would let any IAM policy
    # in the account delegate GenerateMac).
    use = {"kms:generatemac", "kms:verifymac", "kms:creategrant"}
    kfail = []
    for st in sts:
        if st.get("Effect") != "Allow":
            continue
        acts = st.get("Action", [])
        acts = [a.lower() for a in (acts if isinstance(acts, list) else [acts])]
        kacts = [a for a in acts if a.startswith("kms:") or a == "*"]
        if not kacts:
            continue
        sid = st.get("Sid", "?")
        if any("*" in a for a in kacts):
            kfail.append(f"{sid}: wildcard KMS action {kacts}")
        res = st.get("Resource", [])
        res = res if isinstance(res, list) else [res]
        if "Principal" in st:
            pr = st["Principal"]
            aws = pr.get("AWS", []) if isinstance(pr, dict) else pr
            aws = aws if isinstance(aws, list) else [aws]
            if any(a in use for a in kacts) and any(p == "*" or str(p).endswith(":root") for p in aws):
                kfail.append(f"{sid}: key policy lets {aws} use the key ({sorted(set(kacts) & use)})")
        else:
            for r in res:
                if r == "*" or not r.startswith("arn:aws") or ":key/" not in r or r.endswith("/*") or "*" in r.split(":key/")[-1]:
                    kfail.append(f"{sid}: KMS resource {r!r} is not one key ARN")
    if kfail:
        for f in kfail:
            print(f"FAIL {name}: {f}")
        fails += 1
        continue
    if any("kms:" in json.dumps(st.get("Action", "")).lower() for st in sts):
        print(f"ok   {name}: KMS grants name keys by ARN; no wildcard, no delegable use")
        continue
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
