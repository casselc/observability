#!/bin/bash
# Write-side ABAC on SeaweedFS (DECISIONS.md D18, STPA R-S7): one policy
# for every edge, scoped by a policy variable, and a check matrix proving
# that an edge of cluster A cannot write cluster B's prefix, the control
# prefix, or delete anything, while the create-only protocol still works.
#
# SeaweedFS has no session tags (research/lake-ui.md §6.2), so the variable
# is the identity's name: one static identity per cluster, named after it,
# all attached to one policy whose resource is {root}/${aws:username}/*
# (on AWS the same policy uses ${aws:PrincipalTag/cluster}: edge-publisher.json).
# The script starts its own S3 gateway on the running filer with that
# identity file, so the store's main gateway is untouched.
#
#   WEED=weed [FILER=127.0.0.1:18888 PORT=18433 BUCKET=otel RUN=name OTAP=otap-s3pq CFG=otap-rs/configs/edge.yaml] seaweedfs_abac.sh
#
# OTAP (optional): also runs the Rust publisher with cluster c1's key, once
# as cluster c1 (its births commit) and once claiming cluster c2 (none does).
set -u
: "${WEED:?}"
FILER=${FILER:-127.0.0.1:18888}; PORT=${PORT:-18433}; BUCKET=${BUCKET:-otel}; RUN=${RUN:-abac$(date +%s)}
here=$(cd "$(dirname "$0")" && pwd)
W=$(mktemp -d); trap 'kill $gw 2>/dev/null; rm -rf "$W"' EXIT
python3 - "$W/s3.json" "$BUCKET" "$RUN" <<'PY'
import json, sys
out, bucket, root = sys.argv[1:]
edge = {"Version": "2012-10-17", "Statement": [
    {"Sid": "CreateSlotsUnderOwnClusterOnly", "Effect": "Allow", "Action": ["s3:PutObject", "s3:GetObject"],
     "Resource": [f"arn:aws:s3:::{bucket}/{root}/${{aws:username}}/*"]},
    {"Sid": "ListSoAFreeSlotIs404", "Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": [f"arn:aws:s3:::{bucket}"]},
    {"Sid": "NeverDelete", "Effect": "Deny", "Action": ["s3:DeleteObject"], "Resource": [f"arn:aws:s3:::{bucket}/*"]}]}
ident = lambda n, k: {"name": n, "credentials": [{"accessKey": k, "secretKey": k + "secret"}]}
cfg = {"identities": [
    dict(ident("c1", "edgec1"), policyNames=["edge-publisher"]),
    dict(ident("c2", "edgec2"), policyNames=["edge-publisher"]),
    dict(ident("consumer", "consumer"), actions=[f"Read:{bucket}", f"Write:{bucket}", f"List:{bucket}"])],
    "policies": [{"name": "edge-publisher", "content": json.dumps(edge)}]}
json.dump(cfg, open(out, "w"))
PY
"$WEED" s3 -config="$W/s3.json" -filer="$FILER" -ip.bind=127.0.0.1 -port="$PORT" -port.grpc=$((PORT + 10000)) \
  -port.iceberg=0 -port.lance=0 > "$W/gw.log" 2>&1 &
gw=$!
for _ in $(seq 100); do curl -s -o /dev/null "http://127.0.0.1:$PORT/" && break; sleep 0.1; done
fails=0
req() { # key-id method key [header] -> http code
  local m=(-X "$2"); [ "$2" = HEAD ] && m=(-I)
  curl -s -m 10 -o /dev/null -w "%{http_code}" --aws-sigv4 aws:amz:us-east-1:s3 -u "$1:$1secret" "${m[@]}" ${4:+-H "$4"} \
    ${5:+--data "$5"} "http://127.0.0.1:$PORT/$BUCKET/$3"
}
check() { # what want got
  if [ "$2" = "$3" ]; then echo "PASS $1: $3"; else echo "FAIL $1: $3 (want $2)"; fails=$((fails + 1)); fi
}
slot() { echo "$RUN/$1/edge-1/traces/20260927T000000.000Z-00000001/$(printf %020d "$2").parquet"; }
echo "gateway $PORT, root $BUCKET/$RUN, weed $("$WEED" version 2>&1 | head -1)"
check "c1 creates its own slot"                   200 "$(req edgec1 PUT "$(slot c1 0)" 'If-None-Match: *' x)"
check "c1 again on the same slot (create-only)"   412 "$(req edgec1 PUT "$(slot c1 0)" 'If-None-Match: *' y)"
check "c1 HEADs its slot"                          200 "$(req edgec1 HEAD "$(slot c1 0)")"
check "c1 HEADs its next, free slot (ListBucket)"  404 "$(req edgec1 HEAD "$(slot c1 1)")"
check "c2 creates its own slot"                    200 "$(req edgec2 PUT "$(slot c2 0)" 'If-None-Match: *' x)"
check "c1 writes into cluster c2's prefix"         403 "$(req edgec1 PUT "$(slot c2 1)" 'If-None-Match: *' forged)"
check "c1 reads cluster c2's slot"                 403 "$(req edgec1 HEAD "$(slot c2 0)")"
check "c1 writes a control object"                 403 "$(req edgec1 PUT "$RUN/_consumer/lease/c2/edge-1/traces.json" '' '{}')"
check "c1 writes the watermark"                    403 "$(req edgec1 PUT "$RUN/_consumer/watermark.json" '' '{}')"
check "c1 deletes its own slot"                    403 "$(req edgec1 DELETE "$(slot c1 0)")"
check "c1 deletes cluster c2's slot"               403 "$(req edgec1 DELETE "$(slot c2 0)")"
check "c2's slot is intact"                        200 "$(req consumer HEAD "$(slot c2 0)")"
check "consumer writes a control object"           200 "$(req consumer PUT "$RUN/_consumer/lease/c2/edge-1/traces.json" '' '{}')"
check "consumer deletes an ingested slot (GC)"     204 "$(req consumer DELETE "$(slot c2 0)")"
if [ -n "${OTAP:-}" ]; then
  pub() { # cluster key -> births registered
    env -u AWS_PROFILE AWS_ACCESS_KEY_ID="$2" AWS_SECRET_ACCESS_KEY="$2secret" CLUSTER="$1" PRODUCER=edge-9 HEARTBEAT=1h \
      OTLP_HTTP=127.0.0.1:14718 OTLP_GRPC=127.0.0.1:14717 ADMIN_HTTP=127.0.0.1:14780 PUT_TIMEOUT=2s \
      S3_URL="http://127.0.0.1:$PORT/$BUCKET/$RUN" timeout -s INT 12 "$OTAP" -c "${CFG:-$here/../../otap-rs/configs/edge.yaml}" 2>&1 \
      | grep -o 'births: [0-9]* of [0-9]*'
  }
  check "publisher as c1 with c1's key: births"    "births: 7 of 7" "$(pub c1 edgec1)"
  check "publisher claiming c2 with c1's key"      "births: 0 of 7" "$(pub c2 edgec1)"
fi
echo "$fails failed"
exit "$fails"
