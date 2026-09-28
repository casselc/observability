#!/bin/bash
# The acceptance suite against Nutanix Objects (nutanix.md §2), from a Linux
# host (or pod) at the site: the static s3accept binary (acceptance/build.sh),
# static keys, the private CA, path-style, every client-facing IP.
#
#   RUN=v1 ENDPOINT=https://objects.example BUCKET=... CA_BUNDLE=ca.pem \
#   AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... NUTANIX_IPS=10.0.0.11,10.0.0.12,10.0.0.13 \
#   [S3ACCEPT=acceptance/bin/s3accept-linux-amd64 OBJECTS_VERSION=4.x REGION=us-east-1 LOAD_PLAN="cluster-first:500:300 cluster-first:2000:300"] \
#   nutanix/accept.sh [STEP...]
#
# Steps (default: all but load and hold): dry creds run1 run2 race conflicts list perf etag load hold
# Reports in $STATE/nutanix/; objects under $BUCKET/$VPREFIX/accept/ and .../load/, each run deleting
# its own; the bucket and anything else in it are untouched.
# shellcheck source=../lib.sh
. "$(dirname "$0")/../lib.sh"
need python3 aws md5sum
: "${ENDPOINT:?}" "${BUCKET:?}" "${CA_BUNDLE:?}" "${AWS_ACCESS_KEY_ID:?}" "${AWS_SECRET_ACCESS_KEY:?}"
S3ACCEPT=${S3ACCEPT:-$OTEL_CHDB/acceptance/bin/s3accept}
[ -x "$S3ACCEPT" ] || die "build it: acceptance/build.sh (then S3ACCEPT=acceptance/bin/s3accept-linux-amd64)"
REGION=${REGION:-us-east-1}
export S3_ENDPOINT=$ENDPOINT CA_BUNDLE AWS_REGION=$REGION
# The SDKs' default CRC32 trailer is what S3-compatible stores most often reject (acceptance RUNBOOK §3.2);
# `checksum` measures it with the defaults, so they are not set here; the deployed clients set when_required.
URL=$ENDPOINT/$BUCKET/$VPREFIX/accept
EPS=""
for ip in ${NUTANIX_IPS//,/ }; do EPS="${EPS:+$EPS,}$ENDPOINT=$ip"; done
OUT=$STATE/nutanix; mkdir -p "$OUT"
STEPS=${*:-dry creds run1 run2 race conflicts list perf etag}
C=(--store nutanix --ca-bundle "$CA_BUNDLE" --region "$REGION")
acc() { local n=$1; shift; log "s3accept $n: $*"; "$S3ACCEPT" "$@" --out "$OUT/$n.json" 2>&1 | tee "$OUT/$n.txt"; }
st() { python3 -c "import json,sys; r=json.load(open(sys.argv[1])); print(' '.join(f\"{x['id']}={x['status']}\" for x in r['results']))" "$1" 2> /dev/null; }
result nutanix.objects_version "${OBJECTS_VERSION:-unrecorded}"
for s in $STEPS; do
  case $s in
    dry)   "$S3ACCEPT" --url "$URL" "${C[@]}" --dry-run | tee "$OUT/dry.txt" ;;
    creds) acc creds creds --url "$URL" "${C[@]}" --modes static,chain -v ;;
    hold)  acc hold creds --url "$URL" "${C[@]}" --modes static --hold 30m --every 60s ;;
    run1|run2)
           acc "$s" --url "$URL" "${C[@]}" -v
           result "nutanix.$s.verdicts" "$(python3 -c "import json,sys; r=json.load(open(sys.argv[1])); print(' '.join(c['id']+'='+c['verdict'] for c in r['capabilities']))" "$OUT/$s.json")"
           result "nutanix.$s.checks" "$(st "$OUT/$s.json")" ;;
    race)  # across every gateway: a store deciding the condition per gateway passes one and fails this
           [ -n "$EPS" ] || log "NUTANIX_IPS empty: the race runs through DNS only (one gateway, likely)"
           acc race --url "$URL" "${C[@]}" ${EPS:+--race-endpoints "$EPS"} --only create-only,if-match,create-race,cas-race,ambiguous-create,ambiguous-cas \
             --race-writers 32 --race-rounds 50 --cas-writers 16 --ambiguous 50 -v
           result nutanix.race "$(st "$OUT/race.json")" ;;
    conflicts)
           acc conflicts --url "$URL" "${C[@]}" ${EPS:+--race-endpoints "$EPS"} --only create-race --race-writers 64 --race-rounds 200 -v
           result nutanix.conflicts "$(python3 -c "import json,sys; r=json.load(open(sys.argv[1])); x=[c for c in r['results'] if c['id']=='create-race'][0]; print((x.get('data') or {}).get('conflicts_409'), x['status'], x['summary'])" "$OUT/conflicts.json")" ;;
    list)  acc list --url "$URL" "${C[@]}" ${EPS:+--race-endpoints "$EPS"} --only list,list-race,read-after-write,head-missing,metadata \
             --consistency 500 --race-writers 32 --list-lag-max 60s -v
           result nutanix.list "$(st "$OUT/list.json")" ;;
    perf)  acc perf1 --url "$URL" "${C[@]}" --only perf --perf-sizes 200B,1MB,3MiB,8MiB --perf-concurrency 1 --perf-ops 200
           acc perf8 --url "$URL" "${C[@]}" --only perf --perf-sizes 200B,1MB,3MiB,8MiB --perf-concurrency 8 --perf-ops 400 ;;
    etag)  # ETag = MD5 of the body on a single-part PUT? (AMBIGUITY X2: the entity lane resolves a 412 by it)
           tmp=$(mktemp -d); : > "$OUT/etag.txt"
           for sz in 1024 1048576 8388608; do
             head -c "$sz" /dev/urandom > "$tmp/o"
             k=$VPREFIX/etag/o-$sz
             e=$(awss3 s3api put-object --bucket "$BUCKET" --key "$k" --body "$tmp/o" --query ETag --output text | tr -d '"')
             m=$(md5sum < "$tmp/o" | cut -d' ' -f1)
             h=$(awss3 s3api head-object --bucket "$BUCKET" --key "$k" --query ETag --output text | tr -d '"')
             echo "size $sz: put ETag $e, HEAD ETag $h, MD5 $m -> $([ "$e" = "$m" ] && [ "$h" = "$m" ] && echo 'ETag = MD5' || echo 'ETag != MD5')" | tee -a "$OUT/etag.txt"
           done
           awss3 s3 rm --recursive --only-show-errors "s3://$BUCKET/$VPREFIX/etag/"; rm -rf "$tmp"
           result nutanix.etag "$(grep -c 'ETag = MD5' "$OUT/etag.txt") of 3 sizes ETag = MD5" ;;
    load)  for ph in ${LOAD_PLAN:-cluster-first:500:300 cluster-first:2000:300}; do
             IFS=: read -r layout rate secs <<< "$ph"
             acc "load-$layout-$rate" load --url "$ENDPOINT/$BUCKET/$VPREFIX/load" "${C[@]}" --load-layout "$layout" \
               --load-rate "$rate" --load-duration "${secs}s" --load-ramp 60s --load-workers 256
             result "nutanix.load.$layout.$rate" "$(grep -E '^\s+sent|first 503|per minute' "$OUT/load-$layout-$rate.txt" | tr -s ' ' | tr '\n' '|')"
             sleep 60
           done ;;
    *) die "unknown step $s" ;;
  esac
done
log "reports in $OUT"
