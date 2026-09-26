#!/bin/sh
# Exercises `s3accept creds` in every deployment credential mode against the
# local stand-ins (../../parquetgo/compare/cmd/credstubs) and SeaweedFS, so
# the tool itself is tested before it meets a real cluster:
#
#   static keys            AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
#   IRSA                   AWS_ROLE_ARN + AWS_WEB_IDENTITY_TOKEN_FILE → stub STS :18901
#   EKS Pod Identity       AWS_CONTAINER_CREDENTIALS_FULL_URI + token file → stub agent :18901
#   Roles Anywhere, serve  AWS_EC2_METADATA_SERVICE_ENDPOINT → stub IMDSv2 :18904
#   Roles Anywhere, process  a profile with credential_process (a script standing in for aws_signing_helper)
#   Nutanix-style          static keys, https endpoint with a private CA (TLS proxy :18903)
#   all at once            shows the chain's precedence warning
#   refresh                3-minute credentials held for 40 s: the SDK refreshes inside its window
#
# The stubs hand out the SeaweedFS key (otel/otelsecret) with an EMPTY session
# token: SeaweedFS rejects tokens its own STS did not issue.
#
# Usage: credcheck/local-standins.sh [results-file]   (from acceptance/)
# Needs: bin/s3accept, bin/credstubs (see RUNBOOK.md, "Build"), SeaweedFS on 127.0.0.1:18333.
set -u
HERE=$(cd "$(dirname "$0")/.." && pwd)
S3A=$HERE/bin/s3accept
STUBS=$HERE/bin/credstubs
OUT=${1:-$HERE/results/credcheck-local.txt}
W=$(mktemp -d "${TMPDIR:-/tmp}/credcheck.XXXXXX")
cd "$W" || exit 1

STUB_TTL=3m "$STUBS" >stubs.out 2>&1 </dev/null &
STUB_PID=$!
trap 'kill $STUB_PID 2>/dev/null; rm -rf "$W"' EXIT INT TERM
i=0
while [ ! -s ca.pem ] && [ $i -lt 50 ]; do i=$((i + 1)); sleep 0.1; done

# No trailing newline: aws-sdk-go-v2 rejects an Authorization token containing
# one ("invalid newline sequence"); the kubelet-projected token has none.
printf %s "eyJhbGciOiJSUzI1NiJ9.stand-in-projected-sa-token.sig" >token
cat >aws_signing_helper <<'EOF'
#!/bin/sh
# stands in for: aws_signing_helper credential-process --certificate ... --role-arn ...
echo "$(date -u +%H:%M:%S) $*" >>helper.calls
exp=$(date -u -d '+1 hour' +%Y-%m-%dT%H:%M:%SZ)
printf '{"Version":1,"AccessKeyId":"otel","SecretAccessKey":"otelsecret","Expiration":"%s"}\n' "$exp"
EOF
chmod +x aws_signing_helper
cat >aws-config <<EOF
[profile roles-anywhere]
region = us-east-1
credential_process = $W/aws_signing_helper credential-process --certificate /etc/ra/cert.pem --private-key /etc/ra/key.pem --trust-anchor-arn arn:aws:rolesanywhere:us-east-1:111122223333:trust-anchor/TA --profile-arn arn:aws:rolesanywhere:us-east-1:111122223333:profile/PR --role-arn arn:aws:iam::111122223333:role/otel-edge
EOF

# A clean environment per case: nothing inherited but PATH/HOME, so the
# chain sees only what each case sets.
CONN="--endpoint http://127.0.0.1:18333 --bucket otel --prefix accept --store seaweedfs"
run() {
  name=$1
  shift
  echo
  echo "=================== $name"
  env -i PATH="$PATH" HOME="$W" "$@" 2>&1 | grep -v '^JSON report'
}
{
  echo "credcheck against local stand-ins, $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  run "static keys" AWS_ACCESS_KEY_ID=otel AWS_SECRET_ACCESS_KEY=otelsecret \
    "$S3A" creds $CONN --out static.json -v
  run "IRSA (web identity; STS stand-in via AWS_ENDPOINT_URL_STS)" \
    AWS_ROLE_ARN=arn:aws:iam::111122223333:role/otel-edge AWS_WEB_IDENTITY_TOKEN_FILE="$W/token" \
    AWS_ENDPOINT_URL_STS=http://127.0.0.1:18901 AWS_REGION=us-east-1 \
    "$S3A" creds $CONN --out irsa.json -v
  run "EKS Pod Identity (container credentials stand-in)" \
    AWS_CONTAINER_CREDENTIALS_FULL_URI=http://127.0.0.1:18901/v1/credentials \
    AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE="$W/token" \
    "$S3A" creds $CONN --out pod.json -v
  run "Roles Anywhere via aws_signing_helper serve (IMDSv2 stand-in)" \
    AWS_EC2_METADATA_SERVICE_ENDPOINT=http://127.0.0.1:18904 \
    "$S3A" creds $CONN --out imds.json -v
  run "Roles Anywhere via credential_process (profile)" \
    AWS_CONFIG_FILE="$W/aws-config" AWS_PROFILE=roles-anywhere \
    "$S3A" creds $CONN --out process.json -v
  echo "credential_process helper invocations: $(wc -l <helper.calls 2>/dev/null || echo 0)"
  run "Nutanix-style: static keys, https endpoint, private CA (--ca-bundle)" \
    AWS_ACCESS_KEY_ID=otel AWS_SECRET_ACCESS_KEY=otelsecret \
    "$S3A" creds --endpoint https://127.0.0.1:18903 --bucket otel --prefix accept --store nutanix-standin \
    --ca-bundle "$W/ca.pem" --out ca.json -v
  run "same, WITHOUT the CA (must fail with an x509 error)" \
    AWS_ACCESS_KEY_ID=otel AWS_SECRET_ACCESS_KEY=otelsecret \
    "$S3A" creds --endpoint https://127.0.0.1:18903 --bucket otel --prefix accept --store nutanix-standin \
    --out noca.json
  run "everything configured at once (static keys shadow IRSA and Pod Identity)" \
    AWS_ACCESS_KEY_ID=otel AWS_SECRET_ACCESS_KEY=otelsecret \
    AWS_ROLE_ARN=arn:aws:iam::111122223333:role/otel-edge AWS_WEB_IDENTITY_TOKEN_FILE="$W/token" \
    AWS_ENDPOINT_URL_STS=http://127.0.0.1:18901 AWS_REGION=us-east-1 \
    AWS_CONTAINER_CREDENTIALS_FULL_URI=http://127.0.0.1:18901/v1/credentials \
    AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE="$W/token" \
    "$S3A" creds $CONN --out all.json
  run "refresh: Pod Identity credentials valid 3 min, held 40 s (a HEAD every 10 s)" \
    AWS_CONTAINER_CREDENTIALS_FULL_URI=http://127.0.0.1:18901/v1/credentials \
    AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE="$W/token" \
    "$S3A" creds $CONN --modes chain --hold 40s --every 10s --out hold.json -v
  echo
  echo "=================== stand-in request log (stub.log), by kind"
  jq -r '[.m, (.form.Action // .path // "")] | join(" ")' stub.log 2>/dev/null | sort | uniq -c
  echo
  echo "=================== full s3accept run over TLS through the private-CA proxy (trailing checksums need TLS)"
  env -i PATH="$PATH" HOME="$W" AWS_ACCESS_KEY_ID=otel AWS_SECRET_ACCESS_KEY=otelsecret \
    "$S3A" --endpoint https://127.0.0.1:18903 --bucket otel --prefix accept --store seaweedfs-tls \
    --ca-bundle "$W/ca.pem" --perf-ops 20 --perf-sizes 100KB --race-rounds 5 --ambiguous 5 \
    --out "$HERE/results/s3accept-seaweedfs-tls.json" 2>&1
} | tee "$OUT"
