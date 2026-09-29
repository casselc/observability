#!/bin/bash
# Build and push the images the validation runs, to ECR repositories this run
# creates (down.sh deletes them):
#   otap-s3pq       the Rust publisher (deploy/images/otap-s3pq.Dockerfile)
#   otelcol-deploy  the agent (deploy/collector/build.sh)
#   otelcol-deploy-lbpatched  the routing gateway (PATCHED=1 build.sh: collector/patches/0001)
#   otelcol-s3pq    the Go publisher (awss3/collector/builder-config.yaml), if EDGES has go
#   toolbox         consume + otlpsend, faultproxy2, mixgen, s3accept, entityctl, aggregator, ridcheck
#
#   RUN=v1 REGION=us-east-1 [TAG=$RUN EDGES="rust go" PLATFORM=linux/amd64] eks/images.sh
#   RUN=v1 REGISTRY=registry.site.example/otel [TAG=...] eks/images.sh     # any other registry (Nutanix site):
#                                                                        # no ECR, `docker login` done beforehand
#
# Needs docker (buildx), go 1.27, and ~15 GB of free disk for the Rust stages.
# Writes $STATE/images.env (the image references deploy.sh uses).
# shellcheck source=../lib.sh
. "$(dirname "$0")/../lib.sh"
need docker go
REGION=${REGION:-us-east-1}
TAG=${TAG:-$RUN}
EDGES=${EDGES:-rust go}
PLATFORM=${PLATFORM:-linux/amd64}
B=$STATE/build
mkdir -p "$B/bin" "$B/otelcol"
if [ -n "${REGISTRY:-}" ]; then
  img() { echo "$REGISTRY/$1:$TAG"; }
else
  need aws
  ACCOUNT=$(aws sts get-caller-identity --query Account --output text) || die "no AWS credentials"
  REG=$ACCOUNT.dkr.ecr.$REGION.amazonaws.com
  PFX=${REPO_PREFIX:-otel-validate-$RUN}
  for r in otap-s3pq otelcol-deploy otelcol-deploy-lbpatched otelcol-s3pq toolbox; do
    if ! aws ecr describe-repositories --region "$REGION" --repository-names "$PFX/$r" > /dev/null 2>&1; then
      aws ecr create-repository --region "$REGION" --repository-name "$PFX/$r" \
        --tags Key=created-by,Value=otel-chdb-validation Key=run,Value="$RUN" > /dev/null || die "create-repository $PFX/$r"
      created ecr-repo "$PFX/$r" "$REGION"
    fi
  done
  aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$REG" > /dev/null || die "docker login"
  img() { echo "$REG/$PFX/$1:$TAG"; }
fi
pushed() { docker manifest inspect "$1" > /dev/null 2>&1; }

# The Go tools, static.
( cd "$OTEL_CHDB/otap-rs/tools" && CGO_ENABLED=0 go build -trimpath -ldflags=-s -o "$B/bin/" ./cmd/otlpsend ./cmd/faultproxy2 ) || die "otap-rs tools"
( cd "$OTEL_CHDB/bench/sorting/gen" && CGO_ENABLED=0 go build -trimpath -ldflags=-s -o "$B/bin/mixgen" . ) || die mixgen
( cd "$OTEL_CHDB/acceptance/s3accept" && CGO_ENABLED=0 go build -trimpath -ldflags=-s -o "$B/bin/s3accept" . ) || die s3accept
( cd "$OTEL_CHDB/entities/controller" && CGO_ENABLED=0 go build -trimpath -ldflags=-s -o "$B/bin/" ./cmd/entityctl ./cmd/aggregator ./cmd/ridcheck ) || die entities

if ! pushed "$(img toolbox)"; then
  docker buildx build --platform "$PLATFORM" -f "$VALIDATION_DIR/eks/images/toolbox.Dockerfile" --build-context bin="$B/bin" \
    -t "$(img toolbox)" --push "$OTEL_CHDB/otap-rs" || die "toolbox image"
fi
if [[ " $EDGES " == *" rust "* ]] && ! pushed "$(img otap-s3pq)"; then
  docker buildx build --platform "$PLATFORM" -f "$DEPLOY_DIR/images/otap-s3pq.Dockerfile" --build-context edgeprobe="$DEPLOY_DIR/edgeprobe" \
    -t "$(img otap-s3pq)" --push "$OTEL_CHDB/otap-rs" || die "otap-s3pq image"
fi
# otelcol.Dockerfile copies edgeprobe/ and the binary from its context: a small one.
otelcol_image() { # binary image
  local ctx; ctx=$B/ctx-$(basename "$1"); rm -rf "$ctx"; mkdir -p "$ctx"
  cp -r "$DEPLOY_DIR/edgeprobe" "$ctx/edgeprobe"; cp "$1" "$ctx/"
  docker buildx build --platform "$PLATFORM" -f "$DEPLOY_DIR/images/otelcol.Dockerfile" --build-arg BIN="$(basename "$1")" \
    -t "$2" --push "$ctx" || die "image $2"
}
if ! pushed "$(img otelcol-deploy)"; then
  CGO_ENABLED=0 "$DEPLOY_DIR/collector/build.sh" "$B/otelcol/deploy" || die "ocb otelcol-deploy"
  otelcol_image "$B/otelcol/deploy/otelcol-deploy" "$(img otelcol-deploy)"
fi
if ! pushed "$(img otelcol-deploy-lbpatched)"; then
  CGO_ENABLED=0 PATCHED=1 "$DEPLOY_DIR/collector/build.sh" "$B/otelcol/lbpatched" || die "ocb otelcol-deploy (patched)"
  otelcol_image "$B/otelcol/lbpatched/otelcol-deploy" "$(img otelcol-deploy-lbpatched)"
fi
if [[ " $EDGES " == *" go "* ]] && ! pushed "$(img otelcol-s3pq)"; then
  mkdir -p "$B/otelcol/s3pq"
  sed "s#@AWSS3@#$OTEL_CHDB/awss3#; s#@OUT@#$B/otelcol/s3pq#" "$OTEL_CHDB/awss3/collector/builder-config.yaml" > "$B/otelcol/s3pq/builder.yaml"
  ( cd "$B/otelcol/s3pq" && CGO_ENABLED=0 go run go.opentelemetry.io/collector/cmd/builder@v0.161.0 --config builder.yaml ) || die "ocb otelcol-s3pq"
  otelcol_image "$B/otelcol/s3pq/otelcol-s3pq" "$(img otelcol-s3pq)"
fi
cat > "$STATE/images.env" <<EOF
IMG_OTAP=$(img otap-s3pq)
IMG_OTELCOL=$(img otelcol-deploy)
IMG_GATEWAY=$(img otelcol-deploy-lbpatched)
IMG_GOPUB=$(img otelcol-s3pq)
IMG_TOOLBOX=$(img toolbox)
EOF
cat "$STATE/images.env"
