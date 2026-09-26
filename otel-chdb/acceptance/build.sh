#!/bin/sh
# Builds s3accept for this machine and, statically, for linux/amd64 and
# linux/arm64 (to kubectl cp into a pod), plus the local credential stand-ins.
set -eu
cd "$(dirname "$0")"
mkdir -p bin
(cd s3accept && go build -trimpath -ldflags='-s -w' -o ../bin/s3accept .)
for arch in amd64 arm64; do
  (cd s3accept && CGO_ENABLED=0 GOOS=linux GOARCH=$arch go build -trimpath -ldflags='-s -w' -o ../bin/s3accept-linux-$arch .)
done
# credstubs is stdlib-only; build it outside its (heavy) module.
GO111MODULE=off go build -o bin/credstubs ../parquetgo/compare/cmd/credstubs/main.go
ls -la bin
