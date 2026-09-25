#!/bin/sh
# Prepares the upstream otel-arrow checkout this crate builds against:
# a shallow clone at the commit in ../UPSTREAM, plus ../patches/*.patch,
# linked at ../.upstream (gitignored). The clone lives outside the repo.
#
#   scripts/fetch-upstream.sh [DIR]     # DIR defaults to $UPSTREAM_DIR or /tmp/otel-arrow-otaprs
set -eu
here=$(cd "$(dirname "$0")/.." && pwd)
rev=$(cat "$here/UPSTREAM")
dir=${1:-${UPSTREAM_DIR:-/tmp/otel-arrow-otaprs}}
if [ ! -d "$dir/.git" ]; then
  git init -q "$dir"
  git -C "$dir" remote add origin https://github.com/open-telemetry/otel-arrow
fi
git -C "$dir" fetch -q --depth 1 origin "$rev"
git -C "$dir" checkout -q -f FETCH_HEAD
git -C "$dir" reset -q --hard
for p in "$here"/patches/*.patch; do
  git -C "$dir" apply "$p"
done
ln -sfn "$dir" "$here/.upstream"
echo "upstream $rev with $(ls "$here"/patches/*.patch | wc -l) patches at $dir -> $here/.upstream"
