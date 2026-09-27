#!/bin/sh
# Prepares the upstream otel-arrow checkout this crate builds against:
# a shallow clone at the commit in ../UPSTREAM, plus ../patches/[0-9]*.patch,
# linked at ../.upstream (gitignored). The clone lives outside the repo.
# Also the patched tonic of Cargo.toml's [patch.crates-io]: the crates.io
# crate at $tonic_version (checksum-verified; taken from cargo's cache when
# there, and kept for later runs) plus ../patches/tonic-*.patch, in
# .otap-rs/tonic inside the clone (excluded from its git status).
#
#   scripts/fetch-upstream.sh [DIR]     # DIR defaults to $UPSTREAM_DIR or /tmp/otel-arrow-otaprs
set -eu
here=$(cd "$(dirname "$0")/.." && pwd)
rev=$(cat "$here/UPSTREAM")
dir=${1:-${UPSTREAM_DIR:-/tmp/otel-arrow-otaprs}}
tonic_version=0.14.6
tonic_sha256=ac2a5518c70fa84342385732db33fb3f44bc4cc748936eb5833d2df34d6445ef
if [ ! -d "$dir/.git" ]; then
  git init -q "$dir"
  git -C "$dir" remote add origin https://github.com/open-telemetry/otel-arrow
fi
grep -qx '/.otap-rs/' "$dir/.git/info/exclude" 2>/dev/null || echo '/.otap-rs/' >> "$dir/.git/info/exclude"
git -C "$dir" fetch -q --depth 1 origin "$rev"
git -C "$dir" checkout -q -f FETCH_HEAD
git -C "$dir" reset -q --hard
git -C "$dir" clean -q -fd   # files a patch added on an earlier run (ignored ones, e.g. target/, stay)
for p in "$here"/patches/[0-9]*.patch; do
  git -C "$dir" apply "$p"
done

vendor=$dir/.otap-rs
crate=$vendor/tonic-$tonic_version.crate
mkdir -p "$vendor"
if ! echo "$tonic_sha256  $crate" | sha256sum -c --status 2>/dev/null; then
  cached=$(ls "${CARGO_HOME:-$HOME/.cargo}"/registry/cache/*/"tonic-$tonic_version.crate" 2>/dev/null | head -n 1)
  if [ -n "$cached" ]; then cp -f "$cached" "$crate.tmp"
  else curl -sSfL -o "$crate.tmp" "https://static.crates.io/crates/tonic/tonic-$tonic_version.crate"; fi
  echo "$tonic_sha256  $crate.tmp" | sha256sum -c --status || { echo "tonic-$tonic_version.crate: checksum mismatch" >&2; exit 1; }
  mv -f "$crate.tmp" "$crate"
fi
# re-extract only when the crate or its patches changed (keeps cargo's fingerprints)
stamp=$(cat "$crate" "$here"/patches/tonic-*.patch | sha256sum | cut -d' ' -f1)
if [ "$(cat "$vendor/tonic.stamp" 2>/dev/null)" != "$stamp" ]; then
  rm -rf "$vendor/tonic" "$vendor/tonic.stamp"
  mkdir -p "$vendor/tonic"
  tar -xzf "$crate" -C "$vendor/tonic" --strip-components=1
  for p in "$here"/patches/tonic-*.patch; do
    patch -d "$vendor/tonic" -p1 -s -f --no-backup-if-mismatch < "$p"
  done
  echo "$stamp" > "$vendor/tonic.stamp"
fi

ln -sfn "$dir" "$here/.upstream"
echo "upstream $rev with $(ls "$here"/patches/[0-9]*.patch | wc -l) patches at $dir -> $here/.upstream;" \
  "tonic $tonic_version with $(ls "$here"/patches/tonic-*.patch | wc -l) at $vendor/tonic"
