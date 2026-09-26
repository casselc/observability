#!/bin/bash
# Step 0 bisect: build encbench + otap-s3pq at each revision into the shared
# target dir, one after the other, and keep a copy of the two binaries per
# revision in $S/sorting/bin/<name>/. The revision's src/ is checked out into
# the working tree for the build and HEAD's src/ is restored afterwards
# (a git worktree would change the .upstream path dependencies' identity
# and rebuild all of upstream: ~1.5 GB more target dir than the disk has).
#
#   pre     8cf80ad  (edge code identical to 132ad94 = d4bb951^; the earlier
#                     results/series/bench.jsonl was measured on this code,
#                     uncommitted, 45 min before it was committed)
#   head    HEAD     (edge code = d4bb951 + the lane-epoch change in runner.rs)
#   head-nodur HEAD built without the durable-buffer feature (jemalloc only)
# Skips a variant whose binaries already exist. Stops if free disk < 2.5 GB.
set -eu
S=/tmp/claude-0/-home-user/db86342c-d57d-54b7-95b1-90f220828b73/scratchpad
export CARGO_TARGET_DIR=$S/otap-rs-target CARGO_BUILD_JOBS=4 PATH=$HOME/.cargo/bin:$PATH
here=$(cd "$(dirname "$0")" && pwd)
rs=$(cd "$here/../../../otap-rs" && pwd)
free() { df -B1 --output=avail / | tail -1 | awk '{printf "%.2f", $1/1e9}'; }
build() { # name rev features...
  local name=$1 rev=$2; shift 2
  local out=$S/sorting/bin/$name
  [ -x "$out/encbench" ] && [ -x "$out/otap-s3pq" ] && { echo "have $name"; return 0; }
  awk -v f="$(free)" 'BEGIN{exit !(f >= 2.5)}' || { echo "disk $(free) GB < 2.5, stop" >&2; exit 1; }
  cd "$rs"
  if [ "$rev" != HEAD ]; then git checkout "$rev" -- src; fi
  trap 'cd "$rs" && git checkout HEAD -- src' EXIT
  cargo build --release --bin encbench --bin otap-s3pq "$@" 2>&1 | tail -3
  git checkout HEAD -- src; trap - EXIT
  mkdir -p "$out"
  cp -f "$CARGO_TARGET_DIR/release/encbench" "$CARGO_TARGET_DIR/release/otap-s3pq" "$out/"
  echo "built $name ($rev $*) free $(free) GB" | tee -a "$here/build.log"
}
build pre 8cf80ad
build head-nodur HEAD --no-default-features --features jemalloc
build head HEAD
