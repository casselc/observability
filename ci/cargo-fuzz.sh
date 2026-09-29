#!/usr/bin/env bash
# Runs every cargo-fuzz target of otel-chdb/otap-rs/fuzz (the parsers of
# otap-s3pq that read less-trusted input: slot keys, object metadata, the
# offloader's JSON) for FUZZTIME seconds each, one at a time (ci/README.md,
# nightly job `fuzz`, beside ci/fuzz.sh's Go targets). Needs a nightly
# toolchain (FUZZ_TOOLCHAIN, default nightly) and cargo-fuzz on PATH, and
# otap-rs/.upstream prepared (scripts/fetch-upstream.sh).
#
#   ci/cargo-fuzz.sh [TARGET...]
#
# The fuzz crate resolves against otap-rs's Cargo.lock (copied in), so the
# shared dependencies are the versions the crate is tested with. Env:
# FUZZTIME (default 60), OUT: a failing target's crash inputs are copied
# there for upload. Exit status non-zero if any target failed.
set -uo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root/otel-chdb/otap-rs/fuzz" || exit 1
tc=${FUZZ_TOOLCHAIN:-nightly}
cp -f ../Cargo.lock Cargo.lock
targets=("$@")
[ ${#targets[@]} -eq 0 ] && mapfile -t targets < <(cargo "+$tc" fuzz list)
failed=()
for t in "${targets[@]}"; do
  echo "::group::cargo fuzz $t (${FUZZTIME:-60} s)"
  if ! cargo "+$tc" fuzz run "$t" -- -max_total_time="${FUZZTIME:-60}" -rss_limit_mb=4096; then
    failed+=("$t")
    echo "::error::cargo fuzz target $t failed (artifacts/$t)"
    if [ -n "${OUT:-}" ] && [ -d "artifacts/$t" ]; then mkdir -p "$OUT/rust-$t" && cp -f "artifacts/$t"/* "$OUT/rust-$t/"; fi
  fi
  echo "::endgroup::"
done
echo "${#targets[@]} cargo-fuzz targets, ${#failed[@]} failed ${failed[*]}"
[ ${#failed[@]} -eq 0 ]
