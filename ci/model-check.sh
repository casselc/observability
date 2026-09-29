#!/usr/bin/env bash
# Runs otap-rs/scripts/consumer_model.sh (the consumer models: design
# simulations, witnesses, mutants, scripted counterexamples) and turns its
# report into an exit status. consumer_model.sh itself always exits 0.
#
#   OUT=report.txt ci/model-check.sh
#
# The rules are ci/model-verdict.sh's. It fails when
#   - a simulation expected `ok` (a design instance) reports VIOLATED,
#     i.e. the model found a counterexample to a design's invariant;
#   - a witness or mutant expected VIOLATED is reported with anything but
#     ok/VIOLATED (quint errored), or a `quint test` group has failures or
#     ran nothing.
# Warns (does not fail) when a witness `not(w)` is not reached (a coverage
# gap: the scenario did not occur within the samples), or when a mutant
# expected VIOLATED comes out `ok`:
# random simulation does not always reach those counterexamples within the
# sample budget (the recorded report, otap-rs/results/consumer/scale/model.txt,
# has four), and each such mutant is pinned instead by its scripted
# `*BreaksTest` run, which is checked above.
set -uo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
OUT=${OUT:-${RUNNER_TEMP:-/tmp}/consumer-model.txt}
OUT="$OUT" "$root/otel-chdb/otap-rs/scripts/consumer_model.sh"
exec "$root/ci/model-verdict.sh" "$OUT"
