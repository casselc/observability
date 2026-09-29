---
id: incident-bcd0c3
label: CAST-53
batch: ci-sweep-after-d35-2026-09-29
title: "Regression in the row 52 fix: the single-grant fallback in `sqlscope` read a literal `\"*\"` inside a cluster or namespace list as \"all\", and trimmed padded names (`\"a \"` → `\"a\"`); both had been refused as `bad_scope_value`. Also, `f846d66` was pushed after a local run that showed the failure, because a pipe hid the test's exit code"
found_by: "The existing rapid test `TestScopeValueProperty`, on a later run: its generator rarely produces `\"*\"`, so the local runs and CI on a4c5470 passed"
hazard: "H-6 / H-G1, latent: every scope the server builds comes from the principal's pairs, where `\"*\"` is intended"
controller: "The D38 agent: \"a `*` in a list means every value, as it does in grants\" — reusing the new representation's wildcard in the old one"
why: Unifying two representations looked like sharing one meaning of `*`
fix: Old-style lists keep `*` as a literal, invalid name and never trim; `TestLiteralStarAndSpacesAreNotNames` fails on the old code; commit ad0e7a2. Agents now run tests with `set -o pipefail` before pushing
lesson: When two representations are unified, a wildcard in one must not become a wildcard in the other; a weak generator needs a fixed edge-case test beside it; a test run whose exit status is lost is an unrun test (row 41)
state: accepted
---
