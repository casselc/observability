---
id: incident-8d77a3
label: CAST-82
batch: models-and-verification-2026-09-29
title: "The CAST-74 lease-renewal code shipped with three mutants its tests did not kill: dropping `wall_ms` in `renew_after`, and two `||` to `&&` swaps in the `own_late_renewal` guard (the owner check survived the first repair because every pending doc in the test was our own)"
found_by: "Mutation nightlies 46 and 48"
hazard: "Verification gap on CAST-74's fix (H-2): a regression in the owner, epoch or beat check could pass the tests"
controller: "The unit tests: \"the find over pending docs covers the owner and epoch checks\""
why: "Every pending doc really is ours in production, so tests built from production-like state never exercised the refusal"
fix: "Tests of the function's contract, including another owner's and another epoch's lease listed as pending (commits 327eace, fda9c97); nightly 50: 0 new survivors"
lesson: "Test a guard with inputs that should be refused, not only with realistic ones; a baseline key that names two mutation sites can hide a half-killed pair"
state: accepted
---
