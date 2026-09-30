---
id: incident-493b69
label: CAST-79
batch: models-and-verification-2026-09-29
title: "Five test-data generators meant to produce negative zero wrote `-0.0`, which in Go is a constant equal to positive zero, so no test ever saw a negative zero"
found_by: "staticcheck SA4026, from the new per-push staticcheck job"
hazard: "Test-data gap: float edge cases (sorting, equality, Parquet statistics) untested"
controller: "The generator author: \"Go's `-0.0` is negative zero\""
why: "It is negative zero at run time in most languages, and Go's constant rules are easy to miss"
fix: "`math.Copysign(0, -1)`; staticcheck runs over test code in ci.yml (commit d76c5ed)"
lesson: "Run the linters over test code too; a generator's edge cases need a check that they are actually produced"
state: accepted
---
