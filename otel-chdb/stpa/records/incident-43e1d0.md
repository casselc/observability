---
id: incident-43e1d0
label: CAST-45
batch: service-gate-sweep-2026-09-29
title: D33 added `cluster` to the key/value rollups; conformance's `compare.py` listed the rollup columns by hand, so every conformance run since D33 died in the comparison
found_by: The conformance re-run at cdd1e6f (the verification gate, row 41)
hazard: "The Go = Rust row-identity check (D1) silently unavailable: a divergence between the edges would not be caught"
controller: "D33 agent: \"a schema change touches the consumer and the query service\"; coordinator: accepted D33 without the nightly-only conformance run"
why: Conformance is not in per-push CI; the change looked local to the rollup DDL
fix: Group by every column but `count`, derived from the DDL; `conformance/compare_test.py` checks the query against the DDL and runs nightly; commit fc009af
lesson: A check that restates a schema must derive it from the schema; a schema change must run every consumer of that schema, not only the per-push ones
state: accepted
---
