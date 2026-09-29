---
id: incident-7dd538
label: CAST-61
batch: d36-phase1-and-ci-2026-09-29
title: "The nightly chdb comparison (CAST-46's only regression test) went red after two unrelated changes, neither of which dispatched it: the Go 1.27 upgrade changed how map and slice attributes render (121 differing trace rows), and the D36 offloader added `payload_refs`/`payloads` columns the chdb prototype does not write"
found_by: "The traceability job, from a failed tagged record in the nightly chdb job (runs 36538078015, 36543103313)"
hazard: "H-2 (rows differ silently between engines); the D1 row-identity check unavailable"
controller: "The Go upgrade's and the offloader's verification: \"vet, fast tests and the nightlies we dispatched cover it\"; chdb is not in per-push CI and was not among the dispatched jobs"
why: "chdb had been green hours earlier, and neither change looked like it touched the chdb path"
fix: "Rows compared modulo the documented U+FFFD difference (2b1e6aa); the schema check strips the two edge-only payload columns as it already did the edge-only resource columns (2bf50a1); nightly 23 chdb green"
lesson: "A change must dispatch every nightly job that reads what it changes (toolchain: all jobs it builds; schema: every reader); the traceability job now makes a red CAST test impossible to miss"
state: accepted
---
