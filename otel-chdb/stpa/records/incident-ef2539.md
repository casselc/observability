---
id: incident-ef2539
label: CAST-59
batch: d36-phase1-and-ci-2026-09-29
title: "The Go-vs-Rust conformance comparison passed while comparing less data: both edges refused the two largest corpus requests (85 MB and 78 MB) as over the then 64 MiB request cap, identically, so nothing differed; `run.sh` logged the refusals and did not fail"
found_by: "The D36 agent, from row counts in nightly 17's artifact (otel_traces 3,531 where 4,231 were expected)"
hazard: "Verification gap on D1 (edge equivalence) and H-2: a differential check that silently drops inputs"
controller: "`conformance/run.sh`: \"compare passes\" was taken to mean \"everything was compared\""
why: "The request cap was new (D36 phase 1) and both edges applied it the same way, which is exactly what the comparison rewards"
fix: "`run.sh` fails on any refused or failed send; the default cap is 128 MiB (the receivers' body limit) and the conformance run raises it to 256 MiB; commit 2e9e543; nightly 23 green with the check"
lesson: "A differential test must check that both sides consumed every input, not only that their outputs agree"
state: accepted
---
