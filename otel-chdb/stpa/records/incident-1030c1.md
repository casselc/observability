---
id: incident-1030c1
label: CAST-68
batch: go-upgrade-and-verification-2026-09-29
title: "CI runs 133 and 135 died with \"No space left on device\" on the runner: Go caches for two toolchain versions plus the Rust build and the runner's preinstalled SDKs filled its disk; nightly build and rust-integration died the same way (runs 21, 22, 24)"
found_by: "The runs' own logs (the runner's disk, before any test)"
hazard: "Verification gap: jobs die before testing, and traceability reports every tag unrun"
controller: "The CI jobs: \"the hosted runner's disk is ample\""
why: "It had been, until the Go upgrade doubled the caches and D36 grew the Rust build"
fix: "Go caches keyed by Go version (e9cf1ba); jobs that build Rust free the runner's unused SDKs first (4d01506, 2bf50a1, 70eced3)"
lesson: "Disk is a budget on the CI runner as on the dev box; the jobs that fill it should make room first"
state: accepted
---
