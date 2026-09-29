---
id: incident-16bfa2
label: CAST-12
batch: initial
title: Tests ran against binaries built from a different tree
found_by: Our own agent coordination
hazard: H-2 (for results)
controller: "Development process: a shared build directory"
why: Faster builds
fix: Separate build directories per checkout; rebuild before trusting a run
lesson: "Evidence needs provenance: record what was tested"
state: accepted
---
