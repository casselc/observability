---
id: incident-9f4041
label: CAST-51
batch: ci-sweep-after-d35-2026-09-29
title: "Per-push CI was red for about 29 hours (runs 38–95, 09-27 23:05Z to 09-29 04:16Z: 28 failed, ~30 cancelled, none green), and the nightly was red from its first runs, including a Kani harness that stopped compiling on 09-28 because it is only built nightly. Nobody read the results"
found_by: The coordinator, while answering the owner's question about dispatching model runs to CI
hazard: "Verification gap across everything: real bugs (row 50, the fenced-announcement seeds of row 42) sat in red runs; failures that appear only in CI (the service container's environment, races under `-race`, nightly-only compilation) were invisible locally"
controller: "**The coordinator:** \"each agent's local suites passed (the verification gate, row 41), so the branch is green\"; no role owned reading CI"
why: The gate made local evidence rigorous, and with several agents pushing to one branch, per-push status looked like noise (runs cancelling each other)
fix: CI green again (129f3a2, b078ca1, 397456b); the nightly's `jobs` input lets one job be rerun on any ref. **Coordinator rule from now on:** no agent report is accepted as done, and no work is reported to the owner as done, until the latest completed ci.yml and nightly runs at or after its commits have been read; a red run gets a named owner at once
lesson: "\"Tests pass locally\" and \"CI is green\" are different claims; a shared branch's CI status needs an explicit controller with a feedback loop, which here is the coordinator. Recurrence of the row 41/43 theme at the level above the agents"
state: accepted
---
