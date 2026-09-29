---
id: incident-397971
label: CAST-55
batch: ci-sweep-after-d35-2026-09-29
title: "Agents dispatching the nightly with different `jobs` selections cancelled each other's queued runs: the dispatched lake UI e2e run (8) was replaced by another agent's dispatch (9) before it started, so the plan tail's first real e2e never ran"
found_by: The coordinator, watching run 8 end `cancelled` with no jobs
hazard: Verification gap (the check that decides "done" under row 51 silently does not happen)
controller: "The workflow's concurrency: one group per ref; GitHub keeps only the newest *pending* run of a group; the CI fixer who added the `jobs` input (and the coordinator) assumed dispatches queue like pushes"
why: "Row 51's fix (queue instead of cancel) looked sufficient; the pending-run replacement rule applies even with `cancel-in-progress: false`"
fix: The group includes the selection (`nightly-<ref>-<jobs>`), commit 9c3e210; the e2e re-dispatched
lesson: A queue that keeps only the newest pending entry is not a queue for distinct requests; key concurrency on what the request *is*, not only where it came from
state: accepted
---
