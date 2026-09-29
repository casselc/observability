---
id: incident-c31829
label: CAST-36
batch: basis-2026-09-28
title: "Near miss: raising evaluator concurrency, the coordinator first added an `alert-evaluator` entry to queryd's `group_grants` (fleet scope) as well as to `limits`, and told team evaluators to join the group; a team identity would have gained fleet scope"
found_by: The coordinator's own re-read before committing
hazard: "H-6 / R-S8: a scope-restricted identity reads every cluster"
controller: "Coordinator: \"a group name in the config means one thing\"; but group names key both grants and limits, and grants are unioned across a principal's groups"
why: The example config had one group (`sre`) that was both a grant and a limits tier, so the two looked like one concept
fix: Grant and limits groups separated (`alertd-fleet` grants fleet scope; `alert-evaluator` is limits-only and grants nothing); commit 1414bb4
lesson: Where one name keys two policies and one of them is a union, adding a principal to a group for one reason grants the other; a test that a limits-only group never changes scope would make it structural
state: accepted
---
