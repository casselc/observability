---
id: incident-507127
label: CAST-23
batch: entity-announcement-lane-2026-09-28
title: An unanswered announcement insert was not waited out, so it could land after the lane's lease changed hands
found_by: "`dst_consumer`, 2 of 40 seeds (\"statement lands after its lease epoch changed hands\"), before the code was committed"
hazard: "H-2 (a statement outside its lease: the fencing that prevents duplicates no longer holds)"
controller: "Consumer worker: \"announcement inserts are idempotent, so a failed or unanswered one needs no settle wait\""
why: A late duplicate announcement is harmless to the data, and the announcement table folds copies; but a TIMEOUT_EXCEEDED can still commit after the lease moved, which breaks the invariant every statement must hold
fix: An unanswered announcement statement is waited out like any data insert (the D9 rule); `dst_consumer` 300 seeds and `hegel_dst` clean
lesson: Idempotence of the effect does not exempt a statement from the lease discipline; safety rules apply per statement, not per table
variables: [consumer/statement]
state: accepted
---
