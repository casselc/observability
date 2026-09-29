---
id: incident-31fa4c
label: CAST-30
batch: closing-the-validation-gaps-2026-09-28
title: "All four role policies in `deploy/iam` (`edge-publisher`, `entity-controller`, `consumer`, `gc`) granted `s3:ListBucket` only under an `s3:prefix` condition. A HEAD/GET carries no `s3:prefix`, so on AWS a missing key answers 403, not 404: the consumer cannot read a missing `format.json` and exits; edge slots stay unresolved after any ambiguous PUT (403 is correctly kept unknown)"
found_by: The validation runbook's EKS-4 question; confirmed from AWS's HeadObject and IAM condition-operator documentation and a SeaweedFS experiment. Not yet run on AWS [D]
hazard: "Liveness only, no safety loss (a 403 stays unknown): consumer start and first lease fail; lanes stall (R-S5 lag, stale `complete_through`); GC and the entity controller alike"
controller: "Policy author (D18): \"ListBucket on my prefix covers the 404 check\""
why: The SeaweedFS ABAC demo showed "free slot 404" — but it granted ListBucket unconditionally, and SeaweedFS 4.47 answers 404 with no ListBucket at all and ignores `StringLikeIfExists`, so the demo could not see the rule
fix: A `StringLikeIfExists` grant on the same prefixes in all four policies (a LIST of another cluster's prefix still denied; write-side ABAC unchanged); `ci/iam-lint.sh` fails on the old shape; Rust `a_403_on_head_is_an_error_not_a_free_slot`, Go `TestHeadOnlyA404IsFree`; EKS-4 now checks the free-slot 404; commit 09ed92e
lesson: A test store more permissive than the target cannot validate an authorisation rule; label such results [D] until they run on the real store
state: accepted
---
