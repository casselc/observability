---
id: incident-a2b397
label: CAST-75
batch: models-and-verification-2026-09-29
title: "A consumer worker tombstoned an already retired epoch at slot 0 and checkpointed it as closed at 0 after its slots had been ingested (noCommitAfterClose): its own checkpoint write that had compacted the epoch away got no answer and landed late, and the take-back (`refresh_own_ckpt`) pruned known epochs only by the floor, so the compacted epoch above the floor read as open at slot 0"
found_by: "Nightly run 47, dst level 1, seed 4709496 (1 of 10,000); reproduced at 33b4840 and 327eace"
hazard: "H-2 (the checkpoint misstates what was ingested; readers of it are misled); minor H-7 (a key leaked into a retired epoch)"
controller: "The worker's checkpoint take-back: \"every known epoch above the floor is live, and one absent from the checkpoint is new at slot 0\""
why: "The take-back (CAST-50) was written for a late write that advances positions and landed alongside compaction (eb7f1ec); the answered path prunes the dropped epochs from a local list the late path does not have; it needs a lost answer, a late landing and a non-contiguous compaction on one write"
fix: "`refresh_own_ckpt` also forgets every epoch the held checkpoint had and the late version lacks (only compaction removes epochs); unit test and pinned seed `a_late_checkpoint_that_compacted_an_epoch_does_not_reopen_it`; commit 48ab0d7; nightly 49 dst green (10,000 + 200 seeds)"
lesson: "When a second path adopts a state a first path writes, give both the same bookkeeping, derived from the before and after states rather than from locals only one path has; invariants checked at every checkpoint write, not only at the end, are what caught it"
state: accepted
---
