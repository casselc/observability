---
id: incident-f82ee3
label: CAST-50
batch: ci-sweep-after-d35-2026-09-29
title: "A consumer checkpoint PUT got no answer; the read-back showed the checkpoint unchanged, so the worker kept its old version (17), but the store applied the write later (next = 21). GC acts on the stored checkpoint and deleted slots 17–19; the worker's scan then waited at a \"gap\" at 17 forever: the lane stalled for good and nothing after it was ingested"
found_by: Nightly DST Level 1, seed 504836 (10,000 new seeds per night); reproduced at HEAD and root-caused from the trace
hazard: "H-2 class (liveness: a lane never advances, `complete_through` stalls for its cluster — visible as `stale`), and data not ingested until an operator intervened; no duplicate and no silent wrong answer"
controller: "Consumer worker's checkpoint writer: \"no answer, then an unchanged read-back, means the write never applied\""
why: An unchanged read-back normally proves a conditional write did not apply, and the lease still fenced the checkpoint so a retry looked safe; a request applied after the client gave up was not modelled, and GC acting on the stored value turned the stale belief into a stall
fix: After an unchanged read-back the lane is marked `ckpt_unsure`; before scanning, `refresh_own_ckpt` reads the checkpoint and takes it back when it is its own (same lease epoch, later version); stat `ckpt_late_taken`; regression `a_checkpoint_write_landing_late_is_taken_back` (fails before the fix); commit 397456b
lesson: "**The same class as rows 1–6 and 42, again:** \"no answer + read back unchanged\" means *not applied yet*, not *never applied*. When another controller (GC) acts on the stored state, the writer must take its belief back from the store before acting. The model checked non-atomic CAS (row 10's extension) but not a write applied after the reader's check; add that step to `s3InlineConsumer.qnt`"
variables: [consumer/checkpoint]
state: accepted
---
