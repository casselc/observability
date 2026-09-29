---
id: incident-28a2e9
label: CAST-20
batch: hegel-and-the-model-extension-2026-09-27
title: A lost lease-renewal or checkpoint write dropped a lane that was still ours
found_by: Model-based test after the model gained non-atomic lease writes (designSlow, seed 0x29e8aebd)
hazard: L-1 (the lane idles until our own lease expires; safe for data)
controller: "Consumer `write_lease` / `write_ckpt`: \"a read-back that isn't our new document means someone else wrote\""
why: "Before #15's fix, only a 412 was read back; the read-back then compared against our new document, so \"our write never applied\" looked like a takeover"
fix: When the object still has the version we wrote on, keep the lane and write again (commit 7b0f28f); regression `a_lost_lease_or_checkpoint_write_keeps_the_lane`
lesson: "An ambiguous outcome has three results, not two: ours, someone else's, and nothing happened; each needs its own handling"
variables: [consumer/lease, consumer/checkpoint]
state: accepted
---
