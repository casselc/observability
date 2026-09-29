---
id: incident-8fbc40
label: CAST-6
batch: initial
title: Audit read a lagging replica
found_by: Replicated run
hazard: H-2
controller: "Audit: \"any replica is current\""
why: Single node had no lag
fix: Sync the replica first; fail over; fail visibly
lesson: Feedback from a replica needs its freshness attached
state: accepted
---
