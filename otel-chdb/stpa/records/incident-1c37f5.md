---
id: incident-1c37f5
label: CAST-13
batch: deterministic-simulation-2026-09-27
title: A worker took a lease that was still live
found_by: DST level 1, seeds 20 and 34; reproduces with no fault at all, one LIST answering 15 s late
hazard: "H-2 (two workers insert on one lane: duplicates)"
controller: "Consumer lease discovery: \"every lease I list was observed when this round started\" (`heartbeat_and_leases` read the clock once, then dated every listed ETag to the round's start)"
why: Rounds take milliseconds on a healthy store; no test combined a slow round with a renewal inside it; the Quint model's list is atomic and instantaneous
fix: Date each observation when its answer arrives (the LIST answer, `try_take`'s GET); regression test `a_slow_discovery_round_does_not_backdate_lease_observations`
lesson: Feedback is as old as the moment it was received, not the moment it was asked for; when "unchanged for long enough" grants authority, use the latest possible observation time
variables: [consumer/lease]
state: accepted
---
