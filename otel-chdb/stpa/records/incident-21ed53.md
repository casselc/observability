---
id: incident-21ed53
label: CAST-14
batch: deterministic-simulation-2026-09-27
title: A backlog longer than the lease window livelocked a worker
found_by: DST level 1, about 10% of the first 200 seeds
hazard: H-2 and L-1 (ingestion stalls, views incomplete)
controller: "Consumer step scheduling: \"a step is short compared with the lease\" (up to 256 HEADs per lane, lane after lane, renewal only at insert)"
why: The 256 cap was sized for throughput; steady-state backlogs are small; model actions cost no time; soaks recovered from short outages only
fix: Renew between lanes and stop scanning a lane once its renewal is due, keeping the HEADs already made; regression test `a_backlog_longer_than_the_lease_window_is_still_ingested`
lesson: Bound work per step by time, not count, whenever a lease or deadline governs; schedule renewals independently of the work loop; test recovery from long outages, not only steady state
variables: [consumer/lease]
state: accepted
---
