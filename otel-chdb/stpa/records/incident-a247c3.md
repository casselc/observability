---
id: incident-a247c3
label: CAST-38
batch: bitemporal-catalog-replay-2026-09-28
title: "Shared-resource exhaustion by agents, twice: a worktree Rust debug build (D29) and one-INSERT-per-object replay parts (D32, ~1.7 GB of part overhead) took free disk below 200 MB while other agents ran; builds and tests elsewhere failed or were cut short (D30's fork checks ran reduced)"
found_by: The agents' own `df` checks and failed builds
hazard: "Not a product hazard: loss of verification (checks skipped or reduced), and a risk of corrupting shared state (the ClickHouse and SeaweedFS the rigs share)"
controller: "Coordinator: \"each agent keeping ≥ 2.5 GB free is enough\" — a per-agent rule on a shared budget, checked by each agent only before its own builds"
why: Agents checked disk before builds, not before data-producing tests; ClickHouse's per-part overhead was not in anyone's estimate
fix: "Agents dropped their data and caches; the D32 replay switched to batched inserts; the D29 agent built in `/dev/shm`. For the next rounds: one heavy job at a time, and a disk check before any test that loads data"
lesson: A per-actor rule over a shared budget does not bound the sum; the budget needs one owner (a scheduler or a lock), not N polite actors
state: accepted
---
