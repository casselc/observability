---
id: incident-87da55
label: CAST-39
batch: late-row-splitting-2026-09-28
title: The edge lane's append (Go `commit.Lane.Append`, Rust `runner::append`) resent without limit when every PUT got no answer and each HEAD found the slot free; the Go edge spun for 11 minutes at 40% CPU holding the lane mutex, blocking its heartbeats
found_by: The D31 integration run, with SeaweedFS refusing writes near its disk floor; a goroutine dump
hazard: "H-8 (the lane stalls: requests never answered, the edge's buffer fills); stale `complete_through` for the cluster (R-S1/R-S5 lag, visible as `stale`, not wrong); S3 hammered during throttling or an outage"
controller: "Edge lane: \"HEAD says free, so one more PUT will do it\", as if a failing store were a one-off drop"
why: A single dropped PUT is correctly resolved by one resend, and the commit model has no bound on resends because safety never needed one
fix: At most 8 resends (`MaxResends` / `MAX_RESENDS`), then return unresolved; the lane stays at the slot and the next call commits there; `commit/resend_test.go`, `a_store_that_fails_every_put_is_not_resent_forever`; commit dc90809
lesson: Every retry loop on an unknown outcome needs a bound and must hand back to a caller who can back off; a model that only checks safety will not ask for one (add a liveness property)
variables: [edge/slot_state]
state: accepted
---
