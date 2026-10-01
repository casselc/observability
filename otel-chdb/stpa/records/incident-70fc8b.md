---
id: incident-70fc8b
label: CAST-84
batch: models-and-verification-2026-09-29
title: "In the first CAST-83 fix, a late-adopted take whose window was over was given back, but the release itself got no answer and the take had already been dropped from the pending set, so the worker forgot it while the store still named it holder: the lane stalled until expiry"
found_by: "Hegel `hegel_dst_fleet`, nightly 51 (never released)"
hazard: "H-2 class (liveness)"
controller: "`give_back`: \"a release we sent has applied\""
why: "The give-back path was new and its own write's outcome was treated as known"
fix: "First 0902ec1 (keep the take pending), which caused CAST-85; correctly in 11509c1 (`giving_back`); test `a_late_take_whose_release_is_lost_is_given_back_again`"
lesson: "Every new write a fix adds is itself a write whose outcome can be unknown; apply the row-50 rule to the fix's own writes"
state: accepted
---
