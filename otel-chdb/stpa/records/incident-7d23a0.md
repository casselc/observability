---
id: incident-7d23a0
label: CAST-83
batch: models-and-verification-2026-09-29
title: "A consumer lease take that got no answer and read back unchanged, then landed late, named the worker holder of a lane it did not work: the lane idled for TTL + margin (about 95 s in production). Nothing lost or duplicated"
found_by: "The CAST-74 agent's residual-exposure report; then the model (mutant `lateTakeLost` violates `noLateTakeStall`); fixed on the owner's decision of 2026-09-30"
hazard: "H-2 class (liveness: the lane pauses and complete_through lags)"
controller: "`worker::try_take`: \"no answer plus an unchanged read-back means the take never applied\""
why: "The same belief CAST-74 removed from renewals; the model's take step was atomic, so the late take was not modelled"
fix: "Unanswered takes are kept pending (`take_unsure`) with a unique beat; a stored lease that is exactly our take (owner, epoch, beat; never the ETag) is adopted, and given back at once if its window is over; model TAKE_AMBIG with `noLateTakeStall` and `oneHolder`; unit, Hegel (`lease_take_late`), MBT and fleet-oracle tests; commits a6bdb33, 6421712, 0902ec1, corrected by 11509c1; nightly 56 green"
lesson: "A write whose outcome is unknown can still land: whoever sent it keeps it in doubt until a read-back proves otherwise (rows 50, 74, 75); fix every write of the class, takes as well as renewals"
state: accepted
---
