---
id: incident-4cbe39
label: CAST-74
batch: models-and-verification-2026-09-29
title: "A consumer lease renewal got no answer and the read-back showed the held version unchanged, so the worker kept its old window; the PUT applied later. The holder lapsed on the older window (or dropped the lane when its retry met the late renewal's 412) while the store named it holder with a newer version, and nobody, the worker included, could take the lane for up to TTL + margin (95 s in production). Nothing lost or duplicated"
found_by: "The model agent while modelling CAST-50 (LATE_CAS); fixed on the owner's decision of 2026-09-30"
hazard: "H-2 class (liveness: the lane pauses and complete_through lags for its cluster); no safety impact"
controller: "The consumer worker's lease renewer (process-model variable consumer/lease): \"no answer plus an unchanged read-back means the renewal never applied\""
why: "An unchanged read-back normally proves a conditional write did not apply; the pause was bounded and lost no data, so the code comment called it harmless; a write applying after the reader's check was not modelled until CAST-50, and CAST-50's fix covered the checkpoint half only"
fix: "Unresolved renewals are kept as unsure, each with a unique beat; the lease is read back before the lapse check and the next renewal, and on a 412 or timeout; a stored lease that is exactly one of ours (owner, epoch, beat) is adopted with the stored ETag. Model LEASE_REFRESH with invariants noLateLeaseStall and oneHolder; mutant lateLeaseLost; consumer unit, hegel and MBT regressions; commit 2492ba2; nightly 44 green"
lesson: "\"No answer plus an unchanged read-back\" means not applied YET (rows 1-6, 42, 50): every CAS writer that keeps a belief about its own last write must recognise its own late write by an identity it wrote, never by ETag; fix every object a class of bug touches, not only the one first seen"
state: accepted
---
