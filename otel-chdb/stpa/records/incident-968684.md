---
id: incident-968684
label: CAST-73
batch: models-and-verification-2026-09-29
title: "The consumer MBT replay (quint-connect) failed twice on the new CAST-50 steps for harness reasons: the driver took the rejected write's own ETag instead of the stored one when the read-back matched, and two identical sends in one tick collapsed into one element of the model's `writes` set"
found_by: "Nightly runs 30 and 36 (`mbt_s3inline_consumer`), diagnosed by the model agent"
hazard: "Verification false failure (CAST-56 class): a red that is the harness's fault"
controller: "The MBT driver and the model's state: \"the ETag in hand is the stored one\"; \"a set of writes can hold every send\""
why: "Both held until a step made a rejected write and a late landing meet, which only the new steps did"
fix: "The driver reads the stored ETag after a matching read-back (1c7eacc); the model keeps identical sends distinct (b3f5dff); `rust-mbt` re-runs a failure with QUINT_VERBOSE=1 so the divergence is in the log; nightly 41 green"
lesson: "When a model gains a step, re-check the harness's own assumptions about identity (ETags, set elements) that the step can break"
state: accepted
---
