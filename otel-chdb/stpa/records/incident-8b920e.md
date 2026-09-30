---
id: incident-8b920e
label: CAST-80
batch: models-and-verification-2026-09-29
title: "The ingress FuzzBody oracle expected a second object for a resent body that the ingress correctly ACKed without one (resend dedup), so the nightly fuzz run failed on correct behaviour"
found_by: "Nightly fuzz run 34"
hazard: "Verification false failure (CAST-56 class)"
controller: "The fuzz oracle: \"every ACKed body yields a distinct object\""
why: "The oracle was written from the single-request contract before dedup was part of it"
fix: "The oracle keys by canonical content; `TestAResendIsACKedWithoutASecondObject`; the crasher is kept in testdata; commit b836318"
lesson: "An oracle must model every rule of the system it checks, dedup included"
state: accepted
---
