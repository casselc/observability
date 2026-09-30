---
id: incident-2b656f
label: CAST-76
batch: models-and-verification-2026-09-29
title: "The nightly Kani job proved nothing: its harness crate stopped compiling (E0063 in `proofs_plan::obj()`) when D36 added `Obj.payloads`, and no per-push job builds the `cfg(kani)` crate, so the break surfaced about 20 hours after the push"
found_by: "Nightly run 47, kani job"
hazard: "Verification gap: the check range's proofs (H-2, missing or duplicated rows) went unverified"
controller: "The CI pipeline: \"per-push CI compiles everything that mounts the consumer's source\""
why: "Kani is slow and not installed per push; the explicit field list was meant to force a review when a field is added, and it did, a day late"
fix: "The new fields are symbolic (`kani::any()`) so the proofs cover them; a stand-in kani crate lets ci.yml type-check the proofs on every push (about 10 s); commit 48ab0d7; nightly 49 kani green"
lesson: "Every piece of code that mounts production source needs a per-push compile, even when its real check runs nightly"
state: accepted
---
