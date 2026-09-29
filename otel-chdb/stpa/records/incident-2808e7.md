---
id: incident-2808e7
label: CAST-72
batch: models-and-verification-2026-09-29
title: "The S3-native design model (`s3Native.qnt`, `s3NativeDesign` and `noFenceEntry`) broke `noReadOfDeleted` although S3NATIVE.md said all 13 invariants hold: a takeover did not rewrite the consumer checkpoint, so the old holder's checkpoint advance (a CAS on the version it had read) still landed after the new holder read that version; GC deleted the slot and the new holder's INSERT read a deleted object"
found_by: "The first `model-open` nightly (run 30, seed 0x1194b7d6, `noFenceEntry`); the design itself breaks at seeds 0x2 and 0x3 at 3,000 x 120"
hazard: "H-2 class in a proposed, unbuilt design (a failed ingest attempt; nothing lost or duplicated); verification gap: a documented model claim was wrong"
controller: "The S3-native consumer's checkpoint writer: \"a CAS on the checkpoint's version fences an old holder\"; the model's author: \"3,000 traces at one seed show the invariant holds\""
why: "The CAS does stop a later zombie write; CAST-10's takeover fence was built into the real consumer but not carried into this design; one fixed seed looked like enough"
fix: "`takeLease` writes a new checkpoint version (fenced under `CKPT_CAS`), as the built consumer does; regression `designTest.ckptFenceTest` fails before the fix; commit 48b454a; S3NATIVE.md records the finding; model-open re-samples with a new seed every night"
lesson: "A lease fence must cover every object the old holder can still write by CAS; a model result at a fixed seed is a sample, so re-sample with fresh seeds"
state: accepted
---
