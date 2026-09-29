---
id: incident-6780ac
label: CAST-48
batch: building-lane-retirement-2026-09-29
title: "Design gap: GC deletes slots ~2 minutes after the checkpoint passes them, so the only copy of a quarantined object would be gone before an operator could admit it"
found_by: Building `consume admit`
hazard: "H-1 (data lost for good: the recovered path would have nothing to recover)"
controller: "GC: \"a slot below the checkpoint was ingested\"; quarantine made that untrue"
why: Before quarantine, "passed" and "ingested" were the same state
fix: GC reads the quarantine documents after its marks and keeps every listed key; `admit_recovers_quarantined_objects_once_and_gc_keeps_them`; commit b17d8bd
lesson: A new "passed but not ingested" state means re-checking every reader of "passed"
variables: [gc/quarantined]
state: accepted
---
