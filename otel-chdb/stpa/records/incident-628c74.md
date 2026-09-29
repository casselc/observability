---
id: incident-628c74
label: CAST-34
batch: basis-2026-09-28
title: The coordinator's spec (research/bitemporal.md §3) said a basis admits `received_at ≤ C`. The consumer promises only `received_at < complete_through`, and a pending object can carry `received_at` equal to it, so "same basis ⇒ same answer" would fail
found_by: The D30 agent's rapid property ("same basis ⇒ same answer while data arrives"); the `≤` mutant fails on its third case
hazard: "H-4 / R-S1: an answer at a basis that later changes, under a label promising it won't"
controller: "Coordinator: \"`complete_through` is inclusive\" — carried from prose, not from FORMAT.md §3's precise statement"
why: The note was written from memory of the design, and "through" reads as inclusive
fix: Implemented strictly (`<`) for rows and `oscope-received`; spec corrected; the mutant stays in the property suite (D30, 045929b)
lesson: A spec derived from prose inherits its ambiguity; boundary operators come from the formal statement (FORMAT.md §3, the Quint model), and a property with the off-by-one mutant settles them
state: accepted
---
