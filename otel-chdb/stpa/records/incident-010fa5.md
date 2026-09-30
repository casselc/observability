---
id: incident-010fa5
label: CAST-78
batch: models-and-verification-2026-09-29
title: "The consumer accepted a second spelling of a slot key: `…/E1/2.parquet` was parsed and ingested as slot 2 (5 rows); Rust `parse_slot_key` and Go `ParseSlotKey` accepted unpadded, signed and over-long sequences and an empty epoch"
found_by: "Writing the cargo-fuzz target's property and a consumer test (verification agent)"
hazard: "H-2 (duplicate or wrong rows if a second spelling of an ingested slot appears), possibly H-1 (a real slot shadowed), H-6 (anyone who can write under the prefix)"
controller: "The slot parser: \"only the writer's own spelling ever appears under the prefix\""
why: "Only our own writer puts keys there, and the write-side ABAC limits who can"
fix: "Canonical parse only (20-digit sequence, non-empty epoch) in both languages; `a_second_spelling_of_a_slot_key_is_not_a_slot` (Rust) and `TestParseSlotKeyAcceptsOnlyTheSlotsKey` (Go); commit 3302cb7"
lesson: "A parser must accept exactly what its formatter writes (CAST-65 again, for keys): one spelling per identity"
state: accepted
---
