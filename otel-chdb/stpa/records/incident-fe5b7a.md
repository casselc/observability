---
id: incident-fe5b7a
label: CAST-40
batch: late-row-splitting-2026-09-28
title: parquet-go keeps a writer's footer key/value metadata across `Reset`, so the first unsplit object after a split one carried `oscope-part` in its footer; the Go edge also committed the two parts in a different order from Rust
found_by: The first Go-vs-Rust conformance run of the split, before commit
hazard: "R-S1 class: an object whose footer describes another object (readers that trust footer metadata would misplace it)"
controller: "Go edge writer: \"`Reset` returns the writer to a clean state\""
why: The library's `Reset` does clear rows and buffers; footer metadata persisting is undocumented
fix: A new writer whenever a file's key set would drop a key; both edges commit bulk then late; regression in `TestLateSplitTraces` (dc90809)
lesson: Reuse of a stateful library object needs a test that the reused state is what you think; differential testing between two implementations catches what neither's own tests assert
state: accepted
---
