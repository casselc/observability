---
id: incident-6bd0bd
label: CAST-9
batch: initial
title: One 1 MiB value made an object 40× larger
found_by: Hostile conformance data
hazard: H-7
controller: "Go writer: \"statistics are small\""
why: Normal data has short values
fix: Truncate statistics to 64 bytes, as parquet-rs does
lesson: Test with hostile data, not just realistic data
state: accepted
---
