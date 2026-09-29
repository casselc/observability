---
id: incident-a562db
label: CAST-18
batch: hegel-and-the-model-extension-2026-09-27
title: A log body of integer 0 or double 0 read back as empty; an attribute's −0.0 read back as 0
found_by: "Same property, shrunk to one record with body `IntValue(0)`; end to end: body `\"\"` instead of `0`, `-0` became `0`"
hazard: H-2
controller: "Upstream logs view: \"a missing value column means no value\"; encoder: \"`==` identifies the default\""
why: The encoder omits a column whose values are all the default, and other readers treat that as the default; −0.0 == 0.0 under `==`
fix: The view returns the type's default; the double column no longer drops defaults; regression `regression_otap_zero_values`
lesson: Two components must share one meaning for "column absent"; compare floats by bits when identity matters
state: accepted
---
