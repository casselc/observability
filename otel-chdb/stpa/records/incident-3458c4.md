---
id: incident-3458c4
label: CAST-19
batch: hegel-and-the-model-extension-2026-09-27
title: Half-precision floats inside arrays and maps read back as null
found_by: "Same property, shrunk to one attribute `[0.0]`; end to end: `[1.5, 7]` became `[null, 7]`"
hazard: H-2
controller: "Upstream CBOR reader: \"only f32 and f64 occur\""
why: serde_cbor writes the shortest exact float, so 0, 1.5, 65504 and NaN arrive as f16; upstream's own unit test pinned f16 as Empty
fix: Decode CBOR half floats exactly, and fix that test; regression `regression_otap_nested_half_float`
lesson: A test that pins current behaviour can pin a bug; check encoders' real output, not the spec's common case
state: accepted
---
