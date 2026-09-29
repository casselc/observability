---
id: incident-8412e8
label: CAST-17
batch: hegel-and-the-model-extension-2026-09-27
title: A span's Duration was dropped when its start or end time was 0
found_by: "Hegel property \"OTAP rows match OTLP rows\", shrunk to one span with start 0, end 1; confirmed end to end through the Rust edge (`otlp_path: via_otap`) into ClickHouse `s3()`: Duration 0 instead of 1000"
hazard: H-2
controller: "Upstream OTLP→OTAP encoder: \"a time field is either present or meaningless\""
why: proto3 leaves a zero off the wire, so a zero time looks absent; real spans rarely start at the epoch
fix: Treat an absent time as 0, subtract with `wrapping_sub`; regression `regression_otap_duration_with_a_zero_time`
lesson: Encoding defaults are values, not absence; a round-trip property over the whole value space catches what corpora don't
state: accepted
---
