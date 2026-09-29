---
id: incident-22fa01
label: CAST-26
batch: alert-evaluator-2026-09-28
title: "The query service labels an event-time window `complete` as soon as `complete_through` (a bound on `received_at`, custody time) reaches the window's end (`completeness/watermark.go` `MakeLabel`: `w.ToNs > ct`). A row whose event time is in the window but that is received later is missing from a result labelled complete"
found_by: "The alert evaluator agent, designing its gate: its windows are event time, the watermark is custody time"
hazard: "H-4/R-S1: a result labelled complete is not; an alert rule sees \"no rows\" in a complete window and resolves or never fires (the hazard R-S3 exists to prevent)"
controller: "Query service label: \"event time and custody time are the same clock\" — the watermark bounds when rows were received, the window bounds when they happened"
why: D19 made custody time the ordering key for completeness and retention; the query service reused it, and every test published rows whose event time ≈ receive time, so the two never diverged
fix: "Policy `max_lateness` (default 60 s): `complete` only when `complete_through ≥ to + max_lateness`; the label reports `settled_through` (event time); a windowed `/v1/query` counts rows with `received_at > time + max_lateness` (`late` block, metrics); the plan marks late objects; the lake UI, HyperDX adapter and alert evaluator (its `lateness` is now an extra margin) follow. Rapid property with diverging clocks and an integration test with a late row through the real edge, both failing under the old rule; commit b302126 (D26). Remaining: rows later than `max_lateness` are counted, not prevented"
lesson: Two timestamps that usually agree are still two clocks; a completeness claim must say which one it bounds, and tests must include data where they diverge
variables: [qs/complete_through]
state: accepted
---
