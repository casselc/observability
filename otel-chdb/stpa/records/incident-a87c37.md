---
id: incident-a87c37
label: CAST-21
batch: format-v2-2026-09-28
title: The lane watermark could pass a request still in flight in a later-named epoch
found_by: Mapping `model/completeness.qnt` onto the code, before any test ran
hazard: H-2, H-4 (a window treated as complete while data is missing; an alert evaluates too early)
controller: "Consumer lane watermark, as modelled: \"objects ordered by (epoch, seq) are in custody order\", so the highest low over the ingested prefix is safe"
why: "The model has one writer per edge, and D3 says one epoch per lane per incarnation; a publisher with `lanes: N` writes several epochs at once"
fix: Lane watermark = min(highest low passed before the LIST, lowest `received_at` of pending data slots), which needs no ordering between epochs; model instance `completenessImpl` (`PENDING`); the randomized soundness test uses 2 writer lanes per signal and catches the `WmIgnoresPending` mutant
lesson: Model instances must include the deployed parallelism, not the simplest topology
variables: [consumer/complete_through]
state: accepted
---
