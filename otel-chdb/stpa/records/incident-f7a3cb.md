---
id: incident-f7a3cb
label: CAST-2
batch: initial
title: An error answer that still committed
found_by: Replicated-central soak with Keeper faults
hazard: H-2
controller: 'Consumer: "an error means nothing was written"'
why: ClickHouse docs and single-node behaviour
fix: Only pre-write errors settle a statement; others are waited out; mutant `errorSettles`
lesson: Classify outcomes as definite or ambiguous; model the ambiguous ones
state: accepted
---
