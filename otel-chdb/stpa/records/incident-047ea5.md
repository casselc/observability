---
id: incident-047ea5
label: CAST-47
batch: building-lane-retirement-2026-09-29
title: "Model and design gap: an earlier incarnation's zombie PUT could land after a later incarnation's close retired the lane; it fell below R and was quarantined although its request had been ingested (a false page), because a close was taken to seal the whole lane"
found_by: Building the retirement rule; confirmed by a scripted Quint run (the random simulation passed 20,000 × 60 samples with the mutant on)
hazard: "H-4: a false quarantine page (alert fatigue), and a rule that did not mean what the model said"
controller: "Consumer retirement rule and the D35 model: \"a close proves nothing more can land in this lane\", but it proves it only for its own incarnation"
why: The model had one writer per lane; random traces never reach the long multi-incarnation sequence
fix: Retire only when every earlier epoch is sealed (tombstone or its own close); below-R copies pass as copies; model gains `SEAL_OLDER`, mutant `closeUnsealed`, scripted runs; `a_close_proves_empty_custody_only_when_everything_is_sealed`; commit a1841d4
lesson: "Scripted traces for multi-incarnation cases: random simulation misses long, specific traces; a proof is scoped to the actor that gave it"
variables: [consumer/lane_closed]
state: accepted
---
