---
id: incident-22ea07
label: CAST-44
batch: partition-key-and-dead-lane-retirement-2026-09-28
title: "Found by the retirement model, applies today without retirement: deleting a dead publisher pod's volume while a PUT it sent can still land lets that object arrive after the watermark has passed its custody time"
found_by: "`model/retirement.qnt` (D35)"
hazard: "H-4 / R-S1: rows below a published `complete_through` (a result labelled complete is not)"
controller: "Operators (the scale-down runbook): \"a dead pod sends nothing\""
why: A pod is dead to Kubernetes before its in-flight requests are dead to S3
fix: The scale-down runbook now waits out the PUT bound before deleting a volume; operator retirement (D35) requires the same evidence
lesson: "\"Dead\" must be defined by the effects a component can still cause, not by its process state"
state: accepted
---
