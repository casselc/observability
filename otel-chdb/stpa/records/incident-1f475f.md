---
id: incident-1f475f
label: CAST-35
batch: basis-2026-09-28
title: The evaluator's late checks plus two replicas per identity exceeded the query service's default per-caller concurrency (4); the 429s counted as failed evaluations and paged "cannot evaluate"
found_by: The D30 alerts integration test
hazard: "H-4 (false pages erode trust in real ones: the alert-fatigue path to missed hazards)"
controller: "Evaluator: \"the query service's capacity is not my concern\"; service: \"4 concurrent statements per caller is enough for anyone\""
why: The two limits were set independently, before the late checks multiplied the evaluator's load
fix: "Documented; the integration test sets 16; owner to set the production value. Not fixed in code: a 429 is still a failure (fail-visible, by design)"
lesson: Load a feature adds to a shared limit must be budgeted where the limit is set; a back-pressure answer should say so distinctly from a failure
state: accepted
---
