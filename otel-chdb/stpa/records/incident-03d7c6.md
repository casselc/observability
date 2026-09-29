---
id: incident-03d7c6
label: CAST-62
batch: d36-phase1-and-ci-2026-09-29
title: "The regression test for CAST-56 used a fixed 378 ms sleep to reach its failure shape; when D36 added a payload statement to each round, the sleep no longer reached the shape and the test would have passed without testing anything"
found_by: "The D36 agent, while adapting the hegel_dst tests to the payload statement"
hazard: "Verification gap: a vacuous regression test for CAST-56"
controller: "The test: \"378 ms after the restart the original's rows are in and only the copy's announcement is not\""
why: "In a deterministic simulation fixed times are reproducible, so a fixed sleep looked exact"
fix: "The test waits for the shape (up to 0.9 s of simulated time) instead of sleeping a fixed time (b1dd087), and fails if the shape is never reached (25fcbe3)"
lesson: "A timing regression test must wait for the condition it needs, and fail if the condition is never reached"
state: accepted
---
