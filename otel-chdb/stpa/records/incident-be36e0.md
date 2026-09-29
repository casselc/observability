---
id: incident-be36e0
label: CAST-25
batch: query-service-2026-09-28-2
title: The plan service's documented minimum URL lifetime (`url_ttl_s` 60) with the default `replan_margin_s` (60) gave `replan_after` equal to the signing time; a larger margin put it before signing. Every plan was stale when issued, so a client obeying X8 re-planned until its limit and never read
found_by: Building the lake UI's browser test, which runs with a 60 s lifetime
hazard: "R-S1/R-S2 side: the lake UI can never show data (it fails visibly, not silently); a re-plan storm on the query service"
controller: "Plan service config: \"the margin is always smaller than the lifetime\""
why: The defaults (300 s / 60 s) are fine, and the 60–900 s clamp covered only the lifetime; the two knobs were validated separately
fix: Margin capped at half the lifetime; `internal/lake/config_test.go` failed 5 of 7 cases before the fix; commit 55a5b6b
lesson: Parameters that combine into one derived deadline must be validated together, at their combined value, not each in its own range
state: accepted
---
