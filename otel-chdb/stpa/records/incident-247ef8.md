---
id: incident-247ef8
label: CAST-70
batch: stpa-records-and-prd-2026-09-29
title: "The PRD refused a generated controller widget: the renderer wrote \"<\" inside an id-carrying text as a `{'<'}` JSX expression, which is valid JSX but breaks the PRD's text-literal rule"
found_by: "Publishing the consumer's detail widget to the PRD"
hazard: "None on the product; documentation integrity (a generated view that cannot be published drifts from its source)"
controller: "The renderer's PRD output (the development process): \"any valid JSX is accepted by the target\""
why: "The goldens and the JSX parser accepted it; the target's own rules were never part of the test"
fix: "Text is escaped as HTML entities; a test checks every text is a literal; commit 399145c; the widget republished"
lesson: "Test generated output against the target's own acceptance rules, not only its syntax"
state: accepted
---
