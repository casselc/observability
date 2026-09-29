---
id: incident-8d2cf3
label: CAST-71
batch: stpa-records-and-prd-2026-09-29
title: "In the Platform operators detail diagram, controller names such as \"Entity controllers\" ran across their box borders when five nodes sat side by side; the earlier renderer work's review did not catch it"
found_by: "The PRD agent's light/dark PNG review"
hazard: "None on the product; documentation legibility"
controller: "The renderer and its tests: \"golden files that match mean the layout is right\""
why: "Goldens pin bytes, not geometry; the earlier review looked at a subset of the detail diagrams"
fix: "Names wrap at spaces and gaps narrow; a test checks every repository detail diagram's node names fit their boxes; commit 11107fd"
lesson: "A check of generated layout that looks only at golden bytes misses geometry: test the geometry or look at every render"
state: accepted
---
