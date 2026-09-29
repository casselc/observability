---
id: incident-d006a5
label: CAST-7
batch: initial
title: Unescaped quote broke every insert
found_by: Edge-deploy agent's run
hazard: H-8
controller: Consumer SQL builder
why: String built by hand
fix: One quoting helper; a test that the SQL parses
lesson: Build SQL from a tree, never by splicing
state: accepted
---
