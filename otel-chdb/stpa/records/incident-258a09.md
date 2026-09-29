---
id: incident-258a09
label: CAST-33
batch: mosaic-spike-2026-09-28
title: Mosaic 0.31 names its pre-aggregated cube tables by a hash of their SQL and creates them `IF NOT EXISTS`; reloading different rows under the same table names (a new plan, a new window) leaves the old cubes answering. A chart showed 9,000 where 3,000 was right
found_by: The spike's e2e check that every chart's total equals an independent SQL count after each brush
hazard: "R-S1/H-4 class: a chart shows numbers from a previous load as current, with the current load's completeness label on it"
controller: "Mosaic coordinator: \"a table name identifies its contents\" (true within one static dataset, Mosaic's design case)"
why: Mosaic targets fixed datasets; our tables are reloaded per plan under stable names, which Mosaic's cache key does not include
fix: The spike drops the cube schema and clears the query cache on every load; the total-equals-SQL check stays in the e2e (commit 20a6146). Found before any adoption (D28 still proposed)
lesson: A cache keyed on the query but not on the data version is wrong the moment the data is reloaded; every cache in the read path must key on the plan it came from
state: accepted
---
