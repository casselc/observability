---
id: requirement-00ce11
label: R-L11
title: No storage engine merge decides which version of a fact wins (no `ReplacingMergeTree`/`FINAL` or `min`/`anyLast` aggregation for correctness); duplicates of an identical fact are harmless by construction
priority: P0
from: [hazard-29d471, LS-L8, incident-658072]
state: accepted
---
