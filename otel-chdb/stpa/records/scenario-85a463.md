---
id: scenario-85a463
label: LS-2
findings: [uca-5fe6e7]
title: >-
  ClickHouse answered a timeout, but the insert committed up to 29 s later; the consumer's process model
  said "failed"
resolution: 'Fixed: error answers waited out; 20 s margin enforced at start'
state: accepted
---
