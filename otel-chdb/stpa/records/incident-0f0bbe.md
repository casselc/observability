---
id: incident-0f0bbe
label: CAST-10
batch: initial
title: Load-balancer retries were not byte-identical
found_by: Gateway kill tests
hazard: H-2
controller: "Upstream exporter: map order"
why: Order did not matter before content keys
fix: Sort pieces (patch 0001, U20)
lesson: Anything that re-cuts a request must be deterministic
state: accepted
---
