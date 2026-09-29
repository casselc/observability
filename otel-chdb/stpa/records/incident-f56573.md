---
id: incident-f56573
label: CAST-11
batch: initial
title: Configs carried static keys that shadowed IRSA and Pod Identity
found_by: Edge config audit
hazard: H-6
controller: Edge config
why: Convenient for local runs
fix: Credentials only from the environment
lesson: Local convenience must not ship in shared configs
state: accepted
---
