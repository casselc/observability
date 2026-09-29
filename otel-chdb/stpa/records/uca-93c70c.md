---
id: uca-93c70c
label: UCA-4
action: consumer->clickhouse/control/insert
category: too-early-too-late-wrong-order
context: After its lease window closed, racing the new holder
hazards: [hazard-3c8035]
state: accepted
variables: [lease, fence]
---
