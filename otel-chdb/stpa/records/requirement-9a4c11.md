---
id: requirement-9a4c11
label: R-E7
title: "The forwarder keeps no disk buffer: a small bounded in-memory queue only, nothing persisted on the device; every drop (queue full, a long outage, a crash, no token) is counted on the device and never blocks the tool (owner revision, D37)"
priority: P0
from: [UCA-E7, SEC-E6, hazard-10945d]
state: accepted
---
