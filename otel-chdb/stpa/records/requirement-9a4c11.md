---
id: requirement-9a4c11
label: R-E7
title: "The forwarder keeps nothing on the device and holds no request beyond the one it is proxying: each request streams to the ingress under a size and a concurrency bound, its answer streams back, and a 200 to the tool is the ingress's commit, so nothing the forwarder acknowledged can be dropped by it (owner revision of D37; pass-through, D40 amended 2026-10-02)"
priority: P0
from: [UCA-E7, SEC-E6, hazard-10945d]
state: accepted
---
