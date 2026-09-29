---
id: incident-ff59e2
label: CAST-1
batch: initial
title: Replay stamped a new receive time
found_by: Horizon audit design, then a crash/replay test
hazard: H-2
controller: 'Edge: "the time I see the request is when it arrived"'
why: Without a buffer that was true
fix: Keep the buffer's ingestion time (patch 0003, queue metadata)
lesson: Identity and time must come from the first durable custody, not the latest handler
state: accepted
---
