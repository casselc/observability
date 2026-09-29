---
id: scenario-76fdee
label: LS-1
title: The edge crashed after a commit landed but before the buffer's ack; on replay it stamped a new receive time, so the copy fell outside the check horizon
findings: [uca-5fdb2c]
factor: process-model
variables: [edge/received_at]
state: accepted
---
