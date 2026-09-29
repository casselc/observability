---
id: controller-fe59f7
label: Edge collector
title: Edge collectors
description: agents, publishers, buffer; commit protocol per lane
component_type: software
process_model:
- name: buffer_level
  meaning: "How full the durable buffer is: bytes, requests, and the age of the oldest one"
  source: the edge's own buffer (local state)
- name: durable
  meaning: "Whether a request's batch is durable: persisted in the buffer or committed to its slot"
  updated_by: [edge->s3/feedback/outcome]
  source: the buffer's own persist step (D19)
- name: received_at
  meaning: When a request first entered the edge's custody
  source: stamped at first custody and kept through the buffer and every replay (patch 0003, D19)
- name: slot_state
  meaning: Whether the lane's next slot is free, taken by this edge, or unresolved (a PUT with no answer)
  updated_by: [edge->s3/feedback/outcome]
control_algorithm:
- when: the request's batch is durable
  uses: [durable]
  issues: [edge->producers/control/ack]
- when: the buffer is at its cap
  uses: [buffer_level]
  issues: [edge->producers/control/push-back]
- when: a buffered batch waits and the lane's next slot is free
  uses: [slot_state, received_at]
  issues: [edge->s3/control/put]
- when: a PUT got no answer and the slot is unresolved (at most 8 resends, then hand back unresolved); or, after a restart, a buffered batch not yet committed, keeping its received_at
  uses: [slot_state, durable, received_at]
  issues: [edge->s3/control/resend]
state: accepted
---
