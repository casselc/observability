---
id: controller-000b02
title: Writer
description: buffers and commits; one per node
component_type: software
process_model:
  - {name: committed, meaning: Whether the batch is committed, updated_by: [writer->store/feedback/status]}
control_algorithm:
  - {name: commit a batch, when: a batch is buffered and not committed, uses: [committed], issues: [writer->store/control/commit]}
state: accepted
---
