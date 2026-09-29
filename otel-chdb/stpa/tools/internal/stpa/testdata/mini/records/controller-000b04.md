---
id: controller-000b04
title: Janitor
description: deletes old slots
component_type: software
process_model:
  - {name: positions, meaning: Every reader's position, source: the readers' published positions (outside this fixture)}
control_algorithm:
  - {name: delete an old slot, when: a slot is below every reader's position, uses: [positions], issues: [janitor->store/control/delete]}
state: accepted
---
