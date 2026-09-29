---
id: controller-000b01
title: Operators
label: Operator
description: configuration and budgets; deploy and scale
component_type: human
process_model:
  - {name: health, meaning: How the janitor reports the store's health and cost, updated_by: [ops->janitor/feedback/health]}
control_algorithm:
  - {when: the store is over budget, uses: [health], issues: [ops->writer/control/config]}
state: accepted
---
