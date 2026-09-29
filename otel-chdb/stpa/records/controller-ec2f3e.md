---
id: controller-ec2f3e
label: Alerting engine
title: Alerting engine
description: evaluates complete windows; pages on-call
component_type: software
process_model:
- name: completeness
  meaning: "Whether the rule's window is complete in the rule's scope: the query service's label (D23, D29)"
  updated_by: [alerting->qs/feedback/values]
- name: evaluation_state
  meaning: Whether the last evaluation of each rule succeeded, failed or could not run
  updated_by: [alerting->qs/feedback/values]
- name: firing
  meaning: Which groups are pending, firing or resolved (state by compare-and-swap, D23)
  updated_by: [alerting->qs/feedback/values]
- name: rules
  meaning: The rules, acks and silences in force
  source: the on-call engineers' rules, acks and silences
control_algorithm:
- when: a rule's window has ended and the query service labels it complete
  uses: [completeness, rules, firing]
  issues: [alerting->qs/control/evaluate]
state: accepted
---
