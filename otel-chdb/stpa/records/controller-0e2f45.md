---
id: controller-0e2f45
title: Telemetry UI
description: results with source and mode; scope bar, snapshot picker
component_type: software
process_model:
- name: source
  meaning: Which source answered (central or the lake) for each view
  updated_by: [ui->qs/feedback/complete-through]
- name: complete_through
  meaning: The time each result is complete through, per cluster and signal, and whether windows are partial
  updated_by: [ui->qs/feedback/complete-through]
- name: basis
  meaning: The custody time the view's answers are at, and whether they are settled (D30)
  updated_by: [ui->qs/feedback/complete-through]
- name: scope
  meaning: The chosen scope, time range and snapshot
  source: the on-call engineers' queries and snapshot choice
control_algorithm:
- name: refresh the view
  when: the scope, range or snapshot changes, or a view refreshes
  uses: [scope, basis]
  issues: [ui->qs/control/requests]
state: accepted
---
