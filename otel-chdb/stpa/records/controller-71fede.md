---
id: controller-71fede
label: Platform operator
title: Platform operators
description: configuration, budgets, retention, access; deploy and scale
component_type: human
process_model:
- name: pipeline_health
  meaning: Lag, errors, audit findings and cost of the pipeline, per component and cluster
  updated_by: [ops->gc/feedback/health]
- name: budgets
  meaning: Each component's resource budget (memory, disk, S3 and central cost)
  updated_by: [ops->gc/feedback/health]
  source: set by the operators themselves; the health feedback says how close each component is
- name: longest_outage
  meaning: "The longest edge outage the durable buffer must ride out: the custody age of its oldest request (D19)"
  source: the edge buffers' configuration and the operators' own estimate; no feedback reports it
- name: role_needs
  meaning: "What each role needs to read: which clusters and namespaces"
  source: access requests and the organisation's teams, outside this structure
- name: source_state
  meaning: Which sources (central, the lake) are healthy and which ranges each serves
  updated_by: [ops->gc/feedback/health]
control_algorithm:
- name: reconfigure a component
  when: a component is over its budget, failing or lagging
  uses: [pipeline_health, budgets]
  issues: [ops->edge/control/config, ops->ec/control/config]
- name: set retention
  when: "retention is changed for cost: never shorter than the longest outage the buffer rides out"
  uses: [longest_outage, budgets]
  issues: [ops->clickhouse/control/retention]
- name: grant access
  when: "a role needs access: grant exactly the (cluster, namespace) pairs it needs"
  uses: [role_needs]
  issues: [ops->qs/control/access]
- name: route a source
  when: a source is degraded, added or retired
  uses: [source_state]
  issues: [ops->qs/control/routing]
state: accepted
---
