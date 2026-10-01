---
id: controller-2f943e
label: Query service
title: Query service
description: resolves entities; routes to central or the lake; returns a complete-through time with every result
component_type: software
process_model:
- name: grants
  meaning: "The caller's grants: (role, cluster, namespace) tuples (D38)"
  source: the operators' access configuration and the caller's verified token
- name: complete_through
  meaning: Per cluster, signal and lane, the time central is complete through (D29)
  updated_by: [qs->clickhouse/feedback/rows]
- name: basis
  meaning: The custody time an answer is at (D30)
  updated_by: [qs->clickhouse/feedback/rows]
  source: the request's basis, or the newest settled one
- name: complete_through_history
  meaning: "What was complete as of a past wall time T, per cluster and signal, and whether that answer is final (T's hour sealed) or provisional (D29 amendment 2026-10-01)"
  source: the watermark documents' open and frozen hours and the sealed history hours the consumer writes create-only
- name: source_coverage
  meaning: Which time range each source (central, the lake) covers, and whether it is healthy
  updated_by: [qs->clickhouse/feedback/rows, qs->lake/feedback/rows]
- name: entities
  meaning: The entities a query names, resolved to resource ids
  updated_by: [qs->catalog/feedback/entities]
control_algorithm:
- name: answer from central
  when: a request or an evaluation arrives within the caller's grants and central covers its range
  uses: [grants, source_coverage, basis]
  issues: [qs->clickhouse/control/sql]
- name: route to the lake
  when: "the range is older than central keeps, or central is degraded: route to the lake and say so"
  uses: [grants, source_coverage]
  issues: [qs->lake/control/reads]
- name: resolve entities
  when: a query names entities
  uses: [grants, entities]
  issues: [qs->catalog/control/lookups]
state: accepted
---
