---
id: controller-c6c2a0
label: On-call engineer
title: On-call engineers
description: choose scope, time range and snapshot; write alert rules; ack, silence, escalate
component_type: human
process_model:
- name: incident_state
  meaning: Whether an incident is open, what it affects and what is known about it
  updated_by: [oncall->alerting/feedback/pages, oncall->ui/feedback/results]
- name: view_freshness
  meaning: Whether the view is live or a snapshot, its source and the time it is complete through
  updated_by: [oncall->ui/feedback/results]
- name: alert_health
  meaning: Whether the alerting engine is evaluating, so that no page means no condition
  updated_by: [oncall->alerting/feedback/pages]
- name: silences
  meaning: Which silences and pinned snapshots are active, whose, and until when
  source: the engineers' own record of what they set; shown back only once R-S4 is met
control_algorithm:
- when: a page arrives or an incident is suspected
  uses: [incident_state, view_freshness]
  issues: [oncall->ui/control/queries, oncall->ui/control/snapshot]
- when: a page is understood and someone owns it
  uses: [incident_state]
  issues: [oncall->alerting/control/acks]
- when: a known cause keeps paging while the incident is handled
  uses: [incident_state, silences]
  issues: [oncall->alerting/control/silence]
- when: a condition should page and no rule covers it, or a rule pages wrongly
  uses: [incident_state, alert_health]
  issues: [oncall->alerting/control/rules]
state: accepted
---
