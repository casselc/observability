---
id: controller-fc8476
label: Entity controller
title: Entity controllers
description: one per cluster; write entity records
component_type: software
process_model:
- name: pods
  meaning: The running pods of its cluster and their resource identities and versions
  source: a watch on its own cluster's Kubernetes API, outside this structure
- name: cluster
  meaning: Its own cluster's identity, which prefixes every record it writes
  source: its configuration and its credentials' cluster tag (D18)
control_algorithm:
- when: a pod appears, changes version or goes
  uses: [pods, cluster]
  issues: [ec->s3/control/write]
state: accepted
---
