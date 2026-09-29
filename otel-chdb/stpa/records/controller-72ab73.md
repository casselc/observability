---
id: controller-72ab73
title: Central aggregator
description: merges and versions; flags catalog gaps
component_type: software
process_model:
- name: entity_records
  meaning: The entity records the entity controllers wrote, per cluster, and which of them were read
  updated_by: [agg->s3/feedback/records]
- name: catalog_version
  meaning: The catalog's current version of each entity
  source: its own earlier upserts; nothing reads the catalog back
control_algorithm:
- when: a record is newer than the catalog's version of its entity and comes from its writer's own cluster
  uses: [entity_records, catalog_version]
  issues: [agg->catalog/control/upsert]
state: accepted
---
