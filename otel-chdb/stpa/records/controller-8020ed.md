---
id: controller-8020ed
label: Consumer worker
title: Consumer workers
description: leases, time-bound inserts, count check and repair
component_type: software
process_model:
- name: lease
  meaning: Whether this worker holds the lane's lease, its epoch, and until when it is safe to act (safe_until, on its own monotonic clock)
  updated_by: [consumer->s3/feedback/list]
- name: fence
  meaning: "The deadline a statement carries, evaluated on ClickHouse's clock: sent + ttl - margin - budget (D9)"
  source: computed from the lease write's send time, the TTL and the margin (at least 20 s, checked against the Keeper session at start)
- name: checkpoint
  meaning: "The lane's checkpoint as stored: lease epoch, version, floor and epochs (D8)"
  updated_by: [consumer->s3/feedback/list]
- name: pending_objects
  meaning: The lane's objects past the checkpoint, in custody order
  updated_by: [consumer->s3/feedback/list]
- name: statement
  meaning: "Each statement's outcome: committed, not applied, or unresolved until it can no longer land"
  updated_by: [consumer->clickhouse/feedback/answers]
- name: row_counts
  meaning: How many rows of each object central holds, by content key (D11)
  updated_by: [consumer->clickhouse/feedback/counts]
- name: complete_through
  meaning: "The lane's watermark: the custody time up to which everything the edge held is ingested (D29)"
  updated_by: [consumer->s3/feedback/list]
- name: lane_closed
  meaning: Whether every incarnation of the lane's edge has closed or been tombstoned, so its custody is empty (D35)
  updated_by: [consumer->s3/feedback/list]
control_algorithm:
- when: the lane's lease is free or expired on this worker's own clock and the fair share wants it; or the held lease is due for renewal
  uses: [lease, checkpoint]
  issues: [consumer->s3/control/checkpoint, consumer->s3/control/lease]
- when: the lease is held, now + budget <= safe_until, objects are pending and no statement of the lane is unresolved
  uses: [lease, fence, pending_objects, statement]
  issues: [consumer->clickhouse/control/insert]
- when: a settled statement's objects are short in central's counts
  uses: [statement, row_counts]
  issues: [consumer->clickhouse/control/repair]
- when: "every statement of a prefix is settled and counted: advance the checkpoint (and the watermark); a lane whose custody is proved empty is retired"
  uses: [statement, row_counts, checkpoint, complete_through, lane_closed]
  issues: [consumer->s3/control/checkpoint]
state: accepted
---
