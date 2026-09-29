---
id: controller-ac2fd1
label: GC
title: GC, audit, sealer
description: delete old slots; late-copy audit; snapshots
component_type: software
process_model:
- name: positions
  meaning: Every reader's position per lane (consumer checkpoints, the sealer's watermark), each with the time it was marked (D12)
  updated_by: [gc->s3/feedback/marks]
- name: epochs
  meaning: Which lane epochs have retired, so the slot just below a checkpoint can go (LS-3)
  updated_by: [gc->s3/feedback/marks]
- name: quarantined
  meaning: Which slots are quarantined below the bound (D35)
  updated_by: [gc->s3/feedback/marks]
- name: lake_snapshot
  meaning: The lake's latest snapshot and what it has sealed
  updated_by: [gc->lake/feedback/snapshots]
control_algorithm:
- name: delete an old slot
  when: a slot is below every reader's position marked at least the delay ago, its epoch has retired, and it is not quarantined
  uses: [positions, epochs, quarantined]
  issues: [gc->s3/control/delete]
- name: seal into the lake
  when: objects below the watermark are not yet in the lake's latest snapshot
  uses: [positions, lake_snapshot]
  issues: [gc->s3/control/seal, gc->lake/control/commit]
state: accepted
---
