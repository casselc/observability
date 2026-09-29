---
id: scenario-e12f6d
label: LS-3
findings: [uca-057c96]
title: >-
  A writer that never learned its write succeeded retried into a slot GC had already deleted, below the
  checkpoint
resolution: 'Fixed: GC keeps the slot below the checkpoint until the epoch retires'
state: accepted
---
