---
id: incident-6a9fa4
label: CAST-4
batch: initial
title: GC deleted a slot a writer later retried into
found_by: Quint model at short lease with writer faults
hazard: H-1
controller: "GC: \"below the checkpoint nothing will be written again\""
why: Writers were assumed to know their outcome
fix: Keep the slot below the checkpoint until the epoch retires; mutant `gcReopens`
lesson: Deleting needs the writers' view, not only the reader's
variables: [gc/epochs]
state: accepted
---
