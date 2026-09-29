---
id: incident-000f03
label: CAST-1
batch: first
title: A slot was deleted under a slow reader
found_by: A model check
hazard: H-1
controller: "Janitor: \"below the checkpoint nobody reads\""
why: Readers were fast
fix: Keep slots until every reader passed
lesson: Deleting needs every reader's view
variables: [janitor/positions]
state: accepted
---
