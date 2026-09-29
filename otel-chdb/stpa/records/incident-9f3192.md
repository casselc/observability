---
id: incident-9f3192
label: CAST-5
batch: initial
title: 10 s margin too short
found_by: Directed Keeper-overrun test (19 s past the limit)
hazard: H-2
controller: "Consumer: \"a commit lands within max_execution_time + 10 s\""
why: Keeper's operation timeout is 10 s
fix: 20 s margin, checked against the Keeper session at start; mutant `keeperOverrun`
lesson: Derive margins from the real bound (session timeout), and check them at runtime
variables: [consumer/fence]
state: accepted
---
