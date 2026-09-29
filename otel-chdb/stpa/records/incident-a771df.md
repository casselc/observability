---
id: incident-a771df
label: CAST-3
batch: initial
title: Retry after a lost answer re-inserted rows
found_by: Quint model (`releaseInFlight` family)
hazard: H-2
controller: "Consumer: \"verify now, the statement is over\""
why: Local inserts return fast
fix: Lanes with an unanswered statement are left alone until it cannot land
lesson: A check is only valid after the thing checked can no longer change
variables: [consumer/statement]
state: accepted
---
