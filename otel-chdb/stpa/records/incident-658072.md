---
id: incident-658072
label: CAST-37
batch: bitemporal-catalog-replay-2026-09-28
title: "A restarted entity controller dated every pod it had not yet seen from the pod's creation (`vf := created`); the aggregator merges `valid_from` as a minimum, so each restart moved a relabelled pod's current version back over its earlier versions: two versions valid at once in `pods` and `resources`, and the relabel time lost"
found_by: "The D32 fleet replay through the real aggregator SQL: 2,063 pod-hours with two versions at once"
hazard: H-3 (an entity attributed to the wrong version for time-keyed questions); H-5 (a wrong history view, not flagged). Joins by `resource_id` unaffected
controller: "Entity controller: \"a pod I first see has been in this version since creation; each incarnation starts with nothing to reconcile\""
why: Exact on a first-ever start, and the min merge makes replays and multiple writers safe; the README already noted a restart is not a gap record
fix: "`lane.PreviousEnd` finds the previous incarnation's end; a pod first seen and created before it is dated from it; the restart window is written as a `restart` gap record (`--restart-gap`, on by default); `restart_test.go` fails without the fix; replay overlap 2,063 → 2.4 pod-hours (relabels inside an outage, flagged); commit 5a311c1"
lesson: A commutative min merge turns any too-early claim into a rewrite of history; a process that starts without state must bound its claims by what it could have observed
variables: [ec/pods]
state: accepted
---
