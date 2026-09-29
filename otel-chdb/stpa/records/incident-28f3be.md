---
id: incident-28f3be
label: CAST-66
batch: go-upgrade-and-verification-2026-09-29
title: "The entity controller's work-queue goroutine leaked when Run failed (kube-system unreadable, a cache that never synced): Run shut the queue down on its normal exits only"
found_by: "goleak in the controller packages' TestMain"
hazard: "Minor, H-7 class: one goroutine per failed Run, in tests and embedders (entityctl exits after a failed Run)"
controller: "`ctrl.Run`: \"every exit of Run shuts the queue down\""
why: "Run's failure exits were early returns written before the queue existed"
fix: "Run defers the queue shutdown on every exit, and the tests that build a controller without running it shut their queues down; commit 3f861c9"
lesson: "Release a resource on every exit path with a defer at acquisition, not at each return"
state: accepted
---
