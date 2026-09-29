---
id: incident-b3fbf4
label: CAST-57
batch: ci-sweep-after-d35-2026-09-29
title: The lake UI test rig (lakeuirig) left the `otelcol-s3pq` edge processes it had started running after it exited, when it was signalled during setup
found_by: "The journeys agent: the first local run hung on a read-only SeaweedFS; after SIGTERM to the rig two edge processes were still alive"
hazard: H-7 class on the shared test machine only (resources beyond budget; leaked processes keep writing into a store that is already short of disk); no product hazard
controller: "The rig's cleanup: \"every edge was already stopped by `edge.stop()` on the normal path\""
why: Setup had never failed midway before; `edge.post` has no HTTP timeout, so a hung store blocks setup instead of failing it
fix: The rig records every edge it starts and kills them all in cleanup; `rig.mjs` sends SIGTERM when setup times out; commit 004ee01. No automated regression (needs a store that hangs)
lesson: A harness that starts child processes owns their lifetime on every exit path, not only the happy one; a setup step with no timeout turns a broken dependency into a hang
state: accepted
---
