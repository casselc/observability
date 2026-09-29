---
id: incident-0f72d6
label: CAST-69
batch: go-upgrade-and-verification-2026-09-29
title: "Two destructive cleanups on the shared dev box: `rm -rf /tmp/go-build*` while other agents were building (it may have removed their in-flight build directories), and a `go clean -modcache` started by accident and killed within a second"
found_by: "The Go-upgrade agent's own report"
hazard: "Verification integrity on the dev box (another agent's build fails or silently uses partial state); near miss on the shared module cache"
controller: "The agent's cleanup: \"these caches are mine to clear\""
why: "Disk was near zero repeatedly and AGENT-RULES asks every agent to keep caches trimmed, without saying which are shared"
fix: "`go mod verify` in parquetgo and query still passes. Control (coordinator): AGENT-RULES now names the shared caches (module cache, /tmp/go-build*, the shared cargo target) and allows clearing only your own private caches or /tmp dirs older than 90 minutes; the module cache is never cleaned"
lesson: "A cleanup rule on a shared machine must name what is shared; \"trim your caches\" invites clearing someone else's"
state: accepted
---
