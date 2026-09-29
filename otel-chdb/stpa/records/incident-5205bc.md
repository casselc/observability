---
id: incident-5205bc
label: CAST-28
batch: hyperdx-adapter-2026-09-28
title: Fork patch 0001 (overflow modes pinned to throw) left a jest expectation in `clickhouse.test.ts` without the pins, so the suite failed; the patch had never had its tests run
found_by: Running common-utils' jest suites for the first time while building patch 0002
hazard: No runtime hazard; a verification gap on X7 / H-2 (the control against silent partial results was unverified)
controller: "Fork patch author (an earlier agent): \"every default-settings expectation was updated\""
why: The full `yarn install` did not fit on disk, the patch applied and parsed, and it was reviewed by grep; "applies cleanly" stood in for "tested"
fix: Expectation updated and carried in 0002 (0001's history left as is); the suite is the regression check (2,665 tests pass); commit adb427f
lesson: A patch series is unverified until its tests have run; when the full toolchain doesn't fit, a per-package type-check and test install (~230 MB) does
state: accepted
---
