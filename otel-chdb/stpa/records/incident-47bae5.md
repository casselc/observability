---
id: incident-47bae5
label: CAST-81
batch: models-and-verification-2026-09-29
title: "The new mutation-testing nightly repeatedly ran green or red for the wrong reason: the build failed outside the copied tree, the consumer globs matched nothing, release builds took 7 minutes per mutant, overflow panics failed the unmutated tree, timeouts counted as survivors, and runaway mutants killed the runners"
found_by: "Reading each run's logs (a run that tests nothing looks green)"
hazard: "Verification gap: a mutation job that judges nothing gives false assurance"
controller: "The mutants job: \"a finished run judged the mutants it was given\""
why: "Each fault showed as a plausible green or an infrastructure red rather than as \"no mutants judged\""
fix: "`--in-place`, globs on file names, a `mutants` build profile, overflow checks off, timeouts as caught, `--timeout 120` and `ulimit -v`; the job now requires all 16 shard files and counts the mutants judged before trusting a green"
lesson: "A verification job must prove it examined something (counts, shard files) before its green means anything"
state: accepted
---
