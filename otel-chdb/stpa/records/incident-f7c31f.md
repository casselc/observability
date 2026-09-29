---
id: incident-f7c31f
label: CAST-58
batch: d36-phase1-and-ci-2026-09-29
title: "A consumer test that checks a refused-credentials error is redacted asserted that the text contained no \"403\"; the text names the object, whose random id sometimes contained 403, so the test failed about once in 700 runs"
found_by: "ci.yml run 126, the Hegel step (which also runs the consumer's lib tests)"
hazard: "Verification false failure; the test guards H-6 (CAST-31)"
controller: "The test `refused_credentials_are_unsettled_and_redacted`: \"403 appears in the error text only for an S3 refusal\""
why: "The chance per run is about 0.15%; it passed every earlier run"
fix: "The object path is taken out of the message before the check, and the id now always starts with 403 so the case is exercised every run; reproduced locally first (id 4031b1a1fc9); commit 2e9e543"
lesson: "Take random identifiers out of a message before a substring check; an intermittent failure is a counterexample, not a flake"
state: accepted
---
