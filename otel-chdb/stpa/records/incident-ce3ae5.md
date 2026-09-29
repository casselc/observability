---
id: incident-ce3ae5
label: CAST-22
batch: format-v2-2026-09-28
title: A test script deleted the shared `otel` bucket, with every agent's test data in it
found_by: Happened during the format-v2 run of `conformance/go_replay.sh` with `BUCKET=otel`
hazard: H-1-like (test data lost; no real data existed); results already committed were unaffected
controller: "The script's cleanup: \"my bucket is mine\" (its default was private) and \"deleting a non-empty bucket fails\", as on AWS"
why: The default bucket was dedicated to the script; S3 refuses that delete; SeaweedFS does not
fix: The script purges only its own run prefix (`consume purge`); no other script deletes a bucket
lesson: Cleanup must act only on what its own run created; do not rely on a store to refuse a destructive call, since stores differ
state: accepted
---
