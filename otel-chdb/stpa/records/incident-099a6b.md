---
id: incident-099a6b
label: CAST-43
batch: partition-key-and-dead-lane-retirement-2026-09-28
title: "Two tests had been skipping silently for lack of a bucket and were broken underneath: the horizon audit's end-to-end test wrote a format-v1 lane the consumer never discovers, and the announcements test compared quote-escaped TSV"
found_by: The D34 agent running the consumer suite with S3 and ClickHouse present
hazard: "Verification gap: the horizon audit (a detection control for silent loss) and announcements had no working end-to-end test since format v2"
controller: "Test authors: \"a test that skips without services still runs somewhere\"; CI runs these tests only where the services exist, and reports skips as passes"
why: Skipping keeps the fast suite runnable on a laptop; the format-v2 change updated the code but not a test nobody saw fail
fix: "Both tests fixed (3d0e5b3). Follow-up for CI: a service-backed job must fail if any service-gated test skipped"
lesson: A skipped test is an unknown, not a pass; count skips where the services are supposed to exist
state: accepted
---
