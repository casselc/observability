---
id: incident-57f400
label: CAST-46
batch: service-gate-sweep-2026-09-29
title: "Two readers of parquetgo's schema broke silently when D21 (4d382ac) put `resource_id` and `resource_announce` ahead of the envelope: otap's `Flatten` wrote the envelope at a hard-coded field index and panicked on every batch (its `TestSmokeLocal` failed without any service), and `parquetgo/compare`'s `TestSameRowsAsChdb` kept a stale column expectation"
found_by: Running each module's tests with `test.env` loaded and `OSCOPE_REQUIRE_SERVICES` set
hazard: "Verification gap: the otap spike and the chdb comparison had been failing unnoticed for days; no production path affected"
controller: "4d382ac's author: \"everything that reads parquetgo's schemas was updated\"; module owners: \"a test that needs no service still runs somewhere\""
why: otap is a spike built outside the edge; local runs never load `test.env`, so failing tests were skipped or not run
fix: Columns found by name, `resource_id` written; `TestFlattenResourceAndEnvelope` fails on the old code; the compare check drops and requires the resource columns; commits 055aa3e, 3dae9e8. All gates now go through one helper per language (`testgate`), strict where CI starts the services
lesson: Look fields up by name; after a shared schema change run every module's tests; a skip is an unknown (row 43)
state: accepted
---
