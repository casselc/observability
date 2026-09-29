---
id: incident-8f96b4
label: CAST-42
batch: partition-key-and-dead-lane-retirement-2026-09-28
title: A resource announcement statement arriving just past its fence, on a ClickHouse whose clock ran ahead, was skipped by the server's silent fence and answered with an empty OK; `announce_first` took that as landed, so the lane's rows were inserted before their announcements (`sameLane` broken; in another seed the announcements were never ingested)
found_by: "`dst_consumer` seeds 1950 (server 359 ms ahead) and 331"
hazard: "Rows in central without their resource announcement: entity attribution missing (LS-5, AMBIGUITY X9); the ordering the catalog relies on broken"
controller: "Consumer worker: \"an OK answer means the announcement ran\"; its fence check uses its own clock, which cannot see the server's"
why: Data inserts use the same silent fence safely because the count check afterwards finds missing rows; announcements have no verify step, so the same fence was silently unsafe for them
fix: Announcements get a loud fence (`throwIf`, error `OTAPRS_FENCED`, settled with nothing written); `a_server_fenced_announcement_is_not_taken_for_landed` and a real-ClickHouse check; commit 3d0e5b3
lesson: A statement without a verify step needs a fence that fails loudly; a safety mechanism is safe only together with the check that backs it
variables: [consumer/statement, consumer/fence]
state: accepted
---
