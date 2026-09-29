---
id: incident-75bcf1
label: CAST-15
batch: ambiguity-audit-2026-09-27
title: A 412 on the worker's own lease or checkpoint write dropped the lane
found_by: "Audit item a2: a proxy that applies the `If-Match` PUT and then answers 500, 503 or 409; object_store's retry carries the old ETag and gets 412 against our own new object"
hazard: L-1 (the lane stalls for TTL + margin, 95 s) and a misreported `lanes_lost_cas`; safe for data
controller: "Consumer `write_lease` / `write_ckpt`: \"a 412 means another worker changed the object\""
why: The code handled a lost answer by reading back, but a 412 is a definite-looking answer; the client library's own retry was invisible to it; the model's CAS is atomic
fix: A 412 is read back and compared, like no answer; test `a_412_for_our_own_lease_or_checkpoint_write_keeps_the_lane` (fails before the fix)
lesson: A definite-looking error can be produced by our own retry; the outcome class depends on the whole call, including the library's retries, not on the last status code
variables: [consumer/lease, consumer/checkpoint]
state: accepted
---
