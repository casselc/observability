---
id: incident-dedb06
label: CAST-24
batch: query-service-2026-09-28
title: The entity aggregator pasted S3 object key, cluster and lane names into the text of its `ingest_log` and `lane_progress` INSERTs; a key with a quote under cluster `c1` forged a `c2` `ingest_log` row (`put_at` 2100), `c2` was never ingested, and every pass stopped at that object
found_by: Code reading while adding catalog lag (R-S5) to the query service; `TestHostileKeysAreData` failed before the fix
hazard: H-6 (one cluster's controller credentials write another cluster's catalog rows); H-5 (a stale cluster's catalog lag reads as fresh); H-3 (the catalog stalls for every cluster after the bad object)
controller: "Entity aggregator: \"key names are our own well-formed `{epochMs}-{instance}/{seq}.delta.ndjson.gz` and safe in SQL\"; \"a failing object succeeds on retry, so stopping the pass is safe\""
why: Our own lane writer makes those keys; D18 confines each controller to its prefix; `clusterFilter` already checked record bodies, so bodies looked like the only attack surface; tests used only well-behaved writers
fix: "Rows go as JSONEachRow data; gap-record times are parsed and re-rendered (unreadable ones skipped); a failing lane no longer stops other lanes or clusters (errors collected). `TestHostileKeysAreData` (real ClickHouse + SeaweedFS), `TestGapTimesAreParsed`; commit d96e32d. Remaining: a malformed body still fails its own lane each pass, now confined to that lane"
lesson: "Names a less-trusted writer chooses are data, like bodies: prefix ABAC limits *where* a writer writes, not what its key names say. One tenant's bad object must not stop the pipeline for the others"
variables: [agg/entity_records]
state: accepted
---
