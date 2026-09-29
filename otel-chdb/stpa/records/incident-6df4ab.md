---
id: incident-6df4ab
label: CAST-32
batch: closing-the-validation-gaps-2026-09-28
title: "The query service's `metadata` scope (D25) served `SELECT * FROM system.tables` whole: `data_paths`, `metadata_path` (real server paths), `uuid`, `storage_policy`, sizes, and `system.databases.engine_full`, to any caller with the `query` role"
found_by: Coordinator review of the adapter report (the agent's own limits noted the paths); confirmed by a probe on ClickHouse 26.10
hazard: "STPA-Sec / R-S8: information disclosure to every query caller; DDL columns could carry credentials (ClickHouse masks them today [M])"
controller: "Query service `sqlscope`: \"restricting which rows (tables) are visible also bounds which columns are\""
why: The scope was designed around per-row predicates and grants already cut system tables to the served ones; the replay checked answers for equality with ClickHouse, not for what they disclosed
fix: Every metadata read rewritten as a projection onto a per-table column allow-list (`*` expands to it; other columns error), re-checked on the rebuilt text (`metadata_unprojected`); `metadata_test.go` unit + rapid, the replay's column check; commit b302126
lesson: A schema-read scope needs a column allow-list, not only a table allow-list; "equal to ClickHouse" is not "safe to show"
variables: [qs/grants]
state: accepted
---
