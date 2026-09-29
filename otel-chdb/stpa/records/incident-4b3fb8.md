---
id: incident-4b3fb8
label: CAST-16
batch: ambiguity-audit-2026-09-27
title: The count check could silently return short counts
found_by: "Audit item b: the consumer's own query through a ClickHouse profile with `read_overflow_mode = 'break'` returned 3,932 instead of 8,000 per key with HTTP 200"
hazard: "H-2 (a short count makes the worker \"repair\" rows central already holds: duplicates)"
controller: "Consumer `sql.rs`: \"HTTP 200 means a complete answer\""
why: The consumer's settings never set a break mode, and a server or user profile it doesn't control can
fix: Every consumer and audit query pins the eleven `*_overflow_mode` settings to `throw` (`NO_PARTIAL_RESULTS`), so a limit becomes an error; two tests
lesson: Feedback the controller acts on must be complete by construction, not by default; pin anything the environment could change. The same flaw exists in HyperDX (AMBIGUITY.md row X7)
variables: [consumer/row_counts]
state: accepted
---
