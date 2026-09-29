---
id: requirement-33766d
label: R-S7
title: "Writes are scoped by cluster prefix and role; edge keys cannot delete; the aggregator rejects mismatched clusters. Scheme (built with format v2, 2026-09-27): cluster-first keys; publishers and entity controllers may only create under their own cluster's prefix, keyed on a cluster principal tag in the session credentials; only consumer roles can write or delete control objects. Recorded in [DECISIONS.md](DECISIONS.md) D18"
priority: P0
from: [sec-3e6cd4, sec-555d2d, sec-6f6e02]
state: accepted
---
