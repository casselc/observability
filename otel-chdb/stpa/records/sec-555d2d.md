---
id: sec-555d2d
label: SEC-2
title: A stolen edge key overwrites or deletes lane objects
unsafe: Corrupts the process the consumer reads
hazards: [hazard-6fa4d1, hazard-d14498]
mitigation: Create-only writes; no delete permission at the edge; object lock or versioning on lane prefixes
state: accepted
---
