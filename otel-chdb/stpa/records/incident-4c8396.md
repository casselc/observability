---
id: incident-4c8396
label: CAST-49
batch: langfuse-design-2026-09-29
title: "Near miss in the design: resolving score facts \"latest by custody time\" lets a retried older correction, received later, override a newer one"
found_by: The D36 agent, checking its own resolution rule against at-least-once delivery
hazard: "H-L8, H-L3: a corrected score silently reverted"
controller: "Designer: \"custody order is the order facts were meant in\"; at-least-once retries reorder them"
why: Custody order is sound for completeness (D19) and for the basis, so it looked sound for precedence too
fix: Each fact names the fact it supersedes (a `supersedes` chain, R-L6, AMBIGUITY X24); custody order only breaks ties between unrelated facts
lesson: Ordering by custody time is not intent when the writer retries; precedence must be stated by the writer, not inferred from arrival
state: accepted
---
