---
id: incident-57ce6e
label: CAST-41
batch: query-service-and-hyperdx-decisions-2026-09-28
title: "**Recurrence of row 28.** Fork patch 0003's own jest expectation (`completeness.test.ts`, \"reads the adapter headers\") omitted the `atBasis: false` field 0003 added, so the suite failed; 0003 had been checked by transpiling and stubbing, never by running jest"
found_by: Running the full common-utils jest suite for patch 0004
hazard: No runtime hazard; the evidence behind R-S1/R-S2 (the banner) was weaker than reported
controller: "The D30 agent: \"logic and assertions checked\"; the **coordinator** accepted \"partial checks, disk was low\" and reported 0003 as built, although row 28's lesson was exactly this"
why: Disk ran low during D30 (row 38), and the reduced check was stated honestly, so accepting it looked like a reasonable trade
fix: "Expectation fixed in 0004; all 39 suites (2,674 tests) pass (72cee23). Coordinator rule from now on: a fork patch is \"built\" only after its package suites have run under jest; a reduced check is recorded as **not verified** and scheduled, not accepted"
lesson: A lesson recorded but not turned into a gate recurs; when a CAST lesson applies to our own process, it becomes a checklist item the coordinator enforces
state: accepted
---
