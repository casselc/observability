---
id: incident-7cdd84
label: CAST-77
batch: models-and-verification-2026-09-29
title: "The nightly hdxadapter-integration chain test failed once (statement #331) and passed on rerun: it compared `groupUniqArray` answers exactly, and ClickHouse merges each thread's set in the order the threads finish, so element order varies between runs"
found_by: "The nightly hdxadapter-integration job (run 22 area); root-caused by the verification agent"
hazard: "Verification false failure on H-5/H-6 checks (CAST-56 class)"
controller: "The chain test's comparison: \"ClickHouse answers are deterministic\""
why: "The replay test had its own leniency for order; the chain test was written later without it"
fix: "One shared comparison (`unorderedAggregates`, `arrayVerdict`: differ, differ only in order, or cut at the size cap) used by both tests; `TestUnorderedAggregateAnswersCompareAsSets`; commit acf670d"
lesson: "Comparison rules belong in one place, not in each test; an intermittent failure is a counterexample until its cause is named"
state: accepted
---
