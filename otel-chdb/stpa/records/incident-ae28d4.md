---
id: incident-ae28d4
label: CAST-63
batch: d36-phase1-and-ci-2026-09-29
title: "ci.yml went red at 25fcbe3 because a sed edited a CAST record after its tables were rendered; the commit that fixed it (6ef251f) changed only Markdown, so paths-ignore skipped CI and no run verified the head. The STPA migration had made Markdown files CI inputs (records, generated sections, VERIFICATION.md cells) while the CAST-51 rule still treated every .md push as prose"
found_by: "The Go-upgrade agent's report: ci run 141 red in TestRepositoryGeneratedUpToDate; the coordinator then found no run at the newer head"
hazard: "Verification gap (a red or unverified head that looks settled); CAST-51 class"
controller: "The coordinator (render, then edit, then commit without `stpa check`) and ci.yml's trigger: \"a Markdown-only push cannot change what CI checks\""
why: "The skip was added when every .md file was prose (CAST 51); D39 moved checked data into .md files without revisiting the trigger, and trace_test.py passing looked like enough before the commit"
fix: "ci.yml triggers on `paths` that exclude prose Markdown but re-include otel-chdb/stpa/**, STPA.md, VERIFICATION.md and the research notes with generated sections; a ci run dispatched at the head; coordinator rule: `stpa check` is the last command before any commit touching records"
lesson: "When data moves into a file type a trigger filter treats as prose, the filter must move with it; the check that guards generated output must run after the last edit, not after the render"
state: accepted
---
