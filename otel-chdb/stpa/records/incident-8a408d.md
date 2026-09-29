---
id: incident-8a408d
label: CAST-27
batch: alert-evaluator-2026-09-28
title: "Near miss: agents sharing one checkout's git index. The alerts agent's staged edits landed in the lake UI agent's commit 9835f2f, and its own first commit, built from a private index on a stale base, would have reverted the coordinator's CAST commit 5f13738"
found_by: The alerts agent, checking its diff before pushing
hazard: H-7 (the safety record — STPA/CAST — silently reverted); history attributing work to the wrong change
controller: "Coordinator: \"separate paths per agent are enough isolation\"; agents: \"my index holds only my changes\""
why: Agents worked in disjoint directories, but shared files (DECISIONS.md, AMBIGUITY.md, nightly.yml, ci/README.md) and one index made the separation incomplete
fix: "Caught before push; the commit amended. Rule added for agents: check `git diff --cached --name-only` and `git diff origin/<branch> --stat` of the commit before pushing; concurrent agents get separate worktrees next time"
lesson: Isolation must cover the shared state (the index and shared docs), not only the files each actor intends to touch; the check that caught it was a diff against the remote, so make it mandatory
state: accepted
---
