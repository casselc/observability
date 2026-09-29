---
id: incident-747f83
label: CAST-60
batch: d36-phase1-and-ci-2026-09-29
title: "Clippy in one agent's worktree used an `otap_s3pq` rmeta built from another agent's worktree through the shared CARGO_TARGET_DIR (same metadata hash, fingerprint looked fresh) and reported symbols missing that the code had"
found_by: "The D36 agent, running clippy locally on its change"
hazard: "Verification integrity (H-7 class on the dev box): a check can pass or fail on another worktree's code; recurrence of CAST-12"
controller: "The coordinator's AGENT-RULES, which told every agent to reuse one shared CARGO_TARGET_DIR: \"cargo fingerprints every input, so sharing a target across worktrees is safe\""
why: "The disk kept running out and a Rust target is about 2 GB per checkout; the rule traded CAST-12's control (separate target dirs per checkout) for disk without re-checking what CAST-12 was protecting against"
fix: "Workaround in the moment: `touch src/lib.rs` before each check. Control (coordinator, 2026-09-29): AGENT-RULES keeps the shared target for dependencies but requires `cargo clean -p otap-s3pq` (and any other workspace crate being checked) before every clippy or test run from a worktree, so the workspace crate is always rebuilt from that worktree's source; Rust builds stay under the heavy lock"
lesson: "A resource-saving rule that relaxes an earlier CAST control must say which hazard it re-opens and how it is still held; a cache shared across checkouts must not serve the checkout-specific parts"
state: accepted
---
