---
id: incident-94b029
label: CAST-85
batch: models-and-verification-2026-09-29
title: "A regression in 0902ec1 (the CAST-84 fix) let two workers hold one lane: a take being given back was re-adopted while its release (conditional on the take's ETag) was still in flight; the release then landed, another worker took the lane, and both issued statements under one lease"
found_by: "Nightly 53 dst level 1, seeds 5307618, 5307784, 5309558 (the oracle: a statement issued while the lease was another worker's or released); one night after the push, never released"
hazard: "H-2, safety class: a second writer under one lease (exactly-once at risk)"
controller: "`give_back` / `refresh_own_takes`: \"a pending take may be adopted\", even one we had decided to give back"
why: "Keeping the take pending looked like the fix for CAST-84, and adoption was already the rule for pending takes"
fix: "11509c1: a release with no known answer goes to `giving_back`, is re-sent while our take is stored, and that take is never adopted again; tests `a_take_whose_release_may_still_land_is_not_held_again` and pinned seeds `a_take_given_back_is_not_held_while_its_release_may_land`; the mutants nightly then found the guard that ends a pending release untested, closed by `a_pending_release_ends_once_our_take_is_no_longer_stored` (71fe7a7); nightly 56 green"
lesson: "Never act on the version an in-flight conditional write targets; a liveness fix must be re-checked against the safety invariants, and the nightly seed sweep is what caught this one"
state: accepted
---
