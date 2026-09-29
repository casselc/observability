---
id: incident-386343
label: CAST-54
batch: ci-sweep-after-d35-2026-09-29
title: From D30 on, the lake UI and the Mosaic spike planned at the basis `latest` and **left out** everything received after `complete_through` — the lake-only rows D24 exists to show — with only a "N newer object(s) left out" note. Both nightly e2e suites failed at their first test, so 9 lake UI and 6 Mosaic tests after it never ran; three of those were broken too (expiry tests relying on a cached plan, no-watermark tests getting 503 on `latest`, Mosaic URL mode planning without a basis)
found_by: The nightly result (18,000 rows against 18,600), then reading the specs past the first failure
hazard: "R-S1/R-S2 and H-4 class: data silently left out of a view, completeness shown as settled where rows had arrived"
controller: "The D30 implementer and the lake UI's planning: \"a basis is a full answer\" — for a lake-first UI it is only the settled part, so *left out* replaced *shown incomplete*"
why: D30 was about stable answers and caches; the note looked like disclosure; D30 itself recorded that the e2e did not exercise the basis — and nobody ran it (row 51)
fix: "The owner's option (b): `/v1/plan` returns the basis part unchanged plus a labelled, never-cached tail; the UI and Mosaic draw the tail incomplete; the e2e asserts the contract (basis 18,000 unchanged as data arrives, tail 600 + new rows drawn incomplete); af8615e, b5b57d0"
lesson: A decision that changes what a view reads must run that view's end-to-end suite before it counts as built; **a failure at test 1 hides every later test** — a suite with an early failure has not really run
variables: [ui/basis]
state: accepted
---
