---
id: incident-bf5437
label: CAST-56
batch: ci-sweep-after-d35-2026-09-29
title: "The nightly hegel fleet test reported a lost batch that was not lost: the harness judged the run finished once every row had landed, while one copy's announcement was still waiting for a legitimate lease takeover, and the final check then counted that announcement as missing"
found_by: The hegel nightly (run 7), root-caused by an agent into the harness rather than the fleet
hazard: Verification false failure (a red run that is the check's fault erodes trust in every red run)
controller: "The fleet harness: `complete()` and `quiesce` looked at rows only; the final assertion judged rows and announcements"
why: The settle condition was written before announcements existed and was not widened when the final check was
fix: "`complete()` and `quiesce` include announcements and allow the lease failover time; regression test `regression_finish_waits_for_a_copys_announcement`; commit ed669ea; nightly run 11 hegel green"
lesson: A settle condition must wait on everything the final check judges, and must allow time for the failover the system under test is allowed to take
state: accepted
---
