---
id: incident-3865dc
label: CAST-67
batch: go-upgrade-and-verification-2026-09-29
title: "`ingress.Bearer` returned ok with an empty token for \"Bearer \" followed by blanks"
found_by: "FuzzBearer"
hazard: "None in effect: Verify refused the empty token"
controller: "`ingress.Bearer`: \"a header that starts with the scheme carries a token\""
why: "The happy-path and missing-header cases were tested; blanks after the scheme were not"
fix: "Bearer refuses it like a missing header; commit a9b7be9"
lesson: "A parser that feeds an authenticator should refuse what it cannot use rather than rely on the next layer"
state: accepted
---
