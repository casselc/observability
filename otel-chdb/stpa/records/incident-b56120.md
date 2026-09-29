---
id: incident-b56120
label: CAST-65
batch: go-upgrade-and-verification-2026-09-29
title: "A basis token was accepted in four spellings: base64url decoding ignored non-zero trailing bits, so one basis had several strings that all passed the MAC"
found_by: "TestKeyringStateMachine (rapid state machine over the basis keyring)"
hazard: "SEC class: a token string was not an identity for caches or the audit trail (the MAC was never weakened)"
controller: "Basis parse: \"base64url decoding is canonical\""
why: "Go's base64 RawURLEncoding is lenient about trailing bits by default, and the MAC check made the token look tamper-proof"
fix: "Strict base64 decoding; commit 07d9b72; TestTokenTrailingBitsAreRefused and FuzzDecode (an accepted token is the one canonical encoding)"
lesson: "Where a string is used as an identity, parsing must be canonical as well as authenticated"
state: accepted
---
