---
id: incident-02a04a
label: CAST-31
batch: closing-the-validation-gaps-2026-09-28
title: A ClickHouse SYNTAX_ERROR answer echoes the raw statement text, including the `s3()` access key, secret (and now session token); the consumer kept such errors in its logs and state unredacted
found_by: Measuring secret handling while adding session credentials to `s3()` (the server log shows `[HIDDEN]`; the error body does not)
hazard: "STPA-Sec: a credential disclosed to whoever reads consumer logs (H-6 path: another cluster's data readable with it)"
controller: "Consumer: \"ClickHouse hides secrets in `s3()`\", generalised from its server log to its error answers"
why: The server log does mask them, and errors were logged for diagnosis; no test ever produced a syntax error with credentials in the statement
fix: "`sql.rs` `redact` strips key, secret and token from every kept error; `refused_credentials_are_unsettled_and_redacted` against real ClickHouse + SeaweedFS; commit 09ed92e. `--ch-s3-auth server` keeps secrets out of statements entirely"
lesson: A redaction guarantee holds only on the channel it was measured on; check every path a secret-bearing string can travel, errors included
state: accepted
---
