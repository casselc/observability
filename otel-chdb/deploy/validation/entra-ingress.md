# Entra ingress: validation runbook (a real tenant, one Windows and one Mac)

research/entra-ingress.md and DECISIONS.md D37 were built and tested against a **fake** Entra
issuer only (`otel-chdb/ingress/entratest`). A test issuer more permissive than the real one proves
nothing about authorisation (STPA.md CAST row 30), so every claim below is NOT VERIFIED until this
runbook has run. Record the results in `deploy/results/entra-ingress.txt` with the commit, the
ingress image digest, the forwarder build, OS versions and the date (row 12).

## What this settles

| Q | Question | Tied to | Step |
|---|---|---|---|
| ENT-1 | Real Entra v2.0 tokens for the ingress API verify: issuer form, `tid`, `azp`, `scp`, `roles`, `idtyp`, key `issuer` property | R-E2, §5.2 | §2 |
| ENT-2 | The refusal matrix holds against real tokens (Graph audience, another client, a guest or other-tenant token, expired) | SEC-E4, SEC-E7 | §3 |
| ENT-3 | Windows: MSAL.NET + WAM acquires silently for a signed-in, compliant user; prompts only from the status item | §4.1, TM-E2 | §4 |
| ENT-4 | macOS: MSAL.NET ≥ 4.73.1 + Enterprise SSO plug-in (and Platform SSO where enrolled) acquires silently; the signed-bundle and main-thread requirements hold | §4.1 | §5 |
| ENT-5 | The three Langfuse integrations send through the forwarder unchanged, and the committed rows carry the stamped tenant and person | §2, §5.3 | §6 |
| ENT-6 | Buffer fates: offline, sign-out, account switch, non-compliance, TTL; drops counted and reported | R-E7, E9 | §7 |
| ENT-7 | Retries across replicas are copies in central; the drain's close retires lanes | R-E4, R-E5, D35 | §8 |
| ENT-8 | Query scoping: a devtools reader sees only their pairs (after the pair-grant change) | R-E8 | §9 |

## 0. Environment

- An Entra tenant where you may create app registrations and a Conditional Access policy scoped
  to **the ingress API only** (never tenant-wide). Two test users in different team roles; one
  guest user; a second tenant (a free developer tenant) for the cross-tenant token.
- One Windows 11 device and one macOS 14+ device, Intune-enrolled (or Jamf with the Intune
  compliance integration), Company Portal installed, the SSO extension profile on the Mac.
- The EKS validation cluster (deploy/validation/eks-aws.md) with its bucket; the ingress runs as a
  Deployment of 2 replicas behind a TLS load balancer; the consumer and central as for the other
  validation runs. Use a prefix of your own (`ent-`), remove it at the end.

## 1. Registrations

1. Ingress API: single-tenant, App ID URI `api://oscope-ingress`, manifest
   `requestedAccessTokenVersion: 2`, scope `Telemetry.Write`, app roles `Team.Payments`,
   `Team.Search` (users/groups), `Telemetry.Write.App`, `Telemetry.CI.Payments` (applications),
   optional claim `idtyp` on access tokens, assignment required.
2. Forwarder: public client, broker redirect URIs for both platforms, API permission
   `Telemetry.Write` granted.
3. CA policy on the ingress API: require a compliant device; client apps: the forwarder.
4. Ingress config (`cmd/oscope-ingress`): `entra.tenants` = [home tenant id], `audiences` =
   [API client id, `api://oscope-ingress`], `client_apps` = [forwarder id, CI app id], policy
   cluster `devtools`, prefix `dev-`.

## 2. ENT-1: a real token

On the Windows device, with the forwarder signed in, `oscope-forwarder token --decode` (prints
claims, never the token). Expect `ver` 2.0, `iss` = `https://login.microsoftonline.com/<tid>/v2.0`,
`aud` = the API client id, `azp` = the forwarder, `scp` contains `Telemetry.Write`, `roles`
contains the user's team role, `idtyp` = `user`. Fetch
`https://login.microsoftonline.com/common/discovery/v2.0/keys` and record whether the signing
`kid` carries an `issuer` property and its form. Send one span; expect 200 and the ingress log line
without the token.

## 3. ENT-2: refusals with real tokens

With `az account get-access-token` or a small MSAL script (never the forwarder's cache):
a Graph token (`https://graph.microsoft.com/.default`) → 401; a token for the API from another
client (Azure CLI's) → 401 (and, with CA, possibly refused at issuance: record which); the guest
user → 401 or 403 (record which, and the claims); the second tenant's user (API not consented
there) → issuance refused, record the AADSTS code; an expired token (saved, used after 90 min) → 401;
a user with no team role → 403. After each, confirm nothing new in the ingress's lanes.

## 4. ENT-3: Windows

Install the MSIX/`.intunewin` through Intune. Sign in to Windows as user 1. Expect no prompt; the
status item shows the user and "sending". Lock/unlock, reboot, sleep overnight: still no prompt.
Remove the work account from Windows settings: the forwarder shows "signed out", holds nothing new
for the old account and deletes its entries (count in its log).

## 5. ENT-4: macOS

Install the `.pkg` through Intune or Jamf. Confirm the app is a signed, notarised bundle
(`codesign -dv --verbose=4`, `spctl -a -vv`). Expect silent acquisition; with Platform SSO enrolled,
record whether the first acquisition prompts. Run a debug build that calls MSAL off the main thread
and record the failure (documents the requirement). Record MSAL.NET, Company Portal and macOS
versions.

## 6. ENT-5: the integrations

Configure opencode, Codex and Claude Code's Langfuse integrations with the forwarder's
`LANGFUSE_BASE_URL` and local keys (`LANGFUSE_MEDIA_UPLOAD_ENABLED=false`). Run one session in
each. In central: rows with `ResourceAttributes['k8s.cluster.name']='devtools'`,
`['k8s.namespace.name']='dev-payments'`, `['user.id']` = user 1's object id,
`['oscope.ingress.auth']='entra'`; set `k8s.namespace.name=prod-billing` and `user.id=mallory`
in the tool's `OTEL_RESOURCE_ATTRIBUTES` and confirm they appear only under
`oscope.ingress.claimed.*`. From the same Mac as a second local user, post to user 1's forwarder
port without the key → refused.

## 7. ENT-6: buffer fates

Offline for an hour (Wi-Fi off) while using a tool: the tool is never slowed; the buffer grows;
reconnect: sent, 200s, buffer empty. Mark the device non-compliant (an Intune compliance policy
requiring an OS version it lacks): CA refuses the token; the forwarder holds and shows it; set the
TTL to 10 min in the config profile and confirm the drop and the `oscope.forwarder.dropped`
record after compliance returns. Switch accounts: user 1's entries are deleted, never sent under
user 2 (check `user.id` in central). Inspect the buffer files: no plaintext span content, no token
(`strings`, `grep` for a known prompt).

## 8. ENT-7: copies and the close

Kill the load balancer's connection mid-request (or `iptables` drop the answer at the ingress pod)
so the forwarder retries to the other replica: central has one copy (the content key is equal;
`consumer_audit_duplicate_rows` unchanged). Scale the ingress from 2 to 1: the stopped replica's log
says `custody empty, N lanes closed`, and `consumer_lane_retirements_total` counts its lanes;
`complete_through` for cluster `devtools` keeps moving.

## 9. ENT-8: scoping

After the pair-grant change (DECISIONS D37 O-E6): a reader granted `devtools/dev-payments` and
`<k8s cluster>/search` queries `devtools/search` and `<k8s cluster>/dev-payments` and gets nothing
(and the scope error, not an empty success, if the service distinguishes them). Before the change,
record that the crossed pairs ARE granted (the finding), with the query and its result.

## 10. Cleanup

Delete the `ent-` prefix, the test registrations and the CA policy, the test users; unenrol the
devices from the test compliance policy.
