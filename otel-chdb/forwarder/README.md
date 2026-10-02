# forwarder: the device forwarder (MSAL.NET + YARP, a streaming pass-through)

DECISIONS.md D37 and D40 (with its amendment "pass-through, 2026-10-02": the owner chose a real
proxy, leaving HTTP behaviour to the tool), design and STPA in
[../research/entra-ingress.md](../research/entra-ingress.md) (§4, §10b), the server it talks to
in [../ingress](../ingress/README.md), manual validation on real devices in
[../deploy/validation/entra-ingress.md](../deploy/validation/entra-ingress.md) (ENT-6, ENT-F1..F7).

Developer tools (the Langfuse opencode, Codex and Claude Code integrations) send OTLP/HTTP to
this process on `127.0.0.1` with a per-user local key pair as their Langfuse keys. The forwarder
gets an Entra access token for the ingress API through the platform broker (WAM on Windows, the
Enterprise SSO extension / Platform SSO on macOS) and **proxies each request live** to the
ingress with that token: the body streams up as the tool sends it, the ingress's status, headers
and body stream back unchanged. There is no queue, no retry and no buffering beyond what
streaming needs; the tool's exporter owns retries, back-off, timeouts and batching. **A 200 to
the tool is the ingress's, so it means committed.** Nothing is written to disk.

## Layout

| Path | What |
|---|---|
| `src/Oscope.Forwarder/Http/LocalEndpoint.cs` | what the tools talk to: the guards, the bounds, the token, YARP; `GET /status` |
| `src/Oscope.Forwarder/Http/IngressProxy.cs` | the forwarding client and the header transform |
| `src/Oscope.Forwarder/Auth/TokenGate.cs` | the token per request; after a 401, a forced refresh for the next request |
| `src/Oscope.Forwarder/Auth/ITokenAcquirer.cs` | the broker seam (silent only) |
| `src/Oscope.Forwarder/ProxyOptions.cs` | the bounds, validated together |
| `src/Oscope.Forwarder/ForwarderHost.cs` | Kestrel on loopback, YARP, the endpoint |
| `src/Oscope.Forwarder.App/` | the device binary `oscope-forwarder`: config, `--init-keys`, MSAL.NET + broker |
| `tests/Oscope.Forwarder.Tests/` | xUnit: the pass-through property, proxy tests on real sockets, E2E and stress |
| `tests/Oscope.Forwarder.E2EHost/` | a TEST host: the library with the Go harness's broker model instead of MSAL (never shipped) |

## Behaviour

**What the forwarder decides itself** (its answers carry `X-Oscope-Forwarder: <reason>` and a
one-line text body):

| Answer | Reason | When |
|---|---|---|
| `401` | `auth` | not the local key |
| `403` | `origin`, `host` | an `Origin` header (a web page); a non-loopback `Host` (DNS rebinding) |
| `404` | (routing) | another path than `/v1/{traces,logs}`, `/api/public/otel/v1/{traces,logs}` |
| `415` | `content_type`, `content_encoding` | not `application/x-protobuf` / `application/json`; not identity / gzip |
| `413` | `too_large` | over `maxRequestBytes` (16 MiB, the ingress's own cap), by `Content-Length` or while a chunked body streams |
| `503` + `Retry-After` | `busy` | `maxConcurrentRequests` (16) already in flight: answered at once, never queued |
| `503` + `Retry-After` | `no_token` | nobody signed in, CA or the broker refuses, the broker unavailable or slower than `tokenTimeout` (10 s); never a prompt |
| `502` | `ingress_error` | no connection, a reset, a lost answer: **unknown**, the ingress may have committed |
| `504` | `ingress_timeout` | no progress from the ingress within `activityTimeout` (100 s): **unknown** |

Why `503` and not `401` with no token: at this endpoint a 401 already means "wrong local key", and
OTLP exporters retry 429/502/503/504 (honouring `Retry-After`) but drop a 401's batch; a missing
sign-in is usually fixed within minutes from the status item.

**Everything else is the ingress's answer**, passed through: `200` (committed), `401`, `403`,
`413`, `415`, `400`, `429` and `503` with their `Retry-After` and bodies. A `502`/`504` from the
forwarder and the ingress's `503` mean "unknown": the exporter's retry of the same bytes is a
content-key copy if the first attempt landed (D11; CAST rows 50, 74, 83).

**Headers.** Every request header of the tool's is dropped (its `Authorization` is the local key;
`x-langfuse-*`, user ids, `traceparent`, `X-Oscope-Namespace` from the tool, cookies, forwarding
headers…); only `Content-Type`, `Content-Encoding` and `Content-Length` pass (chunked framing is
the client's). Then the bearer token and the MDM-configured `X-Oscope-Namespace` (if any) are set.
The forwarding client propagates no trace context of its own: the default would inject a
`traceparent` parented on the tool's. The ingress stamps the person from the token (R-E1).

**Tokens.** The broker is asked per request (it caches). A `401` from the ingress goes to the tool
and is never replayed; the next request that would get the same token gets a forced refresh
instead, once (single flight), and at most once per `forcedRefreshMinInterval` (10 s) so a refused
refreshed token cannot drive a refresh loop (CAST 39). Each request goes out under whoever is
signed in when it is sent: there is nothing held across an account switch, but a batch the tool
produced before a switch and sends after it goes under the new person (LS-E5 as changed,
AMBIGUITY E9); `/status` counts account changes.

**Memory.** Bounded by `maxConcurrentRequests` × the per-request buffers (Kestrel's 64 KiB request
pipe, YARP's copy buffer, the client's): a slow ingress holds the tool back through TCP, it does
not grow the forwarder (tested: the heap does not grow with a stalled 16 MiB body; stress: resident
memory under a bound independent of the bytes moved).

**Counters** (`GET /status`, with the local key; for the status item, TM-E1): `account`,
`account_changes`, `in_flight`, `max_concurrent_requests`, `proxied.{2xx,4xx,5xx}` (the ingress's
answers), `proxy_errors.*` (YARP's), `local_refused.*`, `token_failures.*`,
`forced_token_refreshes`. What the tool's exporter finally drops is counted in the tool.

## Run

```bash
oscope-forwarder --init-keys        # once per user: writes keys.json (0600) and prints the tools' env
oscope-forwarder --config config.json
```

`config.json` (MDM-delivered; unknown fields are an error):

```json
{
  "ingress": "https://ingress.example.com",
  "entra": { "clientId": "<forwarder app id>", "tenantId": "<home tenant id>",
             "scope": "api://oscope-ingress/Telemetry.Write" },
  "port": 14318,
  "namespace": null,
  "proxy": { "maxRequestBytes": 16777216, "maxConcurrentRequests": 16, "tokenTimeout": "00:00:10",
             "activityTimeout": "00:01:40", "refusalRetryAfter": "00:00:05", "forcedRefreshMinInterval": "00:00:10" }
}
```

Not built: the status item (tray / menu bar; `GET /status` is its data), the interactive
sign-in UI (`MsalBrokerTokenAcquirer.SignInInteractiveAsync` is there for it), packaging
(MSIX / `.intunewin`, a signed and notarised `.app` + LaunchAgent), the check that the config
names only the signed defaults' tenant and ingress (§4.5).

## Test

```bash
ci/forwarder.sh test     # build (-warnaserror); the property and proxy tests (not E2E, not Stress)
ci/forwarder.sh e2e      # the Go ingress harness and a forwarder process; no-disk check on Linux
STRESS_SECONDS=120 ci/forwarder.sh stress
```

CI: `ci.yml` job `forwarder` on ubuntu, windows and macos (test + e2e); `nightly.yml` job
`forwarder-stress` (selectable: `jobs=forwarder-stress`; `stress_seconds`). The proxy tests run
the forwarder and a fake ingress on real Kestrel and real loopback sockets, so streaming and TCP
backpressure are the real ones.

| Hazard / lesson | Must show | Test (technique) |
|---|---|---|
| R-E5, UCA-E4, H-E4: what the tool is told | the tool gets the ingress's status, `Retry-After`, headers and body, unchanged and unmarked; a 200 is a commit | `Any_body_status_and_header_set_passes_through_unchanged…` (PH: CsCheck over bodies, statuses, answers, client headers); E2E `A_200_means_committed…`, `The_ingress_answer_reaches_the_tool_unchanged…` (IT); stress: every ingress answer matched by number (FI) |
| LS-E3, H-E6: bytes unchanged, never replayed | the ingress reads the tool's bytes (SHA-256); one ingress request per tool request | the property (PH); `A_lost_answer_is_502…_never_replayed…` (FI); E2E `A_lost_answer_after_the_commit_is_502…_the_tools_retry_is_a_copy` (IT: one content key); stress |
| R-E1, SEC-E2, SEC-E7: nothing trusted from the tool | only content type, encoding, length, the bearer and the configured namespace reach the ingress; no trace context | the property (PH); `Only_the_bearer_token_the_namespace_and_framing_reach_the_ingress`, `Without_a_configured_namespace…` (FI); E2E and stress: the harness's header names |
| R-E6, UCA-E8, H-E9: a token that expires in flight | the 401 passed through, not replayed; the next request has a fresh token (one forced refresh); never a prompt | `After_a_401_the_next_request_gets_a_fresh_token…` (FI), the property (PH), E2E `A_token_that_expires_in_flight_is_a_401_once…` (IT) |
| CAST 39: no refresh loop | a refused refreshed token forces no second refresh within the interval | `A_refused_refreshed_token_cannot_drive_a_refresh_loop` (ML) |
| H-E9, TM-E2: no token | `503` + `Retry-After` + why, within the token timeout; nothing reaches the ingress | `With_no_token_the_tool_is_answered_503…` (FI: signed out, broker unavailable, broker hung), E2E `With_nobody_signed_in…` (IT) |
| H-E1, LS-E5: account switch | each request goes out under the person signed in when it is sent; counted | `Each_request_goes_out_under_whoever_is_signed_in…` (FI), E2E `After_another_person_signs_in…` (IT) |
| R-E7, CAST 38: bounded memory, streaming | the request streams (the ingress reads the first part while the tool holds the rest); a stalled ingress holds the tool back by TCP and the heap does not grow; the concurrency bound refuses at once | `The_request_is_streamed_not_buffered`, `A_slow_ingress_holds_the_tool_back_by_TCP…`, `Over_the_concurrency_bound…` (FI); E2E `A_slow_ingress_is_seen_by_the_tool_as_latency…` (IT); stress: resident memory (FI) |
| R-E7: no disk | no file created, written, renamed or deleted by the forwarder process | `ci/forwarder.sh e2e` under strace (IT, a workflow step record) |
| CAST 50, CAST 2: failures below HTTP | no answer is `502`, no progress is `504`, both marked and retryable | `A_lost_answer_is_502…`, `An_ingress_that_never_answers_is_504…`, `A_connection_that_never_opened_is_502` (FI) |
| SEC-E3, LS-E2, SEC-E9: the local endpoint | Origin, Host, key, content type, encoding, size (by length and while streaming), path refused, marked | `The_local_endpoint_refuses_anything_but_this_users_tool`, `Status_needs_the_local_key` (FI) |
| CAST 5, CAST 25: parameters together | bounds that cannot work together refused | `Options_that_cannot_work_together_are_refused` (P) |

Every test carries `OscopeTrace.Covers(technique, ids)`; `ci/trace/dotnet_trx.py` turns the
claims and the TRX into trace records (ci/README.md, "Traceability").

**Validated only manually** (deploy/validation/entra-ingress.md, ENT-6 and ENT-F1..F7): the MSAL
broker on Windows (WAM) and macOS (SSO extension, Platform SSO), Conditional Access, real tokens,
the signed bundle and main-thread requirement on macOS, the real integrations through the
forwarder (their exporters' retries of identical bytes), sleep and network loss on a real laptop.
