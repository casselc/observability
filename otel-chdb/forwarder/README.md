# forwarder: the device forwarder (MSAL.NET + YARP, in memory only)

DECISIONS.md D37 (with the owner's revision: no disk buffer; YARP), design and STPA in
[../research/entra-ingress.md](../research/entra-ingress.md) (§4, §10a, §10b), the server it
talks to in [../ingress](../ingress/README.md), manual validation on real devices in
[../deploy/validation/entra-ingress.md](../deploy/validation/entra-ingress.md) (ENT-F1..F7).

Developer tools (the Langfuse opencode, Codex and Claude Code integrations) send OTLP/HTTP to
this process on `127.0.0.1` with a per-user local key pair as their Langfuse keys. The forwarder
answers the tool at once from memory, gets an Entra access token for the ingress API through
the platform broker (WAM on Windows, the Enterprise SSO extension / Platform SSO on macOS), and
passes each request's bytes, unchanged, to the ingress with that token. The telemetry is
**best effort** (D37): what is dropped is counted and reported, never silent, and the tool is
never blocked or prompted.

## Layout

| Path | What |
|---|---|
| `src/Oscope.Forwarder/Core/ForwarderCore.cs` | the queue and retry policy: no I/O, no threads, no clock of its own |
| `src/Oscope.Forwarder/Core/AgeClock.cs` | the core's time: max(monotonic, wall) elapsed, never going back |
| `src/Oscope.Forwarder/Pump.cs` | the I/O shell: takes attempts, gets a token for the entry's own account, sends, reports |
| `src/Oscope.Forwarder/Http/YarpIngressSender.cs` | the pass-through: YARP's `IHttpForwarder` with a bearer transform |
| `src/Oscope.Forwarder/Http/LocalEndpoint.cs` | what the tools talk to; `GET /status` for the status item |
| `src/Oscope.Forwarder/CountersReport.cs` | the counters as an OTLP/JSON log record, sent through the same queue |
| `src/Oscope.Forwarder.App/` | the device binary `oscope-forwarder`: config, `--init-keys`, MSAL.NET + broker |
| `tests/Oscope.Forwarder.Tests/` | xUnit: model-based, property, fault, E2E and stress tests |
| `tests/Oscope.Forwarder.E2EHost/` | a TEST host: the library with the Go harness's broker model instead of MSAL (never shipped) |

## Behaviour

**The tool's answer.** `200` (an empty `Export*ServiceResponse`) means *held in memory, best
effort*: it is not a commit. `503` + `Retry-After`: the queue is full (backpressure, never a
buffer beyond the bound), nobody is signed in yet, or the forwarder is stopping. `413`: over the
per-request bound. `401`/`403`/`415`/`404`: not this user's tool (wrong local key, an `Origin`
header, a non-loopback `Host`, a content type a browser sends without a preflight, another
path). Nothing on this path waits for Entra or the network (H-E9, TM-E2).

**The queue.** Bounded in bytes and in entries, counting what is in flight (default 32 MiB,
1024 entries, 16 MiB per request); memory is bounded by `MaxQueueBytes + MaxConcurrentIntake ×
MaxEntryBytes`. Nothing is written to disk (R-E7; checked in CI under strace). Each entry keeps
the account `(tid, oid)` signed in when it was accepted and is sent only with a token for that
account (R-E6, LS-E5); when the broker answers for someone else, the entry is dropped and counted.

**Outcomes of an attempt** (research/entra-ingress.md §1.9 row 2; CAST rows 50, 74, 83):

| Seen | Meaning | Then |
|---|---|---|
| 2xx | committed (the ingress answers 200 only on the edge's verdict, R-E5) | done |
| 401 | the token was refused before the body was read | refresh once at once; then back off, refreshing |
| 429 | refused before publishing | retry after `Retry-After` (capped) |
| 400, 413, 415, other 4xx | definite refusal | drop, counted `rejected` |
| 403 | no grant for this person | drop, counted `forbidden` |
| 5xx (the ingress's 503 = "may still commit"), a lost answer, a timeout, a reset | **unknown**, not failed: it may have landed | retry the same bytes (a copy if it landed, D11); a later drop is counted apart as `dropped_maybe_landed` |
| connection refused, DNS, TLS handshake, no token | definitely not sent | back off and retry |

Every retry is bounded by attempts (12) and by age (10 min) (CAST 39); back-off doubles from
500 ms to 30 s with jitter, and `Retry-After` is honoured up to 60 s. On stop, running attempts
get a drain timeout; whatever is left is counted (`shutdown`; in flight: maybe landed).

**The ledger** (H-E4): `accepted = committed + dropped + held`, always (`/status` shows
`balances`). Counters: `accepted`, `committed`, `committed_after_unknown` (possibly a second
copy at the ingress, which the consumer skips), `refused.{full,too_large,no_account,shutting_down}`
(never acknowledged), `dropped.{reason}` and `dropped_maybe_landed.{reason}`, `held_entries`,
`held_bytes`, `oldest_age_s`, `token_failures.{status}`. They go to the ingress every 5 min as
an `oscope.forwarder.counters` log record in the person's own namespace (TM-E1). A crash loses
the queue with no count (nothing is persisted); the last report's `held_entries` with no later
report is how it shows.

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
  "queue": { "maxQueueBytes": 33554432, "maxAge": "00:10:00" },
  "pump": { "reportInterval": "00:05:00" }
}
```

Not built: the status item (tray / menu bar; `GET /status` is its data), the interactive
sign-in UI (`MsalBrokerTokenAcquirer.SignInInteractiveAsync` is there for it), packaging
(MSIX / `.intunewin`, a signed and notarised `.app` + LaunchAgent), the check that the config
names only the signed defaults' tenant and ingress (§4.5).

## Test

```bash
ci/forwarder.sh test     # build (-warnaserror) and every test but E2E and Stress
ci/forwarder.sh e2e      # the Go ingress harness and two forwarder processes; no-disk check on Linux
STRESS_SECONDS=120 ci/forwarder.sh stress
```

CI: `ci.yml` job `forwarder` on ubuntu, windows and macos (test + e2e); `nightly.yml` job
`forwarder-stress` (selectable: `jobs=forwarder-stress`; `stress_seconds`).

| Hazard / lesson | Must show | Test (technique) |
|---|---|---|
| H-E4: acked then dropped silently | the ledger balances after every step; every drop has a counted reason | `CoreModelTests` (SM: CsCheck, reference model, swarm faults); `Stopping_counts_every_held_entry` (FI); stress (FI) |
| H-E6, LS-E3, CAST 50/74/83: a lost answer | unknown, not failed; the retry is the same byte array; counted `committed_after_unknown` | `A_lost_answer_is_unknown_not_failed_…` (SM), `A_lost_answer_is_retried_with_the_same_bytes…` (FI, YARP), `A_lost_answer_after_the_commit_is_resent_as_a_copy` (IT: one content key at the ingress) |
| R-E7, CAST 38: bounded memory under a slow ingress | held bytes ≤ bound; refusals instead of growth; resident memory not growing with bytes sent | `A_full_queue_refuses…` (SM), `A_stalled_ingress_never_blocks…` (FI), `A_slow_ingress_never_blocks…` (IT), stress (RSS) |
| R-E7: no disk | no file created, written, renamed or deleted by the forwarder process | `ci/forwarder.sh e2e` under strace (IT, a workflow step record) |
| H-E9, TM-E2: the tool never blocked or prompted | the tool's answer within 2 s under every fault; no interactive call on the path | fault, E2E and stress latency asserts; `ITokenAcquirer` is silent-only |
| R-E6, UCA-E8: token expiry mid-flight | one forced refresh, resent, committed; 401 is definite (not unknown) | `A_401_refreshes…` (SM), `A_token_that_expires_mid_flight…` (FI), `A_token_that_expires_in_flight…` (IT) |
| H-E1, LS-E5: account switch | never sent under another person; dropped and counted | `An_entry_is_never_sent_under_another_account` (SM), `Requests_accepted_under_one_person…` (FI), `Held_requests_are_dropped…` (IT) |
| R-E1, SEC-E2, SEC-E7: nothing trusted from the tool | only content type/encoding, the bearer and the configured namespace leave; the ingress stamps the person | `The_tools_bytes_pass_through_unchanged…` (FI), `Committed_under_the_person_of_the_token…` (IT) |
| SEC-E3, LS-E2: the local endpoint | Origin, Host, key, content type, encoding, size refused | `The_local_endpoint_refuses_anything_but_this_users_tool` (FI) |
| CAST 39: bounded retries | attempts and age bound every loop; Retry-After capped; back-off under the cap | `Every_retry_loop_is_bounded…` (ML), `Retry_after_is_honoured…` (P) |
| CAST 26/34/44: two clocks, sleep | a clock set back changes nothing; a night asleep ages entries | `A_clock_that_goes_back…`, `An_age_clock_only_advances…` (P2C) |
| CAST 25: parameters together | options that cannot work together refused | `Options_that_cannot_work_together_are_refused` (P) |

Every test carries `OscopeTrace.Covers(technique, ids)`; `ci/trace/dotnet_trx.py` turns the
claims and the TRX into trace records (ci/README.md, "Traceability").

**Validated only manually** (deploy/validation/entra-ingress.md, ENT-F1..F7): the MSAL broker
on Windows (WAM) and macOS (SSO extension, Platform SSO), Conditional Access, real tokens, the
signed bundle and main-thread requirement on macOS, the real integrations through the
forwarder, sleep and network loss on a real laptop.
