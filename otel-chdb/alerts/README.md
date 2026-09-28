# alerts: the alert evaluator

`alertd` evaluates alert rules through the query service
([`../query/`](../query/README.md), D22) and delivers notices to Alertmanager
(or a generic webhook). It exists for STPA **R-S3** ("alerts evaluate only up
to the complete-through time; a failed evaluation pages") and for
[AMBIGUITY.md](../AMBIGUITY.md) **X5** (alert evaluation), **X6** (paging)
and **X11** (its own state writes). Decision: [D23](../DECISIONS.md).

In one paragraph: a rule is data (a statement, a window, a condition, a
`for`, labels). A window is evaluated only when the query service labels it
`complete`; partial, unknown and failed answers leave it unevaluated, and a
window that stays that way past a bound pages "cannot evaluate" with the
reason. Windows are evaluated in order, and missed ones are caught up after
downtime. Each rule's state (position, groups, the notification ledger) is
one object on S3, replaced only by compare-and-swap, so two replicas can run
side by side with no leader. Notices are committed before they are sent and
leave the ledger only on a 2xx, with a dedup key stable per (rule, group,
episode).

Labels as elsewhere: **[M]** measured here (the shared 4-vCPU box, SeaweedFS
4.47, ClickHouse 26.10.1), **[Q]** Quint, **[D]** read in a source,
**[E]** estimate.

## 1. Running it

```
go build ./cmd/alertd
ALR_CLIENT_SECRET=… ./alertd -config alertd.example.yaml
./alertd -config alertd.example.yaml -check     # load and print the rules, then exit
```

[`alertd.example.yaml`](alertd.example.yaml) is the whole configuration;
[`rules.example.yaml`](rules.example.yaml) holds rules. The environment can
set or override `ALR_LISTEN`, `ALR_RULES`, `ALR_QUERY_URL`, `ALR_S3_ENDPOINT`,
`ALR_S3_BUCKET`, `ALR_S3_REGION`, `ALR_S3_PREFIX`, `ALR_S3_KEY` /
`ALR_S3_SECRET` (otherwise the AWS default chain: IRSA, Pod Identity),
`ALR_SINK_URL`, `ALR_REPLICA`. A query service URL, a state bucket, a sink
and a rules file are required: the evaluator keeps no state it could not
share with its other replica.

Run **two replicas** (a Deployment of 2 with the same configuration; each
gets its pod name as `replica`). Nothing is elected (§4).

**Identities.** Each rule runs as one of the evaluator's own service
identities (`identities`, a rule's `identity`, default `default`), never as a
person: OAuth 2.0 **client credentials** at the IdP the query service trusts
(`token_url`, `client_id`, the secret in `client_secret_env`, `audience`;
the token is renewed at 80% of its lifetime and after a 401), or a
**token file** re-read when it changes (a projected Kubernetes service
account token, or one a sidecar refreshes). The token's claims are the
rule's scope, exactly as for a person (query README §4): map a fleet group
to `"*"` in queryd's `group_grants` for fleet rules, and give a team's rules
an identity whose claims name the team's clusters and namespaces. The
integration test runs one rule of each kind; the cluster-`aa` identity does
not see cluster `ab`'s errors [M].

## 2. Rules

```yaml
rules:
  - name: ErrorBurst
    sql: |
      SELECT ServiceName AS service, count() AS value
      FROM otel_logs WHERE SeverityText = 'ERROR' GROUP BY service
    window: 5m          # event-time length each evaluation reads
    every: 1m           # step between window ends (default: window)
    for: 2m             # must hold this long (in windows' event time) before firing
    condition: {column: value, op: ">", threshold: 100}
    on_no_rows: ok      # or fire: an absence alert
    lateness: 30s       # default evaluation.lateness_s; a margin on top of the service's max_lateness
    severity: page
    identity: default
    clusters: [prod-eu-1]       # optional (D29): narrows the identity's scope; rows and complete_through are these clusters'
    on_late: reevaluate         # D30 (default; or page, ignore): what rows received after a window was evaluated do (§3.1)
    late_horizon: 1h            # how long after its end a window is re-checked for late rows (at most 360 windows)
    late_every: 1m              # how often (default: every, at least 1 m)
    labels: {team: sre}
    annotations: {summary: "{{labels.service}}: {{value}} errors"}
    cannot_evaluate_after: 5m   # default evaluation.cannot_evaluate_after_s
    failures_to_page: 3         # default evaluation.failures_to_page
    max_groups: 1000
```

- The statement goes to `/v1/query` with `window: {from, to}`: the query
  service restricts every table's time column to it (query README §3), so
  the completeness label describes the rows the statement saw.
- **Scope** (D29): the rule's identity's token, narrowed by `clusters` (sent
  as the request's `clusters`; one outside the token is a refusal, 403). The
  query service labels the result with that scope's `complete_through` (per
  cluster, and per signal of the tables read), so a rule on one cluster is
  not held by another cluster's stalled edge; its "cannot evaluate" page
  names only its scope's lanes.
- **Each row is a group.** The condition's column is the value; every other
  column is a label. A value that is missing, null, not a number or not
  finite, two rows for one group, or more than `max_groups` rows is a
  **failed** evaluation (`bad_result`), never "does not hold".
- **Windows** end at multiples of `every` since the Unix epoch, so every
  replica computes the same ones. A new rule starts at the latest window
  that has ended (no history is paged).
- `for` is counted in event time over consecutive complete windows, not on
  the wall clock: the same data gives the same pages however late it is
  evaluated.
- Annotations take `{{value}}` and `{{labels.NAME}}`; nothing else is
  expanded (a rules file is data, not code).
- Changing what decides a rule's outcome (its SQL, window, every, for,
  condition, `on_no_rows`) resolves its firing groups ("the rule changed")
  and starts over from the current position.

## 3. Evaluation (X5, R-S3)

Per rule and tick (`tick_s`), each replica:

1. reads the rule's state;
2. while the next window has ended and its `lateness` has passed (at most
   `max_windows_per_tick` windows), asks the query service for it;
3. **only a complete answer advances**: 200, `completeness: complete`,
   `partial: false`, watermark `ok`, the window the service applied equal to
   the one asked for, the label reports `max_lateness_s`, and
   `complete_through` ≥ the window's end + `max_lateness` + `lateness`.
   **How the two combine** (STPA CAST row 26, D26): `complete_through` is
   custody time, windows are event time. The service's `max_lateness` is
   the fleet's policy for how late a row may be received after its event
   time; its label is `complete` only past end + `max_lateness`. The
   rule's `lateness` is an extra margin the evaluator waits on top of that
   (0 is allowed: trust the policy). A label without `max_lateness_s` (a
   service from before 2026-09-28, whose `complete` meant custody time only)
   is `unknown`, never complete. `cannot_evaluate_after` must exceed
   `max_lateness + lateness`, or every window pages before it can settle. Then each group whose row meets the condition is pending or
   firing, and each group that no longer does is dropped or resolved. "No
   rows" is therefore "nothing holds" only in a complete window;
4. anything else is an **attempt** on the same window, which stays next:

   | answer | outcome | counts as |
   |---|---|---|
   | `partial` (the window extends past `complete_through − max_lateness`, or into the rule's `lateness` margin) | `partial` | waiting |
   | `unknown` (the watermark is stale, missing or unreadable), or a label the evaluator cannot confirm | `unknown` | waiting |
   | no answer within `query.timeout_s` | `timeout` | failure |
   | 5xx, 408, 429, 422 (a pinned limit), a transport error, an unreadable body | `error` | failure |
   | 400, 401 (after one retry with a fresh token), 403 | `refused` | failure; pages at once |
   | a complete answer the rule cannot read | `bad_result` | failure |

   A firing alert is never resolved by a window that is not complete.
5. **Cannot evaluate.** When the window it is at ended more than
   `cannot_evaluate_after` ago (default 5 min, the owner's "alerts about 5
   minutes behind"), or failed `failures_to_page` times in a row, or was
   refused, the rule raises `AlertCannotEvaluate{alert_rule=…}`
   (`severity: page`) with the **reason**: the outcome and error, the
   watermark's status and age, the lanes holding `complete_through` back and
   how far behind they are, the stale lanes, and their clusters, e.g.
   (integration test [M]):

   > the window extends past complete_through 2026-09-28T14:35:05Z; lanes
   > behind: ab/pub-0/traces (16s behind), ab/pub-0/logs (16s behind), …;
   > clusters: ab

   The reason follows later attempts (it became "completeness unknown:
   watermark stale (published …s ago)" when the consumer stopped
   publishing). It resolves when the window is evaluated.
6. **Catching up.** After downtime (the evaluator's or the pipeline's) the
   missed windows are evaluated in order, `max_windows_per_tick` per tick;
   each notice carries `evaluation_delay`. Windows further behind than
   `max_backlog_s` (6 h) are skipped and counted
   (`alr_windows_skipped_total`, which pages from Prometheus), not paged
   one by one.

### 3.1 Late data (D30)

**Each window is evaluated at a basis**: the evaluator asks for `"basis":
"latest"`, so the answer holds exactly the rows received before the
basis's bound per cluster (query README §2.4), and records the window with
that basis and the groups that held (`recent` in the state, kept for
`late_horizon`). Re-running the rule's statement at that basis gives the
same answer: the decision can be audited and replayed.

**Late rows** are the window's rows received after that basis (a row later
than `max_lateness + lateness`, or held at an edge through an outage). Every
`late_every` the evaluator looks for them as a **delta**: first one query
over all the kept windows (the span from the first window's start to the
last one's end, `basis_from` the lowest recorded basis, `basis` the
latest); a delta of zero rows proves no kept window has late rows, and
every window moves to the new basis. Otherwise each window (oldest first,
5 per tick) gets its own delta from its own basis to that same new one; a
window with rows is **re-evaluated at the new basis** (one query over the
whole window, never "old verdict + delta", so no row is counted twice) and
handled by the rule's `on_late`:

| `on_late` | what late rows that change the verdict do |
|---|---|
| `reevaluate` (default) | a group that now holds, and did not, fires a **late episode** if its run of holding windows (the kept windows, with the late rows) satisfies `for`: its own dedup key (episode `late-{window end}`), labels `alert_late="true"`, an annotation saying how many rows came late; it is sent firing, then resolved (the windows are past). A live **pending** group whose run the late rows lengthen back to the window moves its start back, and fires as a live episode (marked `fired_by_late_data`) if that satisfies `for`. Once per run: more late rows into the same run do not page again. |
| `page` | a window whose set of holding groups changed sends one `AlertLateData` notice ("late data changed window W": the groups now holding, those no longer holding, the late row count), fired then resolved. Alert state is unchanged. |
| `ignore` | windows are not kept or checked. |

**Safety.** Late data **never resolves anything and never touches a firing
group**: a firing alert resolves only by a later window evaluated in order
(a late row that makes a window stop meeting the condition updates the
record, nothing else). A window's basis **only moves forward** (a delta sets
it to the delta's upper basis; the next starts there; the service refuses a
`basis_from` above `basis`, `409 basis_regressed`, which the evaluator reads
as "try later"), so no late row is counted twice. A basis the service stops
accepting (a restart with a per-process key; retention) drops that window
from the checks, counted in the state's `late_lost`. The checks are state
writes like evaluations (compare-and-swap; two replicas compute the same
result from the same bases).

**Cost.** One query per rule per `late_every` while nothing is late; per
late window, one delta and one re-evaluation. In the integration test two
replicas running three rules of one identity with 2 s checks exceeded the
service's default `max_concurrent` (4): a 429 is a failed evaluation and
pages, so size `limits` for evaluators with late checks. **Owner decision
(2026-09-28): 16** — every evaluator identity carries the IdP group
`alert-evaluator`, which queryd maps to limits only (`max_concurrent: 16`,
`queryd.example.json`) and never to a grant, so it raises concurrency without
widening any identity's scope; fleet scope comes from a separate group
(`alertd-fleet`). The integration test uses the same value.
The state grows by the kept windows: the holding groups of each, up to
`late_horizon` / `every` windows (360 at most).

Metrics: `alr_late_checks_total{rule,kind,outcome}` (`span` / `window`),
`alr_late_rows_total{rule}`, `alr_late_windows{rule}`, `alr_late_episodes{rule}`.

## 4. State, and two replicas (X11)

One object per rule, `{state.prefix}/{rule}.json`: the next window, the
groups, the current attempt, the "cannot evaluate" alert, the notification
ledger, counters, and the writer's name and a random `write_id`. It is
written only with `If-Match` on the ETag the writer read (`If-None-Match: *`
to create it): the same primitive as the consumer's leases and checkpoints
(D8), and the same requirement on the store (risk 1: Nutanix Objects
unverified).

**Why compare-and-swap and no lease.** A lease elects one evaluator, but a
paused or partitioned holder can still write after its lease ran out, so
the state write needs a fence anyway: the compare-and-swap *is* the safety
mechanism with or without a lease. With it alone:

- every replica runs every rule; the one that loses the swap discards its
  work and re-reads, so the committed history is a chain in which each
  window is evaluated once, in order (checked in the simulation: the
  position never goes back and advances only window by window);
- evaluation is deterministic in the rows of complete windows (event-time
  `for`, episode = the window end where it began), so both replicas compute
  the same state and the same dedup keys; which one wins does not matter;
- nothing depends on clocks for safety; a dead replica blocks nothing.

The cost is duplicate work: both replicas may query the same window. A
replica skips a rule another replica wrote less than `min_gap` (tick / 2)
ago; that is a saving, not a safety property. In the integration test two
replicas had 12 lost swaps and no window evaluated twice in the committed
state [M].

**An unanswered write** is resolved by reading the object back: our
`write_id` means it landed; anything else means "not ours", and the
replica starts over from what is there. Nothing is sent from a state not
known to be committed. A late copy of the write can land only on the
version it was conditioned on, i.e. if nothing was written since, where it
is a valid successor (runner test with a delayed copy).

## 5. Notifications (X6)

- **Outbox.** A notice is written into the ledger in the same state write
  as the evaluation that caused it, and sent only after that write is
  known to have landed. A crash between the two leaves it in the ledger;
  the next tick of either replica sends it.
- **Dedup key** `{rule}/{group hash}/{episode}`: in the `dedup_key`
  annotation, and for Alertmanager the episode is a label
  (`alert_episode`), so each episode is its own alert (a late resolution of
  one episode cannot resolve the next). Labels also carry `alertname`
  (the rule), `severity`, `alert_rule`, `alert_kind` (`rule` or
  `cannot_evaluate`), the rule's labels and the group's (a group label named
  like a reserved one becomes `exported_…`).
- **Only a 2xx is delivered.** No answer, a reset, a timeout, 5xx, 408, 429
  are ambiguous; a 4xx is "not taken". All are retried with the same key
  after a backoff (`backoff_base_s` doubling to `backoff_max_s`); the answer
  is recorded in the ledger (`attempts`, `last_outcome`). Two replicas may
  both send one notice: at-least-once, deduplicated by the key.
- **Firing** is re-sent every `refresh_s` (60 s) while it fires, with
  `endsAt` = now + 4 × refresh: Alertmanager forgets alerts that are not
  re-sent, and resolves them itself if the evaluator dies (the evaluator's
  own absence pages from Prometheus, §6). With `format: webhook` refresh
  defaults to 0: a 2xx ends the firing phase.
- **Resolved** is sent only after the firing was acknowledged, and
  `hold_resolved_s` (45 s, above Alertmanager's default `group_wait` of
  30 s) later. Alertmanager does not notify a resolution it never notified
  as firing, so an episode that fired and resolved inside a catch-up would
  otherwise reach nobody. A notice leaves the ledger when its resolution is
  acknowledged.
- `format: alertmanager` (default) posts Alertmanager API v2 alerts to
  `sink.url` (`…/api/v2/alerts`); `format: webhook` posts `{"version": "1",
  "alerts": [{"dedup_key", "status", "labels", "annotations", "startsAt",
  "endsAt"}]}` for a pager's events API behind an adapter.

## 6. Metrics, health, and the second channel

`GET /healthz` is 200 when a tick completed within 3 ticks (+ the backoff
cap) and the state store answered; `GET /state/{rule}` shows a rule's state
document. `GET /metrics`:

| metric | |
|---|---|
| `alr_evaluations_total{rule,outcome}` | attempts by outcome: `complete`, `partial`, `unknown`, `error`, `timeout`, `refused`, `bad_result` |
| `alr_complete_through_seconds{rule}`, `alr_evaluated_through_seconds{rule}` | the highest `complete_through` seen, the end of the last evaluated window |
| `alr_lag_behind_complete_through_seconds{rule}` | complete but not yet evaluated (a slow query service, catching up) |
| `alr_evaluation_delay_seconds{rule}` | now − the end of the last evaluated window |
| `alr_pending_windows{rule}`, `alr_pending_window_outcome{rule,outcome}` | windows ended and not evaluated; why the next one is not |
| `alr_cannot_evaluate{rule}`, `alr_alerts_firing{rule}` | |
| `alr_windows_skipped_total{rule}` | past `max_backlog` |
| `alr_deliveries_total{outcome}`, `alr_delivery_requests_total{outcome}` | `acked`, `ambiguous`, `5xx`, `rejected` |
| `alr_notices_undelivered{rule}`, `alr_notice_oldest_undelivered_seconds{rule}` | notices whose current phase the pager has not acknowledged |
| `alr_state_writes_total{outcome}`, `alr_state_errors_total{op}` | `ok`, `ambiguous_landed`, `conflict`, `ambiguous_lost`, `error` |
| `alr_rules_skipped_total{rule}`, `alr_last_tick_seconds` | |
| `alr_late_checks_total{rule,kind,outcome}`, `alr_late_rows_total{rule}` | late-data deltas (§3.1) by `span`/`window` and outcome; late rows found in evaluated windows |
| `alr_late_windows{rule}`, `alr_late_episodes{rule}` | windows kept for late checks; late episodes and late-data notices raised |

**The second channel** is Prometheus, not alertd:
[`../deploy/alerts/alert-evaluator.rules.yaml`](../deploy/alerts/alert-evaluator.rules.yaml)
pages when no replica ticks, a notice is unacknowledged for 10 minutes, and
warns on cannot-evaluate, lag, skipped windows and state errors. Route it
to a receiver that does not go through alertd. The `Watchdog` rule in
`rules.example.yaml` always fires; configure the pager to page when it
stops arriving (the dead man's switch; not built here).

## 7. Tests

```
go test ./...                                              # unit, property, simulation (the integration test skips)
ALR_IT_BIN=<dir with otelcol-s3pq and consume> go test ./integration -v
QUINT_BACKEND=typescript ../model/alert_model.sh           # the Quint model
```

- **`internal/engine`**: gating (partial, unknown and every failure leave
  groups and position alone; a firing alert survives them; no rows resolves
  only in a complete window), `for`, absence rules, deterministic keys, the
  cannot-evaluate page and its reason, the ledger (backoff, firing before
  resolution, the hold, refresh), backlog skipping, a changed spec.
  **Property tests** (`rapid`): for any sequence of outcomes the groups,
  episodes and position equal a reference evaluator's over the complete
  windows alone; under any sequence of pager answers a resolution follows a
  2xx for its firing, a notice leaves only after a 2xx for its resolution,
  and every episode is delivered once the pager recovers; the rule pages
  exactly when stuck. Mutants caught: partial read as complete, a
  resolution sent first, no answer read as delivered.
- **`internal/engine`** (`late_test.go`, D30): windows kept with their
  basis and trimmed by the horizon; a clean check moves bases forward,
  never back; incomparable bases skip the span check; `reevaluate` raises a
  late episode once per run (sent firing, then resolved) and never resolves
  a firing group when late rows make it stop holding; `for` over the run,
  a live pending group's run lengthened fires live; `page` sends one notice
  per changed verdict and none for an unchanged one.
- **`internal/qclient`**: the label gate on every field (a property: an
  answer is `complete` exactly when every condition holds), status codes,
  bad values, lateness; client credentials and one retry after a 401; a
  token file; a timeout is a failure. D30: an answer at a basis records it
  (and one not at it does not); no watermark to mint from is `unknown`, not
  a failure; deltas (`InterpretDelta`: 409 → try later, no counted delta →
  a failure, never "no late rows"); evaluations send `basis: latest`, deltas
  `basis_from` + `basis`.
- **`internal/runner`**: catching up 30 minutes in order at 5 windows per
  tick, each window answered complete once; a stalled lane pages after the
  bound with the lanes and clusters, the reason turns to a stale watermark,
  and after recovery the page resolves at the sink; a failing query keeps
  the alert firing and pages; ambiguous state writes (landed, lost, a late
  copy after a newer write); `/healthz`, `/metrics`, `/state`.
- **The simulation** (`runner/sim_test.go`, `rapid`): two replicas on one
  store; writes lost, answered-but-lost, or delayed (a late copy);
  the query service stalling, going stale, timing out, failing; the pager
  losing requests and answers, 5xx, 4xx; one replica's whole tick run
  between the other's read and write. Each run ends caught up with the
  faults off and checks the committed history (the position never goes
  back and moves only by evaluating each window), the sink against a
  reference evaluation (every episode's firing acknowledged, every
  resolved one's resolution; no rule notice the reference lacks; no
  resolution before its firing's 2xx), and that every cannot-evaluate page
  resolved. 1,000 runs in 10 s [M]. Mutants caught: a store without the
  precondition (the history goes back), no answer read as delivered.
- **The late-data simulation** (`runner/simlate_test.go`, `rapid`, D30): the
  same two replicas, faults and interleavings, over a query service whose
  rows have custody times (on-time rows, and late rows received 2–20 min
  after their window) and which answers at bases and deltas; rules with
  `>=` and `<` conditions, `for` 0–2 min, `reevaluate` or `page`. Each run
  checks the committed history (a firing notice turns resolved only through
  a window evaluated in order in that very commit; a window's basis never
  goes back), that every kept window's `late_rows` equals the late rows
  received between the basis it was evaluated at and the one it was last
  checked at (no row counted twice or missed), that its verdict is the
  verdict at that basis, and that a late episode is raised only for a group
  that holds. 100 runs: 268 late rows found, 177 windows revised, 56 late
  episodes or notices [M]. Mutants caught: late data resolving a firing
  group, a window's basis not advanced after a late delta (counted again),
  a span check that ignores its rows (late rows missed).
- **`../model/alertEvaluator.qnt`** [Q]: two replicas, compare-and-swap with
  lost answers and lost writes, `complete_through` advancing, a pager that
  takes a send and loses the answer. `evalOnlyComplete`,
  `resolvedAfterFiringAck`, `noLostEpisode`, `nextMonotone` hold over
  20,000 × 40-step traces; the witnesses are reached; mutants
  `evalPastCt` (also loses an episode: LS-7's story), `ackOnNoAnswer`,
  `blindWrite` are caught (`alert_model.sh`). **Late data** (D30): windows
  evaluated at a basis `ct + 1`; late rows arriving after `complete_through`
  that flip a window's verdict either way; a late check per window (delta,
  re-evaluation, the basis moved forward, a late episode when the window
  now holds outside the live episode). `noDoubleCount` (each window counted
  exactly its late rows between its evaluation basis and its current one),
  `lateNeverResolves` (every resolved live episode was resolved by a window
  evaluated in order whose verdict at its basis did not hold) and
  `lateOnlyIfHolds` hold with the rest over 20,000 × 40; witnesses (a late
  row counted, a late episode acknowledged resolved, a changed verdict)
  reached; mutants `lateDouble` (the basis not moved) and `lateResolves`
  (a late check resolves the live episode) caught.
- **`integration`** [M] (160 s): the Go edge for clusters `aa` and `ab`,
  SeaweedFS, the Rust consumer (`consume run` and `consume watermark`
  pumped every ~2 s), ClickHouse with a read-only user, **queryd built from
  `../query`** with a small OIDC issuer (JWKS + client credentials), two
  evaluator replicas with their state on SeaweedFS, and a fake
  Alertmanager. Errors in both clusters fire (the fleet rule sees both; the
  `aa` identity's rule and the fleet identity's rule narrowed by
  `clusters: [aa]` only `aa`'s) and resolve; the first delivery is taken and
  its answer lost, and is re-sent with the same key. Then `ab`'s edge stops:
  the fleet rule does **not** evaluate the errors sent to `aa` meanwhile and
  pages "cannot evaluate" naming `ab`'s lanes (after 43 s), while the two
  rules scoped to `aa` evaluate them on time (they fire 15 s after the stall
  began) and never page (D29); one replica stops; the consumer stops
  publishing and the reason becomes a stale watermark. The edge returns
  with a third replica: the missed windows are evaluated in order, the
  stall's errors page for the fleet rule 13 s after recovery (evaluation
  delay 90 s), the pages resolve, no window was skipped, no episode appears
  under two keys. **Late data** (D30, between the first phase and the
  stall): two rules on WARN rows (`on_late: reevaluate` and `page`,
  `late_every: 2s`); once both evaluated a past window with nothing
  holding, 3 WARN rows with event times in it are sent through the edge
  now: 16 s later the reevaluate rule pages a late episode (`alert_late`,
  episode `late-…`) and resolves it, the page rule sends "late data changed
  window [19:18:00, 19:18:10)", no live alert fires, and the state counts
  the 3 late rows once (185 s in all). Everything is named `alr-…` / `alr_…` and removed. (The
  service's `max_age_s` is 30 and `cannot_evaluate_after` 45 s: one pump
  cycle, `consume run` then `consume watermark`, takes 13–16 s on the shared
  test box, which a 12 s `max_age_s` read as a stale watermark.)
  Nightly in CI (`alerts-integration`).

## 8. What it does not do

1. **Late rows** (handled since D30, §3.1): rows received after a window's
   basis are found by the late check within `late_horizon` and handled by
   `on_late`. Past the horizon they are not checked; a late episode is not
   a live alert (it is sent and resolved at once), and `for` is judged over
   the kept windows only.
2. **The fleet minimum.** `complete_through` is the minimum over every
   lane, so one cluster's stalled edge stops every rule, a single-cluster
   rule included (the integration test's `aa` rule paged for `ab`'s stall).
   A per-cluster watermark (query README §7.6) would let a scoped rule go on.
3. **Replica lag in central.** The label is the consumer's; a lagging
   ClickHouse replica behind the query service is not in it (AMBIGUITY C3).
4. **A real pager.** Tested against a fake Alertmanager; not against a real
   one, PagerDuty or Opsgenie. The dead man's switch needs the pager side.
   Silences, inhibition and routing are Alertmanager's.
5. **Bounded ledger.** Notices accumulate while the pager is down (one per
   episode); `alr_notices_undelivered` shows it.
6. **Removed rules.** A rule deleted from the file leaves its state and any
   firing notices behind (Alertmanager resolves them at `endsAt`; a webhook
   pager does not).
7. **Rule history (SEC-6).** Rules are a file; change history and review are
   the repository's. No UI.
8. **HyperDX's own alerts** do not go through this evaluator.
