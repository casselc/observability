# Model template: every external call is ambiguous

The CAST analysis behind [`../AMBIGUITY.md`](../AMBIGUITY.md) found one
control flaw in all twelve defects: a controller took an unknown outcome
(no answer, an error, a timeout, a restart, a lagging replica) for a
definite one. The models caught it wherever the call they modelled could
lose its answer, and missed it wherever the call was atomic in the model
(the consumer's lease and checkpoint CAS: `s3InlineConsumer.qnt` wrote
them in one step, and the code dropped a lane on a 412 for its own write,
fixed 2026-09-27, AMBIGUITY.md audit a; the renewal and the checkpoint
advance are now modelled as request, effect and answer, below).

**Rule:** in a Quint model, every action that crosses a process boundary
(S3, ClickHouse, Keeper, an OTLP hop, a buffer, the Kubernetes API, a pager)
is at least four actions, and the caller's rule is a `const`:

| Behaviour | Action | What it stands for |
|---|---|---|
| lost request | `lose(q)` | never applies; the caller hears nothing |
| late effect | `apply(q)` enabled at any later step, after `timeout` and after a retry | a PUT that lands after the client gave up; a replicated commit 19 s past `max_execution_time` |
| lost answer | `applyAnswerLost(q)` | applies, and the caller hears nothing **or an error** (a 5xx after the write, `TIMEOUT_EXCEEDED` with a commit to come, 412 for our own write) |
| duplicate delivery | `retry(i)` (and a replay after a crash) | the same request sent again, by the caller or by a queue |

and the caller's rule — wait, verify, fence, or idempotent retry — is a
flag, so the unsafe choice is a named mutant next to the design:
[`ambiguousCall.qnt`](ambiguousCall.qnt) (typechecked, quint 0.32):

```quint
  // late effect: a copy applies at any later step ...
  action apply(q: Req): bool = all {
    inflight.contains(q),
    inflight' = inflight.exclude(Set(q)),
    applied' = if (IDEMPOTENT and applied.get(q.id) > 0) applied
               else applied.set(q.id, applied.get(q.id) + 1),
    heard' = heard.set(q.id, Done), attempts' = attempts,
  }
  // ... or lost answer: it applies and the caller hears nothing (or an error)
  action applyAnswerLost(q: Req): bool = all { inflight.contains(q), /* as apply, */ heard' = heard, ... }
  // lost request
  action lose(q: Req): bool = all { inflight.contains(q), inflight' = inflight.exclude(Set(q)), ... }
  // duplicate delivery; with RESOLVE the caller first reads the effect,
  // and with SETTLE only once no copy can still land
  action retry(i: int): bool = all {
    heard.get(i) == NoAnswer, attempts.get(i) > 0,
    if (RESOLVE) (not(SETTLE) or not(inflight.exists(q => q.id == i))) and applied.get(i) == 0 else true,
    sendCopy(i),
  }
```

`quint run ambiguousCall.qnt --main M --invariant atMostOnce --max-steps 20
--max-samples 3000 --seed 0x5eed` (2026-09-27):

| instance | caller rule | `atMostOnce` |
|---|---|---|
| `verifyFirst` | verify after the settle bound, then retry | ✓ |
| `idempotentRetry` | blind retry into an idempotent effect (create-only slot, dedup token) | ✓ |
| `blindRetry` | blind retry, non-idempotent effect | ✗ (the D3 F1 / awss3exporter U15 shape) |
| `verifyEarly` | verify while a copy can still land | ✗ (the `releaseInFlight` / `errorSettles` shape) |

Model the settle bound as time where it matters (`s3InlineConsumer`'s
`landBy(q) = fence + BUDGET + SLACK`): a copy may apply only up to it, and
the caller may verify only after it.

## Durations: a step takes time

The deterministic simulation found two consumer bugs that every model
missed because a Quint step is atomic and instantaneous (STPA.md, "CAST:
bugs found by deterministic simulation", issues 13 and 14). Wherever a
controller's decision depends on *when* something happened, or a deadline
governs a loop of work, add these three behaviours too, each as a `const`
of the environment with the unsafe (or stalling) choice a named mutant:

| Behaviour | Actions | What it stands for | Mutant it catches |
|---|---|---|---|
| slow observation | `listReq(w)` … `listAnswer(w)`, any number of steps (and ticks) apart | a LIST or GET whose answer arrives later, the state changed meanwhile; the observer dates what it learnt | dated at the request: a renewal written after it looks older than it is (`observeAtRequest`) |
| step duration | each unit of work (`head(w, e)`) advances the clock | HEADs, slot checks, a backlog: work costs time against a lease or deadline | work bounded by count, deadline renewed only at the end (`renewOnlyAtInsert`) |
| own-write conflict on retry | `casSend(w)`, `casLand(q)` / `casLose(q)`, `casAnswer(q, ans)` with `ans` in 200 / none / 412 | a conditional write that applied, answered with an error, retried by the client library on the old ETag: 412 for our own write | a 412 taken as "someone else won" (`own412IsTakeover`) |

From [`s3InlineConsumer.qnt`](s3InlineConsumer.qnt) (typechecked, quint 0.32):

```quint
  // slow observation: the LIST goes out now ...
  action wListReq(w: int): bool = all {
    SLOW_OBS, not(wk.holds), not(wk.listing),
    workers' = workers.set(w, { ...wk, listing: true, listAt: time }), ...
  }
  // ... its answer at any later step, showing the store as it is then; a
  // version not seen before is dated at the answer (the design) or at the
  // request (the mutant)
  action wListAnswer(w: int): bool = all {
    SLOW_OBS, wk.listing,
    workers' = workers.set(w, { ...wk, listing: false, obs: lease,
      seen: if (lease == wk.obs) wk.seen else if (OBSERVE_AT_REQUEST) wk.listAt else time }), ...
  }
  // step duration: a HEAD costs a tick of the holder's window; the design
  // stops the scan once the renewal is due, and may renew mid-scan
  action wHead(w: int, e: int): bool = all {
    mayHead(w, e),   // ... and (SCAN_BY_TIME implies not(renewDue(wk)))
    time' = time + 1,
    workers' = workers.set(w, { ...wk, scanned: ..., work: wk.work + 1 }), ...
  }
  // own-write conflict: the store applies the PUT If-Match iff it still matches ...
  action wCasLand(q: Write): bool = {
    val ok = if (q.kind == CRenew) lease == q.prev else ckpt.version == q.base
    all { q.st == CPending, lease' = if (ok and q.kind == CRenew) q.lease else lease, ...,
          writes' = ... { ...q, st: if (ok) CApplied else CRejected } }
  }
  // ... and the answer: 200, none, or 412 (our own write behind an error answer,
  // or someone else's); anything but a 200 is read back: ours if it shows
  // our write, or still the version we wrote on (our request was lost)
  action wCasAnswer(q: Write, ans: CasAns): bool = {
    val readBack = if (q.kind == CRenew) lease == q.lease else ckpt == q.ckpt
    val prevHeld = if (q.kind == CRenew) lease == q.prev else ckpt.version == q.base
    val keep = if (ans == A200) true else if (ans == A412 and OWN412_TAKEOVER) false else readBack or prevHeld
    all { q.st != CPending, ans == A200 implies q.st == CApplied, q.st == CLost implies ans == ANone, ... }
  }
```

Two of the three are liveness hazards, and `quint run` checks state
invariants on finite runs, not liveness. Check each as the state property
whose violation is its mechanism, and pin the stall with a scripted run:

- step duration: `workFitsWindow`, the holder's own work since its lease
  version was written leaves room to start a statement in the window
  (pauses can still end a window; work alone never does);
  `renewOnlyAtInsertBreaksTest` replays the most eager schedule and the
  checkpoint never moves;
- own-write conflict: `noOwnDrop`, no worker drops a lane whose lease or
  checkpoint is its own latest write, or still the version that write was
  conditional on; `own412IsTakeoverBreaksTest` shows nobody may take the
  lane for TTL + MARGIN. A read-back has three outcomes, not two: our doc
  (applied), the doc we wrote on (not applied: still ours, on the old
  window; write again), anything else (lost). The MBT found the code
  treating the second as the third (`lostRenewalKeepsTest`, 2026-09-27);
- slow observation is a safety hazard: `noLiveTakeover` (no take of a version
  younger than TTL + MARGIN) and `atMostOnce`, with
  `observeAtRequestBreaksTest` for the duplicate.

Records compared for "is it ours" must tell two writes apart: a lease
record is (owner, epoch, sent), so the model renews at most once a tick
(the code's doc carries a `beat`).

## Which models have which behaviour

"✓" the model has the action; "atomic" it does the call in one step
(no ambiguity modelled); "–" not applicable. Checked by reading the
actions on 2026-09-27; the last two columns are the durations above
("instant": the model's observation or work takes no time).

| Model | call | lost request | late effect | lost answer | duplicate delivery | slow observation | step duration |
|---|---|---|---|---|---|---|---|
| `s3Inline.qnt` | data PUT, create-only | ✓ `lose` | ✓ `apply` after `timeout` | ✓ `timeout` → `resolve` | ✓ `switchPayload`, `newIncarnation` replays | – | instant |
| `s3Inline.qnt` | consumer tombstone PUT | ✓ | ✓ | ✓ `cTombTimeout`/`cTombResolve` | – | – | instant |
| `s3Inline.qnt` | consumer INSERT | atomic (`cInsert`) | atomic | atomic | – | – | instant |
| `s3InlineMetrics.qnt` | two lanes' PUTs per request | ✓ `gLose`/`sLose` | ✓ | ✓ `gTimeout`/`sTimeout` | ✓ | – | instant |
| `s3InlineConsumer.qnt`, `…Compact.qnt` | edge PUT | ✓ `lLose` | ✓ `lApply` | ✓ `lTimeout` | ✓ `lSwitchPayload`, `lNewIncarnation`, `senderResend` | – | instant |
| same | INSERT (`wSend`) | ✓ `cDrop` | ✓ `cApply` up to `landBy` | ✓ (answers are not modelled: the worker waits `landBy`) | ✓ (retry, repair) | – | instant (the budget bounds it) |
| `s3InlineConsumer.qnt` | lease discovery (LIST, `try_take`'s GET) | – | – | – | – | ✓ `wListReq`/`wListAnswer` (`SLOW_OBS`; mutant `observeAtRequest`) | – |
| `s3InlineConsumer.qnt` | the step's scan (HEADs) | – | – | – | – | – | ✓ `wHead` costs a tick (`SCAN`; mutant `renewOnlyAtInsert`) |
| `s3InlineConsumer.qnt` | lease take (`TAKE_AMBIG`: `wTakeSend`, `wTakeAnswer`, `wTakeTimeout`; CAST-83: `wTakeRefresh` adopts a late one, mutant `lateTakeLost`), lease renewal, checkpoint advance (CAS) | ✓ `wCasLose` | ✓ `wCasLand` after the answer, a dead process's included | ✓ `wCasAnswer` none / 412 for our own write (`CAS_AMBIG`; mutant `own412IsTakeover`); ✓ `wCasTimeout`: no answer while the request is in flight, read back before it applies, then `wCasLand` of the CLate write (`LATE_CAS`, CAST-50; `wRefresh` takes it back; mutant `lateCkptLost`; for a renewal, CAST-74: `wLeaseRefresh` adopts it, mutant `lateLeaseLost`) | – | – | – |
| same, and `…Compact.qnt` | lease take and release, checkpoint fence, tombstone close, compaction (CAS) | atomic (`wAcquire`, `wRelease`, `wTomb`, `wSeeTomb`, `wCompact`) | atomic | atomic (the code reads back the same way since 034f577) | – | – | – |
| `…Compact.qnt` | lease renewal, checkpoint advance, lease discovery | atomic | atomic | atomic | – | instant | instant |
| same | GC delete | atomic (`gc`) | – | – | – | – | – |
| same | series lane (`sPush`) | ✓ `sLose` | ✓ `sLand` | ✓ | ✓ `sResend` | – | – |
| `s3Native.qnt` | log PUT | ✓ `lose` | ✓ `apply` | ✓ `timeout`/`resolve` | ✓ | – | instant |
| `s3Native.qnt` | lease, GC | atomic (`takeLease`, `gcCommit`) | – | – | – | instant | instant |
| `edgePublish.qnt` | table / Parquet / manifest writes | ✓ (writes fail) | – | ✓ ambiguous writes (F1) | ✓ queue retry | – | instant |
| `fastPath.qnt` | edge → central send | ✓ `fpDrop` | ✓ `fpArrive` after `fpTimeout` | ✓ `fpTimeout` | ✓ `fpGiveUp` + resend | – | instant |
| `completeness.qnt` | object PUT | ✓ `lose` | ✓ `land` | ✓ | ✓ `resend` | instant (the watermark reads LIST results) | – |
| `entityCatalog.qnt` | announcement PUT | ✓ `annLose` | ✓ `annLand` | ✓ (`annMark` before landing) | – | – | – |
| `sealer.qnt` | snapshot commit (CAS) | atomic (`commit`) | – | – (a 412 is modelled: `lost412`; a 412 for its own commit is not) | – | instant | instant |
| `retention.qnt` | – (a sizing rule) | – | – | – | – | – | – |
| `partLifetime.qnt` | – (reads) | – | – | – | – | – | – |

Open, in model terms: the consumer's remaining CAS writes (release,
fence, close, compaction, `gc.json`) and the sealer's snapshot commit need
`applyAnswerLost`; `…Compact.qnt` has none of the durations; the consumer model's
count check reads central exactly (no stale replica, no partial result: the
H-2 hazard of AMBIGUITY.md is outside every model).
