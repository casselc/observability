# Model template: every external call is ambiguous

The CAST analysis behind [`../AMBIGUITY.md`](../AMBIGUITY.md) found one
control flaw in all twelve defects: a controller took an unknown outcome
(no answer, an error, a timeout, a restart, a lagging replica) for a
definite one. The models caught it wherever the call they modelled could
lose its answer, and missed it wherever the call was atomic in the model
(the consumer's lease and checkpoint CAS: `s3InlineConsumer.qnt` writes
them in one step, and the code dropped a lane on a 412 for its own write,
fixed 2026-09-27, AMBIGUITY.md audit a).

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

## Which models have which behaviour

"✓" the model has the action; "atomic" it does the call in one step
(no ambiguity modelled); "–" not applicable. Checked by reading the
actions on 2026-09-27.

| Model | call | lost request | late effect | lost answer | duplicate delivery |
|---|---|---|---|---|---|
| `s3Inline.qnt` | data PUT, create-only | ✓ `lose` | ✓ `apply` after `timeout` | ✓ `timeout` → `resolve` | ✓ `switchPayload`, `newIncarnation` replays |
| `s3Inline.qnt` | consumer tombstone PUT | ✓ | ✓ | ✓ `cTombTimeout`/`cTombResolve` | – |
| `s3Inline.qnt` | consumer INSERT | atomic (`cInsert`) | atomic | atomic | – |
| `s3InlineMetrics.qnt` | two lanes' PUTs per request | ✓ `gLose`/`sLose` | ✓ | ✓ `gTimeout`/`sTimeout` | ✓ |
| `s3InlineConsumer.qnt`, `…Compact.qnt` | edge PUT | ✓ `lLose` | ✓ `lApply` | ✓ `lTimeout` | ✓ `lSwitchPayload`, `lNewIncarnation`, `senderResend` |
| same | INSERT (`wSend`) | ✓ `cDrop` | ✓ `cApply` up to `landBy` | ✓ (answers are not modelled: the worker waits `landBy`) | ✓ (retry, repair) |
| same | lease / checkpoint CAS | **atomic** (`wAcquire`, `wRenew`, `wAdvance`) | **atomic** | **atomic** — the gap audit a found in code | – |
| same | GC delete | atomic (`gc`) | – | – | – |
| same | series lane (`sPush`) | ✓ `sLose` | ✓ `sLand` | ✓ | ✓ `sResend` |
| `s3Native.qnt` | log PUT | ✓ `lose` | ✓ `apply` | ✓ `timeout`/`resolve` | ✓ |
| `s3Native.qnt` | lease, GC | atomic (`takeLease`, `gcCommit`) | – | – | – |
| `edgePublish.qnt` | table / Parquet / manifest writes | ✓ (writes fail) | – | ✓ ambiguous writes (F1) | ✓ queue retry |
| `fastPath.qnt` | edge → central send | ✓ `fpDrop` | ✓ `fpArrive` after `fpTimeout` | ✓ `fpTimeout` | ✓ `fpGiveUp` + resend |
| `completeness.qnt` | object PUT | ✓ `lose` | ✓ `land` | ✓ | ✓ `resend` |
| `entityCatalog.qnt` | announcement PUT | ✓ `annLose` | ✓ `annLand` | ✓ (`annMark` before landing) | – |
| `sealer.qnt` | snapshot commit (CAS) | atomic (`commit`) | – | – (a 412 is modelled: `lost412`; a 412 for its own commit is not) | – |
| `retention.qnt` | – (a sizing rule) | – | – | – | – |
| `partLifetime.qnt` | – (reads) | – | – | – | – |

Open, in model terms: the consumer's CAS (lease, checkpoint, `gc.json`) and
the sealer's snapshot commit need `applyAnswerLost`; the consumer model's
count check reads central exactly (no stale replica, no partial result: the
H-2 hazard of AMBIGUITY.md is outside every model).
