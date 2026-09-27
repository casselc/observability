// The standard shape of one external call with an ambiguous outcome
// (../AMBIGUITY.md). A caller sends request `r` to a service; the service
// applies it at most once per copy it receives, and the caller hears
// "done", "not done", or nothing.
module ambiguousCall {
  type Req = { id: int, attempt: int }
  type Answer = Done | NotDone | NoAnswer

  const IDS: Set[int]
  const MAX_ATTEMPTS: int
  const IDEMPOTENT: bool      // the service applies a copy of an applied id as a no-op
  const RESOLVE: bool         // the caller reads the effect before retrying (verify)
  const SETTLE: bool          // ... and only once no copy can still land (the settle bound)

  var inflight: Set[Req]      // sent, not yet applied or lost: the late-effect window
  var applied: int -> int     // how many times each id took effect
  var heard: int -> Answer    // what the caller last heard per id
  var attempts: int -> int

  action init = all {
    inflight' = Set(), applied' = IDS.mapBy(_ => 0),
    heard' = IDS.mapBy(_ => NoAnswer), attempts' = IDS.mapBy(_ => 0),
  }

  // sendCopy: a copy of the request leaves the caller
  action sendCopy(i: int): bool = all {
    attempts.get(i) < MAX_ATTEMPTS, heard.get(i) != Done,
    inflight' = inflight.union(Set({ id: i, attempt: attempts.get(i) })),
    attempts' = attempts.set(i, attempts.get(i) + 1),
    applied' = applied, heard' = heard,
  }

  // send: the first attempt
  action send(i: int): bool = all { attempts.get(i) == 0, sendCopy(i) }

  // late effect: a copy applies at any later step, after a timeout or a
  // retry included; the answer to it may be delivered ...
  action apply(q: Req): bool = all {
    inflight.contains(q),
    inflight' = inflight.exclude(Set(q)),
    applied' = if (IDEMPOTENT and applied.get(q.id) > 0) applied
               else applied.set(q.id, applied.get(q.id) + 1),
    heard' = heard.set(q.id, Done), attempts' = attempts,
  }

  // ... or lost answer: it applies and the caller hears nothing (or an
  // error: a 5xx after the write, TIMEOUT_EXCEEDED with a commit to come)
  action applyAnswerLost(q: Req): bool = all {
    inflight.contains(q),
    inflight' = inflight.exclude(Set(q)),
    applied' = if (IDEMPOTENT and applied.get(q.id) > 0) applied
               else applied.set(q.id, applied.get(q.id) + 1),
    heard' = heard, attempts' = attempts,
  }

  // lost request: it never applies
  action lose(q: Req): bool = all {
    inflight.contains(q),
    inflight' = inflight.exclude(Set(q)),
    applied' = applied, heard' = heard, attempts' = attempts,
  }

  // timeout: the caller stops waiting; the copy stays in flight
  action timeout(i: int): bool = all {
    heard.get(i) == NoAnswer, inflight.exists(q => q.id == i),
    heard' = heard.set(i, NoAnswer),
    inflight' = inflight, applied' = applied, attempts' = attempts,
  }

  // duplicate delivery: the caller (or a queue replay after a crash)
  // sends again. With RESOLVE it first reads the effect and stops if it
  // is there; the read is only sound once no copy can still land.
  action retry(i: int): bool = all {
    heard.get(i) == NoAnswer, attempts.get(i) > 0,
    if (RESOLVE) (not(SETTLE) or not(inflight.exists(q => q.id == i))) and applied.get(i) == 0 else true,
    sendCopy(i),
  }

  // verify: once no copy can still land, read the effect (HEAD the slot,
  // GET the lease, count the rows) and take it as the answer
  action verify(i: int): bool = all {
    RESOLVE, heard.get(i) == NoAnswer, attempts.get(i) > 0,
    not(SETTLE) or not(inflight.exists(q => q.id == i)), applied.get(i) > 0,
    heard' = heard.set(i, Done),
    inflight' = inflight, applied' = applied, attempts' = attempts,
  }

  action step = any {
    nondet i = oneOf(IDS)
    any { send(i), timeout(i), retry(i), verify(i) },
    nondet q = oneOf(inflight)
    any { apply(q), applyAnswerLost(q), lose(q) },
  }

  val atMostOnce: bool = IDS.forall(i => applied.get(i) <= 1)
}

module verifyFirst {
  import ambiguousCall(IDS = Set(1, 2), MAX_ATTEMPTS = 3, IDEMPOTENT = false, RESOLVE = true, SETTLE = true).* from "./ambiguousCall"
}

module blindRetry {
  import ambiguousCall(IDS = Set(1, 2), MAX_ATTEMPTS = 3, IDEMPOTENT = false, RESOLVE = false, SETTLE = true).* from "./ambiguousCall"
}

module idempotentRetry {
  import ambiguousCall(IDS = Set(1, 2), MAX_ATTEMPTS = 3, IDEMPOTENT = true, RESOLVE = false, SETTLE = true).* from "./ambiguousCall"
}

module verifyEarly {
  import ambiguousCall(IDS = Set(1, 2), MAX_ATTEMPTS = 3, IDEMPOTENT = false, RESOLVE = true, SETTLE = false).* from "./ambiguousCall"
}
