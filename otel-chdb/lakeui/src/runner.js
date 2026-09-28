// The re-plan state machine (AMBIGUITY.md X8; the rules every plan carries):
//
//  1. a plan is used only while now < replan_after; no read of an object
//     STARTS at or after it (a read already running may finish: its URL is
//     valid until expires_at, a margin later);
//  2. any failed read (a 403 above all: an expired or revoked URL) means
//     re-plan, never "no data"; re-plans are bounded (maxReplans), and when
//     they run out the result says which objects it lacks;
//  3. what was read is kept by key across re-plans (a slot is create-only:
//     the same key is the same bytes), and the result covers exactly the
//     last plan's objects, so its label describes the rows it holds.
//
// Pure apart from the injected plan / read / clock.

/**
 * @template T
 * @param {object} o
 * @param {(req: any, opts: {force: boolean}) => Promise<any>} o.plan     returns a normalised plan
 * @param {any} o.request
 * @param {(obj: any, plan: any) => Promise<T>} o.read                    reads one object
 * @param {(plan: any) => any[]} [o.select]                               the objects this pass needs (default: all)
 * @param {() => number} [o.now]
 * @param {number} [o.maxReplans]
 * @param {number} [o.concurrency]
 * @param {(ev: object) => void} [o.onEvent]
 * @param {Map<string, T>} [o.done]                                       results by key from an earlier pass
 */
export async function runPlanned({ plan: getPlan, request, read, select = p => p.objects, now = Date.now,
  maxReplans = 3, concurrency = 6, onEvent = () => {}, done = new Map() }) {
  const results = new Map(done)
  const errors = new Map() // key -> last error
  const events = []
  const emit = ev => {
    events.push(ev)
    onEvent(ev)
  }
  let replans = 0
  let skippedForExpiry = 0
  let planError = null
  let plan = await getPlan(request, { force: false }) // PlanError propagates: a refusal is the answer
  emit({ type: 'plan', requestId: plan.requestId, objects: plan.objects.length, replanAfterMs: plan.replanAfterMs })
  for (;;) {
    const todo = select(plan).filter(o => !results.has(o.key))
    if (todo.length === 0) return finish()
    // rule 1: a plan past replan_after is not used for any new read
    if (now() >= plan.replanAfterMs) {
      if (replans >= maxReplans) return finish('the plan expired and re-plans ran out')
      if (!(await replan('replan_after'))) return finish('re-plan failed: ' + planError.message)
      continue
    }
    let failed = false
    let expired = false
    let next = 0
    const worker = async () => {
      while (next < todo.length) {
        const o = todo[next++]
        if (now() >= plan.replanAfterMs) { // rule 1 again, per object
          expired = true
          skippedForExpiry++
          continue
        }
        emit({ type: 'read', key: o.key })
        try {
          results.set(o.key, await read(o, plan))
          errors.delete(o.key)
        } catch (e) {
          failed = true
          errors.set(o.key, e)
          emit({ type: 'read_error', key: o.key, kind: e?.kind ?? 'error', status: e?.status ?? 0, message: String(e?.message ?? e) })
        }
      }
    }
    await Promise.all(Array.from({ length: Math.min(concurrency, todo.length) }, worker))
    if (!failed && !expired) continue // loop once more: todo is empty now → ok
    if (replans >= maxReplans) return finish(failed ? 'reads failed and re-plans ran out' : 'the plan expired and re-plans ran out')
    if (!(await replan(failed ? 'read_error' : 'replan_after'))) return finish('re-plan failed: ' + planError.message)
  }

  // A failed re-plan (the service refused or did not answer) ends the query
  // with what it lacks; it is never retried silently.
  async function replan(why) {
    replans++
    emit({ type: 'replan', why, replans })
    try {
      plan = await getPlan(request, { force: true })
    } catch (e) {
      planError = e
      emit({ type: 'replan_error', kind: e?.kind ?? 'error', status: e?.status ?? 0, message: String(e?.message ?? e) })
      return false
    }
    emit({ type: 'plan', requestId: plan.requestId, objects: plan.objects.length, replanAfterMs: plan.replanAfterMs })
    return true
  }

  function finish(reason = '') {
    const wanted = select(plan)
    const byKey = new Map()
    for (const o of wanted) if (results.has(o.key)) byKey.set(o.key, results.get(o.key))
    const missing = wanted.filter(o => !results.has(o.key)).map(o => o.key)
    const errs = missing.map(k => ({ key: k, error: errors.get(k) ?? null }))
    return { status: missing.length ? 'failed' : 'ok', reason: missing.length ? reason || 'objects unread' : '',
      plan, results: byKey, missing, errors: errs, replans, skippedForExpiry, planError, events }
  }
}
