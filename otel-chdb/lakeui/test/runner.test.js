import { test } from 'node:test'
import assert from 'node:assert/strict'
import fc from 'fast-check'
import { runPlanned } from '../src/runner.js'
import { PlanError } from '../src/planclient.js'
import { ObjectReadError } from '../src/rangereader.js'

/**
 * A simulated service + store. Plans are numbered; an object's URL is valid
 * for the plan that signed it until that plan's expiry. `script` decides each
 * plan's lifetime and each read's outcome.
 */
function sim({ keys, lifetimes, outcomes, planFailures = [], readMs = 1, margin = 60 }) {
  const w = { t: 0, plans: [], reads: [], planCalls: 0 }
  let li = 0
  let oi = 0
  w.now = () => w.t
  w.plan = async () => {
    w.planCalls++
    if (planFailures[w.planCalls - 1]) throw new PlanError('refused', 'plan 403 cluster_not_in_scope', { status: 403 })
    const life = lifetimes[li++ % lifetimes.length]
    const p = { requestId: 'p' + w.plans.length, replanAfterMs: w.t + life, expiresAtMs: w.t + life + margin,
      objects: keys.map(k => ({ key: k, url: `${k}@p${w.plans.length}`, size: 10 })) }
    w.plans.push(p)
    w.t += 1
    return p
  }
  w.read = async (o, plan) => {
    const start = w.t
    const rec = { key: o.key, plan: plan.requestId, start, replanAfterMs: plan.replanAfterMs, expiresAtMs: plan.expiresAtMs }
    w.reads.push(rec)
    await null
    w.t += readMs
    // an expired URL is a 403 whatever the script says
    if (start >= plan.expiresAtMs) throw new ObjectReadError('forbidden', o.key, 'expired', 403)
    const out = outcomes[oi++ % outcomes.length]
    if (out === '403') throw new ObjectReadError('forbidden', o.key, 'revoked', 403)
    if (out === 'net') throw new ObjectReadError('network', o.key, 'reset')
    rec.ok = true
    return `${o.key}:data`
  }
  return w
}

test('a 403 re-plans, and the re-read uses the new plan', async () => {
  const w = sim({ keys: ['a', 'b'], lifetimes: [1000], outcomes: ['ok', '403', 'ok'] })
  const r = await runPlanned({ plan: w.plan, request: {}, read: w.read, now: w.now, concurrency: 1 })
  assert.equal(r.status, 'ok')
  assert.equal(r.replans, 1)
  assert.deepEqual([...r.results.keys()].sort(), ['a', 'b'])
  assert.deepEqual(w.reads.map(x => `${x.key}@${x.plan}`), ['a@p0', 'b@p0', 'b@p1'])
})

test('no read starts at or after replan_after; the plan is renewed first', async () => {
  const w = sim({ keys: ['a', 'b', 'c'], lifetimes: [3, 1000], outcomes: ['ok'], readMs: 2 })
  const r = await runPlanned({ plan: w.plan, request: {}, read: w.read, now: w.now, concurrency: 1 })
  assert.equal(r.status, 'ok')
  assert.ok(r.skippedForExpiry >= 1)
  for (const x of w.reads) assert.ok(x.start < x.replanAfterMs)
})

test('bounded: persistent 403s end as failed with the objects it lacks, never empty-ok', async () => {
  const w = sim({ keys: ['a', 'b'], lifetimes: [1000], outcomes: ['403'] })
  const r = await runPlanned({ plan: w.plan, request: {}, read: w.read, now: w.now, maxReplans: 2 })
  assert.equal(r.status, 'failed')
  assert.deepEqual(r.missing.sort(), ['a', 'b'])
  assert.equal(r.replans, 2)
  assert.equal(w.planCalls, 3)
  assert.equal(r.errors[0].error.kind, 'forbidden')
})

test('a refused re-plan ends the query as failed, with the refusal', async () => {
  const w = sim({ keys: ['a'], lifetimes: [1000], outcomes: ['403', 'ok'], planFailures: [false, true] })
  const r = await runPlanned({ plan: w.plan, request: {}, read: w.read, now: w.now })
  assert.equal(r.status, 'failed')
  assert.equal(r.planError.kind, 'refused')
  assert.deepEqual(r.missing, ['a'])
})

test('a refused first plan is thrown: the refusal is the answer', async () => {
  const w = sim({ keys: ['a'], lifetimes: [1000], outcomes: ['ok'], planFailures: [true] })
  await assert.rejects(runPlanned({ plan: w.plan, request: {}, read: w.read, now: w.now }), e => e.kind === 'refused')
})

test('property: the X8 rules hold for any sequence of lifetimes, 403s, network errors and refusals', async () => {
  await fc.assert(fc.asyncProperty(
    fc.uniqueArray(fc.constantFrom('a', 'b', 'c', 'd', 'e', 'f'), { minLength: 0, maxLength: 6 }),
    fc.array(fc.integer({ min: 0, max: 20 }), { minLength: 1, maxLength: 5 }),
    fc.array(fc.constantFrom('ok', 'ok', 'ok', '403', 'net'), { minLength: 1, maxLength: 12 }),
    fc.array(fc.boolean(), { maxLength: 6 }).map(a => [false, ...a]),
    fc.integer({ min: 0, max: 4 }),
    fc.integer({ min: 1, max: 4 }),
    fc.integer({ min: 1, max: 5 }),
    async (keys, lifetimes, outcomes, planFailures, maxReplans, concurrency, readMs) => {
      const w = sim({ keys, lifetimes, outcomes, planFailures, readMs })
      const r = await runPlanned({ plan: w.plan, request: {}, read: w.read, now: w.now, maxReplans, concurrency })
      // rule 1: no read starts at or after its plan's replan_after
      for (const x of w.reads) assert.ok(x.start < x.replanAfterMs, `read of ${x.key} started at ${x.start}, replan_after ${x.replanAfterMs}`)
      // bounded
      assert.ok(w.planCalls <= maxReplans + 1)
      assert.equal(r.replans, w.planCalls - 1)
      const last = w.plans[w.plans.length - 1]
      if (r.status === 'ok') {
        // rule 2: ok means every object of the plan it reports was read
        assert.deepEqual(r.missing, [])
        assert.deepEqual([...r.results.keys()].sort(), last.objects.map(o => o.key).sort())
        for (const [k, v] of r.results) {
          assert.equal(v, `${k}:data`)
          assert.ok(w.reads.some(x => x.key === k && x.ok))
        }
      } else {
        assert.equal(r.status, 'failed')
        assert.ok(r.missing.length > 0, 'a failed result names what it lacks')
        assert.ok(r.replans === maxReplans || r.planError, 'it gave up only when out of re-plans or refused')
        for (const k of r.missing) assert.ok(!r.results.has(k))
      }
      // results never include a key the reported plan does not list
      for (const k of r.results.keys()) assert.ok(r.plan.objects.some(o => o.key === k))
      // nothing failed and nothing went stale → one plan, ok
      if (!w.reads.some(x => !x.ok) && r.skippedForExpiry === 0 && lifetimes[0] > keys.length * readMs * 2 + 2) {
        assert.equal(r.status, 'ok')
        assert.equal(r.replans, 0)
      }
    }), { numRuns: 1500 })
})
