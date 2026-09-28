import { test } from 'node:test'
import assert from 'node:assert/strict'
import { parseJSONNs } from '../src/ns.js'
import { cachedPlanner, normalizePlan, PlanError, requestPlan } from '../src/planclient.js'

const answer = (over = {}) => ({
  request_id: 'r1', source: 'lake', signal: 'logs', clusters: ['a'],
  from: '2026-09-28T12:00:00Z', to: '2026-09-28T13:00:00Z',
  complete_through: '2026-09-28T12:59:31.2Z', complete_through_ns: '1790600371200000000',
  completeness: 'partial', partial: true, incomplete_from_ns: '1790600371200000000',
  watermark: { status: 'ok' }, snapshot: null, start_complete: true,
  expires_at: '2026-09-28T13:05:00Z', replan_after: '2026-09-28T13:04:00Z', url_ttl_s: 300,
  objects: [{ url: 'https://s3/x?sig', size: 100, key: 'a/p/logs/e/1.parquet', cluster: 'a', min_time_ns: '1790596800000000001', max_time_ns: '1790596800000000009', rows: 3, refined: true }],
  ...over,
})

// the service's JSON has *_ns as bare integers; send it that way
const wire = obj => JSON.stringify(obj).replace(/"(\w*_ns)":"(-?\d+)"/g, '"$1":$2')

test('a plan is normalised with exact ns and the X8 times', () => {
  const p = normalizePlan(parseJSONNs(wire(answer())), 0)
  assert.equal(p.objects[0].maxTimeNs, 1790596800000000009n)
  // a double that lost the ns is refused, not rounded
  assert.throws(() => normalizePlan(JSON.parse(wire(answer()))), e => e.kind === 'bad_plan')
  assert.equal(p.completeness, 'partial')
  assert.equal(p.replanAfterMs, Date.parse('2026-09-28T13:04:00Z'))
  assert.equal(p.expiresAtMs, Date.parse('2026-09-28T13:05:00Z'))
  assert.equal(p.totalBytes, 100)
})

test('a plan it cannot trust is refused, not read partly', () => {
  for (const bad of [
    answer({ objects: [{ url: '', size: 1, key: 'k' }] }),
    answer({ objects: [{ url: 'u', size: 0, key: 'k' }] }),
    answer({ objects: [{ url: 'u', size: 1, key: 'k' }, { url: 'v', size: 1, key: 'k' }] }),
    answer({ expires_at: 'soon' }),
    answer({ replan_after: '2026-09-28T13:06:00Z' }),
    answer({ objects: undefined }),
    answer({ complete_through_ns: 1.5 }),
  ]) {
    assert.throws(() => normalizePlan(bad), e => e instanceof PlanError && e.kind === 'bad_plan')
  }
  // an unknown completeness word is unknown, never complete
  assert.equal(normalizePlan(answer({ completeness: 'mostly' })).completeness, 'unknown')
  // max_lateness (CAST row 26) and late objects are carried; absent, max_lateness is null (nothing settles)
  const late = normalizePlan(answer({ max_lateness_s: 60, settled_through_ns: '1790600311200000000', late_objects: 1,
    objects: [{ ...answer().objects[0], late: true }] }))
  assert.equal(late.maxLatenessNs, 60_000_000_000n)
  assert.equal(late.settledThroughNs, 1790600311200000000n)
  assert.equal(late.lateObjects, 1)
  assert.equal(late.objects[0].late, true)
  assert.equal(normalizePlan(answer()).maxLatenessNs, null)
  assert.equal(normalizePlan(answer({ max_lateness_s: -1 })).maxLatenessNs, null)
})

const respond = (status, body) => async () => new Response(typeof body === 'string' ? body : wire(body), { status })

test('requestPlan: ns precision over the wire, and refusals are errors of their kind', async () => {
  const base = { queryUrl: 'http://qs/', token: 't', signal: 'logs', fromNs: 1n, toNs: 2n }
  let seen
  const p = await requestPlan(base, { fetch: async (u, init) => { seen = { u, init }; return respond(200, answer())() } })
  assert.equal(seen.u, 'http://qs/v1/plan')
  assert.equal(seen.init.headers.authorization, 'Bearer t')
  assert.deepEqual(JSON.parse(seen.init.body), { signal: 'logs', from: '1970-01-01T00:00:00.000000001Z', to: '1970-01-01T00:00:00.000000002Z' })
  assert.equal(p.objects[0].minTimeNs, 1790596800000000001n) // not rounded through a double
  assert.equal(p.completeThroughNs, 1790600371200000000n)

  const cases = [
    [403, { error: 'cluster_not_in_scope', detail: 'cluster "b" is not in the token\'s scope' }, 'refused'],
    [403, { error: 'namespace_scope_needs_filtering_reader' }, 'refused'],
    [401, { error: 'bad_token' }, 'auth'],
    [413, { error: 'plan_too_large' }, 'too_large'],
    [400, { error: 'window_too_long' }, 'bad_request'],
    [502, 'bad gateway', 'server'],
  ]
  for (const [status, body, kind] of cases) {
    await assert.rejects(requestPlan(base, { fetch: respond(status, body) }), e => {
      assert.equal(e.kind, kind)
      assert.equal(e.status, status)
      if (body.error) assert.equal(e.reason, body.error)
      return true
    })
  }
  await assert.rejects(requestPlan(base, { fetch: async () => { throw new TypeError('down') } }), e => e.kind === 'network')
  await assert.rejects(requestPlan(base, { fetch: respond(200, 'not json') }), e => e.kind === 'bad_plan')
})

test('cachedPlanner caches only plans at a basis, under it, until replan_after (D30)', async () => {
  let t = 0
  let n = 0
  const B1 = 'b1.one.mac'
  const B2 = 'b1.two.mac'
  // the service answers "latest" at B1; a token at itself
  const ask = async req => ({ n: ++n, replanAfterMs: t + 100, atBasis: true, basis: req.basis === 'latest' ? B1 : req.basis })
  const plan = cachedPlanner(ask, { now: () => t })
  const req = { signal: 'logs', fromNs: 1n, toNs: 2n }
  // no basis, or "latest": never answered from the cache
  assert.equal((await plan({ ...req, basis: 'latest' })).n, 1)
  assert.equal((await plan({ ...req, basis: 'latest' })).n, 2)
  // ... but kept under the basis it came back at
  assert.equal((await plan({ ...req, basis: B1 })).n, 2)
  t = 99
  assert.equal((await plan({ ...req, basis: B1 })).n, 2)
  t = 100
  assert.equal((await plan({ ...req, basis: B1 })).n, 3) // at replan_after: a new plan (fresh URLs)
  assert.equal((await plan({ ...req, basis: B1 }, { force: true })).n, 4)
  assert.equal((await plan({ ...req, basis: B2 })).n, 5) // another basis, another plan
  assert.equal((await plan({ ...req, toNs: 3n, basis: B1 })).n, 6) // another window, another plan
  const none = cachedPlanner(async () => ({ n: ++n, replanAfterMs: Infinity }), { now: () => t })
  const a = await none(req)
  assert.notEqual((await none(req)).n, a.n) // no basis: nothing cached
  // asked at a basis, answered at another: refused, not cached
  const liar = cachedPlanner(async () => ({ replanAfterMs: Infinity, atBasis: true, basis: B2 }), { now: () => t })
  await assert.rejects(liar({ ...req, basis: B1 }), e => e.kind === 'bad_plan')
  assert.equal(plan.calls(), 6)
})

test('a plan at a basis: its basis, and the objects it could not date', () => {
  const p = normalizePlan(parseJSONNs(wire(answer({ basis: 'b1.x.y', at_basis: true, basis_info: { clusters: [] }, after_basis: 2, basis_unverified: 1,
    objects: [{ url: 'u', size: 1, key: 'k', basis_check: true, received_before_ns: '1790600371200000000' }] }))), 0)
  assert.equal(p.basis, 'b1.x.y')
  assert.equal(p.atBasis, true)
  assert.equal(p.afterBasis, 2)
  assert.equal(p.objects[0].basisCheck, true)
  assert.equal(p.objects[0].receivedBeforeNs, 1790600371200000000n)
  // a basis the plan was not computed at is not "at" it; garbage is no basis
  assert.equal(normalizePlan(parseJSONNs(wire(answer({ basis: 'b1.x.y', at_basis: false }))), 0).atBasis, false)
  assert.equal(normalizePlan(parseJSONNs(wire(answer({ basis: 'nope', at_basis: true }))), 0).atBasis, false)
  // a footer check without its bound cannot be done: refused
  assert.throws(() => normalizePlan(parseJSONNs(wire(answer({ objects: [{ url: 'u', size: 1, key: 'k', basis_check: true }] }))), 0),
    e => e.kind === 'bad_plan')
})
