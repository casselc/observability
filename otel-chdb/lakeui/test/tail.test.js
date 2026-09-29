// The tail (D30 amendment, AMBIGUITY.md #10 (b)): a plan at a basis also
// lists what was received after it (tail_objects). The client reads it,
// draws its rows incomplete whatever their time, keeps the basis part's
// answers keyed on the basis and never the tail's.
import { test } from 'node:test'
import assert from 'node:assert/strict'
import fc from 'fast-check'
import { parquetMetadata, parquetReadObjects } from 'hyparquet'
import { bannerText, bucketState, resultState, rowState } from '../src/completeness.js'
import { execute, footerReceivedNs, ResultCache } from '../src/engine.js'
import { parseJSONNs } from '../src/ns.js'
import { MetaCache, compressors, parsers } from '../src/parquet.js'
import { cachedPlanner, normalizePlan, PlanError, requestPlan } from '../src/planclient.js'
import { logSearch, metricChart, traceById } from '../src/queries.js'
import { fakeStore, fixture, planOf } from './helpers.js'

const files = { logs: fixture('logs-late.parquet'), traces: fixture('traces.parquet'), gauge: fixture('gauge.parquet') }
const ab = u8 => u8.buffer.slice(u8.byteOffset, u8.byteOffset + u8.byteLength)
const logRows = await parquetReadObjects({ file: ab(files.logs), columns: ['Timestamp'], compressors, parsers })
const spanRows = await parquetReadObjects({ file: ab(files.traces), columns: ['Timestamp', 'TraceId'], compressors, parsers })
const gaugeRows = await parquetReadObjects({ file: ab(files.gauge), columns: ['MetricName'], compressors, parsers })
const recv = footerReceivedNs(parquetMetadata(ab(files.logs), { parsers }))
const ALL = { fromNs: 0n, toNs: 10n ** 19n }

// ---- the wire -----------------------------------------------------------------

const answer = (over = {}) => ({
  request_id: 'r1', source: 'lake', signal: 'logs', clusters: ['a'],
  from: '2026-09-28T12:00:00Z', to: '2026-09-28T13:00:00Z',
  complete_through: '2026-09-28T12:59:31.2Z', complete_through_ns: '1790600371200000000',
  completeness: 'partial', partial: true, incomplete_from_ns: '1790600371200000000',
  watermark: { status: 'ok' }, snapshot: null, start_complete: true,
  expires_at: '2026-09-28T13:05:00Z', replan_after: '2026-09-28T13:04:00Z', url_ttl_s: 300,
  basis: 'b1.x.y', at_basis: true, objects_hash: 'h-basis',
  objects: [{ url: 'u1', size: 100, key: 'k1', cluster: 'a' }],
  tail_objects: [
    { url: 'u2', size: 50, key: 'k2', cluster: 'a', tail: true, late: true },
    { url: 'u3', size: 10, key: 'k3', cluster: 'a', tail: true, basis_check: true, received_before_ns: '1790600371200000000' },
  ],
  tail: { completeness: 'incomplete', cache: 'never', objects: 2, bytes: 60, unplaced: 1, late_objects: 1, unrefined: 2,
    received_from: [{ cluster: 'a', received_from_ns: '1790600371200000000', received_from: '2026-09-28T12:59:31.2Z' }],
    received_through: '2026-09-28T13:00:01Z', min_time_ns: null, max_time_ns: null, rows: 0, objects_hash: 'h-tail', note: 'n' },
  ...over,
})
const wire = obj => JSON.stringify(obj).replace(/"(\w*_ns)":"(-?\d+)"/g, '"$1":$2')
const norm = over => normalizePlan(parseJSONNs(wire(answer(over))), 0)

test('normalizePlan: the tail is carried apart from the basis part, and a tail it cannot draw honestly is refused', () => {
  const p = norm()
  assert.deepEqual(p.objects.map(o => [o.key, o.tail]), [['k1', false]])
  assert.deepEqual(p.tailObjects.map(o => [o.key, o.tail, o.basisCheck]), [['k2', true, false], ['k3', true, true]])
  assert.equal(p.tailObjects[1].receivedBeforeNs, 1790600371200000000n)
  assert.equal(p.totalBytes, 100) // the basis part's
  assert.equal(p.objectsHash, 'h-basis')
  assert.equal(p.tail.objects, 2)
  assert.equal(p.tail.unplaced, 1)
  assert.equal(p.tail.receivedFrom[0].receivedFromNs, 1790600371200000000n)
  // a plan without tail has none
  const plain = norm({ tail_objects: undefined, tail: undefined })
  assert.deepEqual(plain.tailObjects, [])
  assert.equal(plain.tail, null)
  const bad = over => assert.throws(() => norm(over), e => e instanceof PlanError && e.kind === 'bad_plan', JSON.stringify(over))
  bad({ tail: { ...answer().tail, completeness: 'complete' } }) // a tail is never complete
  bad({ tail: { ...answer().tail, cache: 'ok' } }) // nor cacheable
  bad({ tail: undefined }) // tail objects without the label
  bad({ objects: [{ url: 'u1', size: 100, key: 'k1', tail: true }] }) // the basis part holds no tail object
  bad({ tail_objects: [{ url: 'u2', size: 50, key: 'k1', tail: true }] }) // one object in both parts
  bad({ tail_objects: {} })
  bad({ tail_objects: [{ url: 'u3', size: 10, key: 'k3', tail: true, basis_check: true }] }) // a check without its bound
})

test('requestPlan asks for the tail only with a basis', async () => {
  const bodies = []
  const fetch = async (u, init) => {
    bodies.push(JSON.parse(init.body))
    return new Response(wire(answer()), { status: 200 })
  }
  const base = { queryUrl: 'http://qs', token: 't', signal: 'logs', fromNs: 1n, toNs: 2n }
  await requestPlan({ ...base, basis: 'latest', tail: true }, { fetch })
  await requestPlan({ ...base, tail: true }, { fetch })
  assert.equal(bodies[0].tail, true)
  assert.equal(bodies[0].basis, 'latest')
  assert.equal('tail' in bodies[1], false)
})

test('cachedPlanner: a request with tail is always asked; only its basis part is kept, under the basis', async () => {
  let n = 0
  const B = 'b1.pinned.mac'
  const plan = cachedPlanner(async req => ({ n: ++n, replanAfterMs: Infinity, atBasis: true, basis: B, objects: [{ key: 'k1' }],
    tailObjects: req.tail ? [{ key: 't' + n, tail: true }] : [], tail: req.tail ? { objects: 1 } : null }), { now: () => 0 })
  const req = { signal: 'logs', fromNs: 1n, toNs: 2n }
  const a = await plan({ ...req, basis: B, tail: true })
  const b = await plan({ ...req, basis: B, tail: true })
  assert.equal(a.n, 1)
  assert.equal(b.n, 2) // never answered from the cache: the tail is never cached
  assert.equal(b.tailObjects[0].key, 't2')
  const kept = plan.peek({ ...req, basis: B })
  assert.deepEqual(kept.tailObjects, [])
  assert.equal(kept.tail, null)
  assert.deepEqual(kept.objects, [{ key: 'k1' }])
  const c = await plan({ ...req, basis: B }) // without tail: the kept basis part
  assert.equal(c.n, 2)
  assert.deepEqual(c.tailObjects, [])
})

// ---- completeness ------------------------------------------------------------------

test('a tail row, and a bucket holding one, is incomplete whatever its time; unknown stays unknown', () => {
  const label = { state: 'complete', fromNs: 0n, toNs: 100n, completeThroughNs: 1000n, incompleteFromNs: null, maxLatenessNs: 10n, startComplete: true }
  assert.equal(rowState(label, 5n), 'complete')
  assert.equal(rowState(label, 5n, true), 'incomplete')
  assert.equal(bucketState(label, 0n, 10n), 'complete')
  assert.equal(bucketState(label, 0n, 10n, true), 'incomplete')
  assert.equal(resultState(label), 'complete')
  assert.equal(resultState(label, { tailRows: 0 }), 'complete')
  assert.equal(resultState(label, { tailRows: 3 }), 'incomplete')
  assert.match(bannerText(label, { tailRows: 3 }).text, /3 row\(s\) received after the basis/)
  const unk = { ...label, state: 'unknown' }
  assert.equal(rowState(unk, 5n, true), 'unknown')
  assert.equal(bucketState(unk, 0n, 10n, true), 'unknown')
  assert.equal(resultState(unk, { tailRows: 3 }), 'unknown')
})

// ---- the engine --------------------------------------------------------------------

const B = 'b1.pinned.mac'

/** A service at basis B: `basis` keys in the basis part, `tail` keys in the tail; every key is a copy of `name`. */
function service(name, s, sets, { unplaced = new Map() } = {}) {
  const asked = []
  const planner = cachedPlanner(async req => {
    asked.push({ basis: req.basis, tail: req.tail === true })
    const obj = k => {
      s.objects.set('url-' + k, files[name])
      return { key: k, url: 'url-' + k, size: files[name].length }
    }
    const p = planOf(sets.basis.map(obj), { requestId: 'p' + asked.length })
    const tail = sets.tail.map(k => ({ ...obj(k), tail: true, minTimeNs: null, maxTimeNs: null, cluster: 'c', producer: 'p',
      ...(unplaced.has(k) ? { basisCheck: true, receivedBeforeNs: unplaced.get(k) } : {}) }))
    return { ...p, atBasis: true, basis: B, objectsHash: sets.basis.join(','), tailObjects: req.tail ? tail : [],
      tail: req.tail ? { objects: tail.length } : null }
  })
  return { planner, asked }
}

test('the engine reads the tail with the basis part: all rows counted, the tail\'s drawn incomplete, and the result says so', async () => {
  const s = fakeStore()
  const sets = { basis: ['a1', 'a2'], tail: ['t1'] }
  const { planner, asked } = service('logs', s, sets)
  const results = new ResultCache()
  const run = () => execute(logSearch({ ...ALL, limit: 5 }), { planner, request: { ...ALL }, fetch: s.fetch, metaCache: new MetaCache(), results })
  const r = await run()
  assert.equal(r.status, 'ok')
  assert.deepEqual(asked[0], { basis: 'latest', tail: true })
  assert.equal(r.result.count, 3 * logRows.length)
  assert.equal(r.result.tailRows, logRows.length)
  assert.equal(r.result.basisCount, 2 * logRows.length)
  assert.equal(r.tailRows, logRows.length)
  assert.equal(r.state, 'incomplete') // the label is complete; the tail is not
  assert.equal(r.label.state, 'complete')
  for (const row of r.result.rows) assert.equal(row.state, row.key === 't1' ? 'incomplete' : 'complete')
  for (const b of r.result.buckets) assert.equal(b.state, b.tailCount > 0 ? 'incomplete' : 'complete')
  assert.ok(r.result.buckets.some(b => b.tailCount > 0))

  // more arrives: at the same basis the basis part is answered from what was
  // kept (no read of a1, a2), the tail is read anew, and the basis count holds
  sets.tail = ['t1', 't2']
  const before = s.log.length
  const again = await execute(logSearch({ ...ALL, limit: 5 }), { planner, request: { ...ALL, basis: B }, fetch: s.fetch, metaCache: new MetaCache(), results })
  const read = new Set(s.log.slice(before).map(x => x.url))
  assert.equal(read.has('url-a1') || read.has('url-a2'), false, [...read].join())
  assert.equal(read.has('url-t1') && read.has('url-t2'), true)
  assert.equal(again.stats.cachedParts, 2)
  assert.equal(again.result.basisCount, r.result.basisCount)
  assert.equal(again.result.tailRows, 2 * logRows.length)
  assert.equal(again.result.count, 4 * logRows.length)
  assert.equal(again.plan.objectsHash, r.plan.objectsHash)
  assert.ok(asked.slice(1).every(a => a.basis === B && a.tail))
  // what is kept is the basis part only
  const k = results.key(B, logSearch({ ...ALL, limit: 5 }), { ...ALL, signal: 'logs' })
  assert.deepEqual([...results.get(k).parts.keys()].sort(), ['a1', 'a2'])
})

test('trace and metric merges draw the tail incomplete and count it', async () => {
  const id = String(spanRows.find(r => r.TraceId).TraceId)
  const spans = spanRows.filter(r => String(r.TraceId) === id).length
  const s = fakeStore()
  const { planner } = service('traces', s, { basis: ['a1'], tail: ['t1'] })
  const r = await execute(traceById({ ...ALL, traceId: id }), { planner, request: { ...ALL }, fetch: s.fetch, metaCache: new MetaCache(), results: new ResultCache() })
  assert.equal(r.result.spans.length, 2 * spans)
  assert.equal(r.result.tailRows, spans)
  for (const sp of r.result.spans) assert.equal(sp.state, sp.key === 't1' ? 'incomplete' : 'complete')

  const metric = String(gaugeRows[0].MetricName)
  const g = fakeStore()
  const m = service('gauge', g, { basis: ['a1'], tail: ['t1'] })
  const mr = await execute(metricChart({ ...ALL, metric, stepNs: 10n ** 18n }), { planner: m.planner, request: { ...ALL }, fetch: g.fetch, metaCache: new MetaCache(), results: new ResultCache() })
  assert.equal(mr.result.tailRows * 2, mr.result.points)
  assert.ok(mr.result.points > 0)
  for (const se of mr.result.series) for (const p of se.points) assert.equal(p.state, 'incomplete') // every bucket holds a copy's tail point
  assert.equal(mr.state, 'incomplete')
})

test('an unplaced tail object: its footer puts it in the basis part (kept) or the tail (drawn incomplete, never kept)', async () => {
  assert.equal(typeof recv, 'bigint')
  for (const [bound, inBasis] of [[recv + 1n, true], [recv, false]]) {
    const s = fakeStore()
    const { planner } = service('logs', s, { basis: ['a1'], tail: ['u1'] }, { unplaced: new Map([['u1', bound]]) })
    const results = new ResultCache()
    const r = await execute(logSearch({ ...ALL, limit: 0 }), { planner, request: { ...ALL }, fetch: s.fetch, metaCache: new MetaCache(), results })
    assert.equal(r.status, 'ok')
    assert.equal(r.result.count, 2 * logRows.length)
    assert.equal(r.result.tailRows, inBasis ? 0 : logRows.length, `bound ${bound}`)
    assert.equal(r.state, inBasis ? 'complete' : 'incomplete')
    const kept = [...results.get(results.key(B, logSearch({ ...ALL, limit: 0 }), { ...ALL, signal: 'logs' })).parts.keys()].sort()
    assert.deepEqual(kept, inBasis ? ['a1', 'u1'] : ['a1'])
  }
})

test('"latest" that cannot be issued (no watermark): planned unpinned and said so; a held basis refused is the answer', async () => {
  const s = fakeStore(new Map([['u', files.logs]]))
  const asked = []
  const refusal = new PlanError('server', 'plan 503 basis_unverifiable', { status: 503, reason: 'basis_unverifiable' })
  const planner = cachedPlanner(async req => {
    asked.push({ basis: req.basis, tail: req.tail === true })
    if (req.basis) throw refusal
    return { ...planOf([{ key: 'k', url: 'u', size: files.logs.length }], { completeness: 'unknown' }), atBasis: false, basis: null, tailObjects: [], tail: null }
  })
  const events = []
  const results = new ResultCache()
  const r = await execute(logSearch({ ...ALL, limit: 0 }), { planner, request: { ...ALL }, fetch: s.fetch, metaCache: new MetaCache(), results, onEvent: e => events.push(e) })
  assert.equal(r.status, 'ok')
  assert.deepEqual(asked, [{ basis: 'latest', tail: true }, { basis: undefined, tail: false }])
  assert.equal(r.basis, null)
  assert.equal(r.unpinned, 'basis_unverifiable')
  assert.equal(r.state, 'unknown')
  assert.equal(results.size, 0)
  assert.ok(events.some(e => e.type === 'unpinned'))
  // a held token refused, or any other refusal of latest, is not unpinned
  await assert.rejects(execute(logSearch({ ...ALL, limit: 0 }), { planner, request: { ...ALL, basis: B }, fetch: s.fetch, metaCache: new MetaCache(), results }),
    e => e.reason === 'basis_unverifiable')
  const scope = cachedPlanner(async () => { throw new PlanError('refused', 'no', { status: 403, reason: 'cluster_not_in_scope' }) })
  await assert.rejects(execute(logSearch({ ...ALL, limit: 0 }), { planner: scope, request: { ...ALL }, fetch: s.fetch, metaCache: new MetaCache(), results }),
    e => e.reason === 'cluster_not_in_scope')
})

test('property: however objects split between the basis part and a growing tail, every row is counted once, the tail\'s incomplete, and the basis count never moves', async () => {
  await fc.assert(fc.asyncProperty(
    fc.array(fc.boolean(), { minLength: 1, maxLength: 5 }), fc.integer({ min: 0, max: 3 }), fc.integer({ min: 0, max: 3 }),
    async (inBasis, tail0, more) => {
      const s = fakeStore()
      const basis = inBasis.map((b, i) => (b ? 'a' + i : null)).filter(Boolean)
      const sets = { basis, tail: Array.from({ length: tail0 }, (_, i) => 't' + i) }
      const { planner } = service('logs', s, sets)
      const results = new ResultCache()
      const q = () => logSearch({ ...ALL, limit: 3 })
      const first = await execute(q(), { planner, request: { ...ALL }, fetch: s.fetch, metaCache: new MetaCache(), results })
      assert.equal(first.result.count, (basis.length + tail0) * logRows.length)
      assert.equal(first.result.basisCount, basis.length * logRows.length)
      assert.equal(first.state, tail0 ? 'incomplete' : 'complete')
      for (const r of first.result.rows) assert.equal(r.state, r.key.startsWith('t') ? 'incomplete' : 'complete')
      sets.tail = Array.from({ length: tail0 + more }, (_, i) => 't' + i)
      const later = await execute(q(), { planner, request: { ...ALL, basis: B }, fetch: s.fetch, metaCache: new MetaCache(), results })
      assert.equal(later.result.basisCount, first.result.basisCount)
      assert.equal(later.result.tailRows, (tail0 + more) * logRows.length)
      assert.equal(later.stats.cachedParts >= basis.length, true)
    }), { numRuns: 25 })
})
