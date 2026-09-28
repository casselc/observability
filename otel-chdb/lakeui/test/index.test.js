// The lake index in the page (D27): the filter goes to the planner; a plan's
// `index: hit` + `row_groups` narrows the read, `scan` (or no index field)
// reads as before; the answer never depends on what the index said, only
// the bytes do.

import { test } from 'node:test'
import assert from 'node:assert/strict'
import { parquetReadObjects } from 'hyparquet'
import { execute } from '../src/engine.js'
import { cachedPlanner, normalizePlan, planKey, requestPlan } from '../src/planclient.js'
import { MetaCache, compressors, parsers } from '../src/parquet.js'
import { indexedGroups, logSearch, traceById } from '../src/queries.js'
import { fakeStore, fixture, planOf } from './helpers.js'

const files = { logs: fixture('logs-late.parquet'), traces: fixture('traces.parquet') }
const ab = u8 => u8.buffer.slice(u8.byteOffset, u8.byteOffset + u8.byteLength)
const spans = await parquetReadObjects({ file: ab(files.traces), columns: ['TraceId'], compressors, parsers })
const logs = await parquetReadObjects({ file: ab(files.logs), columns: ['Body'], compressors, parsers })

const rawPlan = objects => ({
  objects, completeness: 'complete', complete_through_ns: '100', incomplete_from_ns: '90', from_ns: '0', to_ns: '100',
  expires_at: '2026-09-28T12:05:00Z', replan_after: '2026-09-28T12:04:00Z',
})

test('normalizePlan: index and row_groups are carried, and checked', () => {
  const p = normalizePlan(rawPlan([
    { key: 'a', url: 'u', size: 10, index: 'hit', row_groups: [2, 0, 2] },
    { key: 'b', url: 'u', size: 10, index: 'scan' },
    { key: 'c', url: 'u', size: 10 },
  ]))
  assert.deepEqual(p.objects.map(o => [o.index, o.rowGroups]), [['hit', [0, 2]], ['scan', null], [null, null]])
  for (const bad of [{ index: 'hit' }, { index: 'hit', row_groups: [] }, { index: 'hit', row_groups: [-1] }, { index: 'hit', row_groups: [0.5] }, { index: 'none' }]) {
    assert.throws(() => normalizePlan(rawPlan([{ key: 'a', url: 'u', size: 10, ...bad }])), e => e.kind === 'bad_plan', JSON.stringify(bad))
  }
})

test('requestPlan sends the filter; the cache keys on it', async () => {
  let body
  const fetch = async (_u, init) => {
    body = JSON.parse(init.body)
    return new Response(JSON.stringify(rawPlan([])), { status: 200 })
  }
  await requestPlan({ queryUrl: 'http://q', token: 't', signal: 'traces', fromNs: 0n, toNs: 1n, traceId: 'ab'.repeat(16) }, { fetch })
  assert.equal(body.trace_id, 'ab'.repeat(16))
  assert.equal(body.terms, undefined)
  await requestPlan({ queryUrl: 'http://q', token: 't', signal: 'logs', fromNs: 0n, toNs: 1n, terms: ['timeout'] }, { fetch })
  assert.deepEqual(body.terms, ['timeout'])
  assert.notEqual(planKey({ signal: 'logs', fromNs: 0n, toNs: 1n }), planKey({ signal: 'logs', fromNs: 0n, toNs: 1n, terms: ['x'] }))
})

test('the engine passes each query\'s filter to the planner (and not with the index off)', async () => {
  const seen = []
  const planner = cachedPlanner(async req => { seen.push(req); return planOf([]) })
  await execute(traceById({ traceId: 'AB'.repeat(16), fromNs: 0n, toNs: 1n }), { planner, request: { fromNs: 0n, toNs: 1n } })
  await execute(logSearch({ fromNs: 0n, toNs: 1n, text: ' Timeout ' }), { planner, request: { fromNs: 0n, toNs: 1n } })
  await execute(logSearch({ fromNs: 0n, toNs: 1n, text: '' }), { planner, request: { fromNs: 0n, toNs: 2n } })
  await execute(logSearch({ fromNs: 0n, toNs: 1n, text: 'x' }), { planner, request: { fromNs: 0n, toNs: 3n, useIndex: false } })
  assert.equal(seen[0].traceId, 'ab'.repeat(16))
  assert.deepEqual(seen[1].terms, ['Timeout'])
  assert.equal(seen[2].terms, undefined)
  assert.equal(seen[3].terms, undefined)
  assert.equal(seen[3].useIndex, undefined)
})

test('indexedGroups: hit narrows, scan and disagreement read everything', () => {
  const md = { row_groups: [{ num_rows: 2n }, { num_rows: 3n }, { num_rows: 1n }] }
  assert.deepEqual(indexedGroups(md, { index: 'hit', rowGroups: [1] }).map(g => g.index), [1])
  assert.deepEqual(indexedGroups(md, { index: 'scan', rowGroups: null }).map(g => g.index), [0, 1, 2])
  assert.deepEqual(indexedGroups(md, {}).map(g => g.index), [0, 1, 2])
  assert.deepEqual(indexedGroups(md, { index: 'hit', rowGroups: [1, 7] }).map(g => g.index), [0, 1, 2]) // the file has no group 7
})

/** Three copies of a fixture; the planner answers as a service with an index would. */
function world(name, marks) {
  const s = fakeStore()
  const objs = marks.map((m, i) => {
    s.objects.set(`url-${i}`, files[name])
    return { key: `${name}/${i}`, url: `url-${i}`, size: files[name].length, ...m }
  })
  return { s, planner: cachedPlanner(async () => planOf(objs)) }
}

test('trace by id: the same spans whatever the index says, fewer bytes when it prunes', async () => {
  const id = String(spans.find(r => r.TraceId).TraceId).toLowerCase()
  const want = spans.filter(r => String(r.TraceId).toLowerCase() === id).length
  const q = () => traceById({ traceId: id, fromNs: 0n, toNs: 10n ** 19n })
  const run = async marks => {
    const { s, planner } = world('traces', marks)
    return execute(q(), { planner, request: { fromNs: 0n, toNs: 10n ** 19n }, fetch: s.fetch, metaCache: new MetaCache() })
  }
  const all = await run([{}, {}, {}])
  const hit = await run([{ index: 'hit', rowGroups: [0] }, { index: 'scan' }, {}])
  const pruned = await run([{ index: 'hit', rowGroups: [0] }]) // the service left out the objects it ruled out
  assert.equal(all.result.spans.length, 3 * want)
  assert.equal(hit.result.spans.length, 3 * want)
  assert.equal(pruned.result.spans.length, want)
  assert.ok(pruned.stats.bytes < all.stats.bytes / 2, `${pruned.stats.bytes} vs ${all.stats.bytes}`)
})

test('log text search: hit and scan objects both answer every matching row', async () => {
  const text = String(logs[0].Body).split(/\s+/)[0]
  const want = logs.filter(r => String(r.Body).toLowerCase().includes(text.toLowerCase())).length
  assert.ok(want > 0)
  const { s, planner } = world('logs', [{ index: 'scan' }, { index: 'hit', rowGroups: [0] }])
  const r = await execute(logSearch({ fromNs: 0n, toNs: 10n ** 19n, text, limit: 5 }), {
    planner, request: { fromNs: 0n, toNs: 10n ** 19n }, fetch: s.fetch, metaCache: new MetaCache() })
  assert.equal(r.status, 'ok')
  assert.equal(r.result.count, 2 * want)
  assert.ok(r.result.rows.every(x => x.body.toLowerCase().includes(text.toLowerCase())))
})
