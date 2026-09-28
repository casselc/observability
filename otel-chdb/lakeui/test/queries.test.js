import { test } from 'node:test'
import assert from 'node:assert/strict'
import fc from 'fast-check'
import { parquetMetadata, parquetReadObjects } from 'hyparquet'
import { execute } from '../src/engine.js'
import { cachedPlanner } from '../src/planclient.js'
import { MetaCache, compressors, parsers, pruneByRange } from '../src/parquet.js'
import { logSearch, metricChart, traceById } from '../src/queries.js'
import { fakeStore, fixture, planOf, truth } from './helpers.js'

const files = { logs: fixture('logs-late.parquet'), traces: fixture('traces.parquet'), gauge: fixture('gauge.parquet') }
const ab = u8 => u8.buffer.slice(u8.byteOffset, u8.byteOffset + u8.byteLength)
async function allRows(u8, columns) {
  return parquetReadObjects({ file: ab(u8), columns, compressors, parsers })
}
const logRows = await allRows(files.logs, ['Timestamp', 'SeverityText', 'Body', 'ServiceName'])
const spanRows = await allRows(files.traces, ['Timestamp', 'TraceId', 'SpanId'])
const gaugeRows = await allRows(files.gauge, ['TimeUnix', 'MetricName', 'Value', 'ServiceName'])
const minTs = logRows.reduce((a, r) => (r.Timestamp < a ? r.Timestamp : a), logRows[0].Timestamp)
const maxTs = logRows.reduce((a, r) => (r.Timestamp > a ? r.Timestamp : a), logRows[0].Timestamp)

/** Two copies of each fixture under different keys, as a plan of objects. */
function world(name, over = {}) {
  const s = fakeStore()
  const objs = ['x1', 'x2'].map(k => {
    s.objects.set(`url-${k}`, files[name])
    return { key: `${name}/${k}`, url: `url-${k}`, size: files[name].length }
  })
  const plan = planOf(objs, over)
  return { s, plan, planner: cachedPlanner(async () => plan) }
}

test('log search = a brute-force scan of the same rows (count, histogram, newest rows)', async () => {
  const fromNs = minTs
  const toNs = maxTs + 1n
  const { s, plan, planner } = world('logs', { fromNs, toNs, completeness: 'partial', completeThroughNs: minTs + (maxTs - minTs) / 2n, incompleteFromNs: minTs + (maxTs - minTs) / 2n })
  const q = logSearch({ fromNs, toNs, text: '', severities: ['ERROR'], limit: 10 })
  const r = await execute(q, { planner, request: { fromNs, toNs }, fetch: s.fetch, metaCache: new MetaCache() })
  assert.equal(r.status, 'ok')
  const want = logRows.filter(x => x.SeverityText === 'ERROR')
  assert.equal(r.result.count, 2 * want.length)
  assert.equal(r.result.buckets.reduce((a, b) => a + b.count, 0), 2 * want.length)
  assert.equal(r.result.rows.length, 10)
  const newest = want.map(x => x.Timestamp).sort((a, b) => (a < b ? 1 : -1))
  assert.equal(r.result.rows[0].ts, newest[0])
  assert.ok(r.result.rows.every(x => x.body && x.service && x.severity === 'ERROR'), 'the shown rows got their Body and service in pass 2')
  // rows and buckets after incomplete_from are marked
  for (const x of r.result.rows) assert.equal(x.state, x.ts >= plan.incompleteFromNs ? 'incomplete' : 'complete')
  assert.equal(r.state, 'incomplete')
  // narrow: footer + Timestamp + SeverityText (+ Body/ServiceName of 2 objects for the rows shown)
  const size = plan.totalBytes
  assert.ok(r.stats.bytes < size, `${r.stats.bytes} of ${size}`)
})

test('a count-only query reads a small fraction of each object', async () => {
  const { s, plan, planner } = world('logs')
  const q = logSearch({ fromNs: 0n, toNs: 10n ** 19n, severities: ['WARN'], limit: 0 })
  const r = await execute(q, { planner, request: { fromNs: 0n, toNs: 10n ** 19n }, fetch: s.fetch, metaCache: new MetaCache() })
  assert.equal(r.result.count, 2 * logRows.filter(x => x.SeverityText === 'WARN').length)
  const frac = r.stats.bytes / plan.totalBytes
  assert.ok(frac < 0.35, `fetched ${r.stats.bytes} of ${plan.totalBytes} (${(frac * 100).toFixed(1)}%)`)
})

test('property: log search over random windows and filters matches brute force', async () => {
  await fc.assert(fc.asyncProperty(
    fc.bigInt({ min: minTs - 10n ** 9n, max: maxTs + 10n ** 9n }), fc.bigInt({ min: 1n, max: 30n * 10n ** 9n }),
    fc.constantFrom('', 'needle', truth.needle, 'status=500', 'CACHE MISS', 'zzz-nothing'),
    fc.subarray(['INFO', 'DEBUG', 'WARN', 'ERROR']),
    async (fromNs, len, text, sev) => {
      const toNs = fromNs + len
      const { s, planner } = world('logs', { fromNs, toNs })
      const q = logSearch({ fromNs, toNs, text, severities: sev, limit: 5 })
      const r = await execute(q, { planner, request: { fromNs, toNs }, fetch: s.fetch, metaCache: new MetaCache() })
      const want = logRows.filter(x => x.Timestamp >= fromNs && x.Timestamp < toNs && (!sev.length || sev.includes(x.SeverityText)) &&
        (!text || x.Body.toLowerCase().includes(text.toLowerCase())))
      assert.equal(r.status, 'ok')
      assert.equal(r.result.count, 2 * want.length)
      assert.equal(r.result.rows.length, Math.min(5, 2 * want.length))
    }), { numRuns: 60 })
})

test('trace by id: every span, from footer + TraceId, then span columns only where it matched', async () => {
  const ids = [...new Set(spanRows.map(x => x.TraceId))]
  const id = truth.trace_ids.find(t => ids.includes(t))
  assert.ok(id, 'the fixture holds a known trace')
  const { s, planner } = world('traces')
  const q = traceById({ traceId: id.toUpperCase(), fromNs: 0n, toNs: 10n ** 19n })
  const r = await execute(q, { planner, request: { fromNs: 0n, toNs: 10n ** 19n }, fetch: s.fetch, metaCache: new MetaCache() })
  const want = spanRows.filter(x => x.TraceId === id).length
  assert.equal(r.result.spans.length, 2 * want)
  assert.equal(r.result.objects, 2)
  assert.throws(() => traceById({ traceId: 'nope', fromNs: 0n, toNs: 1n }))
  // an id no object has: zero spans, and that is a complete answer only because every object was read
  const none = await execute(traceById({ traceId: '0'.repeat(32), fromNs: 0n, toNs: 10n ** 19n }), { planner, request: { fromNs: 0n, toNs: 10n ** 19n }, fetch: s.fetch, metaCache: new MetaCache() })
  assert.equal(none.status, 'ok')
  assert.equal(none.result.spans.length, 0)
})

test('metric chart: bucketed points = brute force; sum and count match', async () => {
  const { s, planner } = world('gauge')
  const t0 = gaugeRows.reduce((a, r) => (r.TimeUnix < a ? r.TimeUnix : a), gaugeRows[0].TimeUnix)
  const q = metricChart({ metric: truth.metric, fromNs: t0, toNs: t0 + 3600n * 10n ** 9n })
  const r = await execute(q, { planner, request: { fromNs: q.q.fromNs, toNs: q.q.toNs }, fetch: s.fetch, metaCache: new MetaCache() })
  const want = gaugeRows.filter(x => x.MetricName === truth.metric)
  assert.equal(r.result.points, 2 * want.length)
  assert.ok(Math.abs(r.result.sum - 2 * want.reduce((a, x) => a + x.Value, 0)) < 1e-6)
  assert.ok(r.result.series.length >= 1)
})

test('a failed object is never an empty result: 403 everywhere ends failed with the keys', async () => {
  const { s, plan, planner } = world('logs')
  for (const u of s.objects.keys()) s.expired.add(u)
  const r = await execute(logSearch({ fromNs: 0n, toNs: 10n ** 19n }), { planner, request: { fromNs: 0n, toNs: 10n ** 19n }, fetch: s.fetch, metaCache: new MetaCache(), maxReplans: 2 })
  assert.equal(r.status, 'failed')
  assert.equal(r.result, null)
  assert.deepEqual(r.missing.sort(), plan.objects.map(o => o.key).sort())
  assert.equal(r.replans, 2)
})

test('an expired URL re-plans and the re-read succeeds', async () => {
  const s = fakeStore(new Map([['old', files.logs], ['new', files.logs]]))
  s.expired.add('old')
  let n = 0
  const planner = cachedPlanner(async () => planOf([{ key: 'k', url: n++ ? 'new' : 'old', size: files.logs.length }], { requestId: 'p' + n }))
  const r = await execute(logSearch({ fromNs: 0n, toNs: 10n ** 19n, limit: 0 }), { planner, request: { fromNs: 0n, toNs: 10n ** 19n }, fetch: s.fetch, metaCache: new MetaCache() })
  assert.equal(r.status, 'ok')
  assert.equal(r.replans, 1)
  assert.equal(r.result.count, logRows.length)
  assert.ok(r.events.some(e => e.type === 'read_error' && e.kind === 'forbidden'))
})

test('pruning never drops a row group whose truncated string max may still match', () => {
  const md = parquetMetadata(ab(files.logs), { parsers })
  // Body's max statistic is cut to a prefix by the writer; a value extending it must not be pruned
  const st = md.row_groups[0].columns.find(c => c.meta_data.path_in_schema[0] === 'Body').meta_data.statistics
  const lo = st.max_value + 'zzzz'
  assert.equal(pruneByRange(md, 'Body', lo, null).length, 1)
})
