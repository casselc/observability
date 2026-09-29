// The spike's pure parts, against lakeui's real edge-object fixtures:
// the columns the dashboard loads equal a brute-force read (and are labelled
// by lakeui's completeness rules), they survive Arrow IPC into DuckDB-WASM
// unchanged, and the completeness layer starts where lakeui's bucket rule
// says the first unsettled bucket is.
import { test } from 'node:test'
import { mkdtempSync, readFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import assert from 'node:assert/strict'
import { createRequire } from 'node:module'
import fc from 'fast-check'
import * as f from '@uwdata/flechette'
import { parquetReadObjects } from 'hyparquet'
import { bucketState, incompleteStart, labelOf, niceStep, rowState } from '../../src/completeness.js'
import { execute } from '../../src/engine.js'
import { MetaCache, compressors, mapGet, parsers } from '../../src/parquet.js'
import { cachedPlanner } from '../../src/planclient.js'
import { fakeStore, fixture, planOf } from '../../test/helpers.js'
import { toIPC } from '../src/arrow.js'
import { columnsQuery, nsToMsFloor, toColumns } from '../src/columns.js'
import { stateFill, timeLayer } from '../src/overlay.js'
import { createFromFilesSQL } from '../src/urlsql.js'

const files = { logs: fixture('logs-late.parquet'), traces: fixture('traces.parquet') }
const ab = u8 => u8.buffer.slice(u8.byteOffset, u8.byteOffset + u8.byteLength)
const raw = {
  logs: await parquetReadObjects({ file: ab(files.logs), columns: ['Timestamp', 'SeverityText', 'ResourceAttributes'], compressors, parsers }),
  traces: await parquetReadObjects({ file: ab(files.traces), columns: ['Timestamp', 'Duration', 'ResourceAttributes'], compressors, parsers }),
}
const span = s => {
  const ts = raw[s].map(r => r.Timestamp)
  return [ts.reduce((a, b) => (b < a ? b : a)), ts.reduce((a, b) => (b > a ? b : a))]
}

function world(signal, over) {
  const s = fakeStore()
  const objs = ['x1', 'x2'].map(k => {
    s.objects.set(`url-${signal}-${k}`, files[signal])
    return { key: `${signal}/${k}`, url: `url-${signal}-${k}`, size: files[signal].length }
  })
  const plan = planOf(objs, { signal, ...over })
  return { s, plan, planner: cachedPlanner(async () => plan) }
}

async function load(signal, over) {
  const { s, plan, planner } = world(signal, over)
  const q = columnsQuery({ signal, fromNs: over.fromNs, toNs: over.toNs })
  const r = await execute(q, { planner, request: { fromNs: over.fromNs, toNs: over.toNs }, fetch: s.fetch, metaCache: new MetaCache() })
  return { r, plan, q }
}

test('columns = a brute-force read of the window, labelled by lakeui completeness (both signals)', async () => {
  for (const signal of ['logs', 'traces']) {
    const [lo, hi] = span(signal)
    // the boundary 1 ns after a real row: that row is settled, the next instant is not (an off-by-one fails here)
    const sorted = raw[signal].map(r => r.Timestamp).sort((a, b) => (a < b ? -1 : a > b ? 1 : 0))
    const mid = sorted[Math.floor(sorted.length / 2)] + 1n
    const over = { fromNs: lo + (hi - lo) / 5n, toNs: hi, completeness: 'partial', completeThroughNs: mid + 60_000_000_000n,
      maxLatenessNs: 60_000_000_000n, incompleteFromNs: mid }
    const { r, plan, q } = await load(signal, over)
    assert.equal(r.status, 'ok')
    const want = raw[signal].filter(x => x.Timestamp >= over.fromNs && x.Timestamp < over.toNs)
    const c = r.result.columns
    assert.equal(r.result.rows, 2 * want.length, `${signal}: two copies of the fixture`)
    const label = labelOf(plan)
    for (let i = 0; i < c.ts_ns.length; i++) {
      assert.equal(c.cstate[i], rowState(label, c.ts_ns[i]))
      assert.equal(c.ts[i], nsToMsFloor(c.ts_ns[i]))
      assert.ok(c.b0[i] <= c.ts[i] && c.ts[i] < c.b1[i] + 1, 'a row lies in its bucket')
      const b0 = over.fromNs + ((c.ts_ns[i] - over.fromNs) / q.q.stepNs) * q.q.stepNs
      assert.equal(c.bstate[i], bucketState(label, b0, b0 + q.q.stepNs < over.toNs ? b0 + q.q.stepNs : over.toNs))
      assert.ok(c.pod[i] && c.cluster[i], 'pod and cluster from ResourceAttributes')
    }
    assert.ok(c.cstate.includes('incomplete') && c.cstate.includes('complete'), 'the window straddles incomplete_from')
    // the pod column is the map's value, per row
    const pods = new Set(want.map(x => mapGet(x.ResourceAttributes, 'k8s.pod.name')))
    assert.deepEqual(new Set(c.pod), pods)
    if (signal === 'traces') {
      const sum = want.reduce((a, x) => a + Number(x.Duration) / 1e6, 0)
      assert.ok(Math.abs(c.dur_ms.reduce((a, b) => a + b, 0) - 2 * sum) < 1e-6)
    }
    // only the needed column chunks: this fixture's Body chunk is 17,047 of
    // 30,822 bytes, and the 8 KiB footer read is most of the rest
    if (signal === 'logs') assert.ok(r.stats.bytes < plan.totalBytes - 2 * 17047, `${r.stats.bytes} of ${plan.totalBytes}`)
  }
})

test('unknown labels every row and bucket unknown; complete labels none incomplete', async () => {
  const [lo, hi] = span('logs')
  const unk = await load('logs', { fromNs: lo, toNs: hi + 1n, completeness: 'unknown', completeThroughNs: null })
  assert.ok(unk.r.result.columns.cstate.every(s => s === 'unknown'))
  assert.ok(unk.r.result.columns.bstate.every(s => s === 'unknown'))
  const done = await load('logs', { fromNs: lo, toNs: hi + 1n, completeness: 'complete', completeThroughNs: hi + 10n ** 12n, maxLatenessNs: 1n })
  assert.ok(done.r.result.columns.cstate.every(s => s === 'complete'))
})

// DuckDB-WASM in Node (the blocking build; same wasm as the page's eh bundle)
const require = createRequire(import.meta.url)
async function duckDB() {
  const d = new URL('../node_modules/@duckdb/duckdb-wasm/dist/', import.meta.url).pathname
  const duckdb = require(d + 'duckdb-node-blocking.cjs')
  const db = await duckdb.createDuckDB({ mvp: { mainModule: d + 'duckdb-mvp.wasm', mainWorker: '' }, eh: { mainModule: d + 'duckdb-eh.wasm', mainWorker: '' } },
    new duckdb.VoidLogger(), duckdb.NODE_RUNTIME)
  await db.instantiate()
  return db
}
async function duck() {
  return (await duckDB()).connect()
}

test('Arrow IPC → DuckDB-WASM keeps every value (exact ns, states, doubles), and an empty load still makes the table', async () => {
  const conn = await duck()
  const [lo, hi] = span('traces')
  const over = { fromNs: lo, toNs: hi + 1n, completeness: 'partial', completeThroughNs: lo + (hi - lo) / 2n + 5n, maxLatenessNs: 5n, incompleteFromNs: lo + (hi - lo) / 2n }
  const { r } = await load('traces', over)
  conn.insertArrowFromIPCStream(toIPC(f, r.result), { name: 'spans', create: true })
  const c = r.result.columns
  const got = conn.query(`SELECT count(*)::INTEGER AS n, sum(ts_ns)::VARCHAR AS s, count(*) FILTER (WHERE cstate = 'incomplete')::INTEGER AS inc,
    sum(dur_ms) AS d, min(ts) AS t0, count(DISTINCT pod)::INTEGER AS pods FROM spans`).toArray()[0].toJSON()
  assert.equal(got.n, c.ts_ns.length)
  assert.equal(got.s, String(c.ts_ns.reduce((a, b) => a + b, 0n)))
  assert.equal(got.inc, c.cstate.filter(s => s === 'incomplete').length)
  assert.ok(Math.abs(got.d - c.dur_ms.reduce((a, b) => a + b, 0)) < 1e-6)
  assert.equal(Number(got.t0), Math.min(...c.ts))
  assert.equal(got.pods, new Set(c.pod).size)
  const types = Object.fromEntries(conn.query('DESCRIBE spans').toArray().map(x => [x.column_name, x.column_type]))
  assert.deepEqual([types.ts, types.b0, types.ts_ns, types.pod, types.dur_ms], ['TIMESTAMP', 'TIMESTAMP', 'BIGINT', 'VARCHAR', 'DOUBLE'])
  const empty = toColumns('logs', [], labelOf(planOf([], over)), { ...over, stepNs: niceStep(over.fromNs, over.toNs) })
  conn.insertArrowFromIPCStream(toIPC(f, empty), { name: 'logs', create: true })
  assert.equal(Number(conn.query('SELECT count(*) AS n FROM logs').toArray()[0].n), 0)
  assert.ok(conn.query('DESCRIBE logs').toArray().some(x => x.column_name === 'severity'))
})

const labelArb = fc.record({
  from: fc.bigInt({ min: 0n, max: 10n ** 12n }),
  len: fc.bigInt({ min: 1n, max: 10n ** 13n }),
  ct: fc.option(fc.bigInt({ min: 0n, max: 2n * 10n ** 13n }), { nil: null }),
  ml: fc.option(fc.bigInt({ min: 0n, max: 10n ** 11n }), { nil: null }),
  inc: fc.option(fc.bigInt({ min: 0n, max: 2n * 10n ** 13n }), { nil: null }),
  state: fc.constantFrom('complete', 'partial', 'unknown'),
}).map(x => ({ state: x.state, fromNs: x.from, toNs: x.from + x.len, completeThroughNs: x.ct, incompleteFromNs: x.inc,
  maxLatenessNs: x.ml, lateObjects: 0, startComplete: true, watermarkStatus: 'ok', note: '' }))

test('property: the band starts at the first bucket lakeui calls not settled, and covers every such bucket', () => {
  fc.assert(fc.property(labelArb, label => {
    const stepNs = niceStep(label.fromNs, label.toNs, 60)
    const { band } = timeLayer(label, stepNs)
    const firstBad = []
    for (let b0 = label.fromNs; b0 < label.toNs; b0 += stepNs) {
      const b1 = b0 + stepNs < label.toNs ? b0 + stepNs : label.toNs
      if (bucketState(label, b0, b1) !== 'complete') firstBad.push(b0)
    }
    if (firstBad.length === 0) return band.length === 0
    // buckets are not settled from the first one on (monotone), and the band starts there, in ms
    const expect = new Date(nsToMsFloor(firstBad[0]))
    return band.length === 1 && band[0].x1.getTime() === expect.getTime() &&
      band[0].x2.getTime() === nsToMsFloor(label.toNs) && band[0].state === (label.state === 'unknown' ? 'unknown' : 'incomplete')
  }), { numRuns: 2000 })
})

test('the state fill maps every state, patterns for the unsettled ones', () => {
  const s = stateFill()
  assert.deepEqual(s.domain, ['complete', 'incomplete', 'unknown'])
  assert.match(s.range[1], /^url\(#mos-hatch\)$/)
  assert.match(s.range[2], /^url\(#mos-unknown\)$/)
})

test('the DuckDB-reads-URLs SQL: exact BigInt literals, states from incompleteStart, unknown → unknown', () => {
  const label = { state: 'partial', fromNs: 1790000000123456789n, toNs: 1790000600123456789n, completeThroughNs: 1790000500000000001n,
    incompleteFromNs: 1790000440000000001n, maxLatenessNs: 60000000000n, lateObjects: 0, startComplete: true }
  const sql = createFromFilesSQL('logs', 'logs', ["a'b.parquet"], label)
  assert.ok(sql.includes(`${incompleteStart(label)}::BIGINT`), 'the threshold is the module\'s, exact')
  assert.ok(sql.includes('1790000000123456789::BIGINT'), 'from, exact (no double)')
  assert.ok(sql.includes("'a''b.parquet'"), 'names are quoted')
  const u = createFromFilesSQL('spans', 'traces', ['x.parquet'], { ...label, state: 'unknown' })
  assert.ok(/'unknown' AS cstate/.test(u) && /'unknown' AS bstate/.test(u))
  assert.throws(() => createFromFilesSQL('logs', 'logs', [], label), /no files/)
})

// ---- the tail (AMBIGUITY.md #10 (b)) -------------------------------------------------

test('the tail: its rows, and every bucket holding one, are incomplete in the columns; the basis part\'s by the label', async () => {
  const [lo, hi] = span('logs')
  const over = { fromNs: lo, toNs: hi + 1n, completeness: 'complete', completeThroughNs: hi + 10n ** 12n, maxLatenessNs: 1n }
  const s = fakeStore(new Map([['u-a', files.logs], ['u-t', files.logs]]))
  const plan = { ...planOf([{ key: 'a', url: 'u-a', size: files.logs.length }], { signal: 'logs', ...over }), atBasis: true, basis: 'b1.x.y',
    tailObjects: [{ key: 't', url: 'u-t', size: files.logs.length, tail: true, minTimeNs: null, maxTimeNs: null }], tail: { objects: 1 } }
  const q = columnsQuery({ signal: 'logs', fromNs: over.fromNs, toNs: over.toNs })
  const r = await execute(q, { planner: cachedPlanner(async () => plan), request: { fromNs: over.fromNs, toNs: over.toNs }, fetch: s.fetch, metaCache: new MetaCache() })
  assert.equal(r.status, 'ok')
  const c = r.result.columns
  const n = raw.logs.length
  assert.equal(r.result.rows, 2 * n)
  assert.equal(r.result.tailRows, n)
  assert.equal(c.cstate.filter(x => x === 'incomplete').length, n) // exactly the tail's rows: the label says complete
  assert.ok(c.bstate.every(x => x === 'incomplete')) // each bucket holds a copy's tail row
  assert.equal(r.state, 'incomplete')
  // without a tail, the same rows are all complete
  const t0 = toColumns('logs', [{ tsNs: lo, service: '', pod: '', cluster: '', severity: 'INFO' }], labelOf(plan), { ...over, stepNs: niceStep(over.fromNs, over.toNs) })
  assert.deepEqual([t0.columns.cstate[0], t0.columns.bstate[0], t0.tailRows], ['complete', 'complete', 0])
})

test('the DuckDB-reads-URLs SQL with a tail: its files\' rows and their buckets incomplete, a tail column to count them', () => {
  const label = { state: 'complete', fromNs: 1790000000123456789n, toNs: 1790000600123456789n, completeThroughNs: 1790009000000000000n,
    incompleteFromNs: null, maxLatenessNs: 60000000000n, lateObjects: 0, startComplete: true }
  const plain = createFromFilesSQL('logs', 'logs', ['a.parquet'], label)
  assert.ok(/'complete' AS cstate/.test(plain) && /'complete' AS bstate/.test(plain) && !plain.includes('filename'))
  const sql = createFromFilesSQL('logs', 'logs', ['a.parquet', "t'1.parquet"], label, { tailFiles: ["t'1.parquet"] })
  assert.ok(sql.includes("read_parquet(['a.parquet', 't''1.parquet'], filename = true)"))
  assert.ok(sql.includes("filename IN ('t''1.parquet') AS tail"))
  assert.ok(sql.includes("CASE WHEN tail THEN 'incomplete' ELSE 'complete' END AS cstate"))
  assert.ok(sql.includes('bool_or(tail) OVER (PARTITION BY b0_ns) AS tail_bucket'))
  assert.ok(sql.includes("CASE WHEN tail_bucket THEN 'incomplete' ELSE 'complete' END AS bstate"))
  assert.ok(/, tail\nFROM c$/.test(sql))
  const partial = createFromFilesSQL('logs', 'logs', ['a.parquet', 't.parquet'], { ...label, state: 'partial', incompleteFromNs: 1790000300000000000n },
    { tailFiles: ['t.parquet'] })
  assert.ok(partial.includes(`WHEN tail THEN 'incomplete' WHEN ts_ns + 999 >= ${incompleteStart({ ...label, state: 'partial', incompleteFromNs: 1790000300000000000n })}::BIGINT`))
  const unk = createFromFilesSQL('logs', 'logs', ['a.parquet', 't.parquet'], { ...label, state: 'unknown' }, { tailFiles: ['t.parquet'] })
  assert.ok(/'unknown' AS cstate/.test(unk) && /'unknown' AS bstate/.test(unk))
  assert.throws(() => createFromFilesSQL('logs', 'logs', ['a.parquet'], label, { tailFiles: ['t.parquet'] }), /tail file/)
})

test('url mode with a tail, run in DuckDB: per bucket the same rows, tail rows and states as the range reader\'s columns', async (ctx) => {
  const db = await duckDB()
  const conn = db.connect()
  db.registerFileBuffer('a.parquet', files.logs)
  // the tail: another object, the fixture's 100 oldest rows (so their buckets
  // are the early ones), written by DuckDB (in Node, to the real file system)
  const dir = mkdtempSync(join(tmpdir(), 'mosaic-tail-'))
  const t = join(dir, 't.parquet')
  ctx.after(() => rmSync(dir, { recursive: true, force: true }))
  conn.query(`COPY (SELECT * FROM read_parquet('a.parquet') ORDER BY Timestamp LIMIT 100) TO '${t}' (FORMAT parquet)`)
  const tailRaw = await parquetReadObjects({ file: ab(new Uint8Array(readFileSync(t))), columns: ['Timestamp'], compressors, parsers })
  assert.equal(tailRaw.length, 100)
  const [lo, hi] = span('logs')
  const over = { fromNs: lo, toNs: hi + 1n, completeThroughNs: hi + 10n ** 12n, maxLatenessNs: 1n }
  for (const state of ['complete', 'unknown']) {
    const label = labelOf(planOf([], { ...over, completeness: state }))
    const stepNs = niceStep(over.fromNs, over.toNs)
    conn.query(createFromFilesSQL('u', 'logs', ['a.parquet', t], label, { stepNs, tailFiles: [t] }))
    const got = conn.query(`SELECT epoch_ms(b0)::BIGINT AS b, count(*)::INTEGER AS n, count(*) FILTER (WHERE tail)::INTEGER AS t,
      count(*) FILTER (WHERE cstate = 'incomplete')::INTEGER AS inc, count(*) FILTER (WHERE cstate = 'unknown')::INTEGER AS unk,
      min(bstate) AS bmin, max(bstate) AS bmax FROM u GROUP BY 1 ORDER BY 1`).toArray().map(r => r.toJSON())
    const rows = [...raw.logs.map(r => ({ tsNs: r.Timestamp })), ...tailRaw.map(r => ({ tsNs: r.Timestamp, tail: true }))]
      .map(r => ({ ...r, service: '', pod: '', cluster: '', severity: '' }))
    const want = toColumns('logs', rows, label, { ...over, stepNs })
    const byB = new Map()
    want.columns.b0.forEach((b, i) => {
      const x = byB.get(b) ?? { b, n: 0, t: 0, inc: 0, unk: 0, states: new Set() }
      x.n++
      if (rows[i].tail) x.t++
      if (want.columns.cstate[i] === 'incomplete') x.inc++
      if (want.columns.cstate[i] === 'unknown') x.unk++
      x.states.add(want.columns.bstate[i])
      byB.set(b, x)
    })
    const exp = [...byB.values()].sort((x, y) => x.b - y.b)
    assert.equal(got.length, exp.length, state)
    got.forEach((g, i) => {
      const e = exp[i]
      assert.deepEqual([Number(g.b), g.n, g.t, g.inc, g.unk], [e.b, e.n, e.t, e.inc, e.unk], `${state} bucket ${i}`)
      assert.deepEqual([g.bmin, g.bmax], [[...e.states][0], [...e.states][0]]) // one state per bucket, the same
    })
    if (state === 'complete') {
      assert.equal(got.reduce((a, g) => a + g.inc, 0), 100) // exactly the tail's rows
      assert.ok(got.some(g => g.bmin === 'complete') && got.some(g => g.bmin === 'incomplete'))
    }
  }
})
