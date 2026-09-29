// The Mosaic spike in Chromium against the real pieces (lakeuirig with
// LUI_RICH_SPANS=1: the Go edge, the Rust consumer, the query service, a
// counting pass-through in front of SeaweedFS). Checks and measures:
//
//   - range mode (lakeui's range reader → Arrow → DuckDB-WASM): at a basis
//     (D30) with its tail (AMBIGUITY.md #10 (b)), the tables hold what
//     ClickHouse holds for the window (the basis part) plus the late batch
//     (the tail), and the late rows (and only they) are labelled
//     incomplete; every GET is a ranged 206; the completeness band is on
//     the time chart;
//   - url mode (DuckDB reads the presigned URLs): at a basis with its tail
//     too, the same tables (compared column by column; timestamps at µs,
//     which is all DuckDB keeps), read as whole objects; an expired URL (a
//     real 403: the plan answers replayed past their expiry) re-plans;
//   - url mode with the HEAD shim and trusted HEADs: DuckDB fails to open the
//     files; the page says "not read" and draws nothing;
//   - cross-filtering: after each brush and click, every chart's total equals
//     an independent count under that chart's filter;
//   - brush latency with and without Mosaic's pre-aggregation, at 1× and at
//     scaled-up table sizes; memory; lakeui's hyparquet-only path for the
//     same view (bytes, time, and a brush's re-read);
//   - the stale-cube hazard: pre-aggregated tables kept across a reload of
//     different rows under the same table names answer for the old rows;
//   - an unknown watermark draws every row grey (no basis can be issued:
//     planned unpinned, and said so).
//
// Results: test-results/mosaic-e2e.json. MOSAIC_RIG_OUT=<file> reuses a rig
// already running (its stdout); otherwise the rig is started here.
import { test, expect } from '@playwright/test'
import { spawn } from 'node:child_process'
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { covers } from '../../test/pw-trace.js'

const here = dirname(fileURLToPath(import.meta.url))
const queryDir = join(here, '..', '..', '..', 'query')
const out = join(here, '..', 'test-results')
let rig
let info
const measured = {}

async function ch(sql) {
  const r = await fetch(info.ch, { method: 'POST', body: sql })
  const t = (await r.text()).trim()
  if (!r.ok) throw new Error(`${sql}: ${t}`)
  return t
}
const nsOf = iso => {
  const m = /^(.*T\d\d:\d\d:\d\d)(?:\.(\d+))?Z$/.exec(iso)
  return BigInt(Date.parse(m[1] + 'Z')) * 1_000_000n + BigInt(((m[2] ?? '') + '000000000').slice(0, 9))
}
const t64 = ns => `fromUnixTimestamp64Nano(toInt64(${ns}))`
async function chCount(table, cluster, fromNs, toNs) {
  return Number(await ch(`SELECT count() FROM ${info.db}.${table} WHERE ResourceAttributes['k8s.cluster.name'] = '${cluster}' AND Timestamp >= ${t64(fromNs)} AND Timestamp < ${t64(toNs)}`))
}
async function tap(method = 'GET') {
  const r = await fetch(new URL('/rig/tap', info.page), { method })
  return method === 'GET' ? (await r.json()).entries ?? [] : null
}
const tapSummary = es => ({ requests: es.length, bytes: es.reduce((a, e) => a + (e.bytes ?? 0), 0),
  statuses: Object.fromEntries([...new Set(es.map(e => `${e.method} ${e.status}${e.range ? ' range' : ''}`))].map(k => [k, es.filter(e => `${e.method} ${e.status}${e.range ? ' range' : ''}` === k).length])) })

test.describe.configure({ mode: 'serial' })

test.beforeAll(async () => {
  test.setTimeout(300_000)
  if (process.env.MOSAIC_RIG_OUT) {
    info = JSON.parse(readFileSync(process.env.MOSAIC_RIG_OUT, 'utf8').split('LAKEUI_RIG_READY ')[1])
    return
  }
  const cmd = process.env.LAKEUI_RIG_BIN ? [process.env.LAKEUI_RIG_BIN, []] : ['go', ['run', './integration/lakeuirig']]
  rig = spawn(cmd[0], cmd[1], { cwd: queryDir, stdio: ['ignore', 'pipe', 'pipe'], env: { ...process.env, LUI_RICH_SPANS: '1' } })
  const lines = []
  rig.stderr.on('data', d => lines.push(String(d)))
  info = await new Promise((resolve, reject) => {
    let buf = ''
    const timer = setTimeout(() => reject(new Error('the rig did not get ready:\n' + lines.join(''))), 290_000)
    rig.stdout.on('data', d => {
      buf += d
      const m = /LAKEUI_RIG_READY (.*)\n/.exec(buf)
      if (m) {
        clearTimeout(timer)
        resolve(JSON.parse(m[1]))
      }
    })
    rig.on('exit', c => reject(new Error(`the rig exited ${c}:\n` + lines.join(''))))
  })
})

test.afterAll(async () => {
  if (rig && rig.exitCode === null) {
    const done = new Promise(r => rig.on('exit', r))
    rig.kill('SIGTERM') // the rig removes its bucket and database
    await done
  }
  mkdirSync(out, { recursive: true })
  writeFileSync(join(out, 'mosaic-e2e.json'), JSON.stringify(measured, null, 1))
  console.log('measured:', JSON.stringify(measured, null, 1))
})

/** A signed-in page of the spike with the given switches. */
async function open(browser, user, query) {
  const page = await (await browser.newContext({ viewport: { width: 1280, height: 1000 } })).newPage()
  page.on('pageerror', e => console.log('pageerror', e.message))
  await page.goto(`${info.page}mosaic/index.html?${query}`)
  await page.waitForSelector('body[data-ready="1"]')
  await page.click('#signin')
  await page.click(`text=Sign in as ${user}`)
  await page.waitForSelector('body[data-ready="1"]')
  await expect(page.locator('#who')).toContainText(user)
  expect(new URL(page.url()).search).toContain(query.split('&')[0]) // the switches survived the sign-in
  return page
}
async function load(page, { from = info.truth.at, to = info.truth.late_to, cluster = '' } = {}) {
  await page.fill('#from', from)
  await page.fill('#to', to)
  await page.selectOption('#cluster', cluster)
  return page.evaluate(() => window.mos.load())
}
const checkAll = page => page.evaluate(async () => Promise.all(['A', 'B', 'C', 'D'].map(id => window.mos.check(id))))
function expectConsistent(checks, what) {
  for (const c of checks) expect(c.shown, `${what}: chart ${c.id} (${c.where})`).toBe(c.expected)
}
/**
 * With pre-aggregation Mosaic answers a brush from pixel-binned cubes: the
 * brush's edges are rounded to the pixel, so a chart may count the rows of an
 * edge pixel differently from the exact predicate. Recorded, not asserted.
 */
function diffs(checks) {
  return checks.filter(c => c.shown !== c.expected).map(c => ({ id: c.id, shown: c.shown, expected: c.expected }))
}

/** Drag across a chart's frame in `steps` moves; the latency of each move, and the cube tables it made. */
async function brush(page, id, { from = 0.2, to = 0.55, steps = 8, y = 0.5 } = {}) {
  const f = await page.evaluate(i => window.mos.frame(i), id)
  const x = r => f.left + r * (f.right - f.left)
  const yy = f.top + y * (f.bottom - f.top)
  await page.evaluate(() => window.mos.mark())
  await page.mouse.move(x(from), yy)
  const hover = await page.evaluate(() => window.mos.since())
  await page.evaluate(() => window.mos.mark())
  await page.mouse.down()
  const down = await page.evaluate(() => window.mos.since())
  const moves = []
  for (let i = 1; i <= steps; i++) {
    await page.evaluate(() => window.mos.mark())
    await page.mouse.move(x(from + ((to - from) * i) / steps), yy)
    const s = await page.evaluate(() => window.mos.since())
    moves.push({ ms: s.ms, queries: s.queries.length, preaggTables: s.preaggTables })
  }
  await page.mouse.up()
  const ms = moves.map(m => m.ms).sort((a, b) => a - b)
  return { activation: { ms: hover.ms + down.ms, preaggTables: hover.preaggTables + down.preaggTables, queries: hover.queries.length + down.queries.length },
    moves, median: ms[Math.floor(ms.length / 2)], max: ms[ms.length - 1] }
}

let settled // settled_through (ns) as the plans report it
let rangeTables
test('range mode: plan → range reads → Arrow → DuckDB; counts equal ClickHouse, late rows incomplete, every GET ranged', async ({ browser }) => {
  const page = await open(browser, 'alice', 'mode=range')
  await tap('POST')
  const rec = await load(page)
  const taps = tapSummary(await tap())
  expect(rec.status).toBe('ok')
  settled = nsOf(rec.logs.settledThrough)
  const from = nsOf(info.truth.at)
  const to = nsOf(info.truth.late_to)
  // ClickHouse has every row received before complete_through; the late batch (received after) only the lake has
  const chLogs = await chCount('otel_logs', 'lui-a', from, to)
  const chSpans = await chCount('otel_traces', 'lui-a', from, to)
  expect(rec.tables.logs.n).toBe(chLogs + info.truth.late_logs)
  expect(rec.tables.logs.n - rec.tables.logs.inc).toBe(await chCount('otel_logs', 'lui-a', from, settled))
  expect(rec.tables.logs.inc).toBe(info.truth.late_logs) // the late batch, and nothing else, is past settled_through
  // the late batch is the tail: received after the basis, read, never left out
  expect(rec.logs.basis).toMatch(/^b1\./)
  expect(rec.logs.tailObjects).toBeGreaterThan(0)
  expect(rec.logs.tailRows).toBe(info.truth.late_logs)
  expect(rec.logs.rows - rec.logs.tailRows).toBe(chLogs) // the basis part: what central holds
  expect(rec.tables.spans.n).toBe(chSpans + rec.tables.spans.inc)
  expect(rec.tables.spans.inc).toBeGreaterThan(0)
  expect(Object.keys(taps.statuses)).toEqual(['GET 206 range'])
  expect(taps.bytes).toBe(rec.logs.fetchedBytes + rec.spans.fetchedBytes) // the page's count = the tap's
  // the completeness layer: a hatched band and a settled-through rule on the time chart; hatched parts of the other bars
  await expect(page.locator('#chart-A [fill="url(#mos-band)"]')).toHaveCount(1)
  await expect(page.locator('#chart-A text', { hasText: 'settled through' })).toHaveCount(1)
  expect(await page.locator('#chart-B [fill="url(#mos-hatch)"]').count()).toBeGreaterThan(0)
  expect(await page.locator('#chart-C [fill="url(#mos-hatch)"]').count()).toBeGreaterThan(0)
  expectConsistent(await checkAll(page), 'initial')
  await page.screenshot({ path: join(out, 'mosaic-range.png'), fullPage: true })
  const lake = await page.evaluate(() => window.mos.lakeuiLogs())
  expect(lake.count).toBe(rec.tables.logs.n)
  rangeTables = rec.tables
  measured.range = { engine: rec.engine, dataMs: rec.dataMs, chartsMs: rec.chartsMs, firstChartMs: rec.firstChartMs, logs: rec.logs, spans: rec.spans,
    tap: taps, memory: rec.memory, queries: rec.queries.map(q => ({ type: q.type, ms: q.ms, sql: q.sql.slice(0, 300) })), lakeuiSameView: lake }
})

test('url mode: DuckDB reads the URLs: the same tables (at µs), whole-object GETs; an expired URL re-plans', async ({ browser }) => {
  const page = await open(browser, 'alice', 'mode=url')
  await tap('POST')
  const rec = await load(page)
  const taps = tapSummary(await tap())
  expect(rec.status).toBe('ok')
  for (const t of ['logs', 'spans']) {
    for (const k of Object.keys(rec.tables[t])) {
      if (k === 'fp_ts_ns') continue // DuckDB reads Timestamp as TIMESTAMPTZ: µs (urlsql.js)
      expect(rec.tables[t][k], `${t}.${k}`).toEqual(rangeTables[t][k])
    }
  }
  expect(Object.keys(taps.statuses)).toEqual(['GET 200']) // whole objects, no Range, no HEAD
  expect(taps.bytes).toBe(rec.logs.plannedBytes + rec.spans.plannedBytes)
  expect(rec.logs.basis).toMatch(/^b1\./) // at a basis, as range mode
  expect(rec.logs.tailRows).toBe(info.truth.late_logs)
  expectConsistent(await checkAll(page), 'url mode')
  measured.url = { engine: rec.engine, dataMs: rec.dataMs, chartsMs: rec.chartsMs, firstChartMs: rec.firstChartMs, tap: taps, memory: rec.memory,
    ns: { range: rangeTables.logs.fp_ts_ns, url: rec.tables.logs.fp_ts_ns } }
  // X8 in this mode: the page believes its plans valid (their times pushed
  // out, as a page whose clock is behind would); the URLs really expire;
  // DuckDB gets 403s; the page re-plans. The plans are the tail's too, so
  // never cached: the stale answers are replayed, once per signal.
  const answers = new Map()
  const signalOf = r => JSON.parse(r.postData() ?? '{}').signal
  const fulfil = async (r, body) => {
    const resp = await r.fetch() // the real answer's headers (CORS)
    const headers = resp.headers()
    delete headers['content-length']
    const text = await resp.text()
    await r.fulfill({ status: resp.status(), headers, body: body ?? text })
    return text
  }
  await page.route(u => u.pathname === '/v1/plan', async r => {
    if (r.request().method() !== 'POST') return r.continue()
    answers.set(signalOf(r.request()), await fulfil(r))
  })
  const recorded = await load(page)
  await page.unrouteAll()
  expect(recorded.status).toBe('ok')
  expect([...answers.keys()].sort()).toEqual(['logs', 'traces'])
  const expiresAt = Math.max(...[...answers.values()].map(b => Date.parse(/"expires_at":"([^"]+)"/.exec(b)[1])))
  await page.waitForTimeout(Math.max(0, expiresAt - Date.now()) + 2000)
  const later = new Date(Date.now() + 3_600_000).toISOString()
  const replayed = new Set()
  await page.route(u => u.pathname === '/v1/plan', async r => {
    const sig = r.request().method() === 'POST' ? signalOf(r.request()) : null
    if (!sig || replayed.has(sig)) return r.continue()
    replayed.add(sig)
    await fulfil(r, answers.get(sig).replace(/"expires_at":"[^"]+"/, `"expires_at":"${later}"`).replace(/"replan_after":"[^"]+"/, `"replan_after":"${later}"`))
  })
  await tap('POST')
  const again = await load(page)
  await page.unrouteAll()
  const t2 = tapSummary(await tap())
  expect(again.status).toBe('ok')
  expect(again.tables.logs.n).toBe(rec.tables.logs.n)
  expect(again.logs.tailRows).toBe(rec.logs.tailRows)
  expect(t2.statuses['GET 403']).toBeGreaterThan(0)
  expect(again.logs.replans + again.spans.replans).toBeGreaterThan(0)
  measured.urlExpired = { tap: t2, replans: { logs: again.logs.replans, spans: again.spans.replans }, events: again.events.filter(e => e.type !== 'plan').slice(0, 4) }
})

test('url mode with the HEAD shim and trusted HEADs: DuckDB cannot open the files; "not read", nothing drawn', async ({ browser }) => {
  const page = await open(browser, 'alice', 'mode=url-shim&fs=head')
  await tap('POST')
  const rec = await load(page)
  expect(rec.status).toBe('failed')
  await expect(page.locator('#banner')).toHaveAttribute('data-state', 'failed')
  await expect(page.locator('#chart-A svg')).toHaveCount(0)
  const failedTap = tapSummary(await tap())
  const plans = rec.events.filter(e => e.type === 'plan').length
  expect(plans).toBe(8) // per signal: one plan and three re-plans, then it stops
  const shimOnly = await open(browser, 'alice', 'mode=url-shim')
  await tap('POST')
  const r2 = await load(shimOnly)
  const t2 = tapSummary(await tap())
  expect(r2.status).toBe('ok')
  measured.urlShim = { fsHead: { status: rec.status, plans, error: rec.events.find(e => e.type === 'read_error')?.message.slice(0, 160), tap: failedTap },
    shimOnly: { tap: t2, dataMs: r2.dataMs } }
})

test('cross-filtering is exact, and brush latency with and without pre-aggregation, at 1× and scaled up', async ({ browser }) => {
  test.setTimeout(900_000)
  measured.brush = {}
  for (const preagg of ['1', '0']) {
    const page = await open(browser, 'alice', `mode=range&preagg=${preagg}`)
    const rec = await load(page)
    expect(rec.status).toBe('ok')
    const runs = {}
    for (const scale of [1, 20, 100, 300, 600]) {
      let sc = null
      if (scale > 1) {
        try {
          sc = await page.evaluate(n => window.mos.scale(n), scale)
        } catch (e) {
          // DuckDB-WASM's wasm32 heap (≈3.1 GiB here) is the ceiling; recorded, not asserted
          runs[`${scale}x`] = { error: String(e.message).slice(0, 200), memory: await page.evaluate(() => window.mos.memory()).catch(() => null) }
          break
        }
      }
      const exact = preagg === '0'
      const drift = []
      const verify = async what => {
        const cs = await checkAll(page)
        if (exact) expectConsistent(cs, what)
        else drift.push({ after: what, diffs: diffs(cs) })
      }
      const a = await brush(page, 'A')
      await verify(`brushing time, ${scale}×`)
      // click a pod on B (its first bar), then brush latency on C
      const fb = await page.evaluate(() => window.mos.frame('B'))
      await page.evaluate(() => window.mos.mark())
      await page.mouse.click(fb.left + 20, fb.top + (fb.bottom - fb.top) * 0.25)
      const click = await page.evaluate(() => window.mos.since())
      await verify(`clicking a pod, ${scale}×`)
      const c = await brush(page, 'C', { from: 0.3, to: 0.7 })
      await verify(`brushing latency, ${scale}×`)
      const mem = await page.evaluate(() => window.mos.memory())
      runs[`${scale}x`] = { rows: sc?.rows ?? { logs: rec.tables.logs.n, spans: rec.tables.spans.n }, scaleBuildMs: sc?.buildMs ?? null, chartsMs: sc?.chartsMs ?? rec.chartsMs,
        brushTime: a, clickPod: { ms: click.ms, queries: click.queries.length }, brushLatency: c, memory: mem, ...(exact ? {} : { drift }) }
      if (scale === 1 && preagg === '1') await page.screenshot({ path: join(out, 'mosaic-brushed.png'), fullPage: true })
      // reset the selections for the next size: reload the dashboard over the same tables
    }
    measured.brush[preagg === '1' ? 'preagg' : 'noPreagg'] = runs
    await page.context().close()
  }
  // lakeui's way to answer a time brush: re-plan and re-read for the brushed window
  const page = await open(browser, 'alice', 'mode=range')
  const rec = await load(page)
  const from = nsOf(info.truth.at)
  const to = nsOf(info.truth.late_to)
  const narrowFrom = from + ((to - from) * 2n) / 10n
  const narrowTo = from + ((to - from) * 55n) / 100n
  measured.lakeuiBrushReread = await page.evaluate(([a, b]) => window.mos.lakeuiLogs(a, b), [String(narrowFrom), String(narrowTo)])
  measured.lakeuiFullView = await page.evaluate(() => window.mos.lakeuiLogs())
  expect(rec.status).toBe('ok')
  // every statement Mosaic sent for this dashboard (load, cubes, brushes, clicks), for the next test
  await brush(page, 'A')
  const fb = await page.evaluate(() => window.mos.frame('B'))
  await page.mouse.click(fb.left + 20, fb.top + (fb.bottom - fb.top) * 0.25)
  await brush(page, 'C', { from: 0.3, to: 0.7 })
  await page.evaluate(() => window.mos.idle())
  statements = await page.evaluate(() => window.mos.statements())
})

let statements = []
test('the server-connector question: Mosaic\'s statements (DuckDB dialect) on ClickHouse', async () => {
  // the same two tables in a scratch ClickHouse database, empty: this asks
  // whether ClickHouse parses and binds what Mosaic sends, and what it means
  const db = `mos_dialect_${Date.now()}`
  await ch(`CREATE DATABASE ${db}`)
  try {
    const common = 'ts_ns Int64, ts DateTime64(6), b0 DateTime64(6), b1 DateTime64(6), service String, pod String, cluster String, cstate String, bstate String'
    await ch(`CREATE TABLE ${db}.logs (${common}, severity String) ENGINE = MergeTree ORDER BY ts`)
    await ch(`CREATE TABLE ${db}.spans (${common}, name String, status String, dur_ms Float64) ENGINE = MergeTree ORDER BY ts`)
    const res = []
    for (const s of statements) {
      const r = await fetch(`${info.ch}?database=${db}`, { method: 'POST', body: s.sql })
      const t = (await r.text()).trim()
      const code = r.ok ? 'ok' : (/Code: (\d+)\. DB::Exception: [^(]*\(([A-Z_]+)\)/.exec(t)?.slice(1).join(' ') ?? t.slice(0, 80))
      res.push({ type: s.type, kind: s.sql.startsWith('DESC') ? 'describe' : /CREATE (SCHEMA|TABLE)/.test(s.sql) ? 'create' : /preagg_/.test(s.sql) ? 'cube-select' : 'select',
        ok: r.ok, code, sql: s.sql.slice(0, 160), error: r.ok ? '' : t.slice(0, 300) })
    }
    const by = {}
    for (const r of res) {
      by[r.kind] ??= { n: 0, ok: 0, codes: {} }
      by[r.kind].n++
      if (r.ok) by[r.kind].ok++
      else by[r.kind].codes[r.code] = (by[r.kind].codes[r.code] ?? 0) + 1
    }
    measured.clickhouseDialect = { statements: res.length, byKind: by, failures: res.filter(r => !r.ok).slice(0, 12) }
    expect(res.length).toBeGreaterThan(5)
  } finally {
    await ch(`DROP DATABASE IF EXISTS ${db}`)
  }
})

test('stale cubes: pre-aggregated tables kept across a reload of other rows answer for the old rows', async ({ browser }) => {
  // CAST row 33: with the cubes kept (nodrop=1, the old behaviour) the
  // brushed charts show the previous load's counts; the spike's fix (drop the
  // cube schema, clear the query cache on every load) is what "dropped" runs
  const t = test.info()
  covers(t, 'IT', 'CAST-33', 'H-5')
  const res = {}
  for (const nodrop of ['1', '0']) {
    const page = await open(browser, 'sre', `mode=range&nodrop=${nodrop}`)
    const a = await load(page, { cluster: 'lui-a' })
    await brush(page, 'A')
    const b = await load(page, { cluster: 'lui-b' }) // same window, same table names, other rows
    expect(b.tables.logs.n).not.toBe(a.tables.logs.n)
    await brush(page, 'A')
    const checks = await checkAll(page)
    res[nodrop === '1' ? 'kept' : 'dropped'] = checks.map(c => ({ id: c.id, shown: c.shown, expected: c.expected }))
    // (pixel-edge drift aside: a few rows at most, see diffs())
    await page.context().close()
  }
  measured.staleCubes = res
  // with the cubes kept, the brushed charts show the other cluster's counts;
  // dropped, they are within an edge pixel of the exact answer
  const off = c => Math.abs(c.shown - c.expected) / Math.max(1, c.expected)
  expect(Math.max(...res.kept.map(off))).toBeGreaterThan(0.1)
  expect(Math.max(...res.dropped.map(off))).toBeLessThan(0.02)
})

test('an unknown watermark: every row and bucket unknown, the whole time axis grey', async ({ browser }) => {
  const page = await open(browser, 'alice', 'mode=range')
  const drop = await fetch(new URL('/rig/watermark?op=drop', info.page), { method: 'POST' })
  expect(drop.status).toBe(204)
  try {
    const rec = await load(page)
    expect(rec.status).toBe('ok')
    expect(rec.logs.basis).toBe(null) // no basis without a watermark: unpinned, and said so
    expect(rec.logs.unpinned).toBe('basis_unverifiable')
    expect(rec.tables.logs.unk).toBe(rec.tables.logs.n)
    expect(rec.tables.spans.unk).toBe(rec.tables.spans.n)
    await expect(page.locator('#banner')).toHaveAttribute('data-state', 'unknown')
    await expect(page.locator('#chart-A [fill="url(#mos-unknown)"]')).toHaveCount(1)
    expect(await page.locator('#chart-B [fill="url(#mos-unknown)"]').count()).toBeGreaterThan(0)
    await page.screenshot({ path: join(out, 'mosaic-unknown.png'), fullPage: true })
    measured.unknown = { logs: rec.tables.logs, spans: rec.tables.spans }
  } finally {
    await fetch(new URL('/rig/watermark?op=restore', info.page), { method: 'POST' })
  }
})
