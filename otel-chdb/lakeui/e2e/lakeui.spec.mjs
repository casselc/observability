// The lake UI in Chromium against the real pieces: the Go edge publishes,
// the Rust consumer ingests into ClickHouse and publishes the watermark, the
// query service plans (query/integration/lakeuirig brings them up), and the
// page signs in through an authorization-code + PKCE flow on the query
// service's own test issuer.
//
// Checked: results equal ClickHouse's count for the same scope and window;
// every run is at a basis (D30) and reads its tail (AMBIGUITY.md #10 (b)):
// the basis part equals what central holds, the late batch received after
// it is the tail, counted and drawn incomplete; at a held basis the basis
// part does not change when more data arrives (/rig/more) and is answered
// from what was kept, while the tail grows; a cross-cluster or
// namespace-scoped plan shows as refused, not empty; an expired URL (a real
// 403 from SeaweedFS, the plan answer replayed past its expiry) re-plans;
// persistent failures end as a visible error naming the objects; an unknown
// watermark shows unknown (planned unpinned: no basis can be issued); a narrow query
// fetches a fraction of each object (measured by the page and, separately,
// by a counting pass-through in front of SeaweedFS); the lake index (D27)
// gives trace-by-id and text search the same answers for fewer bytes, and
// an object it has not indexed yet is read, not missed.
import { test, expect } from '@playwright/test'
import { spawn } from 'node:child_process'
import { mkdirSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const queryDir = join(here, '..', '..', 'query')
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
  // RFC 3339 with up to 9 fraction digits → ns (BigInt), as the page does
  const m = /^(.*T\d\d:\d\d:\d\d)(?:\.(\d+))?Z$/.exec(iso)
  return BigInt(Date.parse(m[1] + 'Z')) * 1_000_000n + BigInt(((m[2] ?? '') + '000000000').slice(0, 9))
}
const t64 = ns => `fromUnixTimestamp64Nano(toInt64(${ns}))`
const inCluster = (c, col = 'ResourceAttributes') => `${col}['k8s.cluster.name'] = '${c}'`

async function chLogCount(fromNs, toNs, where = '1') {
  return Number(await ch(`SELECT count() FROM ${info.db}.otel_logs WHERE ${inCluster('lui-a')} AND Timestamp >= ${t64(fromNs)} AND Timestamp < ${t64(toNs)} AND ${where}`))
}

test.describe.configure({ mode: 'serial' })

test.beforeAll(async () => {
  test.setTimeout(240_000)
  const cmd = process.env.LAKEUI_RIG_BIN ? [process.env.LAKEUI_RIG_BIN, []] : ['go', ['run', './integration/lakeuirig']]
  rig = spawn(cmd[0], cmd[1], { cwd: queryDir, stdio: ['ignore', 'pipe', 'pipe'], env: process.env })
  const lines = []
  rig.stderr.on('data', d => lines.push(String(d)))
  info = await new Promise((resolve, reject) => {
    let buf = ''
    const timer = setTimeout(() => reject(new Error('the rig did not get ready:\n' + lines.join(''))), 230_000)
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
  mkdirSync(join(here, '..', 'test-results'), { recursive: true })
  writeFileSync(join(here, '..', 'test-results', 'lakeui-e2e.json'), JSON.stringify(measured, null, 1))
  console.log('measured:', JSON.stringify(measured, null, 1))
})

/** Signs in as user through the IdP's page; returns the page. */
async function signedIn(browser, user) {
  const page = await (await browser.newContext()).newPage()
  page.on('pageerror', e => console.log('pageerror', e.message))
  await page.goto(info.page)
  await page.waitForSelector('body[data-ready="1"]')
  await page.click('#signin')
  await page.click(`text=Sign in as ${user}`)
  await page.waitForSelector('body[data-ready="1"]')
  await expect(page.locator('#who')).toContainText(user)
  return page
}

async function runView(page, view, { from, to, cluster = '', set = async () => {} }) {
  await page.evaluate(v => window.lakeui.selectView(v), view)
  await page.selectOption('#cluster', cluster)
  await page.selectOption('#range', 'custom')
  await page.fill('#from', from)
  await page.fill('#to', to)
  await set()
  const n = Number(await page.evaluate(() => document.body.dataset.runs ?? '0'))
  await page.click(`#run-${view}`)
  await page.waitForFunction(k => Number(document.body.dataset.runs ?? '0') > k, n)
  return page.evaluate(() => window.lakeui.state.last)
}

async function tap(method = 'GET') {
  const r = await fetch(new URL('/rig/tap', info.page), { method })
  return method === 'GET' ? (await r.json()).entries ?? [] : null
}

const shot = (p, name) => p.screenshot({ path: join(here, '..', 'test-results', `lakeui-${name}.png`), fullPage: true })
let page
let ct // complete_through (ns, custody time) as the plans report it
let settled // complete_through − max_lateness (ns, event time): what the UI may draw as settled
let centralAll // central's lui-a rows in the whole window: the basis part's
test('sign in with PKCE, then logs over data before and after complete_through', async ({ browser }) => {
  page = await signedIn(browser, 'alice')
  const s = await runView(page, 'logs', { from: info.truth.at, to: info.truth.late_to })
  expect(s.status).toBe('ok')
  expect(s.completeness).toBe('partial')
  ct = nsOf(s.completeThrough)
  settled = nsOf(s.settledThrough)
  expect(settled < ct).toBe(true) // max_lateness > 0 (CAST row 26)
  const lateFrom = nsOf(info.truth.late_from)
  expect(ct < lateFrom).toBe(true)
  const fromNs = nsOf(info.truth.at)
  const toNs = nsOf(info.truth.late_to)
  // central has everything before complete_through and none of the late batch
  const central = await chLogCount(fromNs, toNs)
  expect(await chLogCount(ct, toNs)).toBe(0)
  expect(s.count).toBe(central + info.truth.late_logs)
  // at a basis (D30), its tail read too (AMBIGUITY.md #10 (b)): the basis
  // part is exactly what central holds, the late batch is the tail
  expect(s.atBasis).toBe(true)
  expect(s.basis).toMatch(/^b1\./)
  expect(s.afterBasis).toBe(0) // nothing received after the basis is left out
  expect(s.tailObjects).toBeGreaterThan(0)
  expect(s.tailRows).toBe(info.truth.late_logs)
  expect(s.count - s.tailRows).toBe(central)
  centralAll = central
  // the late rows are the incomplete ones
  await expect(page.locator('#banner')).toHaveAttribute('data-state', 'incomplete')
  await expect(page.locator('#banner')).toContainText(`${info.truth.late_logs} row(s) received after the basis`)
  await expect(page.locator('[data-testid="basis"]')).toContainText('the tail')
  expect(s.rows.length).toBe(50)
  for (const r of s.rows) expect(r.state).toBe(nsOf(r.ts) >= settled ? 'incomplete' : 'complete')
  expect(s.rows.every(r => r.state === 'incomplete' && r.tail)).toBe(true) // the newest 50 are late: the tail
  const inc = s.buckets.filter(b => b.state === 'incomplete')
  expect(inc.length).toBeGreaterThan(0)
  expect(s.buckets.filter(b => b.state === 'complete').every(b => nsOf(b.from) < settled)).toBe(true)
  await expect(page.locator('svg [data-marker="complete-through"]')).toHaveCount(1)
  await shot(page, 'logs-partial')
  measured.full_window = { lake: s.count, basis_part: s.count - s.tailRows, tail: s.tailRows, central, late: info.truth.late_logs, objects: s.objects,
    basis_objects: s.basisObjects, tail_objects: s.tailObjects, fetched: s.fetchedBytes, planned: s.plannedBytes }
})

test('a window closed before complete_through − max_lateness is complete and equals ClickHouse, with and without filters', async () => {
  const from = info.truth.at
  const toNs = settled
  const to = new Date(Number(toNs / 1_000_000n)).toISOString().slice(0, 19) + '.' + String(toNs % 1_000_000_000n).padStart(9, '0') + 'Z'
  const fromNs = nsOf(from)
  const cases = [
    { name: 'all', set: async () => {}, where: '1' },
    { name: 'ERROR + status=500', set: async () => { await page.fill('#log-text', 'status=500'); await page.check('#log-sev input[value="ERROR"]') },
      where: "SeverityText = 'ERROR' AND positionCaseInsensitive(Body, 'status=500') > 0" },
    { name: 'needle', set: async () => { await page.uncheck('#log-sev input[value="ERROR"]'); await page.fill('#log-text', info.truth.needle) },
      where: `positionCaseInsensitive(Body, '${info.truth.needle}') > 0` },
  ]
  measured.complete_window = []
  for (const c of cases) {
    const s = await runView(page, 'logs', { from, to, set: c.set })
    const central = await chLogCount(fromNs, toNs, c.where)
    expect(s.status).toBe('ok')
    expect(s.completeness).toBe('complete')
    await expect(page.locator('#banner')).toHaveAttribute('data-state', 'complete')
    expect(s.count, c.name).toBe(central)
    expect(s.buckets.every(b => b.state === 'complete')).toBe(true)
    measured.complete_window.push({ query: c.name, lake: s.count, central, fetched: s.fetchedBytes, planned: s.plannedBytes })
  }
  await page.fill('#log-text', '')
})

test('a narrow query fetches a fraction of each object (page and tap agree)', async () => {
  await tap('POST')
  const s = await runView(page, 'logs', {
    from: info.truth.at, to: info.truth.late_to,
    set: async () => { await page.check('#log-sev input[value="WARN"]'); await page.selectOption('#log-limit', '0') },
  })
  const fromNs = nsOf(info.truth.at)
  const centralWarn = await chLogCount(fromNs, ct, "SeverityText = 'WARN'")
  expect(s.count).toBeGreaterThanOrEqual(centralWarn) // plus the late batch's WARN rows, which only the lake has
  expect(s.count - centralWarn).toBeLessThanOrEqual(info.truth.late_logs)
  const entries = (await tap()).filter(e => e.method === 'GET')
  const tapBytes = entries.reduce((a, e) => a + e.bytes, 0)
  expect(tapBytes).toBe(s.fetchedBytes) // the page counts what the store sent
  expect(entries.every(e => e.range && e.status === 206)).toBe(true) // range reads only, never a whole object
  expect(entries.some(e => e.method === 'HEAD')).toBe(false)
  const frac = s.fetchedBytes / s.plannedBytes
  expect(frac).toBeLessThan(0.25)
  measured.narrow = { query: 'count of WARN logs, no rows shown', objects: s.objects, requests: s.requests, fetched: s.fetchedBytes, planned: s.plannedBytes,
    fraction: Number(frac.toFixed(4)), perObject: s.perObject }
  await page.uncheck('#log-sev input[value="WARN"]')
  await page.selectOption('#log-limit', '50')
})

test('trace by id equals ClickHouse, across objects', async () => {
  measured.traces = []
  for (const id of info.truth.trace_ids['lui-a'].slice(0, 2)) {
    const s = await runView(page, 'trace', { from: info.truth.at, to: info.truth.late_to, set: async () => page.fill('#trace-id', id) })
    const central = Number(await ch(`SELECT count() FROM ${info.db}.otel_traces WHERE TraceId = '${id}'`))
    expect(s.status).toBe('ok')
    expect(s.count).toBe(central)
    expect(central).toBeGreaterThan(0)
    expect(new Set(s.rows.map(r => r.key)).size).toBeGreaterThan(1)
    await shot(page, 'trace')
    measured.traces.push({ id, lake: s.count, central, objects: s.objects, fetched: s.fetchedBytes, planned: s.plannedBytes })
  }
  // lui-b's trace is outside alice's scope: not found in her plan, and her plan lists only lui-a
  const s = await runView(page, 'trace', { from: info.truth.at, to: info.truth.late_to, set: async () => page.fill('#trace-id', info.truth.trace_ids['lui-b'][0]) })
  expect(s.count).toBe(0)
  expect(s.status).toBe('ok')
})

// The lake index (D27): the rig indexed the three batches before the late
// one. Each query runs without and with the index on the same data; answers
// must be equal (and equal ClickHouse where it has the rows), bytes fewer.
test('the lake index narrows trace-by-id and text search: same answers, fewer bytes', async () => {
  const from = info.truth.at
  const to = info.truth.late_to
  const one = async (view, set) => {
    await tap('POST')
    const s = await runView(page, view, { from, to, set })
    const t = (await tap()).filter(e => e.method === 'GET')
    expect(s.status).toBe('ok')
    expect(t.reduce((a, e) => a + e.bytes, 0)).toBe(s.fetchedBytes) // the page counts what the store sent
    return { count: s.count, objects: s.objects, planned: s.plannedBytes, fetched: s.fetchedBytes, requests: s.requests, index: s.index }
  }
  const queries = [
    ...info.truth.trace_ids['lui-a'].slice(0, 2).map(id => ({ name: `trace ${id.slice(0, 8)}`, view: 'trace', set: () => page.fill('#trace-id', id),
      central: `SELECT count() FROM ${info.db}.otel_traces WHERE TraceId = '${id}'` })),
    { name: `trace ${info.truth.rare_trace_ids['lui-a'][1].slice(0, 8)} (one object)`, view: 'trace', rare: true,
      set: () => page.fill('#trace-id', info.truth.rare_trace_ids['lui-a'][1]),
      central: `SELECT count() FROM ${info.db}.otel_traces WHERE TraceId = '${info.truth.rare_trace_ids['lui-a'][1]}'` },
    { name: `logs "${info.truth.rare_needles[1]}" (one object)`, view: 'logs', rare: true, set: () => page.fill('#log-text', info.truth.rare_needles[1]),
      central: `SELECT count() FROM ${info.db}.otel_logs WHERE ${inCluster('lui-a')} AND positionCaseInsensitive(Body, '${info.truth.rare_needles[1]}') > 0` },
    { name: `logs "${info.truth.needle}"`, view: 'logs', set: () => page.fill('#log-text', info.truth.needle),
      central: `SELECT count() FROM ${info.db}.otel_logs WHERE ${inCluster('lui-a')} AND positionCaseInsensitive(Body, '${info.truth.needle}') > 0` },
    { name: 'logs "status=500"', view: 'logs', set: () => page.fill('#log-text', 'status=500'), central: null },
  ]
  measured.index = { indexer: info.index, queries: [] }
  for (const q of queries) {
    await page.uncheck('#use-index')
    const off = await one(q.view, q.set)
    await page.check('#use-index')
    const on = await one(q.view, q.set)
    expect(on.count, q.name).toBe(off.count)
    expect(off.index).toBe(null)
    expect(on.index).not.toBe(null)
    expect(on.index.errors).toEqual([])
    if (q.central) {
      const central = Number(await ch(q.central))
      // central has none of the late batch; the lake has it (unindexed: "scan")
      if (q.view === 'trace') expect(on.count).toBe(central)
      else expect(on.count).toBeGreaterThanOrEqual(central)
    }
    expect(on.fetched, q.name).toBeLessThanOrEqual(off.fetched)
    if (q.rare) {
      // in one indexed object of three: the others are ruled out, and not read
      expect(on.index.pruned, q.name).toBeGreaterThan(0)
      expect(on.fetched, q.name).toBeLessThan(off.fetched)
    }
    measured.index.queries.push({ query: q.name, count: on.count, off, on })
  }
  // the late batch (logs, lui-a) is not indexed yet: its objects are scanned, never missed
  const needle = measured.index.queries.find(m => m.query.includes(info.truth.needle))
  expect(needle.on.index.scan).toBeGreaterThan(0)
  const r = await fetch(new URL('/rig/index', info.page), { method: 'POST' })
  expect(r.status).toBe(200)
  measured.index.late_pass = await r.json()
  await page.evaluate(() => window.lakeui.planner.clear()) // the cached plan predates the pass
  const after = await one('logs', () => page.fill('#log-text', info.truth.needle))
  expect(after.count).toBe(needle.count)
  expect(after.index.scan).toBe(0)
  expect(after.fetched).toBeLessThanOrEqual(needle.on.fetched)
  measured.index.after_indexing_late = after
  await page.fill('#log-text', '')
})

test('metric chart equals ClickHouse (points and sum)', async () => {
  const fromNs = nsOf(info.truth.at)
  const s = await runView(page, 'metric', { from: info.truth.at, to: info.truth.late_to, set: async () => page.fill('#metric-name', info.truth.metric) })
  const [n, sum] = (await ch(`SELECT count(), sum(Value) FROM ${info.db}.otel_metrics_gauge WHERE MetricName = '${info.truth.metric}' AND ${inCluster('lui-a')} AND TimeUnix >= ${t64(fromNs)} AND TimeUnix < ${t64(nsOf(info.truth.late_to))} FORMAT TSV`)).split('\t').map(Number)
  expect(s.count).toBe(n)
  expect(Math.abs(s.sum - sum)).toBeLessThan(1e-6 * Math.max(1, Math.abs(sum)))
  await expect(page.locator('svg path[data-state="complete"]')).not.toHaveCount(0)
  await shot(page, 'metric')
  measured.metric = { lake: s.count, central: n, lakeSum: s.sum, centralSum: sum, fetched: s.fetchedBytes, planned: s.plannedBytes }
})

test('cross-cluster and namespace-scoped plans show as refused, not empty', async ({ browser }) => {
  const s = await runView(page, 'logs', { from: info.truth.at, to: info.truth.late_to, cluster: 'lui-b' })
  expect(s.status).toBe('refused')
  expect(s.reason).toBe('cluster_not_in_scope')
  await expect(page.locator('#banner')).toHaveAttribute('data-state', 'refused')
  await expect(page.locator('#banner')).toContainText('not an empty result')
  await expect(page.locator('[data-testid="count"]')).toHaveCount(0)
  await shot(page, 'refused')
  const shop = await signedIn(browser, 'shop')
  const s2 = await runView(shop, 'logs', { from: info.truth.at, to: info.truth.late_to })
  expect(s2.status).toBe('refused')
  expect(s2.reason).toBe('namespace_scope_needs_filtering_reader')
  // the fleet sees both clusters
  const sre = await signedIn(browser, 'sre')
  const s3 = await runView(sre, 'logs', { from: info.truth.at, to: info.truth.late_to, cluster: 'lui-b' })
  expect(s3.status).toBe('ok')
  const bCentral = Number(await ch(`SELECT count() FROM ${info.db}.otel_logs WHERE ${inCluster('lui-b')}`))
  expect(s3.count).toBe(bCentral)
  measured.refusals = { cross_cluster: s.reason, namespace: s2.reason, fleet_lui_b: { lake: s3.count, central: bCentral } }
})

// A plan answer with its times replaced (only the times: the ns stay exact
// in the text), as a page whose clock disagrees with the store's would read it.
const retimed = (body, replanAfterMs, expiresAtMs) => body
  .replace(/"expires_at":"[^"]+"/, `"expires_at":"${new Date(expiresAtMs).toISOString()}"`)
  .replace(/"replan_after":"[^"]+"/, `"replan_after":"${new Date(replanAfterMs).toISOString()}"`)
const fulfil = async (r, body) => {
  const resp = await r.fetch() // the real answer's headers (CORS), another body
  const headers = resp.headers()
  delete headers['content-length']
  await r.fulfill({ status: resp.status(), headers, body: body ?? await resp.text() })
}

test('an expired URL (a real 403) re-plans; a stale plan is renewed before reading', async () => {
  const from = info.truth.at
  const to = info.truth.late_to
  // the plan answers of one run: a page that kept them past their expiry reads with their URLs
  const answers = []
  await page.route(u => u.pathname === '/v1/plan', async r => {
    if (r.request().method() !== 'POST') return r.continue()
    const resp = await r.fetch()
    const body = await resp.text()
    answers.push(body)
    const headers = resp.headers()
    delete headers['content-length']
    await r.fulfill({ status: resp.status(), headers, body })
  })
  const first = await runView(page, 'logs', { from, to })
  await page.unrouteAll()
  expect(first.status).toBe('ok')
  expect(answers.length).toBeGreaterThan(0)
  // wait until every URL of those answers has really expired at the store
  const expiresAt = Math.max(...answers.map(b => Date.parse(/"expires_at":"([^"]+)"/.exec(b)[1])))
  const wait = expiresAt - Date.now() + 2000
  await page.waitForTimeout(Math.max(0, wait))
  // a browser whose clock is behind still thinks the first answer valid: it is
  // the next plan's answer (once), its times pushed out; nothing kept from the
  // first run, so every object is read with its expired URL
  let replayed = 0
  await page.route(u => u.pathname === '/v1/plan', async r => {
    if (r.request().method() !== 'POST' || replayed++ > 0) return r.continue()
    await fulfil(r, retimed(answers[0], Date.now() + 3_600_000, Date.now() + 3_600_000))
  })
  await page.evaluate(() => window.lakeui.results.clear())
  await tap('POST')
  const s = await runView(page, 'logs', { from, to })
  await page.unrouteAll()
  const forbidden = s.events.filter(e => e.type === 'read_error' && e.status === 403)
  expect(forbidden.length).toBeGreaterThan(0)
  expect(s.events.some(e => e.type === 'replan' && e.why === 'read_error')).toBe(true)
  expect(s.status).toBe('ok')
  expect(s.replans).toBe(1)
  expect(s.count).toBe(first.count)
  expect(s.tailRows).toBe(first.tailRows)
  expect(s.basis).toBe(first.basis) // the re-plan asks at the basis the stale answer named
  expect(s.requestId).not.toBe(first.requestId)
  const t = await tap()
  expect(t.filter(e => e.status === 403).length).toBe(forbidden.length) // the store really said 403
  measured.expiry = { url_ttl_s: info.url_ttl_s, waited_ms: wait, forbidden: forbidden.length, replans: s.replans, count: s.count, tap403: t.filter(e => e.status === 403).length }
  // a fresh answer that the page reads as past replan_after (the clock caught up): replaced before any read
  replayed = 0
  let staleId = ''
  await page.route(u => u.pathname === '/v1/plan', async r => {
    if (r.request().method() !== 'POST' || replayed++ > 0) return r.continue()
    const resp = await r.fetch()
    const body = await resp.text()
    staleId = /"request_id":"([^"]+)"/.exec(body)[1]
    const headers = resp.headers()
    delete headers['content-length']
    await r.fulfill({ status: resp.status(), headers, body: retimed(body, Date.now() - 1000, Date.now() + 3_600_000) })
  })
  await tap('POST')
  const s2 = await runView(page, 'logs', { from, to })
  await page.unrouteAll()
  expect(s2.status).toBe('ok')
  expect(s2.count).toBe(first.count)
  const firstRead = s2.events.findIndex(e => e.type === 'read')
  const renewed = s2.events.findIndex(e => e.type === 'replan' && e.why === 'replan_after')
  expect(renewed).toBeGreaterThanOrEqual(0)
  expect(firstRead === -1 || renewed < firstRead).toBe(true) // renewed before any read
  expect(s2.requestId).not.toBe(staleId)
  expect((await tap()).filter(e => e.status === 403).length).toBe(0)
})

test('persistent read failures: bounded re-plans, then a visible error naming the objects', async () => {
  const tapHost = new URL(info.tap).host
  let planCalls = 0
  // the first plan is asked for too, and nothing kept answers for an object
  await page.evaluate(() => { window.lakeui.planner.clear(); window.lakeui.results.clear() })
  await page.route(u => u.host === tapHost, r => r.abort('connectionreset'))
  await page.route(u => u.pathname === '/v1/plan', async r => { planCalls++; await r.continue() })
  const s = await runView(page, 'logs', { from: info.truth.at, to: info.truth.late_to })
  await page.unrouteAll()
  expect(s.status).toBe('failed')
  expect(s.replans).toBe(3)
  expect(planCalls).toBe(4)
  expect(s.missing.length).toBe(s.objects)
  expect(s.count).toBe(null)
  await expect(page.locator('#banner')).toHaveAttribute('data-state', 'failed')
  await expect(page.locator('[data-testid="missing"] li')).toHaveCount(s.objects)
  await expect(page.locator('[data-testid="count"]')).toHaveCount(0)
  await shot(page, 'failed')
  measured.failure = { replans: s.replans, planCalls, missing: s.missing.length }
})

test('no watermark: completeness is unknown and nothing is drawn as settled', async () => {
  await fetch(new URL('/rig/watermark?op=drop', info.page), { method: 'POST' })
  try {
    // a plan cached before the drop is still a valid plan with its own label; ask anew
    await page.evaluate(() => window.lakeui.planner.clear())
    const s = await runView(page, 'logs', { from: info.truth.at, to: info.truth.late_to })
    expect(s.status).toBe('ok')
    // no basis can be issued without a watermark: planned unpinned, and said so
    expect(s.atBasis).toBe(false)
    expect(s.unpinned).toBe('basis_unverifiable')
    await expect(page.locator('[data-testid="basis"]')).toContainText('no basis could be issued')
    expect(s.completeness).toBe('unknown')
    expect(s.state).toBe('unknown')
    await expect(page.locator('#banner')).toHaveAttribute('data-state', 'unknown')
    expect(s.buckets.every(b => b.state === 'unknown')).toBe(true)
    expect(s.rows.every(r => r.state === 'unknown')).toBe(true)
    await shot(page, 'unknown')
  } finally {
    await fetch(new URL('/rig/watermark?op=restore', info.page), { method: 'POST' })
  }
})

// Last: /rig/more adds lui-a rows that central never gets, which the tests
// above compare against. At a held basis the basis part is the same objects
// (objects_hash), answered from what was kept, with the same count as
// central; the new rows are the tail, drawn incomplete although their event
// time is long settled.
test('more data after the basis: at a held basis the basis part is unchanged and kept, the tail grows and is drawn incomplete', async () => {
  const from = info.truth.at
  const to = info.truth.late_to
  await page.fill('#log-text', '')
  await page.uncheck('#hold-basis')
  const s0 = await runView(page, 'logs', { from, to })
  expect(s0.status).toBe('ok')
  await page.check('#hold-basis')
  try {
    await tap('POST')
    const s1 = await runView(page, 'logs', { from, to })
    expect(s1.basis).toBe(s0.basis)
    expect(s1.basisHash).toBe(s0.basisHash)
    expect(s1.cachedParts).toBe(s1.basisObjects) // the basis part from what was kept
    const got = new Set((await tap()).filter(e => e.method === 'GET').map(e => e.key.split('/').slice(2).join('/')))
    for (const k of got) expect(s1.tailKeys, `read ${k}`).toContain(k) // only the tail is read again
    expect(s1.count).toBe(s0.count)
    expect(s1.count - s1.tailRows).toBe(centralAll)
    const bucketOf = ns => s1.buckets.find(b => nsOf(b.from) <= ns && ns < nsOf(b.from) + (nsOf(s1.buckets[1].from) - nsOf(s1.buckets[0].from)))

    const more = await (await fetch(new URL('/rig/more', info.page), { method: 'POST' })).json()
    expect(more.logs).toBeGreaterThan(0)
    const moreNs = nsOf(more.from)
    expect(moreNs < settled).toBe(true) // event time long settled
    expect(bucketOf(moreNs).state).toBe('complete')

    const s2 = await runView(page, 'logs', { from, to })
    expect(s2.status).toBe('ok')
    expect(s2.basis).toBe(s0.basis)
    expect(s2.basisHash).toBe(s0.basisHash) // the same basis part: objects_hash
    expect(s2.objectsHash).toBe(s0.objectsHash)
    expect(s2.basisObjects).toBe(s0.basisObjects)
    expect(s2.cachedParts).toBe(s2.basisObjects)
    expect(s2.count - s2.tailRows).toBe(centralAll) // 18,000 before, 18,000 now
    expect(s2.tailRows).toBe(info.truth.late_logs + more.logs)
    expect(s2.count).toBe(centralAll + info.truth.late_logs + more.logs)
    expect(s2.tailObjects).toBeGreaterThan(s1.tailObjects)
    expect(s2.state).toBe('incomplete')
    // the new rows' buckets: settled by the label, incomplete by the tail
    const b = s2.buckets.find(x => x.from === bucketOf(moreNs).from)
    expect(b.state).toBe('incomplete')
    expect(nsOf(b.from) + 1n < settled).toBe(true)
    // central does not have them: they are only in the lake
    expect(await chLogCount(nsOf(from), nsOf(to))).toBe(centralAll)
    await shot(page, 'tail-more')

    measured.tail = { basis_part: s2.count - s2.tailRows, central: centralAll, tail_before: s1.tailRows, more: more.logs, tail_after: s2.tailRows,
      count_after: s2.count, basis_objects: s2.basisObjects, tail_objects: { before: s1.tailObjects, after: s2.tailObjects }, kept_parts: s2.cachedParts,
      objects_hash_same: s2.basisHash === s0.basisHash }
  } finally {
    await page.uncheck('#hold-basis')
  }
})
