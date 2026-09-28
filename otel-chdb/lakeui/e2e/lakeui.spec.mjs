// The lake UI in Chromium against the real pieces: the Go edge publishes,
// the Rust consumer ingests into ClickHouse and publishes the watermark, the
// query service plans (query/integration/lakeuirig brings them up), and the
// page signs in through an authorization-code + PKCE flow on the query
// service's own test issuer.
//
// Checked: results equal ClickHouse's count for the same scope and window;
// rows past complete_through are drawn incomplete; a cross-cluster or
// namespace-scoped plan shows as refused, not empty; an expired URL (a real
// 403 from SeaweedFS) re-plans; persistent failures end as a visible error
// naming the objects; an unknown watermark shows unknown; a narrow query
// fetches a fraction of each object (measured by the page and, separately,
// by a counting pass-through in front of SeaweedFS).
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
  // the late rows are the incomplete ones
  await expect(page.locator('#banner')).toHaveAttribute('data-state', 'incomplete')
  expect(s.rows.length).toBe(50)
  for (const r of s.rows) expect(r.state).toBe(nsOf(r.ts) >= settled ? 'incomplete' : 'complete')
  expect(s.rows.every(r => r.state === 'incomplete')).toBe(true) // the newest 50 are late
  const inc = s.buckets.filter(b => b.state === 'incomplete')
  expect(inc.length).toBeGreaterThan(0)
  expect(s.buckets.filter(b => b.state === 'complete').every(b => nsOf(b.from) < settled)).toBe(true)
  await expect(page.locator('svg [data-marker="complete-through"]')).toHaveCount(1)
  await shot(page, 'logs-partial')
  measured.full_window = { lake: s.count, central, late: info.truth.late_logs, objects: s.objects, fetched: s.fetchedBytes, planned: s.plannedBytes }
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

test('an expired URL (a real 403) re-plans; a stale plan is renewed before reading', async () => {
  const from = info.truth.at
  const to = info.truth.late_to
  const first = await runView(page, 'logs', { from, to })
  expect(first.status).toBe('ok')
  // wait until every URL of the cached plan has really expired at the store
  const plans = await page.evaluate(() => window.lakeui.planner.cached().map(p => p.expiresAtMs))
  const wait = Math.max(...plans) - Date.now() + 2000
  await page.waitForTimeout(Math.max(0, wait))
  // a browser whose clock is behind still thinks the plan is valid: model it
  await page.evaluate(() => { for (const p of window.lakeui.planner.cached()) p.replanAfterMs = Date.now() + 3_600_000 })
  await tap('POST')
  const s = await runView(page, 'logs', { from, to })
  const forbidden = s.events.filter(e => e.type === 'read_error' && e.status === 403)
  expect(forbidden.length).toBeGreaterThan(0)
  expect(s.events.some(e => e.type === 'replan' && e.why === 'read_error')).toBe(true)
  expect(s.status).toBe('ok')
  expect(s.replans).toBe(1)
  expect(s.count).toBe(first.count)
  expect(s.requestId).not.toBe(first.requestId)
  const t = await tap()
  expect(t.filter(e => e.status === 403).length).toBe(forbidden.length) // the store really said 403
  measured.expiry = { url_ttl_s: info.url_ttl_s, waited_ms: wait, forbidden: forbidden.length, replans: s.replans, count: s.count, tap403: t.filter(e => e.status === 403).length }
  // the plan is now fresh; mark it past replan_after (the clock caught up): it is replaced before any read
  await page.evaluate(() => { for (const p of window.lakeui.planner.cached()) p.replanAfterMs = Date.now() - 1 })
  await tap('POST')
  const s2 = await runView(page, 'logs', { from, to })
  expect(s2.status).toBe('ok')
  expect(s2.requestId).not.toBe(s.requestId)
  expect((await tap()).filter(e => e.status === 403).length).toBe(0)
})

test('persistent read failures: bounded re-plans, then a visible error naming the objects', async () => {
  const tapHost = new URL(info.tap).host
  let planCalls = 0
  await page.evaluate(() => window.lakeui.planner.clear()) // the first plan is asked for too
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
