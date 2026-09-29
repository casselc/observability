// Journey 4, "Links expire" (docs/journeys/expiry.md): the plan's presigned
// URLs live 60 s on the rig. A page that holds a plan past that gets real
// 403s from the store; it re-plans (at the same basis) and returns the same
// answer. If the store stays unreachable, re-plans are bounded and the page
// names the objects it could not read instead of drawing a result.
import { test, expect } from '@playwright/test'
import { info, journey, runView, signedIn, tap } from './journey.mjs'

// A plan answer with its times replaced (only the times: the ns stay exact
// in the text), as a page whose clock is behind the store's would read it.
const retimed = (body, replanAfterMs, expiresAtMs) => body
  .replace(/"expires_at":"[^"]+"/, `"expires_at":"${new Date(expiresAtMs).toISOString()}"`)
  .replace(/"replan_after":"[^"]+"/, `"replan_after":"${new Date(replanAfterMs).toISOString()}"`)

test('Links expire', {
  tag: ['@AMBIGUITY-X8', '@D24', '@D30', '@R-S2', '@H-2'],
  annotation: [
    { type: 'demonstrates', description: 'AMBIGUITY X8 rules 1-3: a 403 on a planned object means re-plan, never "no data"; a query that could not read every object shows what it lacks' },
    { type: 'demonstrates', description: 'D30: the re-plan asks at the basis the first answer named, so the answer is the same' },
  ],
}, async ({ browser }) => {
  const j = journey('expiry', 'Links expire')
  const from = info.truth.at
  const to = info.truth.late_to
  const page = await signedIn(browser, 'alice')
  const answers = []
  let first

  await test.step('1. a plan, its URLs and the reads', async () => {
    await page.route(u => u.pathname === '/v1/plan', async r => {
      if (r.request().method() !== 'POST') return r.continue()
      const resp = await r.fetch()
      const body = await resp.text()
      answers.push(body)
      const headers = resp.headers()
      delete headers['content-length']
      await r.fulfill({ status: resp.status(), headers, body })
    })
    first = await runView(page, 'logs', { from, to })
    await page.unrouteAll()
    expect(first.status).toBe('ok')
    expect(first.replans).toBe(0)
    expect(answers.length).toBeGreaterThan(0)
    expect(/"expires_at":"[^"]+"/.test(answers[0])).toBe(true)
    await page.locator('#events').evaluate(d => { d.open = true })
    await expect(page.locator('#event-log')).toContainText('plan')
    await j.step(page, 'planned', 'A plan: every object read with a URL that lives 60 s', { region: ['#banner', '#stats', '#events'], maxHeight: 420 })
  })

  await test.step('2. the same plan held past its URLs\' lifetime: 403, re-plan, the same answer', async () => {
    const expiresAt = Math.max(...answers.map(b => Date.parse(/"expires_at":"([^"]+)"/.exec(b)[1])))
    await page.waitForTimeout(Math.max(0, expiresAt - Date.now() + 2000)) // the URLs have really expired at the store
    let replayed = 0
    await page.route(u => u.pathname === '/v1/plan', async r => {
      if (r.request().method() !== 'POST' || replayed++ > 0) return r.continue()
      const resp = await r.fetch()
      const headers = resp.headers()
      delete headers['content-length']
      // the first answer, as a page with a slow clock still holds it
      await r.fulfill({ status: resp.status(), headers, body: retimed(answers[0], Date.now() + 3_600_000, Date.now() + 3_600_000) })
    })
    await page.evaluate(() => window.lakeui.results.clear()) // nothing kept: every object read with its expired URL
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
    expect(s.basis).toBe(first.basis)
    expect(s.requestId).not.toBe(first.requestId)
    expect((await tap()).filter(e => e.status === 403).length).toBe(forbidden.length) // the store really said 403
    await page.locator('#events').evaluate(d => { d.open = true })
    await expect(page.locator('#event-log')).toContainText('read_error')
    await expect(page.locator('#event-log')).toContainText('replan')
    await j.step(page, 'replanned', 'Held past expiry: the store answers 403, the page re-plans, same count', {
      region: ['#banner', '#stats', '#events'], maxHeight: 420, marks: ['[data-testid="count"]'] })
  })

  await test.step('3. the store stays unreachable: bounded re-plans, then the objects it lacks, no result', async () => {
    const tapHost = new URL(info.tap).host
    let planCalls = 0
    await page.evaluate(() => { window.lakeui.planner.clear(); window.lakeui.results.clear() })
    await page.route(u => u.host === tapHost, r => r.abort('connectionreset'))
    await page.route(u => u.pathname === '/v1/plan', async r => { planCalls++; await r.continue() })
    const s = await runView(page, 'logs', { from, to })
    await page.unrouteAll()
    expect(s.status).toBe('failed')
    expect(s.replans).toBe(3)
    expect(planCalls).toBe(4)
    expect(s.missing.length).toBe(s.objects)
    expect(s.count).toBe(null)
    await expect(page.locator('#banner')).toHaveAttribute('data-state', 'failed')
    await expect(page.locator('[data-testid="missing"] li')).toHaveCount(s.objects)
    await expect(page.locator('[data-testid="count"]')).toHaveCount(0)
    await page.locator('#events').evaluate(d => { d.open = false })
    await j.step(page, 'failed', 'The store stays unreachable: 3 re-plans, then "not read", never a number', { region: ['#banner', '#stats'] })
  })
  await page.context().close()
})
