// Journey 2, "Find a trace" (docs/journeys/trace.md): a trace id that lives
// in one object of the window's three. Without the lake index every object's
// TraceId column is read; with it (D27) the service rules the other objects
// out, the answer is the same and fewer bytes cross the wire. A text search
// the index has not caught up with reads the unindexed object whole, never
// skips it; after an indexer pass nothing is left unindexed.
import { test, expect } from '@playwright/test'
import { ch, inCluster, info, journey, rig, runView, signedIn, tap } from './journey.mjs'

test('Find a trace', {
  tag: ['@D27', '@R-S1', '@H-2', '@FORMAT-7'],
  annotation: [
    { type: 'demonstrates', description: 'D27: the lake index removes objects from the plan, never rows from the answer' },
    { type: 'demonstrates', description: 'FORMAT.md §7.3: an object the index has not covered is read ("scan"), not missed' },
  ],
}, async ({ browser }) => {
  const j = journey('trace', 'Find a trace')
  const from = info.truth.at
  const to = info.truth.late_to
  const traceId = info.truth.rare_trace_ids['lui-a'][1]
  const needle = info.truth.rare_needles[1]
  const page = await signedIn(browser, 'alice')

  // one run, with the pass-through counting what the store sent
  const one = async (view, set) => {
    await tap('POST')
    const s = await runView(page, view, { from, to, set })
    const got = (await tap()).filter(e => e.method === 'GET')
    expect(s.status).toBe('ok')
    expect(got.reduce((a, e) => a + e.bytes, 0)).toBe(s.fetchedBytes) // the page counts what the store sent
    expect(got.every(e => e.status === 206)).toBe(true) // range reads only
    return s
  }
  const central = Number(await ch(`SELECT count() FROM ${info.db}.otel_traces WHERE TraceId = '${traceId}'`))
  expect(central).toBeGreaterThan(0)
  let off
  let on

  await test.step('1. without the index: every object of the window is read', async () => {
    await page.uncheck('#use-index')
    off = await one('trace', () => page.fill('#trace-id', traceId))
    expect(off.count).toBe(central)
    expect(off.index).toBe(null)
    expect(off.objects).toBeGreaterThanOrEqual(3)
    await expect(page.locator('#banner')).toContainText(`trace ${traceId}`)
    await j.step(page, 'no-index', 'Without the index: every object in the window is read', { region: ['#scope', '#view-trace', '#banner', '#stats', '#chart'] })
  })

  await test.step('2. with the index: the other objects are ruled out, same spans, fewer bytes', async () => {
    await page.check('#use-index')
    on = await one('trace', () => page.fill('#trace-id', traceId))
    expect(on.count).toBe(central)
    expect(on.index.errors).toEqual([])
    expect(on.index.pruned).toBeGreaterThan(0)
    expect(on.fetchedBytes).toBeLessThan(off.fetchedBytes)
    expect(on.requests).toBeLessThan(off.requests)
    await expect(page.locator('#stats')).toContainText('ruled out')
    await j.step(page, 'index', 'With the lake index: the same spans, the other objects ruled out', {
      region: ['#scope', '#view-trace', '#banner', '#stats', '#chart'], marks: ['#stats'] })
  })

  let before
  await test.step('3. a word the index has not caught up with: the new object is read, not missed', async () => {
    await page.evaluate(() => window.lakeui.selectView('logs'))
    before = await one('logs', () => page.fill('#log-text', needle))
    const c = Number(await ch(`SELECT count() FROM ${info.db}.otel_logs WHERE ${inCluster('lui-a')} AND positionCaseInsensitive(Body, '${needle}') > 0`))
    expect(c).toBeGreaterThan(0)
    expect(before.count).toBe(c) // the word lives in one indexed object only
    expect(before.index.scan).toBeGreaterThan(0) // the late object: not indexed yet, read whole
    expect(before.index.pruned).toBeGreaterThan(0)
    await expect(page.locator('#stats')).toContainText('not indexed (read whole)')
    await j.step(page, 'scan', 'A text search: one object not indexed yet, so it is read whole, not skipped', {
      region: ['#view-logs', '#banner', '#stats'], marks: ['#stats'] })
  })

  await test.step('4. after an indexer pass: nothing unindexed, the same answer', async () => {
    const r = await rig('/rig/index')
    expect(r.status).toBe(200)
    await page.evaluate(() => window.lakeui.planner.clear()) // a cached plan predates the pass
    const after = await one('logs', () => page.fill('#log-text', needle))
    expect(after.count).toBe(before.count)
    expect(after.index.scan).toBe(0)
    expect(after.fetchedBytes).toBeLessThanOrEqual(before.fetchedBytes)
    await j.step(page, 'indexed', 'After the indexer runs: every object indexed, the same count', { region: ['#view-logs', '#banner', '#stats'], marks: ['#stats'] })
    await page.fill('#log-text', '')
  })
  await page.context().close()
})
