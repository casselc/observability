// Journey 6 (a SPIKE, D28 proposed, not adopted), "Cross-filter, with the
// completeness marks" (docs/journeys/mosaic.md): the Mosaic page
// (../../mosaic, research/mosaic.md) loads logs and spans through lakeui's
// range reader into DuckDB-WASM and draws four cross-filtered charts with
// the same completeness layer. Brushing a time range and clicking a pod
// filter the others; every chart's total equals an independent SQL count
// under its filter (pre-aggregation off, so the counts are exact).
// Needs the spike's vendor/ (`cd lakeui/mosaic && npm ci && npm run vendor`);
// JOURNEYS_MOSAIC=0 skips it. Runs after 05-late, so the tail holds the
// late batch and the rows /rig/more added.
import { test, expect } from '@playwright/test'
import { ch, info, journey, nsOf, signedIn } from './journey.mjs'

const chCount = async (table, fromNs, toNs) => Number(await ch(`SELECT count() FROM ${info.db}.${table} WHERE ResourceAttributes['k8s.cluster.name'] = 'lui-a' AND Timestamp >= fromUnixTimestamp64Nano(toInt64(${fromNs})) AND Timestamp < fromUnixTimestamp64Nano(toInt64(${toNs}))`))

test('Cross-filter, with the completeness marks (Mosaic spike)', {
  tag: ['@D28', '@spike', '@R-S1', '@R-S2', '@H-2', '@AMBIGUITY-10b'],
  annotation: [
    { type: 'demonstrates', description: 'D28 (proposed): Mosaic charts fed by the range reader keep lakeui\'s completeness states on every mark' },
    { type: 'demonstrates', description: 'after every interaction each chart\'s total equals an independent count (the check CAST row 33 introduced; its stale-cube regression is lakeui-mosaic-e2e)' },
  ],
}, async ({ browser }) => {
  test.skip(process.env.JOURNEYS_MOSAIC === '0', 'JOURNEYS_MOSAIC=0')
  const vendored = await fetch(new URL('mosaic/vendor/mosaic.js', info.page))
  expect(vendored.status, 'the Mosaic spike is not vendored: cd lakeui/mosaic && npm ci && npm run vendor (or JOURNEYS_MOSAIC=0)').toBe(200)
  const j = journey('mosaic', 'Cross-filter (Mosaic spike)')
  const page = await signedIn(browser, 'alice', 'mosaic/index.html?mode=range&preagg=0')
  const checkAll = () => page.evaluate(async () => Promise.all(['A', 'B', 'C', 'D'].map(id => window.mos.check(id))))
  const consistent = async what => {
    for (const c of await checkAll()) expect(c.shown, `${what}: chart ${c.id} (${c.where})`).toBe(c.expected)
  }

  await test.step('1. load: four charts, the tail hatched on each', async () => {
    await page.fill('#from', info.truth.at)
    await page.fill('#to', info.truth.late_to)
    await page.selectOption('#cluster', '')
    const rec = await page.evaluate(() => window.mos.load())
    expect(rec.status).toBe('ok')
    const from = nsOf(info.truth.at)
    const to = nsOf(info.truth.late_to)
    expect(rec.logs.basis).toMatch(/^b1\./)
    expect(rec.logs.rows - rec.logs.tailRows).toBe(await chCount('otel_logs', from, to)) // the basis part: central's
    expect(rec.tables.logs.n).toBe(rec.logs.rows)
    expect(rec.tables.logs.inc).toBe(rec.logs.tailRows) // the tail, and only it, incomplete
    await expect(page.locator('#chart-A [fill="url(#mos-band)"]')).toHaveCount(1)
    await expect(page.locator('#chart-A text', { hasText: 'settled through' })).toHaveCount(1)
    expect(await page.locator('#chart-B [fill="url(#mos-hatch)"]').count()).toBeGreaterThan(0)
    await consistent('loaded')
    await j.step(page, 'loaded', 'Four cross-filtered charts; the tail hatched on each', { region: ['#banner', '#stats', '#charts'], maxHeight: 700 })
  })

  await test.step('2. brush a time range: every chart filters, every total exact', async () => {
    const f = await page.evaluate(() => window.mos.frame('A'))
    const x = r => f.left + r * (f.right - f.left)
    const y = f.top + 0.5 * (f.bottom - f.top)
    await page.mouse.move(x(0.2), y)
    await page.mouse.down()
    for (let i = 1; i <= 6; i++) await page.mouse.move(x(0.2 + (0.35 * i) / 6), y)
    await page.mouse.up()
    await page.evaluate(() => window.mos.idle())
    const cs = await checkAll()
    expect(cs.find(c => c.id === 'B').where).not.toBe('') // B is filtered by the brush
    await consistent('brushed time')
    await j.step(page, 'brushed', 'Brush a time range: the other charts follow, each total exact', { region: ['#charts'], maxHeight: 700 })
  })

  await test.step('3. click a pod: spans filtered by time and pod, still exact', async () => {
    const fb = await page.evaluate(() => window.mos.frame('B'))
    await page.mouse.click(fb.left + 20, fb.top + (fb.bottom - fb.top) * 0.25)
    await page.evaluate(() => window.mos.idle())
    const d = (await checkAll()).find(c => c.id === 'D')
    expect(d.where).toMatch(/pod/)
    await consistent('clicked a pod')
    await j.step(page, 'pod', 'Click a pod: spans filtered by time and pod, still exact', { region: ['#charts'], maxHeight: 700 })
  })
  await page.context().close()
})
