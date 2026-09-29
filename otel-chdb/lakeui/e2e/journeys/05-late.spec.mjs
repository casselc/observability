// Journey 5, "Late data" (docs/journeys/late.md): Alice holds her basis so a
// re-run gives her the same answer. Rows stamped two minutes into the window
// (long settled by event time) arrive now. At the held basis the basis part
// is unchanged (same objects, answered from what was kept, equal to
// ClickHouse), the late rows are the tail, and the old bucket they fall in
// turns incomplete; zoomed in, the late rows are the ones tagged
// incomplete. Runs last: it adds data the other journeys compare against.
import { test, expect } from '@playwright/test'
import { chLogCount, info, isoOf, journey, nsOf, rig, runView, signedIn, tap } from './journey.mjs'

test('Late data', {
  tag: ['@D26', '@D30', '@D31', '@R-S1', '@R-S2', '@H-5', '@CAST-26', '@AMBIGUITY-10b'],
  annotation: [
    { type: 'demonstrates', description: 'D30: at a held basis the basis part is the same objects (objects_hash) and the same rows however much arrives' },
    { type: 'demonstrates', description: 'AMBIGUITY #10 (b): rows received after the basis are read as the tail and drawn incomplete whatever their event time' },
    { type: 'demonstrates', description: 'D26 / CAST row 26: event time and receive time are two clocks; a settled event time does not make a late row complete' },
  ],
}, async ({ browser }) => {
  const j = journey('late', 'Late data')
  const from = info.truth.at
  const to = info.truth.late_to
  const page = await signedIn(browser, 'alice')
  const lateAt = nsOf(from) + 120_000_000_000n // /rig/more's rows: two minutes into the window
  const bucketIndex = (s, ns) => {
    const w = nsOf(s.buckets[1].from) - nsOf(s.buckets[0].from)
    return s.buckets.findIndex(b => nsOf(b.from) <= ns && ns < nsOf(b.from) + w)
  }
  const markBucket = async i => page.evaluate(k => {
    for (const e of document.querySelectorAll('[data-jny-bucket]')) delete e.dataset.jnyBucket
    document.querySelectorAll('#chart g.bar')[k].dataset.jnyBucket = '1'
  }, i)
  const central = await chLogCount('lui-a', nsOf(from), nsOf(to))
  let s0
  let s1
  let more
  let s2

  await test.step('1. a held basis: the basis part answered from what was kept', async () => {
    s0 = await runView(page, 'logs', { from, to })
    expect(s0.status).toBe('ok')
    await page.check('#hold-basis')
    await tap('POST')
    s1 = await runView(page, 'logs', { from, to })
    expect(s1.basis).toBe(s0.basis)
    expect(s1.basisHash).toBe(s0.basisHash)
    expect(s1.cachedParts).toBe(s1.basisObjects)
    const read = new Set((await tap()).filter(e => e.method === 'GET').map(e => e.key.split('/').slice(2).join('/')))
    for (const k of read) expect(s1.tailKeys, `read ${k}`).toContain(k) // only the tail is read again
    expect(s1.count - s1.tailRows).toBe(central)
    const i = bucketIndex(s1, lateAt)
    expect(s1.buckets[i].state).toBe('complete') // settled long ago
    await markBucket(i)
    await expect(page.locator('[data-testid="basis"]')).toContainText('answered from the cache')
    await j.step(page, 'held', 'Hold the basis: the old bucket is settled; the basis part comes from what was kept', {
      region: ['#scope', '#banner', '#stats', '#chart'], marks: ['[data-jny-bucket]', '#hold-basis'] })
  })

  await test.step('2. rows stamped 18 minutes ago arrive now: the old bucket turns incomplete', async () => {
    more = await (await rig('/rig/more')).json()
    expect(more.logs).toBeGreaterThan(0)
    expect(nsOf(more.from) >= lateAt).toBe(true)
    expect(nsOf(more.from) < nsOf(s1.settledThrough)).toBe(true) // event time long settled
    s2 = await runView(page, 'logs', { from, to })
    expect(s2.status).toBe('ok')
    expect(s2.basis).toBe(s0.basis)
    expect(s2.basisHash).toBe(s0.basisHash) // the same basis part: objects_hash
    expect(s2.cachedParts).toBe(s2.basisObjects)
    expect(s2.count - s2.tailRows).toBe(central) // unchanged
    expect(s2.tailRows).toBe(info.truth.late_logs + more.logs) // the tail grew
    expect(s2.tailObjects).toBeGreaterThan(s1.tailObjects)
    const i = bucketIndex(s2, nsOf(more.from))
    expect(s2.buckets[i].state).toBe('incomplete') // settled by event time, incomplete by the tail
    expect(await chLogCount('lui-a', nsOf(from), nsOf(to))).toBe(central) // central has not got them
    await expect(page.locator('#banner')).toContainText(`${info.truth.late_logs + more.logs} row(s) received after the basis`)
    await markBucket(i)
    await j.step(page, 'arrived', 'Late rows arrive: the basis part is unchanged, the tail grows, the old bucket turns incomplete', {
      region: ['#scope', '#banner', '#stats', '#chart'], marks: ['[data-jny-bucket]'] })
  })

  await test.step('3. zoomed in: the late rows are the ones tagged incomplete', async () => {
    const zFrom = more.from
    const zTo = isoOf(nsOf(more.to) + 1_000_000_000n)
    await page.selectOption('#log-limit', '200')
    const z = await runView(page, 'logs', { from: zFrom, to: zTo })
    expect(z.status).toBe('ok')
    expect(z.basis).toBe(s0.basis)
    expect(z.tailRows).toBe(more.logs)
    expect(z.count - z.tailRows).toBe(await chLogCount('lui-a', nsOf(zFrom), nsOf(zTo)))
    expect(z.rows.some(r => r.tail)).toBe(true)
    expect(z.rows.some(r => !r.tail)).toBe(true)
    for (const r of z.rows) expect(r.state).toBe(r.tail ? 'incomplete' : 'complete')
    await page.evaluate(() => {
      // bring the first late row near the top of the table picture
      const rows = [...document.querySelectorAll('#table tr[data-state]')]
      const k = rows.findIndex(r => r.dataset.state === 'incomplete')
      rows.slice(0, Math.max(0, k - 3)).forEach(r => { r.hidden = true })
    })
    await j.step(page, 'zoom', 'Zoomed in: rows of the basis are complete, the late rows beside them are not', {
      region: ['#table'], maxHeight: 360, marks: ['#table tr[data-state="incomplete"]'] })
    await page.selectOption('#log-limit', '50')
    await page.uncheck('#hold-basis')
  })
  await page.context().close()
})
