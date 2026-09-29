// Journey 1, "Is this data complete?" (docs/journeys/complete.md): Alice
// searches cluster lui-a's logs over a window that runs to now. The banner
// says how far the answer is settled, the settled buckets are solid, the
// tail (received after the basis) is hatched; a window closed at
// settled-through is complete and equals ClickHouse; without a watermark
// everything is unknown.
import { test, expect } from '@playwright/test'
import { chLogCount, info, isoOf, journey, nsOf, rig, runView, signedIn } from './journey.mjs'

test('Is this data complete?', {
  tag: ['@R-S1', '@R-S2', '@H-2', '@H-5', '@D24', '@D26', '@D29', '@D30', '@AMBIGUITY-10b'],
  annotation: [
    { type: 'demonstrates', description: 'STPA R-S1: every result carries its source and complete-through, and the event time that settles (D26)' },
    { type: 'demonstrates', description: 'STPA R-S2: what is not settled is drawn incomplete and the count is marked partial' },
    { type: 'demonstrates', description: 'D30 + AMBIGUITY #10 (b): one basis per run; the tail received after it is read and drawn incomplete' },
  ],
}, async ({ browser }) => {
  const j = journey('complete', 'Is this data complete?')
  const from = info.truth.at
  const to = info.truth.late_to
  let page
  let s

  await test.step('1. signed in with code + PKCE', async () => {
    page = await signedIn(browser, 'alice')
    await expect(page.locator('#who')).toContainText('alice (clusters lui-a)')
    await expect(page.locator('#who')).toContainText('token valid')
    await j.step(page, 'signed-in', 'Signed in: the token names cluster lui-a', { region: ['header'] })
  })

  await test.step('2. a window that runs to now: settled buckets and the hatched tail', async () => {
    s = await runView(page, 'logs', { from, to })
    expect(s.status).toBe('ok')
    expect(s.completeness).toBe('partial')
    const ct = nsOf(s.completeThrough)
    const settled = nsOf(s.settledThrough)
    expect(settled < ct).toBe(true) // max_lateness > 0 (D26)
    const central = await chLogCount('lui-a', nsOf(from), nsOf(to))
    expect(s.atBasis).toBe(true)
    expect(s.tailRows).toBe(info.truth.late_logs) // the late batch is the tail
    expect(s.count - s.tailRows).toBe(central) // the basis part is what central holds
    expect(s.afterBasis).toBe(0) // nothing received after the basis left out
    await expect(page.locator('#banner')).toHaveAttribute('data-state', 'incomplete')
    await expect(page.locator('#banner')).toContainText(`${info.truth.late_logs} row(s) received after the basis`)
    await expect(page.locator('[data-testid="basis"]')).toContainText('the tail')
    expect(s.buckets.filter(b => b.state === 'complete').every(b => nsOf(b.from) < settled)).toBe(true)
    expect(s.buckets.some(b => b.state === 'incomplete')).toBe(true)
    await expect(page.locator('svg [data-marker="complete-through"]')).toHaveCount(1)
    await j.step(page, 'partial', 'A window to now: settled buckets solid, the tail hatched', {
      region: ['#banner', '#stats', '#chart'], marks: ['svg [data-marker="complete-through"]'] })
  })

  await test.step('3. the newest rows are the tail, each tagged incomplete', async () => {
    expect(s.rows.length).toBe(50)
    expect(s.rows.every(r => r.tail && r.state === 'incomplete')).toBe(true)
    await expect(page.locator('#table tr[data-state="incomplete"]')).toHaveCount(50)
    await j.step(page, 'rows', 'The newest rows arrived after the basis: each says incomplete', { region: ['#table'], maxHeight: 300 })
  })

  await test.step('4. a window closed at settled-through is complete and equals ClickHouse', async () => {
    const settled = nsOf(s.settledThrough)
    const c = await runView(page, 'logs', { from, to: isoOf(settled) })
    const central = await chLogCount('lui-a', nsOf(from), settled)
    expect(c.status).toBe('ok')
    expect(c.completeness).toBe('complete')
    expect(c.count).toBe(central)
    expect(c.buckets.every(b => b.state === 'complete')).toBe(true)
    await expect(page.locator('#banner')).toHaveAttribute('data-state', 'complete')
    await j.step(page, 'complete', 'Close the window at settled-through: complete, equal to ClickHouse', { region: ['#banner', '#stats', '#chart'] })
  })

  await test.step('5. no watermark: completeness unknown, nothing drawn as settled', async () => {
    await rig('/rig/watermark?op=drop')
    try {
      await page.evaluate(() => window.lakeui.planner.clear())
      const u = await runView(page, 'logs', { from, to })
      expect(u.status).toBe('ok')
      expect(u.atBasis).toBe(false)
      expect(u.unpinned).toBe('basis_unverifiable')
      expect(u.completeness).toBe('unknown')
      expect(u.buckets.every(b => b.state === 'unknown')).toBe(true)
      expect(u.rows.every(r => r.state === 'unknown')).toBe(true)
      await expect(page.locator('#banner')).toHaveAttribute('data-state', 'unknown')
      await expect(page.locator('[data-testid="basis"]')).toContainText('no basis could be issued')
      await j.step(page, 'unknown', 'No watermark: the service cannot tell, so nothing is drawn as settled', { region: ['#banner', '#stats', '#chart'] })
    } finally {
      await rig('/rig/watermark?op=restore')
    }
  })
  await page.context().close()
})
