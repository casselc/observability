// Journey 3, "Not your cluster" (docs/journeys/scope.md): a cluster-scoped
// user asks for another cluster and gets a refusal naming the reason, never
// an empty result; a namespace-scoped user is refused the raw objects; the
// fleet user is answered, equal to ClickHouse.
import { test, expect } from '@playwright/test'
import { ch, inCluster, info, journey, runView, signedIn } from './journey.mjs'

test('Not your cluster', {
  tag: ['@R-S8', '@H-6', '@H-2', '@D22', '@D38'],
  annotation: [
    { type: 'demonstrates', description: 'STPA R-S8: reads are role-scoped by cluster and namespace' },
    { type: 'demonstrates', description: 'D22: a named cluster outside the scope is refused (403 cluster_not_in_scope), never narrowed to nothing' },
    { type: 'demonstrates', description: 'H-2: a refusal is never drawn as an empty (complete) result' },
  ],
}, async ({ browser }) => {
  const j = journey('scope', 'Not your cluster')
  const from = info.truth.at
  const to = info.truth.late_to

  await test.step('1. alice (lui-a) asks for lui-b: refused, with the reason', async () => {
    const page = await signedIn(browser, 'alice')
    const s = await runView(page, 'logs', { from, to, cluster: 'lui-b' })
    expect(s.status).toBe('refused')
    expect(s.reason).toBe('cluster_not_in_scope')
    await expect(page.locator('#banner')).toHaveAttribute('data-state', 'refused')
    await expect(page.locator('#banner')).toContainText('not an empty result')
    await expect(page.locator('[data-testid="reason"]')).toHaveText('cluster_not_in_scope')
    await expect(page.locator('[data-testid="count"]')).toHaveCount(0)
    await expect(page.locator('#chart svg')).toHaveCount(0)
    await j.step(page, 'refused', 'alice (cluster lui-a) asks for lui-b: refused, with the reason', {
      region: ['header', '#scope', '#banner'], marks: ['#cluster', '[data-testid="reason"]'] })
    await page.context().close()
  })

  await test.step('2. a namespace-scoped user: refused the raw objects', async () => {
    const page = await signedIn(browser, 'shop')
    await expect(page.locator('#who')).toContainText('namespaces shop')
    const s = await runView(page, 'logs', { from, to })
    expect(s.status).toBe('refused')
    expect(s.reason).toBe('namespace_scope_needs_filtering_reader')
    await expect(page.locator('[data-testid="count"]')).toHaveCount(0)
    await j.step(page, 'namespace', 'shop (namespace shop only): raw objects hold every namespace, so refused', {
      region: ['header', '#scope', '#banner'], marks: ['[data-testid="reason"]'] })
    await page.context().close()
  })

  await test.step('3. the fleet user is answered, equal to ClickHouse', async () => {
    const page = await signedIn(browser, 'sre')
    const s = await runView(page, 'logs', { from, to, cluster: 'lui-b' })
    const central = Number(await ch(`SELECT count() FROM ${info.db}.otel_logs WHERE ${inCluster('lui-b')}`))
    expect(s.status).toBe('ok')
    expect(s.count).toBe(central)
    await expect(page.locator('[data-testid="count"]')).toHaveText(String(central))
    await j.step(page, 'fleet', 'sre (fleet, by group) asks for lui-b: answered, equal to ClickHouse', {
      region: ['header', '#scope', '#banner', '#stats'], marks: ['[data-testid="count"]'] })
    await page.context().close()
  })
})
