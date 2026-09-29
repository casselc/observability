// The lake UI's browser test: `npm run e2e` (needs QS_IT_BIN, ClickHouse and
// SeaweedFS as the query service's integration test does; see README.md).
import { defineConfig } from '@playwright/test'

export default defineConfig({
  testDir: '.',
  testMatch: 'lakeui.spec.mjs', // journeys/ has its own config (one rig for all journeys)
  timeout: 300_000,
  workers: 1,
  reporter: [['list']],
  use: { browserName: 'chromium', headless: true, trace: 'retain-on-failure' },
  outputDir: '../test-results',
})
