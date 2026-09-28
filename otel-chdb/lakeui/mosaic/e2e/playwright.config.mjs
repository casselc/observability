// The Mosaic spike's browser test: `npm run e2e` (needs `npm run vendor`,
// QS_IT_BIN, ClickHouse and SeaweedFS, as lakeui's e2e does).
import { defineConfig } from '@playwright/test'

export default defineConfig({
  testDir: '.',
  timeout: 600_000,
  workers: 1,
  reporter: [['list']],
  use: { browserName: 'chromium', headless: true, viewport: { width: 1280, height: 1000 } },
  outputDir: '../test-results',
})
