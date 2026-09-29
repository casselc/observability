// The user journeys (../../../docs/journeys/README.md): `npm run journeys`
// runs them as tests; `e2e/journeys/render.sh` runs them and refreshes the
// pictures. One rig for all of them (rig.mjs), started once; the files run
// in order in one worker, and 05-late adds data, so it is last.
import { defineConfig } from '@playwright/test'

export default defineConfig({
  testDir: '.',
  testMatch: /\d\d-.*\.spec\.mjs$/,
  globalSetup: './rig.mjs',
  fullyParallel: false,
  workers: 1,
  retries: 0,
  timeout: 420_000,
  reporter: [['list'], ['json', { outputFile: '../../test-results/journeys.json' }]],
  use: {
    browserName: 'chromium',
    headless: true,
    trace: 'retain-on-failure',
    // deterministic pictures: a fixed viewport at scale 1, no motion, UTC
    viewport: { width: 1200, height: 820 },
    deviceScaleFactor: 1,
    colorScheme: 'light',
    reducedMotion: 'reduce',
    timezoneId: 'UTC',
    locale: 'en-US',
  },
  outputDir: '../../test-results/journeys-output',
})
