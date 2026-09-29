import { test } from 'node:test'
import assert from 'node:assert/strict'
import { covers } from './pw-trace.js'
import { outcomeOf, recordsFor } from './pw-trace-reporter.js'

// The Playwright twin of trace.js: a tag on a running test becomes a record
// with the outcome Playwright reports, and only through the reporter.
test('pw-trace: a tagged Playwright test becomes one record per tag, with its outcome', () => {
  const info = { annotations: [] }
  covers(info, 'IT', 'CAST-33', 'H-5')
  const tc = { id: 'x', title: 'stale cubes', location: { file: '/w/otel-chdb/lakeui/mosaic/e2e/mosaic.spec.mjs' }, annotations: [] }
  const env = { GITHUB_SHA: 'abc', GITHUB_JOB: 'lakeui-mosaic-e2e' }
  const [r, ...rest] = recordsFor(tc, { status: 'passed', annotations: info.annotations }, env)
  assert.equal(rest.length, 0)
  assert.deepEqual(r, {
    ids: ['CAST-33', 'H-5'], technique: 'IT', test: 'stale cubes', func: 'stale cubes',
    package: 'otel-chdb/lakeui/mosaic/e2e', file: 'otel-chdb/lakeui/mosaic/e2e/mosaic.spec.mjs',
    lang: 'playwright', commit: 'abc', job: 'lakeui-mosaic-e2e', outcome: 'passed'
  })
  // the same annotation on the test case and the result is one record
  assert.equal(recordsFor({ ...tc, annotations: info.annotations }, { status: 'passed', annotations: info.annotations }, env).length, 1)
  for (const [s, o] of [['passed', 'passed'], ['skipped', 'skipped'], ['failed', 'failed'], ['timedOut', 'failed'], ['interrupted', 'failed']]) {
    assert.equal(outcomeOf(s), o)
  }
  // an untagged test writes nothing
  assert.deepEqual(recordsFor({ ...tc, annotations: [] }, { status: 'passed', annotations: [] }, env), [])
})
