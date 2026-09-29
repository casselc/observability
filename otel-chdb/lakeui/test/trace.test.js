import { test } from 'node:test'
import assert from 'node:assert/strict'
import { spawnSync } from 'node:child_process'
import { mkdtempSync, readFileSync, existsSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { outcomeOf, repoPath } from './trace-reporter.js'

// A child `node --test` that inherits NODE_TEST_CONTEXT takes itself for one of this run's workers.
const childEnv = (over) => {
  const env = { ...process.env, ...over }
  delete env.NODE_TEST_CONTEXT
  return env
}

test('trace: a child run records each tagged test with the outcome it had', () => {
  const out = join(mkdtempSync(join(tmpdir(), 'oscope-trace-')), 'sub', 't.jsonl')
  const r = spawnSync(process.execPath, ['--test', '--test-reporter=./test/trace-reporter.js', '--test-reporter-destination=stdout', 'test/fixtures/trace.fixture.js'], {
    env: childEnv({ OSCOPE_TRACE_OUT: out, GITHUB_SHA: 'abc123', GITHUB_JOB: 'lakeui' }),
    encoding: 'utf8'
  })
  assert.notEqual(r.status, 0, 'the fixture has a planned failure')
  const recs = readFileSync(out, 'utf8').split('\n').filter(Boolean).map(l => JSON.parse(l))
  const got = Object.fromEntries(recs.map(x => [x.func, x.outcome]))
  assert.deepEqual(got, { 'fixture passes': 'passed', 'fixture fails': 'failed', 'fixture skips': 'skipped' })
  for (const x of recs) {
    assert.equal(x.commit, 'abc123')
    assert.equal(x.job, 'lakeui')
    assert.equal(x.lang, 'node')
    assert.equal(x.file, 'otel-chdb/lakeui/test/fixtures/trace.fixture.js')
  }
  assert.deepEqual(recs.find(x => x.func === 'fixture passes').ids, ['H-2', 'CAST-1'])
  assert.equal(existsSync(`${out}.node-claims`), false, 'the reporter took the claims')
})

test('trace: without the reporter a claim is never a record', () => {
  const out = join(mkdtempSync(join(tmpdir(), 'oscope-trace-')), 't.jsonl')
  spawnSync(process.execPath, ['--test', 'test/fixtures/trace.fixture.js'], { env: childEnv({ OSCOPE_TRACE_OUT: out }), encoding: 'utf8' })
  assert.equal(existsSync(out), false)
  assert.equal(existsSync(`${out}.node-claims`), true)
})

test('trace: outcomes and paths', () => {
  assert.equal(outcomeOf('test:fail', {}), 'failed')
  assert.equal(outcomeOf('test:pass', { skip: 'why' }), 'skipped')
  assert.equal(outcomeOf('test:pass', { skip: true }), 'skipped')
  assert.equal(outcomeOf('test:pass', { todo: true }), 'skipped')
  assert.equal(outcomeOf('test:pass', {}), 'passed')
  assert.equal(repoPath('/w/r/otel-chdb/lakeui/test/a.test.js'), 'otel-chdb/lakeui/test/a.test.js')
  assert.equal(repoPath('file:///w/r/otel-chdb/lakeui/test/a.test.js'), 'otel-chdb/lakeui/test/a.test.js')
})
