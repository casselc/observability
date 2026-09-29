// Runtime traceability tags for node:test (the Go twin is
// otel-chdb/testgate/tracetag, the Rust one otap-rs/src/oscope_trace.rs;
// ci/trace/trace.py joins the records, ci/README.md "Traceability").
//
//   test('max_lateness: event time settles ...', (t) => {
//     covers(t, 'P2C', 'CAST-26', 'H-4', 'R-S1')
//
// node:test tells a test's own code nothing about how the test ended, so the
// tag is a claim, appended to `${OSCOPE_TRACE_OUT}.node-claims` when the test
// starts, and test/trace-reporter.js (a --test-reporter, see package.json)
// writes the record once it sees the test's pass, fail or skip. A claim
// without a reporter writes no record, which the traceability job reports as
// a tagged test that did not run: never as a pass. Unset, nothing is written.
import { appendFileSync, mkdirSync } from 'node:fs'
import { dirname } from 'node:path'
import { fileURLToPath } from 'node:url'

export const ENV = 'OSCOPE_TRACE_OUT'
export const claimsPath = (out) => `${out}.node-claims`

export function covers (t, technique, ...ids) {
  const out = process.env[ENV]
  if (!out) return
  const file = t.filePath ?? (t.name && callerFile())
  const claim = { file, name: t.name, fullName: t.fullName ?? t.name, technique, ids }
  mkdirSync(dirname(out), { recursive: true })
  appendFileSync(claimsPath(out), JSON.stringify(claim) + '\n')
}

// The test file that called covers, where the context does not say (node < 22.6).
function callerFile () {
  const line = (new Error().stack ?? '').split('\n').find(l => l.includes('.test.js'))
  const m = line && line.match(/\((file:\/\/[^):]+|\/[^):]+)|at (file:\/\/[^\s:]+|\/[^\s:]+)/)
  const f = m && (m[1] ?? m[2])
  return f && f.startsWith('file:') ? fileURLToPath(f) : f
}
