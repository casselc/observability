// A Playwright reporter that writes the records test/pw-trace.js's tags
// claim (the fields tracetag, oscope_trace and test/trace-reporter.js
// write), with the test's final outcome: passed, skipped, or failed (failed,
// timedOut, interrupted). Nothing unless OSCOPE_TRACE_OUT is set. List it in
// the config's reporters beside the one people read.
import { appendFileSync, mkdirSync } from 'node:fs'
import { dirname } from 'node:path'
import { ANNOTATION } from './pw-trace.js'
import { ENV } from './trace.js'
import { repoPath } from './trace-reporter.js'

export function outcomeOf (status) {
  if (status === 'passed') return 'passed'
  if (status === 'skipped') return 'skipped'
  return 'failed'
}

// records for one finished test: its annotations of our type, one record each
export function recordsFor (test, result, env = process.env, sha = '') {
  const anns = [...(result.annotations ?? []), ...(test.annotations ?? [])].filter(a => a.type === ANNOTATION)
  const seen = new Set()
  const file = repoPath(test.location?.file)
  const out = []
  for (const a of anns) {
    if (seen.has(a.description)) continue // a runtime annotation can be on both
    seen.add(a.description)
    const { technique, ids } = JSON.parse(a.description)
    out.push({
      ids,
      technique,
      test: typeof test.titlePath === 'function' ? test.titlePath().filter(Boolean).slice(-1)[0] ?? test.title : test.title,
      func: test.title,
      package: file.split('/').slice(0, -1).join('/'),
      file,
      lang: 'playwright',
      commit: env.GITHUB_SHA || sha,
      job: env.GITHUB_JOB ?? '',
      outcome: outcomeOf(result.status)
    })
  }
  return out
}

export default class PwTraceReporter {
  constructor () { this.last = new Map() }
  onTestEnd (test, result) { this.last.set(test.id, recordsFor(test, result)) } // the last attempt counts
  printsToStdio () { return false }
  onEnd () {
    const out = process.env[ENV]
    if (!out) return
    mkdirSync(dirname(out), { recursive: true })
    for (const rs of this.last.values()) {
      for (const r of rs) appendFileSync(out, JSON.stringify(r) + '\n')
    }
  }
}
