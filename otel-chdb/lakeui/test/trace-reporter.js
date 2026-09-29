// A node:test reporter that turns the claims test/trace.js writes into
// records (the fields tracetag and oscope_trace write), with the outcome it
// saw: passed, failed, or skipped (t.skip / { skip }). A claim with no
// outcome event is written as failed. Writes nothing unless OSCOPE_TRACE_OUT
// is set; it yields nothing, so its destination can be anything.
import { appendFileSync, existsSync, readFileSync, renameSync } from 'node:fs'
import { execFileSync } from 'node:child_process'
import { ENV, claimsPath } from './trace.js'

export const repoPath = (f) => {
  const s = String(f ?? '').replaceAll('\\', '/').replace(/^file:\/\//, '')
  const i = s.lastIndexOf('/otel-chdb/')
  return i >= 0 ? s.slice(i + 1) : s
}

function commit () {
  if (process.env.GITHUB_SHA) return process.env.GITHUB_SHA
  try { return execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim() } catch { return '' }
}

// outcomes: `${file}\0${name}` -> 'passed' | 'failed' | 'skipped' (any failure wins)
export function outcomeOf (type, data) {
  if (type === 'test:fail') return 'failed'
  if (data.skip !== undefined && data.skip !== false) return 'skipped'
  if (data.todo !== undefined && data.todo !== false) return 'skipped'
  return 'passed'
}

export function records (claims, outcomes, env = process.env) {
  const sha = env.GITHUB_SHA || commit()
  return claims.map(c => {
    const file = repoPath(c.file)
    return {
      ids: c.ids,
      technique: c.technique,
      test: c.fullName ?? c.name,
      func: c.name,
      package: file.split('/').slice(0, -1).join('/'),
      file,
      lang: 'node',
      commit: sha,
      job: env.GITHUB_JOB ?? '',
      outcome: outcomes.get(`${file}\0${c.name}`) ?? 'failed'
    }
  })
}

export default async function * traceReporter (source) {
  const outcomes = new Map()
  for await (const { type, data } of source) {
    if (type !== 'test:pass' && type !== 'test:fail') continue
    const key = `${repoPath(data.file)}\0${data.name}`
    const o = outcomeOf(type, data)
    if (outcomes.get(key) !== 'failed') outcomes.set(key, o)
  }
  const out = process.env[ENV]
  if (!out) return
  const cp = claimsPath(out)
  if (!existsSync(cp)) return
  // Take the claims so a second run in the same job does not record them twice.
  const taken = `${cp}.${process.pid}`
  renameSync(cp, taken)
  const claims = readFileSync(taken, 'utf8').split('\n').filter(Boolean).map(l => JSON.parse(l))
  for (const r of records(claims, outcomes)) appendFileSync(out, JSON.stringify(r) + '\n')
}
