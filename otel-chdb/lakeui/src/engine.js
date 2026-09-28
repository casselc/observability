// One query, end to end: plan → range-read the planned objects (footer, then
// the column chunks the query needs) → merge → label. The re-plan rules are
// runner.js's; this wires them to the range reader and the queries.

import { labelOf, resultState } from './completeness.js'
import { DEFAULT_FOOTER_FETCH, MetaCache, metadata } from './parquet.js'
import { newStats, rangeBuffer } from './rangereader.js'
import { runPlanned } from './runner.js'

export const sharedMeta = new MetaCache()

/**
 * @param {ReturnType<import('./queries.js').logSearch>} query
 * @param {object} o
 * @param {(req: any, opts: {force: boolean}) => Promise<any>} o.planner  cachedPlanner(...)
 * @param {{signal?: string, fromNs: bigint, toNs: bigint, clusters?: string[]}} o.request
 */
export async function execute(query, { planner, request, fetch = globalThis.fetch, now = Date.now, maxReplans = 3,
  concurrency = 6, metaCache = sharedMeta, footerFetch = DEFAULT_FOOTER_FETCH, onEvent = () => {} }) {
  const stats = newStats()
  const t0 = now()
  const req = { ...request, signal: query.signal }
  const readMeta = async (obj) => {
    const file = rangeBuffer(obj, { fetch, stats })
    const md = await metadata(file, obj.key, metaCache, footerFetch)
    return { file, md }
  }
  const p1 = await runPlanned({
    plan: planner, request: req, now, maxReplans, concurrency, onEvent,
    read: async (obj) => {
      const { file, md } = await readMeta(obj)
      return query.scan(file, obj, md)
    },
  })
  const plan = p1.plan
  const label = labelOf(plan)
  const base = { plan, label, stats, replans: p1.replans, events: [...p1.events] }
  if (p1.status !== 'ok') {
    return { ...base, status: 'failed', state: 'incomplete', missing: p1.missing, errors: p1.errors, reason: p1.reason,
      planError: p1.planError, result: null, elapsedMs: now() - t0 }
  }
  let result = query.merge([...p1.results.values()], label)
  if (query.detailNeeds) {
    const needs = query.detailNeeds(result)
    if (needs.size) {
      const p2 = await runPlanned({
        plan: planner, request: req, now, maxReplans, concurrency, onEvent,
        select: p => p.objects.filter(o => needs.has(o.key)),
        read: async (obj) => {
          const { file, md } = await readMeta(obj)
          return query.detail(file, obj, md, needs.get(obj.key))
        },
      })
      base.replans += p2.replans
      base.events.push(...p2.events)
      // detail for keys the re-plan no longer lists is missing, not dropped
      const lacking = [...needs.keys()].filter(k => !p2.results.has(k))
      if (p2.status !== 'ok' || lacking.length) {
        return { ...base, status: 'failed', state: 'incomplete', missing: lacking, errors: p2.errors, reason: p2.reason || 'objects unread',
          planError: p2.planError, result, elapsedMs: now() - t0 }
      }
      result = query.attach(result, [...p2.results.values()])
    }
  }
  return { ...base, status: 'ok', state: resultState(label), missing: [], errors: [], result, elapsedMs: now() - t0 }
}
