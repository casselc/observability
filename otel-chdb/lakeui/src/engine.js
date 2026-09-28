// One query, end to end: plan → range-read the planned objects (footer, then
// the column chunks the query needs) → merge → label. The re-plan rules are
// runner.js's; this wires them to the range reader and the queries.

import { labelOf, resultState } from './completeness.js'
import { DEFAULT_FOOTER_FETCH, MetaCache, metadata } from './parquet.js'
import { isBasisToken, PlanError } from './planclient.js'
import { newStats, rangeBuffer } from './rangereader.js'
import { runPlanned } from './runner.js'

export const sharedMeta = new MetaCache()

/**
 * Results at a basis (D30), by basis and query: an answer at a basis never
 * changes, so it is kept (bounded, oldest out). Nothing without a basis is
 * kept.
 */
export class ResultCache {
  constructor(max = 50) {
    this.max = max
    this.m = new Map()
  }

  key(basis, query, req) {
    return isBasisToken(basis) && query.key ? JSON.stringify([basis, query.key, req.signal, [...(req.clusters ?? [])].sort(), req.traceId ?? '', req.terms ?? []]) : null
  }

  get(k) {
    return k ? this.m.get(k) : undefined
  }

  set(k, v) {
    if (!k) return
    this.m.delete(k)
    this.m.set(k, v)
    while (this.m.size > this.max) this.m.delete(this.m.keys().next().value)
  }

  get size() {
    return this.m.size
  }
}

export const sharedResults = new ResultCache()

/** The footer's custody time (FORMAT.md §2: data objects repeat oscope-received there). */
export function footerReceivedNs(md) {
  const kv = (md.key_value_metadata ?? []).find(x => x.key === 'oscope-received')
  return kv && /^\d+$/.test(kv.value) ? BigInt(kv.value) : null
}


/**
 * @param {ReturnType<import('./queries.js').logSearch>} query
 * @param {object} o
 * @param {(req: any, opts: {force: boolean}) => Promise<any>} o.planner  cachedPlanner(...)
 * @param {{signal?: string, fromNs: bigint, toNs: bigint, clusters?: string[]}} o.request
 */
export async function execute(query, { planner, request, fetch = globalThis.fetch, now = Date.now, maxReplans = 3,
  concurrency = 6, metaCache = sharedMeta, footerFetch = DEFAULT_FOOTER_FETCH, onEvent = () => {}, results = sharedResults }) {
  const stats = newStats()
  stats.basisExcluded = 0
  const t0 = now()
  // a query's filter (a trace id, text terms) goes to the planner, which
  // narrows the plan through the lake index when the service has one
  const req = { ...request, signal: query.signal, ...(request.useIndex === false ? {} : query.filter ?? {}) }
  delete req.useIndex
  delete req.basis
  // D30: one basis per view load. The first plan asks for the latest (or
  // the basis the caller holds); every re-plan and the detail pass ask for
  // the basis that answered, so the whole view reads one set of objects
  // however much data arrives while it loads.
  const pin = { basis: request.basis ?? 'latest' }
  const cached = results.get(results.key(pin.basis, query, req))
  if (cached) return { ...cached, cached: true, elapsedMs: now() - t0 }
  const pinned = async (r, opts) => {
    const p = await planner({ ...r, basis: pin.basis ?? undefined }, opts)
    if (pin.basis === 'latest') pin.basis = p.atBasis ? p.basis : null // a service without bases: unpinned, and said so
    else if (pin.basis && (!p.atBasis || p.basis !== pin.basis)) throw new PlanError('bad_plan', 'plan: not at the pinned basis')
    return p
  }
  const readMeta = async (obj) => {
    const file = rangeBuffer(obj, { fetch, stats })
    const md = await metadata(file, obj.key, metaCache, footerFetch)
    return { file, md }
  }
  // an object the plan could not date: its footer decides (plan rule);
  // no custody time there is an error, never a keep
  const atBasis = (obj, md) => {
    if (!obj.basisCheck) return obj
    const r = footerReceivedNs(md)
    if (r === null) {
      const e = new Error(`${obj.key}: no oscope-received in the footer: it cannot be placed against the basis`)
      e.kind = 'basis_check'
      throw e
    }
    if (r < obj.receivedBeforeNs) return obj
    stats.basisExcluded++
    return { ...obj, excluded: true } // planned, its footer read, and holding nothing at this basis
  }
  const p1 = await runPlanned({
    plan: pinned, request: req, now, maxReplans, concurrency, onEvent,
    read: async (obj) => {
      const { file, md } = await readMeta(obj)
      return query.scan(file, atBasis(obj, md), md)
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
        plan: pinned, request: req, now, maxReplans, concurrency, onEvent,
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
  const out = { ...base, status: 'ok', state: resultState(label), missing: [], errors: [], result, elapsedMs: now() - t0, basis: pin.basis }
  results.set(results.key(pin.basis, query, req), out)
  return out
}
