// One query, end to end: plan → range-read the planned objects (footer, then
// the column chunks the query needs) → merge → label. The re-plan rules are
// runner.js's; this wires them to the range reader and the queries.

import { labelOf, resultState } from './completeness.js'
import { DEFAULT_FOOTER_FETCH, MetaCache, metadata } from './parquet.js'
import { isBasisToken, PlanError, unpinnable } from './planclient.js'
import { newStats, rangeBuffer } from './rangereader.js'
import { allObjects, runPlanned } from './runner.js'

export const sharedMeta = new MetaCache()

/**
 * The basis part's per-object answers (D30), by basis and query: an object
 * of a plan's basis part answers the same at the same basis, so its part is
 * kept (bounded, oldest out) and a later run at that basis reads only what
 * it lacks, the tail above all. The tail is never kept (D30 amendment "the
 * tail"), nor is anything without a basis.
 */
export class ResultCache {
  constructor(max = 50) {
    this.max = max
    this.m = new Map()
  }

  key(basis, query, req) {
    return isBasisToken(basis) && query.key ? JSON.stringify([basis, query.key, req.signal, [...(req.clusters ?? [])].sort(), req.traceId ?? '', req.terms ?? []]) : null
  }

  /** @returns {{parts: Map<string, any>, details: Map<string, Map<string, any>>} | undefined} kept answers by object key */
  get(k) {
    return k ? this.m.get(k) : undefined
  }

  set(k, v) {
    if (!k) return
    this.m.delete(k)
    this.m.set(k, v)
    while (this.m.size > this.max) this.m.delete(this.m.keys().next().value)
  }

  clear() {
    this.m.clear()
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
 * @param {{signal?: string, fromNs: bigint, toNs: bigint, clusters?: string[], basis?: string}} o.request
 */
export async function execute(query, { planner, request, fetch = globalThis.fetch, now = Date.now, maxReplans = 3,
  concurrency = 6, metaCache = sharedMeta, footerFetch = DEFAULT_FOOTER_FETCH, onEvent = () => {}, results = sharedResults }) {
  const stats = newStats()
  stats.basisExcluded = 0
  stats.cachedParts = 0 // basis-part objects answered from what was kept
  stats.cachedDetails = 0 // and their shown rows' detail
  const t0 = now()
  // a query's filter (a trace id, text terms) goes to the planner, which
  // narrows the plan through the lake index when the service has one
  const req = { ...request, signal: query.signal, ...(request.useIndex === false ? {} : query.filter ?? {}) }
  delete req.useIndex
  delete req.basis
  // D30: one basis per view load. The first plan asks for the latest (or
  // the basis the caller holds); every re-plan and the detail pass ask for
  // the basis that answered, so the whole view reads one set of objects
  // however much data arrives while it loads. Every plan at a basis asks
  // for its tail too (AMBIGUITY.md #10 (b)): what was received after the
  // basis is read and drawn incomplete, never left out and never kept.
  const pin = { basis: request.basis ?? 'latest', unpinned: '' }
  let last = null // the latest plan: the detail pass starts from it while it is valid
  const pinned = async (r, opts) => {
    if (!opts.force && last && now() < last.replanAfterMs) return last
    let p
    try {
      p = await planner({ ...r, basis: pin.basis ?? undefined, tail: pin.basis ? true : undefined }, opts)
    } catch (e) {
      // "latest" when no basis can be issued (no watermark, no key): the
      // view is planned unpinned, and says so; a held token's refusal stands
      if (pin.basis !== 'latest' || !unpinnable(e)) throw e
      pin.basis = null
      pin.unpinned = e.reason
      onEvent({ type: 'unpinned', kind: e.kind, status: e.status, why: e.reason })
      p = await planner({ ...r, basis: undefined }, opts)
    }
    if (pin.basis === 'latest') pin.basis = p.atBasis ? p.basis : null // a service without bases: unpinned, and said so
    else if (pin.basis && (!p.atBasis || p.basis !== pin.basis)) throw new PlanError('bad_plan', 'plan: not at the pinned basis')
    last = p
    return p
  }
  const kept = () => results.get(results.key(pin.basis, query, req))
  // a detail answer is a list of per-row entries ({key, group, index, …},
  // queries.js); a basis object's rows shown later at the same basis are a
  // subset of those shown before (the tail can only push them out of the
  // newest N), so each row's entry is kept
  const readMeta = async (obj) => {
    const file = rangeBuffer(obj, { fetch, stats })
    const md = await metadata(file, obj.key, metaCache, footerFetch)
    return { file, md }
  }
  // an object the plan could not date: its footer decides (plan rule);
  // no custody time there is an error, never a keep. In the basis part (a
  // plan without tail) received at or after the bound means "holds nothing
  // here"; in the tail it means "the tail's" and below the bound "the
  // basis part's" (the reader moves it there by its footer, never a guess)
  const atBasis = (obj, md) => {
    if (!obj.basisCheck) return obj
    const r = footerReceivedNs(md)
    if (r === null) {
      const e = new Error(`${obj.key}: no oscope-received in the footer: it cannot be placed against the basis`)
      e.kind = 'basis_check'
      throw e
    }
    if (r < obj.receivedBeforeNs) return { ...obj, tail: false }
    if (obj.tail) return obj
    stats.basisExcluded++
    return { ...obj, excluded: true } // planned, its footer read, and holding nothing at this basis
  }
  const p1 = await runPlanned({
    plan: pinned, request: req, now, maxReplans, concurrency, onEvent,
    read: async (obj) => {
      const hit = kept()?.parts.get(obj.key)
      if (hit) {
        stats.cachedParts++
        return hit
      }
      const { file, md } = await readMeta(obj)
      const o = atBasis(obj, md)
      return { ...(await query.scan(file, o, md)), tail: o.tail === true }
    },
  })
  // keep the basis part's answers (the tail's never), read now or before
  const k = results.key(pin.basis, query, req)
  if (k && p1.plan.atBasis) {
    const m = kept() ?? { parts: new Map(), details: new Map() }
    for (const [key, part] of p1.results) if (!part.tail) m.parts.set(key, part)
    if (m.parts.size) results.set(k, m)
  }
  const plan = p1.plan
  const label = labelOf(plan)
  const base = { plan, label, stats, replans: p1.replans, events: [...p1.events], basis: pin.basis, unpinned: pin.unpinned }
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
        select: p => allObjects(p).filter(o => needs.has(o.key)),
        read: async (obj) => {
          const rows = needs.get(obj.key)
          const known = !obj.tail ? kept()?.details.get(obj.key) : undefined
          if (known && rows.every(r => known.has(`${r.group.index}#${r.index}`))) {
            stats.cachedDetails++
            return rows.map(r => known.get(`${r.group.index}#${r.index}`))
          }
          const { file, md } = await readMeta(obj)
          const got = await query.detail(file, obj, md, rows)
          const m = !obj.tail ? kept() : undefined
          if (m) {
            if (!m.details.has(obj.key)) m.details.set(obj.key, new Map())
            for (const d of got) m.details.get(obj.key).set(`${d.group}#${d.index}`, d)
          }
          return got
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
  const tailRows = result.tailRows ?? 0
  return { ...base, status: 'ok', state: resultState(label, { tailRows }), tailRows, missing: [], errors: [], result,
    cached: stats.cachedParts + stats.cachedDetails > 0, elapsedMs: now() - t0 }
}
