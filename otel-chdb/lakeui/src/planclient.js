// The planner client: POST /v1/plan (query/README.md §2.2), and the plan's
// answer checked and normalised. Nothing here reads an object.
//
// A refusal is an answer, not an empty plan: every non-200 becomes a
// PlanError whose `kind` the views render as such (R-S1, X8).

import { formatTimeNs, parseJSONNs, parseTimeNs } from './ns.js'

export class PlanError extends Error {
  /**
   * @param {'auth'|'refused'|'too_large'|'bad_request'|'server'|'network'|'bad_plan'} kind
   */
  constructor(kind, message, { status = 0, reason = '', detail = '', requestId = '' } = {}) {
    super(message)
    this.name = 'PlanError'
    this.kind = kind
    this.status = status
    this.reason = reason
    this.detail = detail
    this.requestId = requestId
  }
}

/** HTTP status (and the service's error code) → the kind the UI shows. */
export function classify(status) {
  if (status === 401) return 'auth'
  if (status === 403) return 'refused'
  if (status === 413) return 'too_large'
  if (status === 400) return 'bad_request'
  return 'server'
}

const COMPLETENESS = new Set(['complete', 'partial', 'unknown'])

/** A basis token (D30): what a plan was computed at, and what pins the next. */
export const BASIS_TOKEN = /^b1\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/
export const isBasisToken = b => typeof b === 'string' && BASIS_TOKEN.test(b)

function bigOrNull(v) {
  if (v === null || v === undefined) return null
  if (typeof v === 'bigint') return v
  if (typeof v === 'string' && /^-?\d+$/.test(v)) return BigInt(v)
  if (typeof v === 'number' && Number.isSafeInteger(v)) return BigInt(v)
  return undefined // present but unusable
}

function timeMs(s, what) {
  try {
    return Number(parseTimeNs(s) / 1_000_000n)
  } catch {
    throw new PlanError('bad_plan', `plan: ${what} is not a time: ${s}`)
  }
}

/**
 * Checks a parsed /v1/plan answer and returns the normalised plan. A plan
 * that can't be trusted to describe what exists (no expiry, an object without
 * a URL or a size) is refused here rather than read partly.
 */
export function normalizePlan(raw, fetchedAtMs = Date.now()) {
  if (!raw || typeof raw !== 'object') throw new PlanError('bad_plan', 'plan: not an object')
  if (!Array.isArray(raw.objects)) throw new PlanError('bad_plan', 'plan: no objects list')
  const completeness = COMPLETENESS.has(raw.completeness) ? raw.completeness : 'unknown'
  const ct = bigOrNull(raw.complete_through_ns)
  const inc = bigOrNull(raw.incomplete_from_ns)
  if (ct === undefined || inc === undefined) throw new PlanError('bad_plan', 'plan: complete_through_ns / incomplete_from_ns unreadable')
  const fromNs = raw.from ? parseTimeNs(raw.from) : bigOrNull(raw.from_ns)
  const toNs = raw.to ? parseTimeNs(raw.to) : bigOrNull(raw.to_ns)
  if (typeof fromNs !== 'bigint' || typeof toNs !== 'bigint') throw new PlanError('bad_plan', 'plan: no window')
  // max_lateness bridges complete_through (custody time) and the window
  // (event time); a service that does not report it is not trusted with
  // "complete" (completeness.js)
  const ml = raw.max_lateness_s
  const maxLatenessNs = typeof ml === 'number' && Number.isFinite(ml) && ml >= 0 ? BigInt(Math.round(ml * 1e9)) : null
  const settled = bigOrNull(raw.settled_through_ns)
  const objects = raw.objects.map((o, i) => {
    if (!o || typeof o.url !== 'string' || !o.url || typeof o.key !== 'string' || !o.key) {
      throw new PlanError('bad_plan', `plan: object ${i} has no url or key`)
    }
    if (!Number.isSafeInteger(o.size) || o.size <= 0) throw new PlanError('bad_plan', `plan: object ${o.key} has no usable size`)
    const minT = bigOrNull(o.min_time_ns)
    const maxT = bigOrNull(o.max_time_ns)
    // the index's answer for a filtered plan (D27): 'hit' narrows the read
    // to row_groups; anything else (absent, 'scan') reads the object whole
    let index = null
    let rowGroups = null
    if (o.index === 'hit') {
      if (!Array.isArray(o.row_groups) || !o.row_groups.length || !o.row_groups.every(g => Number.isSafeInteger(g) && g >= 0)) {
        throw new PlanError('bad_plan', `plan: object ${o.key} is an index hit without usable row_groups`)
      }
      index = 'hit'
      rowGroups = [...new Set(o.row_groups)].sort((a, b) => a - b)
    } else if (o.index !== undefined && o.index !== null && o.index !== 'scan') {
      throw new PlanError('bad_plan', `plan: object ${o.key} has index ${JSON.stringify(o.index)}`)
    } else if (o.index === 'scan') {
      index = 'scan'
    }
    // a plan at a basis marks the objects it could not date: the reader
    // checks the footer's oscope-received against the bound (plan rule)
    const basisCheck = o.basis_check === true
    const receivedBeforeNs = bigOrNull(o.received_before_ns)
    if (basisCheck && typeof receivedBeforeNs !== 'bigint') {
      throw new PlanError('bad_plan', `plan: object ${o.key} needs a basis check without received_before_ns`)
    }
    return {
      key: o.key, url: o.url, size: o.size, cluster: o.cluster ?? '', producer: o.producer ?? '',
      minTimeNs: minT ?? null, maxTimeNs: maxT ?? null,
      rows: typeof o.rows === 'number' ? o.rows : bigOrNull(o.rows) ?? null, refined: o.refined === true,
      late: o.late === true,
      index, rowGroups,
      basisCheck, receivedBeforeNs: basisCheck ? receivedBeforeNs : null,
    }
  })
  const keys = new Set()
  for (const o of objects) {
    if (keys.has(o.key)) throw new PlanError('bad_plan', `plan: object ${o.key} twice`)
    keys.add(o.key)
  }
  const expiresAtMs = timeMs(raw.expires_at, 'expires_at')
  const replanAfterMs = timeMs(raw.replan_after, 'replan_after')
  if (replanAfterMs > expiresAtMs) throw new PlanError('bad_plan', 'plan: replan_after is after expires_at')
  return {
    requestId: raw.request_id ?? '',
    source: raw.source ?? 'lake',
    signal: raw.signal,
    clusters: raw.clusters ?? [],
    fromNs, toNs,
    completeness,
    partial: raw.partial !== false || completeness !== 'complete',
    completeThroughNs: ct,
    incompleteFromNs: inc,
    maxLatenessNs,
    settledThroughNs: settled ?? null,
    lateObjects: Number.isSafeInteger(raw.late_objects) ? raw.late_objects : 0,
    startComplete: raw.start_complete !== false,
    gcTruncatedLanes: raw.gc_truncated_lanes ?? [],
    gcNote: raw.gc_note ?? '',
    watermark: raw.watermark ?? {},
    snapshot: raw.snapshot ?? null,
    snapshotNote: raw.snapshot_note ?? '',
    listedAt: raw.listed_at ?? '',
    expiresAtMs, replanAfterMs, fetchedAtMs,
    objects,
    totalBytes: objects.reduce((a, o) => a + o.size, 0),
    unrefined: raw.unrefined ?? 0,
    rules: raw.rules ?? [],
    index: raw.index && typeof raw.index === 'object' ? raw.index : null,
    // D30: the basis the plan was computed at (atBasis), or the current one
    basis: isBasisToken(raw.basis) ? raw.basis : null,
    atBasis: raw.at_basis === true && isBasisToken(raw.basis),
    basisInfo: raw.basis_info && typeof raw.basis_info === 'object' ? raw.basis_info : null,
    afterBasis: Number.isSafeInteger(raw.after_basis) ? raw.after_basis : 0,
    basisUnverified: Number.isSafeInteger(raw.basis_unverified) ? raw.basis_unverified : 0,
  }
}

/**
 * Asks the service for a plan.
 * @param {{queryUrl: string, token: string, signal: string, fromNs: bigint, toNs: bigint, clusters?: string[]}} req
 */
export async function requestPlan(req, { fetch = globalThis.fetch, now = Date.now } = {}) {
  const body = { signal: req.signal, from: formatTimeNs(req.fromNs), to: formatTimeNs(req.toNs) }
  if (req.clusters && req.clusters.length) body.clusters = req.clusters
  if (req.traceId) body.trace_id = req.traceId
  if (req.terms && req.terms.length) body.terms = req.terms
  if (req.basis) body.basis = req.basis
  let resp
  try {
    resp = await fetch(req.queryUrl.replace(/\/$/, '') + '/v1/plan', {
      method: 'POST',
      headers: { authorization: 'Bearer ' + req.token, 'content-type': 'application/json' },
      body: JSON.stringify(body),
      cache: 'no-store',
    })
  } catch (e) {
    throw new PlanError('network', `the query service did not answer: ${e.message}`)
  }
  const text = await resp.text()
  let parsed = null
  try {
    parsed = parseJSONNs(text)
  } catch {
    // below
  }
  if (resp.status !== 200) {
    const reason = parsed?.error ?? ''
    throw new PlanError(classify(resp.status), `plan ${resp.status}${reason ? ' ' + reason : ''}${parsed?.detail ? ': ' + parsed.detail : ''}`,
      { status: resp.status, reason, detail: parsed?.detail ?? text.slice(0, 300), requestId: parsed?.request_id ?? '' })
  }
  if (!parsed) throw new PlanError('bad_plan', 'plan: the answer is not JSON')
  return normalizePlan(parsed, now())
}

/** The key a plan answers: the same key → the same plan while it is valid. */
export function planKey(req) {
  return JSON.stringify([req.signal, String(req.fromNs), String(req.toNs), [...(req.clusters ?? [])].sort(), req.traceId ?? '', req.terms ?? [],
    req.basis ?? ''])
}

/**
 * A plan cache keyed on the basis (D30): only a plan AT a basis is cached,
 * under that basis, because only there does the same request list the same
 * objects; a request for "latest" or for no basis is never answered from
 * the cache, and its answer is kept under the basis it came back at. A
 * cached plan is still reused only while now < replan_after (X8 rule 1: its
 * URLs expire); `force` asks again (after a failed read). A plan asked at a
 * basis that comes back at another is refused, never cached.
 */
export function cachedPlanner(ask, { now = Date.now } = {}) {
  const cache = new Map()
  let calls = 0
  const plan = async (req, { force = false } = {}) => {
    const pinned = isBasisToken(req.basis)
    const hit = pinned ? cache.get(planKey(req)) : undefined
    if (!force && hit && now() < hit.replanAfterMs) return hit
    calls++
    const p = await ask(req)
    if (pinned && (!p.atBasis || p.basis !== req.basis)) {
      throw new PlanError('bad_plan', 'plan: asked at a basis, answered at another (or at none)')
    }
    if (p.atBasis) cache.set(planKey({ ...req, basis: p.basis }), p)
    return p
  }
  plan.calls = () => calls
  plan.clear = () => cache.clear()
  /** tests and the "stale plan" demonstration: put a plan in the cache */
  plan.put = (req, p) => cache.set(planKey(req), p)
  plan.peek = req => cache.get(planKey(req))
  /** every cached plan (the browser test shifts their times to model a skewed clock) */
  plan.cached = () => [...cache.values()]
  return plan
}
