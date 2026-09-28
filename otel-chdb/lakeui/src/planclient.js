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
  const objects = raw.objects.map((o, i) => {
    if (!o || typeof o.url !== 'string' || !o.url || typeof o.key !== 'string' || !o.key) {
      throw new PlanError('bad_plan', `plan: object ${i} has no url or key`)
    }
    if (!Number.isSafeInteger(o.size) || o.size <= 0) throw new PlanError('bad_plan', `plan: object ${o.key} has no usable size`)
    const minT = bigOrNull(o.min_time_ns)
    const maxT = bigOrNull(o.max_time_ns)
    return {
      key: o.key, url: o.url, size: o.size, cluster: o.cluster ?? '', producer: o.producer ?? '',
      minTimeNs: minT ?? null, maxTimeNs: maxT ?? null,
      rows: typeof o.rows === 'number' ? o.rows : bigOrNull(o.rows) ?? null, refined: o.refined === true,
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
  }
}

/**
 * Asks the service for a plan.
 * @param {{queryUrl: string, token: string, signal: string, fromNs: bigint, toNs: bigint, clusters?: string[]}} req
 */
export async function requestPlan(req, { fetch = globalThis.fetch, now = Date.now } = {}) {
  const body = { signal: req.signal, from: formatTimeNs(req.fromNs), to: formatTimeNs(req.toNs) }
  if (req.clusters && req.clusters.length) body.clusters = req.clusters
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
  return JSON.stringify([req.signal, String(req.fromNs), String(req.toNs), [...(req.clusters ?? [])].sort()])
}

/**
 * A plan cache: a plan is reused only while now < replan_after (X8 rule 1);
 * `force` asks again whatever the cache holds (after a failed read).
 */
export function cachedPlanner(ask, { now = Date.now } = {}) {
  const cache = new Map()
  let calls = 0
  const plan = async (req, { force = false } = {}) => {
    const k = planKey(req)
    const hit = cache.get(k)
    if (!force && hit && now() < hit.replanAfterMs) return hit
    calls++
    const p = await ask(req)
    cache.set(k, p)
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
