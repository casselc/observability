// Completeness math (STPA R-S1, R-S2; AMBIGUITY.md X8 rule 3). Pure.
//
// A plan's label says how far its source is settled. From it, every view
// derives the state of each row, bucket and total it draws:
//
//   complete    before complete_through, and the label is complete or partial
//               with a current watermark;
//   incomplete  at or after incomplete_from: rows may still arrive;
//   unknown     the label is unknown (no, a stale or an unreadable
//               watermark): nothing may be drawn as settled.
//
// A read that failed makes the whole result incomplete whatever the label
// (X8 rule 2); `resultState` folds that in.

/** The label's facts, from a normalised plan. */
export function labelOf(plan) {
  return {
    state: plan.completeness, // complete | partial | unknown
    fromNs: plan.fromNs,
    toNs: plan.toNs,
    completeThroughNs: plan.completeThroughNs, // bigint | null
    incompleteFromNs: plan.incompleteFromNs, // bigint | null
    startComplete: plan.startComplete !== false,
    watermarkStatus: plan.watermark?.status ?? '',
    note: plan.watermark?.note ?? '',
  }
}

/**
 * Where the incomplete region starts inside [fromNs, toNs), or null when the
 * whole window is settled. Unknown: fromNs (nothing settled). Partial with no
 * incomplete_from given: complete_through, or fromNs if that is missing too.
 */
export function incompleteStart(label) {
  if (label.state === 'unknown') return label.fromNs
  if (label.state === 'complete') return null
  let s = label.incompleteFromNs ?? label.completeThroughNs ?? label.fromNs
  if (s < label.fromNs) s = label.fromNs
  if (s >= label.toNs) return null
  return s
}

/** The window as settled and unsettled segments, in order, covering it exactly. */
export function segments(label) {
  const { fromNs, toNs } = label
  if (toNs <= fromNs) return []
  if (label.state === 'unknown') return [{ fromNs, toNs, state: 'unknown' }]
  const s = incompleteStart(label)
  if (s === null) return [{ fromNs, toNs, state: 'complete' }]
  const out = []
  if (s > fromNs) out.push({ fromNs, toNs: s, state: 'complete' })
  out.push({ fromNs: s, toNs, state: 'incomplete' })
  return out
}

/** One row at event time ts. */
export function rowState(label, ts) {
  if (label.state === 'unknown') return 'unknown'
  const s = incompleteStart(label)
  return s !== null && ts >= s ? 'incomplete' : 'complete'
}

/**
 * A bucket [b0, b1): complete only if it ends at or before the incomplete
 * region starts; any overlap with it makes the bucket incomplete (its count
 * may still grow).
 */
export function bucketState(label, b0, b1) {
  if (label.state === 'unknown') return 'unknown'
  const s = incompleteStart(label)
  return s !== null && b1 > s ? 'incomplete' : 'complete'
}

/**
 * The whole result's state: unknown dominates, then any failed read or an
 * unsettled window (or a start GC truncated) makes it incomplete.
 * @param {{missing?: string[]}} [read]
 */
export function resultState(label, read = {}) {
  if (read.missing && read.missing.length) return 'incomplete'
  if (label.state === 'unknown') return 'unknown'
  if (!label.startComplete) return 'incomplete'
  return incompleteStart(label) === null ? 'complete' : 'incomplete'
}

/**
 * Buckets of `stepNs` over the window, aligned to the window start; the last
 * one is cut at toNs. Each has its state.
 */
export function buckets(label, stepNs) {
  const out = []
  if (stepNs <= 0n) throw new RangeError('buckets: step must be positive')
  for (let b0 = label.fromNs; b0 < label.toNs; b0 += stepNs) {
    const b1 = b0 + stepNs < label.toNs ? b0 + stepNs : label.toNs
    out.push({ fromNs: b0, toNs: b1, state: bucketState(label, b0, b1) })
  }
  return out
}

/** The bucket index of ts, or -1 outside the window. */
export function bucketIndex(label, stepNs, ts) {
  if (ts < label.fromNs || ts >= label.toNs) return -1
  return Number((ts - label.fromNs) / stepNs)
}

/** A step that gives at most `max` buckets, rounded up to a readable unit. */
export function niceStep(fromNs, toNs, max = 60) {
  const span = toNs - fromNs
  const units = [1n, 5n, 10n, 15n, 30n, 60n, 120n, 300n, 600n, 900n, 1800n, 3600n, 7200n, 21600n, 43200n, 86400n].map(s => s * 1_000_000_000n)
  for (const u of units) if (span / u <= BigInt(max)) return u
  return units[units.length - 1]
}

/** The words every view shows (R-S1: source and complete-through on every view). */
export function bannerText(label, read = {}, fmt = String) {
  const parts = []
  const st = resultState(label, read)
  if (read.missing && read.missing.length) {
    parts.push(`INCOMPLETE: ${read.missing.length} planned object(s) could not be read`)
  }
  if (label.state === 'unknown') {
    parts.push('Completeness UNKNOWN: the watermark is ' + (label.watermarkStatus || 'missing') + '; nothing here may be read as settled')
  } else if (label.completeThroughNs !== null && label.completeThroughNs !== undefined) {
    const s = incompleteStart(label)
    parts.push(`Complete through ${fmt(label.completeThroughNs)}` + (s !== null ? `; incomplete from ${fmt(s)} (rows may still arrive)` : ''))
  }
  if (!label.startComplete) parts.push('the start of the window may be missing (GC): older rows are in central')
  return { state: st, text: parts.join(' · ') }
}
