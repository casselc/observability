// The three views' queries over planned objects: what each reads from one
// object (as few column chunks as the answer needs), and how the per-object
// answers merge. Pure apart from the reads they are handed.
//
// Columns are the edges' (ClickStack names, FORMAT.md): logs Timestamp,
// SeverityText, ServiceName, Body; traces Timestamp, TraceId, SpanId,
// ParentSpanId, SpanName, ServiceName, Duration, StatusCode; clickstack
// gauge points TimeUnix, MetricName, Value, ServiceName.
//
// A part (one object's answer) the engine marks `tail: true` is from the
// plan's tail (received after the basis): its rows, and the buckets they
// fall in, are drawn incomplete (completeness.js), and each merge reports
// them as `tailRows` (the basis part's rows are the rest).

import { bucketIndex, bucketState, buckets, niceStep, rowState } from './completeness.js'
import { pruneByRange, readGroup, rowGroups } from './parquet.js'

const inWindow = (ts, q) => ts >= q.fromNs && ts < q.toNs

/**
 * The row groups of one object a query must read: all of them, unless the
 * plan's index narrowed the object to some (index 'hit', D27). A row group
 * the file does not have means the index and the object disagree: read
 * every group (the superset), never fewer.
 */
export function indexedGroups(md, obj) {
  const all = rowGroups(md)
  if (obj.index !== 'hit' || !obj.rowGroups) return all
  if (obj.rowGroups.some(g => g >= all.length)) return all
  const want = new Set(obj.rowGroups)
  return all.filter(g => want.has(g.index))
}

function objectMayOverlap(obj, q) {
  // excluded by its footer at the plan's basis (engine.js): holds nothing here
  if (obj.excluded) return false
  // the plan's per-object range (from the slot's metadata) prunes whole objects
  if (obj.minTimeNs !== null && obj.maxTimeNs !== null && obj.minTimeNs !== undefined) {
    return obj.maxTimeNs >= q.fromNs && obj.minTimeNs < q.toNs
  }
  return true
}

// ---- logs -------------------------------------------------------------------

/**
 * Log search: rows in [fromNs, toNs) whose Body contains `text` (case-
 * insensitive; empty matches all) and whose SeverityText is one of
 * `severities` (empty: all). Count, a histogram, and the newest `limit` rows.
 */
export function logSearch({ fromNs, toNs, text = '', severities = [], limit = 50, stepNs }) {
  const q = { fromNs, toNs, text: text.trim().toLowerCase(), severities: new Set(severities.map(s => s.toUpperCase())), limit }
  q.stepNs = stepNs ?? niceStep(fromNs, toNs, 60)
  return {
    kind: 'logs',
    signal: 'logs',
    q,
    // what decides the answer (with the plan's basis: the result cache's key)
    key: JSON.stringify(['logs', String(q.fromNs), String(q.toNs), q.text, [...q.severities].sort(), q.limit, String(q.stepNs)]),
    // the plan filter: the index narrows a text search (the page still tests
    // every row itself; the index only drops what cannot match)
    filter: q.text ? { terms: [text.trim()] } : null,
    async scan(file, obj, md) {
      const out = { count: 0, hist: new Map(), top: [], bodyRead: q.text !== '', key: obj.key }
      if (!objectMayOverlap(obj, q)) return out
      const cols = ['Timestamp', 'SeverityText']
      if (q.text) cols.push('Body')
      const narrowed = new Set(indexedGroups(md, obj).map(g => g.index))
      for (const g of pruneByRange(md, 'Timestamp', q.fromNs, q.toNs - 1n)) {
        if (!narrowed.has(g.index)) continue
        const rows = await readGroup(file, obj.key, md, g, cols)
        for (let i = 0; i < rows.length; i++) {
          const r = rows[i]
          if (!inWindow(r.Timestamp, q)) continue
          if (q.severities.size && !q.severities.has(String(r.SeverityText).toUpperCase())) continue
          if (q.text && !String(r.Body).toLowerCase().includes(q.text)) continue
          out.count++
          const b = bucketIndex(q, q.stepNs, r.Timestamp)
          out.hist.set(b, (out.hist.get(b) ?? 0) + 1)
          pushTop(out.top, { ts: r.Timestamp, key: obj.key, group: g, index: i, severity: r.SeverityText, body: q.text ? r.Body : undefined }, q.limit)
        }
      }
      return out
    },
    merge(parts, label) {
      let count = 0
      let tailRows = 0
      const bs = buckets({ ...label, fromNs: q.fromNs, toNs: q.toNs }, q.stepNs).map(b => ({ ...b, count: 0, tailCount: 0 }))
      const tailKeys = new Set()
      let top = []
      for (const p of parts) {
        count += p.count
        if (p.tail) {
          tailRows += p.count
          tailKeys.add(p.key)
        }
        for (const [b, n] of p.hist) {
          if (!bs[b]) continue
          bs[b].count += n
          if (p.tail) bs[b].tailCount += n
        }
        for (const r of p.top) pushTop(top, r, q.limit)
      }
      top = top.sort(byNewest)
      for (const b of bs) b.state = bucketState(label, b.fromNs, b.toNs, b.tailCount > 0)
      return { count, tailRows, basisCount: count - tailRows, buckets: bs,
        rows: top.map(r => ({ ...r, tail: tailKeys.has(r.key), state: rowState(label, r.ts, tailKeys.has(r.key)) })) }
    },
    /** Objects whose rows are shown and still need their display columns. */
    detailNeeds(result) {
      const need = new Map()
      for (const r of result.rows) {
        if (!need.has(r.key)) need.set(r.key, [])
        need.get(r.key).push(r)
      }
      return need
    },
    async detail(file, obj, md, rows) {
      const cols = ['ServiceName']
      if (!q.text) cols.push('Body')
      const byGroup = new Map()
      for (const r of rows) {
        if (!byGroup.has(r.group.index)) byGroup.set(r.group.index, { g: r.group, rows: [] })
        byGroup.get(r.group.index).rows.push(r)
      }
      const out = []
      for (const { g, rows: rs } of byGroup.values()) {
        const got = await readGroup(file, obj.key, md, g, cols)
        for (const r of rs) out.push({ key: r.key, group: g.index, index: r.index, service: got[r.index].ServiceName, body: q.text ? r.body : got[r.index].Body })
      }
      return out
    },
    attach(result, details) {
      const m = new Map()
      for (const list of details) for (const d of list) m.set(`${d.key}#${d.group}#${d.index}`, d)
      for (const r of result.rows) {
        const d = m.get(`${r.key}#${r.group.index}#${r.index}`)
        if (d) {
          r.service = d.service
          r.body = d.body
        }
      }
      return result
    },
  }
}

function byNewest(a, b) {
  if (a.ts !== b.ts) return a.ts > b.ts ? -1 : 1
  if (a.key !== b.key) return a.key < b.key ? -1 : 1
  return a.index - b.index
}

/** Keeps the `limit` newest rows (a bounded insertion; limit is small). */
function pushTop(top, r, limit) {
  if (limit <= 0) return
  if (top.length < limit) {
    top.push(r)
    return
  }
  let worst = 0
  for (let i = 1; i < top.length; i++) if (byNewest(top[i], top[worst]) > 0) worst = i
  if (byNewest(r, top[worst]) < 0) top[worst] = r
}

// ---- traces -----------------------------------------------------------------

export const TRACE_ID = /^[0-9a-f]{32}$/

/** Every span of one trace in [fromNs, toNs). */
export function traceById({ traceId, fromNs, toNs }) {
  const id = String(traceId).trim().toLowerCase()
  if (!TRACE_ID.test(id)) throw new RangeError('a trace id is 32 hex digits')
  const q = { fromNs, toNs, id }
  const spanCols = ['Timestamp', 'SpanId', 'ParentSpanId', 'SpanName', 'ServiceName', 'Duration', 'StatusCode']
  return {
    kind: 'trace',
    signal: 'traces',
    key: JSON.stringify(['trace', traceId, String(fromNs), String(toNs)]),
    q,
    filter: { traceId: id },
    async scan(file, obj, md) {
      const out = { spans: [], key: obj.key }
      if (!objectMayOverlap(obj, q)) return out
      const byTime = new Set(pruneByRange(md, 'Timestamp', q.fromNs, q.toNs - 1n).map(g => g.index))
      const narrowed = new Set(indexedGroups(md, obj).map(g => g.index))
      for (const g of pruneByRange(md, 'TraceId', id, id)) {
        if (!byTime.has(g.index) || !narrowed.has(g.index)) continue
        const ids = await readGroup(file, obj.key, md, g, ['TraceId'])
        const hit = []
        for (let i = 0; i < ids.length; i++) if (String(ids[i].TraceId).toLowerCase() === id) hit.push(i)
        if (!hit.length) continue
        const rows = await readGroup(file, obj.key, md, g, spanCols)
        for (const i of hit) {
          const r = rows[i]
          if (!inWindow(r.Timestamp, q)) continue
          out.spans.push({ ts: r.Timestamp, spanId: r.SpanId, parent: r.ParentSpanId, name: r.SpanName, service: r.ServiceName,
            durationNs: BigInt(r.Duration ?? 0), status: r.StatusCode, key: obj.key })
        }
      }
      return out
    },
    merge(parts, label) {
      const spans = parts.flatMap(p => p.spans.map(s => ({ ...s, tail: p.tail === true }))).sort((a, b) => (a.ts < b.ts ? -1 : a.ts > b.ts ? 1 : 0))
      const t0 = spans.length ? spans[0].ts : null
      let t1 = t0
      for (const s of spans) if (s.ts + s.durationNs > t1) t1 = s.ts + s.durationNs
      return { traceId: id, spans: spans.map(s => ({ ...s, state: rowState(label, s.ts, s.tail) })), startNs: t0, endNs: t1,
        objects: new Set(spans.map(s => s.key)).size, tailRows: spans.filter(s => s.tail).length }
    },
  }
}

// ---- metrics ----------------------------------------------------------------

/** A gauge's points in [fromNs, toNs), bucketed by `stepNs`, one series per service. */
export function metricChart({ metric, fromNs, toNs, stepNs }) {
  const q = { metric, fromNs, toNs }
  q.stepNs = stepNs ?? niceStep(fromNs, toNs, 60)
  return {
    kind: 'metric',
    signal: 'metrics_gauge',
    key: JSON.stringify(['metric', metric, String(fromNs), String(toNs), String(stepNs ?? '')]),
    q,
    async scan(file, obj, md) {
      const out = { series: new Map(), points: 0, key: obj.key }
      if (!objectMayOverlap(obj, q)) return out
      const byTime = new Set(pruneByRange(md, 'TimeUnix', q.fromNs, q.toNs - 1n).map(g => g.index))
      for (const g of pruneByRange(md, 'MetricName', metric, metric)) {
        if (!byTime.has(g.index)) continue
        const rows = await readGroup(file, obj.key, md, g, ['MetricName', 'TimeUnix', 'Value', 'ServiceName'])
        for (const r of rows) {
          if (r.MetricName !== metric || r.TimeUnix < q.fromNs || r.TimeUnix >= q.toNs) continue
          const b = bucketIndex(q, q.stepNs, r.TimeUnix)
          const svc = r.ServiceName ?? ''
          if (!out.series.has(svc)) out.series.set(svc, new Map())
          const s = out.series.get(svc)
          const v = Number(r.Value)
          const a = s.get(b) ?? { n: 0, sum: 0, min: Infinity, max: -Infinity }
          a.n++
          a.sum += v
          a.min = Math.min(a.min, v)
          a.max = Math.max(a.max, v)
          s.set(b, a)
          out.points++
        }
      }
      return out
    },
    merge(parts, label) {
      const bs = buckets({ ...label, fromNs: q.fromNs, toNs: q.toNs }, q.stepNs)
      const all = new Map()
      let points = 0
      let sum = 0
      let tailRows = 0
      const tailB = new Set() // buckets holding a tail point (any series)
      for (const p of parts) {
        points += p.points
        if (p.tail) tailRows += p.points
        for (const [svc, m] of p.series) {
          if (!all.has(svc)) all.set(svc, new Map())
          const s = all.get(svc)
          for (const [b, a] of m) {
            sum += a.sum
            const c = s.get(b) ?? { n: 0, sum: 0, min: Infinity, max: -Infinity, tail: false }
            c.n += a.n
            c.sum += a.sum
            c.min = Math.min(c.min, a.min)
            c.max = Math.max(c.max, a.max)
            if (p.tail) {
              c.tail = true
              tailB.add(b)
            }
            s.set(b, c)
          }
        }
      }
      const series = [...all.keys()].sort().map(svc => ({
        service: svc,
        points: [...all.get(svc).entries()].sort((a, b) => a[0] - b[0]).map(([b, a]) => ({
          fromNs: bs[b].fromNs, toNs: bs[b].toNs, avg: a.sum / a.n, min: a.min, max: a.max, n: a.n,
          state: bucketState(label, bs[b].fromNs, bs[b].toNs, a.tail),
        })),
      }))
      return { metric, points, sum, tailRows, series,
        buckets: bs.map((b, i) => ({ ...b, state: bucketState(label, b.fromNs, b.toNs, tailB.has(i)) })) }
    },
  }
}
