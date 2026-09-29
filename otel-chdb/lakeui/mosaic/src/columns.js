// What the dashboard loads into DuckDB, as query objects for lakeui's
// engine.execute (plan → range-read → merge, with X8's re-plan rules): the
// column chunks of the planned objects that the charts need, and nothing
// else, as plain columns. The completeness of every row and bucket is decided
// HERE, in exact BigInt nanoseconds, by lakeui's completeness.js; DuckDB only
// ever sees the decision (`cstate`, `bstate`), never the rule. Rows of the
// plan's tail (received after the basis: the engine marks their parts) are
// incomplete, and so is every bucket holding one (completeness.js).
//
// Pure apart from the reads it is handed (lakeui/src/parquet.js).

import { bucketIndex, bucketState, niceStep, rowState } from '../../src/completeness.js'
import { mapGet, pruneByRange, readGroup } from '../../src/parquet.js'

/** The columns each signal reads (ClickStack names, FORMAT.md) and what it keeps. */
export const SIGNALS = {
  logs: {
    read: ['Timestamp', 'SeverityText', 'ServiceName', 'ResourceAttributes'],
    keep: r => ({ severity: String(r.SeverityText ?? '').toUpperCase() || 'UNSET' }),
    extra: { severity: 'utf8' },
  },
  traces: {
    read: ['Timestamp', 'ServiceName', 'SpanName', 'Duration', 'StatusCode', 'ResourceAttributes'],
    keep: r => ({ name: String(r.SpanName ?? ''), status: String(r.StatusCode ?? ''), dur_ms: Number(r.Duration ?? 0) / 1e6 }),
    extra: { name: 'utf8', status: 'utf8', dur_ms: 'float64' },
  },
}

/** The columns every table has, so one selection clause can filter both (time bucket, pod, service). */
export const COMMON = { ts_ns: 'int64', ts: 'timestamp', b0: 'timestamp', b1: 'timestamp', service: 'utf8', pod: 'utf8', cluster: 'utf8', cstate: 'utf8', bstate: 'utf8' }

function emptyColumns(signal) {
  const cols = {}
  for (const k of [...Object.keys(COMMON), ...Object.keys(SIGNALS[signal].extra)]) cols[k] = []
  return cols
}

/**
 * The rows of one signal in [fromNs, toNs) with the dashboard's columns.
 * Buckets are lakeui's: `stepNs` wide, aligned to fromNs, the last cut at toNs.
 */
export function columnsQuery({ signal, fromNs, toNs, stepNs }) {
  const spec = SIGNALS[signal]
  if (!spec) throw new RangeError(`columnsQuery: no signal ${signal}`)
  const q = { fromNs, toNs, stepNs: stepNs ?? niceStep(fromNs, toNs, 60) }
  return {
    kind: 'columns',
    signal,
    q,
    async scan(file, obj, md) {
      const rows = []
      if (obj.minTimeNs !== null && obj.maxTimeNs !== null && obj.minTimeNs !== undefined &&
        (obj.maxTimeNs < fromNs || obj.minTimeNs >= toNs)) return { key: obj.key, rows }
      for (const g of pruneByRange(md, 'Timestamp', fromNs, toNs - 1n)) {
        for (const r of await readGroup(file, obj.key, md, g, spec.read)) {
          if (r.Timestamp < fromNs || r.Timestamp >= toNs) continue
          rows.push({
            tsNs: r.Timestamp,
            service: String(r.ServiceName ?? ''),
            pod: String(mapGet(r.ResourceAttributes, 'k8s.pod.name') ?? ''),
            cluster: String(mapGet(r.ResourceAttributes, 'k8s.cluster.name') ?? obj.cluster ?? ''),
            ...spec.keep(r),
          })
        }
      }
      return { key: obj.key, rows }
    },
    /** Columns for Arrow, every row labelled by the plan's label (and the tail's rows incomplete). */
    merge(parts, label) {
      return toColumns(signal, parts.flatMap(p => (p.tail ? p.rows.map(r => ({ ...r, tail: true })) : p.rows)), label, q)
    },
  }
}

/** Rows → columns; ts/b0/b1 in ms (for the charts), ts_ns exact, states from the label; `tail` rows incomplete. */
export function toColumns(signal, rows, label, q) {
  const cols = emptyColumns(signal)
  const extra = Object.keys(SIGNALS[signal].extra)
  const tailB = new Set()
  for (const r of rows) if (r.tail) tailB.add(bucketIndex(q, q.stepNs, r.tsNs))
  for (const r of rows) {
    const b = bucketIndex(q, q.stepNs, r.tsNs)
    const b0 = q.fromNs + BigInt(b) * q.stepNs
    const b1 = b0 + q.stepNs < q.toNs ? b0 + q.stepNs : q.toNs
    cols.ts_ns.push(r.tsNs)
    cols.ts.push(nsToMsFloor(r.tsNs))
    cols.b0.push(nsToMsFloor(b0))
    cols.b1.push(nsToMsFloor(b1))
    cols.service.push(r.service)
    cols.pod.push(r.pod)
    cols.cluster.push(r.cluster)
    cols.cstate.push(rowState(label, r.tsNs, r.tail === true))
    cols.bstate.push(bucketState(label, b0, b1, tailB.has(b)))
    for (const k of extra) cols[k].push(r[k])
  }
  return { signal, rows: rows.length, tailRows: rows.filter(r => r.tail).length, columns: cols, q }
}

/** ms for a chart axis; floor, so a row is never drawn after its instant. */
export function nsToMsFloor(ns) {
  const ms = ns / 1_000_000n
  return Number(ns < 0n && ms * 1_000_000n !== ns ? ms - 1n : ms)
}
