// The dashboard as vgplot specs over two DuckDB tables, `logs` and `spans`
// (columns.js), cross-filtered:
//
//   A  log volume over time, stacked by severity      brush (time bucket) → $cf
//   B  logs by pod, stacked by row state              click (pod)         → $cf
//   C  span latency histogram, stacked by row state   brush (dur_ms)      → $lat
//   D  spans by pod, stacked by row state             filtered by $cf ∧ $lat
//
// $cf is a crossfilter selection over columns both tables have (b0, pod), so
// one brush filters logs and spans alike, and no chart filters itself. Every
// chart carries the completeness layer (overlay.js).
//
// `vg` is @uwdata/vgplot (the page passes the vendored bundle's).

import { PATTERN_DEFS, stateFill, timeLayer } from './overlay.js'

export const SEVERITIES = ['DEBUG', 'INFO', 'WARN', 'ERROR', 'UNSET']

/**
 * @param {any} vg
 * @param {{logs: object, spans: object}} labels  labelOf(plan) per table
 * @param {{logs: bigint, spans: bigint}} steps   the bucket step per table (ns)
 * @returns {{charts: Record<string, HTMLElement>, selections: {cf: any, lat: any}, defs: string}}
 */
export function buildDashboard(vg, labels, steps, { width = 560 } = {}) {
  const $cf = vg.Selection.crossfilter()
  const $lat = vg.Selection.intersect()
  const $spanView = vg.Selection.intersect({ include: [$cf, $lat] })
  const fromMs = new Date(Number(labels.logs.fromNs / 1_000_000n))
  const toMs = new Date(Number(labels.logs.toNs / 1_000_000n))
  const layer = timeLayer(labels.logs, steps.logs)
  const fill = stateFill()
  const bandFill = labels.logs.state === 'unknown' ? 'url(#mos-unknown)' : 'url(#mos-band)'

  const A = vg.plot(
    vg.rectY(vg.from('logs', { filterBy: $cf }), { x1: 'b0', x2: 'b1', y: vg.count(), fill: 'severity', inset: 0.5 }),
    vg.rect(layer.band, { x1: 'x1', x2: 'x2', fill: bandFill, fillOpacity: labels.logs.state === 'unknown' ? 0.6 : 1 }),
    vg.ruleX(layer.marker, { x: 'x', stroke: '#c0392b', strokeDasharray: '4,3' }),
    vg.text(layer.marker, { x: 'x', text: 'text', frameAnchor: 'top', textAnchor: 'end', dx: -4, dy: 4, fill: '#c0392b', stroke: 'white', strokeWidth: 3, paintOrder: 'stroke' }),
    vg.intervalX({ as: $cf, field: 'b0' }),
    vg.xScale('utc'), vg.xDomain([fromMs, toMs]), vg.xLabel('event time (bucket)'), vg.yLabel('logs'),
    vg.colorDomain(SEVERITIES), vg.colorRange(['#9aa5b1', '#4269d0', '#efb118', '#ff725c', '#6c6c6c']),
    vg.name('logsTime'),
    vg.width(width), vg.height(200), vg.marginLeft(50),
  )
  const B = vg.plot(
    vg.barX(vg.from('logs', { filterBy: $cf }), { x: vg.count(), y: 'pod', fill: 'cstate' }),
    // no vg.highlight here: it selects the predicate as a column, which is
    // invalid SQL when the predicate's column (b0) is not a group key [M]
    vg.toggleY({ as: $cf }),
    vg.colorDomain(fill.domain), vg.colorRange(fill.range),
    vg.xLabel('logs'), vg.yLabel('pod'), vg.name('logsPod'),
    vg.width(width), vg.height(140), vg.marginLeft(70),
  )
  const C = vg.plot(
    vg.rectY(vg.from('spans', { filterBy: $cf }), { x: vg.bin('dur_ms', { steps: 40 }), y: vg.count(), fill: 'cstate', inset: 0.5 }),
    vg.intervalX({ as: $lat }),
    vg.xScale('log'), vg.xLabel('span duration (ms, log)'), vg.yLabel('spans'),
    vg.colorDomain(fill.domain), vg.colorRange(fill.range), vg.name('spanLatency'),
    vg.width(width), vg.height(180), vg.marginLeft(50),
  )
  const D = vg.plot(
    vg.barX(vg.from('spans', { filterBy: $spanView }), { x: vg.count(), y: 'pod', fill: 'cstate' }),
    vg.colorDomain(fill.domain), vg.colorRange(fill.range),
    vg.xLabel('spans'), vg.yLabel('pod'), vg.name('spansPod'),
    vg.width(width), vg.height(140), vg.marginLeft(70),
  )
  return { charts: { A, B, C, D }, selections: { cf: $cf, lat: $lat }, defs: PATTERN_DEFS, layer }
}
