// The completeness layer every chart carries. Mosaic plots are specs of
// marks; a mark's data can be a plain array instead of a table, so the
// layer is ordinary marks over arrays computed here from the plan's label
// (lakeui's completeness.js decides; this only shapes it for Plot):
//
//   - time charts: a hatched band from the first bucket that is not settled
//     (a bucket that straddles incomplete_from is incomplete, as in lakeui)
//     to the window's end, a rule and a label at "settled through"; unknown
//     draws the whole window grey-hatched;
//   - other charts: bars are stacked by the rows' own state (`cstate`), and
//     the state's fill is a pattern, so an unsettled part of a bar is hatched
//     and an unknown one grey.
//
// Pure.

import { incompleteStart, settledThrough } from '../../src/completeness.js'
import { nsToMsFloor } from './columns.js'

export const STATES = ['complete', 'incomplete', 'unknown']

/** Fill per row state: a colour for settled rows, patterns (defs below) for the rest. */
export function stateFill(settled = '#4269d0') {
  return { domain: STATES, range: [settled, 'url(#mos-hatch)', 'url(#mos-unknown)'] }
}

/** SVG defs for the patterns; put once in the page (url(#id) resolves document-wide). */
export const PATTERN_DEFS = `<svg width="0" height="0" style="position:absolute" aria-hidden="true"><defs>
<pattern id="mos-hatch" patternUnits="userSpaceOnUse" width="6" height="6" patternTransform="rotate(45)">
<rect width="6" height="6" fill="#dbe4f7"/><line x1="0" y1="0" x2="0" y2="6" stroke="#4269d0" stroke-width="3"/></pattern>
<pattern id="mos-band" patternUnits="userSpaceOnUse" width="8" height="8" patternTransform="rotate(45)">
<line x1="0" y1="0" x2="0" y2="8" stroke="#c0392b" stroke-width="1.5" stroke-opacity="0.55"/></pattern>
<pattern id="mos-unknown" patternUnits="userSpaceOnUse" width="6" height="6" patternTransform="rotate(-45)">
<rect width="6" height="6" fill="#e6e6e6"/><line x1="0" y1="0" x2="0" y2="6" stroke="#8a8a8a" stroke-width="2"/></pattern>
</defs></svg>`

/**
 * The band and marker of a time chart with buckets of stepNs aligned to the
 * window start. Dates for Plot's utc scale.
 * @returns {{band: {x1: Date, x2: Date, state: string}[], marker: {x: Date, text: string}[]}}
 */
export function timeLayer(label, stepNs, fmt = ns => new Date(nsToMsFloor(ns)).toISOString().slice(11, 19)) {
  const d = ns => new Date(nsToMsFloor(ns))
  const { fromNs, toNs } = label
  if (label.state === 'unknown') {
    return { band: [{ x1: d(fromNs), x2: d(toNs), state: 'unknown' }], marker: [{ x: d(toNs), text: 'completeness unknown' }] }
  }
  const s = incompleteStart(label)
  const marker = []
  const st = settledThrough(label)
  if (st !== null && st >= fromNs && st < toNs) marker.push({ x: d(st), text: `settled through ${fmt(st)}` })
  if (s === null) return { band: [], marker }
  // the first bucket bucketState calls incomplete is the one holding s (the
  // unit test checks this against bucketState for every bucket)
  const b0 = fromNs + ((s - fromNs) / stepNs) * stepNs
  if (marker.length === 0) marker.push({ x: d(s), text: 'incomplete from here' })
  return { band: [{ x1: d(b0), x2: d(toNs), state: 'incomplete' }], marker }
}
