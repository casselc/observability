import { test } from 'node:test'
import assert from 'node:assert/strict'
import { buckets, incompleteStart } from '../src/completeness.js'
import { esc, histogramSVG, lineSVG } from '../src/charts.js'

const label = over => {
  const l = { state: 'partial', fromNs: 0n, toNs: 100_000_000_000n, completeThroughNs: 60_000_000_000n, incompleteFromNs: 60_000_000_000n, startComplete: true, ...over }
  return { ...l, incompleteStartNs: incompleteStart(l) }
}

test('histogram: settled bars solid, bars from incomplete_from on hatched, a complete-through marker', () => {
  const l = label()
  const bs = buckets(l, 10_000_000_000n).map((b, i) => ({ ...b, count: i + 1 }))
  const svg = histogramSVG(bs, l)
  assert.equal((svg.match(/data-state="complete"/g) ?? []).length, 6)
  assert.equal((svg.match(/data-state="incomplete"/g) ?? []).length, 4)
  assert.match(svg, /data-marker="complete-through"/)
  assert.match(svg, /data-region="incomplete"/)
  for (const m of svg.matchAll(/<g class="bar" data-state="incomplete"[^]*?<\/g>/g)) assert.match(m[0], /url\(#hatch-incomplete\)/)
})

test('unknown: no solid mark and no complete-through marker', () => {
  const l = label({ state: 'unknown' })
  const bs = buckets(l, 10_000_000_000n).map(b => ({ ...b, count: 3 }))
  const svg = histogramSVG(bs, l)
  assert.doesNotMatch(svg, /fill="var\(--series-1\)"/)
  assert.doesNotMatch(svg, /complete-through/)
  assert.match(svg, /completeness unknown/)
})

test('line: solid through settled buckets, dashed after; text is escaped', () => {
  const l = label()
  const bs = buckets(l, 10_000_000_000n)
  const series = [{ service: '<svc>', points: bs.map((b, i) => ({ ...b, avg: i, min: i, max: i, n: 1 })) }]
  const svg = lineSVG(series, bs, l)
  assert.match(svg, /<path d="M[^"]+" [^>]*data-state="complete"/)
  assert.match(svg, /stroke-dasharray="5 4" data-state="incomplete"/)
  assert.doesNotMatch(svg, /<svc>/)
  assert.equal(esc('<a href="x">'), '&lt;a href=&quot;x&quot;&gt;')
})
