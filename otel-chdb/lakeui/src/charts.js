// SVG for the three views, as strings (pure: tested in Node). Settled marks
// are solid; marks at or after incomplete_from are hatched and lighter, with
// the complete-through line drawn and labelled; an unknown label hatches
// everything grey. State is never colour alone: hatching, a line, a legend
// entry and the banner's words carry it too.

import { shortTime } from './ns.js'

export const esc = s => String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c])

const W = 760
const H = 180
const PAD = { l: 44, r: 12, t: 22, b: 24 }

const defs = `<defs>
<pattern id="hatch-incomplete" width="6" height="6" patternUnits="userSpaceOnUse" patternTransform="rotate(45)"><rect width="6" height="6" fill="var(--inc-fill)"/><line x1="0" y1="0" x2="0" y2="6" stroke="var(--series-1)" stroke-width="2"/></pattern>
<pattern id="hatch-unknown" width="6" height="6" patternUnits="userSpaceOnUse" patternTransform="rotate(135)"><rect width="6" height="6" fill="var(--unk-fill)"/><line x1="0" y1="0" x2="0" y2="6" stroke="var(--muted)" stroke-width="2"/></pattern>
</defs>`

function xScale(fromNs, toNs) {
  const span = Number(toNs - fromNs)
  return ns => PAD.l + (W - PAD.l - PAD.r) * (Number(BigInt(ns) - fromNs) / span)
}

function timeTicks(fromNs, toNs, x, n = 5) {
  let s = ''
  for (let i = 0; i <= n; i++) {
    const t = fromNs + ((toNs - fromNs) * BigInt(i)) / BigInt(n)
    const px = x(t)
    s += `<line x1="${px}" x2="${px}" y1="${H - PAD.b}" y2="${H - PAD.b + 4}" stroke="var(--axis)"/>`
    s += `<text x="${px}" y="${H - 6}" text-anchor="${i === 0 ? 'start' : i === n ? 'end' : 'middle'}" class="tick">${shortTime(t).slice(0, 8)}</text>`
  }
  return s
}

/** The complete-through marker and the shaded unsettled region. */
function settledMarks(label, x) {
  if (label.state === 'unknown') {
    return `<rect x="${PAD.l}" y="${PAD.t}" width="${W - PAD.l - PAD.r}" height="${H - PAD.t - PAD.b}" fill="url(#hatch-unknown)" opacity="0.35"/>` +
      `<text x="${W - PAD.r - 4}" y="14" text-anchor="end" class="annot">completeness unknown</text>`
  }
  const s = label.incompleteStartNs
  if (s === null || s === undefined) return ''
  const px = x(s)
  return `<rect x="${px}" y="${PAD.t}" width="${Math.max(0, W - PAD.r - px)}" height="${H - PAD.t - PAD.b}" fill="var(--inc-region)" data-region="incomplete"/>` +
    `<line x1="${px}" x2="${px}" y1="${PAD.t}" y2="${H - PAD.b}" stroke="var(--text-secondary)" stroke-dasharray="4 3" data-marker="complete-through"/>` +
    (px > W / 2
      ? `<text x="${px - 4}" y="14" text-anchor="end" class="annot">complete through ${esc(shortTime(label.completeThroughNs ?? s))} · after: incomplete →</text>`
      : `<text x="${px + 4}" y="14" class="annot">← complete through ${esc(shortTime(label.completeThroughNs ?? s))} · after: incomplete</text>`)
}

/** Log volume histogram. buckets: [{fromNs,toNs,count,state}] */
export function histogramSVG(buckets, label) {
  if (!buckets.length) return ''
  const fromNs = buckets[0].fromNs
  const toNs = buckets[buckets.length - 1].toNs
  const x = xScale(fromNs, toNs)
  const max = Math.max(1, ...buckets.map(b => b.count))
  const y = v => H - PAD.b - (H - PAD.t - PAD.b) * (v / max)
  let bars = ''
  for (const b of buckets) {
    const x0 = x(b.fromNs) + 1
    const w = Math.max(1, x(b.toNs) - x(b.fromNs) - 2)
    const top = y(b.count)
    const fill = b.state === 'complete' ? 'var(--series-1)' : b.state === 'unknown' ? 'url(#hatch-unknown)' : 'url(#hatch-incomplete)'
    const tip = `${shortTime(b.fromNs)}–${shortTime(b.toNs)}: ${b.count} rows (${b.state === 'complete' ? 'complete' : b.state === 'unknown' ? 'completeness unknown' : 'incomplete: may grow'})`
    bars += `<g class="bar" data-state="${b.state}" data-count="${b.count}"><title>${esc(tip)}</title>` +
      `<rect x="${x0}" y="${PAD.t}" width="${w}" height="${H - PAD.t - PAD.b}" fill="transparent"/>` +
      (b.count ? `<rect x="${x0}" y="${top}" width="${w}" height="${H - PAD.b - top}" rx="2" fill="${fill}"/>` : '') + '</g>'
  }
  return `<svg viewBox="0 0 ${W} ${H}" class="chart" role="img" aria-label="log volume over time">${defs}` +
    `<line x1="${PAD.l}" x2="${W - PAD.r}" y1="${H - PAD.b}" y2="${H - PAD.b}" stroke="var(--axis)"/>` +
    `<text x="${PAD.l - 6}" y="${PAD.t + 8}" text-anchor="end" class="tick">${max}</text><text x="${PAD.l - 6}" y="${H - PAD.b}" text-anchor="end" class="tick">0</text>` +
    settledMarks(label, x) + bars + timeTicks(fromNs, toNs, x) + '</svg>'
}

/** Gauge series: [{service, points:[{fromNs,toNs,avg,min,max,n,state}]}] over buckets. */
export function lineSVG(series, bucketsAll, label) {
  if (!bucketsAll.length) return ''
  const fromNs = bucketsAll[0].fromNs
  const toNs = bucketsAll[bucketsAll.length - 1].toNs
  const x = xScale(fromNs, toNs)
  const vals = series.flatMap(s => s.points.map(p => p.avg))
  const lo = vals.length ? Math.min(...vals) : 0
  const hi = vals.length ? Math.max(...vals) : 1
  const pad = (hi - lo) * 0.1 || 1
  const y = v => H - PAD.b - (H - PAD.t - PAD.b) * ((v - (lo - pad)) / (hi - lo + 2 * pad))
  let lines = ''
  series.forEach((s, i) => {
    const color = `var(--series-${(i % 3) + 1})`
    const mid = p => x((p.fromNs + p.toNs) / 2n)
    // settled points joined solid; from the first unsettled point on, dashed
    let solid = ''
    let dashed = ''
    let prev = null
    for (const p of s.points) {
      const pt = `${mid(p).toFixed(1)},${y(p.avg).toFixed(1)}`
      if (p.state === 'complete') solid += (solid ? ' L' : 'M') + pt
      else {
        if (!dashed) dashed = prev && prev.state === 'complete' ? `M${mid(prev).toFixed(1)},${y(prev.avg).toFixed(1)} L${pt}` : 'M' + pt
        else dashed += ' L' + pt
      }
      prev = p
    }
    if (solid) lines += `<path d="${solid}" fill="none" stroke="${color}" stroke-width="2" data-state="complete"/>`
    if (dashed) lines += `<path d="${dashed}" fill="none" stroke="${color}" stroke-width="2" stroke-dasharray="5 4" data-state="${label.state === 'unknown' ? 'unknown' : 'incomplete'}"/>`
    for (const p of s.points) {
      lines += `<circle cx="${mid(p).toFixed(1)}" cy="${y(p.avg).toFixed(1)}" r="4" fill="${p.state === 'complete' ? color : 'var(--surface-1)'}" stroke="${color}" stroke-width="2" data-state="${p.state}">` +
        `<title>${esc(`${s.service} ${shortTime(p.fromNs)}: avg ${p.avg.toFixed(2)} (min ${p.min}, max ${p.max}, ${p.n} points) — ${p.state}`)}</title></circle>`
    }
  })
  return `<svg viewBox="0 0 ${W} ${H}" class="chart" role="img" aria-label="metric over time">${defs}` +
    `<line x1="${PAD.l}" x2="${W - PAD.r}" y1="${H - PAD.b}" y2="${H - PAD.b}" stroke="var(--axis)"/>` +
    `<text x="${PAD.l - 6}" y="${PAD.t + 8}" text-anchor="end" class="tick">${(hi + pad).toFixed(1)}</text><text x="${PAD.l - 6}" y="${H - PAD.b}" text-anchor="end" class="tick">${(lo - pad).toFixed(1)}</text>` +
    settledMarks(label, x) + lines + timeTicks(fromNs, toNs, x) + '</svg>'
}

export function legendHTML(series) {
  return '<div class="legend">' + series.map((s, i) =>
    `<span><i style="background:var(--series-${(i % 3) + 1})"></i>${esc(s.service || '(no service)')}</span>`).join('') +
    '<span><i class="solid"></i>complete</span><span><i class="dashed"></i>incomplete (may change)</span></div>'
}

/** Trace waterfall rows: [{ts, durationNs, name, service, state}] */
export function waterfallHTML(trace) {
  if (!trace.spans.length) return ''
  const span = Number(trace.endNs - trace.startNs) || 1
  return '<div class="waterfall">' + trace.spans.map(s => {
    const left = (100 * Number(s.ts - trace.startNs)) / span
    const width = Math.max(0.5, (100 * Number(s.durationNs)) / span)
    return `<div class="span-row" data-state="${s.state}"><span class="span-name">${esc(s.service)} · ${esc(s.name)}</span>` +
      `<span class="span-track"><span class="span-bar ${s.state}" style="left:${left.toFixed(2)}%;width:${width.toFixed(2)}%" title="${esc(`${shortTime(s.ts)} ${Number(s.durationNs) / 1e6} ms — ${s.state}`)}"></span></span></div>`
  }).join('') + '</div>'
}
