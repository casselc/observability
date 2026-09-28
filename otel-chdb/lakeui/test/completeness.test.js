import { test } from 'node:test'
import assert from 'node:assert/strict'
import fc from 'fast-check'
import { bannerText, bucketState, buckets, incompleteStart, resultState, rowState, segments } from '../src/completeness.js'

const L = (over) => ({ state: 'partial', fromNs: 0n, toNs: 1000n, completeThroughNs: 600n, incompleteFromNs: 600n, maxLatenessNs: 0n, startComplete: true, watermarkStatus: 'ok', ...over })

test('the banner names the scope complete_through is for, and the lanes holding it (D29)', () => {
  const t = bannerText(L({ scope: { clusters: ['prod-a'], signals: ['logs'] }, holding: [{ lane: 'prod-a/pub-0/logs', lag_s: 41.2 }] })).text
  assert.match(t, /Complete through 600 \(clusters: prod-a; signals: logs\); incomplete from 600/)
  assert.match(t, /held by prod-a\/pub-0\/logs \(41 s behind\)/)
  assert.match(bannerText(L({ scope: { clusters: ['*'], signals: ['*'] } })).text, /\(clusters: all; signals: all\)/)
  // settled: no holding lanes shown
  assert.doesNotMatch(bannerText(L({ state: 'complete', toNs: 600n, holding: [{ lane: 'x/y/logs', lag_s: 1 }] })).text, /held by/)
  // a service before D29: no scope, the banner as before
  assert.match(bannerText(L()).text, /Complete through 600; incomplete from 600/)
})

test('examples: complete, partial, unknown', () => {
  assert.deepEqual(segments(L({ state: 'complete', toNs: 600n })), [{ fromNs: 0n, toNs: 600n, state: 'complete' }])
  assert.deepEqual(segments(L()), [{ fromNs: 0n, toNs: 600n, state: 'complete' }, { fromNs: 600n, toNs: 1000n, state: 'incomplete' }])
  assert.deepEqual(segments(L({ state: 'unknown' })), [{ fromNs: 0n, toNs: 1000n, state: 'unknown' }])
  // complete_through before the window: all of it is incomplete
  assert.deepEqual(segments(L({ completeThroughNs: 10n, incompleteFromNs: 100n, fromNs: 100n })), [{ fromNs: 100n, toNs: 1000n, state: 'incomplete' }])
  assert.equal(rowState(L(), 599n), 'complete')
  assert.equal(rowState(L(), 600n), 'incomplete')
  assert.equal(bucketState(L(), 500n, 600n), 'complete')
  assert.equal(bucketState(L(), 500n, 601n), 'incomplete') // straddles: its count may grow
  assert.equal(resultState(L({ state: 'complete', toNs: 600n })), 'complete')
  assert.equal(resultState(L({ state: 'complete', toNs: 600n }), { missing: ['k'] }), 'incomplete')
  assert.equal(resultState(L({ state: 'complete', toNs: 600n, startComplete: false })), 'incomplete')
  assert.match(bannerText(L({ state: 'unknown', watermarkStatus: 'stale' })).text, /UNKNOWN.*stale/)
  assert.match(bannerText(L(), { missing: ['a', 'b'] }).text, /INCOMPLETE: 2 planned object/)
  assert.match(bannerText(L()).text, /Complete through 600; incomplete from 600/)
})

// CAST row 26: complete_through is receive (custody) time; the window is
// event time. A row with event time 950 received at 1050 is not in central
// while complete_through is 1000, so [0, 1000) must not be drawn settled.
test('max_lateness: event time settles complete_through − max_lateness', () => {
  const late = L({ state: 'partial', completeThroughNs: 1000n, incompleteFromNs: 900n, maxLatenessNs: 100n })
  assert.equal(incompleteStart(late), 900n)
  assert.equal(rowState(late, 950n), 'incomplete')
  assert.equal(resultState(late), 'incomplete')
  assert.match(bannerText(late).text, /event time settled through 900 \(max lateness 1e-7 s\)/)
  // a label that says complete without the bound behind it is not believed
  const lying = L({ state: 'complete', completeThroughNs: 1000n, incompleteFromNs: null, maxLatenessNs: 100n })
  assert.equal(incompleteStart(lying), 900n)
  assert.equal(resultState(lying), 'incomplete')
  // nor one from a service that does not report max_lateness at all
  const old = L({ state: 'complete', completeThroughNs: 5000n, incompleteFromNs: null, maxLatenessNs: null })
  assert.deepEqual(segments(old), [{ fromNs: 0n, toNs: 1000n, state: 'incomplete' }])
  assert.match(bannerText(old).text, /did not report max_lateness/)
  // settled past the window's end: complete
  assert.equal(resultState(L({ state: 'complete', completeThroughNs: 1100n, incompleteFromNs: null, maxLatenessNs: 100n })), 'complete')
  assert.match(bannerText(L({ lateObjects: 2 })).text, /2 object\(s\) arrived later than max lateness/)
})

// labels as the fixed service makes them, max_lateness included
const lateLabel = fc.record({
  state: fc.constantFrom('complete', 'partial'),
  fromNs: fc.bigInt({ min: 0n, max: 10n ** 6n }),
  len: fc.bigInt({ min: 1n, max: 10n ** 6n }),
  ct: fc.bigInt({ min: 0n, max: 3n * 10n ** 6n }),
  ml: fc.option(fc.bigInt({ min: 0n, max: 10n ** 6n }), { nil: null }),
  honest: fc.boolean(),
}).map(r => {
  const toNs = r.fromNs + r.len
  const settled = r.ct - (r.ml ?? 0n)
  let state = r.state
  if (r.honest && state === 'complete' && settled < toNs) state = 'partial'
  const inc = state === 'partial' && r.honest ? (settled > r.fromNs ? settled : r.fromNs) : null
  return { state, fromNs: r.fromNs, toNs, completeThroughNs: r.ct, incompleteFromNs: inc, maxLatenessNs: r.ml, startComplete: true, watermarkStatus: 'ok' }
})

test('property: nothing at or after complete_through − max_lateness is drawn complete, whatever the label says', () => {
  fc.assert(fc.property(lateLabel, l => {
    const segs = segments(l)
    for (const s of segs) {
      if (s.state !== 'complete') continue
      assert.ok(l.maxLatenessNs !== null, 'no max_lateness, nothing settled')
      assert.ok(s.toNs <= l.completeThroughNs - l.maxLatenessNs, 'a complete segment ends at or before settled_through')
    }
    if (resultState(l) === 'complete') assert.ok(l.maxLatenessNs !== null && l.completeThroughNs - l.maxLatenessNs >= l.toNs)
  }), { numRuns: 3000 })
})

// arbitrary labels as the service can produce them (and some it shouldn't)
const label = fc.record({
  state: fc.constantFrom('complete', 'partial', 'unknown'),
  fromNs: fc.bigInt({ min: 0n, max: 10n ** 6n }),
  len: fc.bigInt({ min: 1n, max: 10n ** 6n }),
  ct: fc.option(fc.bigInt({ min: 0n, max: 3n * 10n ** 6n }), { nil: null }),
  startComplete: fc.boolean(),
}).map(r => {
  const toNs = r.fromNs + r.len
  let inc = null
  if (r.ct !== null && r.state !== 'unknown' && toNs > r.ct) inc = r.ct > r.fromNs ? r.ct : r.fromNs
  // a "complete" label from the service never has a window past complete_through
  const state = r.state === 'complete' && (r.ct === null || toNs > r.ct) ? 'partial' : r.state
  return { state, fromNs: r.fromNs, toNs, completeThroughNs: state === 'unknown' ? r.ct : r.ct, incompleteFromNs: inc, maxLatenessNs: 0n, startComplete: r.startComplete, watermarkStatus: 'ok' }
})

test('property: segments tile the window in order, and nothing after complete_through is complete', () => {
  fc.assert(fc.property(label, l => {
    const segs = segments(l)
    assert.equal(segs[0].fromNs, l.fromNs)
    assert.equal(segs[segs.length - 1].toNs, l.toNs)
    for (let i = 0; i < segs.length; i++) {
      assert.ok(segs[i].fromNs < segs[i].toNs)
      if (i) assert.equal(segs[i].fromNs, segs[i - 1].toNs)
      if (segs[i].state === 'complete') {
        assert.notEqual(l.state, 'unknown')
        assert.ok(l.completeThroughNs !== null && segs[i].toNs <= l.completeThroughNs, 'a complete segment ends at or before complete_through')
      }
    }
    if (l.state === 'unknown') assert.ok(segs.every(s => s.state === 'unknown'))
  }), { numRuns: 3000 })
})

test('property: rows and buckets agree with the segments; a complete bucket holds only complete rows', () => {
  fc.assert(fc.property(label, fc.bigInt({ min: 1n, max: 10n ** 5n }), fc.array(fc.bigInt({ min: 0n, max: 10n ** 6n }), { maxLength: 30 }), (l, step, offs) => {
    const segs = segments(l)
    const segAt = t => segs.find(s => s.fromNs <= t && t < s.toNs)
    for (const o of offs) {
      const t = l.fromNs + (o % (l.toNs - l.fromNs))
      assert.equal(rowState(l, t), segAt(t).state)
    }
    const bs = buckets(l, step)
    assert.equal(bs[0].fromNs, l.fromNs)
    assert.equal(bs[bs.length - 1].toNs, l.toNs)
    let seenIncomplete = false
    for (const b of bs) {
      if (b.state === 'complete') {
        assert.equal(seenIncomplete, false, 'complete buckets come first')
        assert.equal(rowState(l, b.fromNs), 'complete')
        assert.equal(rowState(l, b.toNs - 1n), 'complete')
      } else {
        seenIncomplete = true
        if (l.state !== 'unknown') assert.ok(incompleteStart(l) < b.toNs)
      }
    }
    // the total is complete only if every bucket is, the start is intact and nothing is missing
    const st = resultState(l)
    assert.equal(st === 'complete', bs.every(b => b.state === 'complete') && l.startComplete && l.state !== 'unknown')
    assert.notEqual(resultState(l, { missing: ['x'] }), 'complete')
  }), { numRuns: 2000 })
})
