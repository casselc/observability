import { test } from 'node:test'
import assert from 'node:assert/strict'
import fc from 'fast-check'
import { bannerText, bucketState, buckets, incompleteStart, resultState, rowState, segments } from '../src/completeness.js'

const L = (over) => ({ state: 'partial', fromNs: 0n, toNs: 1000n, completeThroughNs: 600n, incompleteFromNs: 600n, startComplete: true, watermarkStatus: 'ok', ...over })

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
  return { state, fromNs: r.fromNs, toNs, completeThroughNs: state === 'unknown' ? r.ct : r.ct, incompleteFromNs: inc, startComplete: r.startComplete, watermarkStatus: 'ok' }
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
