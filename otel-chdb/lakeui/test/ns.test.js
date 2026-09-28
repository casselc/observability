import { test } from 'node:test'
import assert from 'node:assert/strict'
import fc from 'fast-check'
import { formatTimeNs, parseJSONNs, parseJSONNsFallback, parseTimeNs, stringifyNs } from '../src/ns.js'

test('RFC 3339 to ns and back, exactly', () => {
  assert.equal(parseTimeNs('2026-09-28T14:18:01.415760575Z'), 1790605081415760575n)
  assert.equal(formatTimeNs(1790605081415760575n), '2026-09-28T14:18:01.415760575Z')
  assert.equal(parseTimeNs('2026-09-28T13:57:43Z'), 1790603863000000000n)
  assert.equal(parseTimeNs('2026-09-28T16:18:01.5+02:00'), parseTimeNs('2026-09-28T14:18:01.500Z'))
  assert.throws(() => parseTimeNs('yesterday'))
  assert.throws(() => parseTimeNs('2026-09-28 14:18'))
})

test('property: format then parse is the identity on ns', () => {
  fc.assert(fc.property(fc.bigInt({ min: 0n, max: 4102444800n * 10n ** 9n }), ns => parseTimeNs(formatTimeNs(ns)) === ns), { numRuns: 2000 })
})

test('*_ns integers survive JSON exactly (above 2^53)', () => {
  const t = '{"complete_through_ns": 1790605081415760575, "objects":[{"min_time_ns":1790603863000000001,"size":5}], "note":"x_ns\\":1"}'
  const v = parseJSONNs(t)
  assert.equal(v.complete_through_ns, 1790605081415760575n)
  assert.equal(v.objects[0].min_time_ns, 1790603863000000001n)
  assert.equal(v.objects[0].size, 5)
  assert.deepEqual(parseJSONNsFallback(t), v)
})

test('property: the source-text reviver and the fallback agree', () => {
  const doc = fc.record({
    a_ns: fc.bigInt({ min: -(2n ** 63n), max: 2n ** 63n - 1n }),
    note: fc.string(),
    nested: fc.array(fc.record({ min_time_ns: fc.bigInt({ min: 0n, max: 2n ** 63n - 1n }), size: fc.nat(), label: fc.string() }), { maxLength: 4 }),
    maybe_ns: fc.constantFrom(null, 'text'),
  })
  fc.assert(fc.property(doc, d => {
    const text = stringifyNs(d)
    const a = parseJSONNs(text)
    const b = parseJSONNsFallback(text)
    assert.equal(stringifyNs(a), text)
    assert.equal(stringifyNs(b), text)
    assert.equal(typeof a.a_ns, 'bigint')
  }), { numRuns: 500 })
})
