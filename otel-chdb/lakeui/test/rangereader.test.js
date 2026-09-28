import { test } from 'node:test'
import assert from 'node:assert/strict'
import { newStats, ObjectReadError, rangeBuffer } from '../src/rangereader.js'
import { fakeStore } from './helpers.js'

const body = Uint8Array.from({ length: 1000 }, (_, i) => i % 251)
const obj = { key: 'k', url: 'u', size: 1000 }

test('range reads: exact bytes, counted, no HEAD, contained slices served locally', async () => {
  const s = fakeStore(new Map([['u', body]]))
  const stats = newStats()
  const f = rangeBuffer(obj, { fetch: s.fetch, stats })
  assert.equal(f.byteLength, 1000)
  const a = new Uint8Array(await f.slice(900))
  assert.deepEqual(a, body.slice(900))
  const b = new Uint8Array(await f.slice(950, 960)) // inside the first read
  assert.deepEqual(b, body.slice(950, 960))
  assert.equal(stats.requests, 1)
  assert.equal(stats.bytes, 100)
  await f.slice(0, 10)
  assert.deepEqual(s.log.map(l => l.range), ['bytes=900-999', 'bytes=0-9'])
  assert.equal(stats.perKey.get('k').bytes, 110)
})

test('a store that ignores Range: the whole object is fetched once and counted', async () => {
  const s = fakeStore(new Map([['u', body]]))
  s.ignoreRange = true
  const stats = newStats()
  const f = rangeBuffer(obj, { fetch: s.fetch, stats })
  assert.deepEqual(new Uint8Array(await f.slice(10, 20)), body.slice(10, 20))
  assert.deepEqual(new Uint8Array(await f.slice(500, 510)), body.slice(500, 510))
  assert.equal(stats.requests, 1)
  assert.equal(stats.bytes, 1000)
})

test('failures are errors of their kind, never short buffers', async () => {
  const s = fakeStore(new Map([['u', body]]))
  s.expired.add('u')
  await assert.rejects(rangeBuffer(obj, { fetch: s.fetch }).slice(0, 10), e => e instanceof ObjectReadError && e.kind === 'forbidden' && e.status === 403)
  await assert.rejects(rangeBuffer({ ...obj, url: 'gone' }, { fetch: s.fetch }).slice(0, 10), e => e.kind === 'missing')
  s.failNetwork.add('n')
  await assert.rejects(rangeBuffer({ ...obj, url: 'n' }, { fetch: s.fetch }).slice(0, 10), e => e.kind === 'network')
  // the object is not the size the plan says
  const s2 = fakeStore(new Map([['u', body.slice(0, 999)]]))
  await assert.rejects(rangeBuffer(obj, { fetch: s2.fetch }).slice(0, 10), e => e.kind === 'size_mismatch')
  s2.ignoreRange = true
  await assert.rejects(rangeBuffer(obj, { fetch: s2.fetch }).slice(0, 10), e => e.kind === 'size_mismatch')
  // a 206 with fewer bytes than asked
  const short = async () => new Response(new Uint8Array(5), { status: 206, headers: { 'content-range': 'bytes 0-9/1000' } })
  await assert.rejects(rangeBuffer(obj, { fetch: short }).slice(0, 10), e => e.kind === 'short')
  await assert.rejects(rangeBuffer(obj, { fetch: s.fetch }).slice(0, 2000), e => e.kind === 'decode')
})
