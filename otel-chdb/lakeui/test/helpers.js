// A fake object store for the unit tests: serves byte ranges of in-memory
// objects at URLs, the way a presigned GET does, and can be told to answer 403
// (an expired URL), ignore Range, or fail.
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'

export const fixture = name => new Uint8Array(readFileSync(fileURLToPath(new URL('./fixtures/' + name, import.meta.url))))
export const truth = JSON.parse(readFileSync(fileURLToPath(new URL('./fixtures/truth.json', import.meta.url)), 'utf8'))

export function fakeStore(objects = new Map()) {
  const s = {
    objects, // url -> Uint8Array
    expired: new Set(), // urls answering 403
    ignoreRange: false,
    failNetwork: new Set(),
    log: [],
    fetch: async (url, init = {}) => {
      const range = init.headers?.Range ?? init.headers?.range
      s.log.push({ url, range })
      if (s.failNetwork.has(url)) throw new TypeError('fetch failed')
      if (s.expired.has(url)) return new Response('<Error><Code>AccessDenied</Code></Error>', { status: 403 })
      const body = s.objects.get(url)
      if (!body) return new Response('no such key', { status: 404 })
      const m = range && /^bytes=(\d+)-(\d+)$/.exec(range)
      if (!m || s.ignoreRange) return new Response(body.slice(), { status: 200, headers: { 'content-length': String(body.length) } })
      const a = +m[1]
      const b = Math.min(+m[2], body.length - 1)
      return new Response(body.slice(a, b + 1), { status: 206, headers: { 'content-range': `bytes ${a}-${b}/${body.length}` } })
    },
  }
  return s
}

/** A normalised plan over the given objects, as planclient.normalizePlan returns. */
export function planOf(objs, over = {}) {
  const fromNs = over.fromNs ?? 0n
  const toNs = over.toNs ?? 10n ** 19n
  return {
    requestId: over.requestId ?? 'r1', source: 'lake', signal: over.signal ?? 'logs', clusters: ['c'], fromNs, toNs,
    completeness: over.completeness ?? 'complete', partial: false, completeThroughNs: over.completeThroughNs ?? toNs,
    incompleteFromNs: over.incompleteFromNs ?? null, maxLatenessNs: over.maxLatenessNs ?? 0n, startComplete: true, watermark: { status: 'ok' },
    expiresAtMs: over.expiresAtMs ?? Infinity, replanAfterMs: over.replanAfterMs ?? Infinity, fetchedAtMs: 0,
    objects: objs.map(o => ({ minTimeNs: null, maxTimeNs: null, rows: null, refined: false, cluster: 'c', producer: 'p', ...o })),
    totalBytes: objs.reduce((a, o) => a + o.size, 0),
  }
}
