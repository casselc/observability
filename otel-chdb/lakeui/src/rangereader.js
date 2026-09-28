// The range reader: an AsyncBuffer (hyparquet's file interface) over one
// planned object's presigned URL. The plan carries the size, so it never
// HEADs (a URL presigned for GET answers HEAD with 403: research/lake-ui.md
// finding 2). Every byte it fetches is counted.
//
// Any failure is an ObjectReadError, never a short or empty buffer: the
// caller re-plans on it (X8 rule 2) and a view never renders a failed fetch
// as "no rows".

export class ObjectReadError extends Error {
  /**
   * @param {'forbidden'|'missing'|'http'|'network'|'short'|'size_mismatch'|'aborted'|'decode'} kind
   */
  constructor(kind, key, message, status = 0) {
    super(message)
    this.name = 'ObjectReadError'
    this.kind = kind
    this.key = key
    this.status = status
  }
}

/** Counters shared by every buffer of one query. */
export function newStats() {
  return { requests: 0, bytes: 0, failed: 0, perKey: new Map() }
}

function note(stats, key, bytes) {
  if (!stats) return
  stats.requests++
  stats.bytes += bytes
  const k = stats.perKey.get(key) ?? { requests: 0, bytes: 0 }
  k.requests++
  k.bytes += bytes
  stats.perKey.set(key, k)
}

/**
 * @param {{key: string, url: string, size: number}} obj  a planned object
 * @param {{fetch?: typeof fetch, stats?: ReturnType<typeof newStats>, signal?: AbortSignal}} [opts]
 */
export function rangeBuffer(obj, { fetch = globalThis.fetch, stats, signal } = {}) {
  const size = obj.size
  /** fetched ranges, [start, end) → ArrayBuffer; a slice inside one is served from it */
  const have = []
  let fullBody = null

  async function get(start, end) {
    let resp
    try {
      resp = await fetch(obj.url, { headers: { Range: `bytes=${start}-${end - 1}` }, signal, cache: 'no-store' })
    } catch (e) {
      if (stats) stats.failed++
      if (e?.name === 'AbortError') throw new ObjectReadError('aborted', obj.key, 'aborted')
      throw new ObjectReadError('network', obj.key, `GET ${obj.key}: ${e?.message ?? e}`)
    }
    if (resp.status === 206) {
      const cr = resp.headers.get('content-range') ?? ''
      const m = /^bytes (\d+)-(\d+)\/(\d+|\*)$/.exec(cr.trim())
      const buf = await resp.arrayBuffer()
      note(stats, obj.key, buf.byteLength)
      if (m && m[3] !== '*' && Number(m[3]) !== size) {
        if (stats) stats.failed++
        throw new ObjectReadError('size_mismatch', obj.key, `${obj.key}: the store says ${m[3]} bytes, the plan ${size}`, 206)
      }
      if (m && (Number(m[1]) !== start || Number(m[2]) !== end - 1)) {
        if (stats) stats.failed++
        throw new ObjectReadError('short', obj.key, `${obj.key}: asked ${start}-${end - 1}, got ${m[1]}-${m[2]}`, 206)
      }
      if (buf.byteLength !== end - start) {
        if (stats) stats.failed++
        throw new ObjectReadError('short', obj.key, `${obj.key}: ${buf.byteLength} bytes for a ${end - start}-byte range`, 206)
      }
      return buf
    }
    if (resp.status === 200) {
      // the store ignored Range: the whole object came back
      const buf = await resp.arrayBuffer()
      note(stats, obj.key, buf.byteLength)
      if (buf.byteLength !== size) {
        if (stats) stats.failed++
        throw new ObjectReadError('size_mismatch', obj.key, `${obj.key}: ${buf.byteLength} bytes, the plan says ${size}`, 200)
      }
      fullBody = buf
      return buf.slice(start, end)
    }
    try {
      await resp.arrayBuffer()
    } catch {
      // the body is irrelevant
    }
    if (stats) stats.failed++
    const kind = resp.status === 403 ? 'forbidden' : resp.status === 404 ? 'missing' : 'http'
    throw new ObjectReadError(kind, obj.key, `GET ${obj.key}: ${resp.status}${resp.status === 403 ? ' (expired or revoked URL: re-plan)' : ''}`, resp.status)
  }

  return {
    byteLength: size,
    async slice(start, end = size) {
      if (start < 0 || end > size || start > end) {
        throw new ObjectReadError('decode', obj.key, `${obj.key}: slice ${start}-${end} outside 0-${size}`)
      }
      if (start === end) return new ArrayBuffer(0)
      if (fullBody) return fullBody.slice(start, end)
      for (const h of have) {
        if (h.start <= start && end <= h.end) return h.buf.slice(start - h.start, end - h.start)
      }
      const buf = await get(start, end)
      if (!fullBody) have.push({ start, end, buf })
      return buf
    },
  }
}
