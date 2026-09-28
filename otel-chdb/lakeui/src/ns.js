// Nanosecond time, exactly. Event times are int64 nanoseconds; a JS number
// holds integers exactly only up to 2^53 (about 104 days of nanoseconds), so
// every *_ns value here is a BigInt, parsed from the text, never through a
// double.

const RFC3339 = /^(\d{4})-(\d{2})-(\d{2})[Tt ](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?([Zz]|[+-]\d{2}:\d{2})$/

/** RFC 3339 text (any fraction up to 9 digits, Z or an offset) → BigInt ns. */
export function parseTimeNs(s) {
  if (typeof s !== 'string') throw new TypeError(`time: not a string: ${s}`)
  const m = RFC3339.exec(s.trim())
  if (!m) throw new RangeError(`time: not RFC 3339: ${s}`)
  const [, y, mo, d, h, mi, se, frac = '', tz] = m
  let ms = Date.UTC(+y, +mo - 1, +d, +h, +mi, +se)
  if (Number.isNaN(ms)) throw new RangeError(`time: invalid: ${s}`)
  if (tz !== 'Z' && tz !== 'z') {
    const sign = tz[0] === '-' ? -1 : 1
    ms -= sign * (+tz.slice(1, 3) * 60 + +tz.slice(4, 6)) * 60000
  }
  return BigInt(ms) * 1_000_000n + BigInt((frac + '000000000').slice(0, 9))
}

/** BigInt ns → RFC 3339 UTC with 9 fraction digits (the service's format). */
export function formatTimeNs(ns) {
  ns = BigInt(ns)
  let sec = ns / 1_000_000_000n
  let frac = ns % 1_000_000_000n
  if (frac < 0n) {
    frac += 1_000_000_000n
    sec -= 1n
  }
  const iso = new Date(Number(sec) * 1000).toISOString().slice(0, 19)
  return `${iso}.${frac.toString().padStart(9, '0')}Z`
}

/** BigInt ns → a short local-free label (UTC, ms). */
export function shortTime(ns) {
  return formatTimeNs(ns).slice(11, 23)
}

export const msToNs = ms => BigInt(Math.trunc(ms)) * 1_000_000n
export const nsToMs = ns => Number(BigInt(ns) / 1_000_000n)

// Keys whose integer values are nanoseconds (or otherwise int64): they are
// read as BigInt from the JSON text itself.
const NS_KEY = /_ns$/

/**
 * JSON.parse, except that every integer under a key ending in `_ns` is a
 * BigInt taken from the source digits. Uses the reviver's source-text access
 * where the engine has it (Chrome 114+, Node 21+); otherwise quotes those
 * integers in the text first (a key ending `_ns` followed by an integer can
 * only occur at a key position: inside a JSON string every `"` is escaped).
 */
export function parseJSONNs(text) {
  let supported = false
  const out = JSON.parse(text, function (key, value, ctx) {
    if (ctx && typeof ctx.source === 'string') supported = true
    if (NS_KEY.test(key) && typeof value === 'number') {
      if (ctx && typeof ctx.source === 'string') return BigInt(ctx.source)
    }
    return value
  })
  if (supported || !/_ns"\s*:\s*-?\d/.test(text)) return out
  return parseJSONNsFallback(text)
}

export function parseJSONNsFallback(text) {
  const quoted = text.replace(/("[A-Za-z0-9_]*_ns"\s*:\s*)(-?\d+)(?=\s*[,}\]])/g, '$1"$2"')
  return JSON.parse(quoted, (key, value) => (NS_KEY.test(key) && typeof value === 'string' && /^-?\d+$/.test(value) ? BigInt(value) : value))
}

/** A JSON.stringify that writes BigInts as numbers (for logs and tests). */
export function stringifyNs(v, space) {
  const marks = []
  const s = JSON.stringify(v, (k, x) => {
    if (typeof x === 'bigint') {
      marks.push(x.toString())
      return `@@BIGINT${marks.length - 1}@@`
    }
    return x
  }, space)
  return s.replace(/"@@BIGINT(\d+)@@"/g, (_, i) => marks[+i])
}
