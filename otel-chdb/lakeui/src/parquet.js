// Reading planned objects with hyparquet: the footer first (a small tail
// read, sized from the plan's `size`), then only the column chunks of the row
// groups a query needs. Timestamps stay BigInt nanoseconds.

import { parquetMetadataAsync, parquetReadObjects } from 'hyparquet'
import { decompress as zstd } from 'fzstd'
import { ObjectReadError } from './rangereader.js'

/** Decoders: the edges write ZSTD (FORMAT.md); SNAPPY is built into hyparquet. */
export const compressors = { ZSTD: input => zstd(input) }

/** Keep every timestamp as BigInt ns (hyparquet's default is a Date: ms). */
export const parsers = {
  timestampFromNanoseconds: v => BigInt(v),
  timestampFromMicroseconds: v => BigInt(v) * 1000n,
  timestampFromMilliseconds: v => BigInt(v) * 1_000_000n,
}

/** The tail read that should hold the whole footer of an edge object. */
export const DEFAULT_FOOTER_FETCH = 8192

/** Metadata by object key: a slot is create-only, so its footer never changes. */
export class MetaCache {
  constructor(limit = 2000) {
    this.limit = limit
    this.map = new Map()
  }
  get(key) {
    return this.map.get(key)
  }
  set(key, md) {
    if (this.map.size >= this.limit) this.map.delete(this.map.keys().next().value)
    this.map.set(key, md)
  }
}

export async function metadata(file, key, cache, initialFetchSize = DEFAULT_FOOTER_FETCH) {
  const hit = cache?.get(key)
  if (hit) return hit
  let md
  try {
    md = await parquetMetadataAsync(file, { parsers, initialFetchSize: Math.min(initialFetchSize, file.byteLength) })
  } catch (e) {
    if (e instanceof ObjectReadError) throw e
    throw new ObjectReadError('decode', key, `${key}: footer: ${e.message}`)
  }
  cache?.set(key, md)
  return md
}

/** The column's statistics in row group rg, or null. */
export function columnStats(md, rg, column) {
  const c = md.row_groups[rg].columns.find(c => c.meta_data?.path_in_schema?.join('.') === column)
  return c?.meta_data?.statistics ?? null
}

/** Row groups [start, end) with their first row. */
export function rowGroups(md) {
  const out = []
  let start = 0
  for (let i = 0; i < md.row_groups.length; i++) {
    const n = Number(md.row_groups[i].num_rows)
    out.push({ index: i, rowStart: start, rowEnd: start + n })
    start += n
  }
  return out
}

/**
 * Row groups that may hold rows with column in [lo, hi] (inclusive; either
 * bound may be null). Missing statistics keep the group: pruning is never a
 * loss.
 */
export function pruneByRange(md, column, lo, hi) {
  return rowGroups(md).filter(g => {
    const st = columnStats(md, g.index, column)
    const min = st?.min_value ?? st?.min
    const max = st?.max_value ?? st?.max
    if (min === undefined || max === undefined || min === null || max === null) return true
    // strings: JS compares UTF-16 units, Parquet unsigned bytes; they agree on ASCII only
    if ([min, max, lo, hi].some(v => typeof v === 'string' && /[^\x00-\x7f]/.test(v))) return true
    if (typeof min !== typeof (lo ?? min) || typeof max !== typeof (hi ?? max)) return true
    // a string max may be truncated to a prefix (the edges' writer cuts long
    // values): it bounds only values that do not extend it
    if (lo !== null && lo !== undefined && max < lo && !(typeof lo === 'string' && lo.startsWith(max))) return false
    if (hi !== null && hi !== undefined && min > hi) return false
    return true
  })
}

/** Reads columns of one row group as objects (only those column chunks are fetched). */
export async function readGroup(file, key, md, group, columns) {
  try {
    return await parquetReadObjects({ file, metadata: md, columns, rowStart: group.rowStart, rowEnd: group.rowEnd, compressors, parsers })
  } catch (e) {
    if (e instanceof ObjectReadError) throw e
    throw new ObjectReadError('decode', key, `${key}: ${e.message}`)
  }
}

/** Reads a map column's value for one key out of hyparquet's decoded map. */
export function mapGet(m, k) {
  if (!m) return undefined
  if (m instanceof Map) return m.get(k)
  if (Array.isArray(m)) {
    const e = m.find(x => x && x.key === k)
    return e?.value
  }
  return m[k]
}
