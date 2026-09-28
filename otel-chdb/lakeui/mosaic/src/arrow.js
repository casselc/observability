// Columns (columns.js) → an Arrow IPC stream → a DuckDB table. flechette
// (mosaic's own Arrow library) is passed in, so this module runs in Node
// tests and in the page alike.

import { COMMON, SIGNALS } from './columns.js'

/** flechette types for our column kinds. */
function typeOf(f, kind) {
  switch (kind) {
    case 'int64': return f.int64()
    case 'timestamp': return f.timestamp(f.TimeUnit.MICROSECOND) // values are ms; DuckDB's TIMESTAMP is µs
    case 'float64': return f.float64()
    case 'utf8': return f.dictionary(f.utf8(), f.int32())
    default: throw new RangeError(`arrow: no type ${kind}`)
  }
}

/** The Arrow IPC stream bytes of one loaded signal. */
export function toIPC(f, loaded) {
  const kinds = { ...COMMON, ...SIGNALS[loaded.signal].extra }
  const types = {}
  const data = {}
  for (const [k, kind] of Object.entries(kinds)) {
    types[k] = typeOf(f, kind)
    data[k] = kind === 'int64' ? BigInt64Array.from(loaded.columns[k]) : loaded.columns[k]
  }
  const table = f.tableFromArrays(data, { types })
  return f.tableToIPC(table, { format: 'stream' })
}

/**
 * Replaces table `name` with the loaded columns. An empty result still
 * creates the table (with its columns), so the charts draw "no rows", not an
 * error.
 * @param {{insertArrowFromIPCStream: Function, query: Function}} conn a DuckDB-WASM connection
 */
export async function replaceTable(conn, f, name, loaded) {
  await conn.query(`DROP TABLE IF EXISTS "${name}"`)
  await conn.insertArrowFromIPCStream(toIPC(f, loaded), { name, create: true })
}
