// The other way to fill the same tables: DuckDB reads the planned objects
// itself (read_parquet over the presigned URLs, registered under plain names
// because DuckDB reads '?' in a path as a glob). Same columns as columns.js,
// same states: the rule stays in lakeui's completeness.js, which gives the
// one instant where the incomplete region starts; SQL only compares with it.
//
// Precision [M]: DuckDB reads the edges' Timestamp (INT64, TIMESTAMP NANOS,
// isAdjustedToUTC) as TIMESTAMP WITH TIME ZONE, which is microseconds: the
// nanoseconds are gone before any SQL runs, and read_parquet has no option
// to keep them (1.5.4). So ts_ns here is the true time truncated to the µs;
// a row's true time is in [ts_ns, ts_ns + 999], and its state is decided on
// the late end (a row within a µs of the boundary is drawn incomplete, never
// the other way). Window and bucket edges can still move such a row by one
// bucket; the range-reader path has no such error.
//
// The tail (AMBIGUITY.md #10 (b)): `tailFiles` names the registered files of
// the plan's tail; their rows, and every bucket holding one, are incomplete
// (unknown stays unknown). A tail object the planner could not date
// (basis_check) stays in the tail here: DuckDB is not asked to read its
// footer, so its rows are drawn incomplete even when they are below the
// bound: the weaker claim, never the basis for rows it has not placed.

import { incompleteStart, niceStep } from '../../src/completeness.js'

const lit = s => `'${String(s).replaceAll("'", "''")}'`
const big = n => {
  if (typeof n !== 'bigint') throw new TypeError('urlsql: a bigint is required')
  return `${n}::BIGINT`
}
// ns → TIMESTAMP (µs) at ms precision, floored, as nsToMsFloor does
const tsOf = e => `make_timestamp(((${e}) // 1000000) * 1000)`

/**
 * CREATE OR REPLACE TABLE `name` from the registered files.
 * @param {string} signal logs | traces
 * @param {string[]} files registered names
 * @param {ReturnType<import('../../src/completeness.js').labelOf>} label
 * @param {{stepNs?: bigint, tailFiles?: string[]}} [o] with tailFiles the table has a `tail` column
 *   too (the caller counts the tail's rows, then drops it: range mode's tables have none)
 */
export function createFromFilesSQL(name, signal, files, label, { stepNs, tailFiles = [] } = {}) {
  const fromNs = label.fromNs
  const toNs = label.toNs
  const step = stepNs ?? niceStep(fromNs, toNs, 60)
  const s = label.state === 'unknown' ? null : incompleteStart(label)
  const tail = tailFiles.length > 0
  if (tailFiles.some(t => !files.includes(t))) throw new RangeError('createFromFilesSQL: a tail file that is not read')
  const cases = (whenTail, whenLate) => {
    const w = [tail ? `WHEN ${whenTail} THEN 'incomplete'` : '', s === null ? '' : `WHEN ${whenLate} THEN 'incomplete'`].filter(Boolean)
    return w.length ? `CASE ${w.join(' ')} ELSE 'complete' END` : `'complete'`
  }
  const rowState = label.state === 'unknown' ? `'unknown'` : cases('tail', `ts_ns + 999 >= ${s}::BIGINT`)
  const bucketState = label.state === 'unknown' ? `'unknown'` : cases('tail_bucket', `b1_ns > ${s}::BIGINT`)
  const extra = signal === 'logs'
    ? `coalesce(nullif(upper("SeverityText"), ''), 'UNSET') AS severity`
    : `coalesce("SpanName", '') AS name, coalesce("StatusCode", '') AS status, "Duration" / 1e6 AS dur_ms`
  const extraCols = signal === 'logs' ? 'severity' : 'name, status, dur_ms'
  const src = files.length ? `read_parquet([${files.map(lit).join(', ')}]${tail ? ', filename = true' : ''})` : null
  if (!src) throw new RangeError('createFromFilesSQL: no files (create the empty table from columns.js instead)')
  return `CREATE OR REPLACE TABLE "${name}" AS
WITH r AS (
  SELECT epoch_ns("Timestamp") AS ts_ns,
    coalesce("ServiceName", '') AS service,
    coalesce("ResourceAttributes"['k8s.pod.name'], '') AS pod,
    coalesce("ResourceAttributes"['k8s.cluster.name'], '') AS cluster,
    ${extra}${tail ? `,
    filename IN (${tailFiles.map(lit).join(', ')}) AS tail` : ''}
  FROM ${src}
), b AS (
  SELECT *, ${big(fromNs)} + ((ts_ns - ${big(fromNs)}) // ${big(step)}) * ${big(step)} AS b0_ns FROM r
  WHERE ts_ns >= ${big(fromNs)} AND ts_ns < ${big(toNs)}
), c AS (
  SELECT *, least(b0_ns + ${big(step)}, ${big(toNs)}) AS b1_ns${tail ? ', bool_or(tail) OVER (PARTITION BY b0_ns) AS tail_bucket' : ''} FROM b
)
SELECT ts_ns, ${tsOf('ts_ns')} AS ts, ${tsOf('b0_ns')} AS b0, ${tsOf('b1_ns')} AS b1,
  service, pod, cluster, ${rowState} AS cstate, ${bucketState} AS bstate, ${extraCols}${tail ? ', tail' : ''}
FROM c`
}
