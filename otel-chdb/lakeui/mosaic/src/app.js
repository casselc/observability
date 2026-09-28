// The spike's page: sign in (as lakeui does), plan both signals, fill two
// DuckDB-WASM tables, and hand them to Mosaic. Two ways to fill them:
//
//   ?mode=range  (default) lakeui's engine: /v1/plan → hyparquet range reads
//                of only the needed column chunks (X8's re-plan rules) →
//                Arrow IPC (flechette) → insertArrowFromIPCStream. DuckDB
//                never sees a URL, and needs no extension.
//   ?mode=url    DuckDB reads the presigned URLs itself (read_parquet, the
//                parquet extension from vendor/ext).
//   ?mode=url-shim  the same with lake-ui's HEAD shim in DuckDB's worker;
//                add &fs=head to trust HEAD and forbid whole-object reads.
//
// Other switches: ?preagg=0 (no data-cube indexes), ?nodrop=1 (keep the
// previous load's pre-aggregated tables: shows why they must be dropped).
// window.mos is the browser test's handle.

import { vg, duckdb, flechette as f } from '../vendor/mosaic.js'
import { installHeadShim } from '../vendor/head-shim.js'
import { bannerText, incompleteStart, labelOf, niceStep, settledThrough } from '../../src/completeness.js'
import { execute, sharedMeta } from '../../src/engine.js'
import { formatTimeNs, parseTimeNs, shortTime } from '../../src/ns.js'
import { authorizeURL, claimsOf, discover, exchangeCode, parseRedirect, pkcePair, randomString, secondsLeft } from '../../src/oidc.js'
import { cachedPlanner, PlanError, requestPlan } from '../../src/planclient.js'
import { logSearch } from '../../src/queries.js'
import { replaceTable } from './arrow.js'
import { columnsQuery, toColumns } from './columns.js'
import { buildDashboard } from './dashboard.js'
import { createFromFilesSQL } from './urlsql.js'

const $ = id => document.getElementById(id)
const esc = s => String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c])
const params = new URLSearchParams(location.search)
const MODE = params.get('mode') ?? 'range'
const PREAGG = params.get('preagg') !== '0'
const NODROP = params.get('nodrop') === '1'
const FS_HEAD = params.get('fs') === 'head'
const store = {
  get: k => { try { return sessionStorage.getItem(k) } catch { return null } },
  set: (k, v) => { try { sessionStorage.setItem(k, v) } catch { /* private mode */ } },
  del: k => { try { sessionStorage.removeItem(k) } catch { /* */ } },
}
const redirectUri = () => location.origin + location.pathname
const state = { config: null, token: store.get('lakeui.token'), loads: [], last: null, engine: null }

// ---- sign-in (lakeui's flow; the token is shared with it under the same key) ----

async function loadConfig() {
  const r = await fetch('../config.json', { cache: 'no-store' })
  if (!r.ok) throw new Error(`config.json: ${r.status}`)
  state.config = await r.json()
  for (const c of state.config.clusters ?? []) $('cluster').insertAdjacentHTML('beforeend', `<option>${esc(c)}</option>`)
}
async function signIn() {
  const doc = await discover(state.config.issuer)
  const { verifier, challenge } = await pkcePair()
  const st = randomString(16)
  store.set('lakeui.pkce', JSON.stringify({ verifier, state: st }))
  store.set('mos.return', location.search)
  location.assign(authorizeURL(doc, { clientId: state.config.client_id, redirectUri: redirectUri(), state: st, challenge }))
}
async function finishSignIn() {
  const r = parseRedirect(location.href)
  if (!r) return
  history.replaceState(null, '', redirectUri() + (store.get('mos.return') ?? ''))
  if (r.error) throw new Error(`sign-in refused by the IdP: ${r.error} ${r.description ?? ''}`)
  const saved = JSON.parse(store.get('lakeui.pkce') ?? 'null')
  store.del('lakeui.pkce')
  if (!saved || saved.state !== r.state) throw new Error('sign-in: the state does not match this browser session')
  const doc = await discover(state.config.issuer)
  setToken(await exchangeCode(doc, { code: r.code, verifier: saved.verifier, clientId: state.config.client_id, redirectUri: redirectUri() }))
  // the switches (?mode=…) are read once, at module load: come back with them
  const back = store.get('mos.return') ?? ''
  store.del('mos.return')
  if (back) {
    location.replace(redirectUri() + back)
    return 'reloading'
  }
}
function setToken(t) {
  state.token = t
  if (t) store.set('lakeui.token', t)
  else store.del('lakeui.token')
  planner.clear()
  showWho()
}
function showWho() {
  let text = 'not signed in'
  if (state.token) {
    try {
      const c = claimsOf(state.token)
      text = `${c.sub} · token valid ${Math.max(0, Math.floor(secondsLeft(c) / 60))} min`
    } catch {
      text = 'signed in (token unreadable)'
    }
  }
  $('who').textContent = text
  $('signin').hidden = !!state.token
  $('signout').hidden = !state.token
}
const planner = cachedPlanner(req => requestPlan({ ...req, queryUrl: state.config.query_url, token: state.token }))

// ---- DuckDB-WASM and Mosaic ---------------------------------------------------

/** Query log and idleness, for the timings (a wrapper around the real connector). */
class TimedConnector {
  constructor(inner) {
    this.inner = inner
    this.log = []
    this.inflight = 0
    this.lastEnd = 0
    this.every = new Map() // every distinct statement, for the dialect check (e2e)
  }
  async query(req) {
    const t0 = performance.now()
    this.every.set(String(req.sql), req.type)
    this.inflight++
    try {
      return await this.inner.query(req)
    } catch (e) {
      this.log.push({ type: req.type, sql: String(req.sql), t0, ms: performance.now() - t0, error: String(e?.message ?? e) })
      throw e
    } finally {
      this.inflight--
      this.lastEnd = performance.now()
      const l = this.log[this.log.length - 1]
      if (!l || l.t0 !== t0) this.log.push({ type: req.type, sql: String(req.sql), t0, ms: this.lastEnd - t0 })
    }
  }
}

async function initEngine() {
  const t0 = performance.now()
  const base = new URL('../vendor/duckdb/', import.meta.url).href
  const shim = MODE === 'url-shim'
  const src = (shim ? `(${installHeadShim})(self);\n` : '') + `importScripts(${JSON.stringify(base + 'duckdb-browser-eh.worker.js')});`
  const workerUrl = URL.createObjectURL(new Blob([src], { type: 'text/javascript' }))
  const db = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), new Worker(workerUrl))
  await db.instantiate(base + 'duckdb-eh.wasm')
  URL.revokeObjectURL(workerUrl)
  if (FS_HEAD) await db.open({ filesystem: { reliableHeadRequests: true, allowFullHTTPReads: false } })
  const conn = await db.connect()
  // an air-gapped page mirrors the extensions it may autoload; only mode=url uses one (parquet)
  await conn.query(`SET custom_extension_repository = '${new URL('../vendor/ext', import.meta.url).href}'`)
  const version = (await conn.query('SELECT version() AS v')).toArray()[0].v
  const connector = new TimedConnector(new vg.DuckDBWASMConnector({ duckdb: db, connection: conn }))
  const mc = new vg.Coordinator(connector, { logger: null, preagg: { enabled: PREAGG, schema: 'mosaic' } })
  vg.coordinator(mc)
  return { db, conn, connector, mc, version, initMs: performance.now() - t0, shim, fsHead: FS_HEAD }
}

/** Resolves when Mosaic has no query in flight and none started for `quietMs`. */
async function idle(quietMs = 60, timeoutMs = 60_000) {
  const c = state.engine.connector
  const q = state.engine.mc.manager
  const start = performance.now()
  for (;;) {
    await new Promise(r => setTimeout(r, 10))
    const busy = c.inflight > 0 || !q.queue.isEmpty() || q.pendingResults.length > 0
    if (!busy && performance.now() - c.lastEnd >= quietMs) break
    if (performance.now() - start > timeoutMs) throw new Error('mosaic did not go idle')
  }
  await new Promise(r => requestAnimationFrame(() => r()))
  return c.lastEnd
}

// ---- loading ------------------------------------------------------------------

let seq = 0

/** Plans and loads one signal the lakeui way (range reads → Arrow). */
async function loadRange(signal, table, w, clusters, onEvent) {
  const q = columnsQuery({ signal, ...w })
  const out = await execute(q, { planner, request: { ...w, clusters }, onEvent })
  if (out.status !== 'ok') return { failed: true, out }
  const t0 = performance.now()
  await replaceTable(state.engine.conn, f, table, out.result)
  return { plan: out.plan, label: out.label, rows: out.result.rows, stepNs: q.q.stepNs, fetchedBytes: out.stats.bytes,
    requests: out.stats.requests, replans: out.replans, insertMs: performance.now() - t0 }
}

/** Plans and lets DuckDB read the URLs (whole objects, as it does). Re-plans on any failure, 3 times. */
async function loadURL(signal, table, w, clusters, onEvent) {
  const { db, conn } = state.engine
  const req = { ...w, clusters, signal }
  const stepNs = niceStep(w.fromNs, w.toNs, 60)
  let force = false
  let lastErr = null
  for (let attempt = 0; attempt <= 3; attempt++) {
    const plan = await planner(req, { force })
    onEvent({ type: 'plan', requestId: plan.requestId, objects: plan.objects.length })
    force = true
    // X8 rule 1 as far as this mode can keep it: no statement STARTS past
    // replan_after; the engine then reads every object inside one statement
    if (Date.now() >= plan.replanAfterMs) continue
    const label = labelOf(plan)
    const names = []
    try {
      if (!plan.objects.length) {
        await replaceTable(conn, f, table, toColumns(signal, [], label, { ...w, stepNs }))
      } else {
        for (const o of plan.objects) {
          const n = `p${seq++}.parquet`
          await db.registerFileURL(n, o.url, duckdb.DuckDBDataProtocol.HTTP, false)
          names.push(n)
        }
        await conn.query(createFromFilesSQL(table, signal, names, label, { stepNs }))
      }
      const rows = (await conn.query(`SELECT count(*)::INTEGER AS n FROM "${table}"`)).toArray()[0].n
      return { plan, label, rows, stepNs, fetchedBytes: null, requests: null, replans: attempt }
    } catch (e) {
      lastErr = e
      onEvent({ type: 'read_error', message: String(e?.message ?? e).slice(0, 300) })
    } finally {
      for (const n of names) await db.dropFile(n).catch(() => {})
    }
  }
  return { failed: true, out: { reason: 'DuckDB could not read the planned objects and re-plans ran out', errors: [{ key: '(statement)', error: lastErr }], missing: ['(all)'] } }
}

function windowNs() {
  const fromNs = parseTimeNs($('from').value.trim())
  const toNs = parseTimeNs($('to').value.trim())
  if (toNs <= fromNs) throw new Error('"to" must be after "from"')
  return { fromNs, toNs }
}

async function load() {
  const rec = { mode: MODE, preagg: PREAGG, events: [] }
  const t0 = performance.now()
  $('load').disabled = true
  setBanner('running', 'planning and loading…')
  try {
    if (!state.token) throw new PlanError('auth', 'sign in first')
    if (!state.engine) state.engine = await initEngine()
    const { mc, connector, conn } = state.engine
    rec.engine = { version: state.engine.version, initMs: Math.round(state.engine.initMs), shim: state.engine.shim, fsHead: state.engine.fsHead }
    // a new load replaces the tables under the same names: every cached
    // answer and every pre-aggregated table over the old rows is stale
    mc.clear({ clients: true, cache: true })
    for (const id of ['A', 'B', 'C', 'D']) $('chart-' + id).innerHTML = ''
    if (!NODROP) await mc.preaggregator.dropSchema()
    connector.log.length = 0
    state.scaledFrom = false
    const w = windowNs()
    const cl = $('cluster').value
    const clusters = cl ? [cl] : []
    const onEvent = ev => rec.events.push({ ...ev, at: Math.round(performance.now() - t0) })
    const loader = MODE === 'range' ? loadRange : loadURL
    // DuckDB-WASM runs one statement at a time; the range reader reads both signals at once
    const [logs, spans] = MODE === 'range'
      ? await Promise.all([loader('logs', 'logs', w, clusters, onEvent), loader('traces', 'spans', w, clusters, onEvent)])
      : [await loader('logs', 'logs', w, clusters, onEvent), await loader('traces', 'spans', w, clusters, onEvent)]
    rec.dataMs = Math.round(performance.now() - t0)
    for (const [name, r] of [['logs', logs], ['spans', spans]]) {
      if (r.failed) {
        const lines = (r.out.errors ?? []).map(e => `<li>${esc(e.key)}: ${esc(e.error?.message ?? 'not read')}</li>`).join('')
        setBanner('failed', `${esc(name)}: ${esc(r.out.reason)}. No chart is drawn: a load that could not read every planned object is not an answer.<ul data-testid="missing">${lines}</ul>`)
        rec.status = 'failed'
        rec.missing = r.out.missing
        return rec
      }
    }
    const labels = { logs: logs.label, spans: spans.label }
    const dash = buildDashboard(vg, labels, { logs: logs.stepNs, spans: spans.stepNs })
    if (!document.getElementById('mos-hatch')) document.body.insertAdjacentHTML('afterbegin', dash.defs)
    const tc = performance.now()
    for (const [id, el] of Object.entries(dash.charts)) $('chart-' + id).replaceChildren(el)
    await idle()
    // the charts' own queries: from their creation to the end of the last one
    const after = connector.log.filter(q => q.t0 >= tc)
    rec.chartsMs = after.length ? Math.round(Math.max(...after.map(q => q.t0 + q.ms)) - tc) : null
    rec.firstChartMs = Math.round(performance.now() - t0)
    rec.status = 'ok'
    state.dash = dash
    Object.assign(rec, summary(logs, spans))
    rec.tables = await tableFacts(conn)
    rec.queries = connector.log.map(q => ({ type: q.type, ms: Math.round(q.ms * 10) / 10, sql: q.sql.slice(0, 2000), error: q.error }))
    rec.memory = await memory()
    const bl = bannerText(logs.label, {}, shortTime)
    const bs = bannerText(spans.label, {}, shortTime)
    const worst = [bl.state, bs.state].includes('unknown') ? 'unknown' : [bl.state, bs.state].includes('incomplete') ? 'incomplete' : 'complete'
    setBanner(worst, `logs: <span data-testid="logs-count">${rec.logs.rows}</span> rows · ${esc(bl.text)}<br>spans: <span data-testid="spans-count">${rec.spans.rows}</span> rows · ${esc(bs.text)}`)
    $('stats').textContent = statsLine(rec)
    $('qlog').textContent = rec.queries.map(q => `${q.ms} ms ${q.type}: ${q.sql}`).join('\n\n')
    return rec
  } catch (e) {
    rec.status = 'error'
    rec.error = String(e?.message ?? e)
    const refused = e instanceof PlanError && e.kind === 'refused'
    setBanner(refused ? 'refused' : 'error', `${esc(e.reason ?? '')} ${esc(e.detail || e.message)}. This is ${refused ? 'a refusal' : 'an error'}, not an empty result.`)
    return rec
  } finally {
    rec.ms = Math.round(performance.now() - t0)
    state.last = rec
    state.loads.push(rec)
    document.body.dataset.loads = String(state.loads.length)
    $('load').disabled = false
  }
}

function summary(logs, spans) {
  const one = r => ({
    rows: Number(r.rows), requestId: r.plan.requestId, completeness: r.plan.completeness, objects: r.plan.objects.length,
    plannedBytes: r.plan.totalBytes, fetchedBytes: r.fetchedBytes, requests: r.requests, replans: r.replans,
    completeThrough: r.plan.completeThroughNs === null ? null : formatTimeNs(r.plan.completeThroughNs),
    settledThrough: settledThrough(r.label) === null ? null : formatTimeNs(settledThrough(r.label)),
    incompleteFrom: incompleteStart(r.label) === null ? null : formatTimeNs(incompleteStart(r.label)),
    lateObjects: r.plan.lateObjects, insertMs: r.insertMs === undefined ? null : Math.round(r.insertMs),
  })
  return { logs: one(logs), spans: one(spans) }
}

/** Per table: rows per state, and a fingerprint per column (the load modes must agree). */
async function tableFacts(conn) {
  const out = {}
  for (const t of ['logs', 'spans']) {
    const cols = ['ts_ns', 'ts_us', 'ts', 'b0', 'b1', 'service', 'pod', 'cluster', 'cstate', 'bstate', ...(t === 'logs' ? ['severity'] : ['name', 'status', 'dur_ms'])]
    // ts_us: DuckDB's own Parquet reader keeps µs only (urlsql.js), so modes are compared on it
    const expr = c => (c === 'ts_us' ? '(ts_ns // 1000)' : `"${c}"`)
    const fps = cols.map(c => `(sum(hash(${expr(c)}) % 1000000007))::VARCHAR AS "fp_${c}"`).join(', ')
    const r = (await conn.query(`SELECT count(*)::INTEGER AS n,
      count(*) FILTER (WHERE cstate = 'incomplete')::INTEGER AS inc, count(*) FILTER (WHERE cstate = 'unknown')::INTEGER AS unk,
      count(*) FILTER (WHERE bstate = 'incomplete')::INTEGER AS binc, ${fps}
      FROM "${t}"`)).toArray()[0].toJSON()
    const types = (await conn.query(`DESCRIBE "${t}"`)).toArray().map(x => `${x.column_name}:${x.column_type}`)
    out[t] = { ...r, types }
  }
  return out
}

async function memory() {
  const m = { jsHeap: performance.memory?.usedJSHeapSize ?? null }
  try {
    const r = (await state.engine.conn.query('SELECT sum(memory_usage_bytes)::BIGINT AS b FROM duckdb_memory()')).toArray()[0]
    m.duckdb = Number(r.b)
  } catch (e) {
    m.duckdb = null
    m.duckdbError = String(e.message)
  }
  return m
}

function statsLine(rec) {
  const s = x => `${x.objects} object(s), ${x.plannedBytes} B planned` + (x.fetchedBytes !== null ? `, fetched ${x.fetchedBytes} B in ${x.requests} range GET(s)` : ', read by DuckDB (see the tap)')
  return `mode ${rec.mode} (DuckDB ${rec.engine.version}${rec.engine.shim ? ', HEAD shim' : ''}) · logs: ${s(rec.logs)} · spans: ${s(rec.spans)} · data ${rec.dataMs} ms, charts +${rec.chartsMs} ms · pre-aggregation ${rec.preagg ? 'on' : 'off'}`
}

function setBanner(st, html) {
  $('banner').dataset.state = st
  $('banner').innerHTML = html
}

// ---- the test's handle: marks, brushes, the lakeui comparison, scale-up ----------

let markAt = 0
const mos = {
  state, load, idle,
  mark() { markAt = performance.now(); state.engine.connector.log.length = 0 },
  /** ms from mark() to the end of the last query it caused, and those queries */
  async since() {
    const end = await idle()
    const qs = state.engine.connector.log.map(q => ({ type: q.type, ms: Math.round(q.ms * 10) / 10, sql: q.sql.slice(0, 400) }))
    return { ms: Math.max(0, Math.round((end - markAt) * 10) / 10), queries: qs, preaggTables: qs.filter(q => /preagg_/.test(q.sql) && /CREATE TABLE/.test(q.sql)).length }
  },
  /** counts per chart as SQL sees them now (for checking cross-filter results) */
  async counts(where = {}) {
    const { conn } = state.engine
    const q = async s => Number((await conn.query(s)).toArray()[0].n)
    return { logs: await q(`SELECT count(*) AS n FROM logs ${where.logs ?? ''}`), spans: await q(`SELECT count(*) AS n FROM spans ${where.spans ?? ''}`) }
  },
  /**
   * A chart's total as drawn (the sum of its count channel) and the same
   * count by an independent SQL statement under the chart's current filter
   * (its selection's predicate for that mark): cross-filtering is right when
   * they agree.
   */
  async check(id) {
    const plot = $('chart-' + id).firstChild.value
    const mark = plot.marks[0]
    const ch = { A: 'y', B: 'x', C: 'y', D: 'x' }[id]
    let shown = 0
    const col = mark.data?.columns?.[ch] ?? []
    for (let i = 0; i < mark.data.numRows; i++) shown += Number(col[i] ?? 0)
    const pred = [mark.filterBy?.predicate(mark, true) ?? []].flat().filter(Boolean).map(p => String(p))
    const table = mark.sourceTable()
    const sql = `SELECT count(*)::INTEGER AS n FROM "${table}"${pred.length ? ' WHERE ' + pred.join(' AND ') : ''}`
    const n = (await state.engine.conn.query(sql)).toArray()[0].n
    return { id, shown, expected: Number(n), where: pred.join(' AND ') }
  },
  /** where chart id's plot frame is on the page (for the test's mouse) */
  frame(id) {
    const plot = $('chart-' + id).firstChild.value
    const svg = $('chart-' + id).querySelector('svg')
    const r = svg.getBoundingClientRect()
    const m = plot.margins()
    return { left: r.left + m.left, right: r.left + plot.getAttribute('width') - m.right, top: r.top + m.top, bottom: r.top + plot.getAttribute('height') - m.bottom }
  },
  /** lakeui's hyparquet-only path for the log-volume view (count + histogram, no rows) */
  async lakeuiLogs(fromNs, toNs) {
    const w = fromNs ? { fromNs: BigInt(fromNs), toNs: BigInt(toNs) } : windowNs()
    const cl = $('cluster').value
    sharedMeta.map.clear()
    const t0 = performance.now()
    const out = await execute(logSearch({ ...w, limit: 0 }), { planner, request: { ...w, clusters: cl ? [cl] : [] } })
    return { ms: Math.round(performance.now() - t0), status: out.status, count: out.result?.count ?? null, fetchedBytes: out.stats.bytes,
      requests: out.stats.requests, plannedBytes: out.plan.totalBytes, objects: out.plan.objects.length, jsHeap: performance.memory?.usedJSHeapSize ?? null }
  },
  /** replace both tables by `n` copies of themselves (same distributions, n× rows) and rebuild */
  async scale(n) {
    const { conn, mc } = state.engine
    const t0 = performance.now()
    for (const t of ['logs', 'spans']) {
      // n copies of the LOADED rows (kept aside on the first call), not of the current table
      if (!state.scaledFrom) await conn.query(`CREATE OR REPLACE TABLE "${t}_base" AS SELECT * FROM "${t}"`)
      await conn.query(`DROP TABLE IF EXISTS "${t}"`)
      await conn.query(`CREATE TABLE "${t}_x" AS SELECT t.* FROM "${t}_base" t, range(${Number(n)})`)
      await conn.query(`ALTER TABLE "${t}_x" RENAME TO "${t}"`)
    }
    state.scaledFrom = true
    const buildMs = performance.now() - t0
    mc.clear({ clients: true, cache: true })
    await mc.preaggregator.dropSchema()
    const last = state.last
    const labels = { logs: labelOf(planner.cached().find(p => p.signal === 'logs')), spans: labelOf(planner.cached().find(p => p.signal === 'traces')) }
    const dash = buildDashboard(vg, labels, { logs: niceStep(labels.logs.fromNs, labels.logs.toNs, 60), spans: niceStep(labels.spans.fromNs, labels.spans.toNs, 60) })
    const tc = performance.now()
    for (const [id, el] of Object.entries(dash.charts)) $('chart-' + id).replaceChildren(el)
    const end = await idle()
    state.dash = dash
    return { rows: await mos.counts(), buildMs: Math.round(buildMs), chartsMs: Math.round(end - tc), memory: await memory(), lastLoad: last?.mode }
  },
  async memory() { return memory() },
  /** every distinct statement Mosaic sent since the page loaded */
  statements() { return [...state.engine.connector.every].map(([sql, type]) => ({ type, sql })) },
  planner,
}
window.mos = mos

async function main() {
  $('mode').value = MODE === 'url-shim' ? 'url' : MODE
  $('mode').addEventListener('change', () => { params.set('mode', $('mode').value); location.search = params.toString() })
  $('preagg').checked = PREAGG
  $('preagg').addEventListener('change', () => { params.set('preagg', $('preagg').checked ? '1' : '0'); location.search = params.toString() })
  $('load').addEventListener('click', () => load())
  $('signin').addEventListener('click', () => signIn().catch(e => setBanner('error', esc(e.message))))
  $('signout').addEventListener('click', () => setToken(null))
  try {
    await loadConfig()
    if ((await finishSignIn()) === 'reloading') return
  } catch (e) {
    setBanner('error', esc(e.message))
  }
  showWho()
  document.body.dataset.ready = '1'
}
main()
