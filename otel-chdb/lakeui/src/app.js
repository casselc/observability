// The page: sign-in, the forms, and rendering. Everything that decides
// anything (plans, reads, re-plans, completeness) is in the plain modules
// beside this one; this file only wires them to the DOM.

import { bannerText, incompleteStart, settledThrough } from './completeness.js'
import { execute } from './engine.js'
import { histogramSVG, legendHTML, lineSVG, waterfallHTML, esc } from './charts.js'
import { formatTimeNs, msToNs, parseTimeNs, shortTime } from './ns.js'
import { authorizeURL, claimsOf, discover, exchangeCode, parseRedirect, pkcePair, randomString, secondsLeft } from './oidc.js'
import { cachedPlanner, PlanError, requestPlan } from './planclient.js'
import { logSearch, metricChart, traceById } from './queries.js'

const $ = id => document.getElementById(id)
const store = {
  get: k => { try { return sessionStorage.getItem(k) } catch { return null } },
  set: (k, v) => { try { sessionStorage.setItem(k, v) } catch { /* private mode: the token lives for this page only */ } },
  del: k => { try { sessionStorage.removeItem(k) } catch { /* */ } },
}
const redirectUri = () => location.origin + location.pathname

const state = { config: null, token: store.get('lakeui.token'), view: 'logs', runs: [], last: null }

async function loadConfig() {
  const r = await fetch('config.json', { cache: 'no-store' })
  if (!r.ok) throw new Error(`config.json: ${r.status}`)
  state.config = await r.json()
  for (const c of state.config.clusters ?? []) $('cluster').insertAdjacentHTML('beforeend', `<option>${esc(c)}</option>`)
  if (state.config.metric) $('metric-name').value = state.config.metric
}

// ---- sign-in ---------------------------------------------------------------

async function signIn() {
  const doc = await discover(state.config.issuer)
  const { verifier, challenge } = await pkcePair()
  const st = randomString(16)
  store.set('lakeui.pkce', JSON.stringify({ verifier, state: st }))
  location.assign(authorizeURL(doc, { clientId: state.config.client_id, redirectUri: redirectUri(), state: st, challenge }))
}

async function finishSignIn() {
  const r = parseRedirect(location.href)
  if (!r) return
  history.replaceState(null, '', redirectUri())
  if (r.error) throw new Error(`sign-in refused by the IdP: ${r.error} ${r.description ?? ''}`)
  const saved = JSON.parse(store.get('lakeui.pkce') ?? 'null')
  store.del('lakeui.pkce')
  if (!saved || saved.state !== r.state) throw new Error('sign-in: the state does not match this browser session')
  const doc = await discover(state.config.issuer)
  setToken(await exchangeCode(doc, { code: r.code, verifier: saved.verifier, clientId: state.config.client_id, redirectUri: redirectUri() }))
}

function setToken(t) {
  state.token = t
  state.basis = null // a basis is the caller's scope's: a new sign-in pins anew
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
      const left = Math.round(secondsLeft(c))
      const scope = [c.clusters && `clusters ${c.clusters}`, c.groups && `groups ${c.groups}`, c.namespaces && `namespaces ${c.namespaces}`].filter(Boolean).join(', ')
      text = `${c.sub}${scope ? ` (${scope})` : ''} · ${left > 0 ? `token valid ${Math.floor(left / 60)} min` : 'token EXPIRED: sign in again'}`
    } catch {
      text = 'signed in (token unreadable)'
    }
  }
  $('who').textContent = text
  $('signin').hidden = !!state.token
  $('signout').hidden = !state.token
}

// ---- queries -----------------------------------------------------------------

const planner = cachedPlanner(req => requestPlan({ ...req, queryUrl: state.config.query_url, token: state.token }))

function windowNs() {
  const r = $('range').value
  if (r === 'custom') {
    const fromNs = parseTimeNs($('from').value)
    const toNs = parseTimeNs($('to').value)
    if (toNs <= fromNs) throw new Error('"to" must be after "from"')
    return { fromNs, toNs }
  }
  const toNs = msToNs(Date.now())
  return { fromNs: toNs - BigInt(r) * 1_000_000_000n, toNs }
}

function buildQuery(view, w) {
  if (view === 'logs') {
    const sev = [...document.querySelectorAll('#log-sev input:checked')].map(i => i.value)
    return logSearch({ ...w, text: $('log-text').value, severities: sev, limit: Number($('log-limit').value) })
  }
  if (view === 'trace') return traceById({ ...w, traceId: $('trace-id').value })
  return metricChart({ ...w, metric: $('metric-name').value.trim() })
}

async function run(view) {
  const btn = $('run-' + view)
  btn.disabled = true
  const log = []
  const t0 = performance.now()
  setBanner('running', 'planning…')
  $('stats').textContent = ''
  $('chart').innerHTML = ''
  $('table').innerHTML = ''
  let rec
  try {
    if (!state.token) throw new PlanError('auth', 'sign in first')
    const w = windowNs()
    const q = buildQuery(view, w)
    const cl = $('cluster').value
    const out = await execute(q, {
      // D30: one basis per run; "hold" reuses the last run's
      planner, request: { ...w, clusters: cl ? [cl] : [], useIndex: $('use-index').checked,
        basis: $('hold-basis').checked && state.basis ? state.basis : 'latest' },
      onEvent: ev => {
        log.push(`${new Date().toISOString().slice(11, 23)} ${ev.type} ${ev.key ?? ''} ${ev.kind ?? ''} ${ev.status || ''} ${ev.why ?? ''} ${ev.requestId ?? ''} ${ev.message ?? ''}`.replace(/\s+/g, ' '))
        $('event-log').textContent = log.join('\n')
        if (ev.type === 'replan') setBanner('running', `re-planning (${ev.why})…`)
      },
    })
    if (out.basis) state.basis = out.basis
    rec = render(view, q, out)
  } catch (e) {
    rec = renderError(e)
  } finally {
    btn.disabled = false
  }
  rec.view = view
  rec.ms = Math.round(performance.now() - t0)
  rec.log = log
  state.last = rec
  state.runs.push(rec)
  document.body.dataset.runs = String(state.runs.length)
}

function setBanner(st, html) {
  $('banner').dataset.state = st
  $('banner').innerHTML = html
}

function statsLine(out) {
  const p = out.plan
  const pct = p.totalBytes ? ((100 * out.stats.bytes) / p.totalBytes).toFixed(1) : '0'
  return `source ${esc(p.source)} · plan ${esc(p.requestId.slice(0, 8))} · ${p.objects.length} object(s), ${p.totalBytes} B planned · fetched ${out.stats.bytes} B (${pct}%) in ${out.stats.requests} range GET(s) · ${out.replans} re-plan(s) · ${out.elapsedMs} ms` +
    ` · ${p.snapshot ? 'snapshot ' + esc(p.snapshot) : 'no snapshot (lanes listed ' + esc(p.listedAt.slice(11, 19)) + ')'}` +
    basisLine(out) + indexLine(p)
}

/** Which basis the run read at (D30): never hidden. */
function basisLine(out) {
  const p = out.plan
  if (!p.atBasis) return ' · <span class="warn" data-testid="basis">not at a basis: this service does not pin one; a re-run may read other objects</span>'
  const cl = (p.basisInfo?.clusters ?? []).map(c => `${c.cluster === '*' ? 'all clusters' : esc(c.cluster)} before ${esc(shortTime(BigInt(c.received_before_ns)))}`).join(', ')
  return ` · <span data-testid="basis">basis: rows received ${cl || '(bounds not shown)'}${out.cached ? ' (cached: the same answer at the same basis)' : ''}` +
    `${p.afterBasis ? `; ${p.afterBasis} newer object(s) left out` : ''}${out.stats.basisExcluded ? `; ${out.stats.basisExcluded} left out by their footer` : ''}</span>`
}

/** What the index did for a filtered plan: never hidden, errors included. */
function indexLine(p) {
  const ix = p.index
  if (!ix) return ''
  const hit = p.objects.filter(o => o.index === 'hit').length
  const errs = ix.errors?.length ? ` · <span class="warn">index errors: ${esc(ix.errors.join('; '))} (those objects were read whole)</span>` : ''
  return ` · index: ${hit} narrowed, ${ix.scan ?? 0} not indexed (read whole), ${ix.pruned ?? 0} ruled out; the service read ${ix.bytes ?? 0} B of index in ${ix.requests ?? 0} request(s)` + errs
}

function render(view, q, out) {
  const label = { ...out.label, incompleteStartNs: incompleteStart(out.label) }
  const rec = summaryOf(out)
  $('stats').innerHTML = statsLine(out)
  if (out.status !== 'ok') {
    const b = bannerText(out.label, { missing: out.missing }, shortTime)
    const lines = out.errors.map(e => `<li>${esc(e.key)}: ${esc(e.error?.kind ?? '')} ${esc(e.error?.message ?? 'not read')}</li>`).join('')
    setBanner('failed', `${esc(b.text)}. ${esc(out.reason)}${out.planError ? ` (${esc(out.planError.message)})` : ''}. ` +
      `No result is shown: a query that could not read every planned object is not an answer.<ul class="missing" data-testid="missing">${lines}</ul>`)
    return rec
  }
  const b = bannerText(out.label, {}, shortTime)
  const r = out.result
  if (view === 'logs') {
    setBanner(b.state, `<span data-testid="count">${r.count}</span> matching row(s) · ${esc(b.text)}${r.count === 0 && b.state === 'complete' ? ' · none: every planned object was read' : ''}`)
    $('chart').innerHTML = histogramSVG(r.buckets, label)
    $('table').innerHTML = r.rows.length ? `<table data-testid="rows"><tr><th>time (UTC)</th><th>severity</th><th>service</th><th>body</th></tr>${r.rows.map(x =>
      `<tr data-state="${x.state}"><td class="ts">${esc(formatTimeNs(x.ts).slice(11, 23))}${x.state !== 'complete' ? ` <span class="state-tag">${x.state}</span>` : ''}</td><td>${esc(x.severity)}</td><td>${esc(x.service ?? '')}</td><td class="body">${esc(x.body ?? '')}</td></tr>`).join('')}</table>` : ''
  } else if (view === 'trace') {
    setBanner(b.state, `trace ${esc(r.traceId)}: <span data-testid="count">${r.spans.length}</span> span(s) in ${r.objects} object(s) · ${esc(b.text)}${r.spans.length === 0 ? ' · not found in the planned objects (all were read)' : ''}`)
    $('chart').innerHTML = waterfallHTML(r)
    $('table').innerHTML = r.spans.length ? `<table data-testid="rows"><tr><th>start (UTC)</th><th>service</th><th>span</th><th>duration</th></tr>${r.spans.map(s =>
      `<tr data-state="${s.state}"><td class="ts">${esc(formatTimeNs(s.ts).slice(11, 26))}${s.state !== 'complete' ? ` <span class="state-tag">${s.state}</span>` : ''}</td><td>${esc(s.service)}</td><td>${esc(s.name)}</td><td>${(Number(s.durationNs) / 1e6).toFixed(3)} ms</td></tr>`).join('')}</table>` : ''
  } else {
    setBanner(b.state, `${esc(r.metric)}: <span data-testid="count">${r.points}</span> point(s), ${r.series.length} series · ${esc(b.text)}`)
    $('chart').innerHTML = r.series.length ? lineSVG(r.series, r.buckets, label) + legendHTML(r.series) : ''
    $('table').innerHTML = r.series.length ? `<details><summary>table</summary><table data-testid="rows"><tr><th>service</th><th>bucket (UTC)</th><th>avg</th><th>min</th><th>max</th><th>n</th><th>state</th></tr>${r.series.flatMap(s => s.points.map(p =>
      `<tr data-state="${p.state}"><td>${esc(s.service)}</td><td class="ts">${esc(shortTime(p.fromNs))}</td><td>${p.avg.toFixed(2)}</td><td>${p.min}</td><td>${p.max}</td><td>${p.n}</td><td>${p.state}</td></tr>`)).join('')}</table></details>` : ''
  }
  return rec
}

function renderError(e) {
  const refused = e instanceof PlanError && e.kind === 'refused'
  const st = refused ? 'refused' : 'error'
  const what = e instanceof PlanError ? ({ refused: 'The query service refused this plan', auth: 'Not signed in, or the token was rejected', too_large: 'The plan is too large: narrow the window or cluster', bad_request: 'The request was malformed', server: 'The query service failed', network: 'The query service did not answer', bad_plan: 'The plan could not be trusted' })[e.kind] : 'Error'
  setBanner(st, `${esc(what)}${e.reason ? ` (<code data-testid="reason">${esc(e.reason)}</code>)` : ''}: ${esc(e.detail || e.message)}. This is ${refused ? 'a refusal' : 'an error'}, not an empty result.`)
  return { status: st, kind: e.kind ?? 'error', reason: e.reason ?? '', message: String(e.message) }
}

/** A JSON-safe summary of a run, for people (the log) and for the browser test. */
function summaryOf(out) {
  const r = out.result
  const p = out.plan
  return {
    status: out.status, state: out.state, requestId: p.requestId, completeness: p.completeness,
    completeThrough: p.completeThroughNs === null ? null : formatTimeNs(p.completeThroughNs),
    incompleteFrom: incompleteStart(out.label) === null ? null : formatTimeNs(incompleteStart(out.label)),
    // event time settled: complete_through − max_lateness (null without max_lateness)
    settledThrough: settledThrough(out.label) === null ? null : formatTimeNs(settledThrough(out.label)),
    lateObjects: p.lateObjects ?? 0,
    index: p.index ? { covered: p.index.covered, scan: p.index.scan, pruned: p.index.pruned, segments: p.index.segments,
      bytes: p.index.bytes, requests: p.index.requests, errors: p.index.errors ?? [] } : null,
    objects: p.objects.length, plannedBytes: p.totalBytes, fetchedBytes: out.stats.bytes, requests: out.stats.requests,
    perObject: [...out.stats.perKey].map(([k, v]) => ({ key: k, bytes: v.bytes, requests: v.requests, size: p.objects.find(o => o.key === k)?.size ?? null })),
    replans: out.replans, missing: out.missing, reason: out.reason ?? '',
    basis: out.basis ?? null, atBasis: p.atBasis === true, cached: out.cached === true, afterBasis: p.afterBasis ?? 0,
    objectsHash: [...p.objects].map(o => o.key).sort().join(','),
    count: r?.count ?? r?.spans?.length ?? r?.points ?? null,
    sum: r?.sum ?? null,
    buckets: r?.buckets?.map(b => ({ from: formatTimeNs(b.fromNs), count: b.count ?? null, state: b.state })) ?? null,
    rows: r?.rows?.map(x => ({ ts: formatTimeNs(x.ts), state: x.state, body: x.body ?? null })) ?? r?.spans?.map(s => ({ ts: formatTimeNs(s.ts), state: s.state, key: s.key })) ?? null,
    events: out.events.map(e => ({ ...e })),
  }
}

// ---- wiring --------------------------------------------------------------------

function selectView(v) {
  state.view = v
  for (const b of document.querySelectorAll('.tabs button')) b.setAttribute('aria-selected', String(b.dataset.view === v))
  for (const f of document.querySelectorAll('form.view')) f.hidden = f.id !== 'view-' + v
}

async function main() {
  $('range').addEventListener('change', () => document.body.classList.toggle('custom-range', $('range').value === 'custom'))
  for (const b of document.querySelectorAll('.tabs button')) b.addEventListener('click', () => selectView(b.dataset.view))
  for (const v of ['logs', 'trace', 'metric']) $('run-' + v).addEventListener('click', () => run(v))
  $('signin').addEventListener('click', () => signIn().catch(e => setBanner('error', esc(e.message))))
  $('signout').addEventListener('click', () => setToken(null))
  $('token-use').addEventListener('click', () => setToken($('token-input').value.trim() || null))
  try {
    await loadConfig()
    await finishSignIn()
  } catch (e) {
    setBanner('error', esc(e.message))
  }
  showWho()
  document.body.dataset.ready = '1'
}

window.lakeui = { state, planner, run, selectView }
main()
