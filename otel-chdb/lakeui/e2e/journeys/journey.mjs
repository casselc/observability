// What the journeys share: the rig's document, ClickHouse and the tap, the
// page's sign-in and query helpers (as ../lakeui.spec.mjs has them), and
// `journey()`, which takes each step's pictures once its assertions passed.
//
// Pictures: JOURNEYS_OUT/<journey>/<nn>-<slug>.png, clipped to the step's
// region (CSS selectors; `marks` are outlined); JOURNEYS_FRAMES/<journey>/
// <nn>.png, the full viewport with the region outlined and a caption bar,
// for the GIF render.sh assembles. Both default under test-results/, so a
// plain test run never touches docs/.
import { expect } from '@playwright/test'
import { mkdirSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const results = join(here, '..', '..', 'test-results', 'journeys')
const OUT = process.env.JOURNEYS_OUT || join(results, 'img')
const FRAMES = process.env.JOURNEYS_FRAMES || join(results, 'frames')

export const info = JSON.parse(process.env.JNY_RIG ?? 'null')

/** RFC 3339 with up to 9 fraction digits → ns (BigInt), as the page does. */
export const nsOf = iso => {
  const m = /^(.*T\d\d:\d\d:\d\d)(?:\.(\d+))?Z$/.exec(iso)
  return BigInt(Date.parse(m[1] + 'Z')) * 1_000_000n + BigInt(((m[2] ?? '') + '000000000').slice(0, 9))
}
/** ns (BigInt) → RFC 3339 with 9 fraction digits. */
export const isoOf = ns => new Date(Number(ns / 1_000_000n)).toISOString().slice(0, 19) + '.' + String(ns % 1_000_000_000n).padStart(9, '0') + 'Z'

export async function ch(sql) {
  const r = await fetch(info.ch, { method: 'POST', body: sql })
  const t = (await r.text()).trim()
  if (!r.ok) throw new Error(`${sql}: ${t}`)
  return t
}
const t64 = ns => `fromUnixTimestamp64Nano(toInt64(${ns}))`
export const inCluster = (c, col = 'ResourceAttributes') => `${col}['k8s.cluster.name'] = '${c}'`
export async function chLogCount(cluster, fromNs, toNs, where = '1') {
  return Number(await ch(`SELECT count() FROM ${info.db}.otel_logs WHERE ${inCluster(cluster)} AND Timestamp >= ${t64(fromNs)} AND Timestamp < ${t64(toNs)} AND ${where}`))
}

/** The counting pass-through: GET its entries, or POST to reset it. */
export async function tap(method = 'GET') {
  const r = await fetch(new URL('/rig/tap', info.page), { method })
  return method === 'GET' ? (await r.json()).entries ?? [] : null
}
export const rig = (path, method = 'POST') => fetch(new URL(path, info.page), { method })

// Fonts that exist on the dev box and on the CI runner alike, so a picture
// does not depend on which system-ui the machine resolves.
const FONTS = `body, input, select, textarea, button { font-family: "DejaVu Sans", sans-serif !important; }
pre, code, .ts, .body { font-family: "DejaVu Sans Mono", monospace !important; }
*, *::before, *::after { transition: none !important; animation: none !important; caret-color: transparent !important; }`

/** A new browser context signed in as user through the IdP's page (path: another page of the rig's). */
export async function signedIn(browser, user, path = '') {
  const ctx = await browser.newContext()
  await ctx.addInitScript(css => {
    document.addEventListener('DOMContentLoaded', () => {
      const s = document.createElement('style')
      s.textContent = css
      document.head.append(s)
    })
  }, FONTS)
  const page = await ctx.newPage()
  page.on('pageerror', e => console.log('pageerror', e.message))
  await page.goto(info.page + path)
  await page.waitForSelector('body[data-ready="1"]')
  await page.click('#signin')
  await page.click(`text=Sign in as ${user}`)
  await page.waitForSelector('body[data-ready="1"]')
  await expect(page.locator('#who')).toContainText(user)
  return page
}

/** Runs one view over a custom window; returns the page's summary of the run. */
export async function runView(page, view, { from, to, cluster = '', set = async () => {} }) {
  await page.evaluate(v => window.lakeui.selectView(v), view)
  await page.selectOption('#cluster', cluster)
  await page.selectOption('#range', 'custom')
  await page.fill('#from', from)
  await page.fill('#to', to)
  await set()
  const n = Number(await page.evaluate(() => document.body.dataset.runs ?? '0'))
  await page.click(`#run-${view}`)
  await page.waitForFunction(k => Number(document.body.dataset.runs ?? '0') > k, n)
  return page.evaluate(() => window.lakeui.state.last)
}

/**
 * A journey's picture taker. step() is called after the step's assertions:
 * a failed assertion throws before it, so no picture of a wrong state is
 * ever written.
 */
export function journey(name, title) {
  let n = 0
  const captions = []
  return {
    async step(page, slug, caption, { region, marks = [], maxHeight = 520, pad = 8 }) {
      n++
      const id = String(n).padStart(2, '0')
      await page.evaluate(() => document.fonts.ready)
      // the region's rectangle in page coordinates (union of the selectors')
      const rect = await page.evaluate(sels => {
        let t = Infinity, l = Infinity, b = -Infinity, r = -Infinity
        for (const s of sels) {
          for (const el of document.querySelectorAll(s)) {
            const q = el.getBoundingClientRect()
            if (!q.width && !q.height) continue
            t = Math.min(t, q.top + scrollY); l = Math.min(l, q.left + scrollX)
            b = Math.max(b, q.bottom + scrollY); r = Math.max(r, q.right + scrollX)
          }
        }
        return t === Infinity ? null : { t, l, b, r, w: document.documentElement.scrollWidth, h: document.documentElement.scrollHeight }
      }, region)
      expect(rect, `step ${id} ${slug}: region ${region.join(', ')} not on the page`).not.toBe(null)
      // outlines around the marks (kept for both pictures, then removed)
      await page.evaluate(sels => {
        for (const s of sels) {
          for (const el of document.querySelectorAll(s)) {
            const q = el.getBoundingClientRect()
            const d = document.createElement('div')
            d.className = 'jny-overlay'
            Object.assign(d.style, { position: 'absolute', left: `${q.left + scrollX - 3}px`, top: `${q.top + scrollY - 3}px`, width: `${q.width + 6}px`,
              height: `${q.height + 6}px`, outline: '3px solid #d9480f', borderRadius: '4px', pointerEvents: 'none', zIndex: 9998 })
            document.body.append(d)
          }
        }
      }, marks)
      const x = Math.max(0, Math.floor(rect.l - pad))
      const y = Math.max(0, Math.floor(rect.t - pad))
      const clip = { x, y, width: Math.min(rect.w - x, Math.ceil(rect.r - rect.l + 2 * pad)), height: Math.min(maxHeight, rect.h - y, Math.ceil(rect.b - rect.t + 2 * pad)) }
      mkdirSync(join(OUT, name), { recursive: true })
      await page.screenshot({ path: join(OUT, name, `${id}-${slug}.png`), clip, fullPage: true, animations: 'disabled', caret: 'hide', scale: 'css' })
      // the GIF frame: the region scrolled under a caption bar and outlined
      await page.evaluate(({ rect, text, maxHeight }) => {
        const bar = document.createElement('div')
        bar.className = 'jny-overlay'
        bar.textContent = text
        Object.assign(bar.style, { position: 'fixed', left: 0, right: 0, top: 0, padding: '10px 16px', background: '#1f2937', color: '#fff',
          font: '600 16px "DejaVu Sans", sans-serif', zIndex: 9999 })
        document.body.append(bar)
        const box = document.createElement('div')
        box.className = 'jny-overlay'
        Object.assign(box.style, { position: 'absolute', left: `${rect.l - 6}px`, top: `${rect.t - 6}px`, width: `${rect.r - rect.l + 12}px`,
          height: `${Math.min(maxHeight, rect.b - rect.t) + 12}px`, outline: '2px dashed #2563eb', borderRadius: '6px', pointerEvents: 'none', zIndex: 9997 })
        document.body.append(box)
        window.scrollTo(0, Math.max(0, rect.t - 56))
      }, { rect, text: `${title} · ${n}. ${caption}`, maxHeight })
      mkdirSync(join(FRAMES, name), { recursive: true })
      await page.screenshot({ path: join(FRAMES, name, `${id}.png`), animations: 'disabled', caret: 'hide', scale: 'css' })
      await page.evaluate(() => { for (const e of document.querySelectorAll('.jny-overlay')) e.remove(); window.scrollTo(0, 0) })
      captions.push({ step: n, file: `${name}/${id}-${slug}.png`, caption })
      writeFileSync(join(OUT, name, 'steps.json'), JSON.stringify({ journey: name, title, steps: captions }, null, 1) + '\n')
    },
  }
}
