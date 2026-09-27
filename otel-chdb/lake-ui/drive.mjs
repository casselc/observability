// Headless driver for index.html: runs window.lakeRun() in Chromium and
// counts, per query step, the S3 requests and bytes the page made (range GETs
// included), next to what the page reports.
//   PLAYWRIGHT_BROWSERS_PATH=/opt/pw-browsers node drive.mjs [trace_id] [duckdb-wasm version]
import { createRequire } from 'node:module';
const require = createRequire(import.meta.url);
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || '/opt/node22/lib/node_modules/playwright');

const PAGE = process.env.LAKEUI_PAGE || 'http://127.0.0.1:18190/index.html';
const trace = process.argv[2];
const ver = process.argv[3] || '1.32.0';
// Playwright's `proxy` option adds <-loopback>, which would send the page and
// S3 on localhost through the proxy too; pass Chromium's own flags instead.
const args = process.env.HTTPS_PROXY
  ? [`--proxy-server=${process.env.HTTPS_PROXY}`, '--proxy-bypass-list=127.0.0.1;localhost'] : [];

const browser = await chromium.launch({ args, channel: process.env.PW_CHANNEL || undefined });
const ctx = await browser.newContext();
const page = await ctx.newPage();
page.on('response', (r) => { if (r.status() >= 400) console.error('HTTP', r.status(), r.url().slice(0, 150)); });
page.on('console', (m) => { if (m.type() === 'error') console.error('console:', m.text().slice(0, 300)); });

const cnt = () => ({ s3Req: 0, s3Bytes: 0, s3Ranged: 0, cdnReq: 0, cdnBytes: 0, planReq: 0 });
let c = cnt();
const all = cnt();
async function account(req) {
  const u = new URL(req.url());
  let size = 0;
  try { size = (await req.sizes()).responseBodySize; } catch { /* aborted */ }
  const add = (k, v) => { c[k] += v; all[k] += v; };
  if (u.port === (process.env.LAKEUI_S3PORT || '18334') && process.env.LAKEUI_DEBUG) {
    const resp = await req.response();
    console.error('S3', req.method(), resp && resp.status(), req.headers()['range'] || '-', size,
      resp && (await resp.allHeaders())['content-length'], u.pathname.slice(-40));
  }
  if (u.port === (process.env.LAKEUI_S3PORT || '18334')) {
    add('s3Req', 1); add('s3Bytes', size);
    if (req.headers()['range']) add('s3Ranged', 1);
  } else if (u.hostname.endsWith('jsdelivr.net') || u.hostname.endsWith('duckdb.org') || u.pathname.startsWith('/vendor/')) {
    add('cdnReq', 1); add('cdnBytes', size);
  } else if (u.pathname === '/plan') add('planReq', 1);
}
const pending = [];
ctx.on('requestfinished', (r) => pending.push(account(r)));
ctx.on('requestfailed', (r) => console.error('failed:', r.url().slice(0, 120), r.failure()?.errorText));

const rows = [];
await page.exposeFunction('__report', async (step) => {
  await Promise.all(pending.splice(0));
  rows.push({ ...step, net: c });
  c = cnt();
});
await page.goto(`${PAGE}?v=${ver}${process.env.LAKEUI_SRC ? '&src=' + process.env.LAKEUI_SRC : ''}${process.env.LAKEUI_BUNDLE ? '&bundle=' + process.env.LAKEUI_BUNDLE : ''}${process.env.LAKEUI_Q || ''}`);
await page.waitForFunction(() => window.lakeReady === true, null, { timeout: 60000 });
await Promise.all(pending.splice(0));
const loadNet = c; c = cnt();
const token = process.env.LAKEUI_TOKEN;
const res = await page.evaluate(([trace, token]) => window.lakeRun({ trace, token, onStep: (s) => window.__report(s) }),
  [trace, token]);
if (process.env.LAKEUI_HYPARQUET) {
  await page.evaluate(([trace, token]) => window.lakeRunHyparquet({ trace, token, onStep: (s) => window.__report(s) }),
    [trace, token]);
}
// Scope checks against the share endpoint: no token; a token for another
// cluster; a token for the right one (LAKEUI_TOKEN_SCOPED: clusters=cluster-2).
const scoped = process.env.LAKEUI_TOKEN_SCOPED;
const authz = await page.evaluate(async (scoped) => {
  const get = async (qs, tok) => {
    const r = await fetch('/plan?' + qs, tok ? { headers: { Authorization: 'Bearer ' + tok } } : {});
    return r.ok ? { status: r.status, objects: (await r.json()).urls.length } : { status: r.status, body: await r.text() };
  };
  return {
    noToken: await get('signal=traces&service=svc-2-3'),
    otherCluster: await get('signal=logs&service=svc-1-2', scoped),
    ownCluster: await get('signal=traces&service=svc-2-3', scoped),
    fleetWide: await get('signal=logs', scoped),
  };
}, scoped);
await Promise.all(pending.splice(0));
console.log(JSON.stringify({ info: res.info, iceberg: res.iceberg, pageLoadNet: loadNet, total: all, authz }, null, 1));
for (const r of rows) {
  console.log([r.name, r.pass, `objects ${r.objects} (${(r.objectBytes / 1e6).toFixed(2)} MB)`,
    `plan ${r.planMs} ms`, r.error ? `ERROR ${r.error.slice(0, 200)}` : `query ${r.ms} ms, ${r.rows} rows`,
    `S3 ${r.net.s3Req} req (${r.net.s3Ranged} ranged) ${(r.net.s3Bytes / 1e6).toFixed(3)} MB`,
    `cdn ${r.net.cdnReq} req ${(r.net.cdnBytes / 1e6).toFixed(2)} MB`].join(' | '));
}
console.log('SAMPLES ' + JSON.stringify(rows.filter((r) => r.pass === 'cold').map((r) => ({ n: r.name, s: r.sample, p: r.planSteps }))));
await browser.close();
