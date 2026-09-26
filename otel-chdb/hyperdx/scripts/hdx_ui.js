// Drives HyperDX's UI headless (Playwright + Chromium) through the screens
// of the evaluation (../README.md). Before each scenario it PUTs the scenario's
// tag to chproxy.py, so every SQL statement the UI sends is attributed to it
// in chproxy.jsonl. Writes OUT/ui.json (per scenario: console errors, error
// banners shown, text excerpts) and OUT/<tag>.png for the scenarios with a shot.
//
//   NODE_PATH=$(npm root -g) IDS=hdx_ids.json OUT=ui node hdx_ui.js [tag-regex]
//
// Env: HDX (default http://localhost:18880), PROXY (http://127.0.0.1:18124),
// TRACE_ID (a trace to open), POD (a k8s.pod.name value), EMAIL, PASSWORD,
// FROM_MS / TO_MS (the time range, default the last 3 h), TAG_PREFIX, VIEW_H
// (viewport height, default 900), CLIP=1 (screenshots without the sidebar).
const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');

const HDX = process.env.HDX || 'http://localhost:18880';
const PROXY = process.env.PROXY || 'http://127.0.0.1:18124';
const OUT = process.env.OUT || 'ui';
const ids = JSON.parse(fs.readFileSync(process.env.IDS || 'hdx_ids.json'));
const dash = fs.existsSync(process.env.DASH || 'hdx_dash.json') ? JSON.parse(fs.readFileSync(process.env.DASH || 'hdx_dash.json')) : {};
const only = process.argv[2] ? new RegExp(process.argv[2]) : null;
const TRACE_ID = process.env.TRACE_ID || '';
const POD = process.env.POD || 'payment-d889-0';
fs.mkdirSync(OUT, { recursive: true });

const to = Number(process.env.TO_MS || Date.now());
const from = Number(process.env.FROM_MS || to - 3 * 3600e3);
const TAG = process.env.TAG_PREFIX || '';
const VIEW_H = Number(process.env.VIEW_H || 900);
// CLIP=1: leave the navigation sidebar and the banner out of the screenshots (smaller PNGs)
const shotOpts = process.env.CLIP ? { clip: { x: 250, y: 56, width: 1150, height: VIEW_H - 56 } } : {};
const range = `from=${from}&to=${to}`;
const B = ids['Metrics (layout B views)'];
const STOCK = ids['Metrics (stock)'];

const enc = s => encodeURIComponent(s);
const search = (source, where, lang = 'lucene', extra = '') =>
  `/search?source=${source}&where=${enc(enc(where))}&whereLanguage=${lang}&isLive=false&${range}${extra}`;
const metric = (source, sel, groupBy = '', where = '', displayType = 'line', whereLanguage = 'lucene') => {
  const cfg = {
    name: '', source, displayType, where, whereLanguage, granularity: 'auto', groupBy,
    select: [{ aggCondition: '', aggConditionLanguage: 'lucene', valueExpression: 'Value', ...sel }],
  };
  return `/chart?config=${enc(JSON.stringify(cfg))}&${range}`;
};

// metric scenarios, run once per metric source (B views, then stock)
const metricScenarios = [
  ['gauge-avg', { aggFn: 'avg', metricName: 'container.cpu.utilization', metricType: 'gauge' }],
  ['gauge-groupby-attr', { aggFn: 'avg', metricName: 'system.cpu.utilization', metricType: 'gauge' }, "Attributes['state']"],
  ['gauge-groupby-res', { aggFn: 'max', metricName: 'container.memory.working_set', metricType: 'gauge' }, "ResourceAttributes['k8s.pod.name']"],
  ['gauge-where-res', { aggFn: 'avg', metricName: 'container.cpu.utilization', metricType: 'gauge' }, '', `ResourceAttributes.k8s.pod.name:"${POD}"`],
  ['gauge-where-res-sql', { aggFn: 'avg', metricName: 'container.cpu.utilization', metricType: 'gauge' }, "ResourceAttributes['service.name']", "ResourceAttributes['cloud.region'] = 'eu-west-1'", 'line', 'sql'],
  ['sum-cumulative', { aggFn: 'sum', metricName: 'http.server.request.count', metricType: 'sum' }, "Attributes['http.route']"],
  ['sum-cumulative-increase', { aggFn: 'increase', metricName: 'http.server.request.count', metricType: 'sum' }, 'ServiceName'],
  ['sum-delta', { aggFn: 'sum', metricName: 'app.orders.placed', metricType: 'sum' }, "Attributes['payment.method']"],
  ['sum-updown', { aggFn: 'avg', metricName: 'db.client.connections.usage', metricType: 'sum' }, "Attributes['state']"],
  ['hist-p95', { aggFn: 'quantile', level: 0.95, metricName: 'http.server.request.duration', metricType: 'histogram' }, "Attributes['http.route']"],
  ['hist-count', { aggFn: 'count', metricName: 'http.server.request.duration', metricType: 'histogram' }],
  ['gauge-where-cluster', { aggFn: 'avg', metricName: 'app.custom.metric.007', metricType: 'gauge' }, 'ServiceName', 'ResourceAttributes.k8s.cluster.name:"big"'],
  ['exphist-p50', { aggFn: 'quantile', level: 0.5, metricName: 'rpc.server.duration', metricType: 'exponential histogram' }, 'ServiceName'],
  ['summary', { aggFn: 'avg', metricName: 'jvm.gc.pause', metricType: 'summary' }],
];

const scenarios = [
  { tag: 'logs-all', url: search(ids.Logs, ''), shot: true },
  { tag: 'logs-fulltext', url: search(ids.Logs, '"card declined"'), shot: false },
  { tag: 'logs-attr', url: search(ids.Logs, 'LogAttributes.http.route:"/api/cart"'), shot: false },
  { tag: 'logs-res-attr', url: search(ids.Logs, `ResourceAttributes.k8s.pod.name:"${POD}" SeverityText:ERROR`), shot: true },
  { tag: 'logs-sql', url: search(ids.Logs, "ResourceAttributes['cloud.region'] = 'us-east-1' AND Body ILIKE '%failed%'", 'sql'), shot: false },
  { tag: 'traces-all', url: search(ids.Traces, ''), shot: true },
  { tag: 'traces-errors', url: search(ids.Traces, 'StatusCode:Error ServiceName:payment'), shot: false },
  { tag: 'traces-res-attr', url: search(ids.Traces, 'ResourceAttributes.telemetry.sdk.language:java SpanAttributes.db.system:redis'), shot: false },
  { tag: 'trace-waterfall', url: search(ids.Traces, '', 'lucene', TRACE_ID ? `&traceId=${TRACE_ID}` : ''), shot: true, action: 'openRow' },
  { tag: 'service-map', url: `/service-map?source=${ids.Traces}&${range}`, shot: true },
  { tag: 'services', url: `/services?${range}`, shot: true },
  { tag: 'kubernetes', url: `/kubernetes?${range}`, shot: false },
  ...(dash.dashboard_b ? [
    { tag: 'dashboard-b', url: `/dashboards/${dash.dashboard_b}?${range}`, shot: true },
    { tag: 'dashboard-stock', url: `/dashboards/${dash.dashboard_stock}?${range}`, shot: true },
    { tag: 'alerts', url: `/alerts`, shot: true },
  ] : []),
  ...[['b', B], ['stock', STOCK]].flatMap(([k, src]) => [
    { tag: `picker-${k}`, url: metric(src, { aggFn: 'avg', metricName: 'container.cpu.utilization', metricType: 'gauge' }), shot: k === 'b', action: 'picker' },
    ...metricScenarios.map(([t, sel, g, w, d, l]) => ({ tag: `m-${t}-${k}`, url: metric(src, sel, g, w, d, l), shot: k === 'b' })),
  ]),
];

async function put(tag) {
  await fetch(`${PROXY}/__tag`, { method: 'PUT', body: tag });
}

async function settle(p, ms) {
  await p.waitForTimeout(1500);
  try {
    await p.waitForLoadState('networkidle', { timeout: ms });
  } catch {}
  await p.waitForTimeout(1500);
}

(async () => {
  const b = await chromium.launch({ executablePath: '/opt/pw-browsers/chromium' });
  const ctx = await b.newContext({ viewport: { width: 1400, height: VIEW_H }, deviceScaleFactor: 1 });
  const p = await ctx.newPage();
  let errors = [];
  p.on('console', m => { if (m.type() === 'error' && !/status of 401/.test(m.text())) errors.push(m.text().slice(0, 400)); });
  p.on('pageerror', e => errors.push('pageerror: ' + String(e).slice(0, 400)));
  await put('login');
  await p.goto(`${HDX}/login`);
  await p.fill('input[name=email]', process.env.EMAIL || 'eval@example.com');
  await p.fill('input[name=password]', process.env.PASSWORD || 'Hdx-eval-2026!');
  await p.click('button[type=submit]');
  await p.waitForTimeout(3000);
  // dismiss the banner once, so it isn't in every screenshot
  const results = fs.existsSync(path.join(OUT, 'ui.json')) ? JSON.parse(fs.readFileSync(path.join(OUT, 'ui.json'))) : {};
  for (const s of scenarios) {
    if (only && !only.test(s.tag)) continue;
    errors = [];
    s.tag = TAG + s.tag;
    await put(s.tag);
    const t0 = Date.now();
    await p.goto(HDX + s.url);
    await settle(p, 20000);
    const extra = {};
    if (s.action === 'openRow') {
      if (!TRACE_ID) {
        const row = p.locator('table tbody tr').nth(1);
        await row.click().catch(e => (extra.clickError = String(e).slice(0, 200)));
      }
      await settle(p, 20000);
    }
    if (s.action === 'picker') {
      // the metric-name select shows the current name; open it, then search
      const input = p.locator('[data-testid=metric-name-selector]').first();
      await input.click().catch(e => (extra.clickError = String(e).slice(0, 200)));
      await settle(p, 20000);
      extra.options = await p.locator('[role=option]').allInnerTexts().catch(() => []);
      if (s.shot) await p.screenshot({ path: path.join(OUT, `${s.tag}.png`), ...shotOpts });
      await put(s.tag + '-search');
      await input.fill('http');
      await settle(p, 20000);
      extra.searchOptions = await p.locator('[role=option]').allInnerTexts().catch(() => []);
      await p.keyboard.press('Escape');
      // the metric catalog (explorer modal): names with unit/description, grouped
      await put(s.tag + '-catalog');
      await p.locator('[data-testid=metric-explorer-open]').first().click().catch(e => (extra.catalogError = String(e).slice(0, 200)));
      await settle(p, 20000);
      extra.catalog = (await p.locator('[data-testid=metric-explorer-modal]').innerText().catch(() => '')).slice(0, 600);
      if (s.shot) await p.screenshot({ path: path.join(OUT, `${s.tag}-catalog.png`), ...shotOpts });
      await p.keyboard.press('Escape');
    }
    const banners = await p.locator('.mantine-Alert-root, [role=alert], .mantine-Notification-root').allInnerTexts().catch(() => []);
    const text = (await p.locator('main, body').first().innerText().catch(() => '')).replace(/\s+/g, ' ');
    const errText = [...text.matchAll(/(Error[^.]{0,200}|Code: \d+[^.]{0,200}|No results[^.]{0,80}|No data[^.]{0,80})/g)].map(m => m[0]).slice(0, 5);
    if (s.shot && s.action !== 'picker') await p.screenshot({ path: path.join(OUT, `${s.tag}.png`), ...shotOpts });
    results[s.tag] = { url: s.url, ms: Date.now() - t0, errors: [...new Set(errors)], banners, errText, ...extra };
    console.log(s.tag, Date.now() - t0, 'ms', errors.length, 'console errors', errText.slice(0, 2).join(' | ').slice(0, 200));
  }
  await put('');
  fs.writeFileSync(path.join(OUT, 'ui.json'), JSON.stringify(results, null, 1));
  await b.close();
})();
