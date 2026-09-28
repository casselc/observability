// Drives the adapter with @clickhouse/client (node), the version HyperDX
// 2.39.1 pins, the way HyperDX's ClickhouseClient calls it: query_params,
// FORMAT via `format`, settings, use_multipart_params_auto, and the token as
// the query's `auth` (what the fork's node client sends; a per-query
// http_headers Authorization is overwritten by the client's Basic header). Prints one JSON line of findings; replay_test.go checks it.
const { createClient } = require('@clickhouse/client');

async function main() {
  const [url, token, db] = process.argv.slice(2);
  const out = {};
  const client = createClient({ url, use_multipart_params_auto: true, request_timeout: 30000 });
  const auth = { access_token: token };
  const settings = { date_time_output_format: 'iso', output_format_json_quote_64bit_integers: 1, result_overflow_mode: 'throw' };
  out.ping = (await client.ping()).success;

  const rs = await client.query({
    query: `SELECT ServiceName, count() AS n, max(Timestamp) AS last FROM {db:Identifier}.otel_logs
            WHERE Timestamp >= fromUnixTimestamp64Milli({a:Int64}) AND Timestamp <= fromUnixTimestamp64Milli({b:Int64})
              AND Body != {s:String} GROUP BY ServiceName ORDER BY ServiceName`,
    query_params: { db, a: 1790451629000, b: 1790469629000, s: "it's\t\\ {x:String}" },
    format: 'JSON', clickhouse_settings: settings, auth,
  });
  const j = await rs.json();
  out.json = { rows: j.rows, first: j.data[0], meta: j.meta.map(m => m.type) };
  out.label = ['x-otel-source', 'x-otel-completeness', 'x-otel-complete-through', 'x-otel-window-to', 'x-otel-dropped-settings']
    .map(h => rs.response_headers[h]);

  const rs2 = await client.query({
    query: `SELECT ServiceName, count() AS n FROM {db:Identifier}.otel_traces GROUP BY ServiceName ORDER BY ServiceName`,
    query_params: { db }, format: 'JSONCompactEachRowWithNamesAndTypes', auth,
  });
  const rows = [];
  for await (const batch of rs2.stream()) for (const r of batch) rows.push(r.json());
  out.compact = rows;

  // multipart: parameters over the client's URL budget
  const big = 'x'.repeat(20000);
  const rs3 = await client.query({
    query: `SELECT count() AS n FROM {db:Identifier}.otel_logs WHERE Body = {big:String}`,
    query_params: { db, big }, format: 'JSONEachRow', auth,
  });
  out.multipart = await rs3.json();

  for (const [name, q] of [['explain', `EXPLAIN ESTIMATE SELECT 1 FROM ${db}.otel_logs`], ['system_parts', 'SELECT name FROM system.parts'], ['no_token', 'SELECT 1']]) {
    try {
      await client.query({ query: q, format: 'JSON', ...(name === 'no_token' ? {} : { auth }) });
      out[name] = 'accepted';
    } catch (e) {
      out[name] = { code: e.code, type: e.type, message: String(e.message).slice(0, 120) };
    }
  }
  await client.close();
  console.log(JSON.stringify(out));
}
main().catch(e => { console.error(e); process.exit(1); });
