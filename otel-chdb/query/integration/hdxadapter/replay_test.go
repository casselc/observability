// Package hdxadapter replays HyperDX 2.39.1's captured statements (the 799
// of ../../../hyperdx/results/schema-replay.json and schema3-replay.json)
// through the adapter and the query service onto a local ClickHouse, and
// compares each answer with what ClickHouse itself returns for the same
// statement and parameters.
//
//	HDXA_IT=1 go test ./integration/hdxadapter -v -timeout 20m
//
// ClickHouse at HDXA_CH (default http://127.0.0.1:18123). It creates the
// databases hdx_it_old, hdx_it_new and hdx_it_full (the three schema sides of
// the capture: pre-alignment, option 2, full ClickStack) and the read-only
// user hdx_it_ro, loads the same synthetic rows into each, and drops them at
// the end (HDXA_IT_KEEP=1 keeps them). HDXA_IT_OUT=<dir> writes
// adapter-replay.jsonl there (the counts, then one line per statement).
package hdxadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/query/internal/audit"
	"github.com/casselc/observability/otel-chdb/query/internal/auth"
	"github.com/casselc/observability/otel-chdb/query/internal/auth/authtest"
	"github.com/casselc/observability/otel-chdb/query/internal/central"
	"github.com/casselc/observability/otel-chdb/query/internal/completeness"
	"github.com/casselc/observability/otel-chdb/query/internal/hdxadapter"
	"github.com/casselc/observability/otel-chdb/query/internal/server"
	"github.com/casselc/observability/otel-chdb/query/internal/sqlscope"
	"github.com/casselc/observability/otel-chdb/query/internal/store"
	"github.com/golang-jwt/jwt/v5"
)

var sides = map[string]string{"hdx_old": "hdx_it_old", "hdx_new": "hdx_it_new", "hdx_full": "hdx_it_full"}

const (
	roUser = "hdx_it_ro"
	roPass = "hdx-it-ro-pw"
	// the capture's time range (ms), and complete_through inside it
	t0Ms  = int64(1790451629000)
	t1Ms  = int64(1790473229000)
	ctMs  = int64(1790469700000)
	nRows = 20000
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

type chc struct {
	t   *testing.T
	url string
}

func (c chc) do(sql string, params url.Values, user string) (int, []byte) {
	u := c.url + "/"
	if len(params) > 0 {
		u += "?" + params.Encode()
	}
	req, _ := http.NewRequest(http.MethodPost, u, strings.NewReader(sql))
	if user != "" {
		req.Header.Set("X-ClickHouse-User", user)
		req.Header.Set("X-ClickHouse-Key", roPass)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func (c chc) exec(sql string) {
	c.t.Helper()
	if code, b := c.do(sql, nil, ""); code != 200 {
		c.t.Fatalf("%s: %s", firstN(sql, 200), b)
	}
}

func firstN(s string, n int) string {
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}

// ddl splits a DDL file into statements with its placeholders substituted.
func ddl(t *testing.T, path string, subs map[string]string) []string {
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var lines []string
	for _, l := range strings.Split(string(b), "\n") {
		if !strings.HasPrefix(strings.TrimSpace(l), "--") {
			lines = append(lines, l)
		}
	}
	body := strings.Join(lines, "\n")
	for k, v := range subs {
		body = strings.ReplaceAll(body, "{"+k+"}", v)
	}
	var out []string
	for _, s := range strings.Split(body, ";\n") {
		if s = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(s), ";")); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func setup(t *testing.T, c chc, root string) {
	for _, db := range sides {
		c.exec("DROP DATABASE IF EXISTS " + db)
		c.exec("CREATE DATABASE " + db)
	}
	for _, s := range ddl(t, filepath.Join(root, "hyperdx", "sql", "pre_alignment_traces_logs.sql"), map[string]string{"db": "hdx_it_old"}) {
		if strings.HasPrefix(s, "CREATE TABLE") {
			c.exec(s)
		}
	}
	for _, sig := range []string{"logs", "traces"} {
		for _, s := range ddl(t, filepath.Join(root, "otap-rs", "sql", "otel_"+sig+".sql"), map[string]string{"table": "hdx_it_new.otel_" + sig}) {
			c.exec(s)
		}
		for _, s := range ddl(t, filepath.Join(root, "hyperdx", "sql", "clickstack_full_"+sig+".sql"), map[string]string{"table": "hdx_it_full.otel_" + sig}) {
			c.exec(s)
		}
	}
	// the same rows on every side: two clusters (qa, qb), two namespaces, the
	// capture's values (pod frontend-f778-0, route /api/cart, db.system
	// redis, trace 7016c445…) among others; deterministic (no rand())
	span := t1Ms - t0Ms
	for _, db := range sides {
		// received_at trails Timestamp by 0 to 90 s: a third of the rows
		// are later than the service's max_lateness (60 s), so the
		// late-row count has something to count
		c.exec(fmt.Sprintf(`INSERT INTO %s.otel_logs (Timestamp, TraceId, SpanId, TraceFlags, SeverityText, SeverityNumber, ServiceName, Body, ResourceAttributes, ScopeName, LogAttributes, received_at)
SELECT fromUnixTimestamp64Milli(toInt64(%d + (cityHash64(number) %% %d))) AS ts,
  if(number %% 500 = 0, '7016c445d992e445e6d36e08529e9487', hex(cityHash64(number, 1))), hex(cityHash64(number, 2)), 1,
  ['INFO', 'ERROR', 'WARN'][1 + number %% 3], [9, 17, 13][1 + number %% 3],
  ['frontend', 'payment', 'cart'][1 + number %% 3],
  ['card declined for order ' || toString(number), 'payment failed: timeout', 'ERROR connecting to redis', 'ok'][1 + intDiv(number, 3) %% 4],
  map('k8s.cluster.name', ['qa', 'qb'][1 + number %% 2], 'k8s.namespace.name', ['shop', 'cart'][1 + intDiv(number, 2) %% 2],
      'k8s.pod.name', ['frontend-f778-0', 'payment-5c9d-1', 'cart-77aa-2'][1 + number %% 3], 'service.name', ['frontend', 'payment', 'cart'][1 + number %% 3],
      'cloud.region', ['us-east-1', 'us-west-2'][1 + intDiv(number, 7) %% 2], 'telemetry.sdk.language', ['java', 'go'][1 + intDiv(number, 5) %% 2],
      'k8s.node.name', 'node-' || toString(number %% 4), 'host.name', 'host-' || toString(number %% 4)),
  'scope', map('http.route', ['/api/cart', '/api/v1/orders'][1 + number %% 2], 'payment.method', ['card', 'paypal'][1 + number %% 2]),
  ts + toIntervalMillisecond(cityHash64(number, 7) %% 90000)
FROM numbers(%d)`, db, t0Ms, span, nRows))
		c.exec(fmt.Sprintf(`INSERT INTO %s.otel_traces (Timestamp, TraceId, SpanId, ParentSpanId, SpanName, SpanKind, ServiceName, ResourceAttributes, SpanAttributes, Duration, StatusCode, received_at)
SELECT fromUnixTimestamp64Milli(toInt64(%d + (cityHash64(number, 9) %% %d))) AS ts,
  if(number %% 500 < 5, '7016c445d992e445e6d36e08529e9487', hex(cityHash64(number, 3))), hex(cityHash64(number, 4)),
  if(number %% 500 = 0, '', hex(cityHash64(number - 1, 4))), ['GET /api/cart', 'charge', 'redis GET'][1 + number %% 3], 'Server',
  ['frontend', 'payment', 'cart'][1 + number %% 3],
  map('k8s.cluster.name', ['qa', 'qb'][1 + number %% 2], 'k8s.namespace.name', ['shop', 'cart'][1 + intDiv(number, 2) %% 2],
      'k8s.pod.name', ['frontend-f778-0', 'payment-5c9d-1', 'cart-77aa-2'][1 + number %% 3], 'service.name', ['frontend', 'payment', 'cart'][1 + number %% 3],
      'cloud.region', ['us-east-1', 'us-west-2'][1 + intDiv(number, 7) %% 2], 'telemetry.sdk.language', ['java', 'go'][1 + intDiv(number, 5) %% 2]),
  map('http.route', ['/api/cart', '/api/v1/orders'][1 + number %% 2], 'db.system', ['redis', 'postgres'][1 + number %% 2],
      'http.response.status_code', ['200', '500'][1 + intDiv(number, 11) %% 2]),
  1000 + cityHash64(number, 5) %% 5000000000, ['Ok', 'Error', 'Unset'][1 + number %% 3],
  ts + toIntervalMillisecond(cityHash64(number, 8) %% 90000)
FROM numbers(%d)`, db, t0Ms, span, nRows))
	}
	c.exec("DROP USER IF EXISTS " + roUser)
	c.exec(fmt.Sprintf("CREATE USER %s IDENTIFIED WITH sha256_password BY '%s' SETTINGS readonly = 2", roUser, roPass))
	// the metadata tables the service serves (26.10 needs a grant for
	// data_skipping_indices; the others are readable by every user, filtered
	// to what the user may see)
	for _, tb := range []string{"tables", "columns", "data_skipping_indices", "settings", "table_engines", "databases"} {
		c.exec(fmt.Sprintf("GRANT SELECT ON system.%s TO %s", tb, roUser))
	}
	for _, db := range sides {
		for _, tb := range []string{"otel_logs", "otel_traces"} {
			c.exec(fmt.Sprintf("GRANT SELECT ON %s.%s TO %s", db, tb, roUser))
		}
		if db != "hdx_it_old" {
			for _, tb := range []string{"otel_logs_kv_rollup_15m", "otel_traces_kv_rollup_15m"} {
				c.exec(fmt.Sprintf("GRANT SELECT ON %s.%s TO %s", db, tb, roUser))
			}
			// HyperDX finds a rollup through its materialized view's
			// definition in system.tables: SHOW, not SELECT
			for _, mv := range []string{"otel_logs_attr_kv_rollup_15m_mv", "otel_traces_kv_rollup_15m_mv"} {
				c.exec(fmt.Sprintf("GRANT SHOW TABLES ON %s.%s TO %s", db, mv, roUser))
			}
		}
	}
}

func teardown(c chc) {
	for _, db := range sides {
		c.do("DROP DATABASE IF EXISTS "+db, nil, "")
	}
	c.do("DROP USER IF EXISTS "+roUser, nil, "")
}

type stmt struct {
	Scenario string            `json:"scenario"`
	Side     string            `json:"side"`
	Query    string            `json:"query"`
	Params   map[string]string `json:"params"`
}

func corpus(t *testing.T, root string) []stmt {
	var out []stmt
	for _, f := range []string{"schema-replay.json", "schema3-replay.json"} {
		b, err := os.ReadFile(filepath.Join(root, "hyperdx", "results", f))
		if err != nil {
			t.Fatal(err)
		}
		var d struct{ Statements []stmt }
		if err := json.Unmarshal(b, &d); err != nil {
			t.Fatal(err)
		}
		for _, s := range d.Statements {
			for k, v := range s.Params {
				if n, ok := sides[v]; ok {
					s.Params[k] = n
				}
			}
			out = append(out, s)
		}
	}
	return out
}

// hdxSettings are the settings HyperDX 2.39.1 (with the fork's patch 0001)
// puts on every query; the adapter drops all but date_time_output_format.
func hdxSettings() url.Values {
	v := url.Values{}
	for k, x := range map[string]string{"allow_experimental_analyzer": "1", "date_time_output_format": "iso", "wait_end_of_query": "0",
		"cancel_http_readonly_queries_on_client_close": "1", "output_format_json_quote_64bit_integers": "1",
		"query_plan_optimize_lazy_materialization": "1", "use_skip_indexes_on_data_read": "1", "enable_full_text_index": "1"} {
		v.Set(k, x)
	}
	for _, m := range central.OverflowModes {
		v.Set(m, "throw")
	}
	return v
}

var formatRE = regexp.MustCompile(`(?i)FORMAT\s+(\w+)\s*$`)

// canon reads an answer as its column names and types and its rows (each a
// JSON text), whatever the format.
type canon struct {
	cols []string
	rows []string
}

func parse(format string, body []byte) (canon, error) {
	var c canon
	switch format {
	case "JSON":
		var r struct {
			Meta []struct{ Name, Type string }
			Data []json.RawMessage
		}
		d := json.NewDecoder(bytes.NewReader(body))
		d.UseNumber()
		if err := d.Decode(&r); err != nil {
			return c, err
		}
		for _, m := range r.Meta {
			c.cols = append(c.cols, m.Name+" "+m.Type)
		}
		res, err := hdxadapter.ParseResult(body)
		if err != nil {
			return c, err
		}
		for _, row := range res.Rows {
			b, _ := json.Marshal(row)
			c.rows = append(c.rows, string(b))
		}
	default:
		for _, l := range strings.Split(strings.TrimSpace(string(body)), "\n") {
			if l != "" {
				var buf bytes.Buffer
				if err := json.Compact(&buf, []byte(l)); err != nil {
					return c, err
				}
				c.rows = append(c.rows, buf.String())
			}
		}
	}
	return c, nil
}

func same(a, b canon) (bool, bool) {
	eq := strings.Join(a.cols, "|") == strings.Join(b.cols, "|") && len(a.rows) == len(b.rows)
	if !eq {
		return false, false
	}
	exact := true
	for i := range a.rows {
		if a.rows[i] != b.rows[i] {
			exact = false
		}
	}
	if exact {
		return true, true
	}
	x, y := append([]string(nil), a.rows...), append([]string(nil), b.rows...)
	sort.Strings(x)
	sort.Strings(y)
	return false, strings.Join(x, "\n") == strings.Join(y, "\n")
}

type outcome struct {
	Scenario, Side, Kind, Outcome, Detail string
	Window                                bool
	Completeness                          string
}

func TestReplayHyperDXThroughAdapter(t *testing.T) {
	if os.Getenv("HDXA_IT") == "" {
		t.Skip("HDXA_IT=1 runs the replay against a local ClickHouse")
	}
	root := filepath.Join("..", "..", "..")
	c := chc{t: t, url: env("HDXA_CH", "http://127.0.0.1:18123")}
	setup(t, c, root)
	if os.Getenv("HDXA_IT_KEEP") == "" {
		defer teardown(c)
	}

	// the service, wired as cmd/queryd wires it, with the watermark in memory
	is := authtest.New()
	defer is.Close()
	v, err := auth.NewVerifier(auth.OIDCConfig{Issuer: is.URL, Audience: "otel-query"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var tables []*sqlscope.Table
	timeCols := map[string]string{}
	for _, db := range sides {
		for _, tb := range []string{"otel_logs", "otel_traces"} {
			tables = append(tables, &sqlscope.Table{Database: db, Name: tb, TimeColumn: "Timestamp", ReceivedColumn: "received_at", Scope: "columns",
				Cluster: "ResourceAttributes['k8s.cluster.name']", Namespace: "ResourceAttributes['k8s.namespace.name']"})
			timeCols[db+"."+tb] = "Timestamp"
		}
		switch db {
		case "hdx_it_new":
			// option 2's rollups carry a cluster column (D33): scoped like
			// the tables, for fleet and cluster-restricted callers alike
			for _, tb := range []string{"otel_logs_kv_rollup_15m", "otel_traces_kv_rollup_15m"} {
				tables = append(tables, &sqlscope.Table{Database: db, Name: tb, Scope: "columns", Cluster: "cluster"})
			}
		case "hdx_it_full":
			// ClickStack's own rollups have none: fleet callers only
			for _, tb := range []string{"otel_logs_kv_rollup_15m", "otel_traces_kv_rollup_15m"} {
				tables = append(tables, &sqlscope.Table{Database: db, Name: tb, Scope: "fleet"})
			}
		}
	}
	for _, tb := range []string{"tables", "columns", "data_skipping_indices", "settings", "table_engines", "databases"} {
		tables = append(tables, &sqlscope.Table{Database: "system", Name: tb, Scope: "metadata"})
	}
	policy, err := sqlscope.NewPolicy("hdx_it_new", tables, 0)
	if err != nil {
		t.Fatal(err)
	}
	mem := store.NewMem()
	wmDoc, _ := json.Marshal(completeness.Doc{Format: 2, CompleteThroughNs: uint64(ctMs) * 1_000_000, WallMs: uint64(time.Now().UnixMilli())})
	mem.Put("edges/_consumer/watermark.json", wmDoc, nil, time.Now())
	wm := completeness.NewReader(func(ctx context.Context, k string) ([]byte, error) {
		b, _, err := mem.Get(ctx, k)
		return b, err
	}, "edges/_consumer/watermark.json", time.Minute, time.Hour)
	sink := &audit.Memory{}
	perf := shippedPerformance(t, root)
	var pass []string
	for n := range perf {
		pass = append(pass, n)
	}
	srv := &server.Server{Verifier: v, Audit: sink, Policy: policy, Watermark: wm, Performance: perf,
		SampleDefaultRows: 3_000_000, SampleMaxRows: 10_000_000,
		Central: central.New(central.Config{URL: c.url, User: roUser, Password: roPass, Database: "hdx_it_new"}),
		Mapping: &auth.Mapping{ClustersClaim: "clusters", NamespacesClaim: "namespaces", RolesClaim: "roles", GroupsClaim: "groups",
			Groups: map[string]auth.Grant{"sre": {Clusters: []string{"*"}, Namespaces: []string{"*"}, Roles: []string{"query"}}}},
		Limits: server.Limits{Default: central.Limits{MaxExecutionTimeS: 60, MaxRowsToRead: 1e9, MaxResultRows: 1e6, MaxConcurrent: 4}}}
	srv.Init()
	qs := httptest.NewServer(srv.Handler())
	defer qs.Close()
	ad := httptest.NewServer(hdxadapter.New(hdxadapter.Config{QueryURL: qs.URL + "/v1/query", DefaultDatabase: "hdx_it_new", TimeColumns: timeCols,
		PassSettings: pass}, nil))
	defer ad.Close()
	mint := func(cl jwt.MapClaims) string {
		cl["aud"], cl["sub"] = "otel-query", "it-user"
		return is.Mint(cl)
	}
	fleet := mint(jwt.MapClaims{"groups": "sre"})
	qa := mint(jwt.MapClaims{"clusters": "qa", "namespaces": "*", "roles": "query"})

	// the scope filter the service applies for qa, to compute what qa
	// should see straight from ClickHouse
	var qaFilter []string
	for _, tb := range tables {
		if tb.Scope == "columns" {
			qaFilter = append(qaFilter, fmt.Sprintf("'%s.%s':'%s IN (\\'qa\\')'", tb.Database, tb.Name, strings.ReplaceAll(tb.Cluster, "'", "\\'")))
		}
	}
	sort.Strings(qaFilter)
	qaSetting := "{" + strings.Join(qaFilter, ", ") + "}"

	viaAdapter := func(s stmt, tok string) (*http.Response, []byte) {
		q := hdxSettings()
		for k, x := range s.Params {
			q.Set("param_"+k, x)
		}
		q.Set("query_id", "hdx-it")
		req, _ := http.NewRequest(http.MethodPost, ad.URL+"/?"+q.Encode(), strings.NewReader(s.Query))
		req.Header.Set("Authorization", "Bearer "+tok)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return resp, b
	}
	native := func(s stmt, extra url.Values) (int, []byte) {
		q := hdxSettings()
		for k, x := range s.Params {
			q.Set("param_"+k, x)
		}
		for k, x := range extra {
			q[k] = x
		}
		return c.do(s.Query, q, "")
	}

	var outs []outcome
	var lat struct{ via, nat []time.Duration }
	counts := map[string]int{}
	for i, s := range corpus(t, root) {
		o := outcome{Scenario: s.Scenario, Side: s.Side}
		format := "JSON"
		if m := formatRE.FindStringSubmatch(s.Query); m != nil {
			format = m[1]
		}
		t0 := time.Now()
		resp, body := viaAdapter(s, fleet)
		viaDur := time.Since(t0)
		o.Kind = resp.Header.Get("X-Otel-Statement")
		o.Window = resp.Header.Get("X-Otel-Window-From") != ""
		o.Completeness = resp.Header.Get("X-Otel-Completeness")
		t1 := time.Now()
		ncode, nbody := native(s, nil)
		natDur := time.Since(t1)
		if resp.StatusCode == 200 && ncode == 200 {
			lat.via = append(lat.via, viaDur)
			lat.nat = append(lat.nat, natDur)
		}
		switch {
		case resp.StatusCode != 200:
			o.Outcome = "refused:" + resp.Header.Get("X-Otel-Refusal")
			o.Detail = firstN(strings.TrimSpace(string(body)), 300)
		case ncode != 200:
			o.Outcome, o.Detail = "native-error", firstN(string(nbody), 300)
		default:
			a, err1 := parse(format, body)
			n, err2 := parse(format, nbody)
			if resp.Header.Get("X-Otel-Source") == "metadata" && err1 == nil && err2 == nil {
				// the service answers a metadata table's allow-listed
				// columns only: compare with ClickHouse's answer cut to them
				var diff []string
				n, a, diff = project(n, a)
				for _, d := range diff {
					counts["metadata: column "+d]++
				}
			}
			exact, rows := same(a, n)
			switch {
			case err1 != nil || err2 != nil:
				o.Outcome, o.Detail = "unparsed", fmt.Sprint(err1, err2)
			case exact:
				o.Outcome = "equal"
				if len(a.rows) > 0 {
					counts["equal, non-empty"]++
				}
			case rows:
				o.Outcome = "equal-rows"
			default:
				// a statement whose answer ClickHouse itself does not repeat
				_, again := native(s, nil)
				n2, _ := parse(format, again)
				if e2, r2 := same(n, n2); !e2 && !r2 {
					o.Outcome = "nondeterministic"
				} else if v := arrayVerdict(s.Query, a, n); v != verdictDiffers {
					// groupUniqArray and friends: ClickHouse does not define
					// the order of their elements (threads merge in any
					// order); only for a statement that has one
					// (compare_test.go), and never past a cap
					o.Outcome = v
					if v == verdictAtCap {
						o.Detail = "an array reached its aggregate's cap: keep the test data below it"
					}
				} else {
					o.Outcome = "MISMATCH"
					o.Detail = firstN(fmt.Sprintf("rows %d/%d; differing columns %v", len(a.rows), len(n.rows), diffCols(a, n)), 600)
				}
			}
			// the label: complete only for a window that ends by
			// complete_through − max_lateness (event time; CAST row 26)
			if o.Kind == "select" && resp.Header.Get("X-Otel-Source") == "central" {
				to := resp.Header.Get("X-Otel-Window-To")
				want := "partial"
				if to != "" {
					tt, _ := time.Parse(time.RFC3339Nano, to)
					if tt.UnixNano()+int64(wm.MaxLateness()) <= ctMs*1_000_000 {
						want = "complete"
					}
				}
				if got := resp.Header.Get("X-Otel-Max-Lateness-S"); got != "60" {
					t.Errorf("#%d: X-Otel-Max-Lateness-S %q", i, got)
				}
				// late rows: counted for a window, "no_window" otherwise
				lr := resp.Header.Get("X-Otel-Late-Rows")
				if n, err := strconv.ParseInt(lr, 10, 64); err == nil {
					counts[map[bool]string{true: "late rows: counted, > 0", false: "late rows: counted, 0"}[n > 0]]++
				} else {
					counts["late rows: "+lr]++
				}
				if o.Window == (lr == "no_window") {
					t.Errorf("#%d: window %v but X-Otel-Late-Rows %q", i, o.Window, lr)
				}
				if o.Completeness != want {
					t.Errorf("#%d: completeness %q, want %q (window to %q)", i, o.Completeness, want, to)
				}
			}
		}
		// the same statement for a caller who may see cluster qa only must
		// answer what ClickHouse answers with the service's filter
		if resp.StatusCode == 200 && o.Kind == "select" {
			r2, b2 := viaAdapter(s, qa)
			var extra url.Values
			if resp.Header.Get("X-Otel-Source") == "central" {
				extra = url.Values{"additional_table_filters": {qaSetting}}
			}
			nc2, nb2 := native(s, extra)
			switch {
			case r2.StatusCode != 200:
				counts["qa:refused:"+r2.Header.Get("X-Otel-Refusal")]++
			case nc2 != 200:
				counts["qa:native-error"]++
			default:
				a, _ := parse(format, b2)
				n, _ := parse(format, nb2)
				if r2.Header.Get("X-Otel-Source") == "metadata" {
					n, a, _ = project(n, a)
					// a restricted caller reads total_rows as 0/1 (D33)
					n = totalRowsAsFlag(n)
				}
				if e, r := same(a, n); e || r || arrayVerdict(s.Query, a, n) == verdictArrayOrder {
					counts["qa:equal"]++
					f, _ := parse(format, body)
					if e2, r3 := same(a, f); !e2 && !r3 && r2.Header.Get("X-Otel-Source") != "metadata" {
						counts["qa:equal, narrower than fleet"]++
					}
				} else {
					counts["qa:differs"]++
				}
			}
		}
		counts[o.Outcome]++
		if o.Kind == "select" && resp.StatusCode == 200 && resp.Header.Get("X-Otel-Source") == "central" {
			counts["label:"+o.Completeness+map[bool]string{true: " (window derived)", false: " (no window)"}[o.Window]]++
		}
		if o.Outcome == "MISMATCH" || o.Outcome == "unparsed" || o.Outcome == verdictAtCap {
			t.Errorf("#%d %s/%s %s: %s", i, s.Scenario, s.Side, o.Outcome, o.Detail)
		}
		outs = append(outs, o)
	}
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		t.Logf("%-60s %d", k, counts[k])
	}
	med := func(d []time.Duration) time.Duration {
		sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
		if len(d) == 0 {
			return 0
		}
		return d[len(d)/2]
	}
	p90 := func(d []time.Duration) time.Duration {
		if len(d) == 0 {
			return 0
		}
		return d[len(d)*9/10]
	}
	t.Logf("latency over %d answered statements: adapter+service p50 %v p90 %v; ClickHouse direct p50 %v p90 %v",
		len(lat.via), med(lat.via), p90(lat.via), med(lat.nat), p90(lat.nat))
	// refusal reasons with an example each
	ex := map[string]string{}
	for _, o := range outs {
		if strings.HasPrefix(o.Outcome, "refused:") && ex[o.Outcome] == "" {
			ex[o.Outcome] = o.Detail
		}
	}
	for k, d := range ex {
		t.Logf("%s e.g. %s", k, d)
	}
	if dir := os.Getenv("HDXA_IT_OUT"); dir != "" {
		// one line per statement, after the counts
		var b []byte
		cb, _ := json.Marshal(map[string]any{"counts": counts, "complete_through_ms": ctMs})
		b = append(cb, '\n')
		for _, o := range outs {
			ob, _ := json.Marshal(o)
			b = append(append(b, ob...), '\n')
		}
		if err := os.WriteFile(filepath.Join(dir, "adapter-replay.jsonl"), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	metadataColumns(t, ad.URL, qa)
	sampleThroughAdapter(t, ad.URL, qa, fleet)
	if nm := os.Getenv("HDXA_IT_NODE_MODULES"); nm != "" {
		nodeClient(t, nm, ad.URL, qa)
	}
	if counts["qa:differs"] > 0 {
		t.Errorf("%d statements answered differently for a qa caller than ClickHouse with the scope filter", counts["qa:differs"])
	}
	if n := len(sink.Snapshot()); n == 0 {
		t.Fatal("nothing audited")
	}
}

// project cuts two JSON-format answers to the columns both have, in via's
// order: the native columns the service withheld, and the columns via
// adds (system.tables' alias `table`, which a subquery's * includes and
// the table's own * does not), are named in diff ("-x", "+x").
func project(native, via canon) (canon, canon, []string) {
	if len(native.cols) == 0 || len(via.cols) == 0 {
		return native, via, nil
	}
	idx := map[string]int{}
	for i, c := range native.cols {
		idx[c] = i
	}
	var keepN, keepV []int
	var diff []string
	inVia := map[string]bool{}
	for j, c := range via.cols {
		inVia[c] = true
		if i, ok := idx[c]; ok {
			keepN, keepV = append(keepN, i), append(keepV, j)
		} else {
			diff = append(diff, "+"+strings.Fields(c)[0])
		}
	}
	for _, c := range native.cols {
		if !inVia[c] {
			diff = append(diff, "-"+strings.Fields(c)[0])
		}
	}
	if len(diff) == 0 {
		return native, via, nil
	}
	cut := func(x canon, keep []int) canon {
		out := canon{}
		for _, i := range keep {
			out.cols = append(out.cols, x.cols[i])
		}
		for _, r := range x.rows {
			var vals []json.RawMessage
			if err := json.Unmarshal([]byte(r), &vals); err != nil {
				return x
			}
			row := make([]json.RawMessage, 0, len(keep))
			for _, i := range keep {
				row = append(row, vals[i])
			}
			b, _ := json.Marshal(row)
			out.rows = append(out.rows, string(b))
		}
		return out
	}
	return cut(native, keepN), cut(via, keepV), diff
}

// metadataColumns checks the metadata scope's allow-list against ClickHouse
// through the adapter: every served system table answers SELECT * (so every
// allow-listed column exists in this ClickHouse), with exactly the
// allow-list, and a withheld column is an error, not a value.
func metadataColumns(t *testing.T, adapterURL, token string) {
	ask := func(sql string) (int, string) {
		q := url.Values{"default_format": {"JSON"}}
		req, _ := http.NewRequest(http.MethodPost, adapterURL+"/?"+q.Encode(), strings.NewReader(sql))
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return resp.StatusCode, string(b)
	}
	for name, cols := range sqlscope.MetadataColumns {
		code, body := ask("SELECT * FROM system." + name + " LIMIT 1 FORMAT JSON")
		if code != 200 {
			t.Errorf("system.%s: %d %s", name, code, firstN(body, 300))
			continue
		}
		var r struct{ Meta []struct{ Name string } }
		if err := json.Unmarshal([]byte(body), &r); err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, m := range r.Meta {
			got = append(got, m.Name)
		}
		if strings.Join(got, ",") != strings.Join(cols, ",") {
			t.Errorf("system.%s: SELECT * answered %v, want %v", name, got, cols)
		}
	}
	code, body := ask("SELECT * FROM system.tables WHERE database = 'hdx_it_new' FORMAT JSON")
	for _, leak := range []string{"data_paths", "metadata_path", "/store/", "uuid"} {
		if code != 200 || strings.Contains(body, leak) {
			t.Errorf("SELECT * FROM system.tables: %d, %s in the answer", code, leak)
		}
	}
	for _, sql := range []string{"SELECT data_paths FROM system.tables", "SELECT t.metadata_path FROM system.tables AS t",
		"SELECT name FROM system.tables WHERE has(data_paths, '')", "SELECT data_path FROM system.databases"} {
		if code, body := ask(sql + " FORMAT JSON"); code == 200 || strings.Contains(body, "/store/") {
			t.Errorf("%s: %d %s", sql, code, firstN(body, 200))
		}
	}
}

// diffCols names the columns whose values differ in the first differing row.
func diffCols(a, b canon) []string {
	var out []string
	for i := range a.rows {
		if i >= len(b.rows) || a.rows[i] == b.rows[i] {
			continue
		}
		var x, y []json.RawMessage
		json.Unmarshal([]byte(a.rows[i]), &x)
		json.Unmarshal([]byte(b.rows[i]), &y)
		for j := range x {
			if j < len(y) && string(x[j]) != string(y[j]) {
				name := fmt.Sprint(j)
				if j < len(a.cols) {
					name = a.cols[j]
				}
				out = append(out, firstN(name+"="+string(x[j])+" vs "+string(y[j]), 160))
			}
		}
		break
	}
	return out
}

func head(rows []string) string {
	if len(rows) == 0 {
		return ""
	}
	return firstN(rows[0], 150)
}

// nodeClient runs client.cjs (@clickhouse/client, as HyperDX calls it)
// against the adapter with a qa token. HDXA_IT_NODE_MODULES is a
// node_modules directory holding @clickhouse/client 1.23.0-head.fae5998.1.
func nodeClient(t *testing.T, nodeModules, adapterURL, token string) {
	cmd := exec.Command("node", "client.cjs", adapterURL, token, "hdx_it_new")
	cmd.Env = append(os.Environ(), "NODE_PATH="+nodeModules)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("node client: %v\n%s%s", err, out, stderr.String())
	}
	t.Logf("node client: %s", out)
	var r struct {
		Ping bool
		JSON struct {
			Rows  int
			First map[string]any
			Meta  []string
		}
		Label       []string
		Compact     [][]any
		Multipart   []map[string]any
		Explain     map[string]any
		SystemParts map[string]any `json:"system_parts"`
		NoToken     map[string]any `json:"no_token"`
	}
	if err := json.Unmarshal(out, &r); err != nil {
		t.Fatalf("node client output: %v", err)
	}
	if !r.Ping || r.JSON.Rows != 3 || r.Label[0] != "central" || r.Label[1] == "" || r.Label[3] == "" ||
		!strings.Contains(r.Label[4], "result_overflow_mode") {
		t.Errorf("JSON query: %+v", r)
	}
	if last, _ := r.JSON.First["last"].(string); !strings.HasSuffix(last, "Z") || !strings.Contains(last, "T") {
		t.Errorf("date_time_output_format iso did not reach the answer: %v", r.JSON.First)
	}
	if len(r.Compact) != 5 || r.Compact[0][0] != "ServiceName" || r.Compact[1][1] != "UInt64" {
		t.Errorf("compact: %v", r.Compact)
	}
	if len(r.Multipart) != 1 {
		t.Errorf("multipart: %v", r.Multipart)
	}
	for name, e := range map[string]map[string]any{"explain": r.Explain, "system.parts": r.SystemParts} {
		if e["code"] != "497" || e["type"] != "ACCESS_DENIED" {
			t.Errorf("%s: %v", name, e)
		}
	}
	if r.NoToken["code"] != "516" {
		t.Errorf("no token: %v", r.NoToken)
	}
}
