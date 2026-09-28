package hdxadapter

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/query/internal/audit"
	"github.com/casselc/observability/otel-chdb/query/internal/auth"
	"github.com/casselc/observability/otel-chdb/query/internal/auth/authtest"
	"github.com/casselc/observability/otel-chdb/query/internal/catalog"
	"github.com/casselc/observability/otel-chdb/query/internal/central"
	"github.com/casselc/observability/otel-chdb/query/internal/completeness"
	"github.com/casselc/observability/otel-chdb/query/internal/hdxadapter"
	"github.com/casselc/observability/otel-chdb/query/internal/server"
	"github.com/casselc/observability/otel-chdb/query/internal/sqlscope"
	"github.com/casselc/observability/otel-chdb/query/internal/store"
	"github.com/golang-jwt/jwt/v5"
)

// The entity rewrite proxy's statements (entities/rwproxy, exact mode) in
// front of the adapter and the service: testdata/rwproxy-rewritten.jsonl
// holds the 82 statements rwproxy rewrote of HyperDX 2.39.1's 799 captured
// ones (13 EXPLAIN; 23 read the catalog's dictionaries, 46 rw_cat.resource_kv),
// as rwproxy sent them, with their parameters. They run against a small
// catalog (two clusters, qa and qb; two namespaces each) and variant-c
// tables (resource_id + ResourceResidual, ResourceAttributes an ALIAS over
// the dictionaries), in databases of their own.
const (
	rwDB   = "qhd_it_rwc"
	rwCat  = "qhd_it_rwcat"
	rwUser = "qhd_it_rw_ro"
)

type rwStmt struct {
	ID     int               `json:"id"`
	Query  string            `json:"query"`
	Params map[string]string `json:"params"`
}

func rwCorpus(t *testing.T) []rwStmt {
	f, err := os.Open(filepath.Join("testdata", "rwproxy-rewritten.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []rwStmt
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var s rwStmt
		if err := json.Unmarshal(sc.Bytes(), &s); err != nil {
			t.Fatal(err)
		}
		s.Query = strings.ReplaceAll(s.Query, "'rw_cat.", "'"+rwCat+".")
		s.Query = strings.ReplaceAll(s.Query, " rw_cat.", " "+rwCat+".")
		for k, v := range s.Params {
			if v == "rw_c" {
				s.Params[k] = rwDB
			}
		}
		out = append(out, s)
	}
	return out
}

// rwSetup builds the catalog (entities/sql: catalog.sql, dictionaries.sql,
// resources.sql), resource_kv with its cluster and namespace columns, the
// variant-c tables, their rows, and a read-only user.
func rwSetup(t *testing.T, c chc, root string) {
	for _, db := range []string{rwDB, rwCat} {
		c.exec("DROP DATABASE IF EXISTS " + db)
		c.exec("CREATE DATABASE " + db)
	}
	sqlDir := filepath.Join(root, "entities", "sql")
	for _, s := range ddl(t, filepath.Join(sqlDir, "catalog.sql"), map[string]string{"db": rwCat}) {
		c.exec(s)
	}
	for _, s := range ddl(t, filepath.Join(sqlDir, "dictionaries.sql"), map[string]string{"db": rwCat}) {
		c.exec(s)
	}
	from, to := "'2026-01-01 00:00:00'", "'2100-01-01 00:00:00'"
	cl := "(SELECT arrayJoin(['qa', 'qb']) AS c)"
	c.exec(fmt.Sprintf(`INSERT INTO %s.clusters SELECT xxh3('uid-' || c), map('k8s.cluster.name', c, 'k8s.cluster.uid', 'uid-' || c,
  'cloud.region', if(c = 'qa', 'us-east-1', 'us-west-2'), 'deployment.environment.name', if(c = 'qa', 'prod', 'staging')), %s, %s FROM %s`, rwCat, from, to, cl))
	c.exec(fmt.Sprintf(`INSERT INTO %s.nodes SELECT xxh3(c || '/node/' || toString(i)), xxh3('uid-' || c),
  map('k8s.node.name', 'node-' || toString(i), 'host.name', 'host-' || toString(i)), %s, %s FROM %s ARRAY JOIN [0, 1] AS i`, rwCat, from, to, cl))
	c.exec(fmt.Sprintf(`INSERT INTO %s.namespaces SELECT xxh3(c || '/ns/' || ns), xxh3('uid-' || c), map('k8s.namespace.name', ns), %s, %s
  FROM %s ARRAY JOIN ['shop', 'cart'] AS ns`, rwCat, from, to, cl))
	c.exec(fmt.Sprintf(`INSERT INTO %s.workloads SELECT xxh3(c || '/' || ns || '/' || w), xxh3('uid-' || c), xxh3(c || '/ns/' || ns), 'Deployment', w,
  map('k8s.deployment.name', w, 'service.name', w, 'service.version', '1.2.3', 'telemetry.sdk.language', if(w = 'cart', 'go', 'java')),
  [(w, 'img/' || w, '1.0')], map(), 1, %s, %s
  FROM %s ARRAY JOIN ['shop', 'cart'] AS ns ARRAY JOIN ['frontend', 'payment', 'cart'] AS w`, rwCat, from, to, cl))
	uuid := "lower(concat(substr(h, 1, 8), '-', substr(h, 9, 4), '-', substr(h, 13, 4), '-', substr(h, 17, 4), '-', substr(h, 21, 12)))"
	c.exec(fmt.Sprintf(`INSERT INTO %s.pods SELECT xxh3(k), %s AS uid, xxh3('uid-' || c), xxh3(c || '/' || ns || '/' || w), xxh3(c || '/node/' || toString(cityHash64(k) %% 2)),
  xxh3(c || '/ns/' || ns), map('k8s.pod.name', multiIf(w = 'frontend', 'frontend-f778-0', w = 'payment', 'payment-5c9d-1', 'cart-77aa-2'),
  'k8s.pod.uid', %s, 'k8s.pod.start_time', '2026-09-27T00:00:00Z'), %s, %s, '2026-09-27 00:00:00', %s
  FROM (SELECT c, ns, w, c || '/' || ns || '/' || w || '/pod' AS k, concat(leftPad(hex(xxh3(k)), 16, '0'), leftPad(hex(xxh3(k || 'x')), 16, '0')) AS h
        FROM %s ARRAY JOIN ['shop', 'cart'] AS ns ARRAY JOIN ['frontend', 'payment', 'cart'] AS w)`, rwCat, uuid, uuid, from, to, to, cl))
	for _, s := range ddl(t, filepath.Join(sqlDir, "resources.sql"), map[string]string{"db": rwCat, "where": "1"}) {
		c.exec(s)
	}
	// resource_kv as rwproxy's setup.py builds it, with the resource's
	// cluster and namespace as columns: the service scopes it by them (D33)
	c.exec(fmt.Sprintf("CREATE TABLE %s.resource_kv (Key LowCardinality(String), Value String, resource_id UInt64, cluster LowCardinality(String), "+
		"namespace LowCardinality(String)) ENGINE = ReplacingMergeTree ORDER BY (Key, Value, resource_id)", rwCat))
	c.exec(fmt.Sprintf("INSERT INTO %s.resource_kv SELECT kv.1, kv.2, resource_id, attrs['k8s.cluster.name'], attrs['k8s.namespace.name'] "+
		"FROM %s.resources ARRAY JOIN CAST(attrs, 'Array(Tuple(String, String))') AS kv", rwCat, rwCat))
	for _, d := range []string{"d_res", "d_pod", "d_wl", "d_node", "d_ns", "d_cluster"} {
		c.exec(fmt.Sprintf("SYSTEM RELOAD DICTIONARY %s.%s", rwCat, d))
	}
	for _, sig := range []string{"logs", "traces"} {
		st := ddl(t, filepath.Join(sqlDir, "generated", "c_"+sig+".sql"), map[string]string{"db": rwDB, "cat": rwCat})
		c.exec(st[0])
	}
	// rows: every catalog resource, and 1 in 50 rows of a resource the
	// catalog does not know yet (the edge's grace window: the covered set in
	// the residual)
	span := t1Ms - t0Ms
	ids := fmt.Sprintf("(SELECT groupArray(resource_id) FROM (SELECT resource_id FROM %s.res_index ORDER BY resource_id))", rwCat)
	rid := fmt.Sprintf("if(number %% 50 = 0, xxh3(toString(number)), arrayElement(%s, 1 + number %% 24))", ids)
	resid := "if(number % 50 = 0, map('k8s.cluster.name', ['qa', 'qb'][1 + number % 2], 'k8s.namespace.name', 'shop', 'k8s.pod.name', 'frontend-f778-0', 'service.name', 'frontend'), map('rum.sessionId', ''))"
	c.exec(fmt.Sprintf(`INSERT INTO %s.otel_logs (Timestamp, TraceId, SpanId, TraceFlags, SeverityText, SeverityNumber, ServiceName, Body, resource_id, ResourceResidual,
  ScopeName, LogAttributes, received_at)
SELECT fromUnixTimestamp64Milli(toInt64(%d + (cityHash64(number) %% %d))) AS ts, hex(cityHash64(number, 1)), hex(cityHash64(number, 2)), 1,
  ['INFO', 'ERROR', 'WARN'][1 + number %% 3], [9, 17, 13][1 + number %% 3], ['frontend', 'payment', 'cart'][1 + number %% 3],
  ['card declined for order ' || toString(number), 'payment failed: timeout', 'ERROR connecting to redis', 'ok'][1 + intDiv(number, 3) %% 4],
  %s, %s, 'scope', map('http.route', ['/api/cart', '/api/v1/orders'][1 + number %% 2], 'http.response.status_code', ['200', '500'][1 + number %% 2],
  'payment.method', ['card', 'paypal'][1 + number %% 2]), ts + toIntervalMillisecond(cityHash64(number, 7) %% 90000)
FROM numbers(%d)`, rwDB, t0Ms, span, rid, resid, nRows))
	c.exec(fmt.Sprintf(`INSERT INTO %s.otel_traces (Timestamp, TraceId, SpanId, ParentSpanId, SpanName, SpanKind, ServiceName, resource_id, ResourceResidual,
  SpanAttributes, Duration, StatusCode, received_at)
SELECT fromUnixTimestamp64Milli(toInt64(%d + (cityHash64(number, 9) %% %d))) AS ts, hex(cityHash64(number, 3)), hex(cityHash64(number, 4)), '',
  ['GET /api/cart', 'charge', 'redis GET'][1 + number %% 3], 'Server', ['frontend', 'payment', 'cart'][1 + number %% 3], %s, %s,
  map('http.route', ['/api/cart', '/api/v1/orders'][1 + number %% 2], 'db.system', ['redis', 'postgres'][1 + number %% 2]),
  1000 + cityHash64(number, 5) %% 5000000000, ['Ok', 'Error', 'Unset'][1 + number %% 3], ts + toIntervalMillisecond(cityHash64(number, 8) %% 90000)
FROM numbers(%d)`, rwDB, t0Ms, span, rid, resid, nRows))
	c.exec("DROP USER IF EXISTS " + rwUser)
	c.exec(fmt.Sprintf("CREATE USER %s IDENTIFIED WITH sha256_password BY '%s' SETTINGS readonly = 2", rwUser, roPass))
	for _, tb := range []string{rwDB + ".otel_logs", rwDB + ".otel_traces", rwCat + ".resource_kv", rwCat + ".resources"} {
		c.exec(fmt.Sprintf("GRANT SELECT ON %s TO %s", tb, rwUser))
	}
	// dictGet per dictionary, not on the database: the second fence behind
	// the service's allow-list
	for _, d := range []string{"d_res", "d_pod", "d_wl", "d_node", "d_ns", "d_cluster"} {
		c.exec(fmt.Sprintf("GRANT dictGet ON %s.%s TO %s", rwCat, d, rwUser))
	}
}

// shippedDictionaries is queryd.example.json's central.dictionaries, with
// the catalog database renamed to db.
func shippedDictionaries(t *testing.T, root, db string) []*sqlscope.Dictionary {
	b, err := os.ReadFile(filepath.Join(root, "query", "queryd.example.json"))
	if err != nil {
		t.Fatal(err)
	}
	b = []byte(strings.ReplaceAll(string(b), "'entities.", "'"+db+"."))
	b = []byte(strings.ReplaceAll(string(b), "\"entities.", "\""+db+"."))
	var c struct {
		Central struct {
			Dictionaries []*sqlscope.Dictionary `json:"dictionaries"`
		} `json:"central"`
	}
	if err := json.Unmarshal(b, &c); err != nil {
		t.Fatal(err)
	}
	return c.Central.Dictionaries
}

type rwStack struct {
	adapter, service string
	mint             func(jwt.MapClaims) string
}

// rwService wires the service as cmd/queryd does, with the catalog; after
// (D33) or before: without the dictionaries and resource_kv.
func rwService(t *testing.T, c chc, root string, after bool) *rwStack {
	is := authtest.New()
	t.Cleanup(is.Close)
	v, err := auth.NewVerifier(auth.OIDCConfig{Issuer: is.URL, Audience: "otel-query"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	tables := []*sqlscope.Table{
		{Database: rwDB, Name: "otel_logs", TimeColumn: "Timestamp", ReceivedColumn: "received_at", Scope: "catalog", Signals: []string{"logs"}},
		{Database: rwDB, Name: "otel_traces", TimeColumn: "Timestamp", ReceivedColumn: "received_at", Scope: "catalog", Signals: []string{"traces"}},
	}
	if after {
		tables = append(tables, &sqlscope.Table{Database: rwCat, Name: "resource_kv", Scope: "columns", Cluster: "cluster", Namespace: "namespace"})
	}
	policy, err := sqlscope.NewPolicy(rwDB, tables, 0)
	if err != nil {
		t.Fatal(err)
	}
	if after {
		if err := policy.SetDictionaries(shippedDictionaries(t, root, rwCat)); err != nil {
			t.Fatal(err)
		}
	}
	ch := central.New(central.Config{URL: c.url, User: rwUser, Password: roPass, Database: rwDB})
	cat, err := catalog.New(catalog.Config{Database: rwCat}, ch)
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
	srv := &server.Server{Verifier: v, Audit: &audit.Memory{}, Policy: policy, Watermark: wm, Central: ch, Catalog: cat,
		Mapping: &auth.Mapping{ClustersClaim: "clusters", NamespacesClaim: "namespaces", RolesClaim: "roles", GroupsClaim: "groups",
			Groups: map[string]auth.Grant{"sre": {Clusters: []string{"*"}, Namespaces: []string{"*"}, Roles: []string{"query"}}}},
		Limits: server.Limits{Default: central.Limits{MaxExecutionTimeS: 60, MaxRowsToRead: 1e9, MaxResultRows: 1e6, MaxConcurrent: 4}}}
	srv.Init()
	qs := httptest.NewServer(srv.Handler())
	t.Cleanup(qs.Close)
	ad := httptest.NewServer(hdxadapter.New(hdxadapter.Config{QueryURL: qs.URL + "/v1/query", DefaultDatabase: rwDB,
		TimeColumns: map[string]string{rwDB + ".otel_logs": "Timestamp", rwDB + ".otel_traces": "Timestamp"}}, nil))
	t.Cleanup(ad.Close)
	return &rwStack{adapter: ad.URL, service: qs.URL, mint: func(cl jwt.MapClaims) string {
		cl["aud"], cl["sub"] = "otel-query", "it-user"
		return is.Mint(cl)
	}}
}

// rwCaller is a token and what ClickHouse answers for it by hand: the
// service's filters applied as additional_table_filters.
type rwCaller struct {
	name   string
	claims jwt.MapClaims
	filter string // "" for the fleet
}

func rwCallers(t *testing.T, c chc) []rwCaller {
	ids := func(where string) string {
		code, b := c.do(fmt.Sprintf("SELECT arrayStringConcat(arraySort(groupArray(toString(resource_id))), ', ') FROM (SELECT DISTINCT resource_id FROM %s.resources WHERE %s) FORMAT TSV", rwCat, where), nil, "")
		if code != 200 {
			t.Fatal(string(b))
		}
		return strings.TrimSpace(string(b))
	}
	filter := func(idList, kv string) string {
		esc := func(s string) string { return strings.ReplaceAll(s, "'", `\'`) }
		return fmt.Sprintf("{'%s.otel_logs':'resource_id IN (%s)', '%s.otel_traces':'resource_id IN (%s)', '%s.resource_kv':'%s'}",
			rwDB, idList, rwDB, idList, rwCat, esc(kv))
	}
	return []rwCaller{
		{name: "fleet", claims: jwt.MapClaims{"groups": "sre"}},
		{name: "qa", claims: jwt.MapClaims{"clusters": "qa", "namespaces": "*", "roles": "query"},
			filter: filter(ids("attrs['k8s.cluster.name'] IN ('qa')"), "cluster IN ('qa')")},
		{name: "qa/shop", claims: jwt.MapClaims{"clusters": "qa", "namespaces": "shop", "roles": "query"},
			filter: filter(ids("attrs['k8s.cluster.name'] IN ('qa') AND attrs['k8s.namespace.name'] IN ('shop')"), "cluster IN ('qa') AND namespace IN ('shop')")},
	}
}

// TestRwproxyChainThroughAdapter: rwproxy's rewritten statements through
// the adapter and the service, before D33 (no dictionaries, no
// resource_kv: every non-EXPLAIN statement refused) and after (every one
// answered; a restricted caller's answer equal to ClickHouse's with the
// service's filters applied by hand). Then the proof that a dictionary
// lookup does not leak another cluster's entities.
func TestRwproxyChainThroughAdapter(t *testing.T) {
	if os.Getenv("HDXA_IT") == "" {
		t.Skip("HDXA_IT=1 runs the chain against a local ClickHouse")
	}
	root := filepath.Join("..", "..", "..")
	c := chc{t: t, url: env("HDXA_CH", "http://127.0.0.1:18123")}
	rwSetup(t, c, root)
	if os.Getenv("HDXA_IT_KEEP") == "" {
		defer func() {
			for _, db := range []string{rwDB, rwCat} {
				c.do("DROP DATABASE IF EXISTS "+db, nil, "")
			}
			c.do("DROP USER IF EXISTS "+rwUser, nil, "")
		}()
	}
	corpus := rwCorpus(t)
	callers := rwCallers(t, c)
	native := func(s rwStmt, filter string) (int, []byte) {
		q := hdxSettings()
		for k, x := range s.Params {
			q.Set("param_"+k, x)
		}
		if filter != "" {
			q.Set("additional_table_filters", filter)
		}
		return c.do(s.Query, q, "")
	}
	via := func(st *rwStack, s rwStmt, tok string) (*http.Response, []byte) {
		q := hdxSettings()
		for k, x := range s.Params {
			q.Set("param_"+k, x)
		}
		req, _ := http.NewRequest(http.MethodPost, st.adapter+"/?"+q.Encode(), strings.NewReader(s.Query))
		req.Header.Set("Authorization", "Bearer "+tok)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return resp, b
	}
	summary := map[string]map[string]int{}
	for _, phase := range []string{"before", "after"} {
		st := rwService(t, c, root, phase == "after")
		fleetAnswers := map[int]canon{}
		for _, cl := range callers {
			tok := st.mint(jwt.MapClaims(copyClaims(cl.claims)))
			counts := map[string]int{}
			for _, s := range corpus {
				format := "JSON"
				if m := formatRE.FindStringSubmatch(s.Query); m != nil {
					format = m[1]
				}
				resp, body := via(st, s, tok)
				if resp.StatusCode != 200 {
					counts["refused:"+resp.Header.Get("X-Otel-Refusal")]++
					if phase == "after" && resp.Header.Get("X-Otel-Refusal") != "explain" {
						t.Errorf("%s/%s #%d refused: %s", phase, cl.name, s.ID, firstN(string(body), 400))
					}
					continue
				}
				ncode, nbody := native(s, cl.filter)
				if ncode != 200 {
					counts["native-error"]++
					t.Errorf("#%d native: %s", s.ID, firstN(string(nbody), 300))
					continue
				}
				a, err1 := parse(format, body)
				n, err2 := parse(format, nbody)
				exact, rows := same(a, n)
				switch {
				case err1 != nil || err2 != nil:
					counts["unparsed"]++
				case exact:
					counts["equal"]++
					if len(a.rows) > 0 {
						counts["equal, non-empty"]++
					}
					if cl.filter == "" {
						fleetAnswers[s.ID] = a
					} else if f, ok := fleetAnswers[s.ID]; ok {
						if e, r := same(a, f); !e && !r {
							counts["equal, narrower than the fleet's"]++
						}
					}
				case rows:
					counts["equal-rows"]++
				default:
					counts["MISMATCH"]++
					t.Errorf("%s/%s #%d: rows %d/%d, %v", phase, cl.name, s.ID, len(a.rows), len(n.rows), diffCols(a, n))
				}
			}
			summary[phase+" "+cl.name] = counts
		}
		if phase == "after" {
			dictionaryLeakProof(t, c, st, root)
		}
	}
	keys := make([]string, 0, len(summary))
	for k := range summary {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		t.Logf("%-16s %v", k, summary[k])
	}
	if dir := os.Getenv("HDXA_IT_OUT"); dir != "" {
		b, _ := json.MarshalIndent(map[string]any{"statements": len(corpus), "counts": summary}, "", " ")
		if err := os.WriteFile(filepath.Join(dir, "rwproxy-chain.json"), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func copyClaims(m jwt.MapClaims) map[string]any {
	out := map[string]any{}
	for k, v := range m {
		out[k] = v
	}
	return out
}

// dictionaryLeakProof: a caller of cluster qa probes qb's entities by key.
// resource_id is the content hash of a resource's attributes, so anyone who
// knows (or guesses) them can compute it; the guarded lookup answers every
// qb key as an absent key, and the fleet (and qa, for its own keys) reads
// the entry. A literal key on a derived dictionary is refused.
func dictionaryLeakProof(t *testing.T, c chc, st *rwStack, root string) {
	one := func(sql string) string {
		code, b := c.do(sql+" FORMAT TSV", nil, "")
		if code != 200 {
			t.Fatalf("%s: %s", sql, b)
		}
		return strings.TrimSpace(string(b))
	}
	qb := one(fmt.Sprintf("SELECT min(resource_id) FROM %s.resources WHERE attrs['k8s.cluster.name'] = 'qb'", rwCat))
	qa := one(fmt.Sprintf("SELECT min(resource_id) FROM %s.resources WHERE attrs['k8s.cluster.name'] = 'qa' AND attrs['k8s.namespace.name'] = 'cart'", rwCat))
	all := one(fmt.Sprintf("SELECT arrayStringConcat(groupArray(toString(resource_id)), ', ') FROM %s.resources", rwCat))
	nQA := one(fmt.Sprintf("SELECT count() FROM %s.resources WHERE attrs['k8s.cluster.name'] = 'qa'", rwCat))
	nAll := one(fmt.Sprintf("SELECT count() FROM %s.resources", rwCat))
	d := func(n string) string { return "'" + rwCat + "." + n + "'" }
	podKey := func(id string) string { return fmt.Sprintf("dictGet(%s, 'pod_key', toUInt64(%s))", d("d_res"), id) }
	ask := func(tok, sql string) (int, string) {
		b, _ := json.Marshal(map[string]any{"sql": sql})
		req, _ := http.NewRequest(http.MethodPost, st.service+"/v1/query", strings.NewReader(string(b)))
		req.Header.Set("Authorization", "Bearer "+tok)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out struct {
			Error  string
			Result struct{ Data []map[string]any }
		}
		_ = json.NewDecoder(resp.Body).Decode(&out)
		if resp.StatusCode != 200 {
			return resp.StatusCode, out.Error
		}
		v, _ := json.Marshal(out.Result.Data[0]["v"])
		return 200, strings.Trim(string(v), `"`)
	}
	fleet := st.mint(jwt.MapClaims{"groups": "sre"})
	tqa := st.mint(jwt.MapClaims{"clusters": "qa", "namespaces": "*", "roles": "query"})
	tqaShop := st.mint(jwt.MapClaims{"clusters": "qa", "namespaces": "shop", "roles": "query"})
	probes := []struct {
		name, sql         string
		fleet, qa, qaShop string // "" means: the fleet's value
		wantFleetNot      string
	}{
		{"dictHas qb", fmt.Sprintf("SELECT dictHas(%s, toUInt64(%s)) AS v", d("d_res"), qb), "1", "0", "0", ""},
		{"pod_key qb", "SELECT " + podKey(qb) + " AS v", "", "0", "0", "0"},
		{"pod name qb", fmt.Sprintf("SELECT dictGet(%s, 'name', %s) AS v", d("d_pod"), podKey(qb)), "", "", "", ""},
		{"namespace qb", fmt.Sprintf("SELECT dictGet(%s, 'attrs', dictGet(%s, 'ns_key', %s)) AS v", d("d_ns"), d("d_pod"), podKey(qb)), "", "{}", "{}", "{}"},
		{"cluster qb", fmt.Sprintf("SELECT dictGet(%s, 'attrs', dictGet(%s, 'cluster_key', %s)) AS v", d("d_cluster"), d("d_pod"), podKey(qb)), "", "{}", "{}", "{}"},
		{"uid qb", fmt.Sprintf("SELECT toString(dictGet(%s, 'uid', %s)) AS v", d("d_pod"), podKey(qb)), "", "00000000-0000-0000-0000-000000000000", "00000000-0000-0000-0000-000000000000", "00000000-0000-0000-0000-000000000000"},
		{"has qa/cart", fmt.Sprintf("SELECT dictHas(%s, toUInt64(%s)) AS v", d("d_res"), qa), "1", "1", "0", ""},
		{"namespace qa/cart", fmt.Sprintf("SELECT JSONExtractString(dictGet(%s, 'attrs', dictGet(%s, 'ns_key', %s)), 'k8s.namespace.name') AS v", d("d_ns"), d("d_pod"), podKey(qa)), "cart", "cart", "", ""},
		{"probe every id", fmt.Sprintf("SELECT sum(dictHas(%s, x)) AS v FROM (SELECT arrayJoin([%s]) AS x)", d("d_res"), all), nAll, nQA, "", ""},
	}
	for _, p := range probes {
		code, fv := ask(fleet, p.sql)
		if code != 200 {
			t.Errorf("%s: fleet %d %s", p.name, code, fv)
			continue
		}
		if p.fleet != "" && fv != p.fleet || p.wantFleetNot != "" && fv == p.wantFleetNot || fv == "" && p.name == "pod name qb" {
			t.Errorf("%s: fleet read %q", p.name, fv)
		}
		for _, x := range []struct {
			who, tok, want string
		}{{"qa", tqa, p.qa}, {"qa/shop", tqaShop, p.qaShop}} {
			code, v := ask(x.tok, p.sql)
			want := x.want
			if code != 200 {
				t.Errorf("%s: %s %d %s", p.name, x.who, code, v)
				continue
			}
			if p.name == "probe every id" && x.who == "qa/shop" {
				want = one(fmt.Sprintf("SELECT count() FROM %s.resources WHERE attrs['k8s.cluster.name'] = 'qa' AND attrs['k8s.namespace.name'] = 'shop'", rwCat))
			}
			if v != want {
				t.Errorf("%s: %s read %q, want %q (fleet reads %q)", p.name, x.who, v, want, fv)
			} else {
				t.Logf("%-18s fleet %-40q %-8s %q", p.name, firstN(fv, 40), x.who, v)
			}
		}
	}
	// a literal key on a derived dictionary: refused for everyone
	pk := one(fmt.Sprintf("SELECT dictGet('%s.d_res', 'pod_key', toUInt64(%s))", rwCat, qb))
	for _, tok := range []string{fleet, tqa} {
		if code, reason := ask(tok, fmt.Sprintf("SELECT dictGet(%s, 'name', toUInt64(%s)) AS v", d("d_pod"), pk)); code != 403 || reason != "dict_key" {
			t.Errorf("derived literal probe: %d %s", code, reason)
		}
	}
	// the configured defaults are what the dictionaries answer for an
	// absent key, with the same type (checked as the admin)
	for _, dict := range shippedDictionaries(t, root, rwCat) {
		for a, at := range dict.Attributes {
			got := one(fmt.Sprintf("SELECT toTypeName(dictGet('%s', '%s', toUInt64(0))) = '%s' AND dictGet('%s', '%s', toUInt64(0)) = CAST(%s, '%s')",
				dict.Name, a, at.Type, dict.Name, a, at.Default, at.Type))
			if got != "1" {
				t.Errorf("%s.%s: the configured default %s %s is not the dictionary's", dict.Name, a, at.Type, at.Default)
			}
		}
	}
	_ = url.Values{}
}
