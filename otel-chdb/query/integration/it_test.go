// Package integration runs the query service against the real pipeline:
// the Go edge (otelcol-s3pq) publishes v2 objects for two clusters to
// SeaweedFS, the Rust consumer ingests them into ClickHouse and publishes
// {ctl}/watermark.json, and the service answers queries and plans for tokens
// of one cluster, of one namespace, and of the fleet.
//
//	QS_IT_BIN=<dir with otelcol-s3pq and consume> go test ./integration -v
//
// Services: ClickHouse at QS_IT_CH (default http://127.0.0.1:18123), S3 at
// QS_IT_S3 (default http://127.0.0.1:18333, keys otel/otelsecret, bucket
// QS_IT_BUCKET, default otel). Everything the run creates is named qs-… (S3
// prefix) or qs_… (databases, the read-only user) and is removed at the end
// (QS_IT_KEEP=1 keeps it).
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/casselc/observability/otel-chdb/query/internal/app"
	"github.com/casselc/observability/otel-chdb/query/internal/audit"
	"github.com/casselc/observability/otel-chdb/query/internal/auth"
	"github.com/casselc/observability/otel-chdb/query/internal/auth/authtest"
	"github.com/casselc/observability/otel-chdb/query/internal/central"
	"github.com/casselc/observability/otel-chdb/query/internal/sqlscope"
	"github.com/golang-jwt/jwt/v5"
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

type rig struct {
	t                 *testing.T
	ch, s3url, bucket string
	run, db, cat, ro  string
	roPass            string
	s3                *s3.Client
}

func (r *rig) sql(q string) string {
	r.t.Helper()
	out, err := r.sqlErr(q, "")
	if err != nil {
		r.t.Fatalf("%s: %v", q, err)
	}
	return out
}

func (r *rig) sqlErr(q, user string) (string, error) {
	req, _ := http.NewRequest(http.MethodPost, r.ch, strings.NewReader(q))
	if user != "" {
		req.Header.Set("X-ClickHouse-User", user)
		req.Header.Set("X-ClickHouse-Key", r.roPass)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("%s", strings.TrimSpace(string(b)))
	}
	return strings.TrimSpace(string(b)), nil
}

func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

type resource struct {
	cluster, namespace, pod string
	logs, spans             int
}

func attrs(r resource) []map[string]any {
	kv := func(k, v string) map[string]any {
		return map[string]any{"key": k, "value": map[string]any{"stringValue": v}}
	}
	return []map[string]any{kv("service.name", "svc-"+r.namespace), kv("k8s.cluster.name", r.cluster),
		kv("k8s.namespace.name", r.namespace), kv("k8s.pod.name", r.pod), kv("k8s.pod.uid", r.cluster+"-"+r.pod+"-uid")}
}

func otlp(t *testing.T, port int, path string, body any) {
	t.Helper()
	b, _ := json.Marshal(body)
	for i := 0; i < 20; i++ {
		resp, err := http.Post(fmt.Sprintf("http://127.0.0.1:%d%s", port, path), "application/json", bytes.NewReader(b))
		if err == nil {
			rb, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
			t.Logf("POST %s: %d %s", path, resp.StatusCode, rb)
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("POST %s never answered 200", path)
}

// publish runs one Go edge for cluster and sends its resources' logs and
// spans at event times around at.
func (r *rig) publish(bin, cluster string, res []resource, at time.Time) {
	t := r.t
	port, health := freePort(t), freePort(t)
	cmd := exec.Command(filepath.Join(bin, "otelcol-s3pq"), "--config", "go-edge.yaml")
	cmd.Dir = "."
	cmd.Env = append(os.Environ(),
		"CLUSTER="+cluster, "PRODUCER=pub-0", "HEARTBEAT=2s", "LOG_LEVEL=warn",
		fmt.Sprintf("OTLP_HTTP=127.0.0.1:%d", port), fmt.Sprintf("HEALTH=127.0.0.1:%d", health),
		"S3_URL="+r.s3url+"/"+r.bucket+"/"+r.run, "AWS_ACCESS_KEY_ID=otel", "AWS_SECRET_ACCESS_KEY=otelsecret", "AWS_REGION=us-east-1")
	logf, _ := os.Create(filepath.Join(t.TempDir(), "edge-"+cluster+".log"))
	cmd.Stdout, cmd.Stderr = logf, logf
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cmd.Process.Signal(syscall.SIGINT)
		_ = cmd.Wait()
		if t.Failed() {
			b, _ := os.ReadFile(logf.Name())
			t.Logf("edge %s log:\n%s", cluster, b)
		}
	}()
	var rl, rs []map[string]any
	n := 0
	for _, x := range res {
		var recs, spans []map[string]any
		for i := 0; i < x.logs; i++ {
			n++
			recs = append(recs, map[string]any{"timeUnixNano": fmt.Sprint(at.Add(time.Duration(n) * time.Second).UnixNano()),
				"severityText": "INFO", "body": map[string]any{"stringValue": fmt.Sprintf("%s %s log %d", x.cluster, x.namespace, i)}})
		}
		for i := 0; i < x.spans; i++ {
			n++
			s := at.Add(time.Duration(n) * time.Second)
			spans = append(spans, map[string]any{"traceId": fmt.Sprintf("%032x", rand.Uint64()), "spanId": fmt.Sprintf("%016x", rand.Uint64()),
				"name": "op", "kind": 2, "startTimeUnixNano": fmt.Sprint(s.UnixNano()), "endTimeUnixNano": fmt.Sprint(s.Add(time.Millisecond).UnixNano())})
		}
		rl = append(rl, map[string]any{"resource": map[string]any{"attributes": attrs(x)}, "scopeLogs": []any{map[string]any{"scope": map[string]any{"name": "it"}, "logRecords": recs}}})
		rs = append(rs, map[string]any{"resource": map[string]any{"attributes": attrs(x)}, "scopeSpans": []any{map[string]any{"scope": map[string]any{"name": "it"}, "spans": spans}}})
	}
	otlp(t, port, "/v1/logs", map[string]any{"resourceLogs": rl})
	otlp(t, port, "/v1/traces", map[string]any{"resourceSpans": rs})
	time.Sleep(3 * time.Second) // a heartbeat per lane after the data
}

func (r *rig) consume(bin string, args ...string) {
	t := r.t
	all := append(args, "--s3", r.s3url+"/"+r.bucket+"/"+r.run, "--key", "otel", "--secret", "otelsecret")
	cmd := exec.Command(filepath.Join(bin, "consume"), all...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("consume %v: %v\n%s", args, err, out)
	}
	t.Logf("consume %s: %s", args[0], lastLines(string(out), 3))
}

func lastLines(s string, n int) string {
	l := strings.Split(strings.TrimSpace(s), "\n")
	if len(l) > n {
		l = l[len(l)-n:]
	}
	return strings.Join(l, " | ")
}

func (r *rig) cleanup() {
	if os.Getenv("QS_IT_KEEP") != "" {
		r.t.Logf("kept: s3 %s/%s, databases %s %s, user %s", r.bucket, r.run, r.db, r.cat, r.ro)
		return
	}
	_, _ = r.sqlErr("DROP USER IF EXISTS "+r.ro, "")
	_, _ = r.sqlErr("DROP DATABASE IF EXISTS "+r.cat+" SYNC", "")
	_, _ = r.sqlErr("DROP DATABASE IF EXISTS "+r.db+" SYNC", "")
	ctx := context.Background()
	p := s3.NewListObjectsV2Paginator(r.s3, &s3.ListObjectsV2Input{Bucket: &r.bucket, Prefix: aws.String(r.run + "/")})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			r.t.Logf("cleanup list: %v", err)
			return
		}
		var ids []s3types.ObjectIdentifier
		for _, o := range page.Contents {
			ids = append(ids, s3types.ObjectIdentifier{Key: o.Key})
		}
		if len(ids) > 0 {
			if _, err := r.s3.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: &r.bucket, Delete: &s3types.Delete{Objects: ids}}); err != nil {
				r.t.Logf("cleanup delete: %v", err)
			}
		}
	}
}

type client struct {
	t   *testing.T
	url string
}

func (c client) post(path, tok string, body any) (int, map[string]any) {
	c.t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, c.url+path, bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

// count runs a one-value query and returns the value (ClickHouse JSON quotes
// 64-bit integers).
func (c client) count(tok, sql string) (int, string, map[string]any) {
	c.t.Helper()
	code, out := c.post("/v1/query", tok, map[string]any{"sql": sql})
	if code != 200 {
		return code, "", out
	}
	res := out["result"].(map[string]any)
	data := res["data"].([]any)
	if len(data) != 1 {
		return code, fmt.Sprint(data), out
	}
	for _, v := range data[0].(map[string]any) {
		return code, fmt.Sprint(v), out
	}
	return code, "", out
}

func TestIntegration(t *testing.T) {
	bin := os.Getenv("QS_IT_BIN")
	if bin == "" {
		t.Skip("QS_IT_BIN (a directory with otelcol-s3pq and consume) is not set")
	}
	id := fmt.Sprintf("%d%04x", time.Now().Unix(), rand.Uint32()&0xffff)
	r := &rig{t: t, ch: env("QS_IT_CH", "http://127.0.0.1:18123"), s3url: env("QS_IT_S3", "http://127.0.0.1:18333"),
		bucket: env("QS_IT_BUCKET", "otel"), run: "qs-" + id, db: "qs_" + id, cat: "qs_" + id + "_cat", ro: "qs_ro_" + id,
		roPass: fmt.Sprintf("%016x", rand.Uint64())}
	if _, err := r.sqlErr("SELECT 1", ""); err != nil {
		t.Skipf("no ClickHouse at %s: %v", r.ch, err)
	}
	r.s3 = s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String(r.s3url), UsePathStyle: true,
		Credentials: credentials.NewStaticCredentialsProvider("otel", "otelsecret", "")})
	t.Cleanup(r.cleanup)
	edgeCfg, err := os.ReadFile("../../conformance/go-edge.yaml")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "go-edge.yaml"), edgeCfg, 0o644); err != nil {
		t.Fatal(err)
	}
	wd, _ := os.Getwd()
	_ = os.Chdir(dir)
	defer os.Chdir(wd)

	// 1. two clusters' edges publish
	at := time.Now().Add(-10 * time.Minute).Truncate(time.Second)
	qa := []resource{{"qa", "shop", "cart-1", 3, 2}, {"qa", "pay", "pay-1", 2, 1}}
	qb := []resource{{"qb", "shop", "cart-9", 4, 2}, {"qb", "pay", "pay-9", 3, 3}}
	r.publish(bin, "qa", qa, at)
	r.publish(bin, "qb", qb, at)

	// 2. the real consumer ingests both lanes and publishes complete_through
	r.consume(bin, "run", "--ch", r.ch, "--db", r.db, "--exit-after-idle", "8s", "--poll", "300ms", "--full-list", "1s")
	r.consume(bin, "watermark", "--wm-skew", "1s")
	if got := r.sql("SELECT count() FROM " + r.db + ".otel_logs"); got != "12" {
		t.Fatalf("central holds %s logs, want 12", got)
	}

	// 3. a read-only ClickHouse user that can read only the served tables
	r.sql(fmt.Sprintf("CREATE USER %s IDENTIFIED WITH plaintext_password BY '%s' SETTINGS readonly = 2", r.ro, r.roPass))
	for _, tb := range []string{"otel_logs", "otel_traces", "otel_resources", "otel_resources_announced"} {
		r.sql(fmt.Sprintf("GRANT SELECT ON %s.%s TO %s", r.db, tb, r.ro))
	}
	// a stand-in entity catalog from the edges' announcements (FORMAT.md §2.1)
	r.sql("CREATE DATABASE " + r.cat)
	r.sql(fmt.Sprintf("CREATE VIEW %s.resources AS SELECT resource_id, CAST(ResourceAttributes, 'Map(String, String)') AS attrs FROM %s.otel_resources_announced", r.cat, r.db))
	r.sql(fmt.Sprintf("CREATE TABLE %s.ingest_log (object String, cluster LowCardinality(String), put_at DateTime64(3, 'UTC'), ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)) ENGINE = MergeTree ORDER BY cluster", r.cat))
	r.sql(fmt.Sprintf("INSERT INTO %s.ingest_log (object, cluster, put_at) VALUES ('o1', 'qa', now64(3)), ('o2', 'qb', now64(3) - INTERVAL 2 HOUR)", r.cat))
	r.sql(fmt.Sprintf("GRANT SELECT ON %s.resources TO %s", r.cat, r.ro))
	r.sql(fmt.Sprintf("GRANT SELECT ON %s.ingest_log TO %s", r.cat, r.ro))

	// 4. the service, as cmd/queryd builds it
	is := authtest.New()
	defer is.Close()
	auditPath := filepath.Join(dir, "audit.jsonl")
	sink, err := audit.OpenFile(auditPath, true)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	cfg := &app.Config{LakeEnabled: true}
	cfg.OIDC = auth.OIDCConfig{Issuer: is.URL, Audience: "otel-query"}
	cfg.Claims = auth.Mapping{ClustersClaim: "clusters", NamespacesClaim: "namespaces", RolesClaim: "roles", GroupsClaim: "groups",
		Groups: map[string]auth.Grant{"sre": {Clusters: []string{"*"}, Namespaces: []string{"*"}, Roles: []string{"query", "plan"}},
			"small": {}}}
	cfg.Central.Config = central.Config{URL: r.ch, User: r.ro, Password: r.roPass, Database: r.db}
	cfg.Central.Tables = []*sqlscope.Table{
		{Name: "otel_logs", TimeColumn: "Timestamp", Scope: "columns", Cluster: "`__hdx_materialized_k8s.cluster.name`", Namespace: "`__hdx_materialized_k8s.namespace.name`"},
		{Name: "otel_traces", TimeColumn: "Timestamp", Scope: "catalog"},
	}
	cfg.Limits.Default = central.DefaultLimits
	cfg.Catalog.Database = r.cat
	cfg.S3.Endpoint, cfg.S3.Bucket, cfg.S3.AccessKey, cfg.S3.SecretKey = r.s3url, r.bucket, "otel", "otelsecret"
	cfg.Lake.Root = r.run
	cfg.Lake.URLTTLS = 120
	cfg.Watermark.CacheS, cfg.Watermark.MaxAgeS = 1, 600
	v, err := auth.NewVerifier(cfg.OIDC, nil)
	if err != nil {
		t.Fatal(err)
	}
	srv, err := app.Build(context.Background(), cfg, v, sink)
	if err != nil {
		t.Fatal(err)
	}
	hs := httptest.NewServer(srv.Handler())
	defer hs.Close()
	c := client{t: t, url: hs.URL}
	tok := func(claims jwt.MapClaims) string {
		cl := jwt.MapClaims{"aud": "otel-query", "sub": "it-user"}
		for k, v := range claims {
			cl[k] = v
		}
		return is.Mint(cl)
	}
	qaTok := tok(jwt.MapClaims{"clusters": "qa", "namespaces": "*", "roles": "query plan"})
	qaShop := tok(jwt.MapClaims{"clusters": "qa", "namespaces": "shop", "roles": "query plan"})
	fleet := tok(jwt.MapClaims{"groups": "sre"})

	t.Run("scoped counts", func(t *testing.T) {
		c.t = t
		for _, x := range []struct {
			name, tok, sql, want string
		}{
			{"fleet", fleet, "SELECT count() FROM otel_logs", "12"},
			{"cluster qa", qaTok, "SELECT count() FROM otel_logs", "5"},
			{"qa asking for qb", qaTok, "SELECT count() FROM otel_logs WHERE `__hdx_materialized_k8s.cluster.name` = 'qb'", "0"},
			{"qa, alias shadowing the scope column", qaTok, "SELECT count() FROM (SELECT 'qb' AS `__hdx_materialized_k8s.cluster.name`, Body FROM otel_logs)", "5"},
			{"qa through a CTE and a join", qaTok, "WITH l AS (SELECT Body FROM otel_logs) SELECT count() FROM l AS a JOIN (SELECT Body FROM otel_logs) AS b ON a.Body = b.Body", "5"},
			{"qa, namespace shop", qaShop, "SELECT count() FROM otel_logs", "3"},
			{"traces by catalog, qa", qaTok, "SELECT count() FROM otel_traces", "3"},
			{"traces by catalog, qa/shop", qaShop, "SELECT count() FROM otel_traces", "2"},
			{"traces fleet", fleet, "SELECT count() FROM otel_traces", "8"},
		} {
			code, got, out := c.count(x.tok, x.sql)
			if code != 200 || got != x.want {
				t.Errorf("%s: %d %s (want %s) %v", x.name, code, got, x.want, out["error"])
			}
		}
	})

	t.Run("refusals", func(t *testing.T) {
		c.t = t
		for sql, want := range map[string]string{
			"SELECT count() FROM system.users":                  "table_not_allowed",
			"SELECT count() FROM " + r.db + ".otel_resources":   "table_not_allowed",
			"SELECT * FROM s3('" + r.s3url + "/otel/x')":        "table_function",
			"SELECT count() FROM otel_logs SETTINGS readonly=0": "settings_clause",
		} {
			code, out := c.post("/v1/query", qaTok, map[string]any{"sql": sql})
			if code != 403 || out["error"] != want {
				t.Errorf("%q: %d %v", sql, code, out)
			}
		}
		// the read-only user cannot write even if the allow-list failed
		if _, err := r.sqlErr("INSERT INTO "+r.db+".otel_logs (Body) VALUES ('x')", r.ro); err == nil {
			t.Error("the service's ClickHouse user can write")
		}
		if _, err := r.sqlErr("SELECT count() FROM system.users", r.ro); err == nil {
			t.Log("note: this server lets the read-only user read system.users (select_from_system_db_requires_grant off)")
		} else {
			t.Log("the read-only user cannot read system.users: grants are a second fence")
		}
	})

	t.Run("limits throw", func(t *testing.T) {
		c.t = t
		srv.Limits.Groups = map[string]central.Limits{}
		old := srv.Limits.Default
		srv.Limits.Default.MaxResultRows = 2
		defer func() { srv.Limits.Default = old }()
		code, out := c.post("/v1/query", fleet, map[string]any{"sql": "SELECT Body FROM otel_logs"})
		if code != 422 || !strings.HasPrefix(fmt.Sprint(out["error"]), "limit_exceeded") {
			t.Fatalf("a result over max_result_rows must fail, not shorten: %d %v", code, out)
		}
	})

	t.Run("labels", func(t *testing.T) {
		c.t = t
		code, out := c.post("/v1/query", qaTok, map[string]any{"sql": "SELECT count() FROM otel_logs"})
		if code != 200 || out["source"] != "central" {
			t.Fatalf("%d %v", code, out)
		}
		wm := out["watermark"].(map[string]any)
		t.Logf("watermark: status %v complete_through %v age %v lag %v", wm["status"], out["complete_through"], wm["age_s"], wm["lag_s"])
		if wm["status"] != "ok" || out["complete_through"] == nil {
			t.Fatalf("the consumer's watermark.json was not read: %v", wm)
		}
		if out["partial"] != true || out["incomplete_from"] == nil {
			t.Fatalf("an unbounded query must be partial: %v", out)
		}
		cat := out["catalog"].(map[string]any)
		if cat["status"] != "ok" || cat["clusters"].(map[string]any)["qb"] != nil {
			t.Fatalf("catalog lag for qa only: %v", cat)
		}
		ct, _ := time.Parse(time.RFC3339Nano, out["complete_through"].(string))
		if !ct.After(at) {
			t.Logf("complete_through %v is not after the data (%v): no closed-window check", ct, at)
			return
		}
		window := map[string]any{"from": at.Add(-time.Minute).Format(time.RFC3339), "to": ct.Format(time.RFC3339Nano)}
		code, out = c.post("/v1/query", qaTok, map[string]any{"sql": "SELECT count() FROM otel_logs", "window": window})
		if code != 200 || out["completeness"] != "complete" || out["partial"] != false {
			t.Fatalf("a window closed before complete_through is complete: %d %v", code, out)
		}
	})

	t.Run("plan", func(t *testing.T) {
		c.t = t
		req := map[string]any{"signal": "logs", "from": at.Add(-time.Minute).Format(time.RFC3339), "to": time.Now().Format(time.RFC3339)}
		code, out := c.post("/v1/plan", qaTok, req)
		if code != 200 {
			t.Fatalf("%d %v", code, out)
		}
		objs := out["objects"].([]any)
		if len(objs) == 0 {
			t.Fatal("no objects planned")
		}
		for _, o := range objs {
			m := o.(map[string]any)
			if !strings.HasPrefix(m["key"].(string), r.run+"/qa/") || m["refined"] != true {
				t.Fatalf("planned %v", m)
			}
			resp, err := http.Get(m["url"].(string))
			if err != nil {
				t.Fatal(err)
			}
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 200 || int64(len(b)) != int64(m["size"].(float64)) || !bytes.HasPrefix(b, []byte("PAR1")) {
				t.Fatalf("GET %s: %d, %d bytes", m["key"], resp.StatusCode, len(b))
			}
			// SigV4 binds the method: the GET URL answers HEAD with 403
			// (research/lake-ui.md finding 2; the plan carries the size)
			hr, _ := http.NewRequest(http.MethodHead, m["url"].(string), nil)
			if resp, err := http.DefaultClient.Do(hr); err == nil {
				resp.Body.Close()
				t.Logf("HEAD with the GET URL: %d", resp.StatusCode)
			}
		}
		if out["source"] != "lake" || out["complete_through"] == nil {
			t.Fatalf("%v", out)
		}
		// the same token asking for the other cluster is refused, not trimmed
		req["clusters"] = []string{"qb"}
		code, out = c.post("/v1/plan", qaTok, req)
		if code != 403 || out["error"] != "cluster_not_in_scope" {
			t.Fatalf("cross-cluster plan: %d %v", code, out)
		}
		delete(req, "clusters")
		code, out = c.post("/v1/plan", qaShop, req)
		if code != 403 || out["error"] != "namespace_scope_needs_filtering_reader" {
			t.Fatalf("namespace-scoped plan: %d %v", code, out)
		}
		code, out = c.post("/v1/plan", fleet, req)
		if code != 200 || len(out["clusters"].([]any)) != 2 {
			t.Fatalf("fleet plan: %d %v", code, out["clusters"])
		}
		t.Logf("fleet plan: %d objects, %v bytes", len(out["objects"].([]any)), out["total_bytes"])
	})

	t.Run("audit", func(t *testing.T) {
		recs, err := audit.ReadFrom(auditPath)
		if err != nil {
			t.Fatal(err)
		}
		var allow, deny, outcome int
		for _, x := range recs {
			switch {
			case x.Event == "outcome":
				outcome++
			case x.Decision == "allow":
				allow++
			case x.Decision == "deny":
				deny++
			}
		}
		t.Logf("audit: %d lines: %d allow, %d deny, %d outcomes", len(recs), allow, deny, outcome)
		if deny < 6 || allow < 10 {
			t.Fatalf("audit too thin: %d allow %d deny", allow, deny)
		}
	})
}
