// Package integration runs the alert evaluator against the real pipeline:
// the Go edge (otelcol-s3pq) publishes logs for clusters aa and ab to
// SeaweedFS, the Rust consumer ingests them into ClickHouse and publishes
// {ctl}/watermark.json, the query service (queryd, built from ../query)
// answers with its completeness label, and two evaluator replicas share
// their state on S3 and deliver to a fake Alertmanager.
//
//	ALR_IT_BIN=<dir with otelcol-s3pq and consume> go test ./integration -v
//
// ALR_IT_QUERYD may name a built queryd (otherwise it is built here).
// Services: ClickHouse at ALR_IT_CH (default http://127.0.0.1:18123), S3 at
// ALR_IT_S3 (default http://127.0.0.1:18333, keys otel/otelsecret, bucket
// ALR_IT_BUCKET, default otel). Everything the run creates is named alr-…
// (S3 prefix) or alr_… (database, user) and removed at the end
// (ALR_IT_KEEP=1 keeps it).
//
// The story: errors in both clusters fire (the fleet rule sees both, the
// rule running as cluster aa's identity sees only aa), are resolved, and
// the first send to the pager goes unanswered and is re-sent with the same
// key. Then cluster ab's edge stops: the fleet's complete_through stalls,
// the fleet rule's windows stay partial, errors sent to aa meanwhile are NOT
// evaluated by it, and past the bound it pages "cannot evaluate" naming ab's
// lanes; the rules scoped to cluster aa (by their identity's token, or by
// the rule's `clusters`) are labelled with aa's own complete_through (D29):
// they evaluate the stall's errors on time and never page. The consumer
// then stops publishing and the reason becomes a stale watermark. One
// replica is stopped. The edge comes back with a third replica: the
// watermark advances, the missed windows are evaluated in order, the errors
// from the stall fire late for the fleet rule, and the "cannot evaluate"
// pages resolve.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/casselc/observability/otel-chdb/alerts/internal/config"
	"github.com/casselc/observability/otel-chdb/alerts/internal/notify"
	"github.com/casselc/observability/otel-chdb/alerts/internal/qclient"
	"github.com/casselc/observability/otel-chdb/alerts/internal/rule"
	"github.com/casselc/observability/otel-chdb/alerts/internal/runner"
	"github.com/casselc/observability/otel-chdb/alerts/internal/store"
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

type rig struct {
	t                 *testing.T
	bin, dir          string
	ch, s3url, bucket string
	run, db, ro       string
	roPass            string
	s3                *s3.Client
}

func (r *rig) sql(q string) string {
	r.t.Helper()
	resp, err := http.Post(r.ch, "text/plain", strings.NewReader(q))
	if err != nil {
		r.t.Fatalf("%s: %v", q, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		r.t.Fatalf("%s: %s", q, b)
	}
	return strings.TrimSpace(string(b))
}

func (r *rig) cleanup() {
	if os.Getenv("ALR_IT_KEEP") != "" {
		r.t.Logf("kept: s3 %s/%s, database %s, user %s", r.bucket, r.run, r.db, r.ro)
		return
	}
	for _, q := range []string{"DROP USER IF EXISTS " + r.ro, "DROP DATABASE IF EXISTS " + r.db + " SYNC"} {
		if resp, err := http.Post(r.ch, "text/plain", strings.NewReader(q)); err == nil {
			resp.Body.Close()
		}
	}
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
			_, _ = r.s3.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: &r.bucket, Delete: &s3types.Delete{Objects: ids}})
		}
	}
}

// ---- the Go edge ----------------------------------------------------------

type edge struct {
	cluster string
	port    int
	cmd     *exec.Cmd
	log     string
}

func (r *rig) startEdge(cluster string) *edge {
	t := r.t
	e := &edge{cluster: cluster, port: freePort(t), log: filepath.Join(r.dir, fmt.Sprintf("edge-%s-%d.log", cluster, time.Now().UnixNano()))}
	e.cmd = exec.Command(filepath.Join(r.bin, "otelcol-s3pq"), "--config", "go-edge.yaml")
	e.cmd.Dir = r.dir
	e.cmd.Env = append(os.Environ(), "CLUSTER="+cluster, "PRODUCER=pub-0", "HEARTBEAT=1s", "LOG_LEVEL=warn",
		fmt.Sprintf("OTLP_HTTP=127.0.0.1:%d", e.port), fmt.Sprintf("HEALTH=127.0.0.1:%d", freePort(t)),
		"S3_URL="+r.s3url+"/"+r.bucket+"/"+r.run, "AWS_ACCESS_KEY_ID=otel", "AWS_SECRET_ACCESS_KEY=otelsecret", "AWS_REGION=us-east-1")
	f, _ := os.Create(e.log)
	e.cmd.Stdout, e.cmd.Stderr = f, f
	if err := e.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	return e
}

func (e *edge) stop() {
	if e.cmd.Process != nil {
		_ = e.cmd.Process.Signal(syscall.SIGINT)
		_ = e.cmd.Wait()
		e.cmd.Process = nil
	}
}

// info sends one INFO log.
func (r *rig) info(e *edge, at time.Time) { r.logs(e, "warmup", "INFO", 1, at) }

// errors sends n ERROR logs of service svc at event time at.
func (r *rig) errors(e *edge, svc string, n int, at time.Time) { r.logs(e, svc, "ERROR", n, at) }

// logs sends n logs of service svc at event time at.
func (r *rig) logs(e *edge, svc, sev string, n int, at time.Time) {
	t := r.t
	kv := func(k, v string) map[string]any {
		return map[string]any{"key": k, "value": map[string]any{"stringValue": v}}
	}
	var recs []map[string]any
	for i := 0; i < n; i++ {
		recs = append(recs, map[string]any{"timeUnixNano": fmt.Sprint(at.Add(time.Duration(i) * time.Millisecond).UnixNano()),
			"severityText": sev, "severityNumber": map[string]int{"INFO": 9, "ERROR": 17}[sev], "body": map[string]any{"stringValue": fmt.Sprintf("%s failed %d", svc, i)}})
	}
	body, _ := json.Marshal(map[string]any{"resourceLogs": []any{map[string]any{
		"resource":  map[string]any{"attributes": []any{kv("service.name", svc), kv("k8s.cluster.name", e.cluster), kv("k8s.namespace.name", "shop"), kv("k8s.pod.name", svc+"-1")}},
		"scopeLogs": []any{map[string]any{"scope": map[string]any{"name": "it"}, "logRecords": recs}}}}})
	for i := 0; i < 40; i++ {
		resp, err := http.Post(fmt.Sprintf("http://127.0.0.1:%d/v1/logs", e.port), "application/json", bytes.NewReader(body))
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	b, _ := os.ReadFile(e.log)
	t.Fatalf("edge %s never took the logs:\n%s", e.cluster, b)
}

// ---- the consumer, pumped ---------------------------------------------------

func (r *rig) consume(args ...string) error {
	all := append(args, "--s3", r.s3url+"/"+r.bucket+"/"+r.run, "--key", "otel", "--secret", "otelsecret")
	out, err := exec.Command(filepath.Join(r.bin, "consume"), all...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("consume %v: %v\n%s", args, err, out)
	}
	return nil
}

func (r *rig) pumpOnce() error {
	if err := r.consume("run", "--ch", r.ch, "--db", r.db, "--exit-after-idle", "1500ms", "--poll", "200ms", "--full-list", "500ms"); err != nil {
		return err
	}
	return r.consume("watermark", "--wm-skew", "1s", "--wm-stale", "8s")
}

type pump struct {
	on   atomic.Bool
	stop chan struct{}
	done chan struct{}
}

func (r *rig) startPump() *pump {
	p := &pump{stop: make(chan struct{}), done: make(chan struct{})}
	p.on.Store(true)
	go func() {
		defer close(p.done)
		for {
			select {
			case <-p.stop:
				return
			default:
			}
			if p.on.Load() {
				t0 := time.Now()
				if err := r.pumpOnce(); err != nil {
					r.t.Logf("pump: %v", err)
				}
				if d := time.Since(t0); d > 6*time.Second {
					r.t.Logf("pump: one cycle took %v (the service's watermark max_age_s is 30)", d.Round(100*time.Millisecond))
				}
			} else {
				time.Sleep(200 * time.Millisecond)
			}
		}
	}()
	return p
}

func (p *pump) close() { close(p.stop); <-p.done }

// ---- a fake Alertmanager ------------------------------------------------------

type got struct {
	at    time.Time
	alert notify.Alert
}

type alertmanager struct {
	mu       sync.Mutex
	alerts   []got
	requests int
	dropped  atomic.Int32 // requests left unanswered (connection closed)
	dropNext atomic.Int32
}

func (a *alertmanager) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var in []notify.Alert
	if r.URL.Path != "/api/v2/alerts" || json.NewDecoder(r.Body).Decode(&in) != nil {
		http.Error(w, "bad", 400)
		return
	}
	a.mu.Lock()
	a.requests++
	now := time.Now()
	for _, x := range in {
		a.alerts = append(a.alerts, got{now, x})
	}
	a.mu.Unlock()
	if a.dropNext.Load() > 0 { // taken, but the answer is lost: the sender cannot know
		a.dropNext.Add(-1)
		a.dropped.Add(1)
		if hj, ok := w.(http.Hijacker); ok {
			c, _, _ := hj.Hijack()
			_ = c.Close()
			return
		}
	}
	w.WriteHeader(200)
}

func (a *alertmanager) find(f func(got) bool) []got {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []got
	for _, x := range a.alerts {
		if f(x) {
			out = append(out, x)
		}
	}
	return out
}

func resolved(x got) bool {
	e, err := time.Parse(time.RFC3339Nano, x.alert.EndsAt)
	return err == nil && !e.After(x.at)
}

func waitFor(t *testing.T, what string, d time.Duration, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", d, what)
}

// ---- the test --------------------------------------------------------------

func TestIntegration(t *testing.T) {
	bin := os.Getenv("ALR_IT_BIN")
	if bin == "" {
		t.Skip("ALR_IT_BIN (a directory with otelcol-s3pq and consume) is not set")
	}
	id := fmt.Sprintf("%d%04x", time.Now().Unix(), rand.Uint32()&0xffff)
	r := &rig{t: t, bin: bin, dir: t.TempDir(), ch: env("ALR_IT_CH", "http://127.0.0.1:18123"), s3url: env("ALR_IT_S3", "http://127.0.0.1:18333"),
		bucket: env("ALR_IT_BUCKET", "otel"), run: "alr-" + id, db: "alr_" + id, ro: "alr_ro_" + id, roPass: fmt.Sprintf("%016x", rand.Uint64())}
	if resp, err := http.Get(r.ch + "/ping"); err != nil {
		t.Skipf("no ClickHouse at %s: %v", r.ch, err)
	} else {
		resp.Body.Close()
	}
	r.s3 = s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String(r.s3url), UsePathStyle: true,
		Credentials: credentials.NewStaticCredentialsProvider("otel", "otelsecret", "")})
	t.Cleanup(r.cleanup)
	edgeCfg, err := os.ReadFile("../../conformance/go-edge.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(r.dir, "go-edge.yaml"), edgeCfg, 0o644); err != nil {
		t.Fatal(err)
	}
	queryd := os.Getenv("ALR_IT_QUERYD")
	if queryd == "" {
		queryd = filepath.Join(r.dir, "queryd")
		b := exec.Command("go", "build", "-o", queryd, "./cmd/queryd")
		b.Dir = "../../query"
		if out, err := b.CombinedOutput(); err != nil {
			t.Fatalf("building queryd: %v\n%s", err, out)
		}
	}

	// 1. both clusters' edges run; the consumer creates the tables and publishes a first watermark
	aa, ab := r.startEdge("aa"), r.startEdge("ab")
	defer func() { aa.stop(); ab.stop() }()
	time.Sleep(2 * time.Second)
	r.info(aa, time.Now()) // the consumer creates the tables with the first data
	if err := r.pumpOnce(); err != nil {
		t.Fatal(err)
	}
	if got := r.sql("SELECT count() FROM " + r.db + ".otel_logs"); got != "1" {
		t.Fatalf("central holds %s logs after the warm-up, want 1", got)
	}
	r.sql(fmt.Sprintf("CREATE USER %s IDENTIFIED WITH plaintext_password BY '%s' SETTINGS readonly = 2", r.ro, r.roPass))
	r.sql(fmt.Sprintf("GRANT SELECT ON %s.otel_logs TO %s", r.db, r.ro))

	// 2. the identity provider, the query service and the pager
	idp := newIssuer(map[string]map[string]any{
		"alertd-fleet": {"groups": "alerts-fleet"},
		"alertd-aa":    {"clusters": "aa", "namespaces": "*", "roles": "query"},
	})
	defer idp.Close()
	qport := freePort(t)
	qcfg := map[string]any{
		"listen": fmt.Sprintf("127.0.0.1:%d", qport),
		"oidc":   map[string]any{"issuer": idp.URL, "audience": "otel-query", "jwks_url": idp.URL + "/jwks"},
		"claims": map[string]any{"subject": "sub", "clusters": "clusters", "namespaces": "namespaces", "roles": "roles", "groups": "groups",
			"group_grants": map[string]any{"alerts-fleet": map[string]any{"clusters": []string{"*"}, "namespaces": []string{"*"}, "roles": []string{"query"}}}},
		"audit": map[string]any{"path": filepath.Join(r.dir, "audit.jsonl")},
		"central": map[string]any{"url": r.ch, "user": r.ro, "password_env": "ALR_IT_RO_PASS", "database": r.db,
			"tables": []any{map[string]any{"name": "otel_logs", "time_column": "Timestamp", "received_column": "received_at", "scope": "columns",
				"cluster_expr": "`__hdx_materialized_k8s.cluster.name`", "namespace_expr": "`__hdx_materialized_k8s.namespace.name`",
				"signals": []string{"logs"}}}},
		"s3":   map[string]any{"endpoint": r.s3url, "bucket": r.bucket, "region": "us-east-1"},
		"lake": map[string]any{"root": r.run},
		// max_lateness 1 s + the rules' lateness 1 s: the 2 s the story was timed with
		"watermark": map[string]any{"cache_s": 1, "max_age_s": 30, "max_lateness_s": 1},
	}
	qb, _ := json.Marshal(qcfg)
	qpath := filepath.Join(r.dir, "queryd.json")
	_ = os.WriteFile(qpath, qb, 0o600)
	qcmd := exec.Command(queryd, "-config", qpath)
	qcmd.Env = append(os.Environ(), "ALR_IT_RO_PASS="+r.roPass, "QS_S3_KEY=otel", "QS_S3_SECRET=otelsecret")
	qlog, _ := os.Create(filepath.Join(r.dir, "queryd.log"))
	qcmd.Stdout, qcmd.Stderr = qlog, qlog
	if err := qcmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = qcmd.Process.Signal(syscall.SIGTERM)
		_ = qcmd.Wait()
		if t.Failed() {
			b, _ := os.ReadFile(qlog.Name())
			t.Logf("queryd log:\n%s", lastLines(string(b), 30))
		}
	}()
	qurl := fmt.Sprintf("http://127.0.0.1:%d", qport)
	waitFor(t, "queryd", 20*time.Second, func() bool {
		resp, err := http.Get(qurl + "/healthz")
		if err == nil {
			resp.Body.Close()
		}
		return err == nil && resp.StatusCode == 200
	})
	am := &alertmanager{}
	am.dropNext.Store(1) // the first delivery is taken but unanswered
	amsrv := httptest.NewServer(am)
	defer amsrv.Close()

	// 3. the rules and two evaluator replicas
	rulesYAML := `
rules:
  - name: fleet_errors
    sql: SELECT ServiceName AS service, count() AS value FROM otel_logs WHERE SeverityText = 'ERROR' GROUP BY service
    window: 10s
    condition: {op: ">", threshold: 2}
    identity: fleet
    labels: {team: sre}
    annotations: {summary: "{{labels.service}}: {{value}} errors in 10 s"}
  - name: aa_errors
    sql: SELECT ServiceName AS service, count() AS value FROM otel_logs WHERE SeverityText = 'ERROR' GROUP BY service
    window: 10s
    condition: {op: ">", threshold: 0}
    identity: team-aa
  - name: aa_via_fleet
    sql: SELECT ServiceName AS service, count() AS value FROM otel_logs WHERE SeverityText = 'ERROR' GROUP BY service
    window: 10s
    condition: {op: ">", threshold: 0}
    identity: fleet
    clusters: [aa]
`
	t.Setenv("ALR_IT_SECRET_FLEET", "fleet-secret")
	t.Setenv("ALR_IT_SECRET_AA", "aa-secret")
	lateness := 1 // on top of the service's max_lateness (1 s)
	mkReplica := func(name string) (*runner.Runner, context.CancelFunc, chan struct{}) {
		rules, err := rule.Parse([]byte(rulesYAML))
		if err != nil {
			t.Fatal(err)
		}
		c := &config.Config{Replica: name, Query: config.QueryConfig{URL: qurl, TimeoutS: 10},
			Identities: map[string]qclient.Identity{
				"fleet":   {TokenURL: idp.URL + "/token", ClientID: "alertd-fleet", ClientSecretEnv: "ALR_IT_SECRET_FLEET", Audience: "otel-query"},
				"team-aa": {TokenURL: idp.URL + "/token", ClientID: "alertd-aa", ClientSecretEnv: "ALR_IT_SECRET_AA", Audience: "otel-query"},
			},
			State: store.S3Config{Endpoint: r.s3url, Bucket: r.bucket, Prefix: r.run + "/alr-state", AccessKey: "otel", SecretKey: "otelsecret"},
			Sink:  notify.Config{URL: amsrv.URL + "/api/v2/alerts", TimeoutS: 2},
			Eval: config.EvalConfig{TickS: 1, CannotEvaluateAfterS: 45, RefreshS: 3, HoldResolvedS: 1, BackoffBaseS: 1, BackoffMaxS: 3,
				LatenessS: &lateness, MaxWindowsPerTick: 3}}
		logf, _ := os.Create(filepath.Join(r.dir, "alertd-"+name+".log"))
		rr, err := config.Build(context.Background(), c, rules, slog.New(slog.NewJSONHandler(logf, nil)))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { rr.Run(ctx); close(done) }()
		return rr, cancel, done
	}
	r1, stop1, done1 := mkReplica("r1")
	r2, stop2, done2 := mkReplica("r2")
	defer func() { stop2(); <-done2 }()
	p := r.startPump()
	defer p.close()
	time.Sleep(3 * time.Second)

	// 4. errors in both clusters fire; the aa identity sees only aa's
	t1 := time.Now()
	r.errors(aa, "checkout", 5, t1)
	r.errors(ab, "payments", 4, t1)
	isFiring := func(rule, svc string) func(got) bool {
		return func(x got) bool {
			return x.alert.Labels["alertname"] == rule && x.alert.Labels["service"] == svc && !resolved(x)
		}
	}
	waitFor(t, "fleet_errors firing for checkout and payments", 90*time.Second, func() bool {
		return len(am.find(isFiring("fleet_errors", "checkout"))) > 0 && len(am.find(isFiring("fleet_errors", "payments"))) > 0 &&
			len(am.find(isFiring("aa_errors", "checkout"))) > 0 && len(am.find(isFiring("aa_via_fleet", "checkout"))) > 0
	})
	if len(am.find(isFiring("aa_errors", "payments"))) != 0 {
		t.Fatal("the rule running as cluster aa's identity saw cluster ab's errors")
	}
	if len(am.find(isFiring("aa_via_fleet", "payments"))) != 0 {
		t.Fatal("the rule narrowed to cluster aa saw cluster ab's errors")
	}
	first := am.find(isFiring("fleet_errors", "checkout"))[0].alert
	if first.Annotations["summary"] != "checkout: 5 errors in 10 s" || first.Labels["team"] != "sre" || first.Labels["severity"] != "page" {
		t.Fatalf("payload %+v", first)
	}
	waitFor(t, "checkout resolved", 60*time.Second, func() bool {
		return len(am.find(func(x got) bool { return isFiringLabels(x, "fleet_errors", "checkout") && resolved(x) })) > 0
	})
	if am.dropped.Load() != 1 {
		t.Fatal("the unanswered delivery was not exercised")
	}
	// the unanswered first send was re-sent with the same key
	checkResent(t, am)
	t.Logf("phase 1: %d alerts in %d requests; dropped %d", len(am.find(func(got) bool { return true })), am.requests, am.dropped.Load())

	// 5. cluster ab's edge stops: complete_through stalls
	ab.stop()
	tStall := time.Now()
	time.Sleep(2 * time.Second)
	t2 := time.Now()
	r.errors(aa, "checkout", 6, t2)
	if early := am.find(func(x got) bool { return x.alert.Labels["alertname"] == "AlertCannotEvaluate" }); len(early) != 0 {
		t.Fatalf("%d cannot-evaluate pages before the stall, the first: %v %v", len(early), early[0].alert.Labels, early[0].alert.Annotations)
	}
	cannot := func(ruleName string) func(got) bool {
		return func(x got) bool {
			return x.alert.Labels["alertname"] == "AlertCannotEvaluate" && x.alert.Labels["alert_rule"] == ruleName && !resolved(x)
		}
	}
	// D29: the rules scoped to aa evaluate the stall's errors on time
	onTime := func(ruleName string) func(got) bool {
		return func(x got) bool {
			st, _ := time.Parse(time.RFC3339Nano, x.alert.StartsAt)
			return isFiringLabels(x, ruleName, "checkout") && !st.Before(t2.Truncate(10*time.Second))
		}
	}
	waitFor(t, "cannot evaluate page for the fleet rule; aa's rules fire on time", 150*time.Second, func() bool {
		return len(am.find(cannot("fleet_errors"))) > 0 && len(am.find(onTime("aa_errors"))) > 0 && len(am.find(onTime("aa_via_fleet"))) > 0
	})
	t.Logf("aa's rules fired %v after the stall began; the fleet rule paged cannot-evaluate",
		am.find(onTime("aa_errors"))[0].at.Sub(tStall).Round(time.Second))
	for _, rn := range []string{"aa_errors", "aa_via_fleet"} {
		if n := len(am.find(cannot(rn))); n != 0 {
			t.Fatalf("%s paged cannot-evaluate while only cluster ab stalled: %v", rn, am.find(cannot(rn))[0].alert.Annotations)
		}
	}
	page := am.find(cannot("fleet_errors"))[0].alert
	t.Logf("cannot-evaluate page after %v: %s", time.Since(tStall).Round(time.Second), page.Annotations["reason"])
	if !strings.Contains(page.Annotations["reason"], "ab/pub-0/") || !strings.Contains(page.Annotations["clusters"], "ab") {
		t.Fatalf("the page does not name cluster ab's lanes: %v", page.Annotations)
	}
	episode2 := func(x got) bool {
		st, _ := time.Parse(time.RFC3339Nano, x.alert.StartsAt)
		return isFiringLabels(x, "fleet_errors", "checkout") && !st.Before(t2.Truncate(10*time.Second))
	}
	if n := len(am.find(episode2)); n != 0 {
		t.Fatal("errors sent during the stall were evaluated before their window was complete")
	}
	// one replica stops; the consumer stops publishing: the watermark goes stale
	stop1()
	<-done1
	_ = r1
	p.on.Store(false)
	waitFor(t, "the reason to become a stale watermark", 90*time.Second, func() bool {
		for _, x := range am.find(cannot("fleet_errors")) {
			if strings.Contains(x.alert.Annotations["reason"], "watermark stale") {
				return true
			}
		}
		return false
	})
	if n := len(am.find(episode2)); n != 0 {
		t.Fatal("evaluated on a stale watermark")
	}

	// (aa's rules may page too once the watermark itself is stale: any such
	// page must resolve at recovery)
	settled := func(rn string) bool {
		return len(am.find(cannot(rn))) == 0 || len(am.find(func(x got) bool {
			return x.alert.Labels["alertname"] == "AlertCannotEvaluate" && x.alert.Labels["alert_rule"] == rn && resolved(x)
		})) > 0
	}

	// 6. ab's edge comes back, the consumer publishes again, a third replica joins
	tRecover := time.Now()
	ab = r.startEdge("ab")
	p.on.Store(true)
	r3, stop3, done3 := mkReplica("r3")
	defer func() { stop3(); <-done3 }()
	waitFor(t, "the stall's errors evaluated late, and the pages resolved", 120*time.Second, func() bool {
		return len(am.find(episode2)) > 0 &&
			len(am.find(func(x got) bool {
				return x.alert.Labels["alertname"] == "AlertCannotEvaluate" && x.alert.Labels["alert_rule"] == "fleet_errors" && resolved(x)
			})) > 0 &&
			settled("aa_errors") && settled("aa_via_fleet")
	})
	late := am.find(episode2)[0]
	if late.at.Before(tRecover) {
		t.Fatal("the stall's errors reached the pager before the watermark advanced")
	}
	t.Logf("stall errors paged %v after recovery, evaluation_delay %s", late.at.Sub(tRecover).Round(time.Second), late.alert.Annotations["evaluation_delay"])

	// 7. the committed state: every window from the start evaluated once, in order
	for _, name := range []string{"fleet_errors", "aa_errors", "aa_via_fleet"} {
		st, _, err := r3.Load(context.Background(), name)
		if err != nil || st == nil {
			t.Fatal(name, err)
		}
		t.Logf("%s: evaluated %d windows, skipped %d, writer %s seq %d, notices %d", name, st.Evaluated, st.Skipped, st.Writer, st.Seq, len(st.Notices))
		if st.Skipped != 0 || st.Meta.Firing {
			t.Fatalf("%s: skipped %d, cannot evaluate %v", name, st.Skipped, st.Meta.Firing)
		}
	}
	for _, rr := range []*runner.Runner{r2, r3} {
		t.Logf("%s: evaluations complete %v partial %v unknown %v; state writes ok %v conflict %v; deliveries acked %v ambiguous %v",
			rr.Writer, rr.Metrics.Value("alr_evaluations_total", "fleet_errors", "complete"), rr.Metrics.Value("alr_evaluations_total", "fleet_errors", "partial"),
			rr.Metrics.Value("alr_evaluations_total", "fleet_errors", "unknown"), rr.Metrics.Value("alr_state_writes_total", "ok"),
			rr.Metrics.Value("alr_state_writes_total", "conflict"), rr.Metrics.Value("alr_deliveries_total", "acked"), rr.Metrics.Value("alr_deliveries_total", "ambiguous"))
	}
	if r2.Metrics.Value("alr_evaluations_total", "fleet_errors", "partial")+r3.Metrics.Value("alr_evaluations_total", "fleet_errors", "partial") == 0 {
		t.Error("no partial evaluations were counted")
	}
	// no episode's firing reached the pager twice under different keys
	keys := map[string]string{}
	for _, x := range am.find(func(x got) bool { return x.alert.Labels["alert_kind"] == "rule" }) {
		k := x.alert.Annotations["dedup_key"]
		ep := x.alert.Labels["alertname"] + "/" + x.alert.Labels["service"] + "/" + x.alert.Labels["alert_episode"]
		if prev, ok := keys[ep]; ok && prev != k {
			t.Fatalf("episode %s under two keys %s and %s", ep, prev, k)
		}
		keys[ep] = k
	}
	t.Logf("episodes paged: %d; pager requests %d", len(keys), am.requests)
}

func isFiringLabels(x got, rule, svc string) bool {
	return x.alert.Labels["alertname"] == rule && x.alert.Labels["service"] == svc
}

// checkResent: the alert in the unanswered request was delivered again with
// the same dedup key.
func checkResent(t *testing.T, am *alertmanager) {
	t.Helper()
	am.mu.Lock()
	defer am.mu.Unlock()
	if len(am.alerts) == 0 {
		t.Fatal("nothing delivered")
	}
	k := am.alerts[0].alert.Annotations["dedup_key"]
	n := 0
	for _, x := range am.alerts {
		if x.alert.Annotations["dedup_key"] == k {
			n++
		}
	}
	if n < 2 {
		t.Fatalf("%s was sent once, and that send was never answered", k)
	}
}

func lastLines(s string, n int) string {
	l := strings.Split(strings.TrimSpace(s), "\n")
	if len(l) > n {
		l = l[len(l)-n:]
	}
	return strings.Join(l, "\n")
}
