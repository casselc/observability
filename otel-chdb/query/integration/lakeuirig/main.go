// Command lakeuirig brings up everything the lake UI's browser test needs,
// from the real pieces, and keeps it up until it is signalled:
//
//   - a bucket of its own (lui-…) on the local SeaweedFS, with CORS for the
//     page's origin;
//   - the Go edge (otelcol-s3pq, conformance/go-edge.yaml, the clickstack
//     metrics layout) publishing logs, spans and gauge points for two
//     clusters, lui-a and lui-b;
//   - the Rust consumer ingesting them into ClickHouse (database lui_…) and
//     publishing {ctl}/watermark.json; then a late batch for lui-a that is
//     published after the watermark, so it lies past complete_through;
//   - the query service as cmd/queryd builds it (internal/app), with the
//     test issuer (internal/auth/authtest) plus an authorization-code + PKCE
//     front on it, so the page signs in the way it would against a real IdP;
//   - a counting pass-through in front of SeaweedFS that the plan's URLs are
//     signed for (SigV4 signs the host), so the test can count every byte
//     the browser fetched independently of the page;
//   - a static server for ../../../lakeui with the page's config.json and a
//     small control API under /rig/ (info, tap counters, watermark drop and
//     restore).
//
// It prints one line, "LAKEUI_RIG_READY <json>", when ready, and cleans up
// everything it created (bucket, database, user) on SIGINT or SIGTERM
// (LUI_KEEP=1 keeps it; LUI_PREFIX names the bucket and database, default
// "lui"). The lake indexer (D27) runs one pass before the late batch, so
// the late batch is unindexed ("scan") until POST /rig/index runs another;
// every batch also carries one trace and one log word that live in its
// object only (truth.rare_trace_ids, truth.rare_needles).
//
//	QS_IT_BIN=<dir with otelcol-s3pq and consume> go run ./integration/lakeuirig
//
// Services: ClickHouse at QS_IT_CH (default http://127.0.0.1:18123), S3 at
// QS_IT_S3 (default http://127.0.0.1:18333, keys otel/otelsecret).
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html"
	"io"
	"log"
	"math"
	mrand "math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
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
	"github.com/casselc/observability/otel-chdb/query/internal/lakeidx"
	"github.com/casselc/observability/otel-chdb/query/internal/sqlscope"
	"github.com/casselc/observability/otel-chdb/query/internal/store"
	"github.com/golang-jwt/jwt/v5"
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func freePort() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

type rig struct {
	ch, s3url, bucket, run, db, bin, work string
	s3                                    *s3.Client
}

func (r *rig) sql(q string) string {
	resp, err := http.Post(r.ch, "text/plain", strings.NewReader(q))
	must(err)
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		log.Fatalf("%s: %s", q, b)
	}
	return strings.TrimSpace(string(b))
}

// ---- the data ------------------------------------------------------------

type resource struct{ cluster, namespace, pod string }

func attrs(r resource) []map[string]any {
	kv := func(k, v string) map[string]any {
		return map[string]any{"key": k, "value": map[string]any{"stringValue": v}}
	}
	return []map[string]any{kv("service.name", "svc-"+r.namespace), kv("k8s.cluster.name", r.cluster),
		kv("k8s.namespace.name", r.namespace), kv("k8s.pod.name", r.pod), kv("k8s.pod.uid", r.cluster+"-"+r.pod+"-uid")}
}

var severities = []string{"INFO", "INFO", "INFO", "DEBUG", "WARN", "ERROR"}

// Needle is a word that appears in a few log bodies only (the narrow search).
const Needle = "needle-7f3a"

func logBody(rng *mrand.Rand, r resource, i int) string {
	paths := []string{"/api/cart", "/api/pay", "/api/items", "/healthz", "/api/user"}
	b := fmt.Sprintf("%s %s req=%08x path=%s/%d status=%d user=u%05d latency_ms=%d bytes=%d msg=%q",
		r.cluster, r.namespace, rng.Uint32(), paths[rng.IntN(len(paths))], rng.IntN(100000),
		[]int{200, 200, 200, 201, 404, 500}[rng.IntN(6)], rng.IntN(20000), rng.IntN(900), rng.IntN(1<<16),
		[]string{"handled", "cache miss", "retrying upstream", "slow downstream call", "ok"}[rng.IntN(5)])
	if i%997 == 13 {
		b += " " + Needle
	}
	return b
}

type edge struct {
	cmd     *exec.Cmd
	port    int
	cluster string
}

func (r *rig) startEdge(cluster string) *edge {
	port, health := freePort(), freePort()
	cmd := exec.Command(filepath.Join(r.bin, "otelcol-s3pq"), "--config", filepath.Join(r.work, "go-edge.yaml"))
	cmd.Env = append(os.Environ(),
		"CLUSTER="+cluster, "PRODUCER=pub-0", "HEARTBEAT=2s", "LOG_LEVEL=warn", "METRICS_LAYOUT=clickstack_tables",
		fmt.Sprintf("OTLP_HTTP=127.0.0.1:%d", port), fmt.Sprintf("HEALTH=127.0.0.1:%d", health),
		"S3_URL="+r.s3url+"/"+r.bucket+"/"+r.run, "AWS_ACCESS_KEY_ID=otel", "AWS_SECRET_ACCESS_KEY=otelsecret", "AWS_REGION=us-east-1")
	lf, err := os.Create(filepath.Join(r.work, "edge-"+cluster+".log"))
	must(err)
	cmd.Stdout, cmd.Stderr = lf, lf
	must(cmd.Start())
	return &edge{cmd: cmd, port: port, cluster: cluster}
}

func (e *edge) stop() {
	_ = e.cmd.Process.Signal(syscall.SIGINT)
	_ = e.cmd.Wait()
}

func (e *edge) post(path string, body any) {
	b, _ := json.Marshal(body)
	for i := 0; i < 40; i++ {
		resp, err := http.Post(fmt.Sprintf("http://127.0.0.1:%d%s", e.port, path), "application/json", bytes.NewReader(b))
		if err == nil {
			rb, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
			log.Printf("POST %s: %d %s", path, resp.StatusCode, rb)
		}
		time.Sleep(500 * time.Millisecond)
	}
	log.Fatalf("edge %s: POST %s never answered 200", e.cluster, path)
}

// Truth is what the rig published, for the test to check against (the test
// counts in ClickHouse itself; this is for choosing inputs).
type Truth struct {
	// TraceIDs: traces whose spans were sent in two batches (so two objects).
	TraceIDs    map[string][]string `json:"trace_ids"` // cluster -> ids
	TraceSpans  int                 `json:"trace_spans"`
	Metric      string              `json:"metric"`
	Needle      string              `json:"needle"`
	At          string              `json:"at"`           // first event time
	LateFrom    string              `json:"late_from"`    // the late batch's first event time
	LateTo      string              `json:"late_to"`      // and last
	LateCluster string              `json:"late_cluster"` // the late batch's cluster
	LateLogs    int                 `json:"late_logs"`
	// RareTraceIDs: per cluster, one trace per batch whose spans are in
	// that batch's object only; RareNeedles: per batch, a word in one lui-a
	// log row of that batch only. What an index can prune (D27).
	RareTraceIDs map[string][]string `json:"rare_trace_ids"`
	RareNeedles  []string            `json:"rare_needles"`
}

type batchSpec struct {
	from     time.Time
	span     time.Duration
	logs     int // per resource
	spans    int // filler spans per resource
	gauge    bool
	traceIDs []string // spans of these traces, split across resources
	seed     uint64
	// rareTrace: two spans of this trace in the first resource only;
	// rareNeedle: appended to one log body of the first resource
	rareTrace, rareNeedle string
}

func sendBatch(e *edge, res []resource, b batchSpec, truth *Truth) {
	rng := mrand.New(mrand.NewPCG(b.seed, 7))
	var rl, rs, rm []map[string]any
	for ri, x := range res {
		var recs, spans []map[string]any
		for i := 0; i < b.logs; i++ {
			t := b.from.Add(time.Duration(float64(b.span) * float64(i) / float64(b.logs)))
			t = t.Add(time.Duration(ri) * time.Millisecond)
			body := logBody(rng, x, i)
			if ri == 0 && i == 7 && b.rareNeedle != "" {
				body += " " + b.rareNeedle
			}
			recs = append(recs, map[string]any{"timeUnixNano": fmt.Sprint(t.UnixNano()),
				"severityText": severities[rng.IntN(len(severities))], "body": map[string]any{"stringValue": body}})
		}
		for i := 0; i < b.spans; i++ {
			s := b.from.Add(time.Duration(float64(b.span) * float64(i) / float64(max(b.spans, 1))))
			spans = append(spans, map[string]any{"traceId": fmt.Sprintf("%016x%016x", rng.Uint64(), rng.Uint64()), "spanId": fmt.Sprintf("%016x", rng.Uint64()),
				"name": "filler", "kind": 2, "startTimeUnixNano": fmt.Sprint(s.UnixNano()), "endTimeUnixNano": fmt.Sprint(s.Add(time.Millisecond).UnixNano())})
		}
		if ri == 0 && b.rareTrace != "" {
			for k := 0; k < 2; k++ {
				s := b.from.Add(time.Duration(30+k) * time.Second)
				spans = append(spans, map[string]any{"traceId": b.rareTrace, "spanId": fmt.Sprintf("%016x", rng.Uint64()),
					"name": fmt.Sprintf("rare-op-%d", k), "kind": 2, "startTimeUnixNano": fmt.Sprint(s.UnixNano()),
					"endTimeUnixNano": fmt.Sprint(s.Add(3 * time.Millisecond).UnixNano())})
			}
		}
		for ti, id := range b.traceIDs {
			if ti%len(res) != ri && len(res) > 1 && ti%3 != 0 {
				continue // most traces: spans in one resource per batch; every third: in all
			}
			for k := 0; k < 2; k++ {
				s := b.from.Add(time.Duration(ti)*time.Second + time.Duration(k*10)*time.Millisecond)
				spans = append(spans, map[string]any{"traceId": id, "spanId": fmt.Sprintf("%016x", rng.Uint64()),
					"name": fmt.Sprintf("%s-op-%d", x.namespace, k), "kind": 2, "startTimeUnixNano": fmt.Sprint(s.UnixNano()),
					"endTimeUnixNano": fmt.Sprint(s.Add(time.Duration(5+k) * time.Millisecond).UnixNano())})
				truth.TraceSpans++
			}
		}
		rl = append(rl, map[string]any{"resource": map[string]any{"attributes": attrs(x)}, "scopeLogs": []any{map[string]any{"scope": map[string]any{"name": "lui"}, "logRecords": recs}}})
		if len(spans) > 0 {
			rs = append(rs, map[string]any{"resource": map[string]any{"attributes": attrs(x)}, "scopeSpans": []any{map[string]any{"scope": map[string]any{"name": "lui"}, "spans": spans}}})
		}
		if b.gauge {
			var pts []map[string]any
			for t := b.from; t.Before(b.from.Add(b.span)); t = t.Add(5 * time.Second) {
				v := 50 + 40*math.Sin(float64(t.Unix())/97.0+float64(ri)) // deterministic, per resource
				pts = append(pts, map[string]any{"timeUnixNano": fmt.Sprint(t.UnixNano()), "asDouble": math.Round(v*100) / 100})
			}
			rm = append(rm, map[string]any{"resource": map[string]any{"attributes": attrs(x)}, "scopeMetrics": []any{map[string]any{
				"scope": map[string]any{"name": "lui"}, "metrics": []any{map[string]any{"name": truth.Metric, "unit": "1",
					"gauge": map[string]any{"dataPoints": pts}}}}}})
		}
	}
	if b.logs > 0 {
		e.post("/v1/logs", map[string]any{"resourceLogs": rl})
	}
	if len(rs) > 0 {
		e.post("/v1/traces", map[string]any{"resourceSpans": rs})
	}
	if len(rm) > 0 {
		e.post("/v1/metrics", map[string]any{"resourceMetrics": rm})
	}
}

func (r *rig) consume(args ...string) {
	all := append(args, "--s3", r.s3url+"/"+r.bucket+"/"+r.run, "--key", "otel", "--secret", "otelsecret")
	out, err := exec.Command(filepath.Join(r.bin, "consume"), all...).CombinedOutput()
	if err != nil {
		log.Fatalf("consume %v: %v\n%s", args, err, out)
	}
	l := strings.Split(strings.TrimSpace(string(out)), "\n")
	log.Printf("consume %s: %s", args[0], l[len(l)-1])
}

// ---- the IdP's authorization-code front ------------------------------------

type codeGrant struct {
	user, redirect, challenge, clientID string
	at                                  time.Time
}

type idp struct {
	is       *authtest.Issuer
	users    map[string]jwt.MapClaims
	origins  []string
	clientID string
	tokenTTL time.Duration
	mu       sync.Mutex
	codes    map[string]codeGrant
	Issued   int
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (d *idp) cors(w http.ResponseWriter, r *http.Request) bool {
	o := r.Header.Get("Origin")
	for _, a := range d.origins {
		if o == a {
			w.Header().Set("Access-Control-Allow-Origin", o)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			w.Header().Add("Vary", "Origin")
		}
	}
	if r.Method == http.MethodOptions {
		w.WriteHeader(204)
		return true
	}
	return false
}

func (d *idp) redirectAllowed(u string) bool {
	for _, o := range d.origins {
		if strings.HasPrefix(u, o+"/") {
			return true
		}
	}
	return false
}

// wrap puts /authorize, /token and a discovery document that names them in
// front of the test issuer's own handler (JWKS, minting).
func (d *idp) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			if d.cors(w, r) {
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"issuer": d.is.URL, "jwks_uri": d.is.URL + "/jwks",
				"authorization_endpoint": d.is.URL + "/authorize", "token_endpoint": d.is.URL + "/token",
				"response_types_supported": []string{"code"}, "code_challenge_methods_supported": []string{"S256"},
				"grant_types_supported": []string{"authorization_code"}, "id_token_signing_alg_values_supported": []string{"RS256"}})
		case "/authorize":
			d.authorize(w, r)
		case "/token":
			if d.cors(w, r) {
				return
			}
			d.token(w, r)
		default:
			next.ServeHTTP(w, r)
		}
	})
}

func (d *idp) authorize(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	q := r.Form
	if q.Get("response_type") != "code" || q.Get("client_id") != d.clientID || !d.redirectAllowed(q.Get("redirect_uri")) ||
		q.Get("code_challenge_method") != "S256" || len(q.Get("code_challenge")) < 43 {
		http.Error(w, "invalid_request: response_type=code, a known client_id, a registered redirect_uri and S256 PKCE are required", 400)
		return
	}
	user := q.Get("user")
	if r.Method != http.MethodPost || d.users[user] == nil {
		// the login page: one button per test user
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		var sb strings.Builder
		sb.WriteString("<!doctype html><title>Test IdP sign-in</title><h1>Test IdP</h1><form method=post>")
		for k, v := range q {
			if k != "user" {
				fmt.Fprintf(&sb, `<input type=hidden name="%s" value="%s">`, html.EscapeString(k), html.EscapeString(v[0]))
			}
		}
		names := make([]string, 0, len(d.users))
		for u := range d.users {
			names = append(names, u)
		}
		sort.Strings(names)
		for _, u := range names {
			fmt.Fprintf(&sb, `<button name=user value="%s">Sign in as %s</button> `, html.EscapeString(u), html.EscapeString(u))
		}
		sb.WriteString("</form>")
		_, _ = io.WriteString(w, sb.String())
		return
	}
	code := randHex(16)
	d.mu.Lock()
	d.codes[code] = codeGrant{user: user, redirect: q.Get("redirect_uri"), challenge: q.Get("code_challenge"), clientID: q.Get("client_id"), at: time.Now()}
	d.mu.Unlock()
	u, _ := url.Parse(q.Get("redirect_uri"))
	rq := u.Query()
	rq.Set("code", code)
	rq.Set("state", q.Get("state"))
	u.RawQuery = rq.Encode()
	http.Redirect(w, r, u.String(), http.StatusFound)
}

func (d *idp) token(w http.ResponseWriter, r *http.Request) {
	fail := func(e string) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(400)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": e})
	}
	if r.Method != http.MethodPost {
		fail("invalid_request")
		return
	}
	_ = r.ParseForm()
	code := r.PostForm.Get("code")
	d.mu.Lock()
	g, ok := d.codes[code]
	delete(d.codes, code) // single use
	d.mu.Unlock()
	if r.PostForm.Get("grant_type") != "authorization_code" || !ok || time.Since(g.at) > time.Minute {
		fail("invalid_grant")
		return
	}
	sum := sha256.Sum256([]byte(r.PostForm.Get("code_verifier")))
	if base64.RawURLEncoding.EncodeToString(sum[:]) != g.challenge || r.PostForm.Get("redirect_uri") != g.redirect ||
		r.PostForm.Get("client_id") != g.clientID {
		fail("invalid_grant")
		return
	}
	cl := jwt.MapClaims{"aud": "otel-query", "sub": g.user, "exp": time.Now().Add(d.tokenTTL).Unix(), "azp": d.clientID}
	for k, v := range d.users[g.user] {
		cl[k] = v
	}
	tok := d.is.Mint(cl)
	d.mu.Lock()
	d.Issued++
	d.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(map[string]any{"access_token": tok, "id_token": tok, "token_type": "Bearer",
		"expires_in": int(d.tokenTTL.Seconds())})
}

// ---- the counting pass-through in front of SeaweedFS -----------------------

type tapEntry struct {
	At     string `json:"at"`
	Method string `json:"method"`
	Key    string `json:"key"`
	Range  string `json:"range,omitempty"`
	Status int    `json:"status"`
	Bytes  int64  `json:"bytes"`
}

type tap struct {
	mu      sync.Mutex
	entries []tapEntry
}

type countingBody struct {
	io.ReadCloser
	n    int64
	done func(int64)
}

func (c *countingBody) Read(p []byte) (int, error) {
	n, err := c.ReadCloser.Read(p)
	c.n += int64(n)
	return n, err
}

func (c *countingBody) Close() error {
	c.done(c.n)
	return c.ReadCloser.Close()
}

func (t *tap) handler(target *url.URL) http.Handler {
	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			pr.Out.Host = pr.In.Host // SigV4 signed the host the browser used
		},
		ModifyResponse: func(resp *http.Response) error {
			e := tapEntry{At: time.Now().UTC().Format(time.RFC3339Nano), Method: resp.Request.Method,
				Key: resp.Request.URL.Path, Range: resp.Request.Header.Get("Range"), Status: resp.StatusCode}
			resp.Body = &countingBody{ReadCloser: resp.Body, done: func(n int64) {
				e.Bytes = n
				t.mu.Lock()
				t.entries = append(t.entries, e)
				t.mu.Unlock()
			}}
			return nil
		},
	}
	return rp
}

// ---- main -------------------------------------------------------------------

func main() {
	uiDir := flag.String("ui", "", "the lakeui directory to serve (default: ../lakeui next to query/)")
	replanMargin := flag.Int("replan-margin-s", 20, "the plan's replan margin (s)")
	urlTTL := flag.Int("url-ttl-s", 60, "presigned URL lifetime (s; the service clamps to 60-900)")
	flag.Parse()
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	log.SetPrefix("lakeuirig ")

	bin := os.Getenv("QS_IT_BIN")
	if bin == "" {
		log.Fatal("QS_IT_BIN (a directory with otelcol-s3pq and consume) is not set")
	}
	wd, _ := os.Getwd()
	if *uiDir == "" {
		*uiDir = filepath.Join(wd, "..", "lakeui")
	}
	edgeCfg, err := os.ReadFile(filepath.Join(wd, "..", "conformance", "go-edge.yaml"))
	must(err)
	if _, err := os.Stat(filepath.Join(*uiDir, "index.html")); err != nil {
		log.Fatalf("no lake UI at %s: %v", *uiDir, err)
	}
	id := fmt.Sprintf("%d%04x", time.Now().Unix(), mrand.Uint32()&0xffff)
	work, err := os.MkdirTemp("", "lakeuirig-")
	must(err)
	must(os.WriteFile(filepath.Join(work, "go-edge.yaml"), edgeCfg, 0o644))
	r := &rig{ch: env("QS_IT_CH", "http://127.0.0.1:18123"), s3url: env("QS_IT_S3", "http://127.0.0.1:18333"),
		bucket: env("LUI_PREFIX", "lui") + "-" + id, run: "edges", db: env("LUI_PREFIX", "lui") + "_" + id, bin: bin, work: work}
	r.s3 = s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String(r.s3url), UsePathStyle: true,
		Credentials: credentials.NewStaticCredentialsProvider("otel", "otelsecret", "")})
	ctx := context.Background()
	r.sql("SELECT 1")

	pagePort := freePort()
	pageOrigin := fmt.Sprintf("http://127.0.0.1:%d", pagePort)
	ready := false
	cleanup := func() {
		if os.Getenv("LUI_KEEP") != "" {
			log.Printf("kept: bucket %s, database %s, work %s", r.bucket, r.db, work)
			return
		}
		r.cleanup(ctx)
		_ = os.RemoveAll(work)
	}
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		if !ready {
			log.Print("interrupted during setup")
		}
		cleanup()
		os.Exit(0)
	}()

	// 1. our own bucket, with CORS for the page
	_, err = r.s3.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &r.bucket})
	must(err)
	_, err = r.s3.PutBucketCors(ctx, &s3.PutBucketCorsInput{Bucket: &r.bucket, CORSConfiguration: &s3types.CORSConfiguration{
		CORSRules: []s3types.CORSRule{{AllowedOrigins: []string{pageOrigin}, AllowedMethods: []string{"GET", "HEAD"},
			AllowedHeaders: []string{"Range"}, ExposeHeaders: []string{"Content-Range", "Content-Length", "ETag", "Accept-Ranges"}, MaxAgeSeconds: aws.Int32(600)}}}})
	must(err)

	// 2. two clusters' edges publish three batches
	truth := &Truth{TraceIDs: map[string][]string{}, RareTraceIDs: map[string][]string{}, Metric: "lui.queue.depth", Needle: Needle}
	at := time.Now().Add(-20 * time.Minute).Truncate(time.Second)
	truth.At = at.UTC().Format(time.RFC3339Nano)
	res := map[string][]resource{
		"lui-a": {{"lui-a", "shop", "cart-1"}, {"lui-a", "pay", "pay-1"}},
		"lui-b": {{"lui-b", "shop", "cart-9"}},
	}
	edges := map[string]*edge{}
	for _, cl := range []string{"lui-a", "lui-b"} {
		edges[cl] = r.startEdge(cl)
		var ids []string
		for i := 0; i < 6; i++ {
			ids = append(ids, fmt.Sprintf("%016x%016x", mrand.Uint64(), mrand.Uint64()))
		}
		truth.TraceIDs[cl] = ids
	}
	for b := 0; b < 3; b++ {
		for ci, cl := range []string{"lui-a", "lui-b"} {
			logs := 3000
			if cl == "lui-b" {
				logs = 2000
			}
			rare := fmt.Sprintf("%016x%016x", mrand.Uint64(), mrand.Uint64())
			truth.RareTraceIDs[cl] = append(truth.RareTraceIDs[cl], rare)
			needle := ""
			if cl == "lui-a" {
				needle = fmt.Sprintf("rare-%d-%08x", b, mrand.Uint32())
				truth.RareNeedles = append(truth.RareNeedles, needle)
			}
			sendBatch(edges[cl], res[cl], batchSpec{from: at.Add(time.Duration(b) * 5 * time.Minute), span: 5 * time.Minute,
				logs: logs, spans: 400, gauge: true, traceIDs: truth.TraceIDs[cl], seed: uint64(b*10 + ci),
				rareTrace: rare, rareNeedle: needle}, truth)
		}
	}
	time.Sleep(3 * time.Second) // a heartbeat per lane after the data

	// 3. the consumer ingests, then publishes complete_through
	r.consume("run", "--ch", r.ch, "--db", r.db, "--exit-after-idle", "8s", "--poll", "300ms", "--full-list", "1s")
	r.consume("watermark", "--wm-skew", "1s")
	log.Printf("central: %s logs, %s spans, %s gauge points", r.sql("SELECT count() FROM "+r.db+".otel_logs"),
		r.sql("SELECT count() FROM "+r.db+".otel_traces"), r.sql("SELECT count() FROM "+r.db+".otel_metrics_gauge"))

	// 3b. the lake indexer (D27) covers everything so far; the late batch
	// below stays unindexed (planned "scan") until /rig/index runs a pass
	idxStore, err := store.NewS3(ctx, store.S3Config{Endpoint: r.s3url, Bucket: r.bucket, AccessKey: "otel", SecretKey: "otelsecret", PathStyle: true})
	must(err)
	indexer := lakeidx.New(lakeidx.Config{Root: r.run}, idxStore)
	indexPass := func() (any, error) {
		reps, err := indexer.RunOnce(ctx)
		n := 0
		for _, rp := range reps {
			n += rp.Indexed
		}
		return map[string]any{"indexed": n, "segments": indexer.Stats.Segments, "segment_bytes": indexer.Stats.SegmentBytes,
			"source_bytes": indexer.Stats.BytesRead, "rows": indexer.Stats.Rows, "build_ms": indexer.Stats.BuildNs / 1e6}, err
	}
	idxRep, err := indexPass()
	must(err)
	log.Printf("indexed: %v", idxRep)

	// 4. a late batch for lui-a, after the watermark: past complete_through
	time.Sleep(1500 * time.Millisecond)
	late := time.Now().Truncate(time.Millisecond)
	truth.LateFrom, truth.LateCluster, truth.LateLogs = late.UTC().Format(time.RFC3339Nano), "lui-a", 2*300
	sendBatch(edges["lui-a"], res["lui-a"], batchSpec{from: late, span: 20 * time.Second, logs: 300, seed: 99}, truth)
	truth.LateTo = late.Add(20 * time.Second).UTC().Format(time.RFC3339Nano)
	time.Sleep(2500 * time.Millisecond)
	for _, e := range edges {
		e.stop()
	}

	// 5. the counting tap, the IdP front, the query service
	tp := &tap{}
	s3u, _ := url.Parse(r.s3url)
	tapSrv := httptest.NewServer(tp.handler(s3u))
	defer tapSrv.Close()
	is := authtest.New()
	defer is.Close()
	d := &idp{is: is, origins: []string{pageOrigin}, clientID: "lakeui", tokenTTL: 30 * time.Minute, codes: map[string]codeGrant{},
		users: map[string]jwt.MapClaims{
			"alice": {"clusters": "lui-a", "namespaces": "*", "roles": "query plan"},
			"sre":   {"groups": "sre"},
			"shop":  {"clusters": "lui-a", "namespaces": "shop", "roles": "query plan"},
		}}
	is.Server.Config.Handler = d.wrap(is.Server.Config.Handler)

	sink, err := audit.OpenFile(filepath.Join(work, "audit.jsonl"), false)
	must(err)
	cfg := &app.Config{LakeEnabled: true, CORSOrigins: []string{pageOrigin}}
	cfg.OIDC = auth.OIDCConfig{Issuer: is.URL, Audience: "otel-query"}
	cfg.Claims = auth.Mapping{ClustersClaim: "clusters", NamespacesClaim: "namespaces", RolesClaim: "roles", GroupsClaim: "groups",
		Groups: map[string]auth.Grant{"sre": {Clusters: []string{"*"}, Namespaces: []string{"*"}, Roles: []string{"query", "plan"}}}}
	cfg.Central.Config = central.Config{URL: r.ch, Database: r.db}
	cfg.Central.Tables = []*sqlscope.Table{
		{Name: "otel_logs", TimeColumn: "Timestamp", Scope: "columns", Cluster: "`__hdx_materialized_k8s.cluster.name`", Namespace: "`__hdx_materialized_k8s.namespace.name`"},
	}
	cfg.Limits.Default = central.DefaultLimits
	cfg.S3.Endpoint, cfg.S3.PublicEndpoint, cfg.S3.Bucket, cfg.S3.AccessKey, cfg.S3.SecretKey = r.s3url, tapSrv.URL, r.bucket, "otel", "otelsecret"
	cfg.Lake.Root = r.run
	cfg.Lake.URLTTLS = *urlTTL
	cfg.Lake.ReplanMarginS = *replanMargin
	cfg.Watermark.CacheS, cfg.Watermark.MaxAgeS = 1, 900
	v, err := auth.NewVerifier(cfg.OIDC, nil)
	must(err)
	srv, err := app.Build(ctx, cfg, v, sink)
	must(err)
	qs := httptest.NewServer(srv.Handler())
	defer qs.Close()

	// 6. the page and the rig's control API
	mux := http.NewServeMux()
	mux.HandleFunc("/config.json", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"query_url": qs.URL, "issuer": is.URL, "client_id": "lakeui",
			"clusters": []string{"lui-a", "lui-b"}, "metric": truth.Metric})
	})
	info := map[string]any{"page": pageOrigin + "/", "query_url": qs.URL, "issuer": is.URL, "tap": tapSrv.URL,
		"ch": r.ch, "db": r.db, "bucket": r.bucket, "root": r.run, "truth": truth, "url_ttl_s": *urlTTL, "replan_margin_s": *replanMargin,
		"index": idxRep}
	mux.HandleFunc("/rig/info", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(info)
	})
	mux.HandleFunc("/rig/tap", func(w http.ResponseWriter, req *http.Request) {
		tp.mu.Lock()
		defer tp.mu.Unlock()
		if req.Method == http.MethodPost {
			tp.entries = nil
			w.WriteHeader(204)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"entries": tp.entries})
	})
	mux.HandleFunc("/rig/index", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "POST runs one indexer pass", 405)
			return
		}
		rp, err := indexPass()
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(rp)
	})
	wmKey := r.run + "/_consumer/watermark.json"
	mux.HandleFunc("/rig/watermark", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "POST ?op=drop|restore", 405)
			return
		}
		var err error
		switch req.URL.Query().Get("op") {
		case "drop":
			_, err = r.s3.CopyObject(ctx, &s3.CopyObjectInput{Bucket: &r.bucket, Key: aws.String(wmKey + ".lui-saved"), CopySource: aws.String(r.bucket + "/" + wmKey)})
			if err == nil {
				_, err = r.s3.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &r.bucket, Key: &wmKey})
			}
		case "restore":
			_, err = r.s3.CopyObject(ctx, &s3.CopyObjectInput{Bucket: &r.bucket, Key: &wmKey, CopySource: aws.String(r.bucket + "/" + wmKey + ".lui-saved")})
		default:
			err = errors.New("op must be drop or restore")
		}
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		time.Sleep(1100 * time.Millisecond) // the service caches the document for 1 s
		w.WriteHeader(204)
	})
	mux.Handle("/", noStore(http.FileServer(http.Dir(*uiDir))))
	ps := &http.Server{Addr: fmt.Sprintf("127.0.0.1:%d", pagePort), Handler: mux}
	go func() {
		if err := ps.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()
	b, _ := json.Marshal(info)
	ready = true
	fmt.Printf("LAKEUI_RIG_READY %s\n", b)
	select {}
}

func noStore(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		h.ServeHTTP(w, r)
	})
}

func (r *rig) cleanup(ctx context.Context) {
	req, _ := http.NewRequest(http.MethodPost, r.ch, strings.NewReader("DROP DATABASE IF EXISTS "+r.db+" SYNC"))
	if resp, err := http.DefaultClient.Do(req); err == nil {
		resp.Body.Close()
	}
	p := s3.NewListObjectsV2Paginator(r.s3, &s3.ListObjectsV2Input{Bucket: &r.bucket})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			log.Printf("cleanup list: %v", err)
			break
		}
		var ids []s3types.ObjectIdentifier
		for _, o := range page.Contents {
			ids = append(ids, s3types.ObjectIdentifier{Key: o.Key})
		}
		if len(ids) > 0 {
			if _, err := r.s3.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: &r.bucket, Delete: &s3types.Delete{Objects: ids}}); err != nil {
				log.Printf("cleanup delete: %v", err)
			}
		}
	}
	if _, err := r.s3.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: &r.bucket}); err != nil {
		log.Printf("cleanup bucket %s: %v", r.bucket, err)
	}
	log.Printf("cleaned up bucket %s and database %s", r.bucket, r.db)
}
