// Command ingress-e2e is a TEST HARNESS, never a deployment: the real
// ingress handler (../../server.go) behind a fake Entra issuer
// (../../entratest) and an in-memory object store, with control endpoints
// under /_e2e/ for the device forwarder's end-to-end and stress tests
// (../../../forwarder, research/entra-ingress.md §10a). It mints tokens for
// anyone who asks, so it binds to loopback only.
//
//	GET  /_e2e/token?oid=<guid>&role=Team.Payments&lifetime_s=N   a v2.0 access token (JSON)
//	GET  /_e2e/broker/token?client=C&expected=<oid>&force=0|1      the broker's answer on device C (below)
//	POST /_e2e/broker?oid=&role=&lifetime_s=&lie_s=&mode=          sign in as another person, shorten tokens, lie about
//	     their expiry by lie_s (a token that expires mid-flight), or fail (mode=interaction_required|unavailable)
//	GET  /_e2e/sample?format=proto|json&n=K&seed=S                an OTLP traces body that claims another tenant
//	GET  /_e2e/commits                                            every committed data object (JSON)
//	GET  /_e2e/counts                                             the ingress's outcome counters
//	POST /_e2e/fault?kind=K&n=N[&code=C&retry_after=S&delay_ms=D] inject faults into the next N requests:
//	     lose_answer  the request is handled (and commits), then the connection is closed unanswered
//	     status       answer C (default 503) without handling the request (with Retry-After S if given)
//	     delay        sleep D ms before handling (a slow ingress)
//	     store_drop   the store loses the next N PUTs and HEADs (the edge answers 503: unresolved)
//	POST /_e2e/fault?kind=clear                                   drop every pending fault
//	GET  /_e2e/answers?since=N                                    every answer given after sequence N (below)
//
// Every request to the ingress gets a sequence number. Its answer carries
// X-E2E-Seq and, when the body was read to the end before the answer,
// X-E2E-Body-SHA256 (the request body's hash as the ingress received it); the
// answers log keeps, per sequence number, the status, Retry-After, the hash of
// the answer's body, the request body's hash and the request's header names, or
// that the answer was lost. A pass-through in front of the ingress can then be
// checked answer by answer: what the sender got is what the ingress gave.
//
// The first line on stdout is "listening <addr>".
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"hash"
	"io"
	"log"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/parquet-go/parquet-go"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/casselc/observability/otel-chdb/ingress"
	"github.com/casselc/observability/otel-chdb/ingress/entratest"
	"github.com/casselc/observability/otel-chdb/parquetgo/commit"
	"github.com/casselc/observability/otel-chdb/parquetgo/edge"
)

// The identities the harness knows: one organisation, the ingress API, the
// forwarder's public client, and the team roles the policy maps.
const (
	Tenant    = "11111111-1111-4111-8111-111111111111"
	APIID     = "33333333-3333-4333-8333-333333333333"
	Forwarder = "44444444-4444-4444-8444-444444444444"
)

type faults struct {
	mu         sync.Mutex
	loseAnswer int
	status     int
	statusCode int
	retryAfter int
	delay      int
	delayMS    int
}

// take reports which fault applies to this request, consuming it.
func (f *faults) take() (lose bool, code, retryAfter, delayMS int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.delay > 0 {
		f.delay--
		delayMS = f.delayMS
	}
	if f.status > 0 {
		f.status--
		return false, f.statusCode, f.retryAfter, delayMS
	}
	if f.loseAnswer > 0 {
		f.loseAnswer--
		return true, 0, 0, delayMS
	}
	return false, 0, 0, delayMS
}

// broker models the platform broker on a device: one signed-in account, a cached
// access token per account until it expires (or a forced refresh), and an account
// that is gone once another signs in.
type broker struct {
	mu       sync.Mutex
	oid      string
	role     string
	lifetime time.Duration
	lie      time.Duration
	mode     string
	tok      map[string]string // per device (?client=): each forwarder has its own broker cache
	exp      map[string]time.Time
	hits     int
}

// answer is one entry of the answers log.
type answer struct {
	Seq        int64    `json:"seq"`
	Status     int      `json:"status"`
	RetryAfter string   `json:"retry_after"`
	RespSHA256 string   `json:"resp_sha256"`
	ReqSHA256  string   `json:"req_sha256"` // empty when the body was not read to the end
	ReqBytes   int64    `json:"req_bytes"`
	Headers    []string `json:"headers"` // the request's header names, lower case, sorted
	Lost       bool     `json:"lost"`
}

type answers struct {
	mu   sync.Mutex
	seq  int64
	log  []answer
	keep int
}

func (a *answers) next() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.seq++
	return a.seq
}

func (a *answers) add(x answer) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.log) >= a.keep {
		a.log = a.log[len(a.log)-a.keep/2:]
	}
	a.log = append(a.log, x)
}

func (a *answers) since(n int64) []answer {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := []answer{}
	for _, x := range a.log {
		if x.Seq > n {
			out = append(out, x)
		}
	}
	return out
}

// hashReader hashes the request body as the handler reads it.
type hashReader struct {
	r   io.ReadCloser
	h   hash.Hash
	n   int64
	eof bool
}

func (h *hashReader) Read(p []byte) (int, error) {
	n, err := h.r.Read(p)
	h.h.Write(p[:n])
	h.n += int64(n)
	if err == io.EOF {
		h.eof = true
	}
	return n, err
}

func (h *hashReader) Close() error { return h.r.Close() }

// recWriter stamps X-E2E-Seq (and the body hash, if read to the end) on the
// answer and hashes the answer's body.
type recWriter struct {
	http.ResponseWriter
	seq    int64
	body   *hashReader
	h      hash.Hash
	status int
}

func (w *recWriter) WriteHeader(code int) {
	if w.status != 0 {
		return
	}
	w.status = code
	w.Header().Set("X-E2E-Seq", strconv.FormatInt(w.seq, 10))
	if w.body.eof {
		w.Header().Set("X-E2E-Body-SHA256", hex.EncodeToString(w.body.h.Sum(nil)))
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *recWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	w.h.Write(b)
	return w.ResponseWriter.Write(b)
}

func (w *recWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *recWriter) record(r *http.Request, lost bool) answer {
	a := answer{Seq: w.seq, Status: w.status, RetryAfter: w.Header().Get("Retry-After"), Lost: lost, ReqBytes: w.body.n}
	if a.Status == 0 && !lost {
		a.Status = http.StatusOK
	}
	if !lost {
		a.RespSHA256 = hex.EncodeToString(w.h.Sum(nil))
	}
	if w.body.eof {
		a.ReqSHA256 = hex.EncodeToString(w.body.h.Sum(nil))
	}
	for k := range r.Header {
		a.Headers = append(a.Headers, strings.ToLower(k))
	}
	if r.Host != "" {
		a.Headers = append(a.Headers, "host")
	}
	if r.ContentLength >= 0 && r.Header.Get("Content-Length") == "" && len(r.TransferEncoding) == 0 {
		a.Headers = append(a.Headers, "content-length")
	}
	for range r.TransferEncoding {
		a.Headers = append(a.Headers, "transfer-encoding")
	}
	slices.Sort(a.Headers)
	a.Headers = slices.Compact(a.Headers)
	return a
}

type commitJSON struct {
	Key        string              `json:"key"`
	Signal     string              `json:"signal"`
	ContentKey string              `json:"content_key"`
	Resources  []map[string]string `json:"resources"`
}

func main() {
	listen := flag.String("listen", "127.0.0.1:0", "address (loopback only)")
	leeway := flag.Int("leeway_s", 1, "token clock leeway (s); small, so short-lived tokens expire in tests")
	rpm := flag.Float64("requests_per_minute", 120, "per-principal request rate")
	bpm := flag.Float64("decoded_bytes_per_minute", 0, "per-principal decoded-byte rate (0: the ingress's default)")
	flag.Parse()
	host, _, err := net.SplitHostPort(*listen)
	if err != nil || (host != "127.0.0.1" && host != "::1" && host != "localhost") {
		log.Fatalf("ingress-e2e: %q: loopback only (this harness mints tokens for anyone)", *listen)
	}
	is := entratest.New()
	defer is.Close()
	st := commit.NewMemStore()
	var n atomic.Int64
	e, err := edge.New(edge.Config{Store: st, Prefix: "root", Cluster: "devtools", ProducerID: "ingress-e2e",
		PutTimeout: time.Second, HeadTimeout: 200 * time.Millisecond,
		NewEpoch: func() string { return fmt.Sprintf("20260929T000000.000Z-%08x", n.Add(1)) }})
	if err != nil {
		log.Fatal(err)
	}
	v, err := ingress.NewEntraVerifier(ingress.EntraConfig{Authority: is.URL, Tenants: []string{Tenant},
		Audiences: []string{APIID}, ClientApps: []string{Forwarder}, UserScope: "Telemetry.Write", LeewayS: *leeway}, nil)
	if err != nil {
		log.Fatal(err)
	}
	pol := &ingress.Policy{Cluster: "devtools", NamespacePrefix: "dev-", Rules: []ingress.Rule{
		{Tenant: Tenant, Role: "Team.Payments", Namespace: "dev-payments"},
		{Tenant: Tenant, Role: "Team.Search", Namespace: "dev-search"},
	}}
	lim := ingress.DefaultLimits()
	lim.RequestsPerMinute = *rpm
	if *bpm > 0 {
		lim.DecodedBytesPerMinute = *bpm
	}
	srv, err := ingress.NewServer(v, pol, lim, e, nil)
	if err != nil {
		log.Fatal(err)
	}
	srv.Logf = log.Printf
	f := &faults{}
	ans := &answers{keep: 400_000}
	br := &broker{oid: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", role: "Team.Payments", lifetime: 75 * time.Minute, mode: "ok",
		tok: map[string]string{}, exp: map[string]time.Time{}}
	h := srv.Handler()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /_e2e/broker/token", func(w http.ResponseWriter, r *http.Request) {
		br.mu.Lock()
		defer br.mu.Unlock()
		br.hits++
		q := r.URL.Query()
		if exp := q.Get("expected"); exp != "" && exp != br.oid {
			writeJSON(w, map[string]any{"status": "account_gone"})
			return
		}
		if br.mode != "ok" {
			writeJSON(w, map[string]any{"status": br.mode})
			return
		}
		now := time.Now()
		c := q.Get("client")
		if br.tok[c] == "" || q.Get("force") == "1" || !now.Before(br.exp[c].Add(br.lie)) {
			br.tok[c] = is.Mint(entratest.Token{Tenant: Tenant, OID: br.oid, Audience: APIID, ClientApp: Forwarder,
				Scope: "Telemetry.Write", Roles: []string{br.role}, Lifetime: br.lifetime})
			br.exp[c] = now.Add(br.lifetime)
		}
		writeJSON(w, map[string]any{"status": "ok", "access_token": br.tok[c], "tenant_id": Tenant, "object_id": br.oid,
			"expires_on": br.exp[c].Add(br.lie).Unix()})
	})
	mux.HandleFunc("POST /_e2e/broker", func(w http.ResponseWriter, r *http.Request) {
		br.mu.Lock()
		defer br.mu.Unlock()
		q := r.URL.Query()
		if v := q.Get("oid"); v != "" {
			if !ingress.IsGUID(v) {
				http.Error(w, "oid must be a lower-case GUID", http.StatusBadRequest)
				return
			}
			br.oid = v
			clear(br.tok)
		}
		if v := q.Get("role"); v != "" {
			br.role = v
			clear(br.tok)
		}
		if v, err := strconv.Atoi(q.Get("lifetime_s")); err == nil && v > 0 {
			br.lifetime = time.Duration(v) * time.Second
			clear(br.tok)
		}
		if v, err := strconv.Atoi(q.Get("lie_s")); err == nil && v >= 0 {
			br.lie = time.Duration(v) * time.Second
		}
		if v := q.Get("mode"); v != "" {
			br.mode = v
		}
		writeJSON(w, map[string]any{"oid": br.oid, "role": br.role, "lifetime_s": int(br.lifetime.Seconds()),
			"lie_s": int(br.lie.Seconds()), "mode": br.mode, "hits": br.hits})
	})
	mux.HandleFunc("GET /_e2e/token", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		oid := q.Get("oid")
		if !ingress.IsGUID(oid) {
			http.Error(w, "oid must be a lower-case GUID", http.StatusBadRequest)
			return
		}
		life := 75 * time.Minute
		if s, err := strconv.Atoi(q.Get("lifetime_s")); err == nil && s > 0 {
			life = time.Duration(s) * time.Second
		}
		roles := q["role"]
		if len(roles) == 0 {
			roles = []string{"Team.Payments"}
		}
		tok := is.Mint(entratest.Token{Tenant: Tenant, OID: oid, Audience: APIID, ClientApp: Forwarder,
			Scope: "Telemetry.Write", Roles: roles, Lifetime: life})
		writeJSON(w, map[string]any{"access_token": tok, "tenant_id": Tenant, "object_id": oid,
			"expires_on": time.Now().Add(life).Unix()})
	})
	mux.HandleFunc("GET /_e2e/sample", func(w http.ResponseWriter, r *http.Request) {
		k, _ := strconv.Atoi(r.URL.Query().Get("n"))
		seed, _ := strconv.Atoi(r.URL.Query().Get("seed"))
		td := sample(k, seed)
		if r.URL.Query().Get("format") == "json" {
			b, _ := (&ptrace.JSONMarshaler{}).MarshalTraces(td)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(b)
			return
		}
		b, _ := (&ptrace.ProtoMarshaler{}).MarshalTraces(td)
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(b)
	})
	mux.HandleFunc("GET /_e2e/commits", func(w http.ResponseWriter, r *http.Request) {
		out := []commitJSON{}
		for _, k := range st.Keys("root/devtools/") {
			o, _ := st.Get(k)
			if (o.Meta[commit.MetaKind] != "" && o.Meta[commit.MetaKind] != "data") || len(o.Body) == 0 {
				continue
			}
			signal := ""
			for _, s := range []string{"traces", "logs"} {
				if strings.Contains(k, "/"+s+"/") {
					signal = s
				}
			}
			if signal == "" {
				continue
			}
			rows, err := parquet.Read[struct {
				Res map[string]string `parquet:"ResourceAttributes"`
			}](bytes.NewReader(o.Body), int64(len(o.Body)))
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			c := commitJSON{Key: k, Signal: signal, ContentKey: o.Meta[commit.MetaContent]}
			for _, row := range rows {
				c.Resources = append(c.Resources, row.Res)
			}
			out = append(out, c)
		}
		writeJSON(w, out)
	})
	mux.HandleFunc("GET /_e2e/counts", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, srv.Counts()) })
	mux.HandleFunc("GET /_e2e/answers", func(w http.ResponseWriter, r *http.Request) {
		n, _ := strconv.ParseInt(r.URL.Query().Get("since"), 10, 64)
		writeJSON(w, ans.since(n))
	})
	mux.HandleFunc("POST /_e2e/fault", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		cnt, _ := strconv.Atoi(q.Get("n"))
		f.mu.Lock()
		defer f.mu.Unlock()
		switch q.Get("kind") {
		case "lose_answer":
			f.loseAnswer += cnt
		case "status":
			f.status += cnt
			f.statusCode, _ = strconv.Atoi(q.Get("code"))
			if f.statusCode == 0 {
				f.statusCode = http.StatusServiceUnavailable
			}
			f.retryAfter, _ = strconv.Atoi(q.Get("retry_after"))
		case "delay":
			f.delay += cnt
			f.delayMS, _ = strconv.Atoi(q.Get("delay_ms"))
		case "store_drop":
			for range cnt {
				st.Inject(commit.Drop)
				st.Inject(commit.HeadFail)
			}
		case "clear":
			f.loseAnswer, f.status, f.delay = 0, 0, 0
			st.ClearFaults()
		default:
			http.Error(w, "unknown kind", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.Handle("/", http.HandlerFunc(func(w0 http.ResponseWriter, r *http.Request) {
		body := &hashReader{r: r.Body, h: sha256.New()}
		r.Body = body
		w := &recWriter{ResponseWriter: w0, seq: ans.next(), body: body, h: sha256.New()}
		lose, code, ra, delay := f.take()
		if delay > 0 {
			time.Sleep(time.Duration(delay) * time.Millisecond)
		}
		if code != 0 {
			// Read the body first, so the answer can say what arrived (a real ingress
			// refuses some answers before the body; the pass-through check does not care).
			_, _ = io.Copy(io.Discard, r.Body)
			if ra > 0 {
				w.Header().Set("Retry-After", strconv.Itoa(ra))
			}
			http.Error(w, "injected", code)
			ans.add(w.record(r, false))
			return
		}
		if !lose {
			h.ServeHTTP(w, r)
			ans.add(w.record(r, false))
			return
		}
		// Handle the request for real (it commits), then lose the answer:
		// the sender cannot tell this from a request that never arrived.
		rec := &discard{header: http.Header{}}
		h.ServeHTTP(rec, r)
		log.Printf("ingress-e2e: lost the answer %d to %s", rec.code, r.URL.Path)
		ans.add(w.record(r, true))
		if hj, ok := w0.(http.Hijacker); ok {
			if c, _, err := hj.Hijack(); err == nil {
				_ = c.Close()
				return
			}
		}
		panic(http.ErrAbortHandler)
	}))
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("listening %s\n", ln.Addr())
	hs := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	log.Fatal(hs.Serve(ln))
}

// discard is a ResponseWriter whose answer nobody receives.
type discard struct {
	header http.Header
	code   int
}

func (d *discard) Header() http.Header { return d.header }
func (d *discard) Write(b []byte) (int, error) {
	if d.code == 0 {
		d.code = http.StatusOK
	}
	return len(b), nil
}
func (d *discard) WriteHeader(c int) { d.code = c }

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// sample is what a Langfuse integration sends, with its own claims about
// who and where (which the ingress must replace), k+1 spans; seed makes the
// bytes distinct.
func sample(k, seed int) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	ra := rs.Resource().Attributes()
	ra.PutStr("service.name", "claude-code")
	ra.PutStr("k8s.namespace.name", "prod-billing")
	ra.PutStr("user.id", "mallory@example.com")
	ss := rs.ScopeSpans().AppendEmpty().Spans()
	now := time.Now()
	for i := 0; i <= k; i++ {
		s := ss.AppendEmpty()
		s.SetName("generation")
		s.SetTraceID(pcommon.TraceID{1, 2, 3, byte(i), byte(i >> 8), byte(seed), byte(seed >> 8), byte(seed >> 16), byte(seed >> 24)})
		s.SetSpanID(pcommon.SpanID{4, 5, 6, byte(i), byte(i >> 8)})
		s.Attributes().PutStr("gen_ai.prompt", strings.Repeat("p", 64))
		s.SetStartTimestamp(pcommon.NewTimestampFromTime(now.Add(-time.Second)))
		s.SetEndTimestamp(pcommon.NewTimestampFromTime(now))
	}
	return td
}
