package ingress

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/casselc/observability/otel-chdb/parquetgo/edge"
)

// Publisher is the edge: *edge.Edge. It returns nil only when every object
// of the request has committed (commit.RequestVerdict); an error that is
// not edge.IsPermanent is retryable and may still commit (the sender's
// retry of identical bytes is then a copy the consumer skips).
type Publisher interface {
	PushTraces(ctx context.Context, td ptrace.Traces) error
	PushLogs(ctx context.Context, ld plog.Logs) error
}

// NamespaceHeader names one of the caller's granted namespaces when it has
// several. It chooses among grants; it never grants.
const NamespaceHeader = "X-Oscope-Namespace"

// Server is the ingress's HTTP handler.
type Server struct {
	Verifier  *EntraVerifier
	Policy    *Policy
	Limiter   *Limiter
	Limits    Limits
	Publisher Publisher
	// Logf receives the reason for every refusal (never sent to the client
	// in detail) and every publish error.
	Logf func(format string, a ...any)

	mu     sync.Mutex
	counts map[string]int64
	gate   gate
}

// NewServer validates the parts together.
func NewServer(v *EntraVerifier, p *Policy, l Limits, pub Publisher, now func() time.Time) (*Server, error) {
	if v == nil || p == nil || pub == nil {
		return nil, errors.New("ingress: verifier, policy and publisher are required")
	}
	if err := p.Validate(); err != nil {
		return nil, err
	}
	if err := l.Validate(); err != nil {
		return nil, err
	}
	return &Server{Verifier: v, Policy: p, Limits: l, Limiter: NewLimiter(l, now), Publisher: pub,
		Logf: func(string, ...any) {}, counts: map[string]int64{}}, nil
}

// Counts are the outcome counters ("ok", "unauthenticated", "forbidden",
// "rate_limited", "too_large", "bad_request", "unavailable", …).
func (s *Server) Counts() map[string]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int64, len(s.counts))
	for k, v := range s.counts {
		out[k] = v
	}
	return out
}

func (s *Server) count(k string) {
	s.mu.Lock()
	s.counts[k]++
	s.mu.Unlock()
}

// Handler routes the OTLP/HTTP paths and Langfuse's alias for them
// (research/langfuse.md §6.1: its SDKs post to {base}/api/public/otel/v1/traces).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	for _, p := range []string{"/v1/traces", "/api/public/otel/v1/traces"} {
		mux.HandleFunc("POST "+p, func(w http.ResponseWriter, r *http.Request) { s.serve(w, r, "traces") })
	}
	for _, p := range []string{"/v1/logs", "/api/public/otel/v1/logs"} {
		mux.HandleFunc("POST "+p, func(w http.ResponseWriter, r *http.Request) { s.serve(w, r, "logs") })
	}
	return mux
}

func (s *Server) refuse(w http.ResponseWriter, code int, outcome, public string, detail error, retryAfter time.Duration) {
	s.count(outcome)
	s.Logf("ingress %s: %v", outcome, detail)
	if code == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", `Bearer error="invalid_token"`)
	}
	if retryAfter > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(int(retryAfter.Seconds())+1))
	}
	http.Error(w, public, code)
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request, signal string) {
	if !s.gate.enter() {
		s.refuse(w, http.StatusServiceUnavailable, "draining", "draining; retry", ErrDraining, time.Second)
		return
	}
	defer s.gate.leave()
	// 1. Identity, before a byte of the body is read.
	raw, ok := Bearer(r.Header.Get("Authorization"))
	if !ok {
		s.refuse(w, http.StatusUnauthorized, "unauthenticated", "a bearer token from Entra is required (project keys are not accepted here; use the device forwarder)", errors.New("no bearer token"), 0)
		return
	}
	id, err := s.Verifier.Verify(r.Context(), raw)
	if err != nil {
		s.refuse(w, http.StatusUnauthorized, "unauthenticated", "invalid token", err, 0)
		return
	}
	// 2. Tenant, from policy only.
	ns, err := s.Policy.Resolve(id, r.Header.Get(NamespaceHeader))
	if err != nil {
		s.refuse(w, http.StatusForbidden, "forbidden", err.Error(), err, 0)
		return
	}
	principal := id.TenantID + "/" + id.ObjectID
	if wait := s.Limiter.Admit(principal); wait > 0 {
		s.refuse(w, http.StatusTooManyRequests, "rate_limited", "rate limited", fmt.Errorf("%s: requests", principal), wait)
		return
	}
	// 3. The body, bounded before and after decompression.
	mt, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if mt != "application/x-protobuf" && mt != "application/json" {
		s.refuse(w, http.StatusUnsupportedMediaType, "bad_request", "content type must be application/x-protobuf or application/json", fmt.Errorf("content type %q", mt), 0)
		return
	}
	if r.ContentLength > s.Limits.MaxBodyBytes {
		s.refuse(w, http.StatusRequestEntityTooLarge, "too_large", "request too large", fmt.Errorf("content-length %d", r.ContentLength), 0)
		return
	}
	var body io.Reader = http.MaxBytesReader(w, r.Body, s.Limits.MaxBodyBytes)
	switch r.Header.Get("Content-Encoding") {
	case "", "identity":
	case "gzip":
		zr, err := gzip.NewReader(body)
		if err != nil {
			s.refuse(w, http.StatusBadRequest, "bad_request", "bad gzip", err, 0)
			return
		}
		body = zr
	default:
		s.refuse(w, http.StatusUnsupportedMediaType, "bad_request", "unsupported content encoding", errors.New(r.Header.Get("Content-Encoding")), 0)
		return
	}
	b, err := io.ReadAll(io.LimitReader(body, s.Limits.MaxDecodedBytes+1))
	var mbe *http.MaxBytesError
	if errors.As(err, &mbe) || int64(len(b)) > s.Limits.MaxDecodedBytes {
		s.refuse(w, http.StatusRequestEntityTooLarge, "too_large", "request too large", fmt.Errorf("body over the cap (%d decoded bytes read)", len(b)), 0)
		return
	}
	if err != nil {
		s.refuse(w, http.StatusBadRequest, "bad_request", "unreadable body", err, 0)
		return
	}
	if wait := s.Limiter.Charge(principal, int64(len(b))); wait > 0 {
		s.refuse(w, http.StatusTooManyRequests, "rate_limited", "rate limited", fmt.Errorf("%s: %d bytes", principal, len(b)), wait)
		return
	}
	a := Attribution{Cluster: s.Policy.Cluster, Namespace: ns, UserID: id.ObjectID, EntraTenant: id.TenantID,
		Type: id.Type, ClientApp: id.ClientApp}
	// 4. Decode, stamp, publish. 200 only after the edge's commit.
	var perr error
	switch signal {
	case "traces":
		var td ptrace.Traces
		if mt == "application/json" {
			td, err = (&ptrace.JSONUnmarshaler{}).UnmarshalTraces(b)
		} else {
			td, err = (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(b)
		}
		if err == nil && td.SpanCount() > s.Limits.MaxItems {
			s.refuse(w, http.StatusRequestEntityTooLarge, "too_large", "too many spans", fmt.Errorf("%d spans", td.SpanCount()), 0)
			return
		}
		if err == nil {
			StampTraces(td, a)
			perr = s.Publisher.PushTraces(r.Context(), td)
		}
	case "logs":
		var ld plog.Logs
		if mt == "application/json" {
			ld, err = (&plog.JSONUnmarshaler{}).UnmarshalLogs(b)
		} else {
			ld, err = (&plog.ProtoUnmarshaler{}).UnmarshalLogs(b)
		}
		if err == nil && ld.LogRecordCount() > s.Limits.MaxItems {
			s.refuse(w, http.StatusRequestEntityTooLarge, "too_large", "too many log records", fmt.Errorf("%d records", ld.LogRecordCount()), 0)
			return
		}
		if err == nil {
			StampLogs(ld, a)
			perr = s.Publisher.PushLogs(r.Context(), ld)
		}
	}
	if err != nil {
		s.refuse(w, http.StatusBadRequest, "bad_request", "undecodable OTLP", err, 0)
		return
	}
	if perr != nil {
		if edge.IsPermanent(perr) {
			s.refuse(w, http.StatusBadRequest, "bad_request", "request cannot be published", perr, 0)
			return
		}
		// Unresolved: it may still commit. The sender retries the same
		// bytes; if the first attempt landed, the retry is a copy.
		s.refuse(w, http.StatusServiceUnavailable, "unavailable", "not committed; retry", perr, 5*time.Second)
		return
	}
	s.count("ok")
	if mt == "application/json" {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("{}"))
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(http.StatusOK) // an empty Export*ServiceResponse
}
