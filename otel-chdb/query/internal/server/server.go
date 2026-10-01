// Package server is the query service's HTTP API: POST /v1/query (central,
// SQL), POST /v1/plan (lake, presigned objects), /healthz and /metrics.
package server

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/casselc/observability/otel-chdb/query/internal/audit"
	"github.com/casselc/observability/otel-chdb/query/internal/auth"
	"github.com/casselc/observability/otel-chdb/query/internal/basis"
	"github.com/casselc/observability/otel-chdb/query/internal/catalog"
	"github.com/casselc/observability/otel-chdb/query/internal/central"
	"github.com/casselc/observability/otel-chdb/query/internal/completeness"
	"github.com/casselc/observability/otel-chdb/query/internal/lake"
	"github.com/casselc/observability/otel-chdb/query/internal/metrics"
	"github.com/casselc/observability/otel-chdb/query/internal/persons"
	"github.com/casselc/observability/otel-chdb/query/internal/sqlscope"
	"github.com/golang-jwt/jwt/v5"
)

// TokenVerifier checks a bearer token.
type TokenVerifier interface {
	Verify(ctx context.Context, raw string) (jwt.MapClaims, error)
}

// Querier runs a statement on central.
type Querier interface {
	Query(ctx context.Context, sql string, settings url.Values) ([]byte, central.Summary, error)
}

// Limits are the per-caller budgets: Default, raised by any group the
// caller is in.
type Limits struct {
	Default central.Limits            `json:"default"`
	Groups  map[string]central.Limits `json:"groups"`
}

// For returns p's limits.
func (l Limits) For(p *auth.Principal) central.Limits {
	out := l.Default
	for _, g := range p.Groups {
		if x, ok := l.Groups[g]; ok {
			out = out.Max(x)
		}
	}
	return out
}

// Server is the service.
type Server struct {
	Verifier TokenVerifier
	Mapping  *auth.Mapping
	Audit    audit.Sink
	Policy   *sqlscope.Policy
	// Recovered is the policy of the recovered tables (D35 (3)): a request
	// with "recovered": true is checked against it alone; nil: refused.
	Recovered *sqlscope.Policy
	Central   Querier
	Catalog   *catalog.Catalog // nil: none configured
	Planner   *lake.Planner    // nil: no lake
	// Persons resolves oids to names (D32 person, O-G9); nil: /v1/persons
	// answers not_configured.
	Persons   *persons.Store
	Watermark *completeness.Reader
	Limits    Limits
	Origins   []string
	Metrics   *metrics.Registry
	MaxBody   int64
	Now       func() time.Time
	// NoLateCount turns the late-row count off (watermark.count_late:
	// false); by default a windowed /v1/query counts them (LateInfo).
	NoLateCount bool
	// Bases mints and verifies basis tokens (D30): a *basis.Bases (static
	// keys or KMS, cached) or a bare *basis.Keyring; nil disables them.
	Bases BasisCodec
	// Retention: a basis older than this, or a window starting before it,
	// is basis_expired (default DefaultRetention). BasisSkew: how early a
	// row may be received before its event time (default DefaultBasisSkew).
	Retention, BasisSkew time.Duration
	// Performance is the allow-list of settings a caller may set per
	// statement (central.performance_settings; D33). Empty: none.
	Performance central.SettingsPolicy
	// Sample bounds labelled samples (D33): a request with "sample" stops
	// reading at min(its rows, SampleMaxRows, the caller's
	// max_rows_to_read) rows. SampleMaxRows 0: samples are refused.
	SampleDefaultRows, SampleMaxRows int64

	mu       sync.Mutex
	inflight map[string]int
}

// Init declares the metrics; call once before serving.
func (s *Server) Init() {
	if s.Now == nil {
		s.Now = time.Now
	}
	if s.MaxBody <= 0 {
		s.MaxBody = 1 << 20
	}
	s.inflight = map[string]int{}
	m := metrics.New()
	s.Metrics = m
	m.Counter("qs_requests_total", "Requests by endpoint and HTTP status.", "endpoint", "code")
	m.Counter("qs_denials_total", "Requests refused (401, 403, 400 on policy, 429), by endpoint and reason.", "endpoint", "reason")
	m.Counter("qs_planned_objects_total", "Objects returned in plans, each with a presigned URL.")
	m.Counter("qs_planned_bytes_total", "Bytes of the objects returned in plans.")
	m.Counter("qs_plan_tail_objects_total", "Objects planned in a plan's tail (received at or after its basis; incomplete, never cached).")
	m.Counter("qs_plan_tail_unplaced_total", "Tail objects the planner could not date against the basis (basis_check: the reader's footer decides).")
	m.Counter("qs_plan_mismatched_objects_total", "Objects under one cluster's prefix whose metadata names another (never planned).")
	m.Counter("qs_plan_index_objects_total", "Objects of filtered plans by index outcome: hit (narrowed to row groups), scan (not indexed or index unreadable), pruned (ruled out, not planned).", "outcome")
	m.Counter("qs_plan_index_errors_total", "Index segments a filtered plan could not use (unreadable, corrupt, over budget): their objects were scanned.")
	m.Counter("qs_plan_index_bytes_total", "Index bytes the service read to resolve filters (cache misses).")
	m.Counter("qs_query_rows_read_total", "Rows central read for allowed queries.")
	m.Counter("qs_clickhouse_errors_total", "ClickHouse errors by code (limits included).", "code")
	m.Counter("qs_audit_errors_total", "Audit records that could not be written (the request was refused if it was a decision).", "event")
	m.Counter("qs_results_total", "Results served, by source and completeness.", "source", "completeness")
	m.Counter("qs_late_results_total", "Results holding rows received more than max_lateness after their event time (query: late rows; plan: late objects), by source and completeness.", "source", "completeness")
	m.Counter("qs_late_count_errors_total", "Late-row counts that failed (the result was served with late.status error).")
	m.Counter("qs_basis_signer_calls_total", "Basis signer calls (KMS or static; cache hits make none), by op (mint, verify) and result (ok, mismatch, error).", "op", "result")
	m.Counter("qs_basis_unavailable_total", "Answers served without a basis because the signer could not mint one (basis_unavailable in the answer).", "endpoint")
	m.Counter("qs_samples_total", "Labelled samples served (D33), by whether reading reached the bound.", "reached_bound")
	m.Counter("qs_settings_total", "Performance settings callers set (D33), by name.", "name")
	m.Gauge("qs_max_lateness_seconds", "The max_lateness policy the labels are made with.", func() []metrics.Sample {
		return []metrics.Sample{{Value: s.Watermark.MaxLateness().Seconds()}}
	})
	m.Gauge("qs_watermark_age_seconds", "Now minus the watermark document's wall_ms (NaN when none).", func() []metrics.Sample {
		st := s.Watermark.Get(context.Background())
		v := math.NaN()
		if st.Doc != nil {
			v = s.Now().Sub(time.UnixMilli(int64(st.Doc.WallMs))).Seconds()
		}
		return []metrics.Sample{{Value: v}}
	})
	m.Gauge("qs_complete_through_seconds", "complete_through as the service reads it (Unix seconds; NaN when none).", func() []metrics.Sample {
		st := s.Watermark.Get(context.Background())
		v := math.NaN()
		if st.Doc != nil {
			v = float64(st.Doc.CompleteThroughNs) / 1e9
		}
		return []metrics.Sample{{Value: v}}
	})
	m.Gauge("qs_watermark_status", "1 for the watermark's current status (ok, stale, missing, error).", func() []metrics.Sample {
		st := s.Watermark.Get(context.Background())
		var out []metrics.Sample
		for _, x := range []string{completeness.StatusOK, completeness.StatusStale, completeness.StatusMissing, completeness.StatusError} {
			v := 0.0
			if st.Status == x {
				v = 1
			}
			out = append(out, metrics.Sample{Labels: []string{"status", x}, Value: v})
		}
		return out
	})
}

// Handler returns the routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/query", s.cors(s.handleQuery))
	mux.HandleFunc("/v1/plan", s.cors(s.handlePlan))
	mux.HandleFunc("/v1/basis", s.cors(s.handleBasis))
	mux.HandleFunc("/v1/persons", s.cors(s.handlePersons))
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		s.Metrics.Write(w)
	})
	return mux
}

func (s *Server) cors(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if o := r.Header.Get("Origin"); o != "" {
			for _, allowed := range s.Origins {
				if o == allowed {
					w.Header().Set("Access-Control-Allow-Origin", o)
					w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
					w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
					w.Header().Set("Access-Control-Max-Age", "600")
					break
				}
			}
			w.Header().Add("Vary", "Origin")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		h(w, r)
	}
}

func newID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

type apiError struct {
	Error     string `json:"error"`
	Detail    string `json:"detail,omitempty"`
	RequestID string `json:"request_id"`
}

func (s *Server) reply(w http.ResponseWriter, endpoint string, code int, v any) {
	s.Metrics.Inc("qs_requests_total", endpoint, strconv.Itoa(code))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) fail(w http.ResponseWriter, endpoint, reqID string, code int, reason, detail string) {
	if code == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", `Bearer error="invalid_token"`)
	}
	if code == 401 || code == 403 || code == 429 || code == 400 || code == 413 {
		s.Metrics.Inc("qs_denials_total", endpoint, reason)
	}
	s.reply(w, endpoint, code, apiError{Error: reason, Detail: detail, RequestID: reqID})
}

// req is what every handler learns first.
type req struct {
	id       string
	endpoint string
	p        *auth.Principal // the view for the endpoint's role (D38)
	all      *auth.Principal // every grant, for an endpoint either role may call
	ip       string
}

func clientIP(r *http.Request) string {
	h, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return h
}

// authenticate returns nil after replying 401 (and auditing it).
func (s *Server) authenticate(w http.ResponseWriter, r *http.Request, endpoint, action, role string) *req {
	rq := &req{id: newID(), endpoint: endpoint, ip: clientIP(r)}
	deny := func(code int, reason, detail string, p *auth.Principal) *req {
		rec := audit.Record{RequestID: rq.id, Event: "decision", Action: action, Decision: "deny", Reason: reason, Detail: detail, ClientIP: rq.ip}
		if p != nil {
			rec.Subject, rec.Groups, rec.Roles = p.Subject, p.Groups, p.Roles
		}
		s.write(rec)
		s.fail(w, endpoint, rq.id, code, reason, detail)
		return nil
	}
	tok, ok := auth.Bearer(r.Header.Get("Authorization"))
	if !ok {
		return deny(http.StatusUnauthorized, "no_token", "an OIDC bearer token is required", nil)
	}
	claims, err := s.Verifier.Verify(r.Context(), tok)
	if err != nil {
		return deny(http.StatusUnauthorized, "bad_token", err.Error(), nil)
	}
	p, err := s.Mapping.Principal(claims)
	if err != nil {
		return deny(http.StatusUnauthorized, "bad_token", err.Error(), nil)
	}
	if !p.Has(role) && !(role == auth.RolePerson && endpoint == "persons") { // anyone may resolve themselves
		return deny(http.StatusForbidden, "role_missing", "the token does not grant role "+role, p)
	}
	rq.p, rq.all = p.For(role), p
	return rq
}

// write records r; false if it could not be (the caller refuses the request
// when r is a decision).
func (s *Server) write(r audit.Record) bool {
	if r.Time.IsZero() {
		r.Time = s.Now().UTC()
	}
	if err := s.Audit.Write(r); err != nil {
		s.Metrics.Inc("qs_audit_errors_total", r.Event)
		log.Printf("audit: %v", err)
		return false
	}
	return true
}

// scopeOf is the view's explicit (cluster, namespace) grants (D38): rows
// are cut by their union, never by the product of the projections.
func scopeOf(p *auth.Principal) sqlscope.Scope { return p.Scope() }

// pairsOf is the view's grants for the audit record.
func pairsOf(p *auth.Principal) []string {
	var out []string
	for _, pr := range p.Pairs() {
		out = append(out, pr.Cluster+"/"+pr.Namespace)
	}
	return out
}

// labelScope is what a statement's label depends on (D29): the scope's
// clusters (every cluster for a fleet caller, or when a table has no
// per-row cluster scope) and the signals of the tables it reads.
func labelScope(sc sqlscope.Scope, tables []*sqlscope.Table) completeness.Scope {
	out := completeness.Scope{Signals: sqlscope.SignalsOf(tables)}
	if !sc.AllClusters {
		out.Clusters = sc.Clusters
	}
	for _, t := range tables {
		if t.Scope == "fleet" {
			out.Clusters = nil
		}
	}
	return out
}

func scopeLists(p *auth.Principal) ([]string, []string) {
	c, n := p.Clusters, p.Namespaces
	if p.AllClusters {
		c = []string{"*"}
	}
	if p.AllNamespaces {
		n = []string{"*"}
	}
	return c, n
}

func (s *Server) acquire(sub string, max int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if max > 0 && s.inflight[sub] >= max {
		return false
	}
	s.inflight[sub]++
	return true
}

func (s *Server) release(sub string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inflight[sub]--; s.inflight[sub] <= 0 {
		delete(s.inflight, sub)
	}
}

// timeArg reads a time as RFC 3339 text or as integer nanoseconds.
func timeArg(raw json.RawMessage) (int64, bool, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, false, nil
	}
	var n int64
	if err := json.Unmarshal(raw, &n); err == nil {
		return n, true, nil
	}
	var str string
	if err := json.Unmarshal(raw, &str); err != nil {
		return 0, false, fmt.Errorf("time %s: want RFC 3339 or integer ns", raw)
	}
	t, err := time.Parse(time.RFC3339Nano, str)
	if err != nil {
		return 0, false, err
	}
	return t.UnixNano(), true, nil
}

// QueryRequest is POST /v1/query's body.
type QueryRequest struct {
	SQL    string `json:"sql"`
	Window *struct {
		From json.RawMessage `json:"from"`
		To   json.RawMessage `json:"to"`
	} `json:"window"`
	// Output changes how values are written, never which rows: only
	// OutputSettings' names and values are accepted.
	Output map[string]string `json:"output"`
	// Clusters narrows the statement to some of the token's clusters (D29):
	// rows are filtered to them, and the label is their complete_through.
	// Default: every cluster in the token's scope.
	Clusters []string `json:"clusters"`
	// Basis (D30): a token from an earlier answer or /v1/basis, or
	// "latest"; the statement then reads, per cluster, only rows received
	// before the basis's bound, and the same statement at the same basis
	// answers the same while new data arrives. Omitted: the statement reads
	// up to now (and the answer names the current basis to pin).
	Basis string `json:"basis"`
	// BasisFrom (with Basis): a delta, the rows with BasisFrom's bound <=
	// received_at < Basis's, per cluster (late data for the alert
	// evaluator). Both are checked like Basis; neither widens scope.
	BasisFrom string `json:"basis_from"`
	// Settings are ClickHouse settings for this statement, each in the
	// service's performance-settings allow-list (D33): settings that change
	// how fast it runs, never what it reads or answers, nor the caller's
	// limits. Anything else is 400 bad_setting and nothing runs.
	Settings map[string]string `json:"settings"`
	// Sample asks for a labelled sample (D33): reading stops at a row bound
	// instead of failing with a limit error, and the answer says
	// completeness "sample" and how much was read. For suggestion lists
	// (HyperDX typeahead), never for a count, a chart or an alert.
	Sample *SampleRequest `json:"sample"`
	// Recovered asks for the recovered tables (D35 (3): rows `consume
	// admit` took out of a quarantine, below a retired lane's bound): the
	// statement may then name only {table}_recovered tables, and the answer
	// is labelled source "recovered", outside complete_through (completeness
	// "unknown"). Without it, no recovered table can be named: the two are
	// never mixed. Not with basis, basis_from or sample.
	Recovered bool `json:"recovered"`
}

// SampleRequest is a sample's bound: Rows (default: the service's
// sample.default_rows), at most sample.max_rows and the caller's
// max_rows_to_read.
type SampleRequest struct {
	Rows int64 `json:"rows"`
}

// SampleInfo describes a labelled sample: how much was read, the bound, and
// whether reading reached it (then the answer is over part of the rows the
// statement would read; if not, it read them all, but is still labelled a
// sample: nothing tells it apart from one that stopped a row short).
type SampleInfo struct {
	Sample        bool  `json:"sample"`
	RowsRead      int64 `json:"rows_read"`
	MaxRowsToRead int64 `json:"max_rows_to_read"`
	ReachedBound  bool  `json:"reached_bound"`
	// DataCompleteness is the label the rows it read would have had
	// (complete / partial / unknown): the watermark's part of the story.
	DataCompleteness string `json:"data_completeness"`
}

// OutputSettings are the output-format settings a caller may choose (a UI
// that parses DateTime as ISO 8601 needs iso). Everything else about a
// statement's execution is the service's.
var OutputSettings = map[string]map[string]bool{
	"date_time_output_format": {"simple": true, "iso": true, "unix_timestamp": true},
}

// QueryResponse is its answer.
type QueryResponse struct {
	RequestID string `json:"request_id"`
	completeness.Label
	basis.Answer
	// Delta: with basis_from, the rows the delta admits, per table.
	Delta *LateInfo `json:"delta,omitempty"`
	// Sample: set when the request asked for a sample (D33); completeness
	// is then "sample".
	Sample *SampleInfo `json:"sample,omitempty"`
	// Settings: the performance settings applied, as sent.
	Settings map[string]string `json:"settings,omitempty"`
	Catalog  catalog.Info      `json:"catalog"`
	Late     LateInfo          `json:"late"`
	Query    QueryInfo         `json:"query"`
	Result   json.RawMessage   `json:"result"`
}

// LateInfo makes late data visible (STPA CAST row 26): rows of the
// result's tables, in its window and scope, whose received_at is more than
// max_lateness after their event time. They are in this result, but a
// result over the same window labelled complete before they arrived did not
// have them, and more may come: a non-zero count says max_lateness is too
// short for this data.
type LateInfo struct {
	MaxLatenessS float64 `json:"max_lateness_s"`
	// Status: counted, no_window (an unbounded statement is not counted),
	// not_measured (no table read has both a time and a received column),
	// disabled (watermark.count_late false), error.
	Status string           `json:"status"`
	Rows   *int64           `json:"rows"`
	Tables map[string]int64 `json:"tables,omitempty"`
	// Uncounted: tables read whose late rows are not measured.
	Uncounted []string `json:"uncounted,omitempty"`
	Error     string   `json:"error,omitempty"`
}

// QueryInfo describes what ran.
type QueryInfo struct {
	Hash      string               `json:"hash"`
	Tables    []string             `json:"tables"`
	SQL       string               `json:"sql"`
	Scoped    bool                 `json:"scoped"`
	Window    *completeness.Window `json:"window,omitempty"`
	RowsRead  int64                `json:"rows_read"`
	ElapsedMs float64              `json:"elapsed_ms"`
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	const ep = "query"
	rq := s.authenticate(w, r, ep, "query", auth.RoleQuery)
	if rq == nil {
		return
	}
	p := rq.p
	cl, ns := scopeLists(p)
	base := audit.Record{RequestID: rq.id, Event: "decision", Action: "query", Subject: p.Subject, Groups: p.Groups, Roles: p.Roles,
		Clusters: cl, Namespaces: ns, Pairs: pairsOf(p), ClientIP: rq.ip}
	deny := func(code int, reason, detail string, rec audit.Record) {
		rec.Decision, rec.Reason, rec.Detail = "deny", reason, detail
		s.write(rec)
		s.fail(w, ep, rq.id, code, reason, detail)
	}
	var body QueryRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, s.MaxBody)).Decode(&body); err != nil {
		deny(http.StatusBadRequest, "bad_request", err.Error(), base)
		return
	}
	in := sha256.Sum256([]byte(body.SQL))
	base.InputHash = hex.EncodeToString(in[:])
	scope := scopeOf(p)
	if len(body.Clusters) > 0 {
		var cs []string
		for _, c := range body.Clusters {
			if !sqlscope.ClusterRE.MatchString(c) {
				deny(http.StatusBadRequest, "bad_cluster", fmt.Sprintf("cluster %q is not a valid name", c), base)
				return
			}
			if !p.MayCluster(c) {
				deny(http.StatusForbidden, "cluster_not_in_scope", fmt.Sprintf("cluster %q is not in the token's scope", c), base)
				return
			}
			if !slices.Contains(cs, c) {
				cs = append(cs, c)
			}
		}
		scope = scope.NarrowClusters(cs)
		base.Clusters = cs
	}
	var window *completeness.Window
	if body.Window != nil {
		from, okF, err1 := timeArg(body.Window.From)
		to, okT, err2 := timeArg(body.Window.To)
		if err1 != nil || err2 != nil || !okF || !okT || to <= from {
			deny(http.StatusBadRequest, "bad_window", "window needs from < to, RFC 3339 or integer ns", base)
			return
		}
		window = &completeness.Window{FromNs: from, ToNs: to}
		scope.Window = &sqlscope.Window{FromNs: from, ToNs: to}
	}
	for k, v := range body.Output {
		if !OutputSettings[k][v] {
			deny(http.StatusBadRequest, "bad_output", fmt.Sprintf("output setting %s=%q is not one a caller may choose", k, v), base)
			return
		}
	}
	perf := map[string]string{}
	for k, v := range body.Settings {
		val, err := s.Performance.Check(k, v)
		if err != nil {
			deny(http.StatusBadRequest, "bad_setting", err.Error(), base)
			return
		}
		perf[k] = val
	}
	pol := s.Policy
	if body.Recovered {
		switch {
		case s.Recovered == nil:
			deny(http.StatusBadRequest, "recovered_unavailable", "this service serves no recovered tables (central.recovered)", base)
			return
		case body.Basis != "" || body.BasisFrom != "" || body.Sample != nil:
			deny(http.StatusBadRequest, "recovered_alone", "recovered rows are outside every basis and are not sampled: no basis, basis_from or sample with recovered", base)
			return
		}
		pol = s.Recovered
		base.Detail = "recovered"
	}
	pr, err := pol.Prepare(body.SQL)
	if err != nil {
		rj, _ := sqlscope.AsRejection(err)
		deny(rejectionCode(rj.Reason), rj.Reason, rj.Detail, base)
		return
	}
	var rb, rbFrom *resolvedBasis
	if body.BasisFrom != "" && body.Basis == "" {
		deny(http.StatusBadRequest, basis.ReasonDeltaNeeds, "basis_from needs basis (the delta's upper bound)", base)
		return
	}
	if body.Basis != "" {
		sigs, tele := telemetrySignals(pr.Tables())
		var wf *int64
		if window != nil {
			wf = &window.FromNs
		}
		var asked []string
		if len(body.Clusters) > 0 {
			asked = scope.Clusters
		}
		var rf *basis.Refusal
		if rb, rf = s.useBasis(r.Context(), p, body.Basis, asked, sigs, tele, wf); rf != nil {
			deny(rf.Status, rf.Reason, rf.Detail, base)
			return
		}
		if body.BasisFrom != "" {
			if body.BasisFrom == Latest {
				deny(http.StatusBadRequest, basis.ReasonInvalid, "basis_from must be a token (the bound an earlier answer was computed at)", base)
				return
			}
			if rbFrom, rf = s.useBasis(r.Context(), p, body.BasisFrom, rb.clusters, sigs, tele, wf); rf == nil {
				rf = basis.CheckDelta(rbFrom.b, rb.b, rb.clusters)
			}
			if rf != nil {
				deny(rf.Status, rf.Reason, rf.Detail, base)
				return
			}
		}
		if rb.clusters != nil {
			scope = scope.NarrowClusters(rb.clusters)
			base.Clusters = rb.clusters
		}
		scope.Received = &sqlscope.Received{Before: bounds(rb.b, rb.clusters)}
		base.Basis = scope.Received.Before
		if rbFrom != nil {
			scope.Received.From = bounds(rbFrom.b, rb.clusters)
			base.BasisFrom = scope.Received.From
		}
	}
	restricted := !scope.Unrestricted()
	for _, t := range pr.Tables() {
		if t.Scope == "catalog" && restricted {
			if s.Catalog == nil {
				deny(http.StatusServiceUnavailable, "catalog_unavailable", "table "+t.FQN()+" is scoped by the entity catalog, which is not configured", base)
				return
			}
			ids, err := s.Catalog.ResourceIDs(r.Context(), scope)
			if err != nil {
				deny(http.StatusServiceUnavailable, "catalog_unavailable", err.Error(), base)
				return
			}
			scope.ResourceIDs = ids
			break
		}
	}
	res, err := pr.Finish(scope)
	if err != nil {
		rj, _ := sqlscope.AsRejection(err)
		deny(rejectionCode(rj.Reason), rj.Reason, rj.Detail, base)
		return
	}
	base.QueryHash, base.SQL, base.Tables, base.Filters = res.Hash, truncate(res.SQL, 8192), res.Tables, len(res.Filters)
	limits := s.Limits.For(p)
	var sampleBound int64
	if body.Sample != nil {
		if s.SampleMaxRows <= 0 {
			deny(http.StatusBadRequest, "sample_disabled", "this service serves no samples (sample.max_rows)", base)
			return
		}
		if body.Sample.Rows < 0 {
			deny(http.StatusBadRequest, "bad_sample", "sample.rows must be positive", base)
			return
		}
		sampleBound = body.Sample.Rows
		if sampleBound == 0 {
			sampleBound = s.SampleDefaultRows
		}
		if sampleBound <= 0 || sampleBound > s.SampleMaxRows {
			sampleBound = s.SampleMaxRows
		}
		if limits.MaxRowsToRead > 0 && sampleBound > limits.MaxRowsToRead {
			sampleBound = limits.MaxRowsToRead
		}
		base.Detail = fmt.Sprintf("sample: max_rows_to_read %d", sampleBound)
	}
	if len(perf) > 0 {
		names := make([]string, 0, len(perf))
		for k, v := range perf {
			names = append(names, k+"="+v)
		}
		sort.Strings(names)
		if base.Detail != "" {
			base.Detail += "; "
		}
		base.Detail += "settings: " + strings.Join(names, ", ")
	}
	if !s.acquire(p.Subject, limits.MaxConcurrent) {
		deny(http.StatusTooManyRequests, "too_many_concurrent", fmt.Sprintf("at most %d concurrent queries per caller", limits.MaxConcurrent), base)
		return
	}
	defer s.release(p.Subject)
	allow := base
	allow.Decision = "allow"
	if !s.write(allow) {
		s.fail(w, ep, rq.id, http.StatusServiceUnavailable, "audit_unavailable", "the decision could not be recorded, so nothing ran")
		return
	}
	// the label's watermark is read before the statement runs: rows below it
	// were in central before the statement started
	ls := labelScope(scope, pr.Tables())
	wm := s.Watermark.For(r.Context(), ls)
	started := s.Now()
	comment := "qs:" + rq.id + ":" + p.Subject
	settings := central.Settings(limits, res.FiltersSetting(), rq.id, truncate(comment, 200))
	for k, v := range body.Output {
		settings.Set(k, v)
	}
	for k, v := range perf {
		// never over a setting the service pinned (the allow-list's classes
		// exclude them; this is the second fence)
		if settings.Has(k) {
			s.fail(w, ep, rq.id, http.StatusInternalServerError, "bad_setting", "setting "+k+" collides with one the service sets")
			return
		}
		settings.Set(k, v)
		s.Metrics.Inc("qs_settings_total", k)
	}
	if sampleBound > 0 {
		central.Sample(settings, sampleBound)
	}
	out, sum, err := s.Central.Query(r.Context(), res.SQL, settings)
	elapsed := s.Now().Sub(started)
	outcome := audit.Record{RequestID: rq.id, Event: "outcome", Action: "query", Subject: p.Subject, QueryHash: res.Hash,
		RowsRead: sum.ReadRows, BytesRead: sum.ReadBytes, Rows: sum.ResultRows, ElapsedMs: float64(elapsed.Microseconds()) / 1000}
	if err != nil {
		code, reason := http.StatusBadGateway, "central_error"
		var ce *central.Error
		if errors.As(err, &ce) {
			s.Metrics.Inc("qs_clickhouse_errors_total", strconv.Itoa(ce.Code))
			if name, ok := central.LimitCodes[ce.Code]; ok {
				code, reason = http.StatusUnprocessableEntity, "limit_exceeded:"+name
			} else if ce.Code == 497 || ce.Code == 516 {
				// the allow-list and ClickHouse's grants disagree: alert
				code, reason = http.StatusForbidden, "central_access_denied"
			} else if ce.HTTPStatus == 400 || ce.HTTPStatus == 404 {
				code, reason = http.StatusBadRequest, "central_rejected"
			}
		} else {
			s.Metrics.Inc("qs_clickhouse_errors_total", "transport")
		}
		outcome.Status, outcome.Error = code, truncate(err.Error(), 2000)
		s.write(outcome)
		s.fail(w, ep, rq.id, code, reason, truncate(err.Error(), 2000))
		return
	}
	s.Metrics.Add("qs_query_rows_read_total", float64(sum.ReadRows))
	maxLate := s.Watermark.MaxLateness()
	var label completeness.Label
	var ans basis.Answer
	var delta *LateInfo
	if rb != nil {
		// at the basis: its own max_lateness, its bound as complete_through
		maxLate = time.Duration(rb.b.MaxLatenessNs)
		label = completeness.LabelAt("central", wm, rb.b.MinFor(rb.clusters), window, s.Now(), s.Watermark.Key(), p.MayCluster, maxLate)
		tok := rb.token
		ans = basis.Answer{Basis: &tok, AtBasis: true, BasisInfo: rb.b.View()}
		if rbFrom != nil {
			ft := rbFrom.token
			ans.BasisFrom, ans.BasisFromInfo = &ft, rbFrom.b.View()
			delta = s.countDelta(r.Context(), res, limits, rq.id, comment)
		}
	} else if body.Recovered {
		label = RecoveredLabel(maxLate)
	} else {
		label = completeness.MakeLabel("central", wm, window, s.Now(), s.Watermark.Key(), p.MayCluster, maxLate)
		if b, ok := mintFrom(wm, ls, maxLate, s.Now()); ok && s.Bases != nil {
			ans = s.currentAnswer(r.Context(), ep, b)
		}
	}
	var late LateInfo
	var sample *SampleInfo
	if sampleBound > 0 {
		// a sample's rows are part of what the statement reads: its late
		// rows count nothing, and its label is "sample", whatever the
		// watermark says (the watermark's label is kept beside it)
		late = LateInfo{MaxLatenessS: maxLate.Seconds(), Status: "sample"}
		sample = &SampleInfo{Sample: true, RowsRead: sum.ReadRows, MaxRowsToRead: sampleBound,
			ReachedBound: sum.ReadRows >= sampleBound, DataCompleteness: label.Completeness}
		label.Completeness, label.Partial = "sample", true
		s.Metrics.Inc("qs_samples_total", strconv.FormatBool(sample.ReachedBound))
	} else if body.Recovered {
		late = LateInfo{MaxLatenessS: maxLate.Seconds(), Status: "recovered"}
	} else {
		late = s.countLate(r.Context(), res, window, limits, rq.id, comment, maxLate)
	}
	var want []string
	if !scope.AllClusters {
		want = scope.Clusters
	}
	if len(perf) == 0 {
		perf = nil
	}
	resp := QueryResponse{RequestID: rq.id, Label: label, Answer: ans, Delta: delta, Result: out, Late: late, Sample: sample, Settings: perf,
		Catalog: s.Catalog.Lag(r.Context(), p.MayCluster, want),
		Query: QueryInfo{Hash: res.Hash, Tables: res.Tables, SQL: res.SQL, Scoped: len(res.Filters) > 0, Window: window,
			RowsRead: sum.ReadRows, ElapsedMs: outcome.ElapsedMs}}
	outcome.Status = http.StatusOK
	s.write(outcome)
	s.Metrics.Inc("qs_results_total", label.Source, label.Completeness)
	if late.Rows != nil && *late.Rows > 0 {
		s.Metrics.Inc("qs_late_results_total", label.Source, label.Completeness)
	}
	s.reply(w, ep, http.StatusOK, resp)
}

// RecoveredLabel labels an answer over the recovered tables (D35 (3)):
// source "recovered", outside complete_through (its rows were quarantined
// below a retired lane's bound, then admitted by an operator), so
// completeness "unknown", with the reason in the watermark's note.
func RecoveredLabel(maxLate time.Duration) completeness.Label {
	return completeness.Label{Source: "recovered", MaxLatenessS: maxLate.Seconds(), Completeness: "unknown", Partial: true,
		Watermark: completeness.WmInfo{Status: "not_applicable", Note: "recovered rows (consume admit, D35) were quarantined below a retired lane's bound: " +
			"complete_through says nothing about them, and they are not in the main tables"}}
}

// countLate counts the late rows of a statement's tables, in its window and
// scope (the same filters), after the statement ran. A failure is reported,
// never fatal: the result stands, and says its late rows are unknown.
func (s *Server) countLate(ctx context.Context, res *sqlscope.Result, window *completeness.Window, limits central.Limits,
	reqID, comment string, maxLate time.Duration) LateInfo {
	li := LateInfo{MaxLatenessS: maxLate.Seconds()}
	switch {
	case s.NoLateCount:
		li.Status = "disabled"
		return li
	case window == nil:
		li.Status = "no_window"
		return li
	}
	lc, err := s.Policy.LateCount(res, maxLate)
	if err != nil {
		li.Status, li.Error = "error", err.Error()
		s.Metrics.Inc("qs_late_count_errors_total")
		return li
	}
	li.Uncounted = lc.Uncounted
	if lc.SQL == "" {
		li.Status = "not_measured"
		return li
	}
	return s.runCount(ctx, li, lc, res, limits, reqID+"-late", comment+":late")
}

// countDelta counts the rows a delta admits (D30), per table, under the
// statement's own filters (scope, window, [basis_from, basis)). Like the
// late count, a failure is reported and never fails the result.
func (s *Server) countDelta(ctx context.Context, res *sqlscope.Result, limits central.Limits, reqID, comment string) *LateInfo {
	li := LateInfo{}
	lc, err := s.Policy.DeltaCount(res)
	if err != nil {
		li.Status, li.Error = "error", err.Error()
		return &li
	}
	li.Uncounted = lc.Uncounted
	if lc.SQL == "" {
		li.Status = "not_measured"
		return &li
	}
	out := s.runCount(ctx, li, lc, res, limits, reqID+"-delta", comment+":delta")
	return &out
}

// runCount runs a per-table count statement (late or delta) with the
// statement's filters and limits.
func (s *Server) runCount(ctx context.Context, li LateInfo, lc *sqlscope.LateCount, res *sqlscope.Result, limits central.Limits,
	reqID, comment string) LateInfo {
	settings := central.Settings(limits, res.FiltersSetting(), reqID, truncate(comment, 200))
	out, _, err := s.Central.Query(ctx, lc.SQL, settings)
	var body struct {
		Data []struct {
			T string          `json:"t"`
			N json.RawMessage `json:"n"`
		} `json:"data"`
	}
	if err == nil {
		err = json.Unmarshal(out, &body)
	}
	var total int64
	tables := map[string]int64{}
	if err == nil {
		for _, d := range body.Data {
			n, perr := strconv.ParseInt(strings.Trim(string(d.N), `"`), 10, 64)
			if perr != nil {
				err = fmt.Errorf("late count for %s: %q is not a count", d.T, d.N)
				break
			}
			tables[d.T] += n
			total += n
		}
	}
	if err == nil && len(tables) != len(lc.Counted) {
		err = fmt.Errorf("late count answered %d tables, %d were asked", len(tables), len(lc.Counted))
	}
	if err != nil {
		li.Status, li.Error = "error", truncate(err.Error(), 500)
		s.Metrics.Inc("qs_late_count_errors_total")
		return li
	}
	li.Status, li.Rows, li.Tables = "counted", &total, tables
	return li
}

func rejectionCode(reason string) int {
	switch reason {
	case "parse_error", "too_long", "statement_count", "bad_window", "query_param", "bad_literal", "bad_identifier", "bad_output", "basis_unservable":
		return http.StatusBadRequest
	case "catalog_unavailable":
		return http.StatusServiceUnavailable
	case "roundtrip":
		return http.StatusInternalServerError
	}
	return http.StatusForbidden
}

// PlanRequest is POST /v1/plan's body.
type PlanRequest struct {
	Signal   string          `json:"signal"`
	From     json.RawMessage `json:"from"`
	To       json.RawMessage `json:"to"`
	Clusters []string        `json:"clusters"`
	// TraceID / Terms: an index filter (lake.Request, D27).
	TraceID string   `json:"trace_id"`
	Terms   []string `json:"terms"`
	// Basis (D30): a token or "latest"; the plan then lists only objects
	// received before the basis's bound of their cluster, and re-planning
	// at the same basis lists the same objects.
	Basis string `json:"basis"`
	// Tail (with a basis): also plan the objects received at or after it
	// (tail_objects: incomplete, never cached; D30 amendment "the tail").
	Tail bool `json:"tail"`
}

// PlanResponse is its answer.
type PlanResponse struct {
	RequestID string `json:"request_id"`
	*lake.Plan
}

// AuditKeys is how many planned keys an audit record lists (all are hashed).
const AuditKeys = 50

func (s *Server) handlePlan(w http.ResponseWriter, r *http.Request) {
	const ep = "plan"
	rq := s.authenticate(w, r, ep, "plan", auth.RolePlan)
	if rq == nil {
		return
	}
	p := rq.p
	cl, ns := scopeLists(p)
	base := audit.Record{RequestID: rq.id, Event: "decision", Action: "plan", Subject: p.Subject, Groups: p.Groups, Roles: p.Roles,
		Clusters: cl, Namespaces: ns, Pairs: pairsOf(p), ClientIP: rq.ip}
	deny := func(code int, reason, detail string) {
		rec := base
		rec.Decision, rec.Reason, rec.Detail = "deny", reason, detail
		s.write(rec)
		s.fail(w, ep, rq.id, code, reason, detail)
	}
	if s.Planner == nil {
		deny(http.StatusNotFound, "no_lake", "no lake is configured")
		return
	}
	var body PlanRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, s.MaxBody)).Decode(&body); err != nil {
		deny(http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	base.Signal = body.Signal
	from, okF, err1 := timeArg(body.From)
	to, okT, err2 := timeArg(body.To)
	if err1 != nil || err2 != nil || !okF || !okT {
		deny(http.StatusBadRequest, "bad_window", "from and to are required: RFC 3339 or integer ns")
		return
	}
	if body.Tail && body.Basis == "" {
		deny(http.StatusBadRequest, "tail_needs_basis", "tail is what was received at or after a basis: ask with basis (\"latest\" or a token)")
		return
	}
	preq := lake.Request{Signal: body.Signal, FromNs: from, ToNs: to, Clusters: body.Clusters, TraceID: body.TraceID, Terms: body.Terms, Tail: body.Tail}
	var rb *resolvedBasis
	if body.Basis != "" {
		if !lake.Signals[body.Signal] {
			deny(http.StatusBadRequest, "bad_signal", fmt.Sprintf("signal %q is not a lane namespace", body.Signal))
			return
		}
		for _, c := range body.Clusters {
			if !sqlscope.ClusterRE.MatchString(c) {
				deny(http.StatusBadRequest, "bad_cluster", fmt.Sprintf("cluster %q is not a valid name", c))
				return
			}
			if !p.MayCluster(c) {
				deny(http.StatusForbidden, "cluster_not_in_scope", fmt.Sprintf("cluster %q is not in the token's scope", c))
				return
			}
		}
		var rf *basis.Refusal
		if rb, rf = s.useBasis(r.Context(), p, body.Basis, body.Clusters, []string{body.Signal}, true, &from); rf != nil {
			deny(rf.Status, rf.Reason, rf.Detail)
			return
		}
		preq.Basis = rb.b
		if len(preq.Clusters) == 0 && rb.clusters != nil {
			preq.Clusters = rb.clusters
		}
		base.Basis = bounds(rb.b, rb.clusters)
	}
	plan, err := s.Planner.Plan(r.Context(), p, preq)
	if err != nil {
		var rf *basis.Refusal
		if errors.As(err, &rf) {
			deny(rf.Status, rf.Reason, rf.Detail)
			return
		}
		var d *lake.Denied
		var b *lake.BadRequest
		var tl *lake.TooLarge
		switch {
		case errors.As(err, &d):
			deny(http.StatusForbidden, d.Reason, d.Detail)
		case errors.As(err, &b):
			deny(http.StatusBadRequest, b.Reason, b.Detail)
		case errors.As(err, &tl):
			deny(http.StatusRequestEntityTooLarge, "plan_too_large", tl.Error())
		default:
			rec := base
			rec.Decision, rec.Reason, rec.Error = "deny", "store_error", truncate(err.Error(), 2000)
			s.write(rec)
			s.fail(w, ep, rq.id, http.StatusBadGateway, "store_error", err.Error())
		}
		return
	}
	if rb != nil {
		tok := rb.token
		plan.Answer = basis.Answer{Basis: &tok, AtBasis: true, BasisInfo: rb.b.View()}
	} else if b, ok := mintFrom(plan.State, plan.WmScope, s.Watermark.MaxLateness(), s.Now()); ok && s.Bases != nil {
		plan.Answer = s.currentAnswer(r.Context(), ep, b)
	}
	allow := base
	allow.Decision, allow.Objects, allow.Bytes, allow.ObjectsHash, allow.ExpiresAt = "allow", len(plan.Objects), plan.TotalBytes, plan.ObjectsHash, plan.ExpiresAt
	allow.Clusters = plan.Clusters
	for _, o := range append(plan.Objects[:len(plan.Objects):len(plan.Objects)], plan.TailObjects...) {
		if len(allow.Keys) >= AuditKeys {
			break
		}
		allow.Keys = append(allow.Keys, o.Key)
	}
	if t := plan.Tail; t != nil {
		allow.TailObjects, allow.TailBytes, allow.TailObjectsHash = t.Objects, t.Bytes, t.ObjectsHash
	}
	if !s.write(allow) {
		s.fail(w, ep, rq.id, http.StatusServiceUnavailable, "audit_unavailable", "the decision could not be recorded, so no URL was issued")
		return
	}
	s.Metrics.Add("qs_planned_objects_total", float64(len(plan.Objects)+len(plan.TailObjects)))
	s.Metrics.Add("qs_planned_bytes_total", float64(plan.TotalBytes))
	if t := plan.Tail; t != nil {
		s.Metrics.Add("qs_planned_bytes_total", float64(t.Bytes))
		s.Metrics.Add("qs_plan_tail_objects_total", float64(t.Objects))
		s.Metrics.Add("qs_plan_tail_unplaced_total", float64(t.Unplaced))
	}
	s.Metrics.Add("qs_plan_mismatched_objects_total", float64(plan.Mismatched))
	if ix := plan.Index; ix != nil {
		s.Metrics.Add("qs_plan_index_objects_total", float64(ix.Covered-ix.Pruned), "hit")
		s.Metrics.Add("qs_plan_index_objects_total", float64(ix.Scan), "scan")
		s.Metrics.Add("qs_plan_index_objects_total", float64(ix.Pruned), "pruned")
		s.Metrics.Add("qs_plan_index_errors_total", float64(len(ix.Errors)))
		s.Metrics.Add("qs_plan_index_bytes_total", float64(ix.Bytes))
	}
	s.Metrics.Inc("qs_results_total", "lake", plan.Completeness)
	if plan.LateObjects > 0 || (plan.Tail != nil && plan.Tail.LateObjects > 0) {
		s.Metrics.Inc("qs_late_results_total", "lake", plan.Completeness)
	}
	s.reply(w, ep, http.StatusOK, PlanResponse{RequestID: rq.id, Plan: plan})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	st := s.Watermark.Get(r.Context())
	body := map[string]any{"status": "ok", "watermark": st.Status}
	if st.Doc != nil {
		body["watermark_age_s"] = s.Now().Sub(time.UnixMilli(int64(st.Doc.WallMs))).Seconds()
	}
	if st.Err != "" {
		body["watermark_error"] = st.Err
	}
	var parts []string
	if s.Planner != nil {
		parts = append(parts, "plan")
	}
	parts = append(parts, "query")
	sort.Strings(parts)
	body["apis"] = strings.Join(parts, ",")
	// liveness: the process answers. The watermark's state is reported,
	// not failed on: a stale watermark labels results, it does not stop them.
	s.reply(w, "healthz", http.StatusOK, body)
}
