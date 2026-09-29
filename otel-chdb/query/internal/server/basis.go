package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"time"

	"github.com/casselc/observability/otel-chdb/query/internal/audit"
	"github.com/casselc/observability/otel-chdb/query/internal/auth"
	"github.com/casselc/observability/otel-chdb/query/internal/basis"
	"github.com/casselc/observability/otel-chdb/query/internal/completeness"
	"github.com/casselc/observability/otel-chdb/query/internal/sqlscope"
)

// Latest is the basis value that asks the service to mint one from the
// current watermark and compute the answer at it.
const Latest = "latest"

// DefaultRetention is basis.retention_s when none is configured: the
// fleet's 90 days (DECISIONS §1.3). A basis older than this, or a window
// starting before it, is basis_expired.
const DefaultRetention = 90 * 24 * time.Hour

// DefaultBasisSkew bounds how far a row's received_at may precede its
// event time (the lake planner's skew_s): a window's rows were received no
// earlier than its start minus this.
const DefaultBasisSkew = 300 * time.Second

// mintFrom makes a basis from a watermark state narrowed to sc (For): per
// cluster of sc the value For computed, or the fleet value when sc names
// no clusters. ok false when the watermark is unknown (no document).
func mintFrom(st completeness.State, sc completeness.Scope, maxLate time.Duration, now time.Time) (*basis.Basis, bool) {
	if st.Doc == nil {
		return nil, false
	}
	b := &basis.Basis{Version: basis.Version, IssuedNs: now.UnixNano(), Clusters: map[string]uint64{},
		Signals: sc.Signals, MaxLatenessNs: int64(max(maxLate, 0))}
	if len(b.Signals) > 0 {
		b.Signals = slices.Clone(b.Signals)
		slices.Sort(b.Signals)
		b.Signals = slices.Compact(b.Signals)
	} else {
		b.Signals = nil
	}
	if sc.Clusters == nil {
		b.Clusters[basis.Fleet] = st.Doc.CompleteThroughNs
		return b, true
	}
	if st.Scope == nil || len(sc.Clusters) == 0 {
		return nil, false
	}
	for _, cv := range st.Scope.By {
		b.Clusters[cv.Cluster] = cv.CompleteThroughNs
	}
	for _, c := range sc.Clusters {
		if _, ok := b.Clusters[c]; !ok {
			return nil, false
		}
	}
	return b, true
}

// BasisCodec mints and verifies basis tokens: *basis.Bases (a static
// keyring or KMS, with caches) or a bare *basis.Keyring. Mint may set b's
// IssuedNs (a reused recent token); both return errors wrapping
// basis.ErrInvalid (the token is not the service's) or basis.ErrUnavailable
// (the signer could not mint or decide: 503, never valid).
type BasisCodec interface {
	Mint(ctx context.Context, b *basis.Basis) (string, error)
	Open(ctx context.Context, tok string) (*basis.Basis, error)
}

// ObserveSigner counts a basis signer call (basis.Options.Observe).
func (s *Server) ObserveSigner(op, result string) {
	if s.Metrics != nil {
		s.Metrics.Inc("qs_basis_signer_calls_total", op, result)
	}
}

// currentAnswer is a plain answer's basis block: the current basis of its
// scope, to pin later requests to. When the signer cannot mint (a KMS
// outage), the answer is still served, with basis null and
// basis_unavailable saying why: a plain answer reads up to now and does
// not depend on a basis, so a signer outage must not take queries down;
// only requests that ask for a basis ("latest", a token) are refused.
func (s *Server) currentAnswer(ctx context.Context, ep string, b *basis.Basis) basis.Answer {
	tok, err := s.Bases.Mint(ctx, b)
	if err != nil {
		if s.Metrics != nil {
			s.Metrics.Inc("qs_basis_unavailable_total", ep)
		}
		reason := basis.ReasonInvalid
		if errors.Is(err, basis.ErrUnavailable) {
			reason = basis.ReasonSignerUnavailable
		}
		return basis.Answer{BasisUnavailable: reason + ": " + err.Error()}
	}
	return basis.Answer{Basis: &tok, BasisInfo: b.View()}
}

// signerRefusal maps a Mint or Open error to its refusal.
func signerRefusal(err error) *basis.Refusal {
	if errors.Is(err, basis.ErrUnavailable) {
		return &basis.Refusal{Status: http.StatusServiceUnavailable, Reason: basis.ReasonSignerUnavailable,
			Detail: err.Error() + " (the basis could not be minted or checked; retry)"}
	}
	return basis.Invalid(err)
}

// current returns cluster c's complete_through now, for signals (c ==
// basis.Fleet: the fleet's), from one For over clusters.
func (s *Server) current(ctx context.Context, clusters []string, signals []string) func(string) (uint64, bool) {
	st := s.Watermark.For(ctx, completeness.Scope{Clusters: clusters, Signals: signals})
	return func(c string) (uint64, bool) {
		if st.Doc == nil {
			return 0, false
		}
		if c == basis.Fleet {
			return st.Doc.CompleteThroughNs, true
		}
		if st.Scope == nil {
			return 0, false
		}
		for _, cv := range st.Scope.By {
			if cv.Cluster == c {
				return cv.CompleteThroughNs, true
			}
		}
		return 0, false
	}
}

// resolvedBasis is a request's basis after every check.
type resolvedBasis struct {
	b        *basis.Basis
	token    string
	clusters []string // the clusters the request runs over; nil: the caller's whole scope (a fleet basis)
}

// useBasis turns a request's basis value into a checked basis: Latest mints
// one for the request's scope from the watermark now; a token is decoded
// and checked against the caller's scope (never widened), the signals the
// request reads (nil: every signal; telemetry false: the request reads no
// telemetry, so no signal is needed), the current watermark (never ahead)
// and retention (never expired).
func (s *Server) useBasis(ctx context.Context, p *auth.Principal, val string, asked []string, signals []string, telemetry bool,
	windowFromNs *int64) (*resolvedBasis, *basis.Refusal) {
	if s.Bases == nil {
		return nil, &basis.Refusal{Status: http.StatusNotImplemented, Reason: "basis_disabled", Detail: "this service has no basis key"}
	}
	now := s.Now()
	var rb resolvedBasis
	if val == Latest {
		var clusters []string
		switch {
		case len(asked) > 0:
			clusters = asked
		case !p.AllClusters:
			clusters = p.Clusters
		}
		sc := completeness.Scope{Clusters: clusters, Signals: signals}
		b, ok := mintFrom(s.Watermark.For(ctx, sc), sc, s.Watermark.MaxLateness(), now)
		if !ok {
			return nil, &basis.Refusal{Status: http.StatusServiceUnavailable, Reason: basis.ReasonUnverifiable,
				Detail: "the watermark is unknown: no basis can be issued"}
		}
		tok, err := s.Bases.Mint(ctx, b)
		if err != nil {
			if errors.Is(err, basis.ErrUnavailable) {
				return nil, signerRefusal(err)
			}
			return nil, &basis.Refusal{Status: http.StatusInternalServerError, Reason: basis.ReasonInvalid, Detail: "the basis could not be encoded: " + err.Error()}
		}
		rb = resolvedBasis{b: b, token: tok, clusters: clusters}
	} else {
		b, err := s.Bases.Open(ctx, val)
		if err != nil {
			return nil, signerRefusal(err)
		}
		clusters, rf := basis.Resolve(b, p.MayCluster, p.AllClusters, asked)
		if rf != nil {
			return nil, rf
		}
		if telemetry {
			if rf := basis.CheckSignals(b, signals); rf != nil {
				return nil, rf
			}
		}
		if rf := basis.CheckCurrent(b, clusters, s.current(ctx, clusters, b.Signals)); rf != nil {
			return nil, rf
		}
		rb = resolvedBasis{b: b, token: val, clusters: clusters}
	}
	if rf := basis.CheckExpiry(rb.b, rb.clusters, now, s.retention(), windowFromNs, s.basisSkew()); rf != nil {
		return nil, rf
	}
	return &rb, nil
}

func (s *Server) retention() time.Duration {
	if s.Retention == 0 {
		return DefaultRetention
	}
	return s.Retention
}

func (s *Server) basisSkew() time.Duration {
	if s.BasisSkew <= 0 {
		return DefaultBasisSkew
	}
	return s.BasisSkew
}

// bounds is rb's bounds over the clusters the request reads, as the
// scope's table filters take them.
func bounds(b *basis.Basis, clusters []string) map[string]uint64 {
	out := map[string]uint64{}
	if b.IsFleet() || clusters == nil {
		for c, v := range b.Clusters {
			out[c] = v
		}
		return out
	}
	for _, c := range clusters {
		if v, ok := b.C(c); ok {
			out[c] = v
		}
	}
	return out
}

// telemetrySignals: the signals of the telemetry tables a statement reads
// (nil: every signal), and whether it reads any (a statement over
// metadata tables only needs no signal from a basis: schema rows have no
// custody time, and a basis leaves them unfiltered).
func telemetrySignals(tables []*sqlscope.Table) ([]string, bool) {
	var tele []*sqlscope.Table
	for _, t := range tables {
		if t.Scope != "metadata" {
			tele = append(tele, t)
		}
	}
	return sqlscope.SignalsOf(tele), len(tele) > 0
}

// BasisRequest is POST /v1/basis's body: the scope to mint a basis for.
type BasisRequest struct {
	// Clusters: a subset of the token's (default: all of them; a fleet
	// caller gets a fleet basis).
	Clusters []string `json:"clusters"`
	// Signals: the lane namespaces the basis will be used for (default:
	// every signal, which is valid for any table and is the lowest value).
	Signals []string `json:"signals"`
}

// BasisResponse is its answer.
type BasisResponse struct {
	RequestID string `json:"request_id"`
	basis.Answer
	Watermark completeness.WmInfo `json:"watermark"`
}

// handleBasis mints a basis for a scope without running anything: a
// dashboard (the HyperDX adapter, the lake UI) pins one per refresh and
// sends it with every panel's request.
func (s *Server) handleBasis(w http.ResponseWriter, r *http.Request) {
	const ep = "basis"
	rq := s.authenticateAny(w, r, ep, "basis")
	if rq == nil {
		return
	}
	p := rq.p
	cl, ns := scopeLists(p)
	base := audit.Record{RequestID: rq.id, Event: "decision", Action: "basis", Subject: p.Subject, Groups: p.Groups, Roles: p.Roles,
		Clusters: cl, Namespaces: ns, Pairs: pairsOf(p), ClientIP: rq.ip}
	deny := func(code int, reason, detail string) {
		rec := base
		rec.Decision, rec.Reason, rec.Detail = "deny", reason, detail
		s.write(rec)
		s.fail(w, ep, rq.id, code, reason, detail)
	}
	var body BasisRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, s.MaxBody)).Decode(&body); err != nil && err != io.EOF {
		deny(http.StatusBadRequest, "bad_request", err.Error())
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
	if !p.AllClusters && len(p.Clusters) == 0 {
		deny(http.StatusForbidden, "empty_scope", "the token grants no cluster")
		return
	}
	for _, sg := range body.Signals {
		if !sqlscope.SignalRE.MatchString(sg) {
			deny(http.StatusBadRequest, "bad_signal", fmt.Sprintf("signal %q is not a lane namespace name", sg))
			return
		}
	}
	rb, rf := s.useBasis(r.Context(), p, Latest, body.Clusters, body.Signals, true, nil)
	if rf != nil {
		deny(rf.Status, rf.Reason, rf.Detail)
		return
	}
	allow := base
	allow.Decision, allow.Basis = "allow", rb.b.Clusters
	if !s.write(allow) {
		s.fail(w, ep, rq.id, http.StatusServiceUnavailable, "audit_unavailable", "the decision could not be recorded")
		return
	}
	st := s.Watermark.For(r.Context(), completeness.Scope{Clusters: rb.clusters, Signals: rb.b.Signals})
	lbl := completeness.MakeLabel("basis", st, nil, s.Now(), s.Watermark.Key(), p.MayCluster, s.Watermark.MaxLateness())
	tok := rb.token
	s.reply(w, ep, http.StatusOK, BasisResponse{RequestID: rq.id, Answer: basis.Answer{Basis: &tok, BasisInfo: rb.b.View()},
		Watermark: lbl.Watermark})
}

// authenticateAny is authenticate for an endpoint either role may call.
func (s *Server) authenticateAny(w http.ResponseWriter, r *http.Request, endpoint, action string) *req {
	role := auth.RoleQuery
	if tok, ok := auth.Bearer(r.Header.Get("Authorization")); ok {
		if claims, err := s.Verifier.Verify(r.Context(), tok); err == nil {
			if p, err := s.Mapping.Principal(claims); err == nil && !p.Has(auth.RoleQuery) && p.Has(auth.RolePlan) {
				role = auth.RolePlan
			}
		}
	}
	rq := s.authenticate(w, r, endpoint, action, role)
	if rq != nil {
		// a basis is watermark bounds for clusters: either role's grants
		rq.p = rq.all.For(auth.RoleQuery, auth.RolePlan)
	}
	return rq
}
