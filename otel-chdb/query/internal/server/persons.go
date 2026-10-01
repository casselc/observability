package server

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"github.com/casselc/observability/otel-chdb/query/internal/audit"
	"github.com/casselc/observability/otel-chdb/query/internal/persons"
)

// PersonsRequest asks for the names of Entra object ids (D32 person;
// research/grants.md §9.3; O-G9).
type PersonsRequest struct {
	OIDs []string `json:"oids"`
	// ValidAtMs: the valid time asked (ms; default now): a row's event time
	// resolves the name the person had then.
	ValidAtMs *int64 `json:"valid_at_ms,omitempty"`
	// AsOfMs: the catalog's system time (ms; default now): the answer as the
	// catalog knew it then. A departure (pseudonymise) recorded later still
	// applies: no basis keeps a departed person's name (D38 item 13).
	AsOfMs *int64 `json:"as_of_ms,omitempty"`
}

// PersonsResponse is the answer, one entry per oid asked, in order.
type PersonsResponse struct {
	Persons   []persons.Answer `json:"persons"`
	ValidAtMs int64            `json:"valid_at_ms"`
	AsOfMs    int64            `json:"as_of_ms"`
	RequestID string           `json:"request_id"`
}

// handlePersons resolves oids for the caller: their own always, others only
// where the caller holds resolve_person on a (cluster, namespace) the oid
// was seen in; anyone else's is the bare oid (resolved: false). The audit
// records the oids asked, never what they resolved to.
func (s *Server) handlePersons(w http.ResponseWriter, r *http.Request) {
	const ep = "persons"
	rq := s.authenticate(w, r, ep, "resolve_person", "resolve_person")
	if rq == nil {
		return
	}
	p := rq.p
	cl, ns := scopeLists(p)
	base := audit.Record{RequestID: rq.id, Event: "decision", Action: "resolve_person", Subject: p.Subject, Groups: p.Groups, Roles: p.Roles,
		Clusters: cl, Namespaces: ns, Pairs: pairsOf(p), ClientIP: rq.ip}
	deny := func(code int, reason, detail string) {
		rec := base
		rec.Decision, rec.Reason, rec.Detail = "deny", reason, detail
		s.write(rec)
		s.fail(w, ep, rq.id, code, reason, detail)
	}
	if s.Persons == nil {
		deny(http.StatusNotFound, "persons_not_configured", "no person catalog is configured")
		return
	}
	var body PersonsRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, s.MaxBody)).Decode(&body); err != nil {
		deny(http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	max := s.Persons.Config().MaxOIDs
	if len(body.OIDs) == 0 || len(body.OIDs) > max {
		deny(http.StatusBadRequest, "bad_oids", "between 1 and "+strconv.Itoa(max)+" oids")
		return
	}
	oids := make([]string, len(body.OIDs))
	for i, o := range body.OIDs {
		n, err := persons.NormaliseOID(o)
		if err != nil {
			deny(http.StatusBadRequest, "bad_oids", err.Error())
			return
		}
		oids[i] = n
	}
	base.OIDs = oids
	now := s.Now().UnixMilli()
	vt, st := now, now
	if body.ValidAtMs != nil {
		vt = *body.ValidAtMs
	}
	if body.AsOfMs != nil {
		if st = *body.AsOfMs; st > now {
			deny(http.StatusBadRequest, "bad_as_of", "as_of_ms is in the future")
			return
		}
	}
	dec := base
	dec.Decision = "allow"
	if !s.write(dec) {
		s.fail(w, ep, rq.id, http.StatusServiceUnavailable, "audit_unavailable", "the decision could not be recorded")
		return
	}
	out := base
	out.Event = "outcome"
	evs, err := s.Persons.Events(r.Context(), oids)
	if err != nil {
		out.Status, out.Error = http.StatusBadGateway, err.Error()
		s.write(out)
		s.fail(w, ep, rq.id, http.StatusBadGateway, "persons_unavailable", err.Error())
		return
	}
	ans := persons.Resolve(evs, s.Persons.Config().Tenant, oids, persons.Reader{Self: p.Subject, Pairs: p.Pairs()}, vt, st, persons.DefaultPolicy)
	for _, a := range ans {
		if a.Resolved {
			out.Rows++
		}
	}
	out.Status = http.StatusOK
	s.write(out)
	s.reply(w, ep, http.StatusOK, PersonsResponse{Persons: ans, ValidAtMs: vt, AsOfMs: st, RequestID: rq.id})
}
