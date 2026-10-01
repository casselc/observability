// Package persons resolves Entra object ids (the `user.id` the ingress
// stamps, D37) to display names from the D32 person events, for the query
// service's /v1/persons, and records a departure as a pseudonymise
// correction (owner decision O-G9: DECISIONS D38 items 11-17,
// research/grants.md §9.3 and §9.6).
//
// Resolution is entities/bitemp's: the person events of each oid are
// resolved at (valid time, system time) and pseudonymised by every
// correction recorded, whatever the system time asked: an answer at an old
// basis shows the pseudonym too (D38 item 13). Who may see a name (or a
// pseudonym, itself a person fact): the caller for their own oid, and a
// caller holding resolve_person on a (cluster, namespace) the oid was seen
// in. Everyone else gets the bare oid, with the same answer for an unknown
// oid and a forbidden one (no enumeration, SEC-G7).
package persons

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base32"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/casselc/observability/otel-chdb/entities/bitemp"
	"github.com/casselc/observability/otel-chdb/query/internal/sqlscope"
)

// Sources and kinds as the person events table stores them.
const (
	SrcController = "controller" // Graph delta (the person controller): names
	SrcOverseer   = "overseer"
	SrcAnnounce   = "announce" // the ingress: the oid was seen in (cluster, namespace)
	SrcSteward    = "steward"  // the departure process

	KindAssert       = "assert"
	KindRetract      = "retract"
	KindUnknown      = "unknown"
	KindPseudonymise = "pseudonymise"
)

// Event is one row of the person events table. Name is the asserted display
// name (controller), or the pseudonym (steward); empty otherwise. Cluster
// and Namespace are set on announcements only (where the oid was seen).
type Event struct {
	Tenant     string `json:"tenant"`
	OID        string `json:"oid"`
	Cluster    string `json:"cluster,omitempty"`
	Namespace  string `json:"namespace,omitempty"`
	ValidFrom  int64  `json:"valid_from"`
	ValidTo    int64  `json:"valid_to"`
	SystemFrom int64  `json:"system_from"`
	Seq        uint64 `json:"seq"`
	Source     string `json:"source"`
	Kind       string `json:"kind"`
	Name       string `json:"name,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

// Open is an open valid_to.
const Open = int64(bitemp.Inf)

var oidRE = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// NormaliseOID lower-cases an object id and checks it is a GUID (the only
// form the ingress stamps; anything else is refused, never interpolated).
func NormaliseOID(s string) (string, error) {
	s = strings.ToLower(strings.TrimSpace(s))
	if !oidRE.MatchString(s) {
		return "", fmt.Errorf("not an Entra object id: %q", s)
	}
	return s, nil
}

// Pseudonym is the stable pseudonym of (tenant, oid) under key: "departed-"
// and 16 base32 characters of HMAC-SHA256(key, tenant "/" oid). Keyed so a
// list of oids from another system cannot be mapped onto pseudonyms by
// someone who sees only pseudonyms (D38 item 14). Stored in the correction,
// so resolution never needs the key.
func Pseudonym(key []byte, tenant, oid string) string {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(strings.ToLower(tenant) + "/" + strings.ToLower(oid)))
	enc := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(m.Sum(nil)[:10])
	return "departed-" + strings.ToLower(enc)
}

// DefaultPolicy is D32's precedence for person facts: the person controller
// re-reads Graph every round, so a 20-minute trust window (two rounds).
var DefaultPolicy = bitemp.Policy{TrustWindow: 20 * 60 * 1000}

// Reader is who asks: their own oid (the token's subject) and their
// resolve_person grants.
type Reader struct {
	Self  string
	Pairs []sqlscope.Pair
}

func (r Reader) holds(cluster, namespace string) bool {
	for _, p := range r.Pairs {
		if (p.Cluster == "*" || p.Cluster == cluster) && (p.Namespace == "*" || p.Namespace == namespace) {
			return true
		}
	}
	return false
}

// Answer is one oid's resolution as the reader may see it.
type Answer struct {
	OID string `json:"oid"`
	// Resolved: false when the reader may not see this person, or the
	// catalog has nothing on them (indistinguishable on purpose).
	Resolved bool   `json:"resolved"`
	State    string `json:"state,omitempty"` // asserted | retracted | unknown | absent
	// Name: the display name, or the pseudonym when Pseudonymised.
	Name          string `json:"name,omitempty"`
	Pseudonymised bool   `json:"pseudonymised,omitempty"`
	// PseudonymisedAt: the correction's system time (ms); an answer at a
	// basis before it changed when it was recorded, and says so.
	PseudonymisedAt int64  `json:"pseudonymised_at,omitempty"`
	Source          string `json:"source,omitempty"`
	Uncertain       bool   `json:"uncertain,omitempty"`
}

// Resolve answers each oid (normalised, in order) at valid time vt as of
// system time st, from evs (the events of those oids; any order).
func Resolve(evs []Event, tenant string, oids []string, r Reader, vt, st int64, p bitemp.Policy) []Answer {
	by := map[string][]Event{}
	for _, e := range evs {
		if strings.EqualFold(e.Tenant, tenant) {
			by[strings.ToLower(e.OID)] = append(by[strings.ToLower(e.OID)], e)
		}
	}
	out := make([]Answer, 0, len(oids))
	for _, oid := range oids {
		out = append(out, resolveOne(by[oid], oid, r, vt, st, p))
	}
	return out
}

func resolveOne(evs []Event, oid string, r Reader, vt, st int64, p bitemp.Policy) Answer {
	a := Answer{OID: oid}
	may := r.Self != "" && strings.EqualFold(r.Self, oid)
	for _, e := range evs {
		if !may && e.Source == SrcAnnounce && e.Kind == KindAssert && r.holds(e.Cluster, e.Namespace) {
			may = true
		}
	}
	if !may || len(evs) == 0 {
		return a
	}
	// arrival order, then local seqs (unique, as bitemp needs); names to versions
	sort.SliceStable(evs, func(i, j int) bool {
		if evs[i].SystemFrom != evs[j].SystemFrom {
			return evs[i].SystemFrom < evs[j].SystemFrom
		}
		return evs[i].Seq < evs[j].Seq
	})
	names := []string{""}
	version := func(n string) uint64 {
		for i, x := range names {
			if x == n {
				return uint64(i)
			}
		}
		names = append(names, n)
		return uint64(len(names) - 1)
	}
	bev := make([]bitemp.Event, 0, len(evs))
	for i, e := range evs {
		b := bitemp.Event{Entity: 1, ValidFrom: bitemp.Time(e.ValidFrom), ValidTo: bitemp.Time(e.ValidTo),
			SystemFrom: bitemp.Time(e.SystemFrom), Seq: uint64(i + 1)}
		switch e.Source {
		case SrcController:
			b.Source = bitemp.Controller
		case SrcOverseer:
			b.Source = bitemp.Overseer
		case SrcAnnounce:
			b.Source = bitemp.Announce
		case SrcSteward:
			b.Source = bitemp.Steward
		default:
			continue
		}
		switch e.Kind {
		case KindAssert:
			b.Kind, b.Version = bitemp.Assert, version(e.Name)
		case KindRetract:
			b.Kind = bitemp.Retract
		case KindUnknown:
			b.Kind = bitemp.Unknown
		case KindPseudonymise:
			if e.Source != SrcSteward || e.Name == "" {
				continue // only the steward corrects, and only with a pseudonym
			}
			b.Kind, b.Version, b.ValidFrom, b.ValidTo = bitemp.Pseudonymise, version(e.Name), 0, bitemp.Inf
		default:
			continue
		}
		if b.Kind != bitemp.Pseudonymise && (b.Source == bitemp.Steward || b.ValidFrom >= b.ValidTo) {
			continue
		}
		bev = append(bev, b)
	}
	row := bitemp.ResolveNamed(bev, 1, bitemp.Time(vt), bitemp.Time(st), p)
	a.Resolved, a.State = true, row.State.String()
	if row.State == bitemp.Asserted || row.State == bitemp.Retracted {
		a.Source = row.Source.String()
	}
	a.Uncertain = row.Uncertain
	if c, ok := bitemp.FirstCorrection(bev, 1); ok {
		// the person left: the pseudonym, whatever the state, never the name
		a.Pseudonymised, a.PseudonymisedAt, a.Name = true, int64(c.SystemFrom), names[c.Version]
	} else if row.State == bitemp.Asserted {
		a.Name = names[row.Version]
	}
	return a
}
