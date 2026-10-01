package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/casselc/observability/otel-chdb/query/internal/auth"
	"github.com/casselc/observability/otel-chdb/query/internal/central"
	"github.com/casselc/observability/otel-chdb/query/internal/persons"
	"github.com/casselc/observability/otel-chdb/testgate/tracetag"
	"github.com/golang-jwt/jwt/v5"
)

const (
	pTenant = "11111111-2222-3333-4444-555555555555"
	pAlice  = "aaaaaaaa-0000-0000-0000-00000000000a"
	pBob    = "aaaaaaaa-0000-0000-0000-00000000000b"
	pCarol  = "aaaaaaaa-0000-0000-0000-00000000000c"
)

// personsCH answers the person events table's SELECT from rows.
type personsCH struct{ rows []persons.Event }

func (p *personsCH) Query(_ context.Context, sql string, _ url.Values) ([]byte, central.Summary, error) {
	var data [][]any
	for _, e := range p.rows {
		if strings.Contains(sql, "'"+e.OID+"'") {
			data = append(data, []any{pTenant, e.OID, e.Cluster, e.Namespace, fmt.Sprint(e.ValidFrom), fmt.Sprint(e.ValidTo),
				fmt.Sprint(e.SystemFrom), fmt.Sprint(e.Seq), e.Source, e.Kind, e.Name})
		}
	}
	b, _ := json.Marshal(map[string]any{"data": data})
	return b, central.Summary{}, nil
}

// /v1/persons (research/grants.md §9.3, O-G9): a resolve_person holder on
// devtools/dev-a sees the names of oids seen there, the pseudonym of one who
// left (also as of a time before the departure was recorded), and the bare
// oid of anyone else; a caller without the role resolves only themselves;
// the audit records the oids asked and never a name or pseudonym.
func TestPersonsEndpoint(t *testing.T) {
	tracetag.Covers(t, "P", "H-G8", "R-G9", "SEC-G7", "UCA-G7", "L-G1")
	f := newFixture(t)
	now := f.now.UnixMilli()
	dep := now - 60_000 // Alice's departure recorded a minute ago
	ch := &personsCH{rows: []persons.Event{
		{OID: pAlice, ValidFrom: 0, ValidTo: persons.Open, SystemFrom: 1000, Seq: 1, Source: persons.SrcController, Kind: persons.KindAssert, Name: "Alice Example"},
		{OID: pAlice, Cluster: "devtools", Namespace: "dev-a", ValidFrom: 5000, ValidTo: 5001, SystemFrom: 5000, Seq: 2, Source: persons.SrcAnnounce, Kind: persons.KindAssert},
		{OID: pAlice, ValidFrom: -1 << 63, ValidTo: persons.Open, SystemFrom: dep, Seq: 3, Source: persons.SrcSteward, Kind: persons.KindPseudonymise, Name: "departed-abcdefghijklmnop"},
		{OID: pBob, ValidFrom: 0, ValidTo: persons.Open, SystemFrom: 1000, Seq: 4, Source: persons.SrcController, Kind: persons.KindAssert, Name: "Bob Example"},
		{OID: pBob, Cluster: "devtools", Namespace: "dev-a", ValidFrom: 6000, ValidTo: 6001, SystemFrom: 6000, Seq: 5, Source: persons.SrcAnnounce, Kind: persons.KindAssert},
		{OID: pCarol, ValidFrom: 0, ValidTo: persons.Open, SystemFrom: 1000, Seq: 6, Source: persons.SrcController, Kind: persons.KindAssert, Name: "Carol Example"},
		{OID: pCarol, Cluster: "devtools", Namespace: "dev-b", ValidFrom: 6000, ValidTo: 6001, SystemFrom: 6000, Seq: 7, Source: persons.SrcAnnounce, Kind: persons.KindAssert},
	}}
	st, err := persons.NewStore(persons.Config{Database: "persons", Tenant: pTenant}, ch)
	if err != nil {
		t.Fatal(err)
	}
	f.srv.Persons = st
	f.srv.Mapping.Groups["dev-a-leads"] = auth.Grant{Tuples: []auth.Tuple{{Role: auth.RolePerson, Cluster: "devtools", Namespace: "dev-a"}}}
	lead := f.token(jwt.MapClaims{"groups": []any{"dev-a-leads"}})
	ask := func(tok string, body map[string]any) (int, []map[string]any, map[string]any) {
		t.Helper()
		code, out := f.post(t, "/v1/persons", tok, body)
		var ps []map[string]any
		if l, ok := out["persons"].([]any); ok {
			for _, x := range l {
				ps = append(ps, x.(map[string]any))
			}
		}
		return code, ps, out
	}
	code, ps, out := ask(lead, map[string]any{"oids": []string{pAlice, strings.ToUpper(pBob), pCarol, "aaaaaaaa-0000-0000-0000-0000000000ff"}, "valid_at_ms": 5500})
	if code != 200 || len(ps) != 4 {
		t.Fatalf("%d %v", code, out)
	}
	if ps[0]["name"] != "departed-abcdefghijklmnop" || ps[0]["pseudonymised"] != true {
		t.Fatalf("alice (departed): %v", ps[0])
	}
	if ps[1]["name"] != "Bob Example" || ps[1]["oid"] != pBob {
		t.Fatalf("bob (seen in dev-a): %v", ps[1])
	}
	for _, p := range ps[2:] { // carol: seen only in dev-b; an unknown oid: the same answer
		if p["resolved"] != false || p["name"] != nil || len(p) != 2 {
			t.Fatalf("not entitled / unknown: %v", p)
		}
	}
	// as of a basis before the departure was recorded: still the pseudonym
	if _, ps, _ := ask(lead, map[string]any{"oids": []string{pAlice}, "valid_at_ms": 5500, "as_of_ms": dep - 1}); ps[0]["name"] != "departed-abcdefghijklmnop" {
		t.Fatalf("alice at an old basis: %v", ps[0])
	}
	// no role: themselves only
	self := f.token(jwt.MapClaims{"sub": pBob})
	if _, ps, _ := ask(self, map[string]any{"oids": []string{pBob, pAlice}, "valid_at_ms": 5500}); ps[0]["name"] != "Bob Example" || ps[1]["resolved"] != false {
		t.Fatalf("self: %v", ps)
	}
	// refusals
	if code, _, out := ask(lead, map[string]any{"oids": []string{"alice' OR 1=1"}}); code != 400 || out["error"] != "bad_oids" {
		t.Fatalf("hostile oid: %d %v", code, out)
	}
	if code, _, out := ask(lead, map[string]any{"oids": []string{pAlice}, "as_of_ms": now + 1}); code != 400 || out["error"] != "bad_as_of" {
		t.Fatalf("future: %d %v", code, out)
	}
	// the audit: oids, never names
	recs := f.sink.Snapshot()
	sawOIDs := false
	for _, r := range recs {
		b, _ := json.Marshal(r)
		if strings.Contains(string(b), "Example") || strings.Contains(string(b), "departed-") {
			t.Fatalf("a name in the audit: %s", b)
		}
		if r.Action == "resolve_person" && r.Event == "decision" && len(r.OIDs) == 4 {
			sawOIDs = true
		}
	}
	if !sawOIDs {
		t.Fatal("no decision record with the oids asked")
	}
	// not configured
	f.srv.Persons = nil
	if code, _, out := ask(lead, map[string]any{"oids": []string{pAlice}}); code != 404 || out["error"] != "persons_not_configured" {
		t.Fatalf("%d %v", code, out)
	}
}
