package persons

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/casselc/observability/otel-chdb/entities/bitemp"
	"github.com/casselc/observability/otel-chdb/query/internal/central"
	"github.com/casselc/observability/otel-chdb/query/internal/sqlscope"
	"github.com/casselc/observability/otel-chdb/testgate/tracetag"
	"pgregory.net/rapid"
)

const tenant = "11111111-2222-3333-4444-555555555555"

var oids = []string{
	"aaaaaaaa-0000-0000-0000-000000000001",
	"aaaaaaaa-0000-0000-0000-000000000002",
	"aaaaaaaa-0000-0000-0000-000000000003",
}

var policy = bitemp.Policy{TrustWindow: 20}

// fakeCH is the person events table in memory, with the answer faults of
// AMBIGUITY G6: an insert that applies and then loses its answer, one that
// fails before applying, a read that fails. system_from is the store's
// arrival clock; insert_deduplication_token makes a retried insert one row.
type fakeCH struct {
	mu     sync.Mutex
	rows   []Event
	tokens map[string]bool
	clock  int64
	// faults, consumed in order: "lost" (applied, error), "fail" (not
	// applied, error), "readfail"
	faults []string
}

var (
	reOID  = regexp.MustCompile(`'([0-9a-f-]{36})'`)
	rePseu = regexp.MustCompile(`'(departed-[a-z2-7]+)'`)
)

func (f *fakeCH) next(kind string) bool {
	if len(f.faults) > 0 && (f.faults[0] == kind || (kind == "insert" && (f.faults[0] == "lost" || f.faults[0] == "fail"))) {
		return true
	}
	return false
}

func (f *fakeCH) Query(_ context.Context, sql string, st url.Values) ([]byte, central.Summary, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.clock++
	if strings.HasPrefix(sql, "INSERT") {
		fault := ""
		if f.next("insert") {
			fault, f.faults = f.faults[0], f.faults[1:]
		}
		if fault == "fail" {
			return nil, central.Summary{}, errors.New("connection refused")
		}
		tok := st.Get("insert_deduplication_token")
		if tok == "" || !f.tokens[tok] {
			if f.tokens == nil {
				f.tokens = map[string]bool{}
			}
			f.tokens[tok] = tok != ""
			m := reOID.FindAllStringSubmatch(sql, -1)
			f.rows = append(f.rows, Event{Tenant: m[0][1], OID: m[1][1], ValidFrom: -1 << 63, ValidTo: Open, SystemFrom: f.clock,
				Seq: uint64(len(f.rows) + 1), Source: SrcSteward, Kind: KindPseudonymise, Name: rePseu.FindStringSubmatch(sql)[1]})
		}
		if fault == "lost" {
			return nil, central.Summary{}, errors.New("context deadline exceeded")
		}
		return nil, central.Summary{}, nil
	}
	if f.next("readfail") {
		f.faults = f.faults[1:]
		return nil, central.Summary{}, errors.New("read timeout")
	}
	var data [][]any
	for _, e := range f.rows {
		if !strings.Contains(sql, "'"+e.OID+"'") {
			continue
		}
		data = append(data, []any{e.Tenant, e.OID, e.Cluster, e.Namespace, fmt.Sprint(e.ValidFrom), fmt.Sprint(e.ValidTo),
			fmt.Sprint(e.SystemFrom), fmt.Sprint(e.Seq), e.Source, e.Kind, e.Name})
	}
	b, _ := json.Marshal(map[string]any{"data": data})
	return b, central.Summary{}, nil
}

func (f *fakeCH) add(e Event) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.clock++
	if e.SystemFrom == 0 {
		e.SystemFrom = f.clock
	}
	e.Tenant, e.Seq = tenant, uint64(len(f.rows)+1)
	f.rows = append(f.rows, e)
}

func newStore(t interface{ Fatal(...any) }, f *fakeCH) *Store {
	s, err := NewStore(Config{Database: "persons", Tenant: tenant}, f)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func name(oid string) string { return "Person " + oid[len(oid)-1:] }

// A departure signal under lost answers, failed inserts and failed reads,
// repeated, retried and sent again under another key (AMBIGUITY G6; the
// class of CAST 50 and 74): the store holds the first correction, every
// confirmed outcome names it, and every answer at every system time shows
// it, never the name.
func TestPseudonymiseIsIdempotent(t *testing.T) {
	tracetag.Covers(t, "P", "H-G8", "R-G9", "H-E8")
	rapid.Check(t, func(t *rapid.T) {
		f := &fakeCH{}
		s := newStore(t, f)
		oid := oids[0]
		f.add(Event{OID: oid, ValidFrom: 0, ValidTo: Open, Source: SrcController, Kind: KindAssert, Name: name(oid)})
		f.add(Event{OID: oid, Cluster: "devtools", Namespace: "dev-a", ValidFrom: 5, ValidTo: 6, Source: SrcAnnounce, Kind: KindAssert})
		keys := [][]byte{[]byte("key-one-0123456789"), []byte("key-two-0123456789")}
		var confirmed []Outcome
		runs := rapid.IntRange(1, 4).Draw(t, "runs")
		for i := 0; i < runs; i++ {
			f.faults = rapid.SliceOfN(rapid.SampledFrom([]string{"lost", "fail", "readfail", "none"}), 0, 4).Draw(t, "faults")
			key := keys[rapid.IntRange(0, 1).Draw(t, "key")]
			out, err := s.Pseudonymise(context.Background(), oid, key, "ticket-1", 3, 0)
			if err == nil {
				confirmed = append(confirmed, out)
			} else if !errors.Is(err, ErrUnknown) && out.Attempts > 0 {
				// only the first read may fail definitely (nothing sent yet)
				t.Fatalf("run %d: %v after %d inserts, not reported as unknown", i, err, out.Attempts)
			}
			if rapid.Bool().Draw(t, "lateName") { // a Graph delta still naming them, after the signal
				f.add(Event{OID: oid, ValidFrom: 0, ValidTo: Open, Source: SrcController, Kind: KindAssert, Name: name(oid)})
			}
		}
		f.faults = nil
		if _, err := s.Pseudonymise(context.Background(), oid, keys[0], "ticket-1", 3, 0); err != nil { // settles it
			t.Fatal(err)
		}
		evs, _ := s.Events(context.Background(), []string{oid})
		var first *Event
		corrections := 0
		for i := range evs {
			if evs[i].Kind == KindPseudonymise {
				corrections++
				if first == nil {
					first = &evs[i]
				}
			}
		}
		if corrections != 1 {
			t.Fatalf("%d corrections stored for one signal (the dedup token makes a retry one row)", corrections)
		}
		for _, o := range confirmed {
			if o.Pseudonym != first.Name {
				t.Fatalf("an outcome named %q, the store holds %q", o.Pseudonym, first.Name)
			}
		}
		r := Reader{Self: oid}
		for st := int64(0); st <= f.clock+1; st++ {
			for _, vt := range []int64{0, 5, 100} {
				a := Resolve(evs, tenant, []string{oid}, r, vt, st, policy)[0]
				if a.Name != first.Name || !a.Pseudonymised || a.PseudonymisedAt != first.SystemFrom {
					t.Fatalf("vt %d as of %d: %+v, want the pseudonym %q", vt, st, a, first.Name)
				}
			}
		}
	})
}

// Who sees what (research/grants.md §9.3, SEC-G7) and O-G9 at every basis:
// a name or pseudonym only for the caller themselves or a resolve_person
// holder on a (cluster, namespace) the oid was seen in; a forbidden oid and
// an unknown one get the same answer; a corrected oid never shows a name,
// at any system time; the lifecycle state is the uncorrected one.
func TestResolveEntitlementAndPseudonym(t *testing.T) {
	tracetag.Covers(t, "P", "H-G8", "R-G9", "SEC-G7", "UCA-G7")
	nss := []string{"dev-a", "dev-b", "dev-c"}
	rapid.Check(t, func(t *rapid.T) {
		var evs []Event
		clock := int64(0)
		n := rapid.IntRange(0, 14).Draw(t, "n")
		for i := 0; i < n; i++ {
			clock += int64(rapid.IntRange(0, 3).Draw(t, "dst"))
			oid := rapid.SampledFrom(oids).Draw(t, "oid")
			e := Event{Tenant: tenant, OID: oid, SystemFrom: clock, Seq: uint64(i + 1), ValidTo: Open}
			switch rapid.IntRange(0, 4).Draw(t, "what") {
			case 0, 1:
				e.Source, e.Kind, e.Name = SrcController, KindAssert, name(oid)+rapid.SampledFrom([]string{"", " (renamed)"}).Draw(t, "nm")
				e.ValidFrom = int64(rapid.IntRange(0, 20).Draw(t, "vf"))
			case 2:
				e.Source, e.Kind = SrcController, KindRetract
				e.ValidFrom = int64(rapid.IntRange(0, 20).Draw(t, "vf"))
			case 3:
				e.Source, e.Kind, e.Cluster, e.Namespace = SrcAnnounce, KindAssert, "devtools", rapid.SampledFrom(nss).Draw(t, "ns")
				e.ValidFrom = int64(rapid.IntRange(0, 20).Draw(t, "vf"))
				e.ValidTo = e.ValidFrom + 1
			case 4:
				e.Source, e.Kind, e.Name = SrcSteward, KindPseudonymise, Pseudonym([]byte(rapid.SampledFrom([]string{"k1-0123456789abcdef", "k2-0123456789abcdef"}).Draw(t, "k")), tenant, oid)
				e.ValidFrom = -1 << 63
			}
			evs = append(evs, e)
		}
		var pairs []sqlscope.Pair
		for _, ns := range nss {
			if rapid.Bool().Draw(t, "holds-"+ns) {
				pairs = append(pairs, sqlscope.Pair{Cluster: "devtools", Namespace: ns})
			}
		}
		r := Reader{Self: rapid.SampledFrom(append([]string{""}, oids...)).Draw(t, "self"), Pairs: pairs}
		for st := int64(0); st <= clock+1; st++ {
			for _, vt := range []int64{0, 7, 15, 30} {
				ans := Resolve(evs, tenant, oids, r, vt, st, policy)
				for i, a := range ans {
					oid := oids[i]
					may, has, corrected := oid == r.Self, false, ""
					for _, e := range evs {
						if e.OID != oid {
							continue
						}
						has = true
						if e.Source == SrcAnnounce && Reader.holds(Reader{Pairs: pairs}, e.Cluster, e.Namespace) {
							may = true
						}
						if e.Kind == KindPseudonymise && corrected == "" {
							corrected = e.Name // the first by arrival
						}
					}
					if a.Resolved != (may && has) {
						t.Fatalf("%s: resolved %v, may %v has %v", oid, a.Resolved, may, has)
					}
					if !a.Resolved && (a.Name != "" || a.State != "" || a.Pseudonymised) {
						t.Fatalf("%s: an unresolved answer says more than the oid: %+v", oid, a)
					}
					if a.Resolved && corrected != "" && (a.Name != corrected || !a.Pseudonymised) {
						t.Fatalf("%s as of %d: %+v, corrected to %q", oid, st, a, corrected)
					}
					if strings.HasPrefix(a.Name, "Person") && corrected != "" {
						t.Fatalf("%s: a departed person's name: %+v", oid, a)
					}
				}
			}
		}
	})
}

func TestPseudonymIsStableAndKeyed(t *testing.T) {
	tracetag.Covers(t, "P", "H-G8", "H-E8")
	a := Pseudonym([]byte("k1"), tenant, oids[0])
	if a != Pseudonym([]byte("k1"), strings.ToUpper(tenant), strings.ToUpper(oids[0])) {
		t.Fatal("not stable under case")
	}
	if a == Pseudonym([]byte("k2"), tenant, oids[0]) || a == Pseudonym([]byte("k1"), tenant, oids[1]) {
		t.Fatal("not keyed by key and oid")
	}
	if !regexp.MustCompile(`^departed-[a-z2-7]{16}$`).MatchString(a) {
		t.Fatalf("form %q", a)
	}
	if _, err := NormaliseOID("x' OR 1=1 --"); err == nil {
		t.Fatal("a non-GUID oid accepted")
	}
	s := newStore(t, &fakeCH{})
	if _, err := s.Pseudonymise(context.Background(), oids[0], []byte("0123456789abcdef"), " ", 1, 0); err == nil {
		t.Fatal("no reason accepted")
	}
}
