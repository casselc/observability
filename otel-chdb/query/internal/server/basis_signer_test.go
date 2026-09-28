package server

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/query/internal/basis"
)

// flakySigner is a signer that can be taken down (a KMS outage): the
// keyring's MAC while up, an error while down.
type flakySigner struct {
	*basis.Keyring
	down atomic.Bool
}

func (s *flakySigner) Kind() string { return "kms" }

func (s *flakySigner) MAC(ctx context.Context, kid string, msg []byte) ([]byte, error) {
	if s.down.Load() {
		return nil, errors.New("ThrottlingException: the fake KMS is down")
	}
	return s.Keyring.MAC(ctx, kid, msg)
}

func (s *flakySigner) Verify(ctx context.Context, kid string, msg, mac []byte) error {
	if s.down.Load() {
		return errors.New("KMSInternalException: the fake KMS is down")
	}
	return s.Keyring.Verify(ctx, kid, msg, mac)
}

// A signer outage (D30 amendment): plain answers are still served, with
// basis null and basis_unavailable; a request that needs a new basis is
// 503 basis_signer_unavailable; a token this replica verified (or minted)
// keeps working; one it never saw is 503, never answered and never
// called invalid. Refusals run nothing.
func TestBasisSignerOutage(t *testing.T) {
	f := newBasisFixture(t)
	fleet := f.token(fleetClaims)
	kr := basis.Ephemeral()
	sig := &flakySigner{Keyring: kr}
	clock := func() time.Time { return f.now }
	mk := func() *basis.Bases {
		bs, err := basis.New(sig, nil, basis.Options{Now: clock, Observe: f.srv.ObserveSigner})
		if err != nil {
			t.Fatal(err)
		}
		return bs
	}
	f.srv.Bases = mk()
	other := mk() // another replica over the same key
	code, out := f.post(t, "/v1/basis", fleet, map[string]any{"signals": []string{"logs"}})
	if code != 200 {
		t.Fatal(code, out)
	}
	known := out["basis"].(string)
	b := basis.Basis{IssuedNs: f.now.UnixNano(), Clusters: map[string]uint64{basis.Fleet: uint64(f.ct["prod-a"])}, Signals: []string{"logs"}}
	unseen, err := other.Mint(context.Background(), &b)
	if err != nil {
		t.Fatal(err)
	}

	sig.down.Store(true)
	ran := len(f.rows.sqls)
	// a plain query: answered, without a basis, saying why
	code, out, _ = f.count(t, fleet, map[string]any{})
	if code != 200 || out["basis"] != nil || !strings.HasPrefix(out["basis_unavailable"].(string), basis.ReasonSignerUnavailable) {
		t.Fatalf("plain query in an outage: %d %v %v", code, out["basis"], out["basis_unavailable"])
	}
	if out["basis_info"] != nil || out["at_basis"] != false {
		t.Fatalf("no basis_info without a basis: %v", out)
	}
	ran = len(f.rows.sqls) - ran
	// a new basis: refused, visibly, running nothing
	before := len(f.rows.sqls)
	for _, c := range []struct {
		path string
		body map[string]any
	}{
		{"/v1/query", map[string]any{"basis": "latest"}},
		{"/v1/query", map[string]any{"basis": unseen}},
		{"/v1/basis", map[string]any{}},
	} {
		if c.path == "/v1/query" {
			code, out, _ = f.count(t, fleet, c.body)
		} else {
			code, out = f.post(t, c.path, fleet, c.body)
		}
		if code != http.StatusServiceUnavailable || out["error"] != basis.ReasonSignerUnavailable {
			t.Errorf("%s %v: %d %v %v", c.path, c.body["basis"], code, out["error"], out["detail"])
		}
	}
	if len(f.rows.sqls) != before {
		t.Fatal("a refused basis ran statements")
	}
	// the token this replica minted: still served, at its basis
	code, out, _ = f.count(t, fleet, map[string]any{"basis": known})
	if code != 200 || out["at_basis"] != true || out["basis"] != known {
		t.Fatalf("a cached token in an outage: %d %v", code, out)
	}
	// the outage ends: the unseen token verifies
	sig.down.Store(false)
	if code, out, _ = f.count(t, fleet, map[string]any{"basis": unseen}); code != 200 {
		t.Fatalf("after the outage: %d %v", code, out)
	}
	if ran < 1 {
		t.Fatalf("the plain query ran %d statements", ran)
	}
	resp, err := http.Get(f.http.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	var m bytes.Buffer
	_, _ = m.ReadFrom(resp.Body)
	resp.Body.Close()
	for _, want := range []string{`qs_basis_signer_calls_total{op="mint",result="error"}`, `qs_basis_signer_calls_total{op="verify",result="error"}`,
		`qs_basis_unavailable_total{endpoint="query"} 1`} {
		if !strings.Contains(m.String(), want) {
			t.Errorf("metrics lack %s", want)
		}
	}
}
