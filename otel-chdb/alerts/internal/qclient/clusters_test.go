package qclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

// TestRuleClustersNarrowTheQuery (D29): a rule's clusters go to the query
// service as the request's `clusters`, so its label is those clusters'
// complete_through; a rule without them sends none (the identity's scope).
func TestRuleClustersNarrowTheQuery(t *testing.T) {
	p := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(p, []byte(jwtWithExp(time.Now().Add(time.Hour))), 0o600); err != nil {
		t.Fatal(err)
	}
	ts, _ := NewTokenSource(Identity{TokenFile: p}, nil)
	var got [][]string
	qs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Clusters []string `json:"clusters"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		got = append(got, body.Clusters)
		w.WriteHeader(503)
	}))
	defer qs.Close()
	c := &Client{URL: qs.URL, Identity: map[string]TokenSource{"default": ts}}
	r := testRule(t)
	_ = c.Evaluate(context.Background(), r, win)
	r.Clusters = []string{"prod-a"}
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	_ = c.Evaluate(context.Background(), r, win)
	if len(got) != 2 || got[0] != nil || !slices.Equal(got[1], []string{"prod-a"}) {
		t.Fatalf("clusters sent: %q", got)
	}
	r.Clusters = []string{"Prod_A"}
	if err := r.Validate(); err == nil {
		t.Fatal("a cluster outside FORMAT.md's names is refused")
	}
}
