package persons

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/casselc/observability/otel-chdb/query/internal/central"
	"github.com/casselc/observability/otel-chdb/query/internal/sqlscope"
	"github.com/casselc/observability/otel-chdb/testgate"
	"github.com/casselc/observability/otel-chdb/testgate/tracetag"
)

// The person events table on a real ClickHouse (QS_IT_CH or
// CHDB_TEST_CLICKHOUSE, default http://127.0.0.1:18123): the schema, the
// arrival-dated correction, the dedup token making a retried signal one
// row, a second signal under another key changing nothing, and resolution
// through the store. Creates and drops its own database qs_persons_….
func TestStoreOnClickHouse(t *testing.T) {
	tracetag.Covers(t, "IT", "H-G8", "R-G9", "H-E8")
	chURL := os.Getenv("QS_IT_CH")
	if chURL == "" {
		chURL = os.Getenv("CHDB_TEST_CLICKHOUSE")
	}
	if chURL == "" {
		chURL = "http://127.0.0.1:18123"
	}
	ctx := context.Background()
	cl := central.New(central.Config{URL: chURL})
	if _, _, err := cl.Query(ctx, "SELECT 1", url.Values{}); err != nil {
		testgate.Skip(t, "clickhouse", "no ClickHouse at %s: %v", chURL, err)
	}
	db := fmt.Sprintf("qs_persons_%08x", rand.Uint32())
	exec := func(sql string) {
		t.Helper()
		if _, _, err := cl.Query(ctx, sql, url.Values{}); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	exec("CREATE DATABASE " + db)
	t.Cleanup(func() { _, _, _ = cl.Query(context.Background(), "DROP DATABASE IF EXISTS "+db, url.Values{}) })
	s, err := NewStore(Config{Database: db, Tenant: strings.ToUpper(tenant)}, cl)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CreateTable(ctx); err != nil {
		t.Fatal(err)
	}
	alice, bob := oids[0], oids[1]
	exec("INSERT INTO " + db + ".person_events (tenant, oid, cluster, namespace, valid_from, valid_to, system_from, seq, source, kind, name) VALUES " +
		"('" + tenant + "', '" + alice + "', '', '', 0, 9223372036854775807, 1000, 1, 'controller', 'assert', 'Alice Example')," +
		"('" + tenant + "', '" + bob + "', '', '', 0, 9223372036854775807, 1000, 2, 'controller', 'assert', 'Bob Example')," +
		"('" + tenant + "', '" + alice + "', 'devtools', 'dev-a', 1500, 1501, 1500, 3, 'announce', 'assert', '')," +
		"('" + tenant + "', '" + alice + "', '', '', 2000, 9223372036854775807, 2000, 4, 'controller', 'retract', '')")
	key := []byte("0123456789abcdef-test")
	out, err := s.Pseudonymise(ctx, alice, key, "TICKET-1", 3, 0)
	if err != nil {
		t.Fatal(err)
	}
	if out.Already || out.Pseudonym != Pseudonym(key, tenant, alice) || out.HiddenName != "Alice Example" || out.At < 1_700_000_000_000 {
		t.Fatalf("outcome %+v (the correction is dated by the store's clock)", out)
	}
	// the same signal retried (a lost answer): the token makes it one row
	for i := 0; i < 2; i++ {
		if err := s.insertCorrection(ctx, alice, out.Pseudonym, "TICKET-1"); err != nil {
			t.Fatal(err)
		}
	}
	// a second operator under a rotated key: already done, nothing written
	again, err := s.Pseudonymise(ctx, alice, []byte("another-key-0123456789"), "TICKET-2", 3, 0)
	if err != nil || !again.Already || again.Pseudonym != out.Pseudonym {
		t.Fatalf("second signal: %+v %v", again, err)
	}
	// a duplicate under another key that lands anyway (no token: another writer)
	exec("INSERT INTO " + db + ".person_events (tenant, oid, valid_from, seq, source, kind, name) VALUES ('" + tenant + "', '" + alice +
		"', 0, 99, 'steward', 'pseudonymise', '" + Pseudonym([]byte("another-key-0123456789"), tenant, alice) + "')")
	evs, err := s.Events(ctx, []string{alice, bob})
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range evs {
		if e.Kind == KindPseudonymise && e.Name == out.Pseudonym {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("%d rows of the retried signal", n)
	}
	team := Reader{Pairs: []sqlscope.Pair{{Cluster: "devtools", Namespace: "dev-a"}}}
	for _, st := range []int64{1000, 1500, 2500, out.At, out.At + 10_000} {
		ans := Resolve(evs, tenant, []string{alice, bob}, team, 1200, st, policy)
		if a := ans[0]; !a.Resolved || a.Name != out.Pseudonym || !a.Pseudonymised || a.State != "asserted" {
			t.Fatalf("alice as of %d: %+v", st, a)
		}
		if b := ans[1]; b.Resolved || b.Name != "" {
			t.Fatalf("bob was never seen in dev-a: %+v", b)
		}
	}
	if b := Resolve(evs, tenant, []string{bob}, Reader{Self: bob}, 1200, out.At, policy)[0]; b.Name != "Bob Example" || b.Pseudonymised {
		t.Fatalf("bob, himself: %+v", b)
	}
}
