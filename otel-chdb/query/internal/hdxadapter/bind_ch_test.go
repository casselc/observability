package hdxadapter

import (
	"encoding/json"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	chp "github.com/AfterShip/clickhouse-sql-parser/parser"

	"github.com/casselc/observability/otel-chdb/testgate"
)

// chURL is a live ClickHouse for the differential tests (HDXA_CH, default
// the shared box's http://127.0.0.1:18123); they skip without one.
func chURL(t *testing.T) string {
	u := os.Getenv("HDXA_CH")
	if u == "" {
		u = "http://127.0.0.1:18123"
	}
	c := http.Client{Timeout: 2 * time.Second}
	resp, err := c.Get(u + "/ping")
	if err != nil {
		testgate.Skip(t, "clickhouse", "no ClickHouse at %s: %v", u, err)
	}
	resp.Body.Close()
	return u
}

func chRun(t *testing.T, base, sql string, params map[string]string) (string, bool) {
	t.Helper()
	v := url.Values{}
	for k, p := range params {
		v.Set("param_"+k, p)
	}
	resp, err := http.Post(base+"/?"+v.Encode(), "text/plain", strings.NewReader(sql))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return strings.TrimSpace(string(b)), resp.StatusCode == 200
}

// TestBindMatchesClickHouse: for hostile and random parameter values, the
// adapter's bound literal reads, on the real server, as exactly what
// ClickHouse's own parameter substitution reads; where ClickHouse refuses
// (or makes up a byte for a bad \x escape) the adapter refuses.
func TestBindMatchesClickHouse(t *testing.T) {
	base := chURL(t)
	values := append([]string{}, hostile...)
	rng := rand.New(rand.NewPCG(7, 11))
	alpha := []byte("ab'\"`\\\\/=xXN0nrtbfeav\x00\n\t\r\xc3\xa9\xff{}:;-#$ 4Fg")
	for i := 0; i < 500; i++ {
		b := make([]byte, rng.IntN(12))
		for j := range b {
			b[j] = alpha[rng.IntN(len(alpha))]
		}
		values = append(values, string(b))
	}
	agree, refusedBoth, refusedOurs := 0, 0, 0
	for _, v := range values {
		native, nok := chRun(t, base, "SELECT hex({p:String}) FORMAT TSV", map[string]string{"p": v})
		root, _, err := Bind("SELECT hex({p:String}) FORMAT TSV", map[string]string{"p": v})
		if err != nil {
			if nok && !strings.Contains(v, `\x`) {
				t.Errorf("%q: ClickHouse reads it as %s, the adapter refused: %v", v, native, err)
			}
			if nok {
				refusedOurs++
			} else {
				refusedBoth++
			}
			continue
		}
		if !nok {
			t.Errorf("%q: ClickHouse refuses it (%s), the adapter bound %s", v, native, chp.Format(root))
			continue
		}
		ours, ok := chRun(t, base, chp.Format(root)+" FORMAT TSV", nil)
		if !ok || ours != native {
			t.Errorf("%q: ClickHouse %s, adapter %s (%s)", v, native, ours, chp.Format(root))
			continue
		}
		agree++
	}
	t.Logf("String: %d agree, %d refused by both, %d refused by the adapter only (\\x with a non-hex digit)", agree, refusedBoth, refusedOurs)

	// Identifier: the column name ClickHouse gives SELECT 1 AS {p:Identifier}
	name := func(body string) string {
		var r struct{ Meta []struct{ Name string } }
		if json.Unmarshal([]byte(body), &r) != nil || len(r.Meta) != 1 {
			return "?" + body
		}
		return r.Meta[0].Name
	}
	for _, v := range []string{"otel_logs", "a.b", "x; DROP TABLE y", "x' OR 1=1 --", "é", "(SELECT 1)", "a b", "{p:String}"} {
		native, nok := chRun(t, base, "SELECT 1 AS {p:Identifier} FORMAT JSON", map[string]string{"p": v})
		root, _, err := Bind("SELECT 1 AS {p:Identifier} FORMAT JSON", map[string]string{"p": v})
		if err != nil || !nok {
			t.Errorf("%q: %v / %v %s", v, err, nok, native)
			continue
		}
		ours, ok := chRun(t, base, chp.Format(root)+" FORMAT JSON", nil)
		if !ok || name(ours) != name(native) {
			t.Errorf("%q: ClickHouse names it %q, the adapter %q", v, name(native), name(ours))
		}
	}
	// integers and floats: the value and the type
	for _, c := range []struct{ typ, v string }{{"Int64", "1790455229000"}, {"Int64", "-9223372036854775808"}, {"Int32", "+5"},
		{"Int32", "007"}, {"UInt64", "18446744073709551615"}, {"UInt8", "0"}, {"Float64", "0.95"}, {"Float64", "1e3"},
		{"Float32", "0.1"}, {"Float64", "-2.5E-3"}, {"Float64", "5."}} {
		q := "SELECT {p:" + c.typ + "} AS v, toTypeName(v) FORMAT TSV"
		native, nok := chRun(t, base, q, map[string]string{"p": c.v})
		root, _, err := Bind(q, map[string]string{"p": c.v})
		if err != nil || !nok {
			t.Errorf("%s %q: %v / %s", c.typ, c.v, err, native)
			continue
		}
		ours, _ := chRun(t, base, chp.Format(root)+" FORMAT TSV", nil)
		if ours != native {
			t.Errorf("%s %q: ClickHouse %q, adapter %q", c.typ, c.v, native, ours)
		}
	}
}
