package main

import (
	"testing"

	chp "github.com/AfterShip/clickhouse-sql-parser/parser"
)

// FuzzRewrite: whatever HyperDX (or anyone) sends, the rewriter never
// panics, and a statement it rewrites still parses (placeholders masked as
// the rewriter masks them).
func FuzzRewrite(f *testing.F) {
	for _, q := range []string{
		"SELECT Body FROM {db:Identifier}.{t:Identifier} WHERE ResourceAttributes['k8s.pod.name'] = {s:String} LIMIT {n:Int32}",
		"SELECT ResourceAttributes['k8s.namespace.name'] AS ns, count() FROM rw_c.otel_logs GROUP BY ns",
		"SELECT * FROM rw_c.otel_traces WHERE ResourceAttributes['k8s.pod.name'] IN ('a', 'b')",
		"SELECT 1",
		"SELECT ResourceAttributes FROM rw_c.otel_logs",
	} {
		f.Add(q)
	}
	cs := []*Config{testConfig("exact"), testConfig("catalog")}
	f.Fuzz(func(t *testing.T, q string) {
		for _, c := range cs {
			res := c.Rewrite(q, hdxParams, "")
			if !res.Rewritten {
				continue
			}
			masked, _ := mask(res.SQL)
			if _, err := chp.NewParser(masked).ParseStmts(); err != nil {
				t.Fatalf("%s: rewritten statement does not parse: %v\n in: %q\nout: %q", c.Mode, err, q, res.SQL)
			}
		}
	})
}

// FuzzParseMultipart: any Content-Type and body never panic the
// multipart reader.
func FuzzParseMultipart(f *testing.F) {
	f.Add("multipart/form-data; boundary=x", []byte("--x\r\nContent-Disposition: form-data; name=\"query\"\r\n\r\nSELECT 1\r\n--x--\r\n"))
	f.Add("multipart/form-data", []byte("--x"))
	f.Add("text/plain", []byte("SELECT 1"))
	f.Fuzz(func(t *testing.T, ct string, body []byte) {
		_, _ = parseMultipart(ct, body)
	})
}
