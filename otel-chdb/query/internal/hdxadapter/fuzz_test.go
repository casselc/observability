package hdxadapter

import (
	"strings"
	"testing"

	chp "github.com/AfterShip/clickhouse-sql-parser/parser"
)

// escapeParam is how @clickhouse/client sends a String parameter (the
// escaped text format).
func escapeParam(s string) string {
	return strings.NewReplacer(`\`, `\\`, "\t", `\t`, "\n", `\n`).Replace(s)
}

// FuzzDecodeString: never panics; a value the client escaped decodes back
// to itself.
func FuzzDecodeString(f *testing.F) {
	for _, s := range []string{"", "plain", `a\b`, "tab\there", "nl\n", `\x41`, `\N`, "\x00\x1f\x7f\xff", `trailing\`} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		_, _ = DecodeString(s) // arbitrary input: no panic
		got, err := DecodeString(escapeParam(s))
		if err != nil || got != s {
			t.Fatalf("DecodeString(escape(%q)) = %q, %v", s, got, err)
		}
	})
}

// FuzzBind: any statement and parameter never panic the binder, and a
// bound statement formats to text that parses again.
func FuzzBind(f *testing.F) {
	f.Add("SELECT Body FROM otel_logs WHERE ServiceName = {svc:String} LIMIT 10", "svc", "cart")
	f.Add("SELECT count() FROM otel_logs WHERE Timestamp >= fromUnixTimestamp64Milli({from:Int64}) FORMAT JSON", "from", "1700000000000")
	f.Add("SELECT {x:Array(String)}", "x", "['a','b']")
	f.Add("SELECT {x:Identifier} FROM t", "x", "Body")
	f.Fuzz(func(t *testing.T, sql, name, value string) {
		e, _, err := Bind(sql, map[string]string{name: value})
		if err != nil || e == nil {
			return
		}
		text := chp.Format(e)
		if _, err := chp.NewParser(text).ParseStmts(); err != nil {
			t.Fatalf("bound statement does not parse again: %v\n in: %q (%s=%q)\nout: %q", err, sql, name, value, text)
		}
	})
}
