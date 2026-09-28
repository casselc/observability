package hdxadapter

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	chp "github.com/AfterShip/clickhouse-sql-parser/parser"
	"pgregory.net/rapid"
)

func mustReason(t *testing.T, err error, reason string) {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) || e.Reason != reason {
		t.Fatalf("want refusal %q, got %v", reason, err)
	}
}

// TestDecodeString: ClickHouse 26.10's reading of a String query parameter,
// escape by escape, as measured against the server (every printable ASCII
// character after a backslash; bind_ch_test.go re-checks it live).
func TestDecodeString(t *testing.T) {
	for in, want := range map[string]string{
		`abc`: "abc", `a\nb`: "a\nb", `a\tb`: "a\tb", `a\rb`: "a\rb", `a\bb`: "a\bb", `a\fb`: "a\fb",
		`a\vb`: "a\vb", `a\ab`: "a\x07b", `a\eb`: "a\x1bb", `a\0b`: "a\x00b", `a\'b`: "a'b", `a\"b`: `a"b`,
		"a\\`b": "a`b", `a\/b`: "a/b", `a\=b`: "a=b", `a\\b`: `a\b`, `a\x41b`: "aAb", `a\Nb`: "ab", `\N`: "",
		`a\qb`: `a\qb`, `a\zb`: `a\zb`, `a\1b`: `a\1b`, `a\ b`: `a\ b`, `a\Xb`: `a\Xb`, "a\\\xc3\xa9": "a\\\xc3\xa9",
	} {
		got, err := DecodeString(in)
		if err != nil || got != want {
			t.Errorf("DecodeString(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	// where ClickHouse's answer is an error or an arbitrary byte, refuse
	for _, in := range []string{`a\`, `a\x4`, `a\x4g`, `a\xZZ`} {
		if _, err := DecodeString(in); err == nil {
			t.Errorf("DecodeString(%q): accepted", in)
		}
	}
}

func bindOne(t *testing.T, sql string, params map[string]string) (chp.Expr, error) {
	t.Helper()
	root, _, err := Bind(sql, params)
	return root, err
}

// literals returns the string literals of a tree.
func literals(root chp.Expr) []string {
	var out []string
	walkAll(reflect.ValueOf(root), func(n any) {
		if s, ok := n.(*chp.StringLiteral); ok {
			out = append(out, s.Literal)
		}
	})
	return out
}

// hostile values: each must land as exactly one literal (or one quoted
// identifier), and the statement's shape must not change.
var hostile = []string{
	`'; DROP TABLE otel_logs; --`, `\' OR 1=1 --`, `\\' OR 1=1 --`, `x' OR '1'='1`, `{p:String}`, `{HYPERDX_PARAM_1:Identifier}`,
	"a`b", `*/ OR 1 /*`, `-- comment`, "line1\\nline2", `\x00`, `\0`, "\xff\xfe", `hdxqp0_`, `') UNION ALL SELECT * FROM system.users --`,
	`\\\\\\`, `$$ $$`, `# x`, "é漢字", `%_%`, `\N`, `SETTINGS max_result_rows=0`, `FORMAT TSV`,
}

func TestHostileStringValues(t *testing.T) {
	const q = "SELECT count() FROM otel_logs WHERE Body = {p:String} AND ServiceName != 'x' \nFORMAT JSON"
	benign, err := bindOne(t, q, map[string]string{"p": "benign"})
	if err != nil {
		t.Fatal(err)
	}
	shape := strings.Replace(chp.Format(benign), "'benign'", "?", 1)
	for _, v := range hostile {
		root, err := bindOne(t, q, map[string]string{"p": v})
		want, derr := DecodeString(v)
		if derr != nil {
			if err == nil {
				t.Errorf("%q: ClickHouse refuses it, the adapter bound it", v)
			}
			continue
		}
		if err != nil {
			t.Errorf("%q: %v", v, err)
			continue
		}
		lits := literals(root)
		if len(lits) != 2 || lits[1] != "x" {
			t.Errorf("%q: literals %q", v, lits)
			continue
		}
		got, err := DecodeString(lits[0])
		if err != nil || got != want {
			t.Errorf("%q: literal %q reads back as %q, want %q", v, lits[0], got, want)
		}
		sql := chp.Format(root)
		if s := strings.Replace(sql, "'"+lits[0]+"'", "?", 1); s != shape {
			t.Errorf("%q: shape changed:\n%s\n%s", v, s, shape)
		}
		// and the text the service receives parses back to the same tree
		st, err := chp.NewParser(sql).ParseStmts()
		if err != nil || len(st) != 1 || chp.Format(st[0]) != sql {
			t.Errorf("%q: %s does not round-trip: %v", v, sql, err)
		}
	}
}

func TestHostileIdentifierValues(t *testing.T) {
	const q = "SELECT count() FROM {db:Identifier}.{t:Identifier} WHERE Body != ''"
	for v, want := range map[string]string{
		"otel_logs":       "SELECT count() FROM `otel`.`otel_logs` WHERE Body != ''",
		"a.b":             "SELECT count() FROM `otel`.`a.b` WHERE Body != ''", // one name, as ClickHouse reads it
		"x; DROP TABLE y": "SELECT count() FROM `otel`.`x; DROP TABLE y` WHERE Body != ''",
		"x' OR 1=1 --":    "SELECT count() FROM `otel`.`x' OR 1=1 --` WHERE Body != ''",
		"system.users":    "SELECT count() FROM `otel`.`system.users` WHERE Body != ''",
		"otel_logs FINAL": "SELECT count() FROM `otel`.`otel_logs FINAL` WHERE Body != ''",
		"(SELECT 1)":      "SELECT count() FROM `otel`.`(SELECT 1)` WHERE Body != ''",
	} {
		root, err := bindOne(t, q, map[string]string{"db": "otel", "t": v})
		if err != nil {
			t.Errorf("%q: %v", v, err)
			continue
		}
		if got := chp.Format(root); got != want {
			t.Errorf("%q:\n got %s\nwant %s", v, got, want)
		}
	}
	for _, v := range []string{"a`b", `a\b`, "", "a\nb", "a\x00b", "\xff", `a"b`} {
		_, err := bindOne(t, q, map[string]string{"db": "otel", "t": v})
		mustReason(t, err, "bad_param")
	}
}

func TestNumericValues(t *testing.T) {
	const q = "SELECT count() FROM otel_logs WHERE Timestamp >= fromUnixTimestamp64Milli({a:Int64}) LIMIT {n:Int32}"
	root, err := bindOne(t, q, map[string]string{"a": "-5", "n": "+10"})
	if err != nil {
		t.Fatal(err)
	}
	if got := chp.Format(root); got != "SELECT count() FROM otel_logs WHERE Timestamp >= fromUnixTimestamp64Milli(toInt64(-5)) LIMIT toInt32(10)" {
		t.Fatal(got)
	}
	for _, bad := range []map[string]string{
		{"a": "1 OR 1=1", "n": "1"}, {"a": "1", "n": "99999999999"}, {"a": "0x10", "n": "1"}, {"a": "1e3", "n": "1"},
		{"a": "", "n": "1"}, {"a": " 1", "n": "1"}, {"a": "1", "n": "1.5"}, {"a": "9223372036854775808", "n": "1"},
	} {
		_, err := bindOne(t, q, bad)
		mustReason(t, err, "bad_param")
	}
	root, err = bindOne(t, "SELECT quantile({q:Float64})(Duration), {u:UInt8} FROM otel_traces", map[string]string{"q": "0.95", "u": "255"})
	if err != nil {
		t.Fatal(err)
	}
	if got := chp.Format(root); got != "SELECT quantile(toFloat64('9.5e-01'))(Duration), toUInt8(255) FROM otel_traces" {
		t.Fatal(got)
	}
	for _, bad := range []map[string]string{{"q": "inf", "u": "1"}, {"q": "nan", "u": "1"}, {"q": "0x1p3", "u": "1"}, {"q": "1", "u": "-1"}, {"q": "1", "u": "256"}} {
		_, err := bindOne(t, "SELECT quantile({q:Float64})(Duration), {u:UInt8} FROM otel_traces", bad)
		mustReason(t, err, "bad_param")
	}
}

func TestPlaceholderPositions(t *testing.T) {
	// not a placeholder inside a string, a quoted identifier or a comment
	root, err := bindOne(t, "SELECT '{p:String}', 1 /* {p:String} */ -- {p:String}\n FROM otel_logs", nil)
	if err != nil || chp.Format(root) != "SELECT '{p:String}', 1 FROM otel_logs" {
		t.Fatalf("%v %v", root, err)
	}
	cases := map[string]string{
		"SELECT {f:Identifier}(Body) FROM otel_logs": "param_function_name",
		"SELECT * FROM {f:Identifier}('x')":          "param_function_name",
		"SELECT count() FROM {t:String}":             "bad_param",
		"SELECT {p:String} FROM otel_logs":           "missing_param",
		"SELECT hdxqp0_ FROM otel_logs":              "parse_error",
		"SELECT `a\\`b` FROM otel_logs":              "bad_identifier",
		"SELECT `a``b` FROM otel_logs":               "bad_identifier",
		"SELECT $$x$$":                               "parse_error",
		"SELECT 1 # comment":                         "parse_error",
		"SELECT 1 /* a /* nested */ */":              "parse_error",
		"SELECT 'unterminated":                       "parse_error",
		"SELECT 1; SELECT 2":                         "parse_error",
	}
	params := map[string]string{"f": "file", "t": "otel_logs"}
	for _, q := range []string{"SELECT {p:Array(String)} FROM otel_logs", "SELECT {p:Nullable(String)} FROM otel_logs"} {
		_, _, err := Bind(q, map[string]string{"p": "x"})
		mustReason(t, err, "bad_param")
	}
	// an Identifier in a type position is an identifier there (ClickHouse
	// then refuses an unknown type)
	if root, _, err := Bind("SELECT CAST(Body AS {t:Identifier}) FROM otel_logs", params); err != nil || chp.Format(root) != "SELECT CAST(Body AS `otel_logs`) FROM otel_logs" {
		t.Errorf("CAST: %v", err)
	}
	for q, reason := range cases {
		_, _, err := Bind(q, params)
		if err == nil {
			t.Errorf("%s: bound", q)
			continue
		}
		var e *Error
		if !errors.As(err, &e) || e.Reason != reason {
			t.Errorf("%s: %v, want %s", q, err, reason)
		}
	}
}

func TestFormatStripping(t *testing.T) {
	tb := Tables{DefaultDatabase: "otel"}
	for q, want := range map[string]string{
		"SELECT 1 \nFORMAT JSON":                              "JSON",
		"SELECT 1 FORMAT JSONEachRow;":                        "JSONEachRow",
		"SELECT 1 format JSONCompactEachRowWithNamesAndTypes": "JSONCompactEachRowWithNamesAndTypes",
		"SELECT 'FORMAT JSONEachRow'":                         "JSON", // the default_format below
		"SELECT 1 AS format":                                  "JSON",
		"SELECT 1 -- FORMAT JSONEachRow":                      "JSON",
	} {
		st, err := tb.Prepare(q, nil, "JSON")
		if err != nil || st.Format != want {
			t.Errorf("%q: %+v %v", q, st, err)
		}
	}
	for _, q := range []string{"SELECT 1 FORMAT TSV", "SELECT 1 FORMAT CSV", "SELECT 1"} {
		_, err := tb.Prepare(q, nil, "")
		mustReason(t, err, "format")
	}
	// a FORMAT that is not last is the service's to refuse
	st, err := tb.Prepare("SELECT * FROM (SELECT 1 FORMAT TSV) FORMAT JSON", nil, "")
	if err == nil && !strings.Contains(st.SQL, "FORMAT") {
		t.Fatalf("an inner FORMAT vanished: %s", st.SQL)
	}
}

func TestMetadataStatements(t *testing.T) {
	tb := Tables{DefaultDatabase: "otel"}
	st, err := tb.Prepare("DESCRIBE {d:Identifier}.{t:Identifier} \nFORMAT JSON", map[string]string{"d": "otel", "t": "x' OR 1=1 --"}, "")
	if err != nil {
		t.Fatal(err)
	}
	if st.Kind != "describe" || !st.Metadata || st.Format != "JSON" ||
		!strings.Contains(st.SQL, `FROM system.columns WHERE database = 'otel' AND table = 'x\' OR 1=1 --' ORDER BY position`) {
		t.Fatalf("%+v", st)
	}
	st, err = tb.Prepare("SHOW TABLES FROM {d:Identifier} FORMAT JSON", map[string]string{"d": "otel' OR '1"}, "")
	if err != nil || st.SQL != `SELECT name FROM system.tables WHERE database = 'otel\' OR \'1' ORDER BY name` {
		t.Fatalf("%+v %v", st, err)
	}
	st, err = tb.Prepare("SHOW DATABASES FORMAT JSON", nil, "")
	if err != nil || st.SQL != "SELECT name FROM system.databases ORDER BY name" {
		t.Fatalf("%+v %v", st, err)
	}
	st, err = tb.Prepare("SELECT * FROM system.tables WHERE database = {d:String} FORMAT JSON", map[string]string{"d": "otel"}, "")
	if err != nil || !st.Metadata || st.Window != nil {
		t.Fatalf("%+v %v", st, err)
	}
	for q, reason := range map[string]string{
		"EXPLAIN ESTIMATE SELECT count() FROM otel_logs FORMAT JSON": "explain",
		"SHOW CREATE TABLE otel_logs FORMAT JSON":                    "not_select",
		"INSERT INTO otel_logs VALUES (1)":                           "format",
		"INSERT INTO otel_logs FORMAT JSON":                          "not_select",
		"KILL QUERY WHERE query_id = 'x' FORMAT JSON":                "parse_error",
	} {
		_, err := tb.Prepare(q, nil, "")
		if err == nil {
			t.Errorf("%s: prepared", q)
			continue
		}
		var e *Error
		if !errors.As(err, &e) || (e.Reason != reason && !(reason == "parse_error" && e.Reason == "not_select")) {
			t.Errorf("%s: %v, want %s", q, err, reason)
		}
	}
}

// TestBindProperty: any byte string, sent as @clickhouse/client sends it,
// lands as exactly one literal that decodes to it, in a statement whose
// shape does not change.
func TestBindProperty(t *testing.T) {
	const q = "SELECT count() FROM otel_logs WHERE Body LIKE {p:String} AND SeverityText = {s:String} FORMAT JSON"
	rapid.Check(t, func(rt *rapid.T) {
		alpha := []byte("ab'\"`\\/{}:;-*#$%_\n\r\t\x00 \x7f\xc3\xa9\xffNxX0SELECThdxqp")
		gen := rapid.SliceOfN(rapid.SampledFrom(alpha), 0, 40)
		v1 := string(gen.Draw(rt, "p"))
		v2 := string(gen.Draw(rt, "s"))
		root, _, err := Bind(q, map[string]string{"p": EncodeEscaped(v1), "s": EncodeEscaped(v2)})
		if err != nil {
			rt.Fatalf("%q %q: %v", v1, v2, err)
		}
		lits := literals(root)
		if len(lits) != 2 {
			rt.Fatalf("literals %q", lits)
		}
		for i, want := range []string{v1, v2} {
			got, err := DecodeString(lits[i])
			if err != nil || got != want {
				rt.Fatalf("literal %q reads back %q, want %q", lits[i], got, want)
			}
		}
		sql := chp.Format(root)
		st, err := chp.NewParser(sql).ParseStmts()
		if err != nil || len(st) != 1 || chp.Format(st[0]) != sql {
			rt.Fatalf("%s: no round trip: %v", sql, err)
		}
		if !strings.HasPrefix(sql, "SELECT count() FROM otel_logs WHERE Body LIKE '") {
			rt.Fatalf("shape: %s", sql)
		}
	})
}
