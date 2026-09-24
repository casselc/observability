package chdbexporter

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"hegel.dev/go/hegel"
)

// Property-based tests (hegel-go, https://hegel.dev). Hegel runs in process:
// hegel-go loads libhegel, the Rust engine, from a copy embedded in the
// module, so these need nothing installed and run under plain `go test`.
//
//	go test -run PBT ./...                 # every property
//	PBT_SCALE=10 go test -run PBT ./...    # 10x the examples
//	HEGEL_STATISTICS=1 go test -run PBT -v # event statistics per property
//
// Failing examples are kept in $HEGEL_DB (default: a directory under the OS
// temp dir, not the repository) and replayed first on the next run.
//
// Tests named TestPBTFinding* encode a known defect as a property and PASS
// when hegel finds (and shrinks) a counterexample; they log it. When a defect
// is fixed, its finding test fails and should become an ordinary property.

func isValidUTF8(s string) bool { return utf8.ValidString(s) }

// pbtOpts returns the options every property uses: n examples scaled by
// PBT_SCALE, and the example database outside the repository.
func pbtOpts(n int, extra ...hegel.Option) []hegel.Option {
	if s, err := strconv.ParseFloat(os.Getenv("PBT_SCALE"), 64); err == nil && s > 0 {
		n = max(1, int(float64(n)*s))
	}
	db := os.Getenv("HEGEL_DB")
	if db == "" {
		db = filepath.Join(os.TempDir(), "chdbexporter-hegel")
	}
	return append([]hegel.Option{hegel.WithTestCases(n), hegel.WithDatabase(db)}, extra...)
}

var stdoutMu sync.Mutex

// expectFinding runs body as a property that is expected to FAIL because it
// states a known defect. It passes when hegel finds a counterexample, and
// logs hegel's report of the shrunk one; it fails if none is found.
func expectFinding(t *testing.T, what string, body func(hegel.TestCase), opts ...hegel.Option) string {
	t.Helper()
	// hegel.Run writes the replay of the minimal example to os.Stdout, read
	// when it is called; capture it for the test log.
	stdoutMu.Lock()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	os.Stdout = w
	outc := make(chan string)
	go func() { b, _ := io.ReadAll(r); outc <- string(b) }()
	// hegel.Run keys its example database by call site, which is this line
	// for every finding: give each test its own database instead.
	db := os.Getenv("HEGEL_DB")
	if db == "" {
		db = filepath.Join(os.TempDir(), "chdbexporter-hegel")
	}
	opts = append(opts, hegel.WithDatabase(filepath.Join(db, strings.ReplaceAll(t.Name(), "/", "_"))))
	runErr := hegel.Run(body, opts...)
	os.Stdout = old
	w.Close()
	out := <-outc
	stdoutMu.Unlock()
	if runErr == nil {
		t.Fatalf("expected hegel to find %s, but every example passed. If it was fixed, turn this into an ordinary property.", what)
	}
	// A run can also fail without a counterexample (a health check, a
	// nondeterministic test); that is not a finding.
	if !strings.Contains(out, "failure:") {
		t.Fatalf("hegel failed without a counterexample: %v\n%s", runErr, out)
	}
	t.Logf("hegel found %s. Shrunk counterexample:\n%s", what, trimReport(out))
	return out
}

// trimReport drops hegel's Go stack frames from a failure report.
func trimReport(s string) string {
	var keep []string
	for _, l := range strings.Split(s, "\n") {
		tl := strings.TrimSpace(l)
		if strings.HasPrefix(tl, "/") || strings.HasSuffix(tl, "(...)") || strings.HasPrefix(tl, "reproduction blob") {
			continue
		}
		keep = append(keep, l)
	}
	return strings.TrimSpace(strings.Join(keep, "\n"))
}

// ---- valueString ----------------------------------------------------------

// TestPBTValueStringMatchesAsString: the encoders' allocation-free rendering
// of attribute values equals pcommon.Value.AsString (what the clickhouse
// exporter stores) for arbitrary values, nested ones included.
func TestPBTValueStringMatchesAsString(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		v := hegel.Draw(ht, pdataGen{}.value())
		want := v.AsString()
		if got := string(valueString([]byte("prefix:"), v)); got != "prefix:"+want {
			ht.Fatalf("%s value: valueString %q, AsString %q", v.Type(), got, want)
		}
	}, pbtOpts(2000)...)
}

// TestPBTValueStringDoubles concentrates on doubles: every float64 bit
// pattern class, near the 1e-6 and 1e21 formatting thresholds included.
func TestPBTValueStringDoubles(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		f := hegel.Draw(ht, hegel.OneOf(genDouble(),
			hegel.Map(hegel.Integers[uint64](0, math.MaxUint64), math.Float64frombits),
			hegel.Floats[float64]().Min(1e-7).Max(1e-5),
			hegel.Floats[float64]().Min(1e20).Max(1e22)))
		v := pcommon.NewValueDouble(f)
		if got, want := string(valueString(nil, v)), v.AsString(); got != want {
			ht.Fatalf("%v (bits %#x): valueString %q, AsString %q", f, math.Float64bits(f), got, want)
		}
	}, pbtOpts(3000)...)
}

// ---- encoders ---------------------------------------------------------------

// The oracle: what each row must hold, built straight from pdata with its
// own APIs (hex.EncodeToString, AsString), independently of the encoders.

func hexOrEmpty(b []byte) string {
	for _, c := range b {
		if c != 0 {
			return hex.EncodeToString(b)
		}
	}
	return ""
}

func oracleAttrs(m pcommon.Map) []rbKV {
	out := []rbKV{}
	m.Range(func(k string, v pcommon.Value) bool {
		out = append(out, rbKV{k, v.AsString()})
		return true
	})
	return out
}

func oracleService(res pcommon.Map) string {
	if v, ok := res.Get("service.name"); ok {
		return v.AsString()
	}
	return ""
}

func oracleEnvelope(row []any, env *envelope, ordinal int) []any {
	if env == nil {
		return row
	}
	return append(row, env.producer, env.epoch, env.batch, uint64(ordinal), env.received, uint64(env.schema))
}

func oracleTraceRows(td ptrace.Traces, env *envelope) [][]any {
	var rows [][]any
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		res := rs.Resource().Attributes()
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)
			for k := 0; k < ss.Spans().Len(); k++ {
				s := ss.Spans().At(k)
				tid, sid, pid := s.TraceID(), s.SpanID(), s.ParentSpanID()
				evTS, evName, evAttr := []any{}, []any{}, []any{}
				for e := 0; e < s.Events().Len(); e++ {
					ev := s.Events().At(e)
					evTS = append(evTS, uint64(ev.Timestamp()))
					evName = append(evName, ev.Name())
					evAttr = append(evAttr, oracleAttrs(ev.Attributes()))
				}
				lT, lS, lState, lAttr := []any{}, []any{}, []any{}, []any{}
				for l := 0; l < s.Links().Len(); l++ {
					ln := s.Links().At(l)
					ltid, lsid := ln.TraceID(), ln.SpanID()
					lT = append(lT, hexOrEmpty(ltid[:]))
					lS = append(lS, hexOrEmpty(lsid[:]))
					lState = append(lState, ln.TraceState().AsRaw())
					lAttr = append(lAttr, oracleAttrs(ln.Attributes()))
				}
				row := []any{
					uint64(s.StartTimestamp()), hexOrEmpty(tid[:]), hexOrEmpty(sid[:]), hexOrEmpty(pid[:]),
					s.TraceState().AsRaw(), s.Name(), s.Kind().String(), oracleService(res), oracleAttrs(res),
					ss.Scope().Name(), ss.Scope().Version(), oracleAttrs(s.Attributes()),
					uint64(s.EndTimestamp()) - uint64(s.StartTimestamp()), s.Status().Code().String(), s.Status().Message(),
					evTS, evName, evAttr, lT, lS, lState, lAttr,
				}
				rows = append(rows, oracleEnvelope(row, env, len(rows)))
			}
		}
	}
	return rows
}

func oracleLogRows(ld plog.Logs, env *envelope) [][]any {
	var rows [][]any
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		res := rl.Resource().Attributes()
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				r := sl.LogRecords().At(k)
				ts := r.Timestamp()
				if ts == 0 {
					ts = r.ObservedTimestamp()
				}
				tid, sid := r.TraceID(), r.SpanID()
				row := []any{
					uint64(ts), hexOrEmpty(tid[:]), hexOrEmpty(sid[:]), uint64(uint8(r.Flags())), r.SeverityText(),
					uint64(uint8(r.SeverityNumber())), oracleService(res), r.Body().AsString(), rl.SchemaUrl(),
					oracleAttrs(res), sl.SchemaUrl(), sl.Scope().Name(), sl.Scope().Version(),
					oracleAttrs(sl.Scope().Attributes()), oracleAttrs(r.Attributes()), r.EventName(),
				}
				rows = append(rows, oracleEnvelope(row, env, len(rows)))
			}
		}
	}
	return rows
}

// normalize turns nil slices into empty ones so DeepEqual compares content.
func normalize(v any) any {
	switch x := v.(type) {
	case []any:
		out := make([]any, len(x))
		for i := range x {
			out[i] = normalize(x[i])
		}
		return out
	case []rbKV:
		out := make([]rbKV, len(x))
		for i := range x {
			out[i] = rbKV{normalize(x[i].K), normalize(x[i].V)}
		}
		return out
	}
	return v
}

func diffRows(cols []rbColumn, want, got [][]any) string {
	if len(want) != len(got) {
		return fmt.Sprintf("%d rows, want %d", len(got), len(want))
	}
	for i := range want {
		if len(want[i]) != len(got[i]) {
			return fmt.Sprintf("row %d: %d columns, want %d", i, len(got[i]), len(want[i]))
		}
		for c := range want[i] {
			if !reflect.DeepEqual(normalize(want[i][c]), normalize(got[i][c])) {
				return fmt.Sprintf("row %d column %s:\n got %#v\nwant %#v", i, cols[c].name, got[i][c], want[i][c])
			}
		}
	}
	return ""
}

// drawEnvelope draws nil (local tables) or a publishing envelope.
func drawEnvelope(tc hegel.TestCase) *envelope {
	if !hegel.Draw(tc, hegel.Booleans()) {
		return nil
	}
	return &envelope{
		producer: hegel.Draw(tc, genString(6, true)), epoch: hegel.Draw(tc, genString(6, true)),
		batch: hegel.Draw(tc, hegel.Integers[uint64](0, math.MaxUint64)), received: hegel.Draw(tc, hegel.Integers[uint64](0, math.MaxUint64)),
		schema: hegel.Draw(tc, hegel.Integers[uint16](0, math.MaxUint16)),
	}
}

func copyEnv(e *envelope) *envelope {
	if e == nil {
		return nil
	}
	c := *e
	return &c
}

// encodeBoth runs one walker through RowBinary and JSONEachRow and checks
// both against the oracle: RowBinary decodes with exactly structure(signal,
// env), row for row equal to it; JSONEachRow has one object per row, keyed by
// columns(signal, env) in order, whose values parse (the way ClickHouse
// parses them) to the same rows.
func encodeBoth(tc hegel.TestCase, signal string, env *envelope, walk func(rowWriter, *envelope) int, want [][]any) {
	rb := &rowBinary{}
	n := walk(rb, copyEnv(env))
	if n != len(want) {
		tc.Errorf("walker returned %d rows, pdata has %d", n, len(want))
	}
	cols, rows, err := decodeSignal(signal, env != nil, rb.buf)
	if err != nil {
		tc.Errorf("RowBinary does not decode as structure(%s, %v): %v", signal, env != nil, err)
	}
	if d := diffRows(cols, want, rows); d != "" {
		tc.Errorf("RowBinary %s: %s", signal, d)
	}

	js := &jsonEachRow{cols: columns(signal, env != nil)}
	walk(js, copyEnv(env))
	lines := strings.Split(strings.TrimSuffix(string(js.buf), "\n"), "\n")
	if len(js.buf) == 0 {
		lines = nil
	}
	if len(lines) != len(want) {
		tc.Errorf("JSONEachRow: %d lines for %d rows", len(lines), len(want))
	}
	var jrows [][]any
	for i, l := range lines {
		obj, err := parseJSONRow(l)
		if err != nil {
			tc.Errorf("JSONEachRow row %d does not parse: %v\n%q", i, err, l)
		}
		row, err := jsonRowToColumns(cols, obj)
		if err != nil {
			tc.Errorf("JSONEachRow row %d: %v", i, err)
		}
		jrows = append(jrows, row)
	}
	if d := diffRows(cols, want, jrows); d != "" {
		tc.Errorf("JSONEachRow %s: %s", signal, d)
	}
}

// TestPBTEncodersMatchPdata: for arbitrary traces and logs, with and without
// the publishing envelope, both encoders write exactly the rows pdata holds,
// in the column order and types that columns()/structure() declare.
func TestPBTEncodersMatchPdata(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		g := pdataGen{}
		env := drawEnvelope(ht)
		if hegel.Draw(ht, hegel.Booleans()) {
			td := hegel.Draw(ht, g.traces())
			ht.Event(fmt.Sprintf("spans=%d", min(td.SpanCount(), 4)))
			encodeBoth(ht, signalTraces, env, func(w rowWriter, e *envelope) int { return writeTraces(w, td, e) }, oracleTraceRows(td, env))
		} else {
			ld := hegel.Draw(ht, g.logs())
			ht.Event(fmt.Sprintf("logs=%d", min(ld.LogRecordCount(), 4)))
			encodeBoth(ht, signalLogs, env, func(w rowWriter, e *envelope) int { return writeLogs(w, ld, e) }, oracleLogRows(ld, env))
		}
	}, pbtOpts(1000)...)
}

// envelopeRange is the manifest's event-time range claim: every row's time
// lies in [min_event_time, max_event_time].
func envelopeRangeViolation(times []uint64, env *envelope) string {
	if len(times) == 0 {
		return ""
	}
	lo := time.Unix(0, int64(env.minTS)).UTC()
	hi := time.Unix(0, int64(env.maxTS)).UTC()
	for _, ts := range times {
		// The manifest renders times with time.Unix(0, int64(ts)).
		at := time.Unix(0, int64(ts)).UTC()
		if at.Before(lo) || at.After(hi) {
			return fmt.Sprintf("row time %d (%s) outside the manifest's [%s, %s] (minTS %d, maxTS %d, row times %v)",
				ts, at.Format(time.RFC3339Nano), lo.Format(time.RFC3339Nano), hi.Format(time.RFC3339Nano), env.minTS, env.maxTS, times)
		}
	}
	return ""
}

func logTimes(ld plog.Logs) []uint64 {
	var out []uint64
	for _, r := range oracleLogRows(ld, nil) {
		out = append(out, r[0].(uint64))
	}
	return out
}

// TestPBTFindingManifestEventTimeRange: the batch manifest's
// min_event_time/max_event_time must bound every row. envelope.write uses
// minTS == 0 as "unset", so a row whose time is 0 (a log record with neither
// Timestamp nor ObservedTimestamp, or a span starting at 0) is forgotten as
// soon as a later row has a time: rows [0, 1] give min_event_time 1.
func TestPBTFindingManifestEventTimeRange(t *testing.T) {
	expectFinding(t, "a manifest event-time range that excludes a row", func(tc hegel.TestCase) {
		ld := hegel.Draw(tc, pdataGen{validUTF8: true, maxTS: math.MaxInt64}.logs())
		env := &envelope{}
		writeLogs(&rowBinary{}, ld, env)
		if v := envelopeRangeViolation(logTimes(ld), env); v != "" {
			tc.Errorf("%s", v)
		}
	}, pbtOpts(1000)...)
}

// TestPBTFindingManifestEventTimeOverflow: pcommon timestamps are uint64 and
// the manifest converts them with int64(ts), so a time past 2262 (2^63 ns)
// becomes a date in 1677 and max_event_time falls before min_event_time.
// Garbage timestamps from clients do happen; the rows store them as Int64.
func TestPBTFindingManifestEventTimeOverflow(t *testing.T) {
	expectFinding(t, "a manifest event-time range broken by a timestamp past 2^63 ns", func(tc hegel.TestCase) {
		// Non-zero times only, so this does not rediscover the minTS == 0 bug.
		n := hegel.Draw(tc, hegel.Integers(1, 3))
		ld := plog.NewLogs()
		recs := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
		for i := 0; i < n; i++ {
			recs.AppendEmpty().SetTimestamp(pcommon.Timestamp(hegel.Draw(tc, hegel.Integers[uint64](1, math.MaxUint64))))
		}
		env := &envelope{}
		writeLogs(&rowBinary{}, ld, env)
		if v := envelopeRangeViolation(logTimes(ld), env); v != "" {
			tc.Errorf("%s", v)
		}
	}, pbtOpts(500)...)
}

// ---- a JSON reader that parses the way ClickHouse's JSONEachRow does -------
//
// encoding/json replaces invalid UTF-8 with U+FFFD; ClickHouse keeps the
// bytes. This reader keeps them too, keeps object key order (maps are
// ordered in ClickHouse), and returns numbers as uint64.

type jsonParser struct {
	s   string
	off int
}

func parseJSONRow(line string) ([]rbKV, error) {
	p := &jsonParser{s: line}
	v, err := p.value()
	if err != nil {
		return nil, err
	}
	if p.off != len(p.s) {
		return nil, fmt.Errorf("trailing data at %d", p.off)
	}
	obj, ok := v.([]rbKV)
	if !ok {
		return nil, errors.New("row is not an object")
	}
	return obj, nil
}

func (p *jsonParser) value() (any, error) {
	if p.off >= len(p.s) {
		return nil, errors.New("unexpected end")
	}
	switch c := p.s[p.off]; {
	case c == '"':
		return p.str()
	case c == '{':
		p.off++
		out := []rbKV{}
		if p.off < len(p.s) && p.s[p.off] == '}' {
			p.off++
			return out, nil
		}
		for {
			k, err := p.str()
			if err != nil {
				return nil, err
			}
			if p.off >= len(p.s) || p.s[p.off] != ':' {
				return nil, fmt.Errorf("want ':' at %d", p.off)
			}
			p.off++
			v, err := p.value()
			if err != nil {
				return nil, err
			}
			out = append(out, rbKV{k, v})
			if p.off < len(p.s) && p.s[p.off] == ',' {
				p.off++
				continue
			}
			if p.off < len(p.s) && p.s[p.off] == '}' {
				p.off++
				return out, nil
			}
			return nil, fmt.Errorf("want ',' or '}' at %d", p.off)
		}
	case c == '[':
		p.off++
		out := []any{}
		if p.off < len(p.s) && p.s[p.off] == ']' {
			p.off++
			return out, nil
		}
		for {
			v, err := p.value()
			if err != nil {
				return nil, err
			}
			out = append(out, v)
			if p.off < len(p.s) && p.s[p.off] == ',' {
				p.off++
				continue
			}
			if p.off < len(p.s) && p.s[p.off] == ']' {
				p.off++
				return out, nil
			}
			return nil, fmt.Errorf("want ',' or ']' at %d", p.off)
		}
	case c >= '0' && c <= '9':
		start := p.off
		for p.off < len(p.s) && p.s[p.off] >= '0' && p.s[p.off] <= '9' {
			p.off++
		}
		return strconv.ParseUint(p.s[start:p.off], 10, 64)
	}
	return nil, fmt.Errorf("unexpected %q at %d", p.s[p.off], p.off)
}

func (p *jsonParser) str() (string, error) {
	if p.off >= len(p.s) || p.s[p.off] != '"' {
		return "", fmt.Errorf("want '\"' at %d", p.off)
	}
	p.off++
	var b strings.Builder
	for p.off < len(p.s) {
		c := p.s[p.off]
		switch {
		case c == '"':
			p.off++
			return b.String(), nil
		case c < 0x20:
			return "", fmt.Errorf("raw control character %#x at %d", c, p.off)
		case c == '\\':
			if p.off+1 >= len(p.s) {
				return "", errors.New("dangling escape")
			}
			e := p.s[p.off+1]
			p.off += 2
			switch e {
			case '"', '\\', '/':
				b.WriteByte(e)
			case 'n':
				b.WriteByte('\n')
			case 'r':
				b.WriteByte('\r')
			case 't':
				b.WriteByte('\t')
			case 'b':
				b.WriteByte('\b')
			case 'f':
				b.WriteByte('\f')
			case 'u':
				if p.off+4 > len(p.s) {
					return "", errors.New("short \\u escape")
				}
				r, err := strconv.ParseUint(p.s[p.off:p.off+4], 16, 32)
				if err != nil {
					return "", err
				}
				p.off += 4
				b.WriteRune(rune(r))
			default:
				return "", fmt.Errorf("bad escape \\%c", e)
			}
		default:
			b.WriteByte(c)
			p.off++
		}
	}
	return "", errors.New("unterminated string")
}

// jsonRowToColumns converts one parsed JSONEachRow object to the decoder's
// representation of the row, checking the keys are exactly the columns.
func jsonRowToColumns(cols []rbColumn, obj []rbKV) ([]any, error) {
	if len(obj) != len(cols) {
		return nil, fmt.Errorf("%d keys for %d columns", len(obj), len(cols))
	}
	row := make([]any, len(cols))
	for i, c := range cols {
		if obj[i].K != c.name {
			return nil, fmt.Errorf("key %d is %q, column is %q", i, obj[i].K, c.name)
		}
		v, err := jsonToType(c.typ, obj[i].V)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", c.name, err)
		}
		row[i] = v
	}
	return row, nil
}

func jsonToType(t *rbType, v any) (any, error) {
	switch t.kind {
	case "String":
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("want a string, got %T", v)
		}
		return s, nil
	case "UInt8", "UInt16", "UInt32", "UInt64":
		n, ok := v.(uint64)
		if !ok {
			return nil, fmt.Errorf("want a number, got %T", v)
		}
		return n, nil
	case "DateTime64":
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("want \"sec.nanos\", got %T", v)
		}
		sec, frac, ok := strings.Cut(s, ".")
		if !ok || len(frac) != 9 {
			return nil, fmt.Errorf("DateTime64 %q", s)
		}
		a, err1 := strconv.ParseUint(sec, 10, 64)
		b, err2 := strconv.ParseUint(frac, 10, 64)
		if err := errors.Join(err1, err2); err != nil {
			return nil, err
		}
		return a*1e9 + b, nil
	case "Array":
		arr, ok := v.([]any)
		if !ok {
			return nil, fmt.Errorf("want an array, got %T", v)
		}
		out := []any{}
		for _, e := range arr {
			x, err := jsonToType(t.elem, e)
			if err != nil {
				return nil, err
			}
			out = append(out, x)
		}
		return out, nil
	case "Map":
		obj, ok := v.([]rbKV)
		if !ok {
			return nil, fmt.Errorf("want an object, got %T", v)
		}
		out := []rbKV{}
		for _, kv := range obj {
			val, err := jsonToType(t.val, kv.V)
			if err != nil {
				return nil, err
			}
			out = append(out, rbKV{kv.K, val})
		}
		return out, nil
	}
	return nil, fmt.Errorf("kind %s", t.kind)
}

// ---- schema -----------------------------------------------------------------

func drawIdent(tc hegel.TestCase) string {
	return hegel.Draw(tc, hegel.FromRegex(`[A-Za-z_][A-Za-z0-9_]{0,8}`, true))
}

// createdName returns the object a CREATE statement creates.
var createRe = regexp.MustCompile(`^CREATE (?:TABLE|MATERIALIZED VIEW|DATABASE) IF NOT EXISTS (\S+)`)

// TestPBTSchemaConsistent: for any valid naming, TTL, Buffer and staging
// choice, signal, envelope and generation, the table set is self-consistent:
// the insert names exactly the encoders' columns; the structure parses to the
// same column names; the MergeTree DDL declares every column; the staging
// table has exactly the structure; every stored, local and target object is
// created by the set's DDL; views are dropped before the tables they touch;
// and the disk function is asked about exactly the stored tables.
func TestPBTSchemaConsistent(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		c := createDefaultConfig().(*Config)
		c.Database, c.TracesTableName, c.LogsTableName = drawIdent(ht), drawIdent(ht), drawIdent(ht)
		ht.Assume(c.TracesTableName != c.LogsTableName)
		c.TTL = time.Duration(hegel.Draw(ht, hegel.Integers[int64](-1, 100*24*3600))) * time.Second
		c.BufferSeconds = hegel.Draw(ht, hegel.Integers(0, 3))
		c.StagingTables = hegel.Draw(ht, hegel.Booleans())
		signal := hegel.Draw(ht, hegel.SampledFrom([]string{signalTraces, signalLogs}))
		env := hegel.Draw(ht, hegel.Booleans())
		gen := ""
		if hegel.Draw(ht, hegel.Booleans()) {
			gen, _ = genID(time.Unix(hegel.Draw(ht, hegel.Integers[int64](0, 1<<33)), 0), time.Hour)
		}
		var diskAsked []string
		ts := buildTableSet(c, signal, gen, env, func(table string) string {
			diskAsked = append(diskAsked, c.Database+"."+table)
			return ""
		})

		cols := columns(signal, env)
		if want := fmt.Sprintf("INSERT INTO %s (%s)", ts.target, quoteColumns(cols)); ts.insert != want {
			ht.Fatalf("insert %q, want %q", ts.insert, want)
		}
		sc, err := parseStructure(structure(signal, env))
		if err != nil {
			ht.Fatal(err)
		}
		if len(sc) != len(cols) {
			ht.Fatalf("structure has %d columns, columns() %d", len(sc), len(cols))
		}
		for i := range sc {
			if sc[i].name != cols[i] {
				ht.Fatalf("column %d: structure %q, columns() %q", i, sc[i].name, cols[i])
			}
		}
		main := ts.ddl[0]
		for _, col := range cols {
			nested, field, ok := strings.Cut(col, ".")
			if ok {
				i := strings.Index(main, "\n    "+nested+" Nested (")
				if i < 0 || !strings.Contains(main[i:], "\n        "+field+" ") {
					ht.Fatalf("DDL lacks %s", col)
				}
			} else if !strings.Contains(main, "\n    "+col+" ") {
				ht.Fatalf("DDL lacks %s", col)
			}
		}
		if hasEnv := strings.Contains(main, "batch_id UInt64"); hasEnv != env {
			ht.Fatalf("envelope in DDL %v, want %v", hasEnv, env)
		}

		created := map[string]int{}
		for i, q := range ts.ddl {
			m := createRe.FindStringSubmatch(q)
			if m == nil {
				ht.Fatalf("not a CREATE ... IF NOT EXISTS: %.80s", q)
			}
			created[m[1]] = i
			if strings.HasSuffix(m[1], "_in") && !strings.Contains(q, "("+structure(signal, env)+") ENGINE = Null") {
				ht.Fatalf("staging table structure differs: %.200s", q)
			}
		}
		for _, name := range append(append(append([]string{}, ts.stored...), ts.local...), ts.target) {
			if _, ok := created[name]; !ok {
				ht.Fatalf("%s is never created (created: %v)", name, created)
			}
		}
		for _, s := range ts.stored {
			for _, l := range ts.local {
				if s == l {
					ht.Fatalf("%s is both stored and local", s)
				}
			}
		}
		// Views before the tables they read or write, in drop order.
		pos := map[string]int{}
		for i, l := range ts.local {
			pos[l] = i
		}
		for i, l := range ts.local {
			if !strings.HasSuffix(l, "_mv") {
				continue
			}
			q := ts.ddl[created[l]]
			for _, re := range []*regexp.Regexp{regexp.MustCompile(`TO (\S+)`), regexp.MustCompile(`FROM (\S+)`)} {
				if m := re.FindStringSubmatch(q); m != nil {
					if j, ok := pos[m[1]]; ok && j < i {
						ht.Fatalf("drop order: %s dropped before its view %s", m[1], l)
					}
				}
			}
		}
		if !reflect.DeepEqual(diskAsked, ts.stored) {
			ht.Fatalf("disk asked for %v, stored tables %v", diskAsked, ts.stored)
		}
		// A reader of a published generation gets the same stored tables.
		if gen != "" && env {
			var mt []ManifestTable
			for _, s := range ts.stored {
				mt = append(mt, ManifestTable{Table: s, Endpoint: "http://x/" + s})
			}
			rd := ReaderDDL(c, "reader", signal, gen, mt, "k", "s", 1)
			if len(rd) != 1+len(ts.stored) {
				ht.Fatalf("reader DDL has %d statements for %d stored tables", len(rd), len(ts.stored))
			}
			for i, s := range ts.stored {
				if !strings.Contains(rd[i+1], "'http://x/"+s+"'") {
					ht.Fatalf("reader DDL for %s lacks its endpoint", s)
				}
			}
		}
	}, pbtOpts(500)...)
}

// ttlRendered parses ttlExpr's interval back to a duration.
var ttlRe = regexp.MustCompile(`toInterval(Day|Hour|Minute|Second)\((\d+)\)$`)

func ttlRendered(expr string) time.Duration {
	m := ttlRe.FindStringSubmatch(expr)
	n, _ := strconv.ParseInt(m[2], 10, 64)
	unit := map[string]time.Duration{"Day": 24 * time.Hour, "Hour": time.Hour, "Minute": time.Minute, "Second": time.Second}[m[1]]
	return time.Duration(n) * unit
}

// TestPBTTTLWholeSeconds: a TTL of whole seconds renders exactly, in the
// largest unit that divides it.
func TestPBTTTLWholeSeconds(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		d := time.Duration(hegel.Draw(ht, hegel.Integers[int64](1, 1e9))) * time.Second
		expr := ttlExpr(d, "ts")
		if got := ttlRendered(expr); got != d {
			ht.Fatalf("ttl %v rendered %q = %v", d, expr, got)
		}
	}, pbtOpts(500)...)
}

// TestPBTFindingSubSecondTTL: ttl is a duration and Validate accepts any
// value, but ttlExpr truncates to whole seconds, so 1500ms keeps data for 1 s
// and anything under a second (ttl: 500ms) renders toIntervalSecond(0): rows
// expire as soon as they are merged.
func TestPBTFindingSubSecondTTL(t *testing.T) {
	expectFinding(t, "a TTL that Validate accepts but ttlExpr does not render", func(tc hegel.TestCase) {
		c := createDefaultConfig().(*Config)
		c.TTL = time.Duration(hegel.Draw(tc, hegel.Integers[int64](1, int64(48*time.Hour))))
		if c.Validate() != nil {
			return
		}
		if expr := ttlExpr(c.TTL, "ts"); ttlRendered(expr) != c.TTL {
			tc.Errorf("ttl %v is accepted and rendered %q (%v)", c.TTL, expr, ttlRendered(expr))
		}
	}, pbtOpts(500)...)
}

// ---- Config.Validate ----------------------------------------------------------

// validateOracle restates the documented rules (config.go, config_publish.go,
// README) independently of Validate, returning the names of the rules a
// config breaks. strict adds rules the documentation implies but Validate
// does not check (see the finding tests).
func validateOracle(c *Config, strict bool) []string {
	var bad []string
	ident := regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	segment := regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)
	http := func(u string) bool { return strings.HasPrefix(u, "http://") || strings.HasPrefix(u, "https://") }
	for name, v := range map[string]string{"database": c.Database, "traces_table_name": c.TracesTableName, "logs_table_name": c.LogsTableName} {
		if !ident.MatchString(v) {
			bad = append(bad, name)
		}
	}
	if c.InsertFormat != "rowbinary" && c.InsertFormat != "json" && c.InsertFormat != "file" {
		bad = append(bad, "insert_format")
	}
	if c.Connections < 1 {
		bad = append(bad, "connections")
	}
	if c.BufferSeconds < 0 {
		bad = append(bad, "buffer_seconds")
	}
	os, pq := c.ObjectStorage.Endpoint != "", c.Parquet.URL != ""
	if !os && !pq {
		if !c.StoreTables {
			bad = append(bad, "store_tables")
		}
	} else {
		if !segment.MatchString(c.Producer.ID) {
			bad = append(bad, "producer.id")
		}
		if !segment.MatchString(c.Producer.Region) {
			bad = append(bad, "producer.region")
		}
		if c.Producer.Epoch != "" && !segment.MatchString(c.Producer.Epoch) {
			bad = append(bad, "producer.epoch")
		}
		if c.InsertFormat != "rowbinary" {
			bad = append(bad, "rowbinary")
		}
		if os {
			if !http(c.ObjectStorage.Endpoint) {
				bad = append(bad, "object_storage.endpoint")
			}
			if !c.StoreTables {
				bad = append(bad, "store_tables")
			}
		}
		// Generations rotate Parquet-only publishers too (Config.generation).
		if g := c.ObjectStorage.Generation; (os || strict) && g != 0 && g < time.Second {
			bad = append(bad, "generation")
		}
		if r := c.ObjectStorage.LocalRetention; r < 0 || r >= 72*time.Hour {
			bad = append(bad, "local_retention")
		}
		if pq {
			if !http(c.Parquet.URL) && !strings.HasPrefix(c.Parquet.URL, "file:///") {
				bad = append(bad, "parquet.url")
			}
			switch c.Parquet.Compression {
			case "zstd", "lz4", "snappy", "gzip", "none":
			default:
				bad = append(bad, "parquet.compression")
			}
		}
	}
	// A TTL has to mean what it says: ttlExpr renders whole seconds.
	if strict && c.TTL > 0 && c.TTL%time.Second != 0 {
		bad = append(bad, "ttl")
	}
	return bad
}

// drawConfig draws a config with every field from a mix of valid and invalid
// values, weighted so that most configs break at most a rule or two.
func drawConfig(tc hegel.TestCase) *Config {
	c := createDefaultConfig().(*Config)
	pick := func(good []string, bad ...string) string {
		if hegel.Draw(tc, hegel.WeightedBooleans(0.85)) {
			return hegel.Draw(tc, hegel.SampledFrom(good))
		}
		return hegel.Draw(tc, hegel.OneOf(hegel.SampledFrom(bad), hegel.Text().MaxSize(6)))
	}
	c.Database = pick([]string{"otel", "_x", "db1"}, "", "1db", "otel; DROP", "a-b", "a.b")
	c.TracesTableName = pick([]string{"otel_traces", "t"}, "", "t t", "`t`")
	c.LogsTableName = pick([]string{"otel_logs", "l"}, "", "9", "l'")
	c.InsertFormat = pick([]string{"rowbinary", "json", "file"}, "", "csv", "RowBinary")
	c.Connections = hegel.Draw(tc, hegel.Integers(-1, 4))
	c.BufferSeconds = hegel.Draw(tc, hegel.Integers(-1, 3))
	c.StoreTables = hegel.Draw(tc, hegel.WeightedBooleans(0.8))
	c.TTL = time.Duration(hegel.Draw(tc, hegel.OneOf(hegel.Just[int64](0), hegel.Integers[int64](-1e10, 1e14))))
	if hegel.Draw(tc, hegel.Booleans()) {
		c.ObjectStorage.Endpoint = pick([]string{"http://127.0.0.1:8333/otel", "https://b.s3.amazonaws.com/p"}, "s3://b", "127.0.0.1:8333", "ftp://x")
	}
	if hegel.Draw(tc, hegel.Booleans()) {
		c.Parquet.URL = pick([]string{"file:///tmp/pq", "http://127.0.0.1:8333/pq"}, "relative/dir", "file://host/x", "/abs")
	}
	c.Parquet.Compression = pick([]string{"zstd", "lz4", "snappy", "gzip", "none"}, "", "brotli", "ZSTD")
	c.Producer.ID = pick([]string{"edge-1", "host.local", "A_b"}, "", "has/slash", "-lead", ".dot", "sp ace")
	c.Producer.Region = pick([]string{"eu-west-1", "local"}, "", "a/b", "_x")
	c.Producer.Epoch = pick([]string{"", "20260924T191120Z-47366a"}, "a/b", "-x")
	c.ObjectStorage.Generation = time.Duration(hegel.Draw(tc, hegel.OneOf(hegel.SampledFrom([]int64{0, int64(time.Hour), int64(time.Second)}),
		hegel.Integers[int64](-int64(time.Hour), int64(2*time.Hour)))))
	c.ObjectStorage.LocalRetention = time.Duration(hegel.Draw(tc, hegel.OneOf(hegel.SampledFrom([]int64{0, int64(6 * time.Hour), int64(72*time.Hour) - 1, int64(72 * time.Hour)}),
		hegel.Integers[int64](-int64(time.Hour), int64(100*time.Hour)))))
	c.ObjectStorage.SecretAccessKey = configopaque.String(hegel.Draw(tc, hegel.Text().MaxSize(4)))
	return c
}

func checkValidate(tc hegel.TestCase, c *Config, strict bool) {
	want := validateOracle(c, strict)
	err := c.Validate()
	if (err == nil) != (len(want) == 0) {
		tc.Errorf("Validate() = %v, but the documented rules say %v\nconfig: db=%q traces=%q logs=%q format=%q conns=%d buf=%d store=%v ttl=%v os=%q gen=%v ret=%v pq=%q comp=%q id=%q region=%q epoch=%q",
			err, want, c.Database, c.TracesTableName, c.LogsTableName, c.InsertFormat, c.Connections, c.BufferSeconds, c.StoreTables, c.TTL,
			c.ObjectStorage.Endpoint, c.ObjectStorage.Generation, c.ObjectStorage.LocalRetention, c.Parquet.URL, c.Parquet.Compression,
			c.Producer.ID, c.Producer.Region, c.Producer.Epoch)
	}
	for _, rule := range want {
		if err != nil && !strings.Contains(err.Error(), rule) {
			tc.Errorf("config breaks %s, but the error does not say so: %v", rule, err)
		}
	}
}

// TestPBTConfigValidateMatchesRules: Validate accepts exactly the configs
// the documented rules accept, and every error names each rule broken.
func TestPBTConfigValidateMatchesRules(t *testing.T) {
	hegel.Test(t, func(ht *hegel.T) {
		c := drawConfig(ht)
		ht.Event(fmt.Sprintf("valid=%v", len(validateOracle(c, false)) == 0))
		checkValidate(ht, c, false)
	}, pbtOpts(2000)...)
}

// TestPBTFindingParquetGenerationUnchecked: Config.generation() rotates a
// Parquet-only publisher's generations by object_storage.generation too, but
// Validate checks "at least 1s" only when object storage is configured, so a
// Parquet-only publisher accepts generation: 1ns and starts (and seals) a new
// generation for nearly every batch.
func TestPBTFindingParquetGenerationUnchecked(t *testing.T) {
	expectFinding(t, "a Parquet-only config with a sub-second generation that Validate accepts", func(tc hegel.TestCase) {
		c := drawConfig(tc)
		if len(validateOracle(c, false)) != 0 { // only configs Validate is meant to accept
			return
		}
		want := validateOracle(c, true)
		if slices.Contains(want, "ttl") {
			return // the TTL finding has its own test
		}
		checkValidate(tc, c, true)
	}, pbtOpts(3000)...)
}
