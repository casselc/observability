package chdbexporter

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.uber.org/zap"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/chdbexporter/testgen"
)

// chDB binds one data path per process, so every test in this binary shares
// one, and tests keep apart by database. reader stays open for the whole run:
// it is how tests look at what an exporter wrote after the exporter closed.
var (
	testPath string
	reader   session
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "chdbexporter-test-")
	if err != nil {
		panic(err)
	}
	testPath = dir
	if reader, err = openSession(testPath); err != nil {
		panic(err)
	}
	code := m.Run()
	reader.Close()
	os.RemoveAll(dir)
	os.Exit(code)
}

func query(t testing.TB, q string) string {
	t.Helper()
	s := reader.(interface {
		Query(string, ...string) (result, error)
	})
	r, err := s.Query(q, "TSV")
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	type stringer interface{ String() string }
	out := r.(stringer).String()
	r.Free()
	return strings.TrimRight(out, "\n")
}

func testConfig(db, format string) *Config {
	c := createDefaultConfig().(*Config)
	c.Path = testPath
	c.Database = db
	c.InsertFormat = format
	c.QueueSettings = configoptional.None[exporterhelper.QueueBatchConfig]()
	return c
}

func startExporter(t testing.TB, cfg *Config) *chdbExporter {
	t.Helper()
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	e := newExporter(zap.NewNop(), cfg)
	if err := e.start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatal(err)
	}
	return e
}

func formats() []string {
	if insertSupported {
		return []string{FormatRowBinary, FormatJSON, FormatFile}
	}
	return []string{FormatJSON, FormatFile}
}

// TestFormatsStoreIdenticalRows writes the same traces and logs through every
// insert format and requires the tables to be byte-for-byte equal, then spot
// checks the values the encoders are most likely to get wrong.
func TestFormatsStoreIdenticalRows(t *testing.T) {
	td, ld := testgen.Traces(600), testgen.Logs(400)
	dumps := map[string][2]string{}
	var variants []string
	for _, f := range formats() {
		variants = append(variants, f, f+"_direct")
	}
	for _, v := range variants {
		f, direct := strings.CutSuffix(v, "_direct")
		db := "ident_" + v
		cfg := testConfig(db, f)
		cfg.StagingTables = !direct
		e := startExporter(t, cfg)
		if err := e.pushTraces(context.Background(), td); err != nil {
			t.Fatalf("%s traces: %v", f, err)
		}
		if err := e.pushLogs(context.Background(), ld); err != nil {
			t.Fatalf("%s logs: %v", f, err)
		}
		if err := e.shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
		dumps[v] = [2]string{
			query(t, fmt.Sprintf("SELECT * FROM %s.otel_traces ORDER BY SpanId FORMAT TSV", db)),
			query(t, fmt.Sprintf("SELECT * FROM %s.otel_logs ORDER BY Timestamp FORMAT TSV", db)),
		}
		if got := query(t, fmt.Sprintf("SELECT count() FROM %s.otel_traces", db)); got != "600" {
			t.Fatalf("%s: %s spans", f, got)
		}
		if got := query(t, fmt.Sprintf("SELECT count() FROM %s.otel_logs", db)); got != "400" {
			t.Fatalf("%s: %s logs", f, got)
		}
	}
	base := variants[0]
	for _, f := range variants[1:] {
		for i, sig := range []string{"traces", "logs"} {
			if dumps[f][i] != dumps[base][i] {
				t.Errorf("%s %s differ from %s:\n%s", f, sig, base, firstDiff(dumps[base][i], dumps[f][i]))
			}
		}
	}

	db := "ident_" + base
	checks := map[string]string{
		// Nanosecond timestamps survive.
		"SELECT toString(Timestamp) FROM %s.otel_traces ORDER BY Timestamp LIMIT 1": "2026-09-24 12:00:00.123456789",
		// Hex ids, and "" for an absent parent.
		"SELECT TraceId, SpanId, ParentSpanId FROM %s.otel_traces WHERE SpanId = '00f0000000000001'": "4bf90000000102030405060708090a0b\t00f0000000000001\t",
		"SELECT Duration FROM %s.otel_traces WHERE SpanId = '00f0000001000001'":                      "634000",
		"SELECT SpanKind, StatusCode FROM %s.otel_traces WHERE SpanId = '00f0000001000001'":          "Server\tOk",
		// Attribute types become strings the way AsString renders them.
		"SELECT SpanAttributes['http.response.status_code'], SpanAttributes['feature.new_checkout'], SpanAttributes['app.cart.total'] FROM %s.otel_traces WHERE SpanId = '00f0000001000001'": "250\tfalse\t1.25",
		"SELECT ResourceAttributes['service.name'], ServiceName FROM %s.otel_traces WHERE SpanId = '00f0000000000001'":                                                                       "frontend\tfrontend",
		// A NUL and control characters survive every format.
		"SELECT hex(StatusMessage) FROM %s.otel_traces WHERE SpanId = '00f0000000000001'": strings.ToUpper(fmt.Sprintf("%x", "payment declined:\n\tcard\x00expired")),
		// Nested columns line up.
		"SELECT Events.Name, Events.Attributes[1]['exception.type'], length(Links.TraceId) FROM %s.otel_traces WHERE SpanId = '00f0000000000001'": "['exception']\tTimeoutError\t1",
		"SELECT count() FROM %s.otel_traces_trace_id_ts": "600",
		// Logs: structured bodies, flags, severity, event names.
		"SELECT Body FROM %s.otel_logs WHERE SeverityText = 'DEBUG' LIMIT 1":             `{"attempt":3,"msg":"structured body"}`,
		"SELECT hex(Body) FROM %s.otel_logs WHERE SeverityText = 'ERROR' LIMIT 1":        strings.ToUpper(fmt.Sprintf("%x", "card\x00expired — déclinée")),
		"SELECT TraceFlags, SeverityNumber FROM %s.otel_logs ORDER BY Timestamp LIMIT 1": "1\t9",
		"SELECT countIf(EventName = 'checkout.completed') FROM %s.otel_logs":             "40",
		"SELECT ScopeAttributes['bridge'], ResourceSchemaUrl FROM %s.otel_logs LIMIT 1":  "otelslog\thttps://opentelemetry.io/schemas/1.34.0",
	}
	for q, want := range checks {
		if got := query(t, fmt.Sprintf(q, db)); got != want {
			t.Errorf("%s\n got %q\nwant %q", fmt.Sprintf(q, db), got, want)
		}
	}
}

func firstDiff(a, b string) string {
	al, bl := strings.Split(a, "\n"), strings.Split(b, "\n")
	for i := 0; i < len(al) && i < len(bl); i++ {
		if al[i] != bl[i] {
			return fmt.Sprintf("line %d\n want %q\n  got %q", i, al[i], bl[i])
		}
	}
	return fmt.Sprintf("line counts %d vs %d", len(al), len(bl))
}

// TestBufferTablesFlushOnShutdown: with buffer_seconds, rows land in the
// Buffer and reach MergeTree at the latest when the exporter shuts down.
func TestBufferTablesFlushOnShutdown(t *testing.T) {
	cfg := testConfig("buffered", formats()[0])
	cfg.BufferSeconds = 3600
	e := startExporter(t, cfg)
	if err := e.pushTraces(context.Background(), testgen.Traces(50)); err != nil {
		t.Fatal(err)
	}
	if got := query(t, "SELECT count() FROM buffered.otel_traces_buf"); got != "50" {
		t.Fatalf("buffer holds %s rows", got)
	}
	if err := e.shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	// The Buffer engine's count() includes the destination, so ask MergeTree.
	if got := query(t, "SELECT count() FROM buffered.otel_traces"); got != "50" {
		t.Fatalf("MergeTree holds %s rows after shutdown", got)
	}
}

// TestConcurrentPushesOverConnectionPool pushes from several goroutines, as
// the sending queue's consumers do, through a pool of connections.
func TestConcurrentPushesOverConnectionPool(t *testing.T) {
	cfg := testConfig("concurrent", formats()[0])
	cfg.Connections = 3
	e := startExporter(t, cfg)
	td := testgen.Traces(200)
	errs := make(chan error, 8)
	for g := 0; g < 8; g++ {
		go func() {
			var err error
			for i := 0; i < 5 && err == nil; i++ {
				err = e.pushTraces(context.Background(), td)
			}
			errs <- err
		}()
	}
	for g := 0; g < 8; g++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if err := e.shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := query(t, "SELECT count() FROM concurrent.otel_traces"); got != "8000" {
		t.Fatalf("%s rows, want 8000", got)
	}
}

// TestThroughFactory drives the exporter the way a collector does: factory,
// default config plus a path, exporterhelper with its queue, start, consume,
// shutdown.
func TestThroughFactory(t *testing.T) {
	f := NewFactory()
	cfg := f.CreateDefaultConfig().(*Config)
	cfg.Path = testPath
	cfg.Database = "factory"
	if !insertSupported {
		cfg.InsertFormat = FormatJSON
	}
	set := exportertest.NewNopSettings(f.Type())
	ctx := context.Background()
	te, err := f.CreateTraces(ctx, set, cfg)
	if err != nil {
		t.Fatal(err)
	}
	le, err := f.CreateLogs(ctx, set, cfg)
	if err != nil {
		t.Fatal(err)
	}
	host := componenttest.NewNopHost()
	if err := te.Start(ctx, host); err != nil {
		t.Fatal(err)
	}
	if err := le.Start(ctx, host); err != nil {
		t.Fatal(err)
	}
	if err := te.ConsumeTraces(ctx, testgen.Traces(30)); err != nil {
		t.Fatal(err)
	}
	if err := le.ConsumeLogs(ctx, testgen.Logs(20)); err != nil {
		t.Fatal(err)
	}
	// Shutdown drains the queue before closing the sessions.
	if err := te.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := le.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if got := query(t, "SELECT (SELECT count() FROM factory.otel_traces), (SELECT count() FROM factory.otel_logs)"); got != "30\t20" {
		t.Fatalf("got %q", got)
	}
}

func TestConfigValidate(t *testing.T) {
	c := createDefaultConfig().(*Config)
	if err := c.Validate(); err != nil {
		t.Fatalf("default config invalid: %v", err)
	}
	c.Database = "otel; DROP TABLE x"
	c.InsertFormat = "csv"
	c.Connections = 0
	err := c.Validate()
	for _, want := range []string{"database", "insert_format", "connections"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("want an error about %s, got %v", want, err)
		}
	}
}
