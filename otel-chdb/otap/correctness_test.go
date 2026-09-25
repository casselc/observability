package otap

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/chdbexporter/testgen"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo/compare"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/arrow_record"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// Environment: OTAP_TEST_S3 (http://host/bucket/prefix), OTAP_TEST_S3_KEY,
// OTAP_TEST_S3_SECRET, OTAP_TEST_CLICKHOUSE (http://host:port).
type env struct{ s3, key, secret, ch string }

func testEnv(t *testing.T) env {
	e := env{os.Getenv("OTAP_TEST_S3"), os.Getenv("OTAP_TEST_S3_KEY"), os.Getenv("OTAP_TEST_S3_SECRET"), os.Getenv("OTAP_TEST_CLICKHOUSE")}
	if e.s3 == "" || e.ch == "" {
		t.Skip("needs OTAP_TEST_S3 and OTAP_TEST_CLICKHOUSE")
	}
	return e
}

func chQuery(chURL, sql string, settings map[string]string) (string, error) {
	q := url.Values{}
	for k, v := range settings {
		q.Set(k, v)
	}
	resp, err := http.Post(chURL+"/?"+q.Encode(), "text/plain", strings.NewReader(sql))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("clickhouse: %s", strings.TrimSpace(string(b)))
	}
	return strings.TrimSpace(string(b)), nil
}

func (e env) q(t *testing.T, sql string) string {
	t.Helper()
	out, err := chQuery(e.ch, sql, nil)
	if err != nil {
		t.Fatalf("%v\n%.600s", err, sql)
	}
	return out
}

// Central target tables: the otel_traces / otel_logs column types
// (parquetgo/compare/correctness_test.go).
const (
	centralEnvelope = `, producer_id LowCardinality(String), producer_epoch LowCardinality(String), batch_id UInt64, row_ordinal UInt32, received_at DateTime64(9), schema_version UInt16`
	CentralTraces   = `(Timestamp DateTime64(9), TraceId String, SpanId String, ParentSpanId String, TraceState String,
  SpanName LowCardinality(String), SpanKind LowCardinality(String), ServiceName LowCardinality(String),
  ResourceAttributes Map(LowCardinality(String), String), ScopeName String, ScopeVersion String,
  SpanAttributes Map(LowCardinality(String), String), Duration UInt64, StatusCode LowCardinality(String), StatusMessage String,
  Events Nested (Timestamp DateTime64(9), Name LowCardinality(String), Attributes Map(LowCardinality(String), String)),
  Links Nested (TraceId String, SpanId String, TraceState String, Attributes Map(LowCardinality(String), String))` +
		centralEnvelope + `) ENGINE = MergeTree ORDER BY (ServiceName, SpanName, toDateTime(Timestamp))`
	CentralLogs = `(Timestamp DateTime64(9), TraceId String, SpanId String, TraceFlags UInt8, SeverityText LowCardinality(String),
  SeverityNumber UInt8, ServiceName LowCardinality(String), Body String, ResourceSchemaUrl LowCardinality(String),
  ResourceAttributes Map(LowCardinality(String), String), ScopeSchemaUrl LowCardinality(String), ScopeName String,
  ScopeVersion LowCardinality(String), ScopeAttributes Map(LowCardinality(String), String),
  LogAttributes Map(LowCardinality(String), String), EventName String` +
		centralEnvelope + `) ENGINE = MergeTree ORDER BY (ServiceName, Timestamp)`
)

// Column lists for comparison. "raw" is every column except row_ordinal
// (OTAP reorders rows, so the ordinal of a row differs by design); "norm"
// also sorts Map keys and the events/links of a span, which OTAP reorders.
var (
	rawTrace  = strings.Replace(TraceCols, " row_ordinal,", "", 1)
	rawLog    = strings.Replace(LogCols, " row_ordinal,", "", 1)
	normTrace = "Timestamp, TraceId, SpanId, ParentSpanId, TraceState, SpanName, SpanKind, ServiceName, toString(mapSort(ResourceAttributes)), ScopeName, ScopeVersion, toString(mapSort(SpanAttributes)), Duration, StatusCode, StatusMessage, " +
		"toString(arraySort(arrayMap((t, n, a) -> toString((t, n, mapSort(a))), `Events.Timestamp`, `Events.Name`, `Events.Attributes`))), " +
		"toString(arraySort(arrayMap((t, s, st, a) -> toString((t, s, st, mapSort(a))), `Links.TraceId`, `Links.SpanId`, `Links.TraceState`, `Links.Attributes`))), " +
		"producer_id, producer_epoch, batch_id, received_at, schema_version"
	normLog = "Timestamp, TraceId, SpanId, TraceFlags, SeverityText, SeverityNumber, ServiceName, Body, ResourceSchemaUrl, toString(mapSort(ResourceAttributes)), ScopeSchemaUrl, ScopeName, ScopeVersion, toString(mapSort(ScopeAttributes)), toString(mapSort(LogAttributes)), EventName, producer_id, producer_epoch, batch_id, received_at, schema_version"
)

type dataset struct {
	name string
	td   ptrace.Traces
	ld   plog.Logs
}

// TestCentralMatchesReference publishes the same batches as parquetgo
// (the reference, verified row-identical to the clickhouse exporter's
// schema) and through each OTAP variant, ingests every variant into
// central-typed tables on the ClickHouse server, and compares.
func TestCentralMatchesReference(t *testing.T) {
	e := testEnv(t)
	run := fmt.Sprintf("c%d", time.Now().Unix())
	base := e.s3 + "/" + run
	t.Logf("objects under %s", base)
	data := []dataset{{"testgen", testgen.Traces(3000), testgen.Logs(3000)}}
	if os.Getenv("OTAP_TEST_NASTY") != "0" {
		data = append(data, dataset{"nasty", compare.NastyTraces(700), compare.NastyLogs(700)})
	}
	fixed := time.Date(2026, 9, 25, 1, 2, 3, 456789012, time.UTC)
	ctx := context.Background()
	variants := []string{Ref, Star, FlatParquet, FlatArrow, ViaPdata, Raw}
	manifests := map[string][]Manifest{}
	for _, v := range variants {
		sink, err := NewSink(base, e.key, e.secret, nil)
		if err != nil {
			t.Fatal(err)
		}
		p := &Publisher{Sink: sink, Producer: "cmp", Epoch: run, SchemaVersion: 1, Parquet: parquetgo.DefaultOptions(),
			Now: func() time.Time { return fixed }}
		for _, d := range data {
			var mt, ml Manifest
			if v == Ref {
				td, ld := d.td, d.ld
				if mt, err = p.PublishRef(ctx, &td, nil); err != nil {
					t.Fatal(err)
				}
				if ml, err = p.PublishRef(ctx, nil, &ld); err != nil {
					t.Fatal(err)
				}
			} else {
				tb, err := EncodeTraces(d.td)
				if err != nil {
					t.Fatal(err)
				}
				lb, err := EncodeLogs(d.ld)
				if err != nil {
					t.Fatal(err)
				}
				if mt, err = p.PublishBAR(ctx, v, tb); err != nil {
					if !errors.Is(err, ErrLibraryDroppedBatch) {
						t.Fatalf("%s %s traces: %v", v, d.name, err)
					}
					t.Logf("%s %s traces: %v", v, d.name, err)
				}
				if ml, err = p.PublishBAR(ctx, v, lb); err != nil {
					if !errors.Is(err, ErrLibraryDroppedBatch) {
						t.Fatalf("%s %s logs: %v", v, d.name, err)
					}
					t.Logf("%s %s logs: %v", v, d.name, err)
				}
			}
			manifests[v] = append(manifests[v], mt, ml)
		}
	}

	db := "otapcmp_" + run
	e.q(t, "CREATE DATABASE IF NOT EXISTS "+db)
	if os.Getenv("OTAP_TEST_KEEP") == "" {
		defer e.q(t, "DROP DATABASE IF EXISTS "+db)
	}
	cred := fmt.Sprintf("'%s', '%s'", e.key, e.secret)
	for _, sig := range []struct{ name, structure, cols, central, raw, norm string }{
		{"traces", FlatTraceStructure, TraceCols, CentralTraces, rawTrace, normTrace},
		{"logs", FlatLogStructure, LogCols, CentralLogs, rawLog, normLog},
	} {
		for _, v := range variants {
			tbl := fmt.Sprintf("%s.%s_%s", db, sig.name, strings.ReplaceAll(v, "-", "_"))
			e.q(t, fmt.Sprintf("CREATE TABLE %s %s", tbl, sig.central))
			var sel string
			switch v {
			case Star:
				present := map[string]bool{}
				for _, m := range manifests[v] {
					if m.Signal == sig.name {
						for _, o := range m.Objects {
							present[o.Table] = true
						}
					}
				}
				src := func(table string) string {
					if !present[table] {
						return fmt.Sprintf("null('%s')", StarStructure(table))
					}
					return fmt.Sprintf("s3('%s/%s/%s/*/%s.parquet', %s, 'Parquet', '%s')", base, sig.name, v, table, cred, StarStructure(table))
				}
				if sig.name == "traces" {
					sel = StarTracesSelect(src)
				} else {
					sel = StarLogsSelect(src)
				}
				sel = fmt.Sprintf("SELECT %s FROM (%s)", sig.cols, sel)
			case Raw:
				sel = fmt.Sprintf("SELECT %s FROM (%s)", sig.cols, rawSelect(t, e, base, cred, sig.name, "*", manifests[v]))
			case FlatArrow:
				sel = fmt.Sprintf("SELECT %s FROM s3('%s/%s/%s/*/otel_%s.arrow', %s, 'Arrow', '%s')", sig.cols, base, sig.name, v, sig.name, cred, sig.structure)
			default:
				sel = fmt.Sprintf("SELECT %s FROM s3('%s/%s/%s/*/otel_%s.parquet', %s, 'Parquet', '%s')", sig.cols, base, sig.name, v, sig.name, cred, sig.structure)
			}
			_, err := chQuery(e.ch, fmt.Sprintf("INSERT INTO %s (%s) %s", tbl, sig.cols, sel), map[string]string{"max_threads": "1"})
			if err != nil && v == Raw {
				t.Logf("%s raw, all batches: %.400s", sig.name, err)
				sel = fmt.Sprintf("SELECT %s FROM (%s)", sig.cols, rawSelect(t, e, base, cred, sig.name, fmt.Sprintf("%020d", 1), manifests[v]))
				_, err = chQuery(e.ch, fmt.Sprintf("INSERT INTO %s (%s) %s", tbl, sig.cols, sel), map[string]string{"max_threads": "1"})
				t.Logf("%s raw: ingested batch 1 (testgen) only", sig.name)
			}
			if err != nil {
				t.Fatalf("%s %s: %v\n%.3000s", sig.name, v, err, sel)
			}
		}
		sum := func(tbl, cols string) string {
			return e.q(t, fmt.Sprintf("SELECT count(), sum(cityHash64(%s)) FROM %s", cols, tbl))
		}
		ref := fmt.Sprintf("%s.%s_ref", db, sig.name)
		wantRaw, wantNorm := sum(ref, sig.raw), sum(ref, sig.norm)
		t.Logf("%s ref: exact %s, normalised %s", sig.name, wantRaw, wantNorm)
		if strings.HasPrefix(wantRaw, "0\t") {
			t.Fatalf("%s: reference ingested no rows", sig.name)
		}
		for _, v := range variants[1:] {
			tbl := fmt.Sprintf("%s.%s_%s", db, sig.name, strings.ReplaceAll(v, "-", "_"))
			gotRaw, gotNorm := sum(tbl, sig.raw), sum(tbl, sig.norm)
			for _, d := range []struct {
				label, cols string
			}{{"exact", sig.raw}, {"normalised", sig.norm}} {
				for _, bid := range []string{"1", "2"} {
					diff := e.q(t, fmt.Sprintf("SELECT count() FROM (SELECT %[1]s FROM %[2]s WHERE batch_id = %[4]s EXCEPT SELECT %[1]s FROM %[3]s WHERE batch_id = %[4]s)", d.cols, tbl, ref, bid))
					diff2 := e.q(t, fmt.Sprintf("SELECT count() FROM (SELECT %[1]s FROM %[3]s WHERE batch_id = %[4]s EXCEPT SELECT %[1]s FROM %[2]s WHERE batch_id = %[4]s)", d.cols, tbl, ref, bid))
					t.Logf("%s %-12s batch %s (%s) %-10s: %s rows not in ref, %s ref rows not in it", sig.name, v, bid, data[mustAtoi(bid)-1].name, d.label, diff, diff2)
				}
			}
			status := "MATCH"
			if gotNorm != wantNorm {
				status = "DIFFERS"
			}
			t.Logf("%s %-12s exact %s (ref %s) | normalised %s (ref %s): %s", sig.name, v, gotRaw, wantRaw, gotNorm, wantNorm, status)
			if gotNorm != wantNorm && os.Getenv("OTAP_TEST_EXPLAIN") != "" {
				explain(t, e, sig.name, tbl, ref)
			}
		}
	}
}

// rawSelect builds the central query over raw OTAP payloads: DESCRIBE each
// table (OTAP columns are optional), take the envelope from the manifests.
func rawSelect(t *testing.T, e env, base, cred, signal, batches string, ms []Manifest) string {
	tables := map[string]bool{}
	renv := RawEnvelope{Received: map[uint64]time.Time{}}
	for _, m := range ms {
		if m.Signal != signal {
			continue
		}
		renv.Producer, renv.Epoch, renv.Schema = m.ProducerID, m.ProducerEpoch, m.SchemaVersion
		renv.Received[m.BatchID] = m.ReceivedAt
		for _, o := range m.Objects {
			tables[o.Table] = true
		}
	}
	src := map[string]RawSource{}
	for tbl := range tables {
		from := fmt.Sprintf("s3('%s/%s/%s/%s/%s.arrows', %s, 'ArrowStream')", base, signal, Raw, batches, tbl, cred)
		cols := map[string]bool{}
		for _, line := range strings.Split(e.q(t, "DESCRIBE "+from+" FORMAT TSV"), "\n") {
			cols[strings.SplitN(line, "\t", 2)[0]] = true
		}
		src[tbl] = RawSource{From: from, Cols: cols}
	}
	if signal == "traces" {
		return RawTracesSelect(src, renv)
	}
	return RawLogsSelect(src, renv)
}

func mustAtoi(s string) int {
	var n int
	fmt.Sscan(s, &n)
	return n
}

// explain prints, for each normalised column whose multiset of values
// differs from the reference (sum of hashes), a few values present on one
// side only.
func explain(t *testing.T, e env, signal, tbl, ref string) {
	cols := normTrace
	if signal == "logs" {
		cols = normLog
	}
	for _, c := range splitTop(cols) {
		q := fmt.Sprintf("SELECT (SELECT sum(cityHash64(%[1]s)) FROM %[2]s) = (SELECT sum(cityHash64(%[1]s)) FROM %[3]s)", c, tbl, ref)
		if out, err := chQuery(e.ch, q, nil); err != nil || out == "1" {
			continue
		}
		only := func(a, b string) string {
			out, _ := chQuery(e.ch, fmt.Sprintf("SELECT groupArray(3)(toString(x)) FROM (SELECT DISTINCT %[1]s AS x FROM %[2]s EXCEPT SELECT DISTINCT %[1]s AS x FROM %[3]s)", c, a, b), nil)
			return out
		}
		t.Logf("  %.60s differs; only in variant: %.300s | only in ref: %.300s", c, only(tbl, ref), only(ref, tbl))
	}
}

// splitTop splits a comma list at top level (outside parentheses).
func splitTop(s string) []string {
	var out []string
	depth, start := 0, 0
	for i, c := range s {
		switch c {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				out = append(out, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	return append(out, strings.TrimSpace(s[start:]))
}

// TestLibraryCBORMapOrder: the Go otel-arrow consumer decodes CBOR maps
// into a Go map, so a map-valued body or attribute comes back with its keys
// in random order. The ClickHouse row does not change, because
// Value.AsString renders maps through json.Marshal, which sorts keys; but
// anything that hashes or re-encodes the pdata (a content key computed after
// decoding, an OTLP re-export) sees a different value on every decode.
func TestLibraryCBORMapOrder(t *testing.T) {
	ld := plog.NewLogs()
	r := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	m := r.Body().SetEmptyMap()
	for _, k := range []string{"h", "a", "g", "b", "f", "c", "e", "d"} {
		m.PutStr(k, k)
	}
	order := func(m pcommon.Map) string {
		var ks []string
		m.Range(func(k string, _ pcommon.Value) bool { ks = append(ks, k); return true })
		return strings.Join(ks, "")
	}
	want, wantStr := order(m), r.Body().AsString()
	bar, err := EncodeLogs(ld)
	if err != nil {
		t.Fatal(err)
	}
	orders, strs := map[string]int{}, map[string]int{}
	for i := 0; i < 50; i++ {
		lds, err := arrow_record.NewConsumer().LogsFrom(bar)
		if err != nil {
			t.Fatal(err)
		}
		body := lds[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body()
		orders[order(body.Map())]++
		strs[body.AsString()]++
	}
	b, _ := Decode(bar)
	defer b.Release()
	pv := pcommon.NewValueEmpty()
	x, _ := bytesAt(child(col(b.Root, "body"), "ser"), 0)
	if err := decodeCBOR(x, pv); err != nil {
		t.Fatal(err)
	}
	t.Logf("pdata key order %s; library consumer, 50 decodes: %d distinct key orders, %d distinct AsString; this package: %s",
		want, len(orders), len(strs), order(pv.Map()))
	if order(pv.Map()) != want || pv.AsString() != wantStr {
		t.Errorf("ordered decoder: got %s", order(pv.Map()))
	}
	if len(strs) != 1 {
		t.Errorf("AsString varied: %v", strs)
	}
}

// TestEncodeDeterministic checks that encoding the same pdata twice gives
// the same OTAP bytes, so a content key over the payload is stable across
// retries that re-encode.
func TestEncodeDeterministic(t *testing.T) {
	td := testgen.Traces(10000)
	a, err := EncodeTraces(td)
	if err != nil {
		t.Fatal(err)
	}
	b, err := EncodeTraces(td)
	if err != nil {
		t.Fatal(err)
	}
	if ContentKey(a) != ContentKey(b) {
		t.Errorf("content key differs between two encodings of the same batch")
	}
	ld := compare.NastyLogs(700)
	x, _ := EncodeLogs(ld)
	y, _ := EncodeLogs(ld)
	t.Logf("traces stable: %v; nasty logs stable: %v", ContentKey(a) == ContentKey(b), ContentKey(x) == ContentKey(y))
}
