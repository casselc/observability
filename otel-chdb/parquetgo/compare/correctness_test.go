package compare

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/chdbexporter/testgen"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// The chdb exporter's plain-typed Parquet structure (chdbexporter/schema.go
// traceStructure/logStructure + envelopeStructure), which is also what the
// central ingest passes to s3().
const (
	envStructure    = ", producer_id String, producer_epoch String, batch_id UInt64, row_ordinal UInt32, received_at DateTime64(9), schema_version UInt16"
	traceStructure  = "Timestamp DateTime64(9), TraceId String, SpanId String, ParentSpanId String, TraceState String, SpanName String, SpanKind String, ServiceName String, ResourceAttributes Map(String, String), ScopeName String, ScopeVersion String, SpanAttributes Map(String, String), Duration UInt64, StatusCode String, StatusMessage String, `Events.Timestamp` Array(DateTime64(9)), `Events.Name` Array(String), `Events.Attributes` Array(Map(String, String)), `Links.TraceId` Array(String), `Links.SpanId` Array(String), `Links.TraceState` Array(String), `Links.Attributes` Array(Map(String, String))" + envStructure
	logStructure    = "Timestamp DateTime64(9), TraceId String, SpanId String, TraceFlags UInt8, SeverityText String, SeverityNumber UInt8, ServiceName String, Body String, ResourceSchemaUrl String, ResourceAttributes Map(String, String), ScopeSchemaUrl String, ScopeName String, ScopeVersion String, ScopeAttributes Map(String, String), LogAttributes Map(String, String), EventName String" + envStructure
	centralEnvelope = `, producer_id LowCardinality(String), producer_epoch LowCardinality(String), batch_id UInt64, row_ordinal UInt32, received_at DateTime64(9), schema_version UInt16`
	// The central targets: the otel_traces/otel_logs column types
	// (LowCardinality, Map(LowCardinality(String), String), Nested).
	centralTraces = `(Timestamp DateTime64(9), TraceId String, SpanId String, ParentSpanId String, TraceState String,
  SpanName LowCardinality(String), SpanKind LowCardinality(String), ServiceName LowCardinality(String),
  ResourceAttributes Map(LowCardinality(String), String), ScopeName String, ScopeVersion String,
  SpanAttributes Map(LowCardinality(String), String), Duration UInt64, StatusCode LowCardinality(String), StatusMessage String,
  Events Nested (Timestamp DateTime64(9), Name LowCardinality(String), Attributes Map(LowCardinality(String), String)),
  Links Nested (TraceId String, SpanId String, TraceState String, Attributes Map(LowCardinality(String), String))` +
		centralEnvelope + `) ENGINE = MergeTree ORDER BY (ServiceName, SpanName, toDateTime(Timestamp))`
	centralLogs = `(Timestamp DateTime64(9), TraceId String, SpanId String, TraceFlags UInt8, SeverityText LowCardinality(String),
  SeverityNumber UInt8, ServiceName LowCardinality(String), Body String, ResourceSchemaUrl LowCardinality(String),
  ResourceAttributes Map(LowCardinality(String), String), ScopeSchemaUrl LowCardinality(String), ScopeName String,
  ScopeVersion LowCardinality(String), ScopeAttributes Map(LowCardinality(String), String),
  LogAttributes Map(LowCardinality(String), String), EventName String` +
		centralEnvelope + `) ENGINE = MergeTree ORDER BY (ServiceName, Timestamp)`
)

var traceCols = "Timestamp, TraceId, SpanId, ParentSpanId, TraceState, SpanName, SpanKind, ServiceName, ResourceAttributes, ScopeName, ScopeVersion, SpanAttributes, Duration, StatusCode, StatusMessage, `Events.Timestamp`, `Events.Name`, `Events.Attributes`, `Links.TraceId`, `Links.SpanId`, `Links.TraceState`, `Links.Attributes`, producer_id, producer_epoch, batch_id, row_ordinal, received_at, schema_version"
var logCols = "Timestamp, TraceId, SpanId, TraceFlags, SeverityText, SeverityNumber, ServiceName, Body, ResourceSchemaUrl, ResourceAttributes, ScopeSchemaUrl, ScopeName, ScopeVersion, ScopeAttributes, LogAttributes, EventName, producer_id, producer_epoch, batch_id, row_ordinal, received_at, schema_version"

func ch(t *testing.T, sql string) string {
	t.Helper()
	out, err := chTry(sql)
	if err != nil {
		t.Fatalf("%v\n%.400s", err, sql)
	}
	return out
}

func chTry(sql string) (string, error) {
	resp, err := http.Post(os.Getenv("CHDB_TEST_CLICKHOUSE")+"/", "text/plain", strings.NewReader(sql))
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

func firstLine(s string, err error) string {
	if err != nil {
		s = err.Error()
	}
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 200 {
		s = s[:200]
	}
	return s
}

type dataset struct {
	name string
	td   ptrace.Traces
	ld   plog.Logs
}

// TestSameRowsAsChdb publishes the same batches through the chdb exporter
// and both Go engines to S3, then has the ClickHouse server read every
// object with s3() and compare: checksums with the explicit structure and
// with schema inference, row-level EXCEPT in both directions, an INSERT into
// central-typed tables, the inferred schemas, and the manifests.
//
// Needs CHDB_LIB_PATH, CHDB_TEST_S3(_KEY/_SECRET) and CHDB_TEST_CLICKHOUSE.
func TestSameRowsAsChdb(t *testing.T) {
	s3, ok := S3FromEnv()
	if !ok || os.Getenv("CHDB_TEST_CLICKHOUSE") == "" || os.Getenv("CHDB_LIB_PATH") == "" {
		t.Skip("needs CHDB_LIB_PATH, CHDB_TEST_S3 and CHDB_TEST_CLICKHOUSE")
	}
	run := fmt.Sprintf("r%d", time.Now().Unix())
	base := s3.Endpoint + "/pqcmp/" + run
	t.Logf("objects under %s", base)
	data := []dataset{
		{"testgen", testgen.Traces(3000), testgen.Logs(3000)},
		{"nasty", NastyTraces(700), NastyLogs(700)},
	}
	ctx := context.Background()

	// 1. chDB.
	dir := t.TempDir()
	cp, err := NewChdbPublisher(dir, "cmp", run, base+"/chdb", s3)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range data {
		if err := cp.Traces.ConsumeTraces(ctx, d.td); err != nil {
			t.Fatal(err)
		}
		if err := cp.Logs.ConsumeLogs(ctx, d.ld); err != nil {
			t.Fatal(err)
		}
	}
	if err := cp.Shutdown(); err != nil {
		t.Fatal(err)
	}

	// The chDB batches' received_at, so the Go publishers stamp the same
	// instant and every column can be compared.
	manifest := func(impl, signal, name string) map[string]any {
		loc := fmt.Sprintf("%s/%s/cmp/%s/v1/cmp/%s/manifests/*/%s", base, impl, signal, run, name)
		raw := ch(t, fmt.Sprintf("SELECT * FROM s3('%s', '%s', '%s', 'RawBLOB') FORMAT RawBLOB", loc, s3.Key, s3.Secret))
		var m map[string]any
		if err := json.Unmarshal([]byte(raw), &m); err != nil {
			t.Fatalf("%s: %v", raw, err)
		}
		return m
	}
	received := func(signal string, batch int) time.Time {
		m := manifest("chdb", signal, fmt.Sprintf("%020d.json", batch))
		ts, err := time.Parse(time.RFC3339Nano, m["received_at"].(string))
		if err != nil {
			t.Fatal(err)
		}
		return ts
	}

	engines := []string{"arrow", "parquet-go", "parquet-go-par4"}
	for _, engine := range engines {
		var now time.Time
		opts := parquetgo.DefaultOptions()
		eng := engine
		if engine == "parquet-go-par4" {
			eng, opts.Parallelism = "parquet-go", 4
		}
		p, err := parquetgo.New(parquetgo.Config{URL: base + "/" + engine, AccessKeyID: s3.Key, SecretAccessKey: s3.Secret,
			ProducerID: "cmp", Region: "cmp", SchemaVersion: 1, Epoch: run, Parquet: opts,
			Engine: eng, Now: func() time.Time { return now }})
		if err != nil {
			t.Fatal(err)
		}
		for i, d := range data {
			now = received("traces", i+1)
			if err := p.PushTraces(ctx, d.td); err != nil {
				t.Fatal(err)
			}
			now = received("logs", i+1)
			if err := p.PushLogs(ctx, d.ld); err != nil {
				t.Fatal(err)
			}
		}
		if err := p.Close(ctx); err != nil {
			t.Fatal(err)
		}
	}

	db := "pqcmp_" + run
	ch(t, "CREATE DATABASE IF NOT EXISTS "+db)
	defer ch(t, "DROP DATABASE IF EXISTS "+db)
	for _, sig := range []struct{ name, structure, cols, central string }{
		{"traces", traceStructure, traceCols, centralTraces},
		{"logs", logStructure, logCols, centralLogs},
	} {
		src := func(impl string, structured bool, batch string) string {
			st := ""
			if structured {
				st = fmt.Sprintf(", '%s'", sig.structure)
			}
			return fmt.Sprintf("s3('%s/%s/cmp/%s/v1/cmp/%s/*/%s.parquet', '%s', '%s', 'Parquet'%s)",
				base, impl, sig.name, run, batch, s3.Key, s3.Secret, st)
		}
		sum := func(from string) string {
			return ch(t, fmt.Sprintf("SELECT count(), sum(cityHash64(%s)) FROM %s SETTINGS use_query_condition_cache = 0", sig.cols, from))
		}
		want := sum(src("chdb", true, "*"))
		t.Logf("%s chdb: %s", sig.name, want)
		if strings.HasPrefix(want, "0\t") {
			t.Fatalf("%s: chdb wrote nothing", sig.name)
		}
		wantInferred := sum(src("chdb", false, "*"))
		chdbSchema := ch(t, "DESCRIBE "+src("chdb", false, "*"))
		ch(t, fmt.Sprintf("CREATE TABLE %s.%s_chdb %s", db, sig.name, sig.central))
		ch(t, fmt.Sprintf("INSERT INTO %s.%s_chdb SELECT %s FROM %s", db, sig.name, sig.cols, src("chdb", true, "*")))
		wantCentral := sum(db + "." + sig.name + "_chdb")
		for _, engine := range engines {
			if got := sum(src(engine, true, "*")); got != want {
				t.Errorf("%s %s with structure: got %s, chdb %s", sig.name, engine, got, want)
			}
			if got := sum(src(engine, false, "*")); got != wantInferred {
				t.Errorf("%s %s inferred: got %s, chdb %s", sig.name, engine, got, wantInferred)
			}
			if got := ch(t, "DESCRIBE "+src(engine, false, "*")); got != chdbSchema {
				t.Errorf("%s %s inferred schema differs:\n%s\nchdb:\n%s", sig.name, engine, got, chdbSchema)
			}
			for _, b := range []string{"00000000000000000001", "00000000000000000002"} {
				for _, dir := range [][2]string{{engine, "chdb"}, {"chdb", engine}} {
					q := fmt.Sprintf("SELECT count() FROM (SELECT %[1]s FROM %[2]s EXCEPT SELECT %[1]s FROM %[3]s)",
						sig.cols, src(dir[0], true, b), src(dir[1], true, b))
					if n := ch(t, q); n != "0" {
						t.Errorf("%s batch %s: %s rows in %s not in %s", sig.name, b, n, dir[0], dir[1])
					}
				}
			}
			tbl := fmt.Sprintf("%s.%s_%s", db, sig.name, strings.ReplaceAll(engine, "-", "_"))
			ch(t, fmt.Sprintf("CREATE TABLE %s %s", tbl, sig.central))
			ch(t, fmt.Sprintf("INSERT INTO %s SELECT %s FROM %s", tbl, sig.cols, src(engine, true, "*")))
			if got := sum(tbl); got != wantCentral {
				t.Errorf("%s %s central insert: got %s, chdb %s", sig.name, engine, got, wantCentral)
			}
			// Without a structure: the server infers types from the Parquet.
			tbl2 := tbl + "_inferred"
			ch(t, fmt.Sprintf("CREATE TABLE %s %s", tbl2, sig.central))
			ch(t, fmt.Sprintf("INSERT INTO %s SELECT %s FROM %s", tbl2, sig.cols, src(engine, false, "*")))
			if got := sum(tbl2); got != wantCentral {
				t.Errorf("%s %s central insert (inferred): got %s, chdb %s", sig.name, engine, got, wantCentral)
			}
		}
		t.Logf("%s: every engine matches chdb (%s), central %s", sig.name, want, wantCentral)

		// Manifests: identical but for where the object is and the size field.
		for b := 1; b <= len(data); b++ {
			name := fmt.Sprintf("%020d.json", b)
			cm := manifest("chdb", sig.name, name)
			for _, engine := range engines {
				gm := manifest(engine, sig.name, name)
				diff := compareManifests(cm, gm, base+"/chdb", base+"/"+engine, "rowbinary_bytes")
				if diff != "" {
					t.Errorf("%s %s manifest %s: %s", sig.name, engine, name, diff)
				}
			}
		}
		cs := manifest("chdb", sig.name, "_sealed.json")
		for _, engine := range engines {
			if diff := compareManifests(cs, manifest(engine, sig.name, "_sealed.json"), base+"/chdb", base+"/"+engine, "sealed_at"); diff != "" {
				t.Errorf("%s %s seal: %s", sig.name, engine, diff)
			}
		}
	}
}

func compareManifests(a, b map[string]any, aBase, bBase string, ignore ...string) string {
	var keys []string
	for k := range a {
		keys = append(keys, k)
	}
	for k := range b {
		if _, ok := a[k]; !ok {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	var out []string
	for _, k := range keys {
		av, aok := a[k]
		bv, bok := b[k]
		if aok != bok {
			out = append(out, fmt.Sprintf("%s present %v/%v", k, aok, bok))
			continue
		}
		skip := false
		for _, i := range ignore {
			skip = skip || i == k
		}
		if skip {
			continue
		}
		as, bs := fmt.Sprint(av), fmt.Sprint(bv)
		if s, ok := av.(string); ok {
			as = strings.Replace(s, aBase, "", 1)
			bs = strings.Replace(bv.(string), bBase, "", 1)
		}
		if as != bs {
			out = append(out, fmt.Sprintf("%s: %q vs %q", k, as, bs))
		}
	}
	return strings.Join(out, "; ")
}
