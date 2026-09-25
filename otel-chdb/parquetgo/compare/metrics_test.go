package compare

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func newRefExporter(t testing.TB, db string) exporter.Metrics {
	t.Helper()
	e, err := NewRefExporter(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// TestMetricsSameRowsAsExporter feeds the same pmetric data to the contrib
// clickhouseexporter (the reference) and to parquetgo, INSERT…SELECTs
// parquetgo's objects into tables created from the exporter's own DDL, and
// compares every table: count and sum(cityHash64(all columns)), and EXCEPT
// in both directions. It does so with the explicit s3() structure and with
// schema inference, for the serial and the 4-way parallel encoder, and checks
// the envelope columns.
//
// Needs CHDB_TEST_S3(_KEY/_SECRET) and CHDB_TEST_CLICKHOUSE (HTTP; the native
// port is CHDB_TEST_CLICKHOUSE_NATIVE, default host:19000).
func TestMetricsSameRowsAsExporter(t *testing.T) {
	s3, ok := S3FromEnv()
	if !ok || os.Getenv("CHDB_TEST_CLICKHOUSE") == "" {
		t.Skip("needs CHDB_TEST_S3 and CHDB_TEST_CLICKHOUSE")
	}
	run := fmt.Sprintf("m%d", time.Now().UnixNano())
	base := s3.Endpoint + "/metrics-go/" + run
	t.Logf("objects under %s", base)
	data := []struct {
		name string
		md   pmetric.Metrics
	}{
		{"generated", Metrics(3000)},
		{"nasty", NastyMetrics(700)},
	}
	for _, d := range data {
		n, err := parquetgo.MetricPoints(d.md)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%s: points per type %v", d.name, n)
	}
	ctx := context.Background()
	refDB, goDB := "pqm_ref_"+run, "pqm_go_"+run
	defer func() {
		if os.Getenv("PQM_KEEP") != "" {
			t.Logf("kept databases %s and %s", refDB, goDB)
			return
		}
		ch(t, "DROP DATABASE IF EXISTS "+refDB)
		ch(t, "DROP DATABASE IF EXISTS "+goDB)
	}()

	// 1. The reference.
	ref := newRefExporter(t, refDB)
	for _, d := range data {
		if err := ref.ConsumeMetrics(ctx, d.md); err != nil {
			t.Fatalf("clickhouseexporter %s: %v", d.name, err)
		}
	}
	if err := ref.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}

	// 2. parquetgo, serial and parallel.
	received := time.Date(2026, 9, 25, 6, 0, 0, 123456789, time.UTC)
	engines := []string{"parquet-go", "parquet-go-par4"}
	for _, engine := range engines {
		opts := parquetgo.DefaultOptions()
		if engine == "parquet-go-par4" {
			opts.Parallelism = 4
		}
		p, err := parquetgo.New(parquetgo.Config{URL: base + "/" + engine, AccessKeyID: s3.Key, SecretAccessKey: s3.Secret,
			ProducerID: "cmp", Region: "cmp", SchemaVersion: 1, Epoch: run, Parquet: opts,
			Engine: "parquet-go", Now: func() time.Time { return received }})
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range data {
			if err := p.PushMetrics(ctx, d.md); err != nil {
				t.Fatal(err)
			}
		}
		if err := p.Close(ctx); err != nil {
			t.Fatal(err)
		}
	}

	// 3. Compare per table.
	ch(t, "CREATE DATABASE "+goDB)
	for mt := range parquetgo.NumMetricTypes {
		sig := parquetgo.MetricSignals[mt]
		table := "otel_" + sig
		cols := ContribCols(mt)
		refT := refDB + "." + table
		ddl := ch(t, "SHOW CREATE TABLE "+refT+" FORMAT TSVRaw")
		sum := func(from string) string {
			return ch(t, fmt.Sprintf("SELECT count(), sum(cityHash64(%s)) FROM %s SETTINGS use_query_condition_cache = 0", cols, from))
		}
		want := sum(refT)
		t.Logf("%s reference: %s", table, want)
		if strings.HasPrefix(want, "0\t") {
			t.Fatalf("%s: reference is empty", table)
		}
		for _, engine := range engines {
			obj := fmt.Sprintf("%s/%s/cmp/%s/v1/cmp/%s/*/*.parquet", base, engine, sig, run)
			for _, structured := range []bool{true, false} {
				st, suffix := "", "_inferred"
				if structured {
					st, suffix = fmt.Sprintf(", '%s'", MetricStructures[mt]), ""
				}
				src := fmt.Sprintf("s3('%s', '%s', '%s', 'Parquet'%s)", obj, s3.Key, s3.Secret, st)
				tbl := fmt.Sprintf("%s.%s_%s%s", goDB, table, strings.ReplaceAll(engine, "-", "_"), suffix)
				ch(t, regexp.MustCompile(`^CREATE TABLE \S+`).ReplaceAllString(ddl, "CREATE TABLE "+tbl))
				ch(t, fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s", tbl, cols, cols, src))
				if got := sum(tbl); got != want {
					t.Errorf("%s %s%s: got %s, exporter %s", table, engine, suffix, got, want)
				}
				for _, dir := range [][2]string{{tbl, refT}, {refT, tbl}} {
					q := fmt.Sprintf("SELECT count() FROM (SELECT %[1]s FROM %[2]s EXCEPT SELECT %[1]s FROM %[3]s)", cols, dir[0], dir[1])
					if n := ch(t, q); n != "0" {
						t.Errorf("%s %s%s: %s rows in %s not in %s", table, engine, suffix, n, dir[0], dir[1])
					}
				}
				if structured {
					// The envelope: one batch per dataset, row_ordinal 0..n-1 in
					// each, one received_at, the schema version.
					env := ch(t, fmt.Sprintf(`SELECT batch_id, count(), min(row_ordinal), max(row_ordinal), uniqExact(row_ordinal),
  any(producer_id), any(producer_epoch) = '%s', uniqExact(received_at), any(received_at) = toDateTime64('%s', 9, 'UTC'), any(schema_version)
  FROM %s GROUP BY batch_id ORDER BY batch_id FORMAT TSV`, run, received.Format("2006-01-02 15:04:05.000000000"), src))
					for _, line := range strings.Split(env, "\n") {
						f := strings.Split(line, "\t")
						if len(f) != 10 || f[2] != "0" || f[3] != fmt.Sprint(atoi(f[1])-1) || f[4] != f[1] ||
							f[5] != "cmp" || f[6] != "1" || f[7] != "1" || f[8] != "1" || f[9] != "1" {
							t.Errorf("%s %s envelope: %q", table, engine, line)
						}
					}
				}
			}
		}
		t.Logf("%s: every engine matches the exporter (%s)", table, want)
		t.Logf("%s inferred schema:\n%s", table, ch(t, fmt.Sprintf("DESCRIBE s3('%s/parquet-go/cmp/%s/v1/cmp/%s/*/*.parquet', '%s', '%s', 'Parquet') FORMAT TSV",
			base, sig, run, s3.Key, s3.Secret)))
	}
}

func atoi(s string) int {
	var n int
	fmt.Sscan(s, &n)
	return n
}

// TestMetricsCentralSingleBlock checks the central INSERT…SELECT the
// importer runs (FASTPATH.md): into the exporter's table plus the envelope,
// partitioned by the per-batch-constant toDate(received_at), with the
// single-block settings and a dedup token. For 10k-point batches (and the
// hostile data) each object must become exactly one part and two retries
// with the same token must add nothing.
//
// It then reports, without asserting, what happens to one much bigger
// object (150k gauge points, about 160 MB decoded): repeated inserts with
// the single-block set and variants, and a token retry. On ClickHouse 26.10
// that object was often split into two blocks at a row that varied from
// insert to insert (intermittently: some series of inserts split, others
// did not; objects of 100k points or fewer never did), so the token alone
// does not make a retry of such an object idempotent. See README "Metrics".
func TestMetricsCentralSingleBlock(t *testing.T) {
	s3, ok := S3FromEnv()
	if !ok || os.Getenv("CHDB_TEST_CLICKHOUSE") == "" {
		t.Skip("needs CHDB_TEST_S3 and CHDB_TEST_CLICKHOUSE")
	}
	run := fmt.Sprintf("b%d", time.Now().UnixNano())
	base := s3.Endpoint + "/metrics-go/" + run
	db := "pqm_blk_" + run
	ctx := context.Background()
	ch(t, "CREATE DATABASE "+db)
	defer ch(t, "DROP DATABASE IF EXISTS "+db)

	publish := func(url string, mds ...pmetric.Metrics) {
		p, err := parquetgo.New(parquetgo.Config{URL: url, AccessKeyID: s3.Key, SecretAccessKey: s3.Secret,
			ProducerID: "blk", Region: "cmp", SchemaVersion: 1, Epoch: run, Engine: "parquet-go"})
		if err != nil {
			t.Fatal(err)
		}
		for _, md := range mds {
			if err := p.PushMetrics(ctx, md); err != nil {
				t.Fatal(err)
			}
		}
		if err := p.Close(ctx); err != nil {
			t.Fatal(err)
		}
	}
	publish(base, Metrics(10000), NastyMetrics(300))
	publish(base+"/big", MetricsBatch(parquetgo.MetricGauge, 150_000, 1))

	// Reference DDL: the exporter's, from a throwaway exporter database.
	refDB := "pqm_blkref_" + run
	defer ch(t, "DROP DATABASE IF EXISTS "+refDB)
	ref := newRefExporter(t, refDB)
	_ = ref.Shutdown(ctx)
	root := s3.Endpoint[:strings.Index(s3.Endpoint, "/otel")] + "/"

	for mt := range parquetgo.NumMetricTypes {
		sig := parquetgo.MetricSignals[mt]
		table := db + ".otel_" + sig
		ddl := ch(t, "SHOW CREATE TABLE "+refDB+".otel_"+sig+" FORMAT TSVRaw")
		ddl, err := CentralDDL(ddl, table, true)
		if err != nil {
			t.Fatal(err)
		}
		ch(t, ddl)
		ch(t, "SYSTEM STOP MERGES "+table)
		cols := ContribCols(mt) + ", " + strings.Join(EnvelopeCols, ", ")
		objs := strings.Fields(ch(t, fmt.Sprintf("SELECT DISTINCT _path FROM s3('%s/cmp/%s/v1/blk/%s/*/*.parquet', '%s', '%s', 'One') ORDER BY _path FORMAT TSV",
			base, sig, run, s3.Key, s3.Secret)))
		if len(objs) != 2 {
			t.Fatalf("%s: %d objects, want 2", sig, len(objs))
		}
		total := 0
		for k, o := range objs {
			src := fmt.Sprintf("s3('%s', '%s', '%s', 'Parquet', '%s')", root+o, s3.Key, s3.Secret, MetricStructures[mt])
			rows := atoi(ch(t, "SELECT count() FROM "+src))
			total += rows
			ins := fmt.Sprintf("INSERT INTO %s (%s) SETTINGS %s, insert_deduplication_token = 'tok-%s' SELECT %s FROM %s", table, cols, OneBlock, o, cols, src)
			for attempt := 0; attempt < 3; attempt++ { // the retries must be deduplicated
				ch(t, ins)
				parts := ch(t, fmt.Sprintf("SELECT count(), sum(rows), groupArray(rows) FROM system.parts WHERE database = '%s' AND table = 'otel_%s' AND active", db, sig))
				f := strings.Fields(parts)
				if atoi(f[0]) != k+1 || atoi(f[1]) != total {
					t.Errorf("%s object %d (%d rows), attempt %d: parts (count, rows, sizes) = %s; want %d parts, %d rows",
						sig, k+1, rows, attempt, parts, k+1, total)
				}
			}
		}
		t.Logf("%s: %d objects, %d rows, one part each, retries deduplicated", sig, len(objs), total)
	}

	// The big object: report only.
	big := ch(t, fmt.Sprintf("SELECT DISTINCT _path FROM s3('%s/big/cmp/metrics_gauge/v1/blk/%s/*/*.parquet', '%s', '%s', 'One') FORMAT TSV", base, run, s3.Key, s3.Secret))
	src := fmt.Sprintf("s3('%s', '%s', '%s', 'Parquet', '%s')", root+big, s3.Key, s3.Secret, MetricStructures[parquetgo.MetricGauge])
	t.Logf("big object: %s rows, %s bytes decoded", ch(t, "SELECT count() FROM "+src), ch(t, "SELECT sum(byteSize(*)) FROM "+src))
	cols := ContribCols(parquetgo.MetricGauge) + ", " + strings.Join(EnvelopeCols, ", ")
	squash := strings.NewReplacer("min_insert_block_size_rows = 0", "min_insert_block_size_rows = 1048576",
		"min_insert_block_size_bytes = 0", "min_insert_block_size_bytes = 17179869184").Replace(OneBlock)
	for i, v := range []struct{ name, settings string }{
		{"single-block", OneBlock},
		{"single-block + preserve_order", OneBlock + ", input_format_parquet_preserve_order = 1"},
		{"single-block + no offset index", OneBlock + ", input_format_parquet_use_offset_index = 0"},
		{"squashing on", squash},
		{"server defaults", ""},
	} {
		var parts []string
		for j := 0; j < 4; j++ {
			name := fmt.Sprintf("big_%d_%d", i, j)
			ch(t, "CREATE TABLE "+db+"."+name+" AS "+db+".otel_metrics_gauge")
			ch(t, "SYSTEM STOP MERGES "+db+"."+name)
			st := ""
			if v.settings != "" {
				st = "SETTINGS " + v.settings
			}
			ch(t, fmt.Sprintf("INSERT INTO %s.%s (%s) %s SELECT %s FROM %s", db, name, cols, st, cols, src))
			parts = append(parts, ch(t, fmt.Sprintf("SELECT groupArray(rows) FROM system.parts WHERE database = '%s' AND table = '%s' AND active", db, name)))
		}
		t.Logf("big object, %s, 4 inserts: parts %v", v.name, parts)
	}
	for _, v := range []struct{ name, settings string }{{"single-block", OneBlock}, {"squashing on", squash}} {
		name := "big_retry_" + strings.Fields(v.name)[0]
		name = strings.ReplaceAll(name, "-", "_")
		ch(t, "CREATE TABLE "+db+"."+name+" AS "+db+".otel_metrics_gauge")
		ch(t, "SYSTEM STOP MERGES "+db+"."+name)
		var after []string
		for a := 0; a < 3; a++ {
			ch(t, fmt.Sprintf("INSERT INTO %s.%s (%s) SETTINGS %s, insert_deduplication_token = 'big' SELECT %s FROM %s", db, name, cols, v.settings, cols, src))
			after = append(after, ch(t, fmt.Sprintf("SELECT groupArray(rows) FROM system.parts WHERE database = '%s' AND table = '%s' AND active", db, name)))
		}
		t.Logf("big object, %s, same token 3 times: parts after each %v", v.name, after)
	}
}

func indexOf(s []string, v string) int {
	for i, x := range s {
		if x == v {
			return i
		}
	}
	return -1
}
