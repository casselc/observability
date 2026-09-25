package chdbexporter

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	execpkg "os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/collector/config/configopaque"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/chdbexporter/testgen"
)

// Object storage tests need an S3 endpoint with a bucket, e.g. SeaweedFS:
//
//	CHDB_TEST_S3=http://127.0.0.1:8333/otel CHDB_TEST_S3_KEY=otel CHDB_TEST_S3_SECRET=otelsecret
//
// Without it they skip; the Parquet-to-local-directory test always runs.
func s3Env(t *testing.T) (endpoint, key, secret string) {
	endpoint = os.Getenv("CHDB_TEST_S3")
	if endpoint == "" {
		t.Skip("CHDB_TEST_S3 not set")
	}
	return strings.TrimRight(endpoint, "/"), os.Getenv("CHDB_TEST_S3_KEY"), os.Getenv("CHDB_TEST_S3_SECRET")
}

var (
	chdbqOnce sync.Once
	chdbqPath string
	chdbqErr  error
)

// chdbq builds cmd/chdbq: readers of published data must be separate
// processes, since chDB binds one data path per process and the point is to
// show another process can read what this one publishes.
func chdbq(t *testing.T) string {
	chdbqOnce.Do(func() {
		chdbqPath = filepath.Join(os.TempDir(), fmt.Sprintf("chdbq-test-%d", os.Getpid()))
		out, err := execpkg.Command("go", "build", "-o", chdbqPath, "./cmd/chdbq").CombinedOutput()
		if err != nil {
			chdbqErr = fmt.Errorf("build chdbq: %v\n%s", err, out)
		}
	})
	if chdbqErr != nil {
		t.Fatal(chdbqErr)
	}
	return chdbqPath
}

func publishConfig(t *testing.T, db, producer string) *Config {
	if !insertSupported {
		t.Skip("publishing streams RowBinary, which needs the chdb-go fork")
	}
	cfg := testConfig(db, FormatRowBinary)
	cfg.Producer.ID = producer
	cfg.Producer.Region = "test"
	return cfg
}

func sortedLines(s string) []string {
	l := strings.Split(strings.TrimSpace(s), "\n")
	sort.Strings(l)
	return l
}

// TestParquetToLocalDirectory: a pure Parquet publisher (no tables) writes
// one object and one manifest per batch, seals the generation at shutdown,
// and the Parquet holds exactly what the table exporter would have stored,
// plus the envelope.
func TestParquetToLocalDirectory(t *testing.T) {
	dir := t.TempDir()
	cfg := publishConfig(t, "pq_local", "pq-local")
	cfg.StoreTables = false
	cfg.Parquet.URL = "file://" + dir
	e := startExporter(t, cfg)
	td1, td2, ld := testgen.Traces(300), testgen.Traces(200), testgen.Logs(250)
	ctx := context.Background()
	for _, err := range []error{e.pushTraces(ctx, td1), e.pushTraces(ctx, td2), e.pushLogs(ctx, ld)} {
		if err != nil {
			t.Fatal(err)
		}
	}
	ns := e.pub.namespace(signalTraces)
	if err := e.shutdown(ctx); err != nil {
		t.Fatal(err)
	}

	// Manifests: one per batch, pointing at its object, then the seal.
	mdirs, _ := filepath.Glob(filepath.Join(dir, ns, "manifests", "g*"))
	if len(mdirs) != 1 {
		t.Fatalf("want one traces generation, got %v", mdirs)
	}
	var ms []batchManifest
	files, _ := filepath.Glob(filepath.Join(mdirs[0], "0*.json"))
	for _, f := range files {
		var m batchManifest
		b, _ := os.ReadFile(f)
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(strings.TrimPrefix(m.Parquet, "file://")); err != nil {
			t.Errorf("manifest %d names a missing object: %v", m.BatchID, err)
		}
		ms = append(ms, m)
	}
	if len(ms) != 2 || ms[0].Rows != 300 || ms[1].Rows != 200 || ms[0].BatchID >= ms[1].BatchID {
		t.Fatalf("manifests: %+v", ms)
	}
	var seal sealManifest
	b, err := os.ReadFile(filepath.Join(mdirs[0], "_sealed.json"))
	if err != nil {
		t.Fatal(err)
	}
	json.Unmarshal(b, &seal)
	if seal.Batches != 2 || seal.Rows != 500 || seal.FirstBatch != ms[0].BatchID || seal.LastBatch != ms[1].BatchID {
		t.Fatalf("seal: %+v", seal)
	}

	// The Parquet, read back: the envelope numbers every row of every batch.
	glob := filepath.Join(dir, "test", "traces") + "/**/*.parquet"
	got := query(t, fmt.Sprintf("SELECT count(), uniqExact(batch_id), min(row_ordinal), max(row_ordinal), any(producer_id), "+
		"countIf(schema_version = 1) FROM file('%s', 'Parquet')", glob))
	if got != "500\t2\t0\t299\tpq-local\t500" {
		t.Fatalf("parquet envelope: %s", got)
	}
	if got := query(t, fmt.Sprintf("SELECT count() FROM file('%s', 'Parquet')", filepath.Join(dir, "test", "logs")+"/**/*.parquet")); got != "250" {
		t.Fatalf("logs parquet rows: %s", got)
	}

	// And the columns hold what a table exporter stores for the same input.
	base := startExporter(t, testConfig("pq_base", FormatRowBinary))
	base.pushTraces(ctx, td1)
	base.pushTraces(ctx, td2)
	base.shutdown(ctx)
	cols := quoteColumns(traceColumns)
	want := sortedLines(query(t, fmt.Sprintf("SELECT %s FROM pq_base.otel_traces FORMAT TSV", cols)))
	have := sortedLines(query(t, fmt.Sprintf("SELECT %s FROM file('%s', 'Parquet') FORMAT TSV", cols, glob)))
	if strings.Join(want, "\n") != strings.Join(have, "\n") {
		t.Fatalf("parquet differs from the table:\n%s", firstDiff(strings.Join(want, "\n"), strings.Join(have, "\n")))
	}
}

// s3Manifests reads every batch manifest under a namespace.
func s3Manifests(t *testing.T, root, ns, key, secret string) []batchManifest {
	raw := query(t, fmt.Sprintf("SELECT json FROM s3('%s/%s/manifests/*/0*.json', '%s', '%s', 'JSONAsString') ORDER BY json FORMAT TSVRaw",
		root, ns, key, secret))
	var ms []batchManifest
	for _, l := range strings.Split(raw, "\n") {
		if l == "" {
			continue
		}
		var m batchManifest
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			t.Fatalf("%v: %s", err, l)
		}
		ms = append(ms, m)
	}
	sort.Slice(ms, func(i, j int) bool { return ms[i].BatchID < ms[j].BatchID })
	return ms
}

// watch starts a reader process that attaches tables read-only and prints
// the last statement's TSV result every 250 ms; lines arrive on the channel.
func watch(t *testing.T, stmts ...string) <-chan string {
	args := append([]string{"-format", "TSV", "-repeat", "120", "-every", "250ms", t.TempDir()}, stmts...)
	cmd := execpkg.Command(chdbq(t), args...)
	cmd.Env = os.Environ()
	out, _ := cmd.StdoutPipe()
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })
	lines := make(chan string, 256)
	go func() {
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			lines <- sc.Text()
		}
		close(lines)
	}()
	return lines
}

func waitFor(t *testing.T, lines <-chan string, want string, within time.Duration) {
	t.Helper()
	deadline := time.After(within)
	last := ""
	for {
		select {
		case l, ok := <-lines:
			if !ok {
				t.Fatalf("reader exited before printing %q (last %q)", want, last)
			}
			last = l
			if l == want {
				return
			}
		case <-deadline:
			t.Fatalf("reader did not print %q within %v (last %q)", want, within, last)
		}
	}
}

// TestObjectStorageReaderFollowsWriter is the architecture's core claim: a
// separate process attaches the writer's s3_plain_rewritable tables
// read-only and sees each new batch while the writer keeps running. Parquet
// goes to the same bucket, and the manifests describe both.
func TestObjectStorageReaderFollowsWriter(t *testing.T) {
	root, key, secret := s3Env(t)
	cfg := publishConfig(t, "os_follow", "os-follow")
	cfg.ObjectStorage.Endpoint, cfg.ObjectStorage.AccessKeyID, cfg.ObjectStorage.SecretAccessKey = root, key, configSecret(secret)
	cfg.Parquet.URL, cfg.Parquet.AccessKeyID, cfg.Parquet.SecretAccessKey = root+"/parquet", key, configSecret(secret)
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	e := startExporter(t, cfg)
	ctx := context.Background()
	if err := e.pushTraces(ctx, testgen.Traces(300)); err != nil {
		t.Fatal(err)
	}
	if err := e.pushLogs(ctx, testgen.Logs(120)); err != nil {
		t.Fatal(err)
	}
	ns := e.pub.namespace(signalTraces)
	ms := s3Manifests(t, root, ns, key, secret)
	if len(ms) != 1 || ms[0].Rows != 300 || len(ms[0].Tables) != 2 || ms[0].Parquet == "" {
		t.Fatalf("manifest after one batch: %+v", ms)
	}
	gen := ms[0].Generation

	ddl := ReaderDDL(cfg, "r", signalTraces, gen, ms[0].Tables, key, secret, 1)
	table := "r." + cfg.TracesTableName + "_" + gen
	lines := watch(t, append(ddl, fmt.Sprintf("SELECT count(), uniqExact(batch_id), (SELECT count() FROM %s_trace_id_ts) FROM %s", table, table))...)
	waitFor(t, lines, "300\t1\t300", 60*time.Second) // the first wait includes loading libchdb

	if err := e.pushTraces(ctx, testgen.Traces(200)); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	waitFor(t, lines, "500\t2\t500", 15*time.Second)
	t.Logf("reader saw the second batch %v after the writer's insert returned", time.Since(start).Round(10*time.Millisecond))

	// Parquet in the bucket matches, batch for batch.
	pq := query(t, fmt.Sprintf("SELECT count(), uniqExact(batch_id) FROM s3('%s/parquet/%s/%s/*.parquet', '%s', '%s', 'Parquet')", root, ns, gen, key, secret))
	if pq != "500\t2" {
		t.Fatalf("parquet: %s", pq)
	}

	if err := e.shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	raw := query(t, fmt.Sprintf("SELECT json FROM s3('%s/%s/manifests/%s/_sealed.json', '%s', '%s', 'JSONAsString') FORMAT TSVRaw", root, ns, gen, key, secret))
	var seal sealManifest
	if err := json.Unmarshal([]byte(raw), &seal); err != nil {
		t.Fatal(err, raw)
	}
	if seal.Batches != 2 || seal.Rows != 500 || len(seal.Tables) != 2 || !strings.HasSuffix(seal.ParquetPrefix, gen+"/") {
		t.Fatalf("seal: %+v", seal)
	}
}

// TestGenerationsRotateSealAndDetach: with 1 s generations, a push in a new
// second starts new tables; the old generation is sealed (views and staging
// dropped, _sealed.json written) and, after local_retention, detached. Its
// objects stay: a reader attaches it afterwards and counts every row.
func TestGenerationsRotateSealAndDetach(t *testing.T) {
	root, key, secret := s3Env(t)
	cfg := publishConfig(t, "os_rotate", "os-rotate")
	cfg.ObjectStorage.Endpoint, cfg.ObjectStorage.AccessKeyID, cfg.ObjectStorage.SecretAccessKey = root, key, configSecret(secret)
	cfg.ObjectStorage.Generation = time.Second
	cfg.ObjectStorage.LocalRetention = 300 * time.Millisecond
	e := startExporter(t, cfg)
	ctx := context.Background()
	if err := e.pushLogs(ctx, testgen.Logs(70)); err != nil {
		t.Fatal(err)
	}
	first := e.pub.signals[signalLogs].cur.id
	// Into the next second, then push again: that rotates.
	time.Sleep(time.Until(time.Now().Truncate(time.Second).Add(time.Second + 50*time.Millisecond)))
	if err := e.pushLogs(ctx, testgen.Logs(30)); err != nil {
		t.Fatal(err)
	}
	second := e.pub.signals[signalLogs].cur.id
	if first == second {
		t.Fatalf("no rotation: %s", first)
	}
	old := fmt.Sprintf("%s_%s", cfg.LogsTableName, first)
	deadline := time.Now().Add(20 * time.Second)
	for query(t, fmt.Sprintf("SELECT count() FROM system.tables WHERE database = 'os_rotate' AND name LIKE '%s%%'", old)) != "0" {
		if time.Now().After(deadline) {
			t.Fatalf("generation %s still attached: %s", first,
				query(t, "SELECT groupArray(name) FROM system.tables WHERE database = 'os_rotate'"))
		}
		time.Sleep(100 * time.Millisecond)
	}
	// The current generation is still attached and still has its staging.
	if got := query(t, fmt.Sprintf("SELECT count() FROM system.tables WHERE database = 'os_rotate' AND name LIKE '%s_%s%%'", cfg.LogsTableName, second)); got != "3" {
		t.Fatalf("current generation objects: %s", got)
	}

	ns := e.pub.namespace(signalLogs)
	raw := query(t, fmt.Sprintf("SELECT json FROM s3('%s/%s/manifests/%s/_sealed.json', '%s', '%s', 'JSONAsString') FORMAT TSVRaw", root, ns, first, key, secret))
	var seal sealManifest
	if err := json.Unmarshal([]byte(raw), &seal); err != nil {
		t.Fatal(err, raw)
	}
	if seal.Rows != 70 || seal.Batches != 1 {
		t.Fatalf("seal of %s: %+v", first, seal)
	}

	// Detached is not deleted: another process attaches the sealed
	// generation from its manifest and reads all of it.
	ddl := ReaderDDL(cfg, "r", signalLogs, first, seal.Tables, key, secret, 1)
	lines := watch(t, append(ddl, fmt.Sprintf("SELECT count(), min(row_ordinal), max(row_ordinal) FROM r.%s", old))...)
	waitFor(t, lines, "70\t0\t69", 60*time.Second)

	if err := e.shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func configSecret(s string) configopaque.String { return configopaque.String(s) }

func TestPublishConfigValidate(t *testing.T) {
	c := createDefaultConfig().(*Config)
	c.StoreTables = false
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "store_tables") {
		t.Errorf("store_tables false without parquet: %v", err)
	}
	c = createDefaultConfig().(*Config)
	c.ObjectStorage.Endpoint = "s3://nope"
	c.Parquet.URL = "relative/dir"
	c.InsertFormat = FormatJSON
	c.Producer.ID = "has/slash"
	c.ObjectStorage.LocalRetention = 80 * time.Hour
	err := c.Validate()
	for _, want := range []string{"object_storage.endpoint", "parquet.url", "rowbinary", "producer.id", "local_retention"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("want an error about %s, got %v", want, err)
		}
	}
}

// TestConcurrentPushesAcrossRotations: pushes from several goroutines while
// 1 s generations rotate under them. Every batch lands in exactly one
// generation: the manifests, the seals and the Parquet all add up.
func TestConcurrentPushesAcrossRotations(t *testing.T) {
	root, key, secret := s3Env(t)
	cfg := publishConfig(t, "os_churn", "os-churn")
	cfg.ObjectStorage.Endpoint, cfg.ObjectStorage.AccessKeyID, cfg.ObjectStorage.SecretAccessKey = root, key, configSecret(secret)
	cfg.ObjectStorage.Generation = time.Second
	cfg.ObjectStorage.LocalRetention = time.Hour
	cfg.ObjectStorage.SealOptimize = false
	cfg.Parquet.URL, cfg.Parquet.AccessKeyID, cfg.Parquet.SecretAccessKey = root+"/parquet", key, configSecret(secret)
	cfg.Connections = 3
	e := startExporter(t, cfg)
	ctx := context.Background()
	ld := testgen.Logs(50)
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				if err := e.pushLogs(ctx, ld); err != nil {
					errs <- err
					return
				}
				time.Sleep(60 * time.Millisecond)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	ns := e.pub.namespace(signalLogs)
	if err := e.shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	ms := s3Manifests(t, root, ns, key, secret)
	perGen := map[string]int{}
	total := 0
	for i, m := range ms {
		if m.BatchID != uint64(i+1) {
			t.Fatalf("batch ids not 1..40 without gaps: %d at %d", m.BatchID, i)
		}
		perGen[m.Generation] += m.Rows
		total += m.Rows
	}
	if len(ms) != 40 || total != 2000 || len(perGen) < 2 {
		t.Fatalf("%d manifests, %d rows, generations %v", len(ms), total, perGen)
	}
	raw := query(t, fmt.Sprintf("SELECT json FROM s3('%s/%s/manifests/*/_sealed.json', '%s', '%s', 'JSONAsString') FORMAT TSVRaw", root, ns, key, secret))
	seals := 0
	for _, l := range strings.Split(raw, "\n") {
		var s sealManifest
		if err := json.Unmarshal([]byte(l), &s); err != nil {
			t.Fatal(err, l)
		}
		if int(s.Rows) != perGen[s.Generation] {
			t.Errorf("seal of %s says %d rows, its manifests %d", s.Generation, s.Rows, perGen[s.Generation])
		}
		seals++
	}
	if seals != len(perGen) {
		t.Fatalf("%d seals for %d generations", seals, len(perGen))
	}
	pq := query(t, fmt.Sprintf("SELECT count(), uniqExact(batch_id) FROM s3('%s/parquet/%s/*/*.parquet', '%s', '%s', 'Parquet')", root, ns, key, secret))
	if pq != "2000\t40" {
		t.Fatalf("parquet: %s", pq)
	}
	t.Logf("40 batches over %d generations", len(perGen))
}

// BenchmarkPublish compares one 10k-span batch per iteration written to
// local tables, to s3_plain_rewritable tables, as Parquet (local directory
// and S3), and both at once. S3 variants need CHDB_TEST_S3.
func BenchmarkPublish(b *testing.B) {
	td := testgen.Traces(10000)
	root, key, secret := os.Getenv("CHDB_TEST_S3"), os.Getenv("CHDB_TEST_S3_KEY"), os.Getenv("CHDB_TEST_S3_SECRET")
	variants := []struct {
		name  string
		s3    bool
		setup func(c *Config, dir string)
	}{
		{"local-tables", false, func(c *Config, dir string) {}},
		{"local-tables+parquet-local", false, func(c *Config, dir string) { c.Parquet.URL = "file://" + dir }},
		{"parquet-local-only", false, func(c *Config, dir string) { c.StoreTables = false; c.Parquet.URL = "file://" + dir }},
		{"s3-tables", true, func(c *Config, dir string) {}},
		{"s3-tables-wide-parts", true, func(c *Config, dir string) { c.ObjectStorage.CompactParts = false }},
		{"parquet-s3-only", true, func(c *Config, dir string) {
			c.StoreTables = false
			c.ObjectStorage.Endpoint = ""
			c.Parquet.URL = root + "/parquet"
		}},
		{"s3-tables+parquet-s3", true, func(c *Config, dir string) { c.Parquet.URL = root + "/parquet" }},
	}
	for i, v := range variants {
		b.Run(v.name, func(b *testing.B) {
			if v.s3 && root == "" {
				b.Skip("CHDB_TEST_S3 not set")
			}
			if !insertSupported {
				b.Skip("publishing needs the chdb-go fork")
			}
			cfg := testConfig(fmt.Sprintf("pub_bench%d", i), FormatRowBinary)
			cfg.Producer.ID, cfg.Producer.Region = fmt.Sprintf("bench-%d", i), "bench"
			if v.s3 {
				cfg.ObjectStorage.Endpoint, cfg.ObjectStorage.AccessKeyID, cfg.ObjectStorage.SecretAccessKey = root, key, configopaque.String(secret)
				cfg.ObjectStorage.SealOptimize = false
				cfg.Parquet.AccessKeyID, cfg.Parquet.SecretAccessKey = key, configopaque.String(secret)
			}
			v.setup(cfg, b.TempDir())
			e := startExporter(b, cfg)
			ctx := context.Background()
			if err := e.pushTraces(ctx, td); err != nil {
				b.Fatal(err)
			}
			s3Writes := func() float64 {
				v, _ := strconv.ParseFloat(query(b, "SELECT sum(value) FROM system.events WHERE event = 'S3WriteRequestsCount'"), 64)
				return v
			}
			w0 := s3Writes()
			b.ResetTimer()
			start := time.Now()
			for i := 0; i < b.N; i++ {
				if err := e.pushTraces(ctx, td); err != nil {
					b.Fatal(err)
				}
			}
			el := time.Since(start)
			b.StopTimer()
			// Writes include the merges the inserts trigger, up to the last
			// batch; the seal at shutdown is not counted.
			b.ReportMetric(float64(b.N*10000)/el.Seconds(), "rows/s")
			b.ReportMetric(float64(el.Milliseconds())/float64(b.N), "ms/batch")
			b.ReportMetric((s3Writes()-w0)/float64(b.N), "s3writes/batch")
			b.ReportMetric(0, "ns/op")
			e.shutdown(ctx)
		})
	}
}

// chHTTP runs one statement on the ClickHouse server at CHDB_TEST_CLICKHOUSE
// (its HTTP interface) and returns the trimmed TSV result.
func chHTTP(t *testing.T, sql string) string {
	t.Helper()
	resp, err := http.Post(os.Getenv("CHDB_TEST_CLICKHOUSE")+"/", "text/plain", strings.NewReader(sql))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("clickhouse: %s\n%.300s", strings.TrimSpace(string(b)), sql)
	}
	return strings.TrimSpace(string(b))
}

// TestClickHouseServerReaderFollowsWriter: the central side as it would
// really be, a ClickHouse server, attaches the chDB writer's tables with the
// same ReaderDDL and follows them while the writer keeps inserting and
// merging. Needs CHDB_TEST_S3 and CHDB_TEST_CLICKHOUSE (e.g.
// http://127.0.0.1:8123), the server able to reach the same S3 endpoint.
func TestClickHouseServerReaderFollowsWriter(t *testing.T) {
	root, key, secret := s3Env(t)
	if os.Getenv("CHDB_TEST_CLICKHOUSE") == "" {
		t.Skip("CHDB_TEST_CLICKHOUSE not set")
	}
	t.Logf("server %s", chHTTP(t, "SELECT version()"))
	cfg := publishConfig(t, "os_server", "os-server")
	cfg.ObjectStorage.Endpoint, cfg.ObjectStorage.AccessKeyID, cfg.ObjectStorage.SecretAccessKey = root, key, configSecret(secret)
	e := startExporter(t, cfg)
	ctx := context.Background()
	if err := e.pushTraces(ctx, testgen.Traces(300)); err != nil {
		t.Fatal(err)
	}
	ns := e.pub.namespace(signalTraces)
	ms := s3Manifests(t, root, ns, key, secret)
	gen := ms[0].Generation
	writerTable := ms[0].Tables[0].Table // os_server.{table}_{gen}_e{epoch}
	db := "srv_" + strings.NewReplacer("-", "_").Replace(e.pub.epoch)
	for _, stmt := range ReaderDDL(cfg, db, signalTraces, gen, ms[0].Tables, key, secret, 1) {
		chHTTP(t, stmt)
	}
	table := db + "." + cfg.TracesTableName + "_" + gen
	count := fmt.Sprintf("SELECT count(), uniqExact(batch_id), (SELECT count() FROM %s_trace_id_ts) FROM %s", table, table)
	poll := func(want string, within time.Duration) time.Duration {
		t.Helper()
		start := time.Now()
		for last := ""; ; time.Sleep(50 * time.Millisecond) {
			if last = chHTTP(t, count); last == want {
				return time.Since(start)
			}
			if time.Since(start) > within {
				t.Fatalf("server did not reach %q in %v (last %q)", want, within, last)
			}
		}
	}
	poll("300\t1\t300", 10*time.Second)

	// Follows new batches.
	if err := e.pushTraces(ctx, testgen.Traces(200)); err != nil {
		t.Fatal(err)
	}
	t.Logf("server saw the second batch %v after the writer's insert returned", poll("500\t2\t500", 15*time.Second).Round(10*time.Millisecond))

	// Consistent through merges: 20 small batches make the writer merge
	// while the server polls. Every answer must be whole batches: never a
	// partial one, never a merged part counted beside the parts it replaced.
	small := testgen.Traces(100)
	done := make(chan error, 1)
	go func() {
		var err error
		for i := 0; i < 20 && err == nil; i++ {
			err = e.pushTraces(ctx, small)
			time.Sleep(100 * time.Millisecond)
		}
		done <- err
	}()
	polls, bad := 0, []string{}
	check := fmt.Sprintf("SELECT count(), uniqExact(batch_id), countIf(batch_id > 2) FROM %s", table)
	for pushing := true; pushing; {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			pushing = false
		default:
		}
		f := strings.Fields(chHTTP(t, check))
		rows, _ := strconv.Atoi(f[0])
		batches, _ := strconv.Atoi(f[1])
		smallRows, _ := strconv.Atoi(f[2])
		if rows != 500+smallRows || smallRows != 100*(batches-2) {
			bad = append(bad, strings.Join(f, " "))
		}
		polls++
		time.Sleep(30 * time.Millisecond)
	}
	if len(bad) > 0 {
		t.Fatalf("%d of %d polls saw inconsistent rows: %v", len(bad), polls, bad)
	}
	poll("2500\t22\t2500", 15*time.Second)
	parts := chHTTP(t, fmt.Sprintf("SELECT count() FROM system.parts WHERE active AND database = '%s' AND table = '%s'", db, cfg.TracesTableName+"_"+gen))
	merges := query(t, fmt.Sprintf("SELECT count() FROM system.parts WHERE database = 'os_server' AND table = '%s' AND level > 0", strings.TrimPrefix(writerTable, "os_server.")))
	t.Logf("%d polls during 20 pushes, all consistent; server sees %s active parts, writer has %s merged parts", polls, parts, merges)

	// Dropping the reader's table must not delete the writer's objects.
	chHTTP(t, "DROP TABLE "+table+"_trace_id_ts SYNC")
	chHTTP(t, "DROP TABLE "+table+" SYNC")
	if got := query(t, "SELECT count() FROM "+writerTable); got != "2500" {
		t.Fatalf("writer after the reader dropped its table: %s", got)
	}
	for _, stmt := range ReaderDDL(cfg, db, signalTraces, gen, ms[0].Tables, key, secret, 1) {
		chHTTP(t, stmt)
	}
	poll("2500\t22\t2500", 15*time.Second)
	chHTTP(t, "DROP DATABASE "+db+" SYNC")
	if err := e.shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestOldPartsLifetimeProtectsServerQueries: a slow query on the ClickHouse
// server reads parts that the writer then merges away. With chDB's default
// old_parts_lifetime of 0 the writer's cleanup deletes the replaced parts'
// objects, and the query fails reading them ("File .../data.bin does not
// exist"). With the exporter's default of 10 minutes it finishes with every
// row.
func TestOldPartsLifetimeProtectsServerQueries(t *testing.T) {
	root, key, secret := s3Env(t)
	if os.Getenv("CHDB_TEST_CLICKHOUSE") == "" {
		t.Skip("CHDB_TEST_CLICKHOUSE not set")
	}
	for _, life := range []time.Duration{0, 10 * time.Minute} {
		secs := int(life.Seconds())
		cfg := publishConfig(t, fmt.Sprintf("os_life%d", secs), fmt.Sprintf("os-life-%d", secs))
		cfg.ObjectStorage.Endpoint, cfg.ObjectStorage.AccessKeyID, cfg.ObjectStorage.SecretAccessKey = root, key, configSecret(secret)
		cfg.ObjectStorage.OldPartsLifetime = life
		e := startExporter(t, cfg)
		ctx := context.Background()
		for i := 0; i < 6; i++ {
			if err := e.pushLogs(ctx, testgen.Logs(2000)); err != nil {
				t.Fatal(err)
			}
		}
		ms := s3Manifests(t, root, e.pub.namespace(signalLogs), key, secret)
		gen := ms[0].Generation
		db := fmt.Sprintf("srv_life%d_%s", secs, strings.NewReplacer("-", "_").Replace(e.pub.epoch))
		for _, stmt := range ReaderDDL(cfg, db, signalLogs, gen, ms[0].Tables, key, secret, 1) {
			chHTTP(t, stmt)
		}
		table := db + "." + cfg.LogsTableName + "_" + gen
		// Let the server's refresh settle on the current parts, then scan them
		// slowly (100-row blocks, 0.3 ms a row, one thread: ~4 s).
		time.Sleep(1500 * time.Millisecond)
		slow := fmt.Sprintf("SELECT count(), sum(sleepEachRow(0.0003)) FROM %s SETTINGS max_threads = 1, max_block_size = 100, preferred_block_size_bytes = 1000", table)
		res := make(chan string, 1)
		go func() {
			resp, err := http.Post(os.Getenv("CHDB_TEST_CLICKHOUSE")+"/", "text/plain", strings.NewReader(slow))
			if err != nil {
				res <- err.Error()
				return
			}
			defer resp.Body.Close()
			b, _ := io.ReadAll(resp.Body)
			res <- fmt.Sprintf("%d %s", resp.StatusCode, strings.TrimSpace(string(b)))
		}()
		time.Sleep(700 * time.Millisecond)
		// Mid-scan: the writer merges everything into one part, and its
		// cleanup is woken now rather than on its next periodic run, so
		// the race a long query would lose happens inside this one.
		if err := exec(e.all[0], "OPTIMIZE TABLE "+ms[0].Tables[0].Table+" FINAL"); err != nil {
			t.Fatal(err)
		}
		if err := exec(e.all[0], "SYSTEM START CLEANUP"); err != nil {
			t.Fatal(err)
		}
		got := <-res
		t.Logf("old_parts_lifetime %v: slow server query during a merge -> %.160s", life, got)
		if life > 0 && got != "200 12000\t0" {
			t.Errorf("with old_parts_lifetime %v the query should survive the merge: %s", life, got)
		}
		if life == 0 && !strings.Contains(got, "does not exist") {
			t.Errorf("with old_parts_lifetime 0 the merge should have deleted objects under the query: %s", got)
		}
		if err := e.shutdown(ctx); err != nil {
			t.Fatal(err)
		}
		chHTTP(t, "DROP DATABASE "+db+" SYNC")
	}
}
