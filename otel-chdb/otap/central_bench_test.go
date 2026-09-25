package otap

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/chdbexporter/testgen"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo"
)

// TestCentralIngestCost measures the central INSERT ... SELECT into the
// ClickStack tables for each layout, from the same 10k-span / 10k-record
// batches the edge benchmarks use, and checks retry idempotency with
// insert_deduplication_token. Needs OTAP_CENTRAL_BENCH=1 plus the S3 and
// ClickHouse variables of TestCentralMatchesReference.
// chQuerySummary is chQuery that also returns X-ClickHouse-Summary.
func chQuerySummary(chURL, sql string, settings map[string]string) (string, map[string]string, error) {
	q := url.Values{}
	for k, v := range settings {
		q.Set(k, v)
	}
	resp, err := http.Post(chURL+"/?"+q.Encode(), "text/plain", strings.NewReader(sql))
	if err != nil {
		return "", nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	sum := map[string]string{}
	_ = json.Unmarshal([]byte(resp.Header.Get("X-ClickHouse-Summary")), &sum)
	if resp.StatusCode != 200 {
		return "", sum, fmt.Errorf("clickhouse: %s", strings.TrimSpace(string(b)))
	}
	return strings.TrimSpace(string(b)), sum, nil
}

func TestCentralIngestCost(t *testing.T) {
	if os.Getenv("OTAP_CENTRAL_BENCH") == "" {
		t.Skip("set OTAP_CENTRAL_BENCH=1")
	}
	e := testEnv(t)
	run := fmt.Sprintf("cb%d", time.Now().Unix())
	base := e.s3 + "/" + run
	const batches, rows = 10, 10000
	reps := 3
	if s := os.Getenv("OTAP_CENTRAL_REPS"); s != "" {
		reps, _ = strconv.Atoi(s)
	}
	ctx := context.Background()
	variants := []string{Ref, Star, FlatParquet, FlatArrow, Raw}
	manifests := map[string][]Manifest{}
	td, ld := testgen.Traces(rows), testgen.Logs(rows)
	tb, err := EncodeTraces(td)
	if err != nil {
		t.Fatal(err)
	}
	lb, err := EncodeLogs(ld)
	if err != nil {
		t.Fatal(err)
	}
	present := map[string]map[string]bool{}
	for _, v := range variants {
		sink, _ := NewSink(base, e.key, e.secret, nil)
		p := &Publisher{Sink: sink, Producer: "cb", Epoch: run, SchemaVersion: 1, Parquet: parquetgo.DefaultOptions()}
		for i := 0; i < batches; i++ {
			var ms []Manifest
			if v == Ref {
				m1, err := p.PublishRef(ctx, &td, nil)
				if err != nil {
					t.Fatal(err)
				}
				m2, err := p.PublishRef(ctx, nil, &ld)
				if err != nil {
					t.Fatal(err)
				}
				ms = append(ms, m1, m2)
			} else {
				m1, err := p.PublishBAR(ctx, v, tb)
				if err != nil {
					t.Fatal(err)
				}
				m2, err := p.PublishBAR(ctx, v, lb)
				if err != nil {
					t.Fatal(err)
				}
				ms = append(ms, m1, m2)
			}
			manifests[v] = append(manifests[v], ms...)
			for _, m := range ms {
				k := v + "/" + m.Signal
				if present[k] == nil {
					present[k] = map[string]bool{}
				}
				for _, o := range m.Objects {
					present[k][o.Table] = true
				}
			}
		}
	}
	db := "otapcb_" + run
	e.q(t, "CREATE DATABASE IF NOT EXISTS "+db)
	if os.Getenv("OTAP_TEST_KEEP") == "" {
		defer e.q(t, "DROP DATABASE IF EXISTS "+db)
	}
	cred := fmt.Sprintf("'%s', '%s'", e.key, e.secret)
	// batchList is the {a,b,...} object list for batches 1..n, as a consumer
	// that read n manifests would name them (no LIST).
	batchList := func(n int) string {
		var ids []string
		for i := 1; i <= n; i++ {
			ids = append(ids, fmt.Sprintf("%020d", i))
		}
		if n == 1 {
			return ids[0]
		}
		return "{" + strings.Join(ids, ",") + "}"
	}
	selectFor := func(v, signal string, n int) string {
		cols, structure := TraceCols, FlatTraceStructure
		if signal == "logs" {
			cols, structure = LogCols, FlatLogStructure
		}
		switch v {
		case Raw:
			return fmt.Sprintf("SELECT %s FROM (%s)", cols, rawSelect(t, e, base, cred, signal, batchList(n), manifests[v]))
		case Star:
			src := func(table string) string {
				if !present[v+"/"+signal][table] {
					return fmt.Sprintf("null('%s')", StarStructure(table))
				}
				return fmt.Sprintf("s3('%s/%s/%s/%s/%s.parquet', %s, 'Parquet', '%s')", base, signal, v, batchList(n), table, cred, StarStructure(table))
			}
			if signal == "traces" {
				return fmt.Sprintf("SELECT %s FROM (%s)", cols, StarTracesSelect(src))
			}
			return fmt.Sprintf("SELECT %s FROM (%s)", cols, StarLogsSelect(src))
		case FlatArrow:
			return fmt.Sprintf("SELECT %s FROM s3('%s/%s/%s/%s/otel_%s.arrow', %s, 'Arrow', '%s')", cols, base, signal, v, batchList(n), signal, cred, structure)
		}
		return fmt.Sprintf("SELECT %s FROM s3('%s/%s/%s/%s/otel_%s.parquet', %s, 'Parquet', '%s')", cols, base, signal, v, batchList(n), signal, cred, structure)
	}
	central := map[string]string{"traces": CentralTraces, "logs": CentralLogs}
	settings := map[string]string{"use_query_condition_cache": "0"}
	single := map[string]string{"use_query_condition_cache": "0", "max_threads": "1", "max_insert_threads": "1",
		"min_insert_block_size_rows": "0", "min_insert_block_size_bytes": "0", "max_insert_block_size": "10000000", "max_block_size": "10000000"}

	type stat struct{ wallMS, cpuMS, memMB, gets, heads, readMB float64 }
	// Cost per statement: deltas of the server-wide system.events counters
	// around it (query_log is off on this server), as ../bench/central does,
	// plus X-ClickHouse-Summary for memory. The server is shared, so other
	// load can leak in; medians over reps absorb most of it.
	events := func() map[string]float64 {
		out := e.q(t, "SELECT event, value FROM system.events WHERE event IN ('UserTimeMicroseconds','SystemTimeMicroseconds','S3GetObject','S3HeadObject','ReadBufferFromS3Bytes') FORMAT TSV")
		m := map[string]float64{}
		for _, l := range strings.Split(out, "\n") {
			f := strings.Split(l, "\t")
			if len(f) == 2 {
				m[f[0]], _ = strconv.ParseFloat(f[1], 64)
			}
		}
		return m
	}
	insert := func(tbl, sel string, st map[string]string, token string) (stat, error) {
		s := map[string]string{}
		for k, v := range st {
			s[k] = v
		}
		if token != "" {
			s["insert_deduplication_token"] = token
		}
		a := events()
		t0 := time.Now()
		_, summary, err := chQuerySummary(e.ch, fmt.Sprintf("INSERT INTO %s %s", tbl, sel), s)
		wall := time.Since(t0)
		if err != nil {
			return stat{}, err
		}
		b := events()
		d := func(k string) float64 { return b[k] - a[k] }
		mem, _ := strconv.ParseFloat(summary["memory_usage"], 64)
		return stat{float64(wall.Microseconds()) / 1000, (d("UserTimeMicroseconds") + d("SystemTimeMicroseconds")) / 1000,
			mem / 1e6, d("S3GetObject"), d("S3HeadObject"), d("ReadBufferFromS3Bytes") / 1e6}, nil
	}
	median := func(xs []stat, pick func(stat) float64) string {
		v := make([]float64, len(xs))
		for i, s := range xs {
			v[i] = pick(s)
		}
		sort.Float64s(v)
		return fmt.Sprintf("%.0f [%.0f–%.0f]", v[len(v)/2], v[0], v[len(v)-1])
	}

	t.Logf("| signal | layout | batches | settings | wall ms | query CPU ms | peak mem MB | S3 GET | S3 HEAD | MB read |")
	for _, signal := range []string{"traces", "logs"} {
		for _, v := range variants {
			tbl := fmt.Sprintf("%s.%s_%s", db, signal, strings.ReplaceAll(v, "-", "_"))
			e.q(t, fmt.Sprintf("CREATE TABLE %s %s SETTINGS non_replicated_deduplication_window = 1000", tbl, central[signal]))
			for _, n := range []int{1, batches} {
				for _, cfg := range []struct {
					name string
					s    map[string]string
				}{{"default", settings}, {"single-thread", single}} {
					if v == Raw && cfg.name == "default" {
						continue // raw decodes ids from row order: single-threaded only
					}
					var xs []stat
					for r := 0; r < reps; r++ {
						e.q(t, "TRUNCATE TABLE "+tbl)
						st, err := insert(tbl, selectFor(v, signal, n), cfg.s, "")
						if err != nil {
							t.Fatalf("%s %s: %v", tbl, cfg.name, err)
						}
						xs = append(xs, st)
					}
					t.Logf("| %s | %s | %d | %s | %s | %s | %s | %s | %s | %s |", signal, v, n, cfg.name,
						median(xs, func(s stat) float64 { return s.wallMS }), median(xs, func(s stat) float64 { return s.cpuMS }),
						median(xs, func(s stat) float64 { return s.memMB }), median(xs, func(s stat) float64 { return s.gets }),
						median(xs, func(s stat) float64 { return s.heads }), median(xs, func(s stat) float64 { return s.readMB }))
				}
			}
		}
	}

	// Determinism without a token: the same INSERT twice into two fresh
	// tables must give byte-identical rows (Map key order included).
	t.Logf("| signal | layout | settings | identical content on 2 runs |")
	for _, signal := range []string{"traces", "logs"} {
		for _, v := range variants {
			for _, cfg := range []struct {
				name string
				s    map[string]string
			}{{"default", settings}, {"single-thread", single}} {
				if v == Raw && cfg.name == "default" {
					continue
				}
				var sums []string
				for i := 0; i < 2; i++ {
					tbl := fmt.Sprintf("%s.det_%s_%s_%d", db, signal, strings.ReplaceAll(v, "-", "_"), i)
					e.q(t, fmt.Sprintf("CREATE TABLE %s %s", tbl, central[signal]))
					if _, err := insert(tbl, selectFor(v, signal, batches), cfg.s, ""); err != nil {
						t.Fatal(err)
					}
					cols := TraceCols
					if signal == "logs" {
						cols = LogCols
					}
					sums = append(sums, e.q(t, fmt.Sprintf("SELECT count(), sum(cityHash64(%s)) FROM %s", cols, tbl)))
					e.q(t, "DROP TABLE "+tbl)
				}
				t.Logf("| %s | %s | %s | %v |", signal, v, cfg.name, sums[0] == sums[1])
			}
		}
	}

	// Retry idempotency: the same INSERT twice with one token must leave one
	// copy. The dedup hash covers the inserted block, so it needs the same
	// rows in the same blocks on every attempt.
	t.Logf("| signal | layout | retry | rows after 2 attempts (want %d) |", batches*rows)
	for _, signal := range []string{"traces", "logs"} {
		for _, v := range variants {
			for _, c := range []struct {
				name   string
				s1, s2 map[string]string
			}{
				{"default settings both times", settings, settings},
				{"single-thread both times", single, single},
				{"max_threads 1 then default", single, settings},
			} {
				if v == Raw && c.s2["max_threads"] != "1" {
					continue
				}
				tbl := fmt.Sprintf("%s.dedup_%s_%s_%d", db, signal, strings.ReplaceAll(v, "-", "_"), time.Now().UnixNano())
				e.q(t, fmt.Sprintf("CREATE TABLE %s %s SETTINGS non_replicated_deduplication_window = 1000", tbl, central[signal]))
				tok := "tok-" + v + "-" + signal
				if _, err := insert(tbl, selectFor(v, signal, batches), c.s1, tok); err != nil {
					t.Fatal(err)
				}
				if _, err := insert(tbl, selectFor(v, signal, batches), c.s2, tok); err != nil {
					t.Fatal(err)
				}
				t.Logf("| %s | %s | %s | %s |", signal, v, c.name, e.q(t, "SELECT count() FROM "+tbl))
				e.q(t, "DROP TABLE "+tbl)
			}
		}
	}
}
