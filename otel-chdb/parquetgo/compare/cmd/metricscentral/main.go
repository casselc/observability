// metricscentral measures the central side of metrics publishing, per metric
// type: parquetgo publishes a contiguous stream of realistic batches
// (compare.MetricsBatch) to S3, then the ClickHouse server ingests every
// object with the importer's INSERT…SELECT FROM s3() (single-block
// settings, a dedup token) into the contrib exporter's table plus the
// envelope. It reports, per type:
//
//   - edge: CPU (getrusage) and Go allocations per point to encode and PUT;
//
//   - S3: object bytes per point;
//
//   - central: per-insert wall time (X-ClickHouse-Summary elapsed_ns) and
//     server CPU (system.events OSCPUVirtualTimeMicroseconds delta, valid
//     only if no other query ran meanwhile, which it checks), per point;
//
//   - stored: compressed and on-disk bytes per point from system.parts
//     after OPTIMIZE FINAL, for the exporter's table alone and with the
//     envelope, plus the biggest columns.
//
//     CHDB_TEST_S3=http://127.0.0.1:18333/otel CHDB_TEST_S3_KEY=otel CHDB_TEST_S3_SECRET=otelsecret \
//     CHDB_TEST_CLICKHOUSE=http://127.0.0.1:18123 metricscentral -n 10000 -batches 100
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo/compare"
)

var chURL = os.Getenv("CHDB_TEST_CLICKHOUSE")

// ch runs one statement; it returns the body and the server's summary.
func ch(sql string) (string, map[string]string) {
	resp, err := http.Post(chURL+"/", "text/plain", strings.NewReader(sql))
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		log.Fatalf("clickhouse: %s\n%.300s", strings.TrimSpace(string(b)), sql)
	}
	var sum map[string]string
	_ = json.Unmarshal([]byte(resp.Header.Get("X-ClickHouse-Summary")), &sum)
	return strings.TrimSpace(string(b)), sum
}

func q(sql string) string { s, _ := ch(sql); return s }

func cpu() time.Duration {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// events reads global ProfileEvents counters.
func events(names ...string) map[string]float64 {
	out := map[string]float64{}
	rows := q("SELECT event, value FROM system.events WHERE event IN ('" + strings.Join(names, "','") + "') FORMAT TSV")
	for _, l := range strings.Split(rows, "\n") {
		f := strings.Split(l, "\t")
		if len(f) == 2 {
			v, _ := strconv.ParseFloat(f[1], 64)
			out[f[0]] = v
		}
	}
	return out
}

type colSize struct {
	Name  string
	Bytes float64 // compressed bytes per point
}

type result struct {
	Type, Load                    string
	Points, Batches               int
	EdgeCPUusPerPoint             float64
	EdgeAllocsPerBatch            float64
	EdgeBytesAllocPerPoint        float64
	S3BytesPerPoint               float64
	InsertWallusPerPoint          [3]float64 // min, median, max over the inserts
	InsertCPUusPerPoint           float64    // server CPU; 0 if other queries interfered
	OtherQueries                  float64
	StoredBytesPerPoint           float64 // exporter table, compressed data, after OPTIMIZE FINAL
	StoredOnDiskPerPoint          float64 // same, bytes_on_disk (incl. marks, primary index, skip indexes)
	StoredEnvBytesPerPoint        float64 // with the envelope columns
	StoredEnvOnDiskPerPoint       float64
	UncompressedBytesPerPoint     float64
	TopColumns                    []colSize
	PartsBeforeOptimize, PartsNow int
}

func main() {
	n := flag.Int("n", 10000, "points per batch")
	batches := flag.Int("batches", 100, "batches per type")
	types := flag.String("types", strings.Join(parquetgo.MetricSignals[:], ","), "metric types")
	keep := flag.Bool("keep", false, "keep the database and objects")
	flag.Parse()
	s3, ok := compare.S3FromEnv()
	if !ok || chURL == "" {
		log.Fatal("needs CHDB_TEST_S3(_KEY/_SECRET) and CHDB_TEST_CLICKHOUSE")
	}
	ctx := context.Background()
	run := fmt.Sprintf("c%d", time.Now().Unix())
	base := s3.Endpoint + "/metrics-go/central/" + run
	root := s3.Endpoint[:strings.Index(s3.Endpoint, "/otel")] + "/"
	db, refDB := "pqm_central_"+run, "pqm_centralref_"+run
	ref, err := compare.NewRefExporter(ctx, refDB)
	if err != nil {
		log.Fatal(err)
	}
	_ = ref.Shutdown(ctx)
	q("CREATE DATABASE " + db)
	defer func() {
		if !*keep {
			q("DROP DATABASE IF EXISTS " + db)
			q("DROP DATABASE IF EXISTS " + refDB)
		}
	}()

	for _, sig := range strings.Split(*types, ",") {
		var mt parquetgo.MetricType = -1
		for i, s := range parquetgo.MetricSignals {
			if s == sig {
				mt = parquetgo.MetricType(i)
			}
		}
		if mt < 0 {
			log.Fatalf("type %q", sig)
		}
		r := result{Type: sig, Points: *n, Batches: *batches}

		// 1. Edge: publish a contiguous stream.
		p, err := parquetgo.New(parquetgo.Config{URL: base, AccessKeyID: s3.Key, SecretAccessKey: s3.Secret,
			ProducerID: "central", Region: "cmp", SchemaVersion: 1, Epoch: run, Engine: "parquet-go"})
		if err != nil {
			log.Fatal(err)
		}
		var edgeCPU time.Duration
		var ms0, ms1 runtime.MemStats
		var allocs, bytes uint64
		for b := 0; b < *batches; b++ {
			md := compare.MetricsBatch(mt, *n, b)
			runtime.ReadMemStats(&ms0)
			c0 := cpu()
			if err := p.PushMetrics(ctx, md); err != nil {
				log.Fatal(err)
			}
			edgeCPU += cpu() - c0
			runtime.ReadMemStats(&ms1)
			allocs += ms1.Mallocs - ms0.Mallocs
			bytes += ms1.TotalAlloc - ms0.TotalAlloc
		}
		if err := p.Close(ctx); err != nil {
			log.Fatal(err)
		}
		pts := float64(*n * *batches)
		r.EdgeCPUusPerPoint = float64(edgeCPU.Microseconds()) / pts
		r.EdgeAllocsPerBatch = float64(allocs) / float64(*batches)
		r.EdgeBytesAllocPerPoint = float64(bytes) / pts
		glob := fmt.Sprintf("%s/cmp/%s/v1/central/%s/*/*.parquet", base, sig, run)
		objs := strings.Fields(q(fmt.Sprintf("SELECT _path FROM s3('%s', '%s', '%s', 'One') ORDER BY _path FORMAT TSV", glob, s3.Key, s3.Secret)))
		size := q(fmt.Sprintf("SELECT sum(_size) FROM (SELECT DISTINCT _path, _size FROM s3('%s', '%s', '%s', 'One'))", glob, s3.Key, s3.Secret))
		sz, _ := strconv.ParseFloat(size, 64)
		r.S3BytesPerPoint = sz / pts

		// 2. Central: the exporter's table, and the same with the envelope.
		ddl := q("SHOW CREATE TABLE " + refDB + ".otel_" + sig + " FORMAT TSVRaw")
		plain, _ := compare.CentralDDL(ddl, db+".otel_"+sig, false)
		withEnv, err := compare.CentralDDL(ddl, db+".otel_"+sig+"_env", true)
		if err != nil {
			log.Fatal(err)
		}
		q(plain)
		q(withEnv)
		// No merges while measuring: their CPU would land in system.events too.
		q(fmt.Sprintf("SYSTEM STOP MERGES %s.otel_%s_env", db, sig))
		contrib := compare.ContribCols(mt)
		all := contrib + ", " + strings.Join(compare.EnvelopeCols, ", ")
		r.Load = strings.Fields(q("SELECT toString(value) FROM system.asynchronous_metrics WHERE metric = 'LoadAverage1'") + " ?")[0]
		cpuEvents := []string{"OSCPUVirtualTimeMicroseconds", "Query"}
		e0 := events(cpuEvents...)
		var walls []float64
		for _, o := range objs {
			src := fmt.Sprintf("s3('%s', '%s', '%s', 'Parquet', '%s')", root+o, s3.Key, s3.Secret, compare.MetricStructures[mt])
			_, sum := ch(fmt.Sprintf("INSERT INTO %s.otel_%s_env (%s) SETTINGS %s, insert_deduplication_token = '%s' SELECT %s FROM %s",
				db, sig, all, compare.OneBlock, o, all, src))
			ns, _ := strconv.ParseFloat(sum["elapsed_ns"], 64)
			walls = append(walls, ns/1e3/float64(*n))
		}
		e1 := events(cpuEvents...)
		// Queries counted: our inserts, plus the two events reads.
		r.OtherQueries = e1["Query"] - e0["Query"] - float64(len(objs)) - 1
		if r.OtherQueries <= 0 {
			r.InsertCPUusPerPoint = (e1["OSCPUVirtualTimeMicroseconds"] - e0["OSCPUVirtualTimeMicroseconds"]) / pts
		}
		sort.Float64s(walls)
		r.InsertWallusPerPoint = [3]float64{walls[0], walls[len(walls)/2], walls[len(walls)-1]}
		// The exporter's table alone, from the envelope table (no second S3 read).
		q(fmt.Sprintf("INSERT INTO %s.otel_%s (%s) SETTINGS max_insert_threads = 1 SELECT %s FROM %s.otel_%s_env", db, sig, contrib, contrib, db, sig))

		// 3. Stored bytes after merges.
		q(fmt.Sprintf("SYSTEM START MERGES %s.otel_%s_env", db, sig))
		r.PartsBeforeOptimize, _ = strconv.Atoi(q(fmt.Sprintf("SELECT count() FROM system.parts WHERE database = '%s' AND table = 'otel_%s_env' AND active", db, sig)))
		for _, tbl := range []string{"otel_" + sig, "otel_" + sig + "_env"} {
			q(fmt.Sprintf("OPTIMIZE TABLE %s.%s FINAL", db, tbl))
			f := strings.Fields(q(fmt.Sprintf("SELECT sum(rows), sum(data_compressed_bytes), sum(bytes_on_disk), sum(data_uncompressed_bytes), count() FROM system.parts WHERE database = '%s' AND table = '%s' AND active", db, tbl)))
			rows, _ := strconv.ParseFloat(f[0], 64)
			comp, _ := strconv.ParseFloat(f[1], 64)
			disk, _ := strconv.ParseFloat(f[2], 64)
			unc, _ := strconv.ParseFloat(f[3], 64)
			if rows != pts {
				log.Fatalf("%s: %v rows, want %v", tbl, rows, pts)
			}
			if strings.HasSuffix(tbl, "_env") {
				r.StoredEnvBytesPerPoint, r.StoredEnvOnDiskPerPoint = comp/pts, disk/pts
				r.PartsNow, _ = strconv.Atoi(f[4])
			} else {
				r.StoredBytesPerPoint, r.StoredOnDiskPerPoint, r.UncompressedBytesPerPoint = comp/pts, disk/pts, unc/pts
			}
		}
		for _, l := range strings.Split(q(fmt.Sprintf("SELECT name, data_compressed_bytes FROM system.columns WHERE database = '%s' AND table = 'otel_%s_env' ORDER BY data_compressed_bytes DESC LIMIT 8 FORMAT TSV", db, sig)), "\n") {
			f := strings.Split(l, "\t")
			if len(f) == 2 {
				v, _ := strconv.ParseFloat(f[1], 64)
				r.TopColumns = append(r.TopColumns, colSize{f[0], v / pts})
			}
		}
		b, _ := json.Marshal(r)
		fmt.Println(string(b))
		if !*keep {
			q(fmt.Sprintf("DROP TABLE %s.otel_%s", db, sig))
			q(fmt.Sprintf("DROP TABLE %s.otel_%s_env", db, sig))
		}
	}
	if !*keep {
		fmt.Fprintf(os.Stderr, "objects left under %s (delete with the S3 cleanup)\n", base)
	}
}
