// queries runs sql/queries.sql against the loaded layouts over the native
// protocol and reports, per (query, layout, window): median wall time and
// median server CPU (ProfileEvents OSCPUVirtualTimeMicroseconds) over -n
// runs after one warm-up, rows and bytes read, and result rows. H* blocks
// written for A are also run against the compatibility views (BV).
//
//	queries -db ml_main -vdb ml_view -tsdb ml_ts -layouts A,B,BV,C
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/metrics-layout/central"
)

type block struct{ q, layout, sql string }

type result struct {
	Query, Layout, Window string
	WallMs, CPUms         float64 // medians
	WallMsAll             []float64
	ReadRows, ReadBytes   uint64
	ResultRows            uint64
	Err                   string `json:",omitempty"`
}

func parse(path string) []block {
	b, err := os.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}
	var out []block
	var cur *block
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, "-- @ ") {
			f := strings.Fields(l[5:])
			out = append(out, block{q: f[0], layout: f[1]})
			cur = &out[len(out)-1]
			continue
		}
		if cur == nil || strings.HasPrefix(strings.TrimSpace(l), "--") {
			continue
		}
		cur.sql += l + "\n"
	}
	for i := range out {
		out[i].sql = strings.TrimSpace(out[i].sql)
		if strings.HasPrefix(out[i].q, "H") && out[i].layout == "A" {
			out = append(out, block{out[i].q, "BV", strings.ReplaceAll(out[i].sql, "{db}.", "{vdb}.")})
		}
	}
	return out
}

func median(v []float64) float64 {
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	return s[len(s)/2]
}

func main() {
	db := flag.String("db", "ml_main", "A/B database")
	vdb := flag.String("vdb", "ml_view", "compatibility views database")
	tsdb := flag.String("tsdb", "ml_ts", "TimeSeries database")
	layouts := flag.String("layouts", "A,B,BV,C", "layouts")
	only := flag.String("only", "", "comma-separated query ids (default all)")
	n := flag.Int("n", 5, "measured runs")
	start := flag.String("start", "2026-09-24 06:00:00", "data start (UTC)")
	flag.Parse()
	ctx := context.Background()
	conn, err := central.Dial(ctx)
	if err != nil {
		log.Fatal(err)
	}
	t0, _ := time.Parse("2006-01-02 15:04:05", *start)
	windows := []struct {
		name     string
		from, to time.Time
		step     int
	}{
		{"1h", t0.Add(5 * time.Hour), t0.Add(6 * time.Hour), 60},
		{"6h", t0, t0.Add(6*time.Hour + time.Minute), 300},
	}
	enc := json.NewEncoder(os.Stdout)
	for _, b := range parse(filepath.Join(central.Dir(), "sql", "queries.sql")) {
		if !strings.Contains(","+*layouts+",", ","+b.layout+",") || *only != "" && !strings.Contains(","+*only+",", ","+b.q+",") {
			continue
		}
		for _, w := range windows {
			dt := func(t time.Time) string { return "toDateTime('" + t.Format("2006-01-02 15:04:05") + "', 'UTC')" }
			sql := strings.NewReplacer("{db}", *db, "{vdb}", *vdb, "{tsdb}", *tsdb, "{from}", dt(w.from), "{to}", dt(w.to),
				"{fromMs}", fmt.Sprint(w.from.UnixMilli()), "{toMs}", fmt.Sprint(w.to.UnixMilli()),
				"{step}", fmt.Sprint(w.step), "{window}", fmt.Sprint(int(w.to.Sub(w.from).Seconds()))).Replace(b.sql)
			if b.layout == "C" {
				// prometheusQuery* return Array(Tuple(String, String)) tags, which
				// ch-go cannot decode generically: materialize them as a string.
				sql = "SELECT length(toString(tuple(*))) FROM (" + sql + ")"
			}
			r := result{Query: b.q, Layout: b.layout, Window: w.name}
			var cpus []float64
			settings := map[string]string{"use_query_cache": "0", "enable_time_series_aggregate_functions": "1", "allow_experimental_time_series_table": "1"}
			for i := 0; i <= *n; i++ {
				c, err := conn.Exec(ctx, sql, "", settings)
				if err != nil {
					r.Err = strings.SplitN(err.Error(), "\n", 2)[0]
					conn.Close()
					conn, _ = central.Dial(ctx)
					break
				}
				if i == 0 {
					continue // warm-up
				}
				r.WallMsAll = append(r.WallMsAll, float64(c.Wall.Microseconds())/1e3)
				cpus = append(cpus, c.CPUus/1e3)
				r.ReadRows, r.ReadBytes = c.ReadRows, c.ReadBytes
			}
			if len(cpus) > 0 {
				r.WallMs, r.CPUms = median(r.WallMsAll), median(cpus)
			}
			if r.Err == "" {
				// Result size, once.
				out, _, err := central.Q("SELECT count() FROM ("+sql+")", "enable_time_series_aggregate_functions", "1", "allow_experimental_time_series_table", "1")
				if err == nil {
					fmt.Sscan(out, &r.ResultRows)
				}
			}
			_ = enc.Encode(r)
		}
	}
}
