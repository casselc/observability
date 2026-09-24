// Package bench compares the chdb exporter with the contrib clickhouse
// exporter writing to a real ClickHouse server, through the same collector
// API (factory, exporterhelper, ConsumeTraces/ConsumeLogs) and the same data.
//
// Run it with run-compare.sh, which starts the server. Without
// CLICKHOUSE_ENDPOINT the clickhouse cases are skipped.
package bench

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouseexporter"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exportertest"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/chdbexporter"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/chdbexporter/testgen"
)

var chdbPath string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "otel-chdb-bench-")
	if err != nil {
		panic(err)
	}
	chdbPath = dir
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

type variant struct {
	name string
	make func(db string) (exporter.Factory, component.Config)
	ch   bool // needs the ClickHouse server
}

func chdbVariant(format string, buffer int) variant {
	name := "chdb-" + format
	if buffer > 0 {
		name += "-buffered"
	}
	return variant{name: name, make: func(db string) (exporter.Factory, component.Config) {
		f := chdbexporter.NewFactory()
		c := f.CreateDefaultConfig().(*chdbexporter.Config)
		c.Path, c.Database, c.InsertFormat, c.BufferSeconds = chdbPath, db, format, buffer
		c.QueueSettings = configoptional.None[exporterhelper.QueueBatchConfig]()
		return f, c
	}}
}

func clickhouseVariant(async bool) variant {
	name := "clickhouse-sync"
	if async {
		name = "clickhouse-async"
	}
	return variant{name: name, ch: true, make: func(db string) (exporter.Factory, component.Config) {
		f := clickhouseexporter.NewFactory()
		c := f.CreateDefaultConfig().(*clickhouseexporter.Config)
		c.Endpoint = os.Getenv("CLICKHOUSE_ENDPOINT")
		c.Database, c.AsyncInsert = db, async
		c.QueueSettings = configoptional.None[exporterhelper.QueueBatchConfig]()
		return f, c
	}}
}

var variants = []variant{
	chdbVariant(chdbexporter.FormatRowBinary, 0),
	chdbVariant(chdbexporter.FormatJSON, 0),
	chdbVariant(chdbexporter.FormatFile, 0),
	chdbVariant(chdbexporter.FormatRowBinary, 10),
	clickhouseVariant(false),
	clickhouseVariant(true),
}

// cpu returns user+system CPU consumed so far by this process and, when
// CLICKHOUSE_PID is set, by the server: an in-process store spends its CPU
// here, a server spends it there, and the machine pays for both.
func cpu() time.Duration {
	var ru syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	d := time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
	if pid := os.Getenv("CLICKHOUSE_PID"); pid != "" {
		if b, err := os.ReadFile("/proc/" + pid + "/stat"); err == nil {
			f := strings.Fields(string(b[strings.LastIndexByte(string(b), ')')+2:]))
			ut, _ := strconv.ParseInt(f[11], 10, 64) // utime, field 14
			st, _ := strconv.ParseInt(f[12], 10, 64) // stime, field 15
			d += time.Duration(ut+st) * (time.Second / 100)
		}
	}
	return d
}

var dbSeq int

func BenchmarkExporters(b *testing.B) {
	ctx := context.Background()
	for _, sig := range []string{"traces", "logs"} {
		for _, batch := range []int{100, 1000, 10000} {
			td, ld := testgen.Traces(batch), testgen.Logs(batch)
			for _, v := range variants {
				b.Run(fmt.Sprintf("%s/batch=%d/%s", sig, batch, v.name), func(b *testing.B) {
					if v.ch && os.Getenv("CLICKHOUSE_ENDPOINT") == "" {
						b.Skip("CLICKHOUSE_ENDPOINT not set")
					}
					dbSeq++
					f, cfg := v.make(fmt.Sprintf("bench_%d", dbSeq))
					set := exportertest.NewNopSettings(f.Type())
					var consume func() error
					var comp component.Component
					if sig == "traces" {
						e, err := f.CreateTraces(ctx, set, cfg)
						if err != nil {
							b.Fatal(err)
						}
						comp, consume = e, func() error { return e.ConsumeTraces(ctx, td) }
					} else {
						e, err := f.CreateLogs(ctx, set, cfg)
						if err != nil {
							b.Fatal(err)
						}
						comp, consume = e, func() error { return e.ConsumeLogs(ctx, ld) }
					}
					if err := comp.Start(ctx, componenttest.NewNopHost()); err != nil {
						b.Fatal(err)
					}
					// One untimed batch: schema creation and first-insert
					// warm-up are not what is being compared.
					if err := consume(); err != nil {
						b.Fatal(err)
					}
					b.ReportAllocs()
					c0, t0 := cpu(), time.Now()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if err := consume(); err != nil {
							b.Fatal(err)
						}
					}
					b.StopTimer()
					el, used := time.Since(t0), cpu()-c0
					rows := float64(b.N * batch)
					b.ReportMetric(rows/el.Seconds(), "rows/s")
					b.ReportMetric(float64(used.Nanoseconds())/rows, "cpu-ns/row")
					b.ReportMetric(0, "ns/op")
					if err := comp.Shutdown(ctx); err != nil {
						b.Fatal(err)
					}
				})
			}
		}
	}
}
