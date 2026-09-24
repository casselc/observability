package chdbexporter

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/chdbexporter/testgen"
)

// BenchmarkEncode measures the format alone: pdata to bytes, no chDB.
func BenchmarkEncode(b *testing.B) {
	td, ld := testgen.Traces(1000), testgen.Logs(1000)
	for _, sig := range []string{"traces", "logs"} {
		for _, f := range []string{FormatRowBinary, FormatJSON} {
			b.Run(sig+"/"+f, func(b *testing.B) {
				var w rowWriter
				var buf *[]byte
				switch f {
				case FormatRowBinary:
					rb := &rowBinary{}
					w, buf = rb, &rb.buf
				default:
					js := &jsonEachRow{cols: traceColumns}
					if sig == "logs" {
						js.cols = logColumns
					}
					w, buf = js, &js.buf
				}
				b.ReportAllocs()
				rows := 0
				for i := 0; i < b.N; i++ {
					*buf = (*buf)[:0]
					if sig == "traces" {
						rows += writeTraces(w, td)
					} else {
						rows += writeLogs(w, ld)
					}
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(rows), "ns/row")
				b.ReportMetric(float64(len(*buf))/1000, "B/row")
			})
		}
	}
}

var benchDB atomic.Int64

// BenchmarkInsert measures a whole push, encode plus insert into MergeTree,
// per format, batch size, table layout and connection count. Each iteration
// is one batch; rows/s is the figure to compare.
//
// Layouts: staged (the default: Null staging table + materialized view),
// direct (insert straight into the MergeTree table), buffered (staged into a
// Buffer table, 10 s).
func BenchmarkInsert(b *testing.B) {
	type layout struct {
		name          string
		staged        bool
		buffer, conns int
		onlyBatch     int
	}
	layouts := []layout{
		{name: "staged", staged: true, conns: 1},
		{name: "direct", conns: 1},
		{name: "staged-conns4", staged: true, conns: 4, onlyBatch: 1000},
		{name: "buffered", staged: true, buffer: 10, conns: 1, onlyBatch: 100},
	}
	for _, sig := range []string{"traces", "logs"} {
		for _, batch := range []int{100, 1000, 10000} {
			td, ld := testgen.Traces(batch), testgen.Logs(batch)
			for _, f := range formats() {
				for _, l := range layouts {
					if l.onlyBatch != 0 && l.onlyBatch != batch {
						continue
					}
					name := fmt.Sprintf("%s/batch=%d/%s/%s", sig, batch, f, l.name)
					b.Run(name, func(b *testing.B) {
						cfg := testConfig(fmt.Sprintf("bench%d", benchDB.Add(1)), f)
						cfg.Connections, cfg.StagingTables, cfg.BufferSeconds = l.conns, l.staged, l.buffer
						e := startExporter(b, cfg)
						ctx := context.Background()
						push := func() error {
							if sig == "traces" {
								return e.pushTraces(ctx, td)
							}
							return e.pushLogs(ctx, ld)
						}
						if err := push(); err != nil { // warm-up, untimed
							b.Fatal(err)
						}
						b.ReportAllocs()
						b.ResetTimer()
						start := time.Now()
						if l.conns == 1 {
							for i := 0; i < b.N; i++ {
								if err := push(); err != nil {
									b.Fatal(err)
								}
							}
						} else {
							b.SetParallelism(1) // GOMAXPROCS goroutines
							b.RunParallel(func(pb *testing.PB) {
								for pb.Next() {
									if err := push(); err != nil {
										b.Error(err)
										return
									}
								}
							})
						}
						el := time.Since(start)
						b.StopTimer()
						b.ReportMetric(float64(b.N*batch)/el.Seconds(), "rows/s")
						b.ReportMetric(float64(el.Nanoseconds())/float64(b.N*batch), "ns/row")
						b.ReportMetric(0, "ns/op")
						e.shutdown(ctx)
						query(b, "DROP DATABASE "+cfg.Database)
					})
				}
			}
		}
	}
}
