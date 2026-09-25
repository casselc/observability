package compare

import (
	"bytes"
	"testing"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/chdbexporter/testgen"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo"
)

// BenchmarkGoEncode measures parquetgo's pdata -> Parquet bytes in memory,
// no I/O: the part of the Go path that replaces chDB.
func BenchmarkGoEncode(b *testing.B) {
	td, ld := testgen.Traces(10000), testgen.Logs(10000)
	for _, bloom := range []bool{true, false} {
		opts := parquetgo.DefaultOptions()
		opts.BloomFilters = bloom
		name := "bloom"
		if !bloom {
			name = "nobloom"
		}
		b.Run("traces-"+name, func(b *testing.B) {
			e := parquetgo.NewEncoder(opts, nil)
			var buf bytes.Buffer
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				buf.Reset()
				env := &parquetgo.Envelope{Producer: "p", Epoch: "e", Batch: uint64(i + 1), Received: 1, Schema: 1}
				if _, err := e.Traces(&buf, td, env); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(buf.Len()), "bytes/obj")
		})
		b.Run("logs-"+name, func(b *testing.B) {
			e := parquetgo.NewEncoder(opts, nil)
			var buf bytes.Buffer
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				buf.Reset()
				env := &parquetgo.Envelope{Producer: "p", Epoch: "e", Batch: uint64(i + 1), Received: 1, Schema: 1}
				if _, err := e.Logs(&buf, ld, env); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(buf.Len()), "bytes/obj")
		})
	}
}
