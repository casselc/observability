package compare

import (
	"bytes"
	"testing"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/chdbexporter/testgen"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo"
)

func encoders(opts parquetgo.Options) map[string]func() parquetgo.BatchEncoder {
	return map[string]func() parquetgo.BatchEncoder{
		"arrow":      func() parquetgo.BatchEncoder { return parquetgo.NewEncoder(opts, nil) },
		"parquet-go": func() parquetgo.BatchEncoder { return parquetgo.NewPGEncoder(opts) },
		"parquet-go-par4": func() parquetgo.BatchEncoder {
			o := opts
			o.Parallelism = 4
			return parquetgo.NewPGEncoder(o)
		},
	}
}

// BenchmarkGoEncode measures pdata -> Parquet bytes in memory, no I/O: the
// part of the Go path that replaces chDB.
func BenchmarkGoEncode(b *testing.B) {
	td, ld := testgen.Traces(10000), testgen.Logs(10000)
	for _, engine := range []string{"arrow", "parquet-go", "parquet-go-par4"} {
		for _, bloom := range []bool{true, false} {
			opts := parquetgo.DefaultOptions()
			opts.BloomFilters = bloom
			name := engine + "-bloom"
			if !bloom {
				name = engine + "-nobloom"
			}
			mk := encoders(opts)[engine]
			b.Run("traces-"+name, func(b *testing.B) {
				e := mk()
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
				e := mk()
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
}

// BenchmarkMetricsEncode measures pdata -> Parquet bytes for 10k data
// points of one metric type, in memory, no I/O.
func BenchmarkMetricsEncode(b *testing.B) {
	for t, sig := range parquetgo.MetricSignals {
		md := MetricsBatch(parquetgo.MetricType(t), 10000, 0)
		for _, bloom := range []bool{true, false} {
			opts := parquetgo.DefaultOptions()
			opts.BloomFilters = bloom
			name := sig + "-bloom"
			if !bloom {
				name = sig + "-nobloom"
			}
			b.Run(name, func(b *testing.B) {
				e := parquetgo.NewPGEncoder(opts)
				var buf bytes.Buffer
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					buf.Reset()
					env := &parquetgo.Envelope{Producer: "p", Epoch: "e", Batch: uint64(i + 1), Received: 1, Schema: 1}
					if _, err := e.MetricsOf(&buf, md, parquetgo.MetricType(t), env); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/10000, "ns/point")
				b.ReportMetric(float64(buf.Len()), "bytes/obj")
			})
		}
	}
}
