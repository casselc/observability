package compare

import (
	"bytes"
	"testing"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/chdbexporter/testgen"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo"
)

func BenchmarkExp(b *testing.B) {
	td := testgen.Traces(10000)
	for _, v := range []struct {
		name string
		f    func(*parquetgo.Options)
	}{
		{"default", func(o *parquetgo.Options) {}},
		{"nodict", func(o *parquetgo.Options) { o.Dictionary = false }},
		{"nodict-nobloom-nostats-noidx", func(o *parquetgo.Options) {
			o.Dictionary = false
			o.BloomFilters = false
			o.Statistics = false
			o.PageIndex = false
		}},
		{"nodict-nobloom-nostats-noidx-none", func(o *parquetgo.Options) {
			o.Dictionary = false
			o.BloomFilters = false
			o.Statistics = false
			o.PageIndex = false
			o.Compression = "none"
		}},
	} {
		opts := parquetgo.DefaultOptions()
		v.f(&opts)
		b.Run(v.name, func(b *testing.B) {
			e := parquetgo.NewEncoder(opts, nil)
			var buf bytes.Buffer
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				buf.Reset()
				env := &parquetgo.Envelope{Producer: "p", Epoch: "e", Batch: 1, Received: 1, Schema: 1}
				e.Traces(&buf, td, env)
			}
			b.ReportMetric(float64(buf.Len()), "bytes/obj")
		})
	}
}
