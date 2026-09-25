package main

// Metrics datasets, as ExportMetricsServiceRequest bytes:
//
//	metrics-testgen-3000.pb   parquetgo compare.Metrics(3000): 3,000 points of every type
//	metrics-nasty-700.pb      compare.NastyMetrics(700): NaN/±Inf, exemplars, negative
//	                          exponential buckets, invalid UTF-8, extreme timestamps, ...
//	metrics-extra.pb          wire-level cases pdata can't build: maps with duplicate keys
//	                          above pdqsort's insertion-sort cutoff (12 entries)
//	metrics-mixed-10000.pb    2,000 points of every type (10,000 per request)
//	metrics-mixed-10000-bNN.pb   the same, batch NN of a contiguous stream
//	metrics-<type>-10000-bNN.pb  10,000 points of one type, batch NN of a contiguous stream
//
// and, with -ref, the same testgen / nasty / extra data published by parquetgo
// (PushMetrics) as the reference objects.

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo/compare"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"google.golang.org/protobuf/encoding/protowire"
)

type metricsSet struct {
	name string
	md   pmetric.Metrics
}

func mustMarshalMetrics(md pmetric.Metrics) []byte {
	b, err := pmetricotlp.NewExportRequestFromMetrics(md).MarshalProto()
	if err != nil {
		log.Fatal(err)
	}
	return b
}

// metricsDatasets are the correctness datasets (testgen, nasty, extra).
func metricsDatasets() []metricsSet {
	return []metricsSet{
		{"testgen-3000", compare.Metrics(3000)},
		{"nasty-700", compare.NastyMetrics(700)},
		{"extra", extraMetrics()},
	}
}

func writeMetrics(out string, variants int) {
	write := func(name string, b []byte) {
		p := filepath.Join(out, name)
		if err := os.WriteFile(p, b, 0o644); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s\t%d bytes\n", p, len(b))
	}
	for _, d := range metricsDatasets() {
		write("metrics-"+d.name+".pb", mustMarshalMetrics(d.md))
	}
	write("metrics-mixed-10000.pb", mustMarshalMetrics(compare.Metrics(2000)))
	// Distinct mixed requests (2,000 points of every type, batch b of the
	// stream), for the fault runs.
	for b := 0; b < max(variants, 1); b++ {
		md := pmetric.NewMetrics()
		for t := range parquetgo.NumMetricTypes {
			compare.MetricsBatch(t, 2000, b).ResourceMetrics().MoveAndAppendTo(md.ResourceMetrics())
		}
		write(fmt.Sprintf("metrics-mixed-10000-b%02d.pb", b), mustMarshalMetrics(md))
	}
	for t := range parquetgo.NumMetricTypes {
		for b := 0; b < max(variants, 1); b++ {
			write(fmt.Sprintf("metrics-%s-10000-b%02d.pb", t.String()[len("metrics_"):], b),
				mustMarshalMetrics(compare.MetricsBatch(t, 10000, b)))
		}
	}
}

// publishMetricsRef publishes each correctness dataset with parquetgo, one
// producer per dataset: {ref}/cmp/metrics_<type>/v1/{dataset}/{epoch}/...
func publishMetricsRef(ref, key, secret, epoch string) {
	for _, d := range metricsDatasets() {
		p, err := parquetgo.New(parquetgo.Config{URL: ref, AccessKeyID: key, SecretAccessKey: secret,
			ProducerID: d.name, Region: "cmp", SchemaVersion: 1, Epoch: epoch,
			Parquet: parquetgo.DefaultOptions(), Engine: "parquet-go"})
		if err != nil {
			log.Fatal(err)
		}
		ctx := context.Background()
		// Round-trip through the wire, so the reference sees what the edge sees.
		md, err := (&pmetric.ProtoUnmarshaler{}).UnmarshalMetrics(mustMarshalMetrics(d.md))
		if err != nil {
			log.Fatal(err)
		}
		if err := p.PushMetrics(ctx, md); err != nil {
			log.Fatal(err)
		}
		if err := p.Close(ctx); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("reference %s: %s/cmp/metrics_*/v1/%s/%s/\n", d.name, ref, d.name, epoch)
	}
}

// ---- wire-level cases -----------------------------------------------------------

func tag(b []byte, num protowire.Number, t protowire.Type) []byte { return protowire.AppendTag(b, num, t) }

func msg(b []byte, num protowire.Number, m []byte) []byte {
	b = tag(b, num, protowire.BytesType)
	return protowire.AppendBytes(b, m)
}

func str(b []byte, num protowire.Number, s string) []byte {
	b = tag(b, num, protowire.BytesType)
	return protowire.AppendString(b, s)
}

func fixed64(b []byte, num protowire.Number, v uint64) []byte {
	b = tag(b, num, protowire.Fixed64Type)
	return protowire.AppendFixed64(b, v)
}

// kv is a KeyValue with a string AnyValue.
func kv(k, v string) []byte {
	var any []byte
	any = str(any, 1, v) // AnyValue.string_value
	var b []byte
	b = str(b, 1, k)
	return msg(b, 2, any)
}

// dupAttrs returns n KeyValues (field num) whose keys repeat, out of order,
// with distinct values: 'k' + one of 5 letters, so a map of 20 has each key
// 4 times. Go's pdqsort (n > 12) leaves equal keys in an order that isn't
// the input order.
func dupAttrs(b []byte, num protowire.Number, n, salt int) []byte {
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k%c", 'a'+byte((i*7+salt)%5))
		b = msg(b, num, kv(k, fmt.Sprintf("v%d-%d", salt, i)))
	}
	return b
}

// extraMetrics is a request with duplicate keys in resource, scope, point
// and exemplar attributes, for every metric type, built on the wire (pdata
// deduplicates keys when a map is built through its API, but keeps them
// when it decodes them).
func extraMetrics() pmetric.Metrics {
	const t0 = uint64(1_790_000_000_123_456_789)
	var scope []byte
	scope = str(scope, 1, "extra.scope")
	scope = str(scope, 2, "1.0")
	scope = dupAttrs(scope, 3, 14, 1)
	var sm []byte
	sm = msg(sm, 1, scope)
	for ti := 0; ti < 5; ti++ {
		for p := 0; p < 3; p++ {
			var ex []byte
			ex = dupAttrs(ex, 7, 13+p, 10*ti+p) // Exemplar.filtered_attributes
			ex = fixed64(ex, 2, t0)             // time_unix_nano
			ex = fixed64(ex, 3, math.Float64bits(1.5))
			var dp []byte
			attrsN := []int{20, 12, 40}[p]
			var m []byte
			m = str(m, 1, fmt.Sprintf("extra.%d", ti))
			switch ti {
			case 0, 1: // gauge (5), sum (7): NumberDataPoint
				dp = dupAttrs(dp, 7, attrsN, ti*100+p)
				dp = fixed64(dp, 2, t0-1e9)
				dp = fixed64(dp, 3, t0)
				dp = fixed64(dp, 4, math.Float64bits(float64(p)))
				dp = msg(dp, 5, ex)
				var g []byte
				g = msg(g, 1, dp)
				if ti == 1 {
					g = protowire.AppendVarint(tag(g, 2, protowire.VarintType), 2)
					g = protowire.AppendVarint(tag(g, 3, protowire.VarintType), 1)
					m = msg(m, 7, g)
				} else {
					m = msg(m, 5, g)
				}
			case 2: // histogram (9)
				dp = dupAttrs(dp, 9, attrsN, ti*100+p)
				dp = fixed64(dp, 2, t0-1e9)
				dp = fixed64(dp, 3, t0)
				dp = fixed64(dp, 4, 3)
				dp = msg(dp, 8, ex)
				var h []byte
				h = msg(h, 1, dp)
				m = msg(m, 9, h)
			case 3: // exponential histogram (10)
				dp = dupAttrs(dp, 1, attrsN, ti*100+p)
				dp = fixed64(dp, 2, t0-1e9)
				dp = fixed64(dp, 3, t0)
				dp = fixed64(dp, 4, 3)
				dp = msg(dp, 11, ex)
				var h []byte
				h = msg(h, 1, dp)
				m = msg(m, 10, h)
			case 4: // summary (11)
				dp = dupAttrs(dp, 7, attrsN, ti*100+p)
				dp = fixed64(dp, 2, t0-1e9)
				dp = fixed64(dp, 3, t0)
				dp = fixed64(dp, 4, 3)
				var s []byte
				s = msg(s, 1, dp)
				m = msg(m, 11, s)
			}
			sm = msg(sm, 2, m)
		}
	}
	var res []byte
	res = dupAttrs(res, 1, 25, 7)
	res = msg(res, 1, kv("service.name", "extra-first"))
	res = msg(res, 1, kv("service.name", "extra-second"))
	var rm []byte
	rm = msg(rm, 1, res)
	rm = msg(rm, 2, sm)
	var req []byte
	req = msg(req, 1, rm)
	md, err := (&pmetric.ProtoUnmarshaler{}).UnmarshalMetrics(req)
	if err != nil {
		log.Fatalf("extra metrics: %v", err)
	}
	return md
}
