package compare

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo"
)

// TestArrowPageSplitVariants: arrow-go (v18.7.0) with V1 data pages can end
// a page in the middle of a row of a repeated column once values are large
// (here: 100 KB attribute values). With a page index in the file, which
// promises row-aligned pages, ClickHouse 26.10's Parquet reader then fails
// with "Invalid array of tuples ... different array lengths".
// This publishes the nasty traces with several writer settings and reports
// which ones the server reads.
func TestArrowPageSplitVariants(t *testing.T) {
	s3, ok := S3FromEnv()
	if !ok || os.Getenv("CHDB_TEST_CLICKHOUSE") == "" {
		t.Skip("needs CHDB_TEST_S3 and CHDB_TEST_CLICKHOUSE")
	}
	run := fmt.Sprintf("s%d", time.Now().Unix())
	td := NastyTraces(700)
	variants := []struct {
		name   string
		engine string
		f      func(*parquetgo.Options)
		want   bool // readable by the server
	}{
		{"arrow-v1", "arrow", func(o *parquetgo.Options) { o.DataPageV2 = false }, false},
		{"arrow-v1-no-page-index", "arrow", func(o *parquetgo.Options) { o.DataPageV2 = false; o.PageIndex = false }, true},
		{"arrow-v1-no-dictionary", "arrow", func(o *parquetgo.Options) { o.DataPageV2 = false; o.Dictionary = false }, false},
		{"arrow-v2", "arrow", func(o *parquetgo.Options) { o.DataPageV2 = true }, true},
		{"parquet-go", "parquet-go", func(o *parquetgo.Options) {}, true},
	}
	for _, v := range variants {
		opts := parquetgo.DefaultOptions()
		v.f(&opts)
		base := s3.Endpoint + "/pqsplit/" + run + "/" + v.name
		p, err := parquetgo.New(parquetgo.Config{URL: base, AccessKeyID: s3.Key, SecretAccessKey: s3.Secret,
			ProducerID: "cmp", Region: "cmp", SchemaVersion: 1, Epoch: run, Parquet: opts, Engine: v.engine})
		if err != nil {
			t.Fatal(err)
		}
		if err := p.PushTraces(context.Background(), td); err != nil {
			t.Fatal(err)
		}
		p.Close(context.Background())
		q := fmt.Sprintf("SELECT count(), sum(length(SpanAttributes)) FROM s3('%s/cmp/traces/v1/cmp/%s/*/*.parquet', '%s', '%s', 'Parquet')",
			base, run, s3.Key, s3.Secret)
		got, err := chTry(q)
		readable := err == nil
		t.Logf("%-24s readable=%v %s", v.name, readable, firstLine(got, err))
		if readable != v.want {
			t.Errorf("%s: readable=%v, expected %v", v.name, readable, v.want)
		}
	}
}
