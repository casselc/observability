package otap

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/chdbexporter/testgen"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo"
	pb "github.com/open-telemetry/otel-arrow/go/api/experimental/arrow/v1"
)

// TestSmokeLocal publishes testgen through every variant to a local
// directory and logs the first flattened row and the object sizes.
func TestSmokeLocal(t *testing.T) {
	dir := t.TempDir()
	sink, _ := NewSink("file://"+dir, "", "", nil)
	td, ld := testgen.Traces(3000), testgen.Logs(3000)
	tb, err := EncodeTraces(td)
	if err != nil {
		t.Fatal(err)
	}
	lb, err := EncodeLogs(ld)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Decode(tb)
	if err != nil {
		t.Fatal(err)
	}
	rec := Flatten(b, &parquetgo.Envelope{Producer: "p", Epoch: "e", Batch: 1}, nil)
	for j, c := range rec.Columns() {
		t.Logf("%s = %s", rec.ColumnName(j), c.ValueStr(0))
	}
	rec.Release()
	b.Release()
	p := &Publisher{Sink: sink, Producer: "smoke", Epoch: "e1", SchemaVersion: 1, Parquet: parquetgo.DefaultOptions()}
	ctx := context.Background()
	if _, err := p.PublishRef(ctx, &td, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := p.PublishRef(ctx, nil, &ld); err != nil {
		t.Fatal(err)
	}
	for _, v := range []string{Star, FlatParquet, FlatArrow, ViaPdata, BAR} {
		for _, bar := range []*pb.BatchArrowRecords{tb, lb} {
			m, err := p.PublishBAR(ctx, v, bar)
			if err != nil {
				t.Fatalf("%s: %v", v, err)
			}
			total := 0
			for _, o := range m.Objects {
				total += o.Bytes
			}
			t.Logf("%-12s %-6s rows=%d objects=%d bytes=%d", v, m.Signal, m.Rows, len(m.Objects), total)
		}
	}
	filepath.Walk(dir, func(p string, fi os.FileInfo, err error) error {
		if !fi.IsDir() {
			t.Logf("%8d %s", fi.Size(), p[len(dir):])
		}
		return nil
	})
}
