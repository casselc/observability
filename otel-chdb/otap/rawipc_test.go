package otap

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/chdbexporter/testgen"
)

// TestRawPayloadsReadable writes each OTAP payload's IPC bytes, exactly as
// they are in the BatchArrowRecords, as its own object, and has ClickHouse
// read them with the ArrowStream format.
func TestRawPayloadsReadable(t *testing.T) {
	e := testEnv(t)
	run := fmt.Sprintf("raw%d", time.Now().Unix())
	sink, _ := NewSink(e.s3+"/"+run, e.key, e.secret, nil)
	for _, sig := range []string{"traces", "logs"} {
		bar, _ := EncodeTraces(testgen.Traces(10000))
		if sig == "logs" {
			bar, _ = EncodeLogs(testgen.Logs(10000))
		}
		for _, pl := range bar.ArrowPayloads {
			name := TableName(pl.Type)
			if err := sink.Put(context.Background(), sig+"/"+name+".arrows", pl.Record, false); err != nil {
				t.Fatal(err)
			}
			src := fmt.Sprintf("s3('%s/%s/%s/%s.arrows', '%s', '%s', 'ArrowStream')", e.s3, run, sig, name, e.key, e.secret)
			out, err := chQuery(e.ch, "SELECT count() FROM "+src, nil)
			desc, _ := chQuery(e.ch, "DESCRIBE "+src+" FORMAT TSV", nil)
			t.Logf("%s %-16s %6d bytes: count=%s err=%v\n%s", sig, name, len(pl.Record), out, err, desc)
		}
	}
	os.Setenv("OTAP_RAW_RUN", run)
}
