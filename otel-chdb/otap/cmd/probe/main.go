// probe prints the OTAP tables the Go otel-arrow producer emits for testgen.
package main

import (
	"fmt"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/chdbexporter/testgen"
	"github.com/open-telemetry/otel-arrow/go/pkg/otel/arrow_record"
)

func main() {
	p := arrow_record.NewProducer()
	bar, err := p.BatchArrowRecordsFromTraces(testgen.Traces(10000))
	if err != nil {
		panic(err)
	}
	c := arrow_record.NewConsumer()
	recs, err := c.Consume(bar)
	if err != nil {
		panic(err)
	}
	for i, r := range recs {
		fmt.Printf("== %v rows=%d ipc=%d\n%v\n", r.PayloadType(), r.Record().NumRows(), len(bar.ArrowPayloads[i].Record), r.Record().Schema())
		if r.Record().NumRows() > 0 {
			s := r.Record().NewSlice(0, 3)
			for j, c := range s.Columns() {
				fmt.Printf("  %s: %v\n", s.ColumnName(j), c)
			}
		}
	}
	lp := arrow_record.NewProducer()
	lbar, _ := lp.BatchArrowRecordsFromLogs(testgen.Logs(10000))
	lrecs, err := arrow_record.NewConsumer().Consume(lbar)
	if err != nil {
		panic(err)
	}
	for i, r := range lrecs {
		fmt.Printf("== %v rows=%d ipc=%d\n%v\n", r.PayloadType(), r.Record().NumRows(), len(lbar.ArrowPayloads[i].Record), r.Record().Schema())
	}
}
