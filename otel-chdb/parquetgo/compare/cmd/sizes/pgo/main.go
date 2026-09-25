// Binary-size probe: the parquet-go encoder only.
package main

import (
	"io"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func main() {
	parquetgo.NewPGEncoder(parquetgo.DefaultOptions()).Traces(io.Discard, ptrace.NewTraces(), &parquetgo.Envelope{})
}
