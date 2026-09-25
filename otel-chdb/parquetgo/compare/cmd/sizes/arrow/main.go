// Binary-size probe: the arrow-go encoder only.
package main

import (
	"io"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func main() {
	parquetgo.NewEncoder(parquetgo.DefaultOptions(), nil).Traces(io.Discard, ptrace.NewTraces(), &parquetgo.Envelope{})
}
