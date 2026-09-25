// Binary-size probe: the whole Go publisher (both engines, minio-go S3 client).
package main

import (
	"context"
	"os"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/parquetgo"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func main() {
	p, err := parquetgo.New(parquetgo.Config{URL: os.Args[1], ProducerID: "p", Region: "r", SchemaVersion: 1})
	if err != nil {
		panic(err)
	}
	p.PushTraces(context.Background(), ptrace.NewTraces())
}
