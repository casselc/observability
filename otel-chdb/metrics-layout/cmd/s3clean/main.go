// s3clean deletes every object this directory's loaders wrote
// (otel/metrics-layout/...).
package main

import (
	"context"
	"fmt"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/metrics-layout/central"
)

func main() {
	n := central.DeletePrefix(context.Background(), central.S3(), central.Prefix+"/")
	fmt.Println("deleted", n, "objects under", central.Bucket+"/"+central.Prefix+"/")
}
