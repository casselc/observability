// Binary-size probe: parquet-go alone.
package main

import (
	"io"

	"github.com/parquet-go/parquet-go"
)

func main() {
	w := parquet.NewGenericWriter[any](io.Discard, parquet.NewSchema("s", parquet.Group{"a": parquet.String()}))
	w.Close()
}
