// Binary-size probe: arrow-go pqarrow alone.
package main

import (
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func main() {
	s := arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.BinaryTypes.String}}, nil)
	w, _ := pqarrow.NewFileWriter(s, io.Discard, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	w.Close()
}
