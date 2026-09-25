// pqpages prints the data pages of each column chunk of a Parquet file:
// values, rows, and whether the page starts at a row boundary (repetition
// level 0), which readers using the page index rely on.
package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/parquet-go/parquet-go"
)

func main() {
	f, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	st, _ := f.Stat()
	pf, err := parquet.OpenFile(f, st.Size())
	if err != nil {
		log.Fatal(err)
	}
	filter := ""
	if len(os.Args) > 2 {
		filter = os.Args[2]
	}
	for _, rg := range pf.RowGroups() {
		for ci, cc := range rg.ColumnChunks() {
			path := strings.Join(pf.Schema().Columns()[ci], ".")
			if filter != "" && !strings.Contains(path, filter) {
				continue
			}
			pages := cc.Pages()
			n, bad := 0, 0
			var desc []string
			for {
				p, err := pages.ReadPage()
				if err == io.EOF {
					break
				}
				if err != nil {
					log.Fatalf("%s: %v", path, err)
				}
				rows := p.NumRows()
				vals := p.NumValues()
				first := -1
				if rl := p.RepetitionLevels(); len(rl) > 0 {
					first = int(rl[0])
				}
				if first > 0 {
					bad++
				}
				desc = append(desc, fmt.Sprintf("[v=%d r=%d rep0=%d]", vals, rows, first))
				n++
				parquet.Release(p)
			}
			pages.Close()
			fmt.Printf("%-50s pages=%d not-row-aligned=%d %s\n", path, n, bad, strings.Join(desc, " "))
		}
	}
}
