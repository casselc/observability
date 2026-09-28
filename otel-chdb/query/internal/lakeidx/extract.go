package lakeidx

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/parquet-go/parquet-go"
)

// RowGroupValues calls f with every non-null value of the named top-level
// string columns, per row group, in file order. rows receives each row
// group's row count first. A column the file lacks yields nothing (an
// object without trace ids indexes no trace entries).
func RowGroupValues(body []byte, columns []string, rows func(rg int, n int64), f func(rg int, col int, v []byte)) error {
	pf, err := parquet.OpenFile(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return fmt.Errorf("parquet: %w", err)
	}
	idx := make([]int, len(columns))
	for i, c := range columns {
		idx[i] = -1
		if leaf, ok := pf.Schema().Lookup(c); ok && leaf.Node != nil && leaf.Node.Leaf() {
			idx[i] = leaf.ColumnIndex
		}
	}
	buf := make([]parquet.Value, 512)
	for g, rg := range pf.RowGroups() {
		rows(g, rg.NumRows())
		chunks := rg.ColumnChunks()
		for ci, x := range idx {
			if x < 0 {
				continue
			}
			if err := readChunk(chunks[x], buf, func(v []byte) { f(g, ci, v) }); err != nil {
				return fmt.Errorf("parquet: row group %d column %s: %w", g, columns[ci], err)
			}
		}
	}
	return nil
}

func readChunk(cc parquet.ColumnChunk, buf []parquet.Value, f func([]byte)) error {
	pages := cc.Pages()
	defer pages.Close()
	for {
		p, err := pages.ReadPage()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		vr := p.Values()
		for {
			n, err := vr.ReadValues(buf)
			for _, v := range buf[:n] {
				if !v.IsNull() {
					f(v.ByteArray())
				}
			}
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				parquet.Release(p)
				return err
			}
		}
		parquet.Release(p)
	}
}
