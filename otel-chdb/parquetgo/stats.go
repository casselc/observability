package parquetgo

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

// TruncateStatistics bounds the column-chunk min/max statistics in a
// Parquet file's footer to n bytes, as parquet-rs does by default
// (statistics_truncate_length 64, the Rust edge's writer): the minimum is
// cut to n bytes (still a lower bound), the maximum cut and incremented
// (still an upper bound); a maximum that can't be incremented (all 0xff)
// drops both. parquet-go writes the whole values, so one 1 MiB attribute
// value put 2 MiB into the footer. Only the footer changes: data pages,
// indexes and bloom filters keep their offsets (they precede it). A file
// with nothing to truncate is returned as is; otherwise file's own memory is
// reused (the result aliases it).
func TruncateStatistics(file []byte, n int) ([]byte, error) {
	size := len(file)
	if size < 12 || !bytes.Equal(file[size-4:], []byte("PAR1")) {
		return nil, errors.New("not a Parquet file")
	}
	flen := int(binary.LittleEndian.Uint32(file[size-8 : size-4]))
	if flen <= 0 || flen > size-12 {
		return nil, errors.New("bad Parquet footer length")
	}
	start := size - 8 - flen
	var md format.FileMetaData
	proto := &thrift.CompactProtocol{}
	if err := thrift.Unmarshal(proto, file[start:size-8], &md); err != nil {
		return nil, fmt.Errorf("decode footer: %w", err)
	}
	changed := false
	for i := range md.RowGroups {
		for j := range md.RowGroups[i].Columns {
			cm := &md.RowGroups[i].Columns[j].MetaData
			if cm.Type != format.ByteArray && cm.Type != format.FixedLenByteArray {
				continue
			}
			st := &cm.Statistics
			if len(st.MinValue) > n {
				st.MinValue, changed = st.MinValue[:n:n], true
			}
			if len(st.Min) > n {
				st.Min, changed = st.Min[:n:n], true
			}
			for _, max := range []*[]byte{&st.MaxValue, &st.Max} {
				if len(*max) <= n {
					continue
				}
				changed = true
				v := append([]byte(nil), (*max)[:n]...)
				for len(v) > 0 && v[len(v)-1] == 0xff {
					v = v[:len(v)-1]
				}
				if len(v) == 0 { // no shorter upper bound: no bounds at all
					st.MaxValue, st.MinValue, st.Max, st.Min = nil, nil, nil, nil
					break
				}
				v[len(v)-1]++
				*max = v
			}
		}
	}
	if !changed {
		return file, nil
	}
	footer, err := thrift.Marshal(proto, &md)
	if err != nil {
		return nil, fmt.Errorf("encode footer: %w", err)
	}
	// In place: the footer only shrank, so it fits where the old one was.
	out := append(file[:start], footer...)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(footer)))
	return append(out, "PAR1"...), nil
}
