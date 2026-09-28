package lakeidx

import (
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"
)

// TestMeasureFixtures prints index costs over the lake UI's real Go-edge
// objects (LAKEIDX_MEASURE=1; README "Numbers"). Not a pass/fail test
// beyond decoding.
func TestMeasureFixtures(t *testing.T) {
	if os.Getenv("LAKEIDX_MEASURE") == "" {
		t.Skip("LAKEIDX_MEASURE=1 prints the numbers")
	}
	for _, c := range []struct{ file, trace, text string }{
		{"traces.parquet", "TraceId", ""},
		{"logs-late.parquet", "TraceId", "Body"},
	} {
		body, err := os.ReadFile("../../../lakeui/test/fixtures/" + c.file)
		if err != nil {
			t.Fatal(err)
		}
		cols := []string{c.trace}
		if c.text != "" {
			cols = append(cols, c.text)
		}
		var rows []int64
		var vals [][2]string
		var textBytes int
		t0 := time.Now()
		if err := RowGroupValues(body, cols, func(_ int, n int64) { rows = append(rows, n) }, func(_ int, col int, v []byte) {
			vals = append(vals, [2]string{fmt.Sprint(col), string(v)})
			if col == 1 {
				textBytes += len(v)
			}
		}); err != nil {
			t.Fatal(err)
		}
		decode := time.Since(t0)
		// one segment covering 1, 16 and 128 copies of the object (distinct
		// keys; the same rows, so the dictionary is shared as in an hour of
		// templated logs: an optimistic bound, stated as such)
		for _, copies := range []int{1, 16, 128} {
			t1 := time.Now()
			b := NewBuilder(BuildConfig{}, c.trace, c.text)
			distinct := map[string]bool{}
			for k := 0; k < copies; k++ {
				base := b.AddObject(SegObject{Key: fmt.Sprintf("c/p/%s/e/%020d.parquet", c.file, k), Size: int64(len(body)), RowGroups: rows})
				for _, v := range vals {
					if v[0] == "0" {
						b.AddTraceID(base, v[1])
						if v[1] != "" {
							distinct[fmt.Sprint(k)+v[1]] = true
						}
					} else {
						b.AddText(base, v[1])
					}
				}
			}
			seg, h, err := b.Build("c", "logs", "20260928T12", 0)
			if err != nil {
				t.Fatal(err)
			}
			build := time.Since(t1)
			if _, err := Decode(seg); err != nil {
				t.Fatal(err)
			}
			nrows := int64(0)
			for _, r := range rows {
				nrows += r
			}
			nrows *= int64(copies)
			hdr := len(seg)
			traceBytes, termBytes := 0, 0
			if h.Trace != nil {
				for _, bl := range h.Trace.Blocks {
					traceBytes += int(bl.Len)
				}
			}
			terms := 0
			if h.Terms != nil {
				terms = h.Terms.Terms
				for _, bl := range h.Terms.Blocks {
					termBytes += int(bl.Len)
				}
			}
			hdr -= traceBytes + termBytes + len(magic) + TrailerLen
			src := len(body) * copies
			// a binary fuse 8 per object: ~9.1 bits per distinct id (Graf &
			// Lemire), probed per object: one read per object in the window
			fuse := int(math.Ceil(float64(len(distinct)) * 9.1 / 8))
			fmt.Printf("%-18s x%-3d rows %7d src %9d B | segment %8d B (%.2f%% of src): header %7d, trace %7d (%d entries, %.2f B/entry, %d blocks), terms %8d (%d terms, %d blocks, %d frequent) | build %6.2f ms (%.0f ns/row) decode %.2f ms/object | fuse8/object %d B total, %d reads per lookup\n",
				c.file, copies, nrows, src, len(seg), 100*float64(len(seg))/float64(src), hdr, traceBytes, h.Trace.Entries,
				float64(traceBytes)/math.Max(1, float64(h.Trace.Entries)), len(h.Trace.Blocks), termBytes, terms, blocksOf(h), frequentOf(h),
				float64(build.Microseconds())/1000, float64(build.Nanoseconds())/float64(nrows), float64(decode.Microseconds())/1000, fuse, copies)
			if c.text != "" && copies == 1 {
				fmt.Printf("  %s: text %d B in %d rows; tokens: %s\n", c.file, textBytes, nrows, strings.Join(sampleTerms(seg), " "))
			}
		}
	}
}

func blocksOf(h *Header) int {
	if h.Terms == nil {
		return 0
	}
	return len(h.Terms.Blocks)
}

func frequentOf(h *Header) int {
	if h.Terms == nil {
		return 0
	}
	return h.Terms.Frequent
}

func sampleTerms(seg []byte) []string {
	s, _ := Decode(seg)
	var out []string
	for t := range s.Terms {
		if len(out) < 12 {
			out = append(out, t)
		}
	}
	return out
}
