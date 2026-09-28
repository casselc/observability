package lakeidx

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/query/internal/store"
	"pgregory.net/rapid"
)

// genObject is a generated source object: rows per row group.
type genObject struct {
	key  string
	rgs  [][]genRow
	size int64
}

type genRow struct{ trace, body string }

var words = []string{"timeout", "acct-7731", "GET", "/api/v1", "error", "ERROR", "db", "retry", "İd", "\u212aey", "café",
	"x", "xx", "user_42", "0xdeadbeef", "status=500", "averyveryverylongtokenthatexceedsthelimit"}

func genRows(t *rapid.T, traces []string) []genRow {
	n := rapid.IntRange(0, 12).Draw(t, "rows")
	out := make([]genRow, n)
	for i := range out {
		ws := rapid.SliceOfN(rapid.SampledFrom(words), 0, 6).Draw(t, "words")
		sep := rapid.SampledFrom([]string{" ", "-", "", ", ", "😀"}).Draw(t, "sep")
		tr := ""
		if len(traces) > 0 && rapid.IntRange(0, 5).Draw(t, "hastrace") > 0 {
			tr = rapid.SampledFrom(traces).Draw(t, "trace")
			if rapid.Bool().Draw(t, "upper") {
				tr = strings.ToUpper(tr)
			}
		}
		out[i] = genRow{trace: tr, body: strings.Join(ws, sep)}
	}
	return out
}

func genTraces(t *rapid.T) []string {
	n := rapid.IntRange(1, 6).Draw(t, "ntraces")
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%032x", rapid.Uint64().Draw(t, "tid"))
	}
	return out
}

// truth: the row groups the page's test matches, by brute force.
func truth(objs []genObject, traceID string, terms []string) map[string][]int {
	out := map[string][]int{}
	for _, o := range objs {
		for g, rows := range o.rgs {
			hit := false
			for _, r := range rows {
				ok := true
				if traceID != "" && strings.ToLower(r.trace) != traceID {
					ok = false
				}
				for _, t := range terms {
					if !strings.Contains(jsLower(r.body), jsLower(t)) {
						ok = false
					}
				}
				hit = hit || ok
			}
			if hit {
				out[o.key] = append(out[o.key], g)
			}
		}
	}
	return out
}

func buildSeg(objs []genObject, cfg BuildConfig, signalText bool) ([]byte, *Header, error) {
	text := ""
	if signalText {
		text = "Body"
	}
	b := NewBuilder(cfg, "TraceId", text)
	for _, o := range objs {
		rows := make([]int64, len(o.rgs))
		for g, r := range o.rgs {
			rows[g] = int64(len(r))
		}
		base := b.AddObject(SegObject{Key: o.key, Size: o.size, RowGroups: rows})
		for g, rs := range o.rgs {
			for _, r := range rs {
				b.AddTraceID(base+uint32(g), r.trace)
				if signalText {
					b.AddText(base+uint32(g), r.body)
				}
			}
		}
	}
	return b.Build("c1", "logs", "20260928T12", 0)
}

func genObjs(t *rapid.T, traces []string, prefix string) []genObject {
	n := rapid.IntRange(1, 8).Draw(t, "objects")
	objs := make([]genObject, n)
	for i := range objs {
		nrg := rapid.IntRange(1, 4).Draw(t, "rgs")
		o := genObject{key: fmt.Sprintf("%s/o%03d.parquet", prefix, i), size: int64(100 + i)}
		for g := 0; g < nrg; g++ {
			o.rgs = append(o.rgs, genRows(t, traces))
		}
		objs[i] = o
	}
	return objs
}

func smallCfg(t *rapid.T) BuildConfig {
	return BuildConfig{
		TraceBlockBytes: rapid.IntRange(1, 64).Draw(t, "tblock"),
		TermBlockBytes:  rapid.IntRange(1, 128).Draw(t, "sblock"),
		MaxTerm:         rapid.IntRange(3, 70).Draw(t, "maxterm"),
		FreqCut:         rapid.Float64Range(0.1, 1).Draw(t, "freqcut"),
		FreqMinRGs:      rapid.IntRange(1, 10).Draw(t, "freqmin"),
	}
}

func queryFor(t *rapid.T, traces []string) (string, []string) {
	tr := ""
	if rapid.Bool().Draw(t, "bytrace") {
		tr = rapid.SampledFrom(append(traces, "0123456789abcdef0123456789abcdef")).Draw(t, "qtrace")
	}
	var terms []string
	if tr == "" || rapid.Bool().Draw(t, "both") {
		for n := rapid.IntRange(1, 2).Draw(t, "nterms"); n > 0; n-- {
			w := rapid.SampledFrom(words).Draw(t, "qword")
			// a random substring of a word, maybe with a neighbouring separator
			i := rapid.IntRange(0, len(w)).Draw(t, "qi")
			j := rapid.IntRange(i, len(w)).Draw(t, "qj")
			q := w[i:j]
			if !isUTF8(q) {
				q = w
			}
			if rapid.Bool().Draw(t, "qsep") {
				q = rapid.SampledFrom([]string{" ", "-"}).Draw(t, "qs") + q
			}
			if rapid.Bool().Draw(t, "qcase") {
				q = strings.ToUpper(q)
			}
			terms = append(terms, q)
		}
	}
	return tr, terms
}

func isUTF8(s string) bool { return strings.ToValidUTF8(s, "\x00") == s }

// resolve through a Mem bucket: objects listed with LastModified in the
// segment's hour, the segment stored under its key.
func resolveVia(_ any, segs map[string][]byte, objs []genObject, traceID string, terms []string) (map[string]ObjResult, Report) {
	m := NewMem()
	at := time.Date(2026, 9, 28, 12, 30, 0, 0, time.UTC)
	for k, b := range segs {
		m.Put(k, b, nil, at)
	}
	var listed []store.Object
	for _, o := range objs {
		listed = append(listed, store.Object{Key: o.key, Size: o.size, LastModified: at})
	}
	r := NewResolver(ResolverConfig{}, m)
	return r.Resolve(context.Background(), "c1", "logs", listed, Filter{TraceID: traceID, Terms: terms})
}

// Property: index lookup ⊇ true matches. Every row group brute force finds
// is kept (hit with that row group, or scan); "none" only for objects with
// no match.
func TestResolveHasNoFalseNegatives(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		traces := genTraces(t)
		objs := genObjs(t, traces, "c1/p/logs/e")
		cfg := smallCfg(t)
		body, _, err := buildSeg(objs, cfg, true)
		if err != nil {
			t.Fatal(err)
		}
		tr, terms := queryFor(t, traces)
		key := Key("", "c1", "logs", "20260928T12", 0, body)
		got, rep := resolveVia(t, map[string][]byte{key: body}, objs, tr, terms)
		if len(rep.Errors) > 0 {
			t.Fatalf("errors %v", rep.Errors)
		}
		want := truth(objs, tr, terms)
		checkSuperset(t, objs, got, want)
		if rep.Scan != 0 && (tr != "" || len(Filter{Terms: terms}.constraints()) > 0) {
			t.Fatalf("covered objects scanned: %+v", rep)
		}
	})
}

func checkSuperset(t interface{ Fatalf(string, ...any) }, objs []genObject, got map[string]ObjResult, want map[string][]int) {
	for _, o := range objs {
		r := got[o.key]
		w := want[o.key]
		switch r.Status {
		case Scan:
		case None:
			if len(w) > 0 {
				t.Fatalf("%s: index says none, true matches in row groups %v", o.key, w)
			}
		case Hit:
			have := map[int]bool{}
			for _, g := range r.RowGroups {
				have[g] = true
			}
			for _, g := range w {
				if !have[g] {
					t.Fatalf("%s: row group %d matches, index gave %v", o.key, g, r.RowGroups)
				}
			}
		default:
			t.Fatalf("%s: no result", o.key)
		}
	}
}

func TestDecodeRoundTrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		traces := genTraces(t)
		objs := genObjs(t, traces, "k")
		body, h, err := buildSeg(objs, smallCfg(t), true)
		if err != nil {
			t.Fatal(err)
		}
		s, err := Decode(body)
		if err != nil {
			t.Fatal(err)
		}
		if s.Header.SourcesHash != h.SourcesHash || len(s.FPs) != h.Trace.Entries || len(s.Terms) != h.Terms.Terms {
			t.Fatalf("decoded %d fps %d terms, header %d %d", len(s.FPs), len(s.Terms), h.Trace.Entries, h.Terms.Terms)
		}
		// every row's trace fingerprint is present for its row group
		for _, o := range objs {
			idx := -1
			for i, so := range s.Header.Objects {
				if so.Key == o.key {
					idx = i
				}
			}
			for g, rows := range o.rgs {
				for _, r := range rows {
					if r.trace == "" {
						continue
					}
					e := fpEntry{FP(r.trace), s.Header.Objects[idx].Base + uint32(g)}
					i := sort.Search(len(s.FPs), func(i int) bool {
						return s.FPs[i].fp > e.fp || s.FPs[i].fp == e.fp && s.FPs[i].rg >= e.rg
					})
					if i == len(s.FPs) || s.FPs[i] != e {
						t.Fatalf("missing %v", e)
					}
				}
			}
		}
	})
}

// A segment whose bytes are damaged anywhere either still verifies with the
// same answers or is refused: never a narrower answer (every object then
// scans).
func TestCorruptionNeverNarrows(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		traces := genTraces(t)
		objs := genObjs(t, traces, "c1/p/logs/e")
		body, _, err := buildSeg(objs, smallCfg(t), true)
		if err != nil {
			t.Fatal(err)
		}
		key := Key("", "c1", "logs", "20260928T12", 0, body)
		bad := bytes.Clone(body)
		i := rapid.IntRange(0, len(bad)-1).Draw(t, "at")
		bad[i] ^= byte(rapid.IntRange(1, 255).Draw(t, "xor"))
		tr, terms := queryFor(t, traces)
		got, rep := resolveVia(t, map[string][]byte{key: bad}, objs, tr, terms)
		want := truth(objs, tr, terms)
		checkSuperset(t, objs, got, want)
		if len(rep.Errors) == 0 {
			// undetected: the flipped byte was not read by this lookup (a
			// block other than the one consulted); answers must be exact
			// anyway, which checkSuperset has shown
			return
		}
		for _, o := range objs {
			if got[o.key].Status == None && len(want[o.key]) > 0 {
				t.Fatalf("pruned a match after an error")
			}
		}
	})
}
