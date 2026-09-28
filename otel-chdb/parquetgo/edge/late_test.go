package edge

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/parquetgo/commit"
	"github.com/parquet-go/parquet-go"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

const lateT = uint64(1_790_000_000_000_000_000)

func seq(ts ...uint64) func(func(uint64) bool) {
	return func(yield func(uint64) bool) {
		for _, t := range ts {
			if !yield(t) {
				return
			}
		}
	}
}

func TestLateCut(t *testing.T) {
	B := 15 * time.Minute
	b := uint64(B)
	for _, c := range []struct {
		name  string
		ts    []uint64
		after time.Duration
		cut   uint64
		ok    bool
	}{
		{"off", []uint64{lateT, lateT - 10*b}, 0, 0, false},
		{"negative is off", []uint64{lateT, lateT - 10*b}, -time.Second, 0, false},
		{"no rows", nil, B, 0, false},
		{"one row", []uint64{lateT}, B, 0, false},
		{"exactly at the bound is bulk", []uint64{lateT, lateT - b}, B, 0, false},
		{"one ns past the bound is late", []uint64{lateT, lateT - b - 1}, B, lateT - b, true},
		{"order does not matter", []uint64{lateT - b - 1, lateT, lateT - 1}, B, lateT - b, true},
		{"all old but close together: whole", []uint64{lateT - 100*b, lateT - 100*b + 5}, B, 0, false},
		{"newest below the bound: no cut below 0", []uint64{b - 1, 0}, B, 0, false},
		{"a zero timestamp is late", []uint64{lateT, 0}, B, lateT - b, true},
	} {
		cut, ok := LateCut(seq(c.ts...), c.after)
		if cut != c.cut || ok != c.ok {
			t.Errorf("%s: LateCut = %d, %v; want %d, %v", c.name, cut, ok, c.cut, c.ok)
		}
	}
}

func lateEdge(t *testing.T, st commit.Store, after time.Duration) *Edge {
	t.Helper()
	var n atomic.Int64
	e, err := New(Config{Store: st, Prefix: "root", Cluster: "c1", ProducerID: "p1", LateSplitAfter: after,
		PutTimeout: time.Second, HeadTimeout: time.Second,
		NewEpoch: func() string { return fmt.Sprintf("20260926T000000.000Z-%08x", n.Add(1)) },
		Now:      func() time.Time { return time.Unix(0, int64(lateT)) }})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// tracesAt is one span per start time, two resources alternating.
func tracesAt(ts ...uint64) ptrace.Traces {
	td := ptrace.NewTraces()
	var sss [2]ptrace.SpanSlice
	for r := range sss {
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", "svc-"+strconv.Itoa(r))
		sss[r] = rs.ScopeSpans().AppendEmpty().Spans()
	}
	for i, x := range ts {
		s := sss[i%2].AppendEmpty()
		s.SetName("op-" + strconv.Itoa(i))
		s.SetTraceID(pcommon.TraceID{byte(i), byte(i >> 8), 7})
		s.SetStartTimestamp(pcommon.Timestamp(x))
		s.SetEndTimestamp(pcommon.Timestamp(x + 1000))
	}
	return td
}

type latePart struct {
	key  string
	meta map[string]string
	ts   []int64
	ords []uint32
}

func laneParts(t *testing.T, st *commit.MemStore, ns string) []latePart {
	t.Helper()
	var out []latePart
	for _, k := range st.Keys("root/c1/p1/" + ns + "/") {
		o, _ := st.Get(k)
		rows, err := parquet.Read[struct {
			Ord uint32 `parquet:"row_ordinal"`
		}](bytes.NewReader(o.Body), int64(len(o.Body)))
		if err != nil {
			t.Fatal(err)
		}
		p := latePart{key: k, meta: o.Meta}
		for _, r := range rows {
			p.ords = append(p.ords, r.Ord)
		}
		if p.ts, err = timestamps(o.Body); err != nil {
			t.Fatal(err)
		}
		f := footer(t, o.Body)
		for _, mk := range []string{commit.MetaPart, commit.MetaLateAfter, commit.MetaMinTime, commit.MetaMaxTime, commit.MetaRows, commit.MetaContent} {
			if f[mk] != o.Meta[mk] {
				t.Fatalf("%s: footer %s = %q, metadata %q", k, mk, f[mk], o.Meta[mk])
			}
		}
		out = append(out, p)
	}
	return out
}

func byPart(t *testing.T, ps []latePart) (bulk, late latePart) {
	t.Helper()
	if len(ps) != 2 {
		t.Fatalf("%d objects, want 2", len(ps))
	}
	for _, p := range ps {
		switch p.meta[commit.MetaPart] {
		case commit.PartBulk:
			bulk = p
		case commit.PartLate:
			late = p
		default:
			t.Fatalf("%s: oscope-part %q", p.key, p.meta[commit.MetaPart])
		}
	}
	if bulk.key == "" || late.key == "" {
		t.Fatal("want one bulk and one late object")
	}
	return bulk, late
}

// A request with rows older than the bound: two objects in the lane, each
// with its own honest range and content key, row_ordinal 0..n-1 in each,
// received_at unchanged; a retry sends nothing.
func TestLateSplitTraces(t *testing.T) {
	B := 15 * time.Minute
	st := commit.NewMemStore()
	e := lateEdge(t, st, B)
	hr := uint64(time.Hour)
	ts := []uint64{lateT, lateT - 1000, lateT - 24*hr, lateT - uint64(B), lateT - 5, lateT - 24*hr + 7, lateT - 2*hr}
	td := tracesAt(ts...)
	b, _ := (&ptrace.ProtoMarshaler{}).MarshalTraces(td)
	if err := e.PushTraces(context.Background(), td); err != nil {
		t.Fatal(err)
	}
	bulk, late := byPart(t, laneParts(t, st, "traces"))
	if bulk.meta[commit.MetaContent] != SplitContent("traces", commit.PartBulk, B, b) ||
		late.meta[commit.MetaContent] != SplitContent("traces", commit.PartLate, B, b) ||
		bulk.meta[commit.MetaContent] == commit.ContentHash("traces", b) || bulk.meta[commit.MetaContent] == late.meta[commit.MetaContent] {
		t.Fatal("content keys", bulk.meta[commit.MetaContent], late.meta[commit.MetaContent])
	}
	for _, p := range []latePart{bulk, late} {
		if p.meta[commit.MetaLateAfter] != strconv.FormatInt(int64(B), 10) || p.meta[commit.MetaReceived] != strconv.FormatUint(lateT, 10) {
			t.Fatal(p.meta)
		}
		if p.meta[commit.MetaRows] != strconv.Itoa(len(p.ts)) || p.meta[commit.MetaMinTime] != strconv.FormatInt(p.ts[0], 10) ||
			p.meta[commit.MetaMaxTime] != strconv.FormatInt(p.ts[len(p.ts)-1], 10) {
			t.Fatalf("%s: meta %v, rows %v", p.meta[commit.MetaPart], p.meta, p.ts)
		}
		ords := slices.Clone(p.ords)
		slices.Sort(ords)
		for i, o := range ords {
			if o != uint32(i) {
				t.Fatalf("row ordinals %v", p.ords)
			}
		}
	}
	if fmt.Sprint(bulk.ts) != fmt.Sprint([]int64{int64(lateT - uint64(B)), int64(lateT - 1000), int64(lateT - 5), int64(lateT)}) {
		t.Fatal("bulk rows", bulk.ts)
	}
	if fmt.Sprint(late.ts) != fmt.Sprint([]int64{int64(lateT - 24*hr), int64(lateT - 24*hr + 7), int64(lateT - 2*hr)}) {
		t.Fatal("late rows", late.ts)
	}
	puts := st.Puts
	if err := e.PushTraces(context.Background(), td); err != nil || st.Puts != puts {
		t.Fatal("retry", err, st.Puts, puts)
	}
	// The next, unsplit, request is encoded by the same (reused) Parquet
	// writer: its footer must not keep the split's keys (parquet-go keeps a
	// writer's key-value metadata across Reset; found by the conformance run).
	if err := e.PushTraces(context.Background(), tracesAt(lateT, lateT-1)); err != nil {
		t.Fatal(err)
	}
	for _, p := range laneParts(t, st, "traces") { // laneParts checks footer = metadata
		if p.meta[commit.MetaPart] == "" && len(p.ts) != 2 {
			t.Fatalf("unsplit object %v", p.ts)
		}
	}
	for _, k := range st.Keys("root/c1/p1/traces/") {
		o, _ := st.Get(k)
		f := footer(t, o.Body)
		if _, ok := f[commit.MetaPart]; ok != (o.Meta[commit.MetaPart] != "") {
			t.Fatalf("%s: footer %v, metadata %v", k, f, o.Meta)
		}
	}
}

// Logs: a record's observed time stands in for a zero time, and the row
// exactly at the bound stays whole; an all-late request (every row far
// behind received_at, but close together) is one ordinary object; with the
// split off nothing splits.
func TestLateSplitBoundaryLogs(t *testing.T) {
	B := time.Minute
	logsAt := func(ts ...uint64) plog.Logs {
		ld := plog.NewLogs()
		recs := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
		for i, x := range ts {
			r := recs.AppendEmpty()
			r.Body().SetStr("m" + strconv.Itoa(i))
			if i == 0 {
				r.SetObservedTimestamp(pcommon.Timestamp(x)) // time 0: the observed time is its time
			} else {
				r.SetTimestamp(pcommon.Timestamp(x))
			}
		}
		return ld
	}
	for _, c := range []struct {
		name  string
		after time.Duration
		ts    []uint64
		split bool
	}{
		{"at the bound", B, []uint64{lateT - uint64(B), lateT}, false},
		{"one ns past it", B, []uint64{lateT - uint64(B) - 1, lateT}, true},
		{"observed time decides", B, []uint64{lateT - 3*uint64(B), lateT}, true},
		{"all late, close together", B, []uint64{lateT - 48*uint64(time.Hour), lateT - 48*uint64(time.Hour) + 30}, false},
		{"off", 0, []uint64{lateT - 48*uint64(time.Hour), lateT}, false},
	} {
		st := commit.NewMemStore()
		e := lateEdge(t, st, c.after)
		ld := logsAt(c.ts...)
		b, _ := (&plog.ProtoMarshaler{}).MarshalLogs(ld)
		if err := e.PushLogs(context.Background(), ld); err != nil {
			t.Fatal(c.name, err)
		}
		ps := laneParts(t, st, "logs")
		if !c.split {
			if len(ps) != 1 || ps[0].meta[commit.MetaContent] != commit.ContentHash("logs", b) || ps[0].meta[commit.MetaPart] != "" || ps[0].meta[commit.MetaLateAfter] != "" {
				t.Fatalf("%s: %+v", c.name, ps)
			}
			continue
		}
		bulk, late := byPart(t, ps)
		if len(bulk.ts) != 1 || len(late.ts) != 1 || uint64(late.ts[0]) != c.ts[0] {
			t.Fatalf("%s: bulk %v late %v", c.name, bulk.ts, late.ts)
		}
	}
}

// Faults: one part's PUT lost or its answer lost; the request is retryable
// (not permanent) or resolved, and after retries the lane holds exactly the
// two parts, whose rows are the request's, once.
func TestLateSplitFaults(t *testing.T) {
	for _, f := range []commit.Fault{commit.Drop, commit.ApplyLoseAnswer} {
		st := commit.NewMemStore()
		e := lateEdge(t, st, time.Minute)
		ts := []uint64{lateT, lateT - uint64(time.Hour), lateT - 3, lateT - uint64(2*time.Hour)}
		td := tracesAt(ts...)
		st.Inject(f)
		var err error
		for range 3 {
			if err = e.PushTraces(context.Background(), td); err == nil {
				break
			}
			if IsPermanent(err) {
				t.Fatal("permanent", err)
			}
		}
		if err != nil {
			t.Fatal(f, err)
		}
		bulk, late := byPart(t, laneParts(t, st, "traces"))
		if len(bulk.ts)+len(late.ts) != len(ts) {
			t.Fatalf("fault %v: rows %v + %v", f, bulk.ts, late.ts)
		}
	}
}

// Property (seeded): for random event times and bounds, the parts hold the
// request's rows exactly once; the split happens iff a row is more than the
// bound older than the newest; the bulk's range is at most the bound wide
// and every late row is below it.
func TestLateSplitProperty(t *testing.T) {
	r := rand.New(rand.NewPCG(31, 31))
	for i := range 300 {
		B := time.Duration(1+r.IntN(30)) * time.Minute
		n := 1 + r.IntN(40)
		var ts []uint64
		for range n {
			switch r.IntN(4) {
			case 0: // far behind
				ts = append(ts, lateT-uint64(r.Int64N(int64(48*time.Hour))))
			case 1: // near the bound
				ts = append(ts, lateT-uint64(B)+uint64(r.Int64N(3))-1)
			default:
				ts = append(ts, lateT-uint64(r.Int64N(int64(10*time.Second))))
			}
		}
		st := commit.NewMemStore()
		e := lateEdge(t, st, B)
		if err := e.PushTraces(context.Background(), tracesAt(ts...)); err != nil {
			t.Fatal(err)
		}
		hi := slices.Max(ts)
		want := false
		for _, x := range ts {
			want = want || x < hi-uint64(B)
		}
		ps := laneParts(t, st, "traces")
		var all []int64
		for _, p := range ps {
			all = append(all, p.ts...)
		}
		slices.Sort(all)
		exp := make([]int64, len(ts))
		for j, x := range ts {
			exp[j] = int64(x)
		}
		slices.Sort(exp)
		if !slices.Equal(all, exp) {
			t.Fatalf("case %d: rows %v, want %v", i, all, exp)
		}
		if !want {
			if len(ps) != 1 {
				t.Fatalf("case %d: %d objects for an unsplit request", i, len(ps))
			}
			continue
		}
		bulk, late := byPart(t, ps)
		cut := int64(hi - uint64(B))
		if bulk.ts[0] < cut || bulk.ts[len(bulk.ts)-1]-bulk.ts[0] > int64(B) || late.ts[len(late.ts)-1] >= cut {
			t.Fatalf("case %d: cut %d bulk %v late %v", i, cut, bulk.ts, late.ts)
		}
	}
}
