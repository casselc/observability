package lake

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/query/internal/auth"
	"github.com/casselc/observability/otel-chdb/query/internal/completeness"
	"github.com/casselc/observability/otel-chdb/query/internal/lakeidx"
	"github.com/parquet-go/parquet-go"
	"pgregory.net/rapid"
)

type row struct {
	Timestamp int64  `parquet:"Timestamp"`
	TraceId   string `parquet:"TraceId"`
	Body      string `parquet:"Body"`
}

var (
	base  = time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	fleet = &auth.Principal{Subject: "sre", AllClusters: true, AllNamespaces: true, Roles: []string{"plan"}}
)

const ep = "20260928T120000.000Z-0a1b2c3d"

func pq(t interface{ Fatalf(string, ...any) }, rgs [][]row) []byte {
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[row](&buf)
	for _, rs := range rgs {
		if _, err := w.Write(rs); err != nil {
			t.Fatalf("%v", err)
		}
		if err := w.Flush(); err != nil {
			t.Fatalf("%v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("%v", err)
	}
	return buf.Bytes()
}

type fixture struct {
	m    *lakeidx.Mem
	seq  int
	keys []string
	rgs  map[string][][]row
}

func newFix() *fixture { return &fixture{m: lakeidx.NewMem(), rgs: map[string][][]row{}} }

// add publishes a logs object of cluster c1 with rows in event time base+5..10 min.
func (f *fixture) add(t interface{ Fatalf(string, ...any) }, signal string, rgs [][]row) string {
	key := fmt.Sprintf("lake/c1/pub-0/%s/%s/%020d.parquet", signal, ep, f.seq)
	f.seq++
	meta := map[string]string{"oscope-kind": "data", "oscope-cluster": "c1", "oscope-rows": "1",
		"oscope-min-time": fmt.Sprint(base.Add(5 * time.Minute).UnixNano()), "oscope-max-time": fmt.Sprint(base.Add(10 * time.Minute).UnixNano())}
	f.m.Put(key, pq(t, rgs), meta, base.Add(time.Duration(10+f.seq)*time.Minute))
	f.keys = append(f.keys, key)
	f.rgs[key] = rgs
	return key
}

func (f *fixture) index(t interface{ Fatalf(string, ...any) }, signals ...string) {
	ix := lakeidx.New(lakeidx.Config{Root: "lake", Signals: signals}, f.m)
	ix.Now = func() time.Time { return base.Add(time.Hour) }
	if _, err := ix.RunOnce(context.Background()); err != nil {
		t.Fatalf("index: %v", err)
	}
}

func (f *fixture) planner(cfg Config) *Planner {
	cfg.Root = "lake"
	get := func(ctx context.Context, k string) ([]byte, error) { b, _, err := f.m.Get(ctx, k); return b, err }
	wm := completeness.NewReader(get, "lake/_consumer/watermark.json", time.Second, time.Hour)
	p := New(cfg, f.m, wm)
	p.SetClock(func() time.Time { return base.Add(time.Hour) })
	return p
}

func (f *fixture) plan(t *testing.T, p *Planner, signal string, trace string, terms ...string) *Plan {
	t.Helper()
	pl, err := p.Plan(context.Background(), fleet, Request{Signal: signal, FromNs: base.UnixNano(), ToNs: base.Add(time.Hour).UnixNano(),
		TraceID: trace, Terms: terms})
	if err != nil {
		t.Fatal(err)
	}
	return pl
}

func byKey(p *Plan) map[string]Object {
	out := map[string]Object{}
	for _, o := range p.Objects {
		out[o.Key] = o
	}
	return out
}

var (
	idA = strings.Repeat("a1", 16)
	idB = strings.Repeat("b2", 16)
)

func TestPlanTraceFilter(t *testing.T) {
	f := newFix()
	k0 := f.add(t, "traces", [][]row{{{TraceId: idA}}, {{TraceId: idB}}})
	k1 := f.add(t, "traces", [][]row{{{TraceId: idB}}})
	f.index(t, "traces")
	k2 := f.add(t, "traces", [][]row{{{TraceId: idB}}, {{TraceId: idA}}}) // after the indexer
	p := f.planner(Config{})

	all := f.plan(t, p, "traces", "")
	if len(all.Objects) != 3 || all.Index != nil || all.Objects[0].Index != "" {
		t.Fatalf("unfiltered: %+v", all)
	}
	pl := f.plan(t, p, "traces", strings.ToUpper(idA))
	got := byKey(pl)
	if len(pl.Objects) != 2 || got[k0].Index != "hit" || fmt.Sprint(got[k0].RowGroups) != "[0]" || got[k2].Index != "scan" || got[k2].RowGroups != nil {
		t.Fatalf("filtered: %+v", pl.Objects)
	}
	if _, ok := got[k1]; ok {
		t.Fatal("an object the index rules out was planned")
	}
	ix := pl.Index
	if ix == nil || ix.TraceID != idA || ix.Covered != 2 || ix.Pruned != 1 || ix.Scan != 1 || ix.Segments != 1 || len(ix.Errors) != 0 || ix.Rule == "" {
		t.Fatalf("report %+v", ix)
	}
	if pl.TotalBytes != got[k0].Size+got[k2].Size {
		t.Fatalf("total bytes %d", pl.TotalBytes)
	}
	// a second plan reads no index bytes: headers and the block are cached
	pl2 := f.plan(t, p, "traces", idA)
	if pl2.Index.Bytes != 0 || pl2.Index.CacheHits == 0 {
		t.Fatalf("second plan %+v", pl2.Index)
	}
}

func TestPlanTermsFilter(t *testing.T) {
	f := newFix()
	k0 := f.add(t, "logs", [][]row{{{Body: "GET /api ok"}}, {{Body: "db Timeout acct-7731"}}})
	k1 := f.add(t, "logs", [][]row{{{Body: "all quiet", TraceId: idA}}})
	f.index(t, "logs")
	p := f.planner(Config{})
	pl := f.plan(t, p, "logs", "", "timeout ACCT")
	got := byKey(pl)
	if len(pl.Objects) != 1 || got[k0].Index != "hit" || fmt.Sprint(got[k0].RowGroups) != "[1]" {
		t.Fatalf("%+v", pl.Objects)
	}
	if fmt.Sprint(pl.Index.Constraints) != "[suffix:timeout prefix:acct]" {
		t.Fatalf("%v", pl.Index.Constraints)
	}
	// logs by trace id (the trace section of a logs segment)
	pl = f.plan(t, p, "logs", idA)
	if got := byKey(pl); len(pl.Objects) != 1 || got[k1].Index != "hit" {
		t.Fatalf("%+v", pl.Objects)
	}
	// a text of separators constrains nothing: everything scans
	pl = f.plan(t, p, "logs", "", " - ")
	if len(pl.Objects) != 2 || pl.Index.Scan != 2 || pl.Objects[0].Index != "scan" {
		t.Fatalf("%+v %+v", pl.Objects, pl.Index)
	}
}

func TestPlanFilterRefusals(t *testing.T) {
	f := newFix()
	p := f.planner(Config{})
	for _, c := range []Request{
		{Signal: "traces", TraceID: "xyz"},
		{Signal: "traces", Terms: []string{"a"}},
		{Signal: "metrics_gauge", TraceID: idA},
		{Signal: "logs", Terms: []string{""}},
		{Signal: "logs", Terms: strings.Split("a b c d e f g h i", " ")},
		{Signal: "logs", Terms: []string{strings.Repeat("x", 257)}},
	} {
		c.FromNs, c.ToNs = base.UnixNano(), base.Add(time.Hour).UnixNano()
		var br *BadRequest
		if _, err := p.Plan(context.Background(), fleet, c); !errors.As(err, &br) {
			t.Errorf("%+v: %v", c, err)
		}
	}
}

// A corrupt segment and an unreadable one: their objects scan, the errors
// are in the report, nothing is pruned.
func TestPlanIndexFailuresScan(t *testing.T) {
	f := newFix()
	f.add(t, "traces", [][]row{{{TraceId: idB}}})
	f.add(t, "traces", [][]row{{{TraceId: idB}}})
	f.index(t, "traces")
	segs := f.m.Keys("lake/c1/_index/")
	var seg string
	for _, k := range segs {
		if strings.HasSuffix(k, ".osix") {
			seg = k
		}
	}
	good := f.m.Body(seg)
	bad := bytes.Clone(good)
	bad[10] ^= 1 // inside the first trace block
	f.m.Put(seg, bad, nil, base.Add(20*time.Minute))
	pl := f.plan(t, f.planner(Config{}), "traces", idA)
	if len(pl.Objects) != 2 || pl.Index.Scan != 2 || pl.Index.Pruned != 0 || len(pl.Index.Errors) != 1 {
		t.Fatalf("corrupt: %+v %+v", pl.Objects, pl.Index)
	}
	f.m.Put(seg, good, nil, base.Add(20*time.Minute))
	f.m.FailGet = func(k string) bool { return strings.HasSuffix(k, ".osix") }
	pl = f.plan(t, f.planner(Config{}), "traces", idA)
	if len(pl.Objects) != 2 || pl.Index.Scan != 2 || len(pl.Index.Errors) != 1 {
		t.Fatalf("unreadable: %+v", pl.Index)
	}
	f.m.FailGet = nil
	pl = f.plan(t, f.planner(Config{Index: IndexConfig{Disabled: true}}), "traces", idA)
	if len(pl.Objects) != 2 || pl.Index.Scan != 2 || pl.Index.Note == "" {
		t.Fatalf("disabled: %+v", pl.Index)
	}
	pl = f.plan(t, f.planner(Config{Index: IndexConfig{MaxMBPerPlan: 0}}), "traces", idA)
	if len(pl.Objects) != 0 || pl.Index.Pruned != 2 {
		t.Fatalf("healthy: %+v", pl.Index)
	}
}

// Property: a filtered plan lists every object with a true match, and a
// "hit" object's row groups include every row group with one.
func TestPlanFilterIsASuperset(t *testing.T) {
	words := []string{"timeout", "acct-7731", "GET", "error", "db", "İd", "x", "user_42"}
	ids := []string{idA, idB, strings.Repeat("c3", 16)}
	rapid.Check(t, func(rt *rapid.T) {
		f := newFix()
		n := rapid.IntRange(1, 6).Draw(rt, "objects")
		split := rapid.IntRange(0, n).Draw(rt, "indexed")
		for i := 0; i < n; i++ {
			var rgs [][]row
			for g := rapid.IntRange(1, 3).Draw(rt, "rgs"); g > 0; g-- {
				var rs []row
				for r := rapid.IntRange(1, 4).Draw(rt, "rows"); r > 0; r-- {
					ws := rapid.SliceOfN(rapid.SampledFrom(words), 0, 4).Draw(rt, "w")
					rs = append(rs, row{Body: strings.Join(ws, " "), TraceId: rapid.SampledFrom(ids).Draw(rt, "id")})
				}
				rgs = append(rgs, rs)
			}
			f.add(rt, "logs", rgs)
			if i+1 == split {
				f.index(rt, "logs")
			}
		}
		var trace string
		var terms []string
		if rapid.Bool().Draw(rt, "bytrace") {
			trace = rapid.SampledFrom(ids).Draw(rt, "qid")
		} else {
			w := rapid.SampledFrom(words).Draw(rt, "qw")
			i := rapid.IntRange(0, len(w)-1).Draw(rt, "qi")
			if !strings.HasPrefix(w, "İ") {
				w = w[i:]
			}
			terms = []string{strings.ToUpper(w)}
		}
		pl, err := f.planner(Config{}).Plan(context.Background(), fleet, Request{Signal: "logs", FromNs: base.UnixNano(),
			ToNs: base.Add(time.Hour).UnixNano(), TraceID: trace, Terms: terms})
		if err != nil {
			rt.Fatalf("%v", err)
		}
		got := byKey(pl)
		for _, k := range f.keys {
			for g, rs := range f.rgs[k] {
				match := false
				for _, r := range rs {
					if trace != "" && r.TraceId == trace {
						match = true
					}
					if len(terms) > 0 && strings.Contains(strings.ToLower(strings.ReplaceAll(r.Body, "İ", "i̇")), strings.ToLower(strings.ReplaceAll(terms[0], "İ", "i̇"))) {
						match = true
					}
				}
				if !match {
					continue
				}
				o, ok := got[k]
				if !ok {
					rt.Fatalf("%s row group %d matches and was not planned (%+v)", k, g, pl.Index)
				}
				if o.Index == "hit" && !slices.Contains(o.RowGroups, g) {
					rt.Fatalf("%s row group %d matches, planned %v", k, g, o.RowGroups)
				}
			}
		}
	})
}
