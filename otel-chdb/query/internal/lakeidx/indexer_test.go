package lakeidx

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/query/internal/store"
	"github.com/parquet-go/parquet-go"
	"pgregory.net/rapid"
)

type pqRow struct {
	Timestamp int64  `parquet:"Timestamp"`
	TraceId   string `parquet:"TraceId"`
	Body      string `parquet:"Body"`
}

func writeParquet(t interface{ Fatalf(string, ...any) }, rgs [][]genRow) []byte {
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[pqRow](&buf)
	for _, rows := range rgs {
		var rs []pqRow
		for i, r := range rows {
			rs = append(rs, pqRow{Timestamp: int64(i), TraceId: r.trace, Body: r.body})
		}
		if len(rs) > 0 {
			if _, err := w.Write(rs); err != nil {
				t.Fatalf("write: %v", err)
			}
		}
		if err := w.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return buf.Bytes()
}

const epochA = "20260928T120000.000Z-0a1b2c3d"

var t0 = time.Date(2026, 9, 28, 12, 10, 0, 0, time.UTC)

type lake struct {
	m    *Mem
	objs []genObject
	seq  map[string]int
	at   time.Time
}

func newLake() *lake { return &lake{m: NewMem(), seq: map[string]int{}, at: t0} }

// add publishes one data object (or a heartbeat when rgs is nil) in producer
// prod's logs lane, dt after the previous one.
func (l *lake) add(t interface{ Fatalf(string, ...any) }, prod string, rgs [][]genRow, dt time.Duration) {
	l.at = l.at.Add(dt)
	key := fmt.Sprintf("lake/c1/%s/logs/%s/%020d.parquet", prod, epochA, l.seq[prod])
	l.seq[prod]++
	if rgs == nil {
		l.m.Put(key, nil, nil, l.at)
		return
	}
	// the parquet file's row groups: a row group with no rows is not written
	var kept [][]genRow
	for _, r := range rgs {
		if len(r) > 0 {
			kept = append(kept, r)
		}
	}
	if len(kept) == 0 {
		kept = [][]genRow{{{trace: "", body: ""}}}
	}
	body := writeParquet(t, kept)
	l.m.Put(key, body, nil, l.at)
	l.objs = append(l.objs, genObject{key: key, rgs: kept, size: int64(len(body))})
}

func (l *lake) listed() []store.Object {
	objs, _, _ := l.m.List(context.Background(), "lake/c1/", 1_000_000)
	var out []store.Object
	for _, o := range objs {
		if o.Size > 0 && strings.HasSuffix(o.Key, ".parquet") {
			out = append(out, o)
		}
	}
	return out
}

func (l *lake) resolve(tr string, terms []string) (map[string]ObjResult, Report) {
	r := NewResolver(ResolverConfig{Root: "lake"}, l.m)
	return r.Resolve(context.Background(), "c1", "logs", l.listed(), Filter{TraceID: tr, Terms: terms})
}

func genLake(t *rapid.T) (*lake, []string) {
	traces := genTraces(t)
	l := newLake()
	n := rapid.IntRange(1, 10).Draw(t, "objects")
	for i := 0; i < n; i++ {
		prod := rapid.SampledFrom([]string{"pub-0", "pub-1"}).Draw(t, "prod")
		dt := time.Duration(rapid.IntRange(0, 40).Draw(t, "dtmin")) * time.Minute
		if rapid.IntRange(0, 5).Draw(t, "beat") == 0 {
			l.add(t, prod, nil, dt)
			continue
		}
		var rgs [][]genRow
		for g := rapid.IntRange(1, 3).Draw(t, "rgs"); g > 0; g-- {
			rgs = append(rgs, genRows(t, traces))
		}
		l.add(t, prod, rgs, dt)
	}
	return l, traces
}

func mustPass(t interface{ Fatalf(string, ...any) }, ix *Indexer) {
	if _, err := ix.RunOnce(context.Background()); err != nil {
		t.Fatalf("pass: %v", err)
	}
}

// Property: after a pass every data object is covered, and resolution is a
// superset of brute force over the real Parquet objects.
func TestIndexerPassThenResolveIsASuperset(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		l, traces := genLake(t)
		ix := New(Config{Root: "lake", Signals: []string{"logs"}, MaxSegmentObjects: rapid.IntRange(1, 4).Draw(t, "segobjs"),
			Build: smallCfg(t)}, l.m)
		ix.Now = func() time.Time { return l.at }
		mustPass(t, ix)
		tr, terms := queryFor(t, traces)
		got, rep := l.resolve(tr, terms)
		if len(rep.Errors) > 0 {
			t.Fatalf("errors: %v", rep.Errors)
		}
		if len(Filter{TraceID: tr, Terms: terms}.constraints()) > 0 || tr != "" {
			if rep.Scan != 0 {
				t.Fatalf("after a pass %d objects scan: %+v", rep.Scan, rep)
			}
		}
		checkSuperset(t, l.objs, got, truth(l.objs, tr, terms))
	})
}

// Objects published after a pass are "scan" until the next pass covers
// them: an index never hides a new object.
func TestUnindexedObjectsScan(t *testing.T) {
	l := newLake()
	l.add(t, "pub-0", [][]genRow{{{trace: strings.Repeat("a", 32), body: "first timeout"}}}, 0)
	ix := New(Config{Root: "lake", Signals: []string{"logs"}}, l.m)
	ix.Now = func() time.Time { return l.at }
	mustPass(t, ix)
	l.add(t, "pub-0", [][]genRow{{{trace: strings.Repeat("a", 32), body: "second timeout"}}}, time.Minute)
	got, rep := l.resolve(strings.Repeat("a", 32), nil)
	if got[l.objs[0].key].Status != Hit || got[l.objs[1].key].Status != Scan || rep.Scan != 1 || rep.Covered != 1 {
		t.Fatalf("got %+v %+v", got, rep)
	}
	got, _ = l.resolve("", []string{"nowhere"})
	if got[l.objs[0].key].Status != None || got[l.objs[1].key].Status != Scan {
		t.Fatalf("got %+v", got)
	}
	mustPass(t, ix)
	got, rep = l.resolve(strings.Repeat("a", 32), nil)
	if got[l.objs[1].key].Status != Hit || rep.Scan != 0 {
		t.Fatalf("after the next pass: %+v %+v", got, rep)
	}
}

// Crash / restart: any PUT may be lost, or applied with its answer lost;
// passes are retried until one succeeds. The end state covers every object
// and answers a superset; no segment is written twice under two keys for
// the same bytes.
func TestIndexerCrashAndRestart(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		l, traces := genLake(t)
		seed := rapid.Uint64().Draw(t, "faults")
		var calls uint64
		faulty := func(string) bool {
			calls++
			return (seed>>(calls%64))&1 == 1 && calls < 40
		}
		l.m.LosePut = faulty
		l.m.LoseAnswer = func(k string) bool { return faulty(k) }
		cfg := Config{Root: "lake", Signals: []string{"logs"}, MaxSegmentObjects: 2, Build: smallCfg(t)}
		var err error
		for i := 0; i < 60; i++ {
			ix := New(cfg, l.m) // a fresh process each time: nothing survives but the bucket
			ix.Now = func() time.Time { return l.at }
			if _, err = ix.RunOnce(context.Background()); err == nil {
				break
			}
		}
		if err != nil {
			t.Fatalf("never converged: %v", err)
		}
		l.m.LosePut, l.m.LoseAnswer = nil, nil
		tr, terms := queryFor(t, traces)
		got, rep := l.resolve(tr, terms)
		if len(rep.Errors) > 0 || (tr != "" && rep.Scan != 0) {
			t.Fatalf("after recovery: %+v", rep)
		}
		checkSuperset(t, l.objs, got, truth(l.objs, tr, terms))
		// the progress document parses and passes nothing it should not
		body := l.m.Body(ProgressKey("lake", "c1", "logs"))
		var p Progress
		if err := json.Unmarshal(body, &p); err != nil {
			t.Fatalf("progress: %v %q", err, body)
		}
	})
}

// Concurrent indexers on one cluster, with objects arriving between passes:
// every object ends covered, resolution stays a superset, and the progress
// CAS merges rather than regresses.
func TestConcurrentIndexers(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		l, traces := genLake(t)
		var mu sync.Mutex
		now := func() time.Time { mu.Lock(); defer mu.Unlock(); return l.at }
		cfg := Config{Root: "lake", Signals: []string{"logs"}, MaxSegmentObjects: rapid.IntRange(1, 3).Draw(t, "segobjs"), Build: smallCfg(t)}
		a, b := New(cfg, l.m), New(cfg, l.m)
		a.Now, b.Now = now, now
		more := rapid.IntRange(0, 3).Draw(t, "more")
		var extra [][][]genRow
		for i := 0; i < more; i++ {
			extra = append(extra, [][]genRow{genRows(t, traces)})
		}
		for round := 0; round <= more; round++ {
			var wg sync.WaitGroup
			errs := make([]error, 2)
			for i, ix := range []*Indexer{a, b} {
				wg.Add(1)
				go func(i int, ix *Indexer) {
					defer wg.Done()
					_, errs[i] = ix.RunOnce(context.Background())
				}(i, ix)
			}
			if round < more {
				mu.Lock()
				l.add(t, "pub-1", extra[round], time.Minute)
				mu.Unlock()
			}
			wg.Wait()
			for _, err := range errs {
				if err != nil && !strings.Contains(err.Error(), "CAS lost") {
					t.Fatalf("round %d: %v", round, err)
				}
			}
		}
		mustPass(t, a)
		tr, terms := queryFor(t, traces)
		got, rep := l.resolve(tr, terms)
		if len(rep.Errors) > 0 || (tr != "" && rep.Scan != 0) {
			t.Fatalf("%+v", rep)
		}
		checkSuperset(t, l.objs, got, truth(l.objs, tr, terms))
	})
}

// A finished hour's segments merge into one L1 that answers alone.
func TestMergeHour(t *testing.T) {
	l := newLake()
	id := strings.Repeat("b", 32)
	ix := New(Config{Root: "lake", Signals: []string{"logs"}, MergeAfterS: 60}, l.m)
	ix.Now = func() time.Time { return l.at }
	for i := 0; i < 5; i++ {
		l.add(t, fmt.Sprintf("pub-%d", i%2), [][]genRow{{{trace: id, body: fmt.Sprintf("row %d timeout", i)}}, {{body: "other"}}}, 5*time.Minute)
		mustPass(t, ix) // one L0 per pass
	}
	segs := l.m.Keys("lake/c1/_index/v1/logs/20260928T12/")
	if len(segs) != 5 {
		t.Fatalf("L0 segments: %v", segs)
	}
	l.at = time.Date(2026, 9, 28, 13, 5, 0, 0, time.UTC)
	mustPass(t, ix)
	segs = l.m.Keys("lake/c1/_index/v1/logs/20260928T12/")
	var l1 []string
	for _, k := range segs {
		if strings.Contains(k, "/L1-") {
			l1 = append(l1, k)
		}
	}
	if len(l1) != 1 {
		t.Fatalf("L1: %v", segs)
	}
	got, rep := l.resolve(id, []string{" timeout"})
	if rep.Segments != 1 || rep.Scan != 0 {
		t.Fatalf("resolution used %d segments: %+v", rep.Segments, rep)
	}
	for _, o := range l.objs {
		if r := got[o.key]; r.Status != Hit || len(r.RowGroups) != 1 || r.RowGroups[0] != 0 {
			t.Fatalf("%s: %+v", o.key, r)
		}
	}
	mustPass(t, ix) // nothing new: no second L1
	if n := len(l.m.Keys("lake/c1/_index/v1/logs/20260928T12/")); n != 6 {
		t.Fatalf("%d segments after an idle pass", n)
	}
}

// An object that is not Parquet is recorded unindexable, passed, and scanned.
func TestUnindexableObjectIsScanned(t *testing.T) {
	l := newLake()
	l.add(t, "pub-0", [][]genRow{{{body: "fine"}}}, 0)
	key := fmt.Sprintf("lake/c1/pub-0/logs/%s/%020d.parquet", epochA, 1)
	l.m.Put(key, []byte("not parquet at all"), nil, l.at)
	ix := New(Config{Root: "lake", Signals: []string{"logs"}}, l.m)
	ix.Now = func() time.Time { return l.at }
	reps, err := ix.RunOnce(context.Background())
	if err != nil || len(reps[0].Unindexable) != 1 {
		t.Fatalf("%v %+v", err, reps)
	}
	got, _ := l.resolve("", []string{"fine"})
	if got[key].Status != Scan || got[l.objs[0].key].Status != Hit {
		t.Fatalf("%+v", got)
	}
	var p Progress
	_ = json.Unmarshal(l.m.Body(ProgressKey("lake", "c1", "logs")), &p)
	if p.Lanes["pub-0"][epochA] != 2 || len(p.Unindexable) != 1 {
		t.Fatalf("progress %+v", p)
	}
}

// A corrupted segment (storage damage under its content-addressed key) is
// refused by readers; the indexer rebuilds beside it.
func TestCorruptSegmentIsRebuilt(t *testing.T) {
	l := newLake()
	id := strings.Repeat("c", 32)
	l.add(t, "pub-0", [][]genRow{{{trace: id, body: "x"}}}, 0)
	ix := New(Config{Root: "lake", Signals: []string{"logs"}}, l.m)
	ix.Now = func() time.Time { return l.at }
	mustPass(t, ix)
	segs := l.m.Keys("lake/c1/_index/v1/logs/")
	b := bytes.Clone(l.m.Body(segs[0]))
	b[len(b)-20] ^= 0xff // inside the header
	l.m.Put(segs[0], b, nil, l.at)
	got, rep := l.resolve(id, nil)
	if got[l.objs[0].key].Status != Scan || len(rep.Errors) != 1 {
		t.Fatalf("%+v %+v", got, rep)
	}
	// a restarted indexer with lost progress rebuilds
	l.m.Delete(ProgressKey("lake", "c1", "logs"))
	ix2 := New(Config{Root: "lake", Signals: []string{"logs"}}, l.m)
	ix2.Now = ix.Now
	mustPass(t, ix2)
	got, rep = l.resolve(id, nil)
	if got[l.objs[0].key].Status != Hit {
		t.Fatalf("after rebuild: %+v %+v %v", got, rep, l.m.Keys("lake/c1/_index/"))
	}
}
