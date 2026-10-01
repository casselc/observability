package lakeidx

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/casselc/observability/otel-chdb/testgate/tracetag"
)

// dayLake publishes objects over up to three days (dt up to 7 h apart),
// indexes them as they arrive, then lets every day end and the indexer
// build the day level. draw(n) is in [0, n).
func dayLake(t interface{ Fatalf(string, ...any) }, draw func(int) int, ix func(*lake) *Indexer) (*lake, []string) {
	traces := make([]string, 1+draw(6))
	for i := range traces {
		traces[i] = fmt.Sprintf("%016x%016x", rand.New(rand.NewSource(int64(draw(1<<30)))).Uint64(), uint64(i))
	}
	l := newLake()
	x := ix(l)
	n := 1 + draw(14)
	for i := 0; i < n; i++ {
		prod := []string{"pub-0", "pub-1"}[draw(2)]
		dt := time.Duration(draw(7*60)) * time.Minute
		var rgs [][]genRow
		for g := 1 + draw(3); g > 0; g-- {
			var rows []genRow
			for r := draw(5); r > 0; r-- {
				tr := ""
				if draw(4) > 0 {
					tr = traces[draw(len(traces))]
					if draw(2) == 0 {
						tr = strings.ToUpper(tr)
					}
				}
				rows = append(rows, genRow{trace: tr, body: "x"})
			}
			rgs = append(rgs, rows)
		}
		l.add(t, prod, rgs, dt)
		if draw(3) == 0 {
			mustPass(t, x)
		}
	}
	mustPass(t, x)
	// every day ends; one more pass builds the day level
	l.at = l.at.Truncate(24 * time.Hour).Add(24*time.Hour + 11*time.Minute)
	mustPass(t, x)
	return l, traces
}

func dayIndexer(l *lake) *Indexer {
	ix := New(Config{Root: "lake", Signals: []string{"logs"}, MaxSegmentObjects: 3, MergeAfterS: 600, DayLookbackD: 10}, l.m)
	ix.Now = func() time.Time { return l.at }
	return ix
}

// dayCase: the day level answers a trace lookup for every object it covers
// (no hourly read for them), and the answer is a superset of the truth;
// mut is a deliberate bug of the reader (0: none).
func dayCase(t interface{ Fatalf(string, ...any) }, draw func(int) int, mut int) error {
	l, traces := dayLake(t, draw, dayIndexer)
	tr := traces[draw(len(traces))]
	if draw(4) == 0 {
		tr = fmt.Sprintf("%032x", draw(1<<30)) // in no object
	}
	r := NewResolver(ResolverConfig{Root: "lake"}, l.m)
	r.mut = mut
	got, rep := r.Resolve(context.Background(), "c1", "logs", l.listed(), Filter{TraceID: tr})
	if len(rep.Errors) > 0 {
		return fmt.Errorf("errors: %v", rep.Errors)
	}
	if rep.Scan != 0 || rep.Segments != 0 || rep.DaySegments == 0 {
		return fmt.Errorf("after the day level: %+v (every object answered by a day shard, no hourly segment)", rep)
	}
	want := truth(l.objs, tr, nil)
	for _, o := range l.objs {
		res, w := got[o.key], want[o.key]
		have := map[int]bool{}
		for _, g := range res.RowGroups {
			have[g] = true
		}
		for _, g := range w {
			if res.Status == None || (res.Status == Hit && !have[g]) {
				return fmt.Errorf("%s: row group %d holds %s, index said %+v", o.key, g, tr, res)
			}
		}
	}
	return nil
}

// The property (rapid): over generated lakes spanning days, a trace lookup
// after the day level is built reads only day shards, and never misses a
// row group that holds the id.
func TestDayLevelResolveIsASuperset(t *testing.T) {
	tracetag.Covers(t, "P2C", "H-2", "H-5")
	rapid.Check(t, func(t *rapid.T) {
		d := func(n int) int { return rapid.IntRange(0, n-1).Draw(t, "d") }
		if err := dayCase(t, d, 0); err != nil {
			t.Fatal(err)
		}
	})
}

// The reader's mutants: the shard by the fingerprint's low bits; a
// manifest trusted for an object by key alone; a shard trusted without
// being its manifest's. Each is caught by the property or by the scenario
// built for it.
func TestDayLevelMutantsCaught(t *testing.T) {
	tracetag.Covers(t, "MU", "H-2", "H-5")
	caught := false
	for seed := int64(1); seed <= 500 && !caught; seed++ {
		rng := rand.New(rand.NewSource(seed))
		caught = dayCase(t, func(n int) int { return rng.Intn(n) }, mutDayLowBits) != nil
	}
	if !caught {
		t.Error("mutDayLowBits not caught in 500 cases")
	}
	if err := replacedObjectCase(t, mutDayNoMatch); err == nil {
		t.Error("mutDayNoMatch not caught")
	}
	if err := forgedShardCase(t, mutDayAnyShard); err == nil {
		t.Error("mutDayAnyShard not caught")
	}
}

// An object rewritten after its day was built (the same key, other bytes)
// is not the object the manifest covers: it is answered as the hourly
// segments answer it (here: scan), never from the day's stale entries.
func TestDayLevelReplacedObjectIsNotTrusted(t *testing.T) {
	if err := replacedObjectCase(t, 0); err != nil {
		t.Fatal(err)
	}
}

func replacedObjectCase(t *testing.T, mut int) error {
	l := newLake()
	ix := dayIndexer(l)
	id := strings.Repeat("d", 32)
	l.add(t, "pub-0", [][]genRow{{{trace: strings.Repeat("e", 32), body: "x"}}}, 0)
	l.add(t, "pub-0", [][]genRow{{{trace: id, body: "x"}}}, time.Hour)
	mustPass(t, ix)
	l.at = time.Date(2026, 9, 29, 0, 11, 0, 0, time.UTC)
	mustPass(t, ix)
	// the first object's bytes replaced: now it holds id too
	body := writeParquet(t, [][]genRow{{{trace: id, body: "x"}, {trace: strings.Repeat("e", 32), body: "longer now"}}})
	lm := t0
	l.m.Put(l.objs[0].key, body, nil, lm)
	r := NewResolver(ResolverConfig{Root: "lake"}, l.m)
	r.mut = mut
	got, rep := r.Resolve(context.Background(), "c1", "logs", l.listed(), Filter{TraceID: id})
	if got[l.objs[0].key].Status == None {
		return fmt.Errorf("the rewritten object was ruled out: %+v %+v", got, rep)
	}
	if got[l.objs[1].key].Status != Hit || rep.DaySegments != 1 {
		return fmt.Errorf("the other object: %+v %+v", got, rep)
	}
	return nil
}

// A manifest that names another manifest's shards (ordinals of another
// object list) is refused for the shard; the next manifest answers.
func TestDayLevelForgedShardIsRefused(t *testing.T) {
	if err := forgedShardCase(t, 0); err != nil {
		t.Fatal(err)
	}
}

func forgedShardCase(t *testing.T, mut int) error {
	l := newLake()
	ix := dayIndexer(l)
	id := strings.Repeat("f", 32)
	l.add(t, "pub-1", [][]genRow{{{body: "x"}}, {{trace: id, body: "x"}}}, 0)
	l.add(t, "pub-1", [][]genRow{{{trace: strings.Repeat("9", 32), body: "x"}}}, time.Hour)
	mustPass(t, ix)
	l.at = time.Date(2026, 9, 29, 0, 11, 0, 0, time.UTC)
	mustPass(t, ix)
	ctx := context.Background()
	mans, err := ix.dayManifests(ctx, "c1", "logs", "20260928")
	if err != nil || len(mans) != 1 {
		return fmt.Errorf("manifests: %v %v", mans, err)
	}
	m1 := mans[0].Header
	// a forged manifest: one more object sorting first (every ordinal
	// shifts), and the first manifest's shards
	b := NewBuilder(BuildConfig{}, "", "")
	b.AddObject(SegObject{Key: "lake/c1/pub-0/logs/" + epochA + "/00000000000000000000.parquet", Size: 1, RowGroups: []int64{1, 1}})
	for _, o := range m1.Objects {
		b.AddObject(o)
	}
	b.day = m1.Day
	body, _, err := b.Build("c1", "logs", "20260928", DayLevel)
	if err != nil {
		return err
	}
	l.m.Put(DayKey("lake", "c1", "logs", "20260928", "M", body), body, nil, l.at)
	r := NewResolver(ResolverConfig{Root: "lake"}, l.m)
	r.mut = mut
	got, rep := r.Resolve(ctx, "c1", "logs", l.listed(), Filter{TraceID: id})
	if res := got[l.objs[0].key]; res.Status == None || (res.Status == Hit && (len(res.RowGroups) != 1 || res.RowGroups[0] != 1)) {
		return fmt.Errorf("the forged manifest's shard answered: %+v %+v", got, rep)
	}
	if mut == 0 && (rep.DaySegments != 1 || len(rep.Errors) != 1 || rep.Scan != 0 || rep.Segments != 0) {
		return fmt.Errorf("want the forged manifest's shard refused and the genuine manifest used: %+v", rep)
	}
	return nil
}

// The day level's layout: 2^bits shards (at least 4), every one present
// even when empty, the manifest naming them; an idle pass writes nothing;
// a straggler object of an ended day is answered hourly until the next
// pass builds a manifest that covers it too.
func TestDayLevelLayoutAndStragglers(t *testing.T) {
	tracetag.Covers(t, "P2C", "H-2", "H-5")
	l := newLake()
	ix := dayIndexer(l)
	id := strings.Repeat("a", 32)
	l.add(t, "pub-0", [][]genRow{{{trace: id, body: "x"}}}, 0)
	mustPass(t, ix)
	day := "lake/c1/_index/v1/logs/20260928/"
	if keys := l.m.Keys(day); len(keys) != 0 {
		t.Fatalf("a day built before it ended: %v", keys)
	}
	l.at = time.Date(2026, 9, 29, 0, 11, 0, 0, time.UTC)
	mustPass(t, ix)
	keys := l.m.Keys(day)
	var shards, mans []string
	for _, k := range keys {
		switch {
		case strings.Contains(k, "/S"):
			shards = append(shards, k)
		case strings.Contains(k, "/M-"):
			mans = append(mans, k)
		}
	}
	if len(shards) != 4 || len(mans) != 1 {
		t.Fatalf("day objects: %v", keys)
	}
	mustPass(t, ix)
	if n := len(l.m.Keys(day)); n != 5 {
		t.Fatalf("an idle pass wrote day objects: %d", n)
	}
	// a straggler committed in the ended day (its LastModified in it)
	l.at = time.Date(2026, 9, 28, 23, 59, 0, 0, time.UTC)
	l.add(t, "pub-1", [][]genRow{{{trace: id, body: "late"}}}, 0)
	l.at = time.Date(2026, 9, 29, 0, 20, 0, 0, time.UTC)
	got, rep := l.resolve(id, nil)
	if got[l.objs[1].key].Status != Scan || got[l.objs[0].key].Status != Hit || rep.DaySegments != 1 {
		t.Fatalf("straggler before its pass: %+v %+v", got, rep)
	}
	mustPass(t, ix)
	got, rep = l.resolve(id, nil)
	if got[l.objs[1].key].Status != Hit || rep.DaySegments != 1 || rep.Segments != 0 || rep.Scan != 0 {
		t.Fatalf("after the pass: %+v %+v", got, rep)
	}
	if mans := len(func() []string {
		var m []string
		for _, k := range l.m.Keys(day) {
			if strings.Contains(k, "/M-") {
				m = append(m, k)
			}
		}
		return m
	}()); mans != 2 {
		t.Fatalf("manifests: %d", mans)
	}
}

func TestDayBits(t *testing.T) {
	for _, c := range []struct{ entries, min, want int }{{0, 2, 2}, {10, 0, 0}, {3 << 20, 2, 2}, {11 << 20, 2, 3}, {1 << 40, 2, MaxDayBits}} {
		if got := dayBits(c.entries, c.min, 8<<20); got != c.want {
			t.Errorf("dayBits(%d, %d) = %d, want %d", c.entries, c.min, got, c.want)
		}
	}
	if ShardOf(0xA0000000, 2) != 2 || ShardOf(0xFFFFFFFF, 0) != 0 || ShardOf(0x80000000, 8) != 0x80 {
		t.Error("ShardOf")
	}
}

// A shard damaged in storage: lookups fall back (hourly, reported), and the
// next pass writes it beside (-r1) under a new manifest the reader then uses.
// Lost requests and lost answers while building a day converge too.
func TestDayLevelCorruptShardAndLostWrites(t *testing.T) {
	tracetag.Covers(t, "P2C", "H-2", "H-5")
	l := newLake()
	ix := dayIndexer(l)
	id := strings.Repeat("7", 32)
	l.add(t, "pub-0", [][]genRow{{{trace: id, body: "x"}}}, 0)
	mustPass(t, ix)
	l.at = time.Date(2026, 9, 29, 0, 11, 0, 0, time.UTC)
	var calls int
	l.m.LosePut = func(k string) bool { calls++; return strings.Contains(k, "/20260928/") && calls < 15 && calls%3 == 0 }
	l.m.LoseAnswer = func(k string) bool { return strings.Contains(k, "/20260928/") && calls < 15 && calls%3 == 1 }
	var err error
	for i := 0; i < 20; i++ {
		x := dayIndexer(l) // a fresh process each time
		if _, err = x.RunOnce(context.Background()); err == nil {
			break
		}
	}
	if err != nil {
		t.Fatalf("never converged: %v", err)
	}
	l.m.LosePut, l.m.LoseAnswer = nil, nil
	got, rep := l.resolve(id, nil)
	if got[l.objs[0].key].Status != Hit || rep.DaySegments != 1 || len(rep.Errors) != 0 {
		t.Fatalf("after lost writes: %+v %+v", got, rep)
	}
	// damage the shard the id lives in
	var shard string
	for _, k := range l.m.Keys("lake/c1/_index/v1/logs/20260928/") {
		if strings.Contains(k, fmt.Sprintf("/S%02x-", ShardOf(FP(id), 2))) {
			shard = k
		}
	}
	b := l.m.Body(shard)
	b[len(b)-20] ^= 0xff
	l.m.Put(shard, b, nil, l.at)
	got, rep = l.resolve(id, nil)
	if got[l.objs[0].key].Status != Hit || rep.DaySegments != 0 || rep.Segments != 1 || len(rep.Errors) == 0 {
		t.Fatalf("a damaged shard: %+v %+v", got, rep)
	}
	ix2 := dayIndexer(l)
	mustPass(t, ix2)
	got, rep = l.resolve(id, nil)
	if got[l.objs[0].key].Status != Hit || rep.DaySegments != 1 {
		t.Fatalf("after the rebuild: %+v %+v %v", got, rep, l.m.Keys("lake/c1/_index/v1/logs/20260928/"))
	}
}
