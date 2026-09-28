package lake

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/query/internal/completeness"
	"github.com/casselc/observability/otel-chdb/query/internal/store"
	"pgregory.net/rapid"
)

// lateObj is one slot for the D31 tests: its rows' event times and how the
// edge described it.
type lateObj struct {
	key       string
	ts        []int64
	recv      int64
	part      string
	unrefined bool // metadata without a time range: planned on its LIST entry
}

func (o lateObj) meta() map[string]string {
	m := map[string]string{"oscope-kind": "data", "oscope-cluster": "c1", "oscope-rows": strconv.Itoa(len(o.ts)),
		"oscope-received": strconv.FormatInt(o.recv, 10)}
	if !o.unrefined {
		m["oscope-min-time"] = strconv.FormatInt(slices.Min(o.ts), 10)
		m["oscope-max-time"] = strconv.FormatInt(slices.Max(o.ts), 10)
	}
	if o.part != "" {
		m["oscope-part"] = o.part
	}
	return m
}

func latePlanner(objs []lateObj, cfg Config) (*Planner, *store.Mem) {
	m := store.NewMem()
	for _, o := range objs {
		m.Objects[o.key] = store.MemObject{Size: int64(100 * len(o.ts)), Meta: o.meta(), LastModified: time.Unix(0, o.recv).Add(time.Second)}
	}
	get := func(ctx context.Context, k string) ([]byte, error) { b, _, err := m.Get(ctx, k); return b, err }
	cfg.Root = "lake"
	p := New(cfg, m, completeness.NewReader(get, "lake/_consumer/watermark.json", time.Second, time.Hour))
	p.SetClock(func() time.Time { return base.Add(48 * time.Hour) })
	return p, m
}

// splitRows is the edges' rule (parquetgo/edge/late.go LateCut): with
// bound b > 0, rows more than b older than the newest are the late part.
func splitRows(ts []int64, b time.Duration) (bulk, late []int64) {
	if b <= 0 {
		return ts, nil
	}
	cut := slices.Max(ts) - int64(b)
	for _, t := range ts {
		if t < cut {
			late = append(late, t)
		} else {
			bulk = append(bulk, t)
		}
	}
	return bulk, late
}

// A request with rows a day late: whole, its object is planned for every
// window between; split, the window between plans neither part, and a
// window over either part plans that part alone, labelled.
func TestPlanLateSplit(t *testing.T) {
	T := base.Add(24 * time.Hour).UnixNano()
	ts := []int64{T, T - int64(time.Second), T - int64(24*time.Hour), T - int64(24*time.Hour) + 5}
	bulk, late := splitRows(ts, 15*time.Minute)
	whole := []lateObj{{key: "lake/c1/p1/traces/" + ep + "/00000000000000000000.parquet", ts: ts, recv: T}}
	parts := []lateObj{
		{key: "lake/c1/p1/traces/" + ep + "/00000000000000000000.parquet", ts: bulk, recv: T, part: "bulk"},
		{key: "lake/c1/p1/traces/" + ep + "/00000000000000000001.parquet", ts: late, recv: T, part: "late"},
	}
	plan := func(objs []lateObj, from, to int64) *Plan {
		p, _ := latePlanner(objs, Config{})
		pl, err := p.Plan(context.Background(), fleet, Request{Signal: "traces", FromNs: from, ToNs: to})
		if err != nil {
			t.Fatal(err)
		}
		return pl
	}
	mid := T - int64(12*time.Hour)
	if n := len(plan(whole, mid, mid+int64(5*time.Minute)).Objects); n != 1 {
		t.Fatalf("whole: %d objects for the window between", n)
	}
	if n := len(plan(parts, mid, mid+int64(5*time.Minute)).Objects); n != 0 {
		t.Fatalf("split: %d objects for the window between", n)
	}
	old := plan(parts, T-int64(24*time.Hour), T-int64(24*time.Hour)+int64(time.Minute))
	if len(old.Objects) != 1 || old.Objects[0].Part != "late" || *old.Objects[0].Rows != 2 || !old.Objects[0].Late {
		t.Fatalf("late window: %+v", old.Objects)
	}
	now := plan(parts, T-int64(time.Minute), T+1)
	if len(now.Objects) != 1 || now.Objects[0].Part != "bulk" || now.Objects[0].Late {
		t.Fatalf("recent window: %+v", now.Objects)
	}
}

// Property: on skewed data (clocks ahead within the planner's skew, behind
// by minutes, rows up to a day late), split or not at any bound, with any
// HEAD budget and some objects without a time range, the plan is a
// superset of the brute-force answer: every object with a row in the
// window is planned. With every object refined, it is also exact on
// ranges: nothing is planned whose [min, max] misses the window.
func TestPlanSupersetOnSkewedData(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		bound := []time.Duration{0, time.Minute, 15 * time.Minute}[rapid.IntRange(0, 2).Draw(t, "bound")]
		n := rapid.IntRange(1, 25).Draw(t, "requests")
		var objs []lateObj
		seq := 0
		allRefined := true
		for i := range n {
			recv := base.UnixNano() + rapid.Int64Range(0, int64(3*time.Hour)).Draw(t, fmt.Sprint("recv", i))
			rows := rapid.IntRange(1, 12).Draw(t, fmt.Sprint("rows", i))
			var ts []int64
			for j := range rows {
				var age int64
				switch rapid.IntRange(0, 3).Draw(t, fmt.Sprint("kind", i, j)) {
				case 0: // a clock ahead, within the planner's skew_s (300 s)
					age = -rapid.Int64Range(0, int64(299*time.Second)).Draw(t, fmt.Sprint("ahead", i, j))
				case 1: // a clock behind
					age = rapid.Int64Range(0, int64(10*time.Minute)).Draw(t, fmt.Sprint("behind", i, j))
				case 2: // late
					age = rapid.Int64Range(int64(15*time.Minute), int64(24*time.Hour)).Draw(t, fmt.Sprint("late", i, j))
				default:
					age = rapid.Int64Range(0, int64(5*time.Second)).Draw(t, fmt.Sprint("now", i, j))
				}
				ts = append(ts, recv-age)
			}
			unref := rapid.IntRange(0, 9).Draw(t, fmt.Sprint("unref", i)) == 0
			allRefined = allRefined && !unref
			bulk, late := splitRows(ts, bound)
			for _, pt := range []struct {
				ts   []int64
				part string
			}{{bulk, "bulk"}, {late, "late"}} {
				if len(pt.ts) == 0 {
					continue
				}
				o := lateObj{key: fmt.Sprintf("lake/c1/p1/traces/%s/%020d.parquet", ep, seq), ts: pt.ts, recv: recv, unrefined: unref}
				if len(late) > 0 {
					o.part = pt.part
				}
				objs = append(objs, o)
				seq++
			}
		}
		heads := rapid.IntRange(1, len(objs)+1).Draw(t, "max_heads")
		p, _ := latePlanner(objs, Config{MaxHeads: heads, MaxObjects: 1000})
		from := base.UnixNano() + rapid.Int64Range(-int64(26*time.Hour), int64(4*time.Hour)).Draw(t, "from")
		to := from + rapid.Int64Range(1, int64(2*time.Hour)).Draw(t, "len")
		pl, err := p.Plan(context.Background(), fleet, Request{Signal: "traces", FromNs: from, ToNs: to})
		if err != nil {
			t.Fatal(err)
		}
		planned := map[string]bool{}
		for _, o := range pl.Objects {
			planned[o.Key] = true
		}
		for _, o := range objs {
			has := slices.ContainsFunc(o.ts, func(x int64) bool { return x >= from && x < to })
			if has && !planned[o.key] {
				t.Fatalf("%s has a row in [%d, %d) and was not planned (rows %v)", o.key, from, to, o.ts)
			}
			if allRefined && heads > len(objs) && planned[o.key] && (slices.Max(o.ts) < from || slices.Min(o.ts) >= to) {
				t.Fatalf("%s planned, its range [%d, %d] misses [%d, %d)", o.key, slices.Min(o.ts), slices.Max(o.ts), from, to)
			}
		}
	})
}

