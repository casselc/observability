package lake

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/query/internal/completeness"
	"github.com/casselc/observability/otel-chdb/query/internal/store"
)

// dumpObj is one object of parquetgo/edge's TestLateMeasureDump: size,
// S3 metadata, LastModified and the rows' event times.
type dumpObj struct {
	Key  string            `json:"k"`
	Size int64             `json:"s"`
	LM   int64             `json:"lm"`
	Meta map[string]string `json:"m"`
	TS   []int64           `json:"ts"`
}

func loadDump(t *testing.T, path string) []dumpObj {
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []dumpObj
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	for sc.Scan() {
		var o dumpObj
		if err := json.Unmarshal(sc.Bytes(), &o); err != nil {
			t.Fatal(err)
		}
		out = append(out, o)
	}
	return out
}

// hasRowIn: the brute-force truth (ts sorted).
func hasRowIn(ts []int64, from, to int64) bool {
	i := sort.Search(len(ts), func(i int) bool { return ts[i] >= from })
	return i < len(ts) && ts[i] < to
}

type winStats struct {
	n                          int
	objs, bytes, ideal, ibytes float64
	maxObjs, maxIdeal          int
	lateObjs                   float64
}

func (w *winStats) add(objs int, bytes int64, ideal int, ibytes int64, late int) {
	w.n++
	w.objs += float64(objs)
	w.bytes += float64(bytes)
	w.ideal += float64(ideal)
	w.ibytes += float64(ibytes)
	w.lateObjs += float64(late)
	w.maxObjs = max(w.maxObjs, objs)
	w.maxIdeal = max(w.maxIdeal, ideal)
}

func (w *winStats) String() string {
	n := float64(w.n)
	return fmt.Sprintf("windows %4d  objects %7.1f (max %5d)  bytes %8.2f MB  | ideal objects %6.1f (max %4d) bytes %7.2f MB  | x%.2f objects, x%.2f bytes  late-marked %.1f",
		w.n, w.objs/n, w.maxObjs, w.bytes/n/1e6, w.ideal/n, w.maxIdeal, w.ibytes/n/1e6, w.objs/w.ideal, w.bytes/w.ibytes, w.lateObjs/n)
}

// TestLateMeasure plans windows over the D31 datasets (LAT_DUMP=a.jsonl,b.jsonl)
// with the real planner, and checks every plan against brute force: an
// object with a row in the window is always planned. LAT_B=5m,15m also
// simulates option (b) (one object, a bulk range and an outlier range) on
// each dump. Not a pass/fail test beyond the superset check.
func TestLateMeasure(t *testing.T) {
	dumps := os.Getenv("LAT_DUMP")
	if dumps == "" {
		t.Skip("LAT_DUMP=file[,file] measures planned objects on the D31 datasets")
	}
	for _, path := range strings.Split(dumps, ",") {
		objs := loadDump(t, path)
		m := store.NewMem()
		var t0, t1 int64 = 1 << 62, 0
		for _, o := range objs {
			m.Objects[o.Key] = store.MemObject{Size: o.Size, Meta: o.Meta, LastModified: time.Unix(0, o.LM)}
			t0 = min(t0, o.LM)
			t1 = max(t1, o.LM)
		}
		get := func(ctx context.Context, k string) ([]byte, error) { b, _, err := m.Get(ctx, k); return b, err }
		wm := completeness.NewReader(get, "lake/_consumer/watermark.json", time.Second, time.Hour)
		p := New(Config{Root: "lake", MaxHeads: 1 << 20, MaxObjects: 1 << 20, ListMax: 1 << 20, MaxWindowS: 7200}, m, wm)
		end := time.Unix(0, t1).Add(time.Minute)
		p.SetClock(func() time.Time { return end })
		byKey := map[string]dumpObj{}
		for _, o := range objs {
			byKey[o.Key] = o
		}
		var bounds []time.Duration
		if v := os.Getenv("LAT_B"); v != "" {
			for _, s := range strings.Split(v, ",") {
				d, err := time.ParseDuration(s)
				if err != nil {
					t.Fatal(err)
				}
				bounds = append(bounds, d)
			}
		}
		t.Logf("%s: %d objects", path, len(objs))
		start := time.Unix(0, t0).Truncate(time.Hour).Add(2 * time.Hour)
		for _, win := range []time.Duration{5 * time.Minute, time.Hour} {
			var recent, hist winStats
			bsim := make([]winStats, len(bounds))
			for from := start; !from.Add(win).After(end); from = from.Add(win) {
				to := from.Add(win)
				pl, err := p.Plan(context.Background(), fleet, Request{Signal: "traces", FromNs: from.UnixNano(), ToNs: to.UnixNano()})
				if err != nil {
					t.Fatal(err)
				}
				planned := map[string]bool{}
				for _, o := range pl.Objects {
					planned[o.Key] = true
				}
				ideal, ib := 0, int64(0)
				for _, o := range objs {
					if hasRowIn(o.TS, from.UnixNano(), to.UnixNano()) {
						ideal++
						ib += o.Size
						if !planned[o.Key] {
							t.Fatalf("%s [%v, %v): %s has a row in the window and was not planned", path, from, to, o.Key)
						}
					}
				}
				st := &hist
				if end.Sub(to) < time.Hour {
					st = &recent
				}
				st.add(len(pl.Objects), pl.TotalBytes, ideal, ib, pl.LateObjects)
				for i, b := range bounds {
					n, by := 0, int64(0)
					for _, o := range objs {
						if bOverlaps(o.TS, int64(b), from.UnixNano(), to.UnixNano()) {
							n++
							by += o.Size
						}
					}
					bsim[i].add(n, by, ideal, ib, 0)
				}
			}
			t.Logf("  %v windows, historical: %v", win, &hist)
			t.Logf("  %v windows, last hour:  %v", win, &recent)
			for i, b := range bounds {
				t.Logf("  %v windows, (b) outlier metadata at %v, all: %v", win, b, &bsim[i])
			}
		}
	}
}

// bOverlaps is option (b)'s planning rule: one object described by its bulk
// range (rows at or after max − bound) and its outliers' range (the rest).
func bOverlaps(ts []int64, bound, from, to int64) bool {
	if len(ts) == 0 {
		return false
	}
	cut := ts[len(ts)-1] - bound
	i := sort.Search(len(ts), func(i int) bool { return ts[i] >= cut })
	over := func(lo, hi int64) bool { return hi >= from && lo < to }
	if over(ts[i], ts[len(ts)-1]) {
		return true
	}
	return i > 0 && over(ts[0], ts[i-1])
}
