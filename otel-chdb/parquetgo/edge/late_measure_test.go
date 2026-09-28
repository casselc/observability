package edge

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/parquetgo/commit"
	"github.com/parquet-go/parquet-go"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// dumpStore records every object's size, metadata, simulated LastModified
// and its rows' Timestamp column (read back from the Parquet), and keeps no
// body: the input of the planner's measurement (query/internal/lake
// late_measure_test.go, DECISIONS.md D31).
type dumpStore struct {
	mu   sync.Mutex
	now  func() time.Time
	objs map[string]dumpObj
	keys []string
}

type dumpObj struct {
	Key  string            `json:"k"`
	Size int               `json:"s"`
	LM   int64             `json:"lm"`
	Meta map[string]string `json:"m"`
	TS   []int64           `json:"ts,omitempty"`
}

func (d *dumpStore) PutCreate(_ context.Context, key string, body []byte, _ string, meta map[string]string) commit.PutOutcome {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.objs[key]; ok {
		return commit.PutExists
	}
	o := dumpObj{Key: key, Size: len(body), LM: d.now().UnixNano(), Meta: meta}
	if len(body) > 0 {
		ts, err := timestamps(body)
		if err != nil {
			panic(err)
		}
		o.TS = ts
	}
	d.objs[key] = o
	d.keys = append(d.keys, key)
	return commit.PutOK
}

func (d *dumpStore) Head(_ context.Context, key string) (map[string]string, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	o, ok := d.objs[key]
	return o.Meta, ok, nil
}

// timestamps reads a traces or logs object's Timestamp column, sorted.
func timestamps(b []byte) ([]int64, error) {
	f, err := parquet.OpenFile(bytes.NewReader(b), int64(len(b)))
	if err != nil {
		return nil, err
	}
	leaf, ok := f.Schema().Lookup("Timestamp")
	if !ok {
		return nil, fmt.Errorf("no Timestamp column")
	}
	var out []int64
	for _, rg := range f.RowGroups() {
		pages := rg.ColumnChunks()[leaf.ColumnIndex].Pages()
		for {
			p, err := pages.ReadPage()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			vals := make([]parquet.Value, p.NumValues())
			n, _ := p.Values().ReadValues(vals)
			for _, v := range vals[:n] {
				out = append(out, v.Int64())
			}
		}
		_ = pages.Close()
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// skewedTraces is one simulated batch: nodes × perNode spans received at t,
// each started up to 5 s before t; node `skewNode` (if >= 0) runs its clock
// `skew` behind; late (> 0) adds a chunk of lateRows spans that started
// `late` before t (a replay of an old buffer).
func skewedTraces(r *rand.Rand, t time.Time, nodes, perNode, skewNode int, skew time.Duration, late time.Duration, lateRows int) ptrace.Traces {
	td := ptrace.NewTraces()
	add := func(node int, n int, start func() time.Time) {
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", "svc-"+strconv.Itoa(node%7))
		rs.Resource().Attributes().PutStr("k8s.node.name", "node-"+strconv.Itoa(node))
		ss := rs.ScopeSpans().AppendEmpty()
		for range n {
			s := ss.Spans().AppendEmpty()
			s.SetName("op-" + strconv.Itoa(r.IntN(40)))
			var tid pcommon.TraceID
			var sid pcommon.SpanID
			for i := range tid {
				tid[i] = byte(r.Uint32())
			}
			for i := range sid {
				sid[i] = byte(r.Uint32())
			}
			s.SetTraceID(tid)
			s.SetSpanID(sid)
			st := start()
			s.SetStartTimestamp(pcommon.NewTimestampFromTime(st))
			s.SetEndTimestamp(pcommon.NewTimestampFromTime(st.Add(time.Duration(r.IntN(50_000_000)))))
			s.Attributes().PutStr("http.route", "/api/v1/items/"+strconv.Itoa(r.IntN(100)))
			s.Attributes().PutInt("http.status_code", int64(200+r.IntN(4)*100))
		}
	}
	for node := range nodes {
		off := time.Duration(0)
		if node == skewNode {
			off = skew
		}
		add(node, perNode, func() time.Time { return t.Add(-off - time.Duration(r.Int64N(int64(5*time.Second)))) })
	}
	if late > 0 {
		add(nodes, lateRows, func() time.Time { return t.Add(-late - time.Duration(r.Int64N(int64(2*time.Second)))) })
	}
	return td
}

// TestLateMeasureDump writes the D31 measurement dataset through the real
// edge (LAT_MEASURE_OUT=file; LAT_SPLIT=duration sets the late split, 0 or
// unset: none; LAT_SKEW=0 drops the skewed node). 26 h of batches every
// 10 s, 20 nodes × 10 spans each, node 19's clock 5 min behind, and 1% of
// batches carrying 20 spans 15 min–24 h old. Not a pass/fail test.
func TestLateMeasureDump(t *testing.T) {
	out := os.Getenv("LAT_MEASURE_OUT")
	if out == "" {
		t.Skip("LAT_MEASURE_OUT=file writes the D31 dataset")
	}
	split := time.Duration(0)
	if v := os.Getenv("LAT_SPLIT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatal(err)
		}
		split = d
	}
	skewNode := 19
	if os.Getenv("LAT_SKEW") == "0" {
		skewNode = -1
	}
	t0 := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	var now time.Time
	clock := func() time.Time { return now }
	st := &dumpStore{now: func() time.Time { return now.Add(200 * time.Millisecond) }, objs: map[string]dumpObj{}}
	var n int
	cfg := Config{Store: st, Prefix: "lake", Cluster: "c1", ProducerID: "p1",
		NewEpoch: func() string { n++; return fmt.Sprintf("20260901T000000.000Z-%08x", n) }, Now: clock, LateSplitAfter: split}
	e, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	r := rand.New(rand.NewPCG(31, 1))
	batches, lateBatches := 0, 0
	for now = t0; now.Before(t0.Add(26 * time.Hour)); now = now.Add(10 * time.Second) {
		late := time.Duration(0)
		if r.Float64() < 0.01 {
			late = 15*time.Minute + time.Duration(r.Int64N(int64(24*time.Hour-15*time.Minute)))
			lateBatches++
		}
		td := skewedTraces(r, now, 20, 10, skewNode, 5*time.Minute, late, 20)
		if err := e.PushTraces(WithReceived(context.Background(), uint64(now.UnixNano())), td); err != nil {
			t.Fatal(err)
		}
		batches++
	}
	f, err := os.Create(out)
	if err != nil {
		t.Fatal(err)
	}
	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)
	objs, bytes := 0, 0
	for _, k := range st.keys {
		o := st.objs[k]
		if o.Size == 0 {
			continue
		}
		objs++
		bytes += o.Size
		if err := enc.Encode(o); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	t.Logf("split %v: %d batches (%d with late rows) -> %d objects, %d bytes, %d PUTs", split, batches, lateBatches, objs, bytes, e.Stats().Puts.Load())
}
