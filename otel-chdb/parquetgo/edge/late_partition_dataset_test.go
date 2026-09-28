package edge

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/parquetgo"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// TestLatePartitionDataset writes the central partition-key measurement's
// dataset (DECISIONS.md D34, the shape of D31's central measurement)
// through the real edge into a real bucket, for the real consumer to
// ingest: DAYS (default 3) days of trace batches received every 10 s, 200
// spans each (20 nodes × 10, 7 services × 40 span names), and 1% of the
// batches carrying 20 more spans 15 min–24 h old, which the edge splits
// into a late part (late_split_after 15 min). ~5.2M spans, ~26,000 objects.
//
// Only the partition layout is measured, so span and trace ids are
// low-entropy (a counter) and spans carry no attributes: the objects and
// central's parts stay small (the disk here is shared), while the rows per
// granule and per part are the real ones.
//
//	LATE_PK_S3=http://127.0.0.1:18333/bucket/prefix LATE_PK_KEY=otel LATE_PK_SECRET=otelsecret \
//	  go test ./edge -run TestLatePartitionDataset -v -timeout 60m
//
// LATE_PK_FROM / LATE_PK_TO (hours) write only that slice of the days, so a
// run can be split (the consumer-during-migration test writes the tail
// while the migration runs); the random stream is the same either way.
func TestLatePartitionDataset(t *testing.T) {
	url := os.Getenv("LATE_PK_S3")
	if url == "" {
		t.Skip("LATE_PK_S3=http://host/bucket/prefix writes the D34 dataset")
	}
	days := envInt(t, "LATE_PK_DAYS", 3)
	from := time.Duration(envInt(t, "LATE_PK_FROM", 0)) * time.Hour
	to := time.Duration(envInt(t, "LATE_PK_TO", days*24)) * time.Hour
	t0 := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	var now time.Time
	var n int
	cfg := Config{
		S3:         parquetgo.Config{URL: url, AccessKeyID: os.Getenv("LATE_PK_KEY"), SecretAccessKey: os.Getenv("LATE_PK_SECRET")},
		Cluster:    "c1",
		ProducerID: "p1",
		// one epoch per slice: a later slice continues in a new epoch
		NewEpoch:       func() string { n++; return fmt.Sprintf("20260901T%06d.000Z-%08x", int(from/time.Hour), n) },
		Now:            func() time.Time { return now },
		LateSplitAfter: 15 * time.Minute,
	}
	e, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	r := rand.New(rand.NewPCG(33, 1))
	var id uint64
	batches, lateBatches, written := 0, 0, 0
	start := time.Now()
	for now = t0; now.Before(t0.Add(time.Duration(days) * 24 * time.Hour)); now = now.Add(10 * time.Second) {
		late := time.Duration(0)
		if r.Float64() < 0.01 {
			late = 15*time.Minute + time.Duration(r.Int64N(int64(24*time.Hour-15*time.Minute)))
		}
		td := compactTraces(r, now, &id, late)
		batches++
		if late > 0 {
			lateBatches++
		}
		if off := now.Sub(t0); off < from || off >= to {
			continue
		}
		if err := e.PushTraces(WithReceived(context.Background(), uint64(now.UnixNano())), td); err != nil {
			t.Fatal(err)
		}
		written++
		if written%2000 == 0 {
			t.Logf("%d batches written (%s simulated), %.0f s", written, now.Format(time.RFC3339), time.Since(start).Seconds())
		}
	}
	t.Logf("%d batches (%d with late rows), %d written in [%v, %v), %d PUTs, %.0f s",
		batches, lateBatches, written, from, to, e.Stats().Puts.Load(), time.Since(start).Seconds())
}

func envInt(t *testing.T, k string, def int) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		t.Fatalf("%s: %v", k, err)
	}
	return n
}

// compactTraces is skewedTraces' shape without its entropy: 20 nodes × 10
// spans received at t, each started up to 5 s before it, and (late > 0) 20
// spans that started `late` before t; ids from a counter, no attributes.
func compactTraces(r *rand.Rand, t time.Time, id *uint64, late time.Duration) ptrace.Traces {
	td := ptrace.NewTraces()
	add := func(node, n int, start func() time.Time) {
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", "svc-"+strconv.Itoa(node%7))
		rs.Resource().Attributes().PutStr("k8s.node.name", "node-"+strconv.Itoa(node))
		ss := rs.ScopeSpans().AppendEmpty()
		for range n {
			*id++
			s := ss.Spans().AppendEmpty()
			s.SetName("op-" + strconv.Itoa(r.IntN(40)))
			var tid pcommon.TraceID
			var sid pcommon.SpanID
			for i := range 8 {
				tid[15-i] = byte(*id >> (8 * i))
				sid[7-i] = byte(*id >> (8 * i))
			}
			s.SetTraceID(tid)
			s.SetSpanID(sid)
			st := start()
			s.SetStartTimestamp(pcommon.NewTimestampFromTime(st))
			s.SetEndTimestamp(pcommon.NewTimestampFromTime(st.Add(time.Duration(r.IntN(50_000_000)))))
		}
	}
	for node := range 20 {
		add(node, 10, func() time.Time { return t.Add(-time.Duration(r.Int64N(int64(5 * time.Second)))) })
	}
	if late > 0 {
		add(20, 20, func() time.Time { return t.Add(-late - time.Duration(r.Int64N(int64(2*time.Second)))) })
	}
	return td
}
