// soaksend generates distinct OTLP requests without end and posts them to an
// edge over OTLP/HTTP, the way a collector exporter with a persistent queue
// and retry_on_failure behaves: each request is resent with identical bytes
// until it gets a 2xx (429/502/503/504 and transport errors are retried;
// any other status drops it). Every request carries a unique resource
// attribute `soak.req` = "<producer>-<signal>-<n>", so central can be checked
// per request. One goroutine per signal; each sends one request at a time.
//
// Each acked request is one JSON line on -out: {"id","signal","rows","points"
// (metrics: per type),"attempts","ack_ms"}. SIGTERM/SIGINT stops sending new
// requests; the one in hand is still resent until acked.
//
//	soaksend -url http://127.0.0.1:14318 -producer edge-1 -signals traces,logs,metrics \
//	         -rows 200 -points 40 -rate 2 -out acked.jsonl [-duration 10m]
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

type acked struct {
	ID       string         `json:"id"`
	Signal   string         `json:"signal"`
	Rows     int            `json:"rows"`
	Points   map[string]int `json:"points,omitempty"`
	Attempts int            `json:"attempts"`
	AckMS    float64        `json:"ack_ms"`
	SentNS   int64          `json:"sent_ns"`
}

func resource(r pcommon.Resource, producer, id string) {
	a := r.Attributes()
	a.PutStr("service.name", "soak-"+producer)
	a.PutStr("host.name", producer)
	a.PutStr("soak.req", id)
}

func traces(producer, id string, rows int, rnd *rand.Rand) []byte {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	resource(rs.Resource(), producer, id)
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("soaksend")
	now := time.Now()
	var tid [16]byte
	for i := 0; i < rows; i++ {
		if i%10 == 0 {
			rnd.Read(tid[:])
		}
		s := ss.Spans().AppendEmpty()
		var sid [8]byte
		rnd.Read(sid[:])
		s.SetTraceID(tid)
		s.SetSpanID(sid)
		s.SetName(fmt.Sprintf("op-%d", i%7))
		s.SetKind(ptrace.SpanKindServer)
		s.SetStartTimestamp(pcommon.NewTimestampFromTime(now.Add(-time.Duration(rnd.Intn(1000)) * time.Millisecond)))
		s.SetEndTimestamp(pcommon.NewTimestampFromTime(now))
		s.Attributes().PutStr("http.route", fmt.Sprintf("/api/v%d/items", i%3))
		s.Attributes().PutInt("i", int64(i))
	}
	b, _ := ptraceotlp.NewExportRequestFromTraces(td).MarshalProto()
	return b
}

func logs(producer, id string, rows int, rnd *rand.Rand) []byte {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	resource(rl.Resource(), producer, id)
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("soaksend")
	now := time.Now()
	for i := 0; i < rows; i++ {
		l := sl.LogRecords().AppendEmpty()
		l.SetTimestamp(pcommon.NewTimestampFromTime(now))
		l.SetSeverityNumber(plog.SeverityNumberInfo)
		l.SetSeverityText("INFO")
		l.Body().SetStr(fmt.Sprintf("message %d of %s: %x", i, id, rnd.Uint64()))
		l.Attributes().PutInt("i", int64(i))
	}
	b, _ := plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
	return b
}

// metrics: `points` points of each of the five types; series vary by an
// attribute so each request has a few series of each type.
func metrics(producer, id string, points int, rnd *rand.Rand) ([]byte, map[string]int) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	resource(rm.Resource(), producer, id)
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("soaksend")
	now := pcommon.NewTimestampFromTime(time.Now())
	start := pcommon.NewTimestampFromTime(time.Now().Add(-10 * time.Second))
	g := sm.Metrics().AppendEmpty()
	g.SetName("soak.gauge")
	gg := g.SetEmptyGauge()
	s := sm.Metrics().AppendEmpty()
	s.SetName("soak.sum")
	ss := s.SetEmptySum()
	ss.SetIsMonotonic(true)
	ss.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	h := sm.Metrics().AppendEmpty()
	h.SetName("soak.histogram")
	hh := h.SetEmptyHistogram()
	hh.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	e := sm.Metrics().AppendEmpty()
	e.SetName("soak.exphistogram")
	ee := e.SetEmptyExponentialHistogram()
	ee.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	q := sm.Metrics().AppendEmpty()
	q.SetName("soak.summary")
	qq := q.SetEmptySummary()
	for i := 0; i < points; i++ {
		series := fmt.Sprintf("s%d", i%4)
		p := gg.DataPoints().AppendEmpty()
		p.SetTimestamp(now)
		p.SetDoubleValue(rnd.Float64() * 100)
		p.Attributes().PutStr("series", series)
		p.Attributes().PutInt("i", int64(i))
		ps := ss.DataPoints().AppendEmpty()
		ps.SetStartTimestamp(start)
		ps.SetTimestamp(now)
		ps.SetIntValue(int64(i))
		ps.Attributes().PutStr("series", series)
		ph := hh.DataPoints().AppendEmpty()
		ph.SetStartTimestamp(start)
		ph.SetTimestamp(now)
		ph.SetCount(3)
		ph.SetSum(6)
		ph.ExplicitBounds().FromRaw([]float64{1, 2, 5})
		ph.BucketCounts().FromRaw([]uint64{1, 1, 1, 0})
		ph.Attributes().PutStr("series", series)
		pe := ee.DataPoints().AppendEmpty()
		pe.SetStartTimestamp(start)
		pe.SetTimestamp(now)
		pe.SetCount(2)
		pe.SetScale(2)
		pe.Positive().SetOffset(1)
		pe.Positive().BucketCounts().FromRaw([]uint64{1, 1})
		pe.Attributes().PutStr("series", series)
		pq := qq.DataPoints().AppendEmpty()
		pq.SetStartTimestamp(start)
		pq.SetTimestamp(now)
		pq.SetCount(4)
		pq.SetSum(10)
		v := pq.QuantileValues().AppendEmpty()
		v.SetQuantile(0.5)
		v.SetValue(2.5)
		pq.Attributes().PutStr("series", series)
	}
	b, _ := pmetricotlp.NewExportRequestFromMetrics(md).MarshalProto()
	return b, map[string]int{"gauge": points, "sum": points, "histogram": points, "exponential_histogram": points, "summary": points}
}

func main() {
	url := flag.String("url", "http://127.0.0.1:14318", "OTLP/HTTP base URL")
	producer := flag.String("producer", "edge-1", "")
	sigs := flag.String("signals", "traces,logs,metrics", "")
	rows := flag.Int("rows", 200, "spans or log records per request")
	points := flag.Int("points", 40, "points per metric type per request")
	rate := flag.Float64("rate", 1, "requests per second per signal")
	duration := flag.Duration("duration", 0, "stop starting new requests after this (0: until SIGTERM)")
	maxN := flag.Int("n", 0, "requests per signal (0: no limit)")
	out := flag.String("out", "acked.jsonl", "")
	timeout := flag.Duration("timeout", 10*time.Second, "per-attempt HTTP timeout")
	backoff := flag.Duration("backoff", 300*time.Millisecond, "")
	seed := flag.Int64("seed", time.Now().UnixNano(), "")
	flag.Parse()
	f, err := os.OpenFile(*out, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		log.Fatal(err)
	}
	var mu sync.Mutex
	var stop atomic.Bool
	ch := make(chan os.Signal, 2)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
	go func() { <-ch; stop.Store(true) }()
	if *duration > 0 {
		time.AfterFunc(*duration, func() { stop.Store(true) })
	}
	cl := &http.Client{Timeout: *timeout}
	var wg sync.WaitGroup
	var total, attempts atomic.Int64
	for si, sig := range strings.Split(*sigs, ",") {
		wg.Add(1)
		go func(si int, sig string) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(*seed + int64(si)))
			interval := time.Duration(float64(time.Second) / *rate)
			next := time.Now()
			for n := 0; !stop.Load() && (*maxN == 0 || n < *maxN); n++ {
				if d := time.Until(next); d > 0 {
					time.Sleep(d)
				}
				next = next.Add(interval)
				id := fmt.Sprintf("%s-%s-%06d", *producer, sig, n)
				var body []byte
				a := acked{ID: id, Signal: sig}
				switch sig {
				case "traces":
					body, a.Rows = traces(*producer, id, *rows, rnd), *rows
				case "logs":
					body, a.Rows = logs(*producer, id, *rows, rnd), *rows
				case "metrics":
					body, a.Points = metrics(*producer, id, *points, rnd)
					a.Rows = 5 * *points
				default:
					log.Fatalf("signal %q", sig)
				}
				endpoint := strings.TrimRight(*url, "/") + "/v1/" + sig
				t0 := time.Now()
				a.SentNS = t0.UnixNano()
				for {
					a.Attempts++
					resp, err := cl.Post(endpoint, "application/x-protobuf", bytes.NewReader(body))
					if err == nil {
						b, _ := io.ReadAll(resp.Body)
						resp.Body.Close()
						if resp.StatusCode/100 == 2 {
							break
						}
						err = fmt.Errorf("HTTP %d: %.200s", resp.StatusCode, b)
						if c := resp.StatusCode; c != 429 && c != 502 && c != 503 && c != 504 {
							log.Printf("%s: %v (permanent: dropped)", id, err)
							a.Attempts = -a.Attempts
							break
						}
					}
					if a.Attempts%20 == 1 {
						log.Printf("%s attempt %d: %v", id, a.Attempts, err)
					}
					time.Sleep(*backoff)
				}
				a.AckMS = float64(time.Since(t0).Microseconds()) / 1000
				j, _ := json.Marshal(a)
				mu.Lock()
				f.Write(append(j, '\n'))
				mu.Unlock()
				total.Add(1)
				attempts.Add(int64(a.Attempts))
			}
		}(si, sig)
	}
	wg.Wait()
	f.Close()
	j, _ := json.Marshal(map[string]any{"summary": true, "producer": *producer, "requests": total.Load(), "attempts": attempts.Load()})
	fmt.Println(string(j))
}
