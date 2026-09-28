package main

// `s3accept load`: create-only PUTs at a target rate over the format-v2 key
// layout, to find where the store throttles and whether the layout spreads
// the load (DECISIONS.md risk 3: AWS documents 3,500 PUT/s per partitioned
// prefix and answers 503 SlowDown above it until it has split the prefix;
// ../../deploy/validation/eks-aws.md §5). Open loop: a pacer emits tokens at
// the target rate (ramped linearly over --load-ramp), workers take a token
// and send one PUT If-None-Match:* to a fresh slot. Tokens a busy worker pool
// cannot take are counted as `missed` (the client, not the store, was the
// limit: add workers or pods). SDK retries are off (newEnv), so every 503,
// 409 and timeout is seen as the store sent it.
//
// Layouts (every key under {prefix}/{run}/load/{layout}/):
//
//	cluster-first  {cNN}/edge-{P}/{signal}/{epoch}/{seq:020d}.parquet: the
//	               edges' layout (FORMAT.md §1), clusters × producers × signals lanes
//	single-lane    c00/edge-0/traces/{epoch}/{seq}: every PUT in one lane (worst case)
//	hashed         {hex4}/...: a random first segment, the textbook spread, for contrast
//
// The keys it acknowledged are deleted at the end (DeleteObjects, in
// parallel) unless --keep; a final LIST of the load prefix removes anything
// an ambiguous PUT left behind. The bucket itself is never touched.

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type LoadParams struct {
	Rate      int           `json:"rate_per_s"`
	Duration  time.Duration `json:"duration_ns"`
	Ramp      time.Duration `json:"ramp_ns"`
	Workers   int           `json:"workers"`
	Clusters  int           `json:"clusters"`
	Producers int           `json:"producers"`
	Signals   int           `json:"signals"`
	Layout    string        `json:"layout"`
	Size      int           `json:"size_bytes"`
	Every     time.Duration `json:"every_ns"`
}

// LoadSecond is one second of the run, as the client saw it.
type LoadSecond struct {
	T         int     `json:"t"`
	Target    int     `json:"target"`
	OK        int64   `json:"ok"`
	SlowDown  int64   `json:"slowdown_503"`
	Other5xx  int64   `json:"other_5xx"`
	Conflict  int64   `json:"conflict_409"`
	Precond   int64   `json:"precondition_412"`
	Forbidden int64   `json:"forbidden_403"`
	Transport int64   `json:"transport"`
	OtherErr  int64   `json:"other"`
	Missed    int64   `json:"missed"`
	P50ms     float64 `json:"p50_ms"`
	P99ms     float64 `json:"p99_ms"`
}

type LoadReport struct {
	Tool     string        `json:"tool"`
	Mode     string        `json:"mode"`
	Store    string        `json:"store"`
	Target   any           `json:"target"`
	Started  time.Time     `json:"started"`
	Finished time.Time     `json:"finished"`
	Params   LoadParams    `json:"params"`
	Lanes    int           `json:"lanes"`
	Seconds  []*LoadSecond `json:"seconds"`
	Totals   LoadSecond    `json:"totals"`
	Summary  []string      `json:"summary"`
	Cleanup  []string      `json:"cleanup"`
}

type loadBucket struct {
	ok, slow, s5xx, c409, c412, c403, transport, other, missed atomic.Int64
	mu                                                         sync.Mutex
	lat                                                        []time.Duration
}

var loadSignals = []string{"traces", "logs", "metrics_number_points", "metrics_histogram_points",
	"metrics_exponential_histogram_points", "metrics_summary_points", "metrics_series"}

// loadLane names lane i of the layout.
func loadLane(p LoadParams, i int) string {
	c := i / (p.Producers * p.Signals)
	pr := (i / p.Signals) % p.Producers
	s := loadSignals[i%p.Signals]
	return fmt.Sprintf("c%02d/edge-%d/%s", c, pr, s)
}

func runLoad(ctx context.Context, e *Env, p LoadParams, out string, keep bool) int {
	if p.Rate <= 0 || p.Duration <= 0 || p.Workers <= 0 {
		fatal(fmt.Errorf("load: --load-rate, --load-duration and --load-workers must be > 0"))
	}
	p.Signals = max(1, min(p.Signals, len(loadSignals)))
	p.Clusters, p.Producers = max(1, p.Clusters), max(1, p.Producers)
	lanes := p.Clusters * p.Producers * p.Signals
	switch p.Layout {
	case "cluster-first", "hashed":
	case "single-lane":
		lanes = 1
	default:
		fatal(fmt.Errorf("load: --load-layout %q (cluster-first | single-lane | hashed)", p.Layout))
	}
	base := e.Key("load", p.Layout)
	epoch := time.Now().UTC().Format("20060102T150405.000Z") + "-" + randHex(4)
	body := make([]byte, p.Size)
	rand.Read(body)
	seqs := make([]atomic.Int64, lanes)
	var laneRR atomic.Int64
	keyFor := func() string {
		l := int(laneRR.Add(1)-1) % lanes
		seq := seqs[l].Add(1) - 1
		switch p.Layout {
		case "single-lane":
			return fmt.Sprintf("%s/c00/edge-0/traces/%s/%020d.parquet", base, epoch, seq)
		case "hashed":
			return fmt.Sprintf("%s/%s/%s/%s/%020d.parquet", base, randHex(2), loadLane(p, l), epoch, seq)
		}
		return fmt.Sprintf("%s/%s/%s/%020d.parquet", base, loadLane(p, l), epoch, seq)
	}

	nsec := int((p.Duration + time.Second - 1) / time.Second)
	buckets := make([]*loadBucket, nsec+1)
	for i := range buckets {
		buckets[i] = &loadBucket{}
	}
	var ackedMu sync.Mutex
	var acked []string
	tokens := make(chan struct{}, p.Workers*4)
	t0 := time.Now()
	sec := func() int { return min(int(time.Since(t0)/time.Second), nsec) }
	target := func(s int) int {
		if p.Ramp <= 0 || time.Duration(s)*time.Second >= p.Ramp {
			return p.Rate
		}
		return max(1, int(float64(p.Rate)*float64(s+1)*float64(time.Second)/float64(p.Ramp)))
	}

	fmt.Printf("s3accept load: store %s, s3://%s/%s/, layout %s, %d lane(s), target %d PUT/s for %s (ramp %s), %d workers, %d B bodies\n",
		e.O.Store, e.O.Bucket, base, p.Layout, lanes, p.Rate, p.Duration, p.Ramp, p.Workers, p.Size)
	runCtx, cancel := context.WithTimeout(ctx, p.Duration)
	defer cancel()

	// Pacer: every 10 ms, the tokens due since the start of this second.
	go func() {
		defer close(tokens)
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		cur, sent := -1, 0
		for {
			select {
			case <-runCtx.Done():
				return
			case now := <-tick.C:
				s := int(now.Sub(t0) / time.Second)
				if s >= nsec {
					return
				}
				if s != cur {
					cur, sent = s, 0
				}
				frac := float64(now.Sub(t0)-time.Duration(s)*time.Second) / float64(time.Second)
				due := int(float64(target(s))*frac+0.5) - sent
				for ; due > 0; due-- {
					select {
					case tokens <- struct{}{}:
					default:
						buckets[s].missed.Add(1)
					}
					sent++
				}
			}
		}
	}()

	var wg sync.WaitGroup
	for w := 0; w < p.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range tokens {
				key := keyFor()
				s0 := time.Now()
				b := buckets[sec()]
				_, err := e.put(ctx, putReq{Key: key, Bytes: body, INM: "*", CT: "application/vnd.apache.parquet"})
				d := time.Since(s0)
				if err == nil {
					b.ok.Add(1)
					b.mu.Lock()
					b.lat = append(b.lat, d)
					b.mu.Unlock()
					ackedMu.Lock()
					acked = append(acked, key)
					ackedMu.Unlock()
					continue
				}
				o, st, code := classify(err)
				switch {
				case st == 503 || code == "SlowDown":
					b.slow.Add(1)
				case o == Conflict:
					b.c409.Add(1)
				case o == PreconditionFailed:
					b.c412.Add(1)
				case o == Forbidden:
					b.c403.Add(1)
				case st >= 500:
					b.s5xx.Add(1)
				case st == 0:
					b.transport.Add(1)
				default:
					b.other.Add(1)
				}
			}
		}()
	}

	// Progress every --load-every.
	stopProg := make(chan struct{})
	go func() {
		t := time.NewTicker(max(p.Every, time.Second))
		defer t.Stop()
		last := 0
		for {
			select {
			case <-stopProg:
				return
			case <-t.C:
				s := sec()
				var ok, slow, errs int64
				for i := last; i < s; i++ {
					b := buckets[i]
					ok += b.ok.Load()
					slow += b.slow.Load()
					errs += b.s5xx.Load() + b.c409.Load() + b.c412.Load() + b.c403.Load() + b.transport.Load() + b.other.Load()
				}
				if n := s - last; n > 0 {
					fmt.Printf("  t=%4ds target %6d/s  ok %8.0f/s  503 %7.0f/s  other errors %6.0f/s\n", s, target(max(0, s-1)),
						float64(ok)/float64(n), float64(slow)/float64(n), float64(errs)/float64(n))
				}
				last = s
			}
		}
	}()
	wg.Wait()
	close(stopProg)

	rep := &LoadReport{Tool: "s3accept", Mode: "load", Store: e.O.Store, Target: target0(e, base), Started: t0.UTC(),
		Finished: time.Now().UTC(), Params: p, Lanes: lanes}
	var all []time.Duration
	// Bucket nsec holds the answers to PUTs sent in the last instant: in the
	// totals, not in the series.
	for i := 0; i <= nsec; i++ {
		b := buckets[i]
		sort.Slice(b.lat, func(x, y int) bool { return b.lat[x] < b.lat[y] })
		all = append(all, b.lat...)
		ls := &LoadSecond{T: i, Target: target(i), OK: b.ok.Load(), SlowDown: b.slow.Load(), Other5xx: b.s5xx.Load(),
			Conflict: b.c409.Load(), Precond: b.c412.Load(), Forbidden: b.c403.Load(), Transport: b.transport.Load(),
			OtherErr: b.other.Load(), Missed: b.missed.Load(), P50ms: pctMs(b.lat, 0.5), P99ms: pctMs(b.lat, 0.99)}
		if i < nsec {
			rep.Seconds = append(rep.Seconds, ls)
		}
		t := &rep.Totals
		t.OK += ls.OK
		t.SlowDown += ls.SlowDown
		t.Other5xx += ls.Other5xx
		t.Conflict += ls.Conflict
		t.Precond += ls.Precond
		t.Forbidden += ls.Forbidden
		t.Transport += ls.Transport
		t.OtherErr += ls.OtherErr
		t.Missed += ls.Missed
	}
	sort.Slice(all, func(x, y int) bool { return all[x] < all[y] })
	rep.Totals.P50ms, rep.Totals.P99ms = pctMs(all, 0.5), pctMs(all, 0.99)
	rep.Summary = loadSummary(rep)

	if !keep {
		rep.Cleanup = loadCleanup(context.Background(), e, base, acked)
	} else {
		rep.Cleanup = []string{fmt.Sprintf("--keep: %d objects left under %s/", len(acked), base)}
	}
	for _, l := range append(rep.Summary, rep.Cleanup...) {
		fmt.Println("  " + l)
	}
	f, err := os.Create(out)
	if err == nil {
		enc := json.NewEncoder(f)
		enc.SetIndent("", " ")
		err = enc.Encode(rep)
		f.Close()
	}
	if err != nil {
		fatal(err)
	}
	fmt.Printf("JSON report: %s\n", out)
	if rep.Totals.Forbidden > 0 {
		return 2 // a permissions problem, not a throughput result
	}
	return 0
}

func target0(e *Env, base string) map[string]any {
	return map[string]any{"bucket": e.O.Bucket, "prefix": base, "endpoint": orDefault(e.O.Endpoint, "aws"), "region": e.O.Region}
}

func pctMs(sorted []time.Duration, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := max(0, min(int(p*float64(len(sorted))+0.5)-1, len(sorted)-1))
	return float64(sorted[i].Microseconds()) / 1000
}

// loadSummary: what the run says about the store's limit.
func loadSummary(r *LoadReport) []string {
	t := r.Totals
	sent := t.OK + t.SlowDown + t.Other5xx + t.Conflict + t.Precond + t.Forbidden + t.Transport + t.OtherErr
	out := []string{fmt.Sprintf("sent %d PUTs: ok %d, 503 SlowDown %d (%.2f%%), other 5xx %d, 409 %d, 412 %d, 403 %d, transport %d, other %d; missed tokens %d; latency p50 %.1f ms p99 %.1f ms",
		sent, t.OK, t.SlowDown, 100*float64(t.SlowDown)/float64(max(sent, 1)), t.Other5xx, t.Conflict, t.Precond, t.Forbidden, t.Transport, t.OtherErr, t.Missed, t.P50ms, t.P99ms)}
	first := -1
	for _, s := range r.Seconds {
		if s.SlowDown > 0 {
			first = s.T
			break
		}
	}
	if first < 0 {
		out = append(out, "no 503 SlowDown at any second")
	} else {
		out = append(out, fmt.Sprintf("first 503 SlowDown at t=%ds (target %d/s)", first, r.Seconds[first].Target))
	}
	// Per minute: achieved ok/s and the 503 share, to see whether the store
	// splits the prefix under sustained load (the share should fall).
	var mins []string
	for m := 0; m*60 < len(r.Seconds); m++ {
		var ok, slow, all int64
		n := 0
		for _, s := range r.Seconds[m*60 : min(len(r.Seconds), (m+1)*60)] {
			ok += s.OK
			slow += s.SlowDown
			all += s.OK + s.SlowDown + s.Other5xx + s.Conflict + s.Precond + s.Forbidden + s.Transport + s.OtherErr
			n++
		}
		mins = append(mins, fmt.Sprintf("m%d %.0f ok/s %.1f%% 503", m, float64(ok)/float64(n), 100*float64(slow)/float64(max(all, 1))))
	}
	out = append(out, "per minute: "+strings.Join(mins, "; "))
	if t.Missed > 0 {
		out = append(out, fmt.Sprintf("WARN %d tokens missed: the client could not keep up; raise --load-workers or run more pods", t.Missed))
	}
	return out
}

// loadCleanup deletes the acknowledged keys (8 parallel DeleteObjects of
// 1,000), then LISTs the load prefix for anything an unanswered PUT left.
func loadCleanup(ctx context.Context, e *Env, base string, acked []string) []string {
	var deleted, failed atomic.Int64
	batches := make(chan []string)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for b := range batches {
				ids := make([]types.ObjectIdentifier, 0, len(b))
				for _, k := range b {
					ids = append(ids, types.ObjectIdentifier{Key: aws.String(k)})
				}
				cctx, cancel := context.WithTimeout(ctx, 2*e.O.Timeout)
				out, err := e.S3.DeleteObjects(cctx, &s3.DeleteObjectsInput{Bucket: &e.O.Bucket, Delete: &types.Delete{Objects: ids, Quiet: aws.Bool(true)}})
				cancel()
				if err != nil {
					failed.Add(int64(len(b)))
					continue
				}
				failed.Add(int64(len(out.Errors)))
				deleted.Add(int64(len(b) - len(out.Errors)))
			}
		}()
	}
	for i := 0; i < len(acked); i += 1000 {
		batches <- acked[i:min(i+1000, len(acked))]
	}
	close(batches)
	wg.Wait()
	lines := []string{fmt.Sprintf("cleanup: deleted %d acknowledged objects (%d failed)", deleted.Load(), failed.Load())}
	r := &Result{ID: "cleanup"}
	cleanupPrefix(ctx, e, base+"/", r)
	lines = append(lines, "cleanup: "+r.Summary)
	return lines
}
