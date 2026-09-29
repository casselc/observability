package dst

// A deterministic simulation of the Go edge's commit path: the real edge
// (encoding, lanes, the commit protocol) and the real aws-sdk-go-v2 client
// (signing, retries, error parsing) against the in-memory S3 emulator
// (../internal/s3emu, a port of the Rust consumer's), over
// httptest.NewTestServer's in-memory network, in a testing/synctest bubble
// where time is fake: a 10 s PUT timeout or a copy landing 90 s late costs
// no wall time. The Go counterpart of otap-rs's dst_net.rs
// (research/go-verification.md §5; VERIFICATION.md DST "Go edge").
//
// Each seed draws a fault menu (swarm: a subset of the faults, each seed its
// own) and a schedule. Several senders push requests concurrently into an
// edge with two lanes per namespace, retrying each until it is ACKed, as
// the exporter's queue does. A "consumer" closes logs by tombstones at
// random. Midway the faults stop; the run ends when every request is ACKed
// and every late copy has landed. Invariants:
//
//   - exactly once: every request's content is in exactly one data object
//     (none lost, none twice), even with late-landing copies (CAST 50);
//   - a closed log stays closed: every tombstone the consumer wrote is
//     still there, and no data object follows one in its epoch;
//   - bounded: every PushTraces call returns within a fake-time bound, and
//     sends a bounded number of PUTs (CAST 39 as a liveness property: no
//     call spins on a slot);
//   - progress: once the faults stop, every request is ACKed by the first
//     call that starts after that;
//   - linearizable: every request the SDK sent (and the consumer's
//     tombstones), with the answers it got or lost, fits one order of the
//     writes on a create-only register per key (../../casreg, porcupine).
//
// Seeds: EDGE_DST_SEEDS (default 24) from EDGE_DST_SEED0 (default 1). The
// nightly `edge-dst` job sweeps thousands. A failure prints the seed and
// its menu; rerun with EDGE_DST_SEED0=<seed> EDGE_DST_SEEDS=1 and
// EDGE_DST_LOG=1 for the request log. The SDK's retry jitter is not seeded,
// so a replay has the same faults in the same order, not byte-identical
// timing.

import (
	"cmp"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http/httptest"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/casselc/observability/otel-chdb/casreg"
	"github.com/casselc/observability/otel-chdb/parquetgo"
	"github.com/casselc/observability/otel-chdb/parquetgo/commit"
	"github.com/casselc/observability/otel-chdb/parquetgo/edge"
	"github.com/casselc/observability/otel-chdb/parquetgo/internal/s3emu"
	"github.com/casselc/observability/otel-chdb/testgate/tracetag"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// dstFault is one entry of the fault menu.
type dstFault struct {
	name  string
	fault s3emu.Fault
	ops   string // the ops it applies to: "PUT-CREATE", "HEAD", or both
}

var dstMenu = []dstFault{
	{"put_refuse", s3emu.Refuse, "PUT-CREATE"},
	{"put_error_after", s3emu.ErrorAfter, "PUT-CREATE"},
	{"put_drop_after", s3emu.DropAfter, "PUT-CREATE"},
	{"put_drop_before", s3emu.DropBefore, "PUT-CREATE"},
	{"put_late", s3emu.Late, "PUT-CREATE"},
	{"put_slow", s3emu.Slow, "PUT-CREATE"},
	{"put_hang", s3emu.Slow, "PUT-CREATE"}, // a hung store: no answer for 3 h (CAST 39)
	{"head_refuse", s3emu.Refuse, "HEAD"},
	{"head_drop", s3emu.DropBefore, "HEAD"},
	{"head_slow", s3emu.Slow, "HEAD"},
	{"tombstones", s3emu.None, ""}, // the consumer closes logs
	{"heartbeats", s3emu.None, ""}, // the exporter's heartbeat loop (edge.Heartbeats) shares the lanes
}

type dstOpts struct {
	laneMut  commit.Mutation
	storeMut s3emu.Mutation
	// putTimeout replaces the edge's PUT timeout (the liveness mutant: a
	// lane that waits on a hung PUT for an hour)
	putTimeout time.Duration
	// beatEvery replaces the heartbeat loop's interval (the mutant: a loop
	// that never beats an idle lane)
	beatEvery time.Duration
}

// dstResult is what one seed found.
type dstResult struct {
	violations []string
	menu       []string
	requests   int
	calls      int
	puts       int
	ops        int // object requests in the linearizability history
	late       int // late copies that applied
	delayed    int // requests delayed (Late, Slow)
	tombs      int
	beats      int // heartbeat objects in the traces lanes
	maxCall    time.Duration
}

func (r *dstResult) fail(format string, args ...any) {
	r.violations = append(r.violations, fmt.Sprintf(format, args...))
}

const (
	dstPutTimeout  = 10 * time.Second
	dstHeadTimeout = 2 * time.Second
	// A call's bound: MaxResends+1 PUTs and as many HEADs, each with up to
	// three SDK attempts at its timeout plus the SDK's backoff (≤ 20 s).
	dstCallBound = time.Duration(commit.MaxResends+1) * 3 * (dstPutTimeout + dstHeadTimeout + 20*time.Second)
	// SDK attempts per request (the default retryer).
	dstAttempts = 3
	// the heartbeat loop's interval and birth timeout (s3pqexporter's
	// defaults are 30 s and 30 s)
	dstBeatInterval = 30 * time.Second
	dstBeatBirth    = 30 * time.Second
)

func dstSimulate(t *testing.T, seed uint64, o dstOpts) *dstResult {
	res := &dstResult{}
	simStart := time.Now()
	rng := rand.New(rand.NewPCG(seed, 0xed6e))
	var rmu sync.Mutex
	intn := func(n int) int { rmu.Lock(); defer rmu.Unlock(); return rng.IntN(n) }

	// swarm: this seed's menu
	var menu []dstFault
	for _, f := range dstMenu {
		if intn(2) == 0 {
			menu = append(menu, f)
		}
	}
	tombs, beats := false, false
	var faults []dstFault
	for _, f := range menu {
		res.menu = append(res.menu, f.name)
		switch f.name {
		case "tombstones":
			tombs = true
		case "heartbeats":
			beats = true
		default:
			faults = append(faults, f)
		}
	}
	rate := 2 + intn(4) // a fault on about 1 in rate requests of its ops
	res.menu = append(res.menu, "rate=1/"+strconv.Itoa(rate))

	emu := s3emu.New("dst")
	emu.Mutation = o.storeMut
	var delayed atomic.Int64
	var faultsOn atomic.Bool
	faultsOn.Store(true)
	emu.Faults = func(op, key string) s3emu.Decision {
		if !faultsOn.Load() || len(faults) == 0 || intn(rate) != 0 {
			return s3emu.Decision{}
		}
		f := faults[intn(len(faults))]
		if !strings.Contains(f.ops, op) {
			return s3emu.Decision{}
		}
		// Late and Slow: half within 500 ms (a copy that lands before the
		// SDK's retry), half up to two minutes (after the lane moved on).
		after := time.Duration(intn(500)) * time.Millisecond
		if intn(2) == 0 {
			after = time.Duration(intn(120_000)) * time.Millisecond
		}
		if f.name == "put_hang" {
			after = 3 * time.Hour
		}
		if f.fault == s3emu.Late || f.fault == s3emu.Slow {
			delayed.Add(1)
		}
		return s3emu.Decision{Fault: f.fault, After: after}
	}
	var late atomic.Int64
	emu.OnChange = func(c s3emu.Change) {
		if c.Late {
			late.Add(1) // a late copy that applied
		}
	}
	if os.Getenv("EDGE_DST_LOG") != "" {
		emu.Log = func(s string) { t.Logf("%v %s", time.Now().Format("15:04:05.000"), s) }
	}

	// every object request the edge sends, for the linearizability check
	hist := &casreg.Recorder{}
	srv := httptest.NewTestServer(t, emu)
	hc := srv.Client()
	hc.Transport = &casreg.Transport{Base: hc.Transport, Rec: hist}
	var epochs atomic.Int64
	e, err := edge.New(edge.Config{
		S3: parquetgo.Config{URL: "http://s3.emu.test/dst/root", AccessKeyID: "k", SecretAccessKey: "s",
			S3Region: "us-east-1", Transport: hc.Transport},
		Cluster: "c1", ProducerID: "p1", Lanes: 2,
		PutTimeout: cmp.Or(o.putTimeout, dstPutTimeout), HeadTimeout: dstHeadTimeout,
		NewEpoch: func() string { return fmt.Sprintf("20000101T000000.000Z-%08x", epochs.Add(1)) },
		Mutation: o.laneMut,
	})
	if err != nil {
		t.Fatal(err)
	}
	prefix := commit.LanePrefix("root", "c1", "p1", "traces")

	// the consumer: closes a lane's log at its next slot now and then
	stop := make(chan struct{})
	var bg sync.WaitGroup
	var tombCount atomic.Int64
	var tombKeys sync.Map
	if tombs {
		bg.Add(1)
		go func() {
			defer bg.Done()
			for {
				select {
				case <-stop:
					return
				case <-time.After(time.Duration(5+intn(60)) * time.Second):
				}
				l := e.Lane("traces")[intn(2)]
				ep, next, _ := l.State()
				if ep == "" {
					continue
				}
				k := commit.SlotKey(prefix, ep, next)
				c := hist.Begin(1000, casreg.Input{Key: "dst/" + k, Op: casreg.Create, Value: dstEmptyMD5})
				put := emu.Put(k, nil, map[string]string{commit.MetaKind: commit.KindTomb})
				c.End(casreg.Output{Result: map[bool]casreg.Result{true: casreg.OK, false: casreg.Failed}[put]})
				if put {
					tombCount.Add(1)
					tombKeys.Store(k, true)
				}
			}
		}()
	}

	// the heartbeats: births first (the exporter starts before its receivers),
	// then a heartbeat per lane idle for dstBeatInterval, as s3pqexporter
	// runs them, with no deadline (CAST 39): the edge's own timeouts bound them
	faultsOff := time.Now().Add(time.Duration(60+intn(240)) * time.Second)
	hbCtx, hbStop := context.WithCancel(context.Background())
	defer hbStop()
	var hbDone <-chan struct{}
	var lateWarn atomic.Value // a keep-alive heartbeat that failed well after the faults stopped
	if beats {
		_, hbDone = e.Heartbeats(hbCtx, cmp.Or(o.beatEvery, dstBeatInterval), dstBeatBirth, func(ns string, err error) {
			if time.Now().After(faultsOff.Add(dstCallBound)) {
				lateWarn.CompareAndSwap(nil, fmt.Sprintf("%s: %v", ns, err))
			}
		})
	}

	// the senders: each pushes its requests in order, retrying each until ACKed
	const senders, perSender = 3, 5
	var mu sync.Mutex
	var wg sync.WaitGroup
	for s := range senders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perSender {
				td := traces(1+intn(3), 100*int(seed%1000)+10*s+i)
				for attempt := 1; ; attempt++ {
					startedAfterFaults := !faultsOn.Load()
					start := time.Now()
					// No deadline, as for a heartbeat: the edge's own timeouts and
					// MaxResends must bound the call (CAST 39).
					err := e.PushTraces(context.Background(), td)
					took := time.Since(start)
					mu.Lock()
					res.calls++
					res.maxCall = max(res.maxCall, took)
					if took > dstCallBound {
						res.fail("sender %d request %d: a call took %v of fake time (bound %v)", s, i, took, dstCallBound)
					}
					if err != nil && startedAfterFaults {
						res.fail("sender %d request %d: attempt %d started after the faults stopped and failed: %v", s, i, attempt, err)
					}
					mu.Unlock()
					if err == nil {
						break
					}
					var ee *commit.ErrEncode
					if errors.As(err, &ee) || attempt > 200 {
						mu.Lock()
						res.fail("sender %d request %d: gave up after %d attempts: %v", s, i, attempt, err)
						mu.Unlock()
						break
					}
					time.Sleep(time.Duration(1+intn(5)) * time.Second) // the queue's backoff
				}
				time.Sleep(time.Duration(intn(40)) * time.Second) // the next request arrives
			}
		}()
	}
	bg.Add(1)
	go func() {
		defer bg.Done()
		time.Sleep(time.Until(faultsOff))
		faultsOn.Store(false)
	}()
	wg.Wait()
	if beats {
		// every lane idle: once the faults are over, each namespace gets its
		// heartbeat within an interval and a half (the loop ticks at half)
		time.Sleep(time.Until(faultsOff.Add(dstCallBound)))
		time.Sleep(2 * dstBeatInterval)
		for _, ns := range e.Registered() {
			if idle := e.IdleFor(ns); idle > 2*dstBeatInterval {
				res.fail("heartbeats: lane %s idle for %v after the faults stopped (interval %v)", ns, idle, dstBeatInterval)
			}
		}
		if w := lateWarn.Load(); w != nil {
			res.fail("heartbeats: a heartbeat failed after the faults stopped: %v", w)
		}
		hbStop()
		select {
		case <-hbDone:
		case <-time.After(dstCallBound):
			res.fail("heartbeats: the loop did not return within %v of its stop", dstCallBound)
		}
	}
	close(stop)
	bg.Wait()
	emu.Wait()  // every late copy has landed (or lost its condition)
	srv.Close() // and every request, answered or cut, has finished
	synctest.Wait()

	res.requests = senders * perSender
	res.puts = emu.Count("PUT-CREATE")
	res.late = int(late.Load())
	res.delayed = int(delayed.Load())
	res.tombs = int(tombCount.Load())
	beatCalls := 0 // at most: a birth round per half second, then a round per tick
	if beats {
		beatCalls = (int(time.Since(simStart)/(dstBeatInterval/2)) + 1 + int(dstBeatBirth/(500*time.Millisecond)) + 1) * len(e.Registered())
	}
	if limit := (res.calls + beatCalls) * (commit.MaxResends + 1) * dstAttempts; res.puts > limit+res.tombs {
		res.fail("%d PUTs for %d calls: more than %d", res.puts, res.calls, limit)
	}

	// exactly once, and closed logs stay closed
	byContent := map[string][]string{}
	tombAt := map[string]uint64{} // epoch → the lowest tombstoned seq
	type slot struct {
		epoch string
		seq   uint64
		key   string
	}
	var data []slot
	for _, k := range emu.Keys(prefix + "/") {
		obj, _ := emu.Get(k)
		ep, seq, ok := commit.ParseSlotKey(prefix, k)
		if !ok {
			continue
		}
		switch obj.Meta[commit.MetaKind] {
		case commit.KindTomb:
			if s, ok := tombAt[ep]; !ok || seq < s {
				tombAt[ep] = seq
			}
		case commit.KindData:
			byContent[obj.Meta[commit.MetaContent]] = append(byContent[obj.Meta[commit.MetaContent]], k)
			data = append(data, slot{ep, seq, k})
		case commit.KindBeat:
			res.beats++
			data = append(data, slot{ep, seq, k}) // a heartbeat after a tombstone reopens a closed log too
		}
	}
	for c, keys := range byContent {
		if len(keys) > 1 {
			res.fail("content %s committed %d times: %v", c, len(keys), keys)
		}
	}
	if len(byContent) != res.requests {
		res.fail("%d distinct contents committed for %d ACKed requests", len(byContent), res.requests)
	}
	tombKeys.Range(func(k, _ any) bool {
		if o, ok := emu.Get(k.(string)); !ok || o.Meta[commit.MetaKind] != commit.KindTomb {
			res.fail("the consumer's tombstone %s was replaced by %v", k, o.Meta)
		}
		return true
	})
	for _, d := range data {
		if s, ok := tombAt[d.epoch]; ok && d.seq > s {
			res.fail("data %s after the tombstone at seq %d of its epoch", d.key, s)
		}
	}
	// the store's history, as the edge and the consumer saw it
	res.ops = hist.Len()
	if r, _ := hist.Check(time.Minute); r == porcupine.Illegal {
		res.fail("the object history (%d requests, %d unanswered) is not linearizable", hist.Len(), hist.Pending())
	}
	return res
}

var dstEmptyMD5 = func() string { h := md5.Sum(nil); return hex.EncodeToString(h[:]) }()

func dstSeeds(def int) (uint64, int) {
	seed0, n := uint64(1), def
	if v, err := strconv.ParseUint(os.Getenv("EDGE_DST_SEED0"), 10, 64); err == nil {
		seed0 = v
	}
	if v, err := strconv.Atoi(os.Getenv("EDGE_DST_SEEDS")); err == nil && v > 0 {
		n = v
	}
	return seed0, n
}

func runSeed(t *testing.T, seed uint64, o dstOpts) *dstResult {
	var res *dstResult
	synctest.Test(t, func(t *testing.T) { res = dstSimulate(t, seed, o) })
	return res
}

func TestDSTEdgeCommit(t *testing.T) {
	tracetag.Covers(t, "DST", "CAST-39", "CAST-50", "CAST-64", "H-1", "H-2")
	seed0, n := dstSeeds(24)
	var calls, puts, late, delayed, tombs, beats, requests, ops int
	var worst time.Duration
	menus := map[string]int{}
	for seed := seed0; seed < seed0+uint64(n); seed++ {
		res := runSeed(t, seed, dstOpts{})
		if len(res.violations) > 0 {
			t.Fatalf("seed %d (menu %v): %d violations:\n  %s\nrerun: EDGE_DST_SEED0=%d EDGE_DST_SEEDS=1 EDGE_DST_LOG=1 go test -run TestDSTEdgeCommit .",
				seed, res.menu, len(res.violations), strings.Join(res.violations, "\n  "), seed)
		}
		calls, puts, late, delayed, tombs, requests = calls+res.calls, puts+res.puts, late+res.late, delayed+res.delayed, tombs+res.tombs, requests+res.requests
		beats += res.beats
		worst = max(worst, res.maxCall)
		ops += res.ops
		for _, m := range res.menu {
			menus[m]++
		}
	}
	var ms []string
	for m, c := range menus {
		ms = append(ms, fmt.Sprintf("%s:%d", m, c))
	}
	sort.Strings(ms)
	t.Logf("%d seeds from %d: %d requests ACKed exactly once in %d calls, %d PUTs, %d delayed requests of which %d landed late, %d tombstones, %d heartbeats; %d object requests checked linearizable; longest call %v (bound %v); menus %v",
		n, seed0, requests, calls, puts, delayed, late, tombs, beats, ops, worst, dstCallBound, ms)
}

// Each planted bug is caught by some seed of the default range.
func TestDSTCatchesMutants(t *testing.T) {
	tracetag.Covers(t, "DST", "CAST-39", "CAST-50")
	for _, c := range []struct {
		name string
		o    dstOpts
	}{
		{"lane_retry_new_key", dstOpts{laneMut: commit.RetryNewKey}},
		{"lane_no_halt", dstOpts{laneMut: commit.NoHalt}},
		{"store_ignores_if_none_match", dstOpts{storeMut: s3emu.IgnoreIfNoneMatch}},
		{"lane_waits_an_hour_on_a_put", dstOpts{putTimeout: time.Hour}},
		{"heartbeats_never_beat_an_idle_lane", dstOpts{beatEvery: 1000 * time.Hour}},
	} {
		t.Run(c.name, func(t *testing.T) {
			for seed := uint64(1); seed <= 40; seed++ {
				if res := runSeed(t, seed, c.o); len(res.violations) > 0 {
					t.Logf("caught by seed %d (menu %v): %s", seed, res.menu, res.violations[0])
					return
				}
			}
			t.Fatal("not caught in 40 seeds")
		})
	}
	// the store mutant is also caught by the history check alone
	t.Run("store_ignores_if_none_match_by_linearizability", func(t *testing.T) {
		for seed := uint64(1); seed <= 40; seed++ {
			for _, v := range runSeed(t, seed, dstOpts{storeMut: s3emu.IgnoreIfNoneMatch}).violations {
				if strings.Contains(v, "not linearizable") {
					t.Logf("caught by seed %d: %s", seed, v)
					return
				}
			}
		}
		t.Fatal("the history check never caught it in 40 seeds")
	})
}

// traces is n spans named for seed (edge_test.go's helper).
func traces(n int, seed int) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "svc")
	ss := rs.ScopeSpans().AppendEmpty()
	for i := range n {
		s := ss.Spans().AppendEmpty()
		s.SetName(fmt.Sprintf("op-%d-%d", seed, i))
		s.SetTraceID(pcommon.TraceID{byte(seed), byte(seed >> 8), byte(i), 1})
		s.SetStartTimestamp(pcommon.Timestamp(1_700_000_000_000_000_000 + int64(i)))
		s.SetEndTimestamp(pcommon.Timestamp(1_700_000_000_000_000_100 + int64(i)))
	}
	return td
}
