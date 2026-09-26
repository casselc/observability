package modelcheck

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/casselc/observability/otel-chdb/parquetgo/commit"
	"github.com/casselc/observability/otel-chdb/parquetgo/edge"
	"github.com/casselc/observability/quintgo/qtrace"
	"github.com/casselc/observability/quintgo/validate"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func backend() string {
	if b := os.Getenv("QUINTGO_BACKEND"); b != "" {
		return b
	}
	// The Rust evaluator (v0.6.0) overflows its stack on a long .then chain.
	return "typescript"
}

func runs(def int) int {
	if n, err := strconv.Atoi(os.Getenv("MODELCHECK_RUNS")); err == nil && n > 0 {
		return n
	}
	return def
}

func check(t *testing.T, binding string, tr *Translator) (*validate.Report, error) {
	t.Helper()
	return checkSteps(t, binding, tr.Steps())
}

func checkSteps(t *testing.T, binding string, steps []qtrace.Step) (*validate.Report, error) {
	t.Helper()
	dir := t.TempDir()
	if k := os.Getenv("QUINTGO_KEEP"); k != "" {
		dir = k + "/" + strings.ReplaceAll(t.Name(), "/", "_")
		_ = os.MkdirAll(dir, 0o755)
	}
	c, err := validate.NewChecker(binding, validate.Options{Backend: backend(), Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	return c.Check(context.Background(), steps)
}

// ---- s3Inline: one lane (traces) ------------------------------------------------

func traceReq(p int) ptrace.Traces {
	td := ptrace.NewTraces()
	s := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	s.SetName(fmt.Sprintf("request-%d", p))
	s.SetStartTimestamp(pcommon.Timestamp(1_790_000_000_000_000_000 + int64(p)))
	return td
}

type inst struct {
	e     *edge.Edge
	tag   string
	alive bool
}

type harness struct {
	t     *testing.T
	st    *commit.MemStore
	tr    *Translator
	edges []*inst
	n     int
	mut   commit.Mutation
	reqs  map[int]ptrace.Traces
	cons  *Consumer
	ctx   context.Context
	stats commit.Stats
	notes map[string]int
}

func newHarness(t *testing.T, payloads int, mut commit.Mutation) *harness {
	st := commit.NewMemStore()
	tr := NewTranslator("m", map[string]string{"traces": ""})
	st.OnApply, st.OnLose = tr.OnApply, tr.OnLose
	h := &harness{t: t, st: st, tr: tr, mut: mut, reqs: map[int]ptrace.Traces{}, ctx: context.Background(), notes: map[string]int{}}
	h.cons = &Consumer{T: tr, NS: "traces", Store: st}
	for p := 1; p <= payloads; p++ {
		td := traceReq(p)
		b, _ := (&ptrace.ProtoMarshaler{}).MarshalTraces(td)
		tr.Payload("traces", commit.ContentHash("traces", b), p)
		h.reqs[p] = td
	}
	h.start(false, true)
	return h
}

// start an edge instance: the first, a restart (the others die), or a zombie start.
func (h *harness) start(zombie, first bool) {
	tag := strconv.Itoa(h.n)
	h.n++
	e, err := edge.New(edge.Config{Store: h.st, Prefix: "m", ProducerID: "p", Observer: h.tr.Tagged(tag), Mutation: h.mut})
	if err != nil {
		h.t.Fatal(err)
	}
	lane := "traces/0#" + tag
	if first {
		h.tr.Bind("traces", lane)
	} else {
		h.tr.NewIncarnation("traces", lane, zombie)
	}
	if !zombie {
		for _, o := range h.edges {
			o.alive = false
		}
	}
	h.edges = append(h.edges, &inst{e: e, tag: tag, alive: true})
}

func (h *harness) live() []*inst {
	var out []*inst
	for _, o := range h.edges {
		if o.alive {
			out = append(out, o)
		}
	}
	return out
}

func (h *harness) queued() []int {
	var out []int
	for p := 1; p <= len(h.reqs); p++ {
		if h.tr.Queued("traces", p) {
			out = append(out, p)
		}
	}
	return out
}

func (h *harness) push(o *inst, p int) error {
	err := o.e.PushTraces(h.ctx, h.reqs[p])
	st := o.e.Stats()
	h.stats.ResolvedOwn.Add(0)
	_ = st
	return err
}

func (h *harness) tally() map[string]int64 {
	out := map[string]int64{}
	for _, o := range h.edges {
		s := o.e.Stats()
		out["committed"] += s.Committed.Load()
		out["resolved_own"] += s.ResolvedOwn.Load()
		out["resent"] += s.Resent.Load()
		out["learned_other"] += s.LearnedOther.Load()
		out["halted"] += s.Halted.Load()
		out["known_skipped"] += s.KnownSkipped.Load()
	}
	return out
}

var faults = []commit.Fault{commit.ApplyLoseAnswer, commit.Drop, commit.Hold}

// random drives one seeded run: pushes under injected faults (ambiguous,
// dropped and late PUTs, HEADs without an answer), late PUTs landing, a
// consumer ingesting and closing superseded epochs with tombstones,
// restarts and zombie writers; then drains.
func (h *harness) random(rng *rand.Rand, steps, maxInc int) {
	for i := 0; i < steps; i++ {
		switch r := rng.Intn(100); {
		case r < 50:
			q := h.queued()
			lv := h.live()
			if len(q) == 0 || len(lv) == 0 {
				continue
			}
			o := lv[len(lv)-1]
			if rng.Intn(4) == 0 {
				o = lv[rng.Intn(len(lv))]
			}
			if rng.Intn(2) == 0 {
				h.st.Inject(faults[rng.Intn(len(faults))])
				if rng.Intn(3) == 0 {
					h.st.Inject(commit.HeadFail)
				}
			}
			_ = h.push(o, q[rng.Intn(len(q))])
		case r < 60:
			h.st.ReleaseHeld()
		case r < 85:
			if c := h.cons.Candidates(); len(c) > 0 {
				h.cons.Step(c[rng.Intn(len(c))])
			}
		case r < 90:
			if h.n < maxInc {
				h.start(false, false)
			}
		default:
			if h.n < maxInc {
				h.start(true, false)
			}
		}
	}
	h.drain()
}

func (h *harness) drain() {
	h.st.ClearFaults()
	h.st.ReleaseHeld()
	for i := 0; i < 20 && len(h.queued()) > 0; i++ {
		lv := h.live()
		if len(lv) == 0 {
			break
		}
		_ = h.push(lv[len(lv)-1], h.queued()[0])
	}
	for i := 0; i < 50; i++ {
		c := h.cons.Candidates()
		if len(c) == 0 {
			break
		}
		h.cons.Step(c[0])
	}
}

func TestLaneConformsToS3Inline(t *testing.T) {
	n := runs(12)
	total := map[string]int64{}
	steps := 0
	for seed := 1; seed <= n; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(int64(seed)))
			h := newHarness(t, 5, commit.NoMutation)
			h.random(rng, 70, 5)
			rep, err := check(t, "s3inline.binding.yaml", h.tr)
			if err != nil {
				t.Fatal(err)
			}
			for k, v := range h.tally() {
				total[k] += v
			}
			steps += len(h.tr.Steps())
			if !rep.OK() {
				t.Fatalf("%s\nnotes: %v", rep.Summary(), h.tr.Notes)
			}
			t.Logf("%d model steps, %d incarnations, %v, notes %d", len(h.tr.Steps()), h.n, h.tally(), len(h.tr.Notes))
		})
	}
	t.Logf("%d runs, %d model steps; lane events %v", n, steps, total)
}

// The scripted faults, each once, so every protocol path is in a checked run.
func TestLaneScriptedFaults(t *testing.T) {
	h := newHarness(t, 6, commit.NoMutation)
	o := func() *inst { lv := h.live(); return lv[len(lv)-1] }
	_ = h.push(o(), 1)                  // clean commit
	h.st.Inject(commit.ApplyLoseAnswer) // landed, answer lost: resolved as ours
	_ = h.push(o(), 2)
	h.st.Inject(commit.Hold) // late: HEAD finds it free, resend wins; the late copy is 412
	h.st.Inject(commit.HeadFail)
	if err := h.push(o(), 3); err == nil {
		t.Fatal("want unresolved")
	}
	h.st.ReleaseHeld()       // the held PUT lands late in the unresolved slot
	_ = h.push(o(), 4)       // switchPayload: 412, learns 3, appends 4 after it
	h.st.Inject(commit.Drop) // lost: HEAD finds the slot free, the same bytes are resent
	_ = h.push(o(), 6)
	h.start(true, false) // a zombie start: the old writer keeps its epoch
	h.cons.Step(1)
	h.cons.Step(1)
	h.cons.Step(1)
	h.cons.Step(1)
	h.cons.Step(1)            // ingest 1, 2, 3, 4, 6 of epoch 1
	h.cons.Step(1)            // tombstone epoch 1's head (it is superseded)
	_ = h.push(h.edges[0], 5) // the zombie meets the tombstone: halts, new epoch
	h.drain()
	rep, err := check(t, "s3inline.binding.yaml", h.tr)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.OK() {
		t.Fatalf("%s\nnotes %v", rep.Summary(), h.tr.Notes)
	}
	tl := h.tally()
	if tl["resolved_own"] == 0 || tl["resent"] == 0 || tl["learned_other"] == 0 || tl["halted"] == 0 {
		t.Fatalf("a path was not taken: %v", tl)
	}
	t.Logf("%d model steps, conforms; %v", len(h.tr.Steps()), tl)
}

// Mutants: the model check must reject them.
func TestLaneMutantsCaught(t *testing.T) {
	for _, m := range []struct {
		name string
		mut  commit.Mutation
		run  func(h *harness)
	}{
		{"retryNewKey", commit.RetryNewKey, func(h *harness) {
			h.st.Inject(commit.Drop)
			_ = h.push(h.live()[0], 1)
		}},
		{"noHalt", commit.NoHalt, func(h *harness) {
			_ = h.push(h.live()[0], 1)
			h.start(true, false)
			h.cons.Step(1)
			h.cons.Step(1) // tombstone at slot 1 of epoch 1
			_ = h.push(h.edges[0], 2)
		}},
	} {
		t.Run(m.name, func(t *testing.T) {
			h := newHarness(t, 2, m.mut)
			m.run(h)
			rep, err := check(t, "s3inline.binding.yaml", h.tr)
			if err != nil {
				t.Fatal(err)
			}
			if rep.OK() {
				t.Fatalf("mutant %s conforms", m.name)
			}
			t.Logf("caught: %s", firstLine(rep.Summary()))
		})
	}
}

// firstLine is the failing step of a report: "step [i] ...: quint: <call>".
func firstLine(s string) string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		l = strings.TrimSpace(l)
		if strings.HasPrefix(l, "step [") || strings.HasPrefix(l, "quint:") || strings.Contains(l, ": FAIL") {
			out = append(out, l)
		}
	}
	if len(out) == 0 {
		return s
	}
	return strings.Join(out, " | ")
}

// ---- s3InlineMetrics: a request over two lanes ---------------------------------------

func metricsReq(p int) pmetric.Metrics {
	md := pmetric.NewMetrics()
	sm := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty()
	ts := pcommon.Timestamp(1_790_000_000_000_000_000 + int64(p)*1e9)
	g := sm.Metrics().AppendEmpty()
	g.SetName("g")
	gp := g.SetEmptyGauge().DataPoints().AppendEmpty()
	gp.SetTimestamp(ts)
	gp.SetDoubleValue(float64(p))
	s := sm.Metrics().AppendEmpty()
	s.SetName("s")
	sp := s.SetEmptySum().DataPoints().AppendEmpty()
	sp.SetTimestamp(ts)
	sp.SetIntValue(int64(p))
	return md
}

type mharness struct {
	t      *testing.T
	st     *commit.MemStore
	tr     *Translator
	e      *edge.Edge
	n      int
	mut    commit.Mutation
	reqs   map[int]pmetric.Metrics
	acked  map[int]bool
	leases int
	ctx    context.Context
	nacks  int // pushes answered with an error (a part unresolved)
	half   int // of those, with another part committed
}

var mns = map[string]string{"metrics_gauge": "g", "metrics_sum": "s"}

func newMHarness(t *testing.T, mut commit.Mutation) *mharness {
	st := commit.NewMemStore()
	tr := NewTranslator("m", mns)
	st.OnApply, st.OnLose = tr.OnApply, tr.OnLose
	h := &mharness{t: t, st: st, tr: tr, mut: mut, reqs: map[int]pmetric.Metrics{}, acked: map[int]bool{}, leases: 1, ctx: context.Background()}
	for p := 1; p <= 2; p++ {
		md := metricsReq(p)
		b, _ := (&pmetric.ProtoMarshaler{}).MarshalMetrics(md)
		for ns := range mns {
			tr.Payload(ns, commit.ContentHash(ns, b), p)
		}
		h.reqs[p] = md
	}
	h.start(true)
	return h
}

func (h *mharness) start(first bool) {
	tag := strconv.Itoa(h.n)
	h.n++
	e, err := edge.New(edge.Config{Store: h.st, Prefix: "m", ProducerID: "p", MetricsLayout: edge.ClickstackTables,
		Observer: h.tr.Tagged(tag), Mutation: h.mut})
	if err != nil {
		h.t.Fatal(err)
	}
	lanes := map[string]string{}
	for ns := range mns {
		lanes[ns] = ns + "/0#" + tag
	}
	if first {
		for ns, l := range lanes {
			h.tr.Bind(ns, l)
		}
	} else {
		var pending []int
		for p := 1; p <= 2; p++ {
			if !h.acked[p] {
				pending = append(pending, p)
			}
		}
		h.tr.Crash(pending, lanes)
		h.leases++
	}
	h.e = e
}

func (h *mharness) push(p int) {
	before := h.e.Stats().Committed.Load() + h.e.Stats().ResolvedOwn.Load()
	if err := h.e.PushMetrics(h.ctx, h.reqs[p]); err == nil {
		h.acked[p] = true
		h.tr.AckRequest(p)
	} else {
		h.nacks++
		if h.e.Stats().Committed.Load()+h.e.Stats().ResolvedOwn.Load() > before {
			h.half++
		}
	}
}

func (h *mharness) random(rng *rand.Rand, steps int) {
	for i := 0; i < steps; i++ {
		switch r := rng.Intn(100); {
		case r < 65:
			var pend []int
			for p := 1; p <= 2; p++ {
				if !h.acked[p] {
					pend = append(pend, p)
				}
			}
			if len(pend) == 0 {
				continue
			}
			if rng.Intn(2) == 0 {
				h.st.Inject(faults[rng.Intn(len(faults))])
				if rng.Intn(2) == 0 {
					h.st.Inject(commit.HeadFail) // the part stays unresolved: a NACK
				}
			}
			h.push(pend[rng.Intn(len(pend))])
		case r < 85:
			h.st.ReleaseHeld()
		default:
			if h.leases < 3 {
				h.start(false)
			}
		}
	}
	h.st.ClearFaults()
	h.st.ReleaseHeld()
	for i := 0; i < 6; i++ {
		for p := 1; p <= 2; p++ {
			if !h.acked[p] {
				h.push(p)
			}
		}
	}
}

func TestMetricsRequestConformsToS3InlineMetrics(t *testing.T) {
	n := runs(12)
	var nacks, half, crashes, steps int
	defer func() {
		t.Logf("%d runs, %d model steps, %d restarts; %d NACKed pushes, %d of them with another part committed", n, steps, crashes, nacks, half)
	}()
	for seed := 1; seed <= n; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			h := newMHarness(t, commit.NoMutation)
			h.random(rand.New(rand.NewSource(int64(seed))), 30)
			rep, err := check(t, "s3inlinemetrics.binding.yaml", h.tr)
			if err != nil {
				t.Fatal(err)
			}
			if !rep.OK() {
				t.Fatalf("%s\nnotes %v", rep.Summary(), h.tr.Notes)
			}
			t.Logf("%d model steps, %d epochs, acked %v, %d NACKs (%d half-committed), notes %d", len(h.tr.Steps()), h.leases, h.acked, h.nacks, h.half, len(h.tr.Notes))
			nacks, half, crashes, steps = nacks+h.nacks, half+h.half, crashes+h.leases-1, steps+len(h.tr.Steps())
		})
	}
}

// The ackOnAny mutant: the edge answers success once one object committed.
func TestMetricsAckOnAnyCaught(t *testing.T) {
	h := newMHarness(t, commit.AckOnAny)
	h.st.Inject(commit.Hold) // one part's PUT stays unresolved
	h.st.Inject(commit.HeadFail)
	h.push(1)
	rep, err := check(t, "s3inlinemetrics.binding.yaml", h.tr)
	if err != nil {
		t.Fatal(err)
	}
	if rep.OK() {
		t.Fatal("ackOnAny conforms")
	}
	// The failing step is the last one, the 2xx (quint reports a run whose
	// last step is disabled as "returned false"): without it the run conforms.
	steps := h.tr.Steps()
	if last := steps[len(steps)-1]; last.Action != "ackRequest" {
		t.Fatalf("last step %s", last.Action)
	}
	rep2, err := checkSteps(t, "s3inlinemetrics.binding.yaml", steps[:len(steps)-1])
	if err != nil || !rep2.OK() {
		t.Fatalf("the run before the 2xx does not conform: %v %s", err, rep2.Summary())
	}
	t.Logf("caught: the 2xx for request 1 (ackRequest(1)) is not a transition: its sum object is unresolved (%d steps before it conform)", len(steps)-1)
}
