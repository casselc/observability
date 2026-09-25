//go:build pbt

package chdbexporter

import (
	"encoding/json"
	"fmt"
	"os"
	execpkg "os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"hegel.dev/go/hegel"
)

// The other direction: Quint generates, Go executes. `quint run --mbt` on
// edgePublish.qnt (currentDesign) writes random traces of the model with
// the action taken and the nondeterministic picks at every step (ITF). The
// writer's actions in a trace are turned into commands for the publisher
// state machine (a push with its outcome at each phase, a rotation, a
// crash), run against the real publisher over the fake sessions, and the
// end state is compared with the model's: which requests have manifests,
// table rows and Parquet in each epoch, and what each seal counts. That is
// model-based testing of the real code with the model as the oracle.
//
// The same command lists also seed hegel: TestPBTSeededByModelTraces draws
// a model trace and a prefix of it, replays the prefix, and then lets the
// state machine continue at random from the state the model reached (after
// crashes and ambiguous writes, say), with every invariant checked.
//
// Mapping choices:
//   - a push runs when the model finishes it (Ok manifest, or a failure at
//     some phase); in-flight pushes interleave in the model, but with the
//     rotation lock a push starts and ends in one generation, and pushes of
//     one generation touch disjoint objects, so executing each push at its
//     last step preserves what is compared;
//   - a push the model loses in flight at a crash runs with a failing write
//     at its next phase, so exactly the phases the model completed land;
//   - rotateGen advances the clock one generation and opens it without a
//     push (the machine's tick), since publish.go rotates lazily;
//   - sealGen needs nothing (publish.go seals at rotation); the consumer's
//     actions are not the publisher's and are skipped.

type modelCmd struct {
	kind    string // push, rotate, crash
	payload int64
	phase   int    // push: the phase whose write fails (0 table, 1 parquet, 2 manifest), or -1
	mode    string // fail, ambiguous
}

func (c modelCmd) String() string {
	switch c.kind {
	case "push":
		if c.phase < 0 {
			return fmt.Sprintf("push q%d", c.payload)
		}
		return fmt.Sprintf("push q%d, %s %s", c.payload, []string{"table", "parquet", "manifest"}[c.phase], c.mode)
	}
	return c.kind
}

type itfState map[string]json.RawMessage

// itfVar returns a state variable, whatever module prefix quint gave it.
func itfVar(s itfState, name string) json.RawMessage {
	for k, v := range s {
		if k == name || strings.HasSuffix(k, "::"+name) {
			return v
		}
	}
	return nil
}

func itfInt(raw json.RawMessage) int64 {
	var b struct {
		Big string `json:"#bigint"`
	}
	if json.Unmarshal(raw, &b) == nil && b.Big != "" {
		n, _ := strconv.ParseInt(b.Big, 10, 64)
		return n
	}
	var n int64
	_ = json.Unmarshal(raw, &n)
	return n
}

type itfVariant struct {
	Tag   string          `json:"tag"`
	Value json.RawMessage `json:"value"`
}

// itfPick returns a nondeterministic pick of a step (Some(v) unwrapped).
func itfPick(s itfState, name string) (json.RawMessage, bool) {
	var picks map[string]itfVariant
	if json.Unmarshal(s["mbt::nondetPicks"], &picks) != nil {
		return nil, false
	}
	p, ok := picks[name]
	if !ok || p.Tag != "Some" {
		return nil, false
	}
	return p.Value, true
}

func itfSet(raw json.RawMessage) []json.RawMessage {
	var s struct {
		Set []json.RawMessage `json:"#set"`
	}
	_ = json.Unmarshal(raw, &s)
	return s.Set
}

type itfKey struct {
	Epoch, N int64
}

// itfBatchKey reads { epoch, batch } or { epoch, gen }.
func itfBatchKey(raw json.RawMessage, second string) itfKey {
	var r map[string]json.RawMessage
	_ = json.Unmarshal(raw, &r)
	return itfKey{itfInt(r["epoch"]), itfInt(r[second])}
}

func itfField(raw json.RawMessage, name string) json.RawMessage {
	var r map[string]json.RawMessage
	_ = json.Unmarshal(raw, &r)
	return r[name]
}

// linearize turns a model trace into publisher commands.
func linearize(states []itfState) []modelCmd {
	var cmds []modelCmd
	inflight := map[int64]int{} // payload -> phases completed
	for _, s := range states[1:] {
		var act string
		_ = json.Unmarshal(s["mbt::actionTaken"], &act)
		switch act {
		case "pushStart":
			p, _ := itfPick(s, "p")
			inflight[itfInt(p)] = 0
		case "writeTable", "writeParquet", "writeManifest":
			x, _ := itfPick(s, "x")
			o, _ := itfPick(s, "o")
			var out itfVariant
			_ = json.Unmarshal(o, &out)
			p := itfInt(itfField(x, "payload"))
			phase := map[string]int{"writeTable": 0, "writeParquet": 1, "writeManifest": 2}[act]
			switch {
			case out.Tag == "Ok" && phase < 2:
				inflight[p] = phase + 1
			case out.Tag == "Ok":
				cmds = append(cmds, modelCmd{kind: "push", payload: p, phase: -1})
				delete(inflight, p)
			default:
				cmds = append(cmds, modelCmd{kind: "push", payload: p, phase: phase, mode: map[string]string{"Fail": "fail", "Ambiguous": "ambiguous"}[out.Tag]})
				delete(inflight, p)
			}
		case "rotateGen":
			cmds = append(cmds, modelCmd{kind: "rotate"})
		case "crash":
			var ps []int64
			for p, done := range inflight {
				if done > 0 {
					ps = append(ps, p)
				}
			}
			sort.Slice(ps, func(i, j int) bool { return ps[i] < ps[j] })
			for _, p := range ps {
				cmds = append(cmds, modelCmd{kind: "push", payload: p, phase: inflight[p], mode: "fail"})
			}
			inflight = map[int64]int{}
			cmds = append(cmds, modelCmd{kind: "crash"})
		}
	}
	return cmds
}

// modelSetup is the fixed configuration model traces run with: the model's
// writer has a table and Parquet.
var modelSetup = pubSetup{layout: "tables+parquet", connections: 1, signals: []string{signalLogs}}

// replayCmd runs one command on the machine.
func replayCmd(m *pubMachine, c modelCmd) error {
	switch c.kind {
	case "push":
		op := ""
		if c.phase >= 0 {
			op = []string{"insert", "parquet", "manifest"}[c.phase]
		}
		pushExec(m, fmt.Sprintf("q%d", c.payload), signalLogs, 1, op, c.mode)
	case "rotate":
		m.clock = m.clock.Add(m.cfg.generation())
		return tick(m, signalLogs)
	case "crash":
		m.p.release()
		restartWorld(m)
		if err := m.start(); err != nil {
			return err
		}
		m.logf("crash; start epoch %s", m.p.epoch)
		return tick(m, signalLogs)
	}
	return nil
}

// epochIndex maps a Go epoch ("r<run>e<n>") to the model's epoch n.
func epochIndex(epoch string) int64 {
	n, _ := strconv.ParseInt(epoch[strings.LastIndexByte(epoch, 'e')+1:], 10, 64)
	return n
}

// compareWithModel checks the machine's world against the model's final
// state: per epoch, the requests with manifests, table rows and Parquet
// (as multisets), and per sealed generation the batch count of its seal.
func compareWithModel(m *pubMachine, final itfState) []string {
	var diffs []string
	payloadOf := map[string]string{}
	for _, a := range m.attempts {
		payloadOf[fmt.Sprintf("%s|%d", a.epoch, a.batch)] = a.payload
	}
	count := func(dst map[string]int, epoch int64, payload string) { dst[fmt.Sprintf("e%d:%s", epoch, payload)]++ }
	cmp := func(what string, model, impl map[string]int) {
		keys := map[string]bool{}
		for k := range model {
			keys[k] = true
		}
		for k := range impl {
			keys[k] = true
		}
		var ks []string
		for k := range keys {
			ks = append(ks, k)
		}
		sort.Strings(ks)
		for _, k := range ks {
			if model[k] != impl[k] {
				diffs = append(diffs, fmt.Sprintf("%s of %s: model %d, implementation %d", what, k, model[k], impl[k]))
			}
		}
	}

	modelMan, implMan := map[string]int{}, map[string]int{}
	for _, raw := range itfSet(itfVar(final, "manifests")) {
		bk := itfBatchKey(itfField(raw, "bk"), "batch")
		count(modelMan, bk.Epoch, fmt.Sprintf("q%d", itfInt(itfField(raw, "payload"))))
	}
	ms, ss := m.w.manifests()
	for _, mf := range ms {
		count(implMan, epochIndex(mf.ProducerEpoch), payloadOf[fmt.Sprintf("%s|%d", mf.ProducerEpoch, mf.BatchID)])
	}
	cmp("manifests", modelMan, implMan)

	modelRows, implRows := map[string]int{}, map[string]int{}
	for _, raw := range itfSet(itfVar(final, "tableRows")) {
		bk := itfBatchKey(itfField(itfField(raw, "src"), "bk"), "batch")
		count(modelRows, bk.Epoch, fmt.Sprintf("q%d", itfInt(itfField(raw, "payload"))))
	}
	seen := map[string]bool{}
	for _, t := range m.w.storedTables() {
		for _, r := range t.rows {
			k := fmt.Sprintf("%s|%d", r.epoch, r.batch)
			if !seen[k] {
				seen[k] = true
				count(implRows, epochIndex(r.epoch), payloadOf[k])
			}
		}
	}
	cmp("table batches", modelRows, implRows)

	// The model's parquet set is keyed by source, not payload: compare the
	// number of objects per epoch.
	modelPq, implPq := map[string]int{}, map[string]int{}
	for _, raw := range itfSet(itfVar(final, "parquet")) {
		modelPq[fmt.Sprintf("e%d", itfBatchKey(itfField(raw, "bk"), "batch").Epoch)]++
	}
	for key := range m.w.objects {
		if k, kind, _ := parseObjKey(key); kind == "parquet" {
			implPq[fmt.Sprintf("e%d", epochIndex(k.epoch))]++
		}
	}
	cmp("parquet objects", modelPq, implPq)

	// Seals: the model's generation n of an epoch is the n-th generation that
	// incarnation opened.
	implSeal := map[itfKey]uint64{}
	gens := map[int64][]string{}
	for _, s := range ss {
		e := epochIndex(s.ProducerEpoch)
		gens[e] = append(gens[e], s.Generation)
		implSeal[itfKey{e, int64(len(gens[e]))}] = s.Batches
	}
	for e := range gens {
		sort.Strings(gens[e])
	}
	for _, raw := range itfSet(itfVar(final, "seals")) {
		gk := itfBatchKey(itfField(raw, "gk"), "gen")
		want := uint64(len(itfSet(itfField(raw, "batches"))))
		idx := int(gk.N) - 1
		if idx >= len(gens[gk.Epoch]) {
			diffs = append(diffs, fmt.Sprintf("model seals epoch %d generation %d (%d batches); the implementation has no such seal", gk.Epoch, gk.N, want))
			continue
		}
		var got uint64
		for _, s := range ss {
			if epochIndex(s.ProducerEpoch) == gk.Epoch && s.Generation == gens[gk.Epoch][idx] {
				got = s.Batches
			}
		}
		if got != want {
			diffs = append(diffs, fmt.Sprintf("seal of epoch %d generation %d: model %d batches, implementation %d", gk.Epoch, gk.N, want, got))
		}
	}
	return diffs
}

// modelTraces runs `quint run --mbt` once per test binary and returns each
// trace's states.
var (
	modelTracesOnce sync.Once
	modelTracesVal  [][]itfState
	modelTracesErr  error
)

func modelTraces(t *testing.T) [][]itfState {
	t.Helper()
	if os.Getenv("PBT_QUINT") == "" {
		t.Skip("set PBT_QUINT=1 to generate traces with quint")
	}
	if _, err := execpkg.LookPath("quint"); err != nil {
		t.Skip("quint not installed")
	}
	modelTracesOnce.Do(func() {
		n := 20
		if v, err := strconv.Atoi(os.Getenv("PBT_QUINT_TRACES")); err == nil {
			n = v
		}
		dir, err := os.MkdirTemp("", "chdbexporter-mbt-")
		if err != nil {
			modelTracesErr = err
			return
		}
		model, _ := filepath.Abs("../model")
		backend := os.Getenv("QUINTGO_BACKEND")
		if backend == "" {
			backend = "typescript"
		}
		cmd := execpkg.Command("quint", "run", "edgePublish.qnt", "--main", "currentDesign", "--mbt",
			"--out-itf", filepath.Join(dir, "trace.itf.json"), "--n-traces", strconv.Itoa(n), "--max-samples", strconv.Itoa(n),
			"--max-steps", "40", "--seed", "20260924", "--backend", backend)
		cmd.Dir = model
		if out, err := cmd.CombinedOutput(); err != nil {
			modelTracesErr = fmt.Errorf("quint run: %v\n%s", err, out)
			return
		}
		files, _ := filepath.Glob(filepath.Join(dir, "trace*.itf.json"))
		sort.Strings(files)
		for _, f := range files {
			b, err := os.ReadFile(f)
			if err != nil {
				modelTracesErr = err
				return
			}
			var tr struct {
				States []itfState `json:"states"`
			}
			if err := json.Unmarshal(b, &tr); err != nil {
				modelTracesErr = fmt.Errorf("%s: %v", f, err)
				return
			}
			modelTracesVal = append(modelTracesVal, tr.States)
		}
	})
	if modelTracesErr != nil {
		t.Fatal(modelTracesErr)
	}
	return modelTracesVal
}

// TestModelTracesDriveThePublisher: every model trace, run on the real
// publisher, ends in the state the model ends in. Needs PBT_QUINT=1.
func TestModelTracesDriveThePublisher(t *testing.T) {
	for i, states := range modelTraces(t) {
		cmds := linearize(states)
		m, err := buildPubMachine(pubOpts{}, modelSetup)
		if err != nil {
			t.Fatal(err)
		}
		if err := tick(m, signalLogs); err != nil {
			t.Fatal(err)
		}
		for _, c := range cmds {
			if err := replayCmd(m, c); err != nil {
				t.Fatalf("trace %d: %s: %v", i, c, err)
			}
		}
		diffs := compareWithModel(m, states[len(states)-1])
		m.p.release()
		var names []string
		for _, c := range cmds {
			names = append(names, c.String())
		}
		if len(diffs) > 0 {
			t.Errorf("trace %d (%d model steps; commands: %s):\n  %s\nhistory:\n  %s", i, len(states)-1,
				strings.Join(names, "; "), strings.Join(diffs, "\n  "), strings.Join(m.log, "\n  "))
			continue
		}
		t.Logf("trace %d: %d model steps -> %d commands, end states agree: %s", i, len(states)-1, len(cmds), strings.Join(names, "; "))
	}
}

// TestPBTSeededByModelTraces: hegel draws a model trace and a prefix of its
// commands, replays the prefix on the real publisher, then continues with
// random commands from the state the model reached, checking every
// invariant of the default machine throughout. Shrinking cuts the prefix
// and the continuation. Needs PBT_QUINT=1.
func TestPBTSeededByModelTraces(t *testing.T) {
	var seeds [][]modelCmd
	for _, states := range modelTraces(t) {
		if cmds := linearize(states); len(cmds) > 0 {
			seeds = append(seeds, cmds)
		}
	}
	if len(seeds) == 0 {
		t.Skip("no model trace has writer commands")
	}
	o := pubOpts{faults: allFaults, ambiguous: true, steps: 15}
	hegel.Test(t, func(ht *hegel.T) {
		cmds := seeds[hegel.Draw(ht, hegel.Integers(0, len(seeds)-1))]
		k := hegel.Draw(ht, hegel.Integers(0, len(cmds)))
		m, err := buildPubMachine(o, modelSetup)
		if err != nil {
			ht.Fatal(err)
		}
		defer func() { m.p.wg.Wait(); m.p.release() }()
		lastPubMachine.Store(m)
		if err := tick(m, signalLogs); err != nil {
			ht.Fatal(err)
		}
		for _, c := range cmds[:k] {
			if err := replayCmd(m, c); err != nil {
				ht.Fatalf("%s: %v", c, err)
			}
			m.logf("(model) %s", c)
		}
		ht.Note(fmt.Sprintf("model prefix: %d commands\n  %s", k, strings.Join(m.log, "\n  ")))
		start := time.Now()
		hegel.RunStateful(ht, m, hegel.WithStatefulStepCount(o.steps), hegel.WithAlwaysCheckInvariants(pubInvariants...))
		ht.EventValue("continuation seconds", time.Since(start).Seconds())
	}, pbtOpts(200)...)
}
