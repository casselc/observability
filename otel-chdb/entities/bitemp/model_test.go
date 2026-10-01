package bitemp

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/casselc/observability/otel-chdb/testgate/tracetag"
)

// Model-based test against ../../model/bitemporalCatalog.qnt: states of
// traces of its `designTrace` instance (W = 2, valid times 0..4, INF = 9).
// For every state: at every (entity, VT, ST <= now) the model's `view` equals
// Resolve and the replay (Rows), and the current view built by Current from
// the same events keeps exactly the model's `live` partition and answers as
// it does. With O-G9's corrections (src "steward", kind "pseudonymise"): the
// model's `rview` (the answer a reader gets, pseudonymised at every basis)
// equals ResolveNamed and RowsNamed, and the current view answers it.
//
// The committed testdata/model_traces.json holds the final state of each of
// 30 traces (seed 0x5eed). Fresh traces, every state:
//
//	TRACES=/tmp/t model/bitemp_model.sh && BITEMP_TRACES=/tmp/t go test -run Model ./...
//	BITEMP_TRACES=/tmp/t go test -run Model ./... -update-model   # rewrite the testdata from them

var updateModel = flag.Bool("update-model", false, "rewrite testdata/model_traces.json from $BITEMP_TRACES (final states)")

const modelINF = 9

type mEvent struct {
	Ent  int    `json:"e"`
	Vf   int    `json:"f"`
	Vt   int    `json:"t"`
	St   int    `json:"s"`
	Seq  int    `json:"q"`
	Src  string `json:"o"`
	Kind string `json:"k"`
	A    int    `json:"a"`
}

type mRes struct {
	Ent  int    `json:"e"`
	V    int    `json:"v"`
	S    int    `json:"s"`
	Kind string `json:"k"`
	A    int    `json:"a"`
	Src  string `json:"o"`
	Unc  bool   `json:"u"`
}

type mState struct {
	Trace  string
	Now    int
	Events []mEvent
	Live   []int // seqs
	View   []mRes
	RView  []mRes
}

// itf values: {"#bigint": "n"}, {"#set": [...]}, {"#map": [[k, v]...]}, {"#tup": [...]}, records.
func itfInt(v any) int {
	switch x := v.(type) {
	case map[string]any:
		n, _ := strconv.Atoi(x["#bigint"].(string))
		return n
	case float64:
		return int(x)
	}
	panic(fmt.Sprintf("not an int: %v", v))
}

func itfEvent(v any) mEvent {
	r := v.(map[string]any)
	return mEvent{Ent: itfInt(r["ent"]), Vf: itfInt(r["vf"]), Vt: itfInt(r["vt"]), St: itfInt(r["st"]), Seq: itfInt(r["seq"]),
		Src: r["src"].(string), Kind: r["kind"].(string), A: itfInt(r["a"])}
}

func readITF(t *testing.T, path string) []mState {
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var tr struct {
		States []map[string]any `json:"states"`
	}
	if err := json.Unmarshal(b, &tr); err != nil {
		t.Fatal(err)
	}
	var out []mState
	for _, s := range tr.States {
		var st mState
		st.Trace = filepath.Base(path)
		for k, v := range s {
			switch {
			case strings.HasSuffix(k, "::now"):
				st.Now = itfInt(v)
			case strings.HasSuffix(k, "::events"):
				for _, e := range v.(map[string]any)["#set"].([]any) {
					st.Events = append(st.Events, itfEvent(e))
				}
			case strings.HasSuffix(k, "::live"):
				for _, e := range v.(map[string]any)["#set"].([]any) {
					st.Live = append(st.Live, itfEvent(e).Seq)
				}
			case strings.HasSuffix(k, "::view"), strings.HasSuffix(k, "::rview"):
				var out []mRes
				for _, kv := range v.(map[string]any)["#map"].([]any) {
					pair := kv.([]any)
					tup := pair[0].(map[string]any)["#tup"].([]any)
					r := pair[1].(map[string]any)
					out = append(out, mRes{Ent: itfInt(tup[0]), V: itfInt(tup[1]), S: itfInt(tup[2]),
						Kind: r["kind"].(string), A: itfInt(r["a"]), Src: r["src"].(string), Unc: r["unc"].(bool)})
				}
				if strings.HasSuffix(k, "::rview") {
					st.RView = out
				} else {
					st.View = out
				}
			}
		}
		sort.Slice(st.Events, func(i, j int) bool { return st.Events[i].Seq < st.Events[j].Seq })
		sort.Ints(st.Live)
		out = append(out, st)
	}
	return out
}

func toEvent(m mEvent) Event {
	e := Event{Entity: uint64(m.Ent), ValidFrom: Time(m.Vf), ValidTo: Time(m.Vt), SystemFrom: Time(m.St), Seq: uint64(m.Seq), Version: uint64(m.A)}
	if m.Vt == modelINF {
		e.ValidTo = Inf
	}
	e.Source = map[string]Source{"controller": Controller, "overseer": Overseer, "announce": Announce, "steward": Steward}[m.Src]
	e.Kind = map[string]Kind{"assert": Assert, "retract": Retract, "unknown": Unknown, "pseudonymise": Pseudonymise}[m.Kind]
	return e
}

func toRow(m mRes) Row {
	r := Row{State: map[string]State{"absent": Absent, "assert": Asserted, "retract": Retracted, "unknown": Unknowable}[m.Kind],
		Version: uint64(m.A), Uncertain: m.Unc}
	if m.Src != "" {
		r.Source = map[string]Source{"controller": Controller, "overseer": Overseer, "announce": Announce}[m.Src]
	}
	return r
}

func checkState(t *testing.T, s mState) (points int) {
	t.Helper()
	p := Policy{TrustWindow: 2}
	evs := make([]Event, len(s.Events))
	for i, m := range s.Events {
		evs[i] = toEvent(m)
	}
	rows := map[[2]int][]Row{}
	for _, m := range s.View {
		want := toRow(m)
		if got := Resolve(evs, uint64(m.Ent), Time(m.V), Time(m.S), p); !samePoint(got, want) {
			t.Fatalf("%s now %d: Resolve(%d, %d, %d) = %+v, model %+v\nevents %+v", s.Trace, s.Now, m.Ent, m.V, m.S, got, want, s.Events)
		}
		k := [2]int{m.Ent, m.S}
		if _, ok := rows[k]; !ok {
			rows[k] = Rows(evs, uint64(m.Ent), 0, 5, Time(m.S), p)
		}
		if got := rowAt(rows[k], Time(m.V)); !samePoint(got, want) {
			t.Fatalf("%s now %d: replay(%d, %d, %d) = %+v, model %+v\nevents %+v", s.Trace, s.Now, m.Ent, m.V, m.S, got, want, s.Events)
		}
		points++
	}
	// O-G9: the pseudonymised answers, at every basis
	if len(s.RView) != len(s.View) {
		t.Fatalf("%s now %d: rview has %d points, view %d (traces from a model without O-G9?)", s.Trace, s.Now, len(s.RView), len(s.View))
	}
	named := map[[2]int][]Row{}
	for _, m := range s.RView {
		want := toRow(m)
		if got := ResolveNamed(evs, uint64(m.Ent), Time(m.V), Time(m.S), p); !samePoint(got, want) {
			t.Fatalf("%s now %d: ResolveNamed(%d, %d, %d) = %+v, model %+v\nevents %+v", s.Trace, s.Now, m.Ent, m.V, m.S, got, want, s.Events)
		}
		k := [2]int{m.Ent, m.S}
		if _, ok := named[k]; !ok {
			named[k] = RowsNamed(evs, uint64(m.Ent), 0, 5, Time(m.S), p)
		}
		if got := rowAt(named[k], Time(m.V)); !samePoint(got, want) {
			t.Fatalf("%s now %d: RowsNamed(%d, %d, %d) = %+v, model %+v\nevents %+v", s.Trace, s.Now, m.Ent, m.V, m.S, got, want, s.Events)
		}
		points++
	}
	// the current partition
	c := NewCurrent(p, 0)
	for _, e := range evs {
		c.Advance(e.SystemFrom)
		c.Add(e)
	}
	c.Advance(Time(s.Now))
	c.Compact()
	var kept []int
	for _, evs := range c.ents {
		for _, e := range evs {
			kept = append(kept, int(e.Seq))
		}
	}
	for _, e := range c.all {
		kept = append(kept, int(e.Seq))
	}
	for _, evs := range c.pseu {
		for _, e := range evs {
			kept = append(kept, int(e.Seq))
		}
	}
	sort.Ints(kept)
	if fmt.Sprint(kept) != fmt.Sprint(s.Live) {
		t.Fatalf("%s now %d: current keeps %v, model's live %v\nevents %+v", s.Trace, s.Now, kept, s.Live, s.Events)
	}
	for ent := uint64(1); ent <= 2; ent++ {
		if got, want := c.Get(ent), ResolveNamed(evs, ent, Time(s.Now), Inf, p); !samePoint(got, want) {
			t.Fatalf("%s: current(%d) %+v, resolved %+v", s.Trace, ent, got, want)
		}
	}
	return points
}

func TestModelTraces(t *testing.T) {
	tracetag.Covers(t, "MBT", "H-G8", "R-G9")
	var states []mState
	if dir := os.Getenv("BITEMP_TRACES"); dir != "" {
		files, _ := filepath.Glob(filepath.Join(dir, "*.itf.json"))
		if len(files) == 0 {
			t.Fatalf("no *.itf.json in %s", dir)
		}
		var finals []mState
		for _, f := range files {
			ss := readITF(t, f)
			states = append(states, ss...)
			finals = append(finals, ss[len(ss)-1])
		}
		if *updateModel {
			sort.Slice(finals, func(i, j int) bool { return finals[i].Trace < finals[j].Trace })
			b, _ := json.Marshal(finals)
			if err := os.WriteFile("testdata/model_traces.json", b, 0o644); err != nil {
				t.Fatal(err)
			}
		}
	} else {
		b, err := os.ReadFile("testdata/model_traces.json")
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(b, &states); err != nil {
			t.Fatal(err)
		}
	}
	n, withEvents := 0, 0
	for _, s := range states {
		n += checkState(t, s)
		if len(s.Events) > 0 {
			withEvents++
		}
	}
	if withEvents == 0 {
		t.Fatal("no state with events")
	}
	t.Logf("%d states (%d with events), %d points agree with the model", len(states), withEvents, n)
}
