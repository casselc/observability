package qtrace

import (
	"container/heap"
	"fmt"
	"sort"
	"time"
)

// Trace is the reconstructed, totally ordered history of one actor.
type Trace struct {
	Actor     string
	Steps     []Step
	Processes []string // incarnations in the order they appear
	Issues    []Issue  // problems that affect only this actor
}

// Issue is something the reconstructor could not make sense of. Severity
// "error" means validation of the affected actor is unsound (e.g. steps are
// missing); "warn" means the result is still meaningful.
type Issue struct {
	Severity string // "error" | "warn" | "info"
	Kind     string // "gap", "duplicate", "cycle", "overlap", "unordered"
	Msg      string
}

func (i Issue) String() string { return fmt.Sprintf("%s %s: %s", i.Severity, i.Kind, i.Msg) }

// Reconstruct turns an unordered bag of steps (from any number of recorders,
// processes and actors) into one ordered trace per actor.
//
// Ordering is a topological sort of a happens-before graph whose HARD edges
// are:
//   - program order: steps with the same Thread (same recorder), by seq;
//   - domain clocks: steps of one (actor, process) carrying the same
//     quint.order.<domain>, by that value (e.g. batch ids allocated by an
//     atomic counter: their allocation order IS the model's order even when
//     the steps were recorded in a different order).
//
// Among steps with no hard edge between them the sort prefers the recording
// order: seq within a recorder, timestamps across recorders. Such steps were
// concurrent; the choice is sound whenever the model actions they map to
// commute, which the model's interleaving semantics requires of steps that
// touch disjoint state.
func Reconstruct(steps []Step) ([]*Trace, []Issue) {
	var issues []Issue

	// 1. Dedupe on (recorder, seq): OTLP exporters retry, collectors fan out.
	type rk struct {
		rec string
		seq uint64
	}
	seen := map[rk]bool{}
	uniq := steps[:0:0]
	for _, s := range steps {
		if s.Seq != 0 {
			k := rk{s.Recorder, s.Seq}
			if seen[k] {
				issues = append(issues, Issue{"info", "duplicate", fmt.Sprintf("recorder %s seq %d seen twice; kept the first", s.Recorder, s.Seq)})
				continue
			}
			seen[k] = true
		}
		uniq = append(uniq, s)
	}

	// 2. Gaps: seq is dense per recorder, so a hole is a lost step (sampling,
	// a dropped export, a crash before flush). We cannot tell which actor it
	// belonged to, so every actor of that recorder is affected.
	byRec := map[string][]uint64{}
	recActors := map[string]map[string]bool{}
	for _, s := range uniq {
		byRec[s.Recorder] = append(byRec[s.Recorder], s.Seq)
		if recActors[s.Recorder] == nil {
			recActors[s.Recorder] = map[string]bool{}
		}
		recActors[s.Recorder][s.Actor] = true
	}
	gapActors := map[string][]string{}
	for rec, seqs := range byRec {
		sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
		var missing []uint64
		next := uint64(1)
		for _, q := range seqs {
			for ; next < q && len(missing) < 20; next++ {
				missing = append(missing, next)
			}
			next = q + 1
		}
		if len(missing) > 0 {
			msg := fmt.Sprintf("recorder %s is missing seq %v (of 1..%d): steps were lost or sampled out", rec, missing, seqs[len(seqs)-1])
			for a := range recActors[rec] {
				gapActors[a] = append(gapActors[a], msg)
			}
		}
	}

	// 3. Partition by actor, then order each.
	byActor := map[string][]Step{}
	for _, s := range uniq {
		byActor[s.Actor] = append(byActor[s.Actor], s)
	}
	actors := make([]string, 0, len(byActor))
	for a := range byActor {
		actors = append(actors, a)
	}
	sort.Strings(actors)
	var out []*Trace
	for _, a := range actors {
		t := order(a, byActor[a])
		for _, m := range gapActors[a] {
			t.Issues = append(t.Issues, Issue{"error", "gap", m})
		}
		out = append(out, t)
	}
	return out, issues
}

type node struct {
	s     Step
	key   time.Time // recording time, made monotone per recorder
	idx   int
	succ  []int
	npred int
}

type pq []*node

func (q pq) Len() int { return len(q) }
func (q pq) Less(i, j int) bool {
	a, b := q[i], q[j]
	if !a.key.Equal(b.key) {
		return a.key.Before(b.key)
	}
	if a.s.Recorder != b.s.Recorder {
		return a.s.Recorder < b.s.Recorder
	}
	return a.s.Seq < b.s.Seq
}
func (q pq) Swap(i, j int)          { q[i], q[j] = q[j], q[i] }
func (q *pq) Push(x any)            { *q = append(*q, x.(*node)) }
func (q *pq) Pop() any              { o := *q; n := o[len(o)-1]; *q = o[:len(o)-1]; return n }
func edge(ns []*node, from, to int) { ns[from].succ = append(ns[from].succ, to); ns[to].npred++ }

func order(actor string, steps []Step) *Trace {
	t := &Trace{Actor: actor}
	ns := make([]*node, len(steps))
	for i, s := range steps {
		ns[i] = &node{s: s, key: s.Time, idx: i}
	}
	// Monotone keys per recorder, in seq order: wall clocks step backwards.
	byRec := map[string][]*node{}
	for _, n := range ns {
		byRec[n.s.Recorder] = append(byRec[n.s.Recorder], n)
	}
	for _, l := range byRec {
		sort.Slice(l, func(i, j int) bool { return l[i].s.Seq < l[j].s.Seq })
		for i := 1; i < len(l); i++ {
			if l[i].key.Before(l[i-1].key) {
				l[i].key = l[i-1].key
			}
		}
	}
	// Program order per thread.
	threads := map[string][]*node{}
	for _, n := range ns {
		if n.s.Thread != "" {
			k := n.s.Recorder + "\x00" + n.s.Thread
			threads[k] = append(threads[k], n)
		}
	}
	for _, l := range threads {
		sort.Slice(l, func(i, j int) bool { return l[i].s.Seq < l[j].s.Seq })
		for i := 1; i < len(l); i++ {
			edge(ns, l[i-1].idx, l[i].idx)
		}
	}
	// Domain clocks per (process, domain).
	domains := map[string][]*node{}
	for _, n := range ns {
		for d := range n.s.Order {
			k := n.s.Process + "\x00" + d
			domains[k] = append(domains[k], n)
		}
	}
	for k, l := range domains {
		d := k[indexNul(k)+1:]
		sort.SliceStable(l, func(i, j int) bool { return l[i].s.Order[d] < l[j].s.Order[d] })
		for i := 1; i < len(l); i++ {
			if l[i-1].s.Order[d] == l[i].s.Order[d] {
				continue // equal clock values: no constraint
			}
			edge(ns, l[i-1].idx, l[i].idx)
		}
	}
	// Kahn's algorithm, preferring recording order.
	q := &pq{}
	for _, n := range ns {
		if n.npred == 0 {
			heap.Push(q, n)
		}
	}
	done := make([]bool, len(ns))
	for len(t.Steps) < len(ns) {
		if q.Len() == 0 {
			// A cycle: the hard constraints contradict each other, which
			// means the instrumentation's thread or order keys are wrong.
			// Break it at the earliest remaining step and report.
			var rest []*node
			for _, n := range ns {
				if !done[n.idx] {
					rest = append(rest, n)
				}
			}
			sort.Slice(rest, func(i, j int) bool { return (pq{rest[i], rest[j]}).Less(0, 1) })
			t.Issues = append(t.Issues, Issue{"error", "cycle", fmt.Sprintf("ordering constraints form a cycle at %s (seq %d); order keys contradict program order", rest[0].s.Action, rest[0].s.Seq)})
			rest[0].npred = 0
			heap.Push(q, rest[0])
		}
		n := heap.Pop(q).(*node)
		if done[n.idx] {
			continue
		}
		done[n.idx] = true
		t.Steps = append(t.Steps, n.s)
		for _, s := range n.succ {
			ns[s].npred--
			if ns[s].npred == 0 && !done[s] {
				heap.Push(q, ns[s])
			}
		}
	}
	// Incarnations: in order of first appearance; interleaving means two
	// incarnations of one actor were alive at once.
	seenP := map[string]bool{}
	cur := ""
	for _, s := range t.Steps {
		if s.Process == cur {
			continue
		}
		if seenP[s.Process] {
			t.Issues = append(t.Issues, Issue{"warn", "overlap", fmt.Sprintf("process %s emitted %s after process %s had started: two incarnations of %s overlap", s.Process, s.Action, cur, actor)})
		} else {
			seenP[s.Process] = true
			t.Processes = append(t.Processes, s.Process)
		}
		cur = s.Process
	}
	return t
}

func indexNul(s string) int {
	for i := 0; i < len(s); i++ {
		if s[i] == 0 {
			return i
		}
	}
	return -1
}
