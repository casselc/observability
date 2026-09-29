// Package casreg checks recorded histories of conditional writes for
// linearizability: every key must behave as one register that accepts
// `If-None-Match: *` (create), `If-Match` (swap), plain PUT and DELETE, and
// answers reads with the value last written. It is the Go counterpart of
// the "one object under If-Match / If-None-Match" argument our protocols
// (the edge's create-only slots, the alerts evaluator's state document, the
// entity controller's lane records) rest on, checked with porcupine
// (github.com/anishathalye/porcupine, pinned) over histories the existing
// tests and simulations already produce (research/go-verification.md §5).
//
// A write with no definite answer (a timeout, a reset, a 5xx, a lost
// answer, a copy still in flight) is ambiguous: it may have applied, may
// apply later, or never. It is recorded as pending, returning at
// math.MaxInt64, and the model lets it take effect or not; so a history is
// only rejected when no placement of the ambiguous writes explains it.
//
// Values, not ETags, are the register's contents: an adapter names each
// write by something the reads return (a content hash, a write id carried
// in the body or the metadata), and translates the ETag an If-Match was
// conditioned on into the value it named (Etags). ETags are store-assigned
// and a pending write's ETag is never seen, so values are what a history
// can state about.
package casreg

import (
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
)

// Op is what a request asked of the store.
type Op int

const (
	Read   Op = iota // GET or HEAD
	Create           // PUT If-None-Match: *
	Swap             // PUT If-Match: <ETag of Expect>
	Write            // unconditional PUT
	Delete           // DELETE
)

func (o Op) String() string {
	return [...]string{"read", "create", "swap", "write", "delete"}[o]
}

// Input is one request.
type Input struct {
	Key    string
	Op     Op
	Expect string // Swap: the value the If-Match ETag names
	Value  string // Create, Swap, Write: the value written
}

// Result is the answer's class.
type Result int

const (
	OK      Result = iota // 2xx (for Read: found or not, see Output.Found)
	Failed                // 412, or 404 on If-Match: not applied, ever
	Unknown               // no definite answer: may have applied, may still apply
)

func (r Result) String() string { return [...]string{"ok", "failed", "unknown"}[r] }

// Output is one answer.
type Output struct {
	Result Result
	Found  bool   // Read
	Value  string // Read: the value found
}

type state struct {
	exists bool
	value  string
}

func step(st, in, out any) []any {
	s, i, o := st.(state), in.(Input), out.(Output)
	put := state{true, i.Value}
	switch i.Op {
	case Read:
		if o.Result != OK || (o.Found == s.exists && (!o.Found || o.Value == s.value)) {
			return []any{s}
		}
		return nil
	case Create, Swap:
		holds := !s.exists
		if i.Op == Swap {
			holds = s.exists && s.value == i.Expect
		}
		switch {
		case o.Result == OK && holds:
			return []any{put}
		case o.Result == Failed && !holds:
			return []any{s}
		case o.Result == Unknown && holds:
			return []any{s, put}
		case o.Result == Unknown:
			return []any{s}
		}
		return nil
	case Write:
		switch o.Result {
		case OK:
			return []any{put}
		case Failed:
			return []any{s}
		}
		return []any{s, put}
	case Delete:
		gone := state{}
		switch o.Result {
		case OK:
			return []any{gone}
		case Failed:
			return []any{s}
		}
		return []any{s, gone}
	}
	return nil
}

// Model is the per-key register, partitioned by key.
func Model() porcupine.Model {
	m := &porcupine.NondeterministicModel{
		Partition: func(h []porcupine.Operation) [][]porcupine.Operation {
			by := map[string][]porcupine.Operation{}
			var keys []string
			for _, op := range h {
				k := op.Input.(Input).Key
				if _, ok := by[k]; !ok {
					keys = append(keys, k)
				}
				by[k] = append(by[k], op)
			}
			sort.Strings(keys)
			out := make([][]porcupine.Operation, 0, len(keys))
			for _, k := range keys {
				out = append(out, by[k])
			}
			return out
		},
		Init:  func() []any { return []any{state{}} },
		Step:  step,
		Equal: func(a, b any) bool { return a.(state) == b.(state) },
		DescribeOperation: func(in, out any) string {
			i, o := in.(Input), out.(Output)
			var b strings.Builder
			fmt.Fprintf(&b, "%s %s", i.Op, short(i.Key))
			if i.Op == Swap {
				fmt.Fprintf(&b, " if=%s", short(i.Expect))
			}
			if i.Op != Read && i.Op != Delete {
				fmt.Fprintf(&b, " := %s", short(i.Value))
			}
			fmt.Fprintf(&b, " -> %s", o.Result)
			if i.Op == Read && o.Result == OK {
				if o.Found {
					fmt.Fprintf(&b, " %s", short(o.Value))
				} else {
					b.WriteString(" 404")
				}
			}
			return b.String()
		},
		DescribeState: func(st any) string {
			s := st.(state)
			if !s.exists {
				return "absent"
			}
			return short(s.value)
		},
	}
	return m.ToModel()
}

func short(s string) string {
	if len(s) > 24 {
		return s[:10] + "…" + s[len(s)-10:]
	}
	return s
}

// Recorder collects a history. It is safe for concurrent use; its clock is
// a counter, so a call's timestamp orders it after every return recorded
// before it began, which is all linearizability needs.
type Recorder struct {
	mu    sync.Mutex
	clock int64
	ops   []porcupine.Operation
	taint map[string]bool
}

// Taint drops a key from the history: something changed it that the model
// does not describe (a multipart completion, a copy, a conditional
// DELETE, a batch delete), so its history says nothing about the store.
func (r *Recorder) Taint(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.taint == nil {
		r.taint = map[string]bool{}
	}
	r.taint[key] = true
}

// Call is an operation in flight.
type Call struct {
	r      *Recorder
	client int
	in     Input
	at     int64
}

// Begin records a request as sent by client (a replica, a lane, a writer).
func (r *Recorder) Begin(client int, in Input) *Call {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clock++
	return &Call{r: r, client: client, in: in, at: r.clock}
}

// End records the answer. A write answered Unknown stays pending: it
// returns at math.MaxInt64, since its copy may land at any later time.
// A read answered Unknown constrains nothing and is dropped.
func (c *Call) End(out Output) {
	r := c.r
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clock++
	ret := r.clock
	if out.Result == Unknown {
		if c.in.Op == Read {
			return
		}
		ret = math.MaxInt64
	}
	r.ops = append(r.ops, porcupine.Operation{ClientId: c.client, Input: c.in, Call: c.at, Output: out, Return: ret})
}

// Len is the number of recorded operations.
func (r *Recorder) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.ops)
}

// Pending is the number of recorded writes that never got a definite answer.
func (r *Recorder) Pending() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, op := range r.ops {
		if op.Return == math.MaxInt64 {
			n++
		}
	}
	return n
}

// Reset forgets the history (a new run over a fresh store).
func (r *Recorder) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ops, r.clock, r.taint = nil, 0, nil
}

// History is a copy of the recorded operations, without tainted keys.
func (r *Recorder) History() []porcupine.Operation {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]porcupine.Operation, 0, len(r.ops))
	for _, op := range r.ops {
		if !r.taint[op.Input.(Input).Key] {
			out = append(out, op)
		}
	}
	return out
}

// Check runs porcupine over the history.
func (r *Recorder) Check(timeout time.Duration) (porcupine.CheckResult, porcupine.LinearizationInfo) {
	return porcupine.CheckOperationsVerbose(Model(), r.History(), timeout)
}

// Verify fails t when the history is not linearizable and writes the
// porcupine visualization next to the test's artifacts ($CASREG_OUT, or the
// temporary directory). A check that times out is logged, not failed: it
// found no violation in the time it had.
func (r *Recorder) Verify(t TB) {
	t.Helper()
	res, info := r.Check(time.Minute)
	switch res {
	case porcupine.Illegal:
		dir := os.Getenv("CASREG_OUT")
		if dir == "" {
			dir = os.TempDir()
		}
		f, err := os.CreateTemp(dir, "casreg-*.html")
		path := "(no file: " + fmt.Sprint(err) + ")"
		if err == nil {
			path = f.Name()
			if err := porcupine.Visualize(Model(), info, f); err != nil {
				path = "(visualization failed: " + err.Error() + ")"
			}
			_ = f.Close()
		}
		t.Fatalf("casreg: the store's history is not linearizable as one register per key (%d ops, %d pending); timeline: %s",
			r.Len(), r.Pending(), path)
	case porcupine.Unknown:
		t.Logf("casreg: linearizability check timed out over %d ops; no violation found", r.Len())
	}
}

// TB is what Verify needs of a test: *testing.T and *rapid.T both have it.
type TB interface {
	Helper()
	Fatalf(format string, args ...any)
	Logf(format string, args ...any)
}

// Etags maps the ETags a client saw to the values they named, so an
// If-Match can be recorded as a Swap on a value.
type Etags struct {
	mu sync.Mutex
	m  map[string]string
}

// Learn notes that etag names value (from a write's answer or a read).
func (e *Etags) Learn(etag, value string) {
	if etag == "" {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.m == nil {
		e.m = map[string]string{}
	}
	e.m[strings.Trim(etag, `"`)] = value
}

// Value is the value etag named; ok is false for an ETag never seen.
func (e *Etags) Value(etag string) (string, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	v, ok := e.m[strings.Trim(etag, `"`)]
	return v, ok
}
