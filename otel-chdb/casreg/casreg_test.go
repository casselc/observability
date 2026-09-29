package casreg

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

// store is a toy one-bucket store with conditional writes; ignoreIfMatch
// is the mutant the checker must catch.
type store struct {
	mu            sync.Mutex
	objs          map[string]string
	ignoreIfMatch bool
	ignoreINM     bool
}

func (s *store) do(in Input) Output {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.objs == nil {
		s.objs = map[string]string{}
	}
	cur, ok := s.objs[in.Key]
	switch in.Op {
	case Read:
		return Output{Result: OK, Found: ok, Value: cur}
	case Create:
		if ok && !s.ignoreINM {
			return Output{Result: Failed}
		}
	case Swap:
		if (!ok || cur != in.Expect) && !s.ignoreIfMatch {
			return Output{Result: Failed}
		}
	case Delete:
		delete(s.objs, in.Key)
		return Output{Result: OK}
	}
	s.objs[in.Key] = in.Value
	return Output{Result: OK}
}

func run(r *Recorder, s *store, client int, in Input) Output {
	c := r.Begin(client, in)
	out := s.do(in)
	c.End(out)
	return out
}

func check(t *testing.T, r *Recorder) porcupine.CheckResult {
	t.Helper()
	res, _ := r.Check(20 * time.Second)
	return res
}

// The research probe's three histories (research/go-verification.md §7.3).
func TestAmbiguousWriteThatApplied(t *testing.T) {
	var r Recorder
	s := &store{}
	run(&r, s, 0, Input{Key: "k", Op: Create, Value: "v1"})
	// A's swap applies but its answer is lost
	a := r.Begin(1, Input{Key: "k", Op: Swap, Expect: "v1", Value: "a"})
	s.do(Input{Key: "k", Op: Swap, Expect: "v1", Value: "a"})
	a.End(Output{Result: Unknown})
	if out := run(&r, s, 2, Input{Key: "k", Op: Swap, Expect: "v1", Value: "b"}); out.Result != Failed {
		t.Fatal(out)
	}
	run(&r, s, 2, Input{Key: "k", Op: Read})
	if res := check(t, &r); res != porcupine.Ok {
		t.Fatal(res)
	}
	if r.Pending() != 1 {
		t.Fatal(r.Pending())
	}
}

func TestAmbiguousWriteThatNeverApplied(t *testing.T) {
	var r Recorder
	s := &store{}
	run(&r, s, 0, Input{Key: "k", Op: Create, Value: "v1"})
	r.Begin(1, Input{Key: "k", Op: Swap, Expect: "v1", Value: "a"}).End(Output{Result: Unknown})
	run(&r, s, 2, Input{Key: "k", Op: Read})
	run(&r, s, 2, Input{Key: "k", Op: Swap, Expect: "v1", Value: "b"})
	if res := check(t, &r); res != porcupine.Ok {
		t.Fatal(res)
	}
}

func TestLostUpdateDetected(t *testing.T) {
	var r Recorder
	s := &store{}
	run(&r, s, 0, Input{Key: "k", Op: Create, Value: "v1"})
	r.Begin(1, Input{Key: "k", Op: Swap, Expect: "v1", Value: "a"}).End(Output{Result: OK})
	r.Begin(2, Input{Key: "k", Op: Swap, Expect: "v1", Value: "b"}).End(Output{Result: OK})
	if res := check(t, &r); res != porcupine.Illegal {
		t.Fatal(res)
	}
}

func TestStaleReadDetected(t *testing.T) {
	var r Recorder
	r.Begin(0, Input{Key: "k", Op: Create, Value: "v1"}).End(Output{Result: OK})
	r.Begin(1, Input{Key: "k", Op: Read}).End(Output{Result: OK, Found: false})
	if res := check(t, &r); res != porcupine.Illegal {
		t.Fatal(res)
	}
}

func TestDoubleCreateDetected(t *testing.T) {
	var r Recorder
	r.Begin(0, Input{Key: "k", Op: Create, Value: "a"}).End(Output{Result: OK})
	r.Begin(1, Input{Key: "k", Op: Create, Value: "b"}).End(Output{Result: OK})
	if res := check(t, &r); res != porcupine.Illegal {
		t.Fatal(res)
	}
}

// A 412 needs a write the failing request could have lost to: a 412 on a
// create of an absent key, with no other write, is illegal.
func TestSpurious412Detected(t *testing.T) {
	var r Recorder
	r.Begin(0, Input{Key: "k", Op: Create, Value: "a"}).End(Output{Result: Failed})
	if res := check(t, &r); res != porcupine.Illegal {
		t.Fatal(res)
	}
	r.Reset()
	// ... unless an ambiguous create by someone else may have landed first
	r.Begin(1, Input{Key: "k", Op: Create, Value: "b"}).End(Output{Result: Unknown})
	r.Begin(0, Input{Key: "k", Op: Create, Value: "a"}).End(Output{Result: Failed})
	if res := check(t, &r); res != porcupine.Ok {
		t.Fatal(res)
	}
}

// casWorkload runs four writers doing read-then-swap on two keys, their
// steps interleaved by a seeded schedule (so a writer's read may be stale by
// the time it swaps), with some answers dropped (applied or not).
// Deterministic per seed. The concurrent variant below adds real goroutines.
func casWorkload(t *testing.T, s *store, seed uint64) *Recorder {
	var r Recorder
	rng := rand.New(rand.NewPCG(seed, 7))
	type pending struct {
		key  string
		read Output
		i    int
	}
	held := map[int]*pending{}
	for i := range 160 {
		w := rng.IntN(4)
		if p := held[w]; p != nil {
			delete(held, w)
			v := fmt.Sprintf("w%d-%d", w, p.i)
			in := Input{Key: p.key, Op: Create, Value: v}
			if p.read.Found {
				in = Input{Key: p.key, Op: Swap, Expect: p.read.Value, Value: v}
			}
			c := r.Begin(w, in)
			switch rng.IntN(6) {
			case 0: // lost request
				c.End(Output{Result: Unknown})
			case 1: // lost answer
				s.do(in)
				c.End(Output{Result: Unknown})
			default:
				c.End(s.do(in))
			}
			continue
		}
		key := fmt.Sprintf("k%d", rng.IntN(2))
		held[w] = &pending{key: key, read: run(&r, s, w, Input{Key: key, Op: Read}), i: i}
	}
	return &r
}

// Real goroutines against the correct store: always linearizable.
func TestConcurrentWritersAreLinearizable(t *testing.T) {
	var r Recorder
	s := &store{}
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 50 {
				key := fmt.Sprintf("k%d", i%2)
				rd := run(&r, s, w, Input{Key: key, Op: Read})
				in := Input{Key: key, Op: Create, Value: fmt.Sprintf("w%d-%d", w, i)}
				if rd.Found {
					in = Input{Key: key, Op: Swap, Expect: rd.Value, Value: in.Value}
				}
				run(&r, s, w, in)
			}
		}()
	}
	wg.Wait()
	if res := check(t, &r); res != porcupine.Ok {
		t.Fatal(res)
	}
}

func TestConcurrentCASIsLinearizable(t *testing.T) {
	for seed := range uint64(20) {
		r := casWorkload(t, &store{}, seed)
		if res := check(t, r); res != porcupine.Ok {
			t.Fatalf("seed %d: %s", seed, res)
		}
	}
}

// The mutant "store ignores If-Match" must be caught: two writers read the
// same version and both swaps succeed.
func TestMutantIgnoresIfMatchIsCaught(t *testing.T) {
	var r Recorder
	s := &store{ignoreIfMatch: true}
	run(&r, s, 0, Input{Key: "k", Op: Create, Value: "v0"})
	a := run(&r, s, 1, Input{Key: "k", Op: Read})
	b := run(&r, s, 2, Input{Key: "k", Op: Read})
	run(&r, s, 1, Input{Key: "k", Op: Swap, Expect: a.Value, Value: "a"})
	run(&r, s, 2, Input{Key: "k", Op: Swap, Expect: b.Value, Value: "b"})
	if res := check(t, &r); res != porcupine.Illegal {
		t.Fatalf("mutant not caught: %s", res)
	}
	// and in the random workload, over a few seeds
	caught := 0
	for seed := range uint64(10) {
		if res := check(t, casWorkload(t, &store{ignoreIfMatch: true}, seed)); res == porcupine.Illegal {
			caught++
		}
	}
	if caught < 10 { // deterministic schedules: every seed has a stale swap
		t.Fatalf("mutant caught in only %d/10 workloads", caught)
	}
	t.Logf("ignores-If-Match caught in %d/10 random workloads", caught)
}

func TestMutantIgnoresIfNoneMatchIsCaught(t *testing.T) {
	caught := 0
	for seed := range uint64(10) {
		if res := check(t, casWorkload(t, &store{ignoreINM: true}, seed)); res == porcupine.Illegal {
			caught++
		}
	}
	t.Logf("ignores-If-None-Match caught in %d/10 random workloads", caught)
	var r Recorder
	s := &store{ignoreINM: true}
	run(&r, s, 0, Input{Key: "k", Op: Create, Value: "a"})
	run(&r, s, 1, Input{Key: "k", Op: Create, Value: "b"})
	if res := check(t, &r); res != porcupine.Illegal {
		t.Fatalf("mutant not caught: %s", res)
	}
}

func TestVerifyWritesTimeline(t *testing.T) {
	var r Recorder
	r.Begin(0, Input{Key: "k", Op: Create, Value: "a"}).End(Output{Result: OK})
	r.Begin(1, Input{Key: "k", Op: Read}).End(Output{Result: OK, Found: true, Value: "a"})
	r.Verify(t) // passes
	var e Etags
	e.Learn(`"v1"`, "a")
	if v, ok := e.Value("v1"); !ok || v != "a" {
		t.Fatal(v, ok)
	}
}
