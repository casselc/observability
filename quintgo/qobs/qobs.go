// Package qobs is the runtime half of quintgo: it records model steps from a
// running Go program. It is what the //quint:action Orchestrion aspect calls,
// and it can be called directly.
//
// Recording is off until SetGlobal installs a Recorder (or a Recorder is used
// directly), so linking qobs into a program changes nothing by itself.
package qobs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chucklehead-dev/oscope/spike/quintgo/qtrace"
	"go.opentelemetry.io/otel/trace"
)

// Sink receives recorded steps. Record may be called concurrently.
type Sink interface {
	Record(ctx context.Context, s *qtrace.Step)
}

// Outcomer lets an error classify itself for the model, e.g. an S3 timeout
// that is known to have been applied reports "ambiguous". Without it a
// non-nil error is recorded as "error".
type Outcomer interface{ QuintOutcome() string }

// Recorder assigns each step a dense sequence number and hands it to a sink.
type Recorder struct {
	id      string
	seq     atomic.Uint64
	sink    Sink
	actor   string
	process string
}

// Option configures a Recorder.
type Option func(*Recorder)

// WithActor sets the default quint.actor of every step.
func WithActor(a string) Option { return func(r *Recorder) { r.actor = a } }

// WithProcess sets the default quint.process (incarnation) of every step.
// It defaults to the recorder id, which is fresh per Recorder.
func WithProcess(p string) Option { return func(r *Recorder) { r.process = p } }

// WithID sets the recorder id (default: random).
func WithID(id string) Option { return func(r *Recorder) { r.id = id } }

// NewRecorder returns a Recorder writing to sink.
func NewRecorder(sink Sink, opts ...Option) *Recorder {
	var b [6]byte
	_, _ = rand.Read(b[:])
	r := &Recorder{id: hex.EncodeToString(b[:]), sink: sink}
	for _, o := range opts {
		o(r)
	}
	if r.process == "" {
		r.process = r.id
	}
	return r
}

// ID is the recorder id (the scope of quint.seq).
func (r *Recorder) ID() string { return r.id }

var global atomic.Pointer[Recorder]

// SetGlobal installs the recorder used by the package-level Record (and so by
// woven //quint:action advice). nil turns recording off.
func SetGlobal(r *Recorder) { global.Store(r) }

// Global returns the installed recorder, or nil.
func Global() *Recorder { return global.Load() }

// Record records one step through the global recorder; a no-op when none is
// installed. This is the function //quint:action advice calls.
func Record(ctx context.Context, action string, err error, kv ...any) {
	if r := global.Load(); r != nil {
		r.Record(ctx, action, err, kv...)
	}
}

type threadKey struct{}

var threadSeq atomic.Uint64

// Thread returns a context whose steps are in program order with each other
// (quint.thread). Use it where one goroutine performs a sequence of steps and
// no OTel span marks that sequence. Without it, the span in ctx is the thread.
func Thread(ctx context.Context) context.Context {
	return context.WithValue(ctx, threadKey{}, fmt.Sprintf("t%d", threadSeq.Add(1)))
}

// Record records one step. kv alternates keys and values:
//
//	"actor", "process", "thread", "outcome"  override the step's identity
//	"when"        "ok" | "error": record only if err is nil / non-nil
//	"obs.<name>"  an observed projection of the state after the step
//	"order.<d>"   a domain logical clock (see qtrace.Reconstruct)
//	anything else an argument of the action (quint.arg.<key>)
//
// Values are normalised to string, int64, bool or float64.
func (r *Recorder) Record(ctx context.Context, action string, err error, kv ...any) {
	if r == nil {
		return
	}
	s := qtrace.Step{Action: action, Recorder: r.id, Actor: r.actor, Process: r.process, Outcome: outcome(err)}
	if err != nil {
		s.Error = err.Error()
	}
	if t, ok := ctx.Value(threadKey{}).(string); ok {
		s.Thread = t
	} else if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		s.Thread = sc.SpanID().String()
	}
	for i := 0; i+1 < len(kv); i += 2 {
		k, _ := kv[i].(string)
		v := Normalize(kv[i+1])
		switch {
		case k == "when":
			if (v == "ok" && err != nil) || (v == "error" && err == nil) {
				return
			}
		case k == "actor":
			s.Actor = fmt.Sprint(v)
		case k == "process":
			s.Process = fmt.Sprint(v)
		case k == "thread":
			s.Thread = fmt.Sprint(v)
		case k == "outcome":
			s.Outcome = fmt.Sprint(v)
		case len(k) > 4 && k[:4] == "obs.":
			if s.Obs == nil {
				s.Obs = map[string]any{}
			}
			s.Obs[k[4:]] = v
		case len(k) > 6 && k[:6] == "order.":
			if n, ok := v.(int64); ok {
				if s.Order == nil {
					s.Order = map[string]int64{}
				}
				s.Order[k[6:]] = n
			}
		default:
			if s.Args == nil {
				s.Args = map[string]any{}
			}
			s.Args[k] = v
		}
	}
	// seq is taken here, after the effect the step describes has happened.
	// Two concurrent steps may be numbered in either order; the
	// reconstructor only relies on seq within a thread (see qtrace).
	s.Seq = r.seq.Add(1)
	s.Time = time.Now()
	r.sink.Record(ctx, &s)
}

func outcome(err error) string {
	if err == nil {
		return "ok"
	}
	var o Outcomer
	if errors.As(err, &o) {
		return o.QuintOutcome()
	}
	return "error"
}

// Normalize maps a Go value to an attribute-friendly one.
func Normalize(v any) any {
	switch x := v.(type) {
	case nil:
		return ""
	case string, bool, int64, float64:
		return x
	case int:
		return int64(x)
	case int8:
		return int64(x)
	case int16:
		return int64(x)
	case int32:
		return int64(x)
	case uint:
		return int64(x)
	case uint8:
		return int64(x)
	case uint16:
		return int64(x)
	case uint32:
		return int64(x)
	case uint64:
		return int64(x)
	case float32:
		return float64(x)
	case fmt.Stringer:
		return x.String()
	case error:
		return x.Error()
	}
	return fmt.Sprint(v)
}

// MemorySink keeps steps in memory: for tests, and for validating a run
// in-process without any OTel pipeline.
type MemorySink struct {
	mu    sync.Mutex
	steps []qtrace.Step
}

func (m *MemorySink) Record(_ context.Context, s *qtrace.Step) {
	m.mu.Lock()
	m.steps = append(m.steps, *s)
	m.mu.Unlock()
}

// Steps returns a copy of what was recorded.
func (m *MemorySink) Steps() []qtrace.Step {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]qtrace.Step(nil), m.steps...)
}

// Reset forgets everything recorded.
func (m *MemorySink) Reset() { m.mu.Lock(); m.steps = nil; m.mu.Unlock() }

// Tee records to several sinks.
type Tee []Sink

func (t Tee) Record(ctx context.Context, s *qtrace.Step) {
	for _, x := range t {
		x.Record(ctx, s)
	}
}
