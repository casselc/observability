// Package qtrace is the model-agnostic core of quintgo: the telemetry schema
// every instrumented program emits, the Step it decodes to, and the
// reconstruction of an ordered, per-actor trace from an unordered bag of
// steps collected from live telemetry.
//
// Nothing here knows about any particular Quint model. The mapping from
// steps to model actions lives in a binding (package binding).
package qtrace

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// The telemetry schema. Every model step is one span event named EventName.
// All attributes live on the event; readers fall back to the enclosing span's
// and resource's attributes for the non-arg keys, so a deployment can hoist
// e.g. quint.process to the resource.
const (
	EventName = "quint.step" // span event name
	SpanName  = "quint "     // prefix of the span a recorder opens per step: "quint <action>"

	KeyAction   = "quint.action"   // model action name (string, required)
	KeySeq      = "quint.seq"      // per-recorder dense sequence number, 1,2,3,... (int)
	KeyRecorder = "quint.recorder" // id of the recorder that assigned quint.seq (string)
	KeyActor    = "quint.actor"    // partition: which model instance the step belongs to (string)
	KeyProcess  = "quint.process"  // incarnation of the actor (string); a change means a restart
	KeyThread   = "quint.thread"   // program-order key: steps with the same thread are totally ordered by seq
	KeyOutcome  = "quint.outcome"  // "ok", "error", or an error's own classification
	KeyError    = "quint.error"    // error text, when the step failed

	PrefixArg   = "quint.arg."   // an observed parameter of the action
	PrefixObs   = "quint.obs."   // an observed projection of the post-state
	PrefixOrder = "quint.order." // a domain logical clock: steps carrying the same domain are ordered by its value
)

// Step is one observed model step, decoded from a quint.step event (or
// recorded directly in memory by qobs.MemorySink).
type Step struct {
	Action   string
	Seq      uint64
	Recorder string
	Actor    string
	Process  string
	Thread   string
	Outcome  string
	Error    string
	Time     time.Time
	Args     map[string]any // string, int64, bool or float64
	Obs      map[string]any
	Order    map[string]int64
	Source   string // where it came from, for diagnostics (span id, file:line of a JSON record, ...)
}

// Get returns an argument or, with the "obs." prefix, an observation.
func (s *Step) Get(name string) (any, bool) {
	if strings.HasPrefix(name, "obs.") {
		v, ok := s.Obs[strings.TrimPrefix(name, "obs.")]
		return v, ok
	}
	v, ok := s.Args[name]
	return v, ok
}

func (s *Step) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s", s.Action)
	keys := make([]string, 0, len(s.Args))
	for k := range s.Args {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	b.WriteString("(")
	for i, k := range keys {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s=%v", k, s.Args[k])
	}
	b.WriteString(")")
	if s.Outcome != "" && s.Outcome != "ok" {
		fmt.Fprintf(&b, " -> %s", s.Outcome)
	}
	return b.String()
}

// FromAttributes decodes a step from flattened attributes: the event's own,
// then (as fallbacks for the non-arg keys) the span's and the resource's.
func FromAttributes(t time.Time, layers ...map[string]any) (Step, error) {
	s := Step{Time: t, Args: map[string]any{}, Obs: map[string]any{}, Order: map[string]int64{}}
	str := func(k string) string {
		for _, l := range layers {
			if v, ok := l[k]; ok {
				return fmt.Sprint(v)
			}
		}
		return ""
	}
	s.Action = str(KeyAction)
	if s.Action == "" {
		return s, fmt.Errorf("%s: missing %s", EventName, KeyAction)
	}
	s.Recorder, s.Actor, s.Process, s.Thread = str(KeyRecorder), str(KeyActor), str(KeyProcess), str(KeyThread)
	s.Outcome, s.Error = str(KeyOutcome), str(KeyError)
	if seq := str(KeySeq); seq != "" {
		n, err := strconv.ParseUint(seq, 10, 64)
		if err != nil {
			return s, fmt.Errorf("%s: bad %s %q", EventName, KeySeq, seq)
		}
		s.Seq = n
	}
	// args, obs and order keys only from the event itself (layer 0)
	if len(layers) > 0 {
		for k, v := range layers[0] {
			switch {
			case strings.HasPrefix(k, PrefixArg):
				s.Args[strings.TrimPrefix(k, PrefixArg)] = v
			case strings.HasPrefix(k, PrefixObs):
				s.Obs[strings.TrimPrefix(k, PrefixObs)] = v
			case strings.HasPrefix(k, PrefixOrder):
				n, err := toInt(v)
				if err != nil {
					return s, fmt.Errorf("%s: %s: %v", EventName, k, err)
				}
				s.Order[strings.TrimPrefix(k, PrefixOrder)] = n
			}
		}
	}
	if s.Actor == "" {
		s.Actor = "default"
	}
	if s.Process == "" {
		s.Process = s.Recorder
	}
	return s, nil
}

func toInt(v any) (int64, error) {
	switch x := v.(type) {
	case int64:
		return x, nil
	case int:
		return int64(x), nil
	case float64:
		return int64(x), nil
	case string:
		return strconv.ParseInt(x, 10, 64)
	}
	return 0, fmt.Errorf("not an integer: %v", v)
}
