// Package itf decodes the Informal Trace Format (ITF) that `quint run
// --out-itf` and Apalache write: https://apalache-mc.org/docs/adr/015adr-trace.html
//
// Values decode to a Value tree with accessors, so drivers can navigate a
// state without declaring Go types for every Quint type.
package itf

import (
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"sort"
	"strings"
)

// Kind of an ITF value.
type Kind int

const (
	Invalid Kind = iota
	Int
	Bool
	Str
	List
	Set
	Map
	Tuple
	Record
	Variant
)

// Value is a decoded ITF value.
type Value struct {
	Kind   Kind
	I      *big.Int
	B      bool
	S      string
	Elems  []Value          // List, Set, Tuple
	Pairs  [][2]Value       // Map
	Fields map[string]Value // Record
	Tag    string           // Variant
	Inner  *Value           // Variant payload
}

// Trace is a decoded ITF trace.
type Trace struct {
	Meta   map[string]any
	Vars   []string
	States []State
}

// State is one state: its variables, plus quint's MBT annotations when the
// trace came from `quint run --mbt`.
type State struct {
	Index  int
	Vars   map[string]Value
	Action string           // mbt::actionTaken
	Picks  map[string]Value // mbt::nondetPicks, with Some(v) unwrapped and None dropped
	// Extra holds quintgo's annotations, which are not model variables:
	// "quintgo::expect" (a Quint boolean over the post-state) and observed
	// projections "obs::<name>". Keys keep their prefix.
	Extra map[string]Value
	Meta  map[string]any // the state's #meta
}

func ReadFile(path string) (*Trace, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(b)
}

func Parse(b []byte) (*Trace, error) {
	var raw struct {
		Meta   map[string]any               `json:"#meta"`
		Vars   []string                     `json:"vars"`
		States []map[string]json.RawMessage `json:"states"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, err
	}
	t := &Trace{Meta: raw.Meta, Vars: raw.Vars}
	for i, rs := range raw.States {
		s := State{Index: i, Vars: map[string]Value{}, Picks: map[string]Value{}, Extra: map[string]Value{}}
		for k, v := range rs {
			switch {
			case k == "#meta":
				_ = json.Unmarshal(v, &s.Meta)
			case strings.HasPrefix(k, "quintgo::") || strings.HasPrefix(k, "obs::"):
				val, err := decode(v)
				if err != nil {
					return nil, fmt.Errorf("state %d %s: %w", i, k, err)
				}
				s.Extra[k] = val
			case k == "mbt::actionTaken":
				_ = json.Unmarshal(v, &s.Action)
			case k == "mbt::nondetPicks":
				pv, err := decode(v)
				if err != nil {
					return nil, fmt.Errorf("state %d picks: %w", i, err)
				}
				for name, opt := range pv.Fields {
					if opt.Kind == Variant && opt.Tag == "Some" && opt.Inner != nil {
						s.Picks[name] = *opt.Inner
					}
				}
			default:
				val, err := decode(v)
				if err != nil {
					return nil, fmt.Errorf("state %d var %s: %w", i, k, err)
				}
				// Strip module qualifiers (observed::edgePublish::writer -> writer).
				if j := strings.LastIndex(k, "::"); j >= 0 && !strings.HasPrefix(k, "mbt::") {
					k = k[j+2:]
				}
				s.Vars[k] = val
			}
		}
		t.States = append(t.States, s)
	}
	return t, nil
}

func decode(raw json.RawMessage) (Value, error) {
	var x any
	if err := json.Unmarshal(raw, &x); err != nil {
		return Value{}, err
	}
	return fromAny(x)
}

func fromAny(x any) (Value, error) {
	switch v := x.(type) {
	case bool:
		return Value{Kind: Bool, B: v}, nil
	case string:
		return Value{Kind: Str, S: v}, nil
	case float64:
		return Value{Kind: Int, I: big.NewInt(int64(v))}, nil
	case []any:
		out := Value{Kind: List}
		for _, e := range v {
			ev, err := fromAny(e)
			if err != nil {
				return Value{}, err
			}
			out.Elems = append(out.Elems, ev)
		}
		return out, nil
	case map[string]any:
		if s, ok := v["#bigint"].(string); ok {
			n, ok := new(big.Int).SetString(s, 10)
			if !ok {
				return Value{}, fmt.Errorf("bad #bigint %q", s)
			}
			return Value{Kind: Int, I: n}, nil
		}
		if es, ok := v["#set"].([]any); ok {
			l, err := fromAny(es)
			l.Kind = Set
			return l, err
		}
		if es, ok := v["#tup"].([]any); ok {
			l, err := fromAny(es)
			l.Kind = Tuple
			return l, err
		}
		if ps, ok := v["#map"].([]any); ok {
			out := Value{Kind: Map}
			for _, p := range ps {
				pair, _ := p.([]any)
				if len(pair) != 2 {
					return Value{}, fmt.Errorf("bad #map entry")
				}
				k, err := fromAny(pair[0])
				if err != nil {
					return Value{}, err
				}
				val, err := fromAny(pair[1])
				if err != nil {
					return Value{}, err
				}
				out.Pairs = append(out.Pairs, [2]Value{k, val})
			}
			return out, nil
		}
		if tag, ok := v["tag"].(string); ok && len(v) == 2 {
			if inner, ok := v["value"]; ok {
				iv, err := fromAny(inner)
				if err != nil {
					return Value{}, err
				}
				return Value{Kind: Variant, Tag: tag, Inner: &iv}, nil
			}
		}
		out := Value{Kind: Record, Fields: map[string]Value{}}
		for k, e := range v {
			ev, err := fromAny(e)
			if err != nil {
				return Value{}, err
			}
			out.Fields[k] = ev
		}
		return out, nil
	}
	return Value{}, fmt.Errorf("unsupported ITF value %T", x)
}

// Int64 of an Int (0 otherwise).
func (v Value) Int64() int64 {
	if v.Kind == Int && v.I != nil {
		return v.I.Int64()
	}
	return 0
}

// Get a record field (the zero Value if absent). Chains: v.Get("bk").Get("batch").
func (v Value) Get(field string) Value { return v.Fields[field] }

// Lookup a map value by key.
func (v Value) Lookup(key Value) (Value, bool) {
	for _, p := range v.Pairs {
		if p[0].String() == key.String() {
			return p[1], true
		}
	}
	return Value{}, false
}

// String renders the value in Quint-like syntax, with sets and record
// fields sorted, so equal values render equally.
func (v Value) String() string {
	switch v.Kind {
	case Int:
		return v.I.String()
	case Bool:
		return fmt.Sprint(v.B)
	case Str:
		return fmt.Sprintf("%q", v.S)
	case List, Tuple:
		parts := make([]string, len(v.Elems))
		for i, e := range v.Elems {
			parts[i] = e.String()
		}
		if v.Kind == Tuple {
			return "(" + strings.Join(parts, ", ") + ")"
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case Set:
		parts := make([]string, len(v.Elems))
		for i, e := range v.Elems {
			parts[i] = e.String()
		}
		sort.Strings(parts)
		return "Set(" + strings.Join(parts, ", ") + ")"
	case Map:
		parts := make([]string, len(v.Pairs))
		for i, p := range v.Pairs {
			parts[i] = p[0].String() + " -> " + p[1].String()
		}
		sort.Strings(parts)
		return "Map(" + strings.Join(parts, ", ") + ")"
	case Record:
		keys := make([]string, 0, len(v.Fields))
		for k := range v.Fields {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, len(keys))
		for i, k := range keys {
			parts[i] = k + ": " + v.Fields[k].String()
		}
		return "{ " + strings.Join(parts, ", ") + " }"
	case Variant:
		if v.Inner == nil || (v.Inner.Kind == Tuple && len(v.Inner.Elems) == 0) {
			return v.Tag
		}
		return v.Tag + "(" + v.Inner.String() + ")"
	}
	return "<invalid>"
}
