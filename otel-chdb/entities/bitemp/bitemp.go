// Package bitemp is the reference resolver for the entity catalog kept as
// append-only bitemporal events (DECISIONS D32, proposed;
// ../../research/bitemporal.md §5; README.md here). It is pure: no I/O.
//
// An Event says what one source claimed about one entity (or, with Entity
// All, about every entity of the event set's scope) over a valid-time
// interval, recorded at a system time. Resolution at (entity, VT, ST) uses
// only events with SystemFrom <= ST:
//
//   - The authority tier (Controller, Overseer) is ordered by the effective
//     time SystemFrom + TrustWindow for the controller, SystemFrom for the
//     overseer; ties go to the controller, then to the later arrival (Seq).
//     The top authority event covering the point decides if it asserts or
//     retracts.
//   - If it is Unknown, or there is none, the latest covering announcement
//     (the evidence tier) fills in: an assertion, Uncertain if an authority
//     said Unknown. Otherwise the answer is Unknown (or Absent).
//
// ResolveRange implements this as XTDB's backwards replay with a ceiling:
// events in descending order; a definite authority event closes its
// uncovered valid time and emits rows for it; an authority Unknown closes its
// valid time to the authority tier only; an announcement emits rows for what
// is still open. Rows are final when emitted, so the replay streams and
// stops as soon as the query's valid-time range is closed.
//
// The Quint model ../../model/bitemporalCatalog.qnt states the same rule and
// its invariants; model_test.go replays its traces through this package.
package bitemp

import (
	"errors"
	"math"
	"sort"
)

// Time is milliseconds since the epoch (lane.Time's unit).
type Time int64

// Inf is an open valid_to.
const Inf Time = math.MaxInt64

// All as an Event's Entity: every entity of the scope the event set covers
// (a cluster's pods, say): a relist or restart gap, a full-state sync's
// "nothing else exists".
const All uint64 = 0

// Source of an event, in precedence order (lowest first).
type Source uint8

const (
	Announce   Source = iota // an edge's announcement: evidence that a resource existed
	Overseer                 // the central multicluster aggregator: the fallback
	Controller               // the cluster's controller: the authority
)

func (s Source) String() string {
	switch s {
	case Announce:
		return "announce"
	case Overseer:
		return "overseer"
	case Controller:
		return "controller"
	}
	return "?"
}

// Kind of claim.
type Kind uint8

const (
	Assert  Kind = iota // the entity existed, as Version
	Retract             // it did not exist
	Unknown             // the source does not know (a gap)
)

// Event is one claim. ValidTo is exclusive (Inf while open). Seq orders
// events of one SystemFrom and must be unique within an event set (the
// arrival order). Version is the asserted version's key (attributes are
// content-addressed by it: a resource_id, a pod version key).
type Event struct {
	Entity     uint64
	ValidFrom  Time
	ValidTo    Time
	SystemFrom Time
	Seq        uint64
	Source     Source
	Kind       Kind
	Version    uint64
}

// State of a resolved interval.
type State uint8

const (
	Absent State = iota // nothing is known: not in the catalog
	Asserted
	Retracted
	Unknowable // an authority said unknown and no announcement fills it
)

func (s State) String() string {
	return [...]string{"absent", "asserted", "retracted", "unknown"}[s]
}

// Row is a resolved valid-time interval [From, To) of one entity. Source is
// the deciding event's; Uncertain marks an announcement that fills an
// authority's Unknown.
type Row struct {
	Entity    uint64
	From, To  Time
	State     State
	Version   uint64
	Source    Source
	Uncertain bool
}

// Policy holds the resolution's one parameter.
type Policy struct {
	// TrustWindow: how much newer an overseer event must be than the
	// controller's to override it. A live controller re-asserts every open
	// version at each full-state sync, so it keeps its authority; a silent
	// one loses it TrustWindow after it last spoke.
	TrustWindow Time
}

func auth(e *Event) bool { return e.Source != Announce }

func (p Policy) eff(e *Event) Time {
	if e.Source == Controller {
		if e.SystemFrom > Inf-p.TrustWindow {
			return Inf
		}
		return e.SystemFrom + p.TrustWindow
	}
	return e.SystemFrom
}

// Higher reports whether a is replayed before b (a takes precedence).
func (p Policy) Higher(a, b *Event) bool {
	ta, tb := auth(a), auth(b)
	if ta != tb {
		return ta
	}
	if ta {
		if ea, eb := p.eff(a), p.eff(b); ea != eb {
			return ea > eb
		}
		if ca, cb := a.Source == Controller, b.Source == Controller; ca != cb {
			return ca
		}
	} else if a.SystemFrom != b.SystemFrom {
		return a.SystemFrom > b.SystemFrom
	}
	return a.Seq > b.Seq
}

// Sort puts events in replay order (descending precedence).
func (p Policy) Sort(evs []Event) {
	sort.Slice(evs, func(i, j int) bool { return p.Higher(&evs[i], &evs[j]) })
}

func applies(e *Event, entity uint64) bool { return e.Entity == entity || e.Entity == All }

// Resolve answers one point (entity, vt) as of system time st by one scan
// for the top covering authority event and the top covering announcement:
// O(len(events)), no sort. Its From/To are the point's.
func Resolve(events []Event, entity uint64, vt, st Time, p Policy) Row {
	var ta, tn *Event
	for i := range events {
		e := &events[i]
		if e.SystemFrom > st || !applies(e, entity) || vt < e.ValidFrom || vt >= e.ValidTo {
			continue
		}
		if auth(e) {
			if ta == nil || p.Higher(e, ta) {
				ta = e
			}
		} else if tn == nil || p.Higher(e, tn) {
			tn = e
		}
	}
	r := Row{Entity: entity, From: vt, To: vt + 1}
	switch {
	case ta != nil && ta.Kind != Unknown:
		r.Source = ta.Source
		if ta.Kind == Assert {
			r.State, r.Version = Asserted, ta.Version
		} else {
			r.State = Retracted
		}
	case tn != nil:
		r.State, r.Version, r.Source, r.Uncertain = Asserted, tn.Version, Announce, ta != nil
	case ta != nil:
		r.State = Unknowable
	}
	return r
}

// ErrOrder: events pushed to a Replayer out of replay order.
var ErrOrder = errors.New("bitemp: events not in replay order")

// Replayer is the backwards replay with a ceiling, for one entity over
// [From, To) as of ST. Push events in replay order (Policy.Sort); rows are
// emitted as they become final.
type Replayer struct {
	Entity   uint64
	From, To Time
	ST       Time
	Policy   Policy
	Emit     func(Row) bool // false stops the replay

	closed, unk ivset
	last        *Event
	lastCopy    Event
	done        bool
	// Examined counts the events pushed that applied (entity, ST, range).
	Examined int
}

// Push takes the next event in replay order. It returns false once the
// replay is over (the range is closed, or Emit said stop): the caller stops
// reading events.
func (r *Replayer) Push(e Event) (bool, error) {
	if r.done {
		return false, nil
	}
	if r.last != nil && !r.Policy.Higher(r.last, &e) {
		return false, ErrOrder
	}
	r.lastCopy = e
	r.last = &r.lastCopy
	if e.SystemFrom > r.ST || !applies(&e, r.Entity) {
		return true, nil
	}
	in := iv{max(e.ValidFrom, r.From), min(e.ValidTo, r.To)}
	if in.a >= in.b {
		return true, nil
	}
	r.Examined++
	if auth(&e) {
		parts := r.unk.subtractFrom(r.closed.subtractFrom([]iv{in}))
		if e.Kind == Unknown {
			for _, x := range parts {
				r.unk.add(x)
			}
			return true, nil
		}
		st := Asserted
		if e.Kind == Retract {
			st = Retracted
		}
		for _, x := range parts {
			r.closed.add(x)
			if !r.emit(Row{Entity: r.Entity, From: x.a, To: x.b, State: st, Version: ver(&e), Source: e.Source}) {
				return false, nil
			}
		}
	} else {
		for _, x := range r.closed.subtractFrom([]iv{in}) {
			r.closed.add(x)
			// split by the authority's unknown: filled there (uncertain), plain elsewhere
			for _, y := range r.unk.intersect(x) {
				if !r.emit(Row{Entity: r.Entity, From: y.a, To: y.b, State: Asserted, Version: e.Version, Source: Announce, Uncertain: true}) {
					return false, nil
				}
			}
			for _, y := range r.unk.subtractFrom([]iv{x}) {
				if !r.emit(Row{Entity: r.Entity, From: y.a, To: y.b, State: Asserted, Version: e.Version, Source: Announce}) {
					return false, nil
				}
			}
		}
	}
	if r.closed.covers(iv{r.From, r.To}) {
		r.done = true
		return false, nil
	}
	return true, nil
}

func ver(e *Event) uint64 {
	if e.Kind == Assert {
		return e.Version
	}
	return 0
}

func (r *Replayer) emit(row Row) bool {
	if r.Emit != nil && !r.Emit(row) {
		r.done = true
		return false
	}
	return true
}

// Finish emits the Unknown rows: what an authority said it did not know and
// no announcement filled. Absent intervals are not emitted.
func (r *Replayer) Finish() {
	if r.done { // closed (nothing is left) or stopped by Emit
		return
	}
	for _, x := range r.unk.subtract(r.closed) {
		if !r.emit(Row{Entity: r.Entity, From: x.a, To: x.b, State: Unknowable}) {
			return
		}
	}
}

// ResolveRange streams the rows of entity over [vtFrom, vtTo) as of st, in
// replay order (not valid-time order), and returns the events examined. It
// sorts a copy of events; a store that keeps events sorted feeds a Replayer
// directly and stops reading when Push returns false.
func ResolveRange(events []Event, entity uint64, vtFrom, vtTo, st Time, p Policy, emit func(Row) bool) int {
	evs := make([]Event, 0, len(events))
	for _, e := range events {
		if applies(&e, entity) && e.SystemFrom <= st {
			evs = append(evs, e)
		}
	}
	p.Sort(evs)
	r := Replayer{Entity: entity, From: vtFrom, To: vtTo, ST: st, Policy: p, Emit: emit}
	for _, e := range evs {
		more, err := r.Push(e)
		if err != nil {
			panic(err) // sorted above
		}
		if !more {
			break
		}
	}
	r.Finish()
	return r.Examined
}

// Rows collects ResolveRange's rows in valid-time order.
func Rows(events []Event, entity uint64, vtFrom, vtTo, st Time, p Policy) []Row {
	var out []Row
	ResolveRange(events, entity, vtFrom, vtTo, st, p, func(r Row) bool { out = append(out, r); return true })
	sort.Slice(out, func(i, j int) bool { return out[i].From < out[j].From })
	return out
}
