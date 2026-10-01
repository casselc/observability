package completeness

// The watermark's history (DECISIONS.md D29, amendment 2026-10-01;
// FORMAT.md §4.1; model/wmHistory.qnt): what was complete AS OF a wall time
// T. The consumer's documents carry their scope's open hour of steps
// (`history`); each earlier hour is a create-only object
// {ctl}/watermark-history/{_fleet|cluster}/{YYYY-MM-DD}T{HH}.json. A step
// (at_ms, values) says: by wall time at_ms, every request of the scope
// received below the values was in central (custody time, strictly below:
// FORMAT.md §3). Two clocks (CAST 26): T and at_ms are wall time; the
// values are custody time. A step counts from its own stamp (at_ms <= T).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
)

// HourMs is one history hour.
const HourMs = int64(3_600_000)

// FleetScope is the fleet's history scope (a cluster name never starts with _).
const FleetScope = "_fleet"

// DefaultLookback bounds how many hours before T a reader looks for a step.
const DefaultLookback = 48

// HistStep is one step of a scope's history.
type HistStep struct {
	AtMs       int64             `json:"at_ms"`
	CtNs       uint64            `json:"ct_ns"`
	Signals    map[string]uint64 `json:"signals,omitempty"`
	UnlistedNs uint64            `json:"unlisted_ns"`
}

// ValueFor is the step's value for signals (nil: every signal, the scope's
// complete_through; else the minimum over them, floored by it).
func (s HistStep) ValueFor(signals []string) uint64 {
	if len(signals) == 0 || len(s.Signals) == 0 {
		return s.CtNs
	}
	v := uint64(math.MaxUint64)
	for _, x := range signals {
		w, ok := s.Signals[x]
		if !ok {
			w = s.UnlistedNs
		}
		v = min(v, w)
	}
	return max(v, s.CtNs)
}

// HistHour is one hour's steps; Carry is the last step before it.
type HistHour struct {
	HourMs int64      `json:"hour_ms"`
	Carry  *HistStep  `json:"carry,omitempty"`
	Steps  []HistStep `json:"steps"`
}

// At is the last step at or before t (the carry when it is the one).
func (h *HistHour) At(t int64) *HistStep { return h.at(t, 0) }

// Deliberate bugs for the tests (CAST 34: boundary operators are pinned by
// a property with the off-by-one mutant); 0 in production.
const (
	mutStrict = 1 // a step counts only after its stamp (at < T)
	mutNext   = 2 // the first step after T
)

func (h *HistHour) at(t int64, mut int) *HistStep {
	le := func(a int64) bool {
		if mut == mutStrict {
			return a < t
		}
		return a <= t
	}
	if mut == mutNext {
		for i := range h.Steps {
			if h.Steps[i].AtMs > t {
				return &h.Steps[i]
			}
		}
	}
	for i := len(h.Steps) - 1; i >= 0; i-- {
		if le(h.Steps[i].AtMs) {
			return &h.Steps[i]
		}
	}
	if h.Carry != nil && le(h.Carry.AtMs) {
		return h.Carry
	}
	return nil
}

// History is a watermark document's `history`: the open hour (flattened)
// and the hours frozen but not yet confirmed sealed.
type History struct {
	HistHour
	Sealing []HistHour `json:"sealing,omitempty"`
	Dropped uint64     `json:"dropped,omitempty"`
}

// HourName is an hour's UTC `YYYY-MM-DDTHH`.
func HourName(hourMs int64) string {
	return time.UnixMilli(hourMs).UTC().Format("2006-01-02T15")
}

// HistKey is a scope's hour object beside the fleet document key.
func (r *Reader) HistKey(scope string, hourMs int64) string {
	return strings.TrimSuffix(r.key, "watermark.json") + "watermark-history/" + scope + "/" + HourName(hourMs) + ".json"
}

// histObj is a sealed hour's object.
type histObj struct {
	Format int    `json:"format"`
	Scope  string `json:"scope"`
	HistHour
}

// hourCache keeps sealed hours: they are create-only, never rewritten, so a
// read one stays true. Absent hours are not cached (one may still be sealed).
type hourCache struct {
	mu sync.Mutex
	m  map[string]*HistHour
}

const maxCachedHours = 4096

func (c *hourCache) get(k string) (*HistHour, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	h, ok := c.m[k]
	return h, ok
}

func (c *hourCache) put(k string, h *HistHour) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m == nil {
		c.m = map[string]*HistHour{}
	}
	if len(c.m) >= maxCachedHours {
		for x := range c.m {
			delete(c.m, x)
			break
		}
	}
	c.m[k] = h
}

// hour reads a scope's sealed hour (nil: none).
func (r *Reader) hour(ctx context.Context, scope string, hourMs int64) (*HistHour, error) {
	key := r.HistKey(scope, hourMs)
	if h, ok := r.hours.get(key); ok {
		return h, nil
	}
	body, err := r.get(ctx, key)
	if err != nil {
		return nil, err
	}
	if body == nil {
		return nil, nil
	}
	var o histObj
	if err := json.Unmarshal(body, &o); err != nil {
		return nil, fmt.Errorf("%s: %v", key, err)
	}
	if o.Scope != scope || o.HourMs != hourMs {
		return nil, fmt.Errorf("%s names scope %q hour %d", key, o.Scope, o.HourMs)
	}
	h := o.HistHour
	r.hours.put(key, &h)
	return &h, nil
}

// StepAsOf is a scope's step as of T, and whether the answer is final.
type StepAsOf struct {
	Step  HistStep
	Final bool
}

// asOf finds the step of a scope as of t: T's hour (its object, else the
// document's frozen or open hour), else the hours before it, at most
// lookback back. Final when T's hour is older than the document's open
// hour: sealed or frozen, so no later write can change it (a stale cached
// document only makes this more conservative). nil: no step within reach.
func (r *Reader) asOf(ctx context.Context, scope string, doc *History, t int64, lookback int) (*StepAsOf, error) {
	tHour := t / HourMs * HourMs
	var open int64
	if doc != nil {
		open = doc.HourMs
	}
	inDoc := func(h int64) *HistHour {
		if doc == nil {
			return nil
		}
		if doc.HourMs == h && (len(doc.Steps) > 0 || doc.Carry != nil) {
			return &doc.HistHour
		}
		for i := range doc.Sealing {
			if doc.Sealing[i].HourMs == h {
				return &doc.Sealing[i]
			}
		}
		return nil
	}
	for back := 0; back <= lookback; back++ {
		h := tHour - int64(back)*HourMs
		if h < 0 {
			break
		}
		var hr *HistHour
		if h < open || doc == nil {
			x, err := r.hour(ctx, scope, h)
			if err != nil {
				return nil, err
			}
			hr = x
		}
		if hr == nil {
			hr = inDoc(h)
		}
		if hr != nil {
			if s := hr.at(t, r.mutant); s != nil {
				return &StepAsOf{Step: *s, Final: tHour < open}, nil
			}
		}
	}
	return nil, nil
}

// ClusterAsOf is one cluster's value as of T.
type ClusterAsOf struct {
	Cluster           string `json:"cluster"`
	CompleteThroughNs uint64 `json:"complete_through_ns"`
	// StepAtMs: the stamp of the step the value comes from.
	StepAtMs int64 `json:"step_at_ms"`
	// Basis: "cluster_signals", "cluster" (the cluster's history) or "fleet"
	// (the fleet's history: the cluster has none at T).
	Basis string `json:"basis"`
	Final bool   `json:"final"`
}

// AsOfInfo is "what was complete as of T" for a scope.
type AsOfInfo struct {
	AtMs              int64         `json:"at_ms"`
	At                string        `json:"at"`
	CompleteThroughNs uint64        `json:"complete_through_ns"`
	CompleteThrough   string        `json:"complete_through"`
	Signals           []string      `json:"signals"`
	StepAtMs          int64         `json:"step_at_ms,omitempty"` // the fleet's (a fleet scope)
	By                []ClusterAsOf `json:"by_cluster,omitempty"`
	// Final: every value comes from a sealed (or frozen) hour, so no later
	// publication can change this answer; else provisional (it can rise).
	Final bool `json:"final"`
}

// ErrNoHistory: no history covers T for some cluster of the scope.
var ErrNoHistory = errors.New("no watermark history covers that time")

// AsOf is For as of wall time t (ms): the scope's complete_through then, from
// the history. Per cluster the highest of the fleet's step value and the
// cluster's own (its complete_through and its per-signal minimum over the
// scope's signals): each is sound for the cluster's requests; then the
// minimum over the clusters. A cluster without history at t falls back to
// the fleet's; with neither, ErrNoHistory.
func (r *Reader) AsOf(ctx context.Context, sc Scope, t int64, lookback int) (*AsOfInfo, error) {
	st := r.Get(ctx)
	if st.Doc == nil {
		return nil, fmt.Errorf("%w: the watermark is %s", ErrNoHistory, st.Status)
	}
	fleet, err := r.asOf(ctx, FleetScope, st.Doc.History, t, lookback)
	if err != nil {
		return nil, err
	}
	info := &AsOfInfo{AtMs: t, At: time.UnixMilli(t).UTC().Format(time.RFC3339Nano), Signals: orStar(sc.Signals), Final: true}
	if sc.Clusters == nil {
		if fleet == nil {
			return nil, ErrNoHistory
		}
		info.CompleteThroughNs, info.StepAtMs, info.Final = fleet.Step.ValueFor(sc.Signals), fleet.Step.AtMs, fleet.Final
	} else {
		ct := uint64(math.MaxUint64)
		for _, c := range sc.Clusters {
			cv := ClusterAsOf{Cluster: c, Basis: "fleet", Final: true}
			found := false
			if fleet != nil {
				cv.CompleteThroughNs, cv.StepAtMs, cv.Final, found = fleet.Step.CtNs, fleet.Step.AtMs, fleet.Final, true
			}
			cd, cerr := r.cluster(ctx, c)
			if cerr != "" && cd == nil {
				return nil, fmt.Errorf("cluster %s: %s", c, cerr)
			}
			var ch *History
			if cd != nil {
				ch = cd.History
			}
			cs, err := r.asOf(ctx, c, ch, t, lookback)
			if err != nil {
				return nil, err
			}
			if cs != nil {
				v, basis := cs.Step.CtNs, "cluster"
				if len(sc.Signals) > 0 && len(cs.Step.Signals) > 0 {
					v, basis = cs.Step.ValueFor(sc.Signals), "cluster_signals"
				}
				if !found || v > cv.CompleteThroughNs {
					cv.CompleteThroughNs, cv.StepAtMs, cv.Basis = v, cs.Step.AtMs, basis
				}
				// final only when every source of the value is
				cv.Final = cs.Final && (!found || fleet.Final)
				found = true
			}
			if !found {
				return nil, fmt.Errorf("%w (cluster %s)", ErrNoHistory, c)
			}
			info.By = append(info.By, cv)
			ct = min(ct, cv.CompleteThroughNs)
			info.Final = info.Final && cv.Final
		}
		if len(sc.Clusters) == 0 {
			return nil, ErrNoHistory
		}
		info.CompleteThroughNs = ct
	}
	info.CompleteThrough = time.Unix(0, int64(min(info.CompleteThroughNs, math.MaxInt64))).UTC().Format(time.RFC3339Nano)
	return info, nil
}
