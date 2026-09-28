// Package completeness reads the consumer's `{ctl}/watermark.json`
// (FORMAT.md §3) and labels results with it (STPA R-S1, R-S2): the source,
// its complete_through, how fresh that is, and whether the result's window
// extends past it. A missing, unreadable or stale document makes every
// result's completeness "unknown", never "complete".
//
// Two clocks (STPA CAST row 26): complete_through bounds CUSTODY time (every
// row with received_at below it is in central); a result's window is EVENT
// time. One policy value bridges them, max_lateness: a row is assumed to be
// received no later than max_lateness after its event time. Rows with an
// event time below complete_through − max_lateness ("settled_through") are
// then all in, and an event-time window is complete only once
// complete_through ≥ its end + max_lateness. A row later than the bound is
// late data: the query service counts it (visible, not silent), but no
// label can promise it.
package completeness

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
)

// LaneWm is one lane's watermark in the document.
type LaneWm struct {
	Lane string  `json:"lane"`
	WmNs uint64  `json:"wm_ns"`
	LagS float64 `json:"lag_s"`
}

// Doc is watermark.json.
type Doc struct {
	Format            int      `json:"format"`
	Version           uint64   `json:"version"`
	CompleteThroughNs uint64   `json:"complete_through_ns"`
	ComputedNs        uint64   `json:"computed_ns"`
	WallMs            uint64   `json:"wall_ms"`
	ListCapNs         uint64   `json:"list_cap_ns"`
	Lanes             int      `json:"lanes"`
	Holding           []LaneWm `json:"holding"`
	Stale             []LaneWm `json:"stale"`
	StaleAfterS       uint64   `json:"stale_after_s"`
}

// Status of the watermark as the service sees it.
const (
	StatusOK      = "ok"      // read within the cache window and published recently
	StatusStale   = "stale"   // the document is older than MaxAge: the publisher is not running
	StatusMissing = "missing" // no document: nothing publishes complete_through
	StatusError   = "error"   // unreadable (and no good copy newer than MaxAge)
)

// Getter reads one object; (nil, nil) means absent.
type Getter func(ctx context.Context, key string) ([]byte, error)

// Reader caches the document.
type Reader struct {
	get    Getter
	key    string
	ttl    time.Duration // re-read after this
	maxAge time.Duration // a document published longer ago than this is stale
	now    func() time.Time
	// maxLateness bridges custody time and event time (package comment).
	maxLateness time.Duration

	mu    sync.Mutex
	state State
}

// State is one read's result.
type State struct {
	Status    string
	Doc       *Doc
	FetchedAt time.Time // when Doc was last read successfully
	TriedAt   time.Time
	Err       string
}

// DefaultMaxLateness is max_lateness when the configuration names none
// (watermark.max_lateness_s): a default of the policy, not of the design.
const DefaultMaxLateness = 60 * time.Second

// NewReader reads key through get, labelling with DefaultMaxLateness.
func NewReader(get Getter, key string, ttl, maxAge time.Duration) *Reader {
	return &Reader{get: get, key: key, ttl: ttl, maxAge: maxAge, now: time.Now, maxLateness: DefaultMaxLateness}
}

// SetMaxLateness sets the policy (negative is 0; 0 claims that event time
// and custody time are the same clock).
func (r *Reader) SetMaxLateness(d time.Duration) { r.maxLateness = max(d, 0) }

// MaxLateness is the policy every label of this watermark is made with.
func (r *Reader) MaxLateness() time.Duration { return r.maxLateness }

// SetClock replaces the clock (tests).
func (r *Reader) SetClock(now func() time.Time) { r.now = now }

// Key is the document's key.
func (r *Reader) Key() string { return r.key }

// Get returns the cached state, re-reading when it is older than ttl.
func (r *Reader) Get(ctx context.Context) State {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	if !r.state.TriedAt.IsZero() && now.Sub(r.state.TriedAt) < r.ttl {
		return r.classify(r.state, now)
	}
	r.state.TriedAt = now
	body, err := r.get(ctx, r.key)
	switch {
	case err != nil:
		r.state.Err = err.Error()
		if r.state.Doc == nil {
			r.state.Status = StatusError
		}
	case body == nil:
		r.state = State{Status: StatusMissing, TriedAt: now, Err: "no " + r.key}
	default:
		var d Doc
		if err := json.Unmarshal(body, &d); err != nil {
			r.state.Err = fmt.Sprintf("%s: %v", r.key, err)
			if r.state.Doc == nil {
				r.state.Status = StatusError
			}
		} else {
			r.state = State{Status: StatusOK, Doc: &d, FetchedAt: now, TriedAt: now}
		}
	}
	return r.classify(r.state, now)
}

// classify decides the status from ages: a good copy older than maxAge (the
// store unreachable since), or a document the consumer last wrote longer
// ago than maxAge, is stale.
func (r *Reader) classify(s State, now time.Time) State {
	if s.Doc == nil {
		if s.Status == "" {
			s.Status = StatusError
		}
		return s
	}
	switch {
	case now.Sub(s.FetchedAt) > r.maxAge:
		s.Status = StatusError
	case now.Sub(time.UnixMilli(int64(s.Doc.WallMs))) > r.maxAge:
		s.Status = StatusStale
	default:
		s.Status = StatusOK
	}
	return s
}

// Window is a half-open event-time range, ns.
type Window struct {
	FromNs int64 `json:"from_ns"`
	ToNs   int64 `json:"to_ns"`
}

// Label is what every response carries (R-S1, R-S2).
type Label struct {
	Source string `json:"source"`
	// CompleteThrough is null when unknown. It is CUSTODY time: every row
	// received before it is in the source.
	CompleteThrough   *string `json:"complete_through"`
	CompleteThroughNs *uint64 `json:"complete_through_ns"`
	// MaxLatenessS is the policy that bridges the two clocks: a row is
	// assumed received within this long after its event time.
	MaxLatenessS float64 `json:"max_lateness_s"`
	// SettledThrough is EVENT time, complete_through − max_lateness: rows
	// with an event time before it are all in, within the policy. Null when
	// complete_through is.
	SettledThrough   *string `json:"settled_through"`
	SettledThroughNs *int64  `json:"settled_through_ns"`
	// Completeness: "complete" (the window ends at or before
	// settled_through, i.e. complete_through ≥ its end + max_lateness, and
	// the watermark is current), "partial" (it extends past it;
	// IncompleteFrom says where, in event time), or "unknown".
	Completeness     string  `json:"completeness"`
	Partial          bool    `json:"partial"`
	IncompleteFrom   *string `json:"incomplete_from,omitempty"`
	IncompleteFromNs *int64  `json:"incomplete_from_ns,omitempty"`
	Watermark        WmInfo  `json:"watermark"`
}

// WmInfo is the watermark's own freshness and what holds it back, cut to the
// caller's clusters.
type WmInfo struct {
	Status    string   `json:"status"`
	Key       string   `json:"key"`
	AgeS      *float64 `json:"age_s,omitempty"` // now − the document's wall_ms
	LagS      *float64 `json:"lag_s,omitempty"` // now − complete_through
	FetchedAt string   `json:"fetched_at,omitempty"`
	Holding   []LaneWm `json:"holding,omitempty"`
	Stale     []LaneWm `json:"stale_lanes,omitempty"`
	Error     string   `json:"error,omitempty"`
	// Note says why completeness is unknown.
	Note string `json:"note,omitempty"`
}

// MakeLabel labels a result of source over the event-time window w (nil:
// unbounded, up to now) with state s, bridging custody time to event time
// by maxLateness (package comment). mayCluster filters the lanes named in
// the document.
func MakeLabel(source string, s State, w *Window, now time.Time, key string, mayCluster func(string) bool, maxLateness time.Duration) Label {
	maxLateness = max(maxLateness, 0)
	l := Label{Source: source, MaxLatenessS: maxLateness.Seconds(), Watermark: WmInfo{Status: s.Status, Key: key, Error: s.Err}}
	if !s.FetchedAt.IsZero() {
		l.Watermark.FetchedAt = s.FetchedAt.UTC().Format(time.RFC3339Nano)
	}
	if s.Doc == nil {
		l.Completeness, l.Partial = "unknown", true
		l.Watermark.Note = "no complete_through: completeness is unknown; nothing in this result may be read as settled"
		return l
	}
	ct := s.Doc.CompleteThroughNs
	cts := time.Unix(0, int64(ct)).UTC().Format(time.RFC3339Nano)
	l.CompleteThrough, l.CompleteThroughNs = &cts, &ct
	age := now.Sub(time.UnixMilli(int64(s.Doc.WallMs))).Seconds()
	lag := now.Sub(time.Unix(0, int64(ct))).Seconds()
	l.Watermark.AgeS, l.Watermark.LagS = &age, &lag
	for _, x := range s.Doc.Holding {
		if laneAllowed(x.Lane, mayCluster) {
			l.Watermark.Holding = append(l.Watermark.Holding, x)
		}
	}
	for _, x := range s.Doc.Stale {
		if laneAllowed(x.Lane, mayCluster) {
			l.Watermark.Stale = append(l.Watermark.Stale, x)
		}
	}
	// event time settled through: complete_through − max_lateness
	settled := SettledNs(ct, maxLateness)
	sts := time.Unix(0, settled).UTC().Format(time.RFC3339Nano)
	l.SettledThrough, l.SettledThroughNs = &sts, &settled
	from := settled
	if w != nil && w.FromNs > from {
		from = w.FromNs
	}
	past := w == nil || w.ToNs > settled
	if past {
		l.Partial = true
		fs := time.Unix(0, from).UTC().Format(time.RFC3339Nano)
		l.IncompleteFrom, l.IncompleteFromNs = &fs, &from
	}
	switch {
	case s.Status != StatusOK:
		l.Completeness, l.Partial = "unknown", true
		l.Watermark.Note = "the watermark is " + s.Status + ": complete_through is a lower bound that is not advancing, or could not be re-read"
	case past:
		l.Completeness = "partial"
	default:
		l.Completeness = "complete"
	}
	return l
}

// SettledNs is complete_through − maxLateness in int64 ns, saturating.
func SettledNs(ct uint64, maxLateness time.Duration) int64 {
	v := int64(min(ct, math.MaxInt64))
	d := int64(max(maxLateness, 0))
	if v < math.MinInt64+d {
		return math.MinInt64
	}
	return v - d
}

// laneAllowed: a lane id is {cluster}/{producer}/{signal}.
func laneAllowed(lane string, may func(string) bool) bool {
	if may == nil {
		return true
	}
	c, _, _ := strings.Cut(lane, "/")
	return may(c)
}
