// Package lake plans reads of the lake for a browser: the objects of a
// signal and window in the caller's clusters, each with a presigned GET URL,
// its size and its time range, labelled with complete_through.
//
// There is no sealer and no snapshot yet (research/central-optional.md
// §5): the plan LISTs the v2 lanes directly ({root}/{cluster}/{producer}/
// {signal}/{epoch}/{seq}.parquet, FORMAT.md §1) and HEADs candidates for
// their event-time range. Raw lane objects are per publisher, publishers are
// per cluster, so access is enforceable per object by cluster, not by
// namespace (research/lake-ui.md §6.1): a namespace-restricted caller is
// refused and pointed at /v1/query.
package lake

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/casselc/observability/otel-chdb/query/internal/auth"
	"github.com/casselc/observability/otel-chdb/query/internal/basis"
	"github.com/casselc/observability/otel-chdb/query/internal/completeness"
	"github.com/casselc/observability/otel-chdb/query/internal/lakeidx"
	"github.com/casselc/observability/otel-chdb/query/internal/sqlscope"
	"github.com/casselc/observability/otel-chdb/query/internal/store"
)

// Config tunes planning.
type Config struct {
	Root string `json:"root"` // {root}: the lanes' prefix in the bucket
	Ctl  string `json:"ctl"`  // {ctl}; default {root}/_consumer
	// URLTTLS is each presigned URL's lifetime, clamped to [60, 900] s
	// (AMBIGUITY.md X8: short; there is no revocation before expiry).
	URLTTLS int `json:"url_ttl_s"`
	// ReplanMarginS: the plan says to re-plan this long before expiry.
	ReplanMarginS   int `json:"replan_margin_s"`
	MaxObjects      int `json:"max_objects"`
	MaxHeads        int `json:"max_heads"`
	HeadConcurrency int `json:"head_concurrency"`
	// SkewS: an event at time t is received no earlier than t − SkewS, so
	// an object written before from − SkewS holds nothing of the window.
	// This bounds EARLY receipt (a producer clock ahead); late receipt
	// (max_lateness, the watermark's policy) is the other direction and
	// never drops an object here: a late object is written after the
	// window, and is kept by its rows' event-time range.
	SkewS      int `json:"skew_s"`
	ListMax    int `json:"list_max"`
	MaxWindowS int `json:"max_window_s"`
	GCCacheS   int `json:"gc_cache_s"`
	// Index: the lake index (lakeidx) narrows plans with a filter.
	Index IndexConfig `json:"index"`
}

func (c *Config) defaults() {
	c.Root = strings.Trim(c.Root, "/")
	if c.Ctl == "" {
		c.Ctl = join(c.Root, "_consumer")
	}
	if c.URLTTLS <= 0 {
		c.URLTTLS = 300
	}
	if c.URLTTLS < 60 {
		c.URLTTLS = 60
	}
	if c.URLTTLS > 900 {
		c.URLTTLS = 900
	}
	if c.ReplanMarginS <= 0 {
		c.ReplanMarginS = 60
	}
	// a margin at or above the lifetime makes every plan stale as it is
	// issued (replan_after ≤ signed_at): a client following X8 re-plans
	// forever. A plan stays usable for at least half its URLs' lifetime.
	if c.ReplanMarginS > c.URLTTLS/2 {
		c.ReplanMarginS = c.URLTTLS / 2
	}
	if c.MaxObjects <= 0 {
		c.MaxObjects = 2000
	}
	if c.MaxHeads <= 0 {
		c.MaxHeads = 2000
	}
	if c.HeadConcurrency <= 0 {
		c.HeadConcurrency = 16
	}
	if c.SkewS <= 0 {
		c.SkewS = 300
	}
	if c.ListMax <= 0 {
		c.ListMax = 100_000
	}
	if c.MaxWindowS <= 0 {
		c.MaxWindowS = 24 * 3600
	}
	if c.GCCacheS <= 0 {
		c.GCCacheS = 30
	}
}

func join(a, b string) string {
	if a == "" {
		return b
	}
	return a + "/" + b
}

// Signals a plan may name (FORMAT.md §1: every layout's lanes).
var Signals = map[string]bool{
	"traces": true, "logs": true, "metrics_number_points": true, "metrics_gauge_points": true,
	"metrics_sum_points": true, "metrics_histogram_points": true, "metrics_exponential_histogram_points": true,
	"metrics_summary_points": true, "metrics_series": true, "metrics_gauge": true, "metrics_sum": true,
	"metrics_histogram": true, "metrics_exponential_histogram": true, "metrics_summary": true,
}

// Planner plans.
type Planner struct {
	cfg   Config
	store store.Store
	wm    *completeness.Reader
	now   func() time.Time

	idx *lakeidx.Resolver // nil: filters narrow nothing

	mu    sync.Mutex
	gc    *gcDoc
	gcAt  time.Time
	gcErr error
}

// New returns a planner reading through st; wm is the watermark reader (the
// same document central's results use: an object with received_at below it
// was committed before the document was written, so before this plan's LIST).
func New(cfg Config, st store.Store, wm *completeness.Reader) *Planner {
	cfg.defaults()
	return &Planner{cfg: cfg, store: st, wm: wm, now: time.Now, idx: newResolver(cfg, st)}
}

// Config returns the effective configuration.
func (p *Planner) Config() Config { return p.cfg }

// SetClock replaces the clock (tests).
func (p *Planner) SetClock(now func() time.Time) { p.now = now }

// Request is a plan call.
type Request struct {
	Signal   string   `json:"signal"`
	FromNs   int64    `json:"-"`
	ToNs     int64    `json:"-"`
	Clusters []string `json:"clusters"`
	// TraceID and Terms narrow the plan through the index (index.go).
	TraceID string   `json:"trace_id,omitempty"`
	Terms   []string `json:"terms,omitempty"`
	// Basis (D30), checked by the caller (scope, signal, watermark,
	// retention): the plan lists only objects received before the bound of
	// their cluster, and is refused (basis_expired) when GC may have
	// deleted some of them.
	Basis *basis.Basis `json:"-"`
}

// Denied is a refusal on scope (HTTP 403).
type Denied struct{ Reason, Detail string }

func (d *Denied) Error() string { return d.Reason + ": " + d.Detail }

// BadRequest is a malformed request (HTTP 400).
type BadRequest struct{ Reason, Detail string }

func (b *BadRequest) Error() string { return b.Reason + ": " + b.Detail }

// TooLarge: the plan exceeds MaxObjects (HTTP 413): narrow the window or
// use the query API.
type TooLarge struct{ Objects, Max int }

func (t *TooLarge) Error() string {
	return fmt.Sprintf("plan_too_large: %d objects or more, the limit is %d; narrow the window or clusters, or use /v1/query", t.Objects, t.Max)
}

// Object is one planned object.
type Object struct {
	URL          string `json:"url"`
	Size         int64  `json:"size"`
	Key          string `json:"key"`
	Cluster      string `json:"cluster"`
	Producer     string `json:"producer"`
	Epoch        string `json:"epoch"`
	Seq          uint64 `json:"seq"`
	LastModified string `json:"last_modified"`
	// From the slot's metadata (FORMAT.md §2); absent when not refined.
	MinTimeNs *int64 `json:"min_time_ns,omitempty"`
	MaxTimeNs *int64 `json:"max_time_ns,omitempty"`
	Rows      *int64 `json:"rows,omitempty"`
	// ReceivedNs is the slot's oscope-received (custody time); Late is
	// true when it is more than max_lateness after min_time_ns: the object
	// holds at least one row later than the label's policy allows.
	ReceivedNs *int64 `json:"received_ns,omitempty"`
	Late       bool   `json:"late,omitempty"`
	// Part is the slot's oscope-part: "bulk" or "late" when the edge split
	// its request by event time (D31, FORMAT.md §2.2), each part with its
	// own honest range; empty for an object holding its whole request.
	Part string `json:"part,omitempty"`
	// Refined is false when the HEAD budget ran out: the object is planned
	// on its LIST entry alone (a superset, never a loss).
	Refined bool `json:"refined"`
	// Index is set when the plan has a filter: "hit" (an index segment
	// covers the object; only RowGroups may match) or "scan" (not covered:
	// read it all). Objects the index rules out are not planned.
	Index     string `json:"index,omitempty"`
	RowGroups []int  `json:"row_groups,omitempty"`
	// BasisCheck (plans at a basis): the object's custody time could not be
	// read here (not HEADed: over the HEAD budget, or its HEAD failed), and
	// its LastModified does not prove it was received before the basis.
	// The reader must read its Parquet footer first and drop the object,
	// unread, unless the footer's oscope-received is below
	// ReceivedBeforeNs (FORMAT.md §2: data objects repeat it there).
	BasisCheck       bool    `json:"basis_check,omitempty"`
	ReceivedBeforeNs *uint64 `json:"received_before_ns,omitempty"`
}

// Plan is the answer.
type Plan struct {
	completeness.Label
	Signal       string   `json:"signal"`
	From         string   `json:"from"`
	To           string   `json:"to"`
	FromNs       int64    `json:"from_ns"`
	ToNs         int64    `json:"to_ns"`
	Clusters     []string `json:"clusters"`
	Snapshot     *string  `json:"snapshot"`
	SnapshotNote string   `json:"snapshot_note"`
	ListedAt     string   `json:"listed_at"`
	// StartComplete is false when GC has deleted ingested slots of a lane
	// in the plan and the window starts before that lane's oldest remaining
	// object: older rows may be only in central. GCTruncated names them.
	StartComplete bool     `json:"start_complete"`
	GCTruncated   []string `json:"gc_truncated_lanes,omitempty"`
	GCNote        string   `json:"gc_note,omitempty"`
	ExpiresAt     string   `json:"expires_at"`
	ReplanAfter   string   `json:"replan_after"`
	URLTTLS       int      `json:"url_ttl_s"`
	Objects       []Object `json:"objects"`
	TotalBytes    int64    `json:"total_bytes"`
	Unrefined     int      `json:"unrefined"`
	Mismatched    int      `json:"mismatched"`
	// LateObjects counts planned objects received more than max_lateness
	// after their earliest row's event time (Object.Late): late data made
	// visible. An object without oscope-received is not judged.
	LateObjects int      `json:"late_objects"`
	Rules       []string `json:"rules"`
	// ObjectsHash is sha256 over the planned keys, in order (audit).
	ObjectsHash string `json:"objects_hash"`
	// Index reports the filter's resolution (nil without a filter).
	Index *IndexReport `json:"index,omitempty"`
	// The basis block (D30): set by the service.
	basis.Answer
	// AfterBasis: objects left out because they were received at or after
	// the basis's bound (by their metadata, or written too long after the
	// basis was issued to hold anything below it). BasisUnverified: objects
	// planned with basis_check.
	AfterBasis      int `json:"after_basis,omitempty"`
	BasisUnverified int `json:"basis_unverified,omitempty"`
	// State and WmScope are the watermark the label was made with (the
	// service mints the answer's basis from them).
	State   completeness.State `json:"-"`
	WmScope completeness.Scope `json:"-"`
}

// Rules are AMBIGUITY.md X8's, in every plan.
var Rules = []string{
	"Each URL is valid until expires_at. Re-plan before replan_after; never start reading an object whose URL expires within the margin.",
	"A 403 (or any error) on a planned object means re-plan, never 'no data': a query that could not read every planned object is incomplete and must say which objects it lacks.",
	"Rows with an event time at or after incomplete_from (complete_through − max_lateness: complete_through is custody time, the window event time) may still arrive: draw that region as incomplete, and count over it as partial. Objects marked late arrived after that policy; a result over them was not settled when they arrived.",
	"snapshot is null: this plan lists lanes directly; a later plan may include objects this one did not.",
}

// BasisRules replace the last rule in a plan at a basis (D30).
var BasisRules = []string{
	"This plan is at a basis (at_basis): it lists only objects received before the basis's bound of their cluster; re-planning with the same basis lists the same objects (fresh URLs), however much data arrives meanwhile.",
	"An object marked basis_check could not be dated here: read its Parquet footer first and drop it, unread, unless its oscope-received key-value is below received_before_ns (a missing key is an error, never a keep).",
}

var (
	epochRE = regexp.MustCompile(`^\d{8}T\d{6}\.\d{3}Z-[0-9a-f]{8}$`)
	seqRE   = regexp.MustCompile(`^(\d{20})\.parquet$`)
)

// Plan plans req for principal pr.
func (p *Planner) Plan(ctx context.Context, pr *auth.Principal, req Request) (*Plan, error) {
	if !Signals[req.Signal] {
		return nil, &BadRequest{"bad_signal", fmt.Sprintf("signal %q is not a lane namespace", req.Signal)}
	}
	if req.ToNs <= req.FromNs {
		return nil, &BadRequest{"bad_window", "to must be after from"}
	}
	if req.ToNs-req.FromNs > int64(p.cfg.MaxWindowS)*1e9 {
		return nil, &BadRequest{"window_too_long", fmt.Sprintf("a plan covers at most %d s; use /v1/query for longer ranges", p.cfg.MaxWindowS)}
	}
	if err := p.checkFilter(&req); err != nil {
		return nil, err
	}
	if !pr.AllNamespaces {
		return nil, &Denied{"namespace_scope_needs_filtering_reader",
			"raw lane objects hold every namespace of their cluster; a namespace-restricted caller reads through /v1/query"}
	}
	clusters, err := p.clusters(ctx, pr, req.Clusters)
	if err != nil {
		return nil, err
	}
	// the watermark first: whatever it covers was committed before the
	// document was written, so before the LIST below
	// (D29: the watermark of the plan's clusters and signal only)
	wmScope := completeness.Scope{Clusters: clusters, Signals: []string{req.Signal}}
	if pr.AllClusters && len(req.Clusters) == 0 {
		wmScope.Clusters = nil
	}
	wmState := p.wm.For(ctx, wmScope)
	b := req.Basis
	skew := time.Duration(p.cfg.SkewS) * time.Second
	// A row received before C was ingested before the watermark that
	// allowed C was written, so before the basis was issued: an object
	// written (S3's clock) more than two clock skews after the issue holds
	// nothing below C, and is left out without a HEAD.
	var afterIssue time.Time
	if b != nil {
		for _, c := range clusters {
			if _, ok := b.C(c); !ok {
				return nil, &basis.Refusal{Status: 400, Reason: basis.ReasonScope, Detail: fmt.Sprintf("the basis does not cover cluster %q", c)}
			}
		}
		afterIssue = time.Unix(0, b.IssuedNs).Add(2 * skew)
	}
	afterBasis := 0
	listedAt := p.now()
	lowLM := time.Unix(0, req.FromNs).Add(-time.Duration(p.cfg.SkewS) * time.Second)
	type cand struct {
		obj                   store.Object
		cluster, producer, ep string
		seq                   uint64
	}
	var cands []cand
	oldest := map[string]time.Time{} // lane -> oldest remaining object's LastModified
	for _, cl := range clusters {
		producers, err := p.store.Dirs(ctx, join(p.cfg.Root, cl)+"/")
		if err != nil {
			return nil, err
		}
		for _, prod := range producers {
			if !sqlscope.ClusterRE.MatchString(prod) {
				continue
			}
			prefix := join(p.cfg.Root, cl+"/"+prod+"/"+req.Signal) + "/"
			objs, truncated, err := p.store.List(ctx, prefix, p.cfg.ListMax)
			if err != nil {
				return nil, err
			}
			if truncated {
				return nil, &TooLarge{Objects: p.cfg.ListMax, Max: p.cfg.MaxObjects}
			}
			lane := cl + "/" + prod + "/" + req.Signal
			for _, o := range objs {
				rest := strings.TrimPrefix(o.Key, prefix)
				ep, name, ok := strings.Cut(rest, "/")
				if !ok || !epochRE.MatchString(ep) {
					continue
				}
				m := seqRE.FindStringSubmatch(name)
				if m == nil {
					continue
				}
				if t, ok := oldest[lane]; !ok || o.LastModified.Before(t) {
					oldest[lane] = o.LastModified
				}
				if o.Size == 0 || o.LastModified.Before(lowLM) {
					continue // heartbeats and tombstones are empty; old objects hold nothing of the window
				}
				if b != nil && o.LastModified.After(afterIssue) {
					afterBasis++
					continue
				}
				seq, _ := strconv.ParseUint(m[1], 10, 64)
				cands = append(cands, cand{obj: o, cluster: cl, producer: prod, ep: ep, seq: seq})
			}
		}
	}
	if len(cands) > p.cfg.MaxHeads+p.cfg.MaxObjects {
		return nil, &TooLarge{Objects: len(cands), Max: p.cfg.MaxObjects}
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i].obj.Key < cands[j].obj.Key })

	// refine by HEAD: kind, the rows' event-time range, the cluster
	type refined struct {
		keep, ok, mismatch bool
		basisCheck         bool
		minT, maxT, rows   int64
		recv               *int64
		part               string
	}
	res := make([]refined, len(cands))
	sem := make(chan struct{}, p.cfg.HeadConcurrency)
	var wg sync.WaitGroup
	for i := range cands {
		if i >= p.cfg.MaxHeads {
			res[i] = refined{keep: true}
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			meta, err := p.store.Head(ctx, cands[i].obj.Key)
			if err != nil {
				res[i] = refined{keep: true} // unknown: plan it unrefined
				return
			}
			r := refined{ok: true}
			if meta["oscope-kind"] != "" && meta["oscope-kind"] != "data" {
				res[i] = r
				return
			}
			if c := meta["oscope-cluster"]; c != "" && c != cands[i].cluster {
				r.mismatch = true // an object under one cluster's prefix naming another: never planned
				res[i] = r
				return
			}
			var e1, e2, e3 error
			r.minT, e1 = strconv.ParseInt(meta["oscope-min-time"], 10, 64)
			r.maxT, e2 = strconv.ParseInt(meta["oscope-max-time"], 10, 64)
			r.rows, e3 = strconv.ParseInt(meta["oscope-rows"], 10, 64)
			if e1 != nil || e2 != nil || e3 != nil {
				res[i] = refined{keep: true}
				return
			}
			if v, err := strconv.ParseInt(meta["oscope-received"], 10, 64); err == nil && v > 0 {
				r.recv = &v
			}
			r.part = meta["oscope-part"]
			r.keep = r.maxT >= req.FromNs && r.minT < req.ToNs
			res[i] = r
		}(i)
	}
	wg.Wait()

	var kept []keptObject
	for i, c := range cands {
		if b == nil || !res[i].keep || res[i].mismatch {
			continue
		}
		cb, _ := b.C(c.cluster)
		switch {
		case res[i].recv != nil:
			// custody time known: below the bound or out
			if *res[i].recv < 0 || uint64(*res[i].recv) >= cb {
				res[i].keep = false
				afterBasis++
			}
		case c.obj.LastModified.Add(skew).UnixNano() < int64(min(cb, math.MaxInt64)):
			// received_at <= its PUT (edge clock) <= LastModified + skew < C
		default:
			res[i].basisCheck = true
		}
	}
	for i, c := range cands {
		if res[i].keep && !res[i].mismatch {
			kept = append(kept, keptObject{cluster: c.cluster, obj: c.obj})
		}
	}
	idxRes, idxRep := p.resolveIndex(ctx, req, kept)

	ttl := time.Duration(p.cfg.URLTTLS) * time.Second
	signedAt := p.now()
	plan := &Plan{
		Signal: req.Signal, FromNs: req.FromNs, ToNs: req.ToNs, Clusters: clusters,
		From:         time.Unix(0, req.FromNs).UTC().Format(time.RFC3339Nano),
		To:           time.Unix(0, req.ToNs).UTC().Format(time.RFC3339Nano),
		SnapshotNote: "no sealer snapshots exist yet: planned from a LIST of the v2 lanes at listed_at",
		ListedAt:     listedAt.UTC().Format(time.RFC3339Nano),
		ExpiresAt:    signedAt.Add(ttl).UTC().Format(time.RFC3339Nano),
		ReplanAfter:  signedAt.Add(ttl - time.Duration(p.cfg.ReplanMarginS)*time.Second).UTC().Format(time.RFC3339Nano),
		URLTTLS:      p.cfg.URLTTLS,
		Rules:        Rules,
		Objects:      []Object{},
		Index:        idxRep,
	}
	maxLate := p.wm.MaxLateness()
	h := sha256.New()
	for i, c := range cands {
		r := res[i]
		if r.mismatch {
			plan.Mismatched++
			continue
		}
		if !r.keep {
			continue
		}
		ir, filtered := idxRes[c.obj.Key]
		if filtered && ir.Status == lakeidx.None {
			continue // the index rules it out
		}
		if len(plan.Objects) >= p.cfg.MaxObjects {
			return nil, &TooLarge{Objects: len(plan.Objects) + 1, Max: p.cfg.MaxObjects}
		}
		u, err := p.store.Presign(ctx, c.obj.Key, ttl)
		if err != nil {
			return nil, err
		}
		o := Object{URL: u, Size: c.obj.Size, Key: c.obj.Key, Cluster: c.cluster, Producer: c.producer, Epoch: c.ep, Seq: c.seq,
			LastModified: c.obj.LastModified.UTC().Format(time.RFC3339Nano), Refined: r.ok}
		if r.ok {
			minT, maxT, rows := r.minT, r.maxT, r.rows
			o.MinTimeNs, o.MaxTimeNs, o.Rows, o.Part = &minT, &maxT, &rows, r.part
			if r.recv != nil {
				o.ReceivedNs = r.recv
				// received − min_time > max_lateness, without overflow
				if o.Late = *r.recv > minT && uint64(*r.recv-minT) > uint64(maxLate); o.Late {
					plan.LateObjects++
				}
			}
		} else {
			plan.Unrefined++
		}
		if filtered {
			o.Index, o.RowGroups = ir.Status, ir.RowGroups
		}
		if r.basisCheck {
			cb, _ := b.C(c.cluster)
			o.BasisCheck, o.ReceivedBeforeNs = true, &cb
			plan.BasisUnverified++
		}
		plan.Objects = append(plan.Objects, o)
		plan.TotalBytes += o.Size
		h.Write([]byte(c.obj.Key))
		h.Write([]byte{0})
	}
	plan.ObjectsHash = hex.EncodeToString(h.Sum(nil))
	w := &completeness.Window{FromNs: req.FromNs, ToNs: req.ToNs}
	plan.State, plan.WmScope, plan.AfterBasis = wmState, wmScope, afterBasis
	if b != nil {
		// at the basis: its bound is complete_through, its policy the bridge
		plan.Label = completeness.LabelAt("lake", wmState, b.MinFor(clusters), w, p.now(), p.wm.Key(), pr.MayCluster, time.Duration(b.MaxLatenessNs))
		plan.Rules = append(append([]string{}, Rules[:len(Rules)-1]...), BasisRules...)
	} else {
		plan.Label = completeness.MakeLabel("lake", wmState, w, p.now(), p.wm.Key(), pr.MayCluster, maxLate)
	}

	// GC deletes ingested slots (D12): the lanes' oldest rows may be gone
	plan.StartComplete = true
	gc, gcErr := p.gcDoc(ctx)
	switch {
	case gcErr != nil:
		plan.StartComplete = false
		plan.GCNote = "gc.json unreadable: whether older slots were deleted is unknown (" + gcErr.Error() + ")"
	case gc != nil:
		lanes := make([]string, 0, len(oldest))
		for l := range oldest {
			lanes = append(lanes, l)
		}
		sort.Strings(lanes)
		for _, l := range lanes {
			if gc.deleted(l) && time.Unix(0, req.FromNs).Before(oldest[l]) {
				plan.GCTruncated = append(plan.GCTruncated, l)
			}
		}
		if len(plan.GCTruncated) > 0 {
			plan.StartComplete = false
			plan.GCNote = "GC deleted ingested slots of these lanes, and the window starts before their oldest remaining object: older rows are in central (/v1/query)"
		}
	}
	if b != nil && !plan.StartComplete {
		// never answered with less data: the rows GC deleted were in the
		// answer at this basis (or may have been)
		if gcErr != nil {
			return nil, &basis.Refusal{Status: 503, Reason: basis.ReasonUnverifiable, Detail: plan.GCNote}
		}
		return nil, &basis.Refusal{Status: 410, Reason: basis.ReasonExpired,
			Detail: "GC deleted ingested slots of " + strings.Join(plan.GCTruncated, ", ") + " before the window's start: the plan at this basis would lack rows (they are in central: /v1/query at the same basis)"}
	}
	if !plan.StartComplete {
		plan.Partial = true
		if plan.Completeness == "complete" {
			plan.Completeness = "partial"
		}
	}
	return plan, nil
}

// clusters resolves the plan's clusters against the principal: a named
// cluster outside the scope is a denial (never silently dropped); none named
// means every cluster in scope.
func (p *Planner) clusters(ctx context.Context, pr *auth.Principal, asked []string) ([]string, error) {
	if !pr.AllClusters && len(pr.Clusters) == 0 {
		return nil, &Denied{"empty_scope", "the token grants no cluster"}
	}
	var out []string
	if len(asked) > 0 {
		for _, c := range asked {
			if !sqlscope.ClusterRE.MatchString(c) {
				return nil, &BadRequest{"bad_cluster", fmt.Sprintf("cluster %q is not a valid name", c)}
			}
			if !pr.MayCluster(c) {
				return nil, &Denied{"cluster_not_in_scope", fmt.Sprintf("cluster %q is not in the token's scope", c)}
			}
			out = append(out, c)
		}
	} else if pr.AllClusters {
		prefix := ""
		if p.cfg.Root != "" {
			prefix = p.cfg.Root + "/"
		}
		dirs, err := p.store.Dirs(ctx, prefix)
		if err != nil {
			return nil, err
		}
		for _, d := range dirs {
			if sqlscope.ClusterRE.MatchString(d) { // "_consumer" and other control prefixes are not clusters
				out = append(out, d)
			}
		}
	} else {
		out = append(out, pr.Clusters...)
	}
	sort.Strings(out)
	uniq := out[:0]
	for i, c := range out {
		if i == 0 || out[i-1] != c {
			uniq = append(uniq, c)
		}
	}
	return uniq, nil
}

type gcDoc struct {
	DeletedBelow map[string]map[string]uint64 `json:"deleted_below"`
	Retired      map[string][]string          `json:"retired"`
}

func (g *gcDoc) deleted(lane string) bool {
	return len(g.DeletedBelow[lane]) > 0 || len(g.Retired[lane]) > 0
}

func (p *Planner) gcDoc(ctx context.Context) (*gcDoc, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.gcAt.IsZero() && p.now().Sub(p.gcAt) < time.Duration(p.cfg.GCCacheS)*time.Second {
		return p.gc, p.gcErr
	}
	p.gcAt = p.now()
	body, _, err := p.store.Get(ctx, join(p.cfg.Ctl, "gc.json"))
	switch {
	case err != nil:
		p.gc, p.gcErr = nil, err
	case body == nil:
		p.gc, p.gcErr = nil, nil // GC never ran: nothing deleted
	default:
		var d gcDoc
		if err := json.Unmarshal(body, &d); err != nil {
			p.gc, p.gcErr = nil, errors.New("gc.json: "+err.Error())
		} else {
			p.gc, p.gcErr = &d, nil
		}
	}
	return p.gc, p.gcErr
}
