package lake

// Plans narrowed by the lake index (FORMAT.md §7, D27). The service resolves
// the filter itself, against the segments of the clusters already in the
// plan (so the caller's cluster grant is the only one involved), and plans
// only what may match:
//
//   - an object a verified segment covers and rules out is not planned;
//   - a covered object that may match is planned with `index: "hit"` and
//     the row groups that may hold a match;
//   - every other object (not yet indexed, a segment unreadable, corrupt,
//     over the byte budget) is planned with `index: "scan"`, as without a
//     filter. The index can only ever remove what cannot match.

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/casselc/observability/otel-chdb/query/internal/lakeidx"
	"github.com/casselc/observability/otel-chdb/query/internal/store"
)

// IndexConfig tunes index resolution.
type IndexConfig struct {
	// Disabled turns resolution off: a filter is then accepted and every
	// object is "scan".
	Disabled bool `json:"disabled"`
	// MaxMBPerPlan bounds index bytes read (cache misses) for one plan.
	MaxMBPerPlan int `json:"max_mb_per_plan"`
	// CacheMB bounds the block cache (headers are cached by count).
	CacheMB int `json:"cache_mb"`
}

// Limits on a filter.
const (
	MaxTerms   = 8
	MaxTermLen = 256
)

var traceIDRE = regexp.MustCompile(`^[0-9a-f]{32}$`)

// IndexReport is the plan's account of its filter.
type IndexReport struct {
	lakeidx.Report
	// Rule is how to read the answer (every plan with a filter carries it).
	Rule string `json:"rule"`
}

// IndexRule is the contract of a filtered plan.
const IndexRule = "Objects marked index=hit hold matches only in row_groups; objects marked scan must be read whole, " +
	"as without a filter; objects the index ruled out are not planned (index.pruned counts them). " +
	"A trace_id matches the TraceId column case-insensitively; each term is a case-insensitive substring of Body " +
	"(JavaScript toLowerCase), and every term must match."

func newResolver(cfg Config, st store.Store) *lakeidx.Resolver {
	if cfg.Index.Disabled {
		return nil
	}
	return lakeidx.NewResolver(lakeidx.ResolverConfig{Root: cfg.Root, MaxBytesPerPlan: int64(cfg.Index.MaxMBPerPlan) << 20,
		CacheBytes: int64(cfg.Index.CacheMB) << 20}, st)
}

// checkFilter validates and normalises a plan's filter.
func (p *Planner) checkFilter(req *Request) error {
	if req.TraceID != "" {
		id := strings.ToLower(strings.TrimSpace(req.TraceID))
		if !traceIDRE.MatchString(id) {
			return &BadRequest{"bad_trace_id", "trace_id is 32 hex digits"}
		}
		if _, _, ok := lakeidx.Columns(req.Signal); !ok {
			return &BadRequest{"bad_filter", fmt.Sprintf("signal %q has no trace-id index; filter traces or logs", req.Signal)}
		}
		req.TraceID = id
	}
	if len(req.Terms) > 0 {
		if _, text, _ := lakeidx.Columns(req.Signal); text == "" {
			return &BadRequest{"bad_filter", fmt.Sprintf("terms search the log body; signal %q has none", req.Signal)}
		}
		if len(req.Terms) > MaxTerms {
			return &BadRequest{"bad_filter", fmt.Sprintf("at most %d terms", MaxTerms)}
		}
		for _, t := range req.Terms {
			if t == "" || len(t) > MaxTermLen {
				return &BadRequest{"bad_filter", fmt.Sprintf("a term is 1 to %d bytes", MaxTermLen)}
			}
		}
	}
	return nil
}

type keptObject struct {
	cluster string
	obj     store.Object
}

// resolveIndex resolves the filter per cluster. It returns nil maps when
// the request has no filter.
func (p *Planner) resolveIndex(ctx context.Context, req Request, kept []keptObject) (map[string]lakeidx.ObjResult, *IndexReport) {
	f := lakeidx.Filter{TraceID: req.TraceID, Terms: req.Terms}
	if f.Empty() {
		return nil, nil
	}
	rep := &IndexReport{Rule: IndexRule}
	rep.TraceID, rep.Terms = f.TraceID, f.Terms
	out := map[string]lakeidx.ObjResult{}
	if p.idx == nil {
		for _, k := range kept {
			out[k.obj.Key] = lakeidx.ObjResult{Status: lakeidx.Scan}
		}
		rep.Scan = len(kept)
		rep.Note = "index resolution is disabled on this service"
		return out, rep
	}
	byCluster := map[string][]store.Object{}
	var order []string
	for _, k := range kept {
		if _, ok := byCluster[k.cluster]; !ok {
			order = append(order, k.cluster)
		}
		byCluster[k.cluster] = append(byCluster[k.cluster], k.obj)
	}
	for _, cl := range order {
		res, r := p.idx.Resolve(ctx, cl, req.Signal, byCluster[cl], f)
		for k, v := range res {
			out[k] = v
		}
		if rep.Constraints == nil {
			rep.Constraints = r.Constraints
		}
		rep.Segments += r.Segments
		rep.Requests += r.Requests
		rep.Bytes += r.Bytes
		rep.CacheHits += r.CacheHits
		rep.Covered += r.Covered
		rep.Scan += r.Scan
		rep.Pruned += r.Pruned
		rep.ElapsedMs += r.ElapsedMs
		for _, e := range r.Errors {
			if len(rep.Errors) < 20 {
				rep.Errors = append(rep.Errors, cl+": "+e)
			}
		}
		if r.Note != "" {
			rep.Note = r.Note
		}
	}
	return out, rep
}
