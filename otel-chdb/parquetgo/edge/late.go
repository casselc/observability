package edge

import (
	"bytes"
	"context"
	"iter"
	"strconv"
	"time"

	"github.com/casselc/observability/otel-chdb/parquetgo"
	"github.com/casselc/observability/otel-chdb/parquetgo/commit"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// The late split (DECISIONS.md D31, ../../FORMAT.md §2.2). An object's
// oscope-min-time/oscope-max-time span all its rows, so a request with one
// old row (a late batch, a replay, a skewed clock) would give its object a
// range over every window in between, and the lake planner would read it
// for all of them. With a bound B > 0, a traces or logs request whose rows
// reach back more than B before its NEWEST row is committed as two objects
// in its lane:
//
//   - bulk: the rows with event time >= max − B;
//   - late: the rest.
//
// The cutoff is relative to the request's own newest row, not to
// received_at, so it is a function of the request's bytes and B alone:
// every retry and every replay (with or without a persistent queue, whose
// received_at the edge may not have) splits the same rows the same way,
// and the content keys, which name the part and B, find the committed parts
// again. A bound changed between two attempts gives other keys: at worst a
// duplicate, never a loss. The row time is the one the object's range is
// made of: a span's start, a log record's time (its observed time when 0).

// SpanTimes is every span's event time, in walk order.
func SpanTimes(td ptrace.Traces) iter.Seq[uint64] {
	return func(yield func(uint64) bool) {
		rss := td.ResourceSpans()
		for i := 0; i < rss.Len(); i++ {
			sss := rss.At(i).ScopeSpans()
			for j := 0; j < sss.Len(); j++ {
				spans := sss.At(j).Spans()
				for k := 0; k < spans.Len(); k++ {
					if !yield(uint64(spans.At(k).StartTimestamp())) {
						return
					}
				}
			}
		}
	}
}

// LogTimes is every log record's event time (its time, else its observed
// time), in walk order.
func LogTimes(ld plog.Logs) iter.Seq[uint64] {
	return func(yield func(uint64) bool) {
		rls := ld.ResourceLogs()
		for i := 0; i < rls.Len(); i++ {
			sls := rls.At(i).ScopeLogs()
			for j := 0; j < sls.Len(); j++ {
				recs := sls.At(j).LogRecords()
				for k := 0; k < recs.Len(); k++ {
					r := recs.At(k)
					ts := r.Timestamp()
					if ts == 0 {
						ts = r.ObservedTimestamp()
					}
					if !yield(uint64(ts)) {
						return
					}
				}
			}
		}
	}
}

// LateCut is the split's cutoff for rows with these event times and bound
// after: max − after, and ok when at least one row is below it (then both
// parts are non-empty: the newest row is always bulk). ok is false for
// after <= 0, no rows, or rows all within after of the newest.
func LateCut(times iter.Seq[uint64], after time.Duration) (cut uint64, ok bool) {
	if after <= 0 {
		return 0, false
	}
	var hi uint64
	n := 0
	for ts := range times {
		hi = max(hi, ts)
		n++
	}
	if n == 0 || hi < uint64(after) {
		return 0, false
	}
	cut = hi - uint64(after)
	for ts := range times {
		if ts < cut {
			return cut, true
		}
	}
	return 0, false
}

// SplitContent is the content key of one part of a split request:
// BLAKE3-128 of "{namespace}/{part}/{bound ns}\0" + the request's bytes
// (namespaces hold no '/', so it never equals an unsplit request's key).
func SplitContent(ns, part string, after time.Duration, request []byte) string {
	return commit.ContentHash(ns+"/"+part+"/"+strconv.FormatInt(int64(after), 10), request)
}

// pushSplit commits a traces or logs request as its bulk and then its late
// object: like a metrics request's objects, it succeeds only when both have
// committed, and a retry finds a committed part by its key.
func (e *Edge) pushSplit(ctx context.Context, ns string, request []byte, received, cut uint64,
	walk func(*parquetgo.PGEncoder, *bytes.Buffer, *parquetgo.Envelope) (int, error)) error {
	after := e.cfg.LateSplitAfter
	var parts []part
	for _, p := range []struct {
		name string
		keep func(uint64) bool
	}{
		{commit.PartBulk, func(ts uint64) bool { return ts >= cut }},
		{commit.PartLate, func(ts uint64) bool { return ts < cut }},
	} {
		content := SplitContent(ns, p.name, after, request)
		ann := e.announcing(e.lane(ns, content), received)
		extra := map[string]string{commit.MetaPart: p.name, commit.MetaLateAfter: strconv.FormatInt(int64(after), 10)}
		parts = append(parts, part{ns: ns, content: content,
			onCommit: func(r commit.Ref) { e.committed(ann, r) },
			encode: func(r commit.Ref) (commit.Object, error) {
				return e.pgObject(ns, content, r, received, ann, extra, func(enc *parquetgo.PGEncoder, buf *bytes.Buffer, env *parquetgo.Envelope) (int, error) {
					env.Keep = p.keep
					return walk(enc, buf, env)
				})
			}})
	}
	return e.commitParts(ctx, parts, true)
}
