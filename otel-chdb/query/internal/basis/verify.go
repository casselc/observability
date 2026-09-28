package basis

import (
	"fmt"
	"net/http"
	"time"
)

// Refusal is why a request's basis cannot be served, with its HTTP status.
type Refusal struct {
	Status int
	Reason string
	Detail string
}

func (r *Refusal) Error() string { return r.Reason + ": " + r.Detail }

func refuse(status int, reason, format string, a ...any) *Refusal {
	return &Refusal{Status: status, Reason: reason, Detail: fmt.Sprintf(format, a...)}
}

// Refusal reasons.
const (
	ReasonInvalid      = "basis_invalid"      // 400: not a token this service issued
	ReasonNotInScope   = "basis_not_in_scope" // 403: the basis names a cluster outside the caller's scope
	ReasonScope        = "basis_scope"        // 400: the request reads clusters or signals the basis does not cover
	ReasonAhead        = "basis_ahead"        // 409: a C above the scope's current complete_through
	ReasonExpired      = "basis_expired"      // 410: retention or GC removed rows the answer at it held
	ReasonUnverifiable = "basis_unverifiable" // 503: the watermark (or gc.json) cannot be read to check it
	ReasonUnservable   = "basis_unservable"   // 400: a table read has no received (or cluster) column
	ReasonRegressed    = "basis_regressed"    // 409: basis_from is above basis for some cluster
	ReasonDeltaNeeds   = "basis_from_needs_basis"
)

// Invalid wraps a Decode error.
func Invalid(err error) *Refusal { return refuse(http.StatusBadRequest, ReasonInvalid, "%v", err) }

// Resolve checks b against the caller's scope and returns the clusters the
// request runs over: asked (the request's own clusters, each of which the
// basis must cover), or, when none was asked, the basis's clusters (nil for
// a fleet basis: the caller's whole scope, which must then be every
// cluster). A basis never widens scope: every cluster it names must be one
// the caller may read, and a fleet basis needs a fleet caller. A basis for
// other clusters is refused, not intersected.
func Resolve(b *Basis, may func(string) bool, allClusters bool, asked []string) ([]string, *Refusal) {
	if b.IsFleet() {
		if !allClusters {
			return nil, refuse(http.StatusForbidden, ReasonNotInScope, "a fleet basis needs a caller with every cluster")
		}
	} else {
		for _, c := range b.ClusterNames() {
			if !may(c) {
				return nil, refuse(http.StatusForbidden, ReasonNotInScope, "the basis names cluster %q, which is not in the token's scope", c)
			}
		}
	}
	if len(asked) > 0 {
		for _, c := range asked {
			if _, ok := b.C(c); !ok {
				return nil, refuse(http.StatusBadRequest, ReasonScope, "the basis does not cover cluster %q", c)
			}
		}
		return asked, nil
	}
	return b.ClusterNames(), nil
}

// CheckSignals: the statement reads signals sigs (nil: every signal).
func CheckSignals(b *Basis, sigs []string) *Refusal {
	if b.CoversSignals(sigs) {
		return nil
	}
	want := "every signal"
	if sigs != nil {
		want = fmt.Sprint(sigs)
	}
	return refuse(http.StatusBadRequest, ReasonScope, "the basis was taken for signals %v; the request reads %s", b.View().Signals, want)
}

// CheckCurrent refuses a basis above the scope's complete_through now:
// current(c) is cluster c's value for the basis's signals (c == Fleet: the
// fleet's); ok false when it cannot be read. clusters nil means the basis
// is a fleet one.
func CheckCurrent(b *Basis, clusters []string, current func(c string) (uint64, bool)) *Refusal {
	names := clusters
	if b.IsFleet() {
		names = []string{Fleet}
	}
	for _, c := range names {
		want, _ := b.C(c)
		have, ok := current(c)
		if !ok {
			return refuse(http.StatusServiceUnavailable, ReasonUnverifiable, "complete_through for %s cannot be read: the basis cannot be checked", c)
		}
		if want > have {
			return refuse(http.StatusConflict, ReasonAhead, "the basis for %s (%s) is above its complete_through (%s): an answer there would not be stable",
				c, fmtNs(int64(want)), fmtNs(int64(have)))
		}
	}
	return nil
}

// CheckExpiry refuses a basis whose answer retention may have cut: a C
// older than the retention (custody age, D19: rows received before now −
// retention are deleted), or a window starting before it (its rows'
// custody times reach back to its start − skew). Never answered with less
// data. retention <= 0 disables the check.
func CheckExpiry(b *Basis, clusters []string, now time.Time, retention time.Duration, windowFromNs *int64, skew time.Duration) *Refusal {
	if retention <= 0 {
		return nil
	}
	floor := now.Add(-retention).UnixNano()
	names := clusters
	if b.IsFleet() || names == nil {
		names = nil
		for c := range b.Clusters {
			names = append(names, c)
		}
	}
	for _, c := range names {
		if v, _ := b.C(c); int64(v) < floor {
			return refuse(http.StatusGone, ReasonExpired, "the basis for %s (%s) is older than retention (%s): its rows are being deleted",
				c, fmtNs(int64(v)), retention)
		}
	}
	if windowFromNs != nil && *windowFromNs-int64(skew) < floor {
		return refuse(http.StatusGone, ReasonExpired, "the window starts before retention (%s ago): its rows are being deleted, so no answer there is stable", retention)
	}
	return nil
}

// CheckDelta: from (basis_from) and to (basis) cover the same clusters and
// signals, and from is at or below to for each cluster the request reads,
// so the delta [from, to) is a set of rows no earlier answer at from had,
// and no later delta from to will have.
func CheckDelta(from, to *Basis, clusters []string) *Refusal {
	if from.IsFleet() != to.IsFleet() {
		return refuse(http.StatusBadRequest, ReasonScope, "basis_from and basis must both be fleet bases, or both per cluster")
	}
	if !to.CoversSignals(from.Signals) || !from.CoversSignals(to.Signals) {
		return refuse(http.StatusBadRequest, ReasonScope, "basis_from and basis were taken for different signals")
	}
	names := clusters
	if to.IsFleet() {
		names = []string{Fleet}
	}
	for _, c := range names {
		f, ok1 := from.C(c)
		t, ok2 := to.C(c)
		if !ok1 || !ok2 {
			return refuse(http.StatusBadRequest, ReasonScope, "basis_from and basis must both cover cluster %q", c)
		}
		if f > t {
			return refuse(http.StatusConflict, ReasonRegressed, "basis_from for %s (%s) is above basis (%s)", c, fmtNs(int64(f)), fmtNs(int64(t)))
		}
	}
	return nil
}
