package sqlscope

import (
	"fmt"
	"sort"
	"strings"
	"time"

	chp "github.com/AfterShip/clickhouse-sql-parser/parser"
)

// LateCount is the statement that counts late rows (STPA CAST row 26) in
// the tables of a finished statement: rows whose custody time (the table's
// received_column) is more than maxLateness after their event time (its
// time_column). It runs with the same additional_table_filters as the
// statement, so it counts only rows the caller may see, in the window.
type LateCount struct {
	SQL string
	// Counted are the tables it counts; Uncounted those it cannot (no time
	// or received column: late rows there are not measured).
	Counted, Uncounted []string
}

// LateCount builds it for the tables r reads; SQL is empty when none has
// both columns. The statement is built from configuration (names checked
// by NewPolicy, expressions parsed there), never from the caller's text.
func (p *Policy) LateCount(r *Result, maxLateness time.Duration) (*LateCount, error) {
	lc := &LateCount{}
	var parts []string
	tables := append([]string(nil), r.Tables...)
	sort.Strings(tables)
	for _, fqn := range tables {
		t := p.Tables[fqn]
		if t == nil || t.Scope == "metadata" {
			continue // schema rows have no event time
		}
		if t.tcol == nil || t.rcol == nil {
			lc.Uncounted = append(lc.Uncounted, fqn)
			continue
		}
		lc.Counted = append(lc.Counted, fqn)
		parts = append(parts, fmt.Sprintf("SELECT %s AS t, count() AS n FROM %s WHERE %s > (%s + toIntervalNanosecond(%d))",
			Quote(fqn), fqn, chp.Format(t.rcol), chp.Format(t.tcol), max(maxLateness, 0).Nanoseconds()))
	}
	if len(parts) == 0 {
		return lc, nil
	}
	lc.SQL = strings.Join(parts, " UNION ALL ")
	st, err := chp.NewParser(lc.SQL).ParseStmts()
	if err != nil || len(st) != 1 {
		return nil, reject("roundtrip", "the late-row count does not parse: %v", err)
	}
	return lc, nil
}

// DeltaCount is the statement that counts, per table a finished statement
// reads, the rows its filters admit (D30: run with a delta's filters, the
// rows with basis_from <= received_at < basis in the scope and window). It
// is built like LateCount: from configuration, never from the caller's
// text. Tables without a received column are Uncounted (a statement over
// them is refused at a basis before this runs).
func (p *Policy) DeltaCount(r *Result) (*LateCount, error) {
	lc := &LateCount{}
	var parts []string
	tables := append([]string(nil), r.Tables...)
	sort.Strings(tables)
	for _, fqn := range tables {
		t := p.Tables[fqn]
		if t == nil || t.Scope == "metadata" {
			continue
		}
		if t.rcol == nil {
			lc.Uncounted = append(lc.Uncounted, fqn)
			continue
		}
		lc.Counted = append(lc.Counted, fqn)
		parts = append(parts, fmt.Sprintf("SELECT %s AS t, count() AS n FROM %s", Quote(fqn), fqn))
	}
	if len(parts) == 0 {
		return lc, nil
	}
	lc.SQL = strings.Join(parts, " UNION ALL ")
	st, err := chp.NewParser(lc.SQL).ParseStmts()
	if err != nil || len(st) != 1 {
		return nil, reject("roundtrip", "the delta count does not parse: %v", err)
	}
	return lc, nil
}
