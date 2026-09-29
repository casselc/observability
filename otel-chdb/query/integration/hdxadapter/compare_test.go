package hdxadapter

import (
	"encoding/json"
	"regexp"
	"strconv"
	"testing"

	"github.com/casselc/observability/otel-chdb/testgate/tracetag"
)

// Answers of statements with an order-undefined aggregate.
//
// groupUniqArray keeps its elements in a hash set and groupArray in arrival
// order; with several threads each thread builds its own state and the states
// are merged in the order the threads finish, so the same statement on the
// same data returns the same elements in a different order from run to run
// (and a rewritten statement, reading through dictionaries, with a different
// plan, more often still). HyperDX's value-suggestion statement
// (groupUniqArray(20) over a sample, 18 of the entity rewrite proxy's
// corpus) is such a statement. The replay compared those answers up to array
// order; the rewrite proxy's chain compared them exactly and failed on
// nightly run 22 (after/fleet #331, param14: the same four pods in another
// order) and passed on the rerun.
//
// With a size cap, groupUniqArray(N) / groupArray(N) keep whichever N
// elements arrive first: past the cap even the SET is undefined. The test
// data keeps every column below 20 distinct values; an array that reaches a
// cap makes the comparison meaningless, which arrayVerdict reports as such
// rather than passing or flaking.

var unorderedAgg = regexp.MustCompile(`(?i)\b(groupUniqArray|groupArray|groupArrayDistinct)\s*(?:\(\s*(\d+)\s*\))?\s*\(`)

// unorderedAggregates reports whether sql uses an aggregate whose element
// order ClickHouse does not define, and the smallest size cap among them
// (0: none capped).
func unorderedAggregates(sql string) (found bool, minCap int) {
	for _, m := range unorderedAgg.FindAllStringSubmatch(sql, -1) {
		found = true
		if m[2] != "" {
			if n, err := strconv.Atoi(m[2]); err == nil && (minCap == 0 || n < minCap) {
				minCap = n
			}
		}
	}
	return found, minCap
}

// Verdicts of arrayVerdict.
const (
	verdictDiffers    = ""                        // not the same, even up to array order (or no unordered aggregate)
	verdictArrayOrder = "equal-up-to-array-order" // same elements, order undefined by ClickHouse
	verdictAtCap      = "undefined-at-cap"        // an array reached the aggregate's cap: its elements are undefined
)

// arrayVerdict judges two answers of sql that differ exactly: equal up to
// the order of array elements, but only when sql has an order-undefined
// aggregate; or not comparable, when an array reached the aggregate's cap.
func arrayVerdict(sql string, a, n canon) string {
	found, maxN := unorderedAggregates(sql)
	if !found {
		return verdictDiffers
	}
	if maxN > 0 && (longestArray(a) >= maxN || longestArray(n) >= maxN) {
		return verdictAtCap
	}
	if sameUpToArrayOrder(a, n) {
		return verdictArrayOrder
	}
	return verdictDiffers
}

// longestArray is the length of the longest array value in any row.
func longestArray(c canon) int {
	longest := 0
	var walk func(v any)
	walk = func(v any) {
		switch x := v.(type) {
		case []any:
			if len(x) > longest {
				longest = len(x)
			}
			for _, e := range x {
				walk(e)
			}
		case map[string]any:
			for _, e := range x {
				walk(e)
			}
		}
	}
	for _, r := range c.rows {
		var row []any
		if json.Unmarshal([]byte(r), &row) != nil {
			continue
		}
		for _, v := range row { // a row is an array of column values, not itself a value
			walk(v)
		}
	}
	return longest
}

// The nightly run 22 failure, as data: the answers the chain and ClickHouse
// gave for rwproxy statement #331 differed only in array order.
func TestUnorderedAggregateAnswersCompareAsSets(t *testing.T) {
	tracetag.Covers(t, "D", "H-5", "H-6")
	const uniq20 = "WITH s AS (SELECT a AS p0, b AS p1 FROM t LIMIT 10) SELECT groupUniqArray(20)(p0) AS p0, groupUniqArray(20)(p1) AS p1 FROM s FORMAT JSON"
	cols := []string{"p0 Array(String)", "p1 Array(String)"}
	via := canon{cols: cols, rows: []string{`[["INFO","ERROR"],["","cart-77aa-2","frontend-f778-0","payment-5c9d-1"]]`}}
	nat := canon{cols: cols, rows: []string{`[["ERROR","INFO"],["","frontend-f778-0","cart-77aa-2","payment-5c9d-1"]]`}}
	if e, r := same(via, nat); e || r {
		t.Fatalf("the #331 pair should differ exactly: %v %v", e, r)
	}
	if v := arrayVerdict(uniq20, via, nat); v != verdictArrayOrder {
		t.Errorf("#331 shape: %q, want %q", v, verdictArrayOrder)
	}
	// different elements are a mismatch, whatever the aggregate
	other := canon{cols: cols, rows: []string{`[["ERROR","INFO"],["","frontend-f778-0","cart-77aa-2","billing-1"]]`}}
	if v := arrayVerdict(uniq20, via, other); v != verdictDiffers {
		t.Errorf("different elements: %q", v)
	}
	// without an order-undefined aggregate, array order is part of the answer
	// (arraySort, an ORDER BY inside groupArray's argument, a literal array)
	if v := arrayVerdict("SELECT arraySort(x) AS p0, y AS p1 FROM t FORMAT JSON", via, nat); v != verdictDiffers {
		t.Errorf("no unordered aggregate: %q", v)
	}
	// an array at the cap: which 20 elements is undefined, so is the comparison
	big := make([]string, 20)
	for i := range big {
		big[i] = strconv.Quote(strconv.Itoa(i))
	}
	capped := canon{cols: cols, rows: []string{`[["INFO"],[` + joinComma(big) + `]]`}}
	if v := arrayVerdict(uniq20, capped, nat); v != verdictAtCap {
		t.Errorf("at cap: %q", v)
	}
	for sql, want := range map[string][2]int{
		"SELECT groupUniqArray(x) FROM t":                       {1, 0},
		"SELECT groupUniqArray(20)(x), groupArray(5)(y) FROM t": {1, 5},
		"SELECT groupArrayDistinct ( x ) FROM t":                {1, 0},
		"SELECT arraySort(x), count() FROM t":                   {0, 0},
	} {
		f, c := unorderedAggregates(sql)
		if (f && want[0] == 0) || (!f && want[0] == 1) || c != want[1] {
			t.Errorf("%s: %v %d, want %v", sql, f, c, want)
		}
	}
}

func joinComma(xs []string) string {
	out := ""
	for i, x := range xs {
		if i > 0 {
			out += ","
		}
		out += x
	}
	return out
}
