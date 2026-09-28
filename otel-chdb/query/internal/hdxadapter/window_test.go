package hdxadapter

import (
	"testing"
)

func TestDeriveWindow(t *testing.T) {
	tb := Tables{DefaultDatabase: "otel", TimeColumns: map[string]string{"otel.otel_logs": "Timestamp", "otel.otel_traces": "Timestamp"}}
	const lo, hi = "fromUnixTimestamp64Milli({a:Int64})", "fromUnixTimestamp64Milli({b:Int64})"
	p := map[string]string{"a": "1000", "b": "2000", "c": "1500"}
	ms := int64(1_000_000)
	for q, want := range map[string]*Window{
		"SELECT count() FROM otel_logs WHERE (Timestamp >= " + lo + " AND Timestamp <= " + hi + ") AND Body != ''": {1000 * ms, 2000*ms + 1},
		"SELECT count() FROM otel_logs WHERE Timestamp > " + lo + " AND Timestamp < " + hi:                         {1000*ms + 1, 2000 * ms},
		// the same bounds in a CTE and an IN subquery
		"WITH s AS (SELECT Body FROM otel_logs WHERE Timestamp >= " + lo + " AND Timestamp <= " + hi + ") SELECT * FROM s WHERE Body IN (SELECT Body FROM otel.otel_logs WHERE Timestamp >= " + lo + " AND Timestamp <= " + hi + ")": {1000 * ms, 2000*ms + 1},
		// conjunctive bounds: the tightest
		"SELECT count() FROM otel_logs WHERE Timestamp >= " + lo + " AND Timestamp <= " + hi + " AND Timestamp >= fromUnixTimestamp64Milli({c:Int64})": {1500 * ms, 2000*ms + 1},
		// no window: an OR, one bound, different bounds per read, an unknown
		// table, a join, a non-literal bound, a bound on another column
		"SELECT count() FROM otel_logs WHERE Timestamp >= " + lo + " OR Timestamp <= " + hi: nil,
		"SELECT count() FROM otel_logs WHERE Timestamp >= " + lo:                            nil,
		"SELECT count() FROM otel_logs WHERE Timestamp >= " + lo + " AND Timestamp <= " + hi + " AND Body IN (SELECT Body FROM otel_logs WHERE Timestamp >= fromUnixTimestamp64Milli({c:Int64}) AND Timestamp <= " + hi + ")": nil,
		"SELECT count() FROM otel_logs WHERE Timestamp >= " + lo + " AND Timestamp <= " + hi + " AND Body IN (SELECT Body FROM otel_logs)":                                                                                    nil,
		"SELECT count() FROM otel_logs_kv_rollup_15m WHERE Timestamp >= " + lo + " AND Timestamp <= " + hi:                                                                                                                    nil,
		"SELECT count() FROM otel_logs AS l JOIN otel_traces AS t ON l.TraceId = t.TraceId WHERE Timestamp >= " + lo + " AND Timestamp <= " + hi:                                                                              nil,
		"SELECT count() FROM otel_logs WHERE Timestamp >= now() - INTERVAL 1 HOUR AND Timestamp <= " + hi:                                                                                                                     nil,
		"SELECT count() FROM otel_logs WHERE TimestampTime >= " + lo + " AND TimestampTime <= " + hi:                                                                                                                          nil,
		"SELECT count() FROM otel_logs WHERE NOT (Timestamp >= " + lo + " AND Timestamp <= " + hi + ")":                                                                                                                       nil,
		"SELECT count() FROM otel_logs WHERE Timestamp >= " + hi + " AND Timestamp <= " + lo:                                                                                                                                  nil,
	} {
		st, err := tb.Prepare(q+" FORMAT JSON", p, "")
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		switch {
		case want == nil && st.Window != nil:
			t.Errorf("%s: window %+v, want none", q, *st.Window)
		case want != nil && (st.Window == nil || *st.Window != *want):
			t.Errorf("%s: window %+v, want %+v", q, st.Window, *want)
		}
	}
}
