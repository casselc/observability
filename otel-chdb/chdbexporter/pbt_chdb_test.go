//go:build pbt

package chdbexporter

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"hegel.dev/go/hegel"
)

// Round trips through chDB: generated traces and logs are pushed through
// every insert format (and RowBinary without staging tables), then read back
// with SELECT * ... FORMAT RowBinary and decoded. Every variant must store
// exactly the rows the Go encoder produced, as a multiset: this generalises
// TestFormatsStoreIdenticalRows from one fixed input to arbitrary ones, and
// compares against the encoder's bytes rather than only against each other.

type rtVariant struct {
	name string
	e    *chdbExporter
	db   string
}

func startRoundTripExporters(t *testing.T, prefix string) []rtVariant {
	var vs []rtVariant
	for _, f := range formats() {
		for _, direct := range []bool{false, true} {
			if direct && f != formats()[0] {
				continue
			}
			name := f
			if direct {
				name += "_direct"
			}
			db := prefix + "_" + name
			cfg := testConfig(db, f)
			cfg.StagingTables = !direct
			vs = append(vs, rtVariant{name: name, e: startExporter(t, cfg), db: db})
		}
	}
	t.Cleanup(func() {
		for _, v := range vs {
			v.e.shutdown(context.Background())
		}
	})
	return vs
}

func queryBytes(tc hegel.TestCase, q string) []byte {
	r, err := reader.Query(q, "RowBinary")
	if err != nil {
		tc.Errorf("%s: %v", q, err)
	}
	if r == nil {
		return nil
	}
	defer r.Free()
	return append([]byte(nil), r.(interface{ Buf() []byte }).Buf()...)
}

// queryTC is query for code running inside a property.
func queryTC(tc hegel.TestCase, q string) string {
	r, err := reader.Query(q, "TSV")
	if err != nil {
		tc.Errorf("%s: %v", q, err)
	}
	defer r.Free()
	return strings.TrimRight(r.(interface{ String() string }).String(), "\n")
}

func canonicalRows(rows [][]any) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = fmt.Sprintf("%#v", normalize(r))
	}
	sort.Strings(out)
	return out
}

func firstRowDiff(want, got []string) string {
	if len(want) != len(got) {
		return fmt.Sprintf("%d rows stored, %d encoded", len(got), len(want))
	}
	for i := range want {
		if want[i] != got[i] {
			return fmt.Sprintf("sorted row %d\n stored  %s\n encoded %s", i, got[i], want[i])
		}
	}
	return ""
}

// roundTrip pushes td and ld through every variant and compares what each
// stored with what the RowBinary encoder wrote.
func roundTrip(tc hegel.TestCase, vs []rtVariant, td ptrace.Traces, ld plog.Logs) {
	ctx := context.Background()
	rbT, rbL := &rowBinary{}, &rowBinary{}
	writeTraces(rbT, td, nil)
	writeLogs(rbL, ld, nil)
	tcols, trows, err := decodeSignal(signalTraces, false, rbT.buf)
	if err != nil {
		tc.Errorf("decode traces: %v", err)
	}
	_, lrows, err := decodeSignal(signalLogs, false, rbL.buf)
	if err != nil {
		tc.Errorf("decode logs: %v", err)
	}
	wantT, wantL := canonicalRows(trows), canonicalRows(lrows)
	// Distinct non-empty trace ids: what the trace-id materialized view
	// should hold after one insert block.
	ids := map[string]bool{}
	for _, r := range trows {
		if id := r[colIndex(tcols, "TraceId")].(string); id != "" {
			ids[id] = true
		}
	}

	for _, v := range vs {
		for _, q := range []string{"otel_traces", "otel_traces_trace_id_ts", "otel_logs"} {
			if err := exec(reader, fmt.Sprintf("TRUNCATE TABLE %s.%s", v.db, q)); err != nil {
				tc.Errorf("truncate: %v", err)
			}
		}
		if err := v.e.pushTraces(ctx, td); err != nil {
			tc.Errorf("%s: push traces: %v", v.name, err)
		}
		if err := v.e.pushLogs(ctx, ld); err != nil {
			tc.Errorf("%s: push logs: %v", v.name, err)
		}
		_, gotT, err := decodeSignal(signalTraces, false, queryBytes(tc, fmt.Sprintf("SELECT * FROM %s.otel_traces", v.db)))
		if err != nil {
			tc.Errorf("%s: decode stored traces: %v", v.name, err)
		}
		if d := firstRowDiff(wantT, canonicalRows(gotT)); d != "" {
			tc.Errorf("%s traces: %s", v.name, d)
		}
		_, gotL, err := decodeSignal(signalLogs, false, queryBytes(tc, fmt.Sprintf("SELECT * FROM %s.otel_logs", v.db)))
		if err != nil {
			tc.Errorf("%s: decode stored logs: %v", v.name, err)
		}
		if d := firstRowDiff(wantL, canonicalRows(gotL)); d != "" {
			tc.Errorf("%s logs: %s", v.name, d)
		}
		if got := queryTC(tc, fmt.Sprintf("SELECT count() FROM %s.otel_traces_trace_id_ts", v.db)); got != fmt.Sprint(len(ids)) {
			tc.Errorf("%s: trace-id table has %s rows, want %d", v.name, got, len(ids))
		}
	}
}

// jsonMinTS is the earliest time chDB's JSONEachRow parser reads back from
// the "seconds.nanos" string jsonEachRow writes: 1e8 s, 1973-03-03. Below
// that it tries to read the digits as a date and rejects the whole insert
// (see TestPBTFindingJSONRejectsEarlyTimestamps).
const jsonMinTS = 100_000_000 * 1e9

// TestPBTFormatsRoundTripThroughChDB: arbitrary traces and logs (every
// attribute type, NULs, control characters, invalid UTF-8, all-zero ids,
// empty maps and arrays, timestamps anywhere from 1973 to DateTime64(9)'s
// end in 2262) are stored exactly as encoded, by every insert format.
func TestPBTFormatsRoundTripThroughChDB(t *testing.T) {
	vs := startRoundTripExporters(t, "pbt_rt")
	hegel.Test(t, func(ht *hegel.T) {
		g := pdataGen{minTS: jsonMinTS, maxTS: math.MaxInt64}
		td := hegel.Draw(ht, g.traces())
		ld := hegel.Draw(ht, g.logs())
		ht.Event(fmt.Sprintf("rows=%d", min(td.SpanCount()+ld.LogRecordCount(), 8)))
		roundTrip(ht, vs, td, ld)
	}, pbtOpts(25, hegel.SuppressHealthCheck(hegel.TooSlow))...)
}

// TestPBTFindingJSONRejectsEarlyTimestamps: with insert_format json (the
// stock chdb-go path), one record whose time is before 1973-03-03, a log
// record with neither Timestamp nor ObservedTimestamp set for instance,
// fails the whole batch: chDB reads "0.000000000" as a malformed date, not
// as seconds since the epoch ("Cannot read DateTime: unexpected number of
// decimal digits after day of month"). The error is permanent, so the queue
// retries the batch until it gives up, and the batch's other rows are lost
// with it. Times past 2262 (2^63 ns) fail the same way ("Decimal math
// overflow"), where RowBinary stores them as a wrapped Int64. rowbinary and
// file are unaffected below 2^63.
func TestPBTFindingJSONRejectsEarlyTimestamps(t *testing.T) {
	vs := startRoundTripExporters(t, "pbt_rt_ts")
	expectFinding(t, "a timestamp that one insert format stores and another rejects", func(tc hegel.TestCase) {
		ld := plog.NewLogs()
		r := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
		r.SetTimestamp(pcommon.Timestamp(hegel.Draw(tc, hegel.Integers[uint64](0, math.MaxInt64))))
		roundTripLogsOnly(tc, vs, ld)
	}, pbtOpts(30, hegel.SuppressHealthCheck(hegel.TooSlow))...)
}

// roundTripLogsOnly is roundTrip for a hegel.TestCase outside hegel.Test.
func roundTripLogsOnly(tc hegel.TestCase, vs []rtVariant, ld plog.Logs) {
	rb := &rowBinary{}
	writeLogs(rb, ld, nil)
	_, rows, err := decodeSignal(signalLogs, false, rb.buf)
	if err != nil {
		tc.Errorf("decode: %v", err)
	}
	want := canonicalRows(rows)
	for _, v := range vs {
		if err := exec(reader, fmt.Sprintf("TRUNCATE TABLE %s.otel_logs", v.db)); err != nil {
			tc.Errorf("truncate: %v", err)
		}
		if err := v.e.pushLogs(context.Background(), ld); err != nil {
			tc.Errorf("%s: push: %v", v.name, err)
			continue
		}
		_, got, err := decodeSignal(signalLogs, false, queryBytes(tc, fmt.Sprintf("SELECT * FROM %s.otel_logs", v.db)))
		if err != nil {
			tc.Errorf("%s: decode stored: %v", v.name, err)
		}
		if d := firstRowDiff(want, canonicalRows(got)); d != "" {
			tc.Errorf("%s: %s", v.name, strings.ReplaceAll(d, "\n", " "))
		}
	}
}
