package hdxadapter

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/casselc/observability/otel-chdb/query/internal/central"
)

// shippedPerformance is the performance-settings allow-list
// queryd.example.json ships (D33): the replay runs with it, so HyperDX's
// own performance settings reach ClickHouse through the service.
func shippedPerformance(t *testing.T, root string) central.SettingsPolicy {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(root, "query", "queryd.example.json"))
	if err != nil {
		t.Fatal(err)
	}
	var c struct {
		Central struct {
			PerformanceSettings central.SettingsPolicy `json:"performance_settings"`
		} `json:"central"`
	}
	if err := json.Unmarshal(b, &c); err != nil {
		t.Fatal(err)
	}
	if err := c.Central.PerformanceSettings.Validate(); err != nil {
		t.Fatal(err)
	}
	return c.Central.PerformanceSettings
}

// totalRowsAsFlag is what a cluster-restricted caller reads for
// system.tables' total_rows (D33): 1 for a table with rows, 0 for an empty
// one, null for null.
func totalRowsAsFlag(c canon) canon {
	idx := -1
	for i, col := range c.cols {
		if strings.HasPrefix(col, "total_rows ") {
			idx = i
		}
	}
	if idx < 0 {
		return c
	}
	out := canon{cols: c.cols}
	for _, r := range c.rows {
		var vals []json.RawMessage
		if err := json.Unmarshal([]byte(r), &vals); err != nil || idx >= len(vals) {
			out.rows = append(out.rows, r)
			continue
		}
		switch v := strings.Trim(string(vals[idx]), `"`); v {
		case "null", "0":
		default:
			vals[idx] = json.RawMessage(`"1"`)
		}
		b, _ := json.Marshal(vals)
		out.rows = append(out.rows, string(b))
	}
	return out
}

// sampleThroughAdapter: HyperDX's flagged read sample (read_overflow_mode
// break at max_rows_to_read, fork patch 0001) through the adapter and the
// service: ClickHouse stops at the bound, the answer is labelled
// completeness "sample" with what it read, for any caller; without the
// flag the same bound is dropped (the service's limits apply) and the
// label is the watermark's.
func sampleThroughAdapter(t *testing.T, adapterURL string, tokens ...string) {
	sql := "SELECT DISTINCT ServiceName FROM hdx_it_new.otel_logs ORDER BY ServiceName FORMAT JSON"
	for _, tok := range tokens {
		for _, sampled := range []bool{true, false} {
			q := url.Values{"max_rows_to_read": {"1000"}}
			if sampled {
				q.Set("read_overflow_mode", "break")
			}
			req, _ := http.NewRequest(http.MethodPost, adapterURL+"/?"+q.Encode(), strings.NewReader(sql))
			req.Header.Set("Authorization", "Bearer "+tok)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 200 {
				t.Errorf("sample %v: %d %s", sampled, resp.StatusCode, firstN(string(b), 300))
				continue
			}
			c, sh := resp.Header.Get("X-Otel-Completeness"), resp.Header.Get("X-Otel-Sample")
			var r struct{ Rows int }
			_ = json.Unmarshal(b, &r)
			t.Logf("sample %v: completeness %s, X-Otel-Sample %q, %d rows", sampled, c, sh, r.Rows)
			if sampled && (c != "sample" || !strings.Contains(sh, "max_rows_to_read=1000; reached_bound=true") || r.Rows == 0) {
				t.Errorf("sample: completeness %q, X-Otel-Sample %q, %d rows", c, sh, r.Rows)
			}
			if !sampled && (c == "sample" || sh != "" || !strings.Contains(resp.Header.Get("X-Otel-Dropped-Settings"), "max_rows_to_read")) {
				t.Errorf("no sample asked: completeness %q, X-Otel-Sample %q, dropped %q", c, sh, resp.Header.Get("X-Otel-Dropped-Settings"))
			}
		}
	}
}

// sameUpToArrayOrder: the same columns and rows once every array of scalars
// is sorted (the order of groupUniqArray's elements is not defined: with
// several threads it varies between runs of the same statement).
func sameUpToArrayOrder(a, b canon) bool {
	if strings.Join(a.cols, "|") != strings.Join(b.cols, "|") || len(a.rows) != len(b.rows) {
		return false
	}
	norm := func(rows []string) string {
		out := make([]string, 0, len(rows))
		for _, r := range rows {
			var v any
			d := json.NewDecoder(strings.NewReader(r))
			d.UseNumber()
			if err := d.Decode(&v); err != nil {
				out = append(out, r)
				continue
			}
			// the row itself (an array of column values) keeps its order:
			// only the values' own arrays are sorted
			switch row := v.(type) {
			case []any:
				for i, e := range row {
					row[i] = sortScalarArrays(e)
				}
			case map[string]any:
				for k, e := range row {
					row[k] = sortScalarArrays(e)
				}
			}
			b, _ := json.Marshal(v)
			out = append(out, string(b))
		}
		sort.Strings(out)
		return strings.Join(out, "\n")
	}
	return norm(a.rows) == norm(b.rows)
}

func sortScalarArrays(v any) any {
	switch x := v.(type) {
	case []any:
		scalars := true
		for i, e := range x {
			x[i] = sortScalarArrays(e)
			switch x[i].(type) {
			case []any, map[string]any:
				scalars = false
			}
		}
		if scalars && len(x) > 0 {
			sort.Slice(x, func(i, j int) bool { return fmt.Sprint(x[i]) < fmt.Sprint(x[j]) })
		}
		return x
	case map[string]any:
		for k, e := range x {
			x[k] = sortScalarArrays(e)
		}
		return x
	}
	return v
}
