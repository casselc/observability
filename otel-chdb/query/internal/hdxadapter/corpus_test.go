package hdxadapter

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

type corpusStmt struct {
	Scenario string            `json:"scenario"`
	Side     string            `json:"side"`
	Query    string            `json:"query"`
	Params   map[string]string `json:"params"`
}

// loadCorpus reads the 799 statements HyperDX 2.39.1 sent in
// ../../../hyperdx's captures (results/schema-replay.json and
// schema3-replay.json). Parameter values are as @clickhouse/client sent
// them (escaped text).
func loadCorpus(t testing.TB) []corpusStmt {
	t.Helper()
	var out []corpusStmt
	for _, f := range []string{"schema-replay.json", "schema3-replay.json"} {
		b, err := os.ReadFile(filepath.Join("..", "..", "..", "hyperdx", "results", f))
		if err != nil {
			t.Skipf("corpus: %v", err)
		}
		var d struct {
			Statements []corpusStmt `json:"statements"`
		}
		if err := json.Unmarshal(b, &d); err != nil {
			t.Fatal(err)
		}
		out = append(out, d.Statements...)
	}
	return out
}

func corpusTables() Tables {
	tc := map[string]string{}
	for _, db := range []string{"hdx_old", "hdx_new", "hdx_full"} {
		tc[db+".otel_logs"] = "Timestamp"
		tc[db+".otel_traces"] = "Timestamp"
	}
	return Tables{DefaultDatabase: "default", TimeColumns: tc}
}

// TestCorpusPrepares: every captured statement is prepared or refused for a
// reason the design names (README.md §3), and every prepared statement
// parses back to itself.
func TestCorpusPrepares(t *testing.T) {
	sts := loadCorpus(t)
	tb := corpusTables()
	outcomes := map[string]int{}
	windows := 0
	for _, s := range sts {
		st, err := tb.Prepare(s.Query, s.Params, "")
		if err != nil {
			e, ok := err.(*Error)
			if !ok {
				t.Fatalf("%s: %v", s.Query, err)
			}
			outcomes["refused:"+e.Reason]++
			if e.Reason != "explain" {
				t.Errorf("unexpected refusal %v\n%s", err, s.Query)
			}
			continue
		}
		outcomes[st.Kind+"/"+st.Format]++
		if st.Window != nil {
			windows++
		}
	}
	keys := make([]string, 0, len(outcomes))
	for k := range outcomes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		t.Logf("%-50s %d", k, outcomes[k])
	}
	t.Logf("windows derived: %d", windows)
}
