package main

import (
	"bytes"
	"encoding/json"
	"github.com/casselc/observability/otel-chdb/testgate/tracetag"
	"testing"
)

// FuzzClusterFilter: whatever a lane object holds (a hostile or broken
// writer's bytes: SEC-1, R-S7), the filter never panics, keeps only whole
// input lines, in order, and every kept record carries the lane's bound
// cluster key (and, for a cluster record, the prefix's cluster name).
func FuzzClusterFilter(f *testing.F) {
	tracetag.Covers(f, "FZ", "SEC-1", "R-S7", "UCA-8")
	f.Add([]byte(`{"level":"cluster","name":"c1","cluster_key":7}`+"\n"+`{"level":"pod","name":"p","cluster_key":7}`+"\n"), "c1", uint64(0))
	f.Add([]byte(`{"level":"pod","cluster_key":8}`+"\n"+`not json`+"\n"), "c1", uint64(7))
	f.Add([]byte(`{"level":"cluster","name":"c2","cluster_key":7}`), "c1", uint64(7))
	f.Fuzz(func(t *testing.T, in []byte, cluster string, bound uint64) {
		b := bound
		kept, rejected := clusterFilter(in, cluster, &b)
		lines := 0
		for _, l := range bytes.SplitAfter(in, []byte{'\n'}) {
			if len(bytes.TrimSpace(l)) > 0 {
				lines++
			}
		}
		var out [][]byte
		for _, l := range bytes.SplitAfter(kept, []byte{'\n'}) {
			if len(l) > 0 {
				out = append(out, l)
			}
		}
		if len(out)+rejected != lines {
			t.Fatalf("%d kept + %d rejected != %d lines", len(out), rejected, lines)
		}
		rest := in
		for _, l := range out {
			i := bytes.Index(rest, l)
			if i < 0 {
				t.Fatalf("kept line %q is not an input line (in order)", l)
			}
			rest = rest[i+len(l):]
			var r struct {
				Level      string `json:"level"`
				Name       string `json:"name"`
				ClusterKey uint64 `json:"cluster_key"`
			}
			if json.Unmarshal(l, &r) != nil || b == 0 || r.ClusterKey != b || (r.Level == "cluster" && r.Name != cluster) {
				t.Fatalf("kept %q (bound %d, cluster %q)", l, b, cluster)
			}
		}
	})
}

// FuzzGapTime: the gap-record time parser never panics, and what it
// accepts renders to a fixed point.
func FuzzGapTime(f *testing.F) {
	f.Add("2026-09-29 10:11:12.345")
	f.Add("2026-13-01 00:00:00.000")
	f.Fuzz(func(t *testing.T, s string) {
		out, err := gapTime(s)
		if err != nil {
			return
		}
		if again, err := gapTime(out); err != nil || again != out {
			t.Fatalf("gapTime(%q) = %q, then %q, %v", s, out, again, err)
		}
	})
}
