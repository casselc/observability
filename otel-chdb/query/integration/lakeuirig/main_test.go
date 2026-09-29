package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/casselc/observability/otel-chdb/testgate/tracetag"
)

// CAST row 57: the rig, signalled while setup hung on a store, exited and
// left the edges it had started running. Every edge it starts is recorded
// and cleanup kills them all, whatever setup had reached. Here the edge is a
// stand-in that never exits by itself (as an edge blocked on a hung store),
// and nothing stops it but cleanup's kill.
func TestCleanupKillsTheEdgesSetupStarted(t *testing.T) {
	tracetag.Covers(t, "FI", "CAST-57")
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "otelcol-s3pq"), []byte("#!/bin/sh\nexec sleep 300\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	r := &rig{bin: bin, work: t.TempDir(), s3url: "http://127.0.0.1:9", bucket: "b", run: "edges"}
	started := []*edge{r.startEdge("lui-a"), r.startEdge("lui-b")}
	time.Sleep(100 * time.Millisecond) // both running: setup is "blocked" past them
	r.killEdges()
	for _, e := range started {
		done := make(chan error, 1)
		go func() { done <- e.cmd.Wait() }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = e.cmd.Process.Kill()
			t.Fatalf("edge %s (pid %d) still running after cleanup's kill", e.cluster, e.cmd.Process.Pid)
		}
	}
}
