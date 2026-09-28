package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// The load mode paces, classifies a 503 SlowDown as such, and names its keys
// by the format-v2 lane layout.
func TestLoadPacesAndClassifies(t *testing.T) {
	fake := &fakeS3{mode: "honest", objs: map[string][]byte{}}
	var puts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && puts.Add(1)%4 == 0 {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(503)
			w.Write([]byte("<Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message></Error>"))
			return
		}
		fake.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	t.Setenv("AWS_CONFIG_FILE", "/dev/null")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")
	o := &Opts{Endpoint: srv.URL, Bucket: "b", AccessKey: "k", SecretKey: "s", Store: "fake"}
	if err := o.resolve(); err != nil {
		t.Fatal(err)
	}
	e, err := newEnv(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	e.Run, e.Root = "run", "p/run"
	out := filepath.Join(t.TempDir(), "load.json")
	p := LoadParams{Rate: 200, Duration: 2 * time.Second, Workers: 16, Clusters: 2, Producers: 3, Signals: 7,
		Layout: "cluster-first", Size: 100, Every: time.Second}
	if rc := runLoad(context.Background(), e, p, out, true); rc != 0 {
		t.Fatalf("rc %d", rc)
	}
	var rep LoadReport
	b, _ := os.ReadFile(out)
	if err := json.Unmarshal(b, &rep); err != nil {
		t.Fatal(err)
	}
	sent := rep.Totals.OK + rep.Totals.SlowDown
	t.Logf("summary %v", rep.Summary)
	if sent < 300 || sent > 440 {
		t.Errorf("sent %d PUTs at 200/s over 2 s (ramp 0)", sent)
	}
	if rep.Totals.SlowDown == 0 || rep.Totals.Other5xx != 0 {
		t.Errorf("503 SlowDown %d, other 5xx %d: want every 4th PUT as SlowDown", rep.Totals.SlowDown, rep.Totals.Other5xx)
	}
	if rep.Lanes != 42 || len(rep.Seconds) != 2 {
		t.Errorf("lanes %d seconds %d", rep.Lanes, len(rep.Seconds))
	}
	n := 0
	fake.mu.Lock()
	for k := range fake.objs {
		if !strings.HasPrefix(k, "/b/p/run/load/cluster-first/c0") || !strings.HasSuffix(k, ".parquet") || strings.Count(k, "/") != 10 {
			t.Errorf("key %s is not {base}/cNN/edge-P/signal/epoch/seq.parquet", k)
		}
		n++
	}
	fake.mu.Unlock()
	if int64(n) != rep.Totals.OK {
		t.Errorf("%d objects stored, %d acknowledged", n, rep.Totals.OK)
	}
}
