package ctrl

// awaitRelist's deadline and quiet-period logic and Run's resync ticker in
// fake time (testing/synctest), at production settings: relistPoll 100 ms,
// relistWait 5 min, resync 10 min. The real-time tests beside these shrink
// the durations to milliseconds and poll, which is what made
// TestRelistWritesAGapRecord flaky under -race (research/go-verification.md
// §5).

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/casselc/observability/otel-chdb/entities/controller/internal/lane"
)

func gapRecords(w *lane.Writer) []lane.Record {
	var g []lane.Record
	for _, r := range w.Buffered() {
		if r.Level == lane.LGap {
			g = append(g, r)
		}
	}
	return g
}

func TestAwaitRelistInFakeTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		out := &lane.Writer{}
		c := New(Config{Resync: time.Hour, Writer: "w1"}, fake.NewClientset(), out)
		defer c.q.ShutDown()
		if c.relistPoll != 100*time.Millisecond || c.relistWait != 5*time.Minute {
			t.Fatalf("defaults changed: poll %v, wait %v", c.relistPoll, c.relistWait)
		}
		var rv atomic.Value
		rv.Store("10")
		c.rvOf = func(string) string { return rv.Load().(string) }
		// each informer's first LIST has happened; the pods watch saw an event
		for _, w := range c.watch {
			w.lists.Store(1)
		}
		c.seen(resPods)
		t1 := lane.Now()

		// a relist 30 s later; its LIST reaches the store 2 min after that
		time.Sleep(30 * time.Second)
		began := lane.Now()
		c.countList(resPods, &metav1.ListOptions{ResourceVersion: "10"})
		time.Sleep(2 * time.Minute)
		synctest.Wait()
		if c.Gaps.Load() != 0 {
			t.Fatal("gap written before the relist completed")
		}
		rv.Store("11")
		moved := lane.Now()
		// the queue is empty: three quiet polls later, the gap is written
		time.Sleep(time.Second)
		synctest.Wait()
		g := gapRecords(out)
		if len(g) != 1 {
			t.Fatalf("%d gap records", len(g))
		}
		r := g[0]
		if r.Name != "relist" || r.ValidFrom != t1 || r.EventAt != began {
			t.Fatalf("%+v; want from the last event %d, relist at %d", r, t1, began)
		}
		// completion = the first poll that sees the new version, then three
		// quiet polls: 300–400 ms after the store moved
		if d := r.ClosedAt - moved; d < 300 || d > 400 {
			t.Fatalf("gap closed %d ms after the store moved", d)
		}

		// a relist that never completes is written at relistWait, exactly
		began = lane.Now()
		c.countList(resNodes, &metav1.ListOptions{})
		time.Sleep(5*time.Minute + time.Second)
		synctest.Wait()
		g = gapRecords(out)
		if len(g) != 2 || g[1].Name != "relist_incomplete" || g[1].Kind != resNodes {
			t.Fatalf("%+v", g)
		}
		if d := g[1].ClosedAt - began; d < 5*60_000 || d > 5*60_000+100 {
			t.Fatalf("incomplete relist written %d ms after it began; want relistWait (300000)", d)
		}

		// the store moves but the work queue never drains: written at the
		// deadline, not before
		rv.Store("12")
		c.q.Add("ns/busy") // no worker takes it
		began = lane.Now()
		c.countList(resNamespaces, &metav1.ListOptions{ResourceVersion: "12"})
		rv.Store("13")
		time.Sleep(4 * time.Minute)
		synctest.Wait()
		if len(gapRecords(out)) != 2 {
			t.Fatal("gap written while the queue was busy")
		}
		time.Sleep(time.Minute + time.Second)
		synctest.Wait()
		g = gapRecords(out)
		if len(g) != 3 || g[2].Kind != resNamespaces {
			t.Fatalf("%+v", g)
		}
		if d := g[2].ClosedAt - began; d < 5*60_000 || d > 5*60_000+100 {
			t.Fatalf("busy-queue relist written %d ms after it began", d)
		}
	})
}

// Run in a bubble: informers over the fake clientset, the initial sync after
// a quiet queue, then one sync per Resync, and a clean stop.
func TestResyncTickerInFakeTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		var syncs []time.Time
		hc := httptest.NewTestServer(t, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			if r.Method == http.MethodPut && strings.Contains(r.URL.Path, ".sync.") {
				mu.Lock()
				syncs = append(syncs, time.Now())
				mu.Unlock()
			}
			rw.Header().Set("ETag", `"x"`)
			rw.WriteHeader(http.StatusOK)
		})).Client()
		s3c := s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String("http://s3.test"), UsePathStyle: true,
			Credentials: credentials.NewStaticCredentialsProvider("k", "s", ""), HTTPClient: hc})
		out := &lane.Writer{S3: s3c, Bucket: "b", Lane: "lanes/c1/1-i", PutTimeout: 5 * time.Second}
		cs := fake.NewClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "kube-system", UID: "cluster-uid"}})
		c := New(Config{ClusterName: "c1", Resync: 10 * time.Minute, Writer: "w"}, cs, out)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		start := time.Now()
		go func() { done <- c.Run(ctx, 2) }()
		time.Sleep(35 * time.Minute)
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		mu.Lock()
		defer mu.Unlock()
		// the initial sync (after ≥ 3 quiet 300 ms polls), then at +10, +20, +30 min
		if len(syncs) != 4 {
			t.Fatalf("%d syncs in 35 minutes: %v", len(syncs), syncs)
		}
		first := syncs[0].Sub(start)
		if first < 900*time.Millisecond || first > 5*time.Second {
			t.Fatalf("initial sync %v after start", first)
		}
		for i := 1; i < len(syncs); i++ {
			if d := syncs[i].Sub(syncs[i-1]); d != 10*time.Minute {
				t.Fatalf("sync %d came %v after the previous one", i, d)
			}
		}
	})
}
