package ctrl

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	"github.com/casselc/observability/otel-chdb/entities/controller/internal/lane"
)

// A relist is a window in which the controller saw no events: it must be
// counted, and so must the deletions only the relist revealed (their close
// time is the relist's, not the deletion's). AMBIGUITY.md, "entity
// controller informer".
func TestRelistsAndUnknownDeletionsAreCounted(t *testing.T) {
	cs := fake.NewClientset()
	c := New(Config{Resync: time.Hour}, cs, &lane.Writer{})
	defer c.q.ShutDown() // its delaying queue's goroutine (goleak)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.inf.Start(ctx.Done())
	c.inf.WaitForCacheSync(ctx.Done())
	if n := c.Lists.Load(); n < informerCount {
		t.Fatalf("initial lists %d, want at least %d", n, informerCount)
	}
	base := c.Lists.Load()
	// What a reflector sends after a 410: a LIST (no watch timeout) ...
	c.relistWait = 0 // the gap bookkeeping has its own test
	c.countList(resPods, &metav1.ListOptions{ResourceVersion: "123"})
	// ... or a streaming watch list; a plain watch is not a relist.
	yes := true
	to := int64(300)
	c.countList(resNodes, &metav1.ListOptions{TimeoutSeconds: &to, SendInitialEvents: &yes})
	c.countList(resNodes, &metav1.ListOptions{TimeoutSeconds: &to})
	if got := c.Lists.Load() - base; got != 2 {
		t.Fatalf("relists counted %d, want 2", got)
	}
	if got := c.Stats()["relists"]; got != c.Lists.Load()-informerCount {
		t.Fatalf("stats relists %d", got)
	}
	// A deletion seen only by the relist.
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "p", UID: "u"}}
	c.podDeleted(cache.DeletedFinalStateUnknown{Key: "ns/p", Obj: p})
	c.podDeleted(p)
	if got := c.Stats()["deleted_unknown"]; got != 1 {
		t.Fatalf("deleted_unknown %d, want 1", got)
	}
}

// A relist writes a gap record to the lane: [the last event that informer
// observed, the relist's completion (its resource version moved and the
// work queue drained)], for that resource, so the aggregator can mark what
// opened or closed in it as uncertain (AMBIGUITY.md X1). Each informer has
// its own window; a first LIST opens none.
func TestRelistWritesAGapRecord(t *testing.T) {
	cs := fake.NewClientset()
	out := &lane.Writer{}
	c := New(Config{Resync: time.Hour, Writer: "w1"}, cs, out)
	c.clusterUID, c.clusterKey = "uid", 42
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.inf.Start(ctx.Done())
	c.inf.WaitForCacheSync(ctx.Done())
	go c.worker(ctx)
	defer c.q.ShutDown()
	for res, w := range c.watch {
		if w.lists.Load() != 1 {
			t.Fatalf("%s: %d initial lists", res, w.lists.Load())
		}
	}
	// a pod event: the pods watch was alive at t1
	before := lane.Now()
	if _, err := cs.CoreV1().Pods("ns").Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "p", UID: "u"}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	for c.watch[resPods].lastEvent.Load() < int64(before) {
		time.Sleep(5 * time.Millisecond)
	}
	var rv atomic.Value
	rv.Store("10")
	c.rvOf = func(string) string { return rv.Load().(string) }
	c.relistPoll = 5 * time.Millisecond
	time.Sleep(20 * time.Millisecond)
	// the pods watch was last alive at t1 (the create's events have settled)
	t1 := lane.Time(c.watch[resPods].lastEvent.Load())
	began := lane.Now()
	c.countList(resPods, &metav1.ListOptions{ResourceVersion: "10"}) // the LIST after a 410
	time.Sleep(50 * time.Millisecond)
	if c.Gaps.Load() != 0 {
		t.Fatal("gap written before the relist completed")
	}
	rv.Store("11") // the LIST is in the store
	deadline := time.Now().Add(5 * time.Second)
	for c.Gaps.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	gaps := func() []lane.Record {
		var g []lane.Record
		for _, r := range out.Buffered() {
			if r.Level == lane.LGap {
				g = append(g, r)
			}
		}
		return g
	}
	g := gaps()
	if len(g) != 1 {
		t.Fatalf("gap records %d, want 1", len(g))
	}
	r := g[0]
	if r.Kind != resPods || r.Name != "relist" || r.ClusterKey != 42 || r.Writer != "w1" || r.Attrs["k8s.resource"] != resPods {
		t.Fatalf("%+v", r)
	}
	if r.ValidFrom != t1 || r.EventAt < began || r.ClosedAt < r.EventAt+50 {
		t.Fatalf("window [%d, %d], relist at %d; want from the last pod event %d, to after the store moved", r.ValidFrom, r.ClosedAt, r.EventAt, t1)
	}
	// Nodes saw no event since their first LIST: the gap starts there. A
	// relist that never completes is written anyway, at relistWait.
	c.relistWait = 30 * time.Millisecond
	c.countList(resNodes, &metav1.ListOptions{})
	for c.Gaps.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	g = gaps()
	if len(g) != 2 || g[1].Kind != resNodes || g[1].Name != "relist_incomplete" || g[1].ValidFrom > before || g[1].ValidFrom == 0 {
		t.Fatalf("%+v", g)
	}
	if c.Stats()["gaps"] != 2 {
		t.Fatal(c.Stats())
	}
}
