package ctrl

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
)

// A relist is a window in which the controller saw no events: it must be
// counted, and so must the deletions only the relist revealed (their close
// time is the relist's, not the deletion's). AMBIGUITY.md, "entity
// controller informer".
func TestRelistsAndUnknownDeletionsAreCounted(t *testing.T) {
	cs := fake.NewClientset()
	c := New(Config{Resync: time.Hour}, cs, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.inf.Start(ctx.Done())
	c.inf.WaitForCacheSync(ctx.Done())
	if n := c.Lists.Load(); n < informerCount {
		t.Fatalf("initial lists %d, want at least %d", n, informerCount)
	}
	base := c.Lists.Load()
	// What a reflector sends after a 410: a LIST (no watch timeout) ...
	c.countList(&metav1.ListOptions{ResourceVersion: "123"})
	// ... or a streaming watch list; a plain watch is not a relist.
	yes := true
	to := int64(300)
	c.countList(&metav1.ListOptions{TimeoutSeconds: &to, SendInitialEvents: &yes})
	c.countList(&metav1.ListOptions{TimeoutSeconds: &to})
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
