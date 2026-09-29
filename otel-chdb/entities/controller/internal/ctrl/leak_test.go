package ctrl

import (
	"context"
	"testing"
	"time"

	"go.uber.org/goleak"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/casselc/observability/otel-chdb/entities/controller/internal/lane"
	"github.com/casselc/observability/otel-chdb/testgate/tracetag"
)

// TestMain fails the package's tests if a goroutine outlives them: a
// worker, ticker, watcher or request that a stop or a cancel did not end
// (research/go-verification.md §5).
func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// CAST row 66: a Run that fails (here kube-system is unreadable: the fake
// cluster has no such namespace) stops the work queue that New started, and
// with it the delaying queue's goroutine. Before 3f861c9 only Run's normal
// exits shut it down; goleak in TestMain found the leak at the end of the
// package, this names the case.
func TestFailedRunStopsTheWorkQueue(t *testing.T) {
	tracetag.Covers(t, "FI", "CAST-66")
	before := goleak.IgnoreCurrent()
	c := New(Config{Resync: time.Hour, Writer: "w1"}, fake.NewClientset(), &lane.Writer{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Run(ctx, 2); err == nil {
		t.Fatal("Run without a kube-system namespace succeeded")
	}
	if !c.q.ShuttingDown() {
		t.Error("the work queue is still running after Run failed")
		c.q.ShutDown()
	}
	goleak.VerifyNone(t, before)
}
