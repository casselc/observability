package ctrl

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/casselc/observability/otel-chdb/entities/controller/internal/lane"
	"github.com/casselc/observability/otel-chdb/entities/controller/internal/rid"
)

// Every resource the controller derives must hash to what an edge computes
// from the same attributes (rid.Split then rid.ID): each derived key is in
// the covered set, so nothing is dropped on the edge's side.
func TestDerivedResourcesAreCovered(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	created := metav1.NewTime(time.Now().Add(-time.Minute).Truncate(time.Second))
	cs := fake.NewClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "kube-system", UID: "cluster-uid"}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "shop"}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1", UID: "node-uid", Labels: map[string]string{
			"node.kubernetes.io/instance-type": "m6i.large", "topology.kubernetes.io/zone": "us-east-1a"}},
			Spec: corev1.NodeSpec{ProviderID: "aws:///us-east-1a/i-0abc"}},
		&appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "cart-5f6d", OwnerReferences: ctrlRef("Deployment", "cart")}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "shop", Name: "cart-5f6d-abcde", UID: "pod-uid", CreationTimestamp: created,
			Labels: map[string]string{"pod-template-hash": "5f6d", "team": "payments", "unlisted": "x"}, OwnerReferences: ctrlRef("ReplicaSet", "cart-5f6d")},
			Spec:   corev1.PodSpec{NodeName: "n1", Containers: []corev1.Container{{Name: "cart", Image: "reg.example/cart:1.2"}, {Name: "istio-proxy", Image: "istio/proxyv2"}}},
			Status: corev1.PodStatus{Phase: corev1.PodRunning}},
	)
	out, _ := fakeLane(t)
	c := New(Config{ClusterName: "c1", Static: map[string]string{"cloud.provider": "aws", "deployment.environment.name": "prod"},
		PodLabels: []string{"team", "pod-template-hash"}, Resync: time.Hour, Writer: "w"}, cs, out)
	done := make(chan struct{})
	go func() { defer close(done); _ = c.Run(ctx, 1) }()
	// Run must be over before the test is: it outlived it, and its initial
	// sync PUT through the writer's then nil S3 client panicked the package's
	// test binary after PASS (CI run 88, 2026-09-29).
	defer func() { cancel(); <-done }()
	deadline := time.Now().Add(10 * time.Second)
	var res []lane.Record
	for len(res) < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		res = res[:0]
		for _, r := range out.Buffered() {
			if r.Level == lane.LResource {
				res = append(res, r)
			}
		}
	}
	if len(res) < 2 {
		t.Fatalf("resources %d", len(res))
	}
	for _, r := range res {
		if bad := rid.Covered(r.Attrs); len(bad) > 0 {
			t.Errorf("%s: keys outside the covered set %v", r.Container, bad)
		}
		kvs := []rid.KV{{Key: "telemetry.sdk.name", Value: "go", Str: true}, {Key: "process.pid", Value: "7"}}
		for k, v := range r.Attrs {
			kvs = append(kvs, rid.KV{Key: k, Value: v, Str: true})
		}
		if got := rid.ID(rid.Split(kvs)); got != r.Key {
			t.Errorf("%s: edge id %d, controller %d", r.Container, got, r.Key)
		}
		if r.Attrs["k8s.pod.label.team"] != "payments" || r.Attrs["k8s.deployment.name"] != "cart" || r.Attrs["host.id"] != "i-0abc" {
			t.Errorf("%v", r.Attrs)
		}
	}
}

// fakeLane is a lane writer whose S3 is an httptest server that accepts
// every request; puts counts the PUTs that reached it.
func fakeLane(t *testing.T) (*lane.Writer, *atomic.Int64) {
	t.Helper()
	var puts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if r.Method == http.MethodPut {
			puts.Add(1)
		}
		rw.Header().Set("ETag", `"x"`)
		rw.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	t.Setenv("AWS_ACCESS_KEY_ID", "k")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "s")
	s3c, err := lane.NewS3(context.Background(), srv.URL, "us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	return &lane.Writer{S3: s3c, Bucket: "b", Lane: "lanes/c1/1-i", PutTimeout: 5 * time.Second}, &puts
}

// A controller stopped before its work queue drained returns without the
// initial sync: that sync would leave out (and so close) every version the
// workers had not processed yet. Regression: Run left its quiet-queue wait
// on cancellation and synced anyway.
func TestRunStoppedBeforeTheFirstSyncWritesNone(t *testing.T) {
	cs := fake.NewClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "kube-system", UID: "cluster-uid"}})
	out, puts := fakeLane(t)
	c := New(Config{ClusterName: "c1", Resync: time.Hour, Writer: "w"}, cs, out)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, 1) }()
	// Stop it inside the wait for a quiet queue (>= 900 ms): after every
	// handler has synced, so Run is past WaitForCacheSync.
	synced := func() bool {
		if len(c.regs) != informerCount {
			return false
		}
		for _, r := range c.regs {
			if !r.HasSynced() {
				return false
			}
		}
		return true
	}
	for deadline := time.Now().Add(10 * time.Second); !synced(); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("handlers never synced")
		}
	}
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil && ctx.Err() == nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	if c.Synced.Load() || puts.Load() != 0 {
		t.Fatalf("synced %v, %d PUTs: a stopped controller wrote its initial sync", c.Synced.Load(), puts.Load())
	}
}
