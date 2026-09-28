package ctrl

import (
	"context"
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
	out := &lane.Writer{}
	c := New(Config{ClusterName: "c1", Static: map[string]string{"cloud.provider": "aws", "deployment.environment.name": "prod"},
		PodLabels: []string{"team", "pod-template-hash"}, Resync: time.Hour, Writer: "w"}, cs, out)
	go func() { _ = c.Run(ctx, 1) }()
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
