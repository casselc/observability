package ctrl

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/casselc/observability/otel-chdb/entities/controller/internal/lane"
)

// A controller that follows an earlier incarnation (Config.Since, from
// lane.PreviousEnd) dates a pod it sees for the first time, and that was
// created before Since, from Since and not from the pod's creation.
// Regression (entities/bitemp's fleet replay, 2026-09-28): the aggregator
// merges a version's valid_from as the min over records, so after every
// restart a pod relabelled earlier got its current version dated from the
// pod's creation, overlapping its earlier versions in `pods` and
// `resources` (2,063 pod-hours of two versions at once in a 7-day,
// one-cluster replay with three restarts, through the aggregator's SQL; 2.4
// with this fix). A pod created after Since keeps its creation time, and a
// first-ever start (Since = 0) is unchanged. The window [Since, first sync]
// is written as a restart gap record.
func TestRestartDatesFromThePreviousIncarnation(t *testing.T) {
	now := time.Now()
	since := lane.FromTime(now.Add(-10 * time.Minute))
	oldCreated, newCreated := now.Add(-time.Hour).Truncate(time.Second), now.Add(-time.Minute).Truncate(time.Second)
	pod := func(name, uid string, created time.Time) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name, UID: types.UID(uid), CreationTimestamp: metav1.NewTime(created)},
			Spec: corev1.PodSpec{NodeName: "n1", Containers: []corev1.Container{{Name: "c", Image: "img:1"}}}}
	}
	for _, c := range []struct {
		name     string
		since    lane.Time
		wantOld  lane.Time
		wantGaps int
	}{
		{"after a previous incarnation", since, since, 1},
		{"first start", 0, lane.FromTime(oldCreated), 0},
	} {
		cs := fake.NewClientset(
			&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1", UID: "nu"}},
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns", UID: "nsu"}},
			pod("old", "u-old", oldCreated), pod("new", "u-new", newCreated))
		out := &lane.Writer{}
		ct := New(Config{Resync: time.Hour, Writer: "w2", Since: c.since}, cs, out)
		ct.clusterUID, ct.clusterKey = "uid", 42
		defer ct.q.ShutDown() // its delaying queue's goroutine (goleak)
		ctx, cancel := context.WithCancel(context.Background())
		ct.inf.Start(ctx.Done())
		ct.inf.WaitForCacheSync(ctx.Done())
		for deadline := time.Now().Add(5 * time.Second); ; {
			ct.mu.Lock()
			_, okN := ct.nodes["n1"]
			_, okS := ct.nss["ns"]
			ct.mu.Unlock()
			if okN && okS {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("node and namespace not cached")
			}
			time.Sleep(5 * time.Millisecond)
		}
		for _, k := range []string{"ns/old", "ns/new"} {
			if err := ct.process(k); err != nil {
				t.Fatalf("%s: process %s: %v", c.name, k, err)
			}
		}
		end := lane.Now()
		ct.restartGap(end)
		cancel()
		vf := map[string]lane.Time{}
		gaps := 0
		for _, r := range out.Buffered() {
			switch r.Level {
			case lane.LPod, lane.LResource:
				if old, ok := vf[r.PodUID+r.Level]; ok && old != r.ValidFrom {
					t.Fatalf("%s: pod and resource disagree", c.name)
				}
				vf[r.PodUID+r.Level] = r.ValidFrom
			case lane.LGap:
				gaps++
				if r.Kind != resRestart || r.Name != "restart" || r.ValidFrom != c.since || r.ClosedAt != end || r.Writer != "w2" {
					t.Fatalf("%s: gap %+v", c.name, r)
				}
			}
		}
		for _, lvl := range []string{lane.LPod, lane.LResource} {
			if got := vf["u-old"+lvl]; got != c.wantOld {
				t.Errorf("%s: %s of the pod created before Since: valid_from %d, want %d", c.name, lvl, got, c.wantOld)
			}
			if got := vf["u-new"+lvl]; got != lane.FromTime(newCreated) {
				t.Errorf("%s: %s of the pod created after Since: valid_from %d, want its creation %d", c.name, lvl, got, lane.FromTime(newCreated))
			}
		}
		if gaps != c.wantGaps {
			t.Errorf("%s: %d restart gap records, want %d", c.name, gaps, c.wantGaps)
		}
	}
}
