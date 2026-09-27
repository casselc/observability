package ctrl

import (
	"strconv"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	appslisters "k8s.io/client-go/listers/apps/v1"
	batchlisters "k8s.io/client-go/listers/batch/v1"
	"k8s.io/client-go/tools/cache"
)

func ctrlRef(kind, name string) []metav1.OwnerReference {
	t := true
	return []metav1.OwnerReference{{Kind: kind, Name: name, Controller: &t}}
}

// The covered attributes must be what the agent (k8sattributes) derives,
// including the two cases where its heuristics disagree with the owner chain
// (deploy/results/k8s-sim.md §3).
func TestWorkloadFollowsAgentRules(t *testing.T) {
	rsIdx := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	jobIdx := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	_ = rsIdx.Add(&appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "api-5f7d8c9b4", OwnerReferences: ctrlRef("Deployment", "api")}})
	_ = rsIdx.Add(&appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "legacy-api-7d9f8c6b5"}}) // bare
	created := time.Date(2026, 9, 27, 10, 0, 30, 0, time.UTC)
	run := created.Unix() / 60
	sched := "report-" + strconv.FormatInt(run, 10)
	_ = jobIdx.Add(&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: sched, OwnerReferences: ctrlRef("CronJob", "report")}})
	_ = jobIdx.Add(&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "report-manual-1", OwnerReferences: ctrlRef("CronJob", "report")}})
	c := &Controller{rs: appslisters.NewReplicaSetLister(rsIdx), jobs: batchlisters.NewJobLister(jobIdx)}

	pod := func(kind, owner, hash string) *corev1.Pod {
		p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: owner + "-x", OwnerReferences: ctrlRef(kind, owner),
			CreationTimestamp: metav1.NewTime(created), Labels: map[string]string{}}}
		if hash != "" {
			p.Labels["pod-template-hash"] = hash
		}
		return p
	}
	for _, tc := range []struct {
		name       string
		p          *corev1.Pod
		kind, wl   string
		attrs      map[string]string // covered workload-level attributes expected
		podJobName string
	}{
		{"deployment", pod("ReplicaSet", "api-5f7d8c9b4", "5f7d8c9b4"), "deployment", "api",
			map[string]string{"k8s.replicaset.name": "api-5f7d8c9b4", "k8s.deployment.name": "api", "service.name": "api"}, ""},
		{"bare replicaset named like a deployment's", pod("ReplicaSet", "legacy-api-7d9f8c6b5", "7d9f8c6b5"), "replicaset", "legacy-api-7d9f8c6b5",
			map[string]string{"k8s.replicaset.name": "legacy-api-7d9f8c6b5", "k8s.deployment.name": "legacy-api", "service.name": "legacy-api"}, ""},
		{"scheduled cronjob run", pod("Job", sched, ""), "cronjob", "report",
			map[string]string{"k8s.cronjob.name": "report", "service.name": "report"}, sched},
		{"job created by hand from a cronjob", pod("Job", "report-manual-1", ""), "cronjob", "report",
			map[string]string{"k8s.job.name": "report-manual-1", "service.name": "report-manual-1"}, ""},
	} {
		kind, name, wl, podAttrs, ok := c.workload(tc.p)
		if !ok || kind != tc.kind || name != tc.wl {
			t.Errorf("%s: kind/name = %q/%q ok=%v, want %q/%q", tc.name, kind, name, ok, tc.kind, tc.wl)
		}
		if len(wl) != len(tc.attrs) {
			t.Errorf("%s: workload attrs %v, want %v", tc.name, wl, tc.attrs)
		}
		for k, v := range tc.attrs {
			if wl[k] != v {
				t.Errorf("%s: %s = %q, want %q", tc.name, k, wl[k], v)
			}
		}
		if podAttrs["k8s.job.name"] != tc.podJobName {
			t.Errorf("%s: pod k8s.job.name = %q, want %q", tc.name, podAttrs["k8s.job.name"], tc.podJobName)
		}
	}
}
