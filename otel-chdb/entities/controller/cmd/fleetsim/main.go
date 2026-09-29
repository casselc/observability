// fleetsim: builds one cluster of the entities spike's fleet shape
// (../../scripts/fleet.py: 200 nodes, 4 daemonsets, 520 deployments in 37
// team namespaces, 40 statefulsets, 30 cronjobs, ~3,000 live pods) in a
// KWOK cluster, then drives its churn in (optionally accelerated) time:
// rollouts, HPA-style daily scaling, evictions, mid-life relabels, node
// replacement. CronJobs run on the real kube-controller-manager.
//
//	fleetsim --kubeconfig k.yaml --index 0 --setup --churn 2h --speed 12
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	teams = []string{"payments", "checkout", "catalog", "search", "identity", "ads", "risk", "ledger", "growth", "mobile-api",
		"notifications", "pricing", "inventory", "shipping", "reviews", "recs", "media", "billing", "support",
		"analytics", "platform", "data-eng", "ml-serving", "partner-api", "fraud"}
	words      = []string{"api", "worker", "gateway", "sync", "indexer", "scheduler", "cache", "router", "consumer", "exporter", "reconciler", "frontend", "backend", "admin", "stream", "batch", "webhook", "auth", "proxy", "store"}
	components = []string{"api", "worker", "frontend", "backend", "cache", "database", "queue"}
	instTypes  = []string{"m6i.4xlarge", "m6i.8xlarge", "c6i.8xlarge", "r6i.4xlarge", "m7g.4xlarge"}
	regions    = [][2]string{{"us-east-1", "use1"}, {"us-west-2", "usw2"}, {"eu-west-1", "euw1"}, {"ap-southeast-1", "apse1"}}
	cronEvery  = []int{5, 5, 15, 15, 15, 15, 15, 15, 15, 15, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 360, 360, 360, 360, 360, 1440, 1440, 1440}
	replicaSet = []int{1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 5, 6, 8, 12}
)

const day = 24 * time.Hour

func h(parts ...any) uint64 {
	f := fnv.New64a()
	fmt.Fprint(f, parts...)
	return f.Sum64()
}

func nsList() []string {
	out := []string{"kube-system", "observability", "istio-system"}
	for i := 0; i < 37; i++ {
		out = append(out, fmt.Sprintf("%s-%s", teams[i%len(teams)], []string{"core", "svc", "jobs"}[i/len(teams)]))
	}
	return out
}

type dep struct {
	ns, name, team string
	replicas       int32
	hpa, sidecar   bool
	nextRoll       time.Time // sim time
	ver            int
	scaled         bool
}

type sim struct {
	cs      kubernetes.Interface
	idx     int
	rnd     *rand.Rand
	nodes   int
	deps    []*dep
	sts     []*dep
	ds      []*dep
	speed   float64
	start   time.Time // real
	simT0   time.Time
	nodeGen map[int]int
	nodeDue map[int]time.Time
	region  [2]string
	stats   map[string]int
	mu      sync.Mutex
}

func (s *sim) now() time.Time {
	return s.simT0.Add(time.Duration(float64(time.Since(s.start)) * s.speed))
}

func (s *sim) nodeName(slot, gen int) string {
	return fmt.Sprintf("ip-10-%d-%d-%d.%s.compute.internal", s.idx*8+slot/32, (slot%32)*8+gen%8, (gen*37+slot)%250+2, s.region[0])
}

func (s *sim) mkNode(ctx context.Context, slot, gen int) error {
	name := s.nodeName(slot, gen)
	zone := s.region[0] + string("abc"[slot%3])
	iid := fmt.Sprintf("i-0%016x", h("iid", s.idx, slot, gen))
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name,
			Labels: map[string]string{"type": "kwok", "kubernetes.io/hostname": name, "kubernetes.io/os": "linux", "kubernetes.io/arch": "amd64",
				"node.kubernetes.io/instance-type": instTypes[slot%len(instTypes)], "topology.kubernetes.io/zone": zone},
			Annotations: map[string]string{"kwok.x-k8s.io/node": "fake", "sim.k8s/ec2-instance-id": iid, "node.alpha.kubernetes.io/ttl": "0"}},
		Spec: corev1.NodeSpec{ProviderID: fmt.Sprintf("aws:///%s/%s", zone, iid)},
		Status: corev1.NodeStatus{
			Capacity:    corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("32"), corev1.ResourceMemory: resource.MustParse("256Gi"), corev1.ResourcePods: resource.MustParse("110")},
			Allocatable: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("32"), corev1.ResourceMemory: resource.MustParse("256Gi"), corev1.ResourcePods: resource.MustParse("110")},
		},
	}
	_, err := s.cs.CoreV1().Nodes().Create(ctx, n, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func labels(name, ns, team, ver string) map[string]string {
	return map[string]string{"app.kubernetes.io/name": name, "app.kubernetes.io/instance": name + "-" + ns, "app.kubernetes.io/version": ver,
		"app.kubernetes.io/component": components[h(name)%uint64(len(components))], "app.kubernetes.io/part-of": team,
		"app.kubernetes.io/managed-by": "Helm", "team": team}
}

func selector(name string) *metav1.LabelSelector {
	return &metav1.LabelSelector{MatchLabels: map[string]string{"app.kubernetes.io/name": name}}
}

var tolerateAll = []corev1.Toleration{{Operator: corev1.TolerationOpExists}}

func podSpec(containers []corev1.Container) corev1.PodSpec {
	return corev1.PodSpec{Containers: containers, Tolerations: tolerateAll, NodeSelector: map[string]string{"type": "kwok"},
		TerminationGracePeriodSeconds: new(int64)}
}

func (s *sim) version(d *dep) string {
	return fmt.Sprintf("%d.%d.%d", 1+d.ver/20, d.ver%20, h(d.name, d.ver)%10)
}

func (s *sim) depContainers(d *dep) []corev1.Container {
	cs := []corev1.Container{{Name: d.name, Image: fmt.Sprintf("123456789012.dkr.ecr.us-east-1.amazonaws.com/%s/%s:%s", d.team, d.name, s.version(d))}}
	if d.sidecar {
		cs = append(cs, corev1.Container{Name: "istio-proxy", Image: "docker.io/istio/proxyv2:1.24.2"})
	}
	return cs
}

func create(_ context.Context, err error, what string) error {
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("%s: %w", what, err)
	}
	return nil
}

func (s *sim) setup(ctx context.Context) error {
	for slot := 0; slot < s.nodes; slot++ {
		if err := s.mkNode(ctx, slot, 0); err != nil {
			return err
		}
	}
	nss := nsList()
	for _, n := range nss {
		_, err := s.cs.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: n}}, metav1.CreateOptions{})
		if err := create(ctx, err, "ns "+n); err != nil {
			return err
		}
	}
	for _, d := range s.ds {
		ver := s.version(d)
		o := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: d.name, Namespace: d.ns},
			Spec: appsv1.DaemonSetSpec{Selector: selector(d.name),
				UpdateStrategy: appsv1.DaemonSetUpdateStrategy{Type: appsv1.RollingUpdateDaemonSetStrategyType, RollingUpdate: &appsv1.RollingUpdateDaemonSet{MaxUnavailable: ptrIS("20%")}},
				Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels(d.name, d.ns, d.team, ver)},
					Spec: podSpec([]corev1.Container{{Name: d.name, Image: "public.ecr.aws/eks/" + d.name + ":v" + ver}})}}}
		_, err := s.cs.AppsV1().DaemonSets(d.ns).Create(ctx, o, metav1.CreateOptions{})
		if err := create(ctx, err, "ds "+d.name); err != nil {
			return err
		}
	}
	for _, d := range s.deps {
		o := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: d.name, Namespace: d.ns},
			Spec: appsv1.DeploymentSpec{Replicas: &d.replicas, Selector: selector(d.name),
				Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels(d.name, d.ns, d.team, s.version(d))}, Spec: podSpec(s.depContainers(d))}}}
		_, err := s.cs.AppsV1().Deployments(d.ns).Create(ctx, o, metav1.CreateOptions{})
		if err := create(ctx, err, "deploy "+d.name); err != nil {
			return err
		}
	}
	for _, d := range s.sts {
		o := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: d.name, Namespace: d.ns},
			Spec: appsv1.StatefulSetSpec{Replicas: &d.replicas, Selector: selector(d.name), ServiceName: d.name, PodManagementPolicy: appsv1.ParallelPodManagement,
				Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels(d.name, d.ns, d.team, s.version(d))},
					Spec: podSpec([]corev1.Container{{Name: d.name, Image: fmt.Sprintf("123456789012.dkr.ecr.us-east-1.amazonaws.com/%s/%s:%s", d.team, d.name, s.version(d))}})}}}
		_, err := s.cs.AppsV1().StatefulSets(d.ns).Create(ctx, o, metav1.CreateOptions{})
		if err := create(ctx, err, "sts "+d.name); err != nil {
			return err
		}
	}
	for j, every := range cronEvery {
		ns := nss[3+(j*11)%37]
		team := ns[:strings.LastIndexByte(ns, '-')]
		name := fmt.Sprintf("%s-%s-%d", team, []string{"report", "cleanup", "sync", "backup", "reindex", "rollup"}[j%6], j)
		ev := int(math.Max(1, math.Round(float64(every)/s.speed)))
		sched := fmt.Sprintf("*/%d * * * *", ev)
		if ev >= 60 {
			sched = fmt.Sprintf("%d */%d * * *", j%60, max(1, ev/60))
			if ev >= 1440 {
				sched = fmt.Sprintf("%d %d * * *", j%60, j%24)
			}
		}
		dur := time.Duration(20+h(name)%460) * time.Second / time.Duration(math.Max(1, s.speed))
		if dur < 5*time.Second {
			dur = 5 * time.Second
		}
		pod := podSpec([]corev1.Container{{Name: name, Image: fmt.Sprintf("123456789012.dkr.ecr.us-east-1.amazonaws.com/%s/%s:1.0.%d", team, name, j)}})
		pod.RestartPolicy = corev1.RestartPolicyNever
		o := &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: batchv1.CronJobSpec{Schedule: sched, ConcurrencyPolicy: batchv1.ForbidConcurrent,
				SuccessfulJobsHistoryLimit: ptr32(2), FailedJobsHistoryLimit: ptr32(1),
				JobTemplate: batchv1.JobTemplateSpec{Spec: batchv1.JobSpec{TTLSecondsAfterFinished: ptr32(300),
					Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels(name, ns, team, fmt.Sprintf("1.0.%d", j)),
						Annotations: map[string]string{"pod-complete.stage.kwok.x-k8s.io/delay": dur.String()}}, Spec: pod}}}}}
		_, err := s.cs.BatchV1().CronJobs(ns).Create(ctx, o, metav1.CreateOptions{})
		if err := create(ctx, err, "cronjob "+name); err != nil {
			return err
		}
	}
	return nil
}

// edgeCases adds the objects where the agents' k8sattributes heuristics and
// the controller's owner chain can disagree (see ../../README.md).
func (s *sim) edgeCases(ctx context.Context) error {
	ns := "platform-core"
	// 1. a Job created by hand from a CronJob (kubectl create job --from=cronjob/...):
	//    owned by the CronJob, but its name has no 8-digit minute suffix
	cjs, err := s.cs.BatchV1().CronJobs("").List(ctx, metav1.ListOptions{Limit: 1})
	if err != nil || len(cjs.Items) == 0 {
		return fmt.Errorf("no cronjob for the edge case: %v", err)
	}
	cj := cjs.Items[0]
	tmpl := cj.Spec.JobTemplate.Spec.DeepCopy()
	tmpl.Template.Annotations = map[string]string{"pod-complete.stage.kwok.x-k8s.io/delay": "10000h"}
	tmpl.TTLSecondsAfterFinished = nil
	j := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: cj.Name + "-manual-1", Namespace: cj.Namespace,
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "CronJob", Name: cj.Name, UID: cj.UID, Controller: ptrB(true)}}},
		Spec: *tmpl}
	_, err = s.cs.BatchV1().Jobs(cj.Namespace).Create(ctx, j, metav1.CreateOptions{})
	if err := create(ctx, err, "manual job"); err != nil {
		return err
	}
	// 2. a ReplicaSet with no Deployment, named and labelled like a Deployment's
	rs := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "legacy-api-7d9f8c6b5", Namespace: ns},
		Spec: appsv1.ReplicaSetSpec{Replicas: ptr32(2), Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "legacy-api"}},
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "legacy-api", "pod-template-hash": "7d9f8c6b5"}},
				Spec: podSpec([]corev1.Container{{Name: "legacy-api", Image: "registry.example/legacy-api@sha256:" + strings.Repeat("ab", 32)}})}}}
	_, err = s.cs.AppsV1().ReplicaSets(ns).Create(ctx, rs, metav1.CreateOptions{})
	if err := create(ctx, err, "bare rs"); err != nil {
		return err
	}
	// 3. a bare pod (service.name = pod name at the pod level), image without a tag
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "debug-shell", Namespace: ns, Labels: map[string]string{"team": "platform"}},
		Spec: podSpec([]corev1.Container{{Name: "shell", Image: "busybox"}})}
	_, err = s.cs.CoreV1().Pods(ns).Create(ctx, p, metav1.CreateOptions{})
	return create(ctx, err, "bare pod")
}

func ptr32(v int32) *int32 { return &v }
func ptrB(v bool) *bool    { return &v }
func ptrIS(v string) *intstr.IntOrString {
	x := intstr.FromString(v)
	return &x
}

func (s *sim) patch(ctx context.Context, kind, ns, name string, body any) error {
	b, _ := json.Marshal(body)
	var err error
	switch kind {
	case "deploy":
		_, err = s.cs.AppsV1().Deployments(ns).Patch(ctx, name, types.StrategicMergePatchType, b, metav1.PatchOptions{})
	case "sts":
		_, err = s.cs.AppsV1().StatefulSets(ns).Patch(ctx, name, types.StrategicMergePatchType, b, metav1.PatchOptions{})
	case "ds":
		_, err = s.cs.AppsV1().DaemonSets(ns).Patch(ctx, name, types.StrategicMergePatchType, b, metav1.PatchOptions{})
	case "pod":
		_, err = s.cs.CoreV1().Pods(ns).Patch(ctx, name, types.StrategicMergePatchType, b, metav1.PatchOptions{})
	}
	return err
}

func (s *sim) rollout(ctx context.Context, kind string, d *dep) {
	d.ver++
	ver := s.version(d)
	img := fmt.Sprintf("123456789012.dkr.ecr.us-east-1.amazonaws.com/%s/%s:%s", d.team, d.name, ver)
	if kind == "ds" {
		img = "public.ecr.aws/eks/" + d.name + ":v" + ver
	}
	body := map[string]any{"spec": map[string]any{"template": map[string]any{
		"metadata": map[string]any{"labels": map[string]string{"app.kubernetes.io/version": ver}},
		"spec":     map[string]any{"containers": []map[string]string{{"name": d.name, "image": img}}}}}}
	if err := s.patch(ctx, kind, d.ns, d.name, body); err != nil {
		log.Printf("rollout %s %s: %v", kind, d.name, err)
		return
	}
	s.count("rollout_" + kind)
}

func (s *sim) count(k string) {
	s.mu.Lock()
	s.stats[k]++
	s.mu.Unlock()
}

func poisson(r *rand.Rand, mean float64) int {
	if mean <= 0 {
		return 0
	}
	if mean > 30 {
		return int(math.Max(0, math.Round(r.NormFloat64()*math.Sqrt(mean)+mean)))
	}
	l, k, p := math.Exp(-mean), 0, 1.0
	for {
		p *= r.Float64()
		if p <= l {
			return k
		}
		k++
	}
}

func (s *sim) churn(ctx context.Context, dur, tick time.Duration) {
	end := time.Now().Add(dur)
	last := s.now()
	for ctx.Err() == nil && time.Now().Before(end) {
		time.Sleep(tick)
		now := s.now()
		dt := now.Sub(last)
		last = now
		for _, d := range s.deps {
			if now.After(d.nextRoll) {
				s.rollout(ctx, "deploy", d)
				d.nextRoll = now.Add(time.Duration((1 + 9*s.rnd.Float64()) * float64(day)))
			}
			if d.hpa {
				hr := now.UTC().Hour()
				want := hr >= 8 && hr < 20
				if want != d.scaled {
					r := d.replicas
					if want {
						r += max(1, d.replicas/2)
					}
					if err := s.patch(ctx, "deploy", d.ns, d.name, map[string]any{"spec": map[string]any{"replicas": r}}); err == nil {
						d.scaled = want
						s.count("scale")
					}
				}
			}
		}
		for _, d := range s.sts {
			if now.After(d.nextRoll) {
				s.rollout(ctx, "sts", d)
				d.nextRoll = now.Add(time.Duration((10 + 30*s.rnd.Float64()) * float64(day)))
			}
		}
		for _, d := range s.ds {
			if now.After(d.nextRoll) {
				s.rollout(ctx, "ds", d)
				d.nextRoll = now.Add(time.Duration((14 + 26*s.rnd.Float64()) * float64(day)))
			}
		}
		// node replacement: new node first, then the old one goes (its pods are
		// garbage-collected by the pod GC and rescheduled by their controllers)
		for slot, due := range s.nodeDue {
			if now.After(due) {
				g := s.nodeGen[slot]
				if err := s.mkNode(ctx, slot, g+1); err != nil {
					log.Printf("node: %v", err)
					continue
				}
				if err := s.cs.CoreV1().Nodes().Delete(ctx, s.nodeName(slot, g), metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
					log.Printf("node delete: %v", err)
				}
				s.nodeGen[slot] = g + 1
				s.nodeDue[slot] = now.Add(time.Duration((5 + 20*s.rnd.Float64()) * float64(day)))
				s.count("node_replace")
			}
		}
		// evictions (1 per pod per 40 days) and relabels (3% of pods over a ~5-day life)
		days := dt.Hours() / 24
		ne, nr := poisson(s.rnd, 3000*days/40), poisson(s.rnd, 3000*0.03*days/5)
		if ne+nr > 0 {
			pods, err := s.cs.CoreV1().Pods("").List(ctx, metav1.ListOptions{ResourceVersion: "0"})
			if err == nil && len(pods.Items) > 0 {
				for i := 0; i < ne; i++ {
					p := pods.Items[s.rnd.Intn(len(pods.Items))]
					if o := metav1.GetControllerOf(&p); o == nil || (o.Kind != "ReplicaSet" && o.Kind != "StatefulSet") || strings.HasPrefix(p.Name, "legacy-") {
						continue
					}
					if err := s.cs.CoreV1().Pods(p.Namespace).Delete(ctx, p.Name, metav1.DeleteOptions{}); err == nil {
						s.count("evict")
					}
				}
				for i := 0; i < nr; i++ {
					p := pods.Items[s.rnd.Intn(len(pods.Items))]
					if p.Labels["debug"] == "true" {
						continue
					}
					if err := s.patch(ctx, "pod", p.Namespace, p.Name, map[string]any{"metadata": map[string]any{"labels": map[string]string{"debug": "true"}}}); err == nil {
						s.count("relabel")
					}
				}
			}
		}
	}
}

func main() {
	kubeconfig := flag.String("kubeconfig", "", "kubeconfig")
	idx := flag.Int("index", 0, "cluster index (names, region, seed)")
	nodes := flag.Int("nodes", 200, "nodes")
	ndeps := flag.Int("deployments", 520, "deployments")
	doSetup := flag.Bool("setup", false, "create the fleet objects")
	edge := flag.Bool("edge-cases", false, "add the agreement edge cases")
	churnFor := flag.Duration("churn", 0, "drive churn for this long")
	speed := flag.Float64("speed", 1, "simulated seconds per real second (rollouts, scaling, evictions, node replacement)")
	tick := flag.Duration("tick", 10*time.Second, "churn tick")
	qps := flag.Float64("qps", 50, "client QPS")
	flag.Parse()
	rc, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		log.Fatal(err)
	}
	rc.QPS, rc.Burst = float32(*qps), int(*qps*2)
	s := &sim{cs: kubernetes.NewForConfigOrDie(rc), idx: *idx, rnd: rand.New(rand.NewSource(int64(7000 + *idx))), nodes: *nodes,
		speed: *speed, start: time.Now(), simT0: time.Now(), nodeGen: map[int]int{}, nodeDue: map[int]time.Time{},
		region: regions[*idx%len(regions)], stats: map[string]int{}}
	nss := nsList()
	for d := 0; d < *ndeps; d++ {
		ns := nss[3+d%37]
		team := ns[:strings.LastIndexByte(ns, '-')]
		name := fmt.Sprintf("%s-%s", team, words[(d/37)%len(words)])
		if d >= 37*len(words) {
			name += fmt.Sprintf("-%d", d/(37*len(words)))
		}
		s.deps = append(s.deps, &dep{ns: ns, name: name, team: team, replicas: int32(replicaSet[s.rnd.Intn(len(replicaSet))]),
			hpa: s.rnd.Float64() < 0.2, sidecar: s.rnd.Float64() < 0.25, nextRoll: s.simT0.Add(time.Duration(10 * s.rnd.Float64() * float64(day)))})
	}
	for i := 0; i < 40; i++ {
		ns := nss[3+(i*7)%37]
		team := ns[:strings.LastIndexByte(ns, '-')]
		name := fmt.Sprintf("%s-%s", team, []string{"db", "kafka", "redis", "zk", "es"}[i%5])
		if i >= 5 {
			name += fmt.Sprintf("-%d", i/5)
		}
		s.sts = append(s.sts, &dep{ns: ns, name: name, team: team, replicas: int32([]int{1, 3, 3, 5}[s.rnd.Intn(4)]),
			nextRoll: s.simT0.Add(time.Duration(40 * s.rnd.Float64() * float64(day)))})
	}
	for _, x := range [][2]string{{"kube-proxy", "kube-system"}, {"aws-node", "kube-system"}, {"otel-agent", "observability"}, {"fluent-bit", "observability"}} {
		s.ds = append(s.ds, &dep{ns: x[1], name: x[0], team: "platform", nextRoll: s.simT0.Add(time.Duration(40 * s.rnd.Float64() * float64(day)))})
	}
	for slot := 0; slot < *nodes; slot++ {
		s.nodeDue[slot] = s.simT0.Add(time.Duration(s.rnd.Float64() * 25 * float64(day)))
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *doSetup {
		t := time.Now()
		if err := s.setup(ctx); err != nil {
			log.Fatal(err)
		}
		log.Printf("setup done in %v", time.Since(t).Round(time.Second))
	}
	if *edge {
		if err := s.edgeCases(ctx); err != nil {
			log.Fatal(err)
		}
	}
	if *churnFor > 0 {
		go func() {
			for ctx.Err() == nil {
				time.Sleep(time.Minute)
				s.mu.Lock()
				b, _ := json.Marshal(s.stats)
				s.mu.Unlock()
				log.Printf("churn stats %s sim=%s", b, s.now().UTC().Format(time.RFC3339))
			}
		}()
		s.churn(ctx, *churnFor, *tick)
		b, _ := json.Marshal(s.stats)
		log.Printf("churn done %s", b)
	}
}
