// ridcheck: two checks against the catalog the aggregator builds.
//
// agree: for every running pod, computes each container's resource_id the
// way an AGENT would (k8sattributes' own rules, read from
// processor/k8sattributesprocessor/internal/kube/client.go at main: owner
// references and name heuristics, no ReplicaSet / Job informer; plus what
// resourcedetection's ec2 detector reads from IMDS), straight from a fresh
// API list, and compares it with the ids the controller wrote. It shares
// only the hash (rid.ID) with the controller, not the derivation.
//
// probe: freshness, event -> visible in resources_current: creates a pod,
// relabels it, deletes it, and times each until the catalog shows it.
//
// watchlog: ground truth for "what did the catalog miss": one JSON line per
// pod that was ever scheduled (uid, created, scheduled, gone), from a watch
// independent of the controller.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/casselc/observability/otel-chdb/entities/controller/internal/ch"
	"github.com/casselc/observability/otel-chdb/entities/controller/internal/rid"
	"github.com/distribution/reference"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

var cronJobRegex = regexp.MustCompile(`^(.*)-(\d{8})$`)

const cronJobSkewMinutes = 60 * 24

// edgeImage is internal/common/docker.ParseImageName.
func edgeImage(image string) (string, string) {
	ref, err := reference.Parse(image)
	if err != nil {
		return "", ""
	}
	named, ok := ref.(reference.Named)
	if !ok {
		return "", ""
	}
	tag := "latest"
	if t, ok := named.(reference.Tagged); ok {
		tag = t.Tag()
	}
	return named.Name(), tag
}

// edgeAttrs: k8sattributes' extractPodAttributes with the rules the agents
// enable (metadata list + the covered labels), then the resource processor's
// k8s.cluster.name and resourcedetection's cloud.* / host.* (IMDS).
func edgeAttrs(p *corev1.Pod, node *corev1.Node, clusterUID string, static map[string]string, labels []string) map[string]string {
	t := map[string]string{}
	t["k8s.pod.name"] = p.Name
	t["service.name"] = p.Name
	t["k8s.namespace.name"] = p.Namespace
	if b, err := p.CreationTimestamp.MarshalText(); err == nil && !p.CreationTimestamp.IsZero() {
		t["k8s.pod.start_time"] = string(b)
	}
	t["k8s.pod.uid"] = string(p.UID)
	for _, ref := range p.OwnerReferences {
		switch ref.Kind {
		case "ReplicaSet":
			t["k8s.replicaset.name"] = ref.Name
			t["service.name"] = ref.Name
			h := p.Labels["pod-template-hash"]
			if h != "" && strings.HasSuffix(ref.Name, "-"+h) {
				d := ref.Name[:len(ref.Name)-len(h)-1]
				t["k8s.deployment.name"] = d
				t["service.name"] = d
			}
		case "DaemonSet":
			t["k8s.daemonset.name"] = ref.Name
			t["service.name"] = ref.Name
		case "StatefulSet":
			t["k8s.statefulset.name"] = ref.Name
			t["service.name"] = ref.Name
		case "Job":
			t["k8s.job.name"] = ref.Name
			t["service.name"] = ref.Name
			if m := cronJobRegex.FindStringSubmatch(ref.Name); len(m) == 3 {
				mins, _ := strconv.ParseInt(m[2], 10, 64)
				d := p.CreationTimestamp.Unix()/60 - mins
				if d < 0 {
					d = -d
				}
				if d <= cronJobSkewMinutes {
					t["k8s.cronjob.name"] = m[1]
					t["service.name"] = m[1]
				}
			}
		}
	}
	t["k8s.node.name"] = p.Spec.NodeName
	t["k8s.cluster.uid"] = clusterUID
	for _, l := range labels {
		if v, ok := p.Labels[l]; ok && v != "" {
			t["k8s.pod.label."+l] = v
		}
	}
	if v := p.Labels["app.kubernetes.io/name"]; v != "" {
		t["service.name"] = v
	}
	if v := p.Labels["app.kubernetes.io/instance"]; v != "" {
		t["service.name"] = v
	}
	// node metadata (k8sattributes node informer) and IMDS (resourcedetection ec2)
	if node != nil {
		t["k8s.node.uid"] = string(node.UID)
		t["host.name"] = node.Name // IMDS hostname = the EKS node name (private DNS)
		t["host.id"] = node.Annotations["sim.k8s/ec2-instance-id"]
		t["host.type"] = node.Labels["node.kubernetes.io/instance-type"]
		t["cloud.availability.zone"] = node.Labels["topology.kubernetes.io/zone"]
	}
	for k, v := range static {
		t[k] = v
	}
	return t
}

type catRow struct {
	id    uint64
	attrs map[string]string
}

func main() {
	kubeconfig := flag.String("kubeconfig", "", "kubeconfig")
	cluster := flag.String("cluster", "", "k8s.cluster.name")
	static := flag.String("static", "", "static attributes k=v,...")
	labels := flag.String("labels", "app.kubernetes.io/name,app.kubernetes.io/instance,app.kubernetes.io/version,app.kubernetes.io/component,app.kubernetes.io/part-of,app.kubernetes.io/managed-by,team,pod-template-hash,controller-revision-hash,statefulset.kubernetes.io/pod-name,batch.kubernetes.io/job-name,debug", "covered labels")
	chURL := flag.String("ch", "http://localhost:18123", "ClickHouse")
	db := flag.String("db", "k8s_cat", "catalog db")
	mode := flag.String("mode", "agree", "agree | probe")
	n := flag.Int("n", 10, "probe iterations")
	flag.Parse()
	st := map[string]string{"k8s.cluster.name": *cluster}
	for _, kv := range strings.Split(*static, ",") {
		if k, v, ok := strings.Cut(kv, "="); ok {
			st[k] = v
		}
	}
	rc, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		log.Fatal(err)
	}
	rc.QPS, rc.Burst = 50, 100
	cs := kubernetes.NewForConfigOrDie(rc)
	ctx := context.Background()
	c := ch.New(*chURL)
	ks, err := cs.CoreV1().Namespaces().Get(ctx, "kube-system", metav1.GetOptions{})
	if err != nil {
		log.Fatal(err)
	}
	ckey := rid.Key("cluster", string(ks.UID))
	if *mode == "watchlog" {
		watchlog(ctx, cs)
		return
	}
	if *mode == "probe" {
		probe(ctx, cs, c, *db, ckey, *n)
		return
	}
	lbl := strings.Split(*labels, ",")
	t0 := time.Now()
	nodes, err := cs.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Fatal(err)
	}
	byName := map[string]*corev1.Node{}
	for i := range nodes.Items {
		byName[nodes.Items[i].Name] = &nodes.Items[i]
	}
	pods, err := cs.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Fatal(err)
	}
	listedAt := time.Now()
	// what the catalog says is live, as of the list (plus a settle delay so both views describe the same moment)
	time.Sleep(25 * time.Second)
	rows, err := c.Query(fmt.Sprintf("SELECT pod_uid, container, resource_id, toJSONString(attrs) FROM %s.resources WHERE cluster_key = %d AND valid_from <= toDateTime64(%f, 3) AND valid_to > toDateTime64(%f, 3)",
		*db, ckey, float64(listedAt.UnixMilli())/1000, float64(listedAt.UnixMilli())/1000))
	if err != nil {
		log.Fatal(err)
	}
	cat := map[string][]catRow{}
	for _, r := range rows {
		id, _ := strconv.ParseUint(r[2], 10, 64)
		var a map[string]string
		_ = json.Unmarshal([]byte(unescapeTSV(r[3])), &a)
		cat[r[0]+"/"+r[1]] = append(cat[r[0]+"/"+r[1]], catRow{id, a})
	}
	type res struct {
		Pods, Containers, Match, Mismatch, Missing, Multi, CatalogExtra int
		MismatchByKeys                                                  map[string]int
		Examples                                                        map[string]string
		ByKind                                                          map[string][2]int
	}
	out := res{MismatchByKeys: map[string]int{}, Examples: map[string]string{}, ByKind: map[string][2]int{}}
	seen := map[string]bool{}
	listed := map[string]bool{}
	for i := range pods.Items {
		listed[string(pods.Items[i].UID)] = true
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Spec.NodeName == "" || p.Status.Phase != corev1.PodRunning || p.DeletionTimestamp != nil || p.CreationTimestamp.Time.After(listedAt.Add(-30*time.Second)) {
			continue // not emitting yet, or too young to be fair to either side
		}
		out.Pods++
		kind := "none"
		if o := metav1.GetControllerOf(p); o != nil {
			kind = o.Kind
		}
		base := edgeAttrs(p, byName[p.Spec.NodeName], string(ks.UID), st, lbl)
		for _, ct := range p.Spec.Containers {
			out.Containers++
			img, tag := edgeImage(ct.Image)
			a := rid.Merge(base, map[string]string{"k8s.container.name": ct.Name, "container.image.name": img, "container.image.tag": tag})
			id := rid.ID(a)
			k := string(p.UID) + "/" + ct.Name
			seen[k] = true
			got := cat[k]
			bk := out.ByKind[kind]
			switch {
			case len(got) == 0:
				out.Missing++
			case len(got) > 1:
				out.Multi++
			case got[0].id == id:
				out.Match++
				bk[0]++
			default:
				out.Mismatch++
				bk[1]++
				var diff []string
				for key, v := range a {
					if got[0].attrs[key] != v {
						diff = append(diff, key)
					}
				}
				for key := range got[0].attrs {
					if _, ok := a[key]; !ok {
						diff = append(diff, key+"(catalog only)")
					}
				}
				sort.Strings(diff)
				sig := kind + ": " + strings.Join(diff, ",")
				out.MismatchByKeys[sig]++
				if _, ok := out.Examples[sig]; !ok {
					var ex []string
					for _, key := range diff {
						kk := strings.TrimSuffix(key, "(catalog only)")
						ex = append(ex, fmt.Sprintf("%s: edge=%q catalog=%q", kk, a[kk], got[0].attrs[kk]))
					}
					out.Examples[sig] = p.Namespace + "/" + p.Name + " " + strings.Join(ex, "; ")
				}
			}
			out.ByKind[kind] = bk
		}
	}
	for k := range cat { // open in the catalog, but the pod is not in the list at all
		if !seen[k] && !listed[k[:strings.IndexByte(k, '/')]] {
			out.CatalogExtra++
		}
	}
	b, _ := json.MarshalIndent(map[string]any{"cluster": *cluster, "listed_at": listedAt.UTC().Format(time.RFC3339), "took_s": time.Since(t0).Seconds(), "result": out}, "", "  ")
	fmt.Println(string(b))
	// the hash, cross-implementation: ClickHouse's xxh3 over the stored covered set = the Go id
	rows, err = c.Query(fmt.Sprintf(`SELECT count(), countIf(resource_id != xxh3(concat('res.v1\0', arrayStringConcat(arrayMap(x -> concat(x.1, '\0', x.2, '\0'),
        arraySort(arrayFilter(x -> x.2 != '', CAST(attrs, 'Array(Tuple(String, String))')))))))) FROM %s.resources WHERE cluster_key = %d`, *db, ckey))
	if err == nil && len(rows) > 0 {
		fmt.Printf("{\"hash_check\": {\"resources\": %s, \"go_ne_clickhouse\": %s}}\n", rows[0][0], rows[0][1])
	} else {
		log.Printf("hash check: %v", err)
	}
}

func unescapeTSV(s string) string {
	r := strings.NewReplacer(`\\`, `\`, `\t`, "\t", `\n`, "\n", `\'`, "'")
	return r.Replace(s)
}

// probe: event -> visible latencies for create, relabel, delete.
func probe(ctx context.Context, cs kubernetes.Interface, c *ch.Client, db string, ckey uint64, n int) {
	ns := "freshness-probe"
	_, _ = cs.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, metav1.CreateOptions{})
	time.Sleep(15 * time.Second)
	wait := func(q string, want func(string) bool) time.Duration {
		t := time.Now()
		for time.Since(t) < 10*time.Minute {
			rows, err := c.Query(q)
			if err == nil && len(rows) > 0 && want(rows[0][0]) {
				return time.Since(t)
			}
			time.Sleep(250 * time.Millisecond)
		}
		return -1
	}
	type sample struct{ Create, Relabel, Delete float64 }
	var all []sample
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("probe-%d-%d", time.Now().Unix(), i)
		p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: map[string]string{"app.kubernetes.io/name": "probe"}},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "probe", Image: "probe:1"}}, Tolerations: []corev1.Toleration{{Operator: corev1.TolerationOpExists}},
				NodeSelector: map[string]string{"type": "kwok"}}}
		t0 := time.Now()
		cp, err := cs.CoreV1().Pods(ns).Create(ctx, p, metav1.CreateOptions{})
		if err != nil {
			log.Fatal(err)
		}
		uid := string(cp.UID)
		cur := fmt.Sprintf("SELECT toString(count()) FROM %s.resources_current WHERE cluster_key = %d AND pod_uid = '%s'", db, ckey, uid)
		d1 := wait(cur, func(s string) bool { return s == "1" })
		c1 := time.Since(t0)
		_ = d1
		t1 := time.Now()
		if _, err := cs.CoreV1().Pods(ns).Patch(ctx, name, types.StrategicMergePatchType, []byte(`{"metadata":{"labels":{"debug":"true"}}}`), metav1.PatchOptions{}); err != nil {
			log.Fatal(err)
		}
		wait(fmt.Sprintf("SELECT toString(countIf(attrs['k8s.pod.label.debug'] = 'true')) || '/' || toString(count()) FROM %s.resources_current WHERE cluster_key = %d AND pod_uid = '%s'", db, ckey, uid),
			func(s string) bool { return s == "1/1" })
		c2 := time.Since(t1)
		t2 := time.Now()
		if err := cs.CoreV1().Pods(ns).Delete(ctx, name, metav1.DeleteOptions{GracePeriodSeconds: new(int64)}); err != nil {
			log.Fatal(err)
		}
		wait(cur, func(s string) bool { return s == "0" })
		c3 := time.Since(t2)
		s := sample{c1.Seconds(), c2.Seconds(), c3.Seconds()}
		all = append(all, s)
		b, _ := json.Marshal(s)
		fmt.Println(string(b))
		os.Stdout.Sync()
	}
	sum := func(f func(sample) float64) string {
		var v []float64
		for _, s := range all {
			v = append(v, f(s))
		}
		sort.Float64s(v)
		if len(v) == 0 {
			return ""
		}
		return fmt.Sprintf("min %.1f median %.1f max %.1f", v[0], v[len(v)/2], v[len(v)-1])
	}
	fmt.Printf("{\"probe_summary\": {\"n\": %d, \"create_s\": %q, \"relabel_s\": %q, \"delete_s\": %q}}\n", len(all),
		sum(func(s sample) float64 { return s.Create }), sum(func(s sample) float64 { return s.Relabel }), sum(func(s sample) float64 { return s.Delete }))
}

// watchlog prints a line when a pod is first seen scheduled and when it goes.
func watchlog(ctx context.Context, cs kubernetes.Interface) {
	f := informers.NewSharedInformerFactory(cs, 0)
	seen := map[types.UID]bool{}
	var mu sync.Mutex
	enc := json.NewEncoder(os.Stdout)
	line := func(ev string, p *corev1.Pod) {
		_ = enc.Encode(map[string]any{"ev": ev, "t": time.Now().UnixMilli(), "uid": p.UID, "ns": p.Namespace, "name": p.Name,
			"created": p.CreationTimestamp.UnixMilli(), "phase": p.Status.Phase})
	}
	f.Core().V1().Pods().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(o any) {
			p := o.(*corev1.Pod)
			mu.Lock()
			defer mu.Unlock()
			if p.Spec.NodeName != "" && !seen[p.UID] {
				seen[p.UID] = true
				line("scheduled", p)
			}
		},
		UpdateFunc: func(_, o any) {
			p := o.(*corev1.Pod)
			mu.Lock()
			defer mu.Unlock()
			if p.Spec.NodeName != "" && !seen[p.UID] {
				seen[p.UID] = true
				line("scheduled", p)
			}
			if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
				if seen[p.UID] {
					line("terminal", p)
				}
			}
		},
		DeleteFunc: func(o any) {
			if d, ok := o.(cache.DeletedFinalStateUnknown); ok {
				o = d.Obj
			}
			if p, ok := o.(*corev1.Pod); ok {
				line("deleted", p)
			}
		},
	})
	f.Start(ctx.Done())
	<-ctx.Done()
}
