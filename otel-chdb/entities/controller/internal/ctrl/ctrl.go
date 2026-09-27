// Package ctrl is the per-cluster entity controller: shared informers on
// Pods, Nodes, Namespaces, ReplicaSets and Jobs; for every change it derives
// the versioned catalog rows (../../sql/catalog.sql levels) and each
// container's resource_id, and hands the opened / closed versions to a lane.
package ctrl

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/casselc/observability/otel-chdb/entities/controller/internal/lane"
	"github.com/casselc/observability/otel-chdb/entities/controller/internal/rid"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	appslisters "k8s.io/client-go/listers/apps/v1"
	batchlisters "k8s.io/client-go/listers/batch/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

type Config struct {
	ClusterName string
	Static      map[string]string // cloud.*, deployment.environment.name
	PodLabels   []string          // label keys copied as k8s.pod.label.<key>
	Writer      string
	Resync      time.Duration // full-state sync objects
	Transform   bool          // strip cached objects to what the controller reads
}

type podState struct {
	uid       types.UID
	podKey    uint64
	wlKey     uint64
	rec       lane.Record            // the open pod version
	res       map[uint64]lane.Record // open resource versions by resource_id
	terminal  bool
	createdMs lane.Time
}

type wlState struct {
	rec  lane.Record
	refs int
}

type Controller struct {
	cfg  Config
	cs   kubernetes.Interface
	out  *lane.Writer
	inf  informers.SharedInformerFactory
	pods corelisters.PodLister
	node corelisters.NodeLister
	ns   corelisters.NamespaceLister
	rs   appslisters.ReplicaSetLister
	jobs batchlisters.JobLister
	q    workqueue.TypedRateLimitingInterface[string]

	mu         sync.Mutex
	cluster    lane.Record
	clusterKey uint64
	clusterUID string
	nodes      map[string]lane.Record // by node name: the open version
	nss        map[string]lane.Record
	wls        map[uint64]*wlState
	podsByUID  map[types.UID]*podState
	regs       []cache.ResourceEventHandlerRegistration

	Events, Emitted, Requeues, Fallbacks atomic.Int64
	// Lists counts the informers' LISTs (initial and relists: a watch that
	// ended with 410 Gone, a broken connection, a restart); anything above
	// one per informer is a relist, i.e. a window in which events were not
	// observed (AMBIGUITY.md, "entity controller informer"). client-go
	// 0.37 relists after a 410 without calling the watch error handler, so
	// the LIST itself is what is counted. DeletedUnknown counts deletions
	// seen only by such a relist (cache.DeletedFinalStateUnknown): their
	// close time is the relist, not the deletion.
	Lists, DeletedUnknown atomic.Int64
	Synced                               atomic.Bool
	inflight                             atomic.Int64
}

func New(cfg Config, cs kubernetes.Interface, out *lane.Writer) *Controller {
	c := &Controller{cfg: cfg, cs: cs, out: out,
		nodes: map[string]lane.Record{}, nss: map[string]lane.Record{},
		wls: map[uint64]*wlState{}, podsByUID: map[types.UID]*podState{},
		q: workqueue.NewTypedRateLimitingQueue(workqueue.NewTypedItemExponentialFailureRateLimiter[string](200*time.Millisecond, 5*time.Second)),
	}
	opts := []informers.SharedInformerOption{}
	if cfg.Transform {
		opts = append(opts, informers.WithTransform(strip))
	}
	opts = append(opts, informers.WithTweakListOptions(func(o *metav1.ListOptions) { c.countList(o) }))
	c.inf = informers.NewSharedInformerFactoryWithOptions(cs, 0, opts...)
	c.pods = c.inf.Core().V1().Pods().Lister()
	c.node = c.inf.Core().V1().Nodes().Lister()
	c.ns = c.inf.Core().V1().Namespaces().Lister()
	c.rs = c.inf.Apps().V1().ReplicaSets().Lister()
	c.jobs = c.inf.Batch().V1().Jobs().Lister()

	reg := func(r cache.ResourceEventHandlerRegistration, err error) {
		if err == nil {
			c.regs = append(c.regs, r)
		}
	}
	reg(c.inf.Core().V1().Pods().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(o any) { c.enqueue(o) },
		UpdateFunc: func(_, o any) { c.enqueue(o) },
		DeleteFunc: func(o any) { c.podDeleted(o) },
	}))
	reg(c.inf.Core().V1().Nodes().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(o any) { c.nodeChanged(o.(*corev1.Node), false) },
		UpdateFunc: func(_, o any) { c.nodeChanged(o.(*corev1.Node), false) },
		DeleteFunc: func(o any) {
			o = c.unknownFinal(o)
			if n, ok := o.(*corev1.Node); ok {
				c.nodeChanged(n, true)
			}
		},
	}))
	reg(c.inf.Core().V1().Namespaces().Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(o any) { c.nsChanged(o.(*corev1.Namespace), false) },
		DeleteFunc: func(o any) {
			o = c.unknownFinal(o)
			if n, ok := o.(*corev1.Namespace); ok {
				c.nsChanged(n, true)
			}
		},
	}))
	// ReplicaSets and Jobs are only looked up (the Deployment / CronJob above a pod).
	c.inf.Apps().V1().ReplicaSets().Informer()
	c.inf.Batch().V1().Jobs().Informer()
	return c
}

// strip keeps only the fields the controller reads: cache memory is most of
// its footprint at 3,000 pods.
func strip(o any) (any, error) {
	switch x := o.(type) {
	case *corev1.Pod:
		x.ManagedFields, x.Annotations = nil, nil
		cs := make([]corev1.Container, len(x.Spec.Containers))
		for i, ct := range x.Spec.Containers {
			cs[i] = corev1.Container{Name: ct.Name, Image: ct.Image}
		}
		var sched metav1.Time
		for _, cd := range x.Status.Conditions {
			if cd.Type == corev1.PodScheduled && cd.Status == corev1.ConditionTrue {
				sched = cd.LastTransitionTime
			}
		}
		x.Spec = corev1.PodSpec{NodeName: x.Spec.NodeName, Containers: cs}
		x.Status = corev1.PodStatus{Phase: x.Status.Phase}
		if !sched.IsZero() {
			x.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: sched}}
		}
	case *corev1.Node:
		x.ManagedFields, x.Annotations = nil, nil
		x.Status = corev1.NodeStatus{}
	case *batchv1.Job:
		x.ManagedFields, x.Annotations = nil, nil
		x.Spec = batchv1.JobSpec{}
		x.Status = batchv1.JobStatus{}
	case *appsv1.ReplicaSet:
		x.ManagedFields, x.Annotations = nil, nil
		x.Spec = appsv1.ReplicaSetSpec{}
		x.Status = appsv1.ReplicaSetStatus{}
	case *corev1.Namespace:
		x.ManagedFields, x.Annotations = nil, nil
	}
	return o, nil
}

// countList sees the options of every LIST and WATCH the informers send. A
// LIST has no watch timeout; a watch that starts with the initial state
// (the streaming "watch list", SendInitialEvents) is a LIST as well.
func (c *Controller) countList(o *metav1.ListOptions) {
	if o.TimeoutSeconds == nil || (o.SendInitialEvents != nil && *o.SendInitialEvents) {
		if n := c.Lists.Add(1); n > informerCount {
			log.Printf("informer relist (%d lists for %d informers): events in the gap were not observed; closes found by it carry the relist time", n, informerCount)
		}
	}
}

// informerCount is the number of informers New starts.
const informerCount = 5

// unknownFinal unwraps a deletion seen only by a relist, counting it.
func (c *Controller) unknownFinal(o any) any {
	if d, ok := o.(cache.DeletedFinalStateUnknown); ok {
		c.DeletedUnknown.Add(1)
		return d.Obj
	}
	return o
}

func (c *Controller) enqueue(o any) {
	c.Events.Add(1)
	if k, err := cache.MetaNamespaceKeyFunc(o); err == nil {
		c.q.Add(k)
	}
}

// Run starts the informers, the cluster record, the workers and the
// periodic sync; it returns when ctx ends.
func (c *Controller) Run(ctx context.Context, workers int) error {
	ksys, err := c.cs.CoreV1().Namespaces().Get(ctx, "kube-system", metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("kube-system (cluster uid): %w", err)
	}
	c.clusterUID = string(ksys.UID)
	attrs := map[string]string{"k8s.cluster.name": c.cfg.ClusterName, "k8s.cluster.uid": c.clusterUID}
	for k, v := range c.cfg.Static {
		attrs[k] = v
	}
	c.clusterKey = rid.Key("cluster", c.clusterUID)
	now := lane.Now()
	c.cluster = lane.Record{Level: lane.LCluster, Key: rid.AttrsKey("cluster", attrs), Entity: c.clusterKey, ClusterKey: c.clusterKey,
		Name: c.cfg.ClusterName, Attrs: attrs, ValidFrom: lane.FromTime(ksys.CreationTimestamp.Time), ObservedAt: now, EventAt: now, Writer: c.cfg.Writer}
	c.emit(c.cluster)

	c.inf.Start(ctx.Done())
	for t, ok := range c.inf.WaitForCacheSync(ctx.Done()) {
		if !ok {
			return fmt.Errorf("cache sync %v", t)
		}
	}
	// the handlers must have seen the initial list too, or the first sync
	// would leave out (and so close) everything they have not processed yet
	for _, r := range c.regs {
		if !cache.WaitForCacheSync(ctx.Done(), r.HasSynced) {
			return fmt.Errorf("handler sync")
		}
	}
	for i := 0; i < workers; i++ {
		go c.worker(ctx)
	}
	// the initial sync once the queue has drained: it is what closes the
	// versions that ended while this controller was not running
	for quiet := 0; quiet < 3 && ctx.Err() == nil; {
		time.Sleep(300 * time.Millisecond)
		if c.q.Len() == 0 && c.inflight.Load() == 0 {
			quiet++
		} else {
			quiet = 0
		}
	}
	c.Synced.Store(true)
	c.sync(ctx)
	t := time.NewTicker(c.cfg.Resync)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			c.q.ShutDown()
			return nil
		case <-t.C:
			c.sync(ctx)
		}
	}
}

func (c *Controller) worker(ctx context.Context) {
	for {
		k, quit := c.q.Get()
		if quit {
			return
		}
		c.inflight.Add(1)
		err := c.process(k)
		c.inflight.Add(-1)
		if err != nil && c.q.NumRequeues(k) < 15 {
			c.Requeues.Add(1)
			c.q.AddRateLimited(k)
		} else {
			c.q.Forget(k)
		}
		c.q.Done(k)
	}
}

func (c *Controller) emit(recs ...lane.Record) {
	c.Emitted.Add(int64(len(recs)))
	c.out.Add(recs...)
}

func (c *Controller) nodeChanged(n *corev1.Node, deleted bool) {
	c.Events.Add(1)
	now := lane.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	old, had := c.nodes[n.Name]
	if deleted {
		if had {
			old.ClosedAt, old.ObservedAt, old.EventAt = now, now, now
			if n.DeletionTimestamp != nil {
				old.EventAt = lane.FromTime(n.DeletionTimestamp.Time)
			}
			c.emit(old)
			delete(c.nodes, n.Name)
		}
		return
	}
	attrs := NodeAttrs(n)
	key := rid.AttrsKey("node", attrs)
	if had && old.Key == key {
		return
	}
	if had {
		old.ClosedAt, old.ObservedAt, old.EventAt = now, now, now
		c.emit(old)
	}
	vf := lane.FromTime(n.CreationTimestamp.Time)
	if had {
		vf = now
	}
	r := lane.Record{Level: lane.LNode, Key: key, Entity: rid.Key("node", string(n.UID)), ClusterKey: c.clusterKey, Name: n.Name,
		Attrs: attrs, ValidFrom: vf, ObservedAt: now, EventAt: vf, Writer: c.cfg.Writer}
	c.nodes[n.Name] = r
	c.emit(r)
	if had { // every resource on the node gets a new id
		for _, p := range c.podsOnNode(n.Name) {
			c.q.Add(p)
		}
	}
}

func (c *Controller) podsOnNode(node string) []string {
	var out []string
	pods, _ := c.pods.List(everything)
	for _, p := range pods {
		if p.Spec.NodeName == node {
			out = append(out, p.Namespace+"/"+p.Name)
		}
	}
	return out
}

func (c *Controller) nsChanged(n *corev1.Namespace, deleted bool) {
	c.Events.Add(1)
	now := lane.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if deleted {
		if r, ok := c.nss[n.Name]; ok {
			r.ClosedAt, r.ObservedAt, r.EventAt = now, now, now
			c.emit(r)
			delete(c.nss, n.Name)
		}
		return
	}
	if _, ok := c.nss[n.Name]; ok {
		return
	}
	attrs := map[string]string{"k8s.namespace.name": n.Name}
	vf := lane.FromTime(n.CreationTimestamp.Time)
	r := lane.Record{Level: lane.LNamespace, Key: rid.Key("ns", c.clusterUID, n.Name), Entity: rid.Key("ns", c.clusterUID, n.Name),
		ClusterKey: c.clusterKey, Name: n.Name, Attrs: attrs, ValidFrom: vf, ObservedAt: now, EventAt: vf, Writer: c.cfg.Writer}
	c.nss[n.Name] = r
	c.emit(r)
}

func (c *Controller) podDeleted(o any) {
	c.Events.Add(1)
	o = c.unknownFinal(o)
	p, ok := o.(*corev1.Pod)
	if !ok {
		return
	}
	now := lane.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if st, ok := c.podsByUID[p.UID]; ok {
		c.closePod(st, now)
		delete(c.podsByUID, p.UID)
	}
}

// closePod emits the close of a pod version and its resources (under mu).
func (c *Controller) closePod(st *podState, now lane.Time) {
	if st.terminal {
		return
	}
	r := st.rec
	r.ClosedAt, r.ObservedAt, r.EventAt = now, now, now
	c.emit(r)
	for _, rr := range st.res {
		rr.ClosedAt, rr.ObservedAt, rr.EventAt = now, now, now
		c.emit(rr)
	}
	if w := c.wls[st.wlKey]; w != nil {
		w.refs--
	}
	st.terminal = true
}

var everything = labels.Everything()

// workload resolves the pod's owner chain through the ReplicaSet and Job
// caches. ok=false: an owner is not in the cache yet (retry).
func (c *Controller) workload(p *corev1.Pod) (kind, name string, wl, pod map[string]string, ok bool) {
	// The covered attributes follow the AGENT's rules (k8sattributes, which
	// has no ReplicaSet or Job informer), not the owner chain: a resource_id
	// the catalog computes differently from the agent joins to nothing.
	//   - ReplicaSet: k8s.deployment.name is the ReplicaSet name minus
	//     "-<pod-template-hash>" whenever the pod carries that label, whether
	//     or not a Deployment owns the ReplicaSet;
	//   - Job: k8s.cronjob.name only when the Job name ends in -<8 digits>
	//     (the scheduled minute) within a day of the pod's creation, so a Job
	//     created by hand from a CronJob has none.
	// kind and name (the workload version's identity, not hashed into any
	// resource_id) still follow the owner chain.
	wl, pod = map[string]string{}, map[string]string{}
	svc := p.Name // k8sattributes' service.name precedence: pod, owner, deployment/cronjob, name label, instance label
	svcAtPod := true
	ref := metav1.GetControllerOf(p)
	if ref != nil {
		svc, svcAtPod = ref.Name, false
		switch ref.Kind {
		case "ReplicaSet":
			kind, name = "replicaset", ref.Name
			wl["k8s.replicaset.name"] = ref.Name
			if h := p.Labels["pod-template-hash"]; h != "" && strings.HasSuffix(ref.Name, "-"+h) {
				d := strings.TrimSuffix(ref.Name, "-"+h)
				wl["k8s.deployment.name"] = d
				svc = d
			}
			rs, err := c.rs.ReplicaSets(p.Namespace).Get(ref.Name)
			if err != nil {
				return "", "", nil, nil, false
			}
			if d := metav1.GetControllerOf(rs); d != nil && d.Kind == "Deployment" {
				kind, name = "deployment", d.Name
			}
		case "StatefulSet":
			kind, name = "statefulset", ref.Name
			wl["k8s.statefulset.name"] = ref.Name
		case "DaemonSet":
			kind, name = "daemonset", ref.Name
			wl["k8s.daemonset.name"] = ref.Name
		case "Job":
			kind, name = "job", ref.Name
			j, err := c.jobs.Jobs(p.Namespace).Get(ref.Name)
			if err != nil {
				return "", "", nil, nil, false
			}
			if cj := metav1.GetControllerOf(j); cj != nil && cj.Kind == "CronJob" {
				kind, name = "cronjob", cj.Name
			}
			if cj, isRun := agentCronJob(ref.Name, p.CreationTimestamp.Time); isRun {
				wl["k8s.cronjob.name"] = cj
				pod["k8s.job.name"] = ref.Name // one job per run: pod level
				svc = cj
			} else {
				wl["k8s.job.name"] = ref.Name
			}
		default:
			kind, name = ref.Kind, ref.Name
		}
	}
	if v := p.Labels["app.kubernetes.io/name"]; v != "" {
		svc, svcAtPod = v, false
	}
	if v := p.Labels["app.kubernetes.io/instance"]; v != "" {
		svc, svcAtPod = v, false
	}
	if svcAtPod {
		pod["service.name"] = svc
	} else {
		wl["service.name"] = svc
	}
	return kind, name, wl, pod, true
}

var cronJobRun = regexp.MustCompile(`^(.*)-(\d{8})$`)

// agentCronJob is k8sattributes' CronJob rule: a Job named <cronjob>-<minute>
// whose scheduled minute is within a day of the pod's creation.
func agentCronJob(job string, created time.Time) (string, bool) {
	m := cronJobRun.FindStringSubmatch(job)
	if len(m) != 3 {
		return "", false
	}
	mins, _ := strconv.ParseInt(m[2], 10, 64)
	d := created.Unix()/60 - mins
	if d < 0 {
		d = -d
	}
	return m[1], d <= 24*60
}

func (c *Controller) process(key string) error {
	ns, name, _ := cache.SplitMetaNamespaceKey(key)
	p, err := c.pods.Pods(ns).Get(name)
	if err != nil {
		return nil // deleted: podDeleted closed it
	}
	if p.Spec.NodeName == "" {
		return nil // not scheduled: no telemetry yet
	}
	now := lane.Now()
	if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
		c.mu.Lock()
		if st, ok := c.podsByUID[p.UID]; ok {
			c.closePod(st, now)
		}
		c.mu.Unlock()
		return nil
	}
	c.mu.Lock()
	nodeRec, okN := c.nodes[p.Spec.NodeName]
	nsRec, okS := c.nss[p.Namespace]
	c.mu.Unlock()
	if !okN || !okS {
		return fmt.Errorf("node or namespace not cached yet")
	}
	kind, wname, wlAttrs, podAttrs, ok := c.workload(p)
	if !ok {
		if c.q.NumRequeues(key) < 10 {
			return fmt.Errorf("owner not cached yet")
		}
		c.Fallbacks.Add(1) // give up on the owner chain: the pod still gets a (different) id
	}
	podAttrs["k8s.pod.name"] = p.Name
	podAttrs["k8s.pod.uid"] = string(p.UID)
	podAttrs["k8s.pod.start_time"] = StartTime(p)
	for _, l := range c.cfg.PodLabels {
		if v, ok := p.Labels[l]; ok && v != "" {
			podAttrs["k8s.pod.label."+l] = v
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	clusterAttrs := c.cluster.Attrs
	wlKey := rid.AttrsKey("wl\x00"+c.clusterUID+"\x00"+p.Namespace+"\x00"+kind+"\x00"+wname, wlAttrs)
	podKey := rid.AttrsKey(fmt.Sprintf("pod\x00%d\x00%d\x00%d", wlKey, nodeRec.Key, nsRec.Key), podAttrs)
	st := c.podsByUID[p.UID]
	if st != nil && st.terminal {
		return nil
	}
	if st != nil && st.podKey == podKey {
		return nil // nothing the catalog holds changed (status updates, heartbeats)
	}
	created := lane.FromTime(p.CreationTimestamp.Time)
	event := created
	for _, cd := range p.Status.Conditions {
		if cd.Type == corev1.PodScheduled && cd.Status == corev1.ConditionTrue && lane.FromTime(cd.LastTransitionTime.Time) > event {
			event = lane.FromTime(cd.LastTransitionTime.Time)
		}
	}
	vf := created
	if st != nil { // a new version of a live pod (relabel, owner change)
		vf, event = now, now
		old := *st
		old.terminal = false
		c.closePodVersionOnly(&old, now)
	}
	w := c.wls[wlKey]
	if w == nil {
		w = &wlState{rec: lane.Record{Level: lane.LWorkload, Key: wlKey, Entity: rid.Key("wl", c.clusterUID, p.Namespace, kind, wname),
			ClusterKey: c.clusterKey, NsKey: nsRec.Key, Kind: kind, Name: wname, Attrs: wlAttrs, ValidFrom: vf, ObservedAt: now, EventAt: event, Writer: c.cfg.Writer}}
		c.wls[wlKey] = w
		c.emit(w.rec)
	}
	w.refs++
	nst := &podState{uid: p.UID, podKey: podKey, wlKey: wlKey, createdMs: created, res: map[uint64]lane.Record{}}
	nst.rec = lane.Record{Level: lane.LPod, Key: podKey, Entity: rid.Key("pod", string(p.UID)), ClusterKey: c.clusterKey,
		NodeKey: nodeRec.Entity, NsKey: nsRec.Key, WlKey: wlKey, Kind: kind, Name: p.Name, PodUID: string(p.UID),
		Attrs: podAttrs, ValidFrom: vf, ObservedAt: now, EventAt: event, Writer: c.cfg.Writer}
	c.emit(nst.rec)
	base := rid.Merge(clusterAttrs, nodeRec.Attrs, nsRec.Attrs, wlAttrs, podAttrs)
	for _, ct := range p.Spec.Containers {
		img, tag := Image(ct.Image)
		attrs := rid.Merge(base, map[string]string{"k8s.container.name": ct.Name, "container.image.name": img, "container.image.tag": tag})
		id := rid.ID(attrs)
		r := lane.Record{Level: lane.LResource, Key: id, Entity: rid.Key("res", string(p.UID), ct.Name), ClusterKey: c.clusterKey,
			NodeKey: nodeRec.Entity, NsKey: nsRec.Key, WlKey: wlKey, PodKey: podKey, Kind: kind, Name: p.Name, PodUID: string(p.UID),
			Container: ct.Name, Attrs: attrs, ValidFrom: vf, ObservedAt: now, EventAt: event, Writer: c.cfg.Writer}
		nst.res[id] = r
		c.emit(r)
	}
	c.podsByUID[p.UID] = nst
	return nil
}

func (c *Controller) closePodVersionOnly(st *podState, now lane.Time) {
	c.closePod(st, now)
}

// sync writes every open version as of now to a sync object; the aggregator
// closes the cluster's versions it holds open that are not in it.
func (c *Controller) sync(ctx context.Context) {
	c.mu.Lock()
	t := lane.Now()
	recs := make([]lane.Record, 0, 4*len(c.podsByUID))
	add := func(r lane.Record) {
		r.ObservedAt = t
		r.ClosedAt = 0
		recs = append(recs, r)
	}
	add(c.cluster)
	for _, r := range c.nodes {
		add(r)
	}
	for _, r := range c.nss {
		add(r)
	}
	for k, w := range c.wls {
		if w.refs <= 0 { // no live pod: the sync's absence closes it
			delete(c.wls, k)
			continue
		}
		add(w.rec)
	}
	for uid, st := range c.podsByUID {
		if st.terminal {
			delete(c.podsByUID, uid)
			continue
		}
		add(st.rec)
		for _, r := range st.res {
			add(r)
		}
	}
	c.mu.Unlock()
	sort.Slice(recs, func(i, j int) bool { return recs[i].Level < recs[j].Level })
	if err := c.out.Sync(ctx, t, recs); err != nil {
		log.Printf("sync: %v", err)
		return
	}
	log.Printf("sync at %s: %d open versions", strconv.FormatInt(int64(t), 10), len(recs))
}

// Stats for the metrics endpoint.
func (c *Controller) Stats() map[string]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	res := 0
	for _, st := range c.podsByUID {
		if !st.terminal {
			res += len(st.res)
		}
	}
	return map[string]int64{"pods": int64(len(c.podsByUID)), "resources": int64(res), "nodes": int64(len(c.nodes)),
		"workload_versions": int64(len(c.wls)), "events": c.Events.Load(), "emitted": c.Emitted.Load(),
		"requeues": c.Requeues.Load(), "fallbacks": c.Fallbacks.Load(), "queue": int64(c.q.Len()),
		"relists": max(c.Lists.Load()-informerCount, 0), "deleted_unknown": c.DeletedUnknown.Load()}
}
