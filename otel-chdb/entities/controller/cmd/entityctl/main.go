// entityctl: the per-cluster entity controller. Watches one cluster and
// writes versioned entity records to its S3 lane.
//
//	entityctl --kubeconfig k.yaml --cluster prod-use1-00 --bucket k8s-entities \
//	  --static cloud.provider=aws,cloud.region=us-east-1 --s3-endpoint http://localhost:18333
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/casselc/observability/otel-chdb/entities/controller/internal/ctrl"
	"github.com/casselc/observability/otel-chdb/entities/controller/internal/lane"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// DefaultLabels: the pod labels both the agents' k8sattributes and the
// controller copy as k8s.pod.label.<key> (the covered label list).
const DefaultLabels = "app.kubernetes.io/name,app.kubernetes.io/instance,app.kubernetes.io/version,app.kubernetes.io/component," +
	"app.kubernetes.io/part-of,app.kubernetes.io/managed-by,team,pod-template-hash,controller-revision-hash," +
	"statefulset.kubernetes.io/pod-name,batch.kubernetes.io/job-name,debug"

func main() {
	kubeconfig := flag.String("kubeconfig", "", "kubeconfig (empty: in-cluster)")
	cluster := flag.String("cluster", "", "k8s.cluster.name")
	static := flag.String("static", "", "static cluster attributes k=v,k=v (cloud.*, deployment.environment.name)")
	labels := flag.String("labels", DefaultLabels, "covered pod label keys")
	bucket := flag.String("bucket", "k8s-entities", "S3 bucket")
	prefix := flag.String("prefix", "lanes", "lane prefix")
	endpoint := flag.String("s3-endpoint", os.Getenv("S3_ENDPOINT"), "S3 endpoint (empty: AWS)")
	region := flag.String("region", "us-east-1", "S3 region")
	flush := flag.Duration("flush", 5*time.Second, "delta object interval")
	putTimeout := flag.Duration("put-timeout", 20*time.Second, "per-attempt S3 PUT / HEAD timeout")
	resync := flag.Duration("resync", 10*time.Minute, "full-state sync interval")
	transform := flag.Bool("transform", true, "strip cached objects")
	workers := flag.Int("workers", 2, "pod workers")
	instance := flag.String("instance", "", "instance id (default hostname)")
	metrics := flag.String("metrics", "", "listen address for /stats")
	qps := flag.Float64("qps", 20, "client QPS")
	flag.Parse()
	if *cluster == "" {
		log.Fatal("--cluster is required")
	}
	st := map[string]string{}
	for _, kv := range strings.Split(*static, ",") {
		if k, v, ok := strings.Cut(kv, "="); ok {
			st[k] = v
		}
	}
	var rc *rest.Config
	var err error
	if *kubeconfig == "" {
		rc, err = rest.InClusterConfig()
	} else {
		rc, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
	}
	if err != nil {
		log.Fatal(err)
	}
	rc.QPS, rc.Burst = float32(*qps), int(*qps*2)
	cs := kubernetes.NewForConfigOrDie(rc)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	s3c, err := lane.NewS3(ctx, *endpoint, *region)
	if err != nil {
		log.Fatal(err)
	}
	if *instance == "" {
		*instance, _ = os.Hostname()
	}
	epoch := time.Now().UnixMilli()
	writer := fmt.Sprintf("%d-%s", epoch, *instance)
	w := &lane.Writer{S3: s3c, Bucket: *bucket, PutTimeout: *putTimeout, Lane: fmt.Sprintf("%s/%s/%s", *prefix, *cluster, writer)}
	c := ctrl.New(ctrl.Config{ClusterName: *cluster, Static: st, PodLabels: strings.Split(*labels, ","), Writer: writer,
		Resync: *resync, Transform: *transform}, cs, w)

	stats := func() map[string]any {
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		m := map[string]any{"t": time.Now().UTC().Format(time.RFC3339), "cluster": *cluster, "synced": c.Synced.Load(),
			"objects": w.Objects.Load(), "bytes": w.Bytes.Load(), "records": w.Records.Load(), "retries": w.Retries.Load(),
			"pending": w.Pending(), "heap_inuse": ms.HeapInuse, "sys": ms.Sys, "goroutines": runtime.NumGoroutine()}
		for k, v := range c.Stats() {
			m[k] = v
		}
		return m
	}
	if *metrics != "" {
		http.HandleFunc("/stats", func(rw http.ResponseWriter, _ *http.Request) { _ = json.NewEncoder(rw).Encode(stats()) })
		go func() { log.Println(http.ListenAndServe(*metrics, nil)) }()
	}
	go func() {
		t := time.NewTicker(30 * time.Second)
		for range t.C {
			b, _ := json.Marshal(stats())
			log.Printf("stats %s", b)
		}
	}()
	wctx, wcancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.Run(wctx, *flush); close(done) }()
	log.Printf("entityctl %s lane s3://%s/%s", *cluster, *bucket, w.Lane)
	if err := c.Run(ctx, *workers); err != nil {
		log.Printf("run: %v", err)
	}
	wcancel() // final flush of what was buffered
	<-done
	b, _ := json.Marshal(stats())
	log.Printf("exit stats %s", b)
}
