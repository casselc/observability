// Command lakeindex is the lake indexer (FORMAT.md §7, D27): it follows
// every lane of the configured clusters' traces and logs and writes
// create-only index segments under each cluster's own prefix
// ({root}/{cluster}/_index/v1/…), so reading a cluster's index needs only
// that cluster's grant.
//
//	lakeindex -bucket otel -root edges -endpoint http://127.0.0.1:18333 [-clusters a,b] [-once]
//
// Credentials: LAKEIDX_S3_KEY / LAKEIDX_S3_SECRET, or the AWS SDK's default
// chain. The indexer needs GetObject and ListBucket on {root}/{cluster}/*
// and PutObject on {root}/{cluster}/_index/* (deploy/iam/README.md).
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/casselc/observability/otel-chdb/query/internal/lakeidx"
	"github.com/casselc/observability/otel-chdb/query/internal/store"
)

func main() {
	var (
		cfg      lakeidx.Config
		s3c      store.S3Config
		clusters = flag.String("clusters", "", "comma-separated clusters (default: every cluster under -root)")
		signals  = flag.String("signals", "traces,logs", "signals to index")
		interval = flag.Duration("interval", 30*time.Second, "pause between passes")
		once     = flag.Bool("once", false, "one pass, then exit (non-zero on error)")
		listen   = flag.String("metrics", "", "serve /metrics on this address (e.g. :9464)")
	)
	flag.StringVar(&s3c.Endpoint, "endpoint", os.Getenv("LAKEIDX_S3_ENDPOINT"), "S3 endpoint (empty: AWS)")
	flag.StringVar(&s3c.Bucket, "bucket", os.Getenv("LAKEIDX_S3_BUCKET"), "bucket")
	flag.StringVar(&s3c.Region, "region", "us-east-1", "region")
	flag.BoolVar(&s3c.PathStyle, "path-style", false, "path-style addressing (implied by -endpoint)")
	flag.StringVar(&cfg.Root, "root", "", "the lanes' {root} inside the bucket")
	flag.IntVar(&cfg.MaxSegmentObjects, "max-segment-objects", 0, "source objects per L0 segment (default 256)")
	flag.IntVar(&cfg.MergeAfterS, "merge-after", 0, "merge an hour's segments this many seconds after it ends (default 600)")
	flag.IntVar(&cfg.Build.MaxTerm, "max-term", 0, "longest indexed token in bytes (default 64)")
	flag.Float64Var(&cfg.Build.FreqCut, "freq-cut", 0, "a term in more than this fraction of a segment's row groups is stored as 'every row group' (default 0.5)")
	flag.Parse()
	s3c.AccessKey, s3c.SecretKey = os.Getenv("LAKEIDX_S3_KEY"), os.Getenv("LAKEIDX_S3_SECRET")
	if *clusters != "" {
		cfg.Clusters = strings.Split(*clusters, ",")
	}
	cfg.Signals = strings.Split(*signals, ",")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	st, err := store.NewS3(ctx, s3c)
	if err != nil {
		log.Fatal(err)
	}
	ix := lakeidx.New(cfg, st)
	var mu sync.Mutex
	var last []lakeidx.PassReport
	var lastErr error
	var stats lakeidx.Stats
	if *listen != "" {
		http.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			writeMetrics(w, stats, last, lastErr)
		})
		go func() { log.Println(http.ListenAndServe(*listen, nil)) }()
	}
	for {
		reps, err := ix.RunOnce(ctx)
		mu.Lock()
		last, lastErr, stats = reps, err, ix.Stats
		mu.Unlock()
		for _, r := range reps {
			if r.Indexed > 0 || len(r.Merged) > 0 || len(r.Unindexable) > 0 {
				log.Printf("%s/%s: listed %d, indexed %d into %d segments, merged %d, unindexable %d, indexed through %s",
					r.Cluster, r.Signal, r.Listed, r.Indexed, len(r.Segments), len(r.Merged), len(r.Unindexable),
					time.UnixMilli(r.IndexedThroughMs).UTC().Format(time.RFC3339))
			}
		}
		if err != nil {
			log.Printf("pass: %v", err)
		}
		if *once {
			if err != nil {
				os.Exit(1)
			}
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(*interval):
		}
	}
}

func writeMetrics(w http.ResponseWriter, s lakeidx.Stats, last []lakeidx.PassReport, lastErr error) {

	c := func(name, help string, v int64) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", name, help, name, name, v)
	}
	c("lakeidx_passes_total", "Indexer passes.", s.Passes)
	c("lakeidx_segments_written_total", "Segments written (L0 and merges; a 412 on a content-addressed key counts: the bytes are there).", s.Segments)
	c("lakeidx_merges_total", "Hour merges written.", s.Merges)
	c("lakeidx_objects_indexed_total", "Source objects indexed.", s.ObjectsIndexed)
	c("lakeidx_rows_indexed_total", "Rows indexed.", s.Rows)
	c("lakeidx_source_bytes_read_total", "Source object bytes read.", s.BytesRead)
	c("lakeidx_segment_bytes_total", "Segment bytes written.", s.SegmentBytes)
	c("lakeidx_put_conflicts_total", "Segment PUTs answered 412 (already written by us or a twin).", s.PutConflicts)
	c("lakeidx_errors_total", "Pass errors and unreadable segments.", s.Errors)
	fmt.Fprintf(w, "# HELP lakeidx_lag_seconds Now minus indexed_through, per cluster and signal (last pass).\n# TYPE lakeidx_lag_seconds gauge\n")
	for _, r := range last {
		fmt.Fprintf(w, "lakeidx_lag_seconds{cluster=%q,signal=%q} %.3f\n", r.Cluster, r.Signal, time.Since(time.UnixMilli(r.IndexedThroughMs)).Seconds())
	}
	up := 1
	if lastErr != nil {
		up = 0
	}
	fmt.Fprintf(w, "# HELP lakeidx_last_pass_ok 1 when the last pass had no error.\n# TYPE lakeidx_last_pass_ok gauge\nlakeidx_last_pass_ok %d\n", up)
}
