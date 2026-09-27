// aggregator: reads every entity lane in the bucket, in sequence order per
// lane, and inserts the records into the catalog database. A sync object
// also closes the cluster's versions that it no longer lists.
//
// Stateless apart from lane_progress (an optimization: records are
// idempotent, and each object is inserted with its key as the deduplication
// token, so a crash between insert and progress re-inserts nothing).
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/casselc/observability/otel-chdb/entities/controller/internal/ch"
	"github.com/casselc/observability/otel-chdb/entities/controller/internal/lane"
)

func main() {
	chURL := flag.String("ch", "http://localhost:18123", "ClickHouse HTTP")
	db := flag.String("db", "k8s_cat", "catalog database")
	bucket := flag.String("bucket", "k8s-entities", "bucket")
	prefix := flag.String("prefix", "lanes", "lane prefix")
	endpoint := flag.String("s3-endpoint", os.Getenv("S3_ENDPOINT"), "S3 endpoint")
	poll := flag.Duration("poll", 5*time.Second, "poll interval")
	margin := flag.Duration("margin", 60*time.Second, "a sync closes versions last observed before sync_at - margin")
	create := flag.String("create", "", "apply this DDL file first")
	once := flag.Bool("once", false, "one pass, then exit")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	c := ch.New(*chURL)
	if *create != "" {
		b, err := os.ReadFile(*create)
		if err != nil {
			log.Fatal(err)
		}
		if _, _, err := c.Exec("CREATE DATABASE IF NOT EXISTS "+*db, nil, nil, nil); err != nil {
			log.Fatal(err)
		}
		if err := c.Script(string(b), *db); err != nil {
			log.Fatal(err)
		}
	}
	s3c, err := lane.NewS3(ctx, *endpoint, "us-east-1")
	if err != nil {
		log.Fatal(err)
	}
	a := &agg{c: c, s3: s3c, db: *db, bucket: *bucket, prefix: *prefix, margin: *margin, progress: map[string]string{}}
	rows, err := c.Query(fmt.Sprintf("SELECT lane, argMax(last_object, updated_at) FROM %s.lane_progress GROUP BY lane", *db))
	if err != nil {
		log.Fatal(err)
	}
	for _, r := range rows {
		a.progress[r[0]] = r[1]
	}
	log.Printf("aggregator: %d lanes known", len(a.progress))
	for {
		start := time.Now()
		n, err := a.pass(ctx)
		if err != nil && ctx.Err() == nil {
			log.Printf("pass: %v", err)
		}
		if n > 0 {
			log.Printf("pass: %d objects in %v", n, time.Since(start).Round(time.Millisecond))
		}
		if *once || ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(*poll):
		}
	}
}

type agg struct {
	c        *ch.Client
	s3       *s3.Client
	db       string
	bucket   string
	prefix   string
	margin   time.Duration
	progress map[string]string
}

func (a *agg) list(ctx context.Context, prefix, delim, after string) (dirs []string, objs []objInfo, err error) {
	in := &s3.ListObjectsV2Input{Bucket: aws.String(a.bucket), Prefix: aws.String(prefix)}
	if delim != "" {
		in.Delimiter = aws.String(delim)
	}
	if after != "" {
		in.StartAfter = aws.String(after)
	}
	p := s3.NewListObjectsV2Paginator(a.s3, in)
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			return nil, nil, err
		}
		for _, cp := range out.CommonPrefixes {
			dirs = append(dirs, aws.ToString(cp.Prefix))
		}
		for _, o := range out.Contents {
			objs = append(objs, objInfo{key: aws.ToString(o.Key), size: aws.ToInt64(o.Size), mod: aws.ToTime(o.LastModified)})
		}
	}
	return dirs, objs, nil
}

type objInfo struct {
	key  string
	size int64
	mod  time.Time
}

func (a *agg) pass(ctx context.Context) (int, error) {
	clusters, _, err := a.list(ctx, a.prefix+"/", "/", "")
	if err != nil {
		return 0, err
	}
	n := 0
	for _, cl := range clusters {
		lanes, _, err := a.list(ctx, cl, "/", "")
		if err != nil {
			return n, err
		}
		for _, ln := range lanes {
			_, objs, err := a.list(ctx, ln, "", a.progress[ln])
			if err != nil {
				return n, err
			}
			sort.Slice(objs, func(i, j int) bool { return objs[i].key < objs[j].key })
			for _, o := range objs {
				if err := a.ingest(ctx, cl, ln, o); err != nil {
					return n, fmt.Errorf("%s: %w", o.key, err)
				}
				n++
			}
		}
	}
	return n, nil
}

func (a *agg) ingest(ctx context.Context, cluster, ln string, o objInfo) error {
	out, err := a.s3.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(a.bucket), Key: aws.String(o.key)})
	if err != nil {
		return err
	}
	body, err := io.ReadAll(out.Body)
	out.Body.Close()
	if err != nil {
		return err
	}
	zr, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return err
	}
	plain, err := io.ReadAll(zr)
	if err != nil {
		return err
	}
	recs := bytes.Count(plain, []byte{'\n'})
	set := map[string]string{"insert_deduplication_token": o.key, "deduplicate_blocks_in_dependent_materialized_views": "1",
		"max_insert_block_size": "10000000", "min_insert_block_size_rows": "0", "min_insert_block_size_bytes": "0"}
	if _, _, err := a.c.Exec(fmt.Sprintf("INSERT INTO %s.records (level, key, entity, cluster_key, node_key, ns_key, wl_key, pod_key, kind, name, pod_uid, container, attrs, valid_from, closed_at, observed_at, event_at, writer) FORMAT JSONEachRow", a.db),
		bytes.NewReader(body), set, map[string]string{"Content-Encoding": "gzip"}); err != nil {
		return err
	}
	kind := "delta"
	closed := int64(0)
	base := o.key[strings.LastIndexByte(o.key, '/')+1:]
	if parts := strings.Split(base, "."); len(parts) >= 3 && parts[1] == "sync" {
		kind = "sync"
		syncAt, _ := strconv.ParseInt(parts[2], 10, 64)
		var first struct {
			ClusterKey uint64 `json:"cluster_key"`
		}
		nl := bytes.IndexByte(plain, '\n')
		if nl > 0 {
			_ = json.Unmarshal(plain[:nl], &first)
		}
		if first.ClusterKey != 0 {
			cut := time.UnixMilli(syncAt).Add(-a.margin).UTC().Format("2006-01-02 15:04:05.000")
			at := time.UnixMilli(syncAt).UTC().Format("2006-01-02 15:04:05.000")
			q := fmt.Sprintf(`INSERT INTO %[1]s.records (level, key, entity, cluster_key, node_key, ns_key, wl_key, pod_key, kind, name, pod_uid, container, attrs, valid_from, closed_at, observed_at, event_at, writer)
SELECT level, key, entity, cluster_key, node_key, ns_key, wl_key, pod_key, kind, name, pod_uid, container, attrs, valid_from,
       toDateTime64('%[2]s', 3, 'UTC'), toDateTime64('%[2]s', 3, 'UTC'), toDateTime64('%[2]s', 3, 'UTC'), 'sync-close'
FROM %[1]s.versions_final
WHERE cluster_key = %[3]d AND closed_at = toDateTime64(0, 3, 'UTC') AND last_observed < toDateTime64('%[4]s', 3, 'UTC')`, a.db, at, first.ClusterKey, cut)
			_, s, err := a.c.Exec(q, nil, map[string]string{"insert_deduplication_token": o.key + "#close",
				"deduplicate_blocks_in_dependent_materialized_views": "1"}, nil)
			if err != nil {
				return fmt.Errorf("sync close: %w", err)
			}
			closed, _ = strconv.ParseInt(s.WrittenRows, 10, 64)
			if closed > 0 {
				closed /= 2 // written_rows counts the materialized view's rows as well
			}
			log.Printf("sync %s: closed %d versions", o.key, closed)
		}
	}
	cl := strings.TrimSuffix(strings.TrimPrefix(cluster, a.prefix+"/"), "/")
	put := o.mod.UTC().Format("2006-01-02 15:04:05.000")
	if _, _, err := a.c.Exec(fmt.Sprintf("INSERT INTO %s.ingest_log (object, cluster, kind, bytes, records, put_at, closed_by_sync) VALUES ('%s', '%s', '%s', %d, %d, '%s', %d)",
		a.db, o.key, cl, kind, len(body), recs, put, closed), nil, nil, nil); err != nil {
		return err
	}
	if _, _, err := a.c.Exec(fmt.Sprintf("INSERT INTO %s.lane_progress (lane, last_object, objects) VALUES ('%s', '%s', 1)", a.db, ln, o.key), nil, nil, nil); err != nil {
		return err
	}
	a.progress[ln] = o.key
	return nil
}
