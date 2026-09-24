// gen publishes synthetic trace batches through the chdb exporter (its public
// factory), both as s3_plain_rewritable MergeTree tables and as Parquet, so a
// central ClickHouse server can be benchmarked ingesting either form.
//
// Timestamps and trace ids are shifted per batch, so batches are time-ordered
// (as one producer's batches are) and ids are unique.
//
// With -control ADDR it stays up after the initial pushes and serves:
//
//	/push?n=N      push N more batches
//	/optimize      OPTIMIZE TABLE ... FINAL on the main table (writer side)
//	/sql?q=...     run SQL on the writer's engine (TSV)
//	/quit          shut down (seals the generation)
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/chdb-io/chdb-go/v2/chdb"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/chucklehead-dev/oscope/spike/otel-chdb/chdbexporter"
	"github.com/chucklehead-dev/oscope/spike/otel-chdb/chdbexporter/testgen"
)

func main() {
	path := flag.String("path", "", "chDB data dir (fresh)")
	root := flag.String("s3", "http://127.0.0.1:18333/otel", "S3 root")
	key := flag.String("key", "otel", "")
	secret := flag.String("secret", "otelsecret", "")
	region := flag.String("region", "ce", "")
	producer := flag.String("producer", "p0", "")
	batches := flag.Int("batches", 10, "initial batches")
	spans := flag.Int("spans", 8000, "spans per batch")
	gen := flag.Duration("gen", 24*time.Hour, "generation period")
	sealOpt := flag.Bool("seal-optimize", false, "OPTIMIZE FINAL at seal")
	sleep := flag.Duration("sleep", 0, "pause between pushes")
	control := flag.String("control", "", "serve control HTTP on this addr and stay up")
	parquet := flag.Bool("parquet", true, "also publish Parquet")
	tables := flag.Bool("tables", true, "publish s3_plain_rewritable tables")
	flag.Parse()

	f := chdbexporter.NewFactory()
	c := f.CreateDefaultConfig().(*chdbexporter.Config)
	c.Path = *path
	c.Database = "otel"
	c.QueueSettings = configoptional.None[exporterhelper.QueueBatchConfig]()
	c.Producer.ID, c.Producer.Region = *producer, *region
	if *tables {
		c.ObjectStorage.Endpoint, c.ObjectStorage.AccessKeyID, c.ObjectStorage.SecretAccessKey = *root, *key, configopaque.String(*secret)
		c.ObjectStorage.Generation = *gen
		c.ObjectStorage.SealOptimize = *sealOpt
		c.ObjectStorage.LocalRetention = time.Hour
	} else {
		c.StoreTables = false
	}
	if *parquet {
		c.Parquet.URL, c.Parquet.AccessKeyID, c.Parquet.SecretAccessKey = *root+"/parquet", *key, configopaque.String(*secret)
	}
	ctx := context.Background()
	e, err := f.CreateTraces(ctx, exportertest.NewNopSettings(f.Type()), c)
	if err != nil {
		log.Fatal(err)
	}
	if err := e.Start(ctx, componenttest.NewNopHost()); err != nil {
		log.Fatal(err)
	}
	base := testgen.Traces(*spans)
	var mu sync.Mutex
	next := 0
	push := func(n int) error {
		mu.Lock()
		defer mu.Unlock()
		for i := 0; i < n; i++ {
			td := ptrace.NewTraces()
			base.CopyTo(td)
			shift(td, next, *spans)
			t0 := time.Now()
			if err := e.ConsumeTraces(ctx, td); err != nil {
				return err
			}
			log.Printf("pushed batch #%d (%d spans) in %v", next+1, *spans, time.Since(t0).Round(time.Millisecond))
			next++
			if *sleep > 0 {
				time.Sleep(*sleep)
			}
		}
		return nil
	}
	if err := push(*batches); err != nil {
		log.Fatal(err)
	}
	if *control != "" {
		sess, err := chdb.NewSession(*path)
		if err != nil {
			log.Fatal(err)
		}
		q := func(sql string) (string, error) {
			r, err := sess.Query(sql, "TSV")
			if err != nil {
				return "", err
			}
			defer r.Free()
			return r.String(), nil
		}
		done := make(chan struct{})
		mux := http.NewServeMux()
		mux.HandleFunc("/push", func(w http.ResponseWriter, r *http.Request) {
			var n int
			fmt.Sscan(r.URL.Query().Get("n"), &n)
			if err := push(n); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			fmt.Fprintln(w, "ok")
		})
		mux.HandleFunc("/optimize", func(w http.ResponseWriter, r *http.Request) {
			tbl, err := q("SELECT name FROM system.tables WHERE database = 'otel' AND name LIKE 'otel_traces_g%' AND name NOT LIKE '%\\_trace\\_id\\_ts%' AND name NOT LIKE '%\\_in%' ORDER BY name DESC LIMIT 1")
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			out, err := q(fmt.Sprintf("OPTIMIZE TABLE otel.%s FINAL", trim(tbl)))
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			fmt.Fprintln(w, "ok", out)
		})
		mux.HandleFunc("/sql", func(w http.ResponseWriter, r *http.Request) {
			out, err := q(r.URL.Query().Get("q"))
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			fmt.Fprint(w, out)
		})
		mux.HandleFunc("/quit", func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintln(w, "bye")
			close(done)
		})
		srv := &http.Server{Addr: *control, Handler: mux}
		go srv.ListenAndServe()
		log.Printf("control on %s", *control)
		<-done
		srv.Close()
	}
	if err := e.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}
	log.Printf("done: %d batches", next)
	os.Exit(0)
}

func trim(s string) string {
	for len(s) > 0 && (s[len(s)-1] == '\n' || s[len(s)-1] == ' ') {
		s = s[:len(s)-1]
	}
	return s
}

// shift moves batch b's spans after batch b-1's in time and makes its ids unique.
func shift(td ptrace.Traces, b, spans int) {
	off := time.Duration(b*spans) * time.Millisecond
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		sss := rss.At(i).ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			ss := sss.At(j).Spans()
			for k := 0; k < ss.Len(); k++ {
				s := ss.At(k)
				s.SetStartTimestamp(pcommon.Timestamp(uint64(s.StartTimestamp()) + uint64(off)))
				s.SetEndTimestamp(pcommon.Timestamp(uint64(s.EndTimestamp()) + uint64(off)))
				evs := s.Events()
				for x := 0; x < evs.Len(); x++ {
					evs.At(x).SetTimestamp(pcommon.Timestamp(uint64(evs.At(x).Timestamp()) + uint64(off)))
				}
				tid := s.TraceID()
				tid[14], tid[15] = byte(b>>8), byte(b)
				s.SetTraceID(tid)
				sid := s.SpanID()
				sid[6], sid[7] = byte(b>>8), byte(b)
				s.SetSpanID(sid)
				if !s.ParentSpanID().IsEmpty() {
					p := s.ParentSpanID()
					p[6], p[7] = byte(b>>8), byte(b)
					s.SetParentSpanID(p)
				}
			}
		}
	}
}
