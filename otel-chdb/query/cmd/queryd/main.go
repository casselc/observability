// Command queryd is the query service: OIDC-authenticated, audited SQL on
// central (POST /v1/query) and presigned lake plans (POST /v1/plan). See
// ../../README.md.
//
//	queryd -config queryd.json
//
// Every setting can come from the file; secrets and endpoints can be set or
// overridden in the environment (QS_* below; AWS credentials through the SDK's
// default chain unless QS_S3_KEY / QS_S3_SECRET are set).
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/casselc/observability/otel-chdb/query/internal/app"
	"github.com/casselc/observability/otel-chdb/query/internal/audit"
	"github.com/casselc/observability/otel-chdb/query/internal/auth"
)

func main() {
	path := flag.String("config", os.Getenv("QS_CONFIG"), "config file (JSON)")
	flag.Parse()
	c, err := app.Load(*path)
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	v, err := auth.NewVerifier(c.OIDC, nil)
	if err != nil {
		log.Fatal(err)
	}
	sink, err := audit.OpenFile(c.Audit.Path, c.Audit.Fsync)
	if err != nil {
		log.Fatal(err)
	}
	defer sink.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	s, err := app.Build(ctx, c, v, sink)
	if err != nil {
		log.Fatal(err)
	}
	srv := &http.Server{Addr: c.Listen, Handler: s.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		sh, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(sh)
	}()
	log.Printf("queryd on %s (lake plan: %v)", c.Listen, c.LakeEnabled)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
