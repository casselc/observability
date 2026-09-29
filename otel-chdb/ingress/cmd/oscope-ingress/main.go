// Command oscope-ingress runs the Entra-authenticated OTLP/HTTP ingress
// (../../README.md, research/entra-ingress.md): one JSON config file, S3
// credentials from the SDK's default chain (environment, profile, IRSA or
// pod identity), TLS terminated here or by the load balancer in front.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/casselc/observability/otel-chdb/ingress"
	"github.com/casselc/observability/otel-chdb/parquetgo"
	"github.com/casselc/observability/otel-chdb/parquetgo/edge"
)

type config struct {
	Listen     string              `json:"listen"`   // ":4318"
	TLSCert    string              `json:"tls_cert"` // empty: plain HTTP behind a TLS-terminating proxy
	TLSKey     string              `json:"tls_key"`
	ProducerID string              `json:"producer_id"` // this replica (e.g. the pod name); unique per replica
	S3URL      string              `json:"s3_url"`      // s3://bucket/prefix or http(s)://endpoint/bucket/prefix
	S3Region   string              `json:"s3_region"`
	RoleARN    string              `json:"role_arn"`
	PathStyle  *bool               `json:"path_style"`
	HeartbeatS int                 `json:"heartbeat_s"` // default 30
	DrainS     int                 `json:"drain_s"`     // default 25 (under the pod's grace period)
	Entra      ingress.EntraConfig `json:"entra"`
	Policy     ingress.Policy      `json:"policy"`
	Limits     *ingress.Limits     `json:"limits"`
}

func main() {
	path := flag.String("config", "/etc/oscope-ingress/config.json", "config file")
	flag.Parse()
	b, err := os.ReadFile(*path)
	if err != nil {
		log.Fatal(err)
	}
	var c config
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields() // a misspelt allow-list is an error, not an empty list
	if err := dec.Decode(&c); err != nil {
		log.Fatalf("%s: %v", *path, err)
	}
	if c.Listen == "" {
		c.Listen = ":4318"
	}
	if c.HeartbeatS <= 0 {
		c.HeartbeatS = 30
	}
	if c.DrainS <= 0 {
		c.DrainS = 25
	}
	lim := ingress.DefaultLimits()
	if c.Limits != nil {
		lim = *c.Limits
	}
	e, err := edge.New(edge.Config{Cluster: c.Policy.Cluster, ProducerID: c.ProducerID,
		S3: parquetgo.Config{URL: c.S3URL, S3Region: c.S3Region, RoleARN: c.RoleARN, PathStyle: c.PathStyle}})
	if err != nil {
		log.Fatal(err)
	}
	v, err := ingress.NewEntraVerifier(c.Entra, nil)
	if err != nil {
		log.Fatal(err)
	}
	srv, err := ingress.NewServer(v, &c.Policy, lim, e, nil)
	if err != nil {
		log.Fatal(err)
	}
	srv.Logf = log.Printf
	beatCtx, stopBeats := context.WithCancel(context.Background())
	beatsDone := make(chan struct{})
	go func() {
		ingress.Heartbeats(beatCtx, e, time.Duration(c.HeartbeatS)*time.Second, log.Printf)
		close(beatsDone)
	}()
	hs := &http.Server{Addr: c.Listen, Handler: srv.Handler(), ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout: 2 * time.Minute, IdleTimeout: 2 * time.Minute}
	go func() {
		var err error
		if c.TLSCert != "" {
			err = hs.ListenAndServeTLS(c.TLSCert, c.TLSKey)
		} else {
			err = hs.ListenAndServe()
		}
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, os.Interrupt)
	<-sig
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(c.DrainS)*time.Second)
	defer cancel()
	n, err := srv.Drain(ctx, e, func() { stopBeats(); <-beatsDone })
	log.Printf("ingress stopped: %d lanes closed, err=%v", n, err)
	_ = hs.Shutdown(ctx)
}
