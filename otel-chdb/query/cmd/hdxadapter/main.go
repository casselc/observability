// hdxadapter: HyperDX's ClickHouse connection, served by the query service.
//
//	hdxadapter -config hdxadapter.json
//
// HyperDX's connection host points here. Every statement goes to the query
// service's POST /v1/query with the caller's own bearer token (README.md
// "HyperDX adapter"); the adapter holds no ClickHouse credentials.
// Environment overrides: HDXA_LISTEN, HDXA_QUERY_URL, HDXA_TOKEN_HEADER.
package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/casselc/observability/otel-chdb/query/internal/hdxadapter"
)

func main() {
	path := flag.String("config", "", "configuration (JSON)")
	flag.Parse()
	cfg := hdxadapter.Config{Listen: ":18191"}
	if *path != "" {
		b, err := os.ReadFile(*path)
		if err != nil {
			log.Fatal(err)
		}
		dec := json.NewDecoder(strings.NewReader(string(b)))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&cfg); err != nil {
			log.Fatalf("%s: %v", *path, err)
		}
	}
	for k, dst := range map[string]*string{"HDXA_LISTEN": &cfg.Listen, "HDXA_QUERY_URL": &cfg.QueryURL, "HDXA_TOKEN_HEADER": &cfg.TokenHeader} {
		if v := os.Getenv(k); v != "" {
			*dst = v
		}
	}
	if cfg.QueryURL == "" {
		log.Fatal("query_url is required (the service's POST /v1/query)")
	}
	a := hdxadapter.New(cfg, nil)
	log.Printf("hdxadapter %s -> %s", cfg.Listen, cfg.QueryURL)
	srv := &http.Server{Addr: cfg.Listen, Handler: a, ReadHeaderTimeout: 30 * time.Second}
	log.Fatal(srv.ListenAndServe())
}
