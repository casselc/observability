// Command alertd evaluates alert rules through the query service, only on
// windows it labels complete, and delivers notices to Alertmanager (README).
//
//	alertd -config alertd.example.yaml
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/casselc/observability/otel-chdb/alerts/internal/config"
	"github.com/casselc/observability/otel-chdb/alerts/internal/rule"
)

func main() {
	path := flag.String("config", "", "configuration file (YAML or JSON)")
	check := flag.Bool("check", false, "load the configuration and rules, print them, and exit")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	c, err := config.Load(*path)
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(2)
	}
	rules, err := rule.Load(c.RulesFile)
	if err != nil {
		log.Error("rules", "err", err)
		os.Exit(2)
	}
	if *check {
		for _, r := range rules {
			log.Info("rule", "name", r.Name, "window", r.Window.D(), "every", r.Every.D(), "for", r.For.D(),
				"identity", r.Identity, "spec", r.Spec())
		}
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	r, err := config.Build(ctx, c, rules, log)
	if err != nil {
		log.Error("build", "err", err)
		os.Exit(2)
	}
	srv := &http.Server{Addr: c.Listen, Handler: r.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("listen", "err", err)
			stop()
		}
	}()
	log.Info("alertd", "listen", c.Listen, "rules", len(rules), "replica", c.Replica, "query", c.Query.URL)
	r.Run(ctx)
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(sctx)
}
