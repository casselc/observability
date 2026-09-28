// Package config is alertd's configuration file and how it is wired into a
// runner; the command and the integration test share it.
package config

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/casselc/observability/otel-chdb/alerts/internal/engine"
	"github.com/casselc/observability/otel-chdb/alerts/internal/notify"
	"github.com/casselc/observability/otel-chdb/alerts/internal/qclient"
	"github.com/casselc/observability/otel-chdb/alerts/internal/rule"
	"github.com/casselc/observability/otel-chdb/alerts/internal/runner"
	"github.com/casselc/observability/otel-chdb/alerts/internal/store"
)

// Config is the file's shape (YAML or JSON).
type Config struct {
	Listen     string                      `yaml:"listen"`
	RulesFile  string                      `yaml:"rules_file"`
	Query      QueryConfig                 `yaml:"query"`
	Identities map[string]qclient.Identity `yaml:"identities"`
	State      store.S3Config              `yaml:"state"`
	Sink       notify.Config               `yaml:"sink"`
	Eval       EvalConfig                  `yaml:"evaluation"`
	// Replica is this replica's name in the state's writer field
	// (default: the host name).
	Replica string `yaml:"replica"`
}

// QueryConfig is the query service.
type QueryConfig struct {
	URL      string `yaml:"url"`
	TimeoutS int    `yaml:"timeout_s"`
}

// EvalConfig is the evaluation policy.
type EvalConfig struct {
	TickS                int `yaml:"tick_s"`
	MaxWindowsPerTick    int `yaml:"max_windows_per_tick"`
	CannotEvaluateAfterS int `yaml:"cannot_evaluate_after_s"`
	FailuresToPage       int `yaml:"failures_to_page"`
	MaxBacklogS          int `yaml:"max_backlog_s"`
	RefreshS             int `yaml:"refresh_s"`
	HoldResolvedS        int `yaml:"hold_resolved_s"`
	BackoffBaseS         int `yaml:"backoff_base_s"`
	BackoffMaxS          int `yaml:"backoff_max_s"`
	Concurrency          int `yaml:"concurrency"`
	// LatenessS is the default rule lateness (default 30 s).
	LatenessS *int `yaml:"lateness_s"`
}

func env(dst *string, k string) {
	if v := os.Getenv(k); v != "" {
		*dst = v
	}
}

// Load reads path and applies the environment (ALR_*).
func Load(path string) (*Config, error) {
	c := &Config{Listen: ":18191"}
	if path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		dec := yaml.NewDecoder(strings.NewReader(string(b)))
		dec.KnownFields(true)
		if err := dec.Decode(c); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
	}
	env(&c.Listen, "ALR_LISTEN")
	env(&c.RulesFile, "ALR_RULES")
	env(&c.Query.URL, "ALR_QUERY_URL")
	env(&c.State.Endpoint, "ALR_S3_ENDPOINT")
	env(&c.State.Bucket, "ALR_S3_BUCKET")
	env(&c.State.Region, "ALR_S3_REGION")
	env(&c.State.Prefix, "ALR_S3_PREFIX")
	env(&c.State.AccessKey, "ALR_S3_KEY")
	env(&c.State.SecretKey, "ALR_S3_SECRET")
	env(&c.Sink.URL, "ALR_SINK_URL")
	env(&c.Replica, "ALR_REPLICA")
	if c.Replica == "" {
		c.Replica, _ = os.Hostname()
	}
	if c.State.Prefix == "" {
		c.State.Prefix = "alerts/state"
	}
	switch {
	case c.Query.URL == "":
		return nil, fmt.Errorf("query.url is required")
	case c.State.Bucket == "":
		return nil, fmt.Errorf("state.bucket is required: the evaluator keeps no state it cannot share with its other replica")
	case c.Sink.URL == "":
		return nil, fmt.Errorf("sink.url is required")
	case c.RulesFile == "":
		return nil, fmt.Errorf("rules_file is required")
	}
	return c, nil
}

func secs(n int) time.Duration { return time.Duration(n) * time.Second }

// Engine is the evaluation policy as the engine takes it. Refresh defaults
// to 60 s for Alertmanager (which forgets alerts not re-sent) and to 0 for
// the generic webhook (a 2xx ends the firing phase's delivery).
func (c *Config) Engine() engine.Config {
	e := engine.Config{CannotEvaluateAfter: secs(c.Eval.CannotEvaluateAfterS), FailuresToPage: c.Eval.FailuresToPage,
		MaxBacklog: secs(c.Eval.MaxBacklogS), Refresh: secs(c.Eval.RefreshS), HoldResolved: secs(c.Eval.HoldResolvedS),
		BackoffBase: secs(c.Eval.BackoffBaseS), BackoffMax: secs(c.Eval.BackoffMaxS)}
	if c.Eval.RefreshS == 0 && c.Sink.Format != "webhook" {
		e.Refresh = time.Minute
	}
	if c.Eval.HoldResolvedS == 0 && c.Sink.Format != "webhook" {
		e.HoldResolved = 45 * time.Second // > Alertmanager's default group_wait (30 s)
	}
	e.Defaults()
	return e
}

// Build wires a runner from the configuration and the rules.
func Build(ctx context.Context, c *Config, rules []*rule.Rule, log *slog.Logger) (*runner.Runner, error) {
	ids := map[string]qclient.TokenSource{}
	for name, id := range c.Identities {
		ts, err := qclient.NewTokenSource(id, nil)
		if err != nil {
			return nil, fmt.Errorf("identity %s: %w", name, err)
		}
		ids[name] = ts
	}
	lateness := 30 * time.Second
	if c.Eval.LatenessS != nil {
		lateness = secs(*c.Eval.LatenessS)
	}
	for _, r := range rules {
		if r.Lateness == 0 {
			r.Lateness = rule.Duration(lateness)
		}
		if ids[r.Identity] == nil {
			return nil, fmt.Errorf("rule %s: identity %q is not configured", r.Name, r.Identity)
		}
	}
	st, err := store.NewS3(ctx, c.State)
	if err != nil {
		return nil, err
	}
	timeout := secs(c.Query.TimeoutS)
	if timeout == 0 {
		timeout = 60 * time.Second
	}
	sink := notify.New(c.Sink)
	e := c.Engine()
	if e.Refresh > 0 {
		sink.EndsAfter = 4 * e.Refresh
	}
	r := &runner.Runner{Rules: rules, Store: st, Prefix: c.State.Prefix, Sink: sink, Engine: e, Writer: c.Replica,
		Eval: &qclient.Client{URL: c.Query.URL, HTTP: &http.Client{}, Timeout: timeout, Identity: ids},
		Tick: secs(c.Eval.TickS), MaxPerTick: c.Eval.MaxWindowsPerTick, Concurrency: c.Eval.Concurrency, Log: log}
	r.Init()
	return r, nil
}
