// Package notify sends notices to a webhook sink. The default payload is
// Alertmanager's API v2 (`POST /api/v2/alerts`, a JSON list of alerts);
// `webhook` sends a generic JSON envelope with the same fields.
//
// AMBIGUITY.md X6: only a 2xx is "delivered". No answer, a timeout, a
// reset or a 5xx is ambiguous (the sink may have taken it), and a 4xx is
// "not taken"; both are retried with the same dedup key, which the sink
// deduplicates: Alertmanager by the alert's label set (the episode is a
// label, `alert_episode`), a pager by `dedup_key`.
package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/casselc/observability/otel-chdb/alerts/internal/engine"
)

// Config is one sink.
type Config struct {
	URL    string `yaml:"url" json:"url"` // Alertmanager: http://alertmanager:9093/api/v2/alerts
	Format string `yaml:"format" json:"format"`
	// BearerTokenEnv names an environment variable holding a token for
	// the sink (optional).
	BearerTokenEnv string `yaml:"bearer_token_env" json:"bearer_token_env"`
	TimeoutS       int    `yaml:"timeout_s" json:"timeout_s"`
	// GeneratorURL is put on every alert (e.g. the evaluator's /state).
	GeneratorURL string `yaml:"generator_url" json:"generator_url"`
}

// Sink posts notices.
type Sink struct {
	cfg Config
	hc  *http.Client
	// EndsAfter: a firing alert's endsAt is now + this, so Alertmanager
	// resolves it by itself if the evaluator stops re-sending (e.g. it died;
	// its own absence pages through Prometheus: deploy/alerts).
	EndsAfter time.Duration
}

// New builds a sink.
func New(c Config) *Sink {
	if c.Format == "" {
		c.Format = "alertmanager"
	}
	t := time.Duration(c.TimeoutS) * time.Second
	if t == 0 {
		t = 10 * time.Second
	}
	return &Sink{cfg: c, hc: &http.Client{Timeout: t}, EndsAfter: 4 * time.Minute}
}

// Answer classifies one POST.
type Answer string

// Answers.
const (
	Acked     Answer = "acked"     // 2xx
	NoAnswer  Answer = "ambiguous" // transport error, timeout, reset: may or may not have been taken
	ServerErr Answer = "5xx"       // may or may not have been taken
	Rejected  Answer = "rejected"  // 4xx: not taken (a bad payload, auth); will not fix itself without a change
)

// Alert is Alertmanager's postable alert.
type Alert struct {
	Labels       map[string]string `json:"labels"`
	Annotations  map[string]string `json:"annotations"`
	StartsAt     string            `json:"startsAt"`
	EndsAt       string            `json:"endsAt,omitempty"`
	GeneratorURL string            `json:"generatorURL,omitempty"`
}

// Payload builds the alerts for sends at now.
func (s *Sink) Payload(sends []engine.Send, now time.Time) []Alert {
	out := make([]Alert, 0, len(sends))
	for _, x := range sends {
		n := x.Notice
		a := Alert{Labels: n.Labels, Annotations: n.Annotations, GeneratorURL: s.cfg.GeneratorURL,
			StartsAt: time.Unix(0, n.StartsNs).UTC().Format(time.RFC3339Nano)}
		if x.Phase == engine.Resolved {
			end := n.EndsNs
			if end < n.StartsNs {
				end = n.StartsNs
			}
			a.EndsAt = time.Unix(0, end).UTC().Format(time.RFC3339Nano)
		} else if s.EndsAfter > 0 {
			a.EndsAt = now.Add(s.EndsAfter).UTC().Format(time.RFC3339Nano)
		}
		out = append(out, a)
	}
	return out
}

type envelope struct {
	DedupKey string `json:"dedup_key"`
	Status   string `json:"status"` // firing | resolved
	Alert
}

// Send posts sends in one request.
func (s *Sink) Send(ctx context.Context, sends []engine.Send, now time.Time) (Answer, string) {
	if len(sends) == 0 {
		return Acked, ""
	}
	alerts := s.Payload(sends, now)
	var body []byte
	if s.cfg.Format == "webhook" {
		env := make([]envelope, len(alerts))
		for i, a := range alerts {
			env[i] = envelope{DedupKey: sends[i].Notice.Key, Status: sends[i].Phase, Alert: a}
		}
		body, _ = json.Marshal(map[string]any{"version": "1", "alerts": env})
	} else {
		body, _ = json.Marshal(alerts)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.URL, bytes.NewReader(body))
	if err != nil {
		return Rejected, err.Error()
	}
	req.Header.Set("Content-Type", "application/json")
	if s.cfg.BearerTokenEnv != "" {
		req.Header.Set("Authorization", "Bearer "+os.Getenv(s.cfg.BearerTokenEnv))
	}
	resp, err := s.hc.Do(req)
	if err != nil {
		return NoAnswer, err.Error()
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return Acked, ""
	case resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		return ServerErr, fmt.Sprintf("HTTP %d %.200s", resp.StatusCode, b)
	}
	return Rejected, fmt.Sprintf("HTTP %d %.200s", resp.StatusCode, b)
}
