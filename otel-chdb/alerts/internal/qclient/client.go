// Package qclient runs a rule's statement through the query service
// (otel-chdb/query, D22) as the evaluator's own service identity, and turns
// the answer into an engine.Result.
//
// The query service's label is the gate (STPA R-S3): a result is Complete
// only when the service says `completeness: complete`, `partial: false`,
// the watermark's status is `ok`, the window it applied is the one asked
// for, and complete_through is at or after the window's end. Every other
// answer (partial, unknown, a 4xx, a 5xx, no answer, an unreadable body)
// is something other than Complete, and none of them is ever read as "no
// rows".
package qclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/casselc/observability/otel-chdb/alerts/internal/engine"
	"github.com/casselc/observability/otel-chdb/alerts/internal/rule"
)

// Client talks to one query service.
type Client struct {
	URL      string // e.g. http://query:18190
	HTTP     *http.Client
	Timeout  time.Duration // per evaluation
	Identity map[string]TokenSource
}

type lane struct {
	Lane string  `json:"lane"`
	LagS float64 `json:"lag_s"`
}

// Response is the part of /v1/query's answer the evaluator reads.
type Response struct {
	RequestID         string  `json:"request_id"`
	Source            string  `json:"source"`
	CompleteThroughNs *uint64 `json:"complete_through_ns"`
	Completeness      string  `json:"completeness"`
	Partial           bool    `json:"partial"`
	Watermark         struct {
		Status string   `json:"status"`
		AgeS   *float64 `json:"age_s"`
		LagS   *float64 `json:"lag_s"`
		Hold   []lane   `json:"holding"`
		Stale  []lane   `json:"stale_lanes"`
		Error  string   `json:"error"`
	} `json:"watermark"`
	Query struct {
		Window *struct {
			FromNs int64 `json:"from_ns"`
			ToNs   int64 `json:"to_ns"`
		} `json:"window"`
	} `json:"query"`
	Result struct {
		Data []map[string]any `json:"data"`
		Rows int              `json:"rows"`
	} `json:"result"`
	Error  string `json:"error"`
	Detail string `json:"detail"`
}

// Evaluate runs r's statement over w.
func (c *Client) Evaluate(ctx context.Context, r *rule.Rule, w rule.Window) engine.Result {
	ts := c.Identity[r.Identity]
	if ts == nil {
		return engine.Result{Outcome: engine.Refused, Err: "no identity " + r.Identity}
	}
	if c.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.Timeout)
		defer cancel()
	}
	res, status := c.do(ctx, ts, r, w)
	if status == http.StatusUnauthorized { // an expired or rotated token: one retry with a fresh one
		ts.Invalidate()
		res, _ = c.do(ctx, ts, r, w)
	}
	return res
}

func (c *Client) do(ctx context.Context, ts TokenSource, r *rule.Rule, w rule.Window) (engine.Result, int) {
	tok, err := ts.Token(ctx)
	if err != nil {
		return engine.Result{Outcome: failure(ctx, engine.Failed), Err: "token: " + err.Error()}, 0
	}
	body, _ := json.Marshal(map[string]any{"sql": r.SQL, "window": map[string]int64{"from": w.FromNs, "to": w.ToNs}})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.URL, "/")+"/v1/query", bytes.NewReader(body))
	if err != nil {
		return engine.Result{Outcome: engine.Refused, Err: err.Error()}, 0
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	hc := c.HTTP
	if hc == nil {
		hc = http.DefaultClient
	}
	resp, err := hc.Do(req)
	if err != nil {
		return engine.Result{Outcome: failure(ctx, engine.Failed), Err: err.Error()}, 0
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return engine.Result{Outcome: failure(ctx, engine.Failed), Err: "reading the answer: " + err.Error()}, resp.StatusCode
	}
	return Interpret(resp.StatusCode, b, r, w), resp.StatusCode
}

func failure(ctx context.Context, o engine.Outcome) engine.Outcome {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return engine.Timeout
	}
	return o
}

// Interpret turns an HTTP status and body into a Result (pure: the unit
// and property tests drive it directly).
func Interpret(status int, body []byte, r *rule.Rule, w rule.Window) engine.Result {
	var q Response
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	perr := dec.Decode(&q)
	res := engine.Result{RequestID: q.RequestID}
	switch {
	case status == http.StatusTooManyRequests || status == http.StatusRequestTimeout:
		res.Outcome, res.Err = engine.Failed, fmt.Sprintf("HTTP %d %s", status, q.Error)
		return res
	case status >= 400 && status < 500 && status != 422:
		// 401/403/400: the identity or the statement is refused. 422 is a
		// pinned limit: the statement failed (and may pass on a smaller
		// window of data), counted as a failure below.
		res.Outcome, res.Err = engine.Refused, fmt.Sprintf("HTTP %d %s %s", status, q.Error, q.Detail)
		return res
	case status != http.StatusOK:
		res.Outcome, res.Err = engine.Failed, fmt.Sprintf("HTTP %d %s %s", status, q.Error, q.Detail)
		return res
	case perr != nil:
		res.Outcome, res.Err = engine.Failed, "unreadable answer: "+perr.Error()
		return res
	}
	res.WatermarkStatus = q.Watermark.Status
	if q.Watermark.AgeS != nil {
		res.WatermarkAgeS = *q.Watermark.AgeS
	}
	if q.CompleteThroughNs != nil {
		res.CompleteThroughNs = int64(*q.CompleteThroughNs)
	}
	for _, l := range q.Watermark.Hold {
		res.Holding = append(res.Holding, engine.Lane(l))
	}
	for _, l := range q.Watermark.Stale {
		res.Stale = append(res.Stale, engine.Lane(l))
	}
	complete := q.Completeness == "complete" && !q.Partial && q.Watermark.Status == "ok" &&
		q.CompleteThroughNs != nil && int64(*q.CompleteThroughNs) >= w.ToNs+int64(r.Lateness) &&
		q.Query.Window != nil && q.Query.Window.FromNs == w.FromNs && q.Query.Window.ToNs == w.ToNs
	switch {
	case complete:
	case q.Completeness == "complete" && q.Watermark.Status == "ok" && !q.Partial && q.CompleteThroughNs != nil &&
		int64(*q.CompleteThroughNs) >= w.ToNs && int64(*q.CompleteThroughNs) < w.ToNs+int64(r.Lateness):
		res.Outcome = engine.Partial // complete by the label, but rows may still arrive within the lateness allowance
		return res
	case q.Completeness == "partial" && q.Watermark.Status == "ok":
		res.Outcome = engine.Partial
		return res
	case q.Completeness == "complete": // labelled complete, but not for this window or not ok: never trust it
		res.Outcome, res.Err = engine.Unknown, "the service labelled a window complete that the evaluator cannot confirm"
		return res
	default:
		res.Outcome = engine.Unknown
		if q.Watermark.Error != "" {
			res.Err = q.Watermark.Error
		}
		return res
	}
	if q.Result.Data == nil && q.Result.Rows != 0 {
		res.Outcome, res.Err = engine.BadResult, "rows but no data"
		return res
	}
	rows, err := r.Rows(q.Result.Data)
	if err != nil {
		res.Outcome, res.Err = engine.BadResult, err.Error()
		return res
	}
	res.Outcome, res.Rows = engine.Complete, rows
	return res
}
