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
	"strconv"
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
	// MaxLatenessS is the service's bridge from custody time
	// (complete_through) to event time (the window); absent from a service
	// that predates it, which is then never trusted with "complete".
	MaxLatenessS *float64 `json:"max_lateness_s"`
	Completeness string   `json:"completeness"`
	Partial      bool     `json:"partial"`
	Watermark    struct {
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
	// The basis (D30): the token the answer was computed at, and its
	// readable bounds.
	Basis     *string `json:"basis"`
	AtBasis   bool    `json:"at_basis"`
	BasisInfo *struct {
		Clusters []struct {
			Cluster          string      `json:"cluster"`
			ReceivedBeforeNs json.Number `json:"received_before_ns"`
		} `json:"clusters"`
	} `json:"basis_info"`
	Delta *struct {
		Status string       `json:"status"`
		Rows   *json.Number `json:"rows"`
		Error  string       `json:"error"`
	} `json:"delta"`
}

// bounds reads basis_info's bounds.
func (q *Response) bounds() map[string]uint64 {
	if q.BasisInfo == nil {
		return nil
	}
	out := map[string]uint64{}
	for _, c := range q.BasisInfo.Clusters {
		v, err := strconv.ParseUint(c.ReceivedBeforeNs.String(), 10, 64)
		if err != nil {
			return nil
		}
		out[c.Cluster] = v
	}
	return out
}

// Evaluate runs r's statement over w at the latest basis (D30): the answer
// is then exactly the rows received before the basis's bounds, and the
// same statement at the same basis answers the same later (an audit, a
// replay, the late-data delta).
func (c *Client) Evaluate(ctx context.Context, r *rule.Rule, w rule.Window) engine.Result {
	return c.EvaluateAt(ctx, r, w, "latest")
}

// EvaluateAt runs r's statement over w at basis (a token or "latest").
func (c *Client) EvaluateAt(ctx context.Context, r *rule.Rule, w rule.Window, basis string) engine.Result {
	status, b, res := c.call(ctx, r, w, map[string]any{"basis": basis})
	if res != nil {
		return *res
	}
	return Interpret(status, b, r, w)
}

// Delta counts the rows of r's scope in w received between two bases:
// from <= received_at < to (a token, or "latest"). The service checks both
// against the rule's scope; from above to is refused (basis_regressed), and
// comes back as Partial: try again later.
func (c *Client) Delta(ctx context.Context, r *rule.Rule, w rule.Window, from, to string) engine.Delta {
	status, b, res := c.call(ctx, r, w, map[string]any{"basis": to, "basis_from": from})
	if res != nil {
		return engine.Delta{Outcome: res.Outcome, Err: res.Err}
	}
	return InterpretDelta(status, b)
}

// InterpretDelta reads a delta's answer (pure).
func InterpretDelta(status int, body []byte) engine.Delta {
	var q Response
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	perr := dec.Decode(&q)
	d := engine.Delta{Reason: q.Error}
	switch {
	case status == http.StatusConflict:
		d.Outcome, d.Err = engine.Partial, fmt.Sprintf("HTTP 409 %s %s", q.Error, q.Detail)
	case status == http.StatusTooManyRequests || status == http.StatusRequestTimeout || status >= 500:
		d.Outcome, d.Err = engine.Failed, fmt.Sprintf("HTTP %d %s %s", status, q.Error, q.Detail)
	case status != http.StatusOK:
		d.Outcome, d.Err = engine.Refused, fmt.Sprintf("HTTP %d %s %s", status, q.Error, q.Detail)
	case perr != nil:
		d.Outcome, d.Err = engine.Failed, "unreadable answer: "+perr.Error()
	case !q.AtBasis || q.Basis == nil || q.Delta == nil || q.Delta.Status != "counted" || q.Delta.Rows == nil:
		// a service that ignored basis_from, or could not count: never
		// read as "no late rows"
		d.Outcome, d.Err = engine.Failed, "the answer has no counted delta"
	default:
		n, err := q.Delta.Rows.Int64()
		c := q.bounds()
		if err != nil || n < 0 || c == nil {
			d.Outcome, d.Err = engine.Failed, "unreadable delta"
			break
		}
		d.Outcome, d.Rows, d.Basis, d.C = engine.Complete, n, *q.Basis, c
	}
	return d
}

// call posts r's statement over w with extra fields, retrying once with a
// fresh token after a 401; res is set when there is no HTTP answer.
func (c *Client) call(ctx context.Context, r *rule.Rule, w rule.Window, extra map[string]any) (int, []byte, *engine.Result) {
	ts := c.Identity[r.Identity]
	if ts == nil {
		return 0, nil, &engine.Result{Outcome: engine.Refused, Err: "no identity " + r.Identity}
	}
	if c.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.Timeout)
		defer cancel()
	}
	status, b, res := c.do(ctx, ts, r, w, extra)
	if status == http.StatusUnauthorized { // an expired or rotated token: one retry with a fresh one
		ts.Invalidate()
		status, b, res = c.do(ctx, ts, r, w, extra)
	}
	return status, b, res
}

func (c *Client) do(ctx context.Context, ts TokenSource, r *rule.Rule, w rule.Window, extra map[string]any) (int, []byte, *engine.Result) {
	tok, err := ts.Token(ctx)
	if err != nil {
		return 0, nil, &engine.Result{Outcome: failure(ctx, engine.Failed), Err: "token: " + err.Error()}
	}
	q := map[string]any{"sql": r.SQL, "window": map[string]int64{"from": w.FromNs, "to": w.ToNs}}
	if len(r.Clusters) > 0 {
		q["clusters"] = r.Clusters
	}
	for k, v := range extra {
		q[k] = v
	}
	body, _ := json.Marshal(q)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.URL, "/")+"/v1/query", bytes.NewReader(body))
	if err != nil {
		return 0, nil, &engine.Result{Outcome: engine.Refused, Err: err.Error()}
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	hc := c.HTTP
	if hc == nil {
		hc = http.DefaultClient
	}
	resp, err := hc.Do(req)
	if err != nil {
		return 0, nil, &engine.Result{Outcome: failure(ctx, engine.Failed), Err: err.Error()}
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return resp.StatusCode, nil, &engine.Result{Outcome: failure(ctx, engine.Failed), Err: "reading the answer: " + err.Error()}
	}
	return resp.StatusCode, b, nil
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
	if q.AtBasis && q.Basis != nil {
		res.Basis, res.BasisC = *q.Basis, q.bounds()
		if res.BasisC == nil {
			res.Basis = ""
		}
	}
	switch {
	case status == http.StatusServiceUnavailable && q.Error == "basis_unverifiable":
		// no watermark to mint a basis from: completeness is unknown (not
		// a failure of the evaluation), as without a basis
		res.Outcome, res.Err = engine.Unknown, q.Detail
		return res
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
	// The bound, in custody time: the window's end + the service's
	// max_lateness (the label's own bridge to event time) + the rule's
	// lateness (the evaluator's extra margin on top of it). The label is
	// "complete" only past end + max_lateness; the rule waits `lateness`
	// more.
	svcLate, hasSvcLate := int64(0), q.MaxLatenessS != nil && *q.MaxLatenessS >= 0
	if hasSvcLate {
		svcLate = int64(*q.MaxLatenessS * float64(time.Second))
	}
	bound := w.ToNs + svcLate + int64(r.Lateness)
	labelled := q.Completeness == "complete" && !q.Partial && q.Watermark.Status == "ok" && q.CompleteThroughNs != nil &&
		q.Query.Window != nil && q.Query.Window.FromNs == w.FromNs && q.Query.Window.ToNs == w.ToNs
	switch {
	case labelled && hasSvcLate && int64(*q.CompleteThroughNs) >= bound:
	case labelled && hasSvcLate && int64(*q.CompleteThroughNs) >= w.ToNs+svcLate:
		res.Outcome = engine.Partial // complete by the label, but rows may still arrive within the rule's lateness margin
		return res
	case labelled && !hasSvcLate:
		// a service that does not say how it bridges custody time to event
		// time: its "complete" may be custody-time complete only (CAST row 26)
		res.Outcome, res.Err = engine.Unknown, "the service's label has no max_lateness: an event-time window cannot be confirmed complete"
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
