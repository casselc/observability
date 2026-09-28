// Package central runs statements on the central ClickHouse over HTTP as a
// read-only user, with the caller's limits pinned and every overflow mode
// set to throw (AMBIGUITY.md X7, C2): a limit fails the statement, it never
// shortens the result.
package central

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Config is the connection.
type Config struct {
	URL      string `json:"url"`
	User     string `json:"user"`
	Password string `json:"-"`
	Database string `json:"database"`
	TimeoutS int    `json:"timeout_s"`
}

// Limits are one caller's per-statement budget (R-S9, SEC-5).
type Limits struct {
	MaxExecutionTimeS int   `json:"max_execution_time_s"`
	MaxRowsToRead     int64 `json:"max_rows_to_read"`
	MaxBytesToRead    int64 `json:"max_bytes_to_read"`
	MaxResultRows     int64 `json:"max_result_rows"`
	MaxResultBytes    int64 `json:"max_result_bytes"`
	MaxMemoryUsage    int64 `json:"max_memory_usage"`
	// MaxConcurrent is per subject, enforced by the service, not ClickHouse.
	MaxConcurrent int `json:"max_concurrent"`
}

// Max is the larger of each field: a principal with several roles gets the
// most generous of their tiers.
func (l Limits) Max(o Limits) Limits {
	m := func(a, b int64) int64 {
		if a > b {
			return a
		}
		return b
	}
	return Limits{
		MaxExecutionTimeS: int(m(int64(l.MaxExecutionTimeS), int64(o.MaxExecutionTimeS))),
		MaxRowsToRead:     m(l.MaxRowsToRead, o.MaxRowsToRead),
		MaxBytesToRead:    m(l.MaxBytesToRead, o.MaxBytesToRead),
		MaxResultRows:     m(l.MaxResultRows, o.MaxResultRows),
		MaxResultBytes:    m(l.MaxResultBytes, o.MaxResultBytes),
		MaxMemoryUsage:    m(l.MaxMemoryUsage, o.MaxMemoryUsage),
		MaxConcurrent:     int(m(int64(l.MaxConcurrent), int64(o.MaxConcurrent))),
	}
}

// DefaultLimits is a conservative tier.
var DefaultLimits = Limits{MaxExecutionTimeS: 30, MaxRowsToRead: 2_000_000_000, MaxBytesToRead: 200 << 30,
	MaxResultRows: 100_000, MaxResultBytes: 64 << 20, MaxMemoryUsage: 4 << 30, MaxConcurrent: 4}

// OverflowModes are the eleven *_overflow_mode settings; each is pinned to
// 'throw' on every statement (the HyperDX fork's patch 0001 pins the same
// eleven).
var OverflowModes = []string{
	"distinct_overflow_mode", "group_by_overflow_mode", "join_overflow_mode",
	"read_overflow_mode", "read_overflow_mode_leaf", "result_overflow_mode",
	"set_overflow_mode", "sort_overflow_mode", "timeout_overflow_mode",
	"timeout_overflow_mode_leaf", "transfer_overflow_mode",
}

// Settings renders a statement's settings: the limits, the pinned overflow
// modes, the scope filters, and the ids that tie ClickHouse's query_log to
// the audit log. They go in the URL, where the caller's text cannot reach.
func Settings(l Limits, filters, queryID, comment string) url.Values {
	v := url.Values{}
	set := func(k string, n int64) {
		if n > 0 {
			v.Set(k, strconv.FormatInt(n, 10))
		}
	}
	set("max_execution_time", int64(l.MaxExecutionTimeS))
	set("max_rows_to_read", l.MaxRowsToRead)
	set("max_bytes_to_read", l.MaxBytesToRead)
	set("max_result_rows", l.MaxResultRows)
	set("max_result_bytes", l.MaxResultBytes)
	set("max_memory_usage", l.MaxMemoryUsage)
	for _, m := range OverflowModes {
		v.Set(m, "throw")
	}
	if filters != "" {
		v.Set("additional_table_filters", filters)
	}
	// the whole answer, or an error status: never HTTP 200 followed by an
	// exception in the body (AMBIGUITY.md C7)
	v.Set("wait_end_of_query", "1")
	v.Set("http_write_exception_in_output_format", "0")
	v.Set("default_format", "JSON")
	v.Set("output_format_json_quote_64bit_integers", "1")
	v.Set("max_query_size", "262144")
	if queryID != "" {
		v.Set("query_id", queryID)
	}
	if comment != "" {
		v.Set("log_comment", comment)
	}
	return v
}

// Client talks to one ClickHouse endpoint.
type Client struct {
	cfg  Config
	http *http.Client
}

// New returns a client.
func New(cfg Config) *Client {
	t := time.Duration(cfg.TimeoutS) * time.Second
	if t <= 0 {
		t = 5 * time.Minute
	}
	return &Client{cfg: cfg, http: &http.Client{Timeout: t}}
}

// Error is a ClickHouse error answer.
type Error struct {
	HTTPStatus int
	Code       int
	Message    string
}

func (e *Error) Error() string {
	return fmt.Sprintf("clickhouse %d (code %d): %s", e.HTTPStatus, e.Code, e.Message)
}

// LimitCodes are the errors a pinned limit raises.
var LimitCodes = map[int]string{
	158: "TOO_MANY_ROWS", 159: "TIMEOUT_EXCEEDED", 160: "TOO_SLOW", 241: "MEMORY_LIMIT_EXCEEDED",
	307: "TOO_MANY_BYTES", 396: "TOO_MANY_ROWS_OR_BYTES", 202: "TOO_MANY_SIMULTANEOUS_QUERIES",
}

// Summary is ClickHouse's X-ClickHouse-Summary.
type Summary struct {
	ReadRows    int64 `json:"read_rows,string"`
	ReadBytes   int64 `json:"read_bytes,string"`
	ResultRows  int64 `json:"result_rows,string"`
	ElapsedNs   int64 `json:"elapsed_ns,string"`
	ResultBytes int64 `json:"result_bytes,string"`
}

// Query runs sql and returns ClickHouse's JSON body.
func (c *Client) Query(ctx context.Context, sql string, settings url.Values) ([]byte, Summary, error) {
	u, err := url.Parse(c.cfg.URL)
	if err != nil {
		return nil, Summary{}, err
	}
	q := u.Query()
	for k, vs := range settings {
		for _, v := range vs {
			q.Set(k, v)
		}
	}
	if c.cfg.Database != "" {
		q.Set("database", c.cfg.Database)
	}
	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), strings.NewReader(sql))
	if err != nil {
		return nil, Summary{}, err
	}
	if c.cfg.User != "" {
		req.Header.Set("X-ClickHouse-User", c.cfg.User)
		req.Header.Set("X-ClickHouse-Key", c.cfg.Password)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, Summary{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<30))
	if err != nil {
		return nil, Summary{}, err
	}
	var sum Summary
	if h := resp.Header.Get("X-ClickHouse-Summary"); h != "" {
		_ = json.Unmarshal([]byte(h), &sum)
	}
	if resp.StatusCode != http.StatusOK {
		code, _ := strconv.Atoi(resp.Header.Get("X-ClickHouse-Exception-Code"))
		return nil, sum, &Error{HTTPStatus: resp.StatusCode, Code: code, Message: strings.TrimSpace(string(bytes.TrimSpace(body)))}
	}
	// C7: an exception after a 200 would still be in the body
	if code := resp.Header.Get("X-ClickHouse-Exception-Code"); code != "" {
		n, _ := strconv.Atoi(code)
		return nil, sum, &Error{HTTPStatus: resp.StatusCode, Code: n, Message: "exception after HTTP 200"}
	}
	return body, sum, nil
}
