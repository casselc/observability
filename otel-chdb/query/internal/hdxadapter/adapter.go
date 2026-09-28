package hdxadapter

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Config is the adapter's configuration.
type Config struct {
	Listen string `json:"listen"`
	// QueryURL is the service's POST /v1/query endpoint.
	QueryURL string `json:"query_url"`
	// DefaultDatabase and TimeColumns mirror the service's central.database
	// and central.tables (name -> time_column), for DeriveWindow.
	DefaultDatabase string            `json:"default_database"`
	TimeColumns     map[string]string `json:"time_columns"`
	// TokenHeader, if set, is read for the caller's token when there is no
	// Authorization: Bearer header (oauth2-proxy's X-Forwarded-Access-Token).
	TokenHeader  string `json:"token_header"`
	MaxBodyBytes int64  `json:"max_body_bytes"`
	TimeoutS     int    `json:"timeout_s"`
}

// Adapter is the HTTP handler.
type Adapter struct {
	cfg    Config
	tables Tables
	client *http.Client

	mu     sync.Mutex
	counts map[string]int64
}

// New returns an adapter.
func New(cfg Config, client *http.Client) *Adapter {
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = 32 << 20
	}
	if cfg.TimeoutS <= 0 {
		cfg.TimeoutS = 300
	}
	if cfg.DefaultDatabase == "" {
		cfg.DefaultDatabase = "otel"
	}
	if client == nil {
		client = &http.Client{Timeout: time.Duration(cfg.TimeoutS) * time.Second}
	}
	tc := map[string]string{}
	for k, v := range cfg.TimeColumns {
		if !strings.Contains(k, ".") {
			k = cfg.DefaultDatabase + "." + k
		}
		tc[k] = v
	}
	return &Adapter{cfg: cfg, client: client, tables: Tables{DefaultDatabase: cfg.DefaultDatabase, TimeColumns: tc}, counts: map[string]int64{}}
}

func (a *Adapter) count(k string) {
	a.mu.Lock()
	a.counts[k]++
	a.mu.Unlock()
}

// LabelHeaders are the response headers that carry the label; the fork's
// client reads them (and a browser may, through the API's proxy).
var LabelHeaders = []string{"X-Otel-Request-Id", "X-Otel-Source", "X-Otel-Completeness", "X-Otel-Complete-Through",
	"X-Otel-Incomplete-From", "X-Otel-Watermark-Status", "X-Otel-Watermark-Lag-S", "X-Otel-Watermark-Note",
	"X-Otel-Window-From", "X-Otel-Window-To", "X-Otel-Dropped-Settings", "X-Otel-Statement",
	"X-Otel-Max-Lateness-S", "X-Otel-Settled-Through", "X-Otel-Late-Rows"}

// clientKeys are URL parameters of the HTTP interface that are not settings.
var clientKeys = map[string]bool{"query": true, "query_id": true, "database": true, "default_format": true,
	"session_id": true, "session_timeout": true, "session_check": true, "role": true, "user": true, "password": true,
	"quota_key": true, "compress": true, "decompress": true, "buffer_size": true, "wait_end_of_query": true}

// ServeHTTP answers ClickHouse's HTTP interface for HyperDX's clients.
func (a *Adapter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/ping":
		io.WriteString(w, "Ok.\n")
		return
	case "/__hdx/metrics":
		a.metrics(w)
		return
	}
	w.Header().Set("Access-Control-Expose-Headers", strings.Join(LabelHeaders, ", "))
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		a.fail(w, refuse(405, 497, "ACCESS_DENIED", "method", "method %s is not served", r.Method))
		return
	}
	req, err := a.read(r)
	if err != nil {
		a.fail(w, err)
		return
	}
	if req.token == "" {
		a.fail(w, refuse(401, 516, "AUTHENTICATION_FAILED", "no_token", "the adapter needs the user's OIDC token (Authorization: Bearer)"))
		return
	}
	if db := req.values.Get("database"); db != "" && db != a.cfg.DefaultDatabase {
		a.fail(w, notAllowed("database", "database %s: the service's default database is %s", db, a.cfg.DefaultDatabase))
		return
	}
	st, err := a.tables.Prepare(req.sql, req.params, req.values.Get("default_format"))
	if err != nil {
		a.fail(w, err)
		return
	}
	body := map[string]any{"sql": st.SQL}
	if st.Window != nil {
		body["window"] = map[string]int64{"from": st.Window.FromNs, "to": st.Window.ToNs}
	}
	var dropped []string
	output := map[string]string{}
	for k := range req.values {
		if clientKeys[k] || strings.HasPrefix(k, "param_") {
			continue
		}
		if k == "date_time_output_format" {
			output[k] = req.values.Get(k)
			continue
		}
		dropped = append(dropped, k)
	}
	sort.Strings(dropped)
	if len(output) > 0 {
		body["output"] = output
	}
	resp, err := a.forward(r.Context(), req.token, body, req.values.Get("query_id"))
	if err != nil {
		a.fail(w, err)
		return
	}
	res, err := ParseResult(resp.Result)
	if err != nil {
		a.fail(w, refuse(502, 210, "NETWORK_ERROR", "bad_answer", "the service's result is not ClickHouse JSON: %v", err))
		return
	}
	if st.Kind == "describe" && len(res.Rows) == 0 {
		a.fail(w, refuse(404, 60, "UNKNOWN_TABLE", "unknown_table", "the table does not exist or is not served"))
		return
	}
	h := w.Header()
	h.Set("X-Otel-Request-Id", resp.RequestID)
	h.Set("X-Otel-Statement", st.Kind)
	if len(dropped) > 0 {
		h.Set("X-Otel-Dropped-Settings", strings.Join(dropped, ","))
	}
	if st.Metadata {
		h.Set("X-Otel-Source", "metadata")
	} else {
		h.Set("X-Otel-Source", resp.Source)
		h.Set("X-Otel-Completeness", resp.Completeness)
		if resp.CompleteThrough != nil {
			h.Set("X-Otel-Complete-Through", *resp.CompleteThrough)
		}
		if resp.IncompleteFrom != nil {
			h.Set("X-Otel-Incomplete-From", *resp.IncompleteFrom)
		}
		// the two clocks (CAST row 26): complete_through is custody time;
		// settled_through = complete_through − max_lateness is event time
		if resp.MaxLatenessS != nil {
			h.Set("X-Otel-Max-Lateness-S", strconv.FormatFloat(*resp.MaxLatenessS, 'f', -1, 64))
		}
		if resp.SettledThrough != nil {
			h.Set("X-Otel-Settled-Through", *resp.SettledThrough)
		}
		// rows later than max_lateness: a count, or why there is none
		if resp.Late.Rows != nil {
			h.Set("X-Otel-Late-Rows", strconv.FormatInt(*resp.Late.Rows, 10))
		} else if resp.Late.Status != "" {
			h.Set("X-Otel-Late-Rows", resp.Late.Status)
		}
		h.Set("X-Otel-Watermark-Status", resp.Watermark.Status)
		if resp.Watermark.LagS != nil {
			h.Set("X-Otel-Watermark-Lag-S", strconv.FormatFloat(*resp.Watermark.LagS, 'f', 1, 64))
		}
		if resp.Watermark.Note != "" {
			h.Set("X-Otel-Watermark-Note", headerSafe(resp.Watermark.Note))
		}
		if st.Window != nil {
			h.Set("X-Otel-Window-From", time.Unix(0, st.Window.FromNs).UTC().Format(time.RFC3339Nano))
			h.Set("X-Otel-Window-To", time.Unix(0, st.Window.ToNs).UTC().Format(time.RFC3339Nano))
		}
	}
	sum, _ := json.Marshal(map[string]string{"read_rows": strconv.FormatInt(resp.Query.RowsRead, 10), "read_bytes": "0",
		"written_rows": "0", "written_bytes": "0", "total_rows_to_read": "0", "result_rows": strconv.Itoa(len(res.Rows)),
		"result_bytes": "0", "elapsed_ns": strconv.FormatInt(int64(resp.Query.ElapsedMs*1e6), 10)})
	h.Set("X-ClickHouse-Summary", string(sum))
	h.Set("X-ClickHouse-Format", st.Format)
	if q := req.values.Get("query_id"); q != "" {
		h.Set("X-ClickHouse-Query-Id", q)
	}
	if st.Format == "JSON" || st.Format == "JSONCompact" {
		h.Set("Content-Type", "application/json; charset=UTF-8")
	} else {
		h.Set("Content-Type", "application/x-ndjson; charset=UTF-8")
	}
	a.count("ok:" + st.Kind)
	w.WriteHeader(http.StatusOK)
	if err := res.Render(w, st.Format); err != nil {
		log.Printf("hdxadapter: render: %v", err)
	}
}

// request is what was read from HyperDX's request.
type request struct {
	sql    string
	params map[string]string
	values url.Values
	token  string
}

func (a *Adapter) read(r *http.Request) (*request, error) {
	rq := &request{params: map[string]string{}, values: r.URL.Query()}
	if tok, ok := bearer(r.Header.Get("Authorization")); ok {
		rq.token = tok
	} else if a.cfg.TokenHeader != "" {
		rq.token = strings.TrimSpace(r.Header.Get(a.cfg.TokenHeader))
	}
	var rd io.Reader = http.MaxBytesReader(nil, r.Body, a.cfg.MaxBodyBytes)
	if strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
		zr, err := gzip.NewReader(rd)
		if err != nil {
			return nil, refuse(400, 354, "CANNOT_DECOMPRESS", "body", "gzip body: %v", err)
		}
		rd = io.LimitReader(zr, a.cfg.MaxBodyBytes+1)
	} else if ce := r.Header.Get("Content-Encoding"); ce != "" && !strings.EqualFold(ce, "identity") {
		return nil, refuse(400, 354, "CANNOT_DECOMPRESS", "body", "content encoding %s is not supported", ce)
	}
	body, err := io.ReadAll(rd)
	if err != nil {
		return nil, refuse(413, 1001, "STD_EXCEPTION", "body", "reading the request: %v", err)
	}
	if int64(len(body)) > a.cfg.MaxBodyBytes {
		return nil, refuse(413, 1001, "STD_EXCEPTION", "body", "the request is larger than %d bytes", a.cfg.MaxBodyBytes)
	}
	for k, v := range rq.values {
		if strings.HasPrefix(k, "param_") && len(v) > 0 {
			rq.params[k[len("param_"):]] = v[0]
		}
	}
	urlQuery := rq.values.Get("query")
	mt, mp, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if mt == "multipart/form-data" {
		// @clickhouse/client sends the query and param_* as form fields when
		// the parameters outgrow its URL budget
		mr := multipart.NewReader(bytes.NewReader(body), mp["boundary"])
		body = nil
		for {
			part, err := mr.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, refuse(400, 62, "SYNTAX_ERROR", "body", "multipart body: %v", err)
			}
			v, err := io.ReadAll(part)
			if err != nil {
				return nil, refuse(400, 62, "SYNTAX_ERROR", "body", "multipart body: %v", err)
			}
			switch name := part.FormName(); {
			case name == "query":
				body = v
			case strings.HasPrefix(name, "param_"):
				rq.params[name[len("param_"):]] = string(v)
			}
		}
	}
	switch {
	case urlQuery != "" && len(bytes.TrimSpace(body)) > 0:
		// ClickHouse concatenates the two; HyperDX never sends both (an
		// INSERT with its data does)
		return nil, notAllowed("not_select", "a statement in both the URL and the body is not served")
	case urlQuery != "":
		rq.sql = urlQuery
	default:
		rq.sql = string(body)
	}
	if strings.TrimSpace(rq.sql) == "" {
		return nil, syntaxErr("empty query")
	}
	return rq, nil
}

func bearer(h string) (string, bool) {
	const p = "bearer "
	if len(h) > len(p) && strings.EqualFold(h[:len(p)], p) {
		t := strings.TrimSpace(h[len(p):])
		return t, t != ""
	}
	return "", false
}

// serviceAnswer is the part of /v1/query's answer the adapter uses.
type serviceAnswer struct {
	RequestID       string   `json:"request_id"`
	Source          string   `json:"source"`
	CompleteThrough *string  `json:"complete_through"`
	MaxLatenessS    *float64 `json:"max_lateness_s"`
	SettledThrough  *string  `json:"settled_through"`
	Completeness    string   `json:"completeness"`
	Partial         bool     `json:"partial"`
	IncompleteFrom  *string  `json:"incomplete_from"`
	Watermark       struct {
		Status string   `json:"status"`
		LagS   *float64 `json:"lag_s"`
		Note   string   `json:"note"`
	} `json:"watermark"`
	Late struct {
		Status string `json:"status"`
		Rows   *int64 `json:"rows"`
	} `json:"late"`
	Query struct {
		RowsRead  int64   `json:"rows_read"`
		ElapsedMs float64 `json:"elapsed_ms"`
	} `json:"query"`
	Result json.RawMessage `json:"result"`
	// errors
	Error  string `json:"error"`
	Detail string `json:"detail"`
}

func (a *Adapter) forward(ctx context.Context, token string, body map[string]any, clientQueryID string) (*serviceAnswer, error) {
	b, _ := json.Marshal(body)
	hr, err := http.NewRequestWithContext(ctx, http.MethodPost, a.cfg.QueryURL, bytes.NewReader(b))
	if err != nil {
		return nil, refuse(502, 210, "NETWORK_ERROR", "service", "%v", err)
	}
	hr.Header.Set("Authorization", "Bearer "+token)
	hr.Header.Set("Content-Type", "application/json")
	if clientQueryID != "" {
		hr.Header.Set("X-Client-Query-Id", clientQueryID)
	}
	resp, err := a.client.Do(hr)
	if err != nil {
		return nil, refuse(502, 210, "NETWORK_ERROR", "service_unreachable", "the query service did not answer: %v", err)
	}
	defer resp.Body.Close()
	var ans serviceAnswer
	if err := json.NewDecoder(resp.Body).Decode(&ans); err != nil {
		return nil, refuse(502, 210, "NETWORK_ERROR", "bad_answer", "the query service's answer (HTTP %d) is not JSON: %v", resp.StatusCode, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, serviceError(resp.StatusCode, ans)
	}
	return &ans, nil
}

// limitCodes are ClickHouse's codes for the limits the service names in
// limit_exceeded:<NAME>.
var limitCodes = map[string]int{"TOO_MANY_ROWS": 158, "TIMEOUT_EXCEEDED": 159, "TOO_SLOW": 160, "MEMORY_LIMIT_EXCEEDED": 241,
	"TOO_MANY_BYTES": 307, "TOO_MANY_ROWS_OR_BYTES": 396, "TOO_MANY_SIMULTANEOUS_QUERIES": 202}

// serviceError maps the service's refusal to a ClickHouse error the client
// parses: the status stays the service's, the reason travels in the message.
func serviceError(status int, ans serviceAnswer) *Error {
	msg := fmt.Sprintf("query service: %s: %s [request %s]", ans.Error, ans.Detail, ans.RequestID)
	switch {
	case status == 401:
		return refuse(401, 516, "AUTHENTICATION_FAILED", ans.Error, "%s", msg)
	case status == 403:
		return refuse(403, 497, "ACCESS_DENIED", ans.Error, "%s", msg)
	case status == 429:
		return refuse(429, 202, "TOO_MANY_SIMULTANEOUS_QUERIES", ans.Error, "%s", msg)
	case strings.HasPrefix(ans.Error, "limit_exceeded:"):
		name := strings.TrimPrefix(ans.Error, "limit_exceeded:")
		code, ok := limitCodes[name]
		if !ok {
			code, name = 158, "TOO_MANY_ROWS"
		}
		return refuse(status, code, name, ans.Error, "%s", msg)
	case status == 400 && ans.Error == "central_rejected":
		// ClickHouse's own error: keep its code
		code, name := 62, "SYNTAX_ERROR"
		if i := strings.Index(ans.Detail, "(code "); i >= 0 {
			if j := strings.IndexByte(ans.Detail[i:], ')'); j > 0 {
				if n, err := strconv.Atoi(ans.Detail[i+6 : i+j]); err == nil {
					code, name = n, "CLICKHOUSE_ERROR"
				}
			}
		}
		return refuse(400, code, name, ans.Error, "%s", msg)
	case status == 400:
		return refuse(400, 62, "SYNTAX_ERROR", ans.Error, "%s", msg)
	}
	return refuse(status, 210, "NETWORK_ERROR", ans.Error, "%s", msg)
}

// fail writes err as ClickHouse writes an error, so that @clickhouse/client
// raises a ClickHouseError with the code, the name and the message.
func (a *Adapter) fail(w http.ResponseWriter, err error) {
	e, ok := err.(*Error)
	if !ok {
		e = refuse(500, 1001, "STD_EXCEPTION", "internal", "%v", err)
	}
	a.count("refused:" + e.Reason)
	w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
	w.Header().Set("X-ClickHouse-Exception-Code", strconv.Itoa(e.Code))
	w.Header().Set("X-Otel-Refusal", e.Reason)
	w.WriteHeader(e.HTTPStatus)
	fmt.Fprintf(w, "Code: %d. DB::Exception: %s. (%s) (version otel-hdxadapter)\n", e.Code, messageSafe(e.Message), e.Name)
}

// messageSafe keeps a message from ending the client's match early: the
// client takes the first "(UPPERCASE_NAME)" as the error's type.
func messageSafe(s string) string {
	return strings.NewReplacer("(", "[", ")", "]", "\n", " ", "\r", " ").Replace(s)
}

func headerSafe(s string) string {
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r > 0x7e {
			return ' '
		}
		return r
	}, s)
}

func (a *Adapter) metrics(w http.ResponseWriter) {
	a.mu.Lock()
	defer a.mu.Unlock()
	keys := make([]string, 0, len(a.counts))
	for k := range a.counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintln(w, "# HELP hdxadapter_requests_total Statements by outcome (ok:<kind> or refused:<reason>).")
	fmt.Fprintln(w, "# TYPE hdxadapter_requests_total counter")
	for _, k := range keys {
		fmt.Fprintf(w, "hdxadapter_requests_total{outcome=%q} %d\n", k, a.counts[k])
	}
}
