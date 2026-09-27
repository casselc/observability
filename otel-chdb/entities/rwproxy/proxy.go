package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/pprof"
	"net/textproto"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// server: ClickHouse's HTTP interface in, ClickHouse's HTTP interface out.
// Each statement is parsed and rewritten (Config.Rewrite); anything that
// isn't a statement the proxy understands is forwarded byte for byte, and
// the response is always streamed back unchanged.
type server struct {
	cfg *Config
	up  *url.URL
	tr  *http.Transport

	logMu sync.Mutex
	logW  io.Writer

	requests, rewritten, passthrough, fallbacks, upstreamErrors atomic.Int64
	reasons                                                     sync.Map // reason -> *atomic.Int64

	sampleMu sync.Mutex
	samples  []int64 // rewrite ns, most recent maxSamples
}

const maxSamples = 100000
const maxBody = 32 << 20

var hop = map[string]bool{"connection": true, "keep-alive": true, "proxy-connection": true, "transfer-encoding": true,
	"te": true, "trailer": true, "upgrade": true, "proxy-authorization": true, "proxy-authenticate": true}

func newServer(cfg *Config) (*server, error) {
	up, err := url.Parse(cfg.Upstream)
	if err != nil || up.Host == "" {
		return nil, fmt.Errorf("upstream %q: %v", cfg.Upstream, err)
	}
	s := &server{cfg: cfg, up: up, tr: &http.Transport{
		Proxy:               nil,
		DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        256,
		MaxIdleConnsPerHost: 64,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true, // responses pass through as ClickHouse encoded them
	}}
	if cfg.Log != "" {
		f, err := os.OpenFile(cfg.Log, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return nil, err
		}
		s.logW = f
	}
	return s, nil
}

type logRec struct {
	T          float64        `json:"t"`
	RewriteUS  float64        `json:"rewrite_us"`
	TotalMS    float64        `json:"total_ms"`
	Status     int            `json:"status"`
	Where      string         `json:"where,omitempty"` // url | body
	Rewritten  bool           `json:"rewritten"`
	Reason     string         `json:"reason"`
	Rules      map[string]int `json:"rules,omitempty"`
	Left       int            `json:"left,omitempty"`
	Fallback   bool           `json:"fallback,omitempty"`
	FallbackOf string         `json:"fallback_error,omitempty"`
	Err        string         `json:"err,omitempty"`
	Query      string         `json:"query,omitempty"`
	QueryID    string         `json:"query_id,omitempty"`
}

func (s *server) count(reason string) {
	v, _ := s.reasons.LoadOrStore(reason, new(atomic.Int64))
	v.(*atomic.Int64).Add(1)
}

func (s *server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if strings.HasPrefix(req.URL.Path, "/__rw/") {
		s.admin(w, req)
		return
	}
	t0 := time.Now()
	s.requests.Add(1)
	rec := logRec{T: float64(t0.UnixNano()) / 1e9, Reason: "passthrough"}

	body, streamed, err := readBody(req)
	if err != nil {
		http.Error(w, "rwproxy: reading request: "+err.Error(), http.StatusBadRequest)
		return
	}
	qs := req.URL.Query()
	outQuery := req.URL.RawQuery
	outBody := body
	var original []byte // for the fallback
	origQuery := req.URL.RawQuery
	if !streamed {
		stmt, where := "", ""
		var form *multipartBody
		params := map[string]string{}
		for k, v := range qs {
			if strings.HasPrefix(k, "param_") && len(v) > 0 {
				params[k[len("param_"):]] = v[0]
			}
		}
		if mb, ok := parseMultipart(req.Header.Get("Content-Type"), body); ok {
			// @clickhouse/client-web sends multipart/form-data when the
			// parameters outgrow its URL budget: query and param_* are fields
			form = mb
			for k, v := range mb.fields {
				if strings.HasPrefix(k, "param_") {
					params[k[len("param_"):]] = v
				}
			}
		}
		switch uq := qs.Get("query"); {
		case form != nil && uq == "" && form.fields["query"] != "":
			stmt, where = form.fields["query"], "multipart"
		case form != nil:
			rec.Reason = "not-a-statement"
		case uq != "" && len(body) == 0:
			stmt, where = uq, "url"
		case uq == "" && len(body) > 0:
			stmt, where = string(body), "body"
		default:
			rec.Reason = "not-a-statement" // e.g. an INSERT with its data in the body
		}
		if where != "" {
			rec.Where = where
			db := qs.Get("database")
			if db == "" {
				db = req.Header.Get("X-ClickHouse-Database")
			}
			tr := time.Now()
			res := s.cfg.Rewrite(stmt, params, db)
			ns := time.Since(tr).Nanoseconds()
			s.sample(ns)
			rec.RewriteUS = float64(ns) / 1e3
			rec.Reason, rec.Rules, rec.Left, rec.Err = res.Reason, res.Rules, res.Left, res.Err
			rec.QueryID = qs.Get("query_id")
			if res.Rewritten {
				rec.Rewritten = true
				original = body
				switch where {
				case "url":
					qs.Set("query", res.SQL)
					outQuery = qs.Encode()
				case "multipart":
					if outBody, err = form.with("query", res.SQL); err != nil {
						rec.Rewritten, outBody, original = false, body, nil
						rec.Reason, rec.Err = "multipart", err.Error()
					}
				default:
					outBody = []byte(res.SQL)
				}
			} else if res.Reason == "parse" || res.Reason == "span" || res.Reason == "reparse" || res.Reason == "unhandled" {
				rec.Query = stmt // log what the proxy could not handle
			}
		}
	}
	s.count(rec.Reason)
	if rec.Rewritten {
		s.rewritten.Add(1)
	} else {
		s.passthrough.Add(1)
	}

	resp, err := s.forward(req, outQuery, outBody)
	if err == nil && rec.Rewritten && s.cfg.Fallback && resp.StatusCode != http.StatusOK {
		// ClickHouse failed before streaming: send the original instead
		eb, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		resp.Body.Close()
		rec.Fallback, rec.FallbackOf = true, string(eb)
		s.fallbacks.Add(1)
		resp, err = s.forward(req, origQuery, original)
	}
	if err != nil {
		s.upstreamErrors.Add(1)
		rec.Err = err.Error()
		http.Error(w, "rwproxy: upstream: "+err.Error(), http.StatusBadGateway)
		s.write(rec, t0)
		return
	}
	defer resp.Body.Close()
	h := w.Header()
	for k, vs := range resp.Header {
		if hop[strings.ToLower(k)] {
			continue
		}
		for _, v := range vs {
			h.Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	rec.Status = resp.StatusCode
	stream(w, resp.Body)
	s.write(rec, t0)
}

// multipartBody: a multipart/form-data request body, kept part by part so
// that one field can be replaced and the rest re-sent as they came.
type multipartBody struct {
	boundary string
	parts    []mpart
	fields   map[string]string
}

type mpart struct {
	header textproto.MIMEHeader
	name   string
	data   []byte
}

func parseMultipart(ct string, body []byte) (*multipartBody, bool) {
	mt, ps, err := mime.ParseMediaType(ct)
	if err != nil || mt != "multipart/form-data" || ps["boundary"] == "" {
		return nil, false
	}
	mb := &multipartBody{boundary: ps["boundary"], fields: map[string]string{}}
	mr := multipart.NewReader(bytes.NewReader(body), mb.boundary)
	for {
		p, err := mr.NextRawPart()
		if err == io.EOF {
			return mb, true
		}
		if err != nil {
			return nil, false
		}
		d, err := io.ReadAll(p)
		if err != nil {
			return nil, false
		}
		name := p.FormName()
		mb.parts = append(mb.parts, mpart{header: p.Header, name: name, data: d})
		if p.FileName() == "" {
			mb.fields[name] = string(d)
		}
	}
}

func (mb *multipartBody) with(name, value string) ([]byte, error) {
	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	if err := w.SetBoundary(mb.boundary); err != nil {
		return nil, err
	}
	for _, p := range mb.parts {
		pw, err := w.CreatePart(p.header)
		if err != nil {
			return nil, err
		}
		d := p.data
		if p.name == name {
			d = []byte(value)
		}
		pw.Write(d)
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// readBody buffers the statement (decompressing a gzip body); a body too
// large to be a statement is streamed through untouched.
func readBody(req *http.Request) ([]byte, bool, error) {
	if req.Body == nil {
		return nil, false, nil
	}
	enc := strings.ToLower(req.Header.Get("Content-Encoding"))
	if enc != "" && enc != "gzip" {
		return nil, true, nil
	}
	b, err := io.ReadAll(io.LimitReader(req.Body, maxBody+1))
	if err != nil {
		return nil, false, err
	}
	if len(b) > maxBody {
		req.Body = io.NopCloser(io.MultiReader(bytes.NewReader(b), req.Body))
		return nil, true, nil
	}
	if enc == "gzip" {
		zr, err := gzip.NewReader(bytes.NewReader(b))
		if err != nil {
			return nil, false, err
		}
		if b, err = io.ReadAll(zr); err != nil {
			return nil, false, err
		}
		req.Header.Del("Content-Encoding")
	}
	return b, false, nil
}

func (s *server) forward(req *http.Request, rawQuery string, body []byte) (*http.Response, error) {
	u := *s.up
	u.Path = req.URL.Path
	u.RawQuery = rawQuery
	var rd io.Reader
	var n int64 = -1
	if body != nil {
		rd, n = bytes.NewReader(body), int64(len(body))
	} else if req.Body != nil {
		rd = req.Body // streamed passthrough
	}
	out, err := http.NewRequestWithContext(req.Context(), req.Method, u.String(), rd)
	if err != nil {
		return nil, err
	}
	for k, vs := range req.Header {
		lk := strings.ToLower(k)
		if hop[lk] || lk == "content-length" || lk == "host" {
			continue
		}
		for _, v := range vs {
			out.Header.Add(k, v)
		}
	}
	if n >= 0 {
		out.ContentLength = n
	}
	return s.tr.RoundTrip(out)
}

var bufPool = sync.Pool{New: func() any { b := make([]byte, 64<<10); return &b }}

func stream(w http.ResponseWriter, r io.Reader) {
	f, _ := w.(http.Flusher)
	bp := bufPool.Get().(*[]byte)
	defer bufPool.Put(bp)
	buf := *bp
	for {
		n, err := r.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return
			}
			if f != nil {
				f.Flush()
			}
		}
		if err != nil {
			return
		}
	}
}

func (s *server) write(rec logRec, t0 time.Time) {
	if s.logW == nil {
		return
	}
	rec.TotalMS = float64(time.Since(t0).Microseconds()) / 1e3
	b, _ := json.Marshal(rec)
	s.logMu.Lock()
	s.logW.Write(append(b, '\n'))
	s.logMu.Unlock()
}

func (s *server) sample(ns int64) {
	s.sampleMu.Lock()
	if len(s.samples) >= maxSamples {
		s.samples = s.samples[1:]
	}
	s.samples = append(s.samples, ns)
	s.sampleMu.Unlock()
}

func (s *server) admin(w http.ResponseWriter, req *http.Request) {
	switch req.URL.Path {
	case "/__rw/stats":
		s.sampleMu.Lock()
		ss := append([]int64(nil), s.samples...)
		s.sampleMu.Unlock()
		sort.Slice(ss, func(i, j int) bool { return ss[i] < ss[j] })
		reasons := map[string]int64{}
		s.reasons.Range(func(k, v any) bool { reasons[k.(string)] = v.(*atomic.Int64).Load(); return true })
		out := map[string]any{"requests": s.requests.Load(), "rewritten": s.rewritten.Load(), "passthrough": s.passthrough.Load(),
			"fallbacks": s.fallbacks.Load(), "upstream_errors": s.upstreamErrors.Load(), "reasons": reasons,
			"rewrite_us": percentiles(ss), "mode": s.cfg.Mode}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(out)
	case "/__rw/pprof":
		if os.Getenv("RWPROXY_PPROF") == "" {
			http.NotFound(w, req)
			return
		}
		pprof.Profile(w, req)
	case "/__rw/reset":
		s.sampleMu.Lock()
		s.samples = nil
		s.sampleMu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		http.NotFound(w, req)
	}
}

func percentiles(ss []int64) map[string]float64 {
	if len(ss) == 0 {
		return nil
	}
	p := func(q float64) float64 { return float64(ss[int(q*float64(len(ss)-1))]) / 1e3 }
	return map[string]float64{"n": float64(len(ss)), "p50": p(0.5), "p90": p(0.9), "p99": p(0.99), "max": p(1)}
}

// refreshCovered re-reads the covered key list (a key the catalog gains
// must not be read from the residual only).
func (s *server) refreshCovered() {
	if s.cfg.RefreshCoveredSQL == "" || s.cfg.RefreshSeconds <= 0 {
		return
	}
	for {
		if keys, err := s.readKeys(); err != nil {
			log.Printf("refresh covered keys: %v (keeping %d)", err, len(s.cfg.covered))
		} else {
			s.cfg.setCovered(keys)
		}
		time.Sleep(time.Duration(s.cfg.RefreshSeconds) * time.Second)
	}
}

func (s *server) readKeys() ([]string, error) {
	u := *s.up
	u.Path = "/"
	req, _ := http.NewRequest("POST", u.String(), strings.NewReader(s.cfg.RefreshCoveredSQL+" FORMAT TSVRaw"))
	if s.cfg.RefreshUser != "" {
		req.Header.Set("X-ClickHouse-User", s.cfg.RefreshUser)
		req.Header.Set("X-ClickHouse-Key", s.cfg.RefreshPassword)
	}
	resp, err := s.tr.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("%d: %s", resp.StatusCode, firstLine(string(b)))
	}
	var keys []string
	for _, l := range strings.Split(strings.TrimRight(string(b), "\n"), "\n") {
		if l != "" {
			keys = append(keys, l)
		}
	}
	return keys, nil
}
