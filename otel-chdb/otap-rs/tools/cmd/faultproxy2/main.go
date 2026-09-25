// faultproxy2 sits between the exporter and S3 and makes matching PUTs
// ambiguous, in three ways (every -every'th matching PUT):
//
//	-mode answer-late  forward at once (the object lands), hold the answer
//	                   for -hold: the client times out with its write committed
//	-mode apply-late   hold the request for -hold, then forward it: the
//	                   client times out, retries, and the late copy arrives
//	                   after the retry (it must get 412)
//	-mode drop         never forward, answer 503 after -hold: nothing lands
//
// SigV4 still verifies: the Host header is passed through unchanged. Every
// request is logged with method, path, If-None-Match and status, so a run's
// S3 request counts can be read off the log.
package main

import (
	"bytes"
	"context"
	"flag"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:18335", "")
	target := flag.String("target", "http://127.0.0.1:18333", "")
	match := flag.String("match", "", "fault PUTs whose path contains this")
	mode := flag.String("mode", "answer-late", "answer-late | apply-late | drop | none")
	hold := flag.Duration("hold", 3*time.Second, "")
	every := flag.Int64("every", 1, "fault every n-th matching PUT")
	skip := flag.Int64("skip", 0, "leave the first n matching PUTs alone")
	limit := flag.Int64("limit", 0, "fault at most this many PUTs (0: no limit)")
	flag.Parse()
	u, err := url.Parse(*target)
	if err != nil {
		log.Fatal(err)
	}
	rp := httputil.NewSingleHostReverseProxy(u)
	var n, faulted atomic.Int64
	forward := func(r *http.Request, body []byte) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		r2 := r.Clone(r.Context())
		r2.Body = io.NopCloser(bytes.NewReader(body))
		rp.ServeHTTP(rec, r2)
		return rec
	}
	log.Fatal(http.ListenAndServe(*listen, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inm := r.Header.Get("If-None-Match")
		if r.Method != http.MethodPut || !strings.Contains(r.URL.Path, *match) || *mode == "none" {
			rec := httptest.NewRecorder()
			rp.ServeHTTP(rec, r)
			log.Printf("%s %s inm=%q -> %d", r.Method, r.URL.Path, inm, rec.Code)
			for k, v := range rec.Header() {
				w.Header()[k] = v
			}
			w.WriteHeader(rec.Code)
			_, _ = w.Write(rec.Body.Bytes())
			return
		}
		i := n.Add(1)
		body, _ := io.ReadAll(r.Body)
		fault := i > *skip && (i-*skip)%*every == 0 && (*limit == 0 || faulted.Load() < *limit)
		if !fault {
			rec := forward(r, body)
			log.Printf("PUT #%d %s inm=%q -> %d", i, r.URL.Path, inm, rec.Code)
			for k, v := range rec.Header() {
				w.Header()[k] = v
			}
			w.WriteHeader(rec.Code)
			_, _ = w.Write(rec.Body.Bytes())
			return
		}
		faulted.Add(1)
		switch *mode {
		case "answer-late":
			rec := forward(r, body)
			log.Printf("PUT #%d %s inm=%q -> %d (applied; answer held %v)", i, r.URL.Path, inm, rec.Code, *hold)
			select {
			case <-time.After(*hold):
			case <-r.Context().Done():
				log.Printf("PUT #%d: client gave up before the answer", i)
				return
			}
			for k, v := range rec.Header() {
				w.Header()[k] = v
			}
			w.WriteHeader(rec.Code)
			_, _ = w.Write(rec.Body.Bytes())
		case "apply-late":
			log.Printf("PUT #%d %s inm=%q: held %v before forwarding", i, r.URL.Path, inm, *hold)
			done := make(chan *httptest.ResponseRecorder, 1)
			// Detached from the client: the request lands late even if the
			// client has given up (a slow network path).
			r2 := r.Clone(r.Context())
			r2 = r2.WithContext(context.Background())
			go func() {
				time.Sleep(*hold)
				rec := forward(r2, body)
				log.Printf("PUT #%d %s (late) -> %d", i, r.URL.Path, rec.Code)
				done <- rec
			}()
			select {
			case rec := <-done:
				for k, v := range rec.Header() {
					w.Header()[k] = v
				}
				w.WriteHeader(rec.Code)
				_, _ = w.Write(rec.Body.Bytes())
			case <-r.Context().Done():
				log.Printf("PUT #%d: client gave up; the request is still on its way", i)
			}
		case "drop":
			log.Printf("PUT #%d %s inm=%q: dropped", i, r.URL.Path, inm)
			time.Sleep(*hold)
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})))
}
