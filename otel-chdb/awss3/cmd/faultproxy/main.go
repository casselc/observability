// faultproxy sits between a collector and S3 and makes PUTs ambiguous: it
// forwards each matching PUT at once (so the object lands) but holds the
// answer back for -hold, so the client times out, or is killed, with its
// write committed and unacknowledged. SigV4 still verifies: the Host header
// is passed through unchanged.
//
//	faultproxy -listen 127.0.0.1:18334 -target http://127.0.0.1:18333 -match /traces/ -hold 3s -every 2
package main

import (
	"flag"
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
	listen := flag.String("listen", "127.0.0.1:18334", "")
	target := flag.String("target", "http://127.0.0.1:18333", "")
	match := flag.String("match", "", "hold PUTs whose path contains this")
	hold := flag.Duration("hold", 3*time.Second, "how long to hold the answer back")
	every := flag.Int64("every", 1, "hold every n-th matching PUT (1: all)")
	flag.Parse()
	u, err := url.Parse(*target)
	if err != nil {
		log.Fatal(err)
	}
	rp := httputil.NewSingleHostReverseProxy(u)
	var n atomic.Int64
	log.Fatal(http.ListenAndServe(*listen, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || !strings.Contains(r.URL.Path, *match) {
			rp.ServeHTTP(w, r)
			return
		}
		i := n.Add(1)
		rec := httptest.NewRecorder()
		rp.ServeHTTP(rec, r)
		log.Printf("PUT #%d %s If-None-Match=%q -> %d", i, r.URL.Path, r.Header.Get("If-None-Match"), rec.Code)
		if i%*every == 0 {
			select {
			case <-time.After(*hold):
				log.Printf("PUT #%d answered after %v", i, *hold)
			case <-r.Context().Done():
				log.Printf("PUT #%d: client gave up before the answer (%v)", i, r.Context().Err())
				return
			}
		}
		for k, v := range rec.Header() {
			w.Header()[k] = v
		}
		w.WriteHeader(rec.Code)
		_, _ = w.Write(rec.Body.Bytes())
	})))
}
