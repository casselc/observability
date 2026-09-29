package casreg

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/anishathalye/porcupine"
)

// md5store is a tiny S3 (MD5 ETags); ignore accepts conditional headers
// and ignores them (old MinIO, Garage).
type md5store struct {
	mu     sync.Mutex
	objs   map[string][]byte
	ignore bool
}

func (f *md5store) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	etag := func(b []byte) string { h := md5.Sum(b); return `"` + hex.EncodeToString(h[:]) + `"` }
	cur, ok := f.objs[r.URL.Path]
	switch r.Method {
	case http.MethodPut:
		b, _ := io.ReadAll(r.Body)
		if !f.ignore {
			if r.Header.Get("If-None-Match") == "*" && ok {
				w.WriteHeader(412)
				return
			}
			if im := r.Header.Get("If-Match"); im != "" && (!ok || etag(cur) != im) {
				w.WriteHeader(412)
				return
			}
		}
		f.objs[r.URL.Path] = b
		w.Header().Set("ETag", etag(b))
	case http.MethodGet, http.MethodHead:
		if !ok {
			w.WriteHeader(404)
			return
		}
		w.Header().Set("ETag", etag(cur))
		if r.Method == http.MethodGet {
			_, _ = w.Write(cur)
		}
	}
}

func casRound(t *testing.T, c *http.Client, url string, w, i int) {
	resp, err := c.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	req, _ := http.NewRequest(http.MethodPut, url, strings.NewReader(fmt.Sprintf("w%d-%d", w, i)))
	if resp.StatusCode == 404 {
		req.Header.Set("If-None-Match", "*")
	} else {
		req.Header.Set("If-Match", resp.Header.Get("ETag"))
	}
	if resp, err = c.Do(req); err == nil {
		resp.Body.Close()
	}
}

func TestTransportRecordsS3Requests(t *testing.T) {
	for _, ignore := range []bool{false, true} {
		srv := httptest.NewServer(&md5store{objs: map[string][]byte{}, ignore: ignore})
		var rec Recorder
		// first, a stale swap for certain: two clients read, then both write
		ca := &http.Client{Transport: &Transport{Base: srv.Client().Transport, Rec: &rec, Client: 0}}
		cb := &http.Client{Transport: &Transport{Base: srv.Client().Transport, Rec: &rec, Client: 1}}
		casRound(t, ca, srv.URL+"/b/s", 0, 0)
		ra, _ := ca.Get(srv.URL + "/b/s")
		ra.Body.Close()
		rb, _ := cb.Get(srv.URL + "/b/s")
		rb.Body.Close()
		for i, x := range []*http.Client{ca, cb} {
			req, _ := http.NewRequest(http.MethodPut, srv.URL+"/b/s", strings.NewReader(fmt.Sprint("stale", i)))
			req.Header.Set("If-Match", ra.Header.Get("ETag"))
			if r, err := x.Do(req); err == nil {
				r.Body.Close()
			}
		}
		var wg sync.WaitGroup
		for w := range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				c := &http.Client{Transport: &Transport{Base: srv.Client().Transport, Rec: &rec, Client: w}}
				for i := range 25 {
					casRound(t, c, srv.URL+"/b/k", w, i)
				}
			}()
		}
		wg.Wait()
		srv.Close()
		res, _ := rec.Check(0)
		t.Logf("ignore=%v: %d ops, %s", ignore, rec.Len(), res)
		if ignore && res != porcupine.Illegal || !ignore && res != porcupine.Ok {
			t.Fatalf("ignore=%v: %s", ignore, res)
		}
	}
}

func TestDecodeAWSChunked(t *testing.T) {
	enc := "5;chunk-signature=ab\r\nhello\r\n6\r\n world\r\n0\r\nx-amz-checksum-crc32:AAAAAA==\r\n\r\n"
	b, err := DecodeAWSChunked([]byte(enc))
	if err != nil || string(b) != "hello world" {
		t.Fatalf("%q %v", b, err)
	}
}
