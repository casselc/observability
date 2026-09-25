package parquetgo

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// recorder keeps the payload-hash header of every request it carries.
type recorder struct {
	rt     http.RoundTripper
	mu     sync.Mutex
	hashes []string
}

func (r *recorder) RoundTrip(req *http.Request) (*http.Response, error) {
	r.mu.Lock()
	r.hashes = append(r.hashes, req.Header.Get("X-Amz-Content-Sha256"))
	r.mu.Unlock()
	return r.rt.RoundTrip(req)
}

// TestPayloadSigning pins the SDK behaviour the publisher relies on for
// cost: over https, PutObject sends UNSIGNED-PAYLOAD (the body is covered by
// the CRC32 checksum and TLS), so SigV4 does not SHA-256 the body; over
// http it does, about 3 ms of CPU per MiB (compare/s3put_bench_test.go).
func TestPayloadSigning(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("ETag", `"x"`)
	})
	tls, plain := httptest.NewTLSServer(h), httptest.NewServer(h)
	defer tls.Close()
	defer plain.Close()
	for _, c := range []struct {
		name     string
		srv      *httptest.Server
		unsigned bool
	}{
		{"https", tls, true},
		{"http", plain, false},
	} {
		rec := &recorder{rt: c.srv.Client().Transport}
		p, err := New(Config{URL: c.srv.URL + "/bkt/pre", AccessKeyID: "k", SecretAccessKey: "s",
			ProducerID: "p", Region: "r", SchemaVersion: 1, Engine: "parquet-go", Transport: rec})
		if err != nil {
			t.Fatal(err)
		}
		if err := p.put(context.Background(), "a/b.parquet", []byte("data"), "application/vnd.apache.parquet"); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if len(rec.hashes) != 1 {
			t.Fatalf("%s: %d requests", c.name, len(rec.hashes))
		}
		if got := rec.hashes[0] == "UNSIGNED-PAYLOAD"; got != c.unsigned {
			t.Errorf("%s: X-Amz-Content-Sha256 = %q, want unsigned=%v", c.name, rec.hashes[0], c.unsigned)
		}
	}
}
