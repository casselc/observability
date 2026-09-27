package lane

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// A hung S3 must not stall the lane: the first PUT hangs past PutTimeout,
// the retry lands, and the records are written once.
func TestPutTimeoutRetries(t *testing.T) {
	var calls atomic.Int32
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if calls.Add(1) == 1 {
			select { // hang until the client gives up (or the test ends)
			case <-r.Context().Done():
			case <-release:
			}
			return
		}
		rw.Header().Set("ETag", `"x"`)
		rw.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	defer close(release)
	t.Setenv("AWS_ACCESS_KEY_ID", "k")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "s")
	c, err := NewS3(context.Background(), srv.URL, "us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	w := &Writer{S3: c, Bucket: "b", Lane: "lanes/c/1-i", PutTimeout: 300 * time.Millisecond}
	w.Add(Record{Level: LPod, Key: 1})
	done := make(chan error, 1)
	go func() { done <- w.Flush(context.Background()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("lane stalled behind a hung PUT")
	}
	if w.Objects.Load() != 1 || w.Records.Load() != 1 || w.Retries.Load() < 1 {
		t.Fatalf("objects %d records %d retries %d", w.Objects.Load(), w.Records.Load(), w.Retries.Load())
	}
}
