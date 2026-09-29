package lane

// Two writers appending to one lane (an overlap of incarnations) through a
// store that loses answers, answers 500 after applying, and applies requests
// late, in fake time (testing/synctest, httptest.NewTestServer): every
// flushed batch lands in exactly one object, and the store's history as the
// writers saw it is linearizable as one create-only register per key
// (../../../../casreg, porcupine).

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/casselc/observability/otel-chdb/casreg"
)

// faultyS3 is a create-only store with MD5 ETags and seeded faults.
type faultyS3 struct {
	mu   sync.Mutex
	objs map[string][]byte
	rng  *rand.Rand
	late sync.WaitGroup
}

func (f *faultyS3) fault() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rng.IntN(10)
}

func (f *faultyS3) apply(key string, b []byte, create bool) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.objs[key]; ok && create {
		return http.StatusPreconditionFailed
	}
	f.objs[key] = b
	return http.StatusOK
}

func (f *faultyS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path
	switch r.Method {
	case http.MethodHead, http.MethodGet:
		f.mu.Lock()
		b, ok := f.objs[key]
		f.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		h := md5.Sum(b)
		w.Header().Set("ETag", `"`+hex.EncodeToString(h[:])+`"`)
		return
	case http.MethodPut:
	default:
		w.WriteHeader(http.StatusNotImplemented)
		return
	}
	b, _ := io.ReadAll(r.Body)
	if strings.Contains(r.Header.Get("Content-Encoding"), "aws-chunked") {
		b, _ = casreg.DecodeAWSChunked(b)
	}
	create := r.Header.Get("If-None-Match") == "*"
	switch f.fault() {
	case 0: // applied, answer lost
		f.apply(key, b, create)
		panic(http.ErrAbortHandler)
	case 1: // applied, 500
		f.apply(key, b, create)
		w.WriteHeader(http.StatusInternalServerError)
		return
	case 2: // applied 30 s later, the connection cut now
		f.late.Add(1)
		go func() { defer f.late.Done(); time.Sleep(30 * time.Second); f.apply(key, b, create) }()
		panic(http.ErrAbortHandler)
	case 3: // 503, not applied
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	st := f.apply(key, b, create)
	if st == http.StatusOK {
		h := md5.Sum(b)
		w.Header().Set("ETag", `"`+hex.EncodeToString(h[:])+`"`)
	}
	w.WriteHeader(st)
}

func TestTwoWritersOneLaneLinearizable(t *testing.T) {
	for seed := range uint64(20) {
		synctest.Test(t, func(t *testing.T) {
			srv := &faultyS3{objs: map[string][]byte{}, rng: rand.New(rand.NewPCG(seed, 3))}
			hist := &casreg.Recorder{}
			base := httptest.NewTestServer(t, srv).Client()
			var ws []*Writer
			for i := range 2 {
				hc := &http.Client{Transport: &casreg.Transport{Base: base.Transport, Rec: hist, Client: i}}
				c := s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String("http://s3.test"), UsePathStyle: true,
					Credentials: credentials.NewStaticCredentialsProvider("k", "s", ""), HTTPClient: hc,
					RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired})
				ws = append(ws, &Writer{S3: c, Bucket: "b", Lane: "lanes/c1/1-i", PutTimeout: 5 * time.Second})
			}
			var wg sync.WaitGroup
			for i, w := range ws {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for n := range 8 {
						w.Add(Record{Level: LPod, Key: uint64(1000*i + n), Name: fmt.Sprintf("w%d-%d", i, n)})
						if err := w.Flush(context.Background()); err != nil {
							t.Error(err)
						}
						time.Sleep(time.Second)
					}
				}()
			}
			wg.Wait()
			srv.late.Wait()
			for _, w := range ws {
				if w.Objects.Load() != 8 {
					t.Fatalf("seed %d: %d objects written", seed, w.Objects.Load())
				}
			}
			// every batch in exactly one object: 16 objects, distinct bodies
			srv.mu.Lock()
			seen := map[string]string{}
			for k, b := range srv.objs {
				h := md5.Sum(b)
				s := hex.EncodeToString(h[:])
				if other, dup := seen[s]; dup {
					t.Fatalf("seed %d: one batch in %s and %s", seed, other, k)
				}
				seen[s] = k
			}
			srv.mu.Unlock()
			if len(seen) != 16 {
				t.Fatalf("seed %d: %d objects for 16 batches", seed, len(seen))
			}
			if res, _ := hist.Check(time.Minute); res != porcupine.Ok {
				t.Fatalf("seed %d: history of %d requests (%d unanswered): %s", seed, hist.Len(), hist.Pending(), res)
			}
		})
	}
}
