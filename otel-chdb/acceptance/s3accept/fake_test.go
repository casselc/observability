package main

// The checks must catch the failures they exist for. A minimal in-memory S3
// (PUT/GET/HEAD/DELETE of single objects; signatures are not verified) runs
// in three modes:
//
//	honest  conditions evaluated under a lock (what AWS documents)
//	ignore  conditional headers accepted and ignored (old MinIO, Garage)
//	racy    condition checked, then the write done without holding a lock
//	        (SeaweedFS's multipart completion bug, applied to every PUT)
//
// and the conditional checks must PASS on the first and FAIL on the others.

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeS3 struct {
	mode string
	mu   sync.Mutex
	objs map[string][]byte
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path
	fail := func(code int, c string) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(code)
		if r.Method != http.MethodHead {
			fmt.Fprintf(w, "<Error><Code>%s</Code><Message>%s</Message></Error>", c, c)
		}
	}
	etag := func(b []byte) string { h := md5.Sum(b); return `"` + hex.EncodeToString(h[:]) + `"` }
	switch r.Method {
	case http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		inm, im := r.Header.Get("If-None-Match"), r.Header.Get("If-Match")
		check := func() (int, string) {
			cur, ok := f.objs[key]
			switch {
			case inm == "*" && ok:
				return 412, "PreconditionFailed"
			case im != "" && !ok:
				return 404, "NoSuchKey"
			case im != "" && strings.Trim(im, `"`) != strings.Trim(etag(cur), `"`):
				return 412, "PreconditionFailed"
			}
			return 0, ""
		}
		switch f.mode {
		case "honest":
			f.mu.Lock()
			if code, c := check(); code != 0 {
				f.mu.Unlock()
				fail(code, c)
				return
			}
			f.objs[key] = body
			f.mu.Unlock()
		case "ignore":
			time.Sleep(time.Millisecond)
			f.mu.Lock()
			f.objs[key] = body
			f.mu.Unlock()
		case "racy":
			f.mu.Lock()
			code, c := check()
			f.mu.Unlock()
			if code != 0 {
				fail(code, c)
				return
			}
			time.Sleep(2 * time.Millisecond) // the window between check and act
			f.mu.Lock()
			f.objs[key] = body
			f.mu.Unlock()
		}
		w.Header().Set("ETag", etag(body))
	case http.MethodGet, http.MethodHead:
		f.mu.Lock()
		b, ok := f.objs[key]
		f.mu.Unlock()
		if !ok {
			fail(404, "NoSuchKey")
			return
		}
		w.Header().Set("ETag", etag(b))
		w.Header().Set("Content-Length", fmt.Sprint(len(b)))
		if r.Method == http.MethodGet {
			w.Write(b)
		}
	case http.MethodDelete:
		f.mu.Lock()
		delete(f.objs, key)
		f.mu.Unlock()
		w.WriteHeader(204)
	default:
		fail(501, "NotImplemented")
	}
}

func fakeEnv(t *testing.T, mode string) *Env {
	t.Helper()
	srv := httptest.NewServer(&fakeS3{mode: mode, objs: map[string][]byte{}})
	t.Cleanup(srv.Close)
	t.Setenv("AWS_CONFIG_FILE", "/dev/null")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")
	o := &Opts{Endpoint: srv.URL, Bucket: "b", AccessKey: "k", SecretKey: "s", Store: "fake"}
	if err := o.resolve(); err != nil {
		t.Fatal(err)
	}
	e, err := newEnv(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	e.Run, e.Root = "run", "p/run"
	e.Params = Params{RaceWriters: 16, RaceRounds: 10, CASWriters: 8, CASIncrements: 10,
		AmbiguousN: 5, Settle: 20 * time.Millisecond}
	return e
}

func TestChecksCatchBrokenStores(t *testing.T) {
	type c struct {
		name string
		run  func(context.Context, *Env, *Result)
	}
	cs := []c{{"create-only", checkCreateOnly}, {"if-match", checkIfMatch}, {"create-race", checkCreateRace}, {"cas-race", checkCASRace}}
	want := map[string]map[string]Status{
		"honest": {"create-only": PASS, "if-match": PASS, "create-race": PASS, "cas-race": PASS},
		"ignore": {"create-only": FAIL, "if-match": FAIL, "create-race": FAIL, "cas-race": FAIL},
		"racy":   {"create-only": PASS, "if-match": PASS, "create-race": FAIL, "cas-race": FAIL},
	}
	for _, mode := range []string{"honest", "ignore", "racy"} {
		e := fakeEnv(t, mode)
		for _, ch := range cs {
			r := &Result{ID: ch.name}
			ch.run(context.Background(), e, r)
			if r.Status == "" {
				r.Status = PASS
			}
			t.Logf("%-6s %-12s %-4s %s", mode, ch.name, r.Status, r.Summary)
			if r.Status != want[mode][ch.name] {
				t.Errorf("%s/%s: %s (%s), want %s; evidence %v", mode, ch.name, r.Status, r.Summary, want[mode][ch.name], r.Evidence)
			}
		}
	}
}

func TestAmbiguousOnHonestFake(t *testing.T) {
	e := fakeEnv(t, "honest")
	for _, f := range []func(context.Context, *Env, *Result){checkAmbiguousCreate, checkAmbiguousCAS} {
		r := &Result{}
		f(context.Background(), e, r)
		t.Logf("%s %s %v", r.Status, r.Summary, r.Evidence)
		if r.Status != PASS {
			t.Errorf("%s: %s", r.Status, r.Summary)
		}
	}
}

func TestParseSizes(t *testing.T) {
	got, err := parseSizes("100KB,1MB,1MiB,512B")
	if err != nil || fmt.Sprint(got) != "[100000 1000000 1048576 512]" {
		t.Fatal(got, err)
	}
	if _, err := parseSizes("abc"); err == nil {
		t.Fatal("want error")
	}
}

func TestResolveURL(t *testing.T) {
	for _, tc := range []struct{ url, bucket, prefix, endpoint, store string }{
		{"s3://bkt/some/prefix/", "bkt", "some/prefix", "", "aws"},
		{"https://objects.example/bkt/edge", "bkt", "edge", "https://objects.example", "other"},
		{"http://127.0.0.1:18333/otel", "otel", "otel-accept", "http://127.0.0.1:18333", "local"},
	} {
		t.Setenv("AWS_ENDPOINT_URL_S3", "")
		o := &Opts{URL: tc.url, Prefix: "otel-accept"}
		if err := o.resolve(); err != nil {
			t.Fatal(err)
		}
		if o.Bucket != tc.bucket || o.Prefix != tc.prefix || o.Endpoint != tc.endpoint || o.Store != tc.store {
			t.Errorf("%s: %+v", tc.url, o)
		}
		if (tc.endpoint != "") != o.pathStyle() {
			t.Errorf("%s: path-style %v", tc.url, o.pathStyle())
		}
	}
}
