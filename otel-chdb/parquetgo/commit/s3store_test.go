package commit

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// A missing key answers 404 only to a caller with s3:ListBucket; without it
// (or with a ListBucket grant a HEAD's context does not match, DECISIONS
// D18 amendment of 2026-09-28) S3 answers 403. The publisher must read only
// 404 as "free": a 403 also comes from an expired or revoked credential for
// a slot that holds data, so it stays an error (AMBIGUITY S5) and the slot
// unresolved, never resent into.
func TestHeadOnlyA404IsFree(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/listable"):
			w.WriteHeader(http.StatusNotFound)
		case strings.HasSuffix(r.URL.Path, "/nolist"), strings.HasSuffix(r.URL.Path, "/expired"):
			w.WriteHeader(http.StatusForbidden)
		default:
			w.Header().Set("x-amz-meta-oscope-kind", "data")
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()
	st := &S3Store{Bucket: "b", Client: s3.New(s3.Options{
		BaseEndpoint: aws.String(srv.URL),
		Region:       "eu-west-2",
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider("k", "s", ""),
		Retryer:      aws.NopRetryer{},
	})}
	ctx := context.Background()
	if m, found, err := st.Head(ctx, "p/listable"); err != nil || found || m != nil {
		t.Fatalf("404: got %v %v %v, want free", m, found, err)
	}
	for _, k := range []string{"p/nolist", "p/expired"} {
		if _, found, err := st.Head(ctx, k); err == nil || found {
			t.Fatalf("403 on %s: got found=%v err=%v, want an error (unknown), not free", k, found, err)
		}
	}
	if m, found, err := st.Head(ctx, "p/there"); err != nil || !found || m["oscope-kind"] != "data" {
		t.Fatalf("200: got %v %v %v", m, found, err)
	}
}
