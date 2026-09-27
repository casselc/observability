// Package ambig tests how the Go edge's commit lane (../../../parquetgo/commit)
// resolves an S3 answer that lies about the outcome: the PUT applied, and the
// store (here ../cmd/faultproxy2 -mode commit-error) answered 500, 503 or 409
// anyway. aws-sdk-go-v2 retries a 5xx on its own; the retry of a create-only
// PUT then meets our own object and gets 412. The lane must recognise its
// own write (HEAD: content key and epoch) instead of taking the slot for
// another writer's (AMBIGUITY.md, audit a).
//
// It needs SeaweedFS (AMBIG_S3, default http://127.0.0.1:18333/otel) and the
// faultproxy2 binary (FAULTPROXY2); without FAULTPROXY2 it is skipped.
package ambig

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/casselc/observability/otel-chdb/parquetgo"
	"github.com/casselc/observability/otel-chdb/parquetgo/commit"
)

func freePort(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// proxy starts faultproxy2 in front of the store, faulting the first
// matching PUT only, and returns its address.
func proxy(t *testing.T, target string, args ...string) string {
	bin := os.Getenv("FAULTPROXY2")
	if bin == "" {
		t.Skip("FAULTPROXY2 (the faultproxy2 binary) not set")
	}
	addr := freePort(t)
	var log bytes.Buffer
	cmd := exec.Command(bin, append([]string{"-listen", addr, "-target", target, "-limit", "1"}, args...)...)
	cmd.Stderr = &log
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Logf("faultproxy2:\n%s", log.String())
	})
	for i := 0; i < 50; i++ {
		if c, err := net.Dial("tcp", addr); err == nil {
			c.Close()
			return addr
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("faultproxy2 did not start")
	return ""
}

func target() (endpoint, bucket string) {
	u := os.Getenv("AMBIG_S3")
	if u == "" {
		u = "http://127.0.0.1:18333/otel"
	}
	i := strings.LastIndex(u, "/")
	return u[:i], u[i+1:]
}

func client(t *testing.T, endpoint, bucket, prefix string) *s3.Client {
	c, _, _, err := parquetgo.NewS3Client(parquetgo.Config{URL: endpoint + "/" + bucket + "/" + prefix,
		AccessKeyID: "otel", SecretAccessKey: "otelsecret"})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

type rec struct{ events []commit.Event }

func (r *rec) Observe(e commit.Event) { r.events = append(r.events, e) }

func TestOwnWriteBehindAnError(t *testing.T) {
	endpoint, bucket := target()
	for _, st := range []int{500, 503, 409} {
		t.Run(fmt.Sprint(st), func(t *testing.T) {
			run := fmt.Sprintf("am-go-%d-%d", st, time.Now().UnixNano())
			p := proxy(t, strings.TrimPrefix(endpoint, ""), "-mode", "commit-error", "-status", fmt.Sprint(st), "-match", run)
			c := client(t, "http://"+p, bucket, run)
			direct := client(t, endpoint, bucket, run)
			obs := &rec{}
			l := &commit.Lane{Name: "t/0", Prefix: run + "/traces", Producer: "am", Observer: obs,
				Store: &commit.S3Store{Client: c, Bucket: bucket}, Timeouts: commit.Timeouts{Put: 5 * time.Second, Head: 2 * time.Second}}
			body := []byte("batch-1")
			ref, err := l.Append(context.Background(), "content-1", func(r commit.Ref) (commit.Object, error) {
				return commit.Object{Body: body, ContentType: "application/octet-stream", Meta: map[string]string{}}, nil
			})
			if err != nil {
				t.Fatalf("append: %v", err)
			}
			var kinds []string
			for _, e := range obs.events {
				kinds = append(kinds, e.Kind+"/"+e.Outcome.String())
			}
			t.Logf("events: %v", kinds)
			if ref.Seq != 0 {
				t.Fatalf("committed at slot %d, want 0 (our own write was taken for another writer's)", ref.Seq)
			}
			if l.Stats.LearnedOther.Load() != 0 {
				t.Fatalf("learnedOther %d: our own write was taken for another batch", l.Stats.LearnedOther.Load())
			}
			// Exactly one object under the lane, ours.
			out, err := direct.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(run + "/")})
			if err != nil {
				t.Fatal(err)
			}
			if len(out.Contents) != 1 {
				t.Fatalf("%d objects under the lane, want 1", len(out.Contents))
			}
			t.Logf("status %d: resolvedOwn %d, heads %d, puts %d", st, l.Stats.ResolvedOwn.Load(), l.Stats.Heads.Load(), l.Stats.Puts.Load())
			for _, o := range out.Contents {
				_, _ = direct.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: o.Key})
			}
		})
	}
}
